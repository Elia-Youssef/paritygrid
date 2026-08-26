"""Simulator lifecycle composition with dynamic ports and owned resources.

:class:`SimulatorStack` starts the three demo services (async source,
blocking source, warehouse) in a fixed order, readiness-probes each over real
loopback HTTP, and publishes endpoints only after every probe succeeds. Any
failure, timeout, or cancellation during startup rolls back every service
already started, in reverse order. Shutdown is idempotent and safe to repeat
after success, failure, timeout, or cancellation.
"""

import asyncio
import contextlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from paritygrid.demo.datasets import SyntheticDataset
from paritygrid.demo.failures import FailureScript
from paritygrid.demo.simulators.async_source import AsyncInventorySource
from paritygrid.demo.simulators.blocking_source import BlockingInventorySource
from paritygrid.demo.simulators.warehouse import (
    SimulatedWarehouse,
    WarehouseSettings,
)

DEFAULT_STARTUP_TIMEOUT_MICROSECONDS = 10_000_000
DEFAULT_PROBE_TIMEOUT_MICROSECONDS = 2_000_000
_PROBE_HOST = "127.0.0.1"

_OwnedService = AsyncInventorySource | BlockingInventorySource | SimulatedWarehouse


class SimulatorLifecycleError(RuntimeError):
    """Raised when the simulator stack is misused or fails to start."""


class SimulatorStartupFault(StrEnum):
    """Controlled startup faults used to exercise rollback behavior."""

    NONE = "none"
    ASYNC_SOURCE = "async-source"
    BLOCKING_SOURCE = "blocking-source"
    WAREHOUSE = "warehouse"


@dataclass(frozen=True, slots=True)
class SimulatorStackConfig:
    """Bounded lifecycle settings for one simulator stack."""

    startup_timeout_microseconds: int = DEFAULT_STARTUP_TIMEOUT_MICROSECONDS
    probe_timeout_microseconds: int = DEFAULT_PROBE_TIMEOUT_MICROSECONDS
    fault: SimulatorStartupFault = SimulatorStartupFault.NONE
    max_page_size: int = 200
    source_request_latency_microseconds: int = 0

    def __post_init__(self) -> None:
        for name in ("startup_timeout_microseconds", "probe_timeout_microseconds"):
            object.__setattr__(self, name, _validated_timeout(getattr(self, name), field=name))
        object.__setattr__(self, "fault", _validated_fault(self.fault))
        object.__setattr__(self, "max_page_size", _validated_page_size(self.max_page_size))
        object.__setattr__(
            self,
            "source_request_latency_microseconds",
            _validated_latency(self.source_request_latency_microseconds),
        )


@dataclass(frozen=True, slots=True)
class SimulatorEndpoints:
    """Published readiness of a fully started simulator stack."""

    async_source_base_url: str
    async_source_port: int
    blocking_source_base_url: str
    blocking_source_port: int
    warehouse_base_url: str
    warehouse_port: int


class SimulatorStack:
    """Owns every simulator resource for one demo composition."""

    def __init__(
        self,
        dataset: SyntheticDataset,
        *,
        source_script: FailureScript | None = None,
        warehouse_script: FailureScript | None = None,
        warehouse_settings: WarehouseSettings | None = None,
        config: SimulatorStackConfig | None = None,
    ) -> None:
        self._dataset = dataset
        self._source_script = source_script if source_script is not None else FailureScript.empty()
        self._warehouse_script = (
            warehouse_script if warehouse_script is not None else FailureScript.empty()
        )
        self._warehouse_settings = warehouse_settings
        self._config = config if config is not None else SimulatorStackConfig()
        self._async_source: AsyncInventorySource | None = None
        self._blocking_source: BlockingInventorySource | None = None
        self._warehouse: SimulatedWarehouse | None = None
        self._started_services: list[tuple[str, _OwnedService]] = []
        self._endpoints: SimulatorEndpoints | None = None
        self._closed = False

    @property
    def config(self) -> SimulatorStackConfig:
        """Return the frozen lifecycle configuration."""
        return self._config

    @property
    def async_source(self) -> AsyncInventorySource:
        """Return the owned async source once created."""
        if self._async_source is None:
            raise SimulatorLifecycleError("the async source has not been created")
        return self._async_source

    @property
    def blocking_source(self) -> BlockingInventorySource:
        """Return the owned blocking source once created."""
        if self._blocking_source is None:
            raise SimulatorLifecycleError("the blocking source has not been created")
        return self._blocking_source

    @property
    def warehouse(self) -> SimulatedWarehouse:
        """Return the owned warehouse once created."""
        if self._warehouse is None:
            raise SimulatorLifecycleError("the warehouse has not been created")
        return self._warehouse

    @property
    def endpoints(self) -> SimulatorEndpoints:
        """Return the published endpoints of a successfully started stack."""
        if self._endpoints is None:
            raise SimulatorLifecycleError("the simulator stack has not completed startup")
        return self._endpoints

    def is_started(self) -> bool:
        """Report whether startup completed and published endpoints."""
        return self._endpoints is not None

    def is_closed(self) -> bool:
        """Report whether shutdown has run."""
        return self._closed

    async def start(self) -> SimulatorEndpoints:
        """Start every service, probe readiness, and publish endpoints."""
        if self._closed:
            raise SimulatorLifecycleError("the simulator stack was closed and cannot restart")
        if self._endpoints is not None:
            raise SimulatorLifecycleError("the simulator stack already started")
        try:
            await asyncio.wait_for(
                self._start_services(),
                timeout=self._config.startup_timeout_microseconds / 1_000_000,
            )
        except BaseException:
            await self._rollback()
            raise
        self._endpoints = SimulatorEndpoints(
            async_source_base_url=self.async_source.base_url,
            async_source_port=self.async_source.port,
            blocking_source_base_url=self.blocking_source.base_url,
            blocking_source_port=self.blocking_source.port,
            warehouse_base_url=self.warehouse.base_url,
            warehouse_port=self.warehouse.port,
        )
        return self._endpoints

    async def aclose(self) -> None:
        """Close every owned service in reverse start order; idempotent."""
        if self._closed:
            return
        self._closed = True
        await self._rollback()

    async def _start_services(self) -> None:
        config = self._config
        if config.fault is SimulatorStartupFault.ASYNC_SOURCE:
            raise SimulatorLifecycleError("Injected startup fault: async-source")
        async_source = AsyncInventorySource(
            self._dataset,
            self._source_script,
            max_page_size=config.max_page_size,
            request_latency_microseconds=config.source_request_latency_microseconds,
        )
        await async_source.start()
        self._async_source = async_source
        self._started_services.append(("async-source", async_source))
        await self._probe(async_source.base_url, expected_service="async-source")

        blocking_source = BlockingInventorySource(
            self._dataset,
            self._source_script,
            max_page_size=config.max_page_size,
            request_latency_microseconds=config.source_request_latency_microseconds,
        )
        blocking_source.start()
        self._blocking_source = blocking_source
        self._started_services.append(("blocking-source", blocking_source))
        if config.fault is SimulatorStartupFault.BLOCKING_SOURCE:
            raise SimulatorLifecycleError("Injected startup fault: blocking-source")
        await self._probe(blocking_source.base_url, expected_service="blocking-source")

        warehouse = SimulatedWarehouse(
            self._warehouse_script,
            settings=self._warehouse_settings,
        )
        await warehouse.start()
        self._warehouse = warehouse
        self._started_services.append(("warehouse", warehouse))
        if config.fault is SimulatorStartupFault.WAREHOUSE:
            raise SimulatorLifecycleError("Injected startup fault: warehouse")
        await self._probe(warehouse.base_url, expected_service="warehouse")

    async def _rollback(self) -> None:
        """Close every created service in reverse creation order."""
        while self._started_services:
            _name, service = self._started_services.pop()
            try:
                await service.aclose()
            except OSError, RuntimeError:
                # Rollback must continue even when one service fails to close.
                continue

    async def _probe(self, base_url: str, *, expected_service: str) -> None:
        """Readiness-probe one service over real loopback HTTP."""
        await probe_service_health(
            base_url,
            expected_service=expected_service,
            timeout_seconds=self._config.probe_timeout_microseconds / 1_000_000,
        )


async def probe_service_health(
    base_url: str,
    *,
    expected_service: str,
    timeout_seconds: float,
) -> None:
    """Require a real loopback health response before readiness is published.

    The probe is intentionally strict: the status line, the JSON body, the
    ``ok`` status, and the expected service identity must all match, so a
    stack never publishes endpoints for a service that is not ready.
    """
    authority = base_url.removeprefix("http://")
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(_PROBE_HOST, _port_of(base_url)), timeout=timeout_seconds
    )
    try:
        request = (
            f"GET /healthz HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n"
        ).encode("ascii")
        writer.write(request)
        await asyncio.wait_for(writer.drain(), timeout=timeout_seconds)
        raw = await asyncio.wait_for(reader.read(-1), timeout=timeout_seconds)
    finally:
        writer.close()
        with contextlib.suppress(ConnectionError, OSError):
            await writer.wait_closed()
    head, _, body = raw.partition(b"\r\n\r\n")
    status_line = head.split(b"\r\n", 1)[0]
    parts = status_line.split(b" ")
    if len(parts) < 2 or parts[1] != b"200":
        raise SimulatorLifecycleError(f"readiness probe for {expected_service} did not return 200")
    try:
        document_value = json.loads(body.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SimulatorLifecycleError(
            f"readiness probe for {expected_service} returned an unreadable body"
        ) from error
    if not isinstance(document_value, dict):
        raise SimulatorLifecycleError(
            f"readiness probe for {expected_service} returned a non-object body"
        )
    document = cast("dict[str, object]", document_value)
    if document.get("status") != "ok":
        raise SimulatorLifecycleError(f"readiness probe for {expected_service} saw no ok status")
    if document.get("service") != expected_service:
        raise SimulatorLifecycleError(
            f"readiness probe for {expected_service} reached a different service"
        )


def _port_of(base_url: str) -> int:
    return int(base_url.rsplit(":", 1)[1])


def _validated_timeout(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SimulatorLifecycleError(f"{field} must be integer microseconds")
    if not 1 <= value <= 60_000_000:
        raise SimulatorLifecycleError(f"{field} must be between 1 microsecond and 60 seconds")
    return value


def _validated_fault(value: object) -> SimulatorStartupFault:
    if not isinstance(value, SimulatorStartupFault):
        raise SimulatorLifecycleError("fault must be a SimulatorStartupFault")
    return value


def _validated_page_size(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SimulatorLifecycleError("max page size must be an integer")
    if not 1 <= value <= 200:
        raise SimulatorLifecycleError("max page size must be between 1 and 200")
    return value


def _validated_latency(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SimulatorLifecycleError("source latency must be integer microseconds")
    if not 0 <= value <= 60_000_000:
        raise SimulatorLifecycleError("source latency must be between 0 and 60 seconds")
    return value
