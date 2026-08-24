"""Immutable captured concurrency bounds and strategy availability facts."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from threading import Lock
from typing import Protocol, cast, runtime_checkable

from paritygrid.application.execution.runner_contract import (
    RUNNER_CONTRACT_VERSION,
    STRATEGY_CAPABILITIES_PROTOCOL,
    StrategyCapabilitiesV1,
)
from paritygrid.application.planner import ResourcePolicy

CONCURRENCY_SETTINGS_VERSION = 1
MIN_CAPTURED_LIMIT = 1
MAX_CAPTURED_LIMIT = 65_536
MAX_CAPTURED_BYTES = 2**31 - 1
MAX_CAPTURED_TIMEOUT_SECONDS = 86_400.0
MIN_CAPTURED_TIMEOUT_SECONDS = 0.001
MAX_STRATEGY_ID_LENGTH = 64
MAX_UNAVAILABILITY_REASON_LENGTH = 256

KNOWN_STRATEGY_IDS: tuple[str, ...] = ("sequential", "threaded", "asyncio")

_DEFAULT_WORK_LIMIT = 4
_DEFAULT_IN_FLIGHT_RECORDS = 16
_DEFAULT_CHANNEL_CAPACITY = 256
_DEFAULT_CONNECTOR_REQUESTS = 8
_DEFAULT_CPU_POOL_OPERATIONS = 4
_DEFAULT_RESPONSE_BYTES = 16 * 1_048_576
_DEFAULT_FILE_BYTES = 64 * 1_048_576
_DEFAULT_ARTIFACT_BYTES = 512 * 1_048_576
_DEFAULT_TIMEOUT_SECONDS = 60.0

_THREADED_UNAVAILABILITY_REASON = "threaded strategy awaits P7.12"
_ASYNCIO_UNAVAILABILITY_REASON = "asyncio strategy awaits P7.13"

_STRATEGY_ID_PATTERN: re.Pattern[str] = re.compile(r"[a-z][a-z0-9-]*")

_SETTINGS_INT_FIELD_NAMES: tuple[str, ...] = (
    "global_concurrent_work",
    "per_strategy_work",
    "per_node_work",
    "connector_requests",
    "cpu_pool_operations",
    "assignment_channel_capacity",
    "result_channel_capacity",
    "telemetry_capacity",
    "writer_channel_capacity",
    "max_in_flight_records",
    "max_response_bytes",
    "max_file_bytes",
    "max_artifact_bytes_per_run",
)
_SETTINGS_FLOAT_FIELD_NAMES: tuple[str, ...] = (
    "cleanup_timeout_seconds",
    "shutdown_timeout_seconds",
)
_SETTINGS_FIELD_NAMES = frozenset(
    {"version", *_SETTINGS_INT_FIELD_NAMES, *_SETTINGS_FLOAT_FIELD_NAMES}
)


class ConcurrencySettingsError(RuntimeError):
    """Base failure for captured concurrency settings or strategy ownership."""


class ConcurrencySettingsValidationError(ConcurrencySettingsError):
    """A captured settings value has an unsupported type, bound, or shape."""


class ConcurrencySettingsVersionError(ConcurrencySettingsError):
    """A captured settings snapshot uses an unknown version."""


class StrategyAvailabilityError(ConcurrencySettingsError):
    """A requested strategy is unknown, malformed, or unsupported."""


class StrategyLifecycleError(ConcurrencySettingsError):
    """A strategy ownership boundary was violated."""


def _validate_captured_int(value: object, minimum: int, maximum: int, subject: str) -> None:
    if type(value) is not int:
        raise ConcurrencySettingsValidationError(f"{subject} must be an integer")
    if not minimum <= value <= maximum:
        raise ConcurrencySettingsValidationError(f"{subject} is outside the supported range")


def _validate_captured_timeout(value: object, subject: str) -> None:
    if type(value) is not float:
        raise ConcurrencySettingsValidationError(f"{subject} must be a float")
    timeout = value
    if not MIN_CAPTURED_TIMEOUT_SECONDS <= timeout <= MAX_CAPTURED_TIMEOUT_SECONDS:
        raise ConcurrencySettingsValidationError(f"{subject} is outside the supported range")


def _validate_strategy_id(value: object) -> None:
    if type(value) is not str:
        raise StrategyAvailabilityError("strategy identifier must be text")
    strategy_id = value
    if not 1 <= len(strategy_id) <= MAX_STRATEGY_ID_LENGTH:
        raise StrategyAvailabilityError("strategy identifier length is outside the supported range")
    if _STRATEGY_ID_PATTERN.fullmatch(strategy_id) is None:
        raise StrategyAvailabilityError("strategy identifier must use a lowercase strategy name")


def _validate_unavailability_reason(value: object) -> None:
    if type(value) is not str:
        raise StrategyAvailabilityError("unavailability reason must be text")
    reason = value
    if not 1 <= len(reason) <= MAX_UNAVAILABILITY_REASON_LENGTH:
        raise StrategyAvailabilityError(
            "unavailability reason length is outside the supported range"
        )
    for character in reason:
        if not "\x20" <= character <= "\x7e":
            raise StrategyAvailabilityError(
                "unavailability reason must use printable ASCII characters"
            )


@dataclass(frozen=True, slots=True, repr=False)
class CapturedConcurrencySettings:
    """Immutable snapshot of every concurrency bound captured for one run.

    Every value is validated before any resource allocation could depend on
    it; nothing here creates a pool, channel, or worker.
    """

    version: int = CONCURRENCY_SETTINGS_VERSION
    global_concurrent_work: int = _DEFAULT_WORK_LIMIT
    per_strategy_work: int = _DEFAULT_WORK_LIMIT
    per_node_work: int = _DEFAULT_WORK_LIMIT
    connector_requests: int = _DEFAULT_CONNECTOR_REQUESTS
    cpu_pool_operations: int = _DEFAULT_CPU_POOL_OPERATIONS
    assignment_channel_capacity: int = _DEFAULT_CHANNEL_CAPACITY
    result_channel_capacity: int = _DEFAULT_CHANNEL_CAPACITY
    telemetry_capacity: int = _DEFAULT_CHANNEL_CAPACITY
    writer_channel_capacity: int = _DEFAULT_CHANNEL_CAPACITY
    max_in_flight_records: int = _DEFAULT_IN_FLIGHT_RECORDS
    max_response_bytes: int = _DEFAULT_RESPONSE_BYTES
    max_file_bytes: int = _DEFAULT_FILE_BYTES
    max_artifact_bytes_per_run: int = _DEFAULT_ARTIFACT_BYTES
    cleanup_timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS
    shutdown_timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        if type(self.version) is not int:
            raise ConcurrencySettingsValidationError("captured settings version must be an integer")
        if self.version != CONCURRENCY_SETTINGS_VERSION:
            raise ConcurrencySettingsVersionError(
                f"captured settings version must equal {CONCURRENCY_SETTINGS_VERSION}"
            )
        _validate_captured_int(
            self.global_concurrent_work,
            MIN_CAPTURED_LIMIT,
            MAX_CAPTURED_LIMIT,
            "captured global concurrent work",
        )
        _validate_captured_int(
            self.per_strategy_work,
            MIN_CAPTURED_LIMIT,
            MAX_CAPTURED_LIMIT,
            "captured per-strategy work",
        )
        _validate_captured_int(
            self.per_node_work,
            MIN_CAPTURED_LIMIT,
            MAX_CAPTURED_LIMIT,
            "captured per-node work",
        )
        _validate_captured_int(
            self.connector_requests,
            MIN_CAPTURED_LIMIT,
            MAX_CAPTURED_LIMIT,
            "captured connector requests",
        )
        _validate_captured_int(
            self.cpu_pool_operations,
            MIN_CAPTURED_LIMIT,
            MAX_CAPTURED_LIMIT,
            "captured CPU-pool operations",
        )
        _validate_captured_int(
            self.assignment_channel_capacity,
            MIN_CAPTURED_LIMIT,
            MAX_CAPTURED_LIMIT,
            "captured assignment channel capacity",
        )
        _validate_captured_int(
            self.result_channel_capacity,
            MIN_CAPTURED_LIMIT,
            MAX_CAPTURED_LIMIT,
            "captured result channel capacity",
        )
        _validate_captured_int(
            self.telemetry_capacity,
            MIN_CAPTURED_LIMIT,
            MAX_CAPTURED_LIMIT,
            "captured telemetry capacity",
        )
        _validate_captured_int(
            self.writer_channel_capacity,
            MIN_CAPTURED_LIMIT,
            MAX_CAPTURED_LIMIT,
            "captured writer channel capacity",
        )
        _validate_captured_int(
            self.max_in_flight_records,
            MIN_CAPTURED_LIMIT,
            MAX_CAPTURED_LIMIT,
            "captured in-flight record bound",
        )
        _validate_captured_int(
            self.max_response_bytes,
            1,
            MAX_CAPTURED_BYTES,
            "captured response byte bound",
        )
        _validate_captured_int(
            self.max_file_bytes,
            1,
            MAX_CAPTURED_BYTES,
            "captured file byte bound",
        )
        _validate_captured_int(
            self.max_artifact_bytes_per_run,
            1,
            MAX_CAPTURED_BYTES,
            "captured artifact byte bound",
        )
        _validate_captured_timeout(self.cleanup_timeout_seconds, "captured cleanup timeout")
        _validate_captured_timeout(self.shutdown_timeout_seconds, "captured shutdown timeout")
        if (
            self.global_concurrent_work < self.per_strategy_work
            or self.global_concurrent_work < self.per_node_work
        ):
            raise ConcurrencySettingsValidationError(
                "captured global concurrent work must cover per-strategy and per-node work"
            )

    @classmethod
    def from_resource_policy(
        cls,
        policy: ResourcePolicy,
        *,
        global_concurrent_work: int | None = None,
    ) -> CapturedConcurrencySettings:
        """Derive one settings snapshot from a planner resource policy."""
        if type(policy) is not ResourcePolicy:
            raise TypeError("resource policy must use ResourcePolicy")
        selected_global = (
            policy.max_concurrency if global_concurrent_work is None else global_concurrent_work
        )
        return cls(
            global_concurrent_work=selected_global,
            per_strategy_work=policy.max_concurrency,
            per_node_work=policy.max_concurrency,
            connector_requests=_DEFAULT_CONNECTOR_REQUESTS,
            cpu_pool_operations=_DEFAULT_CPU_POOL_OPERATIONS,
            assignment_channel_capacity=policy.queue_capacity,
            result_channel_capacity=policy.queue_capacity,
            telemetry_capacity=policy.queue_capacity,
            writer_channel_capacity=policy.queue_capacity,
            max_in_flight_records=policy.max_in_flight,
            max_response_bytes=_DEFAULT_RESPONSE_BYTES,
            max_file_bytes=_DEFAULT_FILE_BYTES,
            max_artifact_bytes_per_run=policy.memory_limit_bytes,
            cleanup_timeout_seconds=float(policy.operation_timeout_seconds),
            shutdown_timeout_seconds=float(policy.operation_timeout_seconds),
        )

    def to_mapping(self) -> dict[str, int | float]:
        """Return the exact total settings object for version 1 snapshots."""
        return {
            "version": self.version,
            "global_concurrent_work": self.global_concurrent_work,
            "per_strategy_work": self.per_strategy_work,
            "per_node_work": self.per_node_work,
            "connector_requests": self.connector_requests,
            "cpu_pool_operations": self.cpu_pool_operations,
            "assignment_channel_capacity": self.assignment_channel_capacity,
            "result_channel_capacity": self.result_channel_capacity,
            "telemetry_capacity": self.telemetry_capacity,
            "writer_channel_capacity": self.writer_channel_capacity,
            "max_in_flight_records": self.max_in_flight_records,
            "max_response_bytes": self.max_response_bytes,
            "max_file_bytes": self.max_file_bytes,
            "max_artifact_bytes_per_run": self.max_artifact_bytes_per_run,
            "cleanup_timeout_seconds": self.cleanup_timeout_seconds,
            "shutdown_timeout_seconds": self.shutdown_timeout_seconds,
        }

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, object]) -> CapturedConcurrencySettings:
        """Rebuild one settings snapshot from a strict total mapping, failing closed."""
        if type(mapping) is not dict:
            raise TypeError("captured settings mapping must be a dictionary")
        entries = cast(dict[str, object], mapping)
        if frozenset(entries) != _SETTINGS_FIELD_NAMES:
            raise ConcurrencySettingsValidationError(
                "captured settings mapping keys must match the settings fields exactly"
            )
        version = entries["version"]
        if type(version) is not int:
            raise ConcurrencySettingsValidationError("captured settings version must be an integer")
        if version != CONCURRENCY_SETTINGS_VERSION:
            raise ConcurrencySettingsVersionError(
                f"captured settings version must equal {CONCURRENCY_SETTINGS_VERSION}"
            )
        for name in _SETTINGS_INT_FIELD_NAMES:
            if type(entries[name]) is not int:
                raise ConcurrencySettingsValidationError(
                    f"captured settings field {name!r} must be an integer"
                )
        for name in _SETTINGS_FLOAT_FIELD_NAMES:
            if type(entries[name]) is not float:
                raise ConcurrencySettingsValidationError(
                    f"captured settings field {name!r} must be a float"
                )
        return cls(
            version=version,
            global_concurrent_work=cast(int, entries["global_concurrent_work"]),
            per_strategy_work=cast(int, entries["per_strategy_work"]),
            per_node_work=cast(int, entries["per_node_work"]),
            connector_requests=cast(int, entries["connector_requests"]),
            cpu_pool_operations=cast(int, entries["cpu_pool_operations"]),
            assignment_channel_capacity=cast(int, entries["assignment_channel_capacity"]),
            result_channel_capacity=cast(int, entries["result_channel_capacity"]),
            telemetry_capacity=cast(int, entries["telemetry_capacity"]),
            writer_channel_capacity=cast(int, entries["writer_channel_capacity"]),
            max_in_flight_records=cast(int, entries["max_in_flight_records"]),
            max_response_bytes=cast(int, entries["max_response_bytes"]),
            max_file_bytes=cast(int, entries["max_file_bytes"]),
            max_artifact_bytes_per_run=cast(int, entries["max_artifact_bytes_per_run"]),
            cleanup_timeout_seconds=cast(float, entries["cleanup_timeout_seconds"]),
            shutdown_timeout_seconds=cast(float, entries["shutdown_timeout_seconds"]),
        )

    def __repr__(self) -> str:
        return (
            "CapturedConcurrencySettings("
            f"version={self.version!r}, "
            f"global_concurrent_work={self.global_concurrent_work!r}, "
            f"per_strategy_work={self.per_strategy_work!r}, "
            f"per_node_work={self.per_node_work!r}, "
            f"connector_requests={self.connector_requests!r}, "
            f"cpu_pool_operations={self.cpu_pool_operations!r}, "
            f"assignment_channel_capacity={self.assignment_channel_capacity!r}, "
            f"result_channel_capacity={self.result_channel_capacity!r}, "
            f"telemetry_capacity={self.telemetry_capacity!r}, "
            f"writer_channel_capacity={self.writer_channel_capacity!r}, "
            f"max_in_flight_records={self.max_in_flight_records!r}, "
            f"max_response_bytes={self.max_response_bytes!r}, "
            f"max_file_bytes={self.max_file_bytes!r}, "
            f"max_artifact_bytes_per_run={self.max_artifact_bytes_per_run!r}, "
            f"cleanup_timeout_seconds={self.cleanup_timeout_seconds!r}, "
            f"shutdown_timeout_seconds={self.shutdown_timeout_seconds!r})"
        )


@dataclass(frozen=True, slots=True, repr=False)
class StrategyAvailability:
    """Immutable availability fact for one runner strategy."""

    strategy_id: str
    available: bool
    unavailability_reason: str | None
    capabilities: StrategyCapabilitiesV1 | None

    def __post_init__(self) -> None:
        _validate_strategy_id(self.strategy_id)
        if type(self.available) is not bool:
            raise StrategyAvailabilityError("strategy availability must be a boolean")
        reason = self.unavailability_reason
        capabilities = self.capabilities
        if self.available:
            if reason is not None:
                raise StrategyAvailabilityError(
                    "available strategies must not carry an unavailability reason"
                )
            if type(capabilities) is not StrategyCapabilitiesV1:
                raise StrategyAvailabilityError("available strategies must carry capabilities")
        else:
            _validate_unavailability_reason(reason)
            if capabilities is not None:
                raise StrategyAvailabilityError(
                    "unavailable strategies must not carry capabilities"
                )

    def __repr__(self) -> str:
        capabilities = "present" if self.capabilities is not None else "absent"
        return (
            "StrategyAvailability("
            f"strategy_id={self.strategy_id!r}, available={self.available!r}, "
            f"unavailability_reason={self.unavailability_reason!r}, "
            f"capabilities=<{capabilities}>)"
        )


def describe_known_strategies(
    settings: CapturedConcurrencySettings,
) -> tuple[StrategyAvailability, ...]:
    """Return one availability fact per known strategy in constant order."""
    if type(settings) is not CapturedConcurrencySettings:
        raise TypeError("captured settings must use CapturedConcurrencySettings")
    sequential_capabilities = StrategyCapabilitiesV1(
        strategy_id="sequential",
        contract_version=RUNNER_CONTRACT_VERSION,
        supports_pause=True,
        supports_cancel=True,
        supports_checkpoint=True,
        max_concurrent_work=1,
        max_in_flight_records=settings.max_in_flight_records,
        platform_requirements=(),
        protocol=STRATEGY_CAPABILITIES_PROTOCOL,
    )
    return (
        StrategyAvailability(
            strategy_id="sequential",
            available=True,
            unavailability_reason=None,
            capabilities=sequential_capabilities,
        ),
        StrategyAvailability(
            strategy_id="threaded",
            available=False,
            unavailability_reason=_THREADED_UNAVAILABILITY_REASON,
            capabilities=None,
        ),
        StrategyAvailability(
            strategy_id="asyncio",
            available=False,
            unavailability_reason=_ASYNCIO_UNAVAILABILITY_REASON,
            capabilities=None,
        ),
    )


def resolve_strategy(
    strategy_id: str,
    settings: CapturedConcurrencySettings,
) -> StrategyAvailability:
    """Return the availability of exactly the requested strategy, never a substitute."""
    if type(strategy_id) is not str:
        raise TypeError("strategy identifier must be text")
    if type(settings) is not CapturedConcurrencySettings:
        raise TypeError("captured settings must use CapturedConcurrencySettings")
    for availability in describe_known_strategies(settings):
        if availability.strategy_id == strategy_id:
            return availability
    raise StrategyAvailabilityError(f"strategy {strategy_id!r} is not a known strategy")


@runtime_checkable
class StrategyLifecycleListener(Protocol):
    """Observer notified at the exact strategy ownership boundaries."""

    def on_strategy_started(self, strategy_id: str) -> None:
        """Called once when ownership of one strategy starts."""
        ...

    def on_strategy_shutdown(self, strategy_id: str) -> None:
        """Called once when ownership of one strategy is released."""
        ...


class _NoOpStrategyLifecycleListener:
    """Default listener used when ownership observation is not required."""

    __slots__ = ()

    def on_strategy_started(self, strategy_id: str) -> None:
        """Ignore one strategy startup boundary."""
        return None

    def on_strategy_shutdown(self, strategy_id: str) -> None:
        """Ignore one strategy shutdown boundary."""
        return None


_NOOP_LISTENER = _NoOpStrategyLifecycleListener()


class StrategyLifecycleCoordinator:
    """Own exactly-once startup and shutdown for the chosen strategies.

    The coordinator records ownership boundaries only; it never builds or
    runs a strategy, pool, channel, or worker itself.
    """

    __slots__ = (
        "_is_shutdown",
        "_is_started",
        "_listener",
        "_lock",
        "_owned",
        "_settings",
    )

    def __init__(
        self,
        settings: CapturedConcurrencySettings,
        *,
        listener: StrategyLifecycleListener | None = None,
    ) -> None:
        if type(settings) is not CapturedConcurrencySettings:
            raise TypeError("strategy lifecycle settings must use CapturedConcurrencySettings")
        if listener is None:
            selected_listener: StrategyLifecycleListener = _NOOP_LISTENER
        else:
            candidate = cast(object, listener)
            if not isinstance(candidate, StrategyLifecycleListener):
                raise TypeError(
                    "strategy lifecycle listener must implement StrategyLifecycleListener"
                )
            selected_listener = candidate
        self._settings = settings
        self._listener = selected_listener
        self._lock = Lock()
        self._owned: tuple[str, ...] = ()
        self._is_started = False
        self._is_shutdown = False

    @property
    def owned_strategies(self) -> tuple[str, ...]:
        """Return the currently owned strategy identifiers in startup order."""
        with self._lock:
            return self._owned

    @property
    def is_started(self) -> bool:
        """Return whether startup has completed and shutdown has not."""
        with self._lock:
            return self._is_started

    @property
    def is_shutdown(self) -> bool:
        """Return whether shutdown has completed."""
        with self._lock:
            return self._is_shutdown

    def startup(self, availabilities: tuple[StrategyAvailability, ...]) -> None:
        """Take exactly-once ownership of every chosen available strategy in order."""
        if type(availabilities) is not tuple:
            raise TypeError("strategy lifecycle startup availabilities must be a tuple")
        entries = cast(tuple[object, ...], availabilities)
        for entry in entries:
            if type(entry) is not StrategyAvailability:
                raise TypeError("strategy lifecycle startup entries must use StrategyAvailability")
        with self._lock:
            if self._is_started or self._is_shutdown:
                raise StrategyLifecycleError(
                    "strategy lifecycle startup ownership is granted exactly once"
                )
            for entry in entries:
                availability = cast(StrategyAvailability, entry)
                if not availability.available or availability.capabilities is None:
                    raise StrategyLifecycleError(
                        f"strategy {availability.strategy_id!r} is not available for this run"
                    )
            owned: list[str] = []
            self._is_started = True
            try:
                for entry in entries:
                    availability = cast(StrategyAvailability, entry)
                    self._listener.on_strategy_started(availability.strategy_id)
                    owned.append(availability.strategy_id)
                    self._owned = tuple(owned)
            except BaseException:
                for strategy_id in reversed(owned):
                    self._listener.on_strategy_shutdown(strategy_id)
                self._owned = ()
                self._is_started = False
                raise

    def shutdown(self) -> None:
        """Release every owned strategy in reverse startup order, idempotently."""
        with self._lock:
            if self._is_shutdown:
                return
            if not self._is_started:
                raise StrategyLifecycleError(
                    "strategy lifecycle shutdown requires a started lifecycle"
                )
            owned = self._owned
            self._owned = ()
            self._is_started = False
            self._is_shutdown = True
            failed: list[str] = []
            for strategy_id in reversed(owned):
                try:
                    self._listener.on_strategy_shutdown(strategy_id)
                except Exception:
                    failed.append(strategy_id)
            if failed:
                raise StrategyLifecycleError(
                    f"strategy shutdown listeners failed for {len(failed)} "
                    f"of {len(owned)} owned strategies"
                )


__all__ = [
    "CONCURRENCY_SETTINGS_VERSION",
    "KNOWN_STRATEGY_IDS",
    "MAX_CAPTURED_BYTES",
    "MAX_CAPTURED_LIMIT",
    "MAX_CAPTURED_TIMEOUT_SECONDS",
    "MAX_STRATEGY_ID_LENGTH",
    "MAX_UNAVAILABILITY_REASON_LENGTH",
    "MIN_CAPTURED_LIMIT",
    "MIN_CAPTURED_TIMEOUT_SECONDS",
    "CapturedConcurrencySettings",
    "ConcurrencySettingsError",
    "ConcurrencySettingsValidationError",
    "ConcurrencySettingsVersionError",
    "StrategyAvailability",
    "StrategyAvailabilityError",
    "StrategyLifecycleCoordinator",
    "StrategyLifecycleError",
    "StrategyLifecycleListener",
    "describe_known_strategies",
    "resolve_strategy",
]
