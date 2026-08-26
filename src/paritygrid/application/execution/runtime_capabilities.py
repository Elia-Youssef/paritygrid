"""Runtime capability detection and exact strategy registration (P7.15).

The catalog registers a full-plan strategy only when its exact
capabilities are present in the running interpreter, registers the
subordinate process pool separately as a subordinate capability, and
never substitutes one strategy for another.  Partial startup rolls
back in reverse order through the accepted lifecycle coordinator;
shutdown is idempotent and each owned strategy or pool starts and
shuts down exactly once.
"""

from __future__ import annotations

import multiprocessing
from dataclasses import dataclass

from paritygrid.application.execution.concurrency_settings import (
    CapturedConcurrencySettings,
    StrategyAvailability,
    StrategyLifecycleCoordinator,
    StrategyLifecycleListener,
    describe_known_strategies,
)

RUNTIME_CAPABILITIES_VERSION = 1
SUBORDINATE_PROCESS_POOL_ID = "subordinate-process-pool"
SUBORDINATE_INTERPRETER_POOL_ID = "subordinate-interpreter-pool"


class RuntimeCapabilityError(RuntimeError):
    """Base failure for runtime capability detection."""


class RuntimeCapabilityInvalidRequestError(RuntimeCapabilityError):
    """A registration request violated the catalog contract."""


@dataclass(frozen=True, slots=True)
class SubordinatePoolCapability:
    """One registered subordinate pool capability fact."""

    pool_id: str
    available: bool
    unavailability_reason: str | None

    def __post_init__(self) -> None:
        if type(self.pool_id) is not str or not self.pool_id:
            raise TypeError("subordinate pool identity must be text")
        if self.available and self.unavailability_reason is not None:
            raise RuntimeCapabilityInvalidRequestError(
                "an available pool carries no unavailability reason"
            )
        if not self.available and not self.unavailability_reason:
            raise RuntimeCapabilityInvalidRequestError(
                "an unavailable pool carries a structured reason"
            )


def detect_threaded_capability() -> bool:
    """Threaded execution is always available in the supported runtime."""
    return True


def detect_asyncio_capability() -> bool:
    """Asyncio execution is always available in the supported runtime."""
    return True


def detect_process_pool_capability() -> SubordinatePoolCapability:
    """Probe spawn-context availability for the subordinate process pool."""
    try:
        multiprocessing.get_context("spawn")
    except ValueError as error:
        return SubordinatePoolCapability(
            pool_id=SUBORDINATE_PROCESS_POOL_ID,
            available=False,
            unavailability_reason=f"spawn context unavailable: {error}",
        )
    return SubordinatePoolCapability(
        pool_id=SUBORDINATE_PROCESS_POOL_ID,
        available=True,
        unavailability_reason=None,
    )


def detect_interpreter_capability() -> SubordinatePoolCapability:
    """Probe the actual interpreter-pool support of this runtime."""
    try:
        from concurrent.futures import InterpreterPoolExecutor  # type: ignore[attr-defined]

        with InterpreterPoolExecutor(max_workers=1) as probe:
            probe.submit(int, 0).result(timeout=5.0)
    except Exception as error:
        return SubordinatePoolCapability(
            pool_id=SUBORDINATE_INTERPRETER_POOL_ID,
            available=False,
            unavailability_reason=f"interpreter pool unavailable: {error.__class__.__name__}",
        )
    return SubordinatePoolCapability(
        pool_id=SUBORDINATE_INTERPRETER_POOL_ID,
        available=True,
        unavailability_reason=None,
    )


class RuntimeStrategyCatalog:
    """Exact registration of full-plan strategies and subordinate pools.

    The catalog composes the accepted lifecycle coordinator: startup
    starts every available strategy exactly once, rolls back in reverse
    order on partial failure, and shutdown is idempotent.  The captured
    capability facts always match the options exposed for the run.
    """

    __slots__ = (
        "_full_plan",
        "_lifecycle",
        "_pools",
        "_settings",
        "_started",
    )

    def __init__(
        self,
        settings: CapturedConcurrencySettings,
        *,
        listener: StrategyLifecycleListener | None = None,
    ) -> None:
        if type(settings) is not CapturedConcurrencySettings:
            raise TypeError("catalog settings must use CapturedConcurrencySettings")
        self._settings = settings
        self._lifecycle = StrategyLifecycleCoordinator(settings, listener=listener)
        self._full_plan: tuple[StrategyAvailability, ...] = ()
        self._pools: tuple[SubordinatePoolCapability, ...] = ()
        self._started = False

    @property
    def full_plan_strategies(self) -> tuple[StrategyAvailability, ...]:
        """Return every registered full-plan availability in fixed order."""
        return self._full_plan

    @property
    def subordinate_pools(self) -> tuple[SubordinatePoolCapability, ...]:
        """Return every registered subordinate pool capability."""
        return self._pools

    def register_detected(
        self,
        *,
        threaded_available: bool | None = None,
        asyncio_available: bool | None = None,
    ) -> None:
        """Register every capability this runtime actually exposes.

        The explicit overrides exist for partial and unavailable
        environments: a failing capability drops its exact entry with a
        structured reason instead of substituting another strategy.
        """

        availabilities = list(describe_known_strategies(self._settings))
        if threaded_available is None:
            threaded_available = detect_threaded_capability()
        if asyncio_available is None:
            asyncio_available = detect_asyncio_capability()
        for index, availability in enumerate(availabilities):
            if availability.strategy_id == "threaded" and not threaded_available:
                availabilities[index] = StrategyAvailability(
                    strategy_id="threaded",
                    available=False,
                    unavailability_reason="threaded capability is unavailable",
                    capabilities=None,
                )
            if availability.strategy_id == "asyncio" and not asyncio_available:
                availabilities[index] = StrategyAvailability(
                    strategy_id="asyncio",
                    available=False,
                    unavailability_reason="asyncio capability is unavailable",
                    capabilities=None,
                )
        self._full_plan = tuple(availabilities)
        self._pools = (
            detect_process_pool_capability(),
            detect_interpreter_capability(),
        )

    def startup(self) -> None:
        """Start every registered full-plan strategy exactly once."""
        if self._started:
            raise RuntimeCapabilityInvalidRequestError("catalog already started")
        self._lifecycle.startup(self._full_plan)
        self._started = True

    def shutdown(self) -> None:
        """Shut every owned strategy down, idempotently."""
        self._lifecycle.shutdown()

    def resolve_full_plan(self, strategy_id: str) -> StrategyAvailability:
        """Return exactly the requested strategy fact, never a substitute."""
        for availability in self._full_plan:
            if availability.strategy_id == strategy_id:
                return availability
        raise RuntimeCapabilityInvalidRequestError(f"strategy {strategy_id!r} is not registered")

    def is_subordinate_pool(self, pool_id: str) -> bool:
        """Pools are subordinate capabilities, never full-plan runners."""
        return any(pool.pool_id == pool_id for pool in self._pools)


__all__ = [
    "RUNTIME_CAPABILITIES_VERSION",
    "SUBORDINATE_INTERPRETER_POOL_ID",
    "SUBORDINATE_PROCESS_POOL_ID",
    "RuntimeCapabilityError",
    "RuntimeCapabilityInvalidRequestError",
    "RuntimeStrategyCatalog",
    "SubordinatePoolCapability",
    "detect_asyncio_capability",
    "detect_interpreter_capability",
    "detect_process_pool_capability",
    "detect_threaded_capability",
]
