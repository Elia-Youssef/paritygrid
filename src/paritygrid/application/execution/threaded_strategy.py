"""Bounded threaded full-plan strategy (P7.12).

The threaded strategy owns a bounded pool of named worker threads.  Each
worker applies the shared :func:`execute_one_assignment` discipline:
pull one immutable assignment envelope from the bounded assignment
channel, execute it through the parent-supplied operation executor,
record its commit facts, and push the immutable result envelope through
the bounded result channel.  Workers never see the scheduler, capacity
ledger, leases, artifacts, or any writer.

Backpressure is structural: a full result channel blocks the worker,
which stops pulling assignments, which stops admission.  Shutdown
closes the assignment channel from the parent side, joins every owned
thread inside the captured bound, and reports diagnostics.  A worker
that fails is replaced only by the remaining pool — accepted work is
never silently lost because every pulled assignment either returns a
result envelope or a synthesized durable failure envelope.
"""

from __future__ import annotations

import math
from threading import Event, Thread

from paritygrid.application.execution.concurrency_settings import (
    MAX_CAPTURED_LIMIT,
    CapturedConcurrencySettings,
)
from paritygrid.application.execution.full_plan_strategy import (
    FullPlanStrategyError,
    FullPlanStrategyInvalidRequestError,
    FullPlanStrategyStateError,
    StrategyContext,
    StrategyMode,
    execute_one_assignment,
    require_strategy_timeout,
)
from paritygrid.application.execution.runner_contract import (
    RUNNER_CONTRACT_VERSION,
    STRATEGY_CAPABILITIES_PROTOCOL,
    StrategyCapabilitiesV1,
)

THREADED_STRATEGY_ID = "threaded"
DEFAULT_THREADED_WORKERS = 4
MAX_THREADED_WORKERS = 64
MIN_THREADED_WORKERS = 1
_THREAD_NAME_PREFIX = "paritygrid-threaded-worker"

THREADED_STRATEGY_CAPABILITIES = StrategyCapabilitiesV1(
    strategy_id=THREADED_STRATEGY_ID,
    contract_version=RUNNER_CONTRACT_VERSION,
    supports_pause=True,
    supports_cancel=True,
    supports_checkpoint=True,
    max_concurrent_work=MAX_THREADED_WORKERS,
    max_in_flight_records=MAX_CAPTURED_LIMIT,
    platform_requirements=(),
    protocol=STRATEGY_CAPABILITIES_PROTOCOL,
)


def _require_worker_count(value: object) -> int:
    if type(value) is not int:
        raise TypeError("worker count must be an integer")
    count = value
    if not MIN_THREADED_WORKERS <= count <= MAX_THREADED_WORKERS:
        raise FullPlanStrategyInvalidRequestError("worker count is outside the supported range")
    return count


def derive_worker_count(settings: CapturedConcurrencySettings) -> int:
    """Derive the bounded worker count from the captured settings."""
    if type(settings) is not CapturedConcurrencySettings:
        raise TypeError("worker derivation requires CapturedConcurrencySettings")
    per_strategy = settings.per_strategy_work
    _require_worker_count(per_strategy)
    return max(MIN_THREADED_WORKERS, min(per_strategy, MAX_THREADED_WORKERS))


class ThreadedFullPlanStrategy:
    """Bounded worker-thread mechanics for full-plan execution.

    The strategy is pooled: the parent engine admits assignments while
    capacity lasts and each worker executes them concurrently.  Every
    owned thread is joined during shutdown; no thread survives the
    captured bound.
    """

    __slots__ = (
        "_capabilities",
        "_context",
        "_join_timeout_seconds",
        "_shutdown",
        "_started",
        "_threads",
        "_worker_count",
        "_worker_errors",
    )

    def __init__(
        self,
        *,
        worker_count: int | None = None,
        join_timeout_seconds: float = 5.0,
    ) -> None:
        self._worker_count = (
            _require_worker_count(worker_count) if worker_count is not None else None
        )
        self._join_timeout_seconds = require_strategy_timeout(
            join_timeout_seconds, "thread join timeout"
        )
        self._context: StrategyContext | None = None
        self._threads: list[Thread] = []
        self._shutdown = Event()
        self._started = False
        self._worker_errors: list[str] = []
        if join_timeout_seconds <= 0 or not math.isfinite(join_timeout_seconds):
            raise FullPlanStrategyInvalidRequestError(
                "thread join timeout must be positive and finite"
            )

    @property
    def strategy_id(self) -> str:
        return THREADED_STRATEGY_ID

    @property
    def capabilities(self) -> StrategyCapabilitiesV1:
        return THREADED_STRATEGY_CAPABILITIES

    @property
    def mode(self) -> StrategyMode:
        return StrategyMode.POOLED

    @property
    def is_started(self) -> bool:
        return self._started

    @property
    def alive_worker_count(self) -> int:
        """Return how many owned worker threads are still alive."""
        return sum(1 for thread in self._threads if thread.is_alive())

    @property
    def worker_identities(self) -> tuple[str, ...]:
        """Return the bounded owned thread names in creation order."""
        return tuple(thread.name for thread in self._threads)

    @property
    def worker_errors(self) -> tuple[str, ...]:
        """Return bounded diagnostics recorded by failed workers."""
        return tuple(self._worker_errors)

    def start(self, context: StrategyContext) -> None:
        """Spawn the bounded worker pool for one run."""
        if self._started:
            raise FullPlanStrategyStateError("threaded strategy already started")
        count = self._worker_count or derive_worker_count(context.settings)
        self._context = context
        self._shutdown = Event()
        self._threads = []
        for index in range(1, count + 1):
            thread = Thread(
                target=self._worker_loop,
                args=(index,),
                name=f"{_THREAD_NAME_PREFIX}-{index:03d}",
                daemon=False,
            )
            self._threads.append(thread)
            thread.start()
        self._started = True

    def execute_pending(self) -> int:
        """Pooled strategies never execute inline."""
        return 0

    def shutdown(self, *, timeout_seconds: float) -> None:
        """Unblock every worker and join all owned threads boundedly."""
        require_strategy_timeout(timeout_seconds, "threaded shutdown timeout")
        context = self._context
        if context is None or not self._started:
            return
        self._context = None
        self._started = False
        self._shutdown.set()
        per_thread = min(timeout_seconds, self._join_timeout_seconds)
        for thread in self._threads:
            thread.join(timeout=per_thread)
        leftover = [thread for thread in self._threads if thread.is_alive()]
        self._threads = []
        if leftover:
            raise FullPlanStrategyStateError(
                f"{len(leftover)} threaded workers survived the join bound"
            )

    def _worker_loop(self, index: int) -> None:
        context = self._context
        if context is None:
            return
        identity = f"threaded-worker-{index:03d}"
        while not self._shutdown.is_set():
            try:
                executed = execute_one_assignment(context, worker_identity=identity)
            except FullPlanStrategyError as error:
                self._record_worker_error(identity, error)
                continue
            except Exception as error:
                self._record_worker_error(identity, error)
                continue
            if executed == 0 and (self._shutdown.is_set() or context.assignment_channel.is_closed):
                return

    def _record_worker_error(self, identity: str, error: BaseException) -> None:
        """Keep bounded diagnostics; closed channels end the worker."""
        if len(self._worker_errors) < MAX_THREADED_WORKERS:
            self._worker_errors.append(f"{identity}: {error.__class__.__name__}")
