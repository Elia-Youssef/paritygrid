"""Structured asyncio full-plan strategy and safe sync adaptation (P7.13).

The asyncio strategy owns a structured task group of async workers.
Workers pull assignment envelopes with the cooperative async channel
wrappers, adapt the blocking operation executor through a bounded
owned thread pool, and push result envelopes without ever blocking the
event loop.  Cancellation propagates through the task tree; clients,
streams, queues, and the owned adaptation pool close during bounded
shutdown; no task is fire-and-forget and no exception is discarded.

Two entry points preserve one semantics: the async entry point runs
inside an already active event loop, while the synchronous facade
refuses to run inside an active loop with a stable typed error and
otherwise drives a dedicated owned loop thread — it never nests
``asyncio.run``.
"""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from typing import cast

from paritygrid.application.execution.channels import ChannelClosedError
from paritygrid.application.execution.concurrency_settings import (
    MAX_CAPTURED_LIMIT,
    CapturedConcurrencySettings,
)
from paritygrid.application.execution.concurrent_scheduler import WorkIdentity
from paritygrid.application.execution.full_plan_strategy import (
    ExecutedWork,
    FullPlanStrategyInvalidRequestError,
    FullPlanStrategyStateError,
    StrategyContext,
    StrategyMode,
    require_strategy_timeout,
)
from paritygrid.application.execution.runner_contract import (
    RUNNER_CONTRACT_VERSION,
    STRATEGY_CAPABILITIES_PROTOCOL,
    WORK_RESULT_PROTOCOL,
    ContractCleanupEvidence,
    ContractCleanupStatus,
    ContractOutcome,
    RunnerContractLoopError,
    StrategyCapabilitiesV1,
    WorkAssignmentV1,
    WorkResultV1,
)

ASYNCIO_STRATEGY_ID = "asyncio"
MAX_ASYNCIO_WORKERS = 64
MIN_ASYNCIO_WORKERS = 1
DEFAULT_ASYNCIO_WORKERS = 4
_LOOP_THREAD_NAME = "paritygrid-asyncio-loop"

ASYNCIO_STRATEGY_CAPABILITIES = StrategyCapabilitiesV1(
    strategy_id=ASYNCIO_STRATEGY_ID,
    contract_version=RUNNER_CONTRACT_VERSION,
    supports_pause=True,
    supports_cancel=True,
    supports_checkpoint=True,
    max_concurrent_work=MAX_ASYNCIO_WORKERS,
    max_in_flight_records=MAX_CAPTURED_LIMIT,
    platform_requirements=(),
    protocol=STRATEGY_CAPABILITIES_PROTOCOL,
)


def derive_asyncio_worker_count(settings: CapturedConcurrencySettings) -> int:
    """Derive the bounded async worker count from the captured settings."""
    if type(settings) is not CapturedConcurrencySettings:
        raise TypeError("worker derivation requires CapturedConcurrencySettings")
    count = settings.per_strategy_work
    if type(count) is not int or not MIN_ASYNCIO_WORKERS <= count <= MAX_ASYNCIO_WORKERS:
        raise FullPlanStrategyInvalidRequestError(
            "async worker count is outside the supported range"
        )
    return count


def _require_worker_count(value: object) -> int:
    if type(value) is not int:
        raise TypeError("async worker count must be an integer")
    count = value
    if not MIN_ASYNCIO_WORKERS <= count <= MAX_ASYNCIO_WORKERS:
        raise FullPlanStrategyInvalidRequestError(
            "async worker count is outside the supported range"
        )
    return count


class AsyncioFullPlanStrategy:
    """Structured async workers with a bounded blocking-adaptation path.

    ``start_async``/``shutdown_async`` compose inside an active event
    loop.  The synchronous ``start``/``shutdown`` facade runs the same
    workers on a dedicated owned loop thread and fails with a stable
    typed error when an event loop already runs in the calling thread.
    """

    __slots__ = (
        "_adaptation_executor",
        "_context",
        "_loop_ready",
        "_loop_thread",
        "_owned_loop",
        "_shutdown_seconds",
        "_started",
        "_tasks",
        "_worker_count",
        "_worker_errors",
    )

    def __init__(
        self,
        *,
        worker_count: int | None = None,
        shutdown_timeout_seconds: float = 5.0,
    ) -> None:
        self._worker_count = (
            _require_worker_count(worker_count) if worker_count is not None else None
        )
        self._shutdown_seconds = require_strategy_timeout(
            shutdown_timeout_seconds, "asyncio shutdown timeout"
        )
        self._context: StrategyContext | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._adaptation_executor: ThreadPoolExecutor | None = None
        self._loop_thread: Thread | None = None
        self._owned_loop: asyncio.AbstractEventLoop | None = None
        self._started = False
        self._worker_errors: list[str] = []

    @property
    def strategy_id(self) -> str:
        return ASYNCIO_STRATEGY_ID

    @property
    def capabilities(self) -> StrategyCapabilitiesV1:
        return ASYNCIO_STRATEGY_CAPABILITIES

    @property
    def mode(self) -> StrategyMode:
        return StrategyMode.POOLED

    @property
    def is_started(self) -> bool:
        return self._started

    @property
    def worker_errors(self) -> tuple[str, ...]:
        """Return bounded diagnostics recorded by failed async workers."""
        return tuple(self._worker_errors)

    @property
    def adaptation_pool_size(self) -> int:
        """Return the bounded blocking-adaptation pool size."""
        return self._worker_count or DEFAULT_ASYNCIO_WORKERS

    async def start_async(self, context: StrategyContext) -> None:
        """Start the structured async workers in the active loop."""
        if self._started:
            raise FullPlanStrategyStateError("asyncio strategy already started")
        count = self._worker_count or derive_asyncio_worker_count(context.settings)
        self._context = context
        self._adaptation_executor = ThreadPoolExecutor(
            max_workers=count,
            thread_name_prefix="paritygrid-asyncio-adaptation",
        )
        self._tasks = [
            asyncio.create_task(
                self._async_worker(index),
                name=f"paritygrid-asyncio-worker-{index:03d}",
            )
            for index in range(1, count + 1)
        ]
        self._started = True

    async def shutdown_async(self, *, timeout_seconds: float | None = None) -> None:
        """Cancel the task tree and close every owned resource boundedly."""
        require_strategy_timeout(
            timeout_seconds if timeout_seconds is not None else self._shutdown_seconds,
            "asyncio shutdown timeout",
        )
        context = self._context
        if context is None:
            return
        self._context = None
        self._started = False
        for task in self._tasks:
            if not task.done():
                task.cancel()
        try:
            await asyncio.wait(self._tasks, timeout=self._shutdown_seconds)
        except Exception as error:
            self._record_worker_error("shutdown", error)
        for task in self._tasks:
            if task.done() and not task.cancelled():
                error = task.exception()
                if error is not None:
                    self._record_worker_error("task", error)
        self._tasks = []
        executor = self._adaptation_executor
        self._adaptation_executor = None
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)
        context.executor.close()

    async def _async_worker(self, index: int) -> None:
        context = self._context
        if context is None:
            return
        identity = f"asyncio-worker-{index:03d}"
        executor = self._adaptation_executor
        if executor is None:
            raise FullPlanStrategyStateError("async worker lacks its adaptation pool")
        loop = asyncio.get_running_loop()
        while True:
            try:
                assignment = await self._recv_assignment(context)
            except _WorkerExitError:
                return
            if assignment is None:
                continue
            try:
                executed = await loop.run_in_executor(
                    executor,
                    self._execute_blocking,
                    context,
                    identity,
                    assignment,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                executed = _synthesized_failure(assignment, identity, error)
            context.facts.record(
                _identity_of(assignment),
                executed,
            )
            await context.result_channel.send_async(executed.result)

    def _execute_blocking(
        self,
        context: StrategyContext,
        identity: str,
        assignment: WorkAssignmentV1,
    ) -> ExecutedWork:
        del identity
        return context.executor.execute(assignment)

    async def _recv_assignment(self, context: StrategyContext) -> WorkAssignmentV1 | None:
        try:
            message = await context.assignment_channel.recv_async(iterations=64)
        except ChannelClosedError:
            raise _WorkerExitError from None
        except Exception:
            # The cooperative budget expired: yield once and retry.
            await asyncio.sleep(0)
            return None
        return cast(WorkAssignmentV1, message)

    # -- synchronous facade -----------------------------------------------

    def start(self, context: StrategyContext) -> None:
        """Start the workers on a dedicated owned loop thread.

        A synchronous start inside an active event loop fails with a
        stable typed error instead of blocking the loop.
        """

        if self._started:
            raise FullPlanStrategyStateError("asyncio strategy already started")
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise RunnerContractLoopError(
                "the synchronous asyncio facade must not run inside an active event loop"
            )
        ready = threading.Event()

        def _run_loop() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._owned_loop = loop
            ready.set()
            loop.run_forever()
            loop.close()

        self._loop_thread = Thread(target=_run_loop, name=_LOOP_THREAD_NAME, daemon=False)
        self._loop_thread.start()
        ready.wait(timeout=5.0)
        loop = self._owned_loop
        if loop is None:
            raise FullPlanStrategyStateError("owned asyncio loop failed to start")
        future = asyncio.run_coroutine_threadsafe(self.start_async(context), loop)
        future.result(timeout=self._shutdown_seconds)
        self._started = True

    def execute_pending(self) -> int:
        """Pooled strategies never execute inline."""
        return 0

    def shutdown(self, *, timeout_seconds: float) -> None:
        """Shut the owned loop thread down and join it boundedly."""
        require_strategy_timeout(timeout_seconds, "asyncio shutdown timeout")
        loop = self._owned_loop
        thread = self._loop_thread
        if loop is None or thread is None:
            return
        future = asyncio.run_coroutine_threadsafe(
            self.shutdown_async(timeout_seconds=timeout_seconds), loop
        )
        try:
            future.result(timeout=self._shutdown_seconds + timeout_seconds)
        finally:
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=self._shutdown_seconds + timeout_seconds)
        self._owned_loop = None
        self._loop_thread = None
        if thread.is_alive():
            raise FullPlanStrategyStateError(
                "the owned asyncio loop thread survived the join bound"
            )

    def _record_worker_error(self, origin: str, error: BaseException) -> None:
        if len(self._worker_errors) < MAX_ASYNCIO_WORKERS:
            self._worker_errors.append(f"{origin}: {error.__class__.__name__}")


class _WorkerExitError(Exception):
    """Internal control-flow signal for a closed assignment channel."""


def _identity_of(assignment: WorkAssignmentV1) -> WorkIdentity:
    return WorkIdentity(assignment.run_id, assignment.node_id, assignment.partition_key)


def _synthesized_failure(
    assignment: WorkAssignmentV1,
    identity: str,
    error: BaseException,
) -> ExecutedWork:
    """Synthesize the durable failure envelope for a crashed execution."""
    detail = f"worker {identity}: {error.__class__.__name__}"[:512]
    result = WorkResultV1(
        protocol=WORK_RESULT_PROTOCOL,
        contract_version=RUNNER_CONTRACT_VERSION,
        plan_fingerprint=assignment.plan_fingerprint,
        run_id=assignment.run_id,
        node_id=assignment.node_id,
        partition_key=assignment.partition_key,
        work_item_id=assignment.work_item_id,
        attempt_number=assignment.attempt_number,
        lease_fence=assignment.lease_fence,
        lease_owner=assignment.lease_owner,
        control_generation=assignment.control_generation,
        outcome=ContractOutcome.FAILED,
        metrics=(),
        artifact_references=(),
        checkpoint_proposal=False,
        failure_detail=detail,
        cleanup=ContractCleanupEvidence(ContractCleanupStatus.FAILED, (), None),
    )
    return ExecutedWork(result=result, failure_classification="unknown")


__all__ = [
    "ASYNCIO_STRATEGY_CAPABILITIES",
    "ASYNCIO_STRATEGY_ID",
    "DEFAULT_ASYNCIO_WORKERS",
    "MAX_ASYNCIO_WORKERS",
    "MIN_ASYNCIO_WORKERS",
    "AsyncioFullPlanStrategy",
    "derive_asyncio_worker_count",
]
