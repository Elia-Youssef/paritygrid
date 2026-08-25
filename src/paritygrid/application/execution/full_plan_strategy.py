"""Full-plan strategy port, worker mechanics, and the sequential strategy.

A full-plan strategy owns only worker mechanics: it receives immutable
``WorkAssignmentV1`` envelopes from the bounded assignment channel,
executes the described operation through the parent-supplied executor,
and returns immutable ``WorkResultV1`` envelopes through the bounded
result channel.  It never sees the scheduler, the capacity ledger, the
result coordinator, leases, artifacts, or any writer.

Every strategy shares :func:`execute_one_assignment` so sequential,
threaded, and asyncio workers apply exactly the same envelope
discipline, facts registration, backpressure, and failure synthesis.
The sequential strategy executes inline in the engine's own admission
step and defines the semantic reference for the shared conformance
suites.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum
from threading import Lock
from typing import Protocol, cast, runtime_checkable

from paritygrid.application.execution.channels import (
    CHANNEL_KIND_ASSIGNMENT,
    CHANNEL_KIND_RESULT,
    BoundedChannel,
    ChannelClosedError,
)
from paritygrid.application.execution.concurrency_settings import (
    CapturedConcurrencySettings,
)
from paritygrid.application.execution.concurrent_scheduler import WorkIdentity
from paritygrid.application.execution.runner_contract import (
    RUNNER_CONTRACT_VERSION,
    STRATEGY_CAPABILITIES_PROTOCOL,
    WORK_RESULT_PROTOCOL,
    ContractCleanupEvidence,
    ContractCleanupStatus,
    ContractOutcome,
    StrategyCapabilitiesV1,
    WorkAssignmentV1,
    WorkResultV1,
)

FULL_PLAN_STRATEGY_CONTRACT_VERSION = 1
MAX_RESULT_FACTS = 256
MAX_WORKER_IDENTITY_LENGTH = 64
MIN_STRATEGY_TIMEOUT_SECONDS = 0.0
MAX_STRATEGY_TIMEOUT_SECONDS = 86_400.0
DEFAULT_WORKER_RECV_TIMEOUT_SECONDS = 0.05


class FullPlanStrategyError(RuntimeError):
    """Base failure for full-plan strategy mechanics."""


class FullPlanStrategyInvalidRequestError(FullPlanStrategyError):
    """A strategy request violated the worker contract."""


class FullPlanStrategyStateError(FullPlanStrategyError):
    """The strategy lifecycle state rejected the request."""


class StrategyMode(StrEnum):
    """How a strategy consumes admitted assignments."""

    INLINE = "inline"
    POOLED = "pooled"


@dataclass(frozen=True, slots=True)
class ExecutedWork:
    """One executor outcome plus the durable facts the envelope cannot carry.

    ``failure_classification`` and ``retry_eligible_at_micros`` are
    parent-side commit facts: the runner-neutral envelope carries the
    outcome, metrics, and redacted detail, while these travel through
    the parent-owned facts registry next to the envelope.
    """

    result: WorkResultV1
    failure_classification: str | None = None
    retry_eligible_at_micros: int = 0

    def __post_init__(self) -> None:
        if type(self.result) is not WorkResultV1:
            raise TypeError("executed work must carry WorkResultV1")


@runtime_checkable
class WorkOperationExecutor(Protocol):
    """Parent-supplied strategy-neutral operation execution."""

    def execute(self, assignment: WorkAssignmentV1) -> ExecutedWork: ...

    def close(self) -> None: ...


class ResultFactsRegistry:
    """Bounded parent-owned registry of per-identity worker commit facts.

    Workers record the classification and retry eligibility for an
    identity before sending its envelope, so the parent's result loop
    can pass them to the durable coordinator without ever widening the
    runner-neutral envelope.  The registry is thread-safe, bounded, and
    removes facts exactly once on take.
    """

    __slots__ = ("_facts", "_lock")

    def __init__(self) -> None:
        self._facts: dict[WorkIdentity, ExecutedWork] = {}
        self._lock = Lock()

    def record(
        self,
        identity: WorkIdentity,
        facts: ExecutedWork,
    ) -> None:
        """Record the commit facts for one in-flight identity."""
        if type(identity) is not WorkIdentity:
            raise TypeError("result facts identity must use WorkIdentity")
        if type(facts) is not ExecutedWork:
            raise TypeError("result facts must use ExecutedWork")
        with self._lock:
            if identity in self._facts:
                raise FullPlanStrategyInvalidRequestError(
                    "result facts are already recorded for this identity"
                )
            if len(self._facts) >= MAX_RESULT_FACTS:
                raise FullPlanStrategyStateError(
                    "result facts registry exceeds the supported bound"
                )
            self._facts[identity] = facts

    def take(self, identity: WorkIdentity) -> ExecutedWork | None:
        """Remove and return the recorded facts for one identity."""
        if type(identity) is not WorkIdentity:
            raise TypeError("result facts identity must use WorkIdentity")
        with self._lock:
            return self._facts.pop(identity, None)

    def pending_count(self) -> int:
        with self._lock:
            return len(self._facts)


@dataclass(frozen=True, slots=True)
class StrategyContext:
    """Everything a strategy may touch — channels, executor, facts, bounds."""

    run_id: str
    plan_fingerprint: str
    settings: CapturedConcurrencySettings
    assignment_channel: BoundedChannel
    result_channel: BoundedChannel
    executor: WorkOperationExecutor
    facts: ResultFactsRegistry

    def __post_init__(self) -> None:
        if type(self.run_id) is not str or not self.run_id:
            raise TypeError("strategy context run identity must be text")
        if type(self.plan_fingerprint) is not str or not self.plan_fingerprint:
            raise TypeError("strategy context plan fingerprint must be text")
        if type(self.settings) is not CapturedConcurrencySettings:
            raise TypeError("strategy context settings must use CapturedConcurrencySettings")
        if type(self.assignment_channel) is not BoundedChannel or (
            self.assignment_channel.kind != CHANNEL_KIND_ASSIGNMENT
        ):
            raise TypeError("strategy assignment channel must use the assignment kind")
        if type(self.result_channel) is not BoundedChannel or (
            self.result_channel.kind != CHANNEL_KIND_RESULT
        ):
            raise TypeError("strategy result channel must use the result kind")
        executor_value = cast(object, self.executor)
        if not isinstance(executor_value, WorkOperationExecutor):
            raise TypeError("strategy executor must implement WorkOperationExecutor")
        if type(self.facts) is not ResultFactsRegistry:
            raise TypeError("strategy facts must use ResultFactsRegistry")


@runtime_checkable
class FullPlanStrategy(Protocol):
    """Worker-mechanics port every full-plan strategy implements."""

    @property
    def strategy_id(self) -> str: ...

    @property
    def capabilities(self) -> StrategyCapabilitiesV1: ...

    @property
    def mode(self) -> StrategyMode: ...

    def start(self, context: StrategyContext) -> None: ...

    def execute_pending(self) -> int:
        """Execute at most one pending assignment inline.

        Pooled strategies return ``0`` — their workers execute
        concurrently — while inline strategies perform exactly one
        bounded pull-execute-push cycle per call.
        """
        ...

    def shutdown(self, *, timeout_seconds: float) -> None: ...


def execute_one_assignment(
    context: StrategyContext,
    *,
    worker_identity: str,
    recv_timeout_seconds: float = DEFAULT_WORKER_RECV_TIMEOUT_SECONDS,
) -> int:
    """Pull one assignment, execute it, and push its envelope.

    Returns ``1`` when one assignment executed and ``0`` when the
    channel is closed and drained.  Executor failures are never
    silently dropped: the worker synthesizes a durable ``FAILED``
    envelope from the assignment facts so the parent coordinator — not
    the worker — owns the failure commit.
    """

    _require_worker_identity(worker_identity)
    try:
        message = context.assignment_channel.recv(timeout=recv_timeout_seconds)
    except ChannelClosedError:
        return 0
    assignment = cast(WorkAssignmentV1, message)
    if type(assignment) is not WorkAssignmentV1:
        raise FullPlanStrategyStateError("assignment channel delivered a non-envelope message")
    identity = WorkIdentity(assignment.run_id, assignment.node_id, assignment.partition_key)
    try:
        executed = context.executor.execute(assignment)
    except FullPlanStrategyError:
        raise
    except Exception as error:
        executed = _failed_execution(assignment, worker_identity, error)
    if type(executed) is not ExecutedWork:
        raise FullPlanStrategyStateError("executor must return ExecutedWork")
    if executed.result.work_identity() != assignment.work_identity():
        raise FullPlanStrategyStateError("executor returned a result for a different work identity")
    context.facts.record(
        identity,
        ExecutedWork(
            result=executed.result,
            failure_classification=executed.failure_classification,
            retry_eligible_at_micros=executed.retry_eligible_at_micros,
        ),
    )
    context.result_channel.send(executed.result)
    return 1


def _failed_execution(
    assignment: WorkAssignmentV1,
    worker_identity: str,
    error: Exception,
) -> ExecutedWork:
    """Synthesize the durable failure envelope for one crashed executor."""
    detail = f"worker {worker_identity}: {error.__class__.__name__}"[:512]
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
        cleanup=ContractCleanupEvidence(
            ContractCleanupStatus.FAILED,
            (),
            None,
        ),
    )
    return ExecutedWork(result=result, failure_classification="unknown")


def _require_worker_identity(value: object) -> str:
    if type(value) is not str:
        raise TypeError("worker identity must be text")
    text = value
    if not 1 <= len(text) <= MAX_WORKER_IDENTITY_LENGTH:
        raise FullPlanStrategyInvalidRequestError(
            "worker identity length is outside the supported range"
        )
    for character in text:
        if not "\x20" <= character <= "\x7e":
            raise FullPlanStrategyInvalidRequestError(
                "worker identity must use printable ASCII characters"
            )
    return text


def require_strategy_timeout(value: object, subject: str) -> float:
    """Validate one bounded strategy timeout in seconds."""
    if type(value) is not float and type(value) is not int:
        raise TypeError(f"{subject} must be a finite non-negative second count")
    seconds = float(value)
    if (
        not math.isfinite(seconds)
        or not MIN_STRATEGY_TIMEOUT_SECONDS <= seconds <= MAX_STRATEGY_TIMEOUT_SECONDS
    ):
        raise FullPlanStrategyInvalidRequestError(f"{subject} is outside the supported range")
    return seconds


SEQUENTIAL_STRATEGY_CAPABILITIES = StrategyCapabilitiesV1(
    strategy_id="sequential",
    contract_version=RUNNER_CONTRACT_VERSION,
    supports_pause=True,
    supports_cancel=True,
    supports_checkpoint=True,
    max_concurrent_work=1,
    max_in_flight_records=1,
    platform_requirements=(),
    protocol=STRATEGY_CAPABILITIES_PROTOCOL,
)


class SequentialFullPlanStrategy:
    """The inline one-at-a-time reference strategy.

    The engine admits exactly one assignment, then asks the strategy to
    execute it inline in the engine's own step, preserving the
    sequential semantic reference while using the same channels, facts
    registry, and durable boundaries as every other strategy.
    """

    __slots__ = ("_capabilities", "_context", "_started")

    def __init__(self) -> None:
        self._context: StrategyContext | None = None
        self._started = False
        self._capabilities = SEQUENTIAL_STRATEGY_CAPABILITIES

    @property
    def strategy_id(self) -> str:
        return "sequential"

    @property
    def capabilities(self) -> StrategyCapabilitiesV1:
        return self._capabilities

    @property
    def mode(self) -> StrategyMode:
        return StrategyMode.INLINE

    @property
    def is_started(self) -> bool:
        return self._started

    def start(self, context: StrategyContext) -> None:
        """Bind the strategy to one run's channels and executor."""
        if self._started:
            raise FullPlanStrategyStateError("sequential strategy already started")
        if context.settings.per_strategy_work < 1:
            raise FullPlanStrategyInvalidRequestError(
                "sequential strategy requires at least one strategy work slot"
            )
        self._context = context
        self._started = True

    def execute_pending(self) -> int:
        """Execute at most one pending assignment inline."""
        context = self._require_context()
        return execute_one_assignment(context, worker_identity="sequential-inline")

    def shutdown(self, *, timeout_seconds: float) -> None:
        """Close the bound executor exactly once."""
        del timeout_seconds
        context = self._context
        if context is None:
            return
        self._context = None
        self._started = False
        context.executor.close()

    def _require_context(self) -> StrategyContext:
        context = self._context
        if context is None or not self._started:
            raise FullPlanStrategyStateError("sequential strategy is not started")
        return context


__all__ = [
    "DEFAULT_WORKER_RECV_TIMEOUT_SECONDS",
    "FULL_PLAN_STRATEGY_CONTRACT_VERSION",
    "MAX_RESULT_FACTS",
    "MAX_STRATEGY_TIMEOUT_SECONDS",
    "MAX_WORKER_IDENTITY_LENGTH",
    "SEQUENTIAL_STRATEGY_CAPABILITIES",
    "ExecutedWork",
    "FullPlanStrategy",
    "FullPlanStrategyError",
    "FullPlanStrategyInvalidRequestError",
    "FullPlanStrategyStateError",
    "ResultFactsRegistry",
    "SequentialFullPlanStrategy",
    "StrategyContext",
    "StrategyMode",
    "WorkOperationExecutor",
    "execute_one_assignment",
    "require_strategy_timeout",
]
