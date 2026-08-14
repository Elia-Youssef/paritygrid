"""Dependency-neutral contracts for the reference sequential runner."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from threading import Event
from types import TracebackType
from typing import Protocol, Self, cast, runtime_checkable

from paritygrid.application.execution.scheduler import (
    DependencyTracker,
    SchedulerState,
    SchedulerStatus,
)
from paritygrid.application.planner import (
    MAX_EXECUTION_PLAN_NODES,
    ExecutionPlan,
    ExecutionPlanNode,
    PlanFingerprint,
    PlannerRunnerKind,
    ResourcePolicy,
    fingerprint_execution_plan,
)
from paritygrid.application.planner.resources import (
    MAX_RESOURCE_MEMORY_BYTES,
    MAX_RESOURCE_TIMEOUT_SECONDS,
    MIN_RESOURCE_MEMORY_BYTES,
    MIN_RESOURCE_TIMEOUT_SECONDS,
)
from paritygrid.domain.models import NodeId

SEQUENTIAL_RUNNER_CONTRACT_VERSION = 1
SEQUENTIAL_RUNNER_MAX_CONCURRENCY = 1
SEQUENTIAL_RUNNER_MAX_IN_FLIGHT = 1
SEQUENTIAL_RUNNER_QUEUE_CAPACITY = 1


class RunnerError(RuntimeError):
    """Base failure for runner admission, execution, or cleanup."""


class RunnerClosedError(RunnerError):
    """A closed runner was asked to execute more work."""


class RunnerBusyError(RunnerError):
    """A sequential runner received an overlapping lifecycle operation."""


class RunnerUnsupportedPlanError(RunnerError):
    """An execution plan contains a node unsupported by this runner."""


class RunnerUnsafeResumeError(RunnerError):
    """An in-flight scheduler frontier requires durable recovery classification."""


class RunnerProtocolError(RunnerError):
    """A node executor returned a malformed or inconsistent outcome."""


class RunnerCleanupError(RunnerError):
    """An owned node executor failed during runner shutdown."""


class RunnerStatus(StrEnum):
    """Closed transient outcome of one runner invocation."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RunnerNodeOutcome(StrEnum):
    """Closed transient outcome returned by one node executor invocation."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class CancellationToken:
    """Thread-safe, dependency-neutral cancellation signal without durable authority."""

    __slots__ = ("_requested",)

    def __init__(self) -> None:
        self._requested = Event()

    @property
    def is_requested(self) -> bool:
        """Return whether cancellation has been requested."""
        return self._requested.is_set()

    def request(self) -> None:
        """Request cancellation idempotently."""
        self._requested.set()

    def __repr__(self) -> str:
        return f"CancellationToken(requested={self.is_requested!r})"


@dataclass(frozen=True, slots=True)
class SequentialRunnerLimits:
    """Effective bounded limits for the one-at-a-time reference runner."""

    memory_limit_bytes: int
    operation_timeout_seconds: int
    max_concurrency: int = SEQUENTIAL_RUNNER_MAX_CONCURRENCY
    max_in_flight: int = SEQUENTIAL_RUNNER_MAX_IN_FLIGHT
    queue_capacity: int = SEQUENTIAL_RUNNER_QUEUE_CAPACITY
    version: int = SEQUENTIAL_RUNNER_CONTRACT_VERSION

    def __post_init__(self) -> None:
        _validate_bounded_integer(
            self.memory_limit_bytes,
            MIN_RESOURCE_MEMORY_BYTES,
            MAX_RESOURCE_MEMORY_BYTES,
            "sequential runner memory limit",
        )
        _validate_bounded_integer(
            self.operation_timeout_seconds,
            MIN_RESOURCE_TIMEOUT_SECONDS,
            MAX_RESOURCE_TIMEOUT_SECONDS,
            "sequential runner operation timeout",
        )
        _require_exact_integer(
            self.max_concurrency,
            SEQUENTIAL_RUNNER_MAX_CONCURRENCY,
            "sequential runner concurrency",
        )
        _require_exact_integer(
            self.max_in_flight,
            SEQUENTIAL_RUNNER_MAX_IN_FLIGHT,
            "sequential runner in-flight limit",
        )
        _require_exact_integer(
            self.queue_capacity,
            SEQUENTIAL_RUNNER_QUEUE_CAPACITY,
            "sequential runner queue capacity",
        )
        _require_exact_integer(
            self.version,
            SEQUENTIAL_RUNNER_CONTRACT_VERSION,
            "sequential runner contract version",
        )

    @classmethod
    def from_resource_policy(cls, policy: ResourcePolicy) -> Self:
        """Derive deterministic sequential limits from one accepted plan policy."""
        if type(policy) is not ResourcePolicy:
            raise TypeError("runner resource policy must use ResourcePolicy")
        return cls(
            memory_limit_bytes=policy.memory_limit_bytes,
            operation_timeout_seconds=policy.operation_timeout_seconds,
        )


@dataclass(frozen=True, slots=True, repr=False)
class RunnerNodeRequest:
    """One immutable, non-authoritative node execution request."""

    node: ExecutionPlanNode
    plan_fingerprint: PlanFingerprint
    limits: SequentialRunnerLimits
    cancellation: CancellationToken

    def __post_init__(self) -> None:
        _require_exact(self.node, ExecutionPlanNode, "runner request node")
        _require_exact(
            self.plan_fingerprint,
            PlanFingerprint,
            "runner request plan fingerprint",
        )
        _require_exact(self.limits, SequentialRunnerLimits, "runner request limits")
        _require_exact(self.cancellation, CancellationToken, "runner request cancellation")

    def __repr__(self) -> str:
        return (
            "RunnerNodeRequest("
            f"node_id={self.node.node_id!r}, limits={self.limits!r}, "
            "plan_fingerprint=<redacted>, cancellation=<redacted>)"
        )


@dataclass(frozen=True, slots=True, repr=False)
class RunnerNodeResult:
    """One normalized transient node outcome without payload or durable authority."""

    node_id: NodeId
    outcome: RunnerNodeOutcome

    def __post_init__(self) -> None:
        _require_exact(self.node_id, NodeId, "runner result node identity")
        _require_exact(self.outcome, RunnerNodeOutcome, "runner result outcome")

    def __repr__(self) -> str:
        return f"RunnerNodeResult(node_id={self.node_id!r}, outcome={self.outcome.value!r})"


@runtime_checkable
class RunnerNodeExecutor(Protocol):
    """Application-facing node executor used by every runner implementation."""

    def execute(self, request: RunnerNodeRequest, /) -> RunnerNodeResult:
        """Execute one node through its configured inward-facing boundaries."""
        ...

    def close(self) -> None:
        """Release resources owned by the executor."""
        ...


@dataclass(frozen=True, slots=True, repr=False)
class RunnerReport:
    """Complete transient outcome and replay frontier for one runner invocation."""

    status: RunnerStatus
    scheduler_state: SchedulerState
    started_node_ids: tuple[NodeId, ...]

    def __post_init__(self) -> None:
        _require_exact(self.status, RunnerStatus, "runner report status")
        _require_exact(self.scheduler_state, SchedulerState, "runner report scheduler state")
        started = _require_exact_tuple(
            self.started_node_ids,
            NodeId,
            "runner report started nodes",
        )
        if len(started) > MAX_EXECUTION_PLAN_NODES:
            raise RunnerProtocolError("runner report exceeds the node limit")
        if len(set(started)) != len(started):
            raise RunnerProtocolError("runner report started nodes must be unique")
        plan_order = tuple(node.node_id for node in self.scheduler_state.nodes)
        if any(node_id not in plan_order for node_id in started):
            raise RunnerProtocolError("runner report contains an unknown started node")
        if tuple(node_id for node_id in plan_order if node_id in started) != started:
            raise RunnerProtocolError("runner report started nodes violate plan order")
        if self.status is RunnerStatus.SUCCEEDED:
            if self.scheduler_state.status is not SchedulerStatus.SUCCEEDED:
                raise RunnerProtocolError("successful runner report requires successful scheduler")
        elif self.status is RunnerStatus.FAILED:
            if self.scheduler_state.status is not SchedulerStatus.FAILED:
                raise RunnerProtocolError("failed runner report requires failed scheduler")
        elif self.scheduler_state.status is not SchedulerStatus.ACTIVE:
            raise RunnerProtocolError("cancelled runner report requires active scheduler")

    def __repr__(self) -> str:
        return (
            "RunnerReport("
            f"status={self.status.value!r}, started_nodes={len(self.started_node_ids)}, "
            f"scheduler_status={self.scheduler_state.status.value!r})"
        )


class SequentialRunner:
    """Run an exact plan one node at a time through a supplied node executor."""

    __slots__ = (
        "_cancellation",
        "_closed",
        "_executor",
        "_owns_executor",
        "_running",
        "_state",
    )

    def __init__(
        self,
        executor: RunnerNodeExecutor,
        *,
        cancellation: CancellationToken | None = None,
        owns_executor: bool = False,
    ) -> None:
        executor_value = cast(object, executor)
        if not isinstance(executor_value, RunnerNodeExecutor):
            raise TypeError("sequential runner executor must implement RunnerNodeExecutor")
        token = cast(object, cancellation)
        if token is not None and type(token) is not CancellationToken:
            raise TypeError("sequential runner cancellation must use CancellationToken or None")
        if type(owns_executor) is not bool:
            raise TypeError("sequential runner executor ownership must be boolean")
        self._executor = executor_value
        self._cancellation = cancellation if cancellation is not None else CancellationToken()
        self._owns_executor = owns_executor
        self._closed = False
        self._running = False
        self._state: SchedulerState | None = None

    @property
    def cancellation(self) -> CancellationToken:
        """Return the shared cancellation token used by this runner."""
        return self._cancellation

    @property
    def state(self) -> SchedulerState | None:
        """Return the latest visible scheduler frontier, if execution began."""
        return self._state

    @property
    def is_closed(self) -> bool:
        """Return whether runner shutdown has completed or begun."""
        return self._closed

    def run(
        self,
        plan: ExecutionPlan,
        *,
        state: SchedulerState | None = None,
    ) -> RunnerReport:
        """Execute or continue one exact plan through the sequential frontier."""
        if type(plan) is not ExecutionPlan:
            raise TypeError("sequential runner plan must use ExecutionPlan")
        restored = cast(object, state)
        if restored is not None and type(restored) is not SchedulerState:
            raise TypeError("sequential runner state must use SchedulerState or None")
        if self._closed:
            raise RunnerClosedError("closed sequential runner cannot execute a plan")
        if self._running:
            raise RunnerBusyError("sequential runner already has an active invocation")
        if any(PlannerRunnerKind.SEQUENTIAL not in node.supported_runners for node in plan.nodes):
            raise RunnerUnsupportedPlanError("execution plan contains a non-sequential node")

        tracker = DependencyTracker(plan, state=state)
        self._state = tracker.state
        if tracker.state.status.is_terminal:
            status = (
                RunnerStatus.SUCCEEDED
                if tracker.state.status is SchedulerStatus.SUCCEEDED
                else RunnerStatus.FAILED
            )
            return RunnerReport(status, tracker.state, ())
        if tracker.state.active_node_id is not None:
            raise RunnerUnsafeResumeError(
                "in-flight scheduler state requires durable recovery classification"
            )

        limits = SequentialRunnerLimits.from_resource_policy(plan.resource_policy)
        plan_fingerprint = fingerprint_execution_plan(plan)
        started: list[NodeId] = []
        self._running = True
        try:
            while True:
                if self._cancellation.is_requested:
                    return RunnerReport(RunnerStatus.CANCELLED, tracker.state, tuple(started))
                node = tracker.next_ready_node()
                if node is None:
                    raise RunnerProtocolError("active scheduler has no admissible node")
                self._state = tracker.start(node.node_id)
                started.append(node.node_id)
                result = self._executor.execute(
                    RunnerNodeRequest(
                        node=node,
                        plan_fingerprint=plan_fingerprint,
                        limits=limits,
                        cancellation=self._cancellation,
                    )
                )
                if type(result) is not RunnerNodeResult:
                    raise RunnerProtocolError("node executor returned an invalid result")
                if result.node_id != node.node_id:
                    raise RunnerProtocolError("node executor result does not match active node")
                if result.outcome is RunnerNodeOutcome.SUCCEEDED:
                    self._state = tracker.succeed(node.node_id)
                    if tracker.state.status is SchedulerStatus.SUCCEEDED:
                        return RunnerReport(
                            RunnerStatus.SUCCEEDED,
                            tracker.state,
                            tuple(started),
                        )
                elif result.outcome is RunnerNodeOutcome.FAILED:
                    self._state = tracker.fail(node.node_id)
                    return RunnerReport(RunnerStatus.FAILED, tracker.state, tuple(started))
                else:
                    if not self._cancellation.is_requested:
                        raise RunnerProtocolError(
                            "cancelled node result requires requested cancellation"
                        )
                    return RunnerReport(RunnerStatus.CANCELLED, tracker.state, tuple(started))
        finally:
            self._running = False

    def close(self) -> None:
        """Close this runner and its executor only when ownership was explicit."""
        if self._closed:
            return
        if self._running:
            raise RunnerBusyError("active sequential runner cannot close")
        self._closed = True
        if not self._owns_executor:
            return
        try:
            self._executor.close()
        except Exception as error:
            raise RunnerCleanupError("owned runner executor cleanup failed") from error

    def __enter__(self) -> Self:
        if self._closed:
            raise RunnerClosedError("closed sequential runner cannot be entered")
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, exception, traceback
        self.close()

    def __repr__(self) -> str:
        return (
            "SequentialRunner("
            f"closed={self._closed!r}, running={self._running!r}, "
            f"owns_executor={self._owns_executor!r}, state={self._state is not None!r})"
        )


def _require_exact(value: object, expected: type[object], subject: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{subject} must use {expected.__name__}")


def _require_exact_tuple[T](value: object, item_type: type[T], subject: str) -> tuple[T, ...]:
    if type(value) is not tuple:
        raise TypeError(f"{subject} must be a tuple")
    values = cast(tuple[object, ...], value)
    if any(type(item) is not item_type for item in values):
        raise TypeError(f"{subject} contains an invalid value")
    return cast(tuple[T, ...], values)


def _validate_bounded_integer(value: object, minimum: int, maximum: int, subject: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    if not minimum <= value <= maximum:
        raise RunnerProtocolError(f"{subject} is outside the supported range")


def _require_exact_integer(value: object, expected: int, subject: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    if value != expected:
        raise RunnerProtocolError(f"{subject} must equal {expected}")
