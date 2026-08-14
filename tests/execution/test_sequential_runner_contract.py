"""Contract and adversarial tests for the reference sequential runner."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import Any, cast

import pytest

import paritygrid.application.execution.runner as runner_module
from paritygrid.application.execution import (
    MAX_SCHEDULER_DEPENDENCIES,
    CancellationToken,
    DependencyTracker,
    RunnerBusyError,
    RunnerCleanupError,
    RunnerClosedError,
    RunnerNodeOutcome,
    RunnerNodeRequest,
    RunnerNodeResult,
    RunnerProtocolError,
    RunnerReport,
    RunnerStatus,
    RunnerUnsafeResumeError,
    RunnerUnsupportedPlanError,
    SchedulerState,
    SequentialRunner,
    SequentialRunnerLimits,
)
from paritygrid.application.planner import (
    ExecutionPlan,
    ExecutionPlanNode,
    NodeRole,
    PlannerRunnerKind,
    ResourcePolicy,
    RetryBehavior,
    fingerprint_execution_plan,
)
from paritygrid.application.planner.registry import ConnectorRequirement
from paritygrid.application.planner.resources import (
    MAX_RESOURCE_MEMORY_BYTES,
    MAX_RESOURCE_TIMEOUT_SECONDS,
    MIN_RESOURCE_MEMORY_BYTES,
    MIN_RESOURCE_TIMEOUT_SECONDS,
)
from paritygrid.application.ports import ConfigurationDocument
from paritygrid.domain.models import NodeId
from paritygrid.domain.pipeline import NodeKind, PipelineEdge, PortName


def _id(value: str) -> NodeId:
    return NodeId(f"nod_runner-{value}")


def _node(
    value: str,
    *,
    runners: tuple[PlannerRunnerKind, ...] = (PlannerRunnerKind.SEQUENTIAL,),
) -> ExecutionPlanNode:
    return ExecutionPlanNode(
        node_id=_id(value),
        kind=NodeKind("transform.normalize"),
        configuration_version=1,
        configuration=ConfigurationDocument.from_mapping({}),
        connector_id=None,
        role=NodeRole.TRANSFORM,
        connector_requirement=ConnectorRequirement.NONE,
        supported_runners=runners,
        retry_behavior=RetryBehavior.NEVER,
        requires_idempotency=False,
    )


def _plan(*values: str) -> ExecutionPlan:
    nodes = tuple(_node(value) for value in values)
    edges = tuple(
        PipelineEdge(
            nodes[index - 1].node_id,
            PortName("out-records"),
            nodes[index].node_id,
            PortName("in-records"),
        )
        for index in range(1, len(nodes))
    )
    return ExecutionPlan(nodes, edges, ResourcePolicy(), ())


class _Executor:
    def __init__(
        self,
        outcomes: tuple[RunnerNodeOutcome, ...] = (),
        callback: Callable[[RunnerNodeRequest], RunnerNodeResult] | None = None,
    ) -> None:
        self.outcomes = list(outcomes)
        self.callback = callback
        self.requests: list[RunnerNodeRequest] = []
        self.close_count = 0

    def execute(self, request: RunnerNodeRequest, /) -> RunnerNodeResult:
        self.requests.append(request)
        if self.callback is not None:
            return self.callback(request)
        outcome = self.outcomes.pop(0) if self.outcomes else RunnerNodeOutcome.SUCCEEDED
        return RunnerNodeResult(request.node.node_id, outcome)

    def close(self) -> None:
        self.close_count += 1


def _succeeded_state(plan: ExecutionPlan) -> SchedulerState:
    tracker = DependencyTracker(plan)
    for node in plan.nodes:
        tracker.start(node.node_id)
        tracker.succeed(node.node_id)
    return tracker.state


def _failed_state(plan: ExecutionPlan) -> SchedulerState:
    tracker = DependencyTracker(plan)
    tracker.start(plan.nodes[0].node_id)
    return tracker.fail(plan.nodes[0].node_id)


def test_limits_are_exact_bounded_and_derived_from_plan_policy() -> None:
    policy = ResourcePolicy(
        memory_limit_bytes=MIN_RESOURCE_MEMORY_BYTES,
        operation_timeout_seconds=MAX_RESOURCE_TIMEOUT_SECONDS,
    )
    limits = SequentialRunnerLimits.from_resource_policy(policy)
    assert limits == SequentialRunnerLimits(
        MIN_RESOURCE_MEMORY_BYTES,
        MAX_RESOURCE_TIMEOUT_SECONDS,
    )
    assert limits.max_concurrency == 1
    assert limits.max_in_flight == 1
    assert limits.queue_capacity == 1
    with pytest.raises(TypeError, match="ResourcePolicy"):
        SequentialRunnerLimits.from_resource_policy(cast(Any, {}))


@pytest.mark.parametrize(
    ("field", "value", "error", "message"),
    [
        ("memory_limit_bytes", True, TypeError, "integer"),
        (
            "memory_limit_bytes",
            MIN_RESOURCE_MEMORY_BYTES - 1,
            RunnerProtocolError,
            "supported range",
        ),
        ("operation_timeout_seconds", 1.0, TypeError, "integer"),
        (
            "operation_timeout_seconds",
            MAX_RESOURCE_TIMEOUT_SECONDS + 1,
            RunnerProtocolError,
            "supported range",
        ),
        ("max_concurrency", False, TypeError, "integer"),
        ("max_concurrency", 2, RunnerProtocolError, "equal 1"),
        ("max_in_flight", 2, RunnerProtocolError, "equal 1"),
        ("queue_capacity", 0, RunnerProtocolError, "equal 1"),
        ("version", 2, RunnerProtocolError, "equal 1"),
    ],
)
def test_limits_reject_invalid_public_values(
    field: str,
    value: object,
    error: type[Exception],
    message: str,
) -> None:
    values: dict[str, object] = {
        "memory_limit_bytes": MAX_RESOURCE_MEMORY_BYTES,
        "operation_timeout_seconds": MIN_RESOURCE_TIMEOUT_SECONDS,
        "max_concurrency": 1,
        "max_in_flight": 1,
        "queue_capacity": 1,
        "version": 1,
    }
    values[field] = value
    with pytest.raises(error, match=message):
        SequentialRunnerLimits(**cast(Any, values))


def test_token_request_is_idempotent_and_repr_is_bounded() -> None:
    token = CancellationToken()
    assert token.is_requested is False
    assert repr(token) == "CancellationToken(requested=False)"
    token.request()
    token.request()
    assert token.is_requested is True
    assert repr(token) == "CancellationToken(requested=True)"


def test_request_and_result_are_exact_immutable_redacted_contracts() -> None:
    plan = _plan("a")
    request = RunnerNodeRequest(
        plan.nodes[0],
        fingerprint_execution_plan(plan),
        SequentialRunnerLimits.from_resource_policy(plan.resource_policy),
        CancellationToken(),
    )
    assert "plan_fingerprint=<redacted>" in repr(request)
    assert "cancellation=<redacted>" in repr(request)
    result = RunnerNodeResult(_id("a"), RunnerNodeOutcome.SUCCEEDED)
    assert repr(result) == (
        "RunnerNodeResult(node_id=NodeId(value='nod_runner-a'), outcome='succeeded')"
    )
    with pytest.raises(AttributeError):
        request.node = plan.nodes[0]  # type: ignore[misc]


_REQUEST_RESULT_INVALID_CASES: list[tuple[Callable[[ExecutionPlan], object], str]] = [
    (
        lambda plan: RunnerNodeRequest(
            cast(Any, object()),
            fingerprint_execution_plan(plan),
            SequentialRunnerLimits.from_resource_policy(plan.resource_policy),
            CancellationToken(),
        ),
        "ExecutionPlanNode",
    ),
    (
        lambda plan: RunnerNodeRequest(
            plan.nodes[0],
            cast(Any, "a" * 64),
            SequentialRunnerLimits.from_resource_policy(plan.resource_policy),
            CancellationToken(),
        ),
        "PlanFingerprint",
    ),
    (
        lambda plan: RunnerNodeRequest(
            plan.nodes[0],
            fingerprint_execution_plan(plan),
            cast(Any, object()),
            CancellationToken(),
        ),
        "SequentialRunnerLimits",
    ),
    (
        lambda plan: RunnerNodeRequest(
            plan.nodes[0],
            fingerprint_execution_plan(plan),
            SequentialRunnerLimits.from_resource_policy(plan.resource_policy),
            cast(Any, object()),
        ),
        "CancellationToken",
    ),
    (
        lambda plan: RunnerNodeResult(
            cast(Any, str(plan.nodes[0].node_id)), RunnerNodeOutcome.SUCCEEDED
        ),
        "NodeId",
    ),
    (
        lambda plan: RunnerNodeResult(plan.nodes[0].node_id, cast(Any, "succeeded")),
        "RunnerNodeOutcome",
    ),
]


@pytest.mark.parametrize(
    ("factory", "message"),
    _REQUEST_RESULT_INVALID_CASES,
)
def test_request_and_result_reject_inexact_types(
    factory: Callable[[ExecutionPlan], object],
    message: str,
) -> None:
    with pytest.raises(TypeError, match=message):
        factory(_plan("a"))


def test_report_validates_status_frontier_started_order_and_repr() -> None:
    plan = _plan("a", "b")
    state = _succeeded_state(plan)
    report = RunnerReport(RunnerStatus.SUCCEEDED, state, (_id("a"), _id("b")))
    assert repr(report) == (
        "RunnerReport(status='succeeded', started_nodes=2, scheduler_status='succeeded')"
    )
    failed = RunnerReport(RunnerStatus.FAILED, _failed_state(plan), (_id("a"),))
    assert failed.status is RunnerStatus.FAILED
    active = DependencyTracker(plan).state
    assert RunnerReport(RunnerStatus.CANCELLED, active, ()).status is RunnerStatus.CANCELLED


_REPORT_INVALID_CASES: list[tuple[Callable[[ExecutionPlan], object], type[Exception], str]] = [
    (
        lambda plan: RunnerReport(
            cast(Any, "succeeded"),
            _succeeded_state(plan),
            (),
        ),
        TypeError,
        "RunnerStatus",
    ),
    (
        lambda plan: RunnerReport(
            RunnerStatus.SUCCEEDED,
            cast(Any, object()),
            (),
        ),
        TypeError,
        "SchedulerState",
    ),
    (
        lambda plan: RunnerReport(
            RunnerStatus.SUCCEEDED,
            _succeeded_state(plan),
            cast(Any, []),
        ),
        TypeError,
        "tuple",
    ),
    (
        lambda plan: RunnerReport(
            RunnerStatus.SUCCEEDED,
            _succeeded_state(plan),
            cast(Any, ("nod_runner-a",)),
        ),
        TypeError,
        "invalid",
    ),
    (
        lambda plan: RunnerReport(
            RunnerStatus.SUCCEEDED,
            _succeeded_state(plan),
            (_id("a"),) * (MAX_SCHEDULER_DEPENDENCIES + 2),
        ),
        RunnerProtocolError,
        "node limit",
    ),
    (
        lambda plan: RunnerReport(
            RunnerStatus.SUCCEEDED,
            _succeeded_state(plan),
            (_id("a"), _id("a")),
        ),
        RunnerProtocolError,
        "unique",
    ),
    (
        lambda plan: RunnerReport(
            RunnerStatus.SUCCEEDED,
            _succeeded_state(plan),
            (_id("unknown"),),
        ),
        RunnerProtocolError,
        "unknown",
    ),
    (
        lambda plan: RunnerReport(
            RunnerStatus.SUCCEEDED,
            _succeeded_state(plan),
            (_id("b"), _id("a")),
        ),
        RunnerProtocolError,
        "plan order",
    ),
    (
        lambda plan: RunnerReport(
            RunnerStatus.SUCCEEDED,
            DependencyTracker(plan).state,
            (),
        ),
        RunnerProtocolError,
        "successful scheduler",
    ),
    (
        lambda plan: RunnerReport(
            RunnerStatus.FAILED,
            _succeeded_state(plan),
            (),
        ),
        RunnerProtocolError,
        "failed scheduler",
    ),
    (
        lambda plan: RunnerReport(
            RunnerStatus.CANCELLED,
            _succeeded_state(plan),
            (),
        ),
        RunnerProtocolError,
        "active scheduler",
    ),
]


@pytest.mark.parametrize(
    ("factory", "error", "message"),
    _REPORT_INVALID_CASES,
)
def test_report_rejects_corrupt_public_state(
    factory: Callable[[ExecutionPlan], object],
    error: type[Exception],
    message: str,
) -> None:
    with pytest.raises(error, match=message):
        factory(_plan("a", "b"))


def test_runner_executes_exact_plan_order_with_effective_limits() -> None:
    executor = _Executor()
    runner = SequentialRunner(executor)
    report = runner.run(_plan("a", "b", "c"))
    assert report.status is RunnerStatus.SUCCEEDED
    assert report.started_node_ids == (_id("a"), _id("b"), _id("c"))
    assert [request.node.node_id for request in executor.requests] == list(report.started_node_ids)
    assert all(request.limits.max_concurrency == 1 for request in executor.requests)
    assert len({request.plan_fingerprint for request in executor.requests}) == 1
    assert runner.state is report.scheduler_state
    assert runner.cancellation is executor.requests[0].cancellation
    assert repr(runner) == (
        "SequentialRunner(closed=False, running=False, owns_executor=False, state=True)"
    )


def test_runner_failure_is_terminal_and_stops_admission() -> None:
    executor = _Executor((RunnerNodeOutcome.SUCCEEDED, RunnerNodeOutcome.FAILED))
    report = SequentialRunner(executor).run(_plan("a", "b", "c"))
    assert report.status is RunnerStatus.FAILED
    assert report.scheduler_state.failed_node_id == _id("b")
    assert report.started_node_ids == (_id("a"), _id("b"))
    assert len(executor.requests) == 2


def test_cancellation_before_and_between_nodes_stops_new_admission() -> None:
    token = CancellationToken()
    token.request()
    executor = _Executor()
    report = SequentialRunner(executor, cancellation=token).run(_plan("a"))
    assert report.status is RunnerStatus.CANCELLED
    assert report.started_node_ids == ()
    assert executor.requests == []

    token = CancellationToken()

    def cancel_after_success(request: RunnerNodeRequest) -> RunnerNodeResult:
        token.request()
        return RunnerNodeResult(request.node.node_id, RunnerNodeOutcome.SUCCEEDED)

    executor = _Executor(callback=cancel_after_success)
    report = SequentialRunner(executor, cancellation=token).run(_plan("a", "b"))
    assert report.status is RunnerStatus.CANCELLED
    assert report.scheduler_state.succeeded_node_ids == (_id("a"),)
    assert report.scheduler_state.active_node_id is None
    assert report.started_node_ids == (_id("a"),)


def test_cooperative_in_flight_cancellation_preserves_recovery_frontier() -> None:
    token = CancellationToken()

    def cancel(request: RunnerNodeRequest) -> RunnerNodeResult:
        token.request()
        return RunnerNodeResult(request.node.node_id, RunnerNodeOutcome.CANCELLED)

    runner = SequentialRunner(_Executor(callback=cancel), cancellation=token)
    report = runner.run(_plan("a", "b"))
    assert report.status is RunnerStatus.CANCELLED
    assert report.scheduler_state.active_node_id == _id("a")
    assert runner.state is report.scheduler_state


def test_unrequested_cancel_and_executor_protocol_errors_preserve_active_state() -> None:
    plan = _plan("a")
    runner = SequentialRunner(_Executor((RunnerNodeOutcome.CANCELLED,)))
    with pytest.raises(RunnerProtocolError, match="requires requested"):
        runner.run(plan)
    assert runner.state is not None
    assert runner.state.active_node_id == _id("a")

    def mismatch(request: RunnerNodeRequest) -> RunnerNodeResult:
        return RunnerNodeResult(_id("other"), RunnerNodeOutcome.SUCCEEDED)

    runner = SequentialRunner(_Executor(callback=mismatch))
    with pytest.raises(RunnerProtocolError, match="does not match"):
        runner.run(plan)
    assert runner.state is not None
    assert runner.state.active_node_id == _id("a")

    class InvalidExecutor(_Executor):
        def execute(self, request: RunnerNodeRequest, /) -> RunnerNodeResult:
            del request
            return cast(Any, object())

    with pytest.raises(RunnerProtocolError, match="invalid result"):
        SequentialRunner(InvalidExecutor()).run(plan)


@pytest.mark.parametrize("error", [RuntimeError("boom"), KeyboardInterrupt()])
def test_unexpected_failures_propagate_without_false_advancement(error: BaseException) -> None:
    def fail(request: RunnerNodeRequest) -> RunnerNodeResult:
        del request
        raise error

    runner = SequentialRunner(_Executor(callback=fail))
    with pytest.raises(type(error), match="boom" if isinstance(error, RuntimeError) else None):
        runner.run(_plan("a"))
    assert runner.state is not None
    assert runner.state.active_node_id == _id("a")
    assert "running=False" in repr(runner)


def test_exact_terminal_replay_is_inert_and_divergence_is_rejected() -> None:
    plan = _plan("a")
    executor = _Executor()
    runner = SequentialRunner(executor)
    succeeded = runner.run(plan).scheduler_state
    replay = runner.run(plan, state=succeeded)
    assert replay.status is RunnerStatus.SUCCEEDED
    assert replay.started_node_ids == ()
    assert len(executor.requests) == 1

    failed = _failed_state(plan)
    failed_replay = runner.run(plan, state=failed)
    assert failed_replay.status is RunnerStatus.FAILED
    assert failed_replay.started_node_ids == ()

    changed = replace(
        plan,
        resource_policy=replace(plan.resource_policy, operation_timeout_seconds=61),
    )
    with pytest.raises(ValueError, match="plan fingerprint"):
        runner.run(changed, state=succeeded)


def test_in_flight_restore_is_rejected_before_executor_replay() -> None:
    plan = _plan("a")
    tracker = DependencyTracker(plan)
    in_flight = tracker.start(_id("a"))
    executor = _Executor()
    runner = SequentialRunner(executor)
    with pytest.raises(RunnerUnsafeResumeError, match="durable recovery"):
        runner.run(plan, state=in_flight)
    assert runner.state is in_flight
    assert executor.requests == []


def test_runner_rejects_invalid_inputs_unsupported_plan_and_invalid_executor() -> None:
    class MissingClose:
        def execute(self, request: RunnerNodeRequest, /) -> RunnerNodeResult:
            return RunnerNodeResult(request.node.node_id, RunnerNodeOutcome.SUCCEEDED)

    with pytest.raises(TypeError, match="RunnerNodeExecutor"):
        SequentialRunner(cast(Any, MissingClose()))
    with pytest.raises(TypeError, match="CancellationToken"):
        SequentialRunner(_Executor(), cancellation=cast(Any, object()))
    with pytest.raises(TypeError, match="ownership"):
        SequentialRunner(_Executor(), owns_executor=cast(Any, 1))

    runner = SequentialRunner(_Executor())
    with pytest.raises(TypeError, match="ExecutionPlan"):
        runner.run(cast(Any, object()))
    with pytest.raises(TypeError, match="SchedulerState"):
        runner.run(_plan("a"), state=cast(Any, object()))
    unsupported = ExecutionPlan(
        (_node("a", runners=(PlannerRunnerKind.THREADED,)),),
        (),
        ResourcePolicy(),
        (),
    )
    with pytest.raises(RunnerUnsupportedPlanError, match="non-sequential"):
        runner.run(unsupported)
    assert runner.state is None


def test_runner_rejects_reentrant_run_and_close_while_active() -> None:
    runner: SequentialRunner

    def reenter(request: RunnerNodeRequest) -> RunnerNodeResult:
        with pytest.raises(RunnerBusyError, match="active invocation"):
            runner.run(_plan("other"))
        with pytest.raises(RunnerBusyError, match="cannot close"):
            runner.close()
        return RunnerNodeResult(request.node.node_id, RunnerNodeOutcome.SUCCEEDED)

    runner = SequentialRunner(_Executor(callback=reenter))
    assert runner.run(_plan("a")).status is RunnerStatus.SUCCEEDED


def test_runner_ownership_context_and_cleanup_failure_semantics() -> None:
    caller_owned = _Executor()
    runner = SequentialRunner(caller_owned)
    runner.close()
    runner.close()
    assert caller_owned.close_count == 0
    assert runner.is_closed is True
    with pytest.raises(RunnerClosedError, match="cannot execute"):
        runner.run(_plan("a"))
    with pytest.raises(RunnerClosedError, match="cannot be entered"):
        runner.__enter__()

    owned = _Executor()
    with SequentialRunner(owned, owns_executor=True) as entered:
        assert entered.run(_plan("a")).status is RunnerStatus.SUCCEEDED
    assert owned.close_count == 1

    class BrokenClose(_Executor):
        def close(self) -> None:
            raise ValueError("sensitive adapter detail")

    runner = SequentialRunner(BrokenClose(), owns_executor=True)
    with pytest.raises(RunnerCleanupError, match="cleanup failed") as captured:
        runner.close()
    assert "sensitive adapter detail" not in str(captured.value)
    assert runner.is_closed is True

    class FatalClose(_Executor):
        def close(self) -> None:
            raise KeyboardInterrupt

    runner = SequentialRunner(FatalClose(), owns_executor=True)
    with pytest.raises(KeyboardInterrupt):
        runner.close()
    assert runner.is_closed is True


def test_defensive_no_admission_guard_is_typed(monkeypatch: pytest.MonkeyPatch) -> None:
    plan = _plan("a")
    actual = DependencyTracker(plan)

    class NoAdmissionTracker:
        def __init__(self, plan: ExecutionPlan, state: SchedulerState | None = None) -> None:
            del plan, state
            self.state = actual.state

        def next_ready_node(self) -> None:
            return None

    monkeypatch.setattr(runner_module, "DependencyTracker", NoAdmissionTracker)
    with pytest.raises(RunnerProtocolError, match="no admissible"):
        SequentialRunner(_Executor()).run(plan)
