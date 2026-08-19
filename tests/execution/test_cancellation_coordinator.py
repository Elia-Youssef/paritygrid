# pyright: reportPrivateUsage=false

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from paritygrid.application.execution import (
    CancellationAction,
    CancellationCleanupError,
    CancellationCoordinator,
    CancellationCoordinatorAdmissionError,
    CancellationCoordinatorBusyError,
    CancellationCoordinatorClockError,
    CancellationCoordinatorIncompleteError,
    CancellationCoordinatorInvalidRequestError,
    CancellationCoordinatorNotReadyError,
    CancellationCoordinatorOutcomeUnknownError,
    CancellationCoordinatorRejectedError,
    CancellationCoordinatorSettings,
    CancellationCoordinatorStateReadError,
    CancellationDurableState,
    CancellationReport,
    RunnerNodeOutcome,
    RunnerNodeRequest,
    RunnerNodeResult,
    RunnerStatus,
    SequentialRunner,
    WorkLease,
    WorkLeaseBusyError,
    WorkLeaseService,
    WorkLeaseSettings,
)
from paritygrid.application.execution.leasing import _LEASE_CONSTRUCTION_TOKEN
from paritygrid.application.execution.result_sink import (
    ResultSinkCommitted,
)
from paritygrid.application.planner import (
    ExecutionPlan,
    ExecutionPlanNode,
    NodeRole,
    PlannerRunnerKind,
    ResourcePolicy,
    RetryBehavior,
)
from paritygrid.application.planner.registry import ConnectorRequirement
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    EventSequence,
    ExecutionEventBatch,
    ExecutionEventRecord,
)
from paritygrid.application.ports.execution import (
    RunNodeRecord,
    RunNodeStatus,
    RunRecord,
    WorkClaim,
)
from paritygrid.application.ports.run_aggregates import WorkMetricDelta
from paritygrid.application.ports.writer import (
    WriterAdmissionTimeoutError,
    WriterCommand,
    WriterCommandKind,
    WriterDefinitelyNotExecutedError,
    WriterReceipt,
    WriterResultTimeoutError,
    WriterSubmissionId,
)
from paritygrid.application.writes import TransitionRun, TransitionRunResult
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    AttemptNumber,
    Duration,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import NodeKind, PartitionKey

RUN_ID = RunId("run_cancel-test")
OTHER_RUN_ID = RunId("run_other-test")
PIPELINE_ID = PipelineId("pip_cancel-test")
NODE_A = NodeId("nod_cancel-a")
NODE_B = NodeId("nod_cancel-b")
WORK_ID = WorkItemId("wrk_cancel-1")
PARTITION = PartitionKey("part-cancel")


def _time(second: int) -> UtcTimestamp:
    return UtcTimestamp(
        datetime(2025, 1, 1, 0, 0, second % 60, tzinfo=UTC) + timedelta(minutes=second // 60)
    )


def _run(
    *,
    state: RunState = RunState.RUNNING,
    row_version: int = 4,
    cancellation_requested_at: UtcTimestamp | None = None,
    finished_at: UtcTimestamp | None = None,
    run_id: RunId = RUN_ID,
) -> RunRecord:
    return RunRecord(
        run_id,
        PIPELINE_ID,
        PipelineVersion(1),
        "sequential",
        ConfigurationDocument(()),
        state,
        row_version,
        None,
        _time(1),
        _time(2) if state is not RunState.QUEUED else None,
        finished_at,
        cancellation_requested_at,
        None,
        None,
        None,
    )


def _plan() -> ExecutionPlan:
    def node(node_id: NodeId) -> ExecutionPlanNode:
        return ExecutionPlanNode(
            node_id=node_id,
            kind=NodeKind("transform.normalize"),
            role=NodeRole.TRANSFORM,
            configuration_version=1,
            configuration=ConfigurationDocument(()),
            connector_requirement=ConnectorRequirement.NONE,
            connector_id=None,
            supported_runners=(PlannerRunnerKind.SEQUENTIAL,),
            retry_behavior=RetryBehavior.NEVER,
            requires_idempotency=False,
        )

    return ExecutionPlan(
        nodes=(node(NODE_A), node(NODE_B)),
        edges=(),
        resource_policy=ResourcePolicy(),
        connector_bindings=(),
    )


class _Clock:
    def __init__(self, value: object = None) -> None:
        self.value = _time(8) if value is None else value

    def now(self) -> UtcTimestamp:
        if isinstance(self.value, BaseException):
            raise self.value
        return cast(UtcTimestamp, self.value)


class _Resource:
    def __init__(self, *, failure: BaseException | None = None) -> None:
        self.failure = failure
        self.closed = False
        self.observed_timeout: float | None = None

    def close(self, *, timeout_seconds: float) -> None:
        self.observed_timeout = timeout_seconds
        self.closed = True
        if self.failure is not None:
            raise self.failure


class _Ticket:
    def __init__(self, submission_id: WriterSubmissionId, outcome: object) -> None:
        self._submission_id = submission_id
        self._outcome = outcome

    @property
    def submission_id(self) -> WriterSubmissionId:
        return self._submission_id

    def result(self, *, timeout_seconds: float) -> WriterReceipt:
        assert timeout_seconds == 60.0
        if isinstance(self._outcome, BaseException):
            raise self._outcome
        if callable(self._outcome):
            return cast(WriterReceipt, self._outcome())
        return cast(WriterReceipt, self._outcome)

    async def result_async(self, *, timeout_seconds: float) -> WriterReceipt:
        return self.result(timeout_seconds=timeout_seconds)


class _Writer:
    def __init__(self, state: CancellationDurableState) -> None:
        self.state = state
        self.commands: list[WriterCommand] = []
        self.result_failures: dict[int, BaseException] = {}
        self.admission_failures: dict[int, BaseException] = {}
        self.malformed_at: set[int] = set()

    def submit(self, command: WriterCommand, *, timeout_seconds: float) -> _Ticket:
        assert timeout_seconds == 5.0
        index = len(self.commands) + 1
        failure = self.admission_failures.get(index)
        if failure is not None:
            raise failure
        self.commands.append(command)
        submission_id = WriterSubmissionId(index)
        result_failure = self.result_failures.get(index)
        if result_failure is not None:
            return _Ticket(submission_id, result_failure)

        def commit() -> WriterReceipt:
            selected = cast(TransitionRun, command)
            previous = self.state.run
            cancellation_requested_at = previous.cancellation_requested_at
            finished_at = previous.finished_at
            if selected.target_state is RunState.CANCELLING or (
                previous.state is RunState.QUEUED and selected.target_state is RunState.CANCELLED
            ):
                cancellation_requested_at = selected.transitioned_at
            if selected.target_state.is_terminal:
                finished_at = selected.transitioned_at
            run = replace(
                previous,
                state=selected.target_state,
                row_version=previous.row_version + 1,
                cancellation_requested_at=cancellation_requested_at,
                finished_at=finished_at,
            )
            pending = selected.event.event
            events = ExecutionEventBatch(
                (
                    ExecutionEventRecord(
                        selected.run_id,
                        selected.event.expected_next_sequence,
                        pending.event_kind,
                        pending.occurred_at,
                        pending.subject_kind,
                        pending.subject_id,
                        pending.correlation_id,
                        pending.payload_schema_version,
                        pending.payload,
                    ),
                ),
                selected.event.expected_next_sequence.advance(1),
                selected.event.expected_counter_row_version + 1,
            )
            self.state = CancellationDurableState(
                run,
                events.next_sequence,
                events.counter_row_version,
            )
            receipt = WriterReceipt(
                submission_id,
                WriterCommandKind.TRANSITION_RUN,
                selected.run_id,
                0,
                True,
                TransitionRunResult(run, events),
            )
            if index in self.malformed_at:
                return replace(receipt, mutated=False)
            return receipt

        return _Ticket(submission_id, commit)


class _Reader:
    def __init__(self, writer: _Writer) -> None:
        self.writer = writer
        self.failure: BaseException | None = None
        self.override: object | None = None

    def read(self, run_id: RunId, /) -> CancellationDurableState:
        assert run_id == RUN_ID
        if self.failure is not None:
            raise self.failure
        if self.override is not None:
            return cast(CancellationDurableState, self.override)
        return self.writer.state


class _Sink:
    def __init__(self) -> None:
        self.submissions: list[object] = []
        self.failure: BaseException | None = None

    def submit(self, submission: Any, /) -> ResultSinkCommitted:
        self.submissions.append(submission)
        if self.failure is not None:
            raise self.failure
        context = submission.result.terminal.context
        return ResultSinkCommitted(
            WriterSubmissionId(9),
            submission.result.kind,
            context.run_id,
            context.node_id,
            context.work_item_id,
            context.attempt_number,
            None,
        )


def _lease(*, expires_second: int = 60) -> WorkLease:
    claim = WorkClaim(
        WORK_ID,
        AttemptNumber(1),
        "lease-owner",
        2,
        _time(4),
        _time(expires_second),
        "sequential",
        "reference-worker",
    )
    node = RunNodeRecord(
        RUN_ID,
        NODE_A,
        RunNodeStatus.RUNNING,
        2,
        1,
        0,
        1,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        Duration(0),
        _time(2),
        None,
    )
    events = ExecutionEventBatch((), EventSequence(6), 6)
    return WorkLease(
        claim,
        node,
        _run(),
        events,
        WriterSubmissionId(3),
        _token=_LEASE_CONSTRUCTION_TOKEN,
    )


def _coordinator(
    *,
    state: CancellationDurableState | None = None,
    clock: _Clock | None = None,
) -> tuple[CancellationCoordinator, _Writer, _Reader, WorkLeaseService, _Sink]:
    durable = state or CancellationDurableState(_run(), EventSequence(5), 5)
    writer = _Writer(durable)
    reader = _Reader(writer)
    selected_clock = clock or _Clock()
    leases = WorkLeaseService(writer, selected_clock, settings=WorkLeaseSettings())
    sink = _Sink()
    coordinator = CancellationCoordinator(
        writer,
        reader,
        leases,
        sink,
        selected_clock,
        settings=CancellationCoordinatorSettings(),
    )
    return coordinator, writer, reader, leases, sink


def test_cancel_running_run_commits_two_arrows_and_cleans_up() -> None:
    coordinator, writer, _reader, leases, _sink = _coordinator()
    resource = _Resource()
    coordinator.register(resource)
    coordinator.request_cancellation(RUN_ID)
    coordinator.request_cancellation(RUN_ID)
    assert coordinator.token.is_requested

    report = coordinator.cancel(correlation_id="cancel:test-1")

    assert report.action is CancellationAction.CANCELLED
    assert report.run.state is RunState.CANCELLED
    assert report.run.row_version == 6
    assert report.run.cancellation_requested_at == _time(8)
    assert report.run.finished_at == _time(8)
    assert [cast(TransitionRun, command).target_state for command in writer.commands] == [
        RunState.CANCELLING,
        RunState.CANCELLED,
    ]
    assert [item.event_kind for item in report.events.items] == [
        "run_cancelling",
        "run_cancelled",
    ]
    assert report.events.items[0].payload.to_mapping() == {
        "from_state": "running",
        "to_state": "cancelling",
    }
    assert report.events.items[1].payload.to_mapping() == {
        "from_state": "cancelling",
        "to_state": "cancelled",
    }
    assert report.submission_ids == (WriterSubmissionId(1), WriterSubmissionId(2))
    assert report.cleanup_closed == 1
    assert resource.closed
    assert resource.observed_timeout == 5.0
    assert writer.state.run.state is RunState.CANCELLED
    reservation = leases.reserve_pause(RUN_ID)
    leases.release_pause(reservation)
    with pytest.raises(CancellationCoordinatorInvalidRequestError):
        coordinator.register(_Resource())


def test_cancel_queued_run_uses_one_arrow_before_start() -> None:
    coordinator, writer, _reader, _leases, _sink = _coordinator(
        state=CancellationDurableState(_run(state=RunState.QUEUED), EventSequence(2), 2),
    )
    coordinator.request_cancellation(RUN_ID)
    report = coordinator.cancel()
    assert report.action is CancellationAction.CANCELLED_BEFORE_START
    assert report.run.state is RunState.CANCELLED
    assert report.run.row_version == 5
    assert report.run.cancellation_requested_at == _time(8)
    assert report.run.finished_at == _time(8)
    assert report.cleanup_closed == 0
    assert [cast(TransitionRun, command).target_state for command in writer.commands] == [
        RunState.CANCELLED
    ]
    assert [item.event_kind for item in report.events.items] == ["run_cancelled"]


def test_cancel_completes_an_interrupted_cancelling_state() -> None:
    coordinator, writer, _reader, _leases, _sink = _coordinator(
        state=CancellationDurableState(
            _run(state=RunState.CANCELLING, cancellation_requested_at=_time(7)),
            EventSequence(6),
            6,
        ),
    )
    coordinator.request_cancellation(RUN_ID)
    report = coordinator.cancel()
    assert report.action is CancellationAction.CANCELLED
    assert report.run.state is RunState.CANCELLED
    assert report.run.cancellation_requested_at == _time(7)
    assert report.run.finished_at == _time(8)
    assert [cast(TransitionRun, command).target_state for command in writer.commands] == [
        RunState.CANCELLED
    ]
    assert [item.event_kind for item in report.events.items] == ["run_cancelled"]
    assert report.events.items[0].payload.to_mapping() == {
        "from_state": "cancelling",
        "to_state": "cancelled",
    }


def test_duplicate_cancellation_converges_without_mutation() -> None:
    coordinator, writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    first = coordinator.cancel()
    assert first.action is CancellationAction.CANCELLED

    coordinator.request_cancellation(RUN_ID)
    second = coordinator.cancel()

    assert second.action is CancellationAction.ALREADY_CANCELLED
    assert second.run.state is RunState.CANCELLED
    assert second.submission_ids == ()
    assert second.events.items == ()
    assert len(writer.commands) == 2


@pytest.mark.parametrize(
    "state",
    [RunState.PAUSING, RunState.PAUSED, RunState.RESUMING],
)
def test_cancel_rejects_pause_lifecycle_without_reinterpreting_it(state: RunState) -> None:
    coordinator, _writer, _reader, _leases, _sink = _coordinator(
        state=CancellationDurableState(_run(state=state), EventSequence(5), 5),
    )
    coordinator.request_cancellation(RUN_ID)
    with pytest.raises(CancellationCoordinatorInvalidRequestError, match="pause lifecycle"):
        coordinator.cancel()


@pytest.mark.parametrize(
    "state",
    [RunState.SUCCEEDED, RunState.PARTIALLY_SUCCEEDED, RunState.FAILED],
)
def test_terminal_runs_cannot_become_active_again(state: RunState) -> None:
    coordinator, writer, _reader, _leases, _sink = _coordinator(
        state=CancellationDurableState(
            _run(state=state, finished_at=_time(6)),
            EventSequence(5),
            5,
        ),
    )
    coordinator.request_cancellation(RUN_ID)
    with pytest.raises(CancellationCoordinatorInvalidRequestError, match="terminal"):
        coordinator.cancel()
    assert writer.commands == []


def test_cancel_requires_a_requested_run_and_single_owner() -> None:
    coordinator, _writer, _reader, _leases, _sink = _coordinator()
    with pytest.raises(CancellationCoordinatorInvalidRequestError, match="not been requested"):
        coordinator.cancel()
    coordinator.request_cancellation(RUN_ID)
    coordinator.request_cancellation(RUN_ID)
    with pytest.raises(CancellationCoordinatorBusyError):
        coordinator.request_cancellation(OTHER_RUN_ID)
    assert coordinator.token.is_requested


def test_request_cancellation_conflicts_with_an_installed_pause_gate() -> None:
    coordinator, _writer, _reader, leases, _sink = _coordinator()
    reservation = leases.reserve_pause(RUN_ID)
    with pytest.raises(CancellationCoordinatorBusyError, match="admission gate"):
        coordinator.request_cancellation(RUN_ID)
    assert not coordinator.token.is_requested
    leases.release_pause(reservation)
    coordinator.request_cancellation(RUN_ID)
    with pytest.raises(WorkLeaseBusyError):
        leases.reserve_pause(RUN_ID)


def test_cancel_waits_for_lease_ownership_and_durable_work_to_drain() -> None:
    coordinator, _writer, _reader, leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    work_id = WorkItemId("wrk_cancel-active")
    states = cast(dict[object, object], cast(object, leases)._states)  # type: ignore[attr-defined]
    runs = cast(dict[object, object], cast(object, leases)._work_runs)  # type: ignore[attr-defined]
    states[work_id] = None
    runs[work_id] = RUN_ID.value
    with pytest.raises(CancellationCoordinatorNotReadyError, match="lease ownership"):
        coordinator.cancel()
    states.clear()
    in_flight = cast(Any, leases)._in_flight
    in_flight.add(work_id)
    try:
        with pytest.raises(CancellationCoordinatorNotReadyError, match="lease ownership"):
            coordinator.cancel()
    finally:
        in_flight.discard(work_id)
    runs.clear()
    coordinator_reader_override_states(coordinator, active_work_count=1)
    with pytest.raises(CancellationCoordinatorNotReadyError, match="running work"):
        coordinator.cancel()


def coordinator_reader_override_states(
    coordinator: CancellationCoordinator,
    *,
    active_work_count: int,
) -> None:
    reader = cast(_Reader, cast(object, coordinator)._reader)  # type: ignore[attr-defined]
    writer = reader.writer
    reader.override = CancellationDurableState(
        writer.state.run,
        writer.state.next_event_sequence,
        writer.state.event_counter_row_version,
        active_work_count,
    )


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (WriterAdmissionTimeoutError(), CancellationCoordinatorAdmissionError),
        (WriterDefinitelyNotExecutedError(), CancellationCoordinatorRejectedError),
        (WriterResultTimeoutError(), CancellationCoordinatorOutcomeUnknownError),
    ],
)
def test_first_arrow_failure_classification(
    failure: BaseException,
    expected: type[BaseException],
) -> None:
    coordinator, writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    if isinstance(failure, WriterAdmissionTimeoutError):
        writer.admission_failures[1] = failure
    else:
        writer.result_failures[1] = failure
    with pytest.raises(expected):
        coordinator.cancel()
    if isinstance(failure, WriterResultTimeoutError):
        with pytest.raises(CancellationCoordinatorOutcomeUnknownError, match="inspection"):
            coordinator.cancel()
    else:
        assert writer.state.run.state is RunState.RUNNING
        writer.admission_failures.clear()
        writer.result_failures.clear()
        assert coordinator.cancel().action is CancellationAction.CANCELLED


def test_second_arrow_confirmed_failure_reports_incomplete() -> None:
    coordinator, writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    writer.result_failures[2] = WriterDefinitelyNotExecutedError()
    with pytest.raises(CancellationCoordinatorIncompleteError):
        coordinator.cancel()
    assert writer.state.run.state is RunState.CANCELLING
    with pytest.raises(CancellationCoordinatorOutcomeUnknownError, match="inspection"):
        coordinator.cancel()


def test_malformed_committed_receipt_is_protocol_unknown() -> None:
    coordinator, writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    writer.malformed_at.add(1)
    with pytest.raises(CancellationCoordinatorOutcomeUnknownError):
        coordinator.cancel()
    assert writer.state.run.state is RunState.CANCELLING


def test_reader_state_and_clock_rejections_are_typed_and_redacted() -> None:
    coordinator, _writer, reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    reader.failure = RuntimeError("credential=secret C:\\machine")
    with pytest.raises(CancellationCoordinatorStateReadError, match="frontier read failed"):
        coordinator.cancel()
    reader.failure = None
    reader.override = object()
    with pytest.raises(CancellationCoordinatorOutcomeUnknownError) as captured:
        coordinator.cancel()
    assert "secret" not in str(captured.value)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


def test_clock_failures_and_headroom_are_rejected_before_admission() -> None:
    clock = _Clock()
    coordinator, _writer, _reader, _leases, _sink = _coordinator(clock=clock)
    coordinator.request_cancellation(RUN_ID)
    clock.value = RuntimeError("clock broke")
    with pytest.raises(CancellationCoordinatorClockError, match="clock failed"):
        coordinator.cancel()
    clock.value = _time(1)
    with pytest.raises(CancellationCoordinatorClockError, match="behind durable"):
        coordinator.cancel()
    clock.value = _time(8)
    crowded, _crowded_writer, _crowded_reader, _crowded_leases, _crowded_sink = _coordinator(
        state=CancellationDurableState(
            _run(row_version=2_147_483_646),
            EventSequence(2_147_483_646),
            2_147_483_646,
        ),
    )
    crowded.request_cancellation(RUN_ID)
    with pytest.raises(CancellationCoordinatorInvalidRequestError, match="advance its arrows"):
        crowded.cancel()


def test_fatal_interruptions_propagate_and_poison_retry() -> None:
    coordinator, writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    fatal = KeyboardInterrupt("interrupted")
    writer.result_failures[1] = fatal
    with pytest.raises(KeyboardInterrupt) as captured:
        coordinator.cancel()
    assert captured.value is fatal
    assert writer.state.run.state is RunState.RUNNING
    with pytest.raises(CancellationCoordinatorOutcomeUnknownError, match="inspection"):
        coordinator.cancel()


def test_correlation_identifiers_are_validated() -> None:
    coordinator, _writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    with pytest.raises(CancellationCoordinatorInvalidRequestError, match="correlation"):
        coordinator.cancel(correlation_id="not portable!")
    with pytest.raises(CancellationCoordinatorInvalidRequestError, match="correlation"):
        coordinator.cancel(correlation_id="x" * 97)
    assert coordinator.cancel(correlation_id="cancel:ok-1").events.items[0].correlation_id == (
        "cancel:ok-1"
    )


def test_constructor_and_settings_validate_collaborators() -> None:
    coordinator, writer, reader, leases, sink = _coordinator()
    clock = _Clock()
    with pytest.raises(TypeError, match="writer"):
        CancellationCoordinator(object(), reader, leases, sink, clock)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="reader"):
        CancellationCoordinator(writer, object(), leases, sink, clock)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="lease service"):
        CancellationCoordinator(writer, reader, object(), sink, clock)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="sink"):
        CancellationCoordinator(writer, reader, leases, object(), clock)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="clock"):
        CancellationCoordinator(writer, reader, leases, sink, object())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="settings"):
        CancellationCoordinator(writer, reader, leases, sink, clock, settings=object())  # type: ignore[arg-type]
    del coordinator
    with pytest.raises(TypeError, match="must be a float"):
        CancellationCoordinatorSettings(admission_timeout_seconds=1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="supported range"):
        CancellationCoordinatorSettings(result_timeout_seconds=86_400.5)
    with pytest.raises(ValueError, match="supported range"):
        CancellationCoordinatorSettings(cleanup_timeout_seconds=-1.0)


def test_resource_registration_is_bounded_and_validated() -> None:
    coordinator, _writer, _reader, _leases, _sink = _coordinator()
    with pytest.raises(CancellationCoordinatorInvalidRequestError, match="bounded close"):
        coordinator.register(object())  # type: ignore[arg-type]
    for _ in range(64):
        coordinator.register(_Resource())
    with pytest.raises(CancellationCoordinatorInvalidRequestError, match="limit"):
        coordinator.register(_Resource())


def test_cleanup_failures_do_not_acknowledge_incomplete_cleanup() -> None:
    coordinator, _writer, _reader, _leases, _sink = _coordinator()
    failing = _Resource(failure=RuntimeError("close failed"))
    coordinator.register(failing)
    coordinator.register(_Resource())
    coordinator.request_cancellation(RUN_ID)
    with pytest.raises(CancellationCleanupError, match="bounded cleanup"):
        coordinator.cancel()
    assert _writer_state_is_cancelled(coordinator)

    other, _other_writer, _other_reader, _other_leases, _other_sink = _coordinator()
    timed_out = _Resource(failure=TimeoutError("cleanup exceeded its bound"))
    other.register(timed_out)
    other.request_cancellation(RUN_ID)
    with pytest.raises(CancellationCleanupError):
        other.cancel()

    fatal_close, _fatal_writer, _fatal_reader, _fatal_leases, _fatal_sink = _coordinator()
    fatal = KeyboardInterrupt("interrupted cleanup")
    fatal_close_resource = _Resource(failure=fatal)
    fatal_close.register(fatal_close_resource)
    fatal_close.request_cancellation(RUN_ID)
    with pytest.raises(KeyboardInterrupt) as captured:
        fatal_close.cancel()
    assert captured.value is fatal


def _writer_state_is_cancelled(coordinator: CancellationCoordinator) -> bool:
    reader = cast(_Reader, cast(object, coordinator)._reader)  # type: ignore[attr-defined]
    return reader.writer.state.run.state is RunState.CANCELLED


def test_gate_release_failure_reports_incomplete_after_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from paritygrid.application.execution.leasing import WorkLeaseError as _LeaseError

    coordinator, _writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)

    def _fail_release(self: object, reservation: object) -> None:
        raise _LeaseError("gate stuck")

    monkeypatch.setattr(WorkLeaseService, "release_pause", _fail_release)
    with pytest.raises(CancellationCoordinatorIncompleteError, match="admission remains closed"):
        coordinator.cancel()
    assert _writer_state_is_cancelled(coordinator)


def test_cancel_work_commits_one_owned_claim_without_retry() -> None:
    coordinator, _writer, _reader, leases, sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    lease = _lease()
    _activate_lease(leases, lease)
    outcome = coordinator.cancel_work(lease, finished_at=_time(5), detail="user stopped")
    assert type(outcome) is ResultSinkCommitted
    assert outcome.result_kind.value == "cancelled"
    assert outcome.checkpoint_version is None
    assert len(sink.submissions) == 1
    submission = cast(Any, sink.submissions[0])
    terminal = submission.result.terminal
    assert terminal.failure_classification.value == "user_cancellation"
    assert submission.result.decision is None
    metrics = submission.result.metrics
    assert (metrics.records_processed, metrics.bytes_processed) == (0, 0)
    assert metrics.aggregate_delta == WorkMetricDelta(0, 0, 0, 0, 0)
    assert leases.snapshot().active == 0


def _activate_lease(leases: WorkLeaseService, lease: WorkLease) -> None:
    from paritygrid.application.execution.leasing import _ActiveWorkLease

    states = cast(dict[object, object], cast(object, leases)._states)  # type: ignore[attr-defined]
    runs = cast(dict[object, object], cast(object, leases)._work_runs)  # type: ignore[attr-defined]
    states[lease.claim.work_item_id] = _ActiveWorkLease.capture(lease)
    runs[lease.claim.work_item_id] = RUN_ID.value


def test_cancel_work_rejects_foreign_leases_and_invalid_evidence() -> None:
    coordinator, _writer, _reader, _leases, _sink = _coordinator()
    with pytest.raises(CancellationCoordinatorInvalidRequestError, match="not been requested"):
        coordinator.cancel_work(_lease(), finished_at=_time(5))
    coordinator.request_cancellation(RUN_ID)
    with pytest.raises(CancellationCoordinatorInvalidRequestError, match="lease"):
        coordinator.cancel_work(object(), finished_at=_time(5))  # type: ignore[arg-type]
    foreign = _lease()
    object.__setattr__(
        cast(object, foreign),
        "_run",
        _run(run_id=OTHER_RUN_ID),
    )
    with pytest.raises(CancellationCoordinatorInvalidRequestError, match="another run"):
        coordinator.cancel_work(foreign, finished_at=_time(5))
    expired = _lease(expires_second=5)
    with pytest.raises(Exception, match="lease expiry"):
        coordinator.cancel_work(expired, finished_at=_time(5))
    unknown_runner = _lease()
    claim = replace(unknown_runner.claim, runner_kind="mystery")
    object.__setattr__(cast(object, unknown_runner), "_claim", claim)
    with pytest.raises(CancellationCoordinatorInvalidRequestError, match="registered"):
        coordinator.cancel_work(unknown_runner, finished_at=_time(5))
    with pytest.raises(CancellationCoordinatorInvalidRequestError, match="detail"):
        coordinator.cancel_work(_lease(), finished_at=_time(5), detail="x" * 4_097)
    with pytest.raises(CancellationCoordinatorInvalidRequestError, match="detail"):
        coordinator.cancel_work(_lease(), finished_at=_time(5), detail="")


def test_cancel_work_refuses_uncertain_and_completed_lifecycles() -> None:
    coordinator, _writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    object.__setattr__(cast(object, coordinator), "_uncertain", True)
    with pytest.raises(CancellationCoordinatorOutcomeUnknownError, match="inspection"):
        coordinator.cancel_work(_lease(), finished_at=_time(5))
    object.__setattr__(cast(object, coordinator), "_uncertain", False)
    object.__setattr__(cast(object, coordinator), "_completed", True)
    with pytest.raises(CancellationCoordinatorInvalidRequestError, match="already completed"):
        coordinator.cancel_work(_lease(), finished_at=_time(5))


def test_sequential_runner_observes_cancellation_before_and_during_work() -> None:
    from paritygrid.application.execution import CancellationToken

    class _CancellingExecutor:
        def __init__(self) -> None:
            self.executed = 0

        def execute(self, request: RunnerNodeRequest, /) -> RunnerNodeResult:
            self.executed += 1
            request.cancellation.request()
            return RunnerNodeResult(request.node.node_id, RunnerNodeOutcome.CANCELLED)

        def close(self) -> None:
            return

    requested = CancellationToken()
    requested.request()
    stopped = SequentialRunner(_CancellingExecutor(), cancellation=requested)
    report = stopped.run(_plan())
    assert report.status is RunnerStatus.CANCELLED
    assert report.started_node_ids == ()

    active = SequentialRunner(_CancellingExecutor())
    active_report = active.run(_plan())
    assert active_report.status is RunnerStatus.CANCELLED
    assert active_report.started_node_ids == (NODE_A,)


def test_cancellation_report_repr_is_bounded() -> None:
    coordinator, _writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    report = coordinator.cancel()
    assert "CancellationReport(" in repr(report)
    assert "cancel-test" in repr(report)
    durable = CancellationDurableState(_run(), EventSequence(5), 5)
    assert "CancellationDurableState(" in repr(durable)
    assert durable.active_work_count == 0


def test_report_requires_a_closed_action_and_matching_run() -> None:
    coordinator, _writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    report = coordinator.cancel()
    assert type(report) is CancellationReport
    with pytest.raises(TypeError, match="cancellation action is invalid"):
        CancellationReport(
            cast(Any, "cancelled"),
            report.run,
            report.events,
            report.submission_ids,
            report.cleanup_closed,
        )
    with pytest.raises(TypeError, match="cancellation submissions"):
        CancellationReport(
            CancellationAction.CANCELLED,
            report.run,
            report.events,
            cast(Any, (1,)),
            report.cleanup_closed,
        )
    with pytest.raises(ValueError, match="cleanup count"):
        CancellationReport(
            CancellationAction.CANCELLED,
            report.run,
            report.events,
            report.submission_ids,
            -1,
        )
