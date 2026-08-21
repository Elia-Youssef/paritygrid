# pyright: reportPrivateUsage=false

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from threading import Event, Thread
from typing import Any, cast

import pytest

import paritygrid.application.execution.pause as pause_module
from paritygrid.application.execution import (
    CancellationToken,
    DependencyTracker,
    PauseAcknowledgement,
    PauseAction,
    PauseCoordinator,
    PauseCoordinatorAdmissionError,
    PauseCoordinatorBusyError,
    PauseCoordinatorClockError,
    PauseCoordinatorIncompleteError,
    PauseCoordinatorInvalidRequestError,
    PauseCoordinatorNotReadyError,
    PauseCoordinatorOutcomeUnknownError,
    PauseCoordinatorRejectedError,
    PauseCoordinatorSettings,
    PauseCoordinatorStateReadError,
    PausedRun,
    PauseDurableState,
    PauseToken,
    RunnerNodeOutcome,
    RunnerNodeRequest,
    RunnerNodeResult,
    RunnerProtocolError,
    RunnerReport,
    RunnerStatus,
    RunnerUnsafeResumeError,
    ScheduledNode,
    ScheduledNodeStatus,
    SchedulerState,
    SchedulerStatus,
    SchedulerTransitionError,
    SequentialRunner,
    WorkLeaseBusyError,
    WorkLeaseOwnershipError,
    WorkLeaseService,
    WorkLeaseSettings,
)
from paritygrid.application.execution.runner import RunnerNodeExecutor
from paritygrid.application.planner import (
    ExecutionPlan,
    ExecutionPlanNode,
    NodeRole,
    PlanFingerprint,
    PlannerRunnerKind,
    ResourcePolicy,
    RetryBehavior,
)
from paritygrid.application.planner.registry import ConnectorRequirement
from paritygrid.application.ports.configuration import (
    ConfigurationDocument,
    DocumentArray,
    NestedDocumentObject,
)
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    ExecutionEventBatch,
    ExecutionEventRecord,
    RedactedDocument,
)
from paritygrid.application.ports.execution import RunRecord
from paritygrid.application.ports.writer import (
    WriterAdmissionTimeoutError,
    WriterClosedError,
    WriterCommand,
    WriterCommandKind,
    WriterCommitOutcomeUnknownError,
    WriterDefinitelyNotExecutedError,
    WriterReceipt,
    WriterResultTimeoutError,
    WriterSubmissionId,
)
from paritygrid.application.writes import TransitionRun, TransitionRunResult
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    StateFingerprint,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import NodeKind

RUN_ID = RunId("run_pause-test")
OTHER_RUN_ID = RunId("run_other-test")
PIPELINE_ID = PipelineId("pip_pause-test")
NODE_A = NodeId("nod_pause-a")
NODE_B = NodeId("nod_pause-b")
FINGERPRINT = PlanFingerprint("0" * 64)
_ACKNOWLEDGEMENT_RUNNERS: dict[PauseToken, SequentialRunner] = {}


def _time(second: int) -> UtcTimestamp:
    return UtcTimestamp(datetime(2025, 1, 1, 0, 0, second, tzinfo=UTC))


def _run(*, state: RunState = RunState.RUNNING, row_version: int = 4) -> RunRecord:
    return RunRecord(
        RUN_ID,
        PIPELINE_ID,
        PipelineVersion(1),
        "sequential",
        ConfigurationDocument(()),
        state,
        row_version,
        None,
        _time(1),
        _time(2),
        None,
        None,
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


def _scheduler_state() -> SchedulerState:
    return DependencyTracker(_plan()).state


def _acknowledgement(
    coordinator: PauseCoordinator,
    state: SchedulerState | None = None,
) -> PauseAcknowledgement:
    class _NeverExecutor:
        def execute(self, request: RunnerNodeRequest, /) -> RunnerNodeResult:
            raise AssertionError(f"paused runner started {request.node.node_id}")

        def close(self) -> None:
            return

    runner = _ACKNOWLEDGEMENT_RUNNERS.get(coordinator.token)
    if runner is None:
        runner = SequentialRunner(_NeverExecutor(), pause=coordinator.token)
        _ACKNOWLEDGEMENT_RUNNERS[coordinator.token] = runner
    report = runner.run(
        _plan(),
        state=state,
    )
    assert report.status is RunnerStatus.PAUSED
    assert report.pause_acknowledgement is not None
    return report.pause_acknowledgement


class _Clock:
    def __init__(self, value: object = None) -> None:
        self.value = _time(8) if value is None else value

    def now(self) -> UtcTimestamp:
        if isinstance(self.value, BaseException):
            raise self.value
        return cast(UtcTimestamp, self.value)


class _Ticket:
    def __init__(
        self,
        submission_id: WriterSubmissionId,
        outcome: object,
    ) -> None:
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
    def __init__(self, state: PauseDurableState) -> None:
        self.state = state
        self.commands: list[WriterCommand] = []
        self.result_failures: dict[int, BaseException] = {}
        self.admission_failures: dict[int, BaseException] = {}
        self.malformed_at: set[int] = set()
        self.closed = False

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
            run = replace(
                previous,
                state=selected.target_state,
                row_version=previous.row_version + 1,
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
            self.state = PauseDurableState(
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

    def read(self, run_id: RunId, /) -> PauseDurableState:
        assert run_id == RUN_ID
        if self.failure is not None:
            raise self.failure
        if self.override is not None:
            return cast(PauseDurableState, self.override)
        return self.writer.state


def _coordinator(
    *,
    state: PauseDurableState | None = None,
    clock: _Clock | None = None,
) -> tuple[PauseCoordinator, _Writer, _Reader, WorkLeaseService]:
    durable = state or PauseDurableState(_run(), EventSequence(5), 5)
    writer = _Writer(durable)
    reader = _Reader(writer)
    selected_clock = clock or _Clock()
    leases = WorkLeaseService(
        writer,
        selected_clock,
        settings=WorkLeaseSettings(),
    )
    coordinator = PauseCoordinator(
        writer,
        reader,
        leases,
        selected_clock,
        settings=PauseCoordinatorSettings(),
    )
    return coordinator, writer, reader, leases


def test_pause_and_resume_commit_exact_pairs_and_reopen_admission() -> None:
    coordinator, writer, _reader, leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    coordinator.request_pause(RUN_ID)
    assert coordinator.token.is_requested

    paused, pause_report = coordinator.pause(
        _acknowledgement(coordinator),
        correlation_id="pause:test-1",
    )
    assert pause_report.action is PauseAction.PAUSED
    assert paused.run.state is RunState.PAUSED
    assert [cast(TransitionRun, command).target_state for command in writer.commands] == [
        RunState.PAUSING,
        RunState.PAUSED,
    ]
    assert [item.event_kind for item in pause_report.events.items] == [
        "run_pausing",
        "run_paused",
    ]
    assert pause_report.events.items[0].occurred_at == pause_report.events.items[1].occurred_at
    assert pause_report.events.items[0].payload.to_mapping() == {
        "from_state": "running",
        "to_state": "pausing",
    }
    with pytest.raises(WorkLeaseBusyError):
        leases.reserve_pause(RUN_ID)

    resume_report = coordinator.resume(paused, correlation_id="pause:test-2")
    assert resume_report.action is PauseAction.RESUMED
    assert resume_report.run.state is RunState.RUNNING
    assert not coordinator.token.is_requested
    assert [cast(TransitionRun, command).target_state for command in writer.commands] == [
        RunState.PAUSING,
        RunState.PAUSED,
        RunState.RESUMING,
        RunState.RUNNING,
    ]
    reservation = leases.reserve_pause(RUN_ID)
    leases.release_pause(reservation)
    assert not writer.closed


def test_pause_request_can_abort_before_any_durable_arrow() -> None:
    coordinator, writer, _reader, leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    coordinator.abort_pause()
    assert not coordinator.token.is_requested
    assert writer.commands == []
    reservation = leases.reserve_pause(RUN_ID)
    leases.release_pause(reservation)
    with pytest.raises(PauseCoordinatorInvalidRequestError):
        coordinator.abort_pause()


def test_pause_requires_request_and_stable_scheduler() -> None:
    coordinator, _writer, _reader, _leases = _coordinator()
    with pytest.raises(PauseCoordinatorInvalidRequestError):
        coordinator.pause(cast(Any, object()))

    coordinator.request_pause(RUN_ID)
    tracker = DependencyTracker(_plan())
    tracker.start(NODE_A)
    with pytest.raises(RunnerUnsafeResumeError):
        _acknowledgement(coordinator, tracker.state)
    coordinator.abort_pause()


def test_pause_waits_for_all_run_scoped_lease_counts() -> None:
    coordinator, _writer, _reader, leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    reservation = cast(object, coordinator)._reservation  # type: ignore[attr-defined]
    work_id = WorkItemId("wrk_fake-work")
    states = cast(dict[object, object], cast(object, leases)._states)  # type: ignore[attr-defined]
    runs = cast(dict[object, object], cast(object, leases)._work_runs)  # type: ignore[attr-defined]
    states[work_id] = None
    runs[work_id] = RUN_ID.value
    with pytest.raises(PauseCoordinatorNotReadyError):
        coordinator.pause(_acknowledgement(coordinator))
    states.clear()
    runs.clear()
    assert reservation is not None
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="acknowledged"):
        coordinator.abort_pause()
    coordinator.pause(_acknowledgement(coordinator))


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (WriterAdmissionTimeoutError(), PauseCoordinatorAdmissionError),
        (WriterDefinitelyNotExecutedError(), PauseCoordinatorRejectedError),
        (WriterResultTimeoutError(), PauseCoordinatorOutcomeUnknownError),
    ],
)
def test_first_arrow_failure_classification(
    failure: BaseException,
    expected: type[BaseException],
) -> None:
    coordinator, writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    if isinstance(failure, WriterAdmissionTimeoutError):
        writer.admission_failures[1] = failure
    else:
        writer.result_failures[1] = failure
    with pytest.raises(expected):
        coordinator.pause(_acknowledgement(coordinator))
    if not isinstance(failure, WriterResultTimeoutError):
        with pytest.raises(PauseCoordinatorInvalidRequestError, match="acknowledged"):
            coordinator.abort_pause()
    else:
        with pytest.raises(PauseCoordinatorInvalidRequestError):
            coordinator.abort_pause()


def test_second_arrow_confirmed_failure_reports_incomplete() -> None:
    coordinator, writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    writer.result_failures[2] = WriterDefinitelyNotExecutedError()
    with pytest.raises(PauseCoordinatorIncompleteError):
        coordinator.pause(_acknowledgement(coordinator))
    assert writer.state.run.state is RunState.PAUSING
    with pytest.raises(PauseCoordinatorInvalidRequestError):
        coordinator.abort_pause()


def test_malformed_committed_receipt_is_protocol_unknown() -> None:
    coordinator, writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    writer.malformed_at.add(1)
    with pytest.raises(PauseCoordinatorOutcomeUnknownError):
        coordinator.pause(_acknowledgement(coordinator))
    assert writer.state.run.state is RunState.PAUSING


def test_reader_clock_state_and_headroom_rejections_are_typed() -> None:
    coordinator, _writer, reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    reader.failure = RuntimeError("credential=secret C:\\machine")
    with pytest.raises(PauseCoordinatorStateReadError, match="frontier read failed"):
        coordinator.pause(_acknowledgement(coordinator))
    reader.failure = None
    reader.override = PauseDurableState(_run(), EventSequence(5), 5, 1)
    with pytest.raises(PauseCoordinatorNotReadyError, match="running work"):
        coordinator.pause(_acknowledgement(coordinator))
    reader.override = PauseDurableState(_run(state=RunState.PAUSED), EventSequence(5), 5)
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="running"):
        coordinator.pause(_acknowledgement(coordinator))
    reader.override = PauseDurableState(
        _run(row_version=2_147_483_646),
        EventSequence(5),
        5,
    )
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="two arrows"):
        coordinator.pause(_acknowledgement(coordinator))
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="acknowledged"):
        coordinator.abort_pause()

    slow, _writer2, _reader2, _leases2 = _coordinator(clock=_Clock(_time(1)))
    slow.request_pause(RUN_ID)
    with pytest.raises(PauseCoordinatorClockError, match="behind"):
        slow.pause(_acknowledgement(slow))
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="acknowledged"):
        slow.abort_pause()


def test_pause_token_and_settings_validate_exact_values() -> None:
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="runner-issued"):
        PauseAcknowledgement(_scheduler_state(), 1, _token=object())
    token = PauseToken()
    generation = token.request_for_coordinator()
    assert token.is_requested
    assert not token.clear_for_coordinator(generation + 1)
    assert token.clear_for_coordinator(generation)
    assert "requested=False" in repr(token)
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="runner-issued"):
        token._acknowledge_for_runner(
            _scheduler_state(),
            authority=object(),
            _token=object(),
        )
    runner_token = pause_module._PAUSE_RUNNER_TOKEN
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="bind only"):
        token._bind_runner(_token=object())
    authority = token._bind_runner(_token=runner_token)
    with pytest.raises(PauseCoordinatorBusyError, match="already bound"):
        token._bind_runner(_token=runner_token)
    assert (
        token._acknowledge_for_runner(
            _scheduler_state(),
            authority=authority,
            _token=runner_token,
        )
        is None
    )
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="authority is foreign"):
        token._acknowledge_for_runner(
            _scheduler_state(),
            authority=object(),
            _token=runner_token,
        )
    token.request_for_coordinator()
    active = DependencyTracker(_plan())
    active.start(NODE_A)
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="stable active"):
        token._acknowledge_for_runner(
            active.state,
            authority=authority,
            _token=runner_token,
        )
    acknowledgement = token._acknowledge_for_runner(
        _scheduler_state(),
        authority=authority,
        _token=runner_token,
    )
    assert acknowledgement is not None
    assert acknowledgement.generation == 2
    assert "authority=<redacted>" in repr(acknowledgement)
    object.__setattr__(acknowledgement, "_scheduler_state", object())
    assert token.snapshot_acknowledgement(acknowledgement, 2) is None
    acknowledgement = token._acknowledge_for_runner(
        _scheduler_state(),
        authority=authority,
        _token=runner_token,
    )
    assert acknowledgement is not None
    object.__setattr__(acknowledgement.scheduler_state, "version", 99)
    assert token.snapshot_acknowledgement(acknowledgement, 2) is None
    with pytest.raises(TypeError):
        PauseCoordinatorSettings(admission_timeout_seconds=1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="outside the supported range"):
        PauseCoordinatorSettings(result_timeout_seconds=86_401.0)
    with pytest.raises(PauseCoordinatorInvalidRequestError):
        PausedRun(  # type: ignore[call-arg]
            _run(state=RunState.PAUSED),
            _scheduler_state(),
            ExecutionEventBatch((), EventSequence(1), 1),
            (WriterSubmissionId(1), WriterSubmissionId(2)),
            _token=object(),
        )


def test_pause_acknowledgement_rechecks_runner_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token = PauseToken()
    runner_token = pause_module._PAUSE_RUNNER_TOKEN
    authority = token._bind_runner(_token=runner_token)
    token.request_for_coordinator()
    snapshot = pause_module._snapshot_scheduler

    def change_binding(state: object) -> SchedulerState:
        clean = snapshot(state)
        object.__setattr__(token, "_runner_authority", object())
        return clean

    monkeypatch.setattr(pause_module, "_snapshot_scheduler", change_binding)
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="authority is foreign"):
        token._acknowledge_for_runner(
            _scheduler_state(),
            authority=authority,
            _token=runner_token,
        )


class _Executor(RunnerNodeExecutor):
    def __init__(self, pause: PauseToken, *, pause_during_first: bool) -> None:
        self.pause = pause
        self.pause_during_first = pause_during_first
        self.requests: list[RunnerNodeRequest] = []

    def execute(self, request: RunnerNodeRequest, /) -> RunnerNodeResult:
        self.requests.append(request)
        if self.pause_during_first and len(self.requests) == 1:
            self.pause.request_for_coordinator()
            return RunnerNodeResult(request.node.node_id, RunnerNodeOutcome.PAUSED)
        return RunnerNodeResult(request.node.node_id, RunnerNodeOutcome.SUCCEEDED)

    def close(self) -> None:
        return


def test_runner_pauses_before_first_node_and_after_executor_checkpoint() -> None:
    before = PauseToken()
    before.request_for_coordinator()
    executor = _Executor(before, pause_during_first=False)
    report = SequentialRunner(executor, pause=before).run(_plan())
    assert report.status is RunnerStatus.PAUSED
    assert report.started_node_ids == ()
    assert report.scheduler_state.active_node_id is None

    during = PauseToken()
    executor = _Executor(during, pause_during_first=True)
    runner = SequentialRunner(executor, pause=during)
    report = runner.run(_plan())
    assert report.status is RunnerStatus.PAUSED
    assert report.started_node_ids == (NODE_A,)
    assert report.scheduler_state.ready_node_ids == (NODE_A, NODE_B)
    assert report.scheduler_state.active_node_id is None
    assert executor.requests[0].pause is during
    RunnerReport(
        RunnerStatus.PAUSED,
        report.scheduler_state,
        report.started_node_ids,
        report.pause_acknowledgement,
    )

    between = PauseToken()

    class SuccessfulCheckpointExecutor(_Executor):
        def execute(self, request: RunnerNodeRequest, /) -> RunnerNodeResult:
            self.requests.append(request)
            if len(self.requests) == 1:
                self.pause.request_for_coordinator()
            return RunnerNodeResult(request.node.node_id, RunnerNodeOutcome.SUCCEEDED)

    executor = SuccessfulCheckpointExecutor(between, pause_during_first=False)
    report = SequentialRunner(executor, pause=between).run(_plan())
    assert report.status is RunnerStatus.PAUSED
    assert report.started_node_ids == (NODE_A,)
    assert report.scheduler_state.succeeded_node_ids == (NODE_A,)
    assert report.scheduler_state.ready_node_ids == (NODE_B,)
    assert report.pause_acknowledgement is not None


def test_runner_rejects_unrequested_paused_outcome() -> None:
    class InvalidExecutor(_Executor):
        def execute(self, request: RunnerNodeRequest, /) -> RunnerNodeResult:
            return RunnerNodeResult(request.node.node_id, RunnerNodeOutcome.PAUSED)

    token = PauseToken()
    with pytest.raises(RunnerProtocolError, match="requires requested pause"):
        SequentialRunner(InvalidExecutor(token, pause_during_first=False), pause=token).run(_plan())


def test_runner_cancellation_wins_when_both_control_signals_are_requested() -> None:
    pause = PauseToken()
    pause.request_for_coordinator()
    cancellation = CancellationToken()
    cancellation.request()
    executor = _Executor(pause, pause_during_first=False)
    report = SequentialRunner(
        executor,
        pause=pause,
        cancellation=cancellation,
    ).run(_plan())
    assert report.status is RunnerStatus.CANCELLED
    assert report.started_node_ids == ()
    assert executor.requests == []


def test_runner_pause_contract_rejects_wrong_token_and_unstable_reports() -> None:
    token = PauseToken()
    executor = _Executor(token, pause_during_first=False)
    with pytest.raises(TypeError, match="PauseToken"):
        SequentialRunner(executor, pause=cast(Any, object()))
    runner = SequentialRunner(executor, pause=token)
    assert runner.pause is token

    active = DependencyTracker(_plan())
    active.start(NODE_A)
    with pytest.raises(RunnerProtocolError, match="stable active"):
        RunnerReport(RunnerStatus.PAUSED, active.state, (NODE_A,))
    terminal = DependencyTracker(_plan())
    terminal.start(NODE_A)
    terminal.succeed(NODE_A)
    terminal.start(NODE_B)
    terminal.succeed(NODE_B)
    with pytest.raises(RunnerProtocolError, match="stable active"):
        RunnerReport(RunnerStatus.PAUSED, terminal.state, ())

    coordinator, _writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    acknowledgement = _acknowledgement(coordinator)
    advanced = DependencyTracker(_plan())
    advanced.start(NODE_A)
    advanced.succeed(NODE_A)
    with pytest.raises(RunnerProtocolError, match="does not match"):
        RunnerReport(
            RunnerStatus.PAUSED,
            advanced.state,
            (NODE_A,),
            acknowledgement,
        )
    with pytest.raises(RunnerProtocolError, match="non-paused"):
        RunnerReport(
            RunnerStatus.CANCELLED,
            _scheduler_state(),
            (),
            acknowledgement,
        )
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="acknowledged"):
        coordinator.abort_pause()


def test_coordinator_rejects_foreign_boundary_while_real_runner_is_active() -> None:
    entered = Event()
    release = Event()

    class BlockingExecutor:
        def execute(self, request: RunnerNodeRequest, /) -> RunnerNodeResult:
            entered.set()
            assert release.wait(timeout=5.0)
            outcome = (
                RunnerNodeOutcome.PAUSED
                if request.pause.is_requested
                else RunnerNodeOutcome.SUCCEEDED
            )
            return RunnerNodeResult(request.node.node_id, outcome)

        def close(self) -> None:
            return

    coordinator, writer, _reader, _leases = _coordinator()
    reports: list[RunnerReport] = []
    failures: list[BaseException] = []
    runner = SequentialRunner(BlockingExecutor(), pause=coordinator.token)

    def run() -> None:
        try:
            reports.append(runner.run(_plan()))
        except BaseException as error:  # pragma: no cover - asserted empty below
            failures.append(error)

    thread = Thread(target=run)
    thread.start()
    assert entered.wait(timeout=5.0)
    coordinator.request_pause(RUN_ID)
    with pytest.raises(PauseCoordinatorBusyError, match="already bound"):
        SequentialRunner(BlockingExecutor(), pause=coordinator.token)

    foreign, _writer, _reader, _leases = _coordinator()
    foreign.request_pause(RUN_ID)
    foreign_acknowledgement = _acknowledgement(foreign)
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="stale or foreign"):
        coordinator.pause(foreign_acknowledgement)
    assert writer.commands == []
    assert thread.is_alive()

    release.set()
    thread.join(timeout=5.0)
    assert not thread.is_alive()
    assert failures == []
    assert reports[0].status is RunnerStatus.PAUSED
    assert reports[0].pause_acknowledgement is not None
    coordinator.pause(reports[0].pause_acknowledgement)
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="acknowledged"):
        foreign.abort_pause()


def test_scheduler_pause_rejects_terminal_or_nonactive_node() -> None:
    tracker = DependencyTracker(_plan())
    tracker.start(NODE_A)
    with pytest.raises(SchedulerTransitionError, match="only the active"):
        tracker.pause(NODE_B)
    tracker.succeed(NODE_A)
    tracker.start(NODE_B)
    tracker.succeed(NODE_B)
    with pytest.raises(SchedulerTransitionError, match="terminal"):
        tracker.pause(NODE_B)


def test_pause_value_objects_and_repr_validate_closed_types() -> None:
    durable = PauseDurableState(_run(), EventSequence(5), 5)
    assert "run_pause-test" in repr(durable)
    for values, error in (
        ((cast(Any, object()), EventSequence(1), 1), TypeError),
        ((_run(), cast(Any, object()), 1), TypeError),
        ((_run(), EventSequence(1), cast(Any, True)), TypeError),
        ((_run(), EventSequence(1), 0), ValueError),
        ((_run(), EventSequence(1), 1, cast(Any, True)), TypeError),
        ((_run(), EventSequence(1), 1, -1), ValueError),
        (
            (_run(), EventSequence(1), 1, pause_module.MAX_CONSISTENCY_SEQUENCE + 1),
            ValueError,
        ),
    ):
        with pytest.raises(error):
            PauseDurableState(*cast(Any, values))


def test_coordinator_constructor_rejects_wrong_collaborators() -> None:
    coordinator, writer, reader, leases = _coordinator()
    clock = _Clock()
    assert coordinator.token is coordinator.token
    invalid = cast(Any, object())
    with pytest.raises(TypeError, match="writer"):
        PauseCoordinator(invalid, reader, leases, clock)
    with pytest.raises(TypeError, match="reader"):
        PauseCoordinator(writer, invalid, leases, clock)
    with pytest.raises(TypeError, match="lease service"):
        PauseCoordinator(writer, reader, invalid, clock)
    with pytest.raises(TypeError, match="clock"):
        PauseCoordinator(writer, reader, leases, invalid)
    with pytest.raises(TypeError, match="settings"):
        PauseCoordinator(writer, reader, leases, clock, settings=invalid)


@pytest.mark.parametrize("value", ["", "space value", "a" * 97, 1])
def test_pause_rejects_nonportable_correlation(value: object) -> None:
    coordinator, _writer, _reader, _leases = _coordinator()
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="correlation"):
        coordinator.pause(cast(Any, object()), correlation_id=cast(Any, value))


def test_pause_control_operations_reject_overlap_and_incompatible_request() -> None:
    coordinator, _writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    acknowledgement = _acknowledgement(coordinator)
    lock = cast(Any, coordinator)._operation_lock
    lock.acquire()
    try:
        with pytest.raises(PauseCoordinatorBusyError):
            coordinator.request_pause(OTHER_RUN_ID)
        with pytest.raises(PauseCoordinatorBusyError):
            coordinator.pause(acknowledgement)
        with pytest.raises(PauseCoordinatorBusyError):
            coordinator.abort_pause()
    finally:
        lock.release()
    with pytest.raises(PauseCoordinatorBusyError, match="owns a request"):
        coordinator.request_pause(OTHER_RUN_ID)
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="acknowledged"):
        coordinator.abort_pause()


def test_paused_authority_rejects_repeat_forgery_mutation_and_stale_state() -> None:
    coordinator, writer, reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    paused, report = coordinator.pause(_acknowledgement(coordinator))
    assert "authority=<redacted>" in repr(paused)
    assert "action='paused'" in repr(report)
    with pytest.raises(PauseCoordinatorBusyError, match="already paused"):
        coordinator.pause(_acknowledgement(coordinator))
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="proof"):
        coordinator.resume(cast(Any, object()))

    replacement = object.__new__(PausedRun)
    for name in ("_run", "_scheduler_state", "_events", "_submission_ids"):
        object.__setattr__(replacement, name, getattr(paused, name))
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="not active"):
        coordinator.resume(replacement)

    original_run = paused.run
    object.__setattr__(paused, "_run", replace(original_run, row_version=99))
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="changed"):
        coordinator.resume(paused)
    object.__setattr__(paused, "_run", original_run)
    reader.override = replace(writer.state, run=replace(writer.state.run, row_version=99))
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="stale"):
        coordinator.resume(paused)


def test_pause_report_is_detached_from_paused_authority() -> None:
    coordinator, _writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    paused, report = coordinator.pause(_acknowledgement(coordinator))
    original_row_version = paused.run.row_version
    object.__setattr__(report.run, "row_version", 99)
    object.__setattr__(report.scheduler_state, "version", 99)
    object.__setattr__(report.events, "counter_row_version", 99)
    assert paused.run.row_version == original_row_version
    assert paused.scheduler_state.version != 99
    assert paused.events.counter_row_version != 99
    assert coordinator.resume(paused).action is PauseAction.RESUMED


@pytest.mark.parametrize(
    ("step", "failure", "expected"),
    [
        (3, WriterResultTimeoutError(), PauseCoordinatorOutcomeUnknownError),
        (3, WriterDefinitelyNotExecutedError(), PauseCoordinatorRejectedError),
        (4, WriterDefinitelyNotExecutedError(), PauseCoordinatorIncompleteError),
        (4, WriterCommitOutcomeUnknownError(), PauseCoordinatorOutcomeUnknownError),
    ],
)
def test_resume_failure_matrix(
    step: int,
    failure: BaseException,
    expected: type[BaseException],
) -> None:
    coordinator, writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    paused, _report = coordinator.pause(_acknowledgement(coordinator))
    writer.result_failures[step] = failure
    with pytest.raises(expected):
        coordinator.resume(paused)


def test_writer_ordinary_admission_and_result_failures_are_redacted() -> None:
    coordinator, writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    writer.admission_failures[1] = WriterClosedError("credential=top-secret C:\\machine")
    with pytest.raises(PauseCoordinatorAdmissionError) as captured:
        coordinator.pause(_acknowledgement(coordinator))
    assert "top-secret" not in str(captured.value)
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="acknowledged"):
        coordinator.abort_pause()

    coordinator, writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    writer.admission_failures[1] = RuntimeError("credential=top-secret C:\\machine")
    with pytest.raises(PauseCoordinatorOutcomeUnknownError) as captured:
        coordinator.pause(_acknowledgement(coordinator))
    assert "top-secret" not in str(captured.value)

    coordinator, writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    writer.result_failures[1] = RuntimeError("credential=top-secret C:\\machine")
    with pytest.raises(PauseCoordinatorOutcomeUnknownError) as captured:
        coordinator.pause(_acknowledgement(coordinator))
    assert "top-secret" not in str(captured.value)


def test_clock_invalid_value_and_exception_are_typed_without_cause() -> None:
    for value, message in (
        (object(), "invalid time"),
        (RuntimeError("credential=secret"), "clock failed"),
    ):
        coordinator, _writer, _reader, _leases = _coordinator(clock=_Clock(value))
        coordinator.request_pause(RUN_ID)
        with pytest.raises(PauseCoordinatorClockError, match=message) as captured:
            coordinator.pause(_acknowledgement(coordinator))
        assert captured.value.__cause__ is None
        assert captured.value.__context__ is None
        with pytest.raises(PauseCoordinatorInvalidRequestError, match="acknowledged"):
            coordinator.abort_pause()


def test_private_canonicalizers_reject_malformed_outer_and_nested_evidence() -> None:
    with pytest.raises(TypeError):
        pause_module._snapshot_durable_state(object())
    invalid_count = PauseDurableState(_run(), EventSequence(1), 1)
    object.__setattr__(
        invalid_count,
        "active_work_count",
        pause_module.MAX_CONSISTENCY_SEQUENCE + 1,
    )
    with pytest.raises(ValueError, match="outside the supported range"):
        pause_module._snapshot_durable_state(invalid_count)
    with pytest.raises(TypeError):
        pause_module._snapshot_run(object())
    with pytest.raises(PauseCoordinatorInvalidRequestError):
        pause_module._snapshot_scheduler(object())
    active = DependencyTracker(_plan())
    active.start(NODE_A)
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="active node"):
        PauseCoordinator._require_stable_scheduler(active.state)
    with pytest.raises(TypeError):
        pause_module._snapshot_event_batch(object())
    with pytest.raises(TypeError):
        pause_module._snapshot_event_record(object())
    with pytest.raises(TypeError):
        pause_module._snapshot_document(object())
    with pytest.raises(TypeError):
        pause_module._snapshot_document_pair(object())
    with pytest.raises(TypeError):
        pause_module._snapshot_document_pair(("only-one",))
    with pytest.raises(TypeError):
        pause_module._snapshot_document_value(object())
    with pytest.raises(TypeError):
        pause_module._snapshot_redacted_document(object())
    for operation in (
        pause_module._snapshot_run_id,
        pause_module._snapshot_node_id,
        pause_module._snapshot_pipeline_id,
        pause_module._snapshot_pipeline_version,
        pause_module._snapshot_plan_fingerprint,
        pause_module._snapshot_timestamp,
        pause_module._snapshot_event_sequence,
        pause_module._snapshot_submission_id,
    ):
        with pytest.raises(TypeError):
            operation(object())


def test_private_canonicalizers_copy_nested_documents_and_optional_run_fields() -> None:
    document = ConfigurationDocument(
        (
            ("array", DocumentArray((True, 3, "text", None))),
            ("nested", NestedDocumentObject((("value", "ok"),))),
        )
    )
    rich = replace(
        _run(),
        runner_configuration=document,
        scenario_seed=7,
        cancellation_requested_at=_time(3),
        recovery_started_at=_time(4),
        recovered_at=_time(5),
        final_reconciliation_fingerprint=StateFingerprint("2" * 64),
    )
    assert pause_module._snapshot_run(rich) == rich
    assert pause_module._snapshot_document(document) == document
    with pytest.raises(TypeError):
        pause_module._exact_enum("running", RunState, "run state")
    with pytest.raises(TypeError):
        pause_module._exact_text(1, "text")
    with pytest.raises(TypeError):
        pause_module._exact_integer(True, "integer")
    with pytest.raises(ValueError, match="outside the supported range"):
        pause_module._bounded_positive(0, "positive")


def test_event_canonicalizers_reject_wrong_subject_and_malformed_scheduler_node() -> None:
    state = _scheduler_state()
    bad_node = object.__new__(ScheduledNode)
    object.__setattr__(bad_node, "node_id", NODE_A)
    object.__setattr__(bad_node, "status", ScheduledNodeStatus.READY)
    object.__setattr__(bad_node, "remaining_dependency_ids", [])
    bad_state = object.__new__(SchedulerState)
    object.__setattr__(bad_state, "status", SchedulerStatus.ACTIVE)
    object.__setattr__(bad_state, "nodes", (bad_node,))
    object.__setattr__(bad_state, "plan_fingerprint", FINGERPRINT)
    object.__setattr__(bad_state, "version", 1)
    with pytest.raises(PauseCoordinatorInvalidRequestError):
        pause_module._snapshot_scheduler(bad_state)

    event = ExecutionEventRecord(
        RUN_ID,
        EventSequence(1),
        "work_started",
        _time(3),
        EventSubjectKind.WORK_ITEM,
        WorkItemId("wrk_pause-event"),
        None,
        1,
        RedactedDocument.from_mapping({"value": "safe"}),
    )
    with pytest.raises(TypeError, match="subject"):
        pause_module._snapshot_event_record(event)
    assert pause_module._snapshot_scheduler(state) == state


def test_ticket_receipt_and_event_pair_validation_guards() -> None:
    state = PauseDurableState(_run(), EventSequence(5), 5)
    command = pause_module._transition_command(state, RunState.PAUSING, _time(8), None)
    writer = _Writer(state)
    ticket = writer.submit(command, timeout_seconds=5.0)
    receipt = ticket.result(timeout_seconds=60.0)
    submission_id = ticket.submission_id
    assert pause_module._ticket_identity(ticket) == submission_id
    assert (
        pause_module._validate_receipt(
            receipt,
            submission_id,
            command,
            state.run,
        )[0].state
        is RunState.PAUSING
    )
    with pytest.raises(PauseCoordinatorOutcomeUnknownError):
        pause_module._validate_receipt(object(), submission_id, command, state.run)
    for changed in (
        replace(receipt, submission_id=WriterSubmissionId(9)),
        replace(receipt, command_kind=WriterCommandKind.CREATE_CAPTURED_RUN),
        replace(receipt, run_id=OTHER_RUN_ID),
        replace(receipt, contention_attempts=cast(Any, True)),
        replace(receipt, contention_attempts=10),
        replace(receipt, mutated=False),
        replace(receipt, result=cast(Any, object())),
        replace(
            receipt,
            result=replace(
                cast(TransitionRunResult, receipt.result),
                run=replace(cast(TransitionRunResult, receipt.result).run, row_version=99),
            ),
        ),
    ):
        with pytest.raises(PauseCoordinatorOutcomeUnknownError):
            pause_module._validate_receipt(changed, submission_id, command, state.run)

    first = cast(TransitionRunResult, receipt.result).events
    second = replace(first, items=(replace(first.items[0], sequence=EventSequence(9)),))
    with pytest.raises(PauseCoordinatorOutcomeUnknownError, match="contiguous"):
        pause_module._combine_events(first, second)


def test_ticket_identity_rejects_wrong_and_raising_evidence() -> None:
    for identity in (object(), TypeError("bad identity")):
        ticket = _Ticket(cast(Any, identity), object())
        with pytest.raises(PauseCoordinatorOutcomeUnknownError, match="identity"):
            pause_module._ticket_identity(ticket)

    class RaisingTicket(_Ticket):
        @property
        def submission_id(self) -> WriterSubmissionId:
            raise TypeError("credential=secret")

    with pytest.raises(PauseCoordinatorOutcomeUnknownError, match="identity"):
        pause_module._ticket_identity(RaisingTicket(WriterSubmissionId(1), object()))


def test_headroom_checks_each_frontier_independently() -> None:
    maximum = 2_147_483_646
    states = (
        PauseDurableState(_run(row_version=maximum), EventSequence(5), 5),
        PauseDurableState(_run(), EventSequence(maximum), 5),
        PauseDurableState(_run(), EventSequence(5), maximum),
    )
    for state in states:
        with pytest.raises(PauseCoordinatorInvalidRequestError, match="two arrows"):
            pause_module._require_pair_headroom(state)


def test_suppressed_cleanup_helper_never_leaks_base_exception() -> None:
    pause_module._suppress_base_exception(lambda: (_ for _ in ()).throw(KeyboardInterrupt()))


def test_request_translates_existing_gate_and_cleans_up_interrupted_signal() -> None:
    coordinator, _writer, _reader, leases = _coordinator()
    existing = leases.reserve_pause(RUN_ID)
    with pytest.raises(PauseCoordinatorBusyError, match="gate"):
        coordinator.request_pause(RUN_ID)
    leases.release_pause(existing)

    class FatalToken:
        def request_for_coordinator(self) -> int:
            raise KeyboardInterrupt

    object.__setattr__(coordinator, "_token", FatalToken())
    with pytest.raises(KeyboardInterrupt):
        coordinator.request_pause(RUN_ID)
    reservation = leases.reserve_pause(RUN_ID)
    leases.release_pause(reservation)


def test_unknown_pause_and_resume_cannot_be_retried_without_recovery() -> None:
    coordinator, writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    writer.result_failures[1] = WriterResultTimeoutError()
    with pytest.raises(PauseCoordinatorOutcomeUnknownError):
        coordinator.pause(_acknowledgement(coordinator))
    with pytest.raises(PauseCoordinatorOutcomeUnknownError, match="recovery"):
        coordinator.pause(_acknowledgement(coordinator))

    coordinator, writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    writer.result_failures[2] = WriterCommitOutcomeUnknownError()
    with pytest.raises(PauseCoordinatorOutcomeUnknownError):
        coordinator.pause(_acknowledgement(coordinator))

    coordinator, writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    paused, _report = coordinator.pause(_acknowledgement(coordinator))
    writer.result_failures[3] = WriterResultTimeoutError()
    with pytest.raises(PauseCoordinatorOutcomeUnknownError):
        coordinator.resume(paused)
    with pytest.raises(PauseCoordinatorOutcomeUnknownError, match="recovery"):
        coordinator.resume(paused)


def test_resume_rejects_overlap_and_fails_closed_on_signal_generation_change() -> None:
    coordinator, _writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    paused, _report = coordinator.pause(_acknowledgement(coordinator))
    lock = cast(Any, coordinator)._operation_lock
    lock.acquire()
    try:
        with pytest.raises(PauseCoordinatorBusyError):
            coordinator.resume(paused)
    finally:
        lock.release()
    object.__setattr__(coordinator, "_generation", 99)
    with pytest.raises(PauseCoordinatorIncompleteError, match="signal"):
        coordinator.resume(paused)


class _FailingDeleteDict(dict[object, object]):
    def __delitem__(self, key: object) -> None:
        super().__delitem__(key)
        raise WorkLeaseOwnershipError("injected gate removal interruption")


def test_resume_and_abort_translate_gate_release_failure() -> None:
    coordinator, _writer, _reader, leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    paused, _report = coordinator.pause(_acknowledgement(coordinator))
    gates = cast(dict[object, object], cast(Any, leases)._pause_gates)
    object.__setattr__(leases, "_pause_gates", _FailingDeleteDict(gates))
    with pytest.raises(PauseCoordinatorIncompleteError, match="admission remains closed"):
        coordinator.resume(paused)

    coordinator, _writer, _reader, leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    gates = cast(dict[object, object], cast(Any, leases)._pause_gates)
    object.__setattr__(leases, "_pause_gates", _FailingDeleteDict(gates))
    with pytest.raises(PauseCoordinatorOutcomeUnknownError, match="gate release"):
        coordinator.abort_pause()


def test_abort_rejects_missing_signal_and_pause_translates_lost_gate() -> None:
    coordinator, _writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    acknowledgement = _acknowledgement(coordinator)
    object.__setattr__(coordinator, "_generation", None)
    with pytest.raises(PauseCoordinatorOutcomeUnknownError, match="generation"):
        coordinator.pause(acknowledgement)
    with pytest.raises(PauseCoordinatorOutcomeUnknownError, match="signal"):
        coordinator.abort_pause()

    coordinator, _writer, _reader, leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    gates = cast(dict[object, object], cast(Any, leases)._pause_gates)
    gates.clear()
    with pytest.raises(PauseCoordinatorOutcomeUnknownError, match="snapshot"):
        coordinator.pause(_acknowledgement(coordinator))


def test_reader_malformed_value_is_protocol_unknown_without_context() -> None:
    coordinator, _writer, reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    reader.override = object()
    with pytest.raises(
        PauseCoordinatorOutcomeUnknownError,
        match="frontier is invalid",
    ) as captured:
        coordinator.pause(_acknowledgement(coordinator))
    assert captured.value.__context__ is None


@pytest.mark.parametrize("boundary", ["submit", "result"])
def test_fatal_writer_interruptions_propagate_and_poison_retry(boundary: str) -> None:
    coordinator, writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    if boundary == "submit":
        writer.admission_failures[1] = KeyboardInterrupt()
    else:
        writer.result_failures[1] = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        coordinator.pause(_acknowledgement(coordinator))
    with pytest.raises(PauseCoordinatorOutcomeUnknownError, match="recovery"):
        coordinator.pause(_acknowledgement(coordinator))


def test_fatal_ticket_identity_and_receipt_validation_interruptions_poison_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FatalIdentityTicket(_Ticket):
        @property
        def submission_id(self) -> WriterSubmissionId:
            raise KeyboardInterrupt

    class FatalIdentityWriter(_Writer):
        def submit(self, command: WriterCommand, *, timeout_seconds: float) -> _Ticket:
            self.commands.append(command)
            return FatalIdentityTicket(WriterSubmissionId(1), object())

    coordinator, _writer, _reader, leases = _coordinator()
    fatal_writer = FatalIdentityWriter(PauseDurableState(_run(), EventSequence(5), 5))
    coordinator = PauseCoordinator(fatal_writer, _Reader(fatal_writer), leases, _Clock())
    coordinator.request_pause(RUN_ID)
    with pytest.raises(KeyboardInterrupt):
        coordinator.pause(_acknowledgement(coordinator))

    coordinator, _writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)

    def fatal_receipt(*_values: object) -> Any:
        raise KeyboardInterrupt

    monkeypatch.setattr(pause_module, "_validate_receipt", fatal_receipt)
    with pytest.raises(KeyboardInterrupt):
        coordinator.pause(_acknowledgement(coordinator))


def test_mark_intermediate_interrupt_falls_back_to_unknown() -> None:
    coordinator, _writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)

    class InterruptingLock:
        def __init__(self) -> None:
            self.count = 0

        def __enter__(self) -> None:
            self.count += 1
            if self.count >= 3:
                raise KeyboardInterrupt

        def __exit__(self, *_values: object) -> None:
            return

    object.__setattr__(coordinator, "_lifecycle_lock", InterruptingLock())
    with pytest.raises(KeyboardInterrupt):
        coordinator.pause(_acknowledgement(coordinator))
    assert cast(Any, coordinator)._uncertain is True


def test_paused_evidence_and_optional_fingerprint_reject_malformed_values() -> None:
    coordinator, _writer, _reader, _leases = _coordinator()
    coordinator.request_pause(RUN_ID)
    paused, _report = coordinator.pause(_acknowledgement(coordinator))
    object.__setattr__(paused, "_run", object())
    assert pause_module._paused_evidence(paused) is None
    with pytest.raises(TypeError):
        pause_module._optional_fingerprint(object())
