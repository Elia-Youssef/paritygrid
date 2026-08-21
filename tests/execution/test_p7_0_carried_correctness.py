"""P7.0 deterministic regressions for the three carried Phase 6 findings."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from threading import Event, Thread
from typing import Any, cast

import pytest

import paritygrid.application.execution.pause as pause_module
from paritygrid.application.execution import (
    MAX_LEASE_EVENT_CORRELATION_ID_LENGTH,
    AcquireWorkLeaseRequest,
    AttemptCancelled,
    AttemptSucceeded,
    CheckpointCommitSettings,
    DependencyTracker,
    PauseAction,
    PauseCoordinator,
    PauseCoordinatorError,
    PauseCoordinatorInvalidRequestError,
    PauseCoordinatorSettings,
    PauseDurableState,
    PauseToken,
    RenewWorkLeaseRequest,
    ResultCheckpoint,
    ResultMetrics,
    ResultSinkInvalidResultError,
    ResultSinkOutcome,
    ResultSinkPreAdmissionError,
    ResultSubmission,
    RunnerNodeOutcome,
    RunnerNodeRequest,
    RunnerNodeResult,
    RunnerProtocolError,
    RunnerReport,
    RunnerStatus,
    SequentialRunner,
    SuccessfulWorkResult,
    TransactionalCheckpointResultSink,
    WorkLeaseBusyError,
    WorkLeaseInvalidRequestError,
    WorkLeaseService,
    WorkLeaseServiceSnapshot,
    WorkLeaseSettings,
    submit_work_result,
)
from paritygrid.application.execution.checkpoint_commit import (
    CheckpointCommitInvalidRequestError,
)
from paritygrid.application.execution.runner import RunnerNodeExecutor
from paritygrid.application.planner import (
    ExecutionPlan,
    ExecutionPlanNode,
    NodeRole,
    PlannerRunnerKind,
    ResourcePolicy,
    RetryBehavior,
)
from paritygrid.application.planner.registry import ConnectorRequirement
from paritygrid.application.ports.artifacts import (
    ArtifactId,
    ArtifactManifestRecord,
    ArtifactRelativePath,
)
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    CheckpointVersion,
    EventSequence,
    EventSubjectKind,
    ExecutionEventBatch,
    ExecutionEventRecord,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.execution import (
    AttemptOutcome,
    RunNodeRecord,
    RunNodeStatus,
    RunRecord,
)
from paritygrid.application.ports.run_aggregates import WorkMetricDelta
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    WriterCommand,
    WriterReceipt,
    WriterSubmissionId,
)
from paritygrid.application.writes import TransitionRunResult
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

RUN_ID = RunId("run_p7-0-carried")
NODE_ID = NodeId("nod_p7-0-carried")
WORK_ID = WorkItemId("wrk_p7-0-carried")
PIPELINE_ID = PipelineId("pip_p7-0-carried")
PARTITION = PartitionKey("part_default")
BASE = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
CORRELATION_96 = (
    "c"
    + "0123456789._:-abcdefghij" * 3
    + "x" * (MAX_LEASE_EVENT_CORRELATION_ID_LENGTH - 1 - 24 * 3)
)
assert len(CORRELATION_96) == MAX_LEASE_EVENT_CORRELATION_ID_LENGTH


def _timestamp(second: int) -> UtcTimestamp:
    return UtcTimestamp(BASE + timedelta(seconds=second))


def _document(**values: object) -> ConfigurationDocument:
    return ConfigurationDocument.from_mapping(values)


class _Clock:
    def __init__(self, *values: object) -> None:
        self.values = list(values or (_timestamp(3),))
        self.calls = 0

    def now(self) -> UtcTimestamp:
        self.calls += 1
        value = self.values.pop(0)
        if isinstance(value, BaseException):
            raise value
        return cast(UtcTimestamp, value)


class _LeaseTicket:
    def __init__(self, submission_id: WriterSubmissionId, receipt: WriterReceipt) -> None:
        self._submission_id = submission_id
        self._receipt = receipt

    @property
    def submission_id(self) -> WriterSubmissionId:
        return self._submission_id

    def result(self, *, timeout_seconds: float) -> WriterReceipt:
        return self._receipt

    async def result_async(self, *, timeout_seconds: float) -> WriterReceipt:
        return self._receipt


class _LeaseWriter:
    """Spy writer whose claim receipts mirror the leasing contract fixtures."""

    def __init__(self) -> None:
        from paritygrid.application.ports.execution import WorkClaim
        from paritygrid.application.writes import ClaimWork, ClaimWorkResult

        self.commands: list[WriterCommand] = []
        self._work_claim = WorkClaim
        self._claim_work = ClaimWork
        self._claim_result = ClaimWorkResult

    def submit(self, command: WriterCommand, *, timeout_seconds: float) -> _LeaseTicket:
        from paritygrid.application.writes import ClaimWork, ClaimWorkResult

        self.commands.append(command)
        submission_id = WriterSubmissionId(len(self.commands))
        assert type(command) is ClaimWork
        claimed = cast(ClaimWork, command)
        claim = self._work_claim(
            work_item_id=claimed.work_item_id,
            attempt_number=AttemptNumber(1),
            lease_owner=claimed.lease_owner,
            row_version=claimed.expected_work_row_version + 1,
            started_at=claimed.started_at,
            lease_expires_at=claimed.lease_expires_at,
            runner_kind=claimed.runner_kind,
            worker_identity=claimed.worker_identity,
        )
        node = RunNodeRecord(
            run_id=claimed.run_id,
            node_id=claimed.node_id,
            status=RunNodeStatus.RUNNING,
            row_version=claimed.expected_node_row_version + 1,
            work_total=1,
            work_pending=0,
            work_running=1,
            work_succeeded=0,
            work_quarantined=0,
            work_failed=0,
            work_cancelled=0,
            records_read=0,
            records_written=0,
            records_quarantined=0,
            bytes_read=0,
            bytes_written=0,
            retry_count=0,
            duration=Duration(0),
            started_at=claimed.started_at,
            finished_at=None,
        )
        run = _run(claimed.expected_run_row_version + 1)
        pending = claimed.event.event
        record = ExecutionEventRecord(
            run_id=claimed.run_id,
            sequence=claimed.event.expected_next_sequence,
            event_kind=pending.event_kind,
            occurred_at=pending.occurred_at,
            subject_kind=pending.subject_kind,
            subject_id=pending.subject_id,
            correlation_id=pending.correlation_id,
            payload_schema_version=pending.payload_schema_version,
            payload=pending.payload,
        )
        events = ExecutionEventBatch(
            (record,),
            claimed.event.expected_next_sequence.advance(1),
            claimed.event.expected_counter_row_version + 1,
        )
        receipt = WriterReceipt(
            submission_id,
            command.kind,
            claimed.run_id,
            0,
            True,
            ClaimWorkResult(claim, node, events, run),
        )
        return _LeaseTicket(submission_id, receipt)


def _run(row_version: int = 3) -> RunRecord:
    return RunRecord(
        run_id=RUN_ID,
        pipeline_id=PIPELINE_ID,
        pipeline_version=PipelineVersion(1),
        runner_kind=PlannerRunnerKind.SEQUENTIAL.value,
        runner_configuration=_document(max_concurrency=1),
        state=RunState.RUNNING,
        row_version=row_version,
        scenario_seed=None,
        created_at=_timestamp(0),
        started_at=_timestamp(1),
        finished_at=None,
        cancellation_requested_at=None,
        recovery_started_at=None,
        recovered_at=None,
        execution_evidence_fingerprint=None,
    )


def _lease_event(sequence: int, kind: str, correlation_id: str | None) -> EventAppendRequest:
    pending = PendingExecutionEvent(
        event_kind=kind,
        occurred_at=_timestamp(3),
        subject_kind=EventSubjectKind.WORK_ITEM,
        subject_id=WORK_ID,
        correlation_id=correlation_id,
        payload_schema_version=1,
        payload=RedactedDocument.from_mapping({"kind": kind}),
    )
    return EventAppendRequest(EventSequence(sequence), sequence, pending)


def _acquire_request(correlation_id: str | None) -> AcquireWorkLeaseRequest:
    return AcquireWorkLeaseRequest(
        run_id=RUN_ID,
        node_id=NODE_ID,
        work_item_id=WORK_ID,
        expected_attempt_number=AttemptNumber(1),
        expected_work_row_version=1,
        expected_node_row_version=2,
        expected_run_row_version=3,
        lease_owner="p7-0-owner",
        runner_kind=PlannerRunnerKind.SEQUENTIAL.value,
        worker_identity="p7-0-worker",
        event=_lease_event(4, "work_claimed", correlation_id),
    )


def _renew_request(correlation_id: str | None) -> RenewWorkLeaseRequest:
    return RenewWorkLeaseRequest(4, _lease_event(5, "work_claim_renewed", correlation_id))


def _lease_service(
    writer: _LeaseWriter | None = None,
    clock: _Clock | None = None,
) -> tuple[WorkLeaseService, _LeaseWriter, _Clock]:
    selected_writer = writer or _LeaseWriter()
    selected_clock = clock or _Clock(_timestamp(3), _timestamp(4))
    service = WorkLeaseService(
        selected_writer,
        selected_clock,
        settings=WorkLeaseSettings(),
    )
    return service, selected_writer, selected_clock


@pytest.mark.parametrize(
    "correlation_id",
    [
        None,
        "a",
        "A9",
        "corr.p7-0:value_1",
        CORRELATION_96,
    ],
)
def test_lease_event_correlation_accepts_none_and_bounded_ascii_values(
    correlation_id: str | None,
) -> None:
    _acquire_request(correlation_id)
    _renew_request(correlation_id)


@pytest.mark.parametrize(
    "correlation_id",
    [
        "",
        " ",
        "\t",
        "\n",
        " \t \n ",
        ".leading-dot",
        "-leading-dash",
        ":leading-colon",
        "_leading-underscore",
        "inner space",
        "tab\tinside",
        "café",
        "运行",
        "co;rel",
        "co?rel",
        "co#rel",
        "co,rel",
        "co=rel",
        "co/rel",
        "co\\rel",
        "a" * (MAX_LEASE_EVENT_CORRELATION_ID_LENGTH + 1),
        cast(Any, 123),
    ],
)
def test_invalid_lease_event_correlation_is_rejected_before_any_side_effect(
    correlation_id: object,
) -> None:
    service, writer, clock = _lease_service()
    with pytest.raises(WorkLeaseInvalidRequestError, match="correlation"):
        AcquireWorkLeaseRequest(
            run_id=RUN_ID,
            node_id=NODE_ID,
            work_item_id=WORK_ID,
            expected_attempt_number=AttemptNumber(1),
            expected_work_row_version=1,
            expected_node_row_version=2,
            expected_run_row_version=3,
            lease_owner="p7-0-owner",
            runner_kind=PlannerRunnerKind.SEQUENTIAL.value,
            worker_identity="p7-0-worker",
            event=_lease_event(4, "work_claimed", cast(Any, correlation_id)),
        )
    with pytest.raises(WorkLeaseInvalidRequestError, match="correlation"):
        _renew_request(cast(Any, correlation_id))
    assert writer.commands == []
    assert clock.calls == 0
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 0, 0)
    with pytest.raises(TypeError):
        service.acquire(cast(Any, object()))
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 0, 0)


def test_maximum_length_correlation_round_trips_into_durable_lease_events() -> None:
    service, writer, _clock = _lease_service()
    lease = service.acquire(_acquire_request(CORRELATION_96))
    assert len(writer.commands) == 1
    assert lease.events.items[0].correlation_id == CORRELATION_96


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
        nodes=(node(NODE_ID), node(NodeId("nod_p7-0-second"))),
        edges=(),
        resource_policy=ResourcePolicy(),
        connector_bindings=(),
    )


class _CheckpointTicket:
    def __init__(self, submission_id: WriterSubmissionId, response: object) -> None:
        self._submission_id = submission_id
        self._response = response

    @property
    def submission_id(self) -> WriterSubmissionId:
        return self._submission_id

    def result(self, *, timeout_seconds: float) -> WriterReceipt:
        if isinstance(self._response, BaseException):
            raise self._response
        return cast(WriterReceipt, self._response)

    async def result_async(self, *, timeout_seconds: float) -> WriterReceipt:
        return self.result(timeout_seconds=timeout_seconds)


class _CheckpointWriter:
    def __init__(self, response: object) -> None:
        self.response = response
        self.commands: list[WriterCommand] = []

    def submit(self, command: WriterCommand, *, timeout_seconds: float) -> _CheckpointTicket:
        self.commands.append(command)
        return _CheckpointTicket(WriterSubmissionId(1), self.response)


def _manifest() -> ArtifactManifestRecord:
    return ArtifactManifestRecord(
        ArtifactId("art_p7-0-carried"),
        RUN_ID,
        NODE_ID,
        PARTITION,
        ArtifactRelativePath("runs/p7-0/output.parquet"),
        "application/vnd.apache.parquet",
        1,
        20,
        10,
        "a" * 64,
        _timestamp(4),
    )


def _success(lease: Any) -> SuccessfulWorkResult:
    from paritygrid.application.execution import AttemptEventContext

    claim = lease.claim
    context = AttemptEventContext(
        RUN_ID,
        NODE_ID,
        WORK_ID,
        AttemptNumber(1),
        claim.started_at,
        PlannerRunnerKind.SEQUENTIAL,
        claim.worker_identity,
        "corr-p7-0",
    )
    return SuccessfulWorkResult(
        AttemptSucceeded(context, _timestamp(5)),
        ResultCheckpoint(
            PARTITION,
            1,
            _document(offset=10),
            _document(rows=10),
            _manifest(),
        ),
        ResultMetrics(10, 20, WorkMetricDelta(7, 8, 1, 9, 10)),
    )


def _success_without_artifact(lease: Any) -> SuccessfulWorkResult:
    from paritygrid.application.execution import AttemptEventContext

    claim = lease.claim
    context = AttemptEventContext(
        RUN_ID,
        NODE_ID,
        WORK_ID,
        AttemptNumber(1),
        claim.started_at,
        PlannerRunnerKind.SEQUENTIAL,
        claim.worker_identity,
        "corr-p7-0",
    )
    return SuccessfulWorkResult(
        AttemptSucceeded(context, _timestamp(5)),
        ResultCheckpoint(
            PARTITION,
            1,
            _document(offset=10),
            _document(rows=10),
            None,
        ),
        ResultMetrics(10, 20, WorkMetricDelta(7, 8, 1, 9, 10)),
    )


def _valid_checkpoint_receipt(
    command: WriterCommand, submission: ResultSubmission
) -> WriterReceipt:
    from paritygrid.application.ports.consistency import (
        CheckpointCommit,
        CheckpointHeadRecord,
        CheckpointRecord,
        UpdatedWorkCheckpoint,
    )
    from paritygrid.application.ports.execution import (
        CompletedWork,
        WorkAttemptRecord,
        WorkItemRecord,
    )
    from paritygrid.application.writes import (
        CommitWorkAttempt,
        CommitWorkResult,
        CommitWorkWithCheckpoint,
    )

    lease = submission.lease
    claim = lease.claim
    result = submission.result
    assert isinstance(result, SuccessfulWorkResult)
    completion = cast(CommitWorkAttempt | CommitWorkWithCheckpoint, command).completion
    finished_at = result.terminal.finished_at
    work = WorkItemRecord(
        WORK_ID,
        RUN_ID,
        NODE_ID,
        PARTITION,
        completion.target_state,
        claim.row_version + 1,
        1,
        0,
        None,
        completion.retry_available_at,
        None,
        None,
        None,
        None,
        None,
        None,
        _timestamp(2),
        finished_at,
    )
    attempt = WorkAttemptRecord(
        WORK_ID,
        AttemptNumber(1),
        claim.started_at,
        finished_at,
        claim.runner_kind,
        claim.worker_identity,
        AttemptOutcome.SUCCEEDED,
        None,
        None,
        None,
        10,
        20,
        result.terminal.duration,
    )
    completed = CompletedWork(work, attempt)
    checkpoint = None
    selected = cast(CommitWorkAttempt | CommitWorkWithCheckpoint, command)
    if type(selected) is CommitWorkWithCheckpoint:
        checkpoint = CheckpointCommit(
            CheckpointHeadRecord(
                RUN_ID,
                NODE_ID,
                PARTITION,
                CheckpointVersion(1),
                finished_at,
                2,
            ),
            CheckpointRecord(
                RUN_ID,
                NODE_ID,
                PARTITION,
                CheckpointVersion(1),
                selected.checkpoint.payload_schema_version,
                selected.checkpoint.source_cursor,
                selected.checkpoint.output_position,
                selected.checkpoint.artifact_id,
                finished_at,
            ),
            UpdatedWorkCheckpoint(
                WORK_ID,
                RUN_ID,
                NODE_ID,
                PARTITION,
                CheckpointVersion(1),
                claim.row_version + 2,
            ),
        )
    delta = result.metrics.aggregate_delta
    node = RunNodeRecord(
        RUN_ID,
        NODE_ID,
        RunNodeStatus.SUCCEEDED,
        lease.node.row_version + 1,
        1,
        0,
        0,
        1,
        0,
        0,
        0,
        delta.records_read,
        delta.records_written,
        delta.records_quarantined,
        delta.bytes_read,
        delta.bytes_written,
        0,
        result.terminal.duration,
        lease.node.started_at,
        finished_at,
    )
    pending_event = selected.event.event
    next_sequence = lease.events.next_sequence
    events = ExecutionEventBatch(
        (
            ExecutionEventRecord(
                RUN_ID,
                next_sequence,
                pending_event.event_kind,
                pending_event.occurred_at,
                pending_event.subject_kind,
                pending_event.subject_id,
                pending_event.correlation_id,
                pending_event.payload_schema_version,
                pending_event.payload,
            ),
        ),
        next_sequence.advance(1),
        lease.events.counter_row_version + 1,
    )
    command_result = CommitWorkResult(
        completed,
        node,
        checkpoint,
        events,
        _run(lease.run.row_version + 1),
    )
    return WriterReceipt(
        WriterSubmissionId(1),
        command.kind,
        RUN_ID,
        0,
        True,
        command_result,
    )


class _RecordingExecutor(RunnerNodeExecutor):
    def __init__(self, *, pause_during_first: bool = False) -> None:
        self.requests: list[RunnerNodeRequest] = []
        self.pause_during_first = pause_during_first

    def execute(self, request: RunnerNodeRequest, /) -> RunnerNodeResult:
        self.requests.append(request)
        if self.pause_during_first and len(self.requests) == 1:
            request.pause.request_for_coordinator()
            return RunnerNodeResult(request.node.node_id, RunnerNodeOutcome.PAUSED)
        return RunnerNodeResult(request.node.node_id, RunnerNodeOutcome.SUCCEEDED)

    def close(self) -> None:
        return


class _ClaimRecorder:
    """Counts runner acknowledgement claims and runs coordinator work before each."""

    def __init__(self, token: PauseToken, during_claim: Callable[[], None] | None) -> None:
        self.token = token
        self.during_claim = during_claim
        self.claims = 0
        self.entered = Event()
        self._original = PauseToken._acknowledge_for_runner

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        recorder = self

        def hooked(
            self: PauseToken,
            state: object,
            *,
            authority: object,
            _token: object,
        ) -> object:
            if self is recorder.token:
                recorder.claims += 1
                if recorder.during_claim is not None:
                    recorder.entered.set()
                    recorder.during_claim()
            return recorder._original(
                self,
                cast(Any, state),
                authority=authority,
                _token=_token,
            )

        monkeypatch.setattr(PauseToken, "_acknowledge_for_runner", hooked)


class _PauseWriter:
    def __init__(self) -> None:
        self.state = PauseDurableState(_run(), EventSequence(5), 5)
        self.commands: list[WriterCommand] = []

    def submit(self, command: WriterCommand, *, timeout_seconds: float) -> _CheckpointTicket:
        from paritygrid.application.writes import TransitionRun

        self.commands.append(command)
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
        self.state = PauseDurableState(run, events.next_sequence, events.counter_row_version)
        receipt = WriterReceipt(
            WriterSubmissionId(len(self.commands)),
            command.kind,
            selected.run_id,
            0,
            True,
            TransitionRunResult(run, events),
        )
        return _CheckpointTicket(
            WriterSubmissionId(len(self.commands)),
            receipt,
        )


class _PauseReader:
    def __init__(self, writer: _PauseWriter) -> None:
        self.writer = writer

    def read(self, run_id: RunId, /) -> PauseDurableState:
        assert run_id == RUN_ID
        return self.writer.state


def _pause_coordinator() -> tuple[PauseCoordinator, _PauseWriter, WorkLeaseService, PauseToken]:
    writer = _PauseWriter()
    leases = WorkLeaseService(writer, _Clock(), settings=WorkLeaseSettings())
    coordinator = PauseCoordinator(
        writer,
        _PauseReader(writer),
        leases,
        _Clock(*(_timestamp(3) for _ in range(8))),
        settings=PauseCoordinatorSettings(),
    )
    return coordinator, writer, leases, coordinator.token


def _run_in_thread(runner: SequentialRunner) -> tuple[RunnerReport | None, BaseException | None]:
    outcome: dict[str, object] = {"report": None, "failure": None}

    def target() -> None:
        try:
            outcome["report"] = runner.run(_plan())
        except BaseException as error:
            outcome["failure"] = error

    thread = Thread(target=target)
    thread.start()
    thread.join(timeout=10.0)
    assert not thread.is_alive()
    return cast(RunnerReport | None, outcome["report"]), cast(
        BaseException | None, outcome["failure"]
    )


def test_pause_abort_wins_before_node_acknowledgement_and_runner_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, writer, leases, token = _pause_coordinator()
    recorder = _ClaimRecorder(token, None)

    def abort_inside_claim() -> None:
        coordinator.abort_pause()

    object.__setattr__(recorder, "during_claim", abort_inside_claim)
    recorder.install(monkeypatch)
    coordinator.request_pause(RUN_ID)
    assert coordinator.token.is_requested
    executor = _RecordingExecutor()
    report, failure = _run_in_thread(SequentialRunner(executor, pause=token))
    assert recorder.entered.is_set()
    assert recorder.claims == 1
    assert failure is None
    assert report is not None
    assert report.status is RunnerStatus.SUCCEEDED
    assert report.scheduler_state.status.value == "succeeded"
    assert len(executor.requests) == 2
    assert not token.is_requested
    assert writer.commands == []
    reservation = leases.reserve_pause(RUN_ID)
    leases.release_pause(reservation)


def test_pause_acknowledgement_wins_before_node_and_requires_explicit_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, writer, leases, token = _pause_coordinator()
    recorder = _ClaimRecorder(token, lambda: None)
    recorder.install(monkeypatch)
    coordinator.request_pause(RUN_ID)
    executor = _RecordingExecutor()
    report, failure = _run_in_thread(SequentialRunner(executor, pause=token))
    assert recorder.claims == 1
    assert failure is None
    assert report is not None
    assert report.status is RunnerStatus.PAUSED
    assert report.pause_acknowledgement is not None
    assert report.scheduler_state.active_node_id is None
    assert executor.requests == []
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="acknowledged"):
        coordinator.abort_pause()
    acknowledgement = report.pause_acknowledgement
    assert acknowledgement is not None
    paused, pause_report = coordinator.pause(acknowledgement)
    assert pause_report.action is PauseAction.PAUSED
    assert paused.run.state is RunState.PAUSED
    with pytest.raises(WorkLeaseBusyError):
        leases.reserve_pause(RUN_ID)
    resume_report = coordinator.resume(paused)
    assert resume_report.action is PauseAction.RESUMED
    from paritygrid.application.writes import TransitionRun

    assert [cast(TransitionRun, command).target_state for command in writer.commands] == [
        RunState.PAUSING,
        RunState.PAUSED,
        RunState.RESUMING,
        RunState.RUNNING,
    ]
    reservation = leases.reserve_pause(RUN_ID)
    leases.release_pause(reservation)


class _ExecutorPausedFirstExecutor(RunnerNodeExecutor):
    """Executor that blocks on its first node and then returns the pause state."""

    def __init__(self) -> None:
        self.requests: list[RunnerNodeRequest] = []
        self.entered = Event()
        self.release = Event()

    def execute(self, request: RunnerNodeRequest, /) -> RunnerNodeResult:
        self.requests.append(request)
        if len(self.requests) == 1:
            self.entered.set()
            assert self.release.wait(timeout=5.0)
        outcome = (
            RunnerNodeOutcome.PAUSED if request.pause.is_requested else RunnerNodeOutcome.SUCCEEDED
        )
        return RunnerNodeResult(request.node.node_id, outcome)

    def close(self) -> None:
        return


def test_pause_abort_wins_against_executor_paused_state_with_redacted_protocol_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, writer, leases, token = _pause_coordinator()
    recorder = _ClaimRecorder(token, None)

    def abort_inside_claim() -> None:
        coordinator.abort_pause()

    object.__setattr__(recorder, "during_claim", abort_inside_claim)
    recorder.install(monkeypatch)
    executor = _ExecutorPausedFirstExecutor()
    runner = SequentialRunner(executor, pause=token)
    outcome: dict[str, object] = {"report": None, "failure": None}

    def target() -> None:
        try:
            outcome["report"] = runner.run(_plan())
        except BaseException as error:
            outcome["failure"] = error

    thread = Thread(target=target)
    thread.start()
    assert executor.entered.wait(timeout=5.0)
    coordinator.request_pause(RUN_ID)
    executor.release.set()
    thread.join(timeout=10.0)
    assert not thread.is_alive()
    report = cast(RunnerReport | None, outcome["report"])
    failure = cast(BaseException | None, outcome["failure"])
    assert recorder.entered.is_set()
    assert recorder.claims == 1
    assert report is None
    assert failure is not None
    assert type(failure) is RunnerProtocolError
    assert not isinstance(failure, PauseCoordinatorError)
    assert failure.__cause__ is None
    assert failure.__context__ is None
    assert "PauseCoordinator" not in str(failure)
    state = runner.state
    assert state is not None
    assert state.active_node_id is None
    assert NODE_ID in state.ready_node_ids
    assert len(executor.requests) == 1
    assert writer.commands == []
    reservation = leases.reserve_pause(RUN_ID)
    leases.release_pause(reservation)


def test_pause_acknowledgement_wins_against_executor_paused_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator, _writer, leases, token = _pause_coordinator()
    recorder = _ClaimRecorder(token, lambda: None)
    recorder.install(monkeypatch)
    executor = _ExecutorPausedFirstExecutor()
    outcome: dict[str, object] = {"report": None, "failure": None}

    def target() -> None:
        try:
            outcome["report"] = SequentialRunner(executor, pause=token).run(_plan())
        except BaseException as error:
            outcome["failure"] = error

    thread = Thread(target=target)
    thread.start()
    assert executor.entered.wait(timeout=5.0)
    coordinator.request_pause(RUN_ID)
    executor.release.set()
    thread.join(timeout=10.0)
    assert not thread.is_alive()
    report = cast(RunnerReport | None, outcome["report"])
    failure = cast(BaseException | None, outcome["failure"])
    assert recorder.claims == 1
    assert failure is None
    assert report is not None
    assert report.status is RunnerStatus.PAUSED
    assert report.scheduler_state.active_node_id is None
    assert report.scheduler_state.ready_node_ids == (NODE_ID, NodeId("nod_p7-0-second"))
    assert len(executor.requests) == 1
    with pytest.raises(PauseCoordinatorInvalidRequestError, match="acknowledged"):
        coordinator.abort_pause()
    acknowledgement = report.pause_acknowledgement
    assert acknowledgement is not None
    paused, _report = coordinator.pause(acknowledgement)
    assert paused.run.state is RunState.PAUSED
    assert coordinator.resume(paused).action is PauseAction.RESUMED
    reservation = leases.reserve_pause(RUN_ID)
    leases.release_pause(reservation)


def test_abort_compare_and_set_refuses_same_generation_after_claim() -> None:
    token = PauseToken()
    runner_token = pause_module._PAUSE_RUNNER_TOKEN
    authority = token._bind_runner(_token=runner_token)
    generation = token.request_for_coordinator()
    assert token.abort_for_coordinator(generation)
    assert not token.is_requested
    generation = token.request_for_coordinator()
    state = DependencyTracker(_plan()).state
    assert (
        token._acknowledge_for_runner(state, authority=authority, _token=runner_token) is not None
    )
    assert not token.abort_for_coordinator(generation)
    assert token.is_requested
    assert not token.abort_for_coordinator(generation + 1)
    assert token.clear_for_coordinator(generation)


def test_pre_admission_invalid_result_retains_lease_and_permits_correction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import paritygrid.application.execution.checkpoint_commit as checkpoint_module

    service, _lease_writer, _clock = _lease_service()
    lease = service.acquire(_acquire_request("corr-p7-0"))
    submission = ResultSubmission(lease, _success(lease))

    def drifting_snapshot(selected: ResultSubmission) -> ResultSubmission:
        drifted = replace(
            cast(Any, selected._lease_evidence),
            claim_row_version=cast(Any, selected._lease_evidence).claim_row_version + 1,
        )
        object.__setattr__(selected, "_lease_evidence", drifted)
        return selected

    with pytest.MonkeyPatch.context() as context:
        context.setattr(
            checkpoint_module,
            "snapshot_result_submission",
            drifting_snapshot,
        )
        spy_writer = _CheckpointWriter(response=object())
        sink = TransactionalCheckpointResultSink(
            spy_writer,
            CheckpointCommitSettings(),
        )
        with pytest.raises(ResultSinkPreAdmissionError) as captured:
            submit_work_result(sink, submission, lease_service=service)
        assert isinstance(captured.value, ResultSinkPreAdmissionError)
        assert not isinstance(captured.value, CheckpointCommitInvalidRequestError)
        assert spy_writer.commands == []
        snapshot = service.snapshot()
        assert snapshot.active == 1
        assert snapshot.unknown == 0
        assert snapshot.in_flight == 0
    corrected = ResultSubmission(lease, _success(lease))

    class _CommittingWriter(_CheckpointWriter):
        def submit(self, command: WriterCommand, *, timeout_seconds: float) -> _CheckpointTicket:
            self.commands.append(command)
            return _CheckpointTicket(
                WriterSubmissionId(1),
                _valid_checkpoint_receipt(command, corrected),
            )

    committing_sink = TransactionalCheckpointResultSink(
        _CommittingWriter(response=None),
        CheckpointCommitSettings(),
    )
    outcome = submit_work_result(committing_sink, corrected, lease_service=service)
    assert outcome.kind.value == "committed"
    final = service.snapshot()
    assert final.active == 0
    assert final.unknown == 0
    assert final.in_flight == 0


def test_generic_invalid_result_failure_remains_outcome_unknown() -> None:
    service, _writer, _clock = _lease_service()
    lease = service.acquire(_acquire_request("corr-p7-0"))
    submission = ResultSubmission(lease, _success_without_artifact(lease))

    class _GenericInvalidSink:
        def submit(self, selected: ResultSubmission, /) -> ResultSinkOutcome:  # pragma: no cover
            raise ResultSinkInvalidResultError("post-admission invalid result")

    with pytest.raises(ResultSinkInvalidResultError) as captured:
        submit_work_result(cast(Any, _GenericInvalidSink()), submission, lease_service=service)
    assert not isinstance(captured.value, ResultSinkPreAdmissionError)
    snapshot = service.snapshot()
    assert snapshot.unknown == 1
    assert snapshot.active == 0
    assert snapshot.in_flight == 0


def test_attempt_cancelled_result_is_available_for_pre_admission_construction() -> None:
    from paritygrid.application.execution import (
        AttemptEventContext,
        RedactedAttemptDetail,
        UnsuccessfulWorkResult,
    )

    service, _writer, _clock = _lease_service()
    lease = service.acquire(_acquire_request("corr-p7-0"))
    claim = lease.claim
    context = AttemptEventContext(
        RUN_ID,
        NODE_ID,
        WORK_ID,
        AttemptNumber(1),
        claim.started_at,
        PlannerRunnerKind.SEQUENTIAL,
        claim.worker_identity,
        "corr-p7-0",
    )
    cancelled = UnsuccessfulWorkResult(
        AttemptCancelled(context, _timestamp(5), RedactedAttemptDetail("safe cancel")),
        None,
        ResultMetrics(0, 0, WorkMetricDelta(0, 0, 0, 0, 0)),
    )
    submission = ResultSubmission(lease, cancelled)
    assert submission.result.kind.value == "cancelled"
