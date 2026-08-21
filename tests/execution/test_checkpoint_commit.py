"""Contract tests for the transactional checkpoint result sink."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import cast

import pytest

import paritygrid.application.execution.checkpoint_commit as checkpoint_commit_module
from paritygrid.adapters.persistence import (
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    create_session_factory,
)
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyCheckpointRepository,
    SqlAlchemyExecutionEventRepository,
    SqlAlchemyPipelineRepository,
    SqlAlchemyRunRepository,
    SqlAlchemyWorkAttemptRepository,
    SqlAlchemyWorkItemRepository,
)
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter
from paritygrid.application.execution.attempt_events import (
    AttemptCancelled,
    AttemptEventContext,
    AttemptFailed,
    AttemptSucceeded,
    RedactedAttemptDetail,
)
from paritygrid.application.execution.checkpoint_commit import (
    CHECKPOINT_COMMIT_EVENT_PAYLOAD_SCHEMA_VERSION,
    MAX_CHECKPOINT_COMMIT_CONTENTION_ATTEMPTS,
    MAX_CHECKPOINT_COMMIT_TIMEOUT_SECONDS,
    CheckpointCommitAdmissionError,
    CheckpointCommitInvalidRequestError,
    CheckpointCommitOutcomeUnknownError,
    CheckpointCommitProtocolError,
    CheckpointCommitSettings,
    TransactionalCheckpointResultSink,
)
from paritygrid.application.execution.leasing import (
    AcquireWorkLeaseRequest,
    WorkLease,
    WorkLeaseService,
    WorkLeaseSettings,
)
from paritygrid.application.execution.result_sink import (
    ResultCheckpoint,
    ResultMetrics,
    ResultRejectionReason,
    ResultSinkCommitted,
    ResultSinkInvalidResultError,
    ResultSinkOutcomeUnknownError,
    ResultSinkRejected,
    ResultSubmission,
    SuccessfulWorkResult,
    UnsuccessfulWorkResult,
    WorkResult,
    WorkResultKind,
    submit_work_result,
)
from paritygrid.application.execution.retry_policy import (
    RetryPolicyName,
    RetryScheduledDecision,
    RetryStoppedDecision,
)
from paritygrid.application.planner import PlannerRunnerKind
from paritygrid.application.ports.artifacts import (
    ArtifactManifestRecord,
    ArtifactRelativePath,
)
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    CheckpointCommit,
    CheckpointHeadRecord,
    CheckpointRecord,
    CheckpointVersion,
    ConsistencyRecordNotFoundError,
    ConsistencyStaleRowVersionError,
    EventSequence,
    EventSubjectKind,
    ExecutionEventBatch,
    ExecutionEventRecord,
    PendingExecutionEvent,
    RedactedDocument,
    UpdatedWorkCheckpoint,
)
from paritygrid.application.ports.execution import (
    AttemptOutcome,
    CompletedWork,
    ExecutionLeaseLostError,
    ExecutionStateConflictError,
    RunNodeRecord,
    RunNodeStatus,
    RunRecord,
    WorkAttemptRecord,
    WorkClaim,
    WorkItemRecord,
)
from paritygrid.application.ports.run_aggregates import WorkMetricDelta
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    WriterAdmissionTimeoutError,
    WriterClosedError,
    WriterCommand,
    WriterCommandKind,
    WriterCommitOutcomeUnknownError,
    WriterDefinitelyNotExecutedError,
    WriterFailedError,
    WriterReceipt,
    WriterResultTimeoutError,
    WriterSettings,
    WriterSubmissionId,
    WriterTicket,
)
from paritygrid.application.writes import (
    WORK_LEASE_EVENT_PAYLOAD_SCHEMA_VERSION,
    BootstrapWork,
    CommitWorkAttempt,
    CommitWorkResult,
    CommitWorkWithCheckpoint,
    CreateCapturedRun,
    TransitionRun,
)
from paritygrid.domain.execution import (
    FailureClassification,
    FailureDisposition,
    RunState,
    WorkItemState,
)
from paritygrid.domain.models import (
    ArtifactId,
    AttemptNumber,
    Duration,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

RUN_ID = RunId("run_checkpoint-commit")
NODE_ID = NodeId("nod_checkpoint-commit")
WORK_ID = WorkItemId("wrk_checkpoint-commit")
PARTITION = PartitionKey("partition-0001")
SUBMISSION_ID = WriterSubmissionId(41)


class _Fatal(BaseException):
    pass


_EVIL_OPERATIONS: list[str] = []


class _EvilInt(int):
    def __eq__(self, other: object) -> bool:
        _EVIL_OPERATIONS.append("int equality")
        return bool(int.__eq__(self, other))

    def __ne__(self, other: object) -> bool:
        _EVIL_OPERATIONS.append("int inequality")
        return bool(int.__ne__(self, other))

    def __add__(self, other: object) -> int:
        _EVIL_OPERATIONS.append("int addition")
        return int(self) + cast(int, other)


class _EvilStr(str):
    def __eq__(self, other: object) -> bool:
        _EVIL_OPERATIONS.append("text equality")
        return bool(str.__eq__(self, other))

    def __ne__(self, other: object) -> bool:
        _EVIL_OPERATIONS.append("text inequality")
        return bool(str.__ne__(self, other))


@dataclass
class _EvilDataclass:
    value: int

    def __getattribute__(self, name: str) -> object:
        if name == "value":
            _EVIL_OPERATIONS.append("dataclass attribute")
        return object.__getattribute__(self, name)


class _EvilEnum(Enum):
    VALUE = "value"

    @property
    def microseconds(self) -> int:
        _EVIL_OPERATIONS.append("enum property")
        return 0


class _Ticket:
    def __init__(
        self,
        submission_id: object,
        result: object,
    ) -> None:
        self._submission_id = submission_id
        self._result = result
        self.result_timeout: float | None = None

    @property
    def submission_id(self) -> WriterSubmissionId:
        value = self._submission_id
        if isinstance(value, BaseException):
            raise value
        return cast(WriterSubmissionId, value)

    def result(self, *, timeout_seconds: float) -> WriterReceipt:
        self.result_timeout = timeout_seconds
        value = self._result
        if isinstance(value, BaseException):
            raise value
        return cast(WriterReceipt, value)

    async def result_async(self, *, timeout_seconds: float) -> WriterReceipt:
        return self.result(timeout_seconds=timeout_seconds)


class _Writer:
    def __init__(
        self,
        response: object | Callable[[WriterCommand], object],
        *,
        submission_id: object = SUBMISSION_ID,
        admission_error: BaseException | None = None,
    ) -> None:
        self.response = response
        self.submission_id = submission_id
        self.admission_error = admission_error
        self.commands: list[WriterCommand] = []
        self.admission_timeout: float | None = None
        self.ticket: _Ticket | None = None

    def submit(self, command: WriterCommand, *, timeout_seconds: float) -> WriterTicket:
        self.commands.append(command)
        self.admission_timeout = timeout_seconds
        if self.admission_error is not None:
            raise self.admission_error
        response = self.response(command) if callable(self.response) else self.response
        self.ticket = _Ticket(self.submission_id, response)
        return self.ticket


def _timestamp(second: int) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 12, 12, 0, second, tzinfo=UTC))


def _document(**values: object) -> ConfigurationDocument:
    return ConfigurationDocument.from_mapping(values)


def _claim() -> WorkClaim:
    return WorkClaim(
        WORK_ID,
        AttemptNumber(1),
        "lease-owner",
        2,
        _timestamp(3),
        _timestamp(9),
        PlannerRunnerKind.SEQUENTIAL.value,
        "reference-worker",
    )


def _node() -> RunNodeRecord:
    return RunNodeRecord(
        RUN_ID,
        NODE_ID,
        RunNodeStatus.RUNNING,
        3,
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
        _timestamp(3),
        None,
    )


def _run() -> RunRecord:
    return RunRecord(
        RUN_ID,
        PipelineId("pip_checkpoint-commit"),
        PipelineVersion(1),
        PlannerRunnerKind.SEQUENTIAL.value,
        _document(mode="reference"),
        RunState.RUNNING,
        4,
        7,
        _timestamp(1),
        _timestamp(2),
        None,
        None,
        None,
        None,
        None,
    )


def _lease() -> WorkLease:
    claim = _claim()
    event = ExecutionEventRecord(
        RUN_ID,
        EventSequence(4),
        "work_claimed",
        claim.started_at,
        EventSubjectKind.WORK_ITEM,
        WORK_ID,
        "corr-checkpoint",
        2,
        RedactedDocument.from_mapping({"attempt_number": 1}),
    )
    lease = object.__new__(WorkLease)
    object.__setattr__(lease, "_claim", claim)
    object.__setattr__(lease, "_node", _node())
    object.__setattr__(lease, "_run", _run())
    object.__setattr__(
        lease,
        "_events",
        ExecutionEventBatch((event,), EventSequence(5), 4),
    )
    object.__setattr__(lease, "_submission_id", WriterSubmissionId(11))
    return lease


def _context(lease: WorkLease) -> AttemptEventContext:
    return AttemptEventContext(
        RUN_ID,
        NODE_ID,
        WORK_ID,
        AttemptNumber(1),
        lease.claim.started_at,
        PlannerRunnerKind.SEQUENTIAL,
        lease.claim.worker_identity,
        "corr-checkpoint",
    )


def _metrics() -> ResultMetrics:
    return ResultMetrics(10, 20, WorkMetricDelta(7, 8, 1, 9, 10))


def _manifest() -> ArtifactManifestRecord:
    return ArtifactManifestRecord(
        ArtifactId("art_checkpoint-commit"),
        RUN_ID,
        NODE_ID,
        PARTITION,
        ArtifactRelativePath("runs/checkpoint/output.parquet"),
        "application/vnd.apache.parquet",
        1,
        20,
        10,
        "a" * 64,
        _timestamp(4),
    )


def _success(lease: WorkLease, *, artifact: bool = False) -> SuccessfulWorkResult:
    return SuccessfulWorkResult(
        AttemptSucceeded(_context(lease), _timestamp(5)),
        ResultCheckpoint(
            PARTITION,
            1,
            _document(offset=10),
            _document(rows=10),
            _manifest() if artifact else None,
        ),
        _metrics(),
    )


def _retry(lease: WorkLease) -> UnsuccessfulWorkResult:
    terminal = AttemptFailed(
        _context(lease),
        _timestamp(5),
        FailureClassification.CONNECTION,
        RedactedAttemptDetail("safe retry"),
    )
    return UnsuccessfulWorkResult(
        terminal,
        RetryScheduledDecision(
            RetryPolicyName.BOUNDED_EXPONENTIAL_V1,
            WORK_ID,
            AttemptNumber(1),
            FailureClassification.CONNECTION,
            _timestamp(5),
            _timestamp(5),
            None,
            Duration(0),
            Duration(1_000_000),
            _timestamp(6),
        ),
        _metrics(),
    )


def _stopped(
    lease: WorkLease,
    classification: FailureClassification,
    disposition: FailureDisposition,
) -> UnsuccessfulWorkResult:
    terminal = AttemptFailed(
        _context(lease),
        _timestamp(5),
        classification,
        RedactedAttemptDetail("safe terminal"),
    )
    return UnsuccessfulWorkResult(
        terminal,
        RetryStoppedDecision(
            RetryPolicyName.BOUNDED_EXPONENTIAL_V1,
            WORK_ID,
            AttemptNumber(1),
            classification,
            _timestamp(5),
            disposition,
            False,
        ),
        _metrics(),
    )


def _cancelled(lease: WorkLease) -> UnsuccessfulWorkResult:
    return UnsuccessfulWorkResult(
        AttemptCancelled(
            _context(lease),
            _timestamp(5),
            RedactedAttemptDetail("safe cancellation"),
        ),
        None,
        _metrics(),
    )


def _quarantined(lease: WorkLease) -> UnsuccessfulWorkResult:
    return _stopped(
        lease,
        FailureClassification.VALIDATION,
        FailureDisposition.QUARANTINE,
    )


def _permanent(lease: WorkLease) -> UnsuccessfulWorkResult:
    return _stopped(
        lease,
        FailureClassification.UNKNOWN,
        FailureDisposition.PERMANENT,
    )


def _conflict(lease: WorkLease) -> UnsuccessfulWorkResult:
    return _stopped(
        lease,
        FailureClassification.IDEMPOTENCY_CONFLICT,
        FailureDisposition.CONFLICT,
    )


def _submission(result_factory: Callable[[WorkLease], WorkResult]) -> ResultSubmission:
    lease = _lease()
    return ResultSubmission(lease, result_factory(lease))


def _receipt(command: WriterCommand, submission: ResultSubmission) -> WriterReceipt:
    claim = submission.lease.claim
    result = submission.result
    completion = cast(CommitWorkAttempt | CommitWorkWithCheckpoint, command).completion
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
        completion.finished_at,
    )
    attempt = WorkAttemptRecord(
        WORK_ID,
        AttemptNumber(1),
        claim.started_at,
        completion.finished_at,
        claim.runner_kind,
        claim.worker_identity,
        {
            WorkItemState.SUCCEEDED: AttemptOutcome.SUCCEEDED,
            WorkItemState.RETRY_WAIT: AttemptOutcome.RETRY_SCHEDULED,
            WorkItemState.QUARANTINED: AttemptOutcome.QUARANTINED,
            WorkItemState.FAILED: AttemptOutcome.FAILED,
            WorkItemState.CANCELLED: AttemptOutcome.CANCELLED,
        }[completion.target_state],
        completion.failure_classification,
        completion.redacted_detail,
        None,
        completion.records_processed,
        completion.bytes_processed,
        result.terminal.duration,
    )
    completed = CompletedWork(work, attempt)
    checkpoint = None
    if type(command) is CommitWorkWithCheckpoint:
        checkpoint = CheckpointCommit(
            CheckpointHeadRecord(
                RUN_ID,
                NODE_ID,
                PARTITION,
                CheckpointVersion(1),
                completion.finished_at,
                2,
            ),
            CheckpointRecord(
                RUN_ID,
                NODE_ID,
                PARTITION,
                CheckpointVersion(1),
                command.checkpoint.payload_schema_version,
                command.checkpoint.source_cursor,
                command.checkpoint.output_position,
                command.checkpoint.artifact_id,
                completion.finished_at,
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
    previous = submission.lease.node
    pending = 1 if completion.target_state is WorkItemState.RETRY_WAIT else 0
    succeeded = 1 if completion.target_state is WorkItemState.SUCCEEDED else 0
    quarantined = 1 if completion.target_state is WorkItemState.QUARANTINED else 0
    failed = 1 if completion.target_state is WorkItemState.FAILED else 0
    cancelled = 1 if completion.target_state is WorkItemState.CANCELLED else 0
    status = (
        RunNodeStatus.RUNNING
        if pending
        else RunNodeStatus.FAILED
        if failed
        else RunNodeStatus.CANCELLED
        if cancelled
        else RunNodeStatus.PARTIALLY_SUCCEEDED
        if quarantined
        else RunNodeStatus.SUCCEEDED
    )
    delta = result.metrics.aggregate_delta
    node = RunNodeRecord(
        RUN_ID,
        NODE_ID,
        status,
        previous.row_version + 1,
        1,
        pending,
        0,
        succeeded,
        quarantined,
        failed,
        cancelled,
        delta.records_read,
        delta.records_written,
        delta.records_quarantined,
        delta.bytes_read,
        delta.bytes_written,
        1 if pending else 0,
        result.terminal.duration,
        previous.started_at,
        None if status is RunNodeStatus.RUNNING else completion.finished_at,
    )
    event = cast(CommitWorkAttempt | CommitWorkWithCheckpoint, command).event.event
    events = ExecutionEventBatch(
        (
            ExecutionEventRecord(
                RUN_ID,
                EventSequence(5),
                event.event_kind,
                event.occurred_at,
                event.subject_kind,
                event.subject_id,
                event.correlation_id,
                event.payload_schema_version,
                event.payload,
            ),
        ),
        EventSequence(6),
        5,
    )
    command_result = CommitWorkResult(
        completed,
        node,
        checkpoint,
        events,
        replace(submission.lease.run, row_version=submission.lease.run.row_version + 1),
    )
    return WriterReceipt(
        SUBMISSION_ID,
        command.kind,
        RUN_ID,
        0,
        True,
        command_result,
    )


def _command_result(receipt: WriterReceipt) -> CommitWorkResult:
    assert type(receipt.result) is CommitWorkResult
    return receipt.result


def _with_result(receipt: WriterReceipt, **changes: object) -> WriterReceipt:
    result = replace(_command_result(receipt), **changes)  # type: ignore[arg-type]
    return replace(receipt, result=result)


def _with_work(receipt: WriterReceipt, **changes: object) -> WriterReceipt:
    result = _command_result(receipt)
    completed = replace(
        result.completed,
        work_item=replace(result.completed.work_item, **changes),  # type: ignore[arg-type]
    )
    return _with_result(receipt, completed=completed)


def _with_attempt(receipt: WriterReceipt, **changes: object) -> WriterReceipt:
    result = _command_result(receipt)
    completed = replace(
        result.completed,
        attempt=replace(result.completed.attempt, **changes),  # type: ignore[arg-type]
    )
    return _with_result(receipt, completed=completed)


def _with_checkpoint_part(
    receipt: WriterReceipt,
    part: str,
    **changes: object,
) -> WriterReceipt:
    result = _command_result(receipt)
    assert type(result.checkpoint) is CheckpointCommit
    value = replace(getattr(result.checkpoint, part), **changes)  # type: ignore[arg-type]
    checkpoint = replace(result.checkpoint, **{part: value})  # type: ignore[arg-type]
    return _with_result(receipt, checkpoint=checkpoint)


def _with_event_batch(receipt: WriterReceipt, **changes: object) -> WriterReceipt:
    result = _command_result(receipt)
    return _with_result(
        receipt,
        events=replace(result.events, **changes),  # type: ignore[arg-type]
    )


def _mutated_submission_id() -> WriterSubmissionId:
    value = WriterSubmissionId(SUBMISSION_ID.number)
    object.__setattr__(value, "number", _EvilInt(SUBMISSION_ID.number))
    return value


def _mutated_work_id() -> WorkItemId:
    value = WorkItemId(str(WORK_ID))
    object.__setattr__(value, "value", _EvilStr(str(WORK_ID)))
    return value


def _mutated_duration(value: Duration) -> Duration:
    copied = Duration(value.microseconds)
    object.__setattr__(copied, "microseconds", _EvilInt(value.microseconds))
    return copied


def _invalid_partition_key() -> PartitionKey:
    value = PartitionKey("partition-valid")
    object.__setattr__(value, "value", "not canonical")
    return value


def _invalid_configuration_document() -> ConfigurationDocument:
    value = _document(ok=1)
    object.__setattr__(value, "items", (("ok", 1.5),))
    return value


def _with_event_record(receipt: WriterReceipt, **changes: object) -> WriterReceipt:
    event = replace(
        _command_result(receipt).events.items[0],
        **changes,  # type: ignore[arg-type]
    )
    return _with_event_batch(receipt, items=(event,))


@pytest.mark.parametrize(
    ("factory", "kind", "command_type", "target"),
    [
        (_success, WorkResultKind.SUCCEEDED, CommitWorkWithCheckpoint, WorkItemState.SUCCEEDED),
        (_retry, WorkResultKind.RETRY_WAIT, CommitWorkAttempt, WorkItemState.RETRY_WAIT),
        (
            _quarantined,
            WorkResultKind.QUARANTINED,
            CommitWorkAttempt,
            WorkItemState.QUARANTINED,
        ),
        (
            _permanent,
            WorkResultKind.FAILED,
            CommitWorkAttempt,
            WorkItemState.FAILED,
        ),
        (
            _conflict,
            WorkResultKind.FAILED,
            CommitWorkAttempt,
            WorkItemState.FAILED,
        ),
        (_cancelled, WorkResultKind.CANCELLED, CommitWorkAttempt, WorkItemState.CANCELLED),
    ],
)
def test_all_result_variants_map_to_one_exact_committed_command(
    factory: Callable[[WorkLease], WorkResult],
    kind: WorkResultKind,
    command_type: type[CommitWorkAttempt] | type[CommitWorkWithCheckpoint],
    target: WorkItemState,
) -> None:
    submission = _submission(factory)
    writer = _Writer(lambda command: _receipt(command, submission))
    sink = TransactionalCheckpointResultSink(
        writer,
        CheckpointCommitSettings(1.5, 2.5),
    )

    outcome = sink.submit(submission)

    assert type(outcome) is ResultSinkCommitted
    assert outcome.result_kind is kind
    assert outcome.checkpoint_version == (
        CheckpointVersion(1) if target is WorkItemState.SUCCEEDED else None
    )
    assert writer.admission_timeout == 1.5
    assert writer.ticket is not None
    assert writer.ticket.result_timeout == 2.5
    assert len(writer.commands) == 1
    command = writer.commands[0]
    assert type(command) is command_type
    command = cast(CommitWorkAttempt | CommitWorkWithCheckpoint, command)
    assert command.claim is submission.lease.claim
    assert command.completion.target_state is target
    assert command.completion.result_reference is None
    assert command.metrics == submission.result.metrics.aggregate_delta
    assert command.expected_node_row_version == submission.lease.node.row_version
    assert command.expected_run_row_version == submission.lease.run.row_version
    assert command.event.expected_next_sequence == submission.lease.events.next_sequence
    assert command.event.expected_counter_row_version == submission.lease.events.counter_row_version
    assert (
        command.event.event.payload_schema_version == CHECKPOINT_COMMIT_EVENT_PAYLOAD_SCHEMA_VERSION
    )
    payload = command.event.event.payload.to_mapping()
    assert payload["attempt_number"] == 1
    assert payload["node_id"] == str(NODE_ID)
    assert payload["runner_kind"] == PlannerRunnerKind.SEQUENTIAL.value
    assert payload["target_state"] == target.value
    if type(command) is CommitWorkWithCheckpoint:
        assert command.checkpoint.expected_partition_key == PARTITION
        assert payload["partition_key"] == str(PARTITION)


def test_artifact_identity_is_checkpoint_only_and_detail_can_be_absent() -> None:
    lease = _lease()
    submission = ResultSubmission(lease, _success(lease, artifact=True))
    writer = _Writer(lambda command: _receipt(command, submission))
    outcome = TransactionalCheckpointResultSink(writer).submit(submission)
    assert type(outcome) is ResultSinkCommitted
    command = cast(CommitWorkWithCheckpoint, writer.commands[0])
    assert command.checkpoint.artifact_id == _manifest().artifact_id
    assert command.completion.result_reference is None
    assert command.event.event.payload.to_mapping()["artifact_id"] == str(_manifest().artifact_id)

    cancelled = UnsuccessfulWorkResult(
        AttemptCancelled(_context(lease), _timestamp(5), None),
        None,
        _metrics(),
    )
    rejected = TransactionalCheckpointResultSink(
        _Writer(WriterDefinitelyNotExecutedError("safe"))
    ).submit(ResultSubmission(lease, cancelled))
    assert type(rejected) is ResultSinkRejected


@pytest.mark.parametrize(
    "values",
    [
        {"admission_timeout_seconds": -0.1},
        {"admission_timeout_seconds": MAX_CHECKPOINT_COMMIT_TIMEOUT_SECONDS + 0.1},
        {"admission_timeout_seconds": 1},
        {"result_timeout_seconds": -0.1},
        {"result_timeout_seconds": MAX_CHECKPOINT_COMMIT_TIMEOUT_SECONDS + 0.1},
        {"result_timeout_seconds": True},
    ],
)
def test_settings_are_exact_and_bounded(values: dict[str, object]) -> None:
    with pytest.raises(CheckpointCommitInvalidRequestError, match="supported range"):
        CheckpointCommitSettings(**values)  # type: ignore[arg-type]
    assert CheckpointCommitSettings(0.0, MAX_CHECKPOINT_COMMIT_TIMEOUT_SECONDS)


def test_constructor_requires_exact_settings_and_writer_protocol() -> None:
    with pytest.raises(TypeError, match="writer"):
        TransactionalCheckpointResultSink(cast(_Writer, object()))
    with pytest.raises(TypeError, match="settings"):
        TransactionalCheckpointResultSink(
            _Writer(WriterDefinitelyNotExecutedError("safe")),
            cast(CheckpointCommitSettings, object()),
        )


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (WriterAdmissionTimeoutError("secret"), CheckpointCommitAdmissionError),
        (WriterClosedError("secret"), CheckpointCommitAdmissionError),
        (RuntimeError("secret"), CheckpointCommitProtocolError),
    ],
)
def test_pre_admission_errors_are_typed_redacted_and_cause_clean(
    error: Exception,
    expected: type[Exception],
) -> None:
    sink = TransactionalCheckpointResultSink(_Writer(object(), admission_error=error))
    with pytest.raises(expected) as captured:
        sink.submit(_submission(_success))
    assert "secret" not in str(captured.value)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


def test_pre_admission_base_exception_propagates() -> None:
    fatal = _Fatal("stop")
    with pytest.raises(_Fatal) as captured:
        TransactionalCheckpointResultSink(_Writer(object(), admission_error=fatal)).submit(
            _submission(_success)
        )
    assert captured.value is fatal


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        (
            WriterDefinitelyNotExecutedError("secret"),
            ResultRejectionReason.DEFINITELY_NOT_EXECUTED,
        ),
        (ExecutionLeaseLostError("secret"), ResultRejectionReason.STALE_CAPABILITY),
        (ConsistencyStaleRowVersionError("secret"), ResultRejectionReason.STALE_CAPABILITY),
        (ExecutionStateConflictError("secret"), ResultRejectionReason.STATE_CONFLICT),
        (ConsistencyRecordNotFoundError("secret"), ResultRejectionReason.STATE_CONFLICT),
    ],
)
def test_confirmed_rollback_maps_to_closed_rejection(
    error: Exception,
    reason: ResultRejectionReason,
) -> None:
    outcome = TransactionalCheckpointResultSink(_Writer(error)).submit(_submission(_success))
    assert type(outcome) is ResultSinkRejected
    assert outcome.reason is reason
    assert outcome.submission_id == SUBMISSION_ID


@pytest.mark.parametrize(
    "error",
    [
        WriterResultTimeoutError("secret"),
        WriterCommitOutcomeUnknownError("secret"),
        WriterFailedError("secret"),
        RuntimeError("secret"),
    ],
)
def test_post_admission_ambiguity_is_redacted(error: Exception) -> None:
    with pytest.raises(CheckpointCommitOutcomeUnknownError) as captured:
        TransactionalCheckpointResultSink(_Writer(error)).submit(_submission(_success))
    assert "secret" not in str(captured.value)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


def test_post_admission_base_exception_propagates() -> None:
    fatal = _Fatal("stop")
    with pytest.raises(_Fatal) as captured:
        TransactionalCheckpointResultSink(_Writer(fatal)).submit(_submission(_success))
    assert captured.value is fatal


@pytest.mark.parametrize("identity", [object(), RuntimeError("secret")])
def test_ticket_identity_is_exact_and_redacted(identity: object) -> None:
    with pytest.raises(CheckpointCommitProtocolError) as captured:
        TransactionalCheckpointResultSink(
            _Writer(WriterDefinitelyNotExecutedError("safe"), submission_id=identity)
        ).submit(_submission(_success))
    assert "secret" not in str(captured.value)


def test_ticket_identity_base_exception_propagates() -> None:
    fatal = _Fatal("stop")
    with pytest.raises(_Fatal) as captured:
        TransactionalCheckpointResultSink(
            _Writer(WriterDefinitelyNotExecutedError("safe"), submission_id=fatal)
        ).submit(_submission(_success))
    assert captured.value is fatal


def test_invalid_submission_is_rejected_before_writer_admission() -> None:
    writer = _Writer(WriterDefinitelyNotExecutedError("safe"))
    with pytest.raises(CheckpointCommitInvalidRequestError):
        TransactionalCheckpointResultSink(writer).submit(cast(ResultSubmission, object()))
    assert writer.commands == []


def test_changed_evidence_between_snapshot_and_command_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = iter((True, False))

    def changing_evidence(_submission: ResultSubmission) -> bool:
        return next(evidence)

    monkeypatch.setattr(
        ResultSubmission,
        "has_current_lease_evidence",
        changing_evidence,
    )
    writer = _Writer(WriterDefinitelyNotExecutedError("safe"))
    with pytest.raises(CheckpointCommitInvalidRequestError):
        TransactionalCheckpointResultSink(writer).submit(_submission(_success))
    assert writer.commands == []


@pytest.mark.parametrize("subject", ["claim", "node", "run", "events"])
def test_malformed_local_evidence_is_rejected_before_writer_handoff(
    monkeypatch: pytest.MonkeyPatch,
    subject: str,
) -> None:
    submission = _submission(_success)
    if subject == "claim":
        object.__setattr__(submission.lease.claim, "worker_identity", _EvilStr("worker"))
    elif subject == "node":
        object.__setattr__(submission.lease.node, "records_read", _EvilInt(0))
    elif subject == "run":
        object.__setattr__(
            submission.lease.run,
            "runner_kind",
            _EvilStr(PlannerRunnerKind.SEQUENTIAL.value),
        )
    else:
        object.__setattr__(submission.lease.events, "counter_row_version", _EvilInt(4))

    def selected_snapshot(_submission: ResultSubmission) -> ResultSubmission:
        return submission

    monkeypatch.setattr(
        checkpoint_commit_module,
        "snapshot_result_submission",
        selected_snapshot,
    )
    writer = _Writer(WriterDefinitelyNotExecutedError("safe"))
    with pytest.raises(CheckpointCommitInvalidRequestError):
        TransactionalCheckpointResultSink(writer).submit(submission)
    assert writer.commands == []


def test_optional_lease_evidence_is_snapshotted_without_durable_action() -> None:
    lease = _lease()
    node = replace(lease.node, started_at=None, finished_at=_timestamp(4))
    run = replace(
        lease.run,
        started_at=None,
        finished_at=_timestamp(4),
        cancellation_requested_at=_timestamp(3),
        recovery_started_at=_timestamp(3),
        recovered_at=_timestamp(4),
    )
    object.__setattr__(lease, "_node", node)
    object.__setattr__(lease, "_run", run)
    submission = ResultSubmission(lease, _success(lease))
    outcome = TransactionalCheckpointResultSink(
        _Writer(WriterDefinitelyNotExecutedError("safe"))
    ).submit(submission)
    assert type(outcome) is ResultSinkRejected


_RECEIPT_MUTATORS: list[Callable[[WriterReceipt], WriterReceipt]] = [
    lambda receipt: replace(receipt, submission_id=WriterSubmissionId(42)),
    lambda receipt: replace(receipt, command_kind=WriterCommandKind.COMMIT_WORK_ATTEMPT),
    lambda receipt: replace(receipt, run_id=RunId("run_checkpoint-other")),
    lambda receipt: replace(receipt, contention_attempts=-1),
    lambda receipt: replace(
        receipt,
        contention_attempts=MAX_CHECKPOINT_COMMIT_CONTENTION_ATTEMPTS + 1,
    ),
    lambda receipt: replace(receipt, mutated=False),
    lambda receipt: replace(receipt, result=cast(CommitWorkResult, object())),
    lambda receipt: replace(receipt, contention_attempts=cast(int, True)),
]


@pytest.mark.parametrize("mutator", _RECEIPT_MUTATORS)
def test_malformed_receipt_header_is_protocol_unknown(
    mutator: Callable[[WriterReceipt], WriterReceipt],
) -> None:
    submission = _submission(_success)

    def malformed(command: WriterCommand) -> WriterReceipt:
        return mutator(_receipt(command, submission))

    with pytest.raises(CheckpointCommitProtocolError):
        TransactionalCheckpointResultSink(_Writer(malformed)).submit(submission)


def test_non_receipt_result_is_protocol_unknown() -> None:
    with pytest.raises(CheckpointCommitProtocolError):
        TransactionalCheckpointResultSink(_Writer(object())).submit(_submission(_success))


def test_ticket_identity_nested_scalar_is_rejected_without_executing_it() -> None:
    _EVIL_OPERATIONS.clear()
    submission = _submission(_success)
    writer = _Writer(
        lambda command: _receipt(command, submission),
        submission_id=_mutated_submission_id(),
    )
    with pytest.raises(CheckpointCommitProtocolError) as captured:
        TransactionalCheckpointResultSink(writer).submit(submission)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert _EVIL_OPERATIONS == []


_NESTED_RECEIPT_MUTATORS: list[Callable[[WriterReceipt], WriterReceipt]] = [
    lambda receipt: _with_result(receipt, completed=cast(CompletedWork, object())),
    lambda receipt: _with_result(
        receipt,
        completed=replace(
            _command_result(receipt).completed,
            work_item=cast(WorkItemRecord, object()),
        ),
    ),
    lambda receipt: _with_result(
        receipt,
        completed=replace(
            _command_result(receipt).completed,
            attempt=cast(WorkAttemptRecord, object()),
        ),
    ),
    lambda receipt: _with_work(receipt, updated_at=_timestamp(6)),
    lambda receipt: _with_work(receipt, partition_key=PartitionKey("partition-other")),
    lambda receipt: _with_attempt(receipt, finished_at=_timestamp(6)),
    lambda receipt: _with_result(receipt, checkpoint=None),
    lambda receipt: _with_result(
        receipt,
        checkpoint=replace(
            cast(CheckpointCommit, _command_result(receipt).checkpoint),
            head=cast(CheckpointHeadRecord, object()),
        ),
    ),
    lambda receipt: _with_result(
        receipt,
        checkpoint=replace(
            cast(CheckpointCommit, _command_result(receipt).checkpoint),
            checkpoint=cast(CheckpointRecord, object()),
        ),
    ),
    lambda receipt: _with_result(
        receipt,
        checkpoint=replace(
            cast(CheckpointCommit, _command_result(receipt).checkpoint),
            work=cast(UpdatedWorkCheckpoint, object()),
        ),
    ),
    lambda receipt: _with_checkpoint_part(receipt, "head", row_version=3),
    lambda receipt: _with_checkpoint_part(receipt, "checkpoint", committed_at=_timestamp(6)),
    lambda receipt: _with_checkpoint_part(receipt, "work", row_version=5),
    lambda receipt: _with_result(receipt, events=cast(ExecutionEventBatch, object())),
    lambda receipt: _with_event_batch(receipt, items=[]),
    lambda receipt: _with_event_batch(receipt, items=()),
    lambda receipt: _with_event_batch(receipt, items=(cast(ExecutionEventRecord, object()),)),
    lambda receipt: _with_event_batch(receipt, next_sequence=EventSequence(7)),
    lambda receipt: _with_event_batch(receipt, counter_row_version=6),
    lambda receipt: _with_result(receipt, node=cast(RunNodeRecord, object())),
    lambda receipt: _with_result(
        receipt,
        node=replace(_command_result(receipt).node, row_version=9),
    ),
    lambda receipt: _with_result(receipt, run=cast(RunRecord, object())),
    lambda receipt: _with_result(
        receipt,
        run=replace(_command_result(receipt).run, row_version=9),
    ),
]


@pytest.mark.parametrize("mutator", _NESTED_RECEIPT_MUTATORS)
def test_nested_receipt_evidence_is_exact(
    mutator: Callable[[WriterReceipt], WriterReceipt],
) -> None:
    submission = _submission(_success)

    def malformed(command: WriterCommand) -> WriterReceipt:
        return mutator(_receipt(command, submission))

    with pytest.raises(CheckpointCommitProtocolError) as captured:
        TransactionalCheckpointResultSink(_Writer(malformed)).submit(submission)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


_REFLECTIVE_RECEIPT_MUTATORS: list[Callable[[WriterReceipt], WriterReceipt]] = [
    lambda receipt: replace(receipt, submission_id=_mutated_submission_id()),
    lambda receipt: _with_attempt(receipt, work_item_id=_mutated_work_id()),
    lambda receipt: _with_attempt(
        receipt,
        worker_identity=_EvilStr("reference-worker"),
    ),
    lambda receipt: _with_attempt(
        receipt,
        duration=_mutated_duration(_command_result(receipt).completed.attempt.duration),
    ),
    lambda receipt: _with_work(receipt, expected_checkpoint_version=_EvilInt(0)),
    lambda receipt: _with_checkpoint_part(
        receipt,
        "checkpoint",
        payload_schema_version=_EvilInt(1),
    ),
    lambda receipt: _with_event_record(
        receipt,
        event_kind=_EvilStr("checkpoint_committed"),
    ),
    lambda receipt: _with_result(
        receipt,
        node=replace(
            _command_result(receipt).node,
            records_read=_EvilInt(_command_result(receipt).node.records_read),
        ),
    ),
    lambda receipt: _with_result(
        receipt,
        run=replace(
            _command_result(receipt).run,
            runner_kind=_EvilStr(PlannerRunnerKind.SEQUENTIAL.value),
        ),
    ),
]


@pytest.mark.parametrize("mutator", _REFLECTIVE_RECEIPT_MUTATORS)
def test_reflective_receipt_scalars_are_rejected_without_execution(
    mutator: Callable[[WriterReceipt], WriterReceipt],
) -> None:
    _EVIL_OPERATIONS.clear()
    submission = _submission(_success)

    def malformed(command: WriterCommand) -> WriterReceipt:
        return mutator(_receipt(command, submission))

    with pytest.raises(CheckpointCommitProtocolError) as captured:
        TransactionalCheckpointResultSink(_Writer(malformed)).submit(submission)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert _EVIL_OPERATIONS == []


def test_arbitrary_receipt_dataclass_is_rejected_without_attribute_execution() -> None:
    _EVIL_OPERATIONS.clear()
    submission = _submission(_success)

    def malformed(command: WriterCommand) -> WriterReceipt:
        return _with_work(
            _receipt(command, submission),
            input_reference=cast(ConfigurationDocument, _EvilDataclass(1)),
        )

    with pytest.raises(CheckpointCommitProtocolError):
        TransactionalCheckpointResultSink(_Writer(malformed)).submit(submission)
    assert _EVIL_OPERATIONS == []


def test_arbitrary_local_enum_is_rejected_without_property_execution() -> None:
    _EVIL_OPERATIONS.clear()
    submission = _submission(_success)
    object.__setattr__(submission.lease.node, "duration", _EvilEnum.VALUE)
    writer = _Writer(WriterDefinitelyNotExecutedError("safe"))
    with pytest.raises(CheckpointCommitInvalidRequestError):
        TransactionalCheckpointResultSink(writer).submit(submission)
    assert writer.commands == []
    assert _EVIL_OPERATIONS == []


@pytest.mark.parametrize(
    "changes",
    [
        {"partition_key": cast(PartitionKey, 1)},
        {"partition_key": _invalid_partition_key()},
        {"input_reference": cast(ConfigurationDocument, ())},
        {"input_reference": _invalid_configuration_document()},
        {"created_at": cast(UtcTimestamp, 1)},
        {"created_at": _timestamp(4)},
        {"created_at": _timestamp(6)},
    ],
)
def test_omitted_work_fields_still_require_closed_safe_contracts(
    changes: dict[str, object],
) -> None:
    submission = _submission(_retry)

    def malformed(command: WriterCommand) -> WriterReceipt:
        return _with_work(_receipt(command, submission), **changes)

    with pytest.raises(CheckpointCommitProtocolError):
        TransactionalCheckpointResultSink(_Writer(malformed)).submit(submission)


def test_omitted_work_document_accepts_canonical_evidence() -> None:
    submission = _submission(_retry)

    def committed(command: WriterCommand) -> WriterReceipt:
        return _with_work(
            _receipt(command, submission),
            input_reference=_document(offset=1),
        )

    outcome = TransactionalCheckpointResultSink(_Writer(committed)).submit(submission)
    assert type(outcome) is ResultSinkCommitted


@pytest.mark.parametrize("field", ["event", "completion", "metrics"])
def test_borrowed_writer_cannot_mutate_private_command_expectation(field: str) -> None:
    submission = _submission(_success)

    def mutate(command: WriterCommand) -> WriterReceipt:
        selected = cast(CommitWorkWithCheckpoint, command)
        if field == "event":
            object.__setattr__(selected.event.event, "event_kind", "checkpoint_forged")
            return _receipt(selected, submission)
        if field == "completion":
            object.__setattr__(selected.completion, "redacted_detail", "forged detail")
            return _receipt(selected, submission)
        object.__setattr__(selected.metrics, "records_read", 99)
        receipt = _receipt(selected, submission)
        return _with_result(
            receipt,
            node=replace(_command_result(receipt).node, records_read=99),
        )

    with pytest.raises(CheckpointCommitProtocolError):
        TransactionalCheckpointResultSink(_Writer(mutate)).submit(submission)


def test_unsuccessful_receipt_cannot_carry_checkpoint() -> None:
    submission = _submission(_retry)
    base_success = _submission(_success)
    success_writer = _Writer(lambda command: _receipt(command, base_success))
    success_sink = TransactionalCheckpointResultSink(success_writer)
    success_sink.submit(base_success)
    success_checkpoint = _command_result(
        _receipt(success_writer.commands[0], base_success)
    ).checkpoint

    def with_checkpoint(command: WriterCommand) -> WriterReceipt:
        return _with_result(
            _receipt(command, submission),
            checkpoint=success_checkpoint,
        )

    with pytest.raises(CheckpointCommitProtocolError):
        TransactionalCheckpointResultSink(_Writer(with_checkpoint)).submit(submission)


class _Clock:
    def now(self) -> UtcTimestamp:
        return _timestamp(3)


def _event_request(
    sequence: int,
    kind: str,
    subject: RunId | WorkItemId,
    occurred_at: UtcTimestamp,
    *,
    payload_schema_version: int = 1,
    payload: RedactedDocument | None = None,
) -> EventAppendRequest:
    return EventAppendRequest(
        EventSequence(sequence),
        sequence,
        PendingExecutionEvent(
            kind,
            occurred_at,
            EventSubjectKind.RUN if type(subject) is RunId else EventSubjectKind.WORK_ITEM,
            subject,
            "corr-checkpoint",
            payload_schema_version,
            RedactedDocument.from_mapping({"kind": kind}) if payload is None else payload,
        ),
    )


def _submit_writer(
    writer: SQLiteTransactionalWriter,
    command: WriterCommand,
) -> WriterReceipt:
    return writer.submit(command, timeout_seconds=2.0).result(timeout_seconds=2.0)


def _prepare_real_lease(
    database_path: Path,
) -> tuple[SQLiteDatabase, SQLiteTransactionalWriter, WorkLeaseService, WorkLease]:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(database_path))
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PipelineId("pip_checkpoint-commit"),
            display_name="Checkpoint commit pipeline",
            description=None,
            created_at=_timestamp(0),
        )
        pipelines.publish_version(
            pipeline_id=PipelineId("pip_checkpoint-commit"),
            expected_latest_version=None,
            specification=_document(nodes=[]),
            planner_format_version=1,
            published_at=_timestamp(0),
        )
    writer = SQLiteTransactionalWriter(
        create_session_factory(database.engine),
        WriterSettings(contention_delay_seconds=0.0),
    )
    writer.start()
    try:
        _submit_writer(
            writer,
            CreateCapturedRun(
                RUN_ID,
                PipelineId("pip_checkpoint-commit"),
                PipelineVersion(1),
                PlannerRunnerKind.SEQUENTIAL.value,
                _document(mode="reference"),
                7,
                (NODE_ID,),
                _timestamp(1),
                _event_request(1, "run_created", RUN_ID, _timestamp(1)),
            ),
        )
        _submit_writer(
            writer,
            TransitionRun(
                RUN_ID,
                1,
                RunState.RUNNING,
                _timestamp(2),
                None,
                None,
                _event_request(2, "run_started", RUN_ID, _timestamp(2)),
            ),
        )
        _submit_writer(
            writer,
            BootstrapWork(
                RUN_ID,
                NODE_ID,
                WORK_ID,
                PARTITION,
                None,
                _timestamp(2),
                1,
                2,
                _event_request(3, "work_created", WORK_ID, _timestamp(2)),
            ),
        )
        service = WorkLeaseService(
            writer,
            _Clock(),
            settings=WorkLeaseSettings(Duration(6_000_000), 2.0, 2.0),
        )
        expiry = _timestamp(9)
        lease = service.acquire(
            AcquireWorkLeaseRequest(
                RUN_ID,
                NODE_ID,
                WORK_ID,
                AttemptNumber(1),
                1,
                2,
                3,
                "lease-owner",
                PlannerRunnerKind.SEQUENTIAL.value,
                "reference-worker",
                _event_request(
                    4,
                    "work_claimed",
                    WORK_ID,
                    _timestamp(3),
                    payload_schema_version=WORK_LEASE_EVENT_PAYLOAD_SCHEMA_VERSION,
                    payload=RedactedDocument.from_mapping(
                        {
                            "attempt_number": 1,
                            "lease_expires_at": str(expiry),
                            "node_id": str(NODE_ID),
                            "runner_kind": PlannerRunnerKind.SEQUENTIAL.value,
                        }
                    ),
                ),
            )
        )
    except BaseException:
        writer.close(timeout_seconds=5.0)
        database.close()
        raise
    return database, writer, service, lease


class _CommitThenTimeoutTicket:
    def __init__(self, ticket: WriterTicket) -> None:
        self._ticket = ticket

    @property
    def submission_id(self) -> WriterSubmissionId:
        return self._ticket.submission_id

    def result(self, *, timeout_seconds: float) -> WriterReceipt:
        self._ticket.result(timeout_seconds=timeout_seconds)
        raise WriterResultTimeoutError("acknowledgement interrupted")

    async def result_async(self, *, timeout_seconds: float) -> WriterReceipt:
        return self.result(timeout_seconds=timeout_seconds)


class _CommitThenTimeoutWriter:
    def __init__(self, writer: SQLiteTransactionalWriter) -> None:
        self._writer = writer

    def submit(self, command: WriterCommand, *, timeout_seconds: float) -> WriterTicket:
        return _CommitThenTimeoutTicket(
            self._writer.submit(command, timeout_seconds=timeout_seconds)
        )


def test_mutated_service_lease_node_is_rejected_before_sink_admission(tmp_path: Path) -> None:
    database, writer, service, lease = _prepare_real_lease(tmp_path / "mutated lease.db")
    accepted = writer.snapshot().accepted
    object.__setattr__(lease.node, "work_running", 0)
    submission = ResultSubmission(lease, _success(lease))
    try:
        with pytest.raises(ResultSinkInvalidResultError):
            submit_work_result(
                TransactionalCheckpointResultSink(writer),
                submission,
                lease_service=service,
            )
        assert writer.snapshot().accepted == accepted
        snapshot = service.snapshot()
        assert (snapshot.active, snapshot.unknown, snapshot.in_flight) == (1, 0, 0)
    finally:
        writer.close(timeout_seconds=5.0)
        database.close()


def test_terminal_node_accepts_authoritative_maximum_attempt_finish(tmp_path: Path) -> None:
    late_work = WorkItemId("wrk_checkpoint-late")
    early_work = WorkItemId("wrk_checkpoint-early")
    late_partition = PartitionKey("partition-late")
    early_partition = PartitionKey("partition-early")
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "nonmonotonic finish.db"))
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PipelineId("pip_checkpoint-commit"),
            display_name="Checkpoint commit pipeline",
            description=None,
            created_at=_timestamp(0),
        )
        pipelines.publish_version(
            pipeline_id=PipelineId("pip_checkpoint-commit"),
            expected_latest_version=None,
            specification=_document(nodes=[]),
            planner_format_version=1,
            published_at=_timestamp(0),
        )
    writer = SQLiteTransactionalWriter(
        create_session_factory(database.engine),
        WriterSettings(contention_delay_seconds=0.0),
    )
    writer.start()

    def acquire(
        work_item_id: WorkItemId,
        *,
        sequence: int,
        node_row_version: int,
        run_row_version: int,
    ) -> WorkLease:
        expiry = _timestamp(33)
        return service.acquire(
            AcquireWorkLeaseRequest(
                RUN_ID,
                NODE_ID,
                work_item_id,
                AttemptNumber(1),
                1,
                node_row_version,
                run_row_version,
                f"owner-{sequence}",
                PlannerRunnerKind.SEQUENTIAL.value,
                f"worker-{sequence}",
                _event_request(
                    sequence,
                    "work_claimed",
                    work_item_id,
                    _timestamp(3),
                    payload_schema_version=WORK_LEASE_EVENT_PAYLOAD_SCHEMA_VERSION,
                    payload=RedactedDocument.from_mapping(
                        {
                            "attempt_number": 1,
                            "lease_expires_at": str(expiry),
                            "node_id": str(NODE_ID),
                            "runner_kind": PlannerRunnerKind.SEQUENTIAL.value,
                        }
                    ),
                ),
            )
        )

    def result_for(
        lease: WorkLease,
        work_item_id: WorkItemId,
        partition: PartitionKey,
        finished_at: UtcTimestamp,
    ) -> SuccessfulWorkResult:
        return SuccessfulWorkResult(
            AttemptSucceeded(
                AttemptEventContext(
                    RUN_ID,
                    NODE_ID,
                    work_item_id,
                    AttemptNumber(1),
                    lease.claim.started_at,
                    PlannerRunnerKind.SEQUENTIAL,
                    lease.claim.worker_identity,
                    "corr-checkpoint",
                ),
                finished_at,
            ),
            ResultCheckpoint(partition, 1, None, None, None),
            ResultMetrics(1, 1, WorkMetricDelta(1, 1, 0, 1, 1)),
        )

    try:
        _submit_writer(
            writer,
            CreateCapturedRun(
                RUN_ID,
                PipelineId("pip_checkpoint-commit"),
                PipelineVersion(1),
                PlannerRunnerKind.SEQUENTIAL.value,
                _document(mode="reference"),
                7,
                (NODE_ID,),
                _timestamp(1),
                _event_request(1, "run_created", RUN_ID, _timestamp(1)),
            ),
        )
        _submit_writer(
            writer,
            TransitionRun(
                RUN_ID,
                1,
                RunState.RUNNING,
                _timestamp(2),
                None,
                None,
                _event_request(2, "run_started", RUN_ID, _timestamp(2)),
            ),
        )
        for index, (work_item_id, partition) in enumerate(
            ((late_work, late_partition), (early_work, early_partition))
        ):
            _submit_writer(
                writer,
                BootstrapWork(
                    RUN_ID,
                    NODE_ID,
                    work_item_id,
                    partition,
                    None,
                    _timestamp(2),
                    1 + index,
                    2 + index,
                    _event_request(3 + index, "work_created", work_item_id, _timestamp(2)),
                ),
            )
        service = WorkLeaseService(
            writer,
            _Clock(),
            settings=WorkLeaseSettings(Duration(30_000_000), 2.0, 2.0),
        )
        sink = TransactionalCheckpointResultSink(writer)
        late_lease = acquire(
            late_work,
            sequence=5,
            node_row_version=3,
            run_row_version=4,
        )
        late_outcome = submit_work_result(
            sink,
            ResultSubmission(
                late_lease,
                result_for(late_lease, late_work, late_partition, _timestamp(10)),
            ),
            lease_service=service,
        )
        assert type(late_outcome) is ResultSinkCommitted

        early_lease = acquire(
            early_work,
            sequence=7,
            node_row_version=5,
            run_row_version=6,
        )
        early_outcome = submit_work_result(
            sink,
            ResultSubmission(
                early_lease,
                result_for(early_lease, early_work, early_partition, _timestamp(5)),
            ),
            lease_service=service,
        )
        assert type(early_outcome) is ResultSinkCommitted
        with database.transaction() as session:
            node = SqlAlchemyRunRepository(session).get_node(RUN_ID, NODE_ID)
            assert node is not None
            assert node.status is RunNodeStatus.SUCCEEDED
            assert node.finished_at == _timestamp(10)
    finally:
        writer.close(timeout_seconds=5.0)
        database.close()


def test_before_commit_definite_nonexecution_retains_exact_lease(tmp_path: Path) -> None:
    database, writer, service, lease = _prepare_real_lease(tmp_path / "before commit.db")
    submission = ResultSubmission(lease, _success(lease))
    try:
        outcome = submit_work_result(
            TransactionalCheckpointResultSink(
                _Writer(WriterDefinitelyNotExecutedError("not dispatched"))
            ),
            submission,
            lease_service=service,
        )
        assert type(outcome) is ResultSinkRejected
        assert outcome.reason is ResultRejectionReason.DEFINITELY_NOT_EXECUTED
        snapshot = service.snapshot()
        assert (snapshot.active, snapshot.unknown, snapshot.in_flight) == (1, 0, 0)
        with database.transaction() as session:
            work = SqlAlchemyWorkItemRepository(session).get(WORK_ID)
            attempt = SqlAlchemyWorkAttemptRepository(session).get(WORK_ID, AttemptNumber(1))
            head = SqlAlchemyCheckpointRepository(session).get_head(RUN_ID, NODE_ID, PARTITION)
            events = SqlAlchemyExecutionEventRepository(session).list_after(
                RUN_ID,
                after=None,
                limit=10,
            )
            assert work is not None
            assert (work.state, work.row_version, work.expected_checkpoint_version) == (
                WorkItemState.RUNNING,
                2,
                0,
            )
            assert attempt is None
            assert head is not None
            assert head.current_version == CheckpointVersion(0)
            assert len(events.items) == 4

        committed = submit_work_result(
            TransactionalCheckpointResultSink(writer),
            submission,
            lease_service=service,
        )
        assert type(committed) is ResultSinkCommitted
        assert service.snapshot().active == 0
    finally:
        writer.close(timeout_seconds=5.0)
        database.close()


def test_after_commit_before_ack_is_unknown_with_complete_projection(tmp_path: Path) -> None:
    database, writer, service, lease = _prepare_real_lease(tmp_path / "after commit.db")
    submission = ResultSubmission(lease, _success(lease))
    try:
        with pytest.raises(ResultSinkOutcomeUnknownError):
            submit_work_result(
                TransactionalCheckpointResultSink(_CommitThenTimeoutWriter(writer)),
                submission,
                lease_service=service,
            )
        snapshot = service.snapshot()
        assert (snapshot.active, snapshot.unknown, snapshot.in_flight) == (0, 1, 0)
        with database.transaction() as session:
            work = SqlAlchemyWorkItemRepository(session).get(WORK_ID)
            attempt = SqlAlchemyWorkAttemptRepository(session).get(WORK_ID, AttemptNumber(1))
            head = SqlAlchemyCheckpointRepository(session).get_head(RUN_ID, NODE_ID, PARTITION)
            events = SqlAlchemyExecutionEventRepository(session).list_after(
                RUN_ID,
                after=None,
                limit=10,
            )
            run = SqlAlchemyRunRepository(session).get(RUN_ID)
            node = SqlAlchemyRunRepository(session).get_node(RUN_ID, NODE_ID)
            assert work is not None
            assert (work.state, work.row_version, work.expected_checkpoint_version) == (
                WorkItemState.SUCCEEDED,
                4,
                1,
            )
            assert attempt is not None
            assert attempt.outcome is AttemptOutcome.SUCCEEDED
            assert head is not None
            assert head.current_version == CheckpointVersion(1)
            assert len(events.items) == 5
            assert run is not None
            assert run.row_version == 5
            assert node is not None
            assert node.status is RunNodeStatus.SUCCEEDED
        assert writer.snapshot().accepted == writer.snapshot().completed == 5
    finally:
        writer.close(timeout_seconds=5.0)
        database.close()


def test_real_wal_commit_reopen_and_duplicate_guard(tmp_path: Path) -> None:
    database_path = tmp_path / "checkpoint commit %.db"
    config = SQLiteDatabaseConfig(database_path)
    database = SQLiteDatabase.open(config)
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PipelineId("pip_checkpoint-commit"),
            display_name="Checkpoint commit pipeline",
            description=None,
            created_at=_timestamp(0),
        )
        pipelines.publish_version(
            pipeline_id=PipelineId("pip_checkpoint-commit"),
            expected_latest_version=None,
            specification=_document(nodes=[]),
            planner_format_version=1,
            published_at=_timestamp(0),
        )
    writer = SQLiteTransactionalWriter(
        create_session_factory(database.engine),
        WriterSettings(contention_delay_seconds=0.0),
    )
    writer.start()
    try:
        _submit_writer(
            writer,
            CreateCapturedRun(
                RUN_ID,
                PipelineId("pip_checkpoint-commit"),
                PipelineVersion(1),
                PlannerRunnerKind.SEQUENTIAL.value,
                _document(mode="reference"),
                7,
                (NODE_ID,),
                _timestamp(1),
                _event_request(1, "run_created", RUN_ID, _timestamp(1)),
            ),
        )
        _submit_writer(
            writer,
            TransitionRun(
                RUN_ID,
                1,
                RunState.RUNNING,
                _timestamp(2),
                None,
                None,
                _event_request(2, "run_started", RUN_ID, _timestamp(2)),
            ),
        )
        _submit_writer(
            writer,
            BootstrapWork(
                RUN_ID,
                NODE_ID,
                WORK_ID,
                PARTITION,
                None,
                _timestamp(2),
                1,
                2,
                _event_request(3, "work_created", WORK_ID, _timestamp(2)),
            ),
        )
        lease_service = WorkLeaseService(
            writer,
            _Clock(),
            settings=WorkLeaseSettings(Duration(6_000_000), 2.0, 2.0),
        )
        expiry = _timestamp(9)
        lease = lease_service.acquire(
            AcquireWorkLeaseRequest(
                RUN_ID,
                NODE_ID,
                WORK_ID,
                AttemptNumber(1),
                1,
                2,
                3,
                "lease-owner",
                PlannerRunnerKind.SEQUENTIAL.value,
                "reference-worker",
                _event_request(
                    4,
                    "work_claimed",
                    WORK_ID,
                    _timestamp(3),
                    payload_schema_version=WORK_LEASE_EVENT_PAYLOAD_SCHEMA_VERSION,
                    payload=RedactedDocument.from_mapping(
                        {
                            "attempt_number": 1,
                            "lease_expires_at": str(expiry),
                            "node_id": str(NODE_ID),
                            "runner_kind": PlannerRunnerKind.SEQUENTIAL.value,
                        }
                    ),
                ),
            )
        )
        submission = ResultSubmission(lease, _success(lease))
        sink = TransactionalCheckpointResultSink(writer)
        outcome = submit_work_result(sink, submission, lease_service=lease_service)
        assert type(outcome) is ResultSinkCommitted
        assert outcome.submission_id == WriterSubmissionId(5)
        assert outcome.checkpoint_version == CheckpointVersion(1)
        assert lease_service.snapshot().active == 0
        before_duplicate = writer.snapshot()
        with pytest.raises(ResultSinkInvalidResultError):
            submit_work_result(sink, submission, lease_service=lease_service)
        after_duplicate = writer.snapshot()
        assert after_duplicate.accepted == before_duplicate.accepted == 5

        with database.transaction() as session:
            work = SqlAlchemyWorkItemRepository(session).get(WORK_ID)
            attempt = SqlAlchemyWorkAttemptRepository(session).get(WORK_ID, AttemptNumber(1))
            head = SqlAlchemyCheckpointRepository(session).get_head(RUN_ID, NODE_ID, PARTITION)
            checkpoint = SqlAlchemyCheckpointRepository(session).get(
                RUN_ID,
                NODE_ID,
                PARTITION,
                CheckpointVersion(1),
            )
            events = SqlAlchemyExecutionEventRepository(session).list_after(
                RUN_ID,
                after=None,
                limit=10,
            )
            run = SqlAlchemyRunRepository(session).get(RUN_ID)
            node = SqlAlchemyRunRepository(session).get_node(RUN_ID, NODE_ID)
            assert work is not None
            assert (work.state, work.row_version, work.expected_checkpoint_version) == (
                WorkItemState.SUCCEEDED,
                4,
                1,
            )
            assert attempt is not None
            assert attempt.outcome is AttemptOutcome.SUCCEEDED
            assert head is not None
            assert head.current_version == CheckpointVersion(1)
            assert checkpoint is not None
            assert checkpoint.committed_at == _timestamp(5)
            assert [event.sequence.number for event in events.items] == [1, 2, 3, 4, 5]
            assert events.items[-1].event_kind == "checkpoint_committed"
            assert run is not None
            assert run.row_version == 5
            assert node is not None
            assert node.status is RunNodeStatus.SUCCEEDED
    finally:
        writer.close(timeout_seconds=5.0)
        database.close()

    reopened = SQLiteDatabase.open(config)
    try:
        with reopened.engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA journal_mode").scalar_one().lower() == "wal"
            assert connection.exec_driver_sql("PRAGMA quick_check").scalar_one() == "ok"
            assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
        with reopened.transaction() as session:
            work = SqlAlchemyWorkItemRepository(session).get(WORK_ID)
            history = SqlAlchemyCheckpointRepository(session).list_history(
                RUN_ID,
                NODE_ID,
                PARTITION,
                limit=10,
            )
            attempts = SqlAlchemyWorkAttemptRepository(session).list_for_work_item(
                WORK_ID,
                limit=10,
            )
            assert work is not None
            assert work.state is WorkItemState.SUCCEEDED
            assert len(history.items) == 1
            assert len(attempts.items) == 1
    finally:
        reopened.close()
