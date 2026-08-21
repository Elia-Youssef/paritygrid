"""Transactional result sink for atomic attempt and checkpoint commits."""

from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass, replace
from datetime import UTC, datetime
from enum import Enum
from typing import Protocol, cast, runtime_checkable

from paritygrid.application.execution.result_sink import (
    ResultRejectionReason,
    ResultSinkAdmissionError,
    ResultSinkCommitted,
    ResultSinkError,
    ResultSinkOutcome,
    ResultSinkOutcomeUnknownError,
    ResultSinkPreAdmissionError,
    ResultSinkProtocolError,
    ResultSinkRejected,
    ResultSubmission,
    SuccessfulWorkResult,
    UnsuccessfulWorkResult,
    WorkResult,
    snapshot_result_submission,
    snapshot_work_result,
)
from paritygrid.application.execution.retry_policy import RetryScheduledDecision
from paritygrid.application.ports.configuration import (
    ConfigurationDocument,
    DocumentArray,
    NestedDocumentObject,
)
from paritygrid.application.ports.consistency import (
    MAX_CONSISTENCY_SEQUENCE,
    CheckpointCommit,
    CheckpointHeadRecord,
    CheckpointRecord,
    CheckpointVersion,
    ConsistencyInvalidRequestError,
    ConsistencyRecordNotFoundError,
    ConsistencyStaleRowVersionError,
    ConsistencyStateConflictError,
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
    ExecutionDuplicateError,
    ExecutionInvalidRequestError,
    ExecutionLeaseLostError,
    ExecutionRecordNotFoundError,
    ExecutionStaleRowVersionError,
    ExecutionStateConflictError,
    RunNodeRecord,
    RunNodeStatus,
    RunRecord,
    WorkAttemptRecord,
    WorkClaim,
    WorkCompletion,
    WorkItemRecord,
)
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    WriterAdmissionTimeoutError,
    WriterCommand,
    WriterCommitOutcomeUnknownError,
    WriterDefinitelyNotExecutedError,
    WriterError,
    WriterReceipt,
    WriterResultTimeoutError,
    WriterSubmissionId,
    WriterTicket,
)
from paritygrid.application.writes import (
    WORK_RESULT_EVENT_PAYLOAD_SCHEMA_VERSION,
    CheckpointWrite,
    CommitWorkAttempt,
    CommitWorkResult,
    CommitWorkWithCheckpoint,
)
from paritygrid.domain.execution import FailureClassification, RunState, WorkItemState
from paritygrid.domain.models import (
    ArtifactId,
    AttemptNumber,
    Duration,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    StateFingerprint,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

CHECKPOINT_COMMIT_EVENT_PAYLOAD_SCHEMA_VERSION = WORK_RESULT_EVENT_PAYLOAD_SCHEMA_VERSION
MAX_CHECKPOINT_COMMIT_TIMEOUT_SECONDS = 86_400.0
MAX_CHECKPOINT_COMMIT_CONTENTION_ATTEMPTS = 9

_STALE_REJECTION_TYPES: tuple[type[Exception], ...] = (
    ExecutionLeaseLostError,
    ExecutionStaleRowVersionError,
    ConsistencyStaleRowVersionError,
)
_STATE_REJECTION_TYPES: tuple[type[Exception], ...] = (
    ExecutionInvalidRequestError,
    ExecutionDuplicateError,
    ExecutionRecordNotFoundError,
    ExecutionStateConflictError,
    ConsistencyInvalidRequestError,
    ConsistencyRecordNotFoundError,
    ConsistencyStateConflictError,
)
_TARGET_OUTCOMES = {
    WorkItemState.SUCCEEDED: AttemptOutcome.SUCCEEDED,
    WorkItemState.RETRY_WAIT: AttemptOutcome.RETRY_SCHEDULED,
    WorkItemState.QUARANTINED: AttemptOutcome.QUARANTINED,
    WorkItemState.FAILED: AttemptOutcome.FAILED,
    WorkItemState.CANCELLED: AttemptOutcome.CANCELLED,
}
_CLOSED_RECEIPT_DATACLASSES = (
    ArtifactId,
    AttemptNumber,
    CheckpointCommit,
    CheckpointHeadRecord,
    CheckpointRecord,
    CheckpointVersion,
    CompletedWork,
    ConfigurationDocument,
    DocumentArray,
    Duration,
    EventSequence,
    ExecutionEventBatch,
    ExecutionEventRecord,
    NestedDocumentObject,
    NodeId,
    PartitionKey,
    PipelineId,
    PipelineVersion,
    RedactedDocument,
    RunId,
    RunNodeRecord,
    RunRecord,
    StateFingerprint,
    UpdatedWorkCheckpoint,
    UtcTimestamp,
    WorkAttemptRecord,
    WorkClaim,
    WorkItemId,
    WorkItemRecord,
    WriterSubmissionId,
)
_CLOSED_RECEIPT_ENUMS = (
    AttemptOutcome,
    EventSubjectKind,
    FailureClassification,
    RunNodeStatus,
    RunState,
    WorkItemState,
)


class CheckpointCommitError(ResultSinkError):
    """Base failure while committing one result through the transactional writer."""


class CheckpointCommitInvalidRequestError(
    CheckpointCommitError,
    ResultSinkPreAdmissionError,
):
    """A result cannot be translated into one closed checkpoint command."""


class CheckpointCommitAdmissionError(
    CheckpointCommitError,
    ResultSinkAdmissionError,
):
    """The writer rejected submission before allocating an identity."""


class CheckpointCommitOutcomeUnknownError(
    CheckpointCommitError,
    ResultSinkOutcomeUnknownError,
):
    """An admitted checkpoint command has no proven durable outcome."""


class CheckpointCommitProtocolError(
    CheckpointCommitOutcomeUnknownError,
    ResultSinkProtocolError,
):
    """Writer acknowledgement evidence is malformed or inconsistent."""


@runtime_checkable
class CheckpointCommitWriter(Protocol):
    """Borrowed transactional-writer surface used without lifecycle ownership."""

    def submit(
        self,
        command: WriterCommand,
        *,
        timeout_seconds: float,
    ) -> WriterTicket:
        """Submit one closed checkpoint or attempt command."""
        ...


@dataclass(frozen=True, slots=True)
class CheckpointCommitSettings:
    """Bounded writer admission and result wait limits."""

    admission_timeout_seconds: float = 5.0
    result_timeout_seconds: float = 60.0

    def __post_init__(self) -> None:
        _validate_timeout(self.admission_timeout_seconds, "checkpoint admission timeout")
        _validate_timeout(self.result_timeout_seconds, "checkpoint result timeout")


@dataclass(frozen=True, slots=True, repr=False)
class _CommitExpectation:
    command: CommitWorkAttempt | CommitWorkWithCheckpoint
    expected_command: CommitWorkAttempt | CommitWorkWithCheckpoint
    result: WorkResult
    claim: WorkClaim
    node: RunNodeRecord
    run: RunRecord


class TransactionalCheckpointResultSink:
    """Translate detached results into one atomic borrowed-writer command."""

    __slots__ = ("_settings", "_writer")

    def __init__(
        self,
        writer: CheckpointCommitWriter,
        settings: CheckpointCommitSettings | None = None,
    ) -> None:
        writer_value = cast(object, writer)
        if not isinstance(writer_value, CheckpointCommitWriter):
            raise TypeError("checkpoint writer must implement CheckpointCommitWriter")
        selected_settings = CheckpointCommitSettings() if settings is None else settings
        if type(selected_settings) is not CheckpointCommitSettings:
            raise TypeError("checkpoint commit settings must use CheckpointCommitSettings")
        self._writer = writer_value
        self._settings = selected_settings

    def submit(self, submission: ResultSubmission, /) -> ResultSinkOutcome:
        """Return only after exact commit or durable non-mutation is proven."""
        expectation = _prepare_commit(submission)
        ticket = self._submit(expectation.command)
        submission_id = _ticket_identity(ticket)
        receipt_or_reason = self._await_result(ticket)
        if type(receipt_or_reason) is ResultRejectionReason:
            return _rejection(expectation, submission_id, receipt_or_reason)
        receipt = cast(WriterReceipt, receipt_or_reason)
        invalid_receipt = False
        try:
            checkpoint_version = _validate_receipt(receipt, submission_id, expectation)
        except Exception:
            invalid_receipt = True
            checkpoint_version = None
        if invalid_receipt:
            raise CheckpointCommitProtocolError("checkpoint writer receipt is invalid")
        context = expectation.result.terminal.context
        return ResultSinkCommitted(
            submission_id,
            expectation.result.kind,
            context.run_id,
            context.node_id,
            context.work_item_id,
            context.attempt_number,
            checkpoint_version,
        )

    def _submit(self, command: WriterCommand) -> WriterTicket:
        admission_timeout = False
        writer_rejected = False
        unexpected = False
        try:
            ticket = self._writer.submit(
                command,
                timeout_seconds=self._settings.admission_timeout_seconds,
            )
        except WriterAdmissionTimeoutError:
            admission_timeout = True
            ticket = None
        except WriterError:
            writer_rejected = True
            ticket = None
        except Exception:
            unexpected = True
            ticket = None
        if admission_timeout or writer_rejected:
            raise CheckpointCommitAdmissionError("checkpoint writer admission failed")
        if unexpected or ticket is None:
            raise CheckpointCommitProtocolError("checkpoint writer admission outcome is unknown")
        return ticket

    def _await_result(
        self,
        ticket: WriterTicket,
    ) -> WriterReceipt | ResultRejectionReason:
        unknown = False
        stale = False
        conflict = False
        definitely_not_executed = False
        unexpected = False
        try:
            receipt = ticket.result(timeout_seconds=self._settings.result_timeout_seconds)
        except WriterResultTimeoutError, WriterCommitOutcomeUnknownError:
            unknown = True
            receipt = None
        except WriterDefinitelyNotExecutedError:
            definitely_not_executed = True
            receipt = None
        except _STALE_REJECTION_TYPES:
            stale = True
            receipt = None
        except _STATE_REJECTION_TYPES:
            conflict = True
            receipt = None
        except WriterError:
            unknown = True
            receipt = None
        except Exception:
            unexpected = True
            receipt = None
        if definitely_not_executed:
            return ResultRejectionReason.DEFINITELY_NOT_EXECUTED
        if stale:
            return ResultRejectionReason.STALE_CAPABILITY
        if conflict:
            return ResultRejectionReason.STATE_CONFLICT
        if unknown or unexpected:
            raise CheckpointCommitOutcomeUnknownError(
                "checkpoint writer durable outcome is unknown"
            )
        if type(receipt) is not WriterReceipt:
            raise CheckpointCommitProtocolError("checkpoint writer receipt is invalid")
        return receipt


def _prepare_commit(submission: object) -> _CommitExpectation:
    invalid = False
    try:
        selected = snapshot_result_submission(cast(ResultSubmission, submission))
        claim = _snapshot_claim(selected.lease.claim)
        node = _snapshot_node(selected.lease.node)
        run = _snapshot_run(selected.lease.run)
        command = _command_for(selected)
        result = snapshot_work_result(selected.result)
        expected_command = _command_for_evidence(
            result,
            claim,
            node,
            run,
            _snapshot_event_frontier(selected.lease.events),
        )
        if not selected.has_current_lease_evidence():
            invalid = True
    except Exception:
        invalid = True
        selected = None
        claim = None
        node = None
        run = None
        command = None
        result = None
        expected_command = None
    if invalid:
        raise CheckpointCommitInvalidRequestError("checkpoint submission is invalid")
    assert selected is not None
    assert claim is not None
    assert node is not None
    assert run is not None
    assert command is not None
    assert result is not None
    assert expected_command is not None
    return _CommitExpectation(command, expected_command, result, claim, node, run)


def _command_for(submission: ResultSubmission) -> CommitWorkAttempt | CommitWorkWithCheckpoint:
    return _command_for_evidence(
        submission.result,
        submission.lease.claim,
        submission.lease.node,
        submission.lease.run,
        submission.lease.events,
    )


def _command_for_evidence(
    result: WorkResult,
    claim: WorkClaim,
    node: RunNodeRecord,
    run: RunRecord,
    events: ExecutionEventBatch,
) -> CommitWorkAttempt | CommitWorkWithCheckpoint:
    context = result.terminal.context
    completion = _completion_for(result)
    event = _event_for(result, events)
    common = (
        context.run_id,
        context.node_id,
        claim,
        completion,
    )
    companions = (
        result.metrics.aggregate_delta,
        node.row_version,
        run.row_version,
        event,
    )
    if type(result) is SuccessfulWorkResult:
        checkpoint = result.checkpoint
        artifact_id = None if checkpoint.artifact is None else checkpoint.artifact.artifact_id
        return CommitWorkWithCheckpoint(
            *common,
            CheckpointWrite(
                checkpoint.partition_key,
                checkpoint.payload_schema_version,
                checkpoint.source_cursor,
                checkpoint.output_position,
                artifact_id,
                result.terminal.finished_at,
            ),
            *companions,
        )
    return CommitWorkAttempt(*common, *companions)


def _completion_for(result: WorkResult) -> WorkCompletion:
    terminal = result.terminal
    retry_available_at = None
    failure_classification = None
    detail = None
    if type(result) is SuccessfulWorkResult:
        target = WorkItemState.SUCCEEDED
    else:
        unsuccessful = cast(UnsuccessfulWorkResult, result)
        rejected_terminal = unsuccessful.terminal
        target = unsuccessful.target_state
        failure_classification = rejected_terminal.failure_classification
        detail = None if rejected_terminal.detail is None else rejected_terminal.detail.text
        if target is WorkItemState.RETRY_WAIT:
            decision = unsuccessful.decision
            assert type(decision) is RetryScheduledDecision
            retry_available_at = decision.retry_available_at
    return WorkCompletion(
        target,
        terminal.finished_at,
        retry_available_at,
        failure_classification,
        detail,
        None,
        result.metrics.records_processed,
        result.metrics.bytes_processed,
    )


def _event_for(result: WorkResult, events: ExecutionEventBatch) -> EventAppendRequest:
    context = result.terminal.context
    target = _completion_for(result).target_state
    event_kind = (
        "checkpoint_committed" if target is WorkItemState.SUCCEEDED else f"work_{target.value}"
    )
    failure = (
        result.terminal.failure_classification if type(result) is UnsuccessfulWorkResult else None
    )
    retry_at = _completion_for(result).retry_available_at
    payload: dict[str, object] = {
        "attempt_number": int(context.attempt_number),
        "failure_classification": None if failure is None else failure.value,
        "node_id": str(context.node_id),
        "retry_available_at": None if retry_at is None else str(retry_at),
        "runner_kind": context.runner_kind.value,
        "target_state": target.value,
    }
    if type(result) is SuccessfulWorkResult:
        artifact = result.checkpoint.artifact
        payload.update(
            {
                "artifact_id": None if artifact is None else str(artifact.artifact_id),
                "checkpoint_payload_schema_version": result.checkpoint.payload_schema_version,
                "partition_key": str(result.checkpoint.partition_key),
            }
        )
    return EventAppendRequest(
        EventSequence(events.next_sequence.number),
        events.counter_row_version,
        PendingExecutionEvent(
            event_kind,
            result.terminal.finished_at,
            EventSubjectKind.WORK_ITEM,
            context.work_item_id,
            context.correlation_id,
            CHECKPOINT_COMMIT_EVENT_PAYLOAD_SCHEMA_VERSION,
            RedactedDocument.from_mapping(payload),
        ),
    )


def _ticket_identity(ticket: WriterTicket) -> WriterSubmissionId:
    failed = False
    try:
        submission_id = cast(object, ticket.submission_id)
        if type(submission_id) is not WriterSubmissionId:
            failed = True
            clean = None
        else:
            number = submission_id.number
            if type(number) is not int:
                failed = True
                clean = None
            else:
                clean = WriterSubmissionId(number)
    except Exception:
        failed = True
        clean = None
    if failed or clean is None:
        raise CheckpointCommitProtocolError("checkpoint writer ticket identity is invalid")
    return clean


def _rejection(
    expectation: _CommitExpectation,
    submission_id: WriterSubmissionId,
    reason: ResultRejectionReason,
) -> ResultSinkRejected:
    context = expectation.result.terminal.context
    return ResultSinkRejected(
        submission_id,
        expectation.result.kind,
        context.run_id,
        context.node_id,
        context.work_item_id,
        context.attempt_number,
        reason,
    )


def _validate_receipt(
    receipt: WriterReceipt,
    submission_id: WriterSubmissionId,
    expectation: _CommitExpectation,
) -> CheckpointVersion | None:
    command = expectation.expected_command
    if (
        not _matches_exact_value(receipt.submission_id, submission_id)
        or receipt.command_kind is not command.kind
        or not _matches_exact_value(receipt.run_id, command.run_id)
        or type(receipt.contention_attempts) is not int
        or not 0 <= receipt.contention_attempts <= MAX_CHECKPOINT_COMMIT_CONTENTION_ATTEMPTS
        or receipt.mutated is not True
        or type(receipt.result) is not CommitWorkResult
    ):
        raise CheckpointCommitProtocolError("checkpoint writer receipt does not match command")
    result = receipt.result
    _validate_completed(result.completed, expectation)
    checkpoint_version = _validate_checkpoint(result.checkpoint, expectation, result.completed)
    _validate_events(result.events, command.run_id, command.event)
    _validate_node(result.node, expectation)
    _validate_run(result.run, expectation.run)
    return checkpoint_version


def _validate_completed(completed: object, expectation: _CommitExpectation) -> None:
    if type(completed) is not CompletedWork:
        raise CheckpointCommitProtocolError("checkpoint completed-work evidence is invalid")
    work = completed.work_item
    attempt = completed.attempt
    result = expectation.result
    context = result.terminal.context
    completion = expectation.expected_command.completion
    if (
        type(work) is not WorkItemRecord
        or type(attempt) is not WorkAttemptRecord
        or not _matches_exact_value(work, work)
        or not _matches_exact_value(attempt, attempt)
        or type(work.partition_key) is not PartitionKey
        or type(work.partition_key.value) is not str
        or (
            work.input_reference is not None
            and type(work.input_reference) is not ConfigurationDocument
        )
        or (
            work.input_reference is not None
            and not _matches_exact_value(work.input_reference, work.input_reference)
        )
        or type(work.created_at) is not UtcTimestamp
        or not _matches_exact_value(work.created_at, work.created_at)
        or work.created_at.value > work.updated_at.value
        or type(work.expected_checkpoint_version) is not int
        or not 0 <= work.expected_checkpoint_version <= MAX_CONSISTENCY_SEQUENCE
    ):
        raise CheckpointCommitProtocolError("checkpoint attempt evidence is invalid")
    _validate_omitted_work_evidence(work, expectation.claim)
    expected_work = (
        context.work_item_id,
        context.run_id,
        context.node_id,
        completion.target_state,
        expectation.claim.row_version + 1,
        int(context.attempt_number),
        completion.retry_available_at,
        None,
        None,
        None,
        None,
        None,
        None,
        result.terminal.finished_at,
    )
    observed_work = (
        work.work_item_id,
        work.run_id,
        work.node_id,
        work.state,
        work.row_version,
        work.completed_attempt_count,
        work.retry_available_at,
        work.lease_owner,
        work.lease_expires_at,
        work.active_attempt_number,
        work.active_attempt_started_at,
        work.active_runner_kind,
        work.active_worker_identity,
        work.updated_at,
    )
    if not _matches_exact_value(observed_work, expected_work):
        raise CheckpointCommitProtocolError("checkpoint work evidence is inconsistent")
    if type(result) is SuccessfulWorkResult and not _matches_exact_value(
        work.partition_key, result.checkpoint.partition_key
    ):
        raise CheckpointCommitProtocolError("checkpoint work partition evidence is inconsistent")
    expected_attempt = (
        context.work_item_id,
        context.attempt_number,
        context.started_at,
        result.terminal.finished_at,
        context.runner_kind.value,
        context.worker_identity,
        _TARGET_OUTCOMES[completion.target_state],
        completion.failure_classification,
        completion.redacted_detail,
        completion.result_reference,
        result.metrics.records_processed,
        result.metrics.bytes_processed,
        result.terminal.duration,
    )
    observed_attempt = (
        attempt.work_item_id,
        attempt.attempt_number,
        attempt.started_at,
        attempt.finished_at,
        attempt.runner_kind,
        attempt.worker_identity,
        attempt.outcome,
        attempt.failure_classification,
        attempt.redacted_detail,
        attempt.result_reference,
        attempt.records_processed,
        attempt.bytes_processed,
        attempt.duration,
    )
    if not _matches_exact_value(observed_attempt, expected_attempt):
        raise CheckpointCommitProtocolError("checkpoint attempt evidence is inconsistent")


def _validate_omitted_work_evidence(work: WorkItemRecord, claim: WorkClaim) -> None:
    """Re-run value invariants for receipt fields omitted from semantic equality."""
    try:
        PartitionKey(work.partition_key.value)
        if work.input_reference is not None:
            ConfigurationDocument(work.input_reference.items)
        UtcTimestamp(work.created_at.value)
    except TypeError, ValueError:
        raise CheckpointCommitProtocolError(
            "checkpoint work metadata evidence is invalid"
        ) from None
    if not work.created_at.value <= claim.started_at.value <= work.updated_at.value:
        raise CheckpointCommitProtocolError("checkpoint work chronology is inconsistent")


def _validate_checkpoint(
    checkpoint: object,
    expectation: _CommitExpectation,
    completed: CompletedWork,
) -> CheckpointVersion | None:
    result = expectation.result
    if type(result) is UnsuccessfulWorkResult:
        if checkpoint is not None:
            raise CheckpointCommitProtocolError("unsuccessful result returned a checkpoint")
        return None
    if type(checkpoint) is not CheckpointCommit:
        raise CheckpointCommitProtocolError("successful result checkpoint evidence is missing")
    if (
        type(checkpoint.head) is not CheckpointHeadRecord
        or type(checkpoint.checkpoint) is not CheckpointRecord
        or type(checkpoint.work) is not UpdatedWorkCheckpoint
    ):
        raise CheckpointCommitProtocolError("checkpoint commit evidence is invalid")
    current = CheckpointVersion(completed.work_item.expected_checkpoint_version)
    version = current.next()
    command = cast(CommitWorkWithCheckpoint, expectation.expected_command)
    successful = cast(SuccessfulWorkResult, result)
    checkpoint_input = successful.checkpoint
    artifact_id = (
        None if checkpoint_input.artifact is None else checkpoint_input.artifact.artifact_id
    )
    head = checkpoint.head
    record = checkpoint.checkpoint
    work = checkpoint.work
    expected_parent = (
        result.terminal.context.run_id,
        result.terminal.context.node_id,
        checkpoint_input.partition_key,
    )
    expected_head = CheckpointHeadRecord(
        *expected_parent,
        version,
        result.terminal.finished_at,
        current.number + 2,
    )
    expected_record = CheckpointRecord(
        *expected_parent,
        version,
        checkpoint_input.payload_schema_version,
        checkpoint_input.source_cursor,
        checkpoint_input.output_position,
        artifact_id,
        result.terminal.finished_at,
    )
    expected_work = UpdatedWorkCheckpoint(
        result.terminal.context.work_item_id,
        *expected_parent,
        version,
        completed.work_item.row_version + 1,
    )
    if (
        not _matches_exact_value(head, expected_head)
        or not _matches_exact_value(record, expected_record)
        or not _matches_exact_value(work, expected_work)
        or not _matches_exact_value(
            command.checkpoint.expected_partition_key,
            checkpoint_input.partition_key,
        )
    ):
        raise CheckpointCommitProtocolError("checkpoint commit evidence is inconsistent")
    return CheckpointVersion(record.version.number)


def _validate_events(batch: object, run_id: RunId, request: EventAppendRequest) -> None:
    if type(batch) is not ExecutionEventBatch or type(batch.items) is not tuple:
        raise CheckpointCommitProtocolError("checkpoint event evidence is invalid")
    if len(batch.items) != 1 or type(batch.items[0]) is not ExecutionEventRecord:
        raise CheckpointCommitProtocolError("checkpoint event batch is inconsistent")
    pending = request.event
    expected = ExecutionEventRecord(
        run_id,
        request.expected_next_sequence,
        pending.event_kind,
        pending.occurred_at,
        pending.subject_kind,
        pending.subject_id,
        pending.correlation_id,
        pending.payload_schema_version,
        pending.payload,
    )
    expected_batch = ExecutionEventBatch(
        (expected,),
        request.expected_next_sequence.advance(1),
        request.expected_counter_row_version + 1,
    )
    if not _matches_exact_value(batch, expected_batch):
        raise CheckpointCommitProtocolError("checkpoint event evidence is inconsistent")


def _validate_node(node: object, expectation: _CommitExpectation) -> None:
    if type(node) is not RunNodeRecord:
        raise CheckpointCommitProtocolError("checkpoint node evidence is invalid")
    previous = expectation.node
    result = expectation.result
    target = expectation.expected_command.completion.target_state
    counts = {
        "pending": previous.work_pending,
        "running": previous.work_running - 1,
        "succeeded": previous.work_succeeded,
        "quarantined": previous.work_quarantined,
        "failed": previous.work_failed,
        "cancelled": previous.work_cancelled,
    }
    counter = {
        WorkItemState.RETRY_WAIT: "pending",
        WorkItemState.SUCCEEDED: "succeeded",
        WorkItemState.QUARANTINED: "quarantined",
        WorkItemState.FAILED: "failed",
        WorkItemState.CANCELLED: "cancelled",
    }[target]
    counts[counter] += 1
    status = _node_status(previous.work_total, counts)
    delta = result.metrics.aggregate_delta
    expected = RunNodeRecord(
        previous.run_id,
        previous.node_id,
        status,
        previous.row_version + 1,
        previous.work_total,
        counts["pending"],
        counts["running"],
        counts["succeeded"],
        counts["quarantined"],
        counts["failed"],
        counts["cancelled"],
        previous.records_read + delta.records_read,
        previous.records_written + delta.records_written,
        previous.records_quarantined + delta.records_quarantined,
        previous.bytes_read + delta.bytes_read,
        previous.bytes_written + delta.bytes_written,
        previous.retry_count + (target is WorkItemState.RETRY_WAIT),
        Duration(previous.duration.microseconds + result.terminal.duration.microseconds),
        previous.started_at,
        None if status is RunNodeStatus.RUNNING else result.terminal.finished_at,
    )
    if status is RunNodeStatus.RUNNING:
        matches = _matches_exact_value(node, expected)
    else:
        finished_at = node.finished_at
        matches = (
            type(finished_at) is UtcTimestamp
            and _matches_exact_value(finished_at, finished_at)
            and _matches_exact_value(node, replace(expected, finished_at=finished_at))
            and finished_at.value >= result.terminal.finished_at.value
            and (previous.started_at is None or finished_at.value >= previous.started_at.value)
        )
    if not matches:
        raise CheckpointCommitProtocolError("checkpoint node evidence is inconsistent")


def _node_status(total: int, counts: dict[str, int]) -> RunNodeStatus:
    if counts["pending"] or counts["running"]:
        return RunNodeStatus.RUNNING
    if counts["failed"]:
        return RunNodeStatus.FAILED
    if counts["cancelled"] == total:
        return RunNodeStatus.CANCELLED
    if counts["quarantined"] or counts["cancelled"]:
        return RunNodeStatus.PARTIALLY_SUCCEEDED
    return RunNodeStatus.SUCCEEDED


def _validate_run(run: object, previous: RunRecord) -> None:
    if type(run) is not RunRecord or not _matches_exact_value(
        run,
        replace(previous, row_version=previous.row_version + 1),
    ):
        raise CheckpointCommitProtocolError("checkpoint run evidence is inconsistent")


def _snapshot_claim(claim: WorkClaim) -> WorkClaim:
    if not _matches_exact_value(claim, claim):
        raise TypeError("work claim evidence is invalid")
    return WorkClaim(
        WorkItemId(str(claim.work_item_id)),
        type(claim.attempt_number)(int(claim.attempt_number)),
        claim.lease_owner,
        claim.row_version,
        UtcTimestamp.parse(str(claim.started_at)),
        UtcTimestamp.parse(str(claim.lease_expires_at)),
        claim.runner_kind,
        claim.worker_identity,
    )


def _snapshot_node(node: RunNodeRecord) -> RunNodeRecord:
    if not _matches_exact_value(node, node):
        raise TypeError("run node evidence is invalid")
    return RunNodeRecord(
        RunId(str(node.run_id)),
        NodeId(str(node.node_id)),
        node.status,
        node.row_version,
        node.work_total,
        node.work_pending,
        node.work_running,
        node.work_succeeded,
        node.work_quarantined,
        node.work_failed,
        node.work_cancelled,
        node.records_read,
        node.records_written,
        node.records_quarantined,
        node.bytes_read,
        node.bytes_written,
        node.retry_count,
        Duration(node.duration.microseconds),
        None if node.started_at is None else UtcTimestamp.parse(str(node.started_at)),
        None if node.finished_at is None else UtcTimestamp.parse(str(node.finished_at)),
    )


def _snapshot_run(run: RunRecord) -> RunRecord:
    if not _matches_exact_value(run, run):
        raise TypeError("run evidence is invalid")
    return RunRecord(
        RunId(str(run.run_id)),
        PipelineId(str(run.pipeline_id)),
        PipelineVersion(run.pipeline_version.number),
        run.runner_kind,
        ConfigurationDocument.from_mapping(run.runner_configuration.to_mapping()),
        run.state,
        run.row_version,
        run.scenario_seed,
        UtcTimestamp.parse(str(run.created_at)),
        None if run.started_at is None else UtcTimestamp.parse(str(run.started_at)),
        None if run.finished_at is None else UtcTimestamp.parse(str(run.finished_at)),
        (
            None
            if run.cancellation_requested_at is None
            else UtcTimestamp.parse(str(run.cancellation_requested_at))
        ),
        None
        if run.recovery_started_at is None
        else UtcTimestamp.parse(str(run.recovery_started_at)),
        None if run.recovered_at is None else UtcTimestamp.parse(str(run.recovered_at)),
        (
            None
            if run.execution_evidence_fingerprint is None
            else StateFingerprint(run.execution_evidence_fingerprint.value)
        ),
    )


def _snapshot_event_frontier(events: ExecutionEventBatch) -> ExecutionEventBatch:
    if not _matches_exact_value(events, events):
        raise TypeError("event frontier evidence is invalid")
    return ExecutionEventBatch(
        (),
        EventSequence(events.next_sequence.number),
        events.counter_row_version,
    )


def _matches_exact_value(observed: object, expected: object) -> bool:
    """Compare a closed contract graph without invoking attacker-defined equality."""
    if type(observed) is not type(expected):
        return False
    value_type = type(expected)
    if expected is None:
        return True
    if value_type in (bool, int, str, bytes, float):
        return observed == expected
    if value_type is datetime:
        observed_time = cast(datetime, observed)
        expected_time = cast(datetime, expected)
        return (
            observed_time.tzinfo is UTC
            and expected_time.tzinfo is UTC
            and (
                observed_time.year,
                observed_time.month,
                observed_time.day,
                observed_time.hour,
                observed_time.minute,
                observed_time.second,
                observed_time.microsecond,
                observed_time.fold,
            )
            == (
                expected_time.year,
                expected_time.month,
                expected_time.day,
                expected_time.hour,
                expected_time.minute,
                expected_time.second,
                expected_time.microsecond,
                expected_time.fold,
            )
        )
    if value_type in _CLOSED_RECEIPT_ENUMS and isinstance(expected, Enum):
        return observed is expected
    if value_type is tuple:
        observed_items = cast(tuple[object, ...], observed)
        expected_items = cast(tuple[object, ...], expected)
        return len(observed_items) == len(expected_items) and all(
            _matches_exact_value(observed_item, expected_item)
            for observed_item, expected_item in zip(observed_items, expected_items, strict=True)
        )
    if value_type in _CLOSED_RECEIPT_DATACLASSES and is_dataclass(expected):
        return all(
            _matches_exact_value(
                getattr(observed, field.name),
                getattr(expected, field.name),
            )
            for field in fields(expected)
        )
    return False


def _validate_timeout(value: object, subject: str) -> float:
    if type(value) is not float or not 0 <= value <= MAX_CHECKPOINT_COMMIT_TIMEOUT_SECONDS:
        raise CheckpointCommitInvalidRequestError(f"{subject} is outside the supported range")
    return value


__all__ = [
    "CHECKPOINT_COMMIT_EVENT_PAYLOAD_SCHEMA_VERSION",
    "MAX_CHECKPOINT_COMMIT_CONTENTION_ATTEMPTS",
    "MAX_CHECKPOINT_COMMIT_TIMEOUT_SECONDS",
    "CheckpointCommitAdmissionError",
    "CheckpointCommitError",
    "CheckpointCommitInvalidRequestError",
    "CheckpointCommitOutcomeUnknownError",
    "CheckpointCommitProtocolError",
    "CheckpointCommitSettings",
    "CheckpointCommitWriter",
    "TransactionalCheckpointResultSink",
]
