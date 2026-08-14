"""Closed worker-result values and the inward-facing result-sink contract."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import cast

from paritygrid.application.execution.attempt_events import (
    AttemptCancelled,
    AttemptEventContext,
    AttemptFailed,
    AttemptSucceeded,
    RedactedAttemptDetail,
    TerminalAttemptEvent,
)
from paritygrid.application.execution.leasing import (
    WorkLease,
    WorkLeaseCompletionDisposition,
    WorkLeaseCompletionReservation,
    WorkLeaseService,
)
from paritygrid.application.execution.retry_policy import (
    Http429RetryDelay,
    RetryDecision,
    RetryPolicyName,
    RetryScheduledDecision,
    RetryStoppedDecision,
)
from paritygrid.application.planner import PlannerRunnerKind
from paritygrid.application.ports.artifacts import (
    MAX_ARTIFACT_ROW_COUNT,
    ArtifactManifestRecord,
    ArtifactRelativePath,
)
from paritygrid.application.ports.configuration import (
    ConfigurationDocument,
    DocumentArray,
    DocumentObject,
    DocumentValue,
    NestedDocumentObject,
)
from paritygrid.application.ports.consistency import (
    CheckpointVersion,
    EventSequence,
    ExecutionEventBatch,
)
from paritygrid.application.ports.execution import RunNodeRecord, RunRecord, WorkClaim
from paritygrid.application.ports.result_sink import ResultSink
from paritygrid.application.ports.run_aggregates import MAX_WORK_METRIC, WorkMetricDelta
from paritygrid.application.ports.writer import MAX_WRITER_SUBMISSION_ID, WriterSubmissionId
from paritygrid.domain.execution import FailureDisposition, WorkItemState
from paritygrid.domain.models import (
    ArtifactId,
    AttemptNumber,
    Duration,
    NodeId,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

RESULT_SINK_SCHEMA_VERSION = 1
MAX_RESULT_CHECKPOINT_SCHEMA_VERSION = 2_147_483_647


class ResultSinkError(RuntimeError):
    """Base failure while validating, submitting, or acknowledging a work result."""


class ResultSinkInvalidResultError(ResultSinkError, ValueError):
    """A result violates the closed, bounded worker-result contract."""


class ResultSinkAdmissionError(ResultSinkError):
    """A sink rejected admission before allocating a durable submission identity."""


class ResultSinkOutcomeUnknownError(ResultSinkError):
    """A sink cannot prove whether the result committed."""


class ResultSinkProtocolError(ResultSinkOutcomeUnknownError):
    """A sink returned malformed or inconsistent acknowledgement evidence."""


class WorkResultKind(StrEnum):
    """Closed durable work targets proposed through every runner boundary."""

    SUCCEEDED = "succeeded"
    RETRY_WAIT = "retry_wait"
    QUARANTINED = "quarantined"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ResultSinkOutcomeKind(StrEnum):
    """Closed sink outcomes that never conflate admission with commit."""

    COMMITTED = "committed"
    REJECTED = "rejected"


class ResultRejectionReason(StrEnum):
    """Stable reasons for a sink-proven non-mutation."""

    STALE_CAPABILITY = "stale_capability"
    STATE_CONFLICT = "state_conflict"
    DEFINITELY_NOT_EXECUTED = "definitely_not_executed"


@dataclass(frozen=True, slots=True)
class ResultMetrics:
    """Bounded attempt counts and the corresponding durable aggregate delta."""

    records_processed: int
    bytes_processed: int
    aggregate_delta: WorkMetricDelta

    def __post_init__(self) -> None:
        _bounded_metric(self.records_processed, "result records processed")
        _bounded_metric(self.bytes_processed, "result bytes processed")
        object.__setattr__(
            self,
            "aggregate_delta",
            _snapshot_metric_delta(self.aggregate_delta),
        )


@dataclass(frozen=True, slots=True, repr=False)
class ResultCheckpoint:
    """Detached partition checkpoint and optional verified artifact manifest."""

    partition_key: PartitionKey
    payload_schema_version: int
    source_cursor: ConfigurationDocument | None
    output_position: ConfigurationDocument | None
    artifact: ArtifactManifestRecord | None

    def __post_init__(self) -> None:
        partition_key = _snapshot_partition_key(self.partition_key)
        _bounded_integer(
            self.payload_schema_version,
            minimum=1,
            maximum=MAX_RESULT_CHECKPOINT_SCHEMA_VERSION,
            subject="result checkpoint schema version",
        )
        object.__setattr__(
            self,
            "source_cursor",
            _snapshot_optional_document(self.source_cursor, "result source cursor"),
        )
        object.__setattr__(
            self,
            "output_position",
            _snapshot_optional_document(self.output_position, "result output position"),
        )
        artifact = None if self.artifact is None else _snapshot_manifest(self.artifact)
        if artifact is not None and artifact.partition_key != partition_key:
            raise ResultSinkInvalidResultError(
                "result artifact does not match the checkpoint partition"
            )
        object.__setattr__(self, "partition_key", partition_key)
        object.__setattr__(self, "artifact", artifact)

    def __repr__(self) -> str:
        return (
            "ResultCheckpoint("
            f"payload_schema_version={self.payload_schema_version!r}, "
            f"has_artifact={self.artifact is not None!r}, "
            "source_cursor=<redacted>, output_position=<redacted>)"
        )


@dataclass(frozen=True, slots=True, repr=False)
class SuccessfulWorkResult:
    """A successful attempt with checkpoint input and an optional manifest."""

    terminal: AttemptSucceeded
    checkpoint: ResultCheckpoint
    metrics: ResultMetrics
    schema_version: int = RESULT_SINK_SCHEMA_VERSION

    def __post_init__(self) -> None:
        terminal = _snapshot_success(self.terminal)
        checkpoint = _snapshot_checkpoint(self.checkpoint)
        metrics = _snapshot_metrics(self.metrics)
        _schema_version(self.schema_version)
        if checkpoint.artifact is not None:
            _validate_artifact_parent(terminal, checkpoint.artifact)
        object.__setattr__(self, "terminal", terminal)
        object.__setattr__(self, "checkpoint", checkpoint)
        object.__setattr__(self, "metrics", metrics)

    @property
    def kind(self) -> WorkResultKind:
        return WorkResultKind.SUCCEEDED

    def __repr__(self) -> str:
        return (
            "SuccessfulWorkResult("
            f"work_item_id={self.terminal.context.work_item_id!r}, "
            f"attempt_number={self.terminal.context.attempt_number!r}, "
            f"has_artifact={self.checkpoint.artifact is not None!r}, "
            f"schema_version={self.schema_version!r}, payload=<redacted>)"
        )


@dataclass(frozen=True, slots=True, repr=False)
class UnsuccessfulWorkResult:
    """A failed or cancelled attempt with an exact terminal disposition."""

    terminal: AttemptFailed | AttemptCancelled
    decision: RetryDecision | None
    metrics: ResultMetrics
    schema_version: int = RESULT_SINK_SCHEMA_VERSION

    def __post_init__(self) -> None:
        terminal = _snapshot_rejected_terminal(self.terminal)
        decision = _snapshot_rejection_decision(self.decision)
        metrics = _snapshot_metrics(self.metrics)
        _schema_version(self.schema_version)
        _validate_rejection(terminal, decision)
        object.__setattr__(self, "terminal", terminal)
        object.__setattr__(self, "decision", decision)
        object.__setattr__(self, "metrics", metrics)

    @property
    def kind(self) -> WorkResultKind:
        return {
            WorkItemState.RETRY_WAIT: WorkResultKind.RETRY_WAIT,
            WorkItemState.QUARANTINED: WorkResultKind.QUARANTINED,
            WorkItemState.FAILED: WorkResultKind.FAILED,
            WorkItemState.CANCELLED: WorkResultKind.CANCELLED,
        }[self.target_state]

    @property
    def disposition(self) -> FailureDisposition:
        if type(self.terminal) is AttemptCancelled:
            return FailureDisposition.CANCEL
        assert self.decision is not None
        return self.decision.disposition

    @property
    def target_state(self) -> WorkItemState:
        return {
            FailureDisposition.RETRY: WorkItemState.RETRY_WAIT,
            FailureDisposition.QUARANTINE: WorkItemState.QUARANTINED,
            FailureDisposition.CONFLICT: WorkItemState.FAILED,
            FailureDisposition.CANCEL: WorkItemState.CANCELLED,
            FailureDisposition.PERMANENT: WorkItemState.FAILED,
        }[self.disposition]

    def __repr__(self) -> str:
        return (
            "UnsuccessfulWorkResult("
            f"work_item_id={self.terminal.context.work_item_id!r}, "
            f"attempt_number={self.terminal.context.attempt_number!r}, "
            f"disposition={self.disposition.value!r}, target_state={self.target_state.value!r}, "
            f"schema_version={self.schema_version!r}, payload=<redacted>)"
        )


type WorkResult = SuccessfulWorkResult | UnsuccessfulWorkResult


@dataclass(frozen=True, slots=True)
class _LeaseEvidence:
    lease: WorkLease
    claim: WorkClaim
    node: RunNodeRecord
    run: RunRecord
    events: ExecutionEventBatch
    submission_id: WriterSubmissionId
    run_id: str
    node_id: str
    work_item_id: str
    attempt_number: int
    claim_row_version: int
    lease_owner: str
    started_at: str
    lease_expires_at: str
    runner_kind: str
    worker_identity: str
    node_row_version: int
    run_row_version: int
    next_event_sequence: int
    event_counter_row_version: int
    lease_submission_number: int

    @classmethod
    def capture(cls, lease: WorkLease) -> _LeaseEvidence:
        _require_exact(lease, WorkLease, "result submission lease")
        claim = _require_exact(lease.claim, WorkClaim, "result submission claim")
        node = _require_exact(lease.node, RunNodeRecord, "result submission node")
        run = _require_exact(lease.run, RunRecord, "result submission run")
        events = _require_exact(
            lease.events,
            ExecutionEventBatch,
            "result submission lease events",
        )
        submission_id = _require_exact(
            lease.submission_id,
            WriterSubmissionId,
            "result submission lease identity",
        )
        run_id = _identifier_text(run.run_id, RunId, "result submission run identity")
        node_run_id = _identifier_text(
            node.run_id,
            RunId,
            "result submission node run identity",
        )
        if node_run_id != run_id:
            raise ResultSinkInvalidResultError("result submission lease parents are inconsistent")
        next_sequence = _require_exact(
            events.next_sequence,
            EventSequence,
            "result submission event frontier",
        )
        return cls(
            lease,
            claim,
            node,
            run,
            events,
            submission_id,
            run_id,
            _identifier_text(node.node_id, NodeId, "result submission node identity"),
            _identifier_text(
                claim.work_item_id,
                WorkItemId,
                "result submission work identity",
            ),
            _attempt_number(claim.attempt_number).number,
            _bounded_row_version(claim.row_version, "result submission claim row version"),
            _exact_text(claim.lease_owner, "result submission lease owner"),
            str(_timestamp(claim.started_at, "result submission attempt start")),
            str(_timestamp(claim.lease_expires_at, "result submission lease expiry")),
            _exact_text(claim.runner_kind, "result submission runner kind"),
            _exact_text(claim.worker_identity, "result submission worker identity"),
            _bounded_row_version(node.row_version, "result submission node row version"),
            _bounded_row_version(run.row_version, "result submission run row version"),
            _exact_integer(
                next_sequence.number,
                "result submission event sequence value",
            ),
            _bounded_row_version(
                events.counter_row_version,
                "result submission event counter row version",
            ),
            _bounded_integer(
                submission_id.number,
                minimum=1,
                maximum=MAX_WRITER_SUBMISSION_ID,
                subject="result submission lease identity value",
            ),
        )

    def matches(self, lease: WorkLease) -> bool:
        failed = False
        try:
            current = _LeaseEvidence.capture(lease)
        except Exception:
            failed = True
            current = None
        return (
            not failed
            and current is not None
            and current.lease is self.lease
            and current.claim is self.claim
            and current.node is self.node
            and current.run is self.run
            and current.events is self.events
            and current.submission_id is self.submission_id
            and current.run_id == self.run_id
            and current.node_id == self.node_id
            and current.work_item_id == self.work_item_id
            and current.attempt_number == self.attempt_number
            and current.claim_row_version == self.claim_row_version
            and current.lease_owner == self.lease_owner
            and current.started_at == self.started_at
            and current.lease_expires_at == self.lease_expires_at
            and current.runner_kind == self.runner_kind
            and current.worker_identity == self.worker_identity
            and current.node_row_version == self.node_row_version
            and current.run_row_version == self.run_row_version
            and current.next_event_sequence == self.next_event_sequence
            and current.event_counter_row_version == self.event_counter_row_version
            and current.lease_submission_number == self.lease_submission_number
        )


@dataclass(frozen=True, slots=True, repr=False)
class ResultSubmission:
    """Runner-owned pairing of one lease candidate and detached worker result."""

    lease: WorkLease
    result: WorkResult
    _lease_evidence: _LeaseEvidence = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        selected_lease = _require_exact(self.lease, WorkLease, "result submission lease")
        evidence = _LeaseEvidence.capture(selected_lease)
        detached_result = snapshot_work_result(self.result)
        _validate_submission_identity(evidence, detached_result)
        object.__setattr__(self, "result", detached_result)
        object.__setattr__(self, "_lease_evidence", evidence)

    def __repr__(self) -> str:
        return (
            "ResultSubmission("
            f"work_item_id={self.result.terminal.context.work_item_id!r}, "
            f"attempt_number={self.result.terminal.context.attempt_number!r}, "
            f"kind={self.result.kind.value!r}, authority=<redacted>, "
            "lease=<redacted>, result=<redacted>)"
        )

    def has_current_lease_evidence(self) -> bool:
        """Return whether the paired capability still matches its captured evidence."""
        return self._lease_evidence.matches(self.lease)


@dataclass(frozen=True, slots=True)
class ResultSinkCommitted:
    """Acknowledgement returned only after the sink proves durable commit."""

    submission_id: WriterSubmissionId
    result_kind: WorkResultKind
    run_id: RunId
    node_id: NodeId
    work_item_id: WorkItemId
    attempt_number: AttemptNumber
    checkpoint_version: CheckpointVersion | None

    def __post_init__(self) -> None:
        submission_id = _snapshot_submission_id(self.submission_id)
        _require_exact(self.result_kind, WorkResultKind, "result receipt kind")
        run_id = _snapshot_run_id(self.run_id)
        node_id = _snapshot_node_id(self.node_id)
        work_item_id = _snapshot_work_item_id(self.work_item_id)
        attempt_number = _attempt_number(self.attempt_number)
        checkpoint_version = (
            None
            if self.checkpoint_version is None
            else _checkpoint_version(self.checkpoint_version)
        )
        if (self.result_kind is WorkResultKind.SUCCEEDED) != (checkpoint_version is not None):
            raise ResultSinkInvalidResultError(
                "only a successful committed result carries a checkpoint version"
            )
        if checkpoint_version is not None and checkpoint_version.number < 1:
            raise ResultSinkInvalidResultError(
                "committed result checkpoint version must be positive"
            )
        object.__setattr__(self, "submission_id", submission_id)
        object.__setattr__(self, "run_id", run_id)
        object.__setattr__(self, "node_id", node_id)
        object.__setattr__(self, "work_item_id", work_item_id)
        object.__setattr__(self, "attempt_number", attempt_number)
        object.__setattr__(self, "checkpoint_version", checkpoint_version)

    @property
    def kind(self) -> ResultSinkOutcomeKind:
        return ResultSinkOutcomeKind.COMMITTED


@dataclass(frozen=True, slots=True)
class ResultSinkRejected:
    """Evidence that an admitted result was proven not to have mutated durable state."""

    submission_id: WriterSubmissionId
    result_kind: WorkResultKind
    run_id: RunId
    node_id: NodeId
    work_item_id: WorkItemId
    attempt_number: AttemptNumber
    reason: ResultRejectionReason

    def __post_init__(self) -> None:
        submission_id = _snapshot_submission_id(self.submission_id)
        _require_exact(self.result_kind, WorkResultKind, "result rejection work kind")
        run_id = _snapshot_run_id(self.run_id)
        node_id = _snapshot_node_id(self.node_id)
        work_item_id = _snapshot_work_item_id(self.work_item_id)
        attempt_number = _attempt_number(self.attempt_number)
        _require_exact(self.reason, ResultRejectionReason, "result rejection reason")
        object.__setattr__(self, "submission_id", submission_id)
        object.__setattr__(self, "run_id", run_id)
        object.__setattr__(self, "node_id", node_id)
        object.__setattr__(self, "work_item_id", work_item_id)
        object.__setattr__(self, "attempt_number", attempt_number)

    @property
    def kind(self) -> ResultSinkOutcomeKind:
        return ResultSinkOutcomeKind.REJECTED


type ResultSinkOutcome = ResultSinkCommitted | ResultSinkRejected


def snapshot_work_result(result: WorkResult) -> WorkResult:
    """Return a detached exact copy of one closed worker-result variant."""
    if type(result) is SuccessfulWorkResult:
        return SuccessfulWorkResult(
            result.terminal,
            result.checkpoint,
            result.metrics,
            result.schema_version,
        )
    if type(result) is UnsuccessfulWorkResult:
        return UnsuccessfulWorkResult(
            result.terminal,
            result.decision,
            result.metrics,
            result.schema_version,
        )
    raise TypeError("work result must use a closed WorkResult variant")


def snapshot_result_submission(submission: ResultSubmission) -> ResultSubmission:
    """Revalidate lease evidence and detach result data before authority reservation."""
    selected = _require_exact(submission, ResultSubmission, "result submission")
    if not selected.has_current_lease_evidence():
        raise ResultSinkInvalidResultError("result submission lease evidence changed")
    return ResultSubmission(selected.lease, selected.result)


def submit_work_result(
    sink: ResultSink,
    submission: ResultSubmission,
    *,
    lease_service: WorkLeaseService,
) -> ResultSinkOutcome:
    """Reserve exact lease authority across one borrowed, blocking sink call."""
    sink_value = cast(object, sink)
    if not isinstance(sink_value, ResultSink):
        raise TypeError("result sink must implement ResultSink")
    service = _require_exact(
        lease_service,
        WorkLeaseService,
        "result sink lease service",
    )
    selected = snapshot_result_submission(submission)
    reservation = _reserve_completion(service, selected.lease)
    failure_type: type[ResultSinkError] | None = None
    failure_disposition = WorkLeaseCompletionDisposition.MARK_UNKNOWN
    try:
        outcome = sink_value.submit(selected)
    except ResultSinkAdmissionError:
        failure_type = ResultSinkAdmissionError
        failure_disposition = WorkLeaseCompletionDisposition.RETAIN_ACTIVE
        outcome = None
    except ResultSinkInvalidResultError:
        failure_type = ResultSinkInvalidResultError
        outcome = None
    except ResultSinkProtocolError:
        failure_type = ResultSinkProtocolError
        outcome = None
    except ResultSinkOutcomeUnknownError:
        failure_type = ResultSinkOutcomeUnknownError
        outcome = None
    except ResultSinkError:
        failure_type = ResultSinkOutcomeUnknownError
        outcome = None
    except Exception:
        failure_type = ResultSinkOutcomeUnknownError
        outcome = None
    except BaseException:
        _best_effort_mark_unknown(service, reservation)
        raise
    if failure_type is not None:
        if not _finalize_completion(service, reservation, failure_disposition):
            raise ResultSinkOutcomeUnknownError("result sink completion state is unknown")
        raise failure_type("result sink operation failed")
    try:
        detached = _snapshot_sink_outcome(outcome)
        _validate_sink_outcome(detached, selected.result)
    except Exception:
        detached = None
    if detached is None:
        finalized = _finalize_completion(
            service,
            reservation,
            WorkLeaseCompletionDisposition.MARK_UNKNOWN,
        )
        if not finalized:
            raise ResultSinkOutcomeUnknownError("result sink completion state is unknown")
        raise ResultSinkProtocolError("result sink returned an invalid outcome")
    disposition = _completion_disposition(detached)
    if not _finalize_completion(service, reservation, disposition):
        raise ResultSinkOutcomeUnknownError("result sink completion state is unknown")
    return detached


def _reserve_completion(
    service: WorkLeaseService,
    lease: WorkLease,
) -> WorkLeaseCompletionReservation:
    failed = False
    try:
        reservation = service.reserve_completion(lease)
    except Exception:
        failed = True
        reservation = None
    if failed or reservation is None:
        raise ResultSinkInvalidResultError(
            "result submission lease is not the active service capability"
        ) from None
    return reservation


def _completion_disposition(
    outcome: ResultSinkOutcome,
) -> WorkLeaseCompletionDisposition:
    if type(outcome) is ResultSinkCommitted:
        return WorkLeaseCompletionDisposition.RETIRE_COMMITTED
    rejected = cast(ResultSinkRejected, outcome)
    if rejected.reason is ResultRejectionReason.DEFINITELY_NOT_EXECUTED:
        return WorkLeaseCompletionDisposition.RETAIN_ACTIVE
    return WorkLeaseCompletionDisposition.MARK_UNKNOWN


def _finalize_completion(
    service: WorkLeaseService,
    reservation: WorkLeaseCompletionReservation,
    disposition: WorkLeaseCompletionDisposition,
) -> bool:
    failed = False
    try:
        service.finalize_completion(reservation, disposition)
    except Exception:
        failed = True
    return not failed


def _best_effort_mark_unknown(
    service: WorkLeaseService,
    reservation: WorkLeaseCompletionReservation,
) -> None:
    try:
        service.finalize_completion(
            reservation,
            WorkLeaseCompletionDisposition.MARK_UNKNOWN,
        )
    except BaseException:
        return


def _snapshot_metrics(metrics: object) -> ResultMetrics:
    selected = _require_exact(metrics, ResultMetrics, "result metrics")
    return ResultMetrics(
        selected.records_processed,
        selected.bytes_processed,
        selected.aggregate_delta,
    )


def _snapshot_checkpoint(checkpoint: object) -> ResultCheckpoint:
    selected = _require_exact(checkpoint, ResultCheckpoint, "result checkpoint")
    return ResultCheckpoint(
        selected.partition_key,
        selected.payload_schema_version,
        selected.source_cursor,
        selected.output_position,
        selected.artifact,
    )


def _snapshot_metric_delta(delta: object) -> WorkMetricDelta:
    selected = _require_exact(delta, WorkMetricDelta, "result aggregate delta")
    values = (
        selected.records_read,
        selected.records_written,
        selected.records_quarantined,
        selected.bytes_read,
        selected.bytes_written,
    )
    for value in values:
        _bounded_metric(value, "result aggregate metric")
    return WorkMetricDelta(*values)


def _snapshot_success(value: object) -> AttemptSucceeded:
    selected = _require_exact(value, AttemptSucceeded, "accepted result terminal")
    return AttemptSucceeded(
        _snapshot_context(selected.context),
        _timestamp(selected.finished_at, "accepted result finish time"),
    )


def _snapshot_rejected_terminal(value: object) -> AttemptFailed | AttemptCancelled:
    if type(value) is AttemptFailed:
        detail = _snapshot_detail(value.detail)
        return AttemptFailed(
            _snapshot_context(value.context),
            _timestamp(value.finished_at, "rejected result finish time"),
            value.failure_classification,
            detail,
        )
    if type(value) is AttemptCancelled:
        return AttemptCancelled(
            _snapshot_context(value.context),
            _timestamp(value.finished_at, "rejected result finish time"),
            _snapshot_detail(value.detail),
        )
    raise TypeError("rejected result terminal must use AttemptFailed or AttemptCancelled")


def _snapshot_context(context: object) -> AttemptEventContext:
    selected = _require_exact(context, AttemptEventContext, "result attempt context")
    return AttemptEventContext(
        _snapshot_run_id(selected.run_id),
        _snapshot_node_id(selected.node_id),
        _snapshot_work_item_id(selected.work_item_id),
        _attempt_number(selected.attempt_number),
        _timestamp(selected.started_at, "result attempt start time"),
        _require_exact(selected.runner_kind, PlannerRunnerKind, "result runner kind"),
        _exact_text(selected.worker_identity, "result worker identity"),
        _optional_exact_text(selected.correlation_id, "result correlation identity"),
        _exact_integer(selected.schema_version, "result attempt-event schema version"),
    )


def _snapshot_detail(value: object) -> RedactedAttemptDetail | None:
    if value is None:
        return None
    detail = _require_exact(value, RedactedAttemptDetail, "result redacted detail")
    return RedactedAttemptDetail(_exact_text(detail.text, "result redacted detail text"))


def _snapshot_rejection_decision(value: object) -> RetryDecision | None:
    if value is None:
        return None
    if type(value) is RetryScheduledDecision:
        return RetryScheduledDecision(
            _require_exact(value.policy_name, RetryPolicyName, "result retry policy"),
            _snapshot_work_item_id(value.work_item_id),
            _attempt_number(value.attempt_number),
            value.classification,
            _timestamp(value.failed_at, "result retry failure time"),
            _timestamp(value.observed_at, "result retry observation time"),
            _snapshot_http_delay(value.http_429_delay),
            _duration(value.jitter, "result retry jitter"),
            _duration(value.delay, "result retry delay"),
            _timestamp(value.retry_available_at, "result retry availability time"),
        )
    if type(value) is RetryStoppedDecision:
        return RetryStoppedDecision(
            _require_exact(value.policy_name, RetryPolicyName, "result retry policy"),
            _snapshot_work_item_id(value.work_item_id),
            _attempt_number(value.attempt_number),
            value.classification,
            _timestamp(value.failed_at, "result retry failure time"),
            _require_exact(
                value.disposition,
                FailureDisposition,
                "result retry disposition",
            ),
            _exact_bool(value.exhausted, "result retry exhaustion marker"),
        )
    raise TypeError("rejected result decision must use a closed RetryDecision variant or None")


def _snapshot_http_delay(value: object) -> Http429RetryDelay | None:
    if value is None:
        return None
    selected = _require_exact(value, Http429RetryDelay, "result HTTP 429 delay")
    return Http429RetryDelay(_duration(selected.duration, "result HTTP 429 delay duration"))


def _validate_rejection(
    terminal: AttemptFailed | AttemptCancelled,
    decision: RetryDecision | None,
) -> None:
    if type(terminal) is AttemptCancelled:
        if decision is not None:
            raise ResultSinkInvalidResultError("cancelled result cannot carry a retry decision")
        return
    if decision is None:
        raise ResultSinkInvalidResultError("failed result requires a retry decision")
    context = terminal.context
    if (
        decision.work_item_id != context.work_item_id
        or decision.attempt_number != context.attempt_number
        or decision.classification is not terminal.failure_classification
        or decision.failed_at != terminal.finished_at
    ):
        raise ResultSinkInvalidResultError(
            "rejected result decision does not match its terminal attempt"
        )


def _validate_artifact_parent(
    terminal: TerminalAttemptEvent,
    artifact: ArtifactManifestRecord,
) -> None:
    context = terminal.context
    if artifact.run_id != context.run_id or artifact.node_id != context.node_id:
        raise ResultSinkInvalidResultError(
            "result artifact does not match the terminal attempt parent"
        )
    if artifact.created_at < context.started_at or artifact.created_at > terminal.finished_at:
        raise ResultSinkInvalidResultError(
            "result artifact time is outside the terminal attempt interval"
        )


def _validate_submission_identity(evidence: _LeaseEvidence, result: WorkResult) -> None:
    context = result.terminal.context
    if (
        str(context.run_id) != evidence.run_id
        or str(context.node_id) != evidence.node_id
        or str(context.work_item_id) != evidence.work_item_id
        or int(context.attempt_number) != evidence.attempt_number
        or str(context.started_at) != evidence.started_at
        or context.runner_kind.value != evidence.runner_kind
        or context.worker_identity != evidence.worker_identity
    ):
        raise ResultSinkInvalidResultError(
            "result attempt does not match the active lease capability"
        )
    if str(result.terminal.finished_at) >= evidence.lease_expires_at:
        raise ResultSinkInvalidResultError("result attempt did not finish before lease expiry")


def _validate_sink_outcome(outcome: ResultSinkOutcome, result: WorkResult) -> None:
    context = result.terminal.context
    if (
        outcome.result_kind is not result.kind
        or outcome.run_id != context.run_id
        or outcome.node_id != context.node_id
        or outcome.work_item_id != context.work_item_id
        or outcome.attempt_number != context.attempt_number
    ):
        raise ResultSinkProtocolError("result sink outcome does not match the submission")


def _snapshot_sink_outcome(value: object) -> ResultSinkOutcome:
    if type(value) is ResultSinkCommitted:
        return ResultSinkCommitted(
            value.submission_id,
            value.result_kind,
            value.run_id,
            value.node_id,
            value.work_item_id,
            value.attempt_number,
            value.checkpoint_version,
        )
    if type(value) is ResultSinkRejected:
        return ResultSinkRejected(
            value.submission_id,
            value.result_kind,
            value.run_id,
            value.node_id,
            value.work_item_id,
            value.attempt_number,
            value.reason,
        )
    raise TypeError("result sink outcome must use a closed variant")


def _snapshot_manifest(value: object) -> ArtifactManifestRecord:
    manifest = _require_exact(value, ArtifactManifestRecord, "result artifact manifest")
    return ArtifactManifestRecord(
        artifact_id=_snapshot_artifact_id(manifest.artifact_id),
        run_id=_snapshot_run_id(manifest.run_id),
        node_id=_snapshot_node_id(manifest.node_id),
        partition_key=_snapshot_partition_key(manifest.partition_key),
        relative_path=_snapshot_artifact_path(manifest.relative_path),
        media_type=_exact_text(manifest.media_type, "result artifact media type"),
        schema_version=_bounded_integer(
            manifest.schema_version,
            minimum=1,
            maximum=MAX_RESULT_CHECKPOINT_SCHEMA_VERSION,
            subject="result artifact schema version",
        ),
        byte_size=_bounded_metric(manifest.byte_size, "result artifact byte size"),
        row_count=_bounded_integer(
            manifest.row_count,
            minimum=0,
            maximum=MAX_ARTIFACT_ROW_COUNT,
            subject="result artifact row count",
        ),
        sha256=_exact_text(manifest.sha256, "result artifact SHA-256"),
        created_at=_timestamp(manifest.created_at, "result artifact creation time"),
    )


def _snapshot_optional_document(
    value: object,
    subject: str,
) -> ConfigurationDocument | None:
    if value is None:
        return None
    document = _require_exact(value, ConfigurationDocument, subject)
    return ConfigurationDocument(_snapshot_document_object(document.items))


def _snapshot_document_object(value: object) -> DocumentObject:
    if type(value) is not tuple:
        raise TypeError("result document object must be a tuple")
    items: list[tuple[str, DocumentValue]] = []
    for entry in cast(tuple[object, ...], value):
        if type(entry) is not tuple:
            raise TypeError("result document entry must be a key-value tuple")
        pair = cast(tuple[object, ...], entry)
        if len(pair) != 2:
            raise TypeError("result document entry must be a key-value tuple")
        key = _exact_text(pair[0], "result document key")
        items.append((key, _snapshot_document_value(pair[1])))
    return tuple(items)


def _snapshot_document_value(value: object) -> DocumentValue:
    if value is None or type(value) in {bool, int, str}:
        return cast(bool | int | str | None, value)
    if type(value) is DocumentArray:
        if type(value.values) is not tuple:
            raise TypeError("result document array values must be a tuple")
        return DocumentArray(tuple(_snapshot_document_value(item) for item in value.values))
    if type(value) is NestedDocumentObject:
        return NestedDocumentObject(_snapshot_document_object(value.items))
    raise TypeError("result document contains an unsupported value")


def _snapshot_artifact_path(value: object) -> ArtifactRelativePath:
    selected = _require_exact(value, ArtifactRelativePath, "result artifact path")
    return ArtifactRelativePath(_exact_text(selected.value, "result artifact path text"))


def _snapshot_partition_key(value: object) -> PartitionKey:
    selected = _require_exact(value, PartitionKey, "result partition key")
    return PartitionKey(_exact_text(selected.value, "result partition key text"))


def _snapshot_run_id(value: object) -> RunId:
    return RunId(_identifier_text(value, RunId, "result run identity"))


def _snapshot_node_id(value: object) -> NodeId:
    return NodeId(_identifier_text(value, NodeId, "result node identity"))


def _snapshot_work_item_id(value: object) -> WorkItemId:
    return WorkItemId(_identifier_text(value, WorkItemId, "result work identity"))


def _snapshot_artifact_id(value: object) -> ArtifactId:
    return ArtifactId(_identifier_text(value, ArtifactId, "result artifact identity"))


def _identifier_text(
    value: object,
    expected: type[RunId] | type[NodeId] | type[WorkItemId] | type[ArtifactId],
    subject: str,
) -> str:
    if type(value) is not expected:
        raise TypeError(f"{subject} must use {expected.__name__}")
    selected = value
    return _exact_text(selected.value, f"{subject} text")


def _attempt_number(value: object) -> AttemptNumber:
    selected = _require_exact(value, AttemptNumber, "result attempt number")
    return AttemptNumber(_exact_integer(selected.number, "result attempt number value"))


def _checkpoint_version(value: object) -> CheckpointVersion:
    selected = _require_exact(value, CheckpointVersion, "result checkpoint version")
    return CheckpointVersion(_exact_integer(selected.number, "result checkpoint version value"))


def _snapshot_submission_id(value: object) -> WriterSubmissionId:
    selected = _require_exact(value, WriterSubmissionId, "result submission identity")
    return WriterSubmissionId(_exact_integer(selected.number, "result submission identity value"))


def _timestamp(value: object, subject: str) -> UtcTimestamp:
    selected = _require_exact(value, UtcTimestamp, subject)
    if type(selected.value) is not datetime:
        raise TypeError(f"{subject} value must be datetime")
    return UtcTimestamp.parse(str(selected))


def _duration(value: object, subject: str) -> Duration:
    selected = _require_exact(value, Duration, subject)
    return Duration(_exact_integer(selected.microseconds, f"{subject} value"))


def _schema_version(value: object) -> None:
    if type(value) is not int:
        raise TypeError("result schema version must be an integer")
    if value != RESULT_SINK_SCHEMA_VERSION:
        raise ResultSinkInvalidResultError("result schema version is not supported")


def _bounded_metric(value: object, subject: str) -> int:
    return _bounded_integer(value, minimum=0, maximum=MAX_WORK_METRIC, subject=subject)


def _bounded_row_version(value: object, subject: str) -> int:
    return _bounded_integer(
        value,
        minimum=1,
        maximum=MAX_RESULT_CHECKPOINT_SCHEMA_VERSION,
        subject=subject,
    )


def _bounded_integer(
    value: object,
    *,
    minimum: int,
    maximum: int,
    subject: str,
) -> int:
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    if not minimum <= value <= maximum:
        raise ResultSinkInvalidResultError(f"{subject} is outside the supported range")
    return value


def _exact_integer(value: object, subject: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    return value


def _exact_bool(value: object, subject: str) -> bool:
    if type(value) is not bool:
        raise TypeError(f"{subject} must be a boolean")
    return value


def _exact_text(value: object, subject: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{subject} must be text")
    return value


def _optional_exact_text(value: object, subject: str) -> str | None:
    if value is None:
        return None
    return _exact_text(value, subject)


def _require_exact[T](value: object, expected: type[T], subject: str) -> T:
    if type(value) is not expected:
        raise TypeError(f"{subject} must use {expected.__name__}")
    return cast(T, value)


__all__ = [
    "MAX_RESULT_CHECKPOINT_SCHEMA_VERSION",
    "RESULT_SINK_SCHEMA_VERSION",
    "ResultCheckpoint",
    "ResultMetrics",
    "ResultRejectionReason",
    "ResultSink",
    "ResultSinkAdmissionError",
    "ResultSinkCommitted",
    "ResultSinkError",
    "ResultSinkInvalidResultError",
    "ResultSinkOutcome",
    "ResultSinkOutcomeKind",
    "ResultSinkOutcomeUnknownError",
    "ResultSinkProtocolError",
    "ResultSinkRejected",
    "ResultSubmission",
    "SuccessfulWorkResult",
    "UnsuccessfulWorkResult",
    "WorkResult",
    "WorkResultKind",
    "snapshot_result_submission",
    "snapshot_work_result",
    "submit_work_result",
]
