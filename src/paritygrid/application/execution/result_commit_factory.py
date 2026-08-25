"""Parent-owned production factory from validated intents to writer commands.

The P7.9 coordinator emits one rebased ``CommitIntent`` per durable
result.  The transactional adapter accepts only intents compiled into
the closed Phase 6 command set by a parent-owned factory.  This module
is that production factory: it derives the ``WorkClaim`` strictly from
the rebased intent facts, maps the runner-neutral outcome and metrics
onto the durable completion records, and builds the exact
command-derived durable event the writer validates.  It owns no
database state and fails closed before writer admission whenever the
intent is not exactly compilable.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from paritygrid.application.execution.result_coordinator import (
    CommitIntent,
    ResultValidationRejection,
)
from paritygrid.application.execution.runner_contract import (
    MAX_METRIC_VALUE,
    WorkResultV1,
)
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.execution import WorkClaim, WorkCompletion
from paritygrid.application.ports.run_aggregates import WorkMetricDelta
from paritygrid.application.ports.writer import EventAppendRequest
from paritygrid.application.writes.execution import (
    WORK_RESULT_EVENT_PAYLOAD_SCHEMA_VERSION,
    CheckpointWrite,
    CommitWorkAttempt,
    CommitWorkWithCheckpoint,
)
from paritygrid.domain.execution import FailureClassification, WorkItemState
from paritygrid.domain.models import (
    ArtifactId,
    AttemptNumber,
    NodeId,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MAX_CORRELATION_ID_LENGTH = 96
_CHECKPOINT_PAYLOAD_SCHEMA_VERSION = 1

_OUTCOME_TARGET_STATES: dict[str, WorkItemState] = {
    "succeeded": WorkItemState.SUCCEEDED,
    "retry_wait": WorkItemState.RETRY_WAIT,
    "quarantined": WorkItemState.QUARANTINED,
    "failed": WorkItemState.FAILED,
    "cancelled": WorkItemState.CANCELLED,
}
_METRIC_NAMES = (
    "records_read",
    "records_written",
    "records_quarantined",
    "bytes_read",
    "bytes_written",
)


def _from_micros(value: int, subject: str) -> UtcTimestamp:
    if type(value) is not int or value < 0:
        raise ResultValidationRejection(f"{subject} is outside the supported range")
    return UtcTimestamp(_EPOCH + timedelta(microseconds=value))


def _require_correlation_id(value: object) -> str | None:
    if value is None:
        return None
    if type(value) is not str:
        raise TypeError("correlation id must be text or None")
    text = value
    if not 1 <= len(text) <= _MAX_CORRELATION_ID_LENGTH:
        raise ResultValidationRejection("correlation id length is outside the range")
    for character in text:
        if not "\x20" <= character <= "\x7e":
            raise ResultValidationRejection("correlation id must use printable ASCII")
    return text


def _require_classification(intent: CommitIntent) -> FailureClassification | None:
    if intent.outcome == "succeeded":
        if intent.failure_classification is not None:
            raise ResultValidationRejection(
                "successful results cannot carry a failure classification"
            )
        return None
    if intent.failure_classification is None:
        raise ResultValidationRejection("unsuccessful results require a failure classification")
    try:
        return FailureClassification(intent.failure_classification)
    except ValueError as error:
        raise ResultValidationRejection("result failure classification is unknown") from error


def _metric_map(result: WorkResultV1) -> dict[str, int]:
    metrics: dict[str, int] = dict.fromkeys(_METRIC_NAMES, 0)
    for metric in result.metrics:
        if metric.name not in metrics:
            raise ResultValidationRejection(
                "result metric name is not part of the durable metric set"
            )
        if metrics[metric.name] != 0:
            raise ResultValidationRejection("result metric name is repeated")
        if metric.value > MAX_METRIC_VALUE:
            raise ResultValidationRejection("result metric value exceeds the durable bound")
        metrics[metric.name] = metric.value
    return metrics


def _metric_delta(metrics: dict[str, int]) -> WorkMetricDelta:
    return WorkMetricDelta(
        records_read=metrics["records_read"],
        records_written=metrics["records_written"],
        records_quarantined=metrics["records_quarantined"],
        bytes_read=metrics["bytes_read"],
        bytes_written=metrics["bytes_written"],
    )


class DurableResultCommitFactory:
    """Compile one validated ``CommitIntent`` into its Phase 6 command.

    The factory is stateless: every command field is derived from the
    rebased intent so the transactional adapter's fencing checks and the
    writer's own dispatch validation see exactly the same facts.  A
    successful outcome compiles to ``CommitWorkWithCheckpoint`` when the
    result proposes a checkpoint; every other outcome compiles to
    ``CommitWorkAttempt`` with its exact failure classification.
    """

    __slots__ = ("_correlation_id",)

    def __init__(self, *, correlation_id: str | None = None) -> None:
        self._correlation_id = _require_correlation_id(correlation_id)

    def __repr__(self) -> str:
        return "DurableResultCommitFactory(stateless=True)"

    def build(self, intent: CommitIntent, /) -> CommitWorkAttempt | CommitWorkWithCheckpoint:
        """Return the closed writer command for one validated intent."""
        if type(intent) is not CommitIntent:
            raise TypeError("result commit factory accepts only CommitIntent")
        classification = _require_classification(intent)
        target = _OUTCOME_TARGET_STATES.get(intent.outcome)
        if target is None:
            raise ResultValidationRejection("result outcome is not commit table")
        finished_at = _from_micros(intent.observed_at_micros, "result observation time")
        started_at = _from_micros(intent.started_at_micros, "attempt start time")
        expires_at = _from_micros(intent.lease_expires_at_micros, "lease expiry")
        if started_at > finished_at:
            raise ResultValidationRejection("attempt start follows its observation")
        if finished_at > expires_at:
            raise ResultValidationRejection("result observation follows lease expiry")
        retry_available_at: UtcTimestamp | None = None
        if target is WorkItemState.RETRY_WAIT:
            retry_available_at = _from_micros(
                intent.retry_eligible_at_micros,
                "retry eligibility",
            )
            if retry_available_at < finished_at:
                raise ResultValidationRejection("retry eligibility precedes the result")
        metrics = _metric_map(intent.result)
        claim = WorkClaim(
            work_item_id=WorkItemId(intent.work_item_id),
            attempt_number=AttemptNumber(_attempt_number(intent.attempt_number)),
            lease_owner=intent.lease_owner,
            row_version=intent.lease_fence,
            started_at=started_at,
            lease_expires_at=expires_at,
            runner_kind=intent.runner_kind,
            worker_identity=intent.worker_identity,
        )
        checkpoint_proposed = target is WorkItemState.SUCCEEDED and intent.checkpoint_proposed
        artifact_id = intent.artifact_ids[0] if intent.artifact_ids else None
        completion = WorkCompletion(
            target_state=target,
            finished_at=finished_at,
            retry_available_at=retry_available_at,
            failure_classification=classification,
            redacted_detail=intent.result.failure_detail,
            result_reference=None,
            records_processed=metrics["records_read"],
            bytes_processed=metrics["bytes_read"],
        )
        event = _completion_event(
            intent,
            claim=claim,
            target=target,
            classification=classification,
            retry_available_at=retry_available_at,
            checkpoint_proposed=checkpoint_proposed,
            artifact_id=artifact_id,
            finished_at=finished_at,
            correlation_id=self._correlation_id,
        )
        run_id = RunId(intent.run_id)
        node_id = NodeId(intent.node_id)
        if target is WorkItemState.SUCCEEDED:
            if not checkpoint_proposed:
                raise ResultValidationRejection(
                    "successful results must propose a durable checkpoint"
                )
            return CommitWorkWithCheckpoint(
                run_id=run_id,
                node_id=node_id,
                claim=claim,
                completion=completion,
                checkpoint=CheckpointWrite(
                    expected_partition_key=PartitionKey(intent.partition_key),
                    payload_schema_version=_CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
                    source_cursor=None,
                    output_position=None,
                    artifact_id=ArtifactId(artifact_id) if artifact_id is not None else None,
                    committed_at=finished_at,
                ),
                metrics=_metric_delta(metrics),
                expected_node_row_version=intent.expected_node_row_version,
                expected_run_row_version=intent.expected_run_row_version,
                event=event,
            )
        return CommitWorkAttempt(
            run_id=run_id,
            node_id=node_id,
            claim=claim,
            completion=completion,
            metrics=_metric_delta(metrics),
            expected_node_row_version=intent.expected_node_row_version,
            expected_run_row_version=intent.expected_run_row_version,
            event=event,
        )


def _attempt_number(value: object) -> int:
    if type(value) is not int or not 1 <= value <= MAX_METRIC_VALUE:
        raise ResultValidationRejection("attempt number is outside the supported range")
    return value


def _completion_event(
    intent: CommitIntent,
    *,
    claim: WorkClaim,
    target: WorkItemState,
    classification: FailureClassification | None,
    retry_available_at: UtcTimestamp | None,
    checkpoint_proposed: bool,
    artifact_id: str | None,
    finished_at: UtcTimestamp,
    correlation_id: str | None,
) -> EventAppendRequest:
    payload: dict[str, object] = {
        "attempt_number": int(claim.attempt_number),
        "failure_classification": None if classification is None else classification.value,
        "node_id": intent.node_id,
        "retry_available_at": None if retry_available_at is None else str(retry_available_at),
        "runner_kind": claim.runner_kind,
        "target_state": target.value,
    }
    if checkpoint_proposed:
        payload.update(
            {
                "artifact_id": artifact_id,
                "checkpoint_payload_schema_version": _CHECKPOINT_PAYLOAD_SCHEMA_VERSION,
                "partition_key": intent.partition_key,
            }
        )
    return EventAppendRequest(
        EventSequence(intent.next_event_sequence),
        intent.event_counter_row_version,
        PendingExecutionEvent(
            event_kind="checkpoint_committed" if checkpoint_proposed else f"work_{target.value}",
            occurred_at=finished_at,
            subject_kind=EventSubjectKind.WORK_ITEM,
            subject_id=claim.work_item_id,
            correlation_id=correlation_id,
            payload_schema_version=WORK_RESULT_EVENT_PAYLOAD_SCHEMA_VERSION,
            payload=RedactedDocument.from_mapping(payload),
        ),
    )


__all__ = ["DurableResultCommitFactory"]
