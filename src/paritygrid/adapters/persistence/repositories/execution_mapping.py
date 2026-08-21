"""Strict stored-row mapping for execution repositories."""

from sqlalchemy.engine import RowMapping

from paritygrid.adapters.persistence.repositories.execution_common import (
    MAX_PERSISTED_INTEGER,
    MAX_SQLITE_INTEGER,
    bounded_text,
    decode_execution_document,
    decode_optional_execution_document,
)
from paritygrid.adapters.persistence.values import RunNodeState, WorkAttemptOutcome
from paritygrid.application.ports.execution import (
    AttemptOutcome,
    ExecutionCorruptionError,
    ExecutionInvalidRequestError,
    RunEventCounterRecord,
    RunNodeRecord,
    RunNodeStatus,
    RunRecord,
    WorkAttemptRecord,
    WorkItemRecord,
)
from paritygrid.domain.execution import FailureClassification, RunState, WorkItemState
from paritygrid.domain.models import (
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


def run_from_row(row: RowMapping) -> RunRecord:
    """Map one run and verify lifecycle timestamp coherence."""
    try:
        state = RunState(stored_text(row["state"], "run state", 32))
        record = RunRecord(
            run_id=stored_run_id(row["run_id"]),
            pipeline_id=stored_pipeline_id(row["pipeline_id"]),
            pipeline_version=PipelineVersion(
                stored_positive_int(row["pipeline_version_number"], "pipeline version")
            ),
            runner_kind=stored_text(row["runner_kind"], "runner kind", 32),
            runner_configuration=decode_execution_document(
                row["runner_configuration_json"], "runner configuration"
            ),
            state=state,
            row_version=stored_positive_int(row["row_version"], "run row version"),
            scenario_seed=stored_optional_sqlite_int(row["scenario_seed"], "scenario seed"),
            created_at=stored_timestamp(row["created_at"], "run creation time"),
            started_at=stored_optional_timestamp(row["started_at"], "run start time"),
            finished_at=stored_optional_timestamp(row["finished_at"], "run finish time"),
            cancellation_requested_at=stored_optional_timestamp(
                row["cancellation_requested_at"], "run cancellation time"
            ),
            recovery_started_at=stored_optional_timestamp(
                row["recovery_started_at"], "run recovery start time"
            ),
            recovered_at=stored_optional_timestamp(row["recovered_at"], "run recovery time"),
            execution_evidence_fingerprint=stored_optional_fingerprint(
                _stored_execution_evidence_fingerprint(row)
            ),
            execution_evidence_fingerprint_version=_stored_execution_evidence_version(row),
        )
        _validate_run_chronology(record)
        return record
    except ExecutionCorruptionError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise ExecutionCorruptionError("run record is corrupt") from error


def run_event_counter_from_row(row: RowMapping) -> RunEventCounterRecord:
    try:
        return RunEventCounterRecord(
            run_id=stored_run_id(row["run_id"]),
            next_sequence_number=stored_positive_int(row["next_sequence_number"], "event sequence"),
            row_version=stored_positive_int(row["row_version"], "event counter row version"),
        )
    except ExecutionCorruptionError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise ExecutionCorruptionError("run event counter is corrupt") from error


def run_node_from_row(row: RowMapping) -> RunNodeRecord:
    try:
        record = RunNodeRecord(
            run_id=stored_run_id(row["run_id"]),
            node_id=stored_node_id(row["node_id"]),
            status=RunNodeStatus(stored_text(row["state"], "run-node state", 32)),
            row_version=stored_positive_int(row["row_version"], "run-node row version"),
            work_total=stored_nonnegative_int(row["work_total"], "total work"),
            work_pending=stored_nonnegative_int(row["work_pending"], "pending work"),
            work_running=stored_nonnegative_int(row["work_running"], "running work"),
            work_succeeded=stored_nonnegative_int(row["work_succeeded"], "succeeded work"),
            work_quarantined=stored_nonnegative_int(row["work_quarantined"], "quarantined work"),
            work_failed=stored_nonnegative_int(row["work_failed"], "failed work"),
            work_cancelled=stored_nonnegative_int(row["work_cancelled"], "cancelled work"),
            records_read=stored_nonnegative_int(row["records_read"], "records read"),
            records_written=stored_nonnegative_int(row["records_written"], "records written"),
            records_quarantined=stored_nonnegative_int(
                row["records_quarantined"], "records quarantined"
            ),
            bytes_read=stored_nonnegative_int(row["bytes_read"], "bytes read"),
            bytes_written=stored_nonnegative_int(row["bytes_written"], "bytes written"),
            retry_count=stored_nonnegative_int(row["retry_count"], "retry count"),
            duration=Duration(
                stored_nonnegative_int(
                    row["duration_microseconds"],
                    "run-node duration",
                    maximum=Duration.MAX_MICROSECONDS,
                )
            ),
            started_at=stored_optional_timestamp(row["started_at"], "run-node start time"),
            finished_at=stored_optional_timestamp(row["finished_at"], "run-node finish time"),
        )
        completed = (
            record.work_pending
            + record.work_running
            + record.work_succeeded
            + record.work_quarantined
            + record.work_failed
            + record.work_cancelled
        )
        if completed > record.work_total:
            raise ExecutionCorruptionError("run-node work counts are corrupt")
        if record.status is RunNodeStatus.PENDING:
            if record.started_at is not None or record.finished_at is not None:
                raise ExecutionCorruptionError("pending run-node timestamps are corrupt")
        elif record.status is RunNodeStatus.RUNNING:
            if record.started_at is None or record.finished_at is not None:
                raise ExecutionCorruptionError("running run-node timestamps are corrupt")
        elif (
            record.started_at is None
            or record.finished_at is None
            or record.finished_at < record.started_at
        ):
            raise ExecutionCorruptionError("completed run-node timestamps are corrupt")
        return record
    except ExecutionCorruptionError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise ExecutionCorruptionError("run-node record is corrupt") from error


def work_item_from_row(row: RowMapping) -> WorkItemRecord:
    """Map a work item and verify durable claim coherence."""
    try:
        state = WorkItemState(stored_text(row["state"], "work-item state", 32))
        if state is WorkItemState.LEASED:
            raise ExecutionCorruptionError("transient leased state was persisted")
        record = WorkItemRecord(
            work_item_id=stored_work_item_id(row["work_item_id"]),
            run_id=stored_run_id(row["run_id"]),
            node_id=stored_node_id(row["node_id"]),
            partition_key=stored_partition_key(row["partition_key"]),
            state=state,
            row_version=stored_positive_int(row["row_version"], "work-item row version"),
            completed_attempt_count=stored_nonnegative_int(
                row["completed_attempt_count"],
                "completed attempt count",
                maximum=MAX_PERSISTED_INTEGER,
            ),
            expected_checkpoint_version=stored_nonnegative_int(
                row["expected_checkpoint_version"],
                "expected checkpoint version",
                maximum=MAX_PERSISTED_INTEGER,
            ),
            input_reference=decode_optional_execution_document(
                row["input_reference_json"], "work input reference"
            ),
            retry_available_at=stored_optional_timestamp(
                row["retry_available_at"], "retry availability time"
            ),
            lease_owner=stored_optional_text(row["lease_owner"], "lease owner", 128),
            lease_expires_at=stored_optional_timestamp(row["lease_expires_at"], "lease expiry"),
            active_attempt_number=stored_optional_attempt(row["active_attempt_number"]),
            active_attempt_started_at=stored_optional_timestamp(
                row["active_attempt_started_at"], "active attempt start time"
            ),
            active_runner_kind=stored_optional_text(
                row["active_runner_kind"], "active runner kind", 32
            ),
            active_worker_identity=stored_optional_text(
                row["active_worker_identity"], "active worker identity", 128
            ),
            created_at=stored_timestamp(row["created_at"], "work-item creation time"),
            updated_at=stored_timestamp(row["updated_at"], "work-item update time"),
        )
        _validate_work_item_coherence(record)
        return record
    except ExecutionCorruptionError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise ExecutionCorruptionError("work-item record is corrupt") from error


def work_attempt_from_row(row: RowMapping) -> WorkAttemptRecord:
    """Map one immutable completed attempt."""
    try:
        outcome = AttemptOutcome(stored_text(row["outcome"], "attempt outcome", 32))
        classification_value = row["failure_classification"]
        classification = (
            None
            if classification_value is None
            else FailureClassification(
                stored_text(classification_value, "failure classification", 32)
            )
        )
        record = WorkAttemptRecord(
            work_item_id=stored_work_item_id(row["work_item_id"]),
            attempt_number=AttemptNumber(
                stored_positive_int(row["attempt_number"], "attempt number")
            ),
            started_at=stored_timestamp(row["started_at"], "attempt start time"),
            finished_at=stored_timestamp(row["finished_at"], "attempt finish time"),
            runner_kind=stored_text(row["runner_kind"], "attempt runner kind", 32),
            worker_identity=stored_text(row["worker_identity"], "worker identity", 128),
            outcome=outcome,
            failure_classification=classification,
            redacted_detail=stored_optional_text(row["redacted_detail"], "attempt detail", 4096),
            result_reference=decode_optional_execution_document(
                row["result_reference_json"], "attempt result reference"
            ),
            records_processed=stored_nonnegative_int(row["records_processed"], "records processed"),
            bytes_processed=stored_nonnegative_int(row["bytes_processed"], "bytes processed"),
            duration=Duration(
                stored_nonnegative_int(
                    row["duration_microseconds"],
                    "attempt duration",
                    maximum=Duration.MAX_MICROSECONDS,
                )
            ),
        )
        if record.finished_at < record.started_at:
            raise ExecutionCorruptionError("attempt timestamps are corrupt")
        delta = record.finished_at.to_datetime() - record.started_at.to_datetime()
        actual_duration = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
        if record.duration.microseconds != actual_duration:
            raise ExecutionCorruptionError("attempt duration is corrupt")
        if (record.outcome is AttemptOutcome.SUCCEEDED) != (record.failure_classification is None):
            raise ExecutionCorruptionError("attempt failure classification is corrupt")
        if (
            record.outcome is AttemptOutcome.LEASE_EXPIRED
            and record.failure_classification is not FailureClassification.TIMEOUT
        ):
            raise ExecutionCorruptionError("expired lease classification is corrupt")
        return record
    except ExecutionCorruptionError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise ExecutionCorruptionError("work-attempt record is corrupt") from error


def stored_run_id(value: object) -> RunId:
    return _stored_identifier(value, RunId, "run identifier")


def stored_work_item_id(value: object) -> WorkItemId:
    return _stored_identifier(value, WorkItemId, "work-item identifier")


def stored_node_id(value: object) -> NodeId:
    return _stored_identifier(value, NodeId, "node identifier")


def stored_pipeline_id(value: object) -> PipelineId:
    return _stored_identifier(value, PipelineId, "pipeline identifier")


def stored_partition_key(value: object) -> PartitionKey:
    try:
        if not isinstance(value, str):
            raise TypeError
        return PartitionKey.parse(value)
    except (TypeError, ValueError) as error:
        raise ExecutionCorruptionError("partition key is corrupt") from error


def _stored_identifier[T](value: object, expected: type[T], subject: str) -> T:
    try:
        if not isinstance(value, str):
            raise TypeError
        return expected.parse(value)  # type: ignore[attr-defined, no-any-return]
    except (TypeError, ValueError) as error:
        raise ExecutionCorruptionError(f"{subject} is corrupt") from error


def stored_text(value: object, subject: str, maximum: int) -> str:
    try:
        return bounded_text(value, subject, maximum)
    except ExecutionInvalidRequestError as error:
        raise ExecutionCorruptionError(f"{subject} is corrupt") from error


def stored_optional_text(value: object, subject: str, maximum: int) -> str | None:
    return None if value is None else stored_text(value, subject, maximum)


def stored_positive_int(value: object, subject: str) -> int:
    if type(value) is not int or not 1 <= value <= MAX_PERSISTED_INTEGER:
        raise ExecutionCorruptionError(f"{subject} is corrupt")
    return value


def stored_nonnegative_int(
    value: object, subject: str, *, maximum: int = MAX_SQLITE_INTEGER
) -> int:
    if type(value) is not int or not 0 <= value <= maximum:
        raise ExecutionCorruptionError(f"{subject} is corrupt")
    return value


def stored_optional_sqlite_int(value: object, subject: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int or not -MAX_SQLITE_INTEGER - 1 <= value <= MAX_SQLITE_INTEGER:
        raise ExecutionCorruptionError(f"{subject} is corrupt")
    return value


def stored_timestamp(value: object, subject: str) -> UtcTimestamp:
    try:
        if not isinstance(value, str):
            raise TypeError
        parsed = UtcTimestamp.parse(value)
        if str(parsed) != value:
            raise ValueError
        return parsed
    except (TypeError, ValueError) as error:
        raise ExecutionCorruptionError(f"{subject} is corrupt") from error


def stored_optional_timestamp(value: object, subject: str) -> UtcTimestamp | None:
    return None if value is None else stored_timestamp(value, subject)


def stored_optional_attempt(value: object) -> AttemptNumber | None:
    return (
        None
        if value is None
        else AttemptNumber(stored_positive_int(value, "active attempt number"))
    )


def stored_optional_fingerprint(value: object) -> StateFingerprint | None:
    if value is None:
        return None
    try:
        if not isinstance(value, str):
            raise TypeError
        return StateFingerprint.parse(value)
    except (TypeError, ValueError) as error:
        raise ExecutionCorruptionError("execution-evidence fingerprint is corrupt") from error


EXECUTION_EVIDENCE_FINGERPRINT_STORAGE_VERSION = 2


def _stored_execution_evidence_fingerprint(row: RowMapping) -> object:
    """Read the digest under its current storage name with migration-window fallback."""
    if "execution_evidence_fingerprint" in row:
        return row["execution_evidence_fingerprint"]
    return row["final_reconciliation_fingerprint"]


def _stored_execution_evidence_version(row: RowMapping) -> int | None:
    """Read the explicit digest version, inferring 2 for former-name storage.

    During the migration window a row may still carry the former storage name
    without a version column; a preserved Phase 6 digest is execution-evidence
    version 2 by definition, so the compatibility read infers exactly that.
    """
    if "execution_evidence_fingerprint_version" not in row:
        if "execution_evidence_fingerprint" in row:
            return None
        legacy = row["final_reconciliation_fingerprint"]
        return EXECUTION_EVIDENCE_FINGERPRINT_STORAGE_VERSION if legacy is not None else None
    version = row["execution_evidence_fingerprint_version"]
    if version is None:
        return None
    if type(version) is not int or not 1 <= version <= 2_147_483_647:
        raise ExecutionCorruptionError("execution-evidence fingerprint version is corrupt")
    return version


def _validate_run_chronology(record: RunRecord) -> None:
    times = (
        record.started_at,
        record.finished_at,
        record.cancellation_requested_at,
        record.recovery_started_at,
        record.recovered_at,
    )
    if any(value is not None and value < record.created_at for value in times):
        raise ExecutionCorruptionError("run timestamps are corrupt")
    if (
        record.finished_at is not None
        and record.started_at is not None
        and record.finished_at < record.started_at
    ):
        raise ExecutionCorruptionError("run timestamps are corrupt")
    if record.recovered_at is not None and (
        record.recovery_started_at is None or record.recovered_at < record.recovery_started_at
    ):
        raise ExecutionCorruptionError("run recovery timestamps are corrupt")
    terminal = record.state.is_terminal
    if terminal != (record.finished_at is not None):
        raise ExecutionCorruptionError("run terminal timestamp is corrupt")
    successful = record.state in {RunState.SUCCEEDED, RunState.PARTIALLY_SUCCEEDED}
    if successful != (record.execution_evidence_fingerprint is not None):
        raise ExecutionCorruptionError("run final fingerprint is corrupt")
    version = record.execution_evidence_fingerprint_version
    if version is not None and version != EXECUTION_EVIDENCE_FINGERPRINT_STORAGE_VERSION:
        raise ExecutionCorruptionError("run execution-evidence version is unsupported")
    active_started = {
        RunState.RUNNING,
        RunState.PAUSING,
        RunState.PAUSED,
        RunState.RESUMING,
        RunState.CANCELLING,
        RunState.SUCCEEDED,
        RunState.PARTIALLY_SUCCEEDED,
        RunState.FAILED,
    }
    if record.state in active_started and record.started_at is None:
        raise ExecutionCorruptionError("run start time is corrupt")
    if record.state is RunState.QUEUED and record.started_at is not None:
        raise ExecutionCorruptionError("queued run start time is corrupt")
    cancellation_states = {RunState.CANCELLING, RunState.CANCELLED}
    if (record.state in cancellation_states) != (record.cancellation_requested_at is not None):
        raise ExecutionCorruptionError("run cancellation time is corrupt")
    if (
        record.finished_at is not None
        and record.cancellation_requested_at is not None
        and record.finished_at < record.cancellation_requested_at
    ):
        raise ExecutionCorruptionError("run cancellation chronology is corrupt")


def _validate_work_item_coherence(record: WorkItemRecord) -> None:
    if record.updated_at < record.created_at:
        raise ExecutionCorruptionError("work-item timestamps are corrupt")
    active = (
        record.lease_owner,
        record.lease_expires_at,
        record.active_attempt_number,
        record.active_attempt_started_at,
        record.active_runner_kind,
        record.active_worker_identity,
    )
    if record.state is WorkItemState.RUNNING:
        if any(value is None for value in active):
            raise ExecutionCorruptionError("work-item active claim is corrupt")
        assert record.active_attempt_number is not None
        assert record.active_attempt_started_at is not None
        assert record.lease_expires_at is not None
        if int(record.active_attempt_number) != record.completed_attempt_count + 1:
            raise ExecutionCorruptionError("work-item active attempt is corrupt")
        if record.lease_expires_at <= record.active_attempt_started_at:
            raise ExecutionCorruptionError("work-item lease timestamps are corrupt")
        if record.active_attempt_started_at < record.created_at:
            raise ExecutionCorruptionError("work-item active start time is corrupt")
        if record.active_attempt_started_at > record.updated_at:
            raise ExecutionCorruptionError("work-item active update time is corrupt")
        if record.lease_expires_at <= record.updated_at:
            raise ExecutionCorruptionError("work-item active lease time is corrupt")
    elif any(value is not None for value in active):
        raise ExecutionCorruptionError("inactive work-item claim is corrupt")
    if (record.state is WorkItemState.RETRY_WAIT) != (record.retry_available_at is not None):
        raise ExecutionCorruptionError("work-item retry time is corrupt")
    if record.retry_available_at is not None and record.retry_available_at < record.updated_at:
        raise ExecutionCorruptionError("work-item retry chronology is corrupt")


assert {value.value for value in RunNodeStatus} == {value.value for value in RunNodeState}
assert {value.value for value in AttemptOutcome} == {value.value for value in WorkAttemptOutcome}
