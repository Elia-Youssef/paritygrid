"""Strict row mapping for checkpoint, event, and idempotency storage."""

from dataclasses import dataclass
from typing import cast

from sqlalchemy.engine import RowMapping

from paritygrid.adapters.persistence.repositories.consistency_common import (
    decode_optional_document,
    decode_redacted_document,
    stored_artifact_id,
    stored_event_kind,
    stored_idempotency_scope,
    stored_node_id,
    stored_nonnegative_int,
    stored_optional_artifact_id,
    stored_optional_timestamp,
    stored_partition_key,
    stored_portable_identity,
    stored_positive_int,
    stored_run_id,
    stored_timestamp,
    stored_work_item_id,
)
from paritygrid.adapters.persistence.values import Sha256Digest
from paritygrid.application.ports.consistency import (
    CheckpointHeadRecord,
    CheckpointRecord,
    CheckpointVersion,
    ConsistencyCorruptionError,
    EventSequence,
    EventSubjectKind,
    ExecutionEventRecord,
    IdempotencyRecord,
    IdempotencyStatus,
    UpdatedWorkCheckpoint,
)
from paritygrid.domain.models import ArtifactId, NodeId, RunId, UtcTimestamp
from paritygrid.domain.pipeline import PartitionKey


@dataclass(frozen=True, slots=True)
class EventCounterState:
    """Validated storage state for one run's event frontier."""

    run_id: RunId
    next_sequence: EventSequence
    row_version: int


@dataclass(frozen=True, slots=True)
class StoredIdempotencyRecord:
    """Public idempotency record paired with its internal request digest."""

    record: IdempotencyRecord
    request_sha256: str


def checkpoint_head_from_row(row: RowMapping) -> CheckpointHeadRecord:
    try:
        return CheckpointHeadRecord(
            run_id=stored_run_id(row["run_id"]),
            node_id=stored_node_id(row["node_id"]),
            partition_key=stored_partition_key(row["partition_key"]),
            current_version=CheckpointVersion(
                stored_nonnegative_int(row["current_version"], "checkpoint current version")
            ),
            updated_at=stored_timestamp(row["updated_at"], "checkpoint update time"),
            row_version=stored_positive_int(row["row_version"], "checkpoint row version"),
        )
    except ConsistencyCorruptionError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise ConsistencyCorruptionError("checkpoint head is corrupt") from error


def checkpoint_from_row(row: RowMapping) -> CheckpointRecord:
    try:
        version = CheckpointVersion(
            stored_positive_int(row["version"], "checkpoint history version")
        )
        return CheckpointRecord(
            run_id=stored_run_id(row["run_id"]),
            node_id=stored_node_id(row["node_id"]),
            partition_key=stored_partition_key(row["partition_key"]),
            version=version,
            payload_schema_version=stored_positive_int(
                row["payload_schema_version"], "checkpoint payload schema version"
            ),
            source_cursor=decode_optional_document(
                row["source_cursor_json"], "checkpoint source cursor"
            ),
            output_position=decode_optional_document(
                row["output_position_json"], "checkpoint output position"
            ),
            artifact_id=stored_optional_artifact_id(row["artifact_id"]),
            committed_at=stored_timestamp(row["committed_at"], "checkpoint commit time"),
        )
    except ConsistencyCorruptionError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise ConsistencyCorruptionError("checkpoint history record is corrupt") from error


def updated_work_checkpoint_from_row(row: RowMapping) -> UpdatedWorkCheckpoint:
    try:
        return UpdatedWorkCheckpoint(
            work_item_id=stored_work_item_id(row["work_item_id"]),
            run_id=stored_run_id(row["run_id"]),
            node_id=stored_node_id(row["node_id"]),
            partition_key=stored_partition_key(row["partition_key"]),
            expected_checkpoint_version=CheckpointVersion(
                stored_nonnegative_int(
                    row["expected_checkpoint_version"], "work checkpoint version"
                )
            ),
            row_version=stored_positive_int(row["row_version"], "work-item row version"),
        )
    except ConsistencyCorruptionError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise ConsistencyCorruptionError("work checkpoint state is corrupt") from error


def event_counter_from_row(row: RowMapping) -> EventCounterState:
    try:
        return EventCounterState(
            run_id=stored_run_id(row["run_id"]),
            next_sequence=EventSequence(
                stored_positive_int(row["next_sequence_number"], "next event sequence")
            ),
            row_version=stored_positive_int(row["row_version"], "event counter row version"),
        )
    except ConsistencyCorruptionError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise ConsistencyCorruptionError("event counter is corrupt") from error


def execution_event_from_row(row: RowMapping) -> ExecutionEventRecord:
    try:
        kind_value = cast(object, row["subject_kind"])
        if type(kind_value) is not str:
            raise TypeError
        subject_kind = EventSubjectKind(kind_value)
        subject_value = row["subject_id"]
        subject_id = (
            stored_run_id(subject_value)
            if subject_kind is EventSubjectKind.RUN
            else stored_work_item_id(subject_value)
        )
        return ExecutionEventRecord(
            run_id=stored_run_id(row["run_id"]),
            sequence=EventSequence(stored_positive_int(row["sequence_number"], "event sequence")),
            event_kind=stored_event_kind(row["event_kind"]),
            occurred_at=stored_timestamp(row["occurred_at"], "event occurrence time"),
            subject_kind=subject_kind,
            subject_id=subject_id,
            correlation_id=(
                None
                if row["correlation_id"] is None
                else stored_portable_identity(row["correlation_id"], "correlation identifier", 96)
            ),
            payload_schema_version=stored_positive_int(
                row["payload_schema_version"], "event payload schema version"
            ),
            payload=decode_redacted_document(row["payload_json"], "event payload"),
        )
    except ConsistencyCorruptionError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise ConsistencyCorruptionError("execution event is corrupt") from error


def stored_idempotency_from_row(row: RowMapping) -> StoredIdempotencyRecord:
    try:
        status_value = cast(object, row["status"])
        digest_value = cast(object, row["request_sha256"])
        if type(status_value) is not str or type(digest_value) is not str:
            raise TypeError
        status = IdempotencyStatus(status_value)
        digest = Sha256Digest(digest_value).value
        schema_value = row["response_schema_version"]
        response_value = row["response_json"]
        completed_at = stored_optional_timestamp(row["completed_at"], "idempotency completion time")
        created_at = stored_timestamp(row["created_at"], "idempotency creation time")
        updated_at = stored_timestamp(row["updated_at"], "idempotency update time")
        if updated_at < created_at:
            raise ConsistencyCorruptionError("idempotency chronology is corrupt")
        if status is IdempotencyStatus.IN_PROGRESS:
            if schema_value is not None or response_value is not None or completed_at is not None:
                raise ConsistencyCorruptionError("in-progress idempotency record is corrupt")
            if updated_at != created_at:
                raise ConsistencyCorruptionError("in-progress idempotency chronology is corrupt")
            schema_version = None
            response = None
        else:
            schema_version = stored_positive_int(
                schema_value, "idempotency response schema version"
            )
            response = decode_redacted_document(response_value, "idempotency response")
            if completed_at is None or updated_at != completed_at or completed_at < created_at:
                raise ConsistencyCorruptionError("terminal idempotency chronology is corrupt")
        return StoredIdempotencyRecord(
            record=IdempotencyRecord(
                scope=stored_idempotency_scope(row["scope"]),
                key=stored_portable_identity(row["idempotency_key"], "idempotency key", 128),
                status=status,
                response_schema_version=schema_version,
                response=response,
                created_at=created_at,
                updated_at=updated_at,
                completed_at=completed_at,
            ),
            request_sha256=digest,
        )
    except ConsistencyCorruptionError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise ConsistencyCorruptionError("idempotency record is corrupt") from error


def artifact_key_from_row(
    row: RowMapping,
) -> tuple[ArtifactId, RunId, NodeId, PartitionKey, UtcTimestamp]:
    """Map the exact checkpoint artifact relationship and creation time."""
    try:
        return (
            stored_artifact_id(row["artifact_id"]),
            stored_run_id(row["run_id"]),
            stored_node_id(row["node_id"]),
            stored_partition_key(row["partition_key"]),
            stored_timestamp(row["created_at"], "artifact creation time"),
        )
    except ConsistencyCorruptionError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise ConsistencyCorruptionError("checkpoint artifact relationship is corrupt") from error
