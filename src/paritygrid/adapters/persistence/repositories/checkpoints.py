"""SQLAlchemy repository for atomic checkpoint frontiers and history."""

from dataclasses import dataclass
from typing import NoReturn, cast

from sqlalchemy import func, insert, select, update
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement

from paritygrid.adapters.persistence.repositories.consistency_common import (
    encode_document,
    positive_int,
    require_artifact_id,
    require_checkpoint_version,
    require_document,
    require_node_id,
    require_partition_key,
    require_run_id,
    require_timestamp,
    stored_timestamp,
    translate_consistency_storage_errors,
)
from paritygrid.adapters.persistence.repositories.consistency_mapping import (
    artifact_key_from_row,
    checkpoint_from_row,
    checkpoint_head_from_row,
    updated_work_checkpoint_from_row,
)
from paritygrid.adapters.persistence.schema import (
    artifact_manifests,
    checkpoint_heads,
    checkpoints,
    work_items,
)
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    ZERO_CHECKPOINT_VERSION,
    CheckpointCommit,
    CheckpointConflictError,
    CheckpointHeadRecord,
    CheckpointPage,
    CheckpointRecord,
    CheckpointRepository,
    CheckpointVersion,
    ConsistencyCorruptionError,
    ConsistencyInvalidRequestError,
    ConsistencyRecordNotFoundError,
    ConsistencyStaleRowVersionError,
    UpdatedWorkCheckpoint,
    validate_consistency_page_limit,
)
from paritygrid.domain.models import ArtifactId, NodeId, RunId, UtcTimestamp
from paritygrid.domain.pipeline import PartitionKey


@dataclass(frozen=True, slots=True)
class _CheckpointState:
    head: CheckpointHeadRecord
    work: UpdatedWorkCheckpoint
    work_updated_at: UtcTimestamp
    work_created_at: UtcTimestamp


class SqlAlchemyCheckpointRepository(CheckpointRepository):
    """Advance checkpoint and work frontiers in a caller-owned transaction."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @translate_consistency_storage_errors
    def get_head(
        self, run_id: RunId, node_id: NodeId, partition_key: PartitionKey
    ) -> CheckpointHeadRecord | None:
        self._require_transaction()
        key = _require_key(run_id, node_id, partition_key)
        state = _load_checkpoint_state(self._session, key)
        return None if state is None else state.head

    @translate_consistency_storage_errors
    def get(
        self,
        run_id: RunId,
        node_id: NodeId,
        partition_key: PartitionKey,
        version: CheckpointVersion,
    ) -> CheckpointRecord | None:
        self._require_transaction()
        key = _require_key(run_id, node_id, partition_key)
        requested = require_checkpoint_version(version)
        if int(requested) == 0:
            raise ConsistencyInvalidRequestError("checkpoint history version must be positive")
        state = _load_checkpoint_state(self._session, key)
        if state is None:
            raise ConsistencyRecordNotFoundError("checkpoint parent does not exist")
        row = (
            self._session.execute(
                select(checkpoints).where(*_history_key_conditions(key, requested))
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        record = checkpoint_from_row(row)
        _validate_checkpoint_identity(record, key)
        _validate_checkpoint_artifacts(self._session, (record,))
        return record

    @translate_consistency_storage_errors
    def list_history(
        self,
        run_id: RunId,
        node_id: NodeId,
        partition_key: PartitionKey,
        *,
        limit: int,
        after: CheckpointVersion = ZERO_CHECKPOINT_VERSION,
    ) -> CheckpointPage:
        self._require_transaction()
        key = _require_key(run_id, node_id, partition_key)
        page_limit = validate_consistency_page_limit(limit)
        cursor = require_checkpoint_version(after)
        state = _load_checkpoint_state(self._session, key)
        if state is None:
            raise ConsistencyRecordNotFoundError("checkpoint parent does not exist")
        rows = (
            self._session.execute(
                select(checkpoints)
                .where(
                    *_history_key_conditions(key),
                    checkpoints.c.version > int(cursor),
                )
                .order_by(checkpoints.c.version)
                .limit(page_limit + 1)
            )
            .mappings()
            .all()
        )
        records = tuple(checkpoint_from_row(row) for row in rows[:page_limit])
        for record in records:
            _validate_checkpoint_identity(record, key)
        _validate_checkpoint_artifacts(self._session, records)
        next_cursor = records[-1].version if len(rows) > page_limit else None
        return CheckpointPage(records, next_cursor)

    @translate_consistency_storage_errors
    def append(
        self,
        run_id: RunId,
        node_id: NodeId,
        partition_key: PartitionKey,
        *,
        expected_current_version: CheckpointVersion,
        expected_head_row_version: int,
        expected_work_row_version: int,
        payload_schema_version: int,
        source_cursor: ConfigurationDocument | None,
        output_position: ConfigurationDocument | None,
        artifact_id: ArtifactId | None,
        committed_at: UtcTimestamp,
    ) -> CheckpointCommit:
        self._require_transaction()
        key = _require_key(run_id, node_id, partition_key)
        expected_version = require_checkpoint_version(expected_current_version)
        expected_head_row = positive_int(expected_head_row_version, "expected head row version")
        expected_work_row = positive_int(expected_work_row_version, "expected work row version")
        schema_version = positive_int(payload_schema_version, "checkpoint payload schema version")
        committed = require_timestamp(committed_at, "checkpoint commit time")
        source_json = (
            None
            if source_cursor is None
            else encode_document(
                require_document(source_cursor, "source cursor"), "source cursor"
            ).text
        )
        output_json = (
            None
            if output_position is None
            else encode_document(
                require_document(output_position, "output position"), "output position"
            ).text
        )
        artifact = None if artifact_id is None else require_artifact_id(artifact_id)
        next_version = expected_version.next()
        state = _load_checkpoint_state(self._session, key)
        if state is None:
            raise ConsistencyRecordNotFoundError("checkpoint parent does not exist")
        requested = _RequestedCheckpoint(
            key,
            next_version,
            schema_version,
            source_json,
            output_json,
            artifact,
            committed,
        )
        if state.head.current_version == next_version:
            return self._replay_append(
                state,
                requested,
                expected_head_row=expected_head_row,
                expected_work_row=expected_work_row,
            )
        _require_expected_frontier(
            state,
            expected_version=expected_version,
            expected_head_row=expected_head_row,
            expected_work_row=expected_work_row,
        )
        if committed < state.head.updated_at or committed < state.work_updated_at:
            raise ConsistencyInvalidRequestError("checkpoint commit time is not monotonic")
        if artifact is not None:
            _require_checkpoint_artifact(self._session, key, artifact, committed)
        head_row = (
            self._session.execute(
                update(checkpoint_heads)
                .where(
                    *_head_key_conditions(key),
                    checkpoint_heads.c.current_version == int(expected_version),
                    checkpoint_heads.c.row_version == expected_head_row,
                )
                .values(
                    current_version=int(next_version),
                    updated_at=str(committed),
                    row_version=expected_head_row + 1,
                )
                .returning(*checkpoint_heads.c)
            )
            .mappings()
            .one_or_none()
        )
        if head_row is None:
            self._raise_head_cas_failure(key, expected_version, expected_head_row)
        work_row = (
            self._session.execute(
                update(work_items)
                .where(
                    *_work_key_conditions(key),
                    work_items.c.expected_checkpoint_version == int(expected_version),
                    work_items.c.row_version == expected_work_row,
                )
                .values(
                    expected_checkpoint_version=int(next_version),
                    row_version=expected_work_row + 1,
                    updated_at=str(committed),
                )
                .returning(*work_items.c)
            )
            .mappings()
            .one_or_none()
        )
        if work_row is None:
            self._raise_work_cas_failure(key, expected_version, expected_work_row)
        history_row = (
            self._session.execute(
                insert(checkpoints)
                .values(
                    run_id=str(key[0]),
                    node_id=str(key[1]),
                    partition_key=str(key[2]),
                    version=int(next_version),
                    payload_schema_version=schema_version,
                    source_cursor_json=source_json,
                    output_position_json=output_json,
                    artifact_id=None if artifact is None else str(artifact),
                    committed_at=str(committed),
                )
                .returning(*checkpoints.c)
            )
            .mappings()
            .one()
        )
        head = checkpoint_head_from_row(head_row)
        work = updated_work_checkpoint_from_row(work_row)
        checkpoint = checkpoint_from_row(history_row)
        _validate_commit_result(head, work, checkpoint, key, next_version)
        return CheckpointCommit(head, checkpoint, work)

    def _replay_append(
        self,
        state: _CheckpointState,
        requested: _RequestedCheckpoint,
        *,
        expected_head_row: int,
        expected_work_row: int,
    ) -> CheckpointCommit:
        if (
            state.head.row_version != expected_head_row + 1
            or state.work.row_version != expected_work_row + 1
            or state.work.expected_checkpoint_version != requested.version
        ):
            raise ConsistencyStaleRowVersionError("checkpoint append frontier is stale")
        row = (
            self._session.execute(
                select(checkpoints).where(
                    *_history_key_conditions(requested.key, requested.version)
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise ConsistencyCorruptionError("checkpoint frontier history is missing")
        checkpoint = checkpoint_from_row(row)
        if not requested.matches(checkpoint):
            raise CheckpointConflictError("checkpoint append conflicts with durable history")
        _validate_checkpoint_artifacts(self._session, (checkpoint,))
        return CheckpointCommit(state.head, checkpoint, state.work)

    def _raise_head_cas_failure(
        self,
        key: tuple[RunId, NodeId, PartitionKey],
        expected_version: CheckpointVersion,
        expected_row: int,
    ) -> NoReturn:
        state = _load_checkpoint_state(self._session, key)
        if state is None:
            raise ConsistencyRecordNotFoundError("checkpoint parent does not exist")
        if state.head.row_version != expected_row:
            raise ConsistencyStaleRowVersionError("checkpoint head row version is stale")
        if state.head.current_version != expected_version:
            raise CheckpointConflictError("checkpoint frontier has changed")
        raise CheckpointConflictError("checkpoint head update was rejected")

    def _raise_work_cas_failure(
        self,
        key: tuple[RunId, NodeId, PartitionKey],
        expected_version: CheckpointVersion,
        expected_row: int,
    ) -> NoReturn:
        row = (
            self._session.execute(select(work_items).where(*_work_key_conditions(key)))
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise ConsistencyRecordNotFoundError("checkpoint work item does not exist")
        work = updated_work_checkpoint_from_row(row)
        if work.row_version != expected_row:
            raise ConsistencyStaleRowVersionError("work-item row version is stale")
        if work.expected_checkpoint_version != expected_version:
            raise CheckpointConflictError("work-item checkpoint frontier has changed")
        raise CheckpointConflictError("work-item checkpoint update was rejected")

    def _require_transaction(self) -> None:
        if not self._session.in_transaction():
            raise ConsistencyInvalidRequestError("repository requires a caller-owned transaction")


@dataclass(frozen=True, slots=True)
class _RequestedCheckpoint:
    key: tuple[RunId, NodeId, PartitionKey]
    version: CheckpointVersion
    payload_schema_version: int
    source_cursor_json: str | None
    output_position_json: str | None
    artifact_id: ArtifactId | None
    committed_at: UtcTimestamp

    def matches(self, record: CheckpointRecord) -> bool:
        source = (
            None
            if record.source_cursor is None
            else encode_document(record.source_cursor, "stored source cursor").text
        )
        output = (
            None
            if record.output_position is None
            else encode_document(record.output_position, "stored output position").text
        )
        return (
            (record.run_id, record.node_id, record.partition_key) == self.key
            and record.version == self.version
            and record.payload_schema_version == self.payload_schema_version
            and source == self.source_cursor_json
            and output == self.output_position_json
            and record.artifact_id == self.artifact_id
            and record.committed_at == self.committed_at
        )


def _require_key(
    run_id: object, node_id: object, partition_key: object
) -> tuple[RunId, NodeId, PartitionKey]:
    return (
        require_run_id(run_id),
        require_node_id(node_id),
        require_partition_key(partition_key),
    )


def _load_checkpoint_state(
    session: Session, key: tuple[RunId, NodeId, PartitionKey]
) -> _CheckpointState | None:
    work_row = (
        session.execute(select(work_items).where(*_work_key_conditions(key)))
        .mappings()
        .one_or_none()
    )
    head_row = (
        session.execute(select(checkpoint_heads).where(*_head_key_conditions(key)))
        .mappings()
        .one_or_none()
    )
    if work_row is None and head_row is None:
        return None
    if work_row is None or head_row is None:
        raise ConsistencyCorruptionError("checkpoint parent relationship is incomplete")
    work = updated_work_checkpoint_from_row(work_row)
    head = checkpoint_head_from_row(head_row)
    if (work.run_id, work.node_id, work.partition_key) != key:
        raise ConsistencyCorruptionError("checkpoint work identity is corrupt")
    if (head.run_id, head.node_id, head.partition_key) != key:
        raise ConsistencyCorruptionError("checkpoint head identity is corrupt")
    if work.expected_checkpoint_version != head.current_version:
        raise ConsistencyCorruptionError("checkpoint and work frontiers diverge")
    work_created = stored_timestamp(work_row["created_at"], "work creation time")
    work_updated = stored_timestamp(work_row["updated_at"], "work update time")
    if head.updated_at < work_created or head.updated_at > work_updated:
        raise ConsistencyCorruptionError("checkpoint parent chronology is corrupt")
    _validate_history_frontier(session, key, head.current_version)
    return _CheckpointState(head, work, work_updated, work_created)


def _validate_history_frontier(
    session: Session,
    key: tuple[RunId, NodeId, PartitionKey],
    current_version: CheckpointVersion,
) -> None:
    count, minimum, maximum = session.execute(
        select(
            func.count(checkpoints.c.version),
            func.min(checkpoints.c.version),
            func.max(checkpoints.c.version),
        ).where(*_history_key_conditions(key))
    ).one()
    count_value = cast(int, count)
    if int(current_version) == 0:
        if count_value != 0 or minimum is not None or maximum is not None:
            raise ConsistencyCorruptionError("checkpoint history exceeds its frontier")
        return
    if count_value != int(current_version) or minimum != 1 or maximum != int(current_version):
        raise ConsistencyCorruptionError("checkpoint history is not contiguous")


def _require_expected_frontier(
    state: _CheckpointState,
    *,
    expected_version: CheckpointVersion,
    expected_head_row: int,
    expected_work_row: int,
) -> None:
    if state.head.row_version != expected_head_row:
        raise ConsistencyStaleRowVersionError("checkpoint head row version is stale")
    if state.work.row_version != expected_work_row:
        raise ConsistencyStaleRowVersionError("work-item row version is stale")
    if state.head.current_version != expected_version:
        raise CheckpointConflictError("checkpoint frontier has changed")


def _require_checkpoint_artifact(
    session: Session,
    key: tuple[RunId, NodeId, PartitionKey],
    artifact_id: ArtifactId,
    committed_at: UtcTimestamp,
) -> None:
    row = (
        session.execute(
            select(artifact_manifests).where(artifact_manifests.c.artifact_id == str(artifact_id))
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise ConsistencyRecordNotFoundError("checkpoint artifact does not exist")
    artifact, run_id, node_id, partition_key, created_at = artifact_key_from_row(row)
    if (artifact, run_id, node_id, partition_key) != (artifact_id, *key):
        raise CheckpointConflictError("checkpoint artifact belongs to another work partition")
    if created_at > committed_at:
        raise ConsistencyInvalidRequestError("checkpoint cannot precede its artifact")


def _validate_checkpoint_artifacts(session: Session, records: tuple[CheckpointRecord, ...]) -> None:
    artifact_ids = tuple(
        sorted({str(record.artifact_id) for record in records if record.artifact_id is not None})
    )
    if not artifact_ids:
        return
    rows = (
        session.execute(
            select(artifact_manifests).where(artifact_manifests.c.artifact_id.in_(artifact_ids))
        )
        .mappings()
        .all()
    )
    relationships = {
        str(relationship[0]): relationship for relationship in map(artifact_key_from_row, rows)
    }
    if set(relationships) != set(artifact_ids):
        raise ConsistencyCorruptionError("checkpoint artifact relationship is missing")
    for record in records:
        if record.artifact_id is None:
            continue
        artifact, run_id, node_id, partition_key, created_at = relationships[
            str(record.artifact_id)
        ]
        if (artifact, run_id, node_id, partition_key) != (
            record.artifact_id,
            record.run_id,
            record.node_id,
            record.partition_key,
        ):
            raise ConsistencyCorruptionError("checkpoint artifact relationship is corrupt")
        if created_at > record.committed_at:
            raise ConsistencyCorruptionError("checkpoint artifact chronology is corrupt")


def _validate_checkpoint_identity(
    record: CheckpointRecord, key: tuple[RunId, NodeId, PartitionKey]
) -> None:
    if (record.run_id, record.node_id, record.partition_key) != key:
        raise ConsistencyCorruptionError("checkpoint history identity is corrupt")


def _validate_commit_result(
    head: CheckpointHeadRecord,
    work: UpdatedWorkCheckpoint,
    checkpoint: CheckpointRecord,
    key: tuple[RunId, NodeId, PartitionKey],
    version: CheckpointVersion,
) -> None:
    if (
        (head.run_id, head.node_id, head.partition_key) != key
        or (work.run_id, work.node_id, work.partition_key) != key
        or (checkpoint.run_id, checkpoint.node_id, checkpoint.partition_key) != key
        or head.current_version != version
        or work.expected_checkpoint_version != version
        or checkpoint.version != version
    ):
        raise ConsistencyCorruptionError("checkpoint append result is corrupt")


def _head_key_conditions(
    key: tuple[RunId, NodeId, PartitionKey],
) -> tuple[ColumnElement[bool], ColumnElement[bool], ColumnElement[bool]]:
    return (
        checkpoint_heads.c.run_id == str(key[0]),
        checkpoint_heads.c.node_id == str(key[1]),
        checkpoint_heads.c.partition_key == str(key[2]),
    )


def _work_key_conditions(
    key: tuple[RunId, NodeId, PartitionKey],
) -> tuple[ColumnElement[bool], ColumnElement[bool], ColumnElement[bool]]:
    return (
        work_items.c.run_id == str(key[0]),
        work_items.c.node_id == str(key[1]),
        work_items.c.partition_key == str(key[2]),
    )


def _history_key_conditions(
    key: tuple[RunId, NodeId, PartitionKey], version: CheckpointVersion | None = None
) -> tuple[ColumnElement[bool], ...]:
    conditions: tuple[ColumnElement[bool], ...] = (
        checkpoints.c.run_id == str(key[0]),
        checkpoints.c.node_id == str(key[1]),
        checkpoints.c.partition_key == str(key[2]),
    )
    return conditions if version is None else (*conditions, checkpoints.c.version == int(version))
