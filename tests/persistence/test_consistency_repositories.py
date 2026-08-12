"""Behavioral tests for checkpoint, event, and idempotency repositories."""

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import event as sqlalchemy_event
from sqlalchemy import func, insert, select
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence import SQLiteDatabase, SQLiteDatabaseConfig
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyCheckpointRepository,
    SqlAlchemyExecutionEventRepository,
    SqlAlchemyIdempotencyRepository,
    SqlAlchemyPipelineRepository,
    SqlAlchemyRunRepository,
    SqlAlchemyWorkItemRepository,
)
from paritygrid.adapters.persistence.repositories.consistency_common import request_digest
from paritygrid.adapters.persistence.schema import (
    artifact_manifests,
    checkpoint_heads,
    checkpoints,
    execution_events,
    idempotency_records,
    run_event_counters,
    work_items,
)
from paritygrid.application.ports import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    CheckpointConflictError,
    CheckpointVersion,
    ConsistencyCorruptionError,
    ConsistencyInvalidRequestError,
    ConsistencyRecordNotFoundError,
    ConsistencyStaleRowVersionError,
    ConsistencyStateConflictError,
    EventSequence,
    EventSequenceConflictError,
    EventSubjectKind,
    IdempotencyBeginDisposition,
    IdempotencyBeginResult,
    IdempotencyConflictError,
    IdempotencyReservation,
    IdempotencyStatus,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    ArtifactId,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

PIPELINE_ID = PipelineId("pip_consistency")
RUN_ID = RunId("run_consistency")
NODE_ID = NodeId("nod_source")
WORK_ID = WorkItemId("wrk_partition")
PARTITION = PartitionKey("page-0001")


@pytest.fixture
def database(tmp_path: Path) -> Iterator[SQLiteDatabase]:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "consistency state %25.db"))
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    try:
        yield database
    finally:
        database.close()


def timestamp(second: int) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 12, 12, 0, second, tzinfo=UTC))


def document(**values: object) -> ConfigurationDocument:
    return ConfigurationDocument.from_mapping(values)


def redacted(**values: object) -> RedactedDocument:
    return RedactedDocument.from_mapping(values)


def reservation(result: IdempotencyBeginResult) -> IdempotencyReservation:
    assert result.reservation is not None
    return result.reservation


def forged_reservation(
    scope: str, key: str, request: ConfigurationDocument, timestamp_value: UtcTimestamp
) -> IdempotencyReservation:
    return IdempotencyReservation(
        scope,
        key,
        request_digest(request),
        timestamp_value,
        timestamp_value,
    )


def seed_execution(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PIPELINE_ID,
            display_name="Consistency pipeline",
            description=None,
            created_at=timestamp(0),
        )
        pipelines.publish_version(
            pipeline_id=PIPELINE_ID,
            expected_latest_version=None,
            specification=document(nodes=[]),
            planner_format_version=1,
            published_at=timestamp(0),
        )
        runs = SqlAlchemyRunRepository(session)
        runs.create(
            run_id=RUN_ID,
            pipeline_id=PIPELINE_ID,
            pipeline_version=PipelineVersion(1),
            runner_kind="threaded",
            runner_configuration=document(max_workers=2),
            scenario_seed=None,
            node_ids=(NODE_ID,),
            created_at=timestamp(1),
        )
        runs.transition(
            RUN_ID,
            expected_row_version=1,
            target_state=RunState.RUNNING,
            transitioned_at=timestamp(2),
        )
        SqlAlchemyWorkItemRepository(session).create(
            work_item_id=WORK_ID,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PARTITION,
            input_reference=document(page=1),
            created_at=timestamp(2),
        )


def event(
    kind: str,
    second: int,
    subject_kind: EventSubjectKind = EventSubjectKind.RUN,
) -> PendingExecutionEvent:
    subject_id: RunId | WorkItemId = RUN_ID if subject_kind is EventSubjectKind.RUN else WORK_ID
    return PendingExecutionEvent(
        event_kind=kind,
        occurred_at=timestamp(second),
        subject_kind=subject_kind,
        subject_id=subject_id,
        correlation_id="corr-consistency",
        payload_schema_version=1,
        payload=redacted(kind=kind),
    )


def test_checkpoint_append_replay_history_and_companion_work_cas(
    database: SQLiteDatabase,
) -> None:
    seed_execution(database)
    with database.transaction() as session:
        repository = SqlAlchemyCheckpointRepository(session)
        initial = repository.get_head(RUN_ID, NODE_ID, PARTITION)
        assert initial is not None
        assert initial.current_version == CheckpointVersion(0)
        first = repository.append(
            RUN_ID,
            NODE_ID,
            PARTITION,
            expected_current_version=CheckpointVersion(0),
            expected_head_row_version=1,
            expected_work_row_version=1,
            payload_schema_version=1,
            source_cursor=document(offset=10),
            output_position=document(rows=9),
            artifact_id=None,
            committed_at=timestamp(3),
        )
        assert first.head.current_version == CheckpointVersion(1)
        assert first.head.row_version == 2
        assert first.work.expected_checkpoint_version == CheckpointVersion(1)
        assert first.work.row_version == 2
        assert first.checkpoint.version == CheckpointVersion(1)
        replay = repository.append(
            RUN_ID,
            NODE_ID,
            PARTITION,
            expected_current_version=CheckpointVersion(0),
            expected_head_row_version=1,
            expected_work_row_version=1,
            payload_schema_version=1,
            source_cursor=document(offset=10),
            output_position=document(rows=9),
            artifact_id=None,
            committed_at=timestamp(3),
        )
        assert replay == first
        second = repository.append(
            RUN_ID,
            NODE_ID,
            PARTITION,
            expected_current_version=CheckpointVersion(1),
            expected_head_row_version=2,
            expected_work_row_version=2,
            payload_schema_version=1,
            source_cursor=document(offset=20),
            output_position=document(rows=19),
            artifact_id=None,
            committed_at=timestamp(4),
        )
        assert second.checkpoint.version == CheckpointVersion(2)
        assert repository.get(RUN_ID, NODE_ID, PARTITION, CheckpointVersion(1)) == first.checkpoint
        page = repository.list_history(
            RUN_ID, NODE_ID, PARTITION, limit=1, after=CheckpointVersion(0)
        )
        assert page.items == (first.checkpoint,)
        assert page.next_cursor == CheckpointVersion(1)
        assert page.next_cursor is not None
        tail = repository.list_history(RUN_ID, NODE_ID, PARTITION, limit=2, after=page.next_cursor)
        assert tail.items == (second.checkpoint,)
        assert tail.next_cursor is None


def test_checkpoint_conflict_stale_and_rollback_are_typed(database: SQLiteDatabase) -> None:
    seed_execution(database)
    with database.transaction() as session:
        repository = SqlAlchemyCheckpointRepository(session)
        repository.append(
            RUN_ID,
            NODE_ID,
            PARTITION,
            expected_current_version=CheckpointVersion(0),
            expected_head_row_version=1,
            expected_work_row_version=1,
            payload_schema_version=1,
            source_cursor=None,
            output_position=None,
            artifact_id=None,
            committed_at=timestamp(3),
        )
        with pytest.raises(CheckpointConflictError):
            repository.append(
                RUN_ID,
                NODE_ID,
                PARTITION,
                expected_current_version=CheckpointVersion(0),
                expected_head_row_version=1,
                expected_work_row_version=1,
                payload_schema_version=1,
                source_cursor=document(changed=True),
                output_position=None,
                artifact_id=None,
                committed_at=timestamp(3),
            )
        with pytest.raises(ConsistencyStaleRowVersionError):
            repository.append(
                RUN_ID,
                NODE_ID,
                PARTITION,
                expected_current_version=CheckpointVersion(1),
                expected_head_row_version=1,
                expected_work_row_version=2,
                payload_schema_version=1,
                source_cursor=None,
                output_position=None,
                artifact_id=None,
                committed_at=timestamp(4),
            )


def test_event_batch_allocation_exact_replay_and_pagination(database: SQLiteDatabase) -> None:
    seed_execution(database)
    pending = (
        event("run_started", 2),
        event("work_started", 3, EventSubjectKind.WORK_ITEM),
    )
    with database.transaction() as session:
        repository = SqlAlchemyExecutionEventRepository(session)
        batch = repository.append(
            RUN_ID,
            expected_next_sequence=EventSequence(1),
            expected_counter_row_version=1,
            events=pending,
        )
        assert tuple(item.sequence for item in batch.items) == (
            EventSequence(1),
            EventSequence(2),
        )
        assert batch.next_sequence == EventSequence(3)
        assert batch.counter_row_version == 2
        replay = repository.append(
            RUN_ID,
            expected_next_sequence=EventSequence(1),
            expected_counter_row_version=1,
            events=pending,
        )
        assert replay == batch
        assert repository.get(RUN_ID, EventSequence(1)) == batch.items[0]
        page = repository.list_after(RUN_ID, after=None, limit=1)
        assert page.items == (batch.items[0],)
        assert page.next_cursor == EventSequence(1)
        tail = repository.list_after(RUN_ID, after=page.next_cursor, limit=2)
        assert tail.items == (batch.items[1],)
        assert tail.next_cursor is None


def test_event_conflict_subject_and_transaction_rollback(database: SQLiteDatabase) -> None:
    seed_execution(database)
    pending = (event("run_started", 2),)
    with database.transaction() as session:
        repository = SqlAlchemyExecutionEventRepository(session)
        repository.append(
            RUN_ID,
            expected_next_sequence=EventSequence(1),
            expected_counter_row_version=1,
            events=pending,
        )
        with pytest.raises(EventSequenceConflictError):
            repository.append(
                RUN_ID,
                expected_next_sequence=EventSequence(1),
                expected_counter_row_version=1,
                events=(event("run_changed", 2),),
            )
        with pytest.raises(ConsistencyStaleRowVersionError):
            repository.append(
                RUN_ID,
                expected_next_sequence=EventSequence(2),
                expected_counter_row_version=1,
                events=(event("run_finished", 3),),
            )
        wrong_subject = PendingExecutionEvent(
            "work_started",
            timestamp(3),
            EventSubjectKind.WORK_ITEM,
            WorkItemId("wrk_missing"),
            None,
            1,
            redacted(event="started"),
        )
        with pytest.raises(ConsistencyInvalidRequestError, match="subject"):
            repository.append(
                RUN_ID,
                expected_next_sequence=EventSequence(2),
                expected_counter_row_version=2,
                events=(wrong_subject,),
            )


def test_idempotency_begin_terminal_replay_conflict_and_listing(
    database: SQLiteDatabase,
) -> None:
    request = document(action="create", run_id=str(RUN_ID))
    with database.transaction() as session:
        repository = SqlAlchemyIdempotencyRepository(session)
        started = repository.begin(
            scope="run:create", key="request-001", request=request, started_at=timestamp(1)
        )
        assert started.disposition is IdempotencyBeginDisposition.STARTED
        assert started.record.status is IdempotencyStatus.IN_PROGRESS
        in_progress = repository.begin(
            scope="run:create", key="request-001", request=request, started_at=timestamp(2)
        )
        assert in_progress.disposition is IdempotencyBeginDisposition.IN_PROGRESS_REPLAY
        assert in_progress.reservation is None
        with pytest.raises(ConsistencyInvalidRequestError, match="reservation"):
            repository.complete(
                in_progress,  # type: ignore[arg-type]
                request=request,
                response_schema_version=1,
                response=redacted(safe=True),
                completed_at=timestamp(3),
            )
        page = repository.list_in_progress(limit=1)
        assert page.items == (started.record,)
        completed = repository.complete(
            reservation(started),
            request=request,
            response_schema_version=1,
            response=redacted(run_id=str(RUN_ID)),
            completed_at=timestamp(3),
        )
        assert completed.status is IdempotencyStatus.COMPLETED
        changes_before_replay = (
            session.connection().exec_driver_sql("SELECT total_changes()").scalar_one()
        )
        assert (
            repository.complete(
                reservation(started),
                request=request,
                response_schema_version=1,
                response=redacted(run_id=str(RUN_ID)),
                completed_at=timestamp(3),
            )
            == completed
        )
        changes_after_replay = (
            session.connection().exec_driver_sql("SELECT total_changes()").scalar_one()
        )
        assert changes_after_replay == changes_before_replay
        terminal = repository.begin(
            scope="run:create", key="request-001", request=request, started_at=timestamp(4)
        )
        assert terminal.disposition is IdempotencyBeginDisposition.COMPLETED_REPLAY
        assert in_progress.reservation is None
        assert terminal.reservation is None
        with pytest.raises(IdempotencyConflictError):
            repository.begin(
                scope="run:create",
                key="request-001",
                request=document(action="different"),
                started_at=timestamp(4),
            )
        with pytest.raises(IdempotencyConflictError):
            repository.fail(
                reservation(started),
                request=request,
                response_schema_version=1,
                response=redacted(error="safe"),
                completed_at=timestamp(3),
            )


def test_idempotency_failed_replay_and_no_transaction_guards(database: SQLiteDatabase) -> None:
    request = document(action="repair")
    with database.transaction() as session:
        repository = SqlAlchemyIdempotencyRepository(session)
        started = repository.begin(
            scope="repair:apply", key="request-002", request=request, started_at=timestamp(1)
        )
        failed = repository.fail(
            reservation(started),
            request=request,
            response_schema_version=1,
            response=redacted(code="rejected"),
            completed_at=timestamp(2),
        )
        assert failed.status is IdempotencyStatus.FAILED
        replay = repository.begin(
            scope="repair:apply", key="request-002", request=request, started_at=timestamp(3)
        )
        assert replay.disposition is IdempotencyBeginDisposition.FAILED_REPLAY

    sessions = database.engine.connect()
    sessions.close()
    factory = database.engine
    with factory.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA quick_check").scalar_one() == "ok"


def test_multi_repository_rollback_leaves_all_frontiers_unchanged(
    database: SQLiteDatabase,
) -> None:
    seed_execution(database)
    with pytest.raises(RuntimeError, match="rollback"):
        append_consistency_then_fail(database)
    with database.transaction() as session:
        assert session.scalar(select(func.count()).select_from(checkpoints)) == 0
        assert session.scalar(select(func.count()).select_from(execution_events)) == 0
        assert session.scalar(select(checkpoint_heads.c.current_version)) == 0
        assert session.scalar(select(run_event_counters.c.next_sequence_number)) == 1
        assert session.scalar(select(work_items.c.expected_checkpoint_version)) == 0
        assert session.scalar(select(func.count()).select_from(idempotency_records)) == 0


def append_consistency_then_fail(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        SqlAlchemyCheckpointRepository(session).append(
            RUN_ID,
            NODE_ID,
            PARTITION,
            expected_current_version=CheckpointVersion(0),
            expected_head_row_version=1,
            expected_work_row_version=1,
            payload_schema_version=1,
            source_cursor=None,
            output_position=None,
            artifact_id=None,
            committed_at=timestamp(3),
        )
        SqlAlchemyExecutionEventRepository(session).append(
            RUN_ID,
            expected_next_sequence=EventSequence(1),
            expected_counter_row_version=1,
            events=(event("checkpoint_committed", 3),),
        )
        raise RuntimeError("rollback")


def test_repositories_require_an_explicit_caller_transaction(database: SQLiteDatabase) -> None:
    session = Session(database.engine)
    try:
        operations = (
            lambda: SqlAlchemyCheckpointRepository(session).get_head(RUN_ID, NODE_ID, PARTITION),
            lambda: SqlAlchemyExecutionEventRepository(session).get(RUN_ID, EventSequence(1)),
            lambda: SqlAlchemyIdempotencyRepository(session).get(scope="scope", key="key"),
        )
        for operation in operations:
            with pytest.raises(ConsistencyInvalidRequestError, match="caller-owned"):
                operation()
    finally:
        session.close()


def test_checkpoint_missing_parent_empty_history_and_input_boundaries(
    database: SQLiteDatabase,
) -> None:
    missing_run = RunId("run_missing")
    missing_node = NodeId("nod_missing")
    missing_partition = PartitionKey("missing")
    with database.transaction() as session:
        repository = SqlAlchemyCheckpointRepository(session)
        assert repository.get_head(missing_run, missing_node, missing_partition) is None
        with pytest.raises(ConsistencyRecordNotFoundError):
            repository.get(missing_run, missing_node, missing_partition, CheckpointVersion(1))
        with pytest.raises(ConsistencyRecordNotFoundError):
            repository.list_history(missing_run, missing_node, missing_partition, limit=1)
        with pytest.raises(ConsistencyRecordNotFoundError):
            repository.append(
                missing_run,
                missing_node,
                missing_partition,
                expected_current_version=CheckpointVersion(0),
                expected_head_row_version=1,
                expected_work_row_version=1,
                payload_schema_version=1,
                source_cursor=None,
                output_position=None,
                artifact_id=None,
                committed_at=timestamp(1),
            )

    seed_execution(database)
    with database.transaction() as session:
        repository = SqlAlchemyCheckpointRepository(session)
        with pytest.raises(ConsistencyInvalidRequestError, match="positive"):
            repository.get(RUN_ID, NODE_ID, PARTITION, CheckpointVersion(0))
        assert repository.get(RUN_ID, NODE_ID, PARTITION, CheckpointVersion(1)) is None
        assert repository.list_history(RUN_ID, NODE_ID, PARTITION, limit=5).items == ()
        with pytest.raises(ConsistencyInvalidRequestError, match="monotonic"):
            repository.append(
                RUN_ID,
                NODE_ID,
                PARTITION,
                expected_current_version=CheckpointVersion(0),
                expected_head_row_version=1,
                expected_work_row_version=1,
                payload_schema_version=1,
                source_cursor=None,
                output_position=None,
                artifact_id=None,
                committed_at=timestamp(1),
            )


def insert_artifact(
    session: Session,
    *,
    artifact_id: ArtifactId,
    partition: PartitionKey = PARTITION,
    created_at: UtcTimestamp | None = None,
) -> None:
    created = timestamp(3) if created_at is None else created_at
    session.execute(
        insert(artifact_manifests).values(
            artifact_id=str(artifact_id),
            run_id=str(RUN_ID),
            node_id=str(NODE_ID),
            partition_key=str(partition),
            relative_path=f"artifacts/{artifact_id}.json",
            media_type="application/json",
            schema_version=1,
            byte_size=2,
            row_count=1,
            sha256="a" * 64,
            created_at=str(created),
        )
    )


def test_checkpoint_artifact_relationship_and_chronology_are_strict(
    database: SQLiteDatabase,
) -> None:
    seed_execution(database)
    artifact_id = ArtifactId("art_checkpoint")
    with database.transaction() as session:
        repository = SqlAlchemyCheckpointRepository(session)
        with pytest.raises(ConsistencyRecordNotFoundError, match="artifact"):
            repository.append(
                RUN_ID,
                NODE_ID,
                PARTITION,
                expected_current_version=CheckpointVersion(0),
                expected_head_row_version=1,
                expected_work_row_version=1,
                payload_schema_version=1,
                source_cursor=None,
                output_position=None,
                artifact_id=artifact_id,
                committed_at=timestamp(3),
            )
        insert_artifact(session, artifact_id=artifact_id, created_at=timestamp(4))
        with pytest.raises(ConsistencyInvalidRequestError, match="precede"):
            repository.append(
                RUN_ID,
                NODE_ID,
                PARTITION,
                expected_current_version=CheckpointVersion(0),
                expected_head_row_version=1,
                expected_work_row_version=1,
                payload_schema_version=1,
                source_cursor=None,
                output_position=None,
                artifact_id=artifact_id,
                committed_at=timestamp(3),
            )
        committed = repository.append(
            RUN_ID,
            NODE_ID,
            PARTITION,
            expected_current_version=CheckpointVersion(0),
            expected_head_row_version=1,
            expected_work_row_version=1,
            payload_schema_version=1,
            source_cursor=document(cursor="safe"),
            output_position=document(position="safe"),
            artifact_id=artifact_id,
            committed_at=timestamp(4),
        )
        assert committed.checkpoint.artifact_id == artifact_id
        assert repository.get(RUN_ID, NODE_ID, PARTITION, CheckpointVersion(1)) == (
            committed.checkpoint
        )


def test_checkpoint_later_frontier_rejects_old_exact_replay(database: SQLiteDatabase) -> None:
    seed_execution(database)
    repository: SqlAlchemyCheckpointRepository
    with database.transaction() as session:
        repository = SqlAlchemyCheckpointRepository(session)
        repository.append(
            RUN_ID,
            NODE_ID,
            PARTITION,
            expected_current_version=CheckpointVersion(0),
            expected_head_row_version=1,
            expected_work_row_version=1,
            payload_schema_version=1,
            source_cursor=None,
            output_position=None,
            artifact_id=None,
            committed_at=timestamp(3),
        )
        repository.append(
            RUN_ID,
            NODE_ID,
            PARTITION,
            expected_current_version=CheckpointVersion(1),
            expected_head_row_version=2,
            expected_work_row_version=2,
            payload_schema_version=1,
            source_cursor=None,
            output_position=None,
            artifact_id=None,
            committed_at=timestamp(4),
        )
        with pytest.raises(ConsistencyStaleRowVersionError):
            repository.append(
                RUN_ID,
                NODE_ID,
                PARTITION,
                expected_current_version=CheckpointVersion(1),
                expected_head_row_version=1,
                expected_work_row_version=1,
                payload_schema_version=1,
                source_cursor=None,
                output_position=None,
                artifact_id=None,
                committed_at=timestamp(3),
            )


def test_event_missing_parent_empty_history_and_input_boundaries(
    database: SQLiteDatabase,
) -> None:
    missing = RunId("run_missing")
    with database.transaction() as session:
        repository = SqlAlchemyExecutionEventRepository(session)
        with pytest.raises(ConsistencyRecordNotFoundError):
            repository.get(missing, EventSequence(1))
        with pytest.raises(ConsistencyRecordNotFoundError):
            repository.list_after(missing, after=None, limit=1)
        with pytest.raises(ConsistencyRecordNotFoundError):
            repository.append(
                missing,
                expected_next_sequence=EventSequence(1),
                expected_counter_row_version=1,
                events=(
                    PendingExecutionEvent(
                        "run_started",
                        timestamp(1),
                        EventSubjectKind.RUN,
                        missing,
                        None,
                        1,
                        redacted(safe=True),
                    ),
                ),
            )

    seed_execution(database)
    with database.transaction() as session:
        repository = SqlAlchemyExecutionEventRepository(session)
        assert repository.get(RUN_ID, EventSequence(1)) is None
        assert repository.list_after(RUN_ID, after=None, limit=5).items == ()
        with pytest.raises(ConsistencyInvalidRequestError, match="precede"):
            repository.append(
                RUN_ID,
                expected_next_sequence=EventSequence(1),
                expected_counter_row_version=1,
                events=(event("run_created", 0),),
            )
        wrong_run = PendingExecutionEvent(
            "run_started",
            timestamp(2),
            EventSubjectKind.RUN,
            RunId("run_other"),
            None,
            1,
            redacted(safe=True),
        )
        with pytest.raises(ConsistencyInvalidRequestError, match="subject"):
            repository.append(
                RUN_ID,
                expected_next_sequence=EventSequence(1),
                expected_counter_row_version=1,
                events=(wrong_run,),
            )


def test_event_later_frontier_rejects_old_replay(database: SQLiteDatabase) -> None:
    seed_execution(database)
    first = (event("run_started", 2),)
    with database.transaction() as session:
        repository = SqlAlchemyExecutionEventRepository(session)
        repository.append(
            RUN_ID,
            expected_next_sequence=EventSequence(1),
            expected_counter_row_version=1,
            events=first,
        )
        repository.append(
            RUN_ID,
            expected_next_sequence=EventSequence(2),
            expected_counter_row_version=2,
            events=(event("work_started", 3, EventSubjectKind.WORK_ITEM),),
        )
        with pytest.raises(ConsistencyStaleRowVersionError):
            repository.append(
                RUN_ID,
                expected_next_sequence=EventSequence(2),
                expected_counter_row_version=1,
                events=first,
            )


def test_idempotency_missing_pagination_stale_and_nonmonotonic_paths(
    database: SQLiteDatabase,
) -> None:
    repository: SqlAlchemyIdempotencyRepository
    first_request = document(action="first")
    second_request = document(action="second")
    with database.transaction() as session:
        repository = SqlAlchemyIdempotencyRepository(session)
        assert repository.get(scope="missing", key="missing") is None
        with pytest.raises(IdempotencyConflictError, match="does not exist"):
            repository.complete(
                forged_reservation("missing", "missing", first_request, timestamp(1)),
                request=first_request,
                response_schema_version=1,
                response=redacted(safe=True),
                completed_at=timestamp(2),
            )
        first = repository.begin(
            scope="scope-a", key="key-a", request=first_request, started_at=timestamp(1)
        )
        repository.begin(
            scope="scope-b", key="key-b", request=second_request, started_at=timestamp(1)
        )
        page = repository.list_in_progress(limit=1)
        assert len(page.items) == 1
        assert page.next_cursor is not None
        tail = repository.list_in_progress(limit=1, after=page.next_cursor)
        assert len(tail.items) == 1
        assert tail.next_cursor is None
        with pytest.raises(IdempotencyConflictError, match="not initial"):
            repository.complete(
                IdempotencyReservation(
                    "scope-a",
                    "key-a",
                    reservation(first).request_sha256,
                    timestamp(1),
                    timestamp(2),
                ),
                request=first_request,
                response_schema_version=1,
                response=redacted(safe=True),
                completed_at=timestamp(3),
            )
        with pytest.raises(IdempotencyConflictError, match="different request"):
            repository.complete(
                reservation(first),
                request=document(action="different"),
                response_schema_version=1,
                response=redacted(safe=True),
                completed_at=timestamp(3),
            )
        with pytest.raises(IdempotencyConflictError, match="reservation has a different request"):
            repository.complete(
                IdempotencyReservation("scope-a", "key-a", "0" * 64, timestamp(1), timestamp(1)),
                request=first_request,
                response_schema_version=1,
                response=redacted(safe=True),
                completed_at=timestamp(3),
            )
        with pytest.raises(IdempotencyConflictError, match="durable state"):
            repository.complete(
                IdempotencyReservation(
                    "scope-a",
                    "key-a",
                    reservation(first).request_sha256,
                    timestamp(2),
                    timestamp(2),
                ),
                request=first_request,
                response_schema_version=1,
                response=redacted(safe=True),
                completed_at=timestamp(3),
            )
        with pytest.raises(ConsistencyInvalidRequestError, match="monotonic"):
            repository.complete(
                reservation(first),
                request=first_request,
                response_schema_version=1,
                response=redacted(safe=True),
                completed_at=timestamp(0),
            )


def test_idempotency_terminal_exact_and_different_replays(database: SQLiteDatabase) -> None:
    request = document(action="terminal")
    with database.transaction() as session:
        repository = SqlAlchemyIdempotencyRepository(session)
        started = repository.begin(
            scope="scope", key="key", request=request, started_at=timestamp(1)
        )
        failed = repository.fail(
            reservation(started),
            request=request,
            response_schema_version=1,
            response=redacted(code="safe"),
            completed_at=timestamp(2),
        )
        assert (
            repository.fail(
                reservation(started),
                request=request,
                response_schema_version=1,
                response=redacted(code="safe"),
                completed_at=timestamp(2),
            )
            == failed
        )
        with pytest.raises(IdempotencyConflictError, match="terminal"):
            repository.fail(
                reservation(started),
                request=request,
                response_schema_version=1,
                response=redacted(code="different"),
                completed_at=timestamp(2),
            )


class AbortConsistencyWrite(BaseException):
    """Sentinel used to prove rollback for non-Exception failures."""


def fail_after_statement(connection: Connection, fragment: str, failure: BaseException) -> None:
    def listener(
        _connection: Connection,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        if fragment in statement:
            raise failure

    sqlalchemy_event.listen(connection, "after_cursor_execute", listener)


def append_checkpoint_with_failpoint(
    database: SQLiteDatabase, fragment: str, failure: BaseException
) -> None:
    with database.transaction() as session:
        fail_after_statement(session.connection(), fragment, failure)
        SqlAlchemyCheckpointRepository(session).append(
            RUN_ID,
            NODE_ID,
            PARTITION,
            expected_current_version=CheckpointVersion(0),
            expected_head_row_version=1,
            expected_work_row_version=1,
            payload_schema_version=1,
            source_cursor=document(offset=10),
            output_position=None,
            artifact_id=None,
            committed_at=timestamp(3),
        )


def append_event_with_failpoint(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        fail_after_statement(
            session.connection(),
            "UPDATE run_event_counters",
            AbortConsistencyWrite("after counter"),
        )
        SqlAlchemyExecutionEventRepository(session).append(
            RUN_ID,
            expected_next_sequence=EventSequence(1),
            expected_counter_row_version=1,
            events=(event("run_started", 2),),
        )


@pytest.mark.parametrize(
    ("fragment", "failure"),
    [
        ("UPDATE checkpoint_heads", RuntimeError("after head")),
        ("UPDATE work_items", AbortConsistencyWrite("after work")),
        ("INSERT INTO checkpoints", AbortConsistencyWrite("after history")),
    ],
)
def test_checkpoint_internal_failpoints_roll_back_every_companion_write(
    database: SQLiteDatabase, fragment: str, failure: BaseException
) -> None:
    seed_execution(database)
    with pytest.raises(type(failure), match=str(failure)):
        append_checkpoint_with_failpoint(database, fragment, failure)
    with database.transaction() as session:
        assert session.scalar(select(checkpoint_heads.c.current_version)) == 0
        assert session.scalar(select(checkpoint_heads.c.row_version)) == 1
        assert session.scalar(select(work_items.c.expected_checkpoint_version)) == 0
        assert session.scalar(select(work_items.c.row_version)) == 1
        assert session.scalar(select(func.count()).select_from(checkpoints)) == 0


def test_event_internal_base_exception_failpoint_rolls_back_counter_and_history(
    database: SQLiteDatabase,
) -> None:
    seed_execution(database)
    with pytest.raises(AbortConsistencyWrite, match="after counter"):
        append_event_with_failpoint(database)
    with database.transaction() as session:
        assert session.scalar(select(run_event_counters.c.next_sequence_number)) == 1
        assert session.scalar(select(run_event_counters.c.row_version)) == 1
        assert session.scalar(select(func.count()).select_from(execution_events)) == 0


def test_event_insert_base_exception_rolls_back_counter_and_batch(
    database: SQLiteDatabase,
) -> None:
    seed_execution(database)

    def append() -> None:
        with database.transaction() as session:
            fail_after_statement(
                session.connection(),
                "INSERT INTO execution_events",
                AbortConsistencyWrite("after event batch"),
            )
            SqlAlchemyExecutionEventRepository(session).append(
                RUN_ID,
                expected_next_sequence=EventSequence(1),
                expected_counter_row_version=1,
                events=(event("run_started", 2), event("work_started", 2)),
            )

    with pytest.raises(AbortConsistencyWrite, match="after event batch"):
        append()
    with database.transaction() as session:
        assert session.scalar(select(run_event_counters.c.next_sequence_number)) == 1
        assert session.scalar(select(func.count()).select_from(execution_events)) == 0


@pytest.mark.parametrize("operation", ["begin", "terminal"])
def test_idempotency_internal_base_exception_rolls_back_atomic_record(
    database: SQLiteDatabase, operation: str
) -> None:
    request = document(action="failpoint")
    if operation == "terminal":
        with database.transaction() as session:
            started = SqlAlchemyIdempotencyRepository(session).begin(
                scope="safe", key="failpoint", request=request, started_at=timestamp(1)
            )
        expected_count = 1
    else:
        started = None
        expected_count = 0

    def invoke() -> None:
        with database.transaction() as session:
            fragment = (
                "INSERT INTO idempotency_records"
                if operation == "begin"
                else "UPDATE idempotency_records"
            )
            fail_after_statement(
                session.connection(), fragment, AbortConsistencyWrite("idempotency write")
            )
            repository = SqlAlchemyIdempotencyRepository(session)
            if started is None:
                repository.begin(
                    scope="safe", key="failpoint", request=request, started_at=timestamp(1)
                )
            else:
                repository.complete(
                    reservation(started),
                    request=request,
                    response_schema_version=1,
                    response=redacted(safe=True),
                    completed_at=timestamp(2),
                )

    with pytest.raises(AbortConsistencyWrite, match="idempotency write"):
        invoke()
    with database.transaction() as session:
        assert (
            session.scalar(select(func.count()).select_from(idempotency_records)) == expected_count
        )
        if expected_count:
            durable = SqlAlchemyIdempotencyRepository(session).get(scope="safe", key="failpoint")
            assert durable is not None
            assert durable.status is IdempotencyStatus.IN_PROGRESS


def test_checkpoint_history_artifact_validation_uses_constant_queries(
    database: SQLiteDatabase,
) -> None:
    seed_execution(database)
    first_artifact = ArtifactId("art_first")
    second_artifact = ArtifactId("art_second")
    with database.transaction() as session:
        insert_artifact(session, artifact_id=first_artifact, created_at=timestamp(3))
        insert_artifact(session, artifact_id=second_artifact, created_at=timestamp(4))
        repository = SqlAlchemyCheckpointRepository(session)
        repository.append(
            RUN_ID,
            NODE_ID,
            PARTITION,
            expected_current_version=CheckpointVersion(0),
            expected_head_row_version=1,
            expected_work_row_version=1,
            payload_schema_version=1,
            source_cursor=None,
            output_position=None,
            artifact_id=first_artifact,
            committed_at=timestamp(3),
        )
        repository.append(
            RUN_ID,
            NODE_ID,
            PARTITION,
            expected_current_version=CheckpointVersion(1),
            expected_head_row_version=2,
            expected_work_row_version=2,
            payload_schema_version=1,
            source_cursor=None,
            output_position=None,
            artifact_id=second_artifact,
            committed_at=timestamp(4),
        )

    statements: list[str] = []

    def count_selects(
        _connection: Connection,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        if statement.lstrip().upper().startswith("SELECT"):
            statements.append(statement)

    with database.transaction() as session:
        sqlalchemy_event.listen(session.connection(), "before_cursor_execute", count_selects)
        page = SqlAlchemyCheckpointRepository(session).list_history(
            RUN_ID, NODE_ID, PARTITION, limit=100
        )
        assert len(page.items) == 2
    assert len(statements) == 5
    assert sum("artifact_manifests" in statement for statement in statements) == 1


def test_two_session_stale_writers_converge_without_gaps(database: SQLiteDatabase) -> None:
    seed_execution(database)
    with Session(database.engine) as first_session, Session(database.engine) as second_session:
        with first_session.begin():
            checkpoint = SqlAlchemyCheckpointRepository(first_session).append(
                RUN_ID,
                NODE_ID,
                PARTITION,
                expected_current_version=CheckpointVersion(0),
                expected_head_row_version=1,
                expected_work_row_version=1,
                payload_schema_version=1,
                source_cursor=document(offset=1),
                output_position=None,
                artifact_id=None,
                committed_at=timestamp(3),
            )
        with second_session.begin(), pytest.raises(CheckpointConflictError):
            SqlAlchemyCheckpointRepository(second_session).append(
                RUN_ID,
                NODE_ID,
                PARTITION,
                expected_current_version=CheckpointVersion(0),
                expected_head_row_version=1,
                expected_work_row_version=1,
                payload_schema_version=1,
                source_cursor=document(offset=2),
                output_position=None,
                artifact_id=None,
                committed_at=timestamp(3),
            )
        with first_session.begin():
            events = SqlAlchemyExecutionEventRepository(first_session).append(
                RUN_ID,
                expected_next_sequence=EventSequence(1),
                expected_counter_row_version=1,
                events=(event("checkpoint_committed", 3),),
            )
        with second_session.begin(), pytest.raises(EventSequenceConflictError):
            SqlAlchemyExecutionEventRepository(second_session).append(
                RUN_ID,
                expected_next_sequence=EventSequence(1),
                expected_counter_row_version=1,
                events=(event("checkpoint_changed", 3),),
            )
        idempotency_request = document(action="multisession")
        with first_session.begin():
            started = SqlAlchemyIdempotencyRepository(first_session).begin(
                scope="scope",
                key="multisession",
                request=idempotency_request,
                started_at=timestamp(3),
            )
        with second_session.begin():
            replay = SqlAlchemyIdempotencyRepository(second_session).begin(
                scope="scope",
                key="multisession",
                request=idempotency_request,
                started_at=timestamp(4),
            )
        assert checkpoint.checkpoint.version == CheckpointVersion(1)
        assert events.items[0].sequence == EventSequence(1)
        assert started.disposition is IdempotencyBeginDisposition.STARTED
        assert replay.disposition is IdempotencyBeginDisposition.IN_PROGRESS_REPLAY


def test_uncommitted_consistency_frontiers_are_invisible_then_publish_atomically(
    database: SQLiteDatabase,
) -> None:
    seed_execution(database)
    writer = Session(database.engine)
    request = document(action="wal-visibility")
    try:
        writer.begin()
        SqlAlchemyCheckpointRepository(writer).append(
            RUN_ID,
            NODE_ID,
            PARTITION,
            expected_current_version=CheckpointVersion(0),
            expected_head_row_version=1,
            expected_work_row_version=1,
            payload_schema_version=1,
            source_cursor=None,
            output_position=None,
            artifact_id=None,
            committed_at=timestamp(3),
        )
        SqlAlchemyExecutionEventRepository(writer).append(
            RUN_ID,
            expected_next_sequence=EventSequence(1),
            expected_counter_row_version=1,
            events=(event("checkpoint_committed", 3),),
        )
        SqlAlchemyIdempotencyRepository(writer).begin(
            scope="safe", key="wal-visibility", request=request, started_at=timestamp(3)
        )

        with database.transaction() as reader:
            head = SqlAlchemyCheckpointRepository(reader).get_head(RUN_ID, NODE_ID, PARTITION)
            assert head is not None
            assert head.current_version == CheckpointVersion(0)
            assert (
                SqlAlchemyExecutionEventRepository(reader)
                .list_after(RUN_ID, after=None, limit=10)
                .items
                == ()
            )
            assert (
                SqlAlchemyIdempotencyRepository(reader).get(scope="safe", key="wal-visibility")
                is None
            )
        writer.commit()
    finally:
        writer.close()

    with database.transaction() as reader:
        head = SqlAlchemyCheckpointRepository(reader).get_head(RUN_ID, NODE_ID, PARTITION)
        assert head is not None
        assert head.current_version == CheckpointVersion(1)
        assert (
            len(
                SqlAlchemyExecutionEventRepository(reader)
                .list_after(RUN_ID, after=None, limit=10)
                .items
            )
            == 1
        )
        assert (
            SqlAlchemyIdempotencyRepository(reader).get(scope="safe", key="wal-visibility")
            is not None
        )


def test_reopen_preserves_all_frontiers_and_sqlite_integrity(database: SQLiteDatabase) -> None:
    seed_execution(database)
    request = document(action="reopen")
    with database.transaction() as session:
        SqlAlchemyCheckpointRepository(session).append(
            RUN_ID,
            NODE_ID,
            PARTITION,
            expected_current_version=CheckpointVersion(0),
            expected_head_row_version=1,
            expected_work_row_version=1,
            payload_schema_version=1,
            source_cursor=None,
            output_position=None,
            artifact_id=None,
            committed_at=timestamp(3),
        )
        SqlAlchemyExecutionEventRepository(session).append(
            RUN_ID,
            expected_next_sequence=EventSequence(1),
            expected_counter_row_version=1,
            events=(event("checkpoint_committed", 3),),
        )
        idempotency = SqlAlchemyIdempotencyRepository(session)
        started = idempotency.begin(
            scope="scope", key="reopen", request=request, started_at=timestamp(3)
        )
        idempotency.complete(
            reservation(started),
            request=request,
            response_schema_version=1,
            response=redacted(safe=True),
            completed_at=timestamp(4),
        )

    database_path = database.engine.url.database
    assert database_path is not None
    reopened = SQLiteDatabase.open(SQLiteDatabaseConfig(Path(database_path)))
    try:
        with reopened.transaction() as session:
            head = SqlAlchemyCheckpointRepository(session).get_head(RUN_ID, NODE_ID, PARTITION)
            assert head is not None
            assert head.current_version == CheckpointVersion(1)
            assert (
                SqlAlchemyExecutionEventRepository(session).get(RUN_ID, EventSequence(1))
                is not None
            )
            idempotency_record = SqlAlchemyIdempotencyRepository(session).get(
                scope="scope", key="reopen"
            )
            assert idempotency_record is not None
            assert idempotency_record.status is IdempotencyStatus.COMPLETED
            connection = session.connection()
            assert connection.exec_driver_sql("PRAGMA quick_check").scalar_one() == "ok"
            assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
            assert connection.exec_driver_sql("PRAGMA journal_mode").scalar_one() == "wal"
    finally:
        reopened.close()


@pytest.mark.parametrize("bad", [" ", "\n", "bad value", "é"])
def test_portable_event_and_idempotency_identities_reject_unsafe_text(
    database: SQLiteDatabase, bad: str
) -> None:
    seed_execution(database)
    with database.transaction() as session:
        events = SqlAlchemyExecutionEventRepository(session)
        with pytest.raises(ConsistencyInvalidRequestError):
            events.append(
                RUN_ID,
                expected_next_sequence=EventSequence(1),
                expected_counter_row_version=1,
                events=(
                    PendingExecutionEvent(
                        "bad value",
                        timestamp(2),
                        EventSubjectKind.RUN,
                        RUN_ID,
                        None,
                        1,
                        redacted(safe=True),
                    ),
                ),
            )
        with pytest.raises(ConsistencyInvalidRequestError):
            events.append(
                RUN_ID,
                expected_next_sequence=EventSequence(1),
                expected_counter_row_version=1,
                events=(
                    PendingExecutionEvent(
                        "run_started",
                        timestamp(2),
                        EventSubjectKind.RUN,
                        RUN_ID,
                        bad,
                        1,
                        redacted(safe=True),
                    ),
                ),
            )
        idempotency = SqlAlchemyIdempotencyRepository(session)
        with pytest.raises(ConsistencyInvalidRequestError):
            idempotency.begin(
                scope=bad, key="safe-key", request=document(safe=True), started_at=timestamp(2)
            )
        with pytest.raises(ConsistencyInvalidRequestError):
            idempotency.begin(
                scope="safe", key=bad, request=document(safe=True), started_at=timestamp(2)
            )


@pytest.mark.parametrize("field", ["event_kind", "correlation_id"])
def test_event_reads_reject_raw_canonical_text_corruption(
    database: SQLiteDatabase, field: str
) -> None:
    seed_execution(database)
    with database.transaction() as session:
        SqlAlchemyExecutionEventRepository(session).append(
            RUN_ID,
            expected_next_sequence=EventSequence(1),
            expected_counter_row_version=1,
            events=(event("run_started", 2),),
        )
    with database.engine.connect() as connection:
        connection.exec_driver_sql('DROP TRIGGER "trg_execution_events_prohibit_update"')
        connection.exec_driver_sql(
            f'UPDATE execution_events SET "{field}" = ? WHERE run_id = ? AND sequence_number = 1',
            ("bad value", str(RUN_ID)),
        )
        connection.commit()
    with database.transaction() as session, pytest.raises(ConsistencyCorruptionError):
        SqlAlchemyExecutionEventRepository(session).get(RUN_ID, EventSequence(1))


@pytest.mark.parametrize("field", ["scope", "idempotency_key"])
def test_idempotency_reads_reject_raw_identity_and_in_progress_touch(
    database: SQLiteDatabase, field: str
) -> None:
    with database.transaction() as session:
        SqlAlchemyIdempotencyRepository(session).begin(
            scope="safe", key="safe-key", request=document(safe=True), started_at=timestamp(1)
        )
    with database.engine.connect() as connection:
        connection.exec_driver_sql(
            'DROP TRIGGER "trg_idempotency_records_protect_immutable_columns"'
        )
        connection.exec_driver_sql(
            f'UPDATE idempotency_records SET "{field}" = ? WHERE scope = ? AND idempotency_key = ?',
            ("bad value", "safe", "safe-key"),
        )
        connection.commit()
    with database.transaction() as session, pytest.raises(ConsistencyCorruptionError):
        SqlAlchemyIdempotencyRepository(session).list_in_progress(limit=10)


def test_idempotency_reads_reject_touched_in_progress_record(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        SqlAlchemyIdempotencyRepository(session).begin(
            scope="safe", key="safe-key", request=document(safe=True), started_at=timestamp(1)
        )
    with database.engine.connect() as connection:
        connection.exec_driver_sql(
            "UPDATE idempotency_records SET updated_at = ? WHERE scope = ? AND idempotency_key = ?",
            (str(timestamp(2)), "safe", "safe-key"),
        )
        connection.commit()
    with database.transaction() as session, pytest.raises(ConsistencyCorruptionError):
        SqlAlchemyIdempotencyRepository(session).get(scope="safe", key="safe-key")


def test_repository_row_version_capacity_is_rejected_before_sql(
    database: SQLiteDatabase,
) -> None:
    maximum = 2_147_483_647
    missing_run = RunId("run_missing")
    missing_node = NodeId("nod_missing")
    missing_partition = PartitionKey("missing")
    with database.transaction() as session:
        checkpoints_repository = SqlAlchemyCheckpointRepository(session)
        with pytest.raises(ConsistencyStateConflictError, match="head row version"):
            checkpoints_repository.append(
                missing_run,
                missing_node,
                missing_partition,
                expected_current_version=CheckpointVersion(maximum - 1),
                expected_head_row_version=maximum,
                expected_work_row_version=1,
                payload_schema_version=1,
                source_cursor=None,
                output_position=None,
                artifact_id=None,
                committed_at=timestamp(2),
            )
        with pytest.raises(ConsistencyStateConflictError, match="work-item row version"):
            checkpoints_repository.append(
                missing_run,
                missing_node,
                missing_partition,
                expected_current_version=CheckpointVersion(maximum - 1),
                expected_head_row_version=maximum - 1,
                expected_work_row_version=maximum,
                payload_schema_version=1,
                source_cursor=None,
                output_position=None,
                artifact_id=None,
                committed_at=timestamp(2),
            )
        with pytest.raises(ConsistencyStateConflictError, match="event counter row version"):
            SqlAlchemyExecutionEventRepository(session).append(
                missing_run,
                expected_next_sequence=EventSequence(maximum - 1),
                expected_counter_row_version=maximum,
                events=(
                    PendingExecutionEvent(
                        "run_started",
                        timestamp(2),
                        EventSubjectKind.RUN,
                        missing_run,
                        None,
                        1,
                        redacted(safe=True),
                    ),
                ),
            )


def test_raw_frontier_row_version_corruption_is_rejected(database: SQLiteDatabase) -> None:
    seed_execution(database)
    with database.engine.connect() as connection:
        connection.exec_driver_sql(
            'DROP TRIGGER "trg_checkpoint_heads_current_version_must_increase"'
        )
        connection.exec_driver_sql(
            'DROP TRIGGER "trg_run_event_counters_next_sequence_number_must_increase"'
        )
        connection.exec_driver_sql("UPDATE checkpoint_heads SET row_version = 2")
        connection.exec_driver_sql("UPDATE run_event_counters SET row_version = 2")
        connection.commit()
    with database.transaction() as session:
        with pytest.raises(ConsistencyCorruptionError, match="checkpoint head row version"):
            SqlAlchemyCheckpointRepository(session).get_head(RUN_ID, NODE_ID, PARTITION)
        with pytest.raises(ConsistencyCorruptionError, match="event counter row version"):
            SqlAlchemyExecutionEventRepository(session).list_after(RUN_ID, after=None, limit=10)


def test_sensitive_canary_never_reaches_database_or_wal(database: SQLiteDatabase) -> None:
    seed_execution(database)
    canary = "p3-consistency-sensitive-canary"
    with pytest.raises(ConsistencyInvalidRequestError) as captured:
        RedactedDocument.from_mapping({"api_key": canary})
    assert canary not in str(captured.value)

    database_path = Path(str(database.engine.url.database))
    for path in (
        database_path,
        Path(f"{database_path}-wal"),
        Path(f"{database_path}-shm"),
    ):
        if path.exists():
            assert canary.encode() not in path.read_bytes()
