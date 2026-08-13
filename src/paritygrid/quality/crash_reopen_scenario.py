"""Fixed composite-write scenario and independent SQLite crash classifier."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from sqlalchemy.engine import Connection

from paritygrid.adapters.persistence import (
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    create_session_factory,
)
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.repositories import SqlAlchemyPipelineRepository
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.execution import WorkClaim, WorkCompletion
from paritygrid.application.ports.run_aggregates import WorkMetricDelta
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    WriterCommand,
    WriterReceipt,
    WriterSettings,
)
from paritygrid.application.writes.execution import (
    BootstrapWork,
    CheckpointWrite,
    ClaimWork,
    CommitWorkWithCheckpoint,
    CreateCapturedRun,
    TransitionRun,
)
from paritygrid.domain.execution import RunState, WorkItemState
from paritygrid.domain.models import (
    AttemptNumber,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

PIPELINE_ID = PipelineId("pip_crash-reopen")
RUN_ID = RunId("run_crash-reopen")
NODE_ID = NodeId("nod_crash-reopen")
WORK_IDS = (
    WorkItemId("wrk_crash-reopen-1"),
    WorkItemId("wrk_crash-reopen-2"),
    WorkItemId("wrk_crash-reopen-3"),
)
PARTITIONS = (
    PartitionKey("partition-crash-1"),
    PartitionKey("partition-crash-2"),
    PartitionKey("partition-crash-3"),
)
CORRELATION_ID = "corr-crash-reopen"
RUNNER_KIND = "threaded"
LEASE_OWNERS = ("lease-owner-1", "lease-owner-2", "lease-owner-3")
WORKERS = ("worker-01", "worker-02", "worker-03")


class CrashDatabaseOutcome(StrEnum):
    ABSENT = "absent"
    COMMITTED = "committed"


class CrashDatabaseIntegrityError(Exception):
    """The reopened database is neither the exact old nor exact new state."""


@dataclass(frozen=True, slots=True)
class CrashDatabaseProjection:
    run: tuple[object, ...]
    counter: tuple[object, ...]
    node: tuple[object, ...]
    work_items: tuple[tuple[object, ...], ...]
    attempts: tuple[tuple[object, ...], ...]
    heads: tuple[tuple[object, ...], ...]
    checkpoints: tuple[tuple[object, ...], ...]
    events: tuple[tuple[object, ...], ...]


def timestamp(second: int) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 13, 12, 0, second, tzinfo=UTC))


def document(**values: object) -> ConfigurationDocument:
    return ConfigurationDocument.from_mapping(values)


def event_request(
    sequence: int,
    kind: str,
    subject_id: RunId | WorkItemId,
    occurred_at: UtcTimestamp,
) -> EventAppendRequest:
    subject_kind = EventSubjectKind.RUN if type(subject_id) is RunId else EventSubjectKind.WORK_ITEM
    return EventAppendRequest(
        expected_next_sequence=EventSequence(sequence),
        expected_counter_row_version=sequence,
        event=PendingExecutionEvent(
            event_kind=kind,
            occurred_at=occurred_at,
            subject_kind=subject_kind,
            subject_id=subject_id,
            correlation_id=CORRELATION_ID,
            payload_schema_version=1,
            payload=RedactedDocument.from_mapping({"kind": kind}),
        ),
    )


def create_run_command(seed: int) -> CreateCapturedRun:
    return CreateCapturedRun(
        run_id=RUN_ID,
        pipeline_id=PIPELINE_ID,
        pipeline_version=PipelineVersion(1),
        runner_kind=RUNNER_KIND,
        runner_configuration=document(max_workers=1),
        scenario_seed=seed,
        node_ids=(NODE_ID,),
        created_at=timestamp(1),
        event=event_request(1, "run_created", RUN_ID, timestamp(1)),
    )


def transition_run_command() -> TransitionRun:
    return TransitionRun(
        run_id=RUN_ID,
        expected_run_row_version=1,
        target_state=RunState.RUNNING,
        transitioned_at=timestamp(2),
        final_reconciliation_fingerprint=None,
        event=event_request(2, "run_started", RUN_ID, timestamp(2)),
    )


def bootstrap_command(index: int) -> BootstrapWork:
    sequence = 3 + index * 2
    return BootstrapWork(
        run_id=RUN_ID,
        node_id=NODE_ID,
        work_item_id=WORK_IDS[index],
        partition_key=PARTITIONS[index],
        input_reference=document(ordinal=index + 1),
        created_at=timestamp(3),
        expected_node_row_version=1 + index * 2,
        expected_run_row_version=2 + index * 2,
        event=event_request(
            sequence,
            "work_created",
            WORK_IDS[index],
            timestamp(3),
        ),
    )


def claim_command(index: int) -> ClaimWork:
    sequence = 4 + index * 2
    return ClaimWork(
        run_id=RUN_ID,
        node_id=NODE_ID,
        work_item_id=WORK_IDS[index],
        expected_work_row_version=1,
        expected_node_row_version=2 + index * 2,
        expected_run_row_version=3 + index * 2,
        lease_owner=LEASE_OWNERS[index],
        started_at=timestamp(3),
        lease_expires_at=timestamp(8),
        runner_kind=RUNNER_KIND,
        worker_identity=WORKERS[index],
        event=event_request(
            sequence,
            "work_claimed",
            WORK_IDS[index],
            timestamp(3),
        ),
    )


def work_claim(index: int) -> WorkClaim:
    return WorkClaim(
        work_item_id=WORK_IDS[index],
        attempt_number=AttemptNumber(1),
        lease_owner=LEASE_OWNERS[index],
        row_version=2,
        started_at=timestamp(3),
        lease_expires_at=timestamp(8),
        runner_kind=RUNNER_KIND,
        worker_identity=WORKERS[index],
    )


def completion_command(index: int) -> CommitWorkWithCheckpoint:
    ordinal = index + 1
    finished = timestamp(5 + index)
    records = 10 * ordinal
    return CommitWorkWithCheckpoint(
        run_id=RUN_ID,
        node_id=NODE_ID,
        claim=work_claim(index),
        completion=WorkCompletion(
            target_state=WorkItemState.SUCCEEDED,
            finished_at=finished,
            retry_available_at=None,
            failure_classification=None,
            redacted_detail=None,
            result_reference=document(output=f"work-{ordinal}"),
            records_processed=records,
            bytes_processed=100 * ordinal,
        ),
        checkpoint=CheckpointWrite(
            expected_head_row_version=1,
            payload_schema_version=1,
            source_cursor=document(offset=ordinal),
            output_position=document(rows=records),
            artifact_id=None,
            committed_at=finished,
        ),
        metrics=WorkMetricDelta(
            records_read=records,
            records_written=records,
            records_quarantined=0,
            bytes_read=100 * ordinal,
            bytes_written=80 * ordinal,
        ),
        expected_node_row_version=7 + index,
        expected_run_row_version=8 + index,
        event=event_request(
            9 + index,
            "checkpoint_committed",
            WORK_IDS[index],
            finished,
        ),
    )


def _writer(database: SQLiteDatabase, thread_name: str) -> SQLiteTransactionalWriter:
    return SQLiteTransactionalWriter(
        create_session_factory(database.engine),
        WriterSettings(
            queue_capacity=4,
            admission_waiter_capacity=4,
            notification_capacity=4,
            max_contention_attempts=3,
            contention_delay_seconds=0.0,
            thread_name=thread_name,
        ),
    )


def _submit(writer: SQLiteTransactionalWriter, command: WriterCommand) -> WriterReceipt:
    ticket = writer.submit(command, timeout_seconds=5.0)
    return ticket.result(timeout_seconds=5.0)


def prepare_crash_database(database_path: Path, seed: int) -> None:
    """Create the exact three-claim baseline through public repositories and writer commands."""
    if not database_path.is_absolute():
        raise TypeError("crash database path must be an absolute Path")
    if database_path.exists():
        raise CrashDatabaseIntegrityError("crash database must be a new file")
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(database_path))
    writer: SQLiteTransactionalWriter | None = None
    try:
        with database.engine.connect() as connection:
            upgrade_to_head(connection)
        with database.transaction() as session:
            repository = SqlAlchemyPipelineRepository(session)
            repository.create(
                pipeline_id=PIPELINE_ID,
                display_name="Crash reopen pipeline",
                description=None,
                created_at=timestamp(0),
            )
            repository.publish_version(
                pipeline_id=PIPELINE_ID,
                expected_latest_version=None,
                specification=document(nodes=[]),
                planner_format_version=1,
                published_at=timestamp(0),
            )
        writer = _writer(database, "paritygrid-crash-seed")
        writer.start()
        _submit(writer, create_run_command(seed))
        _submit(writer, transition_run_command())
        for index in range(3):
            _submit(writer, bootstrap_command(index))
            receipt = _submit(writer, claim_command(index))
            if getattr(receipt.result, "claim", None) != work_claim(index):
                raise CrashDatabaseIntegrityError("seeded work claim is not deterministic")
        if not writer.close(timeout_seconds=5.0).drained:
            raise CrashDatabaseIntegrityError("seed writer did not drain")
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()
    outcome, _ = classify_crash_database(database_path, seed)
    if outcome is not CrashDatabaseOutcome.ABSENT:
        raise CrashDatabaseIntegrityError("seeded crash database is not the exact baseline")


def commit_target_normally(database_path: Path) -> WriterReceipt:
    """Retry the target composite once through the production writer boundary."""
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(database_path))
    writer = _writer(database, "paritygrid-crash-retry")
    try:
        with database.engine.connect() as connection:
            upgrade_to_head(connection)
        writer.start()
        receipt = _submit(writer, completion_command(0))
        if not writer.close(timeout_seconds=5.0).drained:
            raise CrashDatabaseIntegrityError("retry writer did not drain")
        return receipt
    finally:
        writer.close(timeout_seconds=5.0)
        database.close()


def _rows(
    connection: Connection, sql: str, parameters: tuple[object, ...] = ()
) -> tuple[tuple[object, ...], ...]:
    return tuple(tuple(row) for row in connection.exec_driver_sql(sql, parameters).all())


def _read_projection(connection: Connection) -> CrashDatabaseProjection:
    run = _rows(connection, "SELECT * FROM runs WHERE run_id = ?", (str(RUN_ID),))
    counter = _rows(
        connection,
        "SELECT * FROM run_event_counters WHERE run_id = ?",
        (str(RUN_ID),),
    )
    node = _rows(
        connection,
        "SELECT * FROM run_nodes WHERE run_id = ? AND node_id = ?",
        (str(RUN_ID), str(NODE_ID)),
    )
    if len(run) != 1 or len(counter) != 1 or len(node) != 1:
        raise CrashDatabaseIntegrityError("crash aggregate root is incomplete")
    return CrashDatabaseProjection(
        run=run[0],
        counter=counter[0],
        node=node[0],
        work_items=_rows(
            connection,
            "SELECT * FROM work_items WHERE run_id = ? ORDER BY work_item_id",
            (str(RUN_ID),),
        ),
        attempts=_rows(
            connection,
            "SELECT a.* FROM work_attempts AS a JOIN work_items AS w "
            "ON w.work_item_id = a.work_item_id WHERE w.run_id = ? "
            "ORDER BY a.work_item_id, a.attempt_number",
            (str(RUN_ID),),
        ),
        heads=_rows(
            connection,
            "SELECT * FROM checkpoint_heads WHERE run_id = ? ORDER BY partition_key",
            (str(RUN_ID),),
        ),
        checkpoints=_rows(
            connection,
            "SELECT * FROM checkpoints WHERE run_id = ? ORDER BY partition_key, version",
            (str(RUN_ID),),
        ),
        events=_rows(
            connection,
            "SELECT * FROM execution_events WHERE run_id = ? ORDER BY sequence_number",
            (str(RUN_ID),),
        ),
    )


def _run_row(seed: int, row_version: int) -> tuple[object, ...]:
    return (
        str(RUN_ID),
        str(PIPELINE_ID),
        1,
        RUNNER_KIND,
        '{"max_workers":1}',
        "running",
        row_version,
        seed,
        str(timestamp(1)),
        str(timestamp(2)),
        None,
        None,
        None,
        None,
        None,
    )


def _node_row(committed: bool) -> tuple[object, ...]:
    return (
        str(RUN_ID),
        str(NODE_ID),
        "running",
        8 if committed else 7,
        3,
        0,
        2 if committed else 3,
        1 if committed else 0,
        0,
        0,
        0,
        10 if committed else 0,
        10 if committed else 0,
        0,
        100 if committed else 0,
        80 if committed else 0,
        0,
        2_000_000 if committed else 0,
        str(timestamp(3)),
        None,
    )


def _work_row(index: int, committed: bool) -> tuple[object, ...]:
    target_committed = committed and index == 0
    return (
        str(WORK_IDS[index]),
        str(RUN_ID),
        str(NODE_ID),
        str(PARTITIONS[index]),
        "succeeded" if target_committed else "running",
        4 if target_committed else 2,
        1 if target_committed else 0,
        1 if target_committed else 0,
        f'{{"ordinal":{index + 1}}}',
        None,
        None if target_committed else LEASE_OWNERS[index],
        None if target_committed else str(timestamp(8)),
        None if target_committed else 1,
        None if target_committed else str(timestamp(3)),
        None if target_committed else RUNNER_KIND,
        None if target_committed else WORKERS[index],
        str(timestamp(3)),
        str(timestamp(5)) if target_committed else str(timestamp(3)),
    )


def _head_row(index: int, committed: bool) -> tuple[object, ...]:
    target_committed = committed and index == 0
    return (
        str(RUN_ID),
        str(NODE_ID),
        str(PARTITIONS[index]),
        1 if target_committed else 0,
        str(timestamp(5)) if target_committed else str(timestamp(3)),
        2 if target_committed else 1,
    )


def _event_row(
    sequence: int, kind: str, subject: RunId | WorkItemId, at: UtcTimestamp
) -> tuple[object, ...]:
    return (
        str(RUN_ID),
        sequence,
        kind,
        str(at),
        "run" if type(subject) is RunId else "work_item",
        str(subject),
        CORRELATION_ID,
        1,
        f'{{"kind":"{kind}"}}',
    )


def expected_projection(seed: int, committed: bool) -> CrashDatabaseProjection:
    events = [
        _event_row(1, "run_created", RUN_ID, timestamp(1)),
        _event_row(2, "run_started", RUN_ID, timestamp(2)),
    ]
    for index in range(3):
        events.append(_event_row(3 + index * 2, "work_created", WORK_IDS[index], timestamp(3)))
        events.append(_event_row(4 + index * 2, "work_claimed", WORK_IDS[index], timestamp(3)))
    if committed:
        events.append(_event_row(9, "checkpoint_committed", WORK_IDS[0], timestamp(5)))
    return CrashDatabaseProjection(
        run=_run_row(seed, 9 if committed else 8),
        counter=(str(RUN_ID), 10 if committed else 9, 10 if committed else 9),
        node=_node_row(committed),
        work_items=tuple(_work_row(index, committed) for index in range(3)),
        attempts=(
            (
                str(WORK_IDS[0]),
                1,
                str(timestamp(3)),
                str(timestamp(5)),
                RUNNER_KIND,
                WORKERS[0],
                "succeeded",
                None,
                None,
                '{"output":"work-1"}',
                10,
                100,
                2_000_000,
            ),
        )
        if committed
        else (),
        heads=tuple(_head_row(index, committed) for index in range(3)),
        checkpoints=(
            (
                str(RUN_ID),
                str(NODE_ID),
                str(PARTITIONS[0]),
                1,
                1,
                '{"offset":1}',
                '{"rows":10}',
                None,
                str(timestamp(5)),
            ),
        )
        if committed
        else (),
        events=tuple(events),
    )


def _validate_reopened_database(connection: Connection) -> None:
    if connection.exec_driver_sql("PRAGMA quick_check").scalar_one() != "ok":
        raise CrashDatabaseIntegrityError("SQLite quick check failed after reopen")
    if connection.exec_driver_sql("PRAGMA foreign_key_check").all():
        raise CrashDatabaseIntegrityError("SQLite foreign-key check failed after reopen")
    pragmas = (
        connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one(),
        connection.exec_driver_sql("PRAGMA journal_mode").scalar_one(),
        connection.exec_driver_sql("PRAGMA synchronous").scalar_one(),
        connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one(),
    )
    if pragmas != (1, "wal", 2, 5_000):
        raise CrashDatabaseIntegrityError("SQLite durability pragmas changed after reopen")
    version_rows = _rows(connection, "SELECT version_num FROM alembic_version")
    if version_rows != (("0001_operational",),):
        raise CrashDatabaseIntegrityError("migration version is invalid after reopen")
    operational_tables = connection.exec_driver_sql(
        "SELECT count(*) FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%' AND name <> 'alembic_version'"
    ).scalar_one()
    trigger_count = connection.exec_driver_sql(
        "SELECT count(*) FROM sqlite_master WHERE type='trigger'"
    ).scalar_one()
    if operational_tables != 21 or trigger_count != 47:
        raise CrashDatabaseIntegrityError("schema inventory changed after reopen")


def classify_crash_database(
    database_path: Path, seed: int
) -> tuple[CrashDatabaseOutcome, CrashDatabaseProjection]:
    """Reopen twice and classify only exact old or exact committed state."""
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(database_path))
    try:
        with database.engine.connect() as connection:
            upgrade_to_head(connection)
            _validate_reopened_database(connection)
            projection = _read_projection(connection)
    finally:
        database.close()
    reopened = SQLiteDatabase.open(SQLiteDatabaseConfig(database_path))
    try:
        with reopened.engine.connect() as connection:
            _validate_reopened_database(connection)
            second = _read_projection(connection)
    finally:
        reopened.close()
    if second != projection:
        raise CrashDatabaseIntegrityError("database projection changed across reopen")
    if projection == expected_projection(seed, False):
        return CrashDatabaseOutcome.ABSENT, projection
    if projection == expected_projection(seed, True):
        return CrashDatabaseOutcome.COMMITTED, projection
    raise CrashDatabaseIntegrityError("database contains a partial or divergent crash outcome")


__all__ = [
    "NODE_ID",
    "PIPELINE_ID",
    "RUN_ID",
    "WORK_IDS",
    "CrashDatabaseIntegrityError",
    "CrashDatabaseOutcome",
    "CrashDatabaseProjection",
    "classify_crash_database",
    "commit_target_normally",
    "completion_command",
    "expected_projection",
    "prepare_crash_database",
    "timestamp",
]
