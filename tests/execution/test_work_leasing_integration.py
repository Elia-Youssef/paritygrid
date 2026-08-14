"""Real SQLite WAL and transactional-writer integration for work leasing."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import text

from paritygrid.adapters.persistence import (
    SqlAlchemyExecutionEventRepository,
    SqlAlchemyPipelineRepository,
    SqlAlchemyRunRepository,
    SqlAlchemyWorkItemRepository,
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    SQLiteTransactionalWriter,
    create_session_factory,
    upgrade_to_head,
)
from paritygrid.application.execution import (
    AcquireWorkLeaseRequest,
    RenewWorkLeaseRequest,
    WorkLeaseService,
    WorkLeaseServiceSnapshot,
    WorkLeaseSettings,
)
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    WriterCommand,
    WriterCommandKind,
    WriterReceipt,
    WriterState,
)
from paritygrid.application.writes import (
    BootstrapWork,
    CreateCapturedRun,
    TransitionRun,
)
from paritygrid.domain.execution import RunState, WorkItemState
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
from paritygrid.domain.pipeline import PartitionKey

PIPELINE_ID = PipelineId("pip_lease-integration")
RUN_ID = RunId("run_lease-integration")
NODE_ID = NodeId("nod_lease-integration")
WORK_ID = WorkItemId("wrk_lease-integration")
_BASE = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)


class _Clock:
    def __init__(self, *values: UtcTimestamp) -> None:
        self._values = list(values)

    def now(self) -> UtcTimestamp:
        return self._values.pop(0)


def _timestamp(second: int) -> UtcTimestamp:
    return UtcTimestamp(_BASE + timedelta(seconds=second))


def _document(**values: object) -> ConfigurationDocument:
    return ConfigurationDocument.from_mapping(values)


def _event(
    sequence: int,
    kind: str,
    subject_id: RunId | WorkItemId,
    second: int,
) -> EventAppendRequest:
    subject_kind = EventSubjectKind.RUN if type(subject_id) is RunId else EventSubjectKind.WORK_ITEM
    return EventAppendRequest(
        EventSequence(sequence),
        sequence,
        PendingExecutionEvent(
            event_kind=kind,
            occurred_at=_timestamp(second),
            subject_kind=subject_kind,
            subject_id=subject_id,
            correlation_id="corr-lease-integration",
            payload_schema_version=1,
            payload=RedactedDocument.from_mapping({"kind": kind}),
        ),
    )


def _submit(writer: SQLiteTransactionalWriter, command: WriterCommand) -> WriterReceipt:
    return writer.submit(command, timeout_seconds=5.0).result(timeout_seconds=5.0)


def _seed_pipeline(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        repository = SqlAlchemyPipelineRepository(session)
        repository.create(
            pipeline_id=PIPELINE_ID,
            display_name="Work lease integration pipeline",
            description=None,
            created_at=_timestamp(0),
        )
        repository.publish_version(
            pipeline_id=PIPELINE_ID,
            expected_latest_version=None,
            specification=_document(nodes=[]),
            planner_format_version=1,
            published_at=_timestamp(0),
        )


def test_acquire_and_renew_commit_atomically_and_survive_database_restart(tmp_path: Path) -> None:
    database_path = tmp_path / "work leasing restart %.db"
    config = SQLiteDatabaseConfig(database_path)
    database = SQLiteDatabase.open(config)
    writer: SQLiteTransactionalWriter | None = None
    try:
        with database.engine.connect() as connection:
            upgrade_to_head(connection)
        _seed_pipeline(database)
        writer = SQLiteTransactionalWriter(create_session_factory(database.engine))
        writer.start()
        assert writer.snapshot().state is WriterState.RUNNING

        created = _submit(
            writer,
            CreateCapturedRun(
                run_id=RUN_ID,
                pipeline_id=PIPELINE_ID,
                pipeline_version=PipelineVersion(1),
                runner_kind="sequential",
                runner_configuration=_document(max_concurrency=1),
                scenario_seed=17,
                node_ids=(NODE_ID,),
                created_at=_timestamp(1),
                event=_event(1, "run_created", RUN_ID, 1),
            ),
        )
        assert created.command_kind is WriterCommandKind.CREATE_CAPTURED_RUN
        _submit(
            writer,
            TransitionRun(
                run_id=RUN_ID,
                expected_run_row_version=1,
                target_state=RunState.RUNNING,
                transitioned_at=_timestamp(2),
                final_reconciliation_fingerprint=None,
                event=_event(2, "run_started", RUN_ID, 2),
            ),
        )
        _submit(
            writer,
            BootstrapWork(
                run_id=RUN_ID,
                node_id=NODE_ID,
                work_item_id=WORK_ID,
                partition_key=PartitionKey("partition-1"),
                input_reference=_document(source="fixture"),
                created_at=_timestamp(2),
                expected_node_row_version=1,
                expected_run_row_version=2,
                event=_event(3, "work_created", WORK_ID, 2),
            ),
        )

        service = WorkLeaseService(
            writer,
            _Clock(_timestamp(3), _timestamp(4)),
            settings=WorkLeaseSettings(Duration(5_000_000), 5.0, 5.0),
        )
        lease = service.acquire(
            AcquireWorkLeaseRequest(
                run_id=RUN_ID,
                node_id=NODE_ID,
                work_item_id=WORK_ID,
                expected_attempt_number=AttemptNumber(1),
                expected_work_row_version=1,
                expected_node_row_version=2,
                expected_run_row_version=3,
                lease_owner="scheduler-01",
                runner_kind="sequential",
                worker_identity="worker-01",
                event=_event(4, "work_claimed", WORK_ID, 3),
            )
        )
        renewed = service.renew(
            lease,
            RenewWorkLeaseRequest(
                expected_run_row_version=4,
                event=_event(5, "work_claim_renewed", WORK_ID, 4),
            ),
        )
        assert renewed.claim.attempt_number == AttemptNumber(1)
        assert renewed.claim.row_version == 3
        assert renewed.claim.lease_expires_at == _timestamp(9)
        assert service.snapshot() == WorkLeaseServiceSnapshot(1, 0, 0)
        diagnostics = writer.snapshot()
        assert diagnostics.state is WriterState.RUNNING
        assert diagnostics.accepted == diagnostics.completed == 5
        assert diagnostics.in_flight == diagnostics.queue_depth == 0

        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        assert closed.accepted == closed.completed == 5
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()

    restarted = SQLiteDatabase.open(config)
    try:
        assert restarted.capabilities.foreign_keys
        assert restarted.capabilities.journal_mode == "wal"
        assert restarted.capabilities.synchronous_level == 2
        with restarted.transaction() as session:
            work = SqlAlchemyWorkItemRepository(session).get(WORK_ID)
            assert work is not None
            assert work.state is WorkItemState.RUNNING
            assert work.row_version == 3
            assert work.active_attempt_number == AttemptNumber(1)
            assert work.lease_owner == "scheduler-01"
            assert work.lease_expires_at == _timestamp(9)
            assert work.active_runner_kind == "sequential"
            assert work.active_worker_identity == "worker-01"

            runs = SqlAlchemyRunRepository(session)
            run = runs.get(RUN_ID)
            node = runs.get_node(RUN_ID, NODE_ID)
            counter = runs.get_event_counter(RUN_ID)
            assert run is not None
            assert run.state is RunState.RUNNING
            assert run.row_version == 5
            assert node is not None
            assert node.row_version == 3
            assert node.work_running == 1
            assert counter is not None
            assert (counter.next_sequence_number, counter.row_version) == (6, 6)

            events = SqlAlchemyExecutionEventRepository(session)
            claimed = events.get(RUN_ID, EventSequence(4))
            renewed_event = events.get(RUN_ID, EventSequence(5))
            assert claimed is not None
            assert claimed.event_kind == "work_claimed"
            assert renewed_event is not None
            assert renewed_event.event_kind == "work_claim_renewed"

        with restarted.engine.connect() as connection:
            assert connection.execute(text("PRAGMA quick_check")).scalar_one() == "ok"
            assert connection.execute(text("PRAGMA foreign_key_check")).all() == []
    finally:
        restarted.close()

    assert database_path.is_file()
