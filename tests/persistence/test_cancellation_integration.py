# pyright: reportPrivateUsage=false

from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy import event

from paritygrid.adapters.persistence import (
    BoundedCommittedNotificationBuffer,
    SqlAlchemyExecutionEventRepository,
    SqlAlchemyPipelineRepository,
    SqlAlchemyRunRepository,
    SqlAlchemyWorkAttemptRepository,
    SqlAlchemyWorkItemRepository,
    SQLiteCancellationStateReader,
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    SQLiteTransactionalWriter,
    create_session_factory,
    upgrade_to_head,
)
from paritygrid.application.execution import (
    AcquireWorkLeaseRequest,
    AttemptEventContext,
    AttemptSucceeded,
    CancellationAction,
    CancellationCoordinator,
    CancellationCoordinatorOutcomeUnknownError,
    CancellationCoordinatorRejectedError,
    CancellationCoordinatorSettings,
    CancellationDurableState,
    CancellationStateReader,
    ResultCheckpoint,
    ResultMetrics,
    ResultSubmission,
    SuccessfulWorkResult,
    TransactionalCheckpointResultSink,
    WorkLeaseService,
    WorkLeaseSettings,
    submit_work_result,
)
from paritygrid.application.planner import PlannerRunnerKind
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.execution import (
    ExecutionStaleRowVersionError,
    ExecutionStateConflictError,
    WorkItemState,
)
from paritygrid.application.ports.run_aggregates import WorkMetricDelta
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    WriterCommand,
    WriterFailedError,
    WriterSettings,
    WriterState,
    WriterSubmissionId,
)
from paritygrid.application.writes import (
    BootstrapWork,
    CreateCapturedRun,
    TransitionRun,
)
from paritygrid.domain.execution import RunState
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

RUN_ID = RunId("run_cancel-real")
PIPELINE_ID = PipelineId("pip_cancel-real")
NODE_ID = NodeId("nod_cancel-a")
WORK_A = WorkItemId("wrk_cancel-a")
WORK_B = WorkItemId("wrk_cancel-b")
RUNNER_KIND = "sequential"
CORRELATION = "corr-cancel-real"


class _Clock:
    def __init__(self, *values: UtcTimestamp) -> None:
        self._values = list(values)

    def now(self) -> UtcTimestamp:
        return self._values.pop(0)


def _time(second: int) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 15, 12, 0, tzinfo=UTC) + timedelta(seconds=second))


def _event(
    sequence: int,
    kind: str,
    subject: RunId | WorkItemId,
    *,
    second: int = 3,
) -> EventAppendRequest:
    subject_kind = EventSubjectKind.RUN if type(subject) is RunId else EventSubjectKind.WORK_ITEM
    return EventAppendRequest(
        EventSequence(sequence),
        sequence,
        PendingExecutionEvent(
            kind,
            _time(second),
            subject_kind,
            subject,
            CORRELATION,
            1,
            RedactedDocument.from_mapping({"kind": kind}),
        ),
    )


def _submit(writer: SQLiteTransactionalWriter, command: WriterCommand) -> Any:
    return writer.submit(command, timeout_seconds=5.0).result(timeout_seconds=5.0)


def _seed_pipeline(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PIPELINE_ID,
            display_name="Cancellation pipeline",
            description=None,
            created_at=_time(0),
        )
        pipelines.publish_version(
            pipeline_id=PIPELINE_ID,
            expected_latest_version=None,
            specification=ConfigurationDocument.from_mapping({"nodes": []}),
            planner_format_version=1,
            published_at=_time(0),
        )


def _open_database(path: Path) -> SQLiteDatabase:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(path))
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    _seed_pipeline(database)
    return database


def _writer_for(
    database: SQLiteDatabase,
    notifications: BoundedCommittedNotificationBuffer | None = None,
) -> SQLiteTransactionalWriter:
    return SQLiteTransactionalWriter(
        create_session_factory(database.engine),
        settings=WriterSettings(contention_delay_seconds=0.0),
        notifications=notifications,
    )


def _create_running_run(
    writer: SQLiteTransactionalWriter,
    *,
    work_items: tuple[WorkItemId, ...] = (WORK_A, WORK_B),
) -> None:
    _submit(
        writer,
        CreateCapturedRun(
            run_id=RUN_ID,
            pipeline_id=PIPELINE_ID,
            pipeline_version=PipelineVersion(1),
            runner_kind=RUNNER_KIND,
            runner_configuration=ConfigurationDocument(()),
            scenario_seed=None,
            node_ids=(NODE_ID,),
            created_at=_time(1),
            event=_event(1, "run_created", RUN_ID),
        ),
    )
    _submit(
        writer,
        TransitionRun(
            run_id=RUN_ID,
            expected_run_row_version=1,
            target_state=RunState.RUNNING,
            transitioned_at=_time(2),
            final_reconciliation_fingerprint=None,
            event=_event(2, "run_started", RUN_ID, second=2),
        ),
    )
    for index, work_item_id in enumerate(work_items):
        _submit(
            writer,
            BootstrapWork(
                run_id=RUN_ID,
                node_id=NODE_ID,
                work_item_id=work_item_id,
                partition_key=PartitionKey(f"part-cancel-{index}"),
                input_reference=None,
                created_at=_time(3),
                expected_node_row_version=1 + index,
                expected_run_row_version=2 + index,
                event=_event(3 + index, "work_created", work_item_id),
            ),
        )


def _coordinator(
    writer: SQLiteTransactionalWriter,
    reader: CancellationStateReader,
    clock: _Clock,
) -> tuple[CancellationCoordinator, WorkLeaseService]:
    leases = WorkLeaseService(
        writer,
        clock,
        settings=WorkLeaseSettings(
            lease_duration=Duration(3_600_000_000),
            admission_timeout_seconds=5.0,
            result_timeout_seconds=5.0,
        ),
    )
    coordinator = CancellationCoordinator(
        writer,
        reader,
        leases,
        TransactionalCheckpointResultSink(writer),
        clock,
        settings=CancellationCoordinatorSettings(5.0, 5.0, 5.0),
    )
    return coordinator, leases


def _claim(
    leases: WorkLeaseService,
    work_item_id: WorkItemId,
    *,
    sequence: int,
    work_row_version: int,
    node_row_version: int,
    run_row_version: int,
) -> Any:
    return leases.acquire(
        AcquireWorkLeaseRequest(
            run_id=RUN_ID,
            node_id=NODE_ID,
            work_item_id=work_item_id,
            expected_attempt_number=AttemptNumber(1),
            expected_work_row_version=work_row_version,
            expected_node_row_version=node_row_version,
            expected_run_row_version=run_row_version,
            lease_owner="cancel-owner",
            runner_kind=RUNNER_KIND,
            worker_identity="cancel-worker",
            event=_event(sequence, "work_claimed", work_item_id),
        )
    )


def test_real_queued_run_cancels_with_one_arrow_and_reopens(tmp_path: Path) -> None:
    database_path = tmp_path / "cancellation queued %.db"
    database = _open_database(database_path)
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        _submit(
            writer,
            CreateCapturedRun(
                run_id=RUN_ID,
                pipeline_id=PIPELINE_ID,
                pipeline_version=PipelineVersion(1),
                runner_kind=RUNNER_KIND,
                runner_configuration=ConfigurationDocument(()),
                scenario_seed=None,
                node_ids=(NODE_ID,),
                created_at=_time(1),
                event=_event(1, "run_created", RUN_ID),
            ),
        )
        clock = _Clock(_time(20))
        coordinator, _leases = _coordinator(writer, SQLiteCancellationStateReader(database), clock)
        coordinator.request_cancellation(RUN_ID)
        report = coordinator.cancel()
        assert report.action is CancellationAction.CANCELLED_BEFORE_START
        assert report.run.state is RunState.CANCELLED
        assert report.run.cancellation_requested_at == _time(20)
        assert report.run.finished_at == _time(20)
        assert report.submission_ids == (WriterSubmissionId(2),)

        with database.transaction() as session:
            runs = SqlAlchemyRunRepository(session)
            run = runs.get(RUN_ID)
            assert run is not None
            assert run.state is RunState.CANCELLED
            page = SqlAlchemyExecutionEventRepository(session).list_after(
                RUN_ID, after=None, limit=10
            )
            assert [item.event_kind for item in page.items] == ["run_created", "run_cancelled"]
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()

    reopened = SQLiteDatabase.open(SQLiteDatabaseConfig(database_path))
    try:
        with reopened.engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA quick_check").scalar_one() == "ok"
            assert connection.exec_driver_sql("PRAGMA foreign_key_check").first() is None
            assert connection.exec_driver_sql("PRAGMA journal_mode").scalar_one() == "wal"
        with reopened.transaction() as session:
            run = SqlAlchemyRunRepository(session).get(RUN_ID)
            assert run is not None
            assert run.state is RunState.CANCELLED
    finally:
        reopened.close()


def test_real_running_run_cancels_after_checkpoint_and_work_cancellation(tmp_path: Path) -> None:
    database_path = tmp_path / "cancellation running ✓.db"
    database = _open_database(database_path)
    writer: SQLiteTransactionalWriter | None = None
    threads_before = threading.active_count()
    try:
        notifications = BoundedCommittedNotificationBuffer(64)
        writer = _writer_for(database, notifications)
        writer.start()
        _create_running_run(writer)
        clock = _Clock(_time(10), _time(11), _time(20))
        reader = SQLiteCancellationStateReader(database)
        coordinator, service = _coordinator(writer, reader, clock)

        lease_a = _claim(
            service,
            WORK_A,
            sequence=5,
            work_row_version=1,
            node_row_version=3,
            run_row_version=4,
        )
        committed = submit_work_result(
            TransactionalCheckpointResultSink(writer),
            ResultSubmission(
                lease_a,
                SuccessfulWorkResult(
                    AttemptSucceeded(
                        AttemptEventContext(
                            RUN_ID,
                            NODE_ID,
                            WORK_A,
                            AttemptNumber(1),
                            _time(10),
                            PlannerRunnerKind.SEQUENTIAL,
                            "cancel-worker",
                            CORRELATION,
                        ),
                        _time(12),
                    ),
                    ResultCheckpoint(PartitionKey("part-cancel-0"), 1, None, None, None),
                    ResultMetrics(5, 10, WorkMetricDelta(5, 5, 0, 10, 10)),
                ),
            ),
            lease_service=service,
        )
        assert type(committed).__name__ == "ResultSinkCommitted"

        lease_b = _claim(
            service,
            WORK_B,
            sequence=7,
            work_row_version=1,
            node_row_version=5,
            run_row_version=6,
        )
        coordinator.request_cancellation(RUN_ID)
        from paritygrid.application.execution import WorkLeaseBusyError

        with pytest.raises(WorkLeaseBusyError, match="admission is paused"):
            _claim(
                service,
                WORK_A,
                sequence=99,
                work_row_version=3,
                node_row_version=6,
                run_row_version=7,
            )
        work_outcome = coordinator.cancel_work(
            lease_b,
            finished_at=_time(13),
            detail="user requested cancellation",
        )
        assert work_outcome.result_kind.value == "cancelled"

        report = coordinator.cancel(correlation_id="cancel:real-1")
        assert report.action is CancellationAction.CANCELLED
        assert report.run.state is RunState.CANCELLED
        assert report.submission_ids == (WriterSubmissionId(9), WriterSubmissionId(10))

        with database.transaction() as session:
            work_items = SqlAlchemyWorkItemRepository(session)
            work_a = work_items.get(WORK_A)
            work_b = work_items.get(WORK_B)
            assert work_a is not None
            assert work_a.state is WorkItemState.SUCCEEDED
            assert work_b is not None
            assert work_b.state is WorkItemState.CANCELLED
            assert work_b.retry_available_at is None
            assert work_b.lease_owner is None
            attempt_b = SqlAlchemyWorkAttemptRepository(session).get(WORK_B, AttemptNumber(1))
            assert attempt_b is not None
            assert attempt_b.outcome.value == "cancelled"
            assert attempt_b.failure_classification is not None
            assert attempt_b.failure_classification.value == "user_cancellation"
            page = SqlAlchemyExecutionEventRepository(session).list_after(
                RUN_ID, after=None, limit=20
            )
            assert [item.event_kind for item in page.items] == [
                "run_created",
                "run_started",
                "work_created",
                "work_created",
                "work_claimed",
                "checkpoint_committed",
                "work_claimed",
                "work_cancelled",
                "run_cancelling",
                "run_cancelled",
            ]
        stats = notifications.stats()
        assert stats.offered == 10
        assert stats.dropped == 0

        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()
    assert threading.active_count() == threads_before


def test_pending_work_of_a_cancelled_run_is_inert(tmp_path: Path) -> None:
    database_path = tmp_path / "cancellation inert.db"
    database = _open_database(database_path)
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        _create_running_run(writer, work_items=(WORK_A,))
        clock = _Clock(_time(20))
        coordinator, _leases = _coordinator(writer, SQLiteCancellationStateReader(database), clock)
        coordinator.request_cancellation(RUN_ID)
        assert coordinator.cancel().action is CancellationAction.CANCELLED

        with pytest.raises(ExecutionStateConflictError), database.transaction() as session:
            SqlAlchemyWorkItemRepository(session).claim(
                WORK_A,
                expected_row_version=1,
                lease_owner="late-owner",
                started_at=_time(21),
                lease_expires_at=_time(50),
                runner_kind=RUNNER_KIND,
                worker_identity="late-worker",
            )
        with database.transaction() as session:
            work = SqlAlchemyWorkItemRepository(session).get(WORK_A)
            assert work is not None
            assert work.state is WorkItemState.PENDING
            assert work.lease_owner is None
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()


def _durable_snapshot(database: SQLiteDatabase) -> tuple[Any, ...]:
    with database.transaction() as session:
        runs = SqlAlchemyRunRepository(session)
        run = runs.get(RUN_ID)
        counter = runs.get_event_counter(RUN_ID)
        page = SqlAlchemyExecutionEventRepository(session).list_after(RUN_ID, after=None, limit=50)
        return (run, counter, tuple(page.items))


def test_stale_transition_rolls_back_and_leaves_exact_prior_state(tmp_path: Path) -> None:
    database_path = tmp_path / "cancellation rollback.db"
    database = _open_database(database_path)
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        _create_running_run(writer, work_items=(WORK_A,))
        before = _durable_snapshot(database)

        with pytest.raises(ExecutionStaleRowVersionError):
            _submit(
                writer,
                TransitionRun(
                    run_id=RUN_ID,
                    expected_run_row_version=99,
                    target_state=RunState.CANCELLING,
                    transitioned_at=_time(20),
                    final_reconciliation_fingerprint=None,
                    event=_event(99, "run_cancelling", RUN_ID, second=20),
                ),
            )
        assert writer.snapshot().state is WriterState.RUNNING
        assert _durable_snapshot(database) == before

        clock = _Clock(_time(20))
        coordinator, _leases = _coordinator(writer, SQLiteCancellationStateReader(database), clock)
        coordinator.request_cancellation(RUN_ID)
        assert coordinator.cancel().action is CancellationAction.CANCELLED
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()


def test_two_coordinators_converge_with_one_cas_winner(tmp_path: Path) -> None:
    database_path = tmp_path / "cancellation race.db"
    database = _open_database(database_path)
    writer_a: SQLiteTransactionalWriter | None = None
    writer_b: SQLiteTransactionalWriter | None = None
    try:
        writer_a = _writer_for(database)
        writer_a.start()
        _create_running_run(writer_a, work_items=(WORK_A,))
        live_reader = SQLiteCancellationStateReader(database)
        stale_snapshot = live_reader.read(RUN_ID)

        class _FrozenReader:
            def read(self, run_id: RunId, /) -> CancellationDurableState:
                del run_id
                return CancellationDurableState(
                    stale_snapshot.run,
                    stale_snapshot.next_event_sequence,
                    stale_snapshot.event_counter_row_version,
                    stale_snapshot.active_work_count,
                )

        clock_a = _Clock(_time(20))
        coordinator_a, _leases_a = _coordinator(writer_a, live_reader, clock_a)
        coordinator_a.request_cancellation(RUN_ID)
        assert coordinator_a.cancel().action is CancellationAction.CANCELLED

        notifications_b = BoundedCommittedNotificationBuffer(16)
        writer_b = _writer_for(database, notifications_b)
        writer_b.start()
        clock_b = _Clock(_time(21))
        coordinator_b, _leases_b = _coordinator(writer_b, cast(Any, _FrozenReader()), clock_b)
        coordinator_b.request_cancellation(RUN_ID)
        with pytest.raises(CancellationCoordinatorRejectedError):
            coordinator_b.cancel()
        assert writer_b.snapshot().state is WriterState.RUNNING

        coordinator_b_live, _leases_live = _coordinator(writer_b, live_reader, clock_b)
        coordinator_b_live.request_cancellation(RUN_ID)
        converged = coordinator_b_live.cancel()
        assert converged.action is CancellationAction.ALREADY_CANCELLED

        assert notifications_b.stats().offered == 0
        with database.transaction() as session:
            page = SqlAlchemyExecutionEventRepository(session).list_after(
                RUN_ID, after=None, limit=20
            )
            kinds = [item.event_kind for item in page.items]
            assert kinds.count("run_cancelling") == 1
            assert kinds.count("run_cancelled") == 1
        closed_a = writer_a.close(timeout_seconds=5.0)
        closed_b = writer_b.close(timeout_seconds=5.0)
        assert closed_a.drained
        assert closed_b.drained
        writer_a = None
        writer_b = None
    finally:
        if writer_a is not None:
            writer_a.close(timeout_seconds=5.0)
        if writer_b is not None:
            writer_b.close(timeout_seconds=5.0)
        database.close()


def test_storage_failure_fails_the_writer_closed(tmp_path: Path) -> None:
    database_path = tmp_path / "cancellation storage.db"
    database = _open_database(database_path)
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        _create_running_run(writer, work_items=(WORK_A,))
        before = _durable_snapshot(database)

        def fail_run_updates(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: object,
        ) -> None:
            if statement.startswith("UPDATE runs "):
                raise RuntimeError("simulated storage failure")

        event.listen(database.engine, "before_cursor_execute", fail_run_updates)
        try:
            clock = _Clock(_time(20))
            coordinator, _leases = _coordinator(
                writer, SQLiteCancellationStateReader(database), clock
            )
            coordinator.request_cancellation(RUN_ID)
            with pytest.raises(
                CancellationCoordinatorOutcomeUnknownError, match="durable outcome is unknown"
            ):
                coordinator.cancel()
            with pytest.raises(
                CancellationCoordinatorOutcomeUnknownError, match="recovery inspection"
            ):
                coordinator.cancel()
        finally:
            event.remove(database.engine, "before_cursor_execute", fail_run_updates)
        assert writer.snapshot().state is WriterState.FAILED
        with pytest.raises(WriterFailedError):
            writer.submit(
                TransitionRun(
                    run_id=RUN_ID,
                    expected_run_row_version=99,
                    target_state=RunState.CANCELLING,
                    transitioned_at=_time(20),
                    final_reconciliation_fingerprint=None,
                    event=_event(99, "run_cancelling", RUN_ID, second=20),
                ),
                timeout_seconds=5.0,
            )
        assert _durable_snapshot(database) == before
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()


def test_cancellation_state_reader_validates_identity_and_missing_runs(tmp_path: Path) -> None:
    from paritygrid.application.ports.execution import ExecutionRecordNotFoundError

    database_path = tmp_path / "cancellation reader.db"
    database = _open_database(database_path)
    try:
        reader = SQLiteCancellationStateReader(database)
        with pytest.raises(TypeError, match="RunId"):
            reader.read(cast(Any, "run_cancel-real"))
        with pytest.raises(ExecutionRecordNotFoundError, match="does not exist"):
            reader.read(RunId("run_cancel-missing"))
        with pytest.raises(TypeError, match="SQLiteDatabase"):
            SQLiteCancellationStateReader(cast(Any, object()))
    finally:
        database.close()


def test_cancellation_reader_pages_large_node_frontiers(tmp_path: Path) -> None:
    from paritygrid.domain.models import NodeId as _NodeId

    database_path = tmp_path / "cancellation paging %.db"
    database = _open_database(database_path)
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        node_ids = tuple(_NodeId(f"nod_cancel-{index:03d}") for index in range(101))
        _submit(
            writer,
            CreateCapturedRun(
                run_id=RUN_ID,
                pipeline_id=PIPELINE_ID,
                pipeline_version=PipelineVersion(1),
                runner_kind=RUNNER_KIND,
                runner_configuration=ConfigurationDocument(()),
                scenario_seed=None,
                node_ids=node_ids,
                created_at=_time(1),
                event=_event(1, "run_created", RUN_ID),
            ),
        )
        reader = SQLiteCancellationStateReader(database)
        state = reader.read(RUN_ID)
        assert state.run.state is RunState.QUEUED
        assert state.active_work_count == 0
        assert state.next_event_sequence.number == 2
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()
