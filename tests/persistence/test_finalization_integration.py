# pyright: reportPrivateUsage=false
"""Real SQLite, writer, and DuckDB integration for terminal run finalization."""

from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.adapters.analytics.duckdb import DuckDBLifecycleCoordinator
from paritygrid.adapters.analytics.run_statistics import DuckDBRunStatisticsQueryEngine
from paritygrid.adapters.persistence import (
    SqlAlchemyExecutionEventRepository,
    SqlAlchemyPipelineRepository,
    SqlAlchemyRunRepository,
    SqlAlchemyWorkItemRepository,
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    SQLiteFinalizationStateReader,
    SQLiteTransactionalWriter,
    create_session_factory,
    upgrade_to_head,
)
from paritygrid.application.execution import (
    AcquireWorkLeaseRequest,
    AttemptEventContext,
    AttemptFailed,
    AttemptSucceeded,
    FinalizationAction,
    FinalizationConflictError,
    FinalizationNotReadyError,
    FinalizationOutcome,
    FinalizationRejectedError,
    FinalizationSettings,
    ResultCheckpoint,
    ResultMetrics,
    ResultSubmission,
    RetryPolicyName,
    RetryStoppedDecision,
    RunFinalizer,
    SuccessfulWorkResult,
    TransactionalCheckpointResultSink,
    UnsuccessfulWorkResult,
    WorkLeaseService,
    WorkLeaseSettings,
    submit_work_result,
)
from paritygrid.application.planner import PlanFingerprint, PlannerRunnerKind
from paritygrid.application.ports.analytics import AnalyticalDatabaseConfig
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.run_aggregates import WorkMetricDelta
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    WriterCommand,
    WriterSettings,
    WriterState,
    WriterSubmissionId,
)
from paritygrid.application.writes import (
    BootstrapWork,
    CreateCapturedRun,
    FinalizeEmptyRunNode,
    TransitionRun,
)
from paritygrid.domain.execution import FailureClassification, FailureDisposition, RunState
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

RUN_ID = RunId("run_final-real")
PIPELINE_ID = PipelineId("pip_final-real")
NODE_A = NodeId("nod_final-a")
NODE_B = NodeId("nod_final-b")
NODE_EMPTY = NodeId("nod_final-empty")
WORK_A = WorkItemId("wrk_final-a")
WORK_B = WorkItemId("wrk_final-b")
RUNNER_KIND = "sequential"
CORRELATION = "corr-final-real"
PLAN_FINGERPRINT = PlanFingerprint("3" * 64)


class _Clock:
    def __init__(self, *values: UtcTimestamp) -> None:
        self._values = list(values)

    def now(self) -> UtcTimestamp:
        return self._values.pop(0)


def _time(second: int) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 16, 9, 0, tzinfo=UTC) + timedelta(seconds=second))


def _event(sequence: int, kind: str, subject: RunId | WorkItemId, *, second: int = 3) -> Any:
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


def _open_database(path: Path) -> SQLiteDatabase:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(path))
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PIPELINE_ID,
            display_name="Finalization pipeline",
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
    return database


def _writer_for(database: SQLiteDatabase) -> SQLiteTransactionalWriter:
    return SQLiteTransactionalWriter(
        create_session_factory(database.engine),
        settings=WriterSettings(contention_delay_seconds=0.0),
    )


def _start_run(writer: SQLiteTransactionalWriter) -> None:
    _submit(
        writer,
        CreateCapturedRun(
            run_id=RUN_ID,
            pipeline_id=PIPELINE_ID,
            pipeline_version=PipelineVersion(1),
            runner_kind=RUNNER_KIND,
            runner_configuration=ConfigurationDocument(()),
            scenario_seed=7,
            node_ids=(NODE_A, NODE_B, NODE_EMPTY),
            created_at=_time(1),
            event=_event(1, "run_created", RUN_ID, second=1),
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


def _bootstrap(
    writer: SQLiteTransactionalWriter,
    work_item_id: WorkItemId,
    node_id: NodeId,
    *,
    sequence: int,
    node_row_version: int,
    run_row_version: int,
) -> None:
    _submit(
        writer,
        BootstrapWork(
            run_id=RUN_ID,
            node_id=node_id,
            work_item_id=work_item_id,
            partition_key=PartitionKey(f"part-{work_item_id.value}"),
            input_reference=None,
            created_at=_time(3),
            expected_node_row_version=node_row_version,
            expected_run_row_version=run_row_version,
            event=_event(sequence, "work_created", work_item_id),
        ),
    )


_DEFAULT_LEASE_DURATION = Duration(3_600_000_000)


def _lease_service(
    writer: SQLiteTransactionalWriter,
    clock: Any,
    *,
    lease_duration: Duration = _DEFAULT_LEASE_DURATION,
) -> WorkLeaseService:
    return WorkLeaseService(
        writer,
        clock,
        settings=WorkLeaseSettings(
            lease_duration=lease_duration,
            admission_timeout_seconds=5.0,
            result_timeout_seconds=5.0,
        ),
    )


def _claim(
    service: WorkLeaseService,
    work_item_id: WorkItemId,
    node_id: NodeId,
    *,
    sequence: int,
    work_row_version: int = 1,
    node_row_version: int,
    run_row_version: int,
) -> Any:
    return service.acquire(
        AcquireWorkLeaseRequest(
            run_id=RUN_ID,
            node_id=node_id,
            work_item_id=work_item_id,
            expected_attempt_number=AttemptNumber(1),
            expected_work_row_version=work_row_version,
            expected_node_row_version=node_row_version,
            expected_run_row_version=run_row_version,
            lease_owner="final-owner",
            runner_kind=RUNNER_KIND,
            worker_identity="final-worker",
            event=_event(sequence, "work_claimed", work_item_id, second=4),
        )
    )


def _success_submission(lease: Any, work_item_id: WorkItemId, node_id: NodeId) -> ResultSubmission:
    return ResultSubmission(
        lease,
        SuccessfulWorkResult(
            AttemptSucceeded(
                AttemptEventContext(
                    RUN_ID,
                    node_id,
                    work_item_id,
                    AttemptNumber(1),
                    _time(4),
                    PlannerRunnerKind.SEQUENTIAL,
                    "final-worker",
                    CORRELATION,
                ),
                _time(5),
            ),
            ResultCheckpoint(PartitionKey(f"part-{work_item_id.value}"), 1, None, None, None),
            ResultMetrics(4, 8, WorkMetricDelta(4, 4, 0, 8, 8)),
        ),
    )


def _quarantine_submission(
    lease: Any, work_item_id: WorkItemId, node_id: NodeId
) -> ResultSubmission:
    return ResultSubmission(
        lease,
        UnsuccessfulWorkResult(
            AttemptFailed(
                AttemptEventContext(
                    RUN_ID,
                    node_id,
                    work_item_id,
                    AttemptNumber(1),
                    _time(4),
                    PlannerRunnerKind.SEQUENTIAL,
                    "final-worker",
                    CORRELATION,
                ),
                _time(5),
                FailureClassification.VALIDATION,
            ),
            RetryStoppedDecision(
                RetryPolicyName("bounded_exponential_v1"),
                work_item_id,
                AttemptNumber(1),
                FailureClassification.VALIDATION,
                _time(5),
                FailureDisposition.QUARANTINE,
                False,
            ),
            ResultMetrics(1, 2, WorkMetricDelta(1, 0, 1, 2, 0)),
        ),
    )


def _finalizer(
    writer: SQLiteTransactionalWriter,
    database: SQLiteDatabase,
    analytics_path: Path,
    clock: _Clock,
) -> tuple[RunFinalizer, DuckDBLifecycleCoordinator]:
    coordinator = DuckDBLifecycleCoordinator(AnalyticalDatabaseConfig(analytics_path.resolve()))
    coordinator.open()
    engine = DuckDBRunStatisticsQueryEngine(coordinator)
    finalizer = RunFinalizer(
        writer,
        SQLiteFinalizationStateReader(database),
        engine,
        clock,
        settings=FinalizationSettings(5.0, 5.0),
    )
    return finalizer, coordinator


def _prepare_partial_run(
    writer: SQLiteTransactionalWriter,
    clock: _Clock,
) -> WorkLeaseService:
    _start_run(writer)
    _bootstrap(writer, WORK_A, NODE_A, sequence=3, node_row_version=1, run_row_version=2)
    _bootstrap(writer, WORK_B, NODE_B, sequence=4, node_row_version=1, run_row_version=3)
    service = _lease_service(writer, clock)
    lease_a = _claim(service, WORK_A, NODE_A, sequence=5, node_row_version=2, run_row_version=4)
    committed = submit_work_result(
        TransactionalCheckpointResultSink(writer),
        _success_submission(lease_a, WORK_A, NODE_A),
        lease_service=service,
    )
    assert type(committed).__name__ == "ResultSinkCommitted"
    lease_b = _claim(service, WORK_B, NODE_B, sequence=7, node_row_version=2, run_row_version=6)
    quarantined = submit_work_result(
        TransactionalCheckpointResultSink(writer),
        _quarantine_submission(lease_b, WORK_B, NODE_B),
        lease_service=service,
    )
    assert type(quarantined).__name__ == "ResultSinkCommitted"
    return service


PLAN_NODES_ALL = (NODE_A, NODE_B, NODE_EMPTY)


def test_real_partial_success_finalizes_with_fingerprint_and_replay(tmp_path: Path) -> None:
    database = _open_database(tmp_path / "finalization partial ✓.db")
    analytics_path = tmp_path / "analytics final %.duckdb"
    writer: SQLiteTransactionalWriter | None = None
    threads_before = threading.active_count()
    try:
        writer = _writer_for(database)
        writer.start()
        clock = _Clock(_time(4), _time(4), _time(20), _time(21))
        _prepare_partial_run(writer, clock)
        finalizer, coordinator = _finalizer(writer, database, analytics_path, clock)
        report = finalizer.finalize(
            RUN_ID,
            plan_nodes=PLAN_NODES_ALL,
            plan_fingerprint=PLAN_FINGERPRINT,
            correlation_id="final:real-1",
        )
        assert report.action is FinalizationAction.FINALIZED
        assert report.outcome is FinalizationOutcome.PARTIALLY_SUCCEEDED
        assert report.run.state is RunState.PARTIALLY_SUCCEEDED
        assert report.run.finished_at == _time(21)
        assert report.fingerprint is not None
        assert report.submission_ids[-1] == WriterSubmissionId(10)

        with database.transaction() as session:
            runs = SqlAlchemyRunRepository(session)
            run = runs.get(RUN_ID)
            assert run is not None
            assert run.state is RunState.PARTIALLY_SUCCEEDED
            assert run.final_reconciliation_fingerprint == report.fingerprint
            page = SqlAlchemyExecutionEventRepository(session).list_after(
                RUN_ID, after=None, limit=20
            )
            kinds = [item.event_kind for item in page.items]
            assert kinds == [
                "run_created",
                "run_started",
                "work_created",
                "work_created",
                "work_claimed",
                "checkpoint_committed",
                "work_claimed",
                "work_quarantined",
                "run_node_succeeded",
                "run_partially_succeeded",
            ]

        coordinator.close()
        replay_clock = _Clock(_time(21))
        replay_finalizer, replay_coordinator = _finalizer(
            writer, database, tmp_path / "analytics replay.duckdb", replay_clock
        )
        replay = replay_finalizer.finalize(
            RUN_ID,
            plan_nodes=PLAN_NODES_ALL,
            plan_fingerprint=PLAN_FINGERPRINT,
        )
        assert replay.action is FinalizationAction.ALREADY_FINALIZED
        assert replay.fingerprint == report.fingerprint
        assert replay.events.items == ()
        assert replay.submission_ids == ()
        replay_coordinator.close()

        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()
    assert threading.active_count() == threads_before

    reopened = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "finalization partial ✓.db"))
    try:
        with reopened.engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA quick_check").scalar_one() == "ok"
            assert connection.exec_driver_sql("PRAGMA foreign_key_check").first() is None
        with reopened.transaction() as session:
            run = SqlAlchemyRunRepository(session).get(RUN_ID)
            assert run is not None
            assert run.state is RunState.PARTIALLY_SUCCEEDED
            assert run.final_reconciliation_fingerprint == report.fingerprint
    finally:
        reopened.close()


def test_real_active_work_rejects_finalization_without_mutation(tmp_path: Path) -> None:
    database = _open_database(tmp_path / "finalization active.db")
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        clock = _Clock(_time(4))
        _start_run(writer)
        _bootstrap(writer, WORK_A, NODE_A, sequence=3, node_row_version=1, run_row_version=2)
        finalizer, coordinator = _finalizer(
            writer, database, tmp_path / "analytics active.duckdb", clock
        )
        with pytest.raises(FinalizationNotReadyError, match="non-terminal work"):
            finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES_ALL, plan_fingerprint=PLAN_FINGERPRINT)
        with database.transaction() as session:
            run = SqlAlchemyRunRepository(session).get(RUN_ID)
            assert run is not None
            assert run.state is RunState.RUNNING
        coordinator.close()
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()


def test_real_checkpoint_corruption_is_a_typed_conflict(tmp_path: Path) -> None:
    database = _open_database(tmp_path / "finalization mismatch.db")
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        clock = _Clock(_time(4), _time(4), _time(20))
        _prepare_partial_run(writer, clock)
        with database.engine.connect() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys = OFF")
            connection.exec_driver_sql(
                "UPDATE work_items SET expected_checkpoint_version = 9 WHERE run_id = ?",
                (str(RUN_ID),),
            )
            connection.commit()
        finalizer, coordinator = _finalizer(
            writer, database, tmp_path / "analytics mismatch.duckdb", clock
        )
        with pytest.raises(FinalizationConflictError, match="corrupt"):
            finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES_ALL, plan_fingerprint=PLAN_FINGERPRINT)
        with database.transaction() as session:
            run = SqlAlchemyRunRepository(session).get(RUN_ID)
            assert run is not None
            assert run.state is RunState.RUNNING
        assert writer.snapshot().state is WriterState.RUNNING
        coordinator.close()
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()


def test_real_event_gap_is_a_typed_conflict(tmp_path: Path) -> None:
    from paritygrid.application.ports.consistency import ConsistencyCorruptionError

    database = _open_database(tmp_path / "finalization gap.db")
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        clock = _Clock(_time(4), _time(4), _time(20))
        _prepare_partial_run(writer, clock)
        with database.engine.connect() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys = OFF")
            connection.exec_driver_sql('DROP TRIGGER "trg_execution_events_prohibit_delete"')
            connection.exec_driver_sql(
                "DELETE FROM execution_events WHERE run_id = ? AND sequence_number = 3",
                (str(RUN_ID),),
            )
            connection.commit()
        reader = SQLiteFinalizationStateReader(database)
        with pytest.raises(ConsistencyCorruptionError, match="contiguous"):
            reader.read(RUN_ID)
        finalizer, _gap_coordinator = _finalizer(
            writer, database, tmp_path / "analytics gap.duckdb", clock
        )
        with pytest.raises(FinalizationConflictError, match="corrupt"):
            finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES_ALL, plan_fingerprint=PLAN_FINGERPRINT)
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()


def test_real_two_session_race_yields_one_winner(tmp_path: Path) -> None:
    database = _open_database(tmp_path / "finalization race.db")
    writer_a: SQLiteTransactionalWriter | None = None
    writer_b: SQLiteTransactionalWriter | None = None
    try:
        writer_a = _writer_for(database)
        writer_a.start()
        clock_a = _Clock(_time(4), _time(4), _time(20), _time(21))
        _prepare_partial_run(writer_a, clock_a)
        reader = SQLiteFinalizationStateReader(database)
        stale = reader.read(RUN_ID)

        class _FrozenReader:
            def read(self, run_id: RunId, /) -> Any:
                del run_id
                return stale

        finalizer_a, coordinator_a = _finalizer(
            writer_a, database, tmp_path / "analytics race a.duckdb", clock_a
        )
        report = finalizer_a.finalize(
            RUN_ID, plan_nodes=PLAN_NODES_ALL, plan_fingerprint=PLAN_FINGERPRINT
        )
        assert report.action is FinalizationAction.FINALIZED

        writer_b = _writer_for(database)
        writer_b.start()
        frozen_finalizer = RunFinalizer(
            writer_b,
            cast(Any, _FrozenReader()),
            DuckDBRunStatisticsQueryEngine(
                DuckDBLifecycleCoordinator(
                    AnalyticalDatabaseConfig((tmp_path / "analytics race b.duckdb").resolve())
                )
            ),
            _Clock(_time(21), _time(21)),
            settings=FinalizationSettings(5.0, 5.0),
        )
        with pytest.raises(FinalizationRejectedError):
            frozen_finalizer.finalize(
                RUN_ID, plan_nodes=PLAN_NODES_ALL, plan_fingerprint=PLAN_FINGERPRINT
            )
        assert writer_b.snapshot().state is WriterState.RUNNING

        live_finalizer, coordinator_c = _finalizer(
            writer_b, database, tmp_path / "analytics race c.duckdb", _Clock(_time(22))
        )
        converged = live_finalizer.finalize(
            RUN_ID, plan_nodes=PLAN_NODES_ALL, plan_fingerprint=PLAN_FINGERPRINT
        )
        assert converged.action is FinalizationAction.ALREADY_FINALIZED
        assert converged.fingerprint == report.fingerprint
        coordinator_a.close()
        coordinator_c.close()

        with database.transaction() as session:
            page = SqlAlchemyExecutionEventRepository(session).list_after(
                RUN_ID, after=None, limit=20
            )
            kinds = [item.event_kind for item in page.items]
            assert kinds.count("run_partially_succeeded") == 1
            assert kinds.count("run_node_succeeded") == 1
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


def test_real_finalizer_validates_identity_and_missing_runs(tmp_path: Path) -> None:
    from paritygrid.application.ports.execution import ExecutionRecordNotFoundError

    database = _open_database(tmp_path / "finalization reader.db")
    try:
        reader = SQLiteFinalizationStateReader(database)
        with pytest.raises(TypeError, match="RunId"):
            reader.read(cast(Any, "run_final-real"))
        with pytest.raises(ExecutionRecordNotFoundError, match="does not exist"):
            reader.read(RunId("run_final-missing"))
        with pytest.raises(TypeError, match="SQLiteDatabase"):
            SQLiteFinalizationStateReader(cast(Any, object()))
    finally:
        database.close()


def test_real_empty_node_command_advances_frontiers_exactly(tmp_path: Path) -> None:
    database = _open_database(tmp_path / "finalization empty.db")
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        _start_run(writer)
        _submit(
            writer,
            FinalizeEmptyRunNode(
                run_id=RUN_ID,
                node_id=NODE_EMPTY,
                expected_node_row_version=1,
                expected_run_row_version=2,
                finalized_at=_time(10),
                event=_event(3, "run_node_succeeded", RUN_ID, second=10),
            ),
        )
        with database.transaction() as session:
            runs = SqlAlchemyRunRepository(session)
            run = runs.get(RUN_ID)
            counter = runs.get_event_counter(RUN_ID)
            assert run is not None
            assert run.row_version == 3
            assert counter is not None
            assert counter.next_sequence_number == 4
            work = SqlAlchemyWorkItemRepository(session).list_for_run(RUN_ID, limit=10, after=None)
            assert work.items == ()
        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()


class _AdvanceClock:
    def __init__(self, value: UtcTimestamp) -> None:
        self.value = value

    def now(self) -> UtcTimestamp:
        return self.value


def test_real_reader_pages_large_frontiers_and_reports_missing_heads(tmp_path: Path) -> None:
    from paritygrid.application.ports.consistency import ConsistencyRecordNotFoundError

    database = _open_database(tmp_path / "finalization paging %.db")
    writer: SQLiteTransactionalWriter | None = None
    try:
        writer = _writer_for(database)
        writer.start()
        node_ids = tuple(NodeId(f"nod_final-{index:03d}") for index in range(101))
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
                event=_event(1, "run_created", RUN_ID, second=1),
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
        reader = SQLiteFinalizationStateReader(database)
        state = reader.read(RUN_ID)
        assert len(state.nodes) == 101
        assert state.work == ()

        work_ids = tuple(WorkItemId(f"wrk_final-{index:03d}") for index in range(101))
        node_zero = node_ids[0]
        for index, work_id in enumerate(work_ids):
            _submit(
                writer,
                BootstrapWork(
                    run_id=RUN_ID,
                    node_id=node_zero,
                    work_item_id=work_id,
                    partition_key=PartitionKey(f"part-{index:03d}"),
                    input_reference=None,
                    created_at=_time(2),
                    expected_node_row_version=1 + index,
                    expected_run_row_version=2 + index,
                    event=_event(3 + index, "work_created", work_id, second=2),
                ),
            )
        state = reader.read(RUN_ID)
        assert len(state.work) == 101
        assert state.attempts == ()

        cycle_count = 101
        clock: Any = _AdvanceClock(_time(5))
        service = _lease_service(writer, clock, lease_duration=Duration(1_000_000))
        service_settings = service
        del service_settings
        from paritygrid.application.writes import RecoverExpiredWork

        for cycle in range(cycle_count):
            attempt = cycle + 1
            work_id = work_ids[0]
            claim_second = 5 + 2 * cycle
            clock.value = _time(claim_second)
            lease = service.acquire(
                AcquireWorkLeaseRequest(
                    run_id=RUN_ID,
                    node_id=node_zero,
                    work_item_id=work_id,
                    expected_attempt_number=AttemptNumber(attempt),
                    expected_work_row_version=1 + 2 * cycle,
                    expected_node_row_version=102 + 2 * cycle,
                    expected_run_row_version=103 + 2 * cycle,
                    lease_owner="final-owner",
                    runner_kind=RUNNER_KIND,
                    worker_identity="final-worker",
                    event=_event(104 + 2 * cycle, "work_claimed", work_id, second=claim_second),
                )
            )
            observed = _time(claim_second + 2)
            _submit(
                writer,
                RecoverExpiredWork(
                    run_id=RUN_ID,
                    node_id=node_zero,
                    work_item_id=work_id,
                    expected_work_row_version=2 + 2 * cycle,
                    expected_attempt_number=AttemptNumber(attempt),
                    observed_at=observed,
                    retry_available_at=observed,
                    redacted_detail=None,
                    expected_node_row_version=103 + 2 * cycle,
                    expected_run_row_version=104 + 2 * cycle,
                    event=_event(
                        105 + 2 * cycle, "work_lease_expired", work_id, second=claim_second + 2
                    ),
                ),
            )
            service.retire(lease)
        state = reader.read(RUN_ID)
        assert len(state.attempts) == 101
        assert len(state.checkpoint_versions) == 101

        with database.engine.connect() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys = OFF")
            connection.exec_driver_sql('DROP TRIGGER "trg_checkpoint_heads_prohibit_delete"')
            connection.exec_driver_sql(
                "DELETE FROM checkpoint_heads WHERE run_id = ? AND partition_key = ?",
                (str(RUN_ID), "part-000"),
            )
            connection.commit()
        from paritygrid.application.ports.execution import ExecutionCorruptionError

        with pytest.raises((ConsistencyRecordNotFoundError, ExecutionCorruptionError)):
            reader.read(RUN_ID)

        closed = writer.close(timeout_seconds=5.0)
        assert closed.drained
        writer = None
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()
