# pyright: reportPrivateUsage=false
"""Locked-golden sequential end-to-end scenario over public Phase 2-6 contracts."""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.adapters.persistence import (
    SqlAlchemyExecutionEventRepository,
    SqlAlchemyRunRepository,
    SqlAlchemyWorkAttemptRepository,
    SqlAlchemyWorkItemRepository,
    SQLiteCancellationStateReader,
    SQLiteConfigurationError,
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    SQLiteFinalizationStateReader,
    SQLitePauseStateReader,
    SQLiteRecoveryStateReader,
    create_session_factory,
)
from paritygrid.application.execution import (
    CancellationCoordinator,
    CancellationCoordinatorSettings,
    PauseAction,
    PauseCoordinator,
    PauseCoordinatorSettings,
    RecoverySettings,
    RunnerStatus,
    SequentialRunner,
    StartupRecoveryScanner,
    TransactionalCheckpointResultSink,
)
from paritygrid.application.execution.finalization import (
    FinalizationAction,
    FinalizationOutcome,
    FinalizationSettings,
    RunFinalizer,
)
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import NodeId
from paritygrid.domain.pipeline import PartitionKey
from paritygrid.quality.sequential_scenario import (
    ARTIFACT_BODY,
    CANCEL_RUN_ID,
    INTERRUPTED_RUN_ID,
    MAIN_RUN_ID,
    NORMALIZE_NODE,
    PLAN_NODE_ORDER,
    SCENARIO_VERSION,
    SCRIPT,
    SOURCE_NODE,
    VALIDATE_NODE,
    ScenarioExecutor,
    ScenarioHarness,
    ScenarioHarnessCleanupError,
    artifact_bytes,
    compiled_plan,
    prepare_harness,
    scheduler_state_after,
    work_item_id,
)

GOLDEN_PATH = Path(__file__).parent.parent / "fixtures" / "sequential_e2e" / "expected.json"


def _load_golden() -> dict[str, Any]:
    with GOLDEN_PATH.open(encoding="utf-8") as handle:
        return cast(dict[str, Any], json.load(handle))


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[ScenarioHarness]:
    prepared = prepare_harness(
        tmp_path / "sequential scenario ✓.db",
        tmp_path / "artifacts",
        tmp_path / "analytics scenario %.duckdb",
    )
    try:
        yield prepared
    finally:
        prepared.close()


def _run_events(database: SQLiteDatabase, run_id: Any) -> list[str]:
    sessions = create_session_factory(database.engine)
    session = sessions()
    session.begin()
    try:
        page = SqlAlchemyExecutionEventRepository(session).list_after(run_id, after=None, limit=100)
        kinds = [item.event_kind for item in page.items]
        cursor = page.next_cursor
        while cursor is not None:
            page = SqlAlchemyExecutionEventRepository(session).list_after(
                run_id, after=cursor, limit=100
            )
            kinds.extend(item.event_kind for item in page.items)
            cursor = page.next_cursor
        return kinds
    finally:
        session.close()


def _work_state(database: SQLiteDatabase, work_id: Any) -> Any:
    sessions = create_session_factory(database.engine)
    session = sessions()
    session.begin()
    try:
        return SqlAlchemyWorkItemRepository(session).get(work_id)
    finally:
        session.close()


def _attempt_outcomes(database: SQLiteDatabase, work_id: Any) -> list[str]:
    sessions = create_session_factory(database.engine)
    session = sessions()
    session.begin()
    try:
        page = SqlAlchemyWorkAttemptRepository(session).list_for_work_item(
            work_id, limit=100, after=None
        )
        outcomes = [item.outcome.value for item in page.items]
        cursor = page.next_cursor
        while cursor is not None:
            page = SqlAlchemyWorkAttemptRepository(session).list_for_work_item(
                work_id, limit=100, after=cursor
            )
            outcomes.extend(item.outcome.value for item in page.items)
            cursor = page.next_cursor
        return outcomes
    finally:
        session.close()


def _run_record(database: SQLiteDatabase, run_id: Any) -> Any:
    sessions = create_session_factory(database.engine)
    session = sessions()
    session.begin()
    try:
        record = SqlAlchemyRunRepository(session).get(run_id)
        assert record is not None
        return record
    finally:
        session.close()


def test_main_scenario_matches_locked_golden_expectations(harness: ScenarioHarness) -> None:
    golden = _load_golden()["main"]
    database = harness.database
    plan = compiled_plan(harness)
    executor = ScenarioExecutor(harness, MAIN_RUN_ID, renew_at=SOURCE_NODE)
    pause = PauseCoordinator(
        harness.writer,
        SQLitePauseStateReader(database),
        executor.lease_service,
        harness.clock,
        settings=PauseCoordinatorSettings(5.0, 5.0),
    )
    requested: list[bool] = []

    def request_pause_after_source(completed_executor: ScenarioExecutor, node_id: Any) -> None:
        if node_id == SOURCE_NODE and not requested:
            requested.append(True)
            pause.request_pause(MAIN_RUN_ID)

    object.__setattr__(executor, "_on_node_complete", request_pause_after_source)
    runner = SequentialRunner(executor, pause=pause.token)
    report = runner.run(plan)
    assert report.status is RunnerStatus.PAUSED
    assert [str(node) for node in report.started_node_ids] == golden["pause_started_nodes"]
    acknowledgement = report.pause_acknowledgement
    assert acknowledgement is not None
    paused, _pause_report = pause.pause(acknowledgement, correlation_id="e2e:pause")
    resume = pause.resume(paused, correlation_id="e2e:resume")
    assert resume.action is PauseAction.RESUMED
    final_report = runner.run(plan, state=report.scheduler_state)
    assert final_report.status is RunnerStatus.SUCCEEDED
    assert [str(node) for node in final_report.started_node_ids] == golden[
        "post_resume_started_nodes"
    ]

    finalizer = RunFinalizer(
        harness.writer,
        SQLiteFinalizationStateReader(database),
        harness.analytics,
        harness.clock,
        settings=FinalizationSettings(5.0, 5.0),
    )
    finalized = finalizer.finalize(
        MAIN_RUN_ID,
        plan_nodes=PLAN_NODE_ORDER,
        plan_fingerprint=harness.plan_fingerprint,
        correlation_id="e2e:finalize",
    )
    assert finalized.outcome is FinalizationOutcome.PARTIALLY_SUCCEEDED

    record = _run_record(database, MAIN_RUN_ID)
    assert record.state.value == golden["run_state"]
    assert record.row_version == golden["run_row_version"]
    assert str(finalized.fingerprint) == golden["final_fingerprint"]
    assert finalized.fingerprint == record.execution_evidence_fingerprint

    assert _run_events(database, MAIN_RUN_ID) == golden["event_kinds"]

    for node_id_text, partition_text, expectation in golden["work"]:
        work_id = work_item_id(MAIN_RUN_ID, NodeId(node_id_text), PartitionKey(partition_text))
        work = _work_state(database, work_id)
        assert work is not None
        assert work.state.value == expectation["state"], work_id
        assert work.completed_attempt_count == expectation["attempts"], work_id
        assert _attempt_outcomes(database, work_id) == expectation["outcomes"], work_id

    with database.transaction() as session:
        nodes = SqlAlchemyRunRepository(session).list_nodes(MAIN_RUN_ID, limit=100, after=None)
        aggregate = {
            str(node.node_id): {
                "total": node.work_total,
                "succeeded": node.work_succeeded,
                "quarantined": node.work_quarantined,
                "retry_count": node.retry_count,
            }
            for node in nodes.items
        }
    assert aggregate == golden["node_aggregates"]

    assert sha256(artifact_bytes()).hexdigest() == golden["artifact_sha256"]
    receipt = harness.artifact_receipts[
        work_item_id(
            MAIN_RUN_ID,
            NodeId(golden["artifact_work"][0]),
            PartitionKey(golden["artifact_work"][1]),
        )
    ]
    assert receipt.sha256 == golden["artifact_sha256"]
    assert receipt.byte_size == len(ARTIFACT_BODY)

    stats = harness.notifications.stats()
    assert stats.offered >= golden["notification_offered"]
    assert stats.dropped == 0

    replay = finalizer.finalize(
        MAIN_RUN_ID,
        plan_nodes=PLAN_NODE_ORDER,
        plan_fingerprint=harness.plan_fingerprint,
    )
    assert replay.action is FinalizationAction.ALREADY_FINALIZED
    assert replay.fingerprint == finalized.fingerprint

    events_before = _run_events(database, MAIN_RUN_ID)
    finalizer.finalize(
        MAIN_RUN_ID, plan_nodes=PLAN_NODE_ORDER, plan_fingerprint=harness.plan_fingerprint
    )
    assert _run_events(database, MAIN_RUN_ID) == events_before


def test_cancellation_scenario_cancels_active_work_and_run(harness: ScenarioHarness) -> None:
    golden = _load_golden()["cancellation"]
    database = harness.database
    plan = compiled_plan(harness)
    cancellation_executor = ScenarioExecutor(harness, CANCEL_RUN_ID)
    cancellation = CancellationCoordinator(
        harness.writer,
        SQLiteCancellationStateReader(database),
        cancellation_executor.lease_service,
        TransactionalCheckpointResultSink(harness.writer),
        harness.clock,
        settings=CancellationCoordinatorSettings(5.0, 5.0, 5.0),
    )

    def cancel_boundary(executor: ScenarioExecutor, lease: Any) -> None:
        cancellation.request_cancellation(CANCEL_RUN_ID)
        outcome = cancellation.cancel_work(lease, finished_at=harness.clock.advance(1))
        assert outcome.result_kind.value == "cancelled"

    object.__setattr__(cancellation_executor, "_cancel_boundary", cancel_boundary)
    runner = SequentialRunner(cancellation_executor, cancellation=cancellation.token)
    report = runner.run(plan)
    assert report.status is RunnerStatus.CANCELLED
    assert [str(node) for node in report.started_node_ids] == golden["started_nodes"]
    cancelled = cancellation.cancel(correlation_id="e2e:cancel")
    assert cancelled.action.value == golden["action"]
    record = _run_record(database, CANCEL_RUN_ID)
    assert record.state is RunState.CANCELLED
    assert record.cancellation_requested_at is not None
    assert record.finished_at is not None
    assert _run_events(database, CANCEL_RUN_ID) == golden["event_kinds"]
    cancelled_work_id = work_item_id(
        CANCEL_RUN_ID,
        NodeId(golden["cancelled_work"][0]),
        PartitionKey(golden["cancelled_work"][1]),
    )
    work = _work_state(database, cancelled_work_id)
    assert work.state.value == "cancelled"
    assert _attempt_outcomes(database, cancelled_work_id) == golden["cancelled_work_outcomes"]


def test_interrupted_scenario_recovers_from_durable_evidence(harness: ScenarioHarness) -> None:
    golden = _load_golden()["interrupted"]
    database = harness.database
    plan = compiled_plan(harness)
    executor = ScenarioExecutor(harness, INTERRUPTED_RUN_ID, abandon_at=VALIDATE_NODE)
    runner = SequentialRunner(executor)
    report = runner.run(plan)
    assert report.status is RunnerStatus.FAILED
    assert _run_record(database, INTERRUPTED_RUN_ID).state is RunState.RUNNING

    scanner = StartupRecoveryScanner(
        harness.writer,
        SQLiteRecoveryStateReader(database, harness.artifact_root),
        harness.clock,
        settings=RecoverySettings(5.0, 5.0),
    )
    scan = scanner.scan(INTERRUPTED_RUN_ID)
    assert scan.status.value == golden["recovery_status"]
    assert sorted(f.kind.value for f in scan.findings) == golden["recovery_findings"]

    harness.clock.advance(601)
    recovery = scanner.recover(INTERRUPTED_RUN_ID, correlation_id="e2e:recover")
    assert recovery.applied == golden["recovered_count"]
    abandoned_work = work_item_id(
        INTERRUPTED_RUN_ID, VALIDATE_NODE, PartitionKey(golden["abandoned_work"])
    )
    work = _work_state(database, abandoned_work)
    assert work.state.value == "retry_wait"
    assert _attempt_outcomes(database, abandoned_work) == golden["abandoned_work_outcomes"]

    resumed = ScenarioExecutor(harness, INTERRUPTED_RUN_ID)
    resumed_runner = SequentialRunner(resumed)
    durable_completed = (SOURCE_NODE, NORMALIZE_NODE)
    final = resumed_runner.run(plan, state=scheduler_state_after(plan, durable_completed))
    assert final.status is RunnerStatus.SUCCEEDED

    finalizer = RunFinalizer(
        harness.writer,
        SQLiteFinalizationStateReader(database),
        harness.analytics,
        harness.clock,
        settings=FinalizationSettings(5.0, 5.0),
    )
    finalized = finalizer.finalize(
        INTERRUPTED_RUN_ID,
        plan_nodes=PLAN_NODE_ORDER,
        plan_fingerprint=harness.plan_fingerprint,
    )
    assert finalized.outcome is FinalizationOutcome.PARTIALLY_SUCCEEDED
    assert str(finalized.fingerprint) == golden["final_fingerprint"]
    assert _run_events(database, INTERRUPTED_RUN_ID) == golden["event_kinds"]


def test_scenario_reopen_preserves_exact_durable_state(tmp_path: Path) -> None:
    golden = _load_golden()["reopen"]
    database_path = tmp_path / "sequential reopen %.db"
    harness = prepare_harness(
        database_path, tmp_path / "artifacts", tmp_path / "analytics reopen.duckdb"
    )
    try:
        plan = compiled_plan(harness)
        executor = ScenarioExecutor(harness, MAIN_RUN_ID)
        runner = SequentialRunner(executor)
        assert runner.run(plan).status is RunnerStatus.SUCCEEDED
        finalizer = RunFinalizer(
            harness.writer,
            SQLiteFinalizationStateReader(harness.database),
            harness.analytics,
            harness.clock,
            settings=FinalizationSettings(5.0, 5.0),
        )
        finalized = finalizer.finalize(
            MAIN_RUN_ID, plan_nodes=PLAN_NODE_ORDER, plan_fingerprint=harness.plan_fingerprint
        )
        events_before = _run_events(harness.database, MAIN_RUN_ID)
        fingerprint = finalized.fingerprint
    finally:
        harness.close()

    reopened = SQLiteDatabase.open(SQLiteDatabaseConfig(database_path))
    try:
        with reopened.engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA quick_check").scalar_one() == "ok"
            assert connection.exec_driver_sql("PRAGMA foreign_key_check").first() is None
        with reopened.transaction() as session:
            run = SqlAlchemyRunRepository(session).get(MAIN_RUN_ID)
            assert run is not None
            assert run.state.value == golden["run_state"]
            assert run.execution_evidence_fingerprint == fingerprint
        assert _run_events(reopened, MAIN_RUN_ID) == events_before
    finally:
        reopened.close()


def test_scenario_script_is_fully_covered_by_reviewed_steps() -> None:
    outcomes = {step.outcome.value for step in SCRIPT}
    assert outcomes == {"success", "retry_then_success", "quarantine"}
    assert len(SCRIPT) == 6
    assert SCENARIO_VERSION == 1


def test_scenario_helpers_and_scripted_paths_are_exact(tmp_path: Path) -> None:
    from paritygrid.domain.pipeline import PartitionKey
    from paritygrid.quality.sequential_scenario import (
        PARTITION_NODE,
        SOURCE_NODE,
        ScenarioExecutor,
        ScriptedOutcome,
        read_frontier,
        work_item_id,
    )

    harness = prepare_harness(
        tmp_path / "helpers.db", tmp_path / "artifacts", tmp_path / "analytics.duckdb"
    )
    try:
        plan = compiled_plan(harness)
        assert harness.partitions_of(PARTITION_NODE) == (
            PartitionKey("partition-00000000"),
            PartitionKey("partition-00000001"),
        )
        assert harness.partitions_of(SOURCE_NODE) == (PartitionKey("partition-00000000"),)
        assert (
            read_frontier(
                harness.database,
                MAIN_RUN_ID,
                SOURCE_NODE,
                work_item_id(MAIN_RUN_ID, SOURCE_NODE, PartitionKey("partition-00000000")),
            )
            is None
        )
        executor = ScenarioExecutor(harness, MAIN_RUN_ID)
        executor.close()
        from paritygrid.quality import sequential_scenario as module

        assert (
            module._outcome_for(
                next(step for step in SCRIPT if step.outcome.value == "retry_then_success"),
                2,
            )
            is ScriptedOutcome.SUCCESS
        )
        assert (
            module._outcome_for(
                next(step for step in SCRIPT if step.outcome.value == "quarantine"),
                5,
            )
            is ScriptedOutcome.QUARANTINE
        )
        del plan
    finally:
        harness.close()


def test_scenario_close_reports_an_undrained_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = prepare_harness(
        tmp_path / "undrained.db",
        tmp_path / "undrained-artifacts",
        tmp_path / "undrained.duckdb",
    )
    writer_type = type(prepared.writer)
    original_close = writer_type.close

    def report_undrained(self: Any, *, timeout_seconds: float) -> Any:
        return replace(
            original_close(self, timeout_seconds=timeout_seconds),
            drained=False,
        )

    monkeypatch.setattr(writer_type, "close", report_undrained)
    with pytest.raises(ScenarioHarnessCleanupError, match="every owned resource"):
        prepared.close()
    assert prepared.writer.snapshot().state.value == "closed"


def test_scenario_close_continues_after_analytics_cleanup_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = prepare_harness(
        tmp_path / "cleanup failure.db",
        tmp_path / "cleanup-failure-artifacts",
        tmp_path / "cleanup failure.duckdb",
    )
    coordinator_type = type(prepared.analytics_coordinator)
    original_close = coordinator_type.close

    def fail_after_close(self: Any) -> None:
        original_close(self)
        raise RuntimeError("synthetic analytics cleanup failure")

    monkeypatch.setattr(coordinator_type, "close", fail_after_close)
    with pytest.raises(ScenarioHarnessCleanupError, match="every owned resource"):
        prepared.close()
    assert prepared.writer.snapshot().state.value == "closed"
    with (
        pytest.raises(SQLiteConfigurationError, match="lifecycle is closed"),
        prepared.database.transaction(),
    ):
        pass
