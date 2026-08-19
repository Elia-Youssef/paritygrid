# pyright: reportPrivateUsage=false
"""One-shot generator for the committed sequential scenario golden asset."""

from __future__ import annotations

import json
import sys
import tempfile
from hashlib import sha256
from pathlib import Path
from typing import Any

from paritygrid.adapters.persistence import (
    SqlAlchemyExecutionEventRepository,
    SqlAlchemyRunRepository,
    SqlAlchemyWorkAttemptRepository,
    SqlAlchemyWorkItemRepository,
    SQLiteCancellationStateReader,
    SQLiteFinalizationStateReader,
    SQLitePauseStateReader,
    SQLiteRecoveryStateReader,
)
from paritygrid.application.execution import (
    CancellationCoordinator,
    CancellationCoordinatorSettings,
    PauseCoordinator,
    PauseCoordinatorSettings,
    RecoverySettings,
    RunnerStatus,
    SequentialRunner,
    StartupRecoveryScanner,
    TransactionalCheckpointResultSink,
)
from paritygrid.application.execution.finalization import (
    FinalizationSettings,
    RunFinalizer,
)
from paritygrid.domain.models import NodeId
from paritygrid.domain.pipeline import PartitionKey
from paritygrid.quality.sequential_scenario import (
    ARTIFACT_BODY,
    CANCEL_RUN_ID,
    INTERRUPTED_RUN_ID,
    MAIN_RUN_ID,
    NORMALIZE_NODE,
    PLAN_NODE_ORDER,
    SCRIPT,
    SOURCE_NODE,
    VALIDATE_NODE,
    ScenarioExecutor,
    compiled_plan,
    prepare_harness,
    scheduler_state_after,
    work_item_id,
)


def _run_events(harness: Any, run_id: Any) -> list[str]:
    with harness.database.transaction() as session:
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


def _attempt_outcomes(harness: Any, work_id: Any) -> list[str]:
    with harness.database.transaction() as session:
        page = SqlAlchemyWorkAttemptRepository(session).list_for_work_item(
            work_id, limit=100, after=None
        )
        return [item.outcome.value for item in page.items]


def _work_state(harness: Any, work_id: Any) -> Any:
    with harness.database.transaction() as session:
        return SqlAlchemyWorkItemRepository(session).get(work_id)


def _run_record(harness: Any, run_id: Any) -> Any:
    with harness.database.transaction() as session:
        return SqlAlchemyRunRepository(session).get(run_id)


def main() -> int:
    tmp = Path(tempfile.mkdtemp())
    golden: dict[str, Any] = {"main": {}, "cancellation": {}, "interrupted": {}, "reopen": {}}
    harness = prepare_harness(tmp / "gen.db", tmp / "artifacts", tmp / "an.duckdb")
    plan = compiled_plan(harness)

    executor = ScenarioExecutor(harness, MAIN_RUN_ID, renew_at=SOURCE_NODE)
    pause = PauseCoordinator(
        harness.writer,
        SQLitePauseStateReader(harness.database),
        executor.lease_service,
        harness.clock,
        settings=PauseCoordinatorSettings(5.0, 5.0),
    )
    requested: list[bool] = []

    def request_pause_after_source(completed_executor: ScenarioExecutor, node_id: Any) -> None:
        if node_id == SOURCE_NODE and not requested:
            requested.append(True)
            pause.request_pause(MAIN_RUN_ID)

    executor._on_node_complete = request_pause_after_source  # type: ignore[attr-defined]
    runner = SequentialRunner(executor, pause=pause.token)
    report = runner.run(plan)
    assert report.status is RunnerStatus.PAUSED
    golden["main"]["pause_started_nodes"] = [str(node) for node in report.started_node_ids]
    paused, _pause_report = pause.pause(report.pause_acknowledgement, correlation_id="e2e:pause")
    resume = pause.resume(paused, correlation_id="e2e:resume")
    assert resume.action.value == "resumed"
    final_report = runner.run(plan, state=report.scheduler_state)
    assert final_report.status is RunnerStatus.SUCCEEDED
    golden["main"]["post_resume_started_nodes"] = [
        str(node) for node in final_report.started_node_ids
    ]
    finalizer = RunFinalizer(
        harness.writer,
        SQLiteFinalizationStateReader(harness.database),
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
    record = _run_record(harness, MAIN_RUN_ID)
    golden["main"]["run_state"] = record.state.value
    golden["main"]["run_row_version"] = record.row_version
    golden["main"]["final_fingerprint"] = str(finalized.fingerprint)
    golden["main"]["event_kinds"] = _run_events(harness, MAIN_RUN_ID)
    work_expect = []
    for step in SCRIPT:
        work_id = work_item_id(MAIN_RUN_ID, step.node_id, step.partition_key)
        work = _work_state(harness, work_id)
        assert work is not None
        work_expect.append(
            [
                str(step.node_id),
                step.partition_key.value,
                {
                    "state": work.state.value,
                    "attempts": work.completed_attempt_count,
                    "outcomes": _attempt_outcomes(harness, work_id),
                },
            ]
        )
    golden["main"]["work"] = work_expect
    with harness.database.transaction() as session:
        nodes = SqlAlchemyRunRepository(session).list_nodes(MAIN_RUN_ID, limit=100, after=None)
        golden["main"]["node_aggregates"] = {
            str(node.node_id): {
                "total": node.work_total,
                "succeeded": node.work_succeeded,
                "quarantined": node.work_quarantined,
                "retry_count": node.retry_count,
            }
            for node in nodes.items
        }
    golden["main"]["artifact_sha256"] = sha256(ARTIFACT_BODY).hexdigest()
    golden["main"]["artifact_work"] = ["nod_e2e-export01", "partition-00000000"]
    golden["main"]["notification_offered"] = harness.notifications.stats().offered

    harness.close()
    harness = prepare_harness(tmp / "gen2.db", tmp / "artifacts2", tmp / "an2.duckdb")
    plan = compiled_plan(harness)

    cancellation_executor = ScenarioExecutor(harness, CANCEL_RUN_ID)
    cancellation = CancellationCoordinator(
        harness.writer,
        SQLiteCancellationStateReader(harness.database),
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
    cancellation_runner = SequentialRunner(cancellation_executor, cancellation=cancellation.token)
    cancellation_report = cancellation_runner.run(plan)
    assert cancellation_report.status is RunnerStatus.CANCELLED
    golden["cancellation"]["started_nodes"] = [
        str(node) for node in cancellation_report.started_node_ids
    ]
    cancelled = cancellation.cancel(correlation_id="e2e:cancel")
    golden["cancellation"]["action"] = cancelled.action.value
    golden["cancellation"]["event_kinds"] = _run_events(harness, CANCEL_RUN_ID)
    cancelled_work = work_item_id(
        CANCEL_RUN_ID, NodeId("nod_e2e-source01"), PartitionKey("partition-00000000")
    )
    golden["cancellation"]["cancelled_work"] = ["nod_e2e-source01", "partition-00000000"]
    golden["cancellation"]["cancelled_work_outcomes"] = _attempt_outcomes(harness, cancelled_work)

    harness.close()
    harness = prepare_harness(tmp / "gen3.db", tmp / "artifacts3", tmp / "an3.duckdb")
    plan = compiled_plan(harness)

    interrupted_executor = ScenarioExecutor(harness, INTERRUPTED_RUN_ID, abandon_at=VALIDATE_NODE)
    interrupted_runner = SequentialRunner(interrupted_executor)
    interrupted_report = interrupted_runner.run(plan)
    assert interrupted_report.status is RunnerStatus.FAILED
    scanner = StartupRecoveryScanner(
        harness.writer,
        SQLiteRecoveryStateReader(harness.database, harness.artifact_root),
        harness.clock,
        settings=RecoverySettings(5.0, 5.0),
    )
    scan = scanner.scan(INTERRUPTED_RUN_ID)
    golden["interrupted"]["recovery_status"] = scan.status.value
    golden["interrupted"]["recovery_findings"] = sorted(f.kind.value for f in scan.findings)
    harness.clock.advance(601)
    recovery = scanner.recover(INTERRUPTED_RUN_ID, correlation_id="e2e:recover")
    golden["interrupted"]["recovered_count"] = recovery.applied
    abandoned = work_item_id(INTERRUPTED_RUN_ID, VALIDATE_NODE, PartitionKey("partition-00000000"))
    golden["interrupted"]["abandoned_work"] = "partition-00000000"
    golden["interrupted"]["abandoned_work_outcomes"] = _attempt_outcomes(harness, abandoned)
    resumed_executor = ScenarioExecutor(harness, INTERRUPTED_RUN_ID)
    resumed_runner = SequentialRunner(resumed_executor)
    durable_completed = (SOURCE_NODE, NORMALIZE_NODE)
    resumed_final = resumed_runner.run(plan, state=scheduler_state_after(plan, durable_completed))
    assert resumed_final.status is RunnerStatus.SUCCEEDED
    interrupted_finalizer = RunFinalizer(
        harness.writer,
        SQLiteFinalizationStateReader(harness.database),
        harness.analytics,
        harness.clock,
        settings=FinalizationSettings(5.0, 5.0),
    )
    interrupted_final = interrupted_finalizer.finalize(
        INTERRUPTED_RUN_ID,
        plan_nodes=PLAN_NODE_ORDER,
        plan_fingerprint=harness.plan_fingerprint,
    )
    golden["interrupted"]["final_fingerprint"] = str(interrupted_final.fingerprint)
    golden["interrupted"]["event_kinds"] = _run_events(harness, INTERRUPTED_RUN_ID)

    golden["reopen"]["run_state"] = "partially_succeeded"

    out = Path("tests/fixtures/sequential_e2e/expected.json")
    out.write_text(json.dumps(golden, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("golden written to", out)
    print("main fingerprint:", golden["main"]["final_fingerprint"])
    print("interrupted fingerprint:", golden["interrupted"]["final_fingerprint"])
    harness.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
