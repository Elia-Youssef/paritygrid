"""Golden DuckDB run-statistics tests over authoritative execution DTOs."""

# pyright: reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownLambdaType=false

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from paritygrid.adapters.analytics import (
    DuckDBLifecycleCoordinator,
    DuckDBRunStatisticsQueryEngine,
)
from paritygrid.adapters.analytics import run_statistics as adapter
from paritygrid.application.ports import (
    AnalyticalDatabaseConfig,
    AnalyticalDatabaseStorageError,
    AnalyticalViewCatalogSnapshot,
    ConfigurationDocument,
    RunNodeRecord,
    RunNodeStatus,
    RunRecord,
    RunStatisticsCorruptionError,
    RunStatisticsSourceSnapshot,
    RunStatisticsStateError,
    RunStatisticsStorageError,
    WorkAttemptRecord,
    WorkItemRecord,
)
from paritygrid.application.ports.execution import AttemptOutcome
from paritygrid.domain.execution import FailureClassification, RunState, WorkItemState
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


def _timestamp(microseconds: int) -> UtcTimestamp:
    return UtcTimestamp(
        datetime(2026, 8, 13, 12, tzinfo=UTC) + timedelta(microseconds=microseconds)
    )


def _work(
    identity: str,
    node_id: str,
    state: WorkItemState,
    completed_attempt_count: int,
) -> WorkItemRecord:
    return WorkItemRecord(
        WorkItemId(identity),
        RunId("run_statistics"),
        NodeId(node_id),
        PartitionKey("all"),
        state,
        1,
        completed_attempt_count,
        0,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        _timestamp(0),
        _timestamp(1),
    )


def _attempt(
    work_item_id: str,
    number: int,
    outcome: AttemptOutcome,
    duration: int,
) -> WorkAttemptRecord:
    return WorkAttemptRecord(
        WorkItemId(work_item_id),
        AttemptNumber(number),
        _timestamp(1),
        _timestamp(1 + duration),
        "threaded",
        "worker-value",
        outcome,
        None if outcome is AttemptOutcome.SUCCEEDED else FailureClassification.UNKNOWN,
        "redacted-value",
        None,
        0,
        0,
        Duration(duration),
    )


def _source() -> RunStatisticsSourceSnapshot:
    run_id = RunId("run_statistics")
    run = RunRecord(
        run_id,
        PipelineId("pip_statistics"),
        PipelineVersion(1),
        "threaded",
        ConfigurationDocument.from_mapping({"private": "canary-value"}),
        RunState.RUNNING,
        7,
        None,
        _timestamp(0),
        _timestamp(1),
        None,
        None,
        None,
        None,
        None,
    )
    nodes = (
        RunNodeRecord(
            run_id,
            NodeId("nod_alpha"),
            RunNodeStatus.RUNNING,
            4,
            2,
            0,
            0,
            1,
            1,
            0,
            0,
            10,
            8,
            2,
            100,
            80,
            1,
            Duration(60),
            _timestamp(1),
            None,
        ),
        RunNodeRecord(
            run_id,
            NodeId("nod_beta"),
            RunNodeStatus.RUNNING,
            2,
            1,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            Duration(0),
            None,
            None,
        ),
    )
    work_items = (
        _work("wrk_itemaa", "nod_alpha", WorkItemState.SUCCEEDED, 2),
        _work("wrk_itemab", "nod_alpha", WorkItemState.QUARANTINED, 1),
        _work("wrk_itemba", "nod_beta", WorkItemState.PENDING, 0),
    )
    attempts = (
        _attempt("wrk_itemaa", 1, AttemptOutcome.RETRY_SCHEDULED, 10),
        _attempt("wrk_itemaa", 2, AttemptOutcome.SUCCEEDED, 20),
        _attempt("wrk_itemab", 1, AttemptOutcome.QUARANTINED, 30),
    )
    return RunStatisticsSourceSnapshot(run, nodes, work_items, attempts)


@pytest.fixture
def engine(tmp_path: Path) -> tuple[DuckDBRunStatisticsQueryEngine, DuckDBLifecycleCoordinator]:
    lifecycle = DuckDBLifecycleCoordinator(
        AnalyticalDatabaseConfig((tmp_path / "statistics.duckdb").resolve())
    )
    lifecycle.open()
    return DuckDBRunStatisticsQueryEngine(lifecycle), lifecycle


def test_golden_summary_and_node_percentiles_match_python_reference(
    engine: tuple[DuckDBRunStatisticsQueryEngine, DuckDBLifecycleCoordinator],
) -> None:
    query, lifecycle = engine
    snapshot = query.rebuild(_source())
    summary = query.get_summary(snapshot)
    first = query.list_nodes(snapshot, limit=1)
    second = query.list_nodes(snapshot, limit=1, after=first.next_cursor)

    assert (
        summary.work_total,
        summary.work_pending,
        summary.work_succeeded,
        summary.work_quarantined,
        summary.attempt_count,
        summary.retry_count,
        summary.duration_microseconds,
    ) == (3, 1, 1, 1, 3, 1, 60)
    assert (
        summary.attempt_latency_p50_microseconds,
        summary.attempt_latency_p95_microseconds,
        summary.attempt_latency_p99_microseconds,
    ) == (20, 30, 30)
    assert first.items[0].node_id == NodeId("nod_alpha")
    assert (
        first.items[0].attempt_latency_p50_microseconds,
        first.items[0].attempt_latency_p95_microseconds,
        first.items[0].attempt_latency_p99_microseconds,
    ) == (20, 30, 30)
    assert first.next_cursor == NodeId("nod_alpha")
    assert second.items[0].node_id == NodeId("nod_beta")
    assert second.items[0].attempt_latency_p50_microseconds is None
    assert second.next_cursor is None
    assert snapshot.source_sha256 == _source().source_sha256
    assert len(snapshot.view_catalog.views) == 2
    lifecycle.close()


def test_rebuild_invalidates_old_snapshot_and_removes_old_rows(
    engine: tuple[DuckDBRunStatisticsQueryEngine, DuckDBLifecycleCoordinator],
) -> None:
    query, lifecycle = engine
    original = query.rebuild(_source())
    replacement_source = _source()
    replacement_source = RunStatisticsSourceSnapshot(
        replace(replacement_source.run, row_version=8),
        replacement_source.nodes,
        replacement_source.work_items,
        replacement_source.attempts,
    )
    replacement = query.rebuild(replacement_source)

    with pytest.raises(RunStatisticsStateError):
        query.get_summary(original)
    assert query.get_summary(replacement).run_version == 8
    lifecycle.close()


def test_installed_view_or_source_tampering_fails_closed(
    engine: tuple[DuckDBRunStatisticsQueryEngine, DuckDBLifecycleCoordinator],
) -> None:
    query, lifecycle = engine
    snapshot = query.rebuild(_source())
    lifecycle._execute("DROP VIEW pgv_run_statistics_20_summary_v1")
    with pytest.raises(RunStatisticsCorruptionError):
        query.get_summary(snapshot)

    snapshot = query.rebuild(_source())
    lifecycle._execute(
        "UPDATE paritygrid_statistics_node_source SET work_total = 9 WHERE node_id = 'nod_alpha'"
    )
    with pytest.raises(RunStatisticsCorruptionError):
        query.get_summary(snapshot)

    snapshot = query.rebuild(_source())
    lifecycle._execute(
        "UPDATE paritygrid_statistics_node_source SET records_read = 11 WHERE node_id = 'nod_alpha'"
    )
    with pytest.raises(RunStatisticsCorruptionError, match="summary differs"):
        query.get_summary(snapshot)

    snapshot = query.rebuild(_source())
    lifecycle._execute("DROP VIEW pgv_run_statistics_10_nodes_v1")
    with pytest.raises(RunStatisticsCorruptionError, match="views are corrupt"):
        query.list_nodes(snapshot, limit=10)
    lifecycle.close()


def test_minimal_snapshot_has_zero_attempt_metrics(
    engine: tuple[DuckDBRunStatisticsQueryEngine, DuckDBLifecycleCoordinator],
) -> None:
    query, lifecycle = engine
    original = _source()
    node = replace(
        original.nodes[1],
        work_total=0,
        work_pending=0,
        row_version=1,
        status=RunNodeStatus.PENDING,
    )
    source = RunStatisticsSourceSnapshot(original.run, (node,), (), ())

    snapshot = query.rebuild(source)
    summary = query.get_summary(snapshot)
    page = query.list_nodes(snapshot, limit=10)

    assert summary.work_total == 0
    assert summary.attempt_count == 0
    assert summary.attempt_latency_p50_microseconds is None
    assert page.items[0].attempt_count == 0
    lifecycle.close()


def test_public_boundary_types_and_stale_catalog_are_rejected(
    engine: tuple[DuckDBRunStatisticsQueryEngine, DuckDBLifecycleCoordinator],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    query, lifecycle = engine
    with pytest.raises(TypeError):
        DuckDBRunStatisticsQueryEngine(object())  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        query.rebuild(object())  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        query.get_summary(object())  # type: ignore[arg-type]
    snapshot = query.rebuild(_source())
    with pytest.raises(TypeError, match="cursor"):
        query.list_nodes(snapshot, limit=10, after="nod_alpha")  # type: ignore[arg-type]
    monkeypatch.setattr(
        type(query._registry),
        "snapshot",
        lambda _self: AnalyticalViewCatalogSnapshot(()),
    )
    with pytest.raises(RunStatisticsCorruptionError, match="views changed"):
        query.get_summary(snapshot)
    with pytest.raises(RunStatisticsCorruptionError, match="views changed"):
        query.list_nodes(snapshot, limit=10)
    lifecycle.close()


def test_summary_cardinality_and_mapping_corruption_are_rejected(
    engine: tuple[DuckDBRunStatisticsQueryEngine, DuckDBLifecycleCoordinator],
) -> None:
    query, lifecycle = engine
    snapshot = query.rebuild(_source())
    lifecycle._execute("DELETE FROM paritygrid_statistics_run_source")
    with pytest.raises(RunStatisticsCorruptionError, match="cardinality"):
        query.get_summary(snapshot)

    snapshot = query.rebuild(_source())
    lifecycle._execute("UPDATE paritygrid_statistics_run_source SET state = 'bad'")
    with pytest.raises(RunStatisticsCorruptionError, match="malformed"):
        query.get_summary(snapshot)

    snapshot = query.rebuild(_source())
    lifecycle._execute(
        "UPDATE paritygrid_statistics_node_source SET status = 'bad' WHERE node_id = 'nod_alpha'"
    )
    with pytest.raises(RunStatisticsCorruptionError, match="malformed"):
        query.list_nodes(snapshot, limit=10)
    lifecycle.close()


def test_storage_failures_are_translated_without_database_context(
    engine: tuple[DuckDBRunStatisticsQueryEngine, DuckDBLifecycleCoordinator],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    query, lifecycle = engine
    snapshot = query.rebuild(_source())
    lifecycle.close()
    with pytest.raises(RunStatisticsStorageError, match="query failed") as summary_error:
        query.get_summary(snapshot)
    assert summary_error.value.__cause__ is None

    lifecycle.open()
    snapshot = query.rebuild(_source())
    lifecycle.close()
    with pytest.raises(RunStatisticsStorageError, match="query failed"):
        query.list_nodes(snapshot, limit=10)

    lifecycle.open()
    monkeypatch.setattr(
        DuckDBLifecycleCoordinator,
        "recreate",
        lambda _self: (_ for _ in ()).throw(
            AnalyticalDatabaseStorageError("canary storage context")
        ),
    )
    with pytest.raises(RunStatisticsStorageError, match="rebuild failed") as rebuild_error:
        query.rebuild(_source())
    assert "canary" not in str(rebuild_error.value)
    lifecycle.close()


def test_source_count_and_node_metric_tampering_are_rejected(
    engine: tuple[DuckDBRunStatisticsQueryEngine, DuckDBLifecycleCoordinator],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    query, lifecycle = engine
    source = _source()
    snapshot = query.rebuild(source)
    lifecycle._execute(
        "UPDATE paritygrid_statistics_node_source SET records_read = 99 WHERE node_id = 'nod_alpha'"
    )
    with pytest.raises(RunStatisticsCorruptionError, match="differ from source"):
        query.list_nodes(snapshot, limit=10)

    monkeypatch.setattr(
        DuckDBLifecycleCoordinator,
        "_fetch_all",
        lambda _self, _statement, _parameters=None: ((0,),),
    )
    with pytest.raises(RunStatisticsCorruptionError, match="source counts"):
        query._require_source_counts(source)
    lifecycle.close()


def test_defensive_row_helpers_and_batch_failure(
    engine: tuple[DuckDBRunStatisticsQueryEngine, DuckDBLifecycleCoordinator],
) -> None:
    _, lifecycle = engine
    with pytest.raises(RunStatisticsCorruptionError, match="summary row"):
        adapter._summary_from_row(())
    with pytest.raises(RunStatisticsCorruptionError, match="summary row"):
        adapter._summary_from_row((object(),) * len(adapter._SUMMARY_COLUMNS))
    with pytest.raises(RunStatisticsCorruptionError, match="node statistics row"):
        adapter._node_from_row(())
    with pytest.raises(RunStatisticsCorruptionError, match="node statistics row"):
        adapter._node_from_row((object(),) * len(adapter._NODE_COLUMNS))
    with pytest.raises(TypeError, match="timestamp"):
        adapter._timestamp(1)
    with pytest.raises(RunStatisticsCorruptionError, match="count"):
        adapter._single_count(())
    with pytest.raises(TypeError, match="view catalog"):
        adapter._catalog_sha256(object())
    with pytest.raises(AnalyticalDatabaseStorageError, match="batch"):
        lifecycle._execute_many("INSERT INTO missing VALUES (?)", ((1,),))
    lifecycle.close()
