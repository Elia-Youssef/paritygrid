"""Dependency-neutral run-statistics contract tests."""

from dataclasses import replace

import pytest

from paritygrid.application.ports import (
    AnalyticalViewCatalogSnapshot,
    AttemptOutcome,
    ConfigurationDocument,
    RunNodeRecord,
    RunNodeStatisticsPage,
    RunNodeStatisticsRecord,
    RunNodeStatus,
    RunRecord,
    RunStatisticsInvalidError,
    RunStatisticsQuerySnapshot,
    RunStatisticsSourceSnapshot,
    WorkAttemptRecord,
    WorkItemRecord,
    validate_run_statistics_page_limit,
)
from paritygrid.application.ports import run_statistics as contract
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


def _run() -> RunRecord:
    return RunRecord(
        run_id=RunId("run_statistics"),
        pipeline_id=PipelineId("pip_statistics"),
        pipeline_version=PipelineVersion(1),
        runner_kind="threaded",
        runner_configuration=ConfigurationDocument.from_mapping({"private": "canary-value"}),
        state=RunState.RUNNING,
        row_version=2,
        scenario_seed=None,
        created_at=UtcTimestamp.parse("2026-08-13T12:00:00Z"),
        started_at=UtcTimestamp.parse("2026-08-13T12:00:01Z"),
        finished_at=None,
        cancellation_requested_at=None,
        recovery_started_at=None,
        recovered_at=None,
        execution_evidence_fingerprint=None,
    )


def _node(identity: str = "nod_statistics") -> RunNodeRecord:
    return RunNodeRecord(
        run_id=RunId("run_statistics"),
        node_id=NodeId(identity),
        status=RunNodeStatus.PENDING,
        row_version=1,
        work_total=0,
        work_pending=0,
        work_running=0,
        work_succeeded=0,
        work_quarantined=0,
        work_failed=0,
        work_cancelled=0,
        records_read=0,
        records_written=0,
        records_quarantined=0,
        bytes_read=0,
        bytes_written=0,
        retry_count=0,
        duration=Duration(0),
        started_at=None,
        finished_at=None,
    )


def _work(
    *,
    run_id: RunId | None = None,
    node_id: NodeId | None = None,
    state: WorkItemState = WorkItemState.SUCCEEDED,
    completed: int = 1,
) -> WorkItemRecord:
    return WorkItemRecord(
        WorkItemId("wrk_statistics"),
        run_id or RunId("run_statistics"),
        node_id or NodeId("nod_statistics"),
        PartitionKey("all"),
        state,
        1,
        completed,
        0,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        UtcTimestamp.parse("2026-08-13T12:00:00Z"),
        UtcTimestamp.parse("2026-08-13T12:00:01Z"),
    )


def _attempt(number: int = 1) -> WorkAttemptRecord:
    return WorkAttemptRecord(
        WorkItemId("wrk_statistics"),
        AttemptNumber(number),
        UtcTimestamp.parse("2026-08-13T12:00:00Z"),
        UtcTimestamp.parse("2026-08-13T12:00:01Z"),
        "threaded",
        "worker",
        AttemptOutcome.FAILED,
        FailureClassification.UNKNOWN,
        None,
        None,
        0,
        0,
        Duration(1),
    )


def _snapshot() -> RunStatisticsQuerySnapshot:
    return RunStatisticsQuerySnapshot(
        RunId("run_statistics"),
        1,
        "0" * 64,
        "1" * 64,
        1,
        0,
        0,
        AnalyticalViewCatalogSnapshot(()),
    )


def _node_statistics(identity: str = "nod_statistics") -> RunNodeStatisticsRecord:
    return RunNodeStatisticsRecord(
        NodeId(identity),
        RunNodeStatus.PENDING,
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
        0,
        0,
        0,
        0,
        None,
        None,
        None,
    )


def test_source_is_sorted_hashed_and_redacted() -> None:
    source = RunStatisticsSourceSnapshot(_run(), (_node("nod_zulu"), _node("nod_alpha")), (), ())
    repeated = RunStatisticsSourceSnapshot(_run(), (_node("nod_alpha"), _node("nod_zulu")), (), ())

    assert tuple(str(node.node_id) for node in source.nodes) == (
        "nod_alpha",
        "nod_zulu",
    )
    assert source.source_sha256 == repeated.source_sha256
    assert len(source.source_sha256) == 64
    assert "canary-value" not in repr(source)


@pytest.mark.parametrize(
    ("nodes", "message"),
    [
        ((), "node count"),
        ((_node(), _node()), "node identity"),
        ((replace(_node(), run_id=RunId("run_another")),), "another run"),
        ((replace(_node(), work_total=1),), "aggregate"),
    ],
)
def test_source_rejects_invalid_relationships(
    nodes: tuple[RunNodeRecord, ...], message: str
) -> None:
    with pytest.raises(RunStatisticsInvalidError, match=message):
        RunStatisticsSourceSnapshot(_run(), nodes, (), ())


def test_source_rejects_work_attempt_and_transient_state_drift() -> None:
    node = replace(_node(), work_total=1, work_succeeded=1, duration=Duration(1))
    valid_work = _work()
    valid_attempt = _attempt()
    with pytest.raises(RunStatisticsInvalidError, match="invalid parent"):
        RunStatisticsSourceSnapshot(
            _run(), (node,), (replace(valid_work, node_id=NodeId("nod_other")),), ()
        )
    with pytest.raises(RunStatisticsInvalidError, match="invalid parent"):
        RunStatisticsSourceSnapshot(
            _run(),
            (node,),
            (valid_work,),
            (replace(valid_attempt, work_item_id=WorkItemId("wrk_otherxx")),),
        )
    with pytest.raises(RunStatisticsInvalidError, match="not contiguous"):
        RunStatisticsSourceSnapshot(
            _run(), (node,), (replace(valid_work, completed_attempt_count=2),), (valid_attempt,)
        )
    with pytest.raises(RunStatisticsInvalidError, match="transient"):
        RunStatisticsSourceSnapshot(
            _run(),
            (replace(node, work_succeeded=0, work_running=1),),
            (replace(valid_work, state=WorkItemState.LEASED),),
            (valid_attempt,),
        )


def test_source_count_limits_and_exact_tuple_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(TypeError, match="must be a tuple"):
        RunStatisticsSourceSnapshot(_run(), [_node()], (), ())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="invalid value"):
        RunStatisticsSourceSnapshot(_run(), (object(),), (), ())  # type: ignore[arg-type]
    monkeypatch.setattr(contract, "MAX_RUN_STATISTICS_WORK_ITEMS", -1)
    with pytest.raises(RunStatisticsInvalidError, match="work-item count"):
        RunStatisticsSourceSnapshot(_run(), (_node(),), (), ())
    monkeypatch.setattr(contract, "MAX_RUN_STATISTICS_WORK_ITEMS", 1_000_000)
    monkeypatch.setattr(contract, "MAX_RUN_STATISTICS_ATTEMPTS", -1)
    with pytest.raises(RunStatisticsInvalidError, match="attempt count"):
        RunStatisticsSourceSnapshot(_run(), (_node(),), (), ())


@pytest.mark.parametrize("value", [0, 101, True, 1.0, "1"])
def test_page_limit_is_exact_and_bounded(value: object) -> None:
    with pytest.raises(RunStatisticsInvalidError):
        validate_run_statistics_page_limit(value)
    assert validate_run_statistics_page_limit(100) == 100


def test_page_contract_requires_unique_sorted_nodes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = _node_statistics()
    with pytest.raises(TypeError):
        RunNodeStatisticsPage(object(), (record,), None)  # type: ignore[arg-type]
    with pytest.raises(RunStatisticsInvalidError, match="identity order"):
        RunNodeStatisticsPage(
            _snapshot(), (_node_statistics("nod_zulu"), _node_statistics("nod_alpha")), None
        )
    with pytest.raises(RunStatisticsInvalidError, match="final item"):
        RunNodeStatisticsPage(_snapshot(), (record,), NodeId("nod_another"))
    monkeypatch.setattr(contract, "MAX_RUN_STATISTICS_PAGE_SIZE", 0)
    with pytest.raises(RunStatisticsInvalidError, match="page exceeds"):
        RunNodeStatisticsPage(_snapshot(), (record,), None)


def test_snapshot_and_metric_values_reject_bad_types_and_equations() -> None:
    with pytest.raises(TypeError, match="view catalog"):
        replace(_snapshot(), view_catalog=object())
    with pytest.raises(ValueError, match="run version"):
        replace(_snapshot(), run_version=0)
    with pytest.raises(ValueError, match="node count"):
        replace(_snapshot(), node_count=-1)
    with pytest.raises(TypeError, match="source digest"):
        replace(_snapshot(), source_sha256="BAD")
    with pytest.raises(ValueError, match="metric"):
        replace(_node_statistics(), work_total=-1)
    with pytest.raises(RunStatisticsInvalidError, match="work buckets"):
        replace(_node_statistics(), work_total=1)
    with pytest.raises(RunStatisticsInvalidError, match="retry count"):
        replace(_node_statistics(), retry_count=1)
