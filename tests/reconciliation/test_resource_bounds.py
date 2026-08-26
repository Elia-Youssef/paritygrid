"""Resource bounds, contract limits, and cleanup behavior for Phase 10."""

from dataclasses import replace
from pathlib import Path

import pytest

from paritygrid.adapters.analytics import (
    DuckDBLifecycleCoordinator,
    DuckDBReconciliationQueryEngine,
)
from paritygrid.application.ports import AnalyticalDatabaseConfig, NormalizedArtifactSet
from paritygrid.application.ports.parquet import (
    MAX_CONFLICT_BATCH_RECORDS,
    ParquetDatasetKind,
    ReconciliationConflictBatch,
    ReconciliationConflictRow,
)
from paritygrid.application.reconciliation import analyze_reconciliation
from paritygrid.domain.reconciliation import (
    MAX_SOURCE_OBSERVATIONS,
    ComparisonDocument,
    ComparisonDocumentError,
    ComparisonValue,
    MatchingError,
    NormalizationError,
    NormalizedRecord,
    SourceNormalization,
    build_field_differences,
    match_by_canonical_key,
    normalize_source_observations,
)
from paritygrid.domain.reconciliation.matching import MAX_MATCHED_KEYS
from paritygrid.domain.reconciliation.summaries import RECONCILIATION_FINGERPRINT_VERSION
from tests.reconciliation.conftest import source_observation, wire_payload


def test_normalization_batch_observation_bound_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import paritygrid.domain.reconciliation.normalization as module

    monkeypatch.setattr(module, "MAX_SOURCE_OBSERVATIONS", 4)
    observations = [source_observation(index, wire_payload()) for index in range(5)]
    with pytest.raises(NormalizationError, match="observation limit"):
        normalize_source_observations(observations)
    assert MAX_SOURCE_OBSERVATIONS == 100_000


def test_normalization_result_contract_enforces_the_bound_directly() -> None:
    record = normalize_source_observations([source_observation(0, wire_payload())]).records[0]
    overflow = tuple(
        replace(record, position=index) for index in range(MAX_SOURCE_OBSERVATIONS + 1)
    )
    with pytest.raises(NormalizationError, match="observation limit"):
        SourceNormalization(1, overflow, ())


def test_matched_key_bound_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    import paritygrid.domain.reconciliation.matching as module

    monkeypatch.setattr(module, "MAX_MATCHED_KEYS", 3)
    records = normalize_source_observations(
        [source_observation(index, wire_payload(sku=f"GRID-{index:04d}")) for index in range(4)]
    ).records
    with pytest.raises(MatchingError, match="canonical key limit"):
        match_by_canonical_key(records, ())
    assert MAX_MATCHED_KEYS == 2 * MAX_SOURCE_OBSERVATIONS


def test_comparison_document_path_bound_fails_closed() -> None:
    pairs = tuple(
        (f"attributes/k{index:03d}", ComparisonValue.attribute_text_value("v"))
        for index in range(ComparisonDocument.MAX_PATHS + 1)
    )
    with pytest.raises(ComparisonDocumentError, match="path limit"):
        ComparisonDocument(values=pairs)


def test_field_difference_count_bound_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    import paritygrid.domain.reconciliation.differences as module

    monkeypatch.setattr(module, "_MAX_DIFFERENCES", 3)
    pairs = tuple(
        (f"attributes/k{index:03d}", ComparisonValue.attribute_text_value(f"v{index}"))
        for index in range(4)
    )
    source = ComparisonDocument(values=pairs)
    target = ComparisonDocument(
        values=tuple(
            (path, ComparisonValue.attribute_text_value(f"w{index}"))
            for index, (path, _value) in enumerate(pairs)
        )
    )
    with pytest.raises(ComparisonDocumentError, match="difference limit"):
        build_field_differences(source, target)
    three = ComparisonDocument(values=pairs[:3])
    three_target = ComparisonDocument(values=target.values[:3])
    assert len(build_field_differences(three, three_target)) == 3


def test_conflict_batch_row_bound_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    import paritygrid.application.ports.parquet as module

    def _row(index: int) -> ReconciliationConflictRow:
        return ReconciliationConflictRow(
            conflict_index=index,
            sku=f"GRID-{index:04d}",
            classification=module.ReconciliationClassification.MISSING_FROM_TARGET,
            suggested_resolution=module.SuggestedResolution.CREATE_TARGET,
            source_positions=(index,),
            target_positions=(),
            source_record_keys=(f"s-{index}",),
            target_record_keys=(),
            differences=(),
            secondary=(),
        )

    monkeypatch.setattr(module, "MAX_CONFLICT_BATCH_RECORDS", 3)
    with pytest.raises(ValueError, match="row limit"):
        ReconciliationConflictBatch(tuple(_row(index) for index in range(4)))
    assert MAX_CONFLICT_BATCH_RECORDS == 100_000


def test_analysis_rejects_foreign_request_types() -> None:
    with pytest.raises(TypeError, match="ReconciliationAnalysisRequest"):
        analyze_reconciliation("not-a-request")  # type: ignore[arg-type]


def test_duckdb_rebuild_replaces_disposable_state_and_cleans_up(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    database_path = tmp_path / "analytics.duckdb"
    lifecycle = DuckDBLifecycleCoordinator(AnalyticalDatabaseConfig(database_path))
    lifecycle.open()
    try:
        engine = DuckDBReconciliationQueryEngine(lifecycle, artifact_root)
        snapshot = engine.rebuild(NormalizedArtifactSet(()), NormalizedArtifactSet(()))
        page = engine.list_outcomes(snapshot, limit=1)
        assert page.items == ()
        rebuilt = engine.rebuild(NormalizedArtifactSet(()), NormalizedArtifactSet(()))
        engine.list_outcomes(rebuilt, limit=1)
    finally:
        lifecycle.close()
    assert database_path.is_file()
    assert not (tmp_path / "analytics.duckdb.wal").exists()


def test_normalized_records_are_bounded_provenance_values() -> None:
    result = normalize_source_observations([source_observation(0, wire_payload())])
    assert isinstance(result.records[0], NormalizedRecord)
    assert result.rules_version == 1


def test_parquet_dataset_kinds_are_closed() -> None:
    assert {kind.value for kind in ParquetDatasetKind} == {"raw", "normalized", "reconciliation"}


def test_fingerprint_version_is_frozen_at_one() -> None:
    assert RECONCILIATION_FINGERPRINT_VERSION == 1
