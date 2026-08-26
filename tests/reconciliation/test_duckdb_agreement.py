"""Python reference versus DuckDB agreement across golden and generated datasets.

The DuckDB engine is rebuildable analytical state: every SQL classification is
re-validated against the domain outcome contract page by page, and the summary
counts and reconciliation fingerprint computed from DuckDB results must equal
the independent Python reference analysis over the same committed inputs.
"""

import hashlib
import random
from collections.abc import Iterator, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pyarrow.parquet as pq
import pytest

from paritygrid.adapters.analytics import (
    DuckDBLifecycleCoordinator,
    DuckDBReconciliationQueryEngine,
)
from paritygrid.adapters.artifacts import encode_normalized_inventory_batch
from paritygrid.application.ports import (
    AnalyticalDatabaseConfig,
    ArtifactManifestRecord,
    ArtifactRelativePath,
    NormalizedArtifactSet,
    NormalizedInventoryBatch,
    NormalizedInventoryRow,
)
from paritygrid.application.reconciliation import (
    ReconciliationAnalysisRequest,
    analyze_reconciliation,
)
from paritygrid.demo.datasets import (
    DatasetProfile,
    RowRole,
    ScenarioSeed,
    ScenarioVersion,
    WireRow,
    WireValue,
    generate_dataset,
    parse_wire_row,
)
from paritygrid.domain.canonical.encoding import CanonicalVersion
from paritygrid.domain.canonical.fingerprints import FingerprintScope, fingerprint_state
from paritygrid.domain.models import (
    ArtifactId,
    ConnectorId,
    InventoryRecord,
    NodeId,
    RunId,
    UtcTimestamp,
)
from paritygrid.domain.pipeline import PartitionKey
from paritygrid.domain.reconciliation import (
    ReconciliationClassification,
    ReconciliationCountSummary,
    SourceObservation,
    compute_reconciliation_fingerprint,
)

SOURCE_CONNECTOR = ConnectorId("con_agreement-source")
TARGET_CONNECTOR = ConnectorId("con_agreement-target")
ANALYTICAL_QUERY_VERSION = 1


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[tuple[DuckDBReconciliationQueryEngine, Path]]:
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    lifecycle = DuckDBLifecycleCoordinator(
        AnalyticalDatabaseConfig((tmp_path / "analytics.duckdb").resolve())
    )
    lifecycle.open()
    try:
        yield DuckDBReconciliationQueryEngine(lifecycle, artifact_root), artifact_root
    finally:
        lifecycle.close()


def _timestamp() -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 25, 12, 0, 0, tzinfo=UTC))


def _artifact_set(
    root: Path,
    records: tuple[InventoryRecord, ...],
    *,
    side: str,
    partitions: int = 1,
) -> NormalizedArtifactSet:
    manifests: list[ArtifactManifestRecord] = []
    chunk = max(1, -(-len(records) // partitions)) if records else 0
    slices = [
        records[index * chunk : (index + 1) * chunk] if chunk else () for index in range(partitions)
    ]
    for index, chunk_records in enumerate(slices):
        name = f"{side}-{index}"
        relative = ArtifactRelativePath(f"normalized/{name}.parquet")
        path = root / str(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        batch = NormalizedInventoryBatch(
            tuple(NormalizedInventoryRow(row, record) for row, record in enumerate(chunk_records))
        )
        pq.write_table(encode_normalized_inventory_batch(batch), path)  # pyright: ignore[reportUnknownMemberType]
        content = path.read_bytes()
        manifests.append(
            ArtifactManifestRecord(
                artifact_id=ArtifactId(f"art_{name}"),
                run_id=RunId("run_agreement"),
                node_id=NodeId(f"nod_{side}"),
                partition_key=PartitionKey(f"part-{index}"),
                relative_path=relative,
                media_type="application/vnd.apache.parquet",
                schema_version=1,
                byte_size=len(content),
                row_count=len(chunk_records),
                sha256=hashlib.sha256(content).hexdigest(),
                created_at=_timestamp(),
            )
        )
    return NormalizedArtifactSet(tuple(manifests))


def _duckdb_outcomes(
    engine: DuckDBReconciliationQueryEngine,
    snapshot: object,
) -> tuple[tuple[str, str], ...]:
    outcomes: list[tuple[str, str]] = []
    cursor = None
    while True:
        page = engine.list_outcomes(snapshot, limit=100, after=cursor)  # type: ignore[arg-type]
        outcomes.extend((item.sku, item.classification.value) for item in page.items)
        if page.next_cursor is None:
            return tuple(outcomes)
        cursor = page.next_cursor


def _duckdb_fingerprint(
    engine: DuckDBReconciliationQueryEngine,
    snapshot: object,
    source_set: NormalizedArtifactSet,
    target_set: NormalizedArtifactSet,
) -> str:
    sql_counts = dict(engine.classification_counts(snapshot))  # type: ignore[arg-type]
    for classification in ReconciliationClassification:
        sql_counts.setdefault(classification, 0)
    summary_counts = ReconciliationCountSummary(
        by_classification=tuple(
            (classification, sql_counts[classification])
            for classification in sorted(ReconciliationClassification, key=lambda c: c.value)
        ),
        source_record_count=source_set.row_count,
        target_record_count=target_set.row_count,
        canonical_key_count=sum(sql_counts.values()),
        source_quarantined_count=0,
        target_quarantined_count=0,
        quarantine_breakdown=(),
    )
    fingerprint = compute_reconciliation_fingerprint(
        source_input_identity=source_set.manifest_sha256,
        target_input_identity=target_set.manifest_sha256,
        analytical_query_version=ANALYTICAL_QUERY_VERSION,
        counts=summary_counts,
        outcome_state_digest=fingerprint_state(
            _snapshot_outcomes(engine, snapshot),
            scope=FingerprintScope.RECONCILIATION_STATE,
            version=CanonicalVersion.V1,
        ),
    )
    return fingerprint.value


def _snapshot_outcomes(
    engine: DuckDBReconciliationQueryEngine,
    snapshot: object,
) -> list[object]:
    outcomes: list[object] = []
    cursor = None
    while True:
        page = engine.list_outcomes(snapshot, limit=100, after=cursor)  # type: ignore[arg-type]
        outcomes.extend(page.items)
        if page.next_cursor is None:
            return outcomes
        cursor = page.next_cursor


def _assert_agreement(
    harness: tuple[DuckDBReconciliationQueryEngine, Path],
    source: tuple[InventoryRecord, ...],
    target: tuple[InventoryRecord, ...],
    *,
    partitions: int = 1,
) -> None:
    engine, root = harness
    source_set = _artifact_set(root, source, side="source", partitions=partitions)
    target_set = _artifact_set(root, target, side="target", partitions=partitions)
    snapshot = engine.rebuild(source_set, target_set)

    duckdb_outcomes = _duckdb_outcomes(engine, snapshot)
    sql_counts = dict(engine.classification_counts(snapshot))

    analysis = analyze_reconciliation(
        ReconciliationAnalysisRequest(
            source_observations=_synthetic_observations(source),
            target_observations=_synthetic_observations(target),
            source_input_identity=source_set.manifest_sha256,
            target_input_identity=target_set.manifest_sha256,
            analytical_query_version=ANALYTICAL_QUERY_VERSION,
        )
    )
    python_outcomes = tuple(
        (key.outcome.sku, key.outcome.classification.value) for key in analysis.classification.keys
    )
    python_counts = {
        classification: count
        for classification, count in analysis.summary.counts.by_classification
        if count
    }
    assert duckdb_outcomes == python_outcomes
    assert sql_counts == python_counts
    duckdb_fingerprint = _duckdb_fingerprint(engine, snapshot, source_set, target_set)
    assert duckdb_fingerprint == analysis.summary.fingerprint.value


def _synthetic_observations(
    records: tuple[InventoryRecord, ...],
) -> tuple[SourceObservation, ...]:
    return tuple(
        SourceObservation(
            position=index,
            connector_id=record.connector_id,
            payload={
                "attributes": dict(record.attributes.items),
                "name": record.name,
                "quantity": record.quantity,
                "sku": record.sku,
                "source_record_key": record.source_record_key,
                "unit_price": {
                    "amount": str(record.unit_price).split(" ", maxsplit=1)[1],
                    "currency": record.unit_price.currency.value,
                },
                "updated_at": str(record.updated_at),
            },
        )
        for index, record in enumerate(records)
    )


def test_golden_dataset_python_and_duckdb_agree(harness: object) -> None:
    from tests.reconciliation.conftest import wire_payload

    def payload_records(
        *payloads: dict[str, object], connector: ConnectorId
    ) -> tuple[InventoryRecord, ...]:
        return tuple(
            parse_wire_row(cast("Mapping[str, WireValue]", payload), connector_id=connector)
            for payload in payloads
        )

    source = payload_records(
        wire_payload(sku="GRID-A", source_record_key="s-a"),
        wire_payload(sku="GRID-B", source_record_key="s-b", quantity=2),
        wire_payload(sku="GRID-C", source_record_key="s-c"),
        wire_payload(sku="GRID-D", source_record_key="s-d1"),
        wire_payload(sku="GRID-D", source_record_key="s-d2"),
        wire_payload(sku="GRID-E", source_record_key="s-e1"),
        wire_payload(sku="GRID-E", source_record_key="s-e2"),
        connector=SOURCE_CONNECTOR,
    )
    target = payload_records(
        wire_payload(sku="GRID-A", source_record_key="t-a"),
        wire_payload(sku="GRID-B", source_record_key="t-b", quantity=3),
        wire_payload(sku="GRID-F", source_record_key="t-f"),
        wire_payload(sku="GRID-G", source_record_key="t-g1"),
        wire_payload(sku="GRID-G", source_record_key="t-g2"),
        wire_payload(sku="GRID-E", source_record_key="t-e1"),
        wire_payload(sku="GRID-E", source_record_key="t-e2"),
        connector=TARGET_CONNECTOR,
    )
    _assert_agreement(
        harness,  # type: ignore[arg-type]
        source,
        target,
        partitions=2,
    )


def _dataset_records(
    dataset_rows: tuple[WireRow, ...], connector: ConnectorId
) -> tuple[InventoryRecord, ...]:
    return tuple(
        parse_wire_row(row.payload, connector_id=connector)
        for row in dataset_rows
        if row.role is not RowRole.MALFORMED
    )


def _derived_target(source: tuple[InventoryRecord, ...], seed: int) -> tuple[InventoryRecord, ...]:
    generator = random.Random(seed)
    target: list[InventoryRecord] = []
    for record in source:
        roll = generator.randrange(10)
        if roll == 0:
            continue
        if roll == 1:
            target.append(replace(record, quantity=(record.quantity + 7) % 1_000))
        elif roll == 2:
            renamed = f"{record.name} Prime"
            target.append(replace(record, name=renamed) if len(renamed) <= 160 else record)
        else:
            target.append(record)
            if roll == 3:
                target.append(replace(record, source_record_key=f"dup-{record.source_record_key}"))
    return tuple(target)


def test_generated_dataset_python_and_duckdb_agree(harness: object) -> None:
    dataset = generate_dataset(ScenarioSeed(9101), ScenarioVersion(1), DatasetProfile())
    source = _dataset_records(dataset.rows, SOURCE_CONNECTOR)
    target = _derived_target(source, seed=9101)
    _assert_agreement(harness, source, target, partitions=2)  # type: ignore[arg-type]


def test_reordered_dataset_python_and_duckdb_agree(harness: object) -> None:
    dataset = generate_dataset(ScenarioSeed(9102), ScenarioVersion(1), DatasetProfile())
    source = _dataset_records(dataset.rows, SOURCE_CONNECTOR)
    generator = random.Random(77)
    shuffled_source = list(source)
    shuffled_target = list(_derived_target(source, seed=9102))
    generator.shuffle(shuffled_source)
    generator.shuffle(shuffled_target)
    _assert_agreement(
        harness,  # type: ignore[arg-type]
        tuple(shuffled_source),
        tuple(shuffled_target),
    )


def test_duplicate_heavy_dataset_python_and_duckdb_agree(harness: object) -> None:
    dataset = generate_dataset(
        ScenarioSeed(9103),
        ScenarioVersion(1),
        DatasetProfile(record_count=96, duplicate_count=30, malformed_count=4, boundary_count=2),
    )
    source = _dataset_records(dataset.rows, SOURCE_CONNECTOR)
    duplicated: list[InventoryRecord] = []
    for record in source:
        duplicated.append(record)
        if record.quantity % 3 == 0:
            duplicated.append(
                replace(record, source_record_key=f"extra-{record.source_record_key}")
            )
    _assert_agreement(
        harness,  # type: ignore[arg-type]
        tuple(duplicated),
        _derived_target(tuple(duplicated), seed=9103),
    )


def test_showcase_scale_dataset_python_and_duckdb_agree(harness: object) -> None:
    dataset = generate_dataset(
        ScenarioSeed(9104),
        ScenarioVersion(1),
        DatasetProfile(
            record_count=5_000, malformed_count=120, boundary_count=40, duplicate_count=400
        ),
    )
    assert dataset.manifest.counts["total"] == 5_000
    source = _dataset_records(dataset.rows, SOURCE_CONNECTOR)
    target = _derived_target(source, seed=9104)
    _assert_agreement(harness, source, target, partitions=4)  # type: ignore[arg-type]


def test_empty_datasets_python_and_duckdb_agree(harness: object) -> None:
    _assert_agreement(harness, (), ())  # type: ignore[arg-type]
