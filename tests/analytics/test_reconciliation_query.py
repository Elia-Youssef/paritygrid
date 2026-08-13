"""Golden reconciliation comparison tests for verified normalized Parquet inputs."""

# pyright: reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownLambdaType=false

import hashlib
import io
import json
import os
import stat
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from paritygrid.adapters.analytics import (
    DuckDBLifecycleCoordinator,
    DuckDBReconciliationQueryEngine,
)
from paritygrid.adapters.analytics import reconciliation as adapter
from paritygrid.adapters.artifacts import encode_normalized_inventory_batch
from paritygrid.application.ports import (
    AnalyticalDatabaseConfig,
    AnalyticalDatabaseStorageError,
    AnalyticalViewCorruptionError,
    ArtifactManifestRecord,
    ArtifactPathError,
    ArtifactRelativePath,
    NormalizedArtifactSet,
    NormalizedInventoryBatch,
    NormalizedInventoryRow,
    ReconciliationQueryCorruptionError,
    ReconciliationQueryCursor,
    ReconciliationQueryIntegrityError,
    ReconciliationQueryInvalidError,
    ReconciliationQueryStateError,
    ReconciliationQueryStorageError,
)
from paritygrid.domain.models import (
    ArtifactId,
    ConnectorId,
    CurrencyCode,
    InventoryRecord,
    Money,
    NodeId,
    RunId,
    UtcTimestamp,
)
from paritygrid.domain.pipeline import PartitionKey
from paritygrid.domain.reconciliation import ReconciliationOutcome


def _timestamp(second: int = 0) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 13, 12, 0, second, 123456, tzinfo=UTC))


def _record(
    sku: str,
    *,
    suffix: str = "a",
    quantity: int = 1,
    name: str = "Caf\u00e9 inventory",
) -> InventoryRecord:
    return InventoryRecord.create(
        sku=sku,
        name=name,
        quantity=quantity,
        unit_price=Money(Decimal("12.34"), CurrencyCode("USD"), 2),
        updated_at=_timestamp(1),
        connector_id=ConnectorId("con_reconciliation"),
        source_record_key=f"source {sku} {suffix}",
        attributes={"color": "Bl\u00e5", "region": "MENA"},
    )


def _manifest(
    root: Path,
    name: str,
    records: tuple[InventoryRecord, ...],
    *,
    node_id: str,
) -> ArtifactManifestRecord:
    relative = ArtifactRelativePath(f"normalized/{name}.parquet")
    path = root / str(relative)
    path.parent.mkdir(parents=True, exist_ok=True)
    batch = NormalizedInventoryBatch(
        tuple(NormalizedInventoryRow(index, record) for index, record in enumerate(records))
    )
    pq.write_table(  # pyright: ignore[reportUnknownMemberType]
        encode_normalized_inventory_batch(batch), path
    )
    content = path.read_bytes()
    return ArtifactManifestRecord(
        artifact_id=ArtifactId(f"art_{name}"),
        run_id=RunId("run_reconciliation"),
        node_id=NodeId(node_id),
        partition_key=PartitionKey("all"),
        relative_path=relative,
        media_type="application/vnd.apache.parquet",
        schema_version=1,
        byte_size=len(content),
        row_count=len(records),
        sha256=hashlib.sha256(content).hexdigest(),
        created_at=_timestamp(),
    )


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[DuckDBReconciliationQueryEngine]:
    artifact_root = (tmp_path / "artifacts").resolve()
    artifact_root.mkdir()
    lifecycle = DuckDBLifecycleCoordinator(
        AnalyticalDatabaseConfig((tmp_path / "analytics.duckdb").resolve())
    )
    lifecycle.open()
    try:
        yield DuckDBReconciliationQueryEngine(lifecycle, artifact_root)
    finally:
        lifecycle.close()


def _golden_records() -> tuple[tuple[InventoryRecord, ...], tuple[InventoryRecord, ...]]:
    reference = (
        _record("SKU-A"),
        _record("SKU-B", quantity=2),
        _record("SKU-C"),
        _record("SKU-E", suffix="a"),
        _record("SKU-E", suffix="b"),
        _record("SKU-F"),
        _record("SKU-G", suffix="a"),
        _record("SKU-G", suffix="b"),
    )
    target = (
        _record("SKU-A"),
        _record("SKU-B", quantity=3),
        _record("SKU-D"),
        _record("SKU-E"),
        _record("SKU-F", suffix="a"),
        _record("SKU-F", suffix="b"),
        _record("SKU-G", suffix="c"),
        _record("SKU-G", suffix="d"),
    )
    return reference, target


def test_golden_duckdb_results_equal_independent_domain_reference(
    engine: DuckDBReconciliationQueryEngine, tmp_path: Path
) -> None:
    root = tmp_path / "artifacts"
    reference, target = _golden_records()
    reference_set = NormalizedArtifactSet(
        (_manifest(root, "reference", reference, node_id="nod_reference"),)
    )
    target_set = NormalizedArtifactSet((_manifest(root, "target", target, node_id="nod_target"),))

    snapshot = engine.rebuild(reference_set, target_set)
    first = engine.list_outcomes(snapshot, limit=3)
    second = engine.list_outcomes(snapshot, limit=3, after=first.next_cursor)
    third = engine.list_outcomes(snapshot, limit=3, after=second.next_cursor)
    actual = first.items + second.items + third.items

    sources: dict[str, list[InventoryRecord]] = defaultdict(list)
    targets: dict[str, list[InventoryRecord]] = defaultdict(list)
    for record in reference:
        sources[record.sku].append(record)
    for record in target:
        targets[record.sku].append(record)
    expected = tuple(
        ReconciliationOutcome(tuple(sources[sku]), tuple(targets[sku]))
        for sku in sorted(set(sources) | set(targets))
    )

    assert actual == expected
    assert tuple(item.classification.value for item in actual) == (
        "match",
        "field_mismatch",
        "missing_from_target",
        "missing_from_source",
        "duplicate_source",
        "duplicate_target",
        "duplicate_both",
    )
    assert snapshot.reference_manifest_sha256 == reference_set.manifest_sha256
    assert snapshot.target_manifest_sha256 == target_set.manifest_sha256
    assert snapshot.reference_artifact_sha256s == reference_set.artifact_sha256s
    assert snapshot.target_artifact_sha256s == target_set.artifact_sha256s
    assert first.next_cursor == ReconciliationQueryCursor("SKU-C")
    assert second.next_cursor == ReconciliationQueryCursor("SKU-F")
    assert third.next_cursor is None


def test_empty_inputs_and_rebuild_remove_stale_results(
    engine: DuckDBReconciliationQueryEngine, tmp_path: Path
) -> None:
    empty = NormalizedArtifactSet(())
    snapshot = engine.rebuild(empty, empty)
    assert engine.list_outcomes(snapshot, limit=10).items == ()

    root = tmp_path / "artifacts"
    populated = NormalizedArtifactSet(
        (_manifest(root, "one", (_record("SKU-A"),), node_id="nod_reference"),)
    )
    replacement = engine.rebuild(populated, empty)
    page = engine.list_outcomes(replacement, limit=10)
    assert tuple(item.sku for item in page.items) == ("SKU-A",)
    with pytest.raises(ReconciliationQueryStateError):
        engine.list_outcomes(snapshot, limit=10)


def test_tampered_committed_file_is_rejected_before_duckdb_rebuild(
    engine: DuckDBReconciliationQueryEngine, tmp_path: Path
) -> None:
    root = tmp_path / "artifacts"
    manifest = _manifest(root, "tampered", (_record("SKU-A"),), node_id="nod_reference")
    (root / str(manifest.relative_path)).write_bytes(b"not parquet")

    with pytest.raises(ReconciliationQueryIntegrityError):
        engine.rebuild(NormalizedArtifactSet((manifest,)), NormalizedArtifactSet(()))


def test_installed_view_tampering_fails_closed(
    engine: DuckDBReconciliationQueryEngine, tmp_path: Path
) -> None:
    root = tmp_path / "artifacts"
    source = NormalizedArtifactSet(
        (_manifest(root, "stable", (_record("SKU-A"),), node_id="nod_reference"),)
    )
    snapshot = engine.rebuild(source, NormalizedArtifactSet(()))
    engine._database._execute("DROP VIEW pgv_reconciliation_30_outcomes_v1")

    with pytest.raises(ReconciliationQueryCorruptionError):
        engine.list_outcomes(snapshot, limit=10)


def test_constructor_and_public_methods_reject_untyped_boundaries(
    engine: DuckDBReconciliationQueryEngine, tmp_path: Path
) -> None:
    with pytest.raises(TypeError):
        DuckDBReconciliationQueryEngine(object(), tmp_path)  # type: ignore[arg-type]
    with pytest.raises(ReconciliationQueryInvalidError):
        DuckDBReconciliationQueryEngine(engine._database, tmp_path / "missing")
    with pytest.raises(ReconciliationQueryInvalidError):
        engine.rebuild(object(), NormalizedArtifactSet(()))  # type: ignore[arg-type]
    with pytest.raises(ReconciliationQueryInvalidError):
        engine.rebuild(NormalizedArtifactSet(()), object())  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        engine.list_outcomes(object(), limit=1)  # type: ignore[arg-type]

    snapshot = engine.rebuild(NormalizedArtifactSet(()), NormalizedArtifactSet(()))
    with pytest.raises(ReconciliationQueryInvalidError):
        engine.list_outcomes(snapshot, limit=1, after="SKU-A")  # type: ignore[arg-type]


def test_rebuild_and_query_translate_storage_without_retaining_snapshot(
    engine: DuckDBReconciliationQueryEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_recreate(_self: DuckDBLifecycleCoordinator) -> object:
        raise AnalyticalDatabaseStorageError("hidden")

    monkeypatch.setattr(DuckDBLifecycleCoordinator, "recreate", fail_recreate)
    with pytest.raises(ReconciliationQueryStorageError, match="rebuild failed"):
        engine.rebuild(NormalizedArtifactSet(()), NormalizedArtifactSet(()))
    assert engine._snapshot is None


def test_query_translates_storage_and_registry_corruption(
    engine: DuckDBReconciliationQueryEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = engine.rebuild(NormalizedArtifactSet(()), NormalizedArtifactSet(()))

    def fail_snapshot(_self: object) -> object:
        raise AnalyticalViewCorruptionError("hidden")

    monkeypatch.setattr(type(engine._registry), "snapshot", fail_snapshot)
    with pytest.raises(ReconciliationQueryCorruptionError, match="views are corrupt"):
        engine.list_outcomes(snapshot, limit=1)


def test_query_detects_catalog_change_and_storage_failure(
    engine: DuckDBReconciliationQueryEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = engine.rebuild(NormalizedArtifactSet(()), NormalizedArtifactSet(()))
    monkeypatch.setattr(
        type(engine._registry),
        "snapshot",
        lambda _self: type(snapshot.view_catalog)(()),
    )
    with pytest.raises(ReconciliationQueryCorruptionError, match="views changed"):
        engine.list_outcomes(snapshot, limit=1)

    monkeypatch.undo()
    monkeypatch.setattr(
        DuckDBLifecycleCoordinator,
        "_fetch_all",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AnalyticalDatabaseStorageError("hidden")),
    )
    with pytest.raises(ReconciliationQueryStorageError, match="query failed"):
        engine.list_outcomes(snapshot, limit=1)


def test_rebuild_detects_internal_count_and_duplicate_corruption(
    engine: DuckDBReconciliationQueryEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        DuckDBReconciliationQueryEngine,
        "_require_table_count",
        lambda *_args: (_ for _ in ()).throw(ReconciliationQueryCorruptionError("count")),
    )
    with pytest.raises(ReconciliationQueryCorruptionError, match="count"):
        engine.rebuild(NormalizedArtifactSet(()), NormalizedArtifactSet(()))
    assert engine._snapshot is None


@pytest.mark.parametrize("rows", [(), ((1,),), (("bad", 1),), ((1, 1_025),)])
def test_duplicate_bound_classifier_rejects_malformed_or_oversized_counts(
    engine: DuckDBReconciliationQueryEngine,
    monkeypatch: pytest.MonkeyPatch,
    rows: tuple[tuple[object, ...], ...],
) -> None:
    monkeypatch.setattr(
        DuckDBLifecycleCoordinator,
        "_fetch_all",
        lambda *_args, **_kwargs: rows,
    )
    expected = (
        ReconciliationQueryCorruptionError
        if len(rows) != 1 or len(rows[0]) != 2
        else ReconciliationQueryIntegrityError
    )
    with pytest.raises(expected):
        engine._require_duplicate_bounds()


def test_table_count_and_record_page_membership_are_strict(
    engine: DuckDBReconciliationQueryEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        DuckDBLifecycleCoordinator,
        "_fetch_all",
        lambda *_args, **_kwargs: ((2,),),
    )
    with pytest.raises(ReconciliationQueryCorruptionError, match="row count"):
        engine._require_table_count("table_name", 1)

    row = _stored_row(_record("SKU-B"))
    monkeypatch.setattr(
        DuckDBLifecycleCoordinator,
        "_fetch_all",
        lambda *_args, **_kwargs: (row,),
    )
    with pytest.raises(ReconciliationQueryCorruptionError, match="outside"):
        engine._records_for("view_name", ("SKU-A",))


def _stored_row(record: InventoryRecord) -> tuple[object, ...]:
    return (
        0,
        record.sku,
        record.name,
        record.quantity,
        record.unit_price.minor_units,
        str(record.unit_price.currency),
        record.unit_price.minor_unit_exponent,
        str(record.updated_at),
        str(record.connector_id),
        record.source_record_key,
        json.dumps(
            [{"key": key, "value": value} for key, value in record.attributes],
            ensure_ascii=False,
            separators=(",", ":"),
        ),
    )


@pytest.mark.parametrize(
    "row",
    [
        (),
        (0, "bad"),
        (False, "SKU-A", "Name", 1, 1, "USD", 2, str(_timestamp()), "con_bad", "key", "[]"),
        (0, "bad", "Name", 1, 1, "USD", 2, str(_timestamp()), "con_bad", "key", "[]"),
    ],
)
def test_source_row_mapping_rejects_malformed_and_domain_invalid_rows(
    row: tuple[object, ...],
) -> None:
    with pytest.raises(ReconciliationQueryCorruptionError):
        adapter._record_from_row(row)


@pytest.mark.parametrize(
    "value",
    [
        "{",
        "{}",
        "[1]",
        '[{"key":"a"}]',
        '[{"key":1,"value":"x"}]',
        '[{"key":"BAD","value":"x"}]',
        '[{"key":"b","value":"x"},{"key":"a","value":"x"}]',
    ],
)
def test_attribute_mapping_rejects_malformed_or_noncanonical_json(value: str) -> None:
    with pytest.raises(ReconciliationQueryCorruptionError):
        adapter._attributes_from_json(value)


def test_private_mapping_accepts_valid_row_and_rejects_outcome_drift() -> None:
    record = _record("SKU-A")
    assert adapter._record_from_row(_stored_row(record)) == record
    valid = ("SKU-A", 1, 0, "missing_from_target", False, False, False, False, False)
    assert adapter._validated_outcome(valid, (record,), ()) == ReconciliationOutcome((record,), ())
    invalid_rows = (
        (),
        ("SKU-A", 1, 0, "unknown", False, False, False, False, False),
        ("SKU-A", 2, 0, "missing_from_target", False, False, False, False, False),
        ("SKU-A", 1, 0, "missing_from_target", True, False, False, False, False),
    )
    for row in invalid_rows:
        with pytest.raises(ReconciliationQueryCorruptionError):
            adapter._validated_outcome(row, (record,), ())


def test_outcome_key_and_cursor_helpers_reject_malformed_values() -> None:
    with pytest.raises(ReconciliationQueryCorruptionError):
        adapter._outcome_key(("SKU-A",))
    with pytest.raises(ReconciliationQueryInvalidError):
        adapter._require_cursor("SKU-A")
    assert adapter._require_cursor(None) is None


def test_hash_stream_handles_multiple_chunks() -> None:
    stream = io.BytesIO(b"abcdef")
    digest = hashlib.sha256()
    assert adapter._hash_stream(stream, digest) == 6
    assert digest.hexdigest() == hashlib.sha256(b"abcdef").hexdigest()


def test_manifest_verifier_rejects_missing_hash_schema_and_nonregular_file(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "artifacts").resolve()
    root.mkdir()
    manifest = _manifest(root, "verify", (_record("SKU-A"),), node_id="nod_reference")
    path = root / str(manifest.relative_path)

    with pytest.raises(ReconciliationQueryIntegrityError, match="differs"):
        adapter._verify_manifest_file(path, replace(manifest, sha256="0" * 64))

    path.unlink()
    with pytest.raises(ReconciliationQueryIntegrityError, match="could not be verified"):
        adapter._verify_manifest_file(path, manifest)

    path.mkdir()
    with pytest.raises(ReconciliationQueryIntegrityError, match="regular file"):
        adapter._verify_manifest_file(path, manifest)


def test_manifest_verifier_detects_identity_change_and_close_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (tmp_path / "artifacts").resolve()
    root.mkdir()
    manifest = _manifest(root, "identity", (_record("SKU-A"),), node_id="nod_reference")
    path = root / str(manifest.relative_path)
    original_stat = os.stat

    regular_results = iter((True, False))
    monkeypatch.setattr(stat, "S_ISREG", lambda _mode: next(regular_results))
    with pytest.raises(ReconciliationQueryIntegrityError, match="regular file"):
        adapter._verify_manifest_file(path, manifest)
    monkeypatch.undo()

    def changed_stat(value: Any, *, follow_symlinks: bool = True) -> os.stat_result:
        result = original_stat(value, follow_symlinks=follow_symlinks)
        values = list(result)
        values[6] += 1
        return os.stat_result(values)

    monkeypatch.setattr(os, "stat", changed_stat)
    with pytest.raises(ReconciliationQueryIntegrityError, match="changed"):
        adapter._verify_manifest_file(path, manifest)

    monkeypatch.undo()
    original_fstat = os.fstat
    fstat_calls = 0

    def changed_fstat(descriptor: int) -> os.stat_result:
        nonlocal fstat_calls
        result = original_fstat(descriptor)
        fstat_calls += 1
        if fstat_calls == 2:
            values = list(result)
            values[6] += 1
            return os.stat_result(values)
        return result

    monkeypatch.setattr(os, "fstat", changed_fstat)
    with pytest.raises(ReconciliationQueryIntegrityError, match="changed"):
        adapter._verify_manifest_file(path, manifest)

    monkeypatch.undo()
    monkeypatch.setattr(os, "open", lambda *_args, **_kwargs: 99)
    monkeypatch.setattr(os, "fstat", lambda _fd: os.stat_result((stat.S_IFREG,) + (0,) * 9))
    monkeypatch.setattr(os, "fdopen", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError()))
    monkeypatch.setattr(os, "close", lambda _fd: (_ for _ in ()).throw(OSError()))
    with pytest.raises(ReconciliationQueryIntegrityError, match="handle"):
        adapter._verify_manifest_file(path, manifest)


def test_verified_path_stage_rejects_confinement_decode_and_count_drift(
    engine: DuckDBReconciliationQueryEngine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "artifacts"
    manifest = _manifest(root, "staged", (_record("SKU-A"),), node_id="nod_reference")
    source = NormalizedArtifactSet((manifest,))

    monkeypatch.setattr(
        adapter,
        "resolve_artifact_path",
        lambda *_args: (_ for _ in ()).throw(ArtifactPathError("hidden")),
    )
    with pytest.raises(ReconciliationQueryIntegrityError, match="safely confined"):
        engine._verified_paths(source)

    monkeypatch.undo()
    monkeypatch.setattr(
        pq,
        "read_table",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(pa.ArrowInvalid("hidden")),
    )
    with pytest.raises(ReconciliationQueryIntegrityError, match="exact normalized"):
        engine._verified_paths(source)

    monkeypatch.undo()
    monkeypatch.setattr(
        adapter,
        "decode_normalized_inventory_table",
        lambda _table: NormalizedInventoryBatch(()),
    )
    with pytest.raises(ReconciliationQueryIntegrityError, match="row count differs"):
        engine._verified_paths(source)


def test_manifest_verifier_rejects_exact_file_with_wrong_declared_row_count(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "artifacts").resolve()
    root.mkdir()
    manifest = _manifest(root, "rows", (_record("SKU-A"),), node_id="nod_reference")
    path = root / str(manifest.relative_path)
    with pytest.raises(ReconciliationQueryIntegrityError, match="schema or row count"):
        adapter._verify_manifest_file(path, replace(manifest, row_count=2))
