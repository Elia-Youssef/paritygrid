"""Dependency-neutral reconciliation analytics contract tests."""

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from paritygrid.application.ports import (
    MAX_RECONCILIATION_PAGE_SIZE,
    AnalyticalViewCatalogSnapshot,
    AnalyticalViewColumn,
    AnalyticalViewName,
    AnalyticalViewRecord,
    AnalyticalViewVersion,
    ArtifactManifestRecord,
    ArtifactRelativePath,
    NormalizedArtifactSet,
    ReconciliationQueryCursor,
    ReconciliationQueryInvalidError,
    ReconciliationQueryPage,
    ReconciliationQuerySnapshot,
    validate_reconciliation_page_limit,
)
from paritygrid.application.ports import reconciliation_analytics as contract
from paritygrid.domain.models import (
    ArtifactId,
    ConnectorId,
    InventoryRecord,
    Money,
    NodeId,
    RunId,
    UtcTimestamp,
)
from paritygrid.domain.pipeline import PartitionKey
from paritygrid.domain.reconciliation import ReconciliationOutcome


def _manifest(
    suffix: str = "aaa",
    **changes: object,
) -> ArtifactManifestRecord:
    values: dict[str, object] = {
        "artifact_id": ArtifactId(f"art_{suffix}"),
        "run_id": RunId("run_contract"),
        "node_id": NodeId("nod_contract"),
        "partition_key": PartitionKey("all"),
        "relative_path": ArtifactRelativePath(f"normalized/{suffix}.parquet"),
        "media_type": "application/vnd.apache.parquet",
        "schema_version": 1,
        "byte_size": 123,
        "row_count": 1,
        "sha256": suffix[0] * 64,
        "created_at": UtcTimestamp(datetime(2026, 8, 13, tzinfo=UTC)),
    }
    values.update(changes)
    return ArtifactManifestRecord(**cast(Any, values))


def _snapshot(**changes: object) -> ReconciliationQuerySnapshot:
    column = AnalyticalViewColumn("sku", "VARCHAR", True)
    view = AnalyticalViewRecord(
        AnalyticalViewName("pgv_contract"),
        AnalyticalViewVersion(1),
        "a" * 64,
        "b" * 64,
        (column,),
    )
    values: dict[str, object] = {
        "reference_manifest_sha256": "c" * 64,
        "target_manifest_sha256": "d" * 64,
        "reference_artifact_sha256s": ("e" * 64,),
        "target_artifact_sha256s": (),
        "reference_row_count": 1,
        "target_row_count": 0,
        "query_sha256": "f" * 64,
        "view_catalog": AnalyticalViewCatalogSnapshot((view,)),
    }
    values.update(changes)
    return ReconciliationQuerySnapshot(**cast(Any, values))


def _outcome(sku: str = "SKU-A") -> ReconciliationOutcome:
    record = InventoryRecord.create(
        sku=sku,
        name="Inventory",
        quantity=1,
        unit_price=Money.parse("USD 1.00"),
        updated_at=UtcTimestamp(datetime(2026, 8, 13, tzinfo=UTC)),
        connector_id=ConnectorId("con_contract"),
        source_record_key="source",
    )
    return ReconciliationOutcome((record,), ())


def test_artifact_set_is_sorted_detached_and_has_locked_manifest_digest() -> None:
    second = _manifest("bbb", row_count=2, sha256="b" * 64)
    first = _manifest("aaa", sha256="a" * 64)

    source = NormalizedArtifactSet((second, first))

    assert source.manifests == (first, second)
    assert source.artifact_sha256s == ("a" * 64, "b" * 64)
    assert source.row_count == 3
    assert (
        source.manifest_sha256 == "9ca25ab1927ed1e4ebb2dfe82e54bfa4bf2b42199b03c672e35fe6b70c44cf90"
    )
    assert NormalizedArtifactSet(()).manifest_sha256 == (
        "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"
    )


@pytest.mark.parametrize(
    "value",
    [[], (_manifest(), "bad")],
)
def test_artifact_set_requires_exact_tuple_and_manifest_values(value: object) -> None:
    with pytest.raises(TypeError):
        NormalizedArtifactSet(cast(Any, value))


def test_artifact_set_enforces_partition_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(contract, "MAX_RECONCILIATION_ARTIFACTS_PER_SIDE", 0)
    with pytest.raises(ReconciliationQueryInvalidError, match="partition limit"):
        NormalizedArtifactSet((_manifest(),))


@pytest.mark.parametrize(
    "second",
    [
        _manifest("bbb", artifact_id=ArtifactId("art_aaa")),
        _manifest("bbb", relative_path=ArtifactRelativePath("normalized/aaa.parquet")),
    ],
)
def test_artifact_set_rejects_duplicate_identity_or_path(
    second: ArtifactManifestRecord,
) -> None:
    with pytest.raises(ReconciliationQueryInvalidError, match="unique"):
        NormalizedArtifactSet((_manifest(), second))


@pytest.mark.parametrize(
    "changes",
    [
        {"media_type": "application/json"},
        {"schema_version": 2},
        {"relative_path": ArtifactRelativePath("normalized/data.bin")},
    ],
)
def test_artifact_set_accepts_only_normalized_parquet_v1(changes: dict[str, object]) -> None:
    with pytest.raises(ReconciliationQueryInvalidError, match="normalized Parquet v1"):
        NormalizedArtifactSet((_manifest("aaa", **changes),))


def test_artifact_set_requires_one_run_node() -> None:
    other = _manifest("bbb", node_id=NodeId("nod_other"))
    with pytest.raises(ReconciliationQueryInvalidError, match="one run node"):
        NormalizedArtifactSet((_manifest(), other))


def test_artifact_set_enforces_total_row_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(contract, "MAX_RECONCILIATION_INPUT_ROWS_PER_SIDE", 0)
    with pytest.raises(ReconciliationQueryInvalidError, match="row limit"):
        NormalizedArtifactSet((_manifest(),))


@pytest.mark.parametrize("value", [None, 1, "sku", "", "sku-a", "SKU_A", "A" * 65])
def test_cursor_requires_canonical_sku(value: object) -> None:
    with pytest.raises((TypeError, ReconciliationQueryInvalidError)):
        ReconciliationQueryCursor(cast(Any, value))


@pytest.mark.parametrize("value", [True, 0, MAX_RECONCILIATION_PAGE_SIZE + 1, "1"])
def test_page_limit_is_exact_and_bounded(value: object) -> None:
    with pytest.raises(ReconciliationQueryInvalidError):
        validate_reconciliation_page_limit(value)
    assert validate_reconciliation_page_limit(1) == 1
    assert validate_reconciliation_page_limit(MAX_RECONCILIATION_PAGE_SIZE) == (
        MAX_RECONCILIATION_PAGE_SIZE
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("reference_manifest_sha256", "bad"),
        ("target_manifest_sha256", 1),
        ("query_sha256", "A" * 64),
        ("reference_artifact_sha256s", ["a" * 64]),
        ("target_artifact_sha256s", ("bad",)),
        ("reference_row_count", True),
        ("target_row_count", -1),
        ("view_catalog", ()),
    ],
)
def test_snapshot_rejects_malformed_fields(field: str, value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        _snapshot(**{field: value})


def test_page_is_immutable_sorted_and_cursor_bounded() -> None:
    page = ReconciliationQueryPage(
        _snapshot(),
        (_outcome("SKU-A"), _outcome("SKU-B")),
        ReconciliationQueryCursor("SKU-B"),
    )
    assert page.items[0].sku == "SKU-A"

    invalid_values: tuple[dict[str, object], ...] = (
        {"snapshot": object()},
        {"items": []},
        {"items": (object(),)},
        {"items": (_outcome("SKU-B"), _outcome("SKU-A"))},
        {"items": (_outcome("SKU-A"), _outcome("SKU-A"))},
        {"next_cursor": "SKU-A"},
    )
    for values in invalid_values:
        with pytest.raises((TypeError, ValueError, ReconciliationQueryInvalidError)):
            replace(page, **cast(Any, values))


def test_page_rejects_more_than_public_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(contract, "MAX_RECONCILIATION_PAGE_SIZE", 0)
    with pytest.raises(ValueError, match="result limit"):
        ReconciliationQueryPage(_snapshot(), (_outcome(),), None)
