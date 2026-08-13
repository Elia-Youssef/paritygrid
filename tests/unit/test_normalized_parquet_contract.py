"""Dependency-neutral normalized Parquet contract tests."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.application.ports import (
    MAX_NORMALIZED_BATCH_RECORDS,
    MAX_NORMALIZED_BATCH_VARIABLE_BYTES,
    MAX_NORMALIZED_RECORD_INDEX,
    NORMALIZED_PARQUET_SCHEMA_VERSION,
    NormalizedInventoryBatch,
    NormalizedInventoryRow,
    ParquetDecodingError,
    ParquetEncodingError,
    ParquetSchemaError,
    UnsupportedParquetSchemaVersionError,
)
from paritygrid.application.ports import parquet as contract
from paritygrid.domain.models import (
    ConnectorId,
    CurrencyCode,
    InventoryRecord,
    Money,
    UtcTimestamp,
)


def _record() -> InventoryRecord:
    return InventoryRecord.create(
        sku="SKU-1",
        name="Café — ميناء",
        quantity=7,
        unit_price=Money(Decimal("12.34"), CurrencyCode("USD"), 2),
        updated_at=UtcTimestamp(datetime(2026, 8, 13, 12, 0, tzinfo=UTC)),
        connector_id=ConnectorId("con_normalized-source"),
        source_record_key="source one",
        attributes={"color": "Blue", "warehouse-zone": "A-1"},
    )


def test_normalized_contract_constants_are_frozen() -> None:
    assert NORMALIZED_PARQUET_SCHEMA_VERSION == 1
    assert MAX_NORMALIZED_BATCH_RECORDS == 100_000
    assert MAX_NORMALIZED_BATCH_VARIABLE_BYTES == 67_108_864
    assert MAX_NORMALIZED_RECORD_INDEX == 9_223_372_036_854_775_807


def test_normalized_row_is_immutable_and_detached() -> None:
    record = _record()
    row = NormalizedInventoryRow(0, record)
    assert row.record is record
    with pytest.raises(AttributeError):
        row.record_index = 1  # type: ignore[misc]


@pytest.mark.parametrize("value", [True, 0.0, "0", object()])
def test_normalized_row_rejects_non_exact_index_types(value: object) -> None:
    with pytest.raises(TypeError, match="index"):
        NormalizedInventoryRow(cast(Any, value), _record())


@pytest.mark.parametrize("value", [-1, MAX_NORMALIZED_RECORD_INDEX + 1])
def test_normalized_row_rejects_out_of_range_indexes(value: int) -> None:
    with pytest.raises(ValueError, match="range"):
        NormalizedInventoryRow(value, _record())


def test_normalized_row_requires_exact_domain_record() -> None:
    with pytest.raises(TypeError, match="InventoryRecord"):
        NormalizedInventoryRow(0, cast(Any, object()))


def test_normalized_batch_is_immutable_and_preserves_exact_rows() -> None:
    rows = (NormalizedInventoryRow(0, _record()),)
    batch = NormalizedInventoryBatch(rows)
    assert batch.records is rows
    assert batch.schema_version == 1
    with pytest.raises(AttributeError):
        batch.records = ()  # type: ignore[misc]


def test_normalized_batch_rejects_mutable_or_invalid_collections() -> None:
    row = NormalizedInventoryRow(0, _record())
    with pytest.raises(TypeError, match="tuple"):
        NormalizedInventoryBatch(cast(Any, [row]))
    with pytest.raises(TypeError, match="invalid record"):
        NormalizedInventoryBatch(cast(Any, (object(),)))


def test_normalized_batch_rejects_noncontiguous_indexes() -> None:
    with pytest.raises(ValueError, match="contiguous"):
        NormalizedInventoryBatch((NormalizedInventoryRow(1, _record()),))


def test_normalized_batch_rejects_unsupported_or_noninteger_versions() -> None:
    with pytest.raises(UnsupportedParquetSchemaVersionError):
        NormalizedInventoryBatch((), 2)
    with pytest.raises(TypeError, match="version"):
        NormalizedInventoryBatch((), cast(Any, True))


def test_normalized_batch_enforces_the_public_row_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(contract, "MAX_NORMALIZED_BATCH_RECORDS", 0)
    with pytest.raises(ValueError, match="row limit"):
        NormalizedInventoryBatch((NormalizedInventoryRow(0, _record()),))


def test_parquet_errors_share_one_public_schema_base() -> None:
    assert issubclass(UnsupportedParquetSchemaVersionError, ParquetSchemaError)
    assert issubclass(ParquetEncodingError, ParquetSchemaError)
    assert issubclass(ParquetDecodingError, ParquetSchemaError)


def test_normalized_contract_has_no_arrow_or_persistence_dependency() -> None:
    source = Path(contract.__file__).read_text(encoding="utf-8")
    assert "pyarrow" not in source
    assert "sqlalchemy" not in source
    assert "Session" not in source
