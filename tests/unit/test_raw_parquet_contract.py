"""Dependency-neutral raw Parquet contract tests."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.application.ports import (
    MAX_RAW_BATCH_RECORDS,
    MAX_RAW_RECORD_INDEX,
    MAX_RAW_SOURCE_KEY_BYTES,
    RAW_PARQUET_SCHEMA_VERSION,
    RawInventoryBatch,
    RawInventoryRecord,
    RedactedDocument,
    UnsupportedParquetSchemaVersionError,
)
from paritygrid.application.ports import parquet as contracts
from paritygrid.domain.models import ConnectorId, UtcTimestamp


def _timestamp() -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 13, 12, 0, 0, 123456, tzinfo=UTC))


def _record(index: object = 0, **changes: object) -> RawInventoryRecord:
    values: dict[str, object] = {
        "record_index": index,
        "connector_id": ConnectorId("con_raw-source"),
        "source_record_key": "page-0000",
        "captured_at": _timestamp(),
        "payload": RedactedDocument.from_mapping({"sku": "CAFÉ-1", "quantity": 2}),
    }
    values.update(changes)
    return RawInventoryRecord(**cast(Any, values))


def test_raw_record_is_immutable_detached_and_payload_redacted() -> None:
    source = {"sku": "CAFÉ-1", "nested": {"active": True}}
    record = _record(payload=RedactedDocument.from_mapping(source))
    source["sku"] = "changed"

    assert record.payload.to_mapping() == {"nested": {"active": True}, "sku": "CAFÉ-1"}
    assert "CAFÉ-1" not in repr(record)
    assert "payload=<redacted>" in repr(record)
    with pytest.raises(AttributeError):
        record.record_index = 2  # type: ignore[misc]


def test_raw_contract_is_dependency_neutral() -> None:
    source = Path(contracts.__file__).read_text(encoding="utf-8")
    assert "pyarrow" not in source
    assert "sqlalchemy" not in source
    assert "Session" not in source


@pytest.mark.parametrize("value", [True, 1.0, "0", None])
def test_raw_record_rejects_nonexact_indexes(value: object) -> None:
    with pytest.raises(TypeError, match="index"):
        _record(record_index=value)


@pytest.mark.parametrize("value", [-1, MAX_RAW_RECORD_INDEX + 1])
def test_raw_record_rejects_out_of_range_indexes(value: int) -> None:
    with pytest.raises(ValueError, match="index"):
        _record(value)


@pytest.mark.parametrize(
    ("changes", "error"),
    [
        ({"connector_id": "con_raw-source"}, TypeError),
        ({"source_record_key": 1}, TypeError),
        ({"source_record_key": ""}, ValueError),
        ({"source_record_key": " page"}, ValueError),
        ({"source_record_key": "page  key"}, ValueError),
        ({"source_record_key": "page\tkey"}, ValueError),
        ({"source_record_key": "page\x00key"}, ValueError),
        ({"source_record_key": "Cafe\u0301"}, ValueError),
        ({"source_record_key": "x" * (MAX_RAW_SOURCE_KEY_BYTES + 1)}, ValueError),
        ({"captured_at": "2026-08-13T12:00:00.000000Z"}, TypeError),
        ({"payload": {"sku": "A"}}, TypeError),
    ],
)
def test_raw_record_rejects_invalid_boundaries(
    changes: dict[str, object], error: type[Exception]
) -> None:
    with pytest.raises(error):
        _record(**changes)


def test_raw_source_key_locks_independent_character_and_byte_boundaries() -> None:
    assert _record(source_record_key="é" * 128).source_record_key == "é" * 128
    with pytest.raises(ValueError, match="size limit"):
        _record(source_record_key="é" * 127 + "€")


def test_raw_batch_requires_exact_records_version_and_contiguous_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = (_record(0), _record(1))
    assert RawInventoryBatch(records).records == records
    assert RawInventoryBatch(()).schema_version == RAW_PARQUET_SCHEMA_VERSION

    with pytest.raises(TypeError, match="tuple"):
        RawInventoryBatch(cast(Any, list(records)))
    with pytest.raises(TypeError, match="invalid record"):
        RawInventoryBatch(cast(Any, (object(),)))
    with pytest.raises(TypeError, match="version"):
        RawInventoryBatch((), cast(Any, True))
    with pytest.raises(UnsupportedParquetSchemaVersionError):
        RawInventoryBatch((), 2)
    with pytest.raises(ValueError, match="contiguous"):
        RawInventoryBatch((_record(1),))

    monkeypatch.setattr(contracts, "MAX_RAW_BATCH_RECORDS", 1)
    with pytest.raises(ValueError, match="row limit"):
        RawInventoryBatch(records)
    assert MAX_RAW_BATCH_RECORDS == 100_000
