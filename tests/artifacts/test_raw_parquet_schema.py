"""Arrow and Parquet round-trip tests for the frozen raw schema."""

# pyright: reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false

from datetime import UTC, datetime
from typing import Any, Never, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from paritygrid.adapters.artifacts import (
    RAW_INVENTORY_SCHEMA_FINGERPRINT,
    decode_raw_inventory_table,
    encode_raw_inventory_batch,
    raw_inventory_schema,
)
from paritygrid.adapters.artifacts.parquet import raw as codec
from paritygrid.application.ports import (
    ParquetDecodingError,
    ParquetEncodingError,
    RawInventoryBatch,
    RawInventoryRecord,
    RedactedDocument,
    UnsupportedParquetSchemaVersionError,
)
from paritygrid.domain.models import ConnectorId, UtcTimestamp


def _timestamp(second: int) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 13, 12, 0, second, 123456, tzinfo=UTC))


def _record(index: int) -> RawInventoryRecord:
    return RawInventoryRecord(
        record_index=index,
        connector_id=ConnectorId("con_raw-source"),
        source_record_key=f"source-{index:04d}",
        captured_at=_timestamp(index),
        payload=RedactedDocument.from_mapping(
            {
                "active": index == 0,
                "attributes": {"label": "Café — ميناء", "tags": ["a", "b"]},
                "quantity": index + 1,
                "sku": f"RAW-{index + 1}",
            }
        ),
    )


def _table_with_column(name: str, values: list[object]) -> pa.Table:
    arrays = [
        codec._array([0], pa.int64()),
        codec._array(["con_raw-source"], pa.string()),
        codec._array(["source-0000"], pa.string()),
        codec._array([_timestamp(0).to_datetime()], pa.timestamp("us", tz="UTC")),
        codec._array(['{"sku":"RAW-1"}'], pa.large_string()),
    ]
    arrays[raw_inventory_schema().get_field_index(name)] = codec._array(
        values, raw_inventory_schema().field(name).type
    )
    return pa.Table.from_arrays(arrays, schema=raw_inventory_schema())


def test_raw_schema_v1_has_exact_fields_metadata_and_golden_fingerprint() -> None:
    schema = raw_inventory_schema()
    assert RAW_INVENTORY_SCHEMA_FINGERPRINT == (
        "46a71c5c3da90e9b5fa533fef77a8a4c2372b912d314d9ccdcc4510fade71c40"
    )
    assert tuple((field.name, str(field.type), field.nullable) for field in schema) == (
        ("record_index", "int64", False),
        ("connector_id", "string", False),
        ("source_record_key", "string", False),
        ("captured_at", "timestamp[us, tz=UTC]", False),
        ("payload_json", "large_string", False),
    )
    assert schema.metadata == {
        b"paritygrid.dataset": b"raw_inventory",
        b"paritygrid.payload_encoding": b"canonical-json-object-v1",
        b"paritygrid.schema_fingerprint": RAW_INVENTORY_SCHEMA_FINGERPRINT.encode(),
        b"paritygrid.schema_version": b"1",
        b"paritygrid.timestamp_semantics": b"UTC-microsecond",
    }
    with pytest.raises(UnsupportedParquetSchemaVersionError):
        raw_inventory_schema(2)
    with pytest.raises(UnsupportedParquetSchemaVersionError):
        raw_inventory_schema(cast(Any, True))


def test_arrow_and_real_parquet_round_trip_preserve_exact_logical_records() -> None:
    batch = RawInventoryBatch((_record(0), _record(1)))
    table = encode_raw_inventory_batch(batch)
    assert table.schema.equals(raw_inventory_schema(), check_metadata=True)
    assert table.column("payload_json").to_pylist() == [
        '{"active":true,"attributes":{"label":"Caf\\u00e9 \\u2014 '
        '\\u0645\\u064a\\u0646\\u0627\\u0621","tags":["a","b"]},'
        '"quantity":1,"sku":"RAW-1"}',
        '{"active":false,"attributes":{"label":"Caf\\u00e9 \\u2014 '
        '\\u0645\\u064a\\u0646\\u0627\\u0621","tags":["a","b"]},'
        '"quantity":2,"sku":"RAW-2"}',
    ]
    assert decode_raw_inventory_table(table) == batch

    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="none",
        use_dictionary=False,
        write_statistics=True,
        version="2.6",
    )
    parquet_bytes = sink.getvalue().to_pybytes()
    installed = pq.read_table(pa.BufferReader(parquet_bytes))
    assert installed.schema.equals(raw_inventory_schema(), check_metadata=True)
    assert decode_raw_inventory_table(installed) == batch


def test_empty_batch_round_trip_retains_nonnullable_exact_schema() -> None:
    batch = RawInventoryBatch(())
    table = encode_raw_inventory_batch(batch)
    assert table.num_rows == 0
    assert decode_raw_inventory_table(table) == batch
    assert all(not field.nullable for field in table.schema)


@pytest.mark.parametrize(
    "schema",
    [
        raw_inventory_schema().remove_metadata(),
        raw_inventory_schema().with_metadata({b"paritygrid.schema_version": b"2"}),
        raw_inventory_schema().with_metadata(
            {
                **raw_inventory_schema().metadata,
                b"unexpected": b"value",
            }
        ),
        pa.schema(
            tuple(
                raw_inventory_schema().field(name)
                for name in reversed(raw_inventory_schema().names)
            ),
            metadata=cast(dict[bytes | str, bytes | str], raw_inventory_schema().metadata),
        ),
        pa.schema(
            (
                *tuple(raw_inventory_schema())[:3],
                pa.field("captured_at", pa.timestamp("ms", tz="UTC"), nullable=False),
                raw_inventory_schema().field("payload_json"),
            ),
            metadata=cast(dict[bytes | str, bytes | str], raw_inventory_schema().metadata),
        ),
        pa.schema(
            (
                *tuple(raw_inventory_schema())[:3],
                pa.field("captured_at", pa.timestamp("us"), nullable=False),
                raw_inventory_schema().field("payload_json"),
            ),
            metadata=cast(dict[bytes | str, bytes | str], raw_inventory_schema().metadata),
        ),
        pa.schema(
            (
                pa.field("record_index", pa.int32(), nullable=False),
                *tuple(raw_inventory_schema())[1:],
            ),
            metadata=cast(dict[bytes | str, bytes | str], raw_inventory_schema().metadata),
        ),
        pa.schema(
            (
                pa.field("record_index", pa.int64(), nullable=True),
                *tuple(raw_inventory_schema())[1:],
            ),
            metadata=cast(dict[bytes | str, bytes | str], raw_inventory_schema().metadata),
        ),
    ],
)
def test_decode_rejects_schema_lookalikes(schema: pa.Schema) -> None:
    lookalike = pa.Table.from_batches([], schema=schema)
    with pytest.raises(ParquetDecodingError, match="schema"):
        decode_raw_inventory_table(lookalike)


@pytest.mark.parametrize(
    "payload",
    [
        "[]",
        '{"quantity":1.5}',
        '{"quantity":NaN}',
        '{"sku":"A","sku":"B"}',
        '{ "sku":"RAW-1"}',
        '{"z":1,"a":2}',
        '{"api_token":"value"}',
        '{"label":"Café"}',
        "{",
    ],
)
def test_decode_rejects_nonobject_noncanonical_or_unsafe_payload(payload: str) -> None:
    with pytest.raises(ParquetDecodingError, match="payload"):
        decode_raw_inventory_table(_table_with_column("payload_json", [payload]))


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("record_index", None),
        ("connector_id", None),
        ("connector_id", "invalid"),
        ("source_record_key", None),
        ("source_record_key", " bad"),
        ("captured_at", None),
        ("payload_json", None),
    ],
)
def test_decode_rejects_null_or_invalid_rows(name: str, value: object) -> None:
    with pytest.raises(ParquetDecodingError):
        decode_raw_inventory_table(_table_with_column(name, [value]))


def test_timestamp_calendar_boundaries_round_trip_without_host_timezone_data() -> None:
    records = (
        RawInventoryRecord(
            0,
            ConnectorId("con_raw-source"),
            "early",
            UtcTimestamp(datetime(1, 1, 1, tzinfo=UTC)),
            RedactedDocument.from_mapping({}),
        ),
        RawInventoryRecord(
            1,
            ConnectorId("con_raw-source"),
            "late",
            UtcTimestamp(datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)),
            RedactedDocument.from_mapping({}),
        ),
    )
    batch = RawInventoryBatch(records)
    assert decode_raw_inventory_table(encode_raw_inventory_batch(batch)) == batch


def test_codec_rejects_wrong_public_inputs_and_bounded_payloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ParquetEncodingError, match="public contract"):
        encode_raw_inventory_batch(cast(Any, object()))
    with pytest.raises(ParquetDecodingError, match="Arrow table"):
        decode_raw_inventory_table(cast(Any, object()))

    monkeypatch.setattr(codec, "MAX_RAW_PAYLOAD_BYTES", 1)
    with pytest.raises(ParquetEncodingError, match="payload exceeds"):
        encode_raw_inventory_batch(RawInventoryBatch((_record(0),)))
    with pytest.raises(ParquetDecodingError, match="payload exceeds"):
        codec._decode_payload("{}")
    with pytest.raises(ParquetDecodingError, match="payload is corrupt"):
        codec._decode_payload("\ud800")

    monkeypatch.setattr(codec, "MAX_RAW_PAYLOAD_BYTES", 1_048_576)
    monkeypatch.setattr(codec, "MAX_RAW_BATCH_PAYLOAD_BYTES", 1)
    batch = RawInventoryBatch((_record(0),))
    with pytest.raises(ParquetEncodingError, match="batch payload"):
        encode_raw_inventory_batch(batch)
    table = encode_raw_inventory_batch(RawInventoryBatch(()))
    monkeypatch.setattr(codec, "MAX_RAW_BATCH_RECORDS", -1)
    with pytest.raises(ParquetDecodingError, match="row limit"):
        decode_raw_inventory_table(table)

    monkeypatch.setattr(codec, "MAX_RAW_BATCH_RECORDS", 100_000)
    monkeypatch.setattr(codec, "MAX_RAW_BATCH_PAYLOAD_BYTES", 67_108_864)
    populated = encode_raw_inventory_batch(batch)
    exact_bytes = populated.column("payload_json").nbytes
    monkeypatch.setattr(codec, "MAX_RAW_BATCH_PAYLOAD_BYTES", exact_bytes)
    assert decode_raw_inventory_table(populated) == batch
    assert encode_raw_inventory_batch(batch).equals(populated)
    monkeypatch.setattr(codec, "MAX_RAW_BATCH_PAYLOAD_BYTES", 1)
    with pytest.raises(ParquetDecodingError, match="table payload"):
        decode_raw_inventory_table(populated)


def test_codec_maps_arrow_and_json_failures_without_payload_leak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_array(_values: object, _type: object) -> Never:
        raise pa.ArrowInvalid("payload canary")

    monkeypatch.setattr(codec, "_array", fail_array)
    with pytest.raises(ParquetEncodingError) as caught:
        encode_raw_inventory_batch(RawInventoryBatch((_record(0),)))
    assert "canary" not in str(caught.value)

    def fail_json(*_args: object, **_kwargs: object) -> Never:
        raise TypeError("payload canary")

    monkeypatch.setattr(codec.json, "dumps", fail_json)
    with pytest.raises(ParquetEncodingError) as json_error:
        codec._encode_payload(_record(0).payload)
    assert "canary" not in str(json_error.value)


def test_codec_maps_defensive_decode_and_payload_reencode_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = encode_raw_inventory_batch(RawInventoryBatch((_record(0),)))
    with pytest.raises(ParquetEncodingError, match="must be redacted"):
        codec._encode_payload(cast(Any, object()))

    def invalid_schema(_version: int = 1) -> pa.Schema:
        raise ValueError("internal")

    monkeypatch.setattr(codec, "raw_inventory_schema", invalid_schema)
    with pytest.raises(ParquetDecodingError, match="table is corrupt"):
        decode_raw_inventory_table(table)

    def invalid_payload(_document: RedactedDocument) -> Never:
        raise ParquetEncodingError("internal")

    monkeypatch.setattr(codec, "_encode_payload", invalid_payload)
    with pytest.raises(ParquetDecodingError, match="payload is corrupt"):
        codec._decode_payload("{}")
