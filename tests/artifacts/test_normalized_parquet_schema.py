"""Arrow and Parquet tests for the frozen normalized inventory schema."""

# pyright: reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Never, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from paritygrid.adapters.artifacts import (
    NORMALIZED_INVENTORY_SCHEMA_FINGERPRINT,
    decode_normalized_inventory_table,
    encode_normalized_inventory_batch,
    normalized_inventory_schema,
)
from paritygrid.adapters.artifacts.parquet import normalized as codec
from paritygrid.application.ports import (
    NormalizedInventoryBatch,
    NormalizedInventoryRow,
    ParquetDecodingError,
    ParquetEncodingError,
    UnsupportedParquetSchemaVersionError,
)
from paritygrid.domain.models import (
    ConnectorId,
    CurrencyCode,
    InventoryRecord,
    Money,
    UtcTimestamp,
)


def _timestamp(second: int = 0) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 13, 12, 0, second, 123456, tzinfo=UTC))


def _record(index: int = 0, *, attributes: dict[str, str] | None = None) -> InventoryRecord:
    return InventoryRecord.create(
        sku=f"SKU-{index + 1}",
        name="Café — ميناء" if index == 0 else f"Product {index + 1}",
        quantity=index + 1,
        unit_price=Money(
            Decimal("12.345678") if index == 0 else Decimal("7"),
            CurrencyCode("USD" if index == 0 else "EUR"),
            6 if index == 0 else 0,
        ),
        updated_at=_timestamp(index),
        connector_id=ConnectorId("con_normalized-source"),
        source_record_key=f"source {index + 1}",
        attributes=attributes
        if attributes is not None
        else {"color": "Blå", "warehouse-zone": f"A-{index + 1}"},
    )


def _batch(count: int = 2) -> NormalizedInventoryBatch:
    return NormalizedInventoryBatch(
        tuple(NormalizedInventoryRow(index, _record(index)) for index in range(count))
    )


def _replace_column(table: pa.Table, name: str, values: list[object]) -> pa.Table:
    index = table.schema.get_field_index(name)
    field = table.schema.field(index)
    return table.set_column(index, field, codec._array(values, field.type))


def _decode_schema_lookalike(arrays: list[Any], schema: pa.Schema) -> None:
    decode_normalized_inventory_table(pa.Table.from_arrays(arrays, schema=schema))


def test_normalized_schema_v1_has_exact_fields_metadata_and_fingerprint() -> None:
    schema = normalized_inventory_schema()
    assert NORMALIZED_INVENTORY_SCHEMA_FINGERPRINT == (
        "1b395761b0e797187bfeb569aa073379d7fc41f75c52a9486c2d386e6781b252"
    )
    assert tuple((field.name, str(field.type), field.nullable) for field in schema) == (
        ("record_index", "int64", False),
        ("sku", "string", False),
        ("name", "string", False),
        ("quantity", "int32", False),
        ("unit_price_minor_units", "int64", False),
        ("unit_price_currency", "string", False),
        ("unit_price_exponent", "int8", False),
        ("updated_at", "timestamp[us, tz=UTC]", False),
        ("connector_id", "string", False),
        ("source_record_key", "string", False),
        (
            "attributes",
            "list<element: struct<key: string not null, value: string not null> not null>",
            False,
        ),
    )
    assert schema.metadata == {
        b"paritygrid.attributes_encoding": b"sorted-key-value-list-v1",
        b"paritygrid.dataset": b"normalized_inventory",
        b"paritygrid.money_encoding": b"minor-units-v1",
        b"paritygrid.schema_fingerprint": NORMALIZED_INVENTORY_SCHEMA_FINGERPRINT.encode(),
        b"paritygrid.schema_version": b"1",
        b"paritygrid.timestamp_semantics": b"UTC-microsecond",
    }
    with pytest.raises(UnsupportedParquetSchemaVersionError):
        normalized_inventory_schema(2)
    with pytest.raises(UnsupportedParquetSchemaVersionError):
        normalized_inventory_schema(cast(Any, True))


def test_arrow_and_real_parquet_round_trip_exact_domain_records(tmp_path: Path) -> None:
    batch = _batch()
    table = encode_normalized_inventory_batch(batch)
    assert decode_normalized_inventory_table(table) == batch

    path = tmp_path / "normalized inventory % é عربي.parquet"
    pq.write_table(table, path)
    installed = pq.read_table(path)
    assert installed.schema.equals(normalized_inventory_schema(), check_metadata=True)
    assert decode_normalized_inventory_table(installed) == batch


def test_physical_columns_preserve_integer_money_and_sorted_attributes() -> None:
    table = encode_normalized_inventory_batch(_batch(1))
    assert table.column("record_index").to_pylist() == [0]
    assert table.column("quantity").to_pylist() == [1]
    assert table.column("unit_price_minor_units").to_pylist() == [12_345_678]
    assert table.column("unit_price_currency").to_pylist() == ["USD"]
    assert table.column("unit_price_exponent").to_pylist() == [6]
    assert table.column("updated_at").cast(pa.int64()).to_pylist() == [1_786_622_400_123_456]
    assert table.column("attributes").to_pylist() == [
        [
            {"key": "color", "value": "Blå"},
            {"key": "warehouse-zone", "value": "A-1"},
        ]
    ]
    assert codec._table_variable_bytes(table) == codec._encoded_variable_bytes(_batch(1).records)


def test_empty_normalized_batch_round_trips_through_parquet(tmp_path: Path) -> None:
    batch = NormalizedInventoryBatch(())
    table = encode_normalized_inventory_batch(batch)
    assert table.num_rows == 0
    path = tmp_path / "empty.parquet"
    pq.write_table(table, path)
    assert decode_normalized_inventory_table(pq.read_table(path)) == batch


@pytest.mark.parametrize(
    "schema",
    [
        normalized_inventory_schema().remove_metadata(),
        normalized_inventory_schema().with_metadata(
            {**(normalized_inventory_schema().metadata or {}), b"extra": b"value"}
        ),
        normalized_inventory_schema().set(0, pa.field("record_index", pa.int32(), nullable=False)),
        normalized_inventory_schema().set(3, pa.field("quantity", pa.int64(), nullable=False)),
        normalized_inventory_schema().set(
            7, pa.field("updated_at", pa.timestamp("us"), nullable=False)
        ),
        normalized_inventory_schema().set(
            10,
            pa.field(
                "attributes",
                pa.list_(pa.struct((pa.field("key", pa.string()), pa.field("value", pa.string())))),
                nullable=False,
            ),
        ),
    ],
)
def test_decoder_rejects_coercible_schema_lookalikes(schema: pa.Schema) -> None:
    table = encode_normalized_inventory_batch(_batch(1))
    arrays = [table.column(index) for index in range(table.num_columns)]
    with pytest.raises((pa.ArrowException, ParquetDecodingError)):
        _decode_schema_lookalike(arrays, schema)


def test_decoder_rejects_wrong_metadata_value_and_column_order() -> None:
    table = encode_normalized_inventory_batch(_batch(1))
    metadata = dict(normalized_inventory_schema().metadata or {})
    metadata[b"paritygrid.schema_version"] = b"01"
    with pytest.raises(ParquetDecodingError, match="schema"):
        decode_normalized_inventory_table(table.replace_schema_metadata(metadata))
    reversed_table = pa.Table.from_arrays(
        list(reversed(table.columns)), names=list(reversed(table.column_names))
    )
    with pytest.raises(ParquetDecodingError, match="schema"):
        decode_normalized_inventory_table(reversed_table)


@pytest.mark.parametrize(
    "key",
    [
        "api-key",
        "access_key",
        "authorization",
        "bearer",
        "cookie",
        "credential",
        "password",
        "private_key",
        "secret",
        "session",
        "token",
    ],
)
def test_encoder_rejects_sensitive_export_attributes_without_echoing_key(key: str) -> None:
    batch = NormalizedInventoryBatch(
        (NormalizedInventoryRow(0, _record(attributes={key: "canary-value"})),)
    )
    with pytest.raises(ParquetEncodingError) as captured:
        encode_normalized_inventory_batch(batch)
    assert key not in str(captured.value)
    assert "canary-value" not in str(captured.value)


def test_decoder_rejects_sensitive_attribute_from_untrusted_table() -> None:
    table = encode_normalized_inventory_batch(_batch(1))
    corrupted = _replace_column(
        table, "attributes", [[{"key": "api-key", "value": "canary-value"}]]
    )
    with pytest.raises(ParquetDecodingError) as captured:
        decode_normalized_inventory_table(corrupted)
    assert "api-key" not in str(captured.value)
    assert "canary-value" not in str(captured.value)


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("record_index", 1),
        ("sku", "bad sku"),
        ("name", "Cafe\u0301"),
        ("quantity", -1),
        ("unit_price_minor_units", 1_000_000_000_000_000),
        ("unit_price_currency", "usd"),
        ("unit_price_exponent", 7),
        ("connector_id", "con_BAD"),
        ("source_record_key", "source  one"),
    ],
)
def test_decoder_rejects_noncanonical_or_out_of_domain_rows(column: str, value: object) -> None:
    table = _replace_column(encode_normalized_inventory_batch(_batch(1)), column, [value])
    with pytest.raises(ParquetDecodingError):
        decode_normalized_inventory_table(table)


@pytest.mark.parametrize(
    "attributes",
    [
        [{"key": "warehouse-zone", "value": "A-1"}, {"key": "color", "value": "Blue"}],
        [{"key": "color", "value": "Blue"}, {"key": "color", "value": "Red"}],
        [{"key": "color", "value": "Cafe\u0301"}],
        [{"key": "Bad", "value": "Blue"}],
    ],
)
def test_decoder_rejects_noncanonical_nested_attributes(attributes: list[object]) -> None:
    table = _replace_column(
        encode_normalized_inventory_batch(_batch(1)), "attributes", [attributes]
    )
    with pytest.raises(ParquetDecodingError, match=r"attribute|row"):
        decode_normalized_inventory_table(table)


def test_calendar_boundaries_do_not_require_host_timezone_database() -> None:
    records = (
        InventoryRecord.create(
            sku="EARLY",
            name="Early",
            quantity=0,
            unit_price=Money(Decimal("0"), CurrencyCode("USD"), 0),
            updated_at=UtcTimestamp(datetime(1, 1, 1, tzinfo=UTC)),
            connector_id=ConnectorId("con_normalized-source"),
            source_record_key="early",
        ),
        InventoryRecord.create(
            sku="LATE",
            name="Late",
            quantity=2_147_483_647,
            unit_price=Money(Decimal("999999999.999999"), CurrencyCode("JPY"), 6),
            updated_at=UtcTimestamp(datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)),
            connector_id=ConnectorId("con_normalized-source"),
            source_record_key="late",
        ),
    )
    batch = NormalizedInventoryBatch(
        tuple(NormalizedInventoryRow(index, record) for index, record in enumerate(records))
    )
    assert decode_normalized_inventory_table(encode_normalized_inventory_batch(batch)) == batch


def test_batch_row_and_variable_byte_limits_fail_before_materialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batch = _batch(1)
    monkeypatch.setattr(codec, "MAX_NORMALIZED_BATCH_VARIABLE_BYTES", 0)
    with pytest.raises(ParquetEncodingError, match="byte limit"):
        encode_normalized_inventory_batch(batch)


def test_decoder_enforces_row_and_variable_byte_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = encode_normalized_inventory_batch(_batch(1))
    monkeypatch.setattr(codec, "MAX_NORMALIZED_BATCH_RECORDS", 0)
    with pytest.raises(ParquetDecodingError, match="row limit"):
        decode_normalized_inventory_table(table)
    monkeypatch.setattr(codec, "MAX_NORMALIZED_BATCH_RECORDS", 100_000)
    monkeypatch.setattr(codec, "MAX_NORMALIZED_BATCH_VARIABLE_BYTES", 0)
    with pytest.raises(ParquetDecodingError, match="byte limit"):
        decode_normalized_inventory_table(table)


def test_public_codecs_reject_wrong_boundary_types() -> None:
    with pytest.raises(ParquetEncodingError, match="public contract"):
        encode_normalized_inventory_batch(cast(Any, object()))
    with pytest.raises(ParquetDecodingError, match="Arrow table"):
        decode_normalized_inventory_table(cast(Any, object()))


def test_arrow_encoding_failure_is_bounded_and_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_array(_values: object, _data_type: object) -> Never:
        raise pa.ArrowInvalid("canary-sensitive-row")

    monkeypatch.setattr(codec, "_array", fail_array)
    with pytest.raises(ParquetEncodingError) as captured:
        encode_normalized_inventory_batch(_batch(1))
    assert "canary-sensitive-row" not in str(captured.value)


@pytest.mark.parametrize(
    "values",
    [
        (True, "SKU-1", "Name", 1, 100, "USD", 2, 0, "con_source", "source", []),
        (0, 1, "Name", 1, 100, "USD", 2, 0, "con_source", "source", []),
        (0, "SKU-1", "Name", True, 100, "USD", 2, 0, "con_source", "source", []),
    ],
)
def test_private_row_mapper_rejects_python_storage_class_lookalikes(
    values: tuple[object, ...],
) -> None:
    with pytest.raises(ParquetDecodingError):
        codec._decode_row(*values)


@pytest.mark.parametrize(
    "value",
    [None, {}, [{"key": "color"}], [{"key": 1, "value": "Blue"}]],
)
def test_private_attribute_mapper_rejects_malformed_shapes(value: object) -> None:
    with pytest.raises(ParquetDecodingError, match="attributes"):
        codec._decode_attributes(value)
