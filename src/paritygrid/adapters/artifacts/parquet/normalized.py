"""Frozen Arrow schema and codec for normalized inventory records."""

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol, cast

import pyarrow as pa

from paritygrid.application.ports.parquet import (
    MAX_NORMALIZED_BATCH_RECORDS,
    MAX_NORMALIZED_BATCH_VARIABLE_BYTES,
    NORMALIZED_PARQUET_SCHEMA_VERSION,
    NormalizedInventoryBatch,
    NormalizedInventoryRow,
    ParquetDecodingError,
    ParquetEncodingError,
    UnsupportedParquetSchemaVersionError,
)
from paritygrid.domain.models import (
    ConnectorId,
    CurrencyCode,
    InventoryAttributes,
    InventoryRecord,
    Money,
    UtcTimestamp,
)

NORMALIZED_INVENTORY_SCHEMA_FINGERPRINT = (
    "1b395761b0e797187bfeb569aa073379d7fc41f75c52a9486c2d386e6781b252"
)
_NORMALIZED_SCHEMA_METADATA: dict[bytes | str, bytes | str] = {
    b"paritygrid.attributes_encoding": b"sorted-key-value-list-v1",
    b"paritygrid.dataset": b"normalized_inventory",
    b"paritygrid.money_encoding": b"minor-units-v1",
    b"paritygrid.schema_fingerprint": NORMALIZED_INVENTORY_SCHEMA_FINGERPRINT.encode("ascii"),
    b"paritygrid.schema_version": b"1",
    b"paritygrid.timestamp_semantics": b"UTC-microsecond",
}
_SENSITIVE_ATTRIBUTE_PARTS = frozenset(
    {
        "access_key",
        "api_key",
        "authorization",
        "bearer",
        "cookie",
        "credential",
        "password",
        "private_key",
        "secret",
        "session",
        "token",
    }
)


class _ArrayFactory(Protocol):
    def __call__(
        self, values: Iterable[object], data_type: pa.DataType, /
    ) -> pa.Array[pa.Scalar[pa.DataType]]: ...


_array = cast(_ArrayFactory, pa.array)  # pyright: ignore[reportUnknownMemberType]
_ATTRIBUTE_STRUCT = pa.struct(
    (
        pa.field("key", pa.string(), nullable=False),
        pa.field("value", pa.string(), nullable=False),
    )
)
_ATTRIBUTES_TYPE = pa.list_(pa.field("element", _ATTRIBUTE_STRUCT, nullable=False))
_NORMALIZED_SCHEMA_V1 = pa.schema(
    (
        pa.field("record_index", pa.int64(), nullable=False),
        pa.field("sku", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("quantity", pa.int32(), nullable=False),
        pa.field("unit_price_minor_units", pa.int64(), nullable=False),
        pa.field("unit_price_currency", pa.string(), nullable=False),
        pa.field("unit_price_exponent", pa.int8(), nullable=False),
        pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("connector_id", pa.string(), nullable=False),
        pa.field("source_record_key", pa.string(), nullable=False),
        pa.field("attributes", _ATTRIBUTES_TYPE, nullable=False),
    ),
    metadata=_NORMALIZED_SCHEMA_METADATA,
)
_VARIABLE_COLUMNS = (
    "sku",
    "name",
    "unit_price_currency",
    "connector_id",
    "source_record_key",
    "attributes",
)
_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def normalized_inventory_schema(
    version: int = NORMALIZED_PARQUET_SCHEMA_VERSION,
) -> pa.Schema:
    """Return the exact immutable normalized-inventory schema."""
    if type(version) is not int or version != NORMALIZED_PARQUET_SCHEMA_VERSION:
        raise UnsupportedParquetSchemaVersionError(
            "normalized inventory schema version is unsupported"
        )
    return _NORMALIZED_SCHEMA_V1


def encode_normalized_inventory_batch(batch: NormalizedInventoryBatch) -> pa.Table:
    """Encode exact domain records without lossy Arrow coercion."""
    value = cast(object, batch)
    if type(value) is not NormalizedInventoryBatch:
        raise ParquetEncodingError("normalized inventory batch must use the public contract")
    exact = value
    for row in exact.records:
        _require_export_safe(row.record)
    if _encoded_variable_bytes(exact.records) > MAX_NORMALIZED_BATCH_VARIABLE_BYTES:
        raise ParquetEncodingError("normalized inventory batch exceeds the byte limit")
    try:
        table = pa.Table.from_arrays(
            (
                _array((row.record_index for row in exact.records), pa.int64()),
                _array((row.record.sku for row in exact.records), pa.string()),
                _array((row.record.name for row in exact.records), pa.string()),
                _array((row.record.quantity for row in exact.records), pa.int32()),
                _array(
                    (row.record.unit_price.minor_units for row in exact.records),
                    pa.int64(),
                ),
                _array(
                    (row.record.unit_price.currency.value for row in exact.records),
                    pa.string(),
                ),
                _array(
                    (row.record.unit_price.minor_unit_exponent for row in exact.records),
                    pa.int8(),
                ),
                _array(
                    (row.record.updated_at.to_datetime() for row in exact.records),
                    pa.timestamp("us", tz="UTC"),
                ),
                _array(
                    (str(row.record.connector_id) for row in exact.records),
                    pa.string(),
                ),
                _array(
                    (row.record.source_record_key for row in exact.records),
                    pa.string(),
                ),
                _array(
                    (
                        tuple({"key": key, "value": item} for key, item in row.record.attributes)
                        for row in exact.records
                    ),
                    _ATTRIBUTES_TYPE,
                ),
            ),
            schema=normalized_inventory_schema(exact.schema_version),
        )
        table.validate(full=True)
    except pa.ArrowException, OverflowError, TypeError, ValueError:
        raise ParquetEncodingError("normalized inventory batch could not be encoded") from None
    return table


def decode_normalized_inventory_table(table: pa.Table) -> NormalizedInventoryBatch:
    """Decode an exact v1 table and reject normalized lookalikes."""
    value = cast(object, table)
    if not isinstance(value, pa.Table):
        raise ParquetDecodingError("normalized inventory input must be an Arrow table")
    exact = value
    try:
        exact.validate(full=True)
        if not exact.schema.equals(normalized_inventory_schema(), check_metadata=True):
            raise ParquetDecodingError("normalized inventory Arrow schema is not exact v1")
        if exact.num_rows > MAX_NORMALIZED_BATCH_RECORDS:
            raise ParquetDecodingError("normalized inventory table exceeds the row limit")
        if _table_variable_bytes(exact) > MAX_NORMALIZED_BATCH_VARIABLE_BYTES:
            raise ParquetDecodingError("normalized inventory table exceeds the byte limit")
        columns = {
            name: exact.column(name).to_pylist()
            for name in exact.schema.names
            if name != "updated_at"
        }
        columns["updated_at"] = exact.column("updated_at").cast(pa.int64()).to_pylist()
        rows = zip(
            *(cast(list[object], columns[name]) for name in exact.schema.names),
            strict=True,
        )
        records = tuple(_decode_row(*row) for row in rows)
        return NormalizedInventoryBatch(records=records)
    except ParquetDecodingError:
        raise
    except KeyError, pa.ArrowException, OverflowError, TypeError, ValueError:
        raise ParquetDecodingError("normalized inventory table is corrupt") from None


def _decode_row(
    record_index: object,
    sku: object,
    name: object,
    quantity: object,
    unit_price_minor_units: object,
    unit_price_currency: object,
    unit_price_exponent: object,
    updated_at_microseconds: object,
    connector_id: object,
    source_record_key: object,
    attributes: object,
) -> NormalizedInventoryRow:
    if type(record_index) is not int:
        raise ParquetDecodingError("normalized inventory record index is corrupt")
    text_values = (sku, name, unit_price_currency, connector_id, source_record_key)
    if any(not isinstance(item, str) for item in text_values):
        raise ParquetDecodingError("normalized inventory text value is corrupt")
    if any(
        type(item) is not int
        for item in (
            quantity,
            unit_price_minor_units,
            unit_price_exponent,
            updated_at_microseconds,
        )
    ):
        raise ParquetDecodingError("normalized inventory numeric value is corrupt")
    sku_value = cast(str, sku)
    name_value = cast(str, name)
    quantity_value = cast(int, quantity)
    minor_units_value = cast(int, unit_price_minor_units)
    currency_value = cast(str, unit_price_currency)
    exponent_value = cast(int, unit_price_exponent)
    microseconds_value = cast(int, updated_at_microseconds)
    connector_value = cast(str, connector_id)
    source_key_value = cast(str, source_record_key)
    try:
        attribute_value = _decode_attributes(attributes)
        record = InventoryRecord(
            sku=sku_value,
            name=name_value,
            quantity=quantity_value,
            unit_price=Money(
                amount=Decimal(minor_units_value).scaleb(-exponent_value),
                currency=CurrencyCode(currency_value),
                minor_unit_exponent=exponent_value,
            ),
            updated_at=UtcTimestamp(_UNIX_EPOCH + timedelta(microseconds=microseconds_value)),
            connector_id=ConnectorId(connector_value),
            source_record_key=source_key_value,
            attributes=attribute_value,
        )
        if (
            record.name != name_value
            or record.source_record_key != source_key_value
            or record.attributes is not attribute_value
        ):
            raise ValueError("noncanonical text")
        _require_export_safe(record)
        return NormalizedInventoryRow(record_index=record_index, record=record)
    except ParquetDecodingError:
        raise
    except ParquetEncodingError:
        raise ParquetDecodingError("normalized inventory row is corrupt") from None
    except TypeError, ValueError:
        raise ParquetDecodingError("normalized inventory row is corrupt") from None


def _decode_attributes(value: object) -> InventoryAttributes:
    if not isinstance(value, list):
        raise ParquetDecodingError("normalized inventory attributes are corrupt")
    pairs: list[tuple[str, str]] = []
    for item in cast(list[object], value):
        if not isinstance(item, dict) or set(cast(dict[object, object], item)) != {
            "key",
            "value",
        }:
            raise ParquetDecodingError("normalized inventory attributes are corrupt")
        pair = cast(dict[str, object], item)
        key = pair["key"]
        child = pair["value"]
        if not isinstance(key, str) or not isinstance(child, str):
            raise ParquetDecodingError("normalized inventory attributes are corrupt")
        pairs.append((key, child))
    try:
        attributes = InventoryAttributes(items=tuple(pairs))
    except TypeError, ValueError:
        raise ParquetDecodingError("normalized inventory attributes are corrupt") from None
    if attributes.items != tuple(pairs):
        raise ParquetDecodingError("normalized inventory attributes are not canonical")
    return attributes


def _encoded_variable_bytes(records: tuple[NormalizedInventoryRow, ...]) -> int:
    rows = len(records)
    attributes = sum(len(row.record.attributes) for row in records)
    encoded = 24 * rows + 8 * attributes
    for row in records:
        record = row.record
        encoded += sum(
            len(value.encode("utf-8"))
            for value in (
                record.sku,
                record.name,
                record.unit_price.currency.value,
                str(record.connector_id),
                record.source_record_key,
            )
        )
        encoded += sum(
            len(key.encode("utf-8")) + len(item.encode("utf-8")) for key, item in record.attributes
        )
    return encoded


def _table_variable_bytes(table: pa.Table) -> int:
    return sum(table.column(name).nbytes for name in _VARIABLE_COLUMNS)


def _require_export_safe(record: InventoryRecord) -> None:
    for key, _value in record.attributes:
        normalized = key.replace("-", "_")
        padded = f"_{normalized}_"
        if any(f"_{part}_" in padded for part in _SENSITIVE_ATTRIBUTE_PARTS):
            raise ParquetEncodingError("normalized inventory attributes contain a prohibited field")
