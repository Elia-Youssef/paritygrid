"""Frozen Arrow schema and codec for raw inventory observations."""

import json
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Never, Protocol, cast

import pyarrow as pa

from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    ConsistencyInvalidRequestError,
    RedactedDocument,
)
from paritygrid.application.ports.parquet import (
    MAX_RAW_BATCH_PAYLOAD_BYTES,
    MAX_RAW_BATCH_RECORDS,
    MAX_RAW_PAYLOAD_BYTES,
    RAW_PARQUET_SCHEMA_VERSION,
    ParquetDecodingError,
    ParquetEncodingError,
    RawInventoryBatch,
    RawInventoryRecord,
    UnsupportedParquetSchemaVersionError,
)
from paritygrid.domain.models import ConnectorId, UtcTimestamp

RAW_INVENTORY_SCHEMA_FINGERPRINT = (
    "46a71c5c3da90e9b5fa533fef77a8a4c2372b912d314d9ccdcc4510fade71c40"
)
_RAW_SCHEMA_METADATA: dict[bytes | str, bytes | str] = {
    b"paritygrid.dataset": b"raw_inventory",
    b"paritygrid.payload_encoding": b"canonical-json-object-v1",
    b"paritygrid.schema_fingerprint": RAW_INVENTORY_SCHEMA_FINGERPRINT.encode("ascii"),
    b"paritygrid.schema_version": b"1",
    b"paritygrid.timestamp_semantics": b"UTC-microsecond",
}


class _ArrayFactory(Protocol):
    def __call__(
        self, values: Iterable[object], data_type: pa.DataType, /
    ) -> pa.Array[pa.Scalar[pa.DataType]]: ...


_array = cast(_ArrayFactory, pa.array)  # pyright: ignore[reportUnknownMemberType]
_RAW_SCHEMA_V1 = pa.schema(
    (
        pa.field("record_index", pa.int64(), nullable=False),
        pa.field("connector_id", pa.string(), nullable=False),
        pa.field("source_record_key", pa.string(), nullable=False),
        pa.field("captured_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("payload_json", pa.large_string(), nullable=False),
    ),
    metadata=_RAW_SCHEMA_METADATA,
)
_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def raw_inventory_schema(version: int = RAW_PARQUET_SCHEMA_VERSION) -> pa.Schema:
    """Return the exact immutable raw-inventory schema for one supported version."""
    if type(version) is not int or version != RAW_PARQUET_SCHEMA_VERSION:
        raise UnsupportedParquetSchemaVersionError("raw inventory schema version is unsupported")
    return _RAW_SCHEMA_V1


def encode_raw_inventory_batch(batch: RawInventoryBatch) -> pa.Table:
    """Encode a validated raw partition without implicit Arrow coercion."""
    value = cast(object, batch)
    if type(value) is not RawInventoryBatch:
        raise ParquetEncodingError("raw inventory batch must use the public contract")
    exact = value
    payloads = tuple(_encode_payload(record.payload) for record in exact.records)
    encoded_payload_bytes = sum(len(payload.encode("utf-8")) + 8 for payload in payloads)
    if encoded_payload_bytes > MAX_RAW_BATCH_PAYLOAD_BYTES:
        raise ParquetEncodingError("raw inventory batch payload exceeds the byte limit")
    try:
        table = pa.Table.from_arrays(
            (
                _array((record.record_index for record in exact.records), pa.int64()),
                _array((str(record.connector_id) for record in exact.records), pa.string()),
                _array((record.source_record_key for record in exact.records), pa.string()),
                _array(
                    (record.captured_at.to_datetime() for record in exact.records),
                    pa.timestamp("us", tz="UTC"),
                ),
                _array(payloads, pa.large_string()),
            ),
            schema=raw_inventory_schema(exact.schema_version),
        )
        table.validate(full=True)
    except pa.ArrowException, OverflowError, TypeError, ValueError:
        raise ParquetEncodingError("raw inventory batch could not be encoded") from None
    return table


def decode_raw_inventory_table(table: pa.Table) -> RawInventoryBatch:
    """Decode an exact v1 table and reject coercible or noncanonical lookalikes."""
    value = cast(object, table)
    if not isinstance(value, pa.Table):
        raise ParquetDecodingError("raw inventory input must be an Arrow table")
    exact = value
    try:
        exact.validate(full=True)
        if not exact.schema.equals(raw_inventory_schema(), check_metadata=True):
            raise ParquetDecodingError("raw inventory Arrow schema is not exact v1")
        if exact.num_rows > MAX_RAW_BATCH_RECORDS:
            raise ParquetDecodingError("raw inventory table exceeds the row limit")
        if exact.column("payload_json").nbytes > MAX_RAW_BATCH_PAYLOAD_BYTES:
            raise ParquetDecodingError("raw inventory table payload exceeds the byte limit")
        columns = {
            "record_index": exact.column("record_index").to_pylist(),
            "connector_id": exact.column("connector_id").to_pylist(),
            "source_record_key": exact.column("source_record_key").to_pylist(),
            "captured_at": exact.column("captured_at").cast(pa.int64()).to_pylist(),
            "payload_json": exact.column("payload_json").to_pylist(),
        }
        rows = zip(
            cast(list[object], columns["record_index"]),
            cast(list[object], columns["connector_id"]),
            cast(list[object], columns["source_record_key"]),
            cast(list[object], columns["captured_at"]),
            cast(list[object], columns["payload_json"]),
            strict=True,
        )
        records = tuple(_decode_row(*row) for row in rows)
        return RawInventoryBatch(records=records)
    except ParquetDecodingError:
        raise
    except KeyError, pa.ArrowException, OverflowError, TypeError, ValueError:
        raise ParquetDecodingError("raw inventory table is corrupt") from None


def _encode_payload(payload: RedactedDocument) -> str:
    value = cast(object, payload)
    if type(value) is not RedactedDocument:
        raise ParquetEncodingError("raw inventory payload must be redacted")
    try:
        encoded = json.dumps(
            value.to_mapping(),
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except TypeError, ValueError:
        raise ParquetEncodingError("raw inventory payload could not be encoded") from None
    if len(encoded.encode("utf-8")) > MAX_RAW_PAYLOAD_BYTES:
        raise ParquetEncodingError("raw inventory payload exceeds the byte limit")
    return encoded


def _decode_row(
    record_index: object,
    connector_id: object,
    source_record_key: object,
    captured_at_microseconds: object,
    payload_json: object,
) -> RawInventoryRecord:
    if type(record_index) is not int:
        raise ParquetDecodingError("raw inventory record index is corrupt")
    if not isinstance(connector_id, str) or not isinstance(source_record_key, str):
        raise ParquetDecodingError("raw inventory record identity is corrupt")
    if type(captured_at_microseconds) is not int:
        raise ParquetDecodingError("raw inventory capture time is corrupt")
    if not isinstance(payload_json, str):
        raise ParquetDecodingError("raw inventory payload is corrupt")
    try:
        return RawInventoryRecord(
            record_index=record_index,
            connector_id=ConnectorId(connector_id),
            source_record_key=source_record_key,
            captured_at=UtcTimestamp(
                _UNIX_EPOCH + timedelta(microseconds=captured_at_microseconds)
            ),
            payload=_decode_payload(payload_json),
        )
    except ParquetDecodingError:
        raise
    except TypeError, ValueError:
        raise ParquetDecodingError("raw inventory row is corrupt") from None


def _decode_payload(value: str) -> RedactedDocument:
    try:
        if len(value.encode("utf-8")) > MAX_RAW_PAYLOAD_BYTES:
            raise ParquetDecodingError("raw inventory payload exceeds the byte limit")
        decoded = json.loads(
            value,
            object_pairs_hook=_unique_object,
            parse_float=_reject_float,
            parse_constant=_reject_constant,
        )
        if not isinstance(decoded, dict):
            raise ValueError("object required")
        document = RedactedDocument(
            ConfigurationDocument.from_mapping(cast(dict[str, object], decoded))
        )
        if _encode_payload(document) != value:
            raise ValueError("noncanonical payload")
        return document
    except ParquetEncodingError:
        raise ParquetDecodingError("raw inventory payload is corrupt") from None
    except (
        ConsistencyInvalidRequestError,
        RecursionError,
        json.JSONDecodeError,
        TypeError,
        UnicodeError,
        ValueError,
    ):
        raise ParquetDecodingError("raw inventory payload is corrupt") from None


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate object key")
        value[key] = item
    return value


def _reject_float(_value: str) -> Never:
    raise ValueError("floating-point payload")


def _reject_constant(_value: str) -> Never:
    raise ValueError("non-finite payload")
