"""Dependency-neutral contracts for versioned analytical datasets."""

import unicodedata
from dataclasses import dataclass
from typing import cast

from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.domain.models import ConnectorId, InventoryRecord, UtcTimestamp

RAW_PARQUET_SCHEMA_VERSION = 1
NORMALIZED_PARQUET_SCHEMA_VERSION = 1
MAX_RAW_BATCH_RECORDS = 100_000
MAX_RAW_BATCH_PAYLOAD_BYTES = 67_108_864
MAX_RAW_RECORD_INDEX = 9_223_372_036_854_775_807
MAX_RAW_SOURCE_KEY_CHARACTERS = 128
MAX_RAW_SOURCE_KEY_BYTES = 256
MAX_RAW_PAYLOAD_BYTES = 1_048_576
MAX_NORMALIZED_BATCH_RECORDS = 100_000
MAX_NORMALIZED_BATCH_VARIABLE_BYTES = 67_108_864
MAX_NORMALIZED_RECORD_INDEX = 9_223_372_036_854_775_807


class ParquetSchemaError(RuntimeError):
    """Base failure for a versioned Parquet data contract."""


class UnsupportedParquetSchemaVersionError(ParquetSchemaError):
    """The requested schema version is not implemented."""


class ParquetEncodingError(ParquetSchemaError):
    """A public record could not be represented by the frozen schema."""


class ParquetDecodingError(ParquetSchemaError):
    """An Arrow or Parquet value violates the frozen schema contract."""


@dataclass(frozen=True, slots=True, repr=False)
class RawInventoryRecord:
    """One ordered connector observation before inventory normalization."""

    record_index: int
    connector_id: ConnectorId
    source_record_key: str
    captured_at: UtcTimestamp
    payload: RedactedDocument

    def __post_init__(self) -> None:
        index = cast(object, self.record_index)
        connector = cast(object, self.connector_id)
        timestamp = cast(object, self.captured_at)
        payload = cast(object, self.payload)
        if type(index) is not int:
            raise TypeError("raw record index must be an integer")
        if not 0 <= index <= MAX_RAW_RECORD_INDEX:
            raise ValueError("raw record index is outside the supported range")
        if type(connector) is not ConnectorId:
            raise TypeError("raw record connector must be a ConnectorId")
        object.__setattr__(self, "source_record_key", _validate_source_key(self.source_record_key))
        if type(timestamp) is not UtcTimestamp:
            raise TypeError("raw record capture time must be a UtcTimestamp")
        if type(payload) is not RedactedDocument:
            raise TypeError("raw record payload must be a RedactedDocument")

    def __repr__(self) -> str:
        return (
            "RawInventoryRecord("
            f"record_index={self.record_index!r}, connector_id={self.connector_id!r}, "
            f"source_record_key={self.source_record_key!r}, "
            f"captured_at={self.captured_at!r}, payload=<redacted>)"
        )


@dataclass(frozen=True, slots=True)
class RawInventoryBatch:
    """One immutable raw partition with canonical zero-based row ordering."""

    records: tuple[RawInventoryRecord, ...]
    schema_version: int = RAW_PARQUET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        records = cast(object, self.records)
        version = cast(object, self.schema_version)
        if not isinstance(records, tuple):
            raise TypeError("raw inventory batch records must be a tuple")
        trusted = cast(tuple[object, ...], records)
        if len(trusted) > MAX_RAW_BATCH_RECORDS:
            raise ValueError("raw inventory batch exceeds the row limit")
        if any(type(record) is not RawInventoryRecord for record in trusted):
            raise TypeError("raw inventory batch contains an invalid record")
        if type(version) is not int:
            raise TypeError("raw inventory schema version must be an integer")
        if version != RAW_PARQUET_SCHEMA_VERSION:
            raise UnsupportedParquetSchemaVersionError(
                "raw inventory schema version is unsupported"
            )
        for expected_index, record in enumerate(cast(tuple[RawInventoryRecord, ...], trusted)):
            if record.record_index != expected_index:
                raise ValueError("raw inventory record indexes must be contiguous from zero")


@dataclass(frozen=True, slots=True)
class NormalizedInventoryRow:
    """One canonically ordered normalized inventory record."""

    record_index: int
    record: InventoryRecord

    def __post_init__(self) -> None:
        index = cast(object, self.record_index)
        record = cast(object, self.record)
        if type(index) is not int:
            raise TypeError("normalized record index must be an integer")
        if not 0 <= index <= MAX_NORMALIZED_RECORD_INDEX:
            raise ValueError("normalized record index is outside the supported range")
        if type(record) is not InventoryRecord:
            raise TypeError("normalized record must be an InventoryRecord")


@dataclass(frozen=True, slots=True)
class NormalizedInventoryBatch:
    """One immutable normalized partition in canonical row order."""

    records: tuple[NormalizedInventoryRow, ...]
    schema_version: int = NORMALIZED_PARQUET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        records = cast(object, self.records)
        version = cast(object, self.schema_version)
        if not isinstance(records, tuple):
            raise TypeError("normalized inventory batch records must be a tuple")
        trusted = cast(tuple[object, ...], records)
        if len(trusted) > MAX_NORMALIZED_BATCH_RECORDS:
            raise ValueError("normalized inventory batch exceeds the row limit")
        if any(type(record) is not NormalizedInventoryRow for record in trusted):
            raise TypeError("normalized inventory batch contains an invalid record")
        if type(version) is not int:
            raise TypeError("normalized inventory schema version must be an integer")
        if version != NORMALIZED_PARQUET_SCHEMA_VERSION:
            raise UnsupportedParquetSchemaVersionError(
                "normalized inventory schema version is unsupported"
            )
        for expected_index, row in enumerate(cast(tuple[NormalizedInventoryRow, ...], trusted)):
            if row.record_index != expected_index:
                raise ValueError("normalized inventory record indexes must be contiguous from zero")


def _validate_source_key(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("raw source record key must be text")
    if unicodedata.normalize("NFC", value) != value:
        raise ValueError("raw source record key must use NFC Unicode")
    if not value or value != value.strip(" ") or "  " in value:
        raise ValueError("raw source record key must use canonical spacing")
    if any(character.isspace() and character != " " for character in value):
        raise ValueError("raw source record key contains unsupported whitespace")
    if any(unicodedata.category(character).startswith("C") for character in value):
        raise ValueError("raw source record key contains an unsupported code point")
    if (
        len(value) > MAX_RAW_SOURCE_KEY_CHARACTERS
        or len(value.encode("utf-8")) > MAX_RAW_SOURCE_KEY_BYTES
    ):
        raise ValueError("raw source record key exceeds the size limit")
    return value
