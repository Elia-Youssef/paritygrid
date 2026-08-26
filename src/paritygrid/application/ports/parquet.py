"""Dependency-neutral contracts for versioned analytical datasets."""

import unicodedata
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, cast

from paritygrid.application.ports.artifacts import ArtifactWriteReceipt
from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.domain.models import ConnectorId, InventoryRecord, NodeId, RunId, UtcTimestamp
from paritygrid.domain.pipeline import PartitionKey
from paritygrid.domain.reconciliation import (
    FieldDifference,
    ReconciliationClassification,
    SecondaryEvidence,
    SuggestedResolution,
    suggested_resolution_for,
)

RAW_PARQUET_SCHEMA_VERSION = 1
NORMALIZED_PARQUET_SCHEMA_VERSION = 1
CONFLICT_PARQUET_SCHEMA_VERSION = 1
MAX_RAW_BATCH_RECORDS = 100_000
MAX_RAW_BATCH_PAYLOAD_BYTES = 67_108_864
MAX_RAW_RECORD_INDEX = 9_223_372_036_854_775_807
MAX_RAW_SOURCE_KEY_CHARACTERS = 128
MAX_RAW_SOURCE_KEY_BYTES = 256
MAX_RAW_PAYLOAD_BYTES = 1_048_576
MAX_NORMALIZED_BATCH_RECORDS = 100_000
MAX_NORMALIZED_BATCH_VARIABLE_BYTES = 67_108_864
MAX_NORMALIZED_RECORD_INDEX = 9_223_372_036_854_775_807
MAX_CONFLICT_BATCH_RECORDS = 100_000
MAX_CONFLICT_CONFLICT_INDEX = 9_223_372_036_854_775_807
# Mirrors ReconciliationOutcome.MAX_RECORDS_PER_SIDE so a bounded duplicate
# group never loses member provenance in the conflict artifact.
MAX_CONFLICT_MEMBER_KEYS = 1_024
MAX_CONFLICT_DIFFERENCES = 64
MAX_CONFLICT_SECONDARY = 8
MAX_PARQUET_PARTITION_NUMBER = 2_147_483_647
PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"


class ParquetSchemaError(RuntimeError):
    """Base failure for a versioned Parquet data contract."""


class UnsupportedParquetSchemaVersionError(ParquetSchemaError):
    """The requested schema version is not implemented."""


class ParquetEncodingError(ParquetSchemaError):
    """A public record could not be represented by the frozen schema."""


class ParquetDecodingError(ParquetSchemaError):
    """An Arrow or Parquet value violates the frozen schema contract."""


class ParquetDatasetKind(StrEnum):
    """Closed analytical datasets that can be published as partitions."""

    RAW = "raw"
    NORMALIZED = "normalized"
    RECONCILIATION = "reconciliation"


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


@dataclass(frozen=True, slots=True)
class ReconciliationConflictRow:
    """One artifact-ready conflict for a canonical key that is not a match."""

    conflict_index: int
    sku: str
    classification: ReconciliationClassification
    suggested_resolution: SuggestedResolution
    source_positions: tuple[int, ...]
    target_positions: tuple[int, ...]
    source_record_keys: tuple[str, ...]
    target_record_keys: tuple[str, ...]
    differences: tuple[FieldDifference, ...]
    secondary: tuple[SecondaryEvidence, ...]

    def __post_init__(self) -> None:
        if type(self.conflict_index) is not int or not 0 <= self.conflict_index <= (
            MAX_CONFLICT_CONFLICT_INDEX
        ):
            raise ValueError("conflict index is outside the supported range")
        if type(self.sku) is not str or not self.sku:
            raise TypeError("conflict SKU must be nonempty text")
        if type(self.classification) is not ReconciliationClassification:
            raise TypeError("conflict classification must use ReconciliationClassification")
        if self.classification is ReconciliationClassification.MATCH:
            raise ValueError("conflict rows cannot carry the match classification")
        if type(self.suggested_resolution) is not SuggestedResolution:
            raise TypeError("conflict resolution must use SuggestedResolution")
        if self.suggested_resolution is not suggested_resolution_for(self.classification):
            raise ValueError("conflict resolution does not match its classification")
        for side_positions, side_keys in (
            (self.source_positions, self.source_record_keys),
            (self.target_positions, self.target_record_keys),
        ):
            if type(side_positions) is not tuple or type(side_keys) is not tuple:
                raise TypeError("conflict member provenance must be tuples")
            if len(side_positions) != len(side_keys):
                raise ValueError("conflict provenance must be parallel tuples")
            if len(side_keys) > MAX_CONFLICT_MEMBER_KEYS:
                raise ValueError("conflict provenance exceeds the member limit")
            if any(type(item) is not int or item < 0 for item in side_positions):
                raise ValueError("conflict positions must be nonnegative integers")
            if any(type(item) is not str or not item for item in side_keys):
                raise ValueError("conflict record keys must be nonempty text")
            if list(side_positions) != sorted(side_positions) or len(set(side_positions)) != len(
                side_positions
            ):
                raise ValueError("conflict positions must be sorted and unique")
        if not self.source_record_keys and not self.target_record_keys:
            raise ValueError("conflict rows must reference at least one member record")
        if type(self.differences) is not tuple or len(self.differences) > MAX_CONFLICT_DIFFERENCES:
            raise ValueError("conflict differences exceed the limit")
        if any(type(item) is not FieldDifference for item in self.differences):
            raise TypeError("conflict differences must be FieldDifference values")
        if type(self.secondary) is not tuple or len(self.secondary) > MAX_CONFLICT_SECONDARY:
            raise ValueError("conflict secondary evidence exceeds the limit")
        if any(type(item) is not SecondaryEvidence for item in self.secondary):
            raise TypeError("conflict secondary evidence must be SecondaryEvidence values")


@dataclass(frozen=True, slots=True)
class ReconciliationConflictBatch:
    """One immutable conflict partition in canonical conflict order."""

    rows: tuple[ReconciliationConflictRow, ...]
    schema_version: int = CONFLICT_PARQUET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        rows = cast(object, self.rows)
        version = cast(object, self.schema_version)
        if not isinstance(rows, tuple):
            raise TypeError("reconciliation conflict rows must be a tuple")
        trusted = cast(tuple[object, ...], rows)
        if len(trusted) > MAX_CONFLICT_BATCH_RECORDS:
            raise ValueError("reconciliation conflict batch exceeds the row limit")
        if any(type(row) is not ReconciliationConflictRow for row in trusted):
            raise TypeError("reconciliation conflict batch contains an invalid row")
        if type(version) is not int:
            raise TypeError("reconciliation conflict schema version must be an integer")
        if version != CONFLICT_PARQUET_SCHEMA_VERSION:
            raise UnsupportedParquetSchemaVersionError(
                "reconciliation conflict schema version is unsupported"
            )
        for expected_index, row in enumerate(cast(tuple[ReconciliationConflictRow, ...], trusted)):
            if row.conflict_index != expected_index:
                raise ValueError("reconciliation conflict indexes must be contiguous from zero")


@dataclass(frozen=True, slots=True)
class ParquetPartitionReceipt:
    """Manifest-ready identity and content metadata for one partition."""

    run_id: RunId
    node_id: NodeId
    partition_key: PartitionKey
    dataset: ParquetDatasetKind
    partition_number: int
    row_count: int
    schema_version: int
    schema_fingerprint: str
    write_receipt: ArtifactWriteReceipt

    def __post_init__(self) -> None:
        _require_exact(self.run_id, RunId, "partition run")
        _require_exact(self.node_id, NodeId, "partition node")
        _require_exact(self.partition_key, PartitionKey, "partition key")
        _require_exact(self.dataset, ParquetDatasetKind, "partition dataset")
        _validate_partition_number(self.partition_number)
        row_count = cast(object, self.row_count)
        if type(row_count) is not int:
            raise TypeError("partition row count must be an integer")
        if not 0 <= row_count <= max(MAX_RAW_BATCH_RECORDS, MAX_NORMALIZED_BATCH_RECORDS):
            raise ValueError("partition row count is outside the supported range")
        schema_version = cast(object, self.schema_version)
        if type(schema_version) is not int or schema_version != 1:
            raise ValueError("partition schema version is unsupported")
        fingerprint = cast(object, self.schema_fingerprint)
        if not isinstance(fingerprint, str):
            raise TypeError("partition schema fingerprint must be text")
        if len(fingerprint) != 64 or any(
            character not in "0123456789abcdef" for character in fingerprint
        ):
            raise ValueError("partition schema fingerprint must be lowercase SHA-256")
        _require_exact(self.write_receipt, ArtifactWriteReceipt, "partition write receipt")

    @property
    def media_type(self) -> str:
        """Return the stable media type used by artifact manifests."""
        return PARQUET_MEDIA_TYPE


class ParquetPartitionWriter(Protocol):
    """Port for deterministic immutable Parquet partition publication."""

    def write_raw(
        self,
        *,
        run_id: RunId,
        node_id: NodeId,
        partition_key: PartitionKey,
        partition_number: int,
        batch: RawInventoryBatch,
    ) -> ParquetPartitionReceipt:
        """Encode and publish one raw inventory partition."""
        ...

    def write_normalized(
        self,
        *,
        run_id: RunId,
        node_id: NodeId,
        partition_key: PartitionKey,
        partition_number: int,
        batch: NormalizedInventoryBatch,
    ) -> ParquetPartitionReceipt:
        """Encode and publish one normalized inventory partition."""
        ...

    def write_conflicts(
        self,
        *,
        run_id: RunId,
        node_id: NodeId,
        partition_key: PartitionKey,
        partition_number: int,
        batch: ReconciliationConflictBatch,
    ) -> ParquetPartitionReceipt:
        """Encode and publish one reconciliation conflict partition."""
        ...


def _require_exact(value: object, expected: type[object], subject: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{subject} must use {expected.__name__}")


def _validate_partition_number(value: object) -> int:
    if type(value) is not int:
        raise TypeError("partition number must be an integer")
    if not 0 <= value <= MAX_PARQUET_PARTITION_NUMBER:
        raise ValueError("partition number is outside the supported range")
    return value


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
