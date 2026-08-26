"""Frozen Arrow schema and codec for reconciliation conflict artifacts."""

import json
from collections.abc import Iterable
from typing import Protocol, cast

import pyarrow as pa

from paritygrid.application.ports.parquet import (
    CONFLICT_PARQUET_SCHEMA_VERSION,
    MAX_CONFLICT_BATCH_RECORDS,
    ParquetDecodingError,
    ParquetEncodingError,
    ReconciliationConflictBatch,
    ReconciliationConflictRow,
    UnsupportedParquetSchemaVersionError,
)
from paritygrid.domain.reconciliation import (
    FieldDifference,
    FieldDifferenceKind,
    ReconciliationClassification,
    SecondaryEvidence,
    SecondaryEvidenceKind,
    SuggestedResolution,
)

CONFLICT_ARTIFACT_SCHEMA_FINGERPRINT = (
    "73ce51b87039b1381f0940c9679120ac8fca96c1cd24e1987cf29289bcf10509"
)
_MAX_CONFLICT_BATCH_STRING_BYTES = 67_108_864
# One member-provenance JSON value can carry 1,024 source record keys.
_MAX_JSON_COLUMN_BYTES = 262_144
_CONFLICT_SCHEMA_METADATA: dict[bytes | str, bytes | str] = {
    b"paritygrid.dataset": b"reconciliation_conflicts",
    b"paritygrid.encoding": b"canonical-json-v1",
    b"paritygrid.schema_fingerprint": CONFLICT_ARTIFACT_SCHEMA_FINGERPRINT.encode("ascii"),
    b"paritygrid.schema_version": b"1",
}


class _ArrayFactory(Protocol):
    def __call__(
        self, values: Iterable[object], data_type: pa.DataType, /
    ) -> pa.Array[pa.Scalar[pa.DataType]]: ...


_array = cast(_ArrayFactory, pa.array)  # pyright: ignore[reportUnknownMemberType]
_CONFLICT_SCHEMA_V1 = pa.schema(
    (
        pa.field("conflict_index", pa.int64(), nullable=False),
        pa.field("sku", pa.string(), nullable=False),
        pa.field("classification", pa.string(), nullable=False),
        pa.field("suggested_resolution", pa.string(), nullable=False),
        pa.field("source_positions", pa.string(), nullable=False),
        pa.field("target_positions", pa.string(), nullable=False),
        pa.field("source_record_keys", pa.string(), nullable=False),
        pa.field("target_record_keys", pa.string(), nullable=False),
        pa.field("differences_json", pa.string(), nullable=False),
        pa.field("secondary_json", pa.string(), nullable=False),
    ),
    metadata=_CONFLICT_SCHEMA_METADATA,
)


def conflict_artifact_schema(version: int = CONFLICT_PARQUET_SCHEMA_VERSION) -> pa.Schema:
    """Return the exact immutable conflict-artifact schema."""
    if type(version) is not int or version != CONFLICT_PARQUET_SCHEMA_VERSION:
        raise UnsupportedParquetSchemaVersionError(
            "reconciliation conflict schema version is unsupported"
        )
    return _CONFLICT_SCHEMA_V1


def encode_reconciliation_conflict_batch(batch: ReconciliationConflictBatch) -> pa.Table:
    """Encode validated conflict rows without lossy Arrow coercion."""
    value = cast(object, batch)
    if type(value) is not ReconciliationConflictBatch:
        raise ParquetEncodingError("reconciliation conflict batch must use the public contract")
    exact = value
    string_columns = (
        _skus(exact.rows),
        _classifications(exact.rows),
        _resolutions(exact.rows),
        _source_positions(exact.rows),
        _target_positions(exact.rows),
        _source_keys(exact.rows),
        _target_keys(exact.rows),
        _differences(exact.rows),
        _secondary(exact.rows),
    )
    encoded_bytes = sum(
        len(item.encode("utf-8")) + 8 for column in string_columns for item in column
    )
    if encoded_bytes > _MAX_CONFLICT_BATCH_STRING_BYTES:
        raise ParquetEncodingError("reconciliation conflict batch exceeds the byte limit")
    indexes = [row.conflict_index for row in exact.rows]
    try:
        table = pa.Table.from_arrays(
            (
                _array((item for item in indexes), pa.int64()),
                *(_array((item for item in column), pa.string()) for column in string_columns),
            ),
            schema=conflict_artifact_schema(exact.schema_version),
        )
        table.validate(full=True)
    except pa.ArrowException, OverflowError, TypeError, ValueError:
        raise ParquetEncodingError("reconciliation conflict batch could not be encoded") from None
    return table


def decode_reconciliation_conflict_table(table: pa.Table) -> ReconciliationConflictBatch:
    """Decode an exact v1 table and reject conflict-artifact lookalikes."""
    value = cast(object, table)
    if not isinstance(value, pa.Table):
        raise ParquetDecodingError("reconciliation conflict input must be an Arrow table")
    exact = value
    try:
        exact.validate(full=True)
        if not exact.schema.equals(conflict_artifact_schema(), check_metadata=True):
            raise ParquetDecodingError("reconciliation conflict Arrow schema is not exact v1")
        if exact.num_rows > MAX_CONFLICT_BATCH_RECORDS:
            raise ParquetDecodingError("reconciliation conflict table exceeds the row limit")
        for name in exact.schema.names:
            column = exact.column(name)
            if column.nbytes > _MAX_JSON_COLUMN_BYTES * exact.num_rows + _MAX_JSON_COLUMN_BYTES:
                raise ParquetDecodingError("reconciliation conflict column exceeds the byte limit")
        rows = tuple(
            _decode_row(*cast("tuple[object, ...]", row))
            for row in zip(
                *(
                    cast(list[object], exact.column(name).to_pylist())
                    for name in exact.schema.names
                ),
                strict=True,
            )
        )
        return ReconciliationConflictBatch(rows=rows)
    except ParquetDecodingError:
        raise
    except KeyError, pa.ArrowException, OverflowError, TypeError, ValueError:
        raise ParquetDecodingError("reconciliation conflict table is corrupt") from None


def _skus(rows: tuple[ReconciliationConflictRow, ...]) -> list[str]:
    return [row.sku for row in rows]


def _classifications(rows: tuple[ReconciliationConflictRow, ...]) -> list[str]:
    return [row.classification.value for row in rows]


def _resolutions(rows: tuple[ReconciliationConflictRow, ...]) -> list[str]:
    return [row.suggested_resolution.value for row in rows]


def _source_positions(rows: tuple[ReconciliationConflictRow, ...]) -> list[str]:
    return [_encode_integers(row.source_positions) for row in rows]


def _target_positions(rows: tuple[ReconciliationConflictRow, ...]) -> list[str]:
    return [_encode_integers(row.target_positions) for row in rows]


def _source_keys(rows: tuple[ReconciliationConflictRow, ...]) -> list[str]:
    return [_encode_strings(row.source_record_keys) for row in rows]


def _target_keys(rows: tuple[ReconciliationConflictRow, ...]) -> list[str]:
    return [_encode_strings(row.target_record_keys) for row in rows]


def _differences(rows: tuple[ReconciliationConflictRow, ...]) -> list[str]:
    return [
        _canonical_json(
            [
                {
                    "field": difference.field,
                    "kind": difference.kind.value,
                    "source": difference.source_text,
                    "target": difference.target_text,
                }
                for difference in row.differences
            ]
        )
        for row in rows
    ]


def _secondary(rows: tuple[ReconciliationConflictRow, ...]) -> list[str]:
    return [
        _canonical_json(
            [{"kind": evidence.kind.value, "value": evidence.value} for evidence in row.secondary]
        )
        for row in rows
    ]


def _encode_integers(values: tuple[int, ...]) -> str:
    return _canonical_json(list(values))


def _encode_strings(values: tuple[str, ...]) -> str:
    return _canonical_json(list(values))


def _canonical_json(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    if len(encoded) > _MAX_JSON_COLUMN_BYTES:
        raise ParquetEncodingError("reconciliation conflict column value exceeds the byte limit")
    return encoded


def _decode_row(
    conflict_index: object,
    sku: object,
    classification: object,
    suggested_resolution: object,
    source_positions: object,
    target_positions: object,
    source_record_keys: object,
    target_record_keys: object,
    differences_json: object,
    secondary_json: object,
) -> ReconciliationConflictRow:
    text_values = (
        sku,
        classification,
        suggested_resolution,
        source_positions,
        target_positions,
        source_record_keys,
        target_record_keys,
        differences_json,
        secondary_json,
    )
    if type(conflict_index) is not int or any(not isinstance(item, str) for item in text_values):
        raise ParquetDecodingError("reconciliation conflict row is malformed")
    try:
        row = ReconciliationConflictRow(
            conflict_index=conflict_index,
            sku=cast(str, sku),
            classification=ReconciliationClassification(cast(str, classification)),
            suggested_resolution=SuggestedResolution(cast(str, suggested_resolution)),
            source_positions=_decode_integers(cast(str, source_positions)),
            target_positions=_decode_integers(cast(str, target_positions)),
            source_record_keys=_decode_strings(cast(str, source_record_keys)),
            target_record_keys=_decode_strings(cast(str, target_record_keys)),
            differences=_decode_differences(cast(str, differences_json)),
            secondary=_decode_secondary(cast(str, secondary_json)),
        )
    except ParquetDecodingError:
        raise
    except (TypeError, ValueError) as error:
        raise ParquetDecodingError("reconciliation conflict row is corrupt") from error
    return row


def _decode_integers(value: str) -> tuple[int, ...]:
    items = _decode_json_list(value)
    if any(type(item) is not int or item < 0 for item in items):
        raise ParquetDecodingError("reconciliation conflict positions are malformed")
    return tuple(cast("list[int]", items))


def _decode_strings(value: str) -> tuple[str, ...]:
    items = _decode_json_list(value)
    if any(not isinstance(item, str) or not item for item in items):
        raise ParquetDecodingError("reconciliation conflict record keys are malformed")
    return tuple(cast("list[str]", items))


def _decode_differences(value: str) -> tuple[FieldDifference, ...]:
    items = _decode_json_list(value)
    differences: list[FieldDifference] = []
    for item in items:
        if not isinstance(item, dict):
            raise ParquetDecodingError("reconciliation conflict differences are malformed")
        mapping = cast(dict[object, object], item)
        if set(mapping) != {"field", "kind", "source", "target"}:
            raise ParquetDecodingError("reconciliation conflict differences are malformed")
        field, kind, source, target = (
            mapping["field"],
            mapping["kind"],
            mapping["source"],
            mapping["target"],
        )
        if any(not isinstance(part, str) for part in (field, kind, source, target)):
            raise ParquetDecodingError("reconciliation conflict differences are malformed")
        try:
            differences.append(
                FieldDifference(
                    field=cast(str, field),
                    kind=FieldDifferenceKind(cast(str, kind)),
                    source_text=cast(str, source),
                    target_text=cast(str, target),
                )
            )
        except (TypeError, ValueError) as error:
            raise ParquetDecodingError("reconciliation conflict differences are corrupt") from error
    return tuple(differences)


def _decode_secondary(value: str) -> tuple[SecondaryEvidence, ...]:
    items = _decode_json_list(value)
    evidence: list[SecondaryEvidence] = []
    for item in items:
        if not isinstance(item, dict):
            raise ParquetDecodingError("reconciliation conflict evidence is malformed")
        mapping = cast(dict[object, object], item)
        if set(mapping) != {"kind", "value"}:
            raise ParquetDecodingError("reconciliation conflict evidence is malformed")
        kind, evidence_value = mapping["kind"], mapping["value"]
        if not isinstance(kind, str) or not isinstance(evidence_value, str):
            raise ParquetDecodingError("reconciliation conflict evidence is malformed")
        try:
            evidence.append(
                SecondaryEvidence(
                    SecondaryEvidenceKind(kind),
                    evidence_value,
                )
            )
        except (TypeError, ValueError) as error:
            raise ParquetDecodingError("reconciliation conflict evidence is corrupt") from error
    return tuple(evidence)


def _decode_json_list(value: str) -> list[object]:
    try:
        decoded = cast(object, json.loads(value))
    except json.JSONDecodeError:
        raise ParquetDecodingError("reconciliation conflict column is malformed") from None
    if not isinstance(decoded, list):
        raise ParquetDecodingError("reconciliation conflict column is malformed")
    return cast("list[object]", decoded)
