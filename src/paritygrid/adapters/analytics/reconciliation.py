"""DuckDB reconciliation queries over verified normalized Parquet artifacts."""

import hashlib
import json
import os
import stat
from decimal import Decimal
from pathlib import Path
from typing import BinaryIO, Protocol, cast

import pyarrow as pa
import pyarrow.parquet as pq

from paritygrid.adapters.analytics.duckdb import DuckDBLifecycleCoordinator
from paritygrid.adapters.analytics.views import (
    DuckDBAnalyticalViewRegistry,
    DuckDBViewColumnDefinition,
    DuckDBViewDefinition,
)
from paritygrid.adapters.artifacts.parquet.normalized import (
    decode_normalized_inventory_table,
    normalized_inventory_schema,
)
from paritygrid.adapters.artifacts.paths import resolve_artifact_path, resolve_artifact_root
from paritygrid.application.ports.analytics import (
    AnalyticalDatabaseError,
    AnalyticalViewCatalogSnapshot,
    AnalyticalViewCorruptionError,
    AnalyticalViewName,
    AnalyticalViewSchemaError,
    AnalyticalViewVersion,
)
from paritygrid.application.ports.artifacts import ArtifactManifestRecord, ArtifactPathError
from paritygrid.application.ports.parquet import ParquetDecodingError
from paritygrid.application.ports.reconciliation_analytics import (
    NormalizedArtifactSet,
    ReconciliationQueryCorruptionError,
    ReconciliationQueryCursor,
    ReconciliationQueryEngine,
    ReconciliationQueryIntegrityError,
    ReconciliationQueryInvalidError,
    ReconciliationQueryPage,
    ReconciliationQuerySnapshot,
    ReconciliationQueryStateError,
    ReconciliationQueryStorageError,
    validate_reconciliation_page_limit,
)
from paritygrid.domain.models import (
    ConnectorId,
    CurrencyCode,
    InventoryAttributes,
    InventoryRecord,
    Money,
    UtcTimestamp,
)
from paritygrid.domain.reconciliation import (
    ReconciliationClassification,
    ReconciliationField,
    ReconciliationOutcome,
)

_REFERENCE_TABLE = "paritygrid_reconciliation_reference"
_TARGET_TABLE = "paritygrid_reconciliation_target"
_REFERENCE_VIEW = "pgv_reconciliation_10_reference_v1"
_TARGET_VIEW = "pgv_reconciliation_20_target_v1"
_OUTCOMES_VIEW = "pgv_reconciliation_30_outcomes_v1"
_SUMMARY_VIEW = "pgv_reconciliation_40_summary_v1"
_VERIFY_CHUNK_BYTES = 1_048_576


class _Digest(Protocol):
    def update(self, value: bytes, /) -> None: ...


_TABLE_DEFINITION = (
    "record_index BIGINT, sku VARCHAR, name VARCHAR, quantity INTEGER, "
    "unit_price_minor_units BIGINT, unit_price_currency VARCHAR, "
    "unit_price_exponent TINYINT, updated_at TIMESTAMPTZ, connector_id VARCHAR, "
    'source_record_key VARCHAR, attributes STRUCT("key" VARCHAR, "value" VARCHAR)[]'
)
_ROW_COLUMNS = (
    DuckDBViewColumnDefinition("record_index", "BIGINT", True),
    DuckDBViewColumnDefinition("sku", "VARCHAR", True),
    DuckDBViewColumnDefinition("name", "VARCHAR", True),
    DuckDBViewColumnDefinition("quantity", "INTEGER", True),
    DuckDBViewColumnDefinition("unit_price_minor_units", "BIGINT", True),
    DuckDBViewColumnDefinition("unit_price_currency", "VARCHAR", True),
    DuckDBViewColumnDefinition("unit_price_exponent", "TINYINT", True),
    DuckDBViewColumnDefinition("updated_at", "VARCHAR", True),
    DuckDBViewColumnDefinition("connector_id", "VARCHAR", True),
    DuckDBViewColumnDefinition("source_record_key", "VARCHAR", True),
    DuckDBViewColumnDefinition("attributes_json", "JSON", True),
)
_OUTCOME_COLUMNS = (
    DuckDBViewColumnDefinition("sku", "VARCHAR", True),
    DuckDBViewColumnDefinition("source_count", "BIGINT", True),
    DuckDBViewColumnDefinition("target_count", "BIGINT", True),
    DuckDBViewColumnDefinition("classification", "VARCHAR", True),
    DuckDBViewColumnDefinition("name_mismatch", "BOOLEAN", True),
    DuckDBViewColumnDefinition("quantity_mismatch", "BOOLEAN", True),
    DuckDBViewColumnDefinition("unit_price_mismatch", "BOOLEAN", True),
    DuckDBViewColumnDefinition("updated_at_mismatch", "BOOLEAN", True),
    DuckDBViewColumnDefinition("attributes_mismatch", "BOOLEAN", True),
)
_SUMMARY_COLUMNS = (
    DuckDBViewColumnDefinition("classification", "VARCHAR", True),
    DuckDBViewColumnDefinition("key_count", "BIGINT", True),
)
_SUMMARY_SELECT = (
    f"SELECT classification, count(*) AS key_count FROM {_OUTCOMES_VIEW} "
    "GROUP BY classification ORDER BY classification"
)
_SOURCE_SELECT = (
    "SELECT record_index, sku, name, quantity, unit_price_minor_units, "
    "unit_price_currency, unit_price_exponent, "
    "strftime(updated_at, '%Y-%m-%dT%H:%M:%S.%fZ') AS updated_at, connector_id, "
    "source_record_key, to_json(attributes) AS attributes_json FROM {table}"
)
_OUTCOMES_SELECT = f"""WITH source_rows AS (
 SELECT sku, count(*) AS record_count,
  first(name ORDER BY record_index, connector_id, source_record_key) AS name,
  first(quantity ORDER BY record_index, connector_id, source_record_key) AS quantity,
  first(unit_price_minor_units ORDER BY record_index, connector_id, source_record_key)
   AS price_units,
  first(unit_price_currency ORDER BY record_index, connector_id, source_record_key)
   AS price_currency,
  first(unit_price_exponent ORDER BY record_index, connector_id, source_record_key)
   AS price_exponent,
  first(updated_at ORDER BY record_index, connector_id, source_record_key) AS updated_at,
  first(attributes_json ORDER BY record_index, connector_id, source_record_key) AS attributes_json
 FROM {_REFERENCE_VIEW} GROUP BY sku
), target_rows AS (
 SELECT sku, count(*) AS record_count,
  first(name ORDER BY record_index, connector_id, source_record_key) AS name,
  first(quantity ORDER BY record_index, connector_id, source_record_key) AS quantity,
  first(unit_price_minor_units ORDER BY record_index, connector_id, source_record_key)
   AS price_units,
  first(unit_price_currency ORDER BY record_index, connector_id, source_record_key)
   AS price_currency,
  first(unit_price_exponent ORDER BY record_index, connector_id, source_record_key)
   AS price_exponent,
  first(updated_at ORDER BY record_index, connector_id, source_record_key) AS updated_at,
  first(attributes_json ORDER BY record_index, connector_id, source_record_key) AS attributes_json
 FROM {_TARGET_VIEW} GROUP BY sku
), compared AS (
 SELECT coalesce(source_rows.sku, target_rows.sku) AS sku,
  coalesce(source_rows.record_count, 0) AS source_count,
  coalesce(target_rows.record_count, 0) AS target_count,
  source_rows.name IS DISTINCT FROM target_rows.name AS name_mismatch,
  source_rows.quantity IS DISTINCT FROM target_rows.quantity AS quantity_mismatch,
  (source_rows.price_units, source_rows.price_currency, source_rows.price_exponent)
   IS DISTINCT FROM
  (target_rows.price_units, target_rows.price_currency, target_rows.price_exponent)
   AS unit_price_mismatch,
  source_rows.updated_at IS DISTINCT FROM target_rows.updated_at AS updated_at_mismatch,
  source_rows.attributes_json IS DISTINCT FROM target_rows.attributes_json AS attributes_mismatch
 FROM source_rows FULL OUTER JOIN target_rows USING (sku)
)
SELECT sku, source_count, target_count,
 CASE
  WHEN source_count > 1 AND target_count > 1 THEN 'duplicate_both'
  WHEN source_count > 1 THEN 'duplicate_source'
  WHEN target_count > 1 THEN 'duplicate_target'
  WHEN source_count = 0 THEN 'missing_from_source'
  WHEN target_count = 0 THEN 'missing_from_target'
  WHEN name_mismatch OR quantity_mismatch OR unit_price_mismatch
    OR updated_at_mismatch OR attributes_mismatch THEN 'field_mismatch'
  ELSE 'match'
 END AS classification,
 CASE WHEN source_count = 1 AND target_count = 1 THEN name_mismatch ELSE false END AS name_mismatch,
 CASE WHEN source_count = 1 AND target_count = 1
  THEN quantity_mismatch ELSE false END AS quantity_mismatch,
 CASE WHEN source_count = 1 AND target_count = 1
  THEN unit_price_mismatch ELSE false END AS unit_price_mismatch,
 CASE WHEN source_count = 1 AND target_count = 1
  THEN updated_at_mismatch ELSE false END AS updated_at_mismatch,
 CASE WHEN source_count = 1 AND target_count = 1
  THEN attributes_mismatch ELSE false END AS attributes_mismatch
FROM compared"""


class DuckDBReconciliationQueryEngine(ReconciliationQueryEngine):
    """Rebuild fixed reviewed views and return independently validated outcomes."""

    __slots__ = ("_artifact_root", "_database", "_registry", "_snapshot")

    def __init__(self, database: DuckDBLifecycleCoordinator, artifact_root: Path) -> None:
        value = cast(object, database)
        if type(value) is not DuckDBLifecycleCoordinator:
            raise TypeError("reconciliation query requires a DuckDB lifecycle coordinator")
        try:
            root = resolve_artifact_root(artifact_root)
        except ArtifactPathError, TypeError:
            raise ReconciliationQueryInvalidError("artifact root is invalid") from None
        self._database = value
        self._registry = DuckDBAnalyticalViewRegistry(value)
        self._artifact_root = root
        self._snapshot: ReconciliationQuerySnapshot | None = None

    def rebuild(
        self,
        reference: NormalizedArtifactSet,
        target: NormalizedArtifactSet,
    ) -> ReconciliationQuerySnapshot:
        """Verify committed inputs, recreate DuckDB, and install the fixed v1 views."""
        reference_set = _require_artifact_set(reference, "reference")
        target_set = _require_artifact_set(target, "target")
        reference_paths = self._verified_paths(reference_set)
        target_paths = self._verified_paths(target_set)
        try:
            self._database.recreate()
            with self._database._transaction():  # pyright: ignore[reportPrivateUsage]
                self._load_table(_REFERENCE_TABLE, reference_paths)
                self._load_table(_TARGET_TABLE, target_paths)
                self._require_table_count(_REFERENCE_TABLE, reference_set.row_count)
                self._require_table_count(_TARGET_TABLE, target_set.row_count)
            catalog = self._registry.synchronize(_view_definitions())
            self._require_duplicate_bounds()
            for path, manifest in zip(reference_paths, reference_set.manifests, strict=True):
                _verify_manifest_file(path, manifest)
            for path, manifest in zip(target_paths, target_set.manifests, strict=True):
                _verify_manifest_file(path, manifest)
        except ReconciliationQueryCorruptionError, ReconciliationQueryIntegrityError:
            self._snapshot = None
            raise
        except AnalyticalDatabaseError:
            self._snapshot = None
            raise ReconciliationQueryStorageError(
                "reconciliation analytical rebuild failed"
            ) from None
        query_sha256 = _query_sha256(
            reference_set.manifest_sha256,
            target_set.manifest_sha256,
            catalog,
        )
        snapshot = ReconciliationQuerySnapshot(
            reference_manifest_sha256=reference_set.manifest_sha256,
            target_manifest_sha256=target_set.manifest_sha256,
            reference_artifact_sha256s=reference_set.artifact_sha256s,
            target_artifact_sha256s=target_set.artifact_sha256s,
            reference_row_count=reference_set.row_count,
            target_row_count=target_set.row_count,
            query_sha256=query_sha256,
            view_catalog=catalog,
        )
        self._snapshot = snapshot
        return snapshot

    def list_outcomes(
        self,
        snapshot: ReconciliationQuerySnapshot,
        *,
        limit: int,
        after: ReconciliationQueryCursor | None = None,
    ) -> ReconciliationQueryPage:
        """Return one SKU page and verify SQL classifications with domain values."""
        installed = self._require_prepared(snapshot)
        page_size = validate_reconciliation_page_limit(limit)
        cursor = _require_cursor(after)
        try:
            parameters: tuple[object, ...]
            predicate = ""
            if cursor is None:
                parameters = (page_size + 1,)
            else:
                predicate = " WHERE sku > ?"
                parameters = (cursor.sku, page_size + 1)
            rows = self._database._fetch_all(  # pyright: ignore[reportPrivateUsage]
                f"SELECT * FROM {_OUTCOMES_VIEW}{predicate} ORDER BY sku LIMIT ?",
                parameters,
            )
            visible = rows[:page_size]
            skus = tuple(_outcome_key(row) for row in visible)
            source = self._records_for(_REFERENCE_VIEW, skus)
            target = self._records_for(_TARGET_VIEW, skus)
            items = tuple(
                _validated_outcome(
                    row,
                    source.get(sku, ()),
                    target.get(sku, ()),
                )
                for row, sku in zip(visible, skus, strict=True)
            )
        except ReconciliationQueryCorruptionError:
            raise
        except AnalyticalViewCorruptionError, AnalyticalViewSchemaError:
            raise ReconciliationQueryCorruptionError(
                "reconciliation analytical views are corrupt"
            ) from None
        except AnalyticalDatabaseError:
            raise ReconciliationQueryStorageError("reconciliation query failed") from None
        next_cursor = ReconciliationQueryCursor(items[-1].sku) if len(rows) > page_size else None
        return ReconciliationQueryPage(installed, items, next_cursor)

    def classification_counts(
        self,
        snapshot: ReconciliationQuerySnapshot,
    ) -> tuple[tuple[ReconciliationClassification, int], ...]:
        """Return the SQL classification counts for the prepared snapshot."""
        self._require_prepared(snapshot)
        try:
            rows = self._database._fetch_all(  # pyright: ignore[reportPrivateUsage]
                f"SELECT classification, key_count FROM {_SUMMARY_VIEW} ORDER BY classification"
            )
            totals = self._database._fetch_all(  # pyright: ignore[reportPrivateUsage]
                f"SELECT count(*) FROM {_OUTCOMES_VIEW}"
            )
        except AnalyticalViewCorruptionError, AnalyticalViewSchemaError:
            raise ReconciliationQueryCorruptionError(
                "reconciliation analytical views are corrupt"
            ) from None
        except AnalyticalDatabaseError:
            raise ReconciliationQueryStorageError("reconciliation summary query failed") from None
        counts: list[tuple[ReconciliationClassification, int]] = []
        names: list[str] = []
        for row in rows:
            if len(row) != 2 or type(row[0]) is not str or type(row[1]) is not int or row[1] < 1:
                raise ReconciliationQueryCorruptionError("reconciliation summary row is malformed")
            try:
                counts.append((ReconciliationClassification(row[0]), row[1]))
            except ValueError:
                raise ReconciliationQueryCorruptionError(
                    "reconciliation summary classification is unknown"
                ) from None
            names.append(row[0])
        if names != sorted(names) or len(set(names)) != len(names):
            raise ReconciliationQueryCorruptionError(
                "reconciliation summary rows are not canonically ordered"
            )
        if totals != ((sum(count for _classification, count in counts),),):
            raise ReconciliationQueryCorruptionError(
                "reconciliation summary disagrees with the outcome total"
            )
        return tuple(counts)

    def _require_prepared(
        self,
        snapshot: ReconciliationQuerySnapshot,
    ) -> ReconciliationQuerySnapshot:
        installed = cast(object, snapshot)
        if type(installed) is not ReconciliationQuerySnapshot:
            raise TypeError("reconciliation snapshot is invalid")
        if self._snapshot is None or installed != self._snapshot:
            raise ReconciliationQueryStateError("reconciliation snapshot is not currently prepared")
        try:
            if self._registry.snapshot() != installed.view_catalog:
                raise ReconciliationQueryCorruptionError("reconciliation analytical views changed")
        except ReconciliationQueryCorruptionError:
            raise
        except AnalyticalViewCorruptionError, AnalyticalViewSchemaError:
            raise ReconciliationQueryCorruptionError(
                "reconciliation analytical views are corrupt"
            ) from None
        except AnalyticalDatabaseError:
            raise ReconciliationQueryStorageError("reconciliation query failed") from None
        return installed

    def _verified_paths(self, source: NormalizedArtifactSet) -> tuple[Path, ...]:
        paths: list[Path] = []
        for manifest in source.manifests:
            try:
                path = resolve_artifact_path(self._artifact_root, manifest.relative_path)
            except ArtifactPathError:
                raise ReconciliationQueryIntegrityError(
                    "reconciliation artifact is not safely confined"
                ) from None
            _verify_manifest_file(path, manifest)
            try:
                table = pq.read_table(path)  # pyright: ignore[reportUnknownMemberType]
                decoded = decode_normalized_inventory_table(table)
            except OSError, pa.ArrowException, ParquetDecodingError:
                raise ReconciliationQueryIntegrityError(
                    "reconciliation artifact is not exact normalized Parquet v1"
                ) from None
            if len(decoded.records) != manifest.row_count:
                raise ReconciliationQueryIntegrityError(
                    "reconciliation artifact row count differs from its manifest"
                )
            paths.append(path)
        return tuple(paths)

    def _load_table(self, name: str, paths: tuple[Path, ...]) -> None:
        self._database._execute(  # pyright: ignore[reportPrivateUsage]
            f"CREATE TABLE {name} ({_TABLE_DEFINITION})"
        )
        if paths:
            self._database._execute(  # pyright: ignore[reportPrivateUsage]
                f"INSERT INTO {name} SELECT * FROM read_parquet(?, union_by_name = false)",
                ([path.as_posix() for path in paths],),
            )

    def _require_table_count(self, name: str, expected: int) -> None:
        rows = self._database._fetch_all(  # pyright: ignore[reportPrivateUsage]
            f"SELECT count(*) FROM {name}"
        )
        if rows != ((expected,),):
            raise ReconciliationQueryCorruptionError(
                "reconciliation analytical row count is corrupt"
            )

    def _require_duplicate_bounds(self) -> None:
        rows = self._database._fetch_all(  # pyright: ignore[reportPrivateUsage]
            f"SELECT max(source_count), max(target_count) FROM {_OUTCOMES_VIEW}"
        )
        if len(rows) != 1 or len(rows[0]) != 2:
            raise ReconciliationQueryCorruptionError(
                "reconciliation duplicate counts are unavailable"
            )
        for value in rows[0]:
            if value is not None and (
                type(value) is not int or value > ReconciliationOutcome.MAX_RECORDS_PER_SIDE
            ):
                raise ReconciliationQueryIntegrityError(
                    "reconciliation input exceeds the duplicate-key limit"
                )

    def _records_for(
        self, view: str, skus: tuple[str, ...]
    ) -> dict[str, tuple[InventoryRecord, ...]]:
        if not skus:
            return {}
        placeholders = ", ".join("?" for _ in skus)
        rows = self._database._fetch_all(  # pyright: ignore[reportPrivateUsage]
            f"SELECT * FROM {view} WHERE sku IN ({placeholders}) "
            "ORDER BY sku, name, quantity, unit_price_currency, "
            "unit_price_minor_units, unit_price_exponent, updated_at, "
            "connector_id, source_record_key, record_index",
            skus,
        )
        grouped: dict[str, list[InventoryRecord]] = {sku: [] for sku in skus}
        for row in rows:
            record = _record_from_row(row)
            if record.sku not in grouped:
                raise ReconciliationQueryCorruptionError(
                    "reconciliation row returned outside the requested page"
                )
            grouped[record.sku].append(record)
        return {key: tuple(value) for key, value in grouped.items()}


def _view_definitions() -> tuple[DuckDBViewDefinition, ...]:
    return (
        DuckDBViewDefinition(
            AnalyticalViewName(_REFERENCE_VIEW),
            AnalyticalViewVersion(1),
            _SOURCE_SELECT.format(table=_REFERENCE_TABLE),
            _ROW_COLUMNS,
        ),
        DuckDBViewDefinition(
            AnalyticalViewName(_TARGET_VIEW),
            AnalyticalViewVersion(1),
            _SOURCE_SELECT.format(table=_TARGET_TABLE),
            _ROW_COLUMNS,
        ),
        DuckDBViewDefinition(
            AnalyticalViewName(_OUTCOMES_VIEW),
            AnalyticalViewVersion(1),
            _OUTCOMES_SELECT,
            _OUTCOME_COLUMNS,
        ),
        DuckDBViewDefinition(
            AnalyticalViewName(_SUMMARY_VIEW),
            AnalyticalViewVersion(1),
            _SUMMARY_SELECT,
            _SUMMARY_COLUMNS,
        ),
    )


def _require_artifact_set(value: object, side: str) -> NormalizedArtifactSet:
    if type(value) is not NormalizedArtifactSet:
        raise ReconciliationQueryInvalidError(
            f"{side} reconciliation input must use NormalizedArtifactSet"
        )
    return value


def _require_cursor(value: object) -> ReconciliationQueryCursor | None:
    if value is not None and type(value) is not ReconciliationQueryCursor:
        raise ReconciliationQueryInvalidError(
            "reconciliation cursor must use ReconciliationQueryCursor"
        )
    return value


def _verify_manifest_file(path: Path, manifest: ArtifactManifestRecord) -> None:
    descriptor: int | None = None
    try:
        initial = os.stat(path, follow_symlinks=False)
        if not stat.S_ISREG(initial.st_mode):
            raise ReconciliationQueryIntegrityError(
                "reconciliation manifest does not reference a regular file"
            )
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ReconciliationQueryIntegrityError(
                "reconciliation manifest does not reference a regular file"
            )
        digest = hashlib.sha256()
        size = 0
        with os.fdopen(descriptor, "rb", closefd=True) as stream:
            descriptor = None
            size = _hash_stream(stream, digest)
            after = os.fstat(stream.fileno())
        installed = os.stat(path, follow_symlinks=False)
        if (
            initial.st_dev,
            initial.st_ino,
            initial.st_size,
            initial.st_mtime_ns,
        ) != (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ):
            raise ReconciliationQueryIntegrityError(
                "reconciliation artifact changed during verification"
            )
        expected_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        if expected_identity != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ) or expected_identity != (
            installed.st_dev,
            installed.st_ino,
            installed.st_size,
            installed.st_mtime_ns,
        ):
            raise ReconciliationQueryIntegrityError(
                "reconciliation artifact changed during verification"
            )
        if size != manifest.byte_size or digest.hexdigest() != manifest.sha256:
            raise ReconciliationQueryIntegrityError(
                "reconciliation artifact differs from its committed manifest"
            )
        parquet_file = pq.ParquetFile(path)
        if (
            parquet_file.metadata.num_rows != manifest.row_count
            or not parquet_file.schema_arrow.equals(
                normalized_inventory_schema(), check_metadata=True
            )
        ):
            raise ReconciliationQueryIntegrityError(
                "reconciliation artifact schema or row count is invalid"
            )
    except ReconciliationQueryIntegrityError:
        raise
    except OSError, pa.ArrowException:
        raise ReconciliationQueryIntegrityError(
            "reconciliation artifact could not be verified"
        ) from None
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                raise ReconciliationQueryIntegrityError(
                    "reconciliation verification handle could not be closed"
                ) from None


def _hash_stream(stream: BinaryIO, digest: _Digest) -> int:
    total = 0
    while True:
        chunk = stream.read(_VERIFY_CHUNK_BYTES)
        if not chunk:
            return total
        total += len(chunk)
        digest.update(chunk)


def _outcome_key(row: tuple[object, ...]) -> str:
    if len(row) != 9 or type(row[0]) is not str:
        raise ReconciliationQueryCorruptionError("reconciliation outcome row is malformed")
    return row[0]


def _record_from_row(row: tuple[object, ...]) -> InventoryRecord:
    if len(row) != 11:
        raise ReconciliationQueryCorruptionError("reconciliation source row is malformed")
    (
        record_index,
        sku,
        name,
        quantity,
        minor_units,
        currency,
        exponent,
        updated_at,
        connector_id,
        source_key,
        attributes_json,
    ) = row
    if (
        type(record_index) is not int
        or type(sku) is not str
        or type(name) is not str
        or type(quantity) is not int
        or type(minor_units) is not int
        or type(currency) is not str
        or type(exponent) is not int
        or type(updated_at) is not str
        or type(connector_id) is not str
        or type(source_key) is not str
        or type(attributes_json) is not str
    ):
        raise ReconciliationQueryCorruptionError("reconciliation source row is malformed")
    try:
        attributes = _attributes_from_json(attributes_json)
        return InventoryRecord(
            sku=sku,
            name=name,
            quantity=quantity,
            unit_price=Money(
                Decimal(minor_units).scaleb(-exponent),
                CurrencyCode(currency),
                exponent,
            ),
            updated_at=UtcTimestamp.parse(updated_at),
            connector_id=ConnectorId(connector_id),
            source_record_key=source_key,
            attributes=attributes,
        )
    except TypeError, ValueError:
        raise ReconciliationQueryCorruptionError(
            "reconciliation source row violates the domain contract"
        ) from None


def _attributes_from_json(value: str) -> InventoryAttributes:
    try:
        decoded = cast(object, json.loads(value))
    except json.JSONDecodeError:
        raise ReconciliationQueryCorruptionError(
            "reconciliation attributes are malformed"
        ) from None
    if type(decoded) is not list:
        raise ReconciliationQueryCorruptionError("reconciliation attributes are malformed")
    pairs: list[tuple[str, str]] = []
    for item in cast(list[object], decoded):
        if type(item) is not dict:
            raise ReconciliationQueryCorruptionError("reconciliation attributes are malformed")
        mapping = cast(dict[object, object], item)
        if set(mapping) != {"key", "value"}:
            raise ReconciliationQueryCorruptionError("reconciliation attributes are malformed")
        key, child = mapping["key"], mapping["value"]
        if type(key) is not str or type(child) is not str:
            raise ReconciliationQueryCorruptionError("reconciliation attributes are malformed")
        pairs.append((key, child))
    try:
        attributes = InventoryAttributes(tuple(pairs))
    except TypeError, ValueError:
        raise ReconciliationQueryCorruptionError(
            "reconciliation attributes violate the domain contract"
        ) from None
    if attributes.items != tuple(pairs):
        raise ReconciliationQueryCorruptionError("reconciliation attributes are not canonical")
    return attributes


def _validated_outcome(
    row: tuple[object, ...],
    source: tuple[InventoryRecord, ...],
    target: tuple[InventoryRecord, ...],
) -> ReconciliationOutcome:
    if (
        len(row) != 9
        or type(row[0]) is not str
        or type(row[1]) is not int
        or type(row[2]) is not int
        or type(row[3]) is not str
        or any(type(value) is not bool for value in row[4:])
    ):
        raise ReconciliationQueryCorruptionError("reconciliation outcome row is malformed")
    try:
        classification = ReconciliationClassification(row[3])
        outcome = ReconciliationOutcome(source, target)
    except TypeError, ValueError:
        raise ReconciliationQueryCorruptionError(
            "reconciliation outcome violates the domain contract"
        ) from None
    if (
        outcome.sku != row[0]
        or len(source) != row[1]
        or len(target) != row[2]
        or outcome.classification is not classification
    ):
        raise ReconciliationQueryCorruptionError(
            "reconciliation SQL and domain classifications disagree"
        )
    mismatch_flags = {
        ReconciliationField.NAME: row[4],
        ReconciliationField.QUANTITY: row[5],
        ReconciliationField.UNIT_PRICE: row[6],
        ReconciliationField.UPDATED_AT: row[7],
        ReconciliationField.ATTRIBUTES: row[8],
    }
    actual_fields = {mismatch.field for mismatch in outcome.mismatches}
    expected_fields = {field for field, differs in mismatch_flags.items() if differs}
    if actual_fields != expected_fields:
        raise ReconciliationQueryCorruptionError(
            "reconciliation SQL and domain mismatch evidence disagree"
        )
    return outcome


def _query_sha256(reference: str, target: str, catalog: AnalyticalViewCatalogSnapshot) -> str:
    encoded = json.dumps(
        {
            "reference_manifest_sha256": reference,
            "target_manifest_sha256": target,
            "views": [
                {
                    "definition_sha256": view.definition_sha256,
                    "name": str(view.name),
                    "output_schema_sha256": view.output_schema_sha256,
                    "version": view.version.value,
                }
                for view in catalog.views
            ],
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
