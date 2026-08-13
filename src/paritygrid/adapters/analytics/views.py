"""Versioned DuckDB analytical view registry."""

import hashlib
import json
import re
from dataclasses import dataclass
from typing import cast

from paritygrid.adapters.analytics.duckdb import DuckDBLifecycleCoordinator
from paritygrid.application.ports.analytics import (
    AnalyticalViewCatalogSnapshot,
    AnalyticalViewColumn,
    AnalyticalViewConflictError,
    AnalyticalViewCorruptionError,
    AnalyticalViewInvalidError,
    AnalyticalViewName,
    AnalyticalViewRecord,
    AnalyticalViewSchemaError,
    AnalyticalViewVersion,
)

_REGISTRY_TABLE = "paritygrid_analytical_view_registry"
_COLUMN_NAME = re.compile(r"[a-z][a-z0-9_]*\Z")
_TYPE_NAME = re.compile(r"[A-Z][A-Z0-9_ (),]*\Z")
_LOWER_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_SELECT_SQL_BYTES = 262_144
_METADATA_COLUMNS = (
    ("view_name", "VARCHAR", False),
    ("definition_version", "INTEGER", False),
    ("definition_sha256", "VARCHAR", False),
    ("installed_sql_sha256", "VARCHAR", False),
    ("output_schema_json", "VARCHAR", False),
)


@dataclass(frozen=True, slots=True)
class DuckDBViewColumnDefinition:
    """Exact DuckDB output column declared by trusted adapter code."""

    name: str
    type_name: str
    nullable: bool

    def __post_init__(self) -> None:
        name = cast(object, self.name)
        type_name = cast(object, self.type_name)
        if type(name) is not str or len(name) > 128 or not _COLUMN_NAME.fullmatch(name):
            raise AnalyticalViewInvalidError("analytical view column name is not canonical")
        if (
            type(type_name) is not str
            or len(type_name) > 128
            or not _TYPE_NAME.fullmatch(type_name)
        ):
            raise AnalyticalViewInvalidError("analytical view column type is not canonical")
        if type(self.nullable) is not bool:
            raise TypeError("analytical view column nullability must be boolean")

    def record(self) -> AnalyticalViewColumn:
        """Return the detached dependency-neutral column projection."""
        return AnalyticalViewColumn(self.name, self.type_name, self.nullable)


@dataclass(frozen=True, slots=True)
class DuckDBViewDefinition:
    """One immutable versioned SELECT and its exact declared output."""

    name: AnalyticalViewName
    version: AnalyticalViewVersion
    select_sql: str
    columns: tuple[DuckDBViewColumnDefinition, ...]

    def __post_init__(self) -> None:
        if type(self.name) is not AnalyticalViewName:
            raise TypeError("analytical view definition name is invalid")
        if type(self.version) is not AnalyticalViewVersion:
            raise TypeError("analytical view definition version is invalid")
        sql = cast(object, self.select_sql)
        if type(sql) is not str:
            raise TypeError("analytical view SELECT must be a string")
        encoded = sql.encode("utf-8")
        first_word = sql.split(maxsplit=1)[0].casefold() if sql else ""
        if (
            not sql
            or sql != sql.strip()
            or first_word not in {"select", "with"}
            or ";" in sql
            or "\x00" in sql
            or len(encoded) > _MAX_SELECT_SQL_BYTES
        ):
            raise AnalyticalViewInvalidError("analytical view SELECT is not canonical")
        if type(self.columns) is not tuple or not self.columns:
            raise TypeError("analytical view definition columns must be a nonempty tuple")
        if any(type(column) is not DuckDBViewColumnDefinition for column in self.columns):
            raise TypeError("analytical view definition columns are invalid")
        names = tuple(column.name for column in self.columns)
        if len(set(names)) != len(names):
            raise AnalyticalViewInvalidError("analytical view definition columns must be unique")

    @property
    def definition_sha256(self) -> str:
        """Hash the exact reviewed SELECT bytes."""
        return _sha256(self.select_sql)

    @property
    def output_schema_json(self) -> str:
        """Return canonical JSON for the declared ordered schema."""
        return _encode_schema(tuple(column.record() for column in self.columns))

    @property
    def output_schema_sha256(self) -> str:
        """Hash the exact canonical output schema."""
        return _sha256(self.output_schema_json)


class DuckDBAnalyticalViewRegistry:
    """Synchronize and verify a complete managed view catalog."""

    __slots__ = ("_database",)

    def __init__(self, database: DuckDBLifecycleCoordinator) -> None:
        value = cast(object, database)
        if type(value) is not DuckDBLifecycleCoordinator:
            raise TypeError("analytical view registry requires a DuckDB lifecycle coordinator")
        self._database = value

    def synchronize(
        self, definitions: tuple[DuckDBViewDefinition, ...]
    ) -> AnalyticalViewCatalogSnapshot:
        """Atomically install the exact supplied catalog and remove stale managed views."""
        ordered = _validated_definitions(definitions)
        with self._database._transaction():  # pyright: ignore[reportPrivateUsage]
            self._ensure_registry_table()
            existing = {record.name: record for record in self._read_snapshot().views}
            desired_names = {definition.name for definition in ordered}
            for name in sorted(set(existing) - desired_names):
                self._database._execute(  # pyright: ignore[reportPrivateUsage]
                    f"DROP VIEW {_quote_identifier(str(name))}"
                )
                self._database._execute(  # pyright: ignore[reportPrivateUsage]
                    f"DELETE FROM {_REGISTRY_TABLE} WHERE view_name = ?", (str(name),)
                )
            for definition in ordered:
                current = existing.get(definition.name)
                if current is not None:
                    if current.version.value > definition.version.value:
                        raise AnalyticalViewConflictError(
                            "analytical view version cannot move backward"
                        )
                    if current.version == definition.version:
                        if (
                            current.definition_sha256 != definition.definition_sha256
                            or current.output_schema_sha256 != definition.output_schema_sha256
                        ):
                            raise AnalyticalViewConflictError(
                                "analytical view version has different content"
                            )
                        continue
                self._install(definition)
            return self._read_snapshot()

    def rebuild(
        self, definitions: tuple[DuckDBViewDefinition, ...]
    ) -> AnalyticalViewCatalogSnapshot:
        """Discard disposable state and install the supplied catalog from empty."""
        ordered = _validated_definitions(definitions)
        self._database.recreate()
        return self.synchronize(ordered)

    def snapshot(self) -> AnalyticalViewCatalogSnapshot:
        """Strictly validate and return the currently installed catalog."""
        return self._read_snapshot()

    def _install(self, definition: DuckDBViewDefinition) -> None:
        name = str(definition.name)
        self._database._execute(  # pyright: ignore[reportPrivateUsage]
            f"CREATE OR REPLACE VIEW {_quote_identifier(name)} AS {definition.select_sql}"
        )
        actual_columns = self._describe_view(definition.name)
        expected_columns = tuple(column.record() for column in definition.columns)
        if actual_columns != expected_columns:
            raise AnalyticalViewSchemaError(
                "analytical view output differs from its declared schema"
            )
        installed_sql_sha256 = _sha256(self._installed_sql(definition.name))
        self._database._execute(  # pyright: ignore[reportPrivateUsage]
            f"INSERT OR REPLACE INTO {_REGISTRY_TABLE} ("
            "view_name, definition_version, definition_sha256, "
            "installed_sql_sha256, output_schema_json"
            ") VALUES (?, ?, ?, ?, ?)",
            (
                name,
                definition.version.value,
                definition.definition_sha256,
                installed_sql_sha256,
                definition.output_schema_json,
            ),
        )

    def _ensure_registry_table(self) -> None:
        relation = self._relation_type(_REGISTRY_TABLE)
        if relation is None:
            self._database._execute(  # pyright: ignore[reportPrivateUsage]
                f"CREATE TABLE {_REGISTRY_TABLE} ("
                "view_name VARCHAR PRIMARY KEY, "
                "definition_version INTEGER NOT NULL, "
                "definition_sha256 VARCHAR NOT NULL, "
                "installed_sql_sha256 VARCHAR NOT NULL, "
                "output_schema_json VARCHAR NOT NULL"
                ")"
            )
        elif relation != "BASE TABLE":
            raise AnalyticalViewCorruptionError(
                "analytical view registry relation has the wrong kind"
            )
        columns = self._database._fetch_all(  # pyright: ignore[reportPrivateUsage]
            f"DESCRIBE {_REGISTRY_TABLE}"
        )
        shape = tuple((row[0], row[1], row[2] == "YES") if len(row) >= 3 else () for row in columns)
        if shape != _METADATA_COLUMNS:
            raise AnalyticalViewCorruptionError(
                "analytical view registry table has the wrong schema"
            )

    def _read_snapshot(self) -> AnalyticalViewCatalogSnapshot:
        relation = self._relation_type(_REGISTRY_TABLE)
        managed_views = self._managed_view_names()
        if relation is None:
            if managed_views:
                raise AnalyticalViewCorruptionError(
                    "managed analytical views exist without a registry"
                )
            return AnalyticalViewCatalogSnapshot(())
        if relation != "BASE TABLE":
            raise AnalyticalViewCorruptionError(
                "analytical view registry relation has the wrong kind"
            )
        self._ensure_registry_table()
        rows = self._database._fetch_all(  # pyright: ignore[reportPrivateUsage]
            f"SELECT view_name, definition_version, definition_sha256, "
            f"installed_sql_sha256, output_schema_json FROM {_REGISTRY_TABLE} "
            "ORDER BY view_name"
        )
        registered_names = {row[0] for row in rows if len(row) == 5 and type(row[0]) is str}
        if len(registered_names) != len(rows):
            raise AnalyticalViewCorruptionError("analytical view registry row is malformed")
        if managed_views != registered_names:
            raise AnalyticalViewCorruptionError(
                "managed analytical view inventory differs from the registry"
            )
        records = tuple(self._record_from_row(row) for row in rows)
        return AnalyticalViewCatalogSnapshot(records)

    def _record_from_row(self, row: tuple[object, ...]) -> AnalyticalViewRecord:
        if len(row) != 5:
            raise AnalyticalViewCorruptionError("analytical view registry row is malformed")
        name_value, version_value, definition_hash, installed_hash, schema_json = row
        try:
            name = AnalyticalViewName(cast(str, name_value))
            version = AnalyticalViewVersion(cast(int, version_value))
        except TypeError, ValueError, AnalyticalViewInvalidError:
            raise AnalyticalViewCorruptionError(
                "analytical view registry identity is malformed"
            ) from None
        if (
            type(definition_hash) is not str
            or not _LOWER_SHA256.fullmatch(definition_hash)
            or type(installed_hash) is not str
            or not _LOWER_SHA256.fullmatch(installed_hash)
            or type(schema_json) is not str
        ):
            raise AnalyticalViewCorruptionError("analytical view registry metadata is malformed")
        columns = _decode_schema(schema_json)
        if self._describe_view(name) != columns:
            raise AnalyticalViewCorruptionError(
                "installed analytical view schema differs from the registry"
            )
        if _sha256(self._installed_sql(name)) != installed_hash:
            raise AnalyticalViewCorruptionError(
                "installed analytical view definition differs from the registry"
            )
        return AnalyticalViewRecord(
            name,
            version,
            definition_hash,
            _sha256(schema_json),
            columns,
        )

    def _describe_view(self, name: AnalyticalViewName) -> tuple[AnalyticalViewColumn, ...]:
        try:
            rows = self._database._fetch_all(  # pyright: ignore[reportPrivateUsage]
                f"DESCRIBE SELECT * FROM {_quote_identifier(str(name))}"
            )
            return tuple(_column_from_describe(row) for row in rows)
        except AnalyticalViewCorruptionError:
            raise
        except Exception:
            raise AnalyticalViewCorruptionError(
                "installed analytical view could not be described"
            ) from None

    def _installed_sql(self, name: AnalyticalViewName) -> str:
        rows = self._database._fetch_all(  # pyright: ignore[reportPrivateUsage]
            "SELECT sql FROM duckdb_views() WHERE schema_name = 'main' AND view_name = ?",
            (str(name),),
        )
        if len(rows) != 1 or len(rows[0]) != 1 or type(rows[0][0]) is not str:
            raise AnalyticalViewCorruptionError(
                "installed analytical view definition is unavailable"
            )
        return rows[0][0]

    def _managed_view_names(self) -> set[str]:
        rows = self._database._fetch_all(  # pyright: ignore[reportPrivateUsage]
            "SELECT view_name FROM duckdb_views() WHERE schema_name = 'main'"
        )
        names: set[str] = set()
        for row in rows:
            if len(row) != 1 or type(row[0]) is not str:
                raise AnalyticalViewCorruptionError("analytical view inventory is malformed")
            if row[0].startswith("pgv_"):
                try:
                    names.add(str(AnalyticalViewName(row[0])))
                except TypeError, ValueError, AnalyticalViewInvalidError:
                    raise AnalyticalViewCorruptionError(
                        "managed analytical view name is malformed"
                    ) from None
        return names

    def _relation_type(self, name: str) -> str | None:
        rows = self._database._fetch_all(  # pyright: ignore[reportPrivateUsage]
            "SELECT table_type FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name = ?",
            (name,),
        )
        if not rows:
            return None
        if len(rows) != 1 or len(rows[0]) != 1 or type(rows[0][0]) is not str:
            raise AnalyticalViewCorruptionError(
                "analytical registry relation inventory is malformed"
            )
        return rows[0][0]


def _validated_definitions(
    definitions: tuple[DuckDBViewDefinition, ...],
) -> tuple[DuckDBViewDefinition, ...]:
    if type(definitions) is not tuple:
        raise TypeError("analytical view definitions must be a tuple")
    if any(type(definition) is not DuckDBViewDefinition for definition in definitions):
        raise TypeError("analytical view definition is invalid")
    ordered = tuple(sorted(definitions, key=lambda definition: definition.name))
    names = tuple(definition.name for definition in ordered)
    if len(set(names)) != len(names):
        raise AnalyticalViewInvalidError("analytical view definitions must have unique names")
    return ordered


def _column_from_describe(row: tuple[object, ...]) -> AnalyticalViewColumn:
    if (
        len(row) < 3
        or type(row[0]) is not str
        or type(row[1]) is not str
        or row[2] not in {"YES", "NO"}
    ):
        raise AnalyticalViewCorruptionError("analytical view schema row is malformed")
    try:
        return AnalyticalViewColumn(row[0], row[1], row[2] == "YES")
    except TypeError, ValueError:
        raise AnalyticalViewCorruptionError("analytical view schema row is invalid") from None


def _encode_schema(columns: tuple[AnalyticalViewColumn, ...]) -> str:
    return json.dumps(
        [
            {"name": column.name, "nullable": column.nullable, "type": column.type_name}
            for column in columns
        ],
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _decode_schema(value: str) -> tuple[AnalyticalViewColumn, ...]:
    try:
        decoded = cast(object, json.loads(value))
    except json.JSONDecodeError, TypeError:
        raise AnalyticalViewCorruptionError("analytical view output schema is malformed") from None
    if type(decoded) is not list or not decoded:
        raise AnalyticalViewCorruptionError("analytical view output schema is malformed")
    decoded_items = cast(list[object], decoded)
    columns: list[AnalyticalViewColumn] = []
    try:
        for item_value in decoded_items:
            item = item_value
            if type(item) is not dict:
                raise TypeError
            mapping = cast(dict[object, object], item)
            if set(mapping) != {"name", "nullable", "type"}:
                raise TypeError
            columns.append(
                AnalyticalViewColumn(
                    cast(str, mapping["name"]),
                    cast(str, mapping["type"]),
                    cast(bool, mapping["nullable"]),
                )
            )
    except KeyError, TypeError, ValueError:
        raise AnalyticalViewCorruptionError("analytical view output schema is malformed") from None
    result = tuple(columns)
    if _encode_schema(result) != value:
        raise AnalyticalViewCorruptionError("analytical view output schema is not canonical")
    return result


def _quote_identifier(value: str) -> str:
    return f'"{value}"'


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
