"""Dependency-neutral lifecycle contracts for disposable analytics state."""

import re
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Protocol, cast

MEBIBYTE = 1_048_576
MIN_ANALYTICS_MEMORY_BYTES = 64 * MEBIBYTE
MAX_ANALYTICS_MEMORY_BYTES = 16_384 * MEBIBYTE
MAX_ANALYTICS_THREADS = 64
MAX_ANALYTICAL_VIEW_NAME_LENGTH = 63
MAX_ANALYTICAL_VIEW_VERSION = 2_147_483_647
_VIEW_NAME = re.compile(r"pgv_[a-z][a-z0-9_]*\Z")
_LOWER_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class AnalyticalDatabaseError(RuntimeError):
    """Base failure for the disposable analytical database."""


class AnalyticalDatabaseInvalidError(AnalyticalDatabaseError):
    """Analytical database configuration or input is invalid."""


class AnalyticalDatabaseStateError(AnalyticalDatabaseError):
    """The requested operation is invalid for the current lifecycle state."""


class AnalyticalDatabaseOwnershipError(AnalyticalDatabaseError):
    """A non-owner thread attempted to use the writable database."""


class AnalyticalDatabaseStorageError(AnalyticalDatabaseError):
    """DuckDB or the filesystem rejected a bounded lifecycle operation."""


class AnalyticalViewError(AnalyticalDatabaseError):
    """Base failure for the rebuildable analytical view catalog."""


class AnalyticalViewInvalidError(AnalyticalViewError):
    """A view definition or catalog input is invalid."""


class AnalyticalViewConflictError(AnalyticalViewError):
    """A view version conflicts with already installed state."""


class AnalyticalViewSchemaError(AnalyticalViewError):
    """An installed view does not expose its declared output schema."""


class AnalyticalViewCorruptionError(AnalyticalViewError):
    """The disposable registry and installed view state disagree."""


class AnalyticalDatabaseState(StrEnum):
    """Closed lifecycle states exposed without leaking a DuckDB connection."""

    CLOSED = "closed"
    OPEN = "open"
    FAILED = "failed"


@dataclass(frozen=True, slots=True, order=True)
class AnalyticalViewName:
    """Portable reserved name for one managed analytical view."""

    value: str

    def __post_init__(self) -> None:
        value = cast(object, self.value)
        if type(value) is not str:
            raise TypeError("analytical view name must be a string")
        if len(value) > MAX_ANALYTICAL_VIEW_NAME_LENGTH or not _VIEW_NAME.fullmatch(value):
            raise AnalyticalViewInvalidError("analytical view name is not canonical")

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True, order=True)
class AnalyticalViewVersion:
    """Positive immutable definition version for one analytical view."""

    value: int

    def __post_init__(self) -> None:
        value = cast(object, self.value)
        if type(value) is not int:
            raise TypeError("analytical view version must be an integer")
        if not 1 <= value <= MAX_ANALYTICAL_VIEW_VERSION:
            raise AnalyticalViewInvalidError("analytical view version is outside the range")


@dataclass(frozen=True, slots=True)
class AnalyticalViewColumn:
    """Detached installed column description without a DuckDB type object."""

    name: str
    type_name: str
    nullable: bool

    def __post_init__(self) -> None:
        if type(self.name) is not str or not self.name or len(self.name) > 128:
            raise TypeError("analytical view column name is invalid")
        if type(self.type_name) is not str or not self.type_name or len(self.type_name) > 128:
            raise TypeError("analytical view column type is invalid")
        if type(self.nullable) is not bool:
            raise TypeError("analytical view column nullability must be boolean")


@dataclass(frozen=True, slots=True)
class AnalyticalViewRecord:
    """Verified installed view definition and output schema."""

    name: AnalyticalViewName
    version: AnalyticalViewVersion
    definition_sha256: str
    output_schema_sha256: str
    columns: tuple[AnalyticalViewColumn, ...]

    def __post_init__(self) -> None:
        if type(self.name) is not AnalyticalViewName:
            raise TypeError("analytical view record name is invalid")
        if type(self.version) is not AnalyticalViewVersion:
            raise TypeError("analytical view record version is invalid")
        for digest in (self.definition_sha256, self.output_schema_sha256):
            if type(digest) is not str or not _LOWER_SHA256.fullmatch(digest):
                raise TypeError("analytical view digest is invalid")
        if type(self.columns) is not tuple or not self.columns:
            raise TypeError("analytical view columns must be a nonempty tuple")
        if any(type(column) is not AnalyticalViewColumn for column in self.columns):
            raise TypeError("analytical view columns are invalid")
        names = tuple(column.name for column in self.columns)
        if len(set(names)) != len(names):
            raise AnalyticalViewInvalidError("analytical view columns must be unique")


@dataclass(frozen=True, slots=True)
class AnalyticalViewCatalogSnapshot:
    """Sorted verified set of managed analytical views."""

    views: tuple[AnalyticalViewRecord, ...]

    def __post_init__(self) -> None:
        if type(self.views) is not tuple:
            raise TypeError("analytical view catalog must be a tuple")
        if any(type(view) is not AnalyticalViewRecord for view in self.views):
            raise TypeError("analytical view catalog contains an invalid record")
        names = tuple(view.name for view in self.views)
        if names != tuple(sorted(names)) or len(set(names)) != len(names):
            raise AnalyticalViewInvalidError("analytical view catalog must be unique and sorted")


@dataclass(frozen=True, slots=True)
class AnalyticalDatabaseConfig:
    """Validated configuration for one file-backed writable DuckDB owner."""

    database_path: Path
    threads: int = 4
    memory_limit_bytes: int = 512 * MEBIBYTE

    def __post_init__(self) -> None:
        path = cast(object, self.database_path)
        threads = cast(object, self.threads)
        memory = cast(object, self.memory_limit_bytes)
        if not isinstance(path, Path):
            raise TypeError("analytical database path must be a Path")
        if not path.is_absolute():
            raise AnalyticalDatabaseInvalidError("analytical database path must be absolute")
        if path.suffix.lower() != ".duckdb":
            raise AnalyticalDatabaseInvalidError(
                "analytical database path must use the .duckdb suffix"
            )
        if type(threads) is not int:
            raise TypeError("analytical database thread count must be an integer")
        if not 1 <= threads <= MAX_ANALYTICS_THREADS:
            raise AnalyticalDatabaseInvalidError(
                "analytical database thread count is outside the supported range"
            )
        if type(memory) is not int:
            raise TypeError("analytical database memory limit must be an integer")
        if not MIN_ANALYTICS_MEMORY_BYTES <= memory <= MAX_ANALYTICS_MEMORY_BYTES:
            raise AnalyticalDatabaseInvalidError(
                "analytical database memory limit is outside the supported range"
            )
        if memory % MEBIBYTE != 0:
            raise AnalyticalDatabaseInvalidError(
                "analytical database memory limit must use whole mebibytes"
            )


@dataclass(frozen=True, slots=True)
class AnalyticalDatabaseSnapshot:
    """Immutable lifecycle and storage diagnostics safe for application code."""

    state: AnalyticalDatabaseState
    database_path: Path
    database_size_bytes: int
    wal_size_bytes: int

    def __post_init__(self) -> None:
        path = cast(object, self.database_path)
        if type(self.state) is not AnalyticalDatabaseState:
            raise TypeError("analytical database state must use the closed enum")
        if not isinstance(path, Path) or not path.is_absolute():
            raise TypeError("analytical snapshot path must be absolute")
        for value, subject in (
            (self.database_size_bytes, "database size"),
            (self.wal_size_bytes, "WAL size"),
        ):
            if type(value) is not int:
                raise TypeError(f"analytical {subject} must be an integer")
            if value < 0:
                raise ValueError(f"analytical {subject} must not be negative")


class AnalyticalDatabaseLifecycle(Protocol):
    """Single-owner lifecycle for rebuildable analytical database state."""

    def open(self) -> AnalyticalDatabaseSnapshot:
        """Open the configured database and establish the calling thread as owner."""
        ...

    def checkpoint(self) -> AnalyticalDatabaseSnapshot:
        """Checkpoint owned writable state and return current diagnostics."""
        ...

    def close(self) -> AnalyticalDatabaseSnapshot:
        """Checkpoint and close the owner connection; repeated close is safe."""
        ...

    def recreate(self) -> AnalyticalDatabaseSnapshot:
        """Replace disposable database state with one new empty database."""
        ...

    def snapshot(self) -> AnalyticalDatabaseSnapshot:
        """Return lifecycle diagnostics without exposing a connection."""
        ...
