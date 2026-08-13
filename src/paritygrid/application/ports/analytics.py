"""Dependency-neutral lifecycle contracts for disposable analytics state."""

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Protocol, cast

MEBIBYTE = 1_048_576
MIN_ANALYTICS_MEMORY_BYTES = 64 * MEBIBYTE
MAX_ANALYTICS_MEMORY_BYTES = 16_384 * MEBIBYTE
MAX_ANALYTICS_THREADS = 64


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


class AnalyticalDatabaseState(StrEnum):
    """Closed lifecycle states exposed without leaking a DuckDB connection."""

    CLOSED = "closed"
    OPEN = "open"
    FAILED = "failed"


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
