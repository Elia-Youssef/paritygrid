"""SQLite library and connection capability validation."""

import sqlite3
from dataclasses import dataclass

from paritygrid.adapters.persistence.errors import (
    SQLiteCapabilityError,
    SQLiteConfigurationError,
)

MINIMUM_SQLITE_VERSION = (3, 35, 0)
REQUIRED_BUSY_TIMEOUT_MS = 5_000
REQUIRED_JOURNAL_MODE = "wal"
REQUIRED_SYNCHRONOUS_LEVEL = 2

type SQLiteVersionTuple = tuple[int, int, int]


@dataclass(frozen=True, slots=True)
class SQLiteLibraryInfo:
    """SQLite library identity reported by the active Python runtime."""

    version: str
    version_info: SQLiteVersionTuple
    threadsafety: int

    def __post_init__(self) -> None:
        if any(component < 0 for component in self.version_info):
            raise SQLiteConfigurationError("SQLite version components cannot be negative.")
        if self.version != ".".join(str(component) for component in self.version_info):
            raise SQLiteConfigurationError(
                "SQLite version text must match its numeric version tuple."
            )
        if self.threadsafety not in {0, 1, 3}:
            raise SQLiteConfigurationError("SQLite thread-safety level is not recognized.")


@dataclass(frozen=True, slots=True)
class SQLitePragmaState:
    """Observed durability and concurrency settings for one SQLite connection."""

    foreign_keys: bool
    journal_mode: str
    synchronous_level: int
    busy_timeout_ms: int


@dataclass(frozen=True, slots=True)
class SQLiteCapabilities:
    """Immutable startup report for the authoritative operational database."""

    library_version: str
    library_version_info: SQLiteVersionTuple
    minimum_supported_version: SQLiteVersionTuple
    threadsafety: int
    foreign_keys: bool
    journal_mode: str
    synchronous_level: int
    busy_timeout_ms: int


def current_sqlite_library() -> SQLiteLibraryInfo:
    """Return SQLite capability metadata from the active Python runtime."""
    version_info = sqlite3.sqlite_version_info
    return SQLiteLibraryInfo(
        version=sqlite3.sqlite_version,
        version_info=(version_info[0], version_info[1], version_info[2]),
        threadsafety=sqlite3.threadsafety,
    )


def validate_sqlite_library(library: SQLiteLibraryInfo) -> None:
    """Reject SQLite libraries that cannot support the persistence contract."""
    if library.version_info < MINIMUM_SQLITE_VERSION:
        required = ".".join(str(component) for component in MINIMUM_SQLITE_VERSION)
        raise SQLiteCapabilityError(
            f"SQLite {required} or newer is required; found {library.version}."
        )
    if library.threadsafety == 0:
        raise SQLiteCapabilityError("SQLite must be compiled with thread-safe connection support.")


def validate_sqlite_pragmas(state: SQLitePragmaState) -> None:
    """Reject a connection whose observed settings weaken durability or integrity."""
    if not state.foreign_keys:
        raise SQLiteCapabilityError("SQLite foreign-key enforcement could not be enabled.")
    if state.journal_mode.casefold() != REQUIRED_JOURNAL_MODE:
        raise SQLiteCapabilityError("SQLite WAL journal mode could not be enabled.")
    if state.synchronous_level != REQUIRED_SYNCHRONOUS_LEVEL:
        raise SQLiteCapabilityError("SQLite full synchronous durability could not be enabled.")
    if state.busy_timeout_ms != REQUIRED_BUSY_TIMEOUT_MS:
        raise SQLiteCapabilityError("SQLite busy timeout could not be configured to 5000 ms.")


def build_capability_report(
    library: SQLiteLibraryInfo,
    pragmas: SQLitePragmaState,
) -> SQLiteCapabilities:
    """Validate and combine the library and connection observations."""
    validate_sqlite_library(library)
    validate_sqlite_pragmas(pragmas)
    return SQLiteCapabilities(
        library_version=library.version,
        library_version_info=library.version_info,
        minimum_supported_version=MINIMUM_SQLITE_VERSION,
        threadsafety=library.threadsafety,
        foreign_keys=pragmas.foreign_keys,
        journal_mode=pragmas.journal_mode.casefold(),
        synchronous_level=pragmas.synchronous_level,
        busy_timeout_ms=pragmas.busy_timeout_ms,
    )
