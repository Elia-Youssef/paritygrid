"""Authoritative SQLite persistence foundation."""

from paritygrid.adapters.persistence.capabilities import (
    MINIMUM_SQLITE_VERSION,
    REQUIRED_BUSY_TIMEOUT_MS,
    REQUIRED_JOURNAL_MODE,
    REQUIRED_SYNCHRONOUS_LEVEL,
    SQLiteCapabilities,
    SQLiteLibraryInfo,
    SQLitePragmaState,
    build_capability_report,
    current_sqlite_library,
    validate_sqlite_library,
    validate_sqlite_pragmas,
)
from paritygrid.adapters.persistence.errors import (
    PersistenceError,
    SQLiteCapabilityError,
    SQLiteConfigurationError,
)
from paritygrid.adapters.persistence.sqlite import (
    SessionFactory,
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    create_session_factory,
    create_sqlite_engine,
    inspect_sqlite_engine,
    transactional_session,
)

__all__ = (
    "MINIMUM_SQLITE_VERSION",
    "REQUIRED_BUSY_TIMEOUT_MS",
    "REQUIRED_JOURNAL_MODE",
    "REQUIRED_SYNCHRONOUS_LEVEL",
    "PersistenceError",
    "SQLiteCapabilities",
    "SQLiteCapabilityError",
    "SQLiteConfigurationError",
    "SQLiteDatabase",
    "SQLiteDatabaseConfig",
    "SQLiteLibraryInfo",
    "SQLitePragmaState",
    "SessionFactory",
    "build_capability_report",
    "create_session_factory",
    "create_sqlite_engine",
    "current_sqlite_library",
    "inspect_sqlite_engine",
    "transactional_session",
    "validate_sqlite_library",
    "validate_sqlite_pragmas",
)
