"""File-based SQLite engine, session, and lifecycle factories."""

import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Self, cast

from sqlalchemy import URL, Engine, create_engine, event
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import ConnectionPoolEntry

from paritygrid.adapters.persistence.capabilities import (
    REQUIRED_BUSY_TIMEOUT_MS,
    SQLiteCapabilities,
    SQLiteLibraryInfo,
    SQLitePragmaState,
    build_capability_report,
    current_sqlite_library,
    validate_sqlite_library,
    validate_sqlite_pragmas,
)
from paritygrid.adapters.persistence.errors import (
    SQLiteCapabilityError,
    SQLiteConfigurationError,
)

type SessionFactory = sessionmaker[Session]

_SUPPORTED_DRIVERS = frozenset({"sqlite", "sqlite+pysqlite"})


@dataclass(frozen=True, slots=True)
class SQLiteDatabaseConfig:
    """Validated configuration for one absolute, file-based SQLite database."""

    database_path: Path
    create_parent: bool = False

    def __post_init__(self) -> None:
        raw_path = str(self.database_path)
        if raw_path == ":memory:" or raw_path.startswith("file:"):
            raise SQLiteConfigurationError("A file-based SQLite database path is required.")
        try:
            resolved_path = self.database_path.expanduser().resolve(strict=False)
        except (OSError, ValueError) as error:
            raise SQLiteConfigurationError("SQLite database path could not be resolved.") from error
        object.__setattr__(self, "database_path", resolved_path)

    @classmethod
    def from_url(
        cls,
        database_url: str | URL,
        *,
        create_parent: bool = False,
    ) -> Self:
        """Build file configuration from a non-URI absolute SQLite URL."""
        try:
            url = make_url(database_url)
        except SQLAlchemyError as error:
            raise SQLiteConfigurationError("Database URL is not valid.") from error
        if url.drivername not in _SUPPORTED_DRIVERS:
            raise SQLiteConfigurationError("Only the SQLite pysqlite driver is supported.")
        if url.username or url.password or url.host or url.port:
            raise SQLiteConfigurationError("SQLite database URLs cannot contain authority fields.")
        if url.query:
            raise SQLiteConfigurationError("SQLite URI query parameters are not supported.")
        database = url.database
        if not database or database == ":memory:" or database.startswith("file:"):
            raise SQLiteConfigurationError("A file-based SQLite database is required.")
        database_path = Path(database)
        if not database_path.is_absolute():
            raise SQLiteConfigurationError("SQLite database URLs must contain an absolute path.")
        return cls(database_path=database_path, create_parent=create_parent)

    @property
    def database_url(self) -> URL:
        """Return a SQLAlchemy URL without an SQLite URI interpretation mode."""
        return URL.create("sqlite+pysqlite", database=str(self.database_path))


def _prepare_database_path(config: SQLiteDatabaseConfig) -> None:
    path = config.database_path
    parent = path.parent
    if parent.exists() and not parent.is_dir():
        raise SQLiteConfigurationError("SQLite database parent must be a directory.")
    if not parent.exists():
        if not config.create_parent:
            raise SQLiteConfigurationError("SQLite database parent directory does not exist.")
        try:
            parent.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise SQLiteConfigurationError(
                "SQLite database parent directory could not be created."
            ) from error
    if path.exists() and not path.is_file():
        raise SQLiteConfigurationError("SQLite database path must name a regular file.")


def _fetch_pragma_value(cursor: sqlite3.Cursor, name: str) -> object:
    row = cast(tuple[object, ...] | None, cursor.execute(f"PRAGMA {name}").fetchone())
    if row is None or len(row) != 1:
        raise SQLiteCapabilityError(f"SQLite did not report PRAGMA {name}.")
    return row[0]


def _coerce_pragma_state(
    foreign_keys: object,
    journal_mode: object,
    synchronous_level: object,
    busy_timeout_ms: object,
) -> SQLitePragmaState:
    if type(foreign_keys) is not int or foreign_keys not in {0, 1}:
        raise SQLiteCapabilityError("SQLite reported an invalid foreign-key setting.")
    if not isinstance(journal_mode, str):
        raise SQLiteCapabilityError("SQLite reported an invalid journal mode.")
    if type(synchronous_level) is not int:
        raise SQLiteCapabilityError("SQLite reported an invalid synchronous setting.")
    if type(busy_timeout_ms) is not int:
        raise SQLiteCapabilityError("SQLite reported an invalid busy-timeout setting.")
    return SQLitePragmaState(
        foreign_keys=bool(foreign_keys),
        journal_mode=journal_mode,
        synchronous_level=synchronous_level,
        busy_timeout_ms=busy_timeout_ms,
    )


def _read_pragma_state(cursor: sqlite3.Cursor) -> SQLitePragmaState:
    return _coerce_pragma_state(
        _fetch_pragma_value(cursor, "foreign_keys"),
        _fetch_pragma_value(cursor, "journal_mode"),
        _fetch_pragma_value(cursor, "synchronous"),
        _fetch_pragma_value(cursor, "busy_timeout"),
    )


def _initialize_sqlite_connection(
    dbapi_connection: DBAPIConnection,
    _connection_record: ConnectionPoolEntry,
) -> None:
    if not isinstance(dbapi_connection, sqlite3.Connection):
        raise SQLiteConfigurationError("SQLite engine received an unexpected database driver.")
    connection = cast(sqlite3.Connection, dbapi_connection)
    previous_autocommit = connection.autocommit
    cursor = connection.cursor()
    try:
        # Pragmas that change connection behavior must run outside an implicit transaction.
        connection.autocommit = True
        cursor.execute("PRAGMA foreign_keys = ON")
        cursor.execute("PRAGMA journal_mode = WAL")
        cursor.execute("PRAGMA synchronous = FULL")
        cursor.execute(f"PRAGMA busy_timeout = {REQUIRED_BUSY_TIMEOUT_MS}")
        validate_sqlite_pragmas(_read_pragma_state(cursor))
    except SQLiteCapabilityError:
        raise
    except sqlite3.DatabaseError as error:
        raise SQLiteCapabilityError(
            "SQLite connection durability settings could not be configured."
        ) from error
    finally:
        cursor.close()
        connection.autocommit = previous_autocommit


def create_sqlite_engine(
    config: SQLiteDatabaseConfig,
    *,
    library: SQLiteLibraryInfo | None = None,
) -> Engine:
    """Create a lazy SQLAlchemy engine with mandatory per-connection safeguards."""
    active_library = library or current_sqlite_library()
    validate_sqlite_library(active_library)
    _prepare_database_path(config)
    try:
        engine = create_engine(
            config.database_url,
            connect_args={
                "check_same_thread": False,
                "timeout": REQUIRED_BUSY_TIMEOUT_MS / 1_000,
            },
        )
    except SQLAlchemyError as error:
        raise SQLiteConfigurationError("SQLite engine could not be created.") from error
    event.listen(engine, "connect", _initialize_sqlite_connection)
    return engine


def create_session_factory(engine: Engine) -> SessionFactory:
    """Create a factory whose calls each own an independent synchronous Session."""
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


@contextmanager
def transactional_session(session_factory: SessionFactory) -> Generator[Session]:
    """Commit one short session scope or roll it back when its operation fails."""
    with session_factory.begin() as session:
        yield session


def _pragma_value(connection: Connection, name: str) -> object:
    return cast(object, connection.exec_driver_sql(f"PRAGMA {name}").scalar_one())


def inspect_sqlite_engine(
    engine: Engine,
    *,
    library: SQLiteLibraryInfo | None = None,
) -> SQLiteCapabilities:
    """Open one validated connection and return its startup capability report."""
    active_library = library or current_sqlite_library()
    validate_sqlite_library(active_library)
    try:
        with engine.connect() as connection:
            foreign_keys = _pragma_value(connection, "foreign_keys")
            journal_mode = _pragma_value(connection, "journal_mode")
            synchronous_level = _pragma_value(connection, "synchronous")
            busy_timeout_ms = _pragma_value(connection, "busy_timeout")
    except SQLiteCapabilityError:
        raise
    except SQLAlchemyError as error:
        raise SQLiteConfigurationError("SQLite database could not be opened.") from error
    return build_capability_report(
        active_library,
        _coerce_pragma_state(
            foreign_keys,
            journal_mode,
            synchronous_level,
            busy_timeout_ms,
        ),
    )


class SQLiteDatabase:
    """Owned SQLite engine, session factory, and verified capability report."""

    __slots__ = ("_capabilities", "_closed", "_engine", "_sessions")

    def __init__(
        self,
        engine: Engine,
        sessions: SessionFactory,
        capabilities: SQLiteCapabilities,
    ) -> None:
        self._engine = engine
        self._sessions = sessions
        self._capabilities = capabilities
        self._closed = False

    @classmethod
    def open(
        cls,
        config: SQLiteDatabaseConfig,
        *,
        library: SQLiteLibraryInfo | None = None,
    ) -> Self:
        """Open and verify an owned database, disposing partial startup on failure."""
        engine = create_sqlite_engine(config, library=library)
        try:
            capabilities = inspect_sqlite_engine(engine, library=library)
        except BaseException:
            engine.dispose()
            raise
        return cls(engine, create_session_factory(engine), capabilities)

    @property
    def engine(self) -> Engine:
        """Return the owned engine for schema and migration operations."""
        return self._engine

    @property
    def capabilities(self) -> SQLiteCapabilities:
        """Return the immutable startup capability report."""
        return self._capabilities

    @contextmanager
    def transaction(self) -> Generator[Session]:
        """Yield a new transaction-scoped Session owned by the current caller."""
        if self._closed:
            raise SQLiteConfigurationError("SQLite database lifecycle is closed.")
        with transactional_session(self._sessions) as session:
            yield session

    def close(self) -> None:
        """Dispose pooled connections and prevent creation of new owned sessions."""
        if not self._closed:
            self._engine.dispose()
            self._closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        _exception_type: type[BaseException] | None,
        _exception: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        self.close()
