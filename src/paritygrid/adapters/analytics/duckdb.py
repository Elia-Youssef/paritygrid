"""Single-owner lifecycle coordinator for rebuildable DuckDB state."""

from collections.abc import Generator, Sequence
from contextlib import contextmanager, suppress
from decimal import Decimal, InvalidOperation
from pathlib import Path
from threading import RLock, get_ident
from typing import cast

import duckdb

from paritygrid.application.ports.analytics import (
    MEBIBYTE,
    AnalyticalDatabaseConfig,
    AnalyticalDatabaseInvalidError,
    AnalyticalDatabaseOwnershipError,
    AnalyticalDatabaseSnapshot,
    AnalyticalDatabaseState,
    AnalyticalDatabaseStateError,
    AnalyticalDatabaseStorageError,
)

type _Parameters = Sequence[object] | None


class DuckDBLifecycleCoordinator:
    """Own one writable DuckDB connection on exactly one calling thread."""

    __slots__ = ("_config", "_connection", "_lock", "_owner_thread", "_state")

    def __init__(self, config: AnalyticalDatabaseConfig) -> None:
        value = cast(object, config)
        if type(value) is not AnalyticalDatabaseConfig:
            raise TypeError("DuckDB lifecycle requires AnalyticalDatabaseConfig")
        self._config = value
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._lock = RLock()
        self._owner_thread: int | None = None
        self._state = AnalyticalDatabaseState.CLOSED

    def open(self) -> AnalyticalDatabaseSnapshot:
        """Open or idempotently observe the caller-owned connection."""
        with self._lock:
            if self._state is AnalyticalDatabaseState.OPEN:
                self._require_owner()
                return self._snapshot_unlocked()
            if self._state is AnalyticalDatabaseState.FAILED:
                raise AnalyticalDatabaseStateError(
                    "failed analytical database must be closed before reopening"
                )
            path = _validate_database_location(self._config.database_path)
            existed = path.exists()
            connection: duckdb.DuckDBPyConnection | None = None
            storage_failure = False
            try:
                connection = duckdb.connect(
                    str(path),
                    read_only=False,
                    config={
                        "allow_unsigned_extensions": "false",
                        "autoinstall_known_extensions": "false",
                        "autoload_known_extensions": "false",
                        "memory_limit": (f"{self._config.memory_limit_bytes // MEBIBYTE}MiB"),
                        "threads": str(self._config.threads),
                    },
                )
                connection.execute("SET TimeZone = 'UTC'")
                connection.execute("PRAGMA enable_checkpoint_on_shutdown")
                _validate_runtime_settings(connection, self._config)
            except duckdb.Error, OSError:
                if connection is not None:
                    with suppress(duckdb.Error, OSError):
                        connection.close()
                if not existed:
                    _remove_new_database(path)
                self._connection = None
                self._owner_thread = None
                self._state = AnalyticalDatabaseState.CLOSED
                storage_failure = True
            except BaseException:
                if connection is not None:
                    with suppress(duckdb.Error, OSError):
                        connection.close()
                if not existed:
                    _remove_new_database(path)
                self._connection = None
                self._owner_thread = None
                self._state = AnalyticalDatabaseState.CLOSED
                raise
            if storage_failure:
                raise AnalyticalDatabaseStorageError(
                    "analytical database could not be opened"
                ) from None
            self._connection = connection
            self._owner_thread = get_ident()
            self._state = AnalyticalDatabaseState.OPEN
            return self._snapshot_unlocked()

    def checkpoint(self) -> AnalyticalDatabaseSnapshot:
        """Checkpoint current state on the owner thread."""
        with self._lock:
            connection = self._require_open_owner()
            storage_failure = False
            try:
                connection.execute("CHECKPOINT")
            except duckdb.Error, OSError:
                self._state = AnalyticalDatabaseState.FAILED
                storage_failure = True
            except BaseException:
                self._state = AnalyticalDatabaseState.FAILED
                raise
            if storage_failure:
                raise AnalyticalDatabaseStorageError(
                    "analytical database checkpoint failed"
                ) from None
            return self._snapshot_unlocked()

    def close(self) -> AnalyticalDatabaseSnapshot:
        """Checkpoint and close; closing an already closed coordinator is safe."""
        with self._lock:
            if self._connection is None:
                self._owner_thread = None
                self._state = AnalyticalDatabaseState.CLOSED
                return self._snapshot_unlocked()
            self._require_owner()
            checkpoint_error = False
            if self._state is AnalyticalDatabaseState.OPEN:
                try:
                    self._connection.execute("CHECKPOINT")
                except duckdb.Error, OSError:
                    checkpoint_error = True
            close_error = False
            try:
                self._connection.close()
            except duckdb.Error, OSError:
                self._state = AnalyticalDatabaseState.FAILED
                close_error = True
            except BaseException:
                self._state = AnalyticalDatabaseState.FAILED
                raise
            if close_error:
                raise AnalyticalDatabaseStorageError(
                    "analytical database could not be closed"
                ) from None
            self._connection = None
            self._owner_thread = None
            self._state = AnalyticalDatabaseState.CLOSED
            if checkpoint_error:
                raise AnalyticalDatabaseStorageError(
                    "analytical database checkpoint failed during close"
                ) from None
            return self._snapshot_unlocked()

    def recreate(self) -> AnalyticalDatabaseSnapshot:
        """Discard only the configured database and WAL, then reopen empty state."""
        with self._lock:
            if self._connection is not None:
                self.close()
            path = _validate_database_location(self._config.database_path)
            storage_failure = False
            try:
                _remove_database_files(path)
            except OSError:
                self._state = AnalyticalDatabaseState.FAILED
                storage_failure = True
            if storage_failure:
                raise AnalyticalDatabaseStorageError(
                    "analytical database could not be recreated"
                ) from None
            self._state = AnalyticalDatabaseState.CLOSED
            return self.open()

    def snapshot(self) -> AnalyticalDatabaseSnapshot:
        """Return safe state and exact owned-file sizes."""
        with self._lock:
            if self._connection is not None:
                self._require_owner()
            return self._snapshot_unlocked()

    def _execute(self, statement: str, parameters: _Parameters = None) -> None:
        """Execute one trusted adapter statement on the owner connection."""
        with self._lock:
            connection = self._require_open_owner()
            storage_failure = False
            try:
                _execute_connection(connection, statement, parameters)
            except duckdb.Error, OSError:
                storage_failure = True
            if storage_failure:
                raise AnalyticalDatabaseStorageError(
                    "analytical database statement failed"
                ) from None

    def _fetch_all(
        self, statement: str, parameters: _Parameters = None
    ) -> tuple[tuple[object, ...], ...]:
        """Fetch a bounded trusted adapter query on the owner connection."""
        with self._lock:
            connection = self._require_open_owner()
            rows: list[tuple[object, ...]] | None = None
            try:
                cursor = _execute_connection(connection, statement, parameters)
                rows = cursor.fetchall()
            except duckdb.Error, OSError:
                pass
            if rows is None:
                raise AnalyticalDatabaseStorageError("analytical database query failed") from None
            return tuple(tuple(row) for row in rows)

    @contextmanager
    def _transaction(self) -> Generator[None]:
        """Own one short transaction without exposing the DuckDB connection."""
        with self._lock:
            connection = self._require_open_owner()
            try:
                connection.execute("BEGIN TRANSACTION")
                yield
                connection.execute("COMMIT")
            except BaseException:
                with suppress(duckdb.Error, OSError):
                    connection.execute("ROLLBACK")
                raise

    def _require_owner(self) -> None:
        if self._owner_thread != get_ident():
            raise AnalyticalDatabaseOwnershipError(
                "writable analytical database is owned by another thread"
            )

    def _require_open_owner(self) -> duckdb.DuckDBPyConnection:
        if self._state is not AnalyticalDatabaseState.OPEN or self._connection is None:
            raise AnalyticalDatabaseStateError("analytical database is not open")
        self._require_owner()
        return self._connection

    def _snapshot_unlocked(self) -> AnalyticalDatabaseSnapshot:
        path = self._config.database_path
        return AnalyticalDatabaseSnapshot(
            state=self._state,
            database_path=path,
            database_size_bytes=_file_size(path),
            wal_size_bytes=_file_size(_wal_path(path)),
        )


def _execute_connection(
    connection: duckdb.DuckDBPyConnection,
    statement: str,
    parameters: _Parameters,
) -> duckdb.DuckDBPyConnection:
    if parameters is None:
        return connection.execute(statement)
    return connection.execute(statement, parameters)


def _validate_database_location(path: Path) -> Path:
    try:
        parent = path.parent
        if not parent.exists() or not parent.is_dir():
            raise AnalyticalDatabaseInvalidError(
                "analytical database parent must be an existing directory"
            )
        _reject_link_components(parent)
        if path.exists():
            if path.is_symlink() or path.is_junction():
                raise AnalyticalDatabaseInvalidError(
                    "analytical database path cannot be a filesystem link"
                )
            if not path.is_file():
                raise AnalyticalDatabaseInvalidError(
                    "analytical database path must reference a regular file"
                )
        return path
    except AnalyticalDatabaseInvalidError:
        raise
    except OSError:
        pass
    raise AnalyticalDatabaseInvalidError(
        "analytical database location could not be inspected"
    ) from None


def _reject_link_components(path: Path) -> None:
    current = path
    while current != current.parent:
        if current.is_symlink() or current.is_junction():
            raise AnalyticalDatabaseInvalidError(
                "analytical database path cannot traverse a filesystem link"
            )
        current = current.parent


def _validate_runtime_settings(
    connection: duckdb.DuckDBPyConnection, config: AnalyticalDatabaseConfig
) -> None:
    rows = connection.execute(
        "SELECT current_setting('threads'), current_setting('memory_limit'), "
        "current_setting('TimeZone'), current_setting('allow_unsigned_extensions'), "
        "current_setting('autoinstall_known_extensions'), "
        "current_setting('autoload_known_extensions')"
    ).fetchall()
    if len(rows) != 1 or len(rows[0]) != 6:
        raise duckdb.IOException("runtime settings unavailable")
    threads, memory_limit, timezone, unsigned, autoinstall, autoload = rows[0]
    if (
        type(threads) is not int
        or threads != config.threads
        or not isinstance(memory_limit, str)
        or _memory_bytes(memory_limit) != config.memory_limit_bytes
        or timezone != "UTC"
        or unsigned is not False
        or autoinstall is not False
        or autoload is not False
    ):
        raise duckdb.IOException("runtime settings differ")


def _memory_bytes(value: str) -> int:
    parts = value.split()
    if len(parts) != 2 or parts[1] != "MiB":
        return -1
    try:
        mebibytes = Decimal(parts[0])
    except InvalidOperation:
        return -1
    bytes_value = mebibytes * MEBIBYTE
    if bytes_value != bytes_value.to_integral_value():
        return -1
    return int(bytes_value)


def _remove_new_database(path: Path) -> None:
    with suppress(OSError):
        _remove_database_files(path)


def _remove_database_files(path: Path) -> None:
    candidates = tuple(candidate for candidate in (path, _wal_path(path)) if candidate.exists())
    for candidate in candidates:
        if candidate.is_symlink() or candidate.is_junction() or not candidate.is_file():
            raise OSError("unsafe analytical database file")
    for candidate in candidates:
        candidate.unlink()


def _wal_path(path: Path) -> Path:
    return Path(f"{path}.wal")


def _file_size(path: Path) -> int:
    try:
        if not path.exists():
            return 0
        if path.is_symlink() or path.is_junction() or not path.is_file():
            raise AnalyticalDatabaseStorageError(
                "analytical database storage could not be inspected"
            )
        return path.stat().st_size
    except OSError:
        raise AnalyticalDatabaseStorageError(
            "analytical database storage could not be inspected"
        ) from None
