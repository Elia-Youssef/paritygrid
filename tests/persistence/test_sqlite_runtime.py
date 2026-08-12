"""File-based SQLite runtime integration tests."""

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import URL, Engine, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from paritygrid.adapters.persistence import (
    MINIMUM_SQLITE_VERSION,
    SQLiteCapabilities,
    SQLiteCapabilityError,
    SQLiteConfigurationError,
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    SQLiteLibraryInfo,
    SQLitePragmaState,
    create_session_factory,
    create_sqlite_engine,
    inspect_sqlite_engine,
    transactional_session,
)
from paritygrid.adapters.persistence import sqlite as sqlite_runtime


def _config(tmp_path: Path, name: str = "operational.db") -> SQLiteDatabaseConfig:
    return SQLiteDatabaseConfig(tmp_path / name)


def _create_value_table(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE sample_values (id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
        )


def _old_library() -> SQLiteLibraryInfo:
    return SQLiteLibraryInfo("3.34.9", (3, 34, 9), 3)


def _write_then_fail(engine: Engine) -> None:
    sessions = create_session_factory(engine)
    with transactional_session(sessions) as session:
        session.execute(text("INSERT INTO sample_values (id, value) VALUES (2, 'rolled-back')"))
        raise RuntimeError("stop transaction")


def test_config_resolves_file_path_and_emits_pysqlite_url(tmp_path: Path) -> None:
    relative_path = tmp_path / "nested" / ".." / "operational.db"
    config = SQLiteDatabaseConfig(relative_path)

    assert config.database_path == (tmp_path / "operational.db").resolve()
    assert config.database_url.drivername == "sqlite+pysqlite"
    assert Path(cast(str, config.database_url.database)) == config.database_path


def test_config_round_trips_absolute_sqlite_url(tmp_path: Path) -> None:
    original = SQLiteDatabaseConfig(tmp_path / "state.db")

    restored = SQLiteDatabaseConfig.from_url(original.database_url)

    assert restored == original


@pytest.mark.parametrize(
    "database_url",
    [
        "postgresql://localhost/paritygrid",
        "sqlite:///:memory:",
        "sqlite+pysqlite:///:memory:",
        "sqlite:///relative.db",
        "sqlite:///file:runtime.db",
        "sqlite:///C:/runtime.db?uri=true",
        "sqlite://user:password@host:123/runtime.db",
        "://",
    ],
)
def test_config_rejects_non_file_or_ambiguous_database_urls(database_url: str) -> None:
    with pytest.raises(SQLiteConfigurationError):
        SQLiteDatabaseConfig.from_url(database_url)


def test_config_accepts_sqlite_driver_url_object(tmp_path: Path) -> None:
    url = URL.create("sqlite", database=str(tmp_path / "runtime.db"))

    config = SQLiteDatabaseConfig.from_url(url)

    assert config.database_path == (tmp_path / "runtime.db").resolve()


@pytest.mark.parametrize("database_path", [Path(":memory:"), Path("file:runtime.db")])
def test_config_rejects_memory_and_uri_path_spellings(database_path: Path) -> None:
    with pytest.raises(SQLiteConfigurationError, match="file-based"):
        SQLiteDatabaseConfig(database_path)


def test_config_translates_path_resolution_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_resolution(_path: Path, *, strict: bool = False) -> Path:
        del strict
        raise OSError("synthetic resolution failure")

    monkeypatch.setattr(Path, "resolve", fail_resolution)

    with pytest.raises(SQLiteConfigurationError, match="could not be resolved"):
        SQLiteDatabaseConfig(Path("runtime.db"))


def test_engine_creation_is_lazy_and_database_file_lifecycle_is_explicit(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)

    engine = create_sqlite_engine(config)
    try:
        assert not config.database_path.exists()

        capabilities = inspect_sqlite_engine(engine)

        assert config.database_path.is_file()
        assert capabilities.library_version == sqlite3.sqlite_version
        assert capabilities.library_version_info == sqlite3.sqlite_version_info
        assert capabilities.minimum_supported_version == MINIMUM_SQLITE_VERSION
        assert capabilities.threadsafety == sqlite3.threadsafety
        assert capabilities.foreign_keys is True
        assert capabilities.journal_mode == "wal"
        assert capabilities.synchronous_level == 2
        assert capabilities.busy_timeout_ms == 5_000
    finally:
        engine.dispose()


def test_missing_parent_requires_explicit_creation(tmp_path: Path) -> None:
    parent = tmp_path / "missing" / "nested"
    config = SQLiteDatabaseConfig(parent / "runtime.db")

    with pytest.raises(SQLiteConfigurationError, match="does not exist"):
        create_sqlite_engine(config)

    assert not parent.exists()


def test_database_parent_that_is_a_file_is_rejected(tmp_path: Path) -> None:
    parent = tmp_path / "not-a-directory"
    parent.write_text("synthetic", encoding="utf-8")

    with pytest.raises(SQLiteConfigurationError, match="parent must be a directory"):
        create_sqlite_engine(SQLiteDatabaseConfig(parent / "runtime.db"))


def test_parent_creation_failure_is_typed_and_leaves_no_database(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    parent = tmp_path / "cannot-create"
    config = SQLiteDatabaseConfig(parent / "runtime.db", create_parent=True)

    def fail_creation(
        _path: Path,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        del mode, parents, exist_ok
        raise OSError("synthetic creation failure")

    monkeypatch.setattr(Path, "mkdir", fail_creation)

    with pytest.raises(SQLiteConfigurationError, match="could not be created"):
        create_sqlite_engine(config)

    assert not config.database_path.exists()


def test_engine_factory_translates_sqlalchemy_configuration_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def fail_engine_creation(*_args: object, **_kwargs: object) -> Engine:
        raise SQLAlchemyError("synthetic engine failure")

    monkeypatch.setattr(sqlite_runtime, "create_engine", fail_engine_creation)

    with pytest.raises(SQLiteConfigurationError, match="engine could not be created"):
        create_sqlite_engine(_config(tmp_path))


def test_parent_creation_does_not_open_database_until_first_connection(tmp_path: Path) -> None:
    parent = tmp_path / "created" / "nested"
    config = SQLiteDatabaseConfig(parent / "runtime.db", create_parent=True)

    engine = create_sqlite_engine(config)
    try:
        assert parent.is_dir()
        assert not config.database_path.exists()

        inspect_sqlite_engine(engine)

        assert config.database_path.is_file()
    finally:
        engine.dispose()


def test_directory_database_path_is_rejected_without_creating_files(tmp_path: Path) -> None:
    database_directory = tmp_path / "runtime.db"
    database_directory.mkdir()
    config = SQLiteDatabaseConfig(database_directory)

    with pytest.raises(SQLiteConfigurationError, match="regular file"):
        create_sqlite_engine(config)

    assert list(database_directory.iterdir()) == []


def test_unsupported_library_is_rejected_before_parent_or_file_creation(tmp_path: Path) -> None:
    parent = tmp_path / "unsupported"
    config = SQLiteDatabaseConfig(parent / "runtime.db", create_parent=True)

    with pytest.raises(SQLiteCapabilityError, match=r"3\.35\.0"):
        create_sqlite_engine(config, library=_old_library())

    assert not parent.exists()


def test_each_new_physical_connection_receives_exact_pragmas(tmp_path: Path) -> None:
    engine = create_sqlite_engine(_config(tmp_path))
    try:
        inspect_sqlite_engine(engine)
        with engine.connect() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys = OFF")
            connection.exec_driver_sql("PRAGMA synchronous = NORMAL")
            connection.exec_driver_sql("PRAGMA busy_timeout = 1")
        engine.dispose()

        report = inspect_sqlite_engine(engine)

        assert report.foreign_keys is True
        assert report.journal_mode == "wal"
        assert report.synchronous_level == 2
        assert report.busy_timeout_ms == 5_000
    finally:
        engine.dispose()


def test_session_factory_returns_independent_sessions_and_hides_uncommitted_rows(
    tmp_path: Path,
) -> None:
    engine = create_sqlite_engine(_config(tmp_path))
    try:
        _create_value_table(engine)
        sessions = create_session_factory(engine)
        first = sessions()
        second = sessions()
        try:
            assert first is not second
            first.execute(text("INSERT INTO sample_values (id, value) VALUES (1, 'pending')"))

            visible_count = second.execute(text("SELECT COUNT(*) FROM sample_values")).scalar_one()

            assert visible_count == 0
        finally:
            first.close()
            second.close()
    finally:
        engine.dispose()


def test_transactional_session_commits_successful_work_and_rolls_back_failure(
    tmp_path: Path,
) -> None:
    engine = create_sqlite_engine(_config(tmp_path))
    try:
        _create_value_table(engine)
        sessions = create_session_factory(engine)
        with transactional_session(sessions) as session:
            session.execute(text("INSERT INTO sample_values (id, value) VALUES (1, 'committed')"))

        with pytest.raises(RuntimeError, match="stop transaction"):
            _write_then_fail(engine)

        with engine.connect() as connection:
            rows = connection.exec_driver_sql(
                "SELECT id, value FROM sample_values ORDER BY id"
            ).all()
        assert rows == [(1, "committed")]
    finally:
        engine.dispose()


def test_foreign_key_enforcement_rejects_invalid_reference(tmp_path: Path) -> None:
    engine = create_sqlite_engine(_config(tmp_path))
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE TABLE parents (id INTEGER PRIMARY KEY)")
            connection.exec_driver_sql(
                "CREATE TABLE children ("
                "id INTEGER PRIMARY KEY, "
                "parent_id INTEGER NOT NULL REFERENCES parents(id)"
                ")"
            )
        sessions = create_session_factory(engine)

        with pytest.raises(IntegrityError), transactional_session(sessions) as session:
            session.execute(text("INSERT INTO children (id, parent_id) VALUES (1, 99)"))

        with transactional_session(sessions) as session:
            session.execute(text("INSERT INTO parents (id) VALUES (99)"))
            session.execute(text("INSERT INTO children (id, parent_id) VALUES (1, 99)"))
    finally:
        engine.dispose()


def test_disposed_engine_reopens_existing_file_with_data_and_pragmas(tmp_path: Path) -> None:
    config = _config(tmp_path)
    first_engine = create_sqlite_engine(config)
    _create_value_table(first_engine)
    with first_engine.begin() as connection:
        connection.exec_driver_sql("INSERT INTO sample_values VALUES (1, 'durable')")
    first_engine.dispose()

    second_engine = create_sqlite_engine(config)
    try:
        report = inspect_sqlite_engine(second_engine)
        with second_engine.connect() as connection:
            value = connection.exec_driver_sql(
                "SELECT value FROM sample_values WHERE id = 1"
            ).scalar_one()

        assert value == "durable"
        assert report.journal_mode == "wal"
    finally:
        second_engine.dispose()


def test_pooled_file_connection_can_move_between_threads_without_session_sharing(
    tmp_path: Path,
) -> None:
    engine = create_sqlite_engine(_config(tmp_path))
    try:
        with engine.connect() as connection:
            assert connection.exec_driver_sql("SELECT 1").scalar_one() == 1

        def read_from_worker() -> int:
            sessions = create_session_factory(engine)
            with sessions() as session:
                return cast(int, session.execute(text("SELECT 1")).scalar_one())

        with ThreadPoolExecutor(max_workers=1) as executor:
            result = executor.submit(read_from_worker).result(timeout=5)

        assert result == 1
    finally:
        engine.dispose()


def test_database_lifecycle_owns_capabilities_transactions_and_close(tmp_path: Path) -> None:
    database = SQLiteDatabase.open(_config(tmp_path))
    with database:
        assert database.capabilities.foreign_keys is True
        with database.engine.begin() as connection:
            connection.exec_driver_sql(
                "CREATE TABLE lifecycle_values (id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
            )
        with database.transaction() as session:
            session.execute(text("INSERT INTO lifecycle_values (id, value) VALUES (1, 'owned')"))

    database.close()
    with (
        pytest.raises(SQLiteConfigurationError, match="lifecycle is closed"),
        database.transaction(),
    ):
        pass


def test_unknown_pragma_and_unexpected_driver_are_typed_capability_failures() -> None:
    connection = sqlite3.connect(":memory:")
    try:
        with pytest.raises(SQLiteCapabilityError, match="did not report"):
            sqlite_runtime._fetch_pragma_value(  # pyright: ignore[reportPrivateUsage]
                connection.cursor(), "unknown_paritygrid_pragma"
            )
    finally:
        connection.close()

    with pytest.raises(SQLiteConfigurationError, match="unexpected database driver"):
        sqlite_runtime._initialize_sqlite_connection(  # pyright: ignore[reportPrivateUsage]
            cast(sqlite_runtime.DBAPIConnection, object()),
            cast(sqlite_runtime.ConnectionPoolEntry, object()),
        )


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ((True, "wal", 2, 5_000), "foreign-key"),
        ((1, 1, 2, 5_000), "journal mode"),
        ((1, "wal", "full", 5_000), "synchronous"),
        ((1, "wal", 2, "5000"), "busy-timeout"),
    ],
)
def test_malformed_pragma_reports_are_typed_capability_failures(
    values: tuple[object, object, object, object],
    message: str,
) -> None:
    with pytest.raises(SQLiteCapabilityError, match=message):
        sqlite_runtime._coerce_pragma_state(  # pyright: ignore[reportPrivateUsage]
            values[0], values[1], values[2], values[3]
        )


def test_read_only_database_reports_pragma_configuration_failure(tmp_path: Path) -> None:
    path = tmp_path / "read-only.db"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY)")
    connection.close()
    read_only = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        with pytest.raises(SQLiteCapabilityError, match="could not be configured"):
            sqlite_runtime._initialize_sqlite_connection(  # pyright: ignore[reportPrivateUsage]
                cast(sqlite_runtime.DBAPIConnection, read_only),
                cast(sqlite_runtime.ConnectionPoolEntry, object()),
            )
    finally:
        read_only.close()


def test_connection_initializer_preserves_typed_pragma_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_pragmas(_state: SQLitePragmaState) -> None:
        raise SQLiteCapabilityError("synthetic pragma rejection")

    monkeypatch.setattr(sqlite_runtime, "validate_sqlite_pragmas", reject_pragmas)
    connection = sqlite3.connect(":memory:")
    previous_autocommit = connection.autocommit
    try:
        with pytest.raises(SQLiteCapabilityError, match="synthetic pragma rejection"):
            sqlite_runtime._initialize_sqlite_connection(  # pyright: ignore[reportPrivateUsage]
                cast(sqlite_runtime.DBAPIConnection, connection),
                cast(sqlite_runtime.ConnectionPoolEntry, object()),
            )
        assert connection.autocommit == previous_autocommit
    finally:
        connection.close()


@pytest.mark.parametrize(
    "failure",
    [
        SQLiteCapabilityError("synthetic capability failure"),
        SQLAlchemyError("synthetic connection failure"),
    ],
)
def test_engine_inspection_translates_only_sqlalchemy_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: Exception,
) -> None:
    def fail_pragma(_connection: object, _name: str) -> object:
        raise failure

    engine = create_sqlite_engine(_config(tmp_path))
    monkeypatch.setattr(sqlite_runtime, "_pragma_value", fail_pragma)
    expected = (
        SQLiteCapabilityError
        if isinstance(failure, SQLiteCapabilityError)
        else SQLiteConfigurationError
    )
    try:
        with pytest.raises(expected):
            inspect_sqlite_engine(engine)
    finally:
        engine.dispose()


def test_database_open_disposes_engine_after_capability_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class DisposableEngine:
        disposed = False

        def dispose(self) -> None:
            self.disposed = True

    disposable = DisposableEngine()

    def return_engine(
        _config_value: SQLiteDatabaseConfig,
        *,
        library: SQLiteLibraryInfo | None = None,
    ) -> Engine:
        del library
        return cast(Engine, disposable)

    def reject_inspection(
        _engine: Engine,
        *,
        library: SQLiteLibraryInfo | None = None,
    ) -> SQLiteCapabilities:
        del library
        raise SQLiteCapabilityError("synthetic startup rejection")

    monkeypatch.setattr(sqlite_runtime, "create_sqlite_engine", return_engine)
    monkeypatch.setattr(sqlite_runtime, "inspect_sqlite_engine", reject_inspection)

    with pytest.raises(SQLiteCapabilityError, match="synthetic startup rejection"):
        SQLiteDatabase.open(_config(tmp_path))

    assert disposable.disposed is True
