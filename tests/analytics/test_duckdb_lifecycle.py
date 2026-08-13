"""File-backed DuckDB lifecycle integration and failure tests."""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import get_ident
from typing import cast

import duckdb
import pytest

from paritygrid.adapters.analytics import DuckDBLifecycleCoordinator
from paritygrid.adapters.analytics import duckdb as analytics_duckdb
from paritygrid.application.ports.analytics import (
    MEBIBYTE,
    AnalyticalDatabaseConfig,
    AnalyticalDatabaseInvalidError,
    AnalyticalDatabaseOwnershipError,
    AnalyticalDatabaseState,
    AnalyticalDatabaseStateError,
    AnalyticalDatabaseStorageError,
)


class StopOperation(BaseException):
    """Synthetic non-Exception interruption."""


class FakeConnection:
    """Small controllable connection for lifecycle cleanup branches."""

    def __init__(
        self,
        *,
        settings: list[tuple[object, ...]] | None = None,
        execute_failure: BaseException | None = None,
        checkpoint_failure: BaseException | None = None,
        close_failure: BaseException | None = None,
    ) -> None:
        self.settings: list[tuple[object, ...]] = (
            settings
            if settings is not None
            else [cast(tuple[object, ...], (2, "128.0 MiB", "UTC", False, False, False))]
        )
        self.execute_failure = execute_failure
        self.checkpoint_failure = checkpoint_failure
        self.close_failure = close_failure
        self.closed = False
        self.statements: list[str] = []

    def execute(self, statement: str, _parameters: object = None) -> FakeConnection:
        self.statements.append(statement)
        if statement == "CHECKPOINT" and self.checkpoint_failure is not None:
            raise self.checkpoint_failure
        if self.execute_failure is not None:
            raise self.execute_failure
        return self

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.settings

    def close(self) -> None:
        if self.close_failure is not None:
            raise self.close_failure
        self.closed = True


def _config(tmp_path: Path, name: str = "analytics.duckdb") -> AnalyticalDatabaseConfig:
    return AnalyticalDatabaseConfig(
        (tmp_path / name).resolve(),
        threads=2,
        memory_limit_bytes=128 * MEBIBYTE,
    )


def _connect_returning(
    connection: FakeConnection,
) -> Callable[..., duckdb.DuckDBPyConnection]:
    def connect(
        _database: str = ":memory:",
        *,
        read_only: bool = False,
        config: dict[str, str] | None = None,
    ) -> duckdb.DuckDBPyConnection:
        del read_only, config
        return cast(duckdb.DuckDBPyConnection, connection)

    return connect


def test_open_configures_file_and_repeated_open_is_idempotent(tmp_path: Path) -> None:
    coordinator = DuckDBLifecycleCoordinator(
        _config(tmp_path, "Caf\u00e9 % \u0645\u064a\u0646\u0627\u0621.duckdb")
    )

    initial = coordinator.snapshot()
    opened = coordinator.open()
    repeated = coordinator.open()
    settings = coordinator._fetch_all(  # pyright: ignore[reportPrivateUsage]
        "SELECT current_setting('threads'), current_setting('memory_limit'), "
        "current_setting('TimeZone'), current_setting('allow_unsigned_extensions'), "
        "current_setting('autoinstall_known_extensions'), "
        "current_setting('autoload_known_extensions')"
    )

    assert initial.state is AnalyticalDatabaseState.CLOSED
    assert initial.database_size_bytes == 0
    assert opened.state is AnalyticalDatabaseState.OPEN
    assert repeated.state is AnalyticalDatabaseState.OPEN
    assert opened.database_path.is_file()
    assert settings == ((2, "128.0 MiB", "UTC", False, False, False),)
    assert coordinator.close().state is AnalyticalDatabaseState.CLOSED


def test_transactions_commit_rollback_and_reopen_persistent_state(tmp_path: Path) -> None:
    coordinator = DuckDBLifecycleCoordinator(_config(tmp_path))
    coordinator.open()
    coordinator._execute("CREATE TABLE values_table(id INTEGER, value VARCHAR)")  # pyright: ignore[reportPrivateUsage]
    with coordinator._transaction():  # pyright: ignore[reportPrivateUsage]
        coordinator._execute(  # pyright: ignore[reportPrivateUsage]
            "INSERT INTO values_table VALUES (?, ?)", (1, "durable")
        )

    def write_then_fail() -> None:
        with coordinator._transaction():  # pyright: ignore[reportPrivateUsage]
            coordinator._execute(  # pyright: ignore[reportPrivateUsage]
                "INSERT INTO values_table VALUES (?, ?)", (2, "pending")
            )
            raise RuntimeError("rollback")

    def write_then_stop() -> None:
        with coordinator._transaction():  # pyright: ignore[reportPrivateUsage]
            coordinator._execute("INSERT INTO values_table VALUES (3, 'pending')")  # pyright: ignore[reportPrivateUsage]
            raise StopOperation

    with pytest.raises(RuntimeError, match="rollback"):
        write_then_fail()
    with pytest.raises(StopOperation):
        write_then_stop()

    assert coordinator._fetch_all("SELECT * FROM values_table ORDER BY id") == (  # pyright: ignore[reportPrivateUsage]
        (1, "durable"),
    )
    assert coordinator.checkpoint().state is AnalyticalDatabaseState.OPEN
    coordinator.close()
    coordinator.open()
    assert coordinator._fetch_all("SELECT * FROM values_table") == ((1, "durable"),)  # pyright: ignore[reportPrivateUsage]
    coordinator.close()


def test_recreate_discards_only_configured_disposable_state(tmp_path: Path) -> None:
    config = _config(tmp_path)
    adjacent = tmp_path / "keep.txt"
    adjacent.write_text("preserve", encoding="utf-8")
    coordinator = DuckDBLifecycleCoordinator(config)
    coordinator.open()
    coordinator._execute("CREATE TABLE disposable(id INTEGER)")  # pyright: ignore[reportPrivateUsage]
    coordinator._execute("INSERT INTO disposable VALUES (1)")  # pyright: ignore[reportPrivateUsage]

    recreated = coordinator.recreate()

    assert recreated.state is AnalyticalDatabaseState.OPEN
    assert (
        coordinator._fetch_all(  # pyright: ignore[reportPrivateUsage]
            "SELECT table_name FROM information_schema.tables WHERE table_name='disposable'"
        )
        == ()
    )
    assert adjacent.read_text(encoding="utf-8") == "preserve"
    coordinator.close()


def test_close_is_safe_before_open_and_when_repeated(tmp_path: Path) -> None:
    coordinator = DuckDBLifecycleCoordinator(_config(tmp_path))

    assert coordinator.close().state is AnalyticalDatabaseState.CLOSED
    assert coordinator.close().state is AnalyticalDatabaseState.CLOSED
    assert not _config(tmp_path).database_path.exists()


def test_open_connection_is_restricted_to_owner_thread(tmp_path: Path) -> None:
    coordinator = DuckDBLifecycleCoordinator(_config(tmp_path))
    coordinator.open()

    def attack() -> tuple[type[BaseException], ...]:
        failures: list[type[BaseException]] = []
        for operation in (
            coordinator.open,
            coordinator.snapshot,
            coordinator.checkpoint,
            coordinator.close,
            lambda: coordinator._execute("SELECT 1"),  # pyright: ignore[reportPrivateUsage]
            lambda: coordinator._fetch_all("SELECT 1"),  # pyright: ignore[reportPrivateUsage]
        ):
            try:
                operation()
            except BaseException as error:
                failures.append(type(error))
        return tuple(failures)

    with ThreadPoolExecutor(max_workers=1) as executor:
        failures = executor.submit(attack).result(timeout=5)

    assert failures == (AnalyticalDatabaseOwnershipError,) * 6
    assert coordinator._fetch_all("SELECT 1") == ((1,),)  # pyright: ignore[reportPrivateUsage]
    coordinator.close()


def test_closed_coordinator_can_transfer_ownership_to_another_thread(tmp_path: Path) -> None:
    coordinator = DuckDBLifecycleCoordinator(_config(tmp_path))
    coordinator.open()
    coordinator.close()
    owner_thread = get_ident()

    def own_lifecycle() -> tuple[int, AnalyticalDatabaseState, AnalyticalDatabaseState]:
        return get_ident(), coordinator.open().state, coordinator.close().state

    with ThreadPoolExecutor(max_workers=1) as executor:
        worker, opened, closed = executor.submit(own_lifecycle).result(timeout=5)

    assert worker != owner_thread
    assert opened is AnalyticalDatabaseState.OPEN
    assert closed is AnalyticalDatabaseState.CLOSED


@pytest.mark.parametrize("operation", ["execute", "fetch", "transaction"])
def test_closed_data_operations_are_rejected(tmp_path: Path, operation: str) -> None:
    coordinator = DuckDBLifecycleCoordinator(_config(tmp_path))

    def run_operation() -> None:
        if operation == "execute":
            coordinator._execute("SELECT 1")  # pyright: ignore[reportPrivateUsage]
        elif operation == "fetch":
            coordinator._fetch_all("SELECT 1")  # pyright: ignore[reportPrivateUsage]
        else:
            with coordinator._transaction():  # pyright: ignore[reportPrivateUsage]
                pass

    with pytest.raises(AnalyticalDatabaseStateError, match="not open"):
        run_operation()


def test_wrong_constructor_type_is_rejected() -> None:
    with pytest.raises(TypeError, match="AnalyticalDatabaseConfig"):
        DuckDBLifecycleCoordinator(cast(AnalyticalDatabaseConfig, object()))


def test_missing_or_invalid_parent_and_directory_target_are_rejected(tmp_path: Path) -> None:
    missing = DuckDBLifecycleCoordinator(_config(tmp_path / "missing"))
    parent_file = tmp_path / "parent-file"
    parent_file.write_text("fixed", encoding="utf-8")
    wrong_parent = DuckDBLifecycleCoordinator(
        AnalyticalDatabaseConfig((parent_file / "state.duckdb").resolve())
    )
    directory_target = tmp_path / "directory.duckdb"
    directory_target.mkdir()
    wrong_target = DuckDBLifecycleCoordinator(AnalyticalDatabaseConfig(directory_target.resolve()))

    for coordinator in (missing, wrong_parent, wrong_target):
        with pytest.raises(AnalyticalDatabaseInvalidError):
            coordinator.open()


def test_linked_parent_or_database_is_rejected_without_target_mutation(tmp_path: Path) -> None:
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked"
    try:
        linked_parent.symlink_to(real_parent, target_is_directory=True)
    except OSError:
        pytest.skip("Filesystem links are unavailable in this environment.")

    coordinator = DuckDBLifecycleCoordinator(
        AnalyticalDatabaseConfig((linked_parent / "state.duckdb").absolute())
    )
    with pytest.raises(AnalyticalDatabaseInvalidError, match="link"):
        coordinator.open()
    assert list(real_parent.iterdir()) == []

    target = real_parent / "target.duckdb"
    target.write_bytes(b"fixed")
    link = tmp_path / "linked.duckdb"
    link.symlink_to(target)
    linked_database = DuckDBLifecycleCoordinator(AnalyticalDatabaseConfig(link.absolute()))
    with pytest.raises(AnalyticalDatabaseInvalidError, match="link"):
        linked_database.open()
    assert target.read_bytes() == b"fixed"


def test_corrupt_existing_database_is_preserved_on_open_failure(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config.database_path.write_bytes(b"not a database")
    coordinator = DuckDBLifecycleCoordinator(config)

    with pytest.raises(AnalyticalDatabaseStorageError, match="opened"):
        coordinator.open()

    assert config.database_path.read_bytes() == b"not a database"
    assert coordinator.snapshot().state is AnalyticalDatabaseState.CLOSED


def test_new_database_is_removed_when_open_configuration_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = _config(tmp_path)

    def fail_connect(path: str, **_kwargs: object) -> duckdb.DuckDBPyConnection:
        Path(path).write_bytes(b"partial")
        Path(f"{path}.wal").write_bytes(b"partial")
        raise duckdb.IOException("synthetic open failure")

    monkeypatch.setattr(analytics_duckdb.duckdb, "connect", fail_connect)
    coordinator = DuckDBLifecycleCoordinator(config)

    with pytest.raises(AnalyticalDatabaseStorageError, match="opened"):
        coordinator.open()

    assert not config.database_path.exists()
    assert not Path(f"{config.database_path}.wal").exists()


def test_unexpected_open_interruption_is_propagated_and_cleaned(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = _config(tmp_path)

    def stop_connect(path: str, **_kwargs: object) -> duckdb.DuckDBPyConnection:
        Path(path).write_bytes(b"partial")
        raise StopOperation

    monkeypatch.setattr(analytics_duckdb.duckdb, "connect", stop_connect)
    coordinator = DuckDBLifecycleCoordinator(config)

    with pytest.raises(StopOperation):
        coordinator.open()

    assert not config.database_path.exists()
    assert coordinator.snapshot().state is AnalyticalDatabaseState.CLOSED


def test_statement_and_query_failures_are_redacted_and_connection_remains_usable(
    tmp_path: Path,
) -> None:
    coordinator = DuckDBLifecycleCoordinator(_config(tmp_path))
    coordinator.open()

    with pytest.raises(AnalyticalDatabaseStorageError, match="statement failed") as statement:
        coordinator._execute("SELECT * FROM missing_private_table")  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(AnalyticalDatabaseStorageError, match="query failed") as query:
        coordinator._fetch_all("SELECT * FROM missing_private_table")  # pyright: ignore[reportPrivateUsage]

    assert statement.value.__cause__ is None
    assert statement.value.__context__ is None
    assert query.value.__cause__ is None
    assert query.value.__context__ is None
    assert coordinator._fetch_all("SELECT 1") == ((1,),)  # pyright: ignore[reportPrivateUsage]
    coordinator.close()


def test_recreate_validates_all_owned_paths_before_removing_database(tmp_path: Path) -> None:
    config = _config(tmp_path)
    coordinator = DuckDBLifecycleCoordinator(config)
    coordinator.open()
    coordinator._execute("CREATE TABLE preserved(id INTEGER)")  # pyright: ignore[reportPrivateUsage]
    coordinator.close()
    unsafe_target = tmp_path / "outside.txt"
    unsafe_target.write_text("preserve", encoding="utf-8")
    wal = Path(f"{config.database_path}.wal")
    try:
        wal.symlink_to(unsafe_target)
    except OSError:
        pytest.skip("Filesystem links are unavailable in this environment.")

    with pytest.raises(AnalyticalDatabaseStorageError, match="recreated"):
        coordinator.recreate()

    assert config.database_path.is_file()
    assert unsafe_target.read_text(encoding="utf-8") == "preserve"
    wal.unlink()
    assert coordinator.snapshot().state is AnalyticalDatabaseState.FAILED


def test_source_uses_explicit_connection_and_has_no_import_side_effect(tmp_path: Path) -> None:
    source = Path("src/paritygrid/adapters/analytics/duckdb.py").read_text(encoding="utf-8")

    assert "duckdb.sql(" not in source
    assert "default_connection" not in source
    assert "duckdb.connect(" in source
    assert list(tmp_path.iterdir()) == []


def test_open_closes_partial_connection_after_typed_configuration_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    connection = FakeConnection(execute_failure=duckdb.IOException("settings"))
    monkeypatch.setattr(
        analytics_duckdb.duckdb,
        "connect",
        _connect_returning(connection),
    )
    coordinator = DuckDBLifecycleCoordinator(_config(tmp_path))

    with pytest.raises(AnalyticalDatabaseStorageError):
        coordinator.open()

    assert connection.closed is True


def test_open_closes_partial_connection_after_base_exception(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    connection = FakeConnection(execute_failure=StopOperation())
    monkeypatch.setattr(
        analytics_duckdb.duckdb,
        "connect",
        _connect_returning(connection),
    )
    coordinator = DuckDBLifecycleCoordinator(_config(tmp_path))

    with pytest.raises(StopOperation):
        coordinator.open()

    assert connection.closed is True


@pytest.mark.parametrize(
    "failure",
    [duckdb.IOException("checkpoint"), StopOperation()],
)
def test_checkpoint_failure_marks_coordinator_failed_and_requires_close(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: BaseException,
) -> None:
    connection = FakeConnection(checkpoint_failure=failure)
    monkeypatch.setattr(
        analytics_duckdb.duckdb,
        "connect",
        _connect_returning(connection),
    )
    coordinator = DuckDBLifecycleCoordinator(_config(tmp_path))
    coordinator.open()
    expected = (
        AnalyticalDatabaseStorageError if isinstance(failure, duckdb.Error) else StopOperation
    )

    with pytest.raises(expected):
        coordinator.checkpoint()
    with pytest.raises(AnalyticalDatabaseStateError, match="must be closed"):
        coordinator.open()

    assert coordinator.snapshot().state is AnalyticalDatabaseState.FAILED
    assert coordinator.close().state is AnalyticalDatabaseState.CLOSED


def test_close_reports_checkpoint_failure_after_releasing_connection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    connection = FakeConnection(checkpoint_failure=duckdb.IOException("checkpoint"))
    monkeypatch.setattr(
        analytics_duckdb.duckdb,
        "connect",
        _connect_returning(connection),
    )
    coordinator = DuckDBLifecycleCoordinator(_config(tmp_path))
    coordinator.open()

    with pytest.raises(AnalyticalDatabaseStorageError, match="during close"):
        coordinator.close()

    assert connection.closed is True
    assert coordinator.snapshot().state is AnalyticalDatabaseState.CLOSED


@pytest.mark.parametrize(
    "failure",
    [duckdb.IOException("close"), StopOperation()],
)
def test_close_failure_marks_coordinator_failed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: BaseException,
) -> None:
    connection = FakeConnection(close_failure=failure)
    monkeypatch.setattr(
        analytics_duckdb.duckdb,
        "connect",
        _connect_returning(connection),
    )
    coordinator = DuckDBLifecycleCoordinator(_config(tmp_path))
    coordinator.open()
    expected = (
        AnalyticalDatabaseStorageError if isinstance(failure, duckdb.Error) else StopOperation
    )

    with pytest.raises(expected):
        coordinator.close()

    assert coordinator.snapshot().state is AnalyticalDatabaseState.FAILED


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [(2, "128.0 MiB")],
        [(True, "128.0 MiB", "UTC", False, False, False)],
        [(3, "128.0 MiB", "UTC", False, False, False)],
        [(2, 128, "UTC", False, False, False)],
        [(2, "127.0 MiB", "UTC", False, False, False)],
        [(2, "128.0 MiB", "local", False, False, False)],
        [(2, "128.0 MiB", "UTC", True, False, False)],
        [(2, "128.0 MiB", "UTC", False, True, False)],
        [(2, "128.0 MiB", "UTC", False, False, True)],
    ],
)
def test_runtime_setting_drift_is_rejected(rows: list[tuple[object, ...]], tmp_path: Path) -> None:
    connection = FakeConnection(settings=rows)

    with pytest.raises(duckdb.IOException, match="settings"):
        analytics_duckdb._validate_runtime_settings(  # pyright: ignore[reportPrivateUsage]
            cast(duckdb.DuckDBPyConnection, connection), _config(tmp_path)
        )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("128.0 MiB", 128 * MEBIBYTE),
        ("128 MB", -1),
        ("128", -1),
        ("invalid MiB", -1),
        ("0.0000001 MiB", -1),
    ],
)
def test_memory_setting_parser_is_exact(value: str, expected: int) -> None:
    assert analytics_duckdb._memory_bytes(value) == expected  # pyright: ignore[reportPrivateUsage]


def test_path_helpers_translate_inspection_failures(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "state.duckdb"

    def fail_exists(_path: Path) -> bool:
        raise OSError("inspection")

    monkeypatch.setattr(Path, "exists", fail_exists)
    with pytest.raises(AnalyticalDatabaseInvalidError, match="inspected"):
        analytics_duckdb._validate_database_location(path)  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(AnalyticalDatabaseStorageError, match="inspected"):
        analytics_duckdb._file_size(path)  # pyright: ignore[reportPrivateUsage]


def test_path_helpers_reject_reported_links_without_platform_support(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "state.duckdb"
    path.write_bytes(b"fixed")
    original = Path.is_symlink

    def report_database_link(candidate: Path) -> bool:
        return candidate == path or original(candidate)

    monkeypatch.setattr(Path, "is_symlink", report_database_link)
    with pytest.raises(AnalyticalDatabaseInvalidError, match="link"):
        analytics_duckdb._validate_database_location(path)  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(AnalyticalDatabaseStorageError, match="inspected"):
        analytics_duckdb._file_size(path)  # pyright: ignore[reportPrivateUsage]


def test_parent_link_detection_is_independent_of_database_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    parent = tmp_path / "linked-parent"
    parent.mkdir()
    original = Path.is_symlink

    def report_parent_link(candidate: Path) -> bool:
        return candidate == parent or original(candidate)

    monkeypatch.setattr(Path, "is_symlink", report_parent_link)
    with pytest.raises(AnalyticalDatabaseInvalidError, match="traverse"):
        analytics_duckdb._reject_link_components(parent)  # pyright: ignore[reportPrivateUsage]


def test_new_database_cleanup_suppresses_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fail_removal(_path: Path) -> None:
        raise OSError("cleanup")

    monkeypatch.setattr(analytics_duckdb, "_remove_database_files", fail_removal)

    analytics_duckdb._remove_new_database(tmp_path / "state.duckdb")  # pyright: ignore[reportPrivateUsage]


def test_existing_database_survives_unexpected_open_interruption(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = _config(tmp_path)
    config.database_path.write_bytes(b"existing")
    connection = FakeConnection(execute_failure=StopOperation())
    monkeypatch.setattr(
        analytics_duckdb.duckdb,
        "connect",
        _connect_returning(connection),
    )
    coordinator = DuckDBLifecycleCoordinator(config)

    with pytest.raises(StopOperation):
        coordinator.open()

    assert config.database_path.read_bytes() == b"existing"


def test_recreate_can_open_from_closed_state(tmp_path: Path) -> None:
    coordinator = DuckDBLifecycleCoordinator(_config(tmp_path))

    assert coordinator.recreate().state is AnalyticalDatabaseState.OPEN

    coordinator.close()


def test_recreate_removal_failure_is_typed_and_failed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    coordinator = DuckDBLifecycleCoordinator(_config(tmp_path))

    def fail_removal(_path: Path) -> None:
        raise OSError("remove")

    monkeypatch.setattr(analytics_duckdb, "_remove_database_files", fail_removal)

    with pytest.raises(AnalyticalDatabaseStorageError, match="recreated"):
        coordinator.recreate()

    assert coordinator.snapshot().state is AnalyticalDatabaseState.FAILED


def test_removal_rejects_non_file_owned_path(tmp_path: Path) -> None:
    directory = tmp_path / "state.duckdb"
    directory.mkdir()

    with pytest.raises(OSError, match="unsafe"):
        analytics_duckdb._remove_database_files(directory)  # pyright: ignore[reportPrivateUsage]
