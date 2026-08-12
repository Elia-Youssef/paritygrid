"""Atomic migration runtime boundary tests."""

from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic.config import Config
from alembic.util.exc import CommandError
from sqlalchemy import Connection, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from paritygrid.adapters.persistence import migration as migration_runtime
from paritygrid.adapters.persistence.errors import (
    MigrationConfigurationError,
    MigrationExecutionError,
    MigrationIntegrityError,
)
from paritygrid.adapters.persistence.migration import (
    HEAD_REVISION,
    MigrationReport,
    upgrade_to_head,
)


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    database = tmp_path / "migrations.db"
    engine = create_engine(f"sqlite+pysqlite:///{database}")
    yield engine
    engine.dispose()


def _install_revision(connection: Connection, revision: str = HEAD_REVISION) -> None:
    connection.exec_driver_sql(
        "CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL PRIMARY KEY)"
    )
    connection.exec_driver_sql(
        "INSERT INTO alembic_version (version_num) VALUES (?)",
        (revision,),
    )


def _foreign_keys(connection: Connection) -> int:
    value = connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one()
    connection.rollback()
    assert type(value) is int
    return value


def _expected_head(_config: Config) -> str:
    return HEAD_REVISION


def _no_upgrade(_config: Config, _revision: str) -> None:
    return None


def _accept_schema(_connection: Connection, _revision: str) -> None:
    return None


def test_report_is_immutable_and_records_noop_status() -> None:
    report = MigrationReport(HEAD_REVISION, HEAD_REVISION, HEAD_REVISION)

    assert report.previous_revision == HEAD_REVISION
    assert report.current_revision == HEAD_REVISION
    assert report.target_revision == HEAD_REVISION
    with pytest.raises((AttributeError, TypeError)):
        report.current_revision = "changed"  # type: ignore[misc]


def test_upgrade_uses_exact_connection_and_returns_report(
    monkeypatch: pytest.MonkeyPatch,
    engine: Engine,
) -> None:
    observed: list[Connection] = []

    def install(config: Config, revision: str) -> None:
        assert revision == HEAD_REVISION
        supplied = config.attributes["connection"]
        assert isinstance(supplied, Connection)
        observed.append(supplied)
        _install_revision(supplied)

    monkeypatch.setattr(migration_runtime, "_configured_head", _expected_head)
    monkeypatch.setattr(migration_runtime.command, "upgrade", install)
    monkeypatch.setattr(migration_runtime, "_validate_revision_state", _accept_schema)

    with engine.connect() as connection:
        report = upgrade_to_head(connection)

        assert observed == [connection]
        assert report == MigrationReport(None, HEAD_REVISION, HEAD_REVISION)
        assert connection.in_transaction() is False
        assert _foreign_keys(connection) == 1


def test_repeat_upgrade_is_noop_and_still_checks_integrity(
    monkeypatch: pytest.MonkeyPatch,
    engine: Engine,
) -> None:
    validations: list[str] = []

    def record_validation(_connection: Connection, revision: str) -> None:
        validations.append(revision)

    monkeypatch.setattr(migration_runtime, "_configured_head", _expected_head)
    monkeypatch.setattr(migration_runtime.command, "upgrade", _no_upgrade)
    monkeypatch.setattr(migration_runtime, "_validate_revision_state", record_validation)

    with engine.begin() as connection:
        _install_revision(connection)
    with engine.connect() as connection:
        report = upgrade_to_head(connection)

        assert report == MigrationReport(HEAD_REVISION, HEAD_REVISION, HEAD_REVISION)
        assert validations == [HEAD_REVISION]
        assert connection.in_transaction() is False
        assert _foreign_keys(connection) == 1


def test_sqlalchemy_failure_rolls_back_all_schema_and_restores_postconditions(
    monkeypatch: pytest.MonkeyPatch,
    engine: Engine,
) -> None:
    def fail_mid_migration(config: Config, _revision: str) -> None:
        connection = config.attributes["connection"]
        assert isinstance(connection, Connection)
        connection.exec_driver_sql("CREATE TABLE partial_state (id INTEGER PRIMARY KEY)")
        raise SQLAlchemyError("synthetic migration failure")

    monkeypatch.setattr(migration_runtime, "_configured_head", _expected_head)
    monkeypatch.setattr(migration_runtime.command, "upgrade", fail_mid_migration)

    with engine.connect() as connection:
        with pytest.raises(MigrationExecutionError, match="failed atomically"):
            upgrade_to_head(connection)

        residue = connection.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE name IN ('partial_state', 'alembic_version')"
        ).all()
        connection.rollback()
        assert residue == []
        assert connection.in_transaction() is False
        assert _foreign_keys(connection) == 1


def test_base_exception_rolls_back_and_propagates_original_failure(
    monkeypatch: pytest.MonkeyPatch,
    engine: Engine,
) -> None:
    class ControlledStop(BaseException):
        pass

    def stop(config: Config, _revision: str) -> None:
        connection = config.attributes["connection"]
        assert isinstance(connection, Connection)
        connection.exec_driver_sql("CREATE TABLE interrupted (id INTEGER PRIMARY KEY)")
        raise ControlledStop

    monkeypatch.setattr(migration_runtime, "_configured_head", _expected_head)
    monkeypatch.setattr(migration_runtime.command, "upgrade", stop)

    with engine.connect() as connection:
        with pytest.raises(ControlledStop):
            upgrade_to_head(connection)
        assert connection.in_transaction() is False
        assert _foreign_keys(connection) == 1
        names = connection.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE name = 'interrupted'"
        ).all()
        connection.rollback()
        assert names == []


def test_open_transaction_is_rejected_without_touching_caller_work(engine: Engine) -> None:
    with engine.connect() as connection:
        connection.exec_driver_sql("CREATE TABLE caller_work (id INTEGER PRIMARY KEY)")

        with pytest.raises(MigrationConfigurationError, match="idle"):
            upgrade_to_head(connection)

        assert connection.in_transaction() is True
        connection.rollback()


def test_closed_connection_is_rejected(engine: Engine) -> None:
    connection = engine.connect()
    connection.close()

    with pytest.raises(MigrationConfigurationError, match="open"):
        upgrade_to_head(connection)


def test_non_connection_and_non_sqlite_connection_are_rejected() -> None:
    with pytest.raises(MigrationConfigurationError, match="SQLAlchemy Connection"):
        upgrade_to_head(object())  # type: ignore[arg-type]

    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.connect() as connection:
        original_name = connection.dialect.name
        connection.dialect.name = "synthetic"
        try:
            with pytest.raises(MigrationConfigurationError, match="only SQLite"):
                upgrade_to_head(connection)
        finally:
            connection.dialect.name = original_name
    engine.dispose()


def test_multiple_heads_and_missing_ending_revision_fail_integrity(
    monkeypatch: pytest.MonkeyPatch,
    engine: Engine,
) -> None:
    monkeypatch.setattr(migration_runtime, "_configured_head", _expected_head)
    monkeypatch.setattr(migration_runtime.command, "upgrade", _no_upgrade)

    with engine.connect() as connection:
        with pytest.raises(MigrationIntegrityError, match="did not install"):
            upgrade_to_head(connection)
        assert connection.in_transaction() is False
        assert _foreign_keys(connection) == 1


def test_head_stamped_database_without_schema_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    engine: Engine,
) -> None:
    monkeypatch.setattr(migration_runtime, "_configured_head", _expected_head)
    monkeypatch.setattr(migration_runtime.command, "upgrade", _no_upgrade)
    with engine.begin() as connection:
        _install_revision(connection)

    with engine.connect() as connection:
        with pytest.raises(MigrationIntegrityError, match="table inventory"):
            upgrade_to_head(connection)
        assert connection.in_transaction() is False
        assert _foreign_keys(connection) == 1


def test_runtime_config_uses_package_resource_and_exact_connection(engine: Engine) -> None:
    with engine.connect() as connection:
        config = migration_runtime._migration_config(  # pyright: ignore[reportPrivateUsage]
            connection
        )

        assert config.get_main_option("script_location") == (
            "paritygrid.adapters.persistence:migrations"
        )
        assert config.attributes["connection"] is connection


def test_packaged_history_requires_one_expected_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SyntheticScript:
        def __init__(self, heads: list[str]) -> None:
            self._heads = heads

        def get_heads(self) -> list[str]:
            return self._heads

    def unexpected_script(_config: Config) -> SyntheticScript:
        return SyntheticScript(["unexpected"])

    monkeypatch.setattr(migration_runtime.ScriptDirectory, "from_config", unexpected_script)

    with pytest.raises(MigrationConfigurationError, match="unexpected head"):
        migration_runtime._configured_head(Config())  # pyright: ignore[reportPrivateUsage]


def test_alembic_command_error_is_translated_to_configuration_failure(
    monkeypatch: pytest.MonkeyPatch,
    engine: Engine,
) -> None:
    def fail_upgrade(_config: Config, _revision: str) -> None:
        raise CommandError("synthetic history failure")

    monkeypatch.setattr(migration_runtime, "_configured_head", _expected_head)
    monkeypatch.setattr(migration_runtime.command, "upgrade", fail_upgrade)

    with engine.connect() as connection:
        with pytest.raises(MigrationConfigurationError, match="history or database revision"):
            upgrade_to_head(connection)
        assert connection.in_transaction() is False
        assert _foreign_keys(connection) == 1


def test_malformed_version_table_is_typed_and_rolled_back(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE alembic_version (wrong_column TEXT)")

    with engine.connect() as connection:
        with pytest.raises(MigrationExecutionError, match="failed atomically"):
            upgrade_to_head(connection)
        assert connection.in_transaction() is False
        assert _foreign_keys(connection) == 1


def test_multiple_revision_rows_are_rejected_as_multiple_heads(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE alembic_version (version_num VARCHAR(32))")
        connection.exec_driver_sql(
            "INSERT INTO alembic_version VALUES ('0001_operational'), ('other_head')"
        )

    with (
        engine.connect() as connection,
        pytest.raises(MigrationIntegrityError, match="multiple migration heads"),
    ):
        upgrade_to_head(connection)


def test_revision_validator_rejects_unexpected_revision(engine: Engine) -> None:
    with engine.connect() as connection:
        with pytest.raises(MigrationIntegrityError, match="expected revision"):
            migration_runtime._validate_revision_state(  # pyright: ignore[reportPrivateUsage]
                connection, "unexpected"
            )
        connection.rollback()


def test_revision_validator_rejects_missing_trigger_inventory(
    monkeypatch: pytest.MonkeyPatch,
    engine: Engine,
) -> None:
    original_expected = migration_runtime._EXPECTED_TABLE_NAMES  # pyright: ignore[reportPrivateUsage]
    monkeypatch.setattr(migration_runtime, "_EXPECTED_TABLE_NAMES", frozenset[str]())
    with engine.connect() as connection:
        with pytest.raises(MigrationIntegrityError, match="trigger inventory"):
            migration_runtime._validate_revision_state(  # pyright: ignore[reportPrivateUsage]
                connection, HEAD_REVISION
            )
        connection.rollback()
    monkeypatch.setattr(migration_runtime, "_EXPECTED_TABLE_NAMES", original_expected)


def test_revision_validator_rejects_malformed_schema_metadata(
    monkeypatch: pytest.MonkeyPatch,
    engine: Engine,
) -> None:
    class MalformedResult:
        def all(self) -> list[tuple[object, object, object, object]]:
            return [("table", "sample", "sample", None)]

    with engine.connect() as connection:
        upgrade_to_head(connection)

    original_execute = Connection.exec_driver_sql

    def intercept_schema_rows(
        connection: Connection,
        statement: str,
        parameters: object | None = None,
        execution_options: object | None = None,
    ) -> object:
        if statement.startswith("SELECT type, name, tbl_name, sql"):
            return MalformedResult()
        return original_execute(
            connection,
            statement,
            parameters,  # type: ignore[arg-type]
            execution_options=execution_options,  # type: ignore[arg-type]
        )

    monkeypatch.setattr(Connection, "exec_driver_sql", intercept_schema_rows)
    with (
        engine.connect() as connection,
        pytest.raises(MigrationIntegrityError, match="malformed schema metadata"),
    ):
        migration_runtime._validate_revision_state(  # pyright: ignore[reportPrivateUsage]
            connection, HEAD_REVISION
        )


def test_postcondition_failure_without_migration_failure_has_no_note(
    monkeypatch: pytest.MonkeyPatch,
    engine: Engine,
) -> None:
    def complete_upgrade(config: Config, _revision: str) -> None:
        connection = config.attributes["connection"]
        assert isinstance(connection, Connection)
        _install_revision(connection)

    def fail_postconditions(_connection: Connection) -> None:
        raise MigrationIntegrityError("synthetic postcondition failure")

    monkeypatch.setattr(migration_runtime, "_configured_head", _expected_head)
    monkeypatch.setattr(migration_runtime.command, "upgrade", complete_upgrade)
    monkeypatch.setattr(migration_runtime, "_validate_revision_state", _accept_schema)
    monkeypatch.setattr(
        migration_runtime, "_restore_connection_postconditions", fail_postconditions
    )

    with (
        engine.connect() as connection,
        pytest.raises(MigrationIntegrityError, match="postcondition") as captured,
    ):
        upgrade_to_head(connection)

    assert not hasattr(captured.value, "__notes__")


def test_foreign_key_restore_failure_is_typed_and_leaves_connection_idle(
    monkeypatch: pytest.MonkeyPatch,
    engine: Engine,
) -> None:
    def foreign_keys_disabled(_connection: Connection) -> bool:
        return False

    monkeypatch.setattr(migration_runtime, "_foreign_keys_enabled", foreign_keys_disabled)
    with engine.connect() as connection:
        connection.exec_driver_sql("SELECT 1")
        assert connection.in_transaction() is True
        with pytest.raises(MigrationIntegrityError, match="could not be restored"):
            migration_runtime._restore_connection_postconditions(  # pyright: ignore[reportPrivateUsage]
                connection
            )
        assert connection.in_transaction() is False


def test_postcondition_failure_is_preserved_and_annotates_migration_failure(
    monkeypatch: pytest.MonkeyPatch,
    engine: Engine,
) -> None:
    def fail_upgrade(_config: Config, _revision: str) -> None:
        raise MigrationIntegrityError("synthetic migration integrity failure")

    def fail_postconditions(_connection: Connection) -> None:
        raise MigrationIntegrityError("synthetic postcondition failure")

    monkeypatch.setattr(migration_runtime, "_configured_head", _expected_head)
    monkeypatch.setattr(migration_runtime.command, "upgrade", fail_upgrade)
    monkeypatch.setattr(
        migration_runtime, "_restore_connection_postconditions", fail_postconditions
    )

    with (
        engine.connect() as connection,
        pytest.raises(MigrationIntegrityError, match="postcondition") as captured,
    ):
        upgrade_to_head(connection)

    assert any("migration also failed" in note for note in captured.value.__notes__)
