"""Initial operational migration contract tests."""

import ast
import subprocess
import sys
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest
from alembic import command
from alembic.script import ScriptDirectory
from mako.template import Template  # pyright: ignore[reportMissingTypeStubs]
from sqlalchemy import Connection, create_engine, event, inspect
from sqlalchemy.engine import Engine

from paritygrid.adapters.persistence.errors import (
    MigrationConfigurationError,
    MigrationIntegrityError,
)
from paritygrid.adapters.persistence.migration import (
    HEAD_REVISION,
    MigrationReport,
    _migration_config,  # pyright: ignore[reportPrivateUsage]
    upgrade_to_head,
)
from paritygrid.adapters.persistence.schema import metadata
from paritygrid.adapters.persistence.triggers import install_integrity_triggers

REVISION_PATH = (
    Path(__file__).parents[2]
    / "src/paritygrid/adapters/persistence/migrations/versions/0001_operational.py"
)
ROOT = Path(__file__).parents[2]
TEMPLATE_PATH = ROOT / "src/paritygrid/adapters/persistence/migrations/script.py.mako"


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'operational.db'}")
    yield engine
    engine.dispose()


def _operational_tables(connection: Connection) -> tuple[str, ...]:
    return tuple(
        str(row[0])
        for row in connection.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name NOT LIKE 'sqlite_%' AND name <> 'alembic_version' ORDER BY name"
        )
    )


def _schema_definitions(connection: Connection) -> tuple[tuple[object, ...], ...]:
    return tuple(
        tuple(row)
        for row in connection.exec_driver_sql(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' "
            "AND name <> 'alembic_version' ORDER BY type, name"
        ).all()
    )


def test_packaged_history_has_one_initial_head(engine: Engine) -> None:
    with engine.connect() as connection:
        script = ScriptDirectory.from_config(_migration_config(connection))

    assert script.get_heads() == [HEAD_REVISION]
    revision = script.get_revision(HEAD_REVISION)
    assert revision.revision == HEAD_REVISION
    assert revision.down_revision is None


def test_revision_is_self_contained_and_has_fixed_statement_inventory() -> None:
    tree = ast.parse(REVISION_PATH.read_text(encoding="utf-8"), filename=str(REVISION_PATH))
    imported_modules = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    string_values = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]

    assert not any(module.startswith("paritygrid") for module in imported_modules)
    assert sum(value.startswith("CREATE TABLE") for value in string_values) == 21
    assert sum(value.startswith("CREATE INDEX") for value in string_values) == 27
    assert sum(value.startswith("CREATE TRIGGER") for value in string_values) == 47


def test_future_revision_template_never_renders_noop_downgrade() -> None:
    template = Template(filename=str(TEMPLATE_PATH))
    rendered = cast(
        str,
        template.render(  # pyright: ignore[reportUnknownMemberType]
            message="Future migration",
            up_revision="future_revision",
            down_revision=HEAD_REVISION,
            branch_labels=None,
            depends_on=None,
            imports="",
            upgrades="pass",
            downgrades="",
        ),
    )
    tree = ast.parse(rendered)
    downgrade = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "downgrade"
    )
    raised = downgrade.body[0]

    assert not any(isinstance(node, ast.Pass) for node in downgrade.body)
    assert isinstance(raised, ast.Raise)
    assert isinstance(raised.exc, ast.Call)
    assert isinstance(raised.exc.func, ast.Name)
    assert raised.exc.func.id == "RuntimeError"


def test_fresh_upgrade_installs_exact_structural_baseline(engine: Engine) -> None:
    with engine.connect() as connection:
        report = upgrade_to_head(connection)
        inspector = inspect(connection)
        table_names = _operational_tables(connection)
        column_count = sum(len(inspector.get_columns(name)) for name in table_names)
        primary_key_count = sum(
            bool(inspector.get_pk_constraint(name).get("constrained_columns"))
            for name in table_names
        )
        unique_count = sum(len(inspector.get_unique_constraints(name)) for name in table_names)
        foreign_key_count = sum(len(inspector.get_foreign_keys(name)) for name in table_names)
        check_count = sum(len(inspector.get_check_constraints(name)) for name in table_names)
        indexes = [index for name in table_names for index in inspector.get_indexes(name)]
        trigger_count = cast(
            int,
            connection.exec_driver_sql(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger'"
            ).scalar_one(),
        )
        connection.rollback()

    assert report == MigrationReport(None, HEAD_REVISION, HEAD_REVISION)
    assert len(table_names) == 21
    assert column_count == 211
    assert primary_key_count == 21
    assert unique_count == 11
    assert foreign_key_count == 18
    assert check_count == 209
    assert len(indexes) == 27
    assert trigger_count == 47
    assert sum("sqlite_where" in index.get("dialect_options", {}) for index in indexes) == 1


def test_frozen_revision_matches_current_metadata_and_trigger_contract(
    tmp_path: Path,
) -> None:
    migrated = create_engine(f"sqlite+pysqlite:///{tmp_path / 'migrated.db'}")
    reference = create_engine(f"sqlite+pysqlite:///{tmp_path / 'reference.db'}")
    try:
        with migrated.connect() as connection:
            upgrade_to_head(connection)
            migrated_definitions = _schema_definitions(connection)
            connection.rollback()
        with reference.begin() as connection:
            metadata.create_all(connection)
            install_integrity_triggers(connection)
        with reference.connect() as connection:
            reference_definitions = _schema_definitions(connection)
            connection.rollback()
    finally:
        migrated.dispose()
        reference.dispose()

    assert migrated_definitions == reference_definitions


def test_unversioned_exact_schema_lookalike_is_rejected_without_changes(
    engine: Engine,
) -> None:
    with engine.begin() as connection:
        metadata.create_all(connection)
        install_integrity_triggers(connection)
    with engine.connect() as connection:
        before = _schema_definitions(connection)
        connection.rollback()

        with pytest.raises(MigrationIntegrityError, match="unversioned operational database"):
            upgrade_to_head(connection)

        after = _schema_definitions(connection)
        version_table = connection.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE name = 'alembic_version'"
        ).all()
        connection.rollback()

    assert after == before
    assert version_table == []


def test_repeat_upgrade_detects_semantic_index_tampering(engine: Engine) -> None:
    with engine.connect() as connection:
        upgrade_to_head(connection)
        connection.exec_driver_sql("DROP INDEX ix_artifact_manifests_sha256")
        connection.exec_driver_sql(
            "CREATE INDEX ix_artifact_manifests_sha256 ON artifact_manifests (sha256, media_type)"
        )
        connection.commit()

        with pytest.raises(MigrationIntegrityError, match="does not match"):
            upgrade_to_head(connection)

        assert connection.in_transaction() is False
        foreign_keys = connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one()
        connection.rollback()
        assert foreign_keys == 1


@pytest.mark.parametrize(
    "tamper_sql",
    [
        "CREATE TABLE unexpected_object (value TEXT)",
        "DROP TRIGGER trg_artifact_manifests_prohibit_delete\n-- next\n"
        'CREATE TRIGGER "trg_artifact_manifests_prohibit_delete" '
        'BEFORE DELETE ON "artifact_manifests" BEGIN SELECT RAISE(ABORT, "changed"); END',
    ],
)
def test_repeat_upgrade_rejects_extra_or_same_count_trigger_drift(
    engine: Engine,
    tamper_sql: str,
) -> None:
    with engine.connect() as connection:
        upgrade_to_head(connection)
        for statement in tamper_sql.split("\n-- next\n"):
            connection.exec_driver_sql(statement)
        connection.commit()

        with pytest.raises(MigrationIntegrityError):
            upgrade_to_head(connection)


def test_repeat_upgrade_rejects_same_count_table_definition_drift(engine: Engine) -> None:
    with engine.connect() as connection:
        upgrade_to_head(connection)
        connection.exec_driver_sql("PRAGMA foreign_keys = OFF")
        connection.exec_driver_sql("ALTER TABLE system_metadata RENAME TO original_metadata")
        connection.exec_driver_sql(
            'CREATE TABLE "system_metadata" '
            '("key" VARCHAR(96) NOT NULL, value TEXT NOT NULL, updated_at VARCHAR(27) NOT NULL, '
            'CONSTRAINT pk_system_metadata PRIMARY KEY ("key"), CHECK (length(value) > 0))'
        )
        connection.exec_driver_sql("DROP TABLE original_metadata")
        connection.commit()

        with pytest.raises(MigrationIntegrityError):
            upgrade_to_head(connection)


def test_repeat_upgrade_is_verified_noop(engine: Engine) -> None:
    with engine.connect() as connection:
        first = upgrade_to_head(connection)
        before = connection.exec_driver_sql(
            "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name"
        ).all()
        before_total_changes = connection.exec_driver_sql("SELECT total_changes()").scalar_one()
        before_schema_version = connection.exec_driver_sql("PRAGMA schema_version").scalar_one()
        connection.rollback()

        second = upgrade_to_head(connection)
        after = connection.exec_driver_sql(
            "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name"
        ).all()
        after_total_changes = connection.exec_driver_sql("SELECT total_changes()").scalar_one()
        after_schema_version = connection.exec_driver_sql("PRAGMA schema_version").scalar_one()
        connection.rollback()

    assert first == MigrationReport(None, HEAD_REVISION, HEAD_REVISION)
    assert second == MigrationReport(HEAD_REVISION, HEAD_REVISION, HEAD_REVISION)
    assert after == before
    assert after_total_changes == before_total_changes
    assert after_schema_version == before_schema_version


@pytest.mark.parametrize(
    "failure_prefix",
    [
        "CREATE TABLE pipelines",
        "CREATE INDEX ix_artifact_manifests_sha256",
        'CREATE TRIGGER "trg_artifact_manifests_prohibit_delete"',
        "INSERT INTO alembic_version",
    ],
)
def test_real_upgrade_rolls_back_base_exception_at_each_schema_stage(
    tmp_path: Path,
    failure_prefix: str,
) -> None:
    class ControlledStop(BaseException):
        pass

    database = tmp_path / f"failure-{failure_prefix.split()[1]}.db"
    engine = create_engine(f"sqlite+pysqlite:///{database}")

    def stop_at_statement(
        _connection: Connection,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        if statement.startswith(failure_prefix):
            raise ControlledStop

    event.listen(engine, "before_cursor_execute", stop_at_statement)
    try:
        with engine.connect() as connection:
            with pytest.raises(ControlledStop):
                upgrade_to_head(connection)

            residue = connection.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name"
            ).all()
            connection.rollback()
            assert residue == []
            assert connection.in_transaction() is False
            foreign_keys = connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one()
            connection.rollback()
            assert foreign_keys == 1
    finally:
        engine.dispose()


def test_downgrade_is_irreversible_before_schema_changes(engine: Engine) -> None:
    with engine.connect() as connection:
        upgrade_to_head(connection)
        before = connection.exec_driver_sql(
            "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name"
        ).all()
        connection.rollback()
        config = _migration_config(connection)

        with pytest.raises(RuntimeError, match="cannot be downgraded"):
            command.downgrade(config, "base")

        if connection.in_transaction():
            connection.rollback()
        connection.exec_driver_sql("PRAGMA foreign_keys = ON")
        foreign_keys = connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one()
        connection.rollback()
        after = connection.exec_driver_sql(
            "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name"
        ).all()
        connection.rollback()
        assert after == before
        assert foreign_keys == 1
        assert connection.in_transaction() is False


def test_direct_alembic_upgrade_without_supplied_connection_fails_closed(tmp_path: Path) -> None:
    empty_cwd = tmp_path / "direct-alembic"
    empty_cwd.mkdir()
    config_path = ROOT / "alembic.ini"
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "-c", str(config_path), "upgrade", "head"],
        cwd=empty_cwd,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "caller-owned SQLAlchemy Connection" in result.stderr
    assert tuple(empty_cwd.iterdir()) == ()


def test_root_alembic_configuration_contains_no_database_url() -> None:
    configuration = (ROOT / "alembic.ini").read_text(encoding="utf-8")

    assert "sqlalchemy.url" not in configuration
    assert "script_location = %(here)s/src/paritygrid/adapters/persistence/migrations" in (
        configuration
    )


def test_root_alembic_heads_works_from_arbitrary_cwd(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "-c", str(ROOT / "alembic.ini"), "heads"],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "0001_operational (head)"
    assert tuple(tmp_path.iterdir()) == ()


def test_upgrade_works_from_arbitrary_unicode_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    engine: Engine,
) -> None:
    unrelated = tmp_path / "percentage-100%-é"
    unrelated.mkdir()
    monkeypatch.chdir(unrelated)

    with engine.connect() as connection:
        report = upgrade_to_head(connection)

    assert report == MigrationReport(None, HEAD_REVISION, HEAD_REVISION)
    assert tuple(unrelated.iterdir()) == ()


def test_wheel_contains_and_executes_packaged_history_outside_checkout(tmp_path: Path) -> None:
    distribution = tmp_path / "wheel"
    build = subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(distribution)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert build.returncode == 0, build.stderr
    wheel = next(distribution.glob("paritygrid-*.whl"))
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
    assert "paritygrid/adapters/persistence/migrations/env.py" in names
    assert "paritygrid/adapters/persistence/migrations/versions/0001_operational.py" in names
    external = tmp_path / "outside-checkout-100%-é"
    external.mkdir()
    database = external / "wheel.db"
    source = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
checkout = Path(sys.argv[3]).resolve()
assert checkout not in tuple(Path(item).resolve() for item in sys.path if item)
from sqlalchemy import create_engine
from paritygrid.adapters.persistence.migration import upgrade_to_head
engine = create_engine(f"sqlite+pysqlite:///{Path(sys.argv[2])}")
with engine.connect() as connection:
    report = upgrade_to_head(connection)
    assert report.current_revision == "0001_operational"
    table_count = connection.exec_driver_sql(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' "
        "AND name NOT LIKE 'sqlite_%' AND name <> 'alembic_version'"
    ).scalar_one()
    trigger_count = connection.exec_driver_sql(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger'"
    ).scalar_one()
    assert table_count == 21
    assert trigger_count == 47
engine.dispose()
"""
    result = subprocess.run(
        [sys.executable, "-c", source, str(wheel), str(database), str(ROOT)],
        cwd=external,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_offline_and_missing_connection_environment_are_rejected() -> None:
    with pytest.raises(MigrationConfigurationError):
        command.upgrade(_migration_config(cast(Connection, object())), HEAD_REVISION)
