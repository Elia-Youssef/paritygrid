"""Deterministic v0001 fixture generation and verification tests."""

import ast
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import cast

import pytest

from paritygrid.quality import frozen_schema
from paritygrid.quality.frozen_schema import FrozenFixtureError

EXPECTED_SCHEMA_HASH = "50ff2626553f6e5250e217b79f06fc3a957a59ab2ffc3341fbd23b52fdcc243c"
EXPECTED_SEED_HASH = "4d8d9221c63c2a38dd85c5b68807a993ca8962a9193fc4d883c901d9374f10d2"
EXPECTED_SCHEMA_INVENTORY = {
    "check_constraint_count": 209,
    "column_count": 211,
    "explicit_index_count": 27,
    "foreign_key_count": 18,
    "primary_key_count": 21,
    "table_count": 21,
    "trigger_count": 47,
    "unique_constraint_count": 11,
}
FIXTURE_DIRECTORY = Path(__file__).parents[1] / "fixtures/persistence/v0001"
ROOT = Path(__file__).parents[2]


def _copy_fixture(destination: Path) -> None:
    destination.mkdir(parents=True)
    for source in FIXTURE_DIRECTORY.iterdir():
        (destination / source.name).write_bytes(source.read_bytes())


def _state(directory: Path) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in directory.iterdir() if path.is_file()}


def test_build_is_pinned_to_reviewed_revision_and_exact_hashes() -> None:
    fixture = frozen_schema.build_fixture()
    manifest = json.loads(fixture.manifest)

    assert frozen_schema.TARGET_REVISION == "0001_operational"
    assert frozen_schema.REVISION_RESOURCE == "0001_operational.py"
    assert hashlib.sha256(fixture.schema).hexdigest() == EXPECTED_SCHEMA_HASH
    assert hashlib.sha256(fixture.seed).hexdigest() == EXPECTED_SEED_HASH
    assert b"0001_operational" in fixture.schema
    assert manifest["schema_inventory"] == EXPECTED_SCHEMA_INVENTORY


def test_generator_imports_only_the_pinned_revision_resource() -> None:
    source = Path(frozen_schema.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    resource_names = {
        argument.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "files"
        for argument in node.args
        if isinstance(argument, ast.Constant) and isinstance(argument.value, str)
    }

    assert "paritygrid.adapters.persistence.schema" not in imports
    assert "paritygrid.adapters.persistence.migration" not in imports
    assert resource_names == {"paritygrid.adapters.persistence.migrations.versions"}
    assert "HEAD_REVISION" not in source
    assert "metadata.create_all" not in source


@pytest.mark.parametrize(
    ("source", "message"),
    [
        ("revision = 'future'", "Expected revision"),
        ("revision = '0001_operational'", "missing _TABLE_STATEMENTS"),
        (
            "revision = '0001_operational'\n"
            "_TABLE_STATEMENTS = (1,)\n"
            "_INDEX_STATEMENTS = ()\n"
            "_TRIGGER_STATEMENTS = ()",
            "statement inventory",
        ),
        ("revision =", "not deterministic data"),
    ],
)
def test_revision_source_fails_closed(source: str, message: str) -> None:
    with pytest.raises(FrozenFixtureError, match=message):
        frozen_schema.build_fixture(source)


def test_revision_assignment_and_statement_groups_fail_closed() -> None:
    with pytest.raises(FrozenFixtureError, match="missing revision"):
        frozen_schema.build_fixture(
            "revision: str\n_TABLE_STATEMENTS=()\n_INDEX_STATEMENTS=()\n_TRIGGER_STATEMENTS=()"
        )
    with pytest.raises(FrozenFixtureError, match="statement inventory"):
        frozen_schema.build_fixture(
            "revision='0001_operational'\n"
            "_TABLE_STATEMENTS=[]\n_INDEX_STATEMENTS=()\n_TRIGGER_STATEMENTS=()"
        )


def test_sql_literal_preserves_null() -> None:
    assert frozen_schema._sql_literal(None) == "NULL"  # pyright: ignore[reportPrivateUsage]


def test_check_mode_is_read_only_on_success(tmp_path: Path) -> None:
    fixture_directory = tmp_path / "v0001"
    _copy_fixture(fixture_directory)
    before = _state(fixture_directory)

    assert frozen_schema.main(["--fixture-directory", str(fixture_directory)]) == 0

    assert _state(fixture_directory) == before


def test_check_mode_rejects_missing_directory(tmp_path: Path) -> None:
    assert frozen_schema.main(["--fixture-directory", str(tmp_path / "missing")]) == 1


@pytest.mark.parametrize("mutation", ["missing", "extra", "drift", "bom", "crlf", "utf8"])
def test_check_mode_rejects_file_set_encoding_and_drift(
    tmp_path: Path,
    mutation: str,
) -> None:
    fixture_directory = tmp_path / "v0001"
    _copy_fixture(fixture_directory)
    if mutation == "missing":
        (fixture_directory / "seed.sql").unlink()
    elif mutation == "extra":
        (fixture_directory / "extra.sql").write_text("SELECT 1;\n", encoding="utf-8")
    else:
        path = fixture_directory / "seed.sql"
        content = path.read_bytes()
        if mutation == "drift":
            content += b"-- changed\n"
        elif mutation == "bom":
            content = b"\xef\xbb\xbf" + content
        elif mutation == "utf8":
            content = b"\xff" + content
        else:
            content = content.replace(b"\n", b"\r\n")
        path.write_bytes(content)
    before = _state(fixture_directory)

    assert frozen_schema.main(["--fixture-directory", str(fixture_directory)]) == 1

    assert _state(fixture_directory) == before


def test_coordinated_fixture_and_manifest_tampering_fails_independent_generation(
    tmp_path: Path,
) -> None:
    fixture_directory = tmp_path / "v0001"
    _copy_fixture(fixture_directory)
    seed = fixture_directory / "seed.sql"
    manifest = fixture_directory / "manifest.json"
    seed.write_bytes(seed.read_bytes().replace(b"Harbor Lamp", b"Harbor Beam"))
    description = json.loads(manifest.read_text(encoding="utf-8"))
    description["files"]["seed.sql"]["sha256"] = hashlib.sha256(seed.read_bytes()).hexdigest()
    manifest.write_text(
        json.dumps(description, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
        newline="\n",
    )

    with pytest.raises(FrozenFixtureError, match=r"seed\.sql differs"):
        frozen_schema.verify_fixture(fixture_directory)


def test_write_mode_publishes_verified_candidate(tmp_path: Path) -> None:
    fixture_directory = tmp_path / "v0001"

    assert frozen_schema.main(["--fixture-directory", str(fixture_directory), "--write"]) == 0
    assert frozen_schema.verify_fixture(fixture_directory) == frozen_schema.build_fixture()


def test_write_mode_replaces_an_existing_fixture_atomically(tmp_path: Path) -> None:
    fixture_directory = tmp_path / "v0001"
    _copy_fixture(fixture_directory)

    frozen_schema.write_fixture(fixture_directory)

    assert frozen_schema.verify_fixture(fixture_directory) == frozen_schema.build_fixture()
    assert not (tmp_path / ".v0001-backup").exists()


def test_write_restores_previous_fixture_when_publish_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fixture_directory = tmp_path / "v0001"
    _copy_fixture(fixture_directory)
    before = _state(fixture_directory)
    original_replace = os.replace
    calls = 0

    def fail_candidate_publish(source: Path, destination: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("synthetic publish failure")
        original_replace(source, destination)

    monkeypatch.setattr(frozen_schema.os, "replace", fail_candidate_publish)

    with pytest.raises(OSError, match="synthetic publish failure"):
        frozen_schema.write_fixture(fixture_directory)

    assert _state(fixture_directory) == before
    assert not (tmp_path / ".v0001-backup").exists()


def test_write_failure_without_previous_fixture_leaves_no_partial_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fixture_directory = tmp_path / "v0001"

    def fail_publish(_source: Path, _destination: Path) -> None:
        raise OSError("synthetic initial publish failure")

    monkeypatch.setattr(frozen_schema.os, "replace", fail_publish)

    assert frozen_schema.main(["--fixture-directory", str(fixture_directory), "--write"]) == 1
    assert not fixture_directory.exists()


def test_write_rejects_preexisting_backup_without_touching_fixture(tmp_path: Path) -> None:
    fixture_directory = tmp_path / "v0001"
    _copy_fixture(fixture_directory)
    backup = tmp_path / ".v0001-backup"
    backup.mkdir()
    before = _state(fixture_directory)

    with pytest.raises(FrozenFixtureError, match="backup path"):
        frozen_schema.write_fixture(fixture_directory)

    assert _state(fixture_directory) == before


def test_write_candidate_base_exception_leaves_previous_fixture_untouched(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fixture_directory = tmp_path / "v0001"
    _copy_fixture(fixture_directory)
    before = _state(fixture_directory)

    def interrupt_candidate(directory: Path, fixture: frozen_schema.FrozenFixture) -> None:
        (directory / "schema.sql").write_bytes(fixture.schema)
        raise KeyboardInterrupt

    monkeypatch.setattr(frozen_schema, "_write_candidate", interrupt_candidate)

    with pytest.raises(KeyboardInterrupt):
        frozen_schema.write_fixture(fixture_directory)

    assert _state(fixture_directory) == before
    assert not tuple(tmp_path.glob(".v0001-*"))


@pytest.mark.parametrize("interrupt_call", [1, 2])
def test_write_recovers_whole_directory_when_rename_is_interrupted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, interrupt_call: int
) -> None:
    fixture_directory = tmp_path / "v0001"
    _copy_fixture(fixture_directory)
    (fixture_directory / "previous-release-marker.txt").write_text(
        "previous\n", encoding="utf-8", newline="\n"
    )
    previous = _state(fixture_directory)
    expected = frozen_schema.build_fixture().files()
    original_replace = os.replace
    calls = 0

    def interrupt_after_replace(source: Path, destination: Path) -> None:
        nonlocal calls
        calls += 1
        original_replace(source, destination)
        if calls == interrupt_call:
            raise KeyboardInterrupt

    monkeypatch.setattr(frozen_schema.os, "replace", interrupt_after_replace)

    with pytest.raises(KeyboardInterrupt):
        frozen_schema.write_fixture(fixture_directory)

    assert _state(fixture_directory) in (previous, expected)
    assert not (tmp_path / ".v0001-backup").exists()
    assert not tuple(path for path in tmp_path.glob(".v0001-*") if path.exists())


def test_write_keeps_complete_new_fixture_when_backup_cleanup_is_interrupted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fixture_directory = tmp_path / "v0001"
    _copy_fixture(fixture_directory)
    original_rmtree = frozen_schema.shutil.rmtree
    calls = 0

    def interrupt_cleanup(directory: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise KeyboardInterrupt
        original_rmtree(directory)

    monkeypatch.setattr(frozen_schema.shutil, "rmtree", interrupt_cleanup)

    with pytest.raises(KeyboardInterrupt):
        frozen_schema.write_fixture(fixture_directory)

    assert _state(fixture_directory) == frozen_schema.build_fixture().files()
    assert not (tmp_path / ".v0001-backup").exists()
    assert not tuple(path for path in tmp_path.glob(".v0001-*") if path.exists())


@pytest.mark.parametrize("failure_stage", ["early", "middle", "late"])
def test_reconstruction_failure_leaves_no_schema_residue(failure_stage: str) -> None:
    fixture = frozen_schema.build_fixture()
    if failure_stage == "early":
        schema = b"CREATE TABLE early(value INTEGER);\nBROKEN STATEMENT;\n"
        seed = b"SELECT 1;\n"
    elif failure_stage == "middle":
        schema = fixture.schema.replace(
            b"CREATE INDEX ix_audit_entries_correlation_id",
            b"CREATE BROKEN ix_audit_entries_correlation_id",
        )
        seed = fixture.seed
    else:
        schema = fixture.schema
        seed = fixture.seed.replace(
            b"COMMIT;\n", b"INSERT INTO missing_table(value) VALUES (1);\nCOMMIT;\n"
        )
    connection = sqlite3.connect(":memory:")
    try:
        with pytest.raises(sqlite3.Error):
            frozen_schema.reconstruct_fixture(connection, schema, seed)
        objects = connection.execute(
            "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
        ).fetchall()
        assert objects == []
        assert connection.in_transaction is False
        assert connection.execute("PRAGMA foreign_keys").fetchone() == (1,)
    finally:
        connection.close()


def test_reconstruction_rejects_incomplete_sql_before_transaction() -> None:
    connection = sqlite3.connect(":memory:")
    try:
        with pytest.raises(FrozenFixtureError, match="incomplete statement"):
            frozen_schema.reconstruct_fixture(
                connection, b"CREATE TABLE broken (\n", b"SELECT 1;\n"
            )
        assert connection.in_transaction is False
    finally:
        connection.close()


def test_reconstruction_rolls_back_base_exception() -> None:
    class Rows:
        def fetchall(self) -> list[tuple[str]]:
            return []

    class InterruptingConnection:
        rolled_back = False

        def execute(self, statement: str) -> Rows:
            if statement == "SELECT 2;":
                raise KeyboardInterrupt
            return Rows()

        def rollback(self) -> None:
            self.rolled_back = True

    connection = InterruptingConnection()
    with pytest.raises(KeyboardInterrupt):
        frozen_schema.reconstruct_fixture(
            cast(sqlite3.Connection, connection), b"SELECT 1;\n", b"SELECT 2;\n"
        )

    assert connection.rolled_back is True


@pytest.mark.parametrize(("failure", "message"), [("quick", "quick check"), ("fk", "foreign-key")])
def test_reconstruction_rejects_integrity_failures(failure: str, message: str) -> None:
    class Rows:
        def __init__(self, values: list[tuple[object, ...]]) -> None:
            self.values = values

        def fetchall(self) -> list[tuple[object, ...]]:
            return self.values

    class IntegrityConnection:
        rolled_back = False

        def execute(self, statement: str) -> Rows:
            if statement == "PRAGMA quick_check":
                return Rows([("corrupt",)] if failure == "quick" else [("ok",)])
            if statement == "PRAGMA foreign_key_check" and failure == "fk":
                return Rows([("child", 1, "parent", 0)])
            return Rows([])

        def commit(self) -> None:
            raise AssertionError("Integrity failures must not commit.")

        def rollback(self) -> None:
            self.rolled_back = True

    connection = IntegrityConnection()
    with pytest.raises(FrozenFixtureError, match=message):
        frozen_schema.reconstruct_fixture(
            cast(sqlite3.Connection, connection), b"SELECT 1;\n", b"SELECT 2;\n"
        )

    assert connection.rolled_back is True


def test_manifest_generation_rejects_missing_fk_witness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_rows = tuple(
        row
        for row in frozen_schema._SEED_ROWS  # pyright: ignore[reportPrivateUsage]
        if row[0] != "connector_secret_references"
    )
    monkeypatch.setattr(frozen_schema, "_SEED_ROWS", seed_rows)

    with pytest.raises(FrozenFixtureError, match="no witness"):
        frozen_schema.build_fixture()


def test_manifest_generation_rejects_cross_row_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_rows = tuple(
        (
            table,
            {**values, "next_sequence_number": 5}
            if table == "run_event_counters" and values["run_id"] == "run_completed-demo"
            else values,
        )
        for table, values in frozen_schema._SEED_ROWS  # pyright: ignore[reportPrivateUsage]
    )
    monkeypatch.setattr(frozen_schema, "_SEED_ROWS", seed_rows)

    with pytest.raises(FrozenFixtureError, match="cross-row invariant"):
        frozen_schema.build_fixture()


def test_manifest_generation_rejects_schema_inventory_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(frozen_schema, "_EXPECTED_SCHEMA_INVENTORY", {})

    with pytest.raises(FrozenFixtureError, match="reviewed v0001 schema inventory"):
        frozen_schema.build_fixture()


def test_schema_inventory_rejects_missing_table_sql() -> None:
    connection = sqlite3.connect(":memory:")
    try:
        with pytest.raises(FrozenFixtureError, match="missing table SQL"):
            frozen_schema._schema_inventory(  # pyright: ignore[reportPrivateUsage]
                connection, ["missing_table"]
            )
    finally:
        connection.close()


def test_manifest_generation_maps_sqlite_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_reconstruction(_connection: sqlite3.Connection, _schema: bytes, _seed: bytes) -> None:
        raise sqlite3.DatabaseError("synthetic database failure")

    monkeypatch.setattr(frozen_schema, "reconstruct_fixture", fail_reconstruction)

    with pytest.raises(FrozenFixtureError, match="cannot be reconstructed"):
        frozen_schema.build_fixture()


def test_cleanup_propagates_interruption_after_directory_was_removed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    directory = tmp_path / "cleanup"
    directory.mkdir()
    original_rmtree = frozen_schema.shutil.rmtree

    def remove_then_interrupt(path: Path) -> None:
        original_rmtree(path)
        raise KeyboardInterrupt

    monkeypatch.setattr(frozen_schema.shutil, "rmtree", remove_then_interrupt)

    with pytest.raises(KeyboardInterrupt):
        frozen_schema._remove_tree(directory)  # pyright: ignore[reportPrivateUsage]

    assert not directory.exists()


def test_command_generates_and_checks_from_arbitrary_unicode_directory(
    tmp_path: Path,
) -> None:
    external = tmp_path / "outside 100%-Café-Cafe\u0301-عربي"
    external.mkdir()
    generated: list[dict[str, bytes]] = []
    for index, hash_seed in enumerate(("1", "8675309"), start=1):
        fixture_directory = external / f"released fixture {index}"
        command = [
            sys.executable,
            "-c",
            "from paritygrid.quality.frozen_schema import main; raise SystemExit(main())",
            "--fixture-directory",
            str(fixture_directory),
        ]
        environment = {**os.environ, "PYTHONHASHSEED": hash_seed}
        write = subprocess.run(
            [*command, "--write"],
            cwd=external,
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        check = subprocess.run(
            command,
            cwd=external,
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        assert write.returncode == 0, write.stderr
        assert check.returncode == 0, check.stderr
        assert EXPECTED_SCHEMA_HASH in check.stdout
        generated.append(_state(fixture_directory))

    assert generated[0] == generated[1] == _state(FIXTURE_DIRECTORY)
    assert set(generated[0]) == {
        "manifest.json",
        "schema.sql",
        "seed.sql",
    }


def test_wheel_command_uses_explicit_external_fixture_directory(tmp_path: Path) -> None:
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
    assert "paritygrid/quality/frozen_schema.py" in names
    assert not any("fixtures/persistence/v0001" in name for name in names)

    external = tmp_path / "installed command 100%-Café-عربي"
    external.mkdir()
    missing = external / "missing"
    generated = external / "generated"
    source = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from paritygrid.quality.frozen_schema import main
missing = Path(sys.argv[2])
generated = Path(sys.argv[3])
assert main(["--fixture-directory", str(missing)]) == 1
assert not missing.exists()
assert main(["--fixture-directory", str(generated), "--write"]) == 0
assert main(["--fixture-directory", str(generated)]) == 0
"""
    result = subprocess.run(
        [sys.executable, "-c", source, str(wheel), str(missing), str(generated)],
        cwd=external,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert not tuple(external.glob("*.db*"))
