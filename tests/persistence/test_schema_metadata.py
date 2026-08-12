"""Structural acceptance tests for the authoritative relational metadata."""

import ast
import importlib
import os
import subprocess
import sys
from pathlib import Path
from typing import cast

from sqlalchemy import CheckConstraint, ForeignKeyConstraint, Index, PrimaryKeyConstraint, Table
from sqlalchemy import UniqueConstraint as SQLUniqueConstraint
from sqlalchemy.dialects import sqlite
from sqlalchemy.schema import CreateIndex, CreateTable

from paritygrid.adapters import persistence
from paritygrid.adapters.persistence.schema import (
    OPERATIONAL_TABLE_NAMES,
    ORM_ROW_TYPES,
    metadata,
)

EXPECTED_TABLES = {
    "system_metadata",
    "pipelines",
    "pipeline_versions",
    "connectors",
    "connector_secret_references",
    "runs",
    "run_event_counters",
    "run_nodes",
    "work_items",
    "work_attempts",
    "artifact_manifests",
    "checkpoint_heads",
    "checkpoints",
    "execution_events",
    "idempotency_records",
    "reconciliation_summaries",
    "reconciliation_conflicts",
    "repair_plans",
    "repair_approvals",
    "repair_actions",
    "audit_entries",
}


def _compiled_schema() -> str:
    dialect = sqlite.dialect()
    statements: list[str] = []
    for table in metadata.sorted_tables:
        statements.append(str(CreateTable(table).compile(dialect=dialect)))
        statements.extend(
            str(CreateIndex(index).compile(dialect=dialect))
            for index in sorted(table.indexes, key=lambda item: item.name or "")
        )
    return "\n".join(statements)


def test_metadata_contains_exact_operational_table_inventory() -> None:
    assert set(OPERATIONAL_TABLE_NAMES) == EXPECTED_TABLES
    assert set(metadata.tables) == EXPECTED_TABLES
    assert {cast(Table, row_type.__table__).name for row_type in ORM_ROW_TYPES} == EXPECTED_TABLES


def test_every_schema_object_has_an_explicit_stable_name() -> None:
    seen: set[str] = set()
    for table in metadata.tables.values():
        for schema_object in (*table.constraints, *table.indexes):
            name = schema_object.name
            assert isinstance(name, str)
            assert name not in seen
            seen.add(name)
            if isinstance(schema_object, PrimaryKeyConstraint):
                assert name == f"pk_{table.name}"
            elif isinstance(schema_object, CheckConstraint):
                assert name.startswith(f"ck_{table.name}_")
            elif isinstance(schema_object, ForeignKeyConstraint):
                assert name.startswith(f"fk_{table.name}_")
            elif isinstance(schema_object, SQLUniqueConstraint):
                assert name.startswith(f"uq_{table.name}_")
            elif isinstance(schema_object, Index):
                assert name.startswith(f"ix_{table.name}_")


def test_foreign_keys_never_cascade_or_nullify_operational_history() -> None:
    for table in metadata.tables.values():
        for constraint in table.foreign_key_constraints:
            assert constraint.ondelete is None
            assert constraint.onupdate is None


def test_every_foreign_key_has_a_supporting_child_index() -> None:
    for table in metadata.tables.values():
        indexed_prefixes = [
            tuple(column.name for column in constraint.columns)
            for constraint in table.constraints
            if isinstance(constraint, (PrimaryKeyConstraint, SQLUniqueConstraint))
        ]
        indexed_prefixes.extend(
            tuple(column.name for column in index.columns) for index in table.indexes
        )
        for constraint in table.foreign_key_constraints:
            child_columns = tuple(column.name for column in constraint.columns)
            assert any(
                columns[: len(child_columns)] == child_columns for columns in indexed_prefixes
            ), f"{table.name} foreign key {constraint.name} lacks a supporting child index"


def test_every_foreign_key_targets_an_exact_unique_parent_key() -> None:
    for table in metadata.tables.values():
        for constraint in table.foreign_key_constraints:
            parent = constraint.referred_table
            parent_columns = tuple(element.column.name for element in constraint.elements)
            unique_parent_keys = [
                tuple(column.name for column in parent_constraint.columns)
                for parent_constraint in parent.constraints
                if isinstance(parent_constraint, (PrimaryKeyConstraint, SQLUniqueConstraint))
            ]
            unique_parent_keys.extend(
                tuple(column.name for column in index.columns)
                for index in parent.indexes
                if index.unique
            )
            assert parent_columns in unique_parent_keys, (
                f"{table.name} foreign key {constraint.name} does not target an exact unique key"
            )


def test_sqlite_schema_compiles_deterministically() -> None:
    first = _compiled_schema()
    second = _compiled_schema()
    assert first == second
    assert "CREATE TABLE work_items" in first
    assert "CREATE INDEX ix_work_items_run_id_state_retry_available_at" in first
    assert "ON DELETE" not in first


def test_schema_import_does_not_create_runtime_files(tmp_path: Path) -> None:
    before = set(tmp_path.iterdir())
    previous = Path.cwd()
    os.chdir(tmp_path)
    try:
        importlib.import_module("paritygrid.adapters.persistence.schema")
    finally:
        os.chdir(previous)
    assert set(tmp_path.iterdir()) == before


def test_fresh_mapping_import_and_configuration_have_no_file_side_effects(tmp_path: Path) -> None:
    source = """
from pathlib import Path
from sqlalchemy.orm import configure_mappers
from paritygrid.adapters.persistence.schema import OPERATIONAL_TABLE_NAMES

before = tuple(sorted(path.name for path in Path.cwd().iterdir()))
configure_mappers()
after = tuple(sorted(path.name for path in Path.cwd().iterdir()))
assert len(OPERATIONAL_TABLE_NAMES) == 21
assert after == before
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", source],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert tuple(tmp_path.iterdir()) == ()


def test_schema_dependency_direction_stays_inside_adapter_boundary() -> None:
    source_path = Path(__file__).parents[2] / "src/paritygrid/adapters/persistence/schema.py"
    module = ast.parse(source_path.read_text(encoding="utf-8"))
    imported = {
        node.module
        for node in ast.walk(module)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    assert not any(
        name.startswith(
            (
                "paritygrid.application",
                "paritygrid.api",
                "paritygrid.runtime",
            )
        )
        for name in imported
    )


def test_public_persistence_boundary_does_not_export_orm_rows_or_metadata() -> None:
    exported = set(persistence.__all__)
    assert "Base" not in exported
    assert "metadata" not in exported
    assert not any(name.endswith("Row") for name in exported)
