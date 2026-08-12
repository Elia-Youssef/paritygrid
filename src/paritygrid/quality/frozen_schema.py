"""Generate and verify the released v0001 persistence fixture."""

import argparse
import ast
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import TypeGuard, cast

TARGET_REVISION = "0001_operational"
REVISION_RESOURCE = "0001_operational.py"
DEFAULT_FIXTURE_DIRECTORY = Path("tests/fixtures/persistence/v0001")
_FIXTURE_FILENAMES = ("schema.sql", "seed.sql", "manifest.json")
_SCHEMA_GROUPS = ("_TABLE_STATEMENTS", "_INDEX_STATEMENTS", "_TRIGGER_STATEMENTS")
_TIMESTAMP = "2026-08-12T12:00:00.000000Z"
_TIMESTAMP_LATER = "2026-08-12T12:05:00.000000Z"
_TIMESTAMP_LATEST = "2026-08-12T12:10:00.000000Z"

Scalar = str | int | None
SeedValues = Mapping[str, Scalar]


class FrozenFixtureError(RuntimeError):
    """The released fixture cannot be generated or verified safely."""


@dataclass(frozen=True, slots=True)
class FrozenFixture:
    """Deterministic bytes for one released persistence fixture."""

    schema: bytes
    seed: bytes
    manifest: bytes

    def files(self) -> Mapping[str, bytes]:
        """Return fixture bytes keyed by their stable filenames."""
        return {"schema.sql": self.schema, "seed.sql": self.seed, "manifest.json": self.manifest}


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


_SEED_ROWS: tuple[tuple[str, SeedValues], ...] = (
    (
        "system_metadata",
        {"key": "fixture_revision", "value": TARGET_REVISION, "updated_at": _TIMESTAMP},
    ),
    (
        "system_metadata",
        {"key": "instance_identity", "value": "fixture-instance-v0001", "updated_at": _TIMESTAMP},
    ),
    (
        "pipelines",
        {
            "pipeline_id": "pip_inventory-demo",
            "display_name": "Harbor Inventory Reconciliation",
            "description": "Synthetic inventory comparison and non-destructive repair flow.",
            "created_at": _TIMESTAMP,
            "row_version": 1,
        },
    ),
    (
        "pipeline_versions",
        {
            "pipeline_id": "pip_inventory-demo",
            "version_number": 1,
            "specification_json": _json(
                {
                    "connectors": ["con_async-source", "con_warehouse-target"],
                    "nodes": ["nod_inventory-sync"],
                    "scenario": "harbor-inventory",
                }
            ),
            "specification_sha256": "1" * 64,
            "planner_format_version": 1,
            "published_at": _TIMESTAMP,
        },
    ),
    (
        "connectors",
        {
            "connector_id": "con_async-source",
            "kind": "synthetic_async_http",
            "display_name": "Harbor Catalog Source",
            "configuration_json": _json(
                {"base_url": "http://127.0.0.1:8765", "secret_reference": "api_token"}
            ),
            "capabilities_json": _json({"pagination": True, "read": True}),
            "schema_discovery_json": _json({"version": 1}),
            "revision": 1,
            "created_at": _TIMESTAMP,
            "updated_at": _TIMESTAMP,
            "row_version": 1,
        },
    ),
    (
        "connectors",
        {
            "connector_id": "con_warehouse-target",
            "kind": "synthetic_warehouse",
            "display_name": "Harbor Warehouse Target",
            "configuration_json": _json({"warehouse": "north-dock"}),
            "capabilities_json": _json({"read": True, "upsert": True}),
            "revision": 1,
            "created_at": _TIMESTAMP,
            "updated_at": _TIMESTAMP,
            "row_version": 1,
        },
    ),
    (
        "connector_secret_references",
        {
            "connector_id": "con_async-source",
            "reference_name": "api_token",
            "environment_variable_name": "PARITYGRID_DEMO_API_TOKEN",
            "created_at": _TIMESTAMP,
        },
    ),
    (
        "runs",
        {
            "run_id": "run_completed-demo",
            "pipeline_id": "pip_inventory-demo",
            "pipeline_version_number": 1,
            "runner_kind": "sequential",
            "runner_configuration_json": _json({"concurrency": 1}),
            "state": "succeeded",
            "row_version": 4,
            "scenario_seed": 1403,
            "created_at": _TIMESTAMP,
            "started_at": _TIMESTAMP,
            "finished_at": _TIMESTAMP_LATEST,
            "final_reconciliation_fingerprint": "4" * 64,
        },
    ),
    (
        "runs",
        {
            "run_id": "run_active-demo",
            "pipeline_id": "pip_inventory-demo",
            "pipeline_version_number": 1,
            "runner_kind": "threaded",
            "runner_configuration_json": _json({"max_workers": 2}),
            "state": "running",
            "row_version": 2,
            "scenario_seed": 1404,
            "created_at": _TIMESTAMP,
            "started_at": _TIMESTAMP_LATER,
        },
    ),
    (
        "run_event_counters",
        {"run_id": "run_completed-demo", "next_sequence_number": 4, "row_version": 4},
    ),
    (
        "run_event_counters",
        {"run_id": "run_active-demo", "next_sequence_number": 2, "row_version": 2},
    ),
    (
        "run_nodes",
        {
            "run_id": "run_completed-demo",
            "node_id": "nod_inventory-sync",
            "state": "succeeded",
            "row_version": 3,
            "work_total": 1,
            "work_succeeded": 1,
            "records_read": 12,
            "records_written": 11,
            "records_quarantined": 1,
            "bytes_read": 2048,
            "bytes_written": 1800,
            "retry_count": 1,
            "duration_microseconds": 600000000,
            "started_at": _TIMESTAMP,
            "finished_at": _TIMESTAMP_LATEST,
        },
    ),
    (
        "run_nodes",
        {
            "run_id": "run_active-demo",
            "node_id": "nod_inventory-sync",
            "state": "running",
            "row_version": 2,
            "work_total": 1,
            "work_running": 1,
            "started_at": _TIMESTAMP_LATER,
        },
    ),
    (
        "work_items",
        {
            "work_item_id": "wrk_completed-partition",
            "run_id": "run_completed-demo",
            "node_id": "nod_inventory-sync",
            "partition_key": "catalog-page-0001",
            "state": "succeeded",
            "row_version": 4,
            "completed_attempt_count": 2,
            "expected_checkpoint_version": 2,
            "input_reference_json": _json({"page": 1}),
            "created_at": _TIMESTAMP,
            "updated_at": _TIMESTAMP_LATEST,
        },
    ),
    (
        "work_items",
        {
            "work_item_id": "wrk_active-partition",
            "run_id": "run_active-demo",
            "node_id": "nod_inventory-sync",
            "partition_key": "catalog-page-0002",
            "state": "running",
            "row_version": 2,
            "completed_attempt_count": 0,
            "expected_checkpoint_version": 0,
            "input_reference_json": _json({"page": 2}),
            "lease_owner": "threaded-runner",
            "lease_expires_at": _TIMESTAMP_LATEST,
            "active_attempt_number": 1,
            "active_attempt_started_at": _TIMESTAMP_LATER,
            "active_runner_kind": "threaded",
            "active_worker_identity": "worker-01",
            "created_at": _TIMESTAMP,
            "updated_at": _TIMESTAMP_LATER,
        },
    ),
    (
        "work_attempts",
        {
            "work_item_id": "wrk_completed-partition",
            "attempt_number": 1,
            "started_at": _TIMESTAMP,
            "finished_at": _TIMESTAMP_LATER,
            "runner_kind": "sequential",
            "worker_identity": "reference-runner",
            "outcome": "retry_scheduled",
            "failure_classification": "http_429",
            "redacted_detail": "Synthetic rate limit response.",
            "records_processed": 0,
            "bytes_processed": 0,
            "duration_microseconds": 300000000,
        },
    ),
    (
        "work_attempts",
        {
            "work_item_id": "wrk_completed-partition",
            "attempt_number": 2,
            "started_at": _TIMESTAMP_LATER,
            "finished_at": _TIMESTAMP_LATEST,
            "runner_kind": "sequential",
            "worker_identity": "reference-runner",
            "outcome": "succeeded",
            "result_reference_json": _json({"artifact_id": "art_inventory-output"}),
            "records_processed": 12,
            "bytes_processed": 2048,
            "duration_microseconds": 300000000,
        },
    ),
    (
        "artifact_manifests",
        {
            "artifact_id": "art_inventory-output",
            "run_id": "run_completed-demo",
            "node_id": "nod_inventory-sync",
            "partition_key": "catalog-page-0001",
            "relative_path": "runs/run_completed-demo/normalized/catalog-page-0001.parquet",
            "media_type": "application/vnd.apache.parquet",
            "schema_version": 1,
            "byte_size": 1800,
            "row_count": 11,
            "sha256": "2" * 64,
            "created_at": _TIMESTAMP_LATEST,
        },
    ),
    (
        "checkpoint_heads",
        {
            "run_id": "run_completed-demo",
            "node_id": "nod_inventory-sync",
            "partition_key": "catalog-page-0001",
            "current_version": 2,
            "updated_at": _TIMESTAMP_LATEST,
            "row_version": 3,
        },
    ),
    (
        "checkpoint_heads",
        {
            "run_id": "run_active-demo",
            "node_id": "nod_inventory-sync",
            "partition_key": "catalog-page-0002",
            "current_version": 0,
            "updated_at": _TIMESTAMP_LATER,
            "row_version": 1,
        },
    ),
    (
        "checkpoints",
        {
            "run_id": "run_completed-demo",
            "node_id": "nod_inventory-sync",
            "partition_key": "catalog-page-0001",
            "version": 1,
            "payload_schema_version": 1,
            "source_cursor_json": _json({"offset": 6}),
            "output_position_json": _json({"rows": 6}),
            "committed_at": _TIMESTAMP_LATER,
        },
    ),
    (
        "checkpoints",
        {
            "run_id": "run_completed-demo",
            "node_id": "nod_inventory-sync",
            "partition_key": "catalog-page-0001",
            "version": 2,
            "payload_schema_version": 1,
            "source_cursor_json": _json({"offset": 12}),
            "output_position_json": _json({"rows": 11}),
            "artifact_id": "art_inventory-output",
            "committed_at": _TIMESTAMP_LATEST,
        },
    ),
    (
        "execution_events",
        {
            "run_id": "run_completed-demo",
            "sequence_number": 1,
            "event_kind": "run_started",
            "occurred_at": _TIMESTAMP,
            "subject_kind": "run",
            "subject_id": "run_completed-demo",
            "correlation_id": "corr-completed-demo",
            "payload_schema_version": 1,
            "payload_json": _json({"runner": "sequential"}),
        },
    ),
    (
        "execution_events",
        {
            "run_id": "run_completed-demo",
            "sequence_number": 2,
            "event_kind": "checkpoint_committed",
            "occurred_at": _TIMESTAMP_LATER,
            "subject_kind": "work_item",
            "subject_id": "wrk_completed-partition",
            "correlation_id": "corr-completed-demo",
            "payload_schema_version": 1,
            "payload_json": _json({"version": 1}),
        },
    ),
    (
        "execution_events",
        {
            "run_id": "run_completed-demo",
            "sequence_number": 3,
            "event_kind": "run_succeeded",
            "occurred_at": _TIMESTAMP_LATEST,
            "subject_kind": "run",
            "subject_id": "run_completed-demo",
            "correlation_id": "corr-completed-demo",
            "payload_schema_version": 1,
            "payload_json": _json({"fingerprint": "4" * 64}),
        },
    ),
    (
        "execution_events",
        {
            "run_id": "run_active-demo",
            "sequence_number": 1,
            "event_kind": "work_started",
            "occurred_at": _TIMESTAMP_LATER,
            "subject_kind": "work_item",
            "subject_id": "wrk_active-partition",
            "correlation_id": "corr-active-demo",
            "payload_schema_version": 1,
            "payload_json": _json({"attempt": 1}),
        },
    ),
    (
        "idempotency_records",
        {
            "scope": "run:create",
            "idempotency_key": "fixture-run-completed",
            "request_sha256": "3" * 64,
            "status": "completed",
            "response_schema_version": 1,
            "response_json": _json({"run_id": "run_completed-demo"}),
            "created_at": _TIMESTAMP,
            "updated_at": _TIMESTAMP_LATEST,
            "completed_at": _TIMESTAMP_LATEST,
        },
    ),
    (
        "reconciliation_summaries",
        {
            "run_id": "run_completed-demo",
            "match_count": 10,
            "missing_from_target_count": 1,
            "missing_from_source_count": 0,
            "field_mismatch_count": 1,
            "duplicate_source_count": 0,
            "duplicate_target_count": 0,
            "duplicate_both_count": 0,
            "total_count": 12,
            "source_fingerprint": "5" * 64,
            "target_fingerprint": "6" * 64,
            "reconciliation_fingerprint": "4" * 64,
            "analytical_query_version": 1,
            "created_at": _TIMESTAMP_LATEST,
        },
    ),
    (
        "reconciliation_conflicts",
        {
            "conflict_id": "cnf_missing-widget",
            "run_id": "run_completed-demo",
            "canonical_key": "sku-harbor-lamp",
            "classification": "missing_from_target",
            "source_references_json": _json([{"connector_id": "con_async-source", "row": 7}]),
            "field_differences_json": _json([{"field": "record", "target": None}]),
            "suggested_resolution": "create_target",
            "created_at": _TIMESTAMP_LATEST,
        },
    ),
    (
        "repair_plans",
        {
            "repair_plan_id": "rpl_harbor-repair",
            "run_id": "run_completed-demo",
            "reconciliation_fingerprint": "4" * 64,
            "content_fingerprint": "7" * 64,
            "status": "applied",
            "row_version": 4,
            "created_at": _TIMESTAMP_LATEST,
            "applying_at": _TIMESTAMP_LATEST,
            "applied_at": _TIMESTAMP_LATEST,
        },
    ),
    (
        "repair_approvals",
        {
            "repair_plan_id": "rpl_harbor-repair",
            "reconciliation_fingerprint": "4" * 64,
            "approved_by": "fixture-operator",
            "approved_at": _TIMESTAMP_LATEST,
            "correlation_id": "corr-repair-demo",
            "approval_schema_version": 1,
            "detail_json": _json({"reason": "Reviewed synthetic mismatch evidence."}),
        },
    ),
    (
        "repair_actions",
        {
            "repair_action_id": "rac_create-harbor-lamp",
            "repair_plan_id": "rpl_harbor-repair",
            "run_id": "run_completed-demo",
            "conflict_id": "cnf_missing-widget",
            "canonical_key": "sku-harbor-lamp",
            "action_kind": "create_target",
            "external_idempotency_key": "repair-harbor-lamp-v1",
            "proposed_after_sha256": "8" * 64,
            "proposed_record_json": _json(
                {"currency": "USD", "name": "Harbor Lamp", "price_minor": 4599, "sku": "HL-1"}
            ),
            "mismatch_evidence_json": _json([{"classification": "missing_from_target"}]),
            "application_status": "applied",
            "application_result_json": _json({"effect": "created"}),
            "target_version": 1,
            "applied_at": _TIMESTAMP_LATEST,
        },
    ),
    (
        "audit_entries",
        {
            "sequence_number": 1,
            "actor": "fixture-operator",
            "operation": "repair_plan_applied",
            "object_kind": "repair_plan",
            "object_id": "rpl_harbor-repair",
            "correlation_id": "corr-repair-demo",
            "occurred_at": _TIMESTAMP_LATEST,
            "detail_schema_version": 1,
            "detail_json": _json({"action_count": 1, "synthetic": True}),
        },
    ),
)


def _revision_text() -> str:
    resource = files("paritygrid.adapters.persistence.migrations.versions").joinpath(
        REVISION_RESOURCE
    )
    return resource.read_text(encoding="utf-8")


def _assignment(tree: ast.Module, name: str) -> object:
    for statement in tree.body:
        if isinstance(statement, ast.Assign | ast.AnnAssign):
            targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target]
            if any(isinstance(target, ast.Name) and target.id == name for target in targets):
                if statement.value is None:
                    break
                return ast.literal_eval(statement.value)
    raise FrozenFixtureError(f"The frozen migration is missing {name}.")


def _is_statement_group(value: object) -> TypeGuard[tuple[str, ...]]:
    if not isinstance(value, tuple):
        return False
    values = cast(tuple[object, ...], value)
    return all(isinstance(statement, str) for statement in values)


def _revision_statements(source: str) -> tuple[str, ...]:
    try:
        tree = ast.parse(source, filename=REVISION_RESOURCE)
        revision = _assignment(tree, "revision")
    except (SyntaxError, ValueError) as error:
        raise FrozenFixtureError(
            "The frozen migration source is not deterministic data."
        ) from error
    if revision != TARGET_REVISION:
        raise FrozenFixtureError(f"Expected revision {TARGET_REVISION!r}, received {revision!r}.")
    raw_groups = tuple(_assignment(tree, name) for name in _SCHEMA_GROUPS)
    statements: list[str] = []
    for group in raw_groups:
        if not _is_statement_group(group):
            raise FrozenFixtureError("The frozen migration statement inventory is invalid.")
        statements.extend(group)
    return tuple(statements)


def _sql_literal(value: Scalar) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, int):
        return str(value)
    return "'" + value.replace("'", "''") + "'"


def _insert_statement(table: str, values: SeedValues) -> str:
    columns = ", ".join(f'"{column}"' for column in values)
    literals = ", ".join(_sql_literal(value) for value in values.values())
    return f'INSERT INTO "{table}" ({columns}) VALUES ({literals});'


def _schema_text(revision_source: str) -> str:
    statements = _revision_statements(revision_source)
    lines = [
        "PRAGMA foreign_keys = ON;",
        "BEGIN IMMEDIATE;",
        *(statement.rstrip(";") + ";" for statement in statements),
        "CREATE TABLE alembic_version (",
        "    version_num VARCHAR(32) NOT NULL,",
        "    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)",
        ");",
        f"INSERT INTO alembic_version (version_num) VALUES ('{TARGET_REVISION}');",
        "COMMIT;",
    ]
    return "\n".join(lines) + "\n"


def _seed_text() -> str:
    lines = [
        "PRAGMA foreign_keys = ON;",
        "BEGIN IMMEDIATE;",
        *(_insert_statement(table, values) for table, values in _SEED_ROWS),
        "COMMIT;",
    ]
    return "\n".join(lines) + "\n"


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _complete_statements(content: bytes) -> tuple[str, ...]:
    _validate_text_file("fixture SQL", content)
    script = content.decode("utf-8")
    statements: list[str] = []
    buffer = ""
    for line in script.splitlines(keepends=True):
        buffer += line
        if sqlite3.complete_statement(buffer):
            statement = buffer.strip()
            buffer = ""
            if statement.upper() not in {
                "PRAGMA FOREIGN_KEYS = ON;",
                "BEGIN IMMEDIATE;",
                "COMMIT;",
            }:
                statements.append(statement)
    if buffer.strip():
        raise FrozenFixtureError("Fixture SQL ends with an incomplete statement.")
    return tuple(statements)


def reconstruct_fixture(connection: sqlite3.Connection, schema: bytes, seed: bytes) -> None:
    """Reconstruct both fixture scripts in one explicit atomic transaction."""
    statements = (*_complete_statements(schema), *_complete_statements(seed))
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("BEGIN IMMEDIATE")
    try:
        for statement in statements:
            connection.execute(statement)
        if connection.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
            raise FrozenFixtureError("The generated fixture fails SQLite quick check.")
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            raise FrozenFixtureError("The generated fixture fails SQLite foreign-key checks.")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


def _logical_manifest(schema: bytes, seed: bytes) -> dict[str, object]:
    connection = sqlite3.connect(":memory:")
    try:
        reconstruct_fixture(connection, schema, seed)
        table_names = [
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name NOT LIKE 'sqlite_%' AND name <> 'alembic_version' ORDER BY name"
            )
        ]
        tables: dict[str, dict[str, object]] = {}
        for table in table_names:
            column_rows = connection.execute(f'PRAGMA table_info("{table}")').fetchall()
            columns = [str(row[1]) for row in column_rows]
            primary_key_columns = [
                str(row[1]) for row in sorted(column_rows, key=lambda row: int(row[5])) if row[5]
            ]
            order_columns = primary_key_columns or columns
            order_sql = ", ".join(f'"{column}"' for column in order_columns)
            rows = [
                list(row)
                for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY {order_sql}')
            ]
            primary_keys = [
                [row[columns.index(column)] for column in primary_key_columns] for row in rows
            ]
            tables[table] = {
                "columns": columns,
                "primary_key_columns": primary_key_columns,
                "primary_keys_sha256": _sha256(_json(primary_keys).encode("ascii")),
                "row_count": len(rows),
                "rows_sha256": _sha256(_json(rows).encode("ascii")),
            }
        foreign_key_witnesses: list[dict[str, object]] = []
        for table in table_names:
            foreign_keys = connection.execute(f'PRAGMA foreign_key_list("{table}")').fetchall()
            for foreign_key_id in sorted({int(row[0]) for row in foreign_keys}):
                definition = [row for row in foreign_keys if int(row[0]) == foreign_key_id]
                columns = [str(row[3]) for row in definition]
                predicate = " AND ".join(f'"{column}" IS NOT NULL' for column in columns)
                non_null_rows = int(
                    connection.execute(
                        f'SELECT COUNT(*) FROM "{table}" WHERE {predicate}'
                    ).fetchone()[0]
                )
                if non_null_rows < 1:
                    raise FrozenFixtureError(
                        f"The generated fixture has no witness for {table} "
                        f"foreign key {foreign_key_id}."
                    )
                foreign_key_witnesses.append(
                    {
                        "columns": columns,
                        "foreign_key_id": foreign_key_id,
                        "non_null_rows": non_null_rows,
                        "table": table,
                    }
                )
        invariants = {
            "checkpoint_head_matches_history": not connection.execute(
                "SELECT 1 FROM checkpoint_heads h WHERE h.current_version <> "
                "COALESCE((SELECT MAX(c.version) FROM checkpoints c WHERE c.run_id=h.run_id "
                "AND c.node_id=h.node_id AND c.partition_key=h.partition_key),0)"
            ).fetchall(),
            "event_counter_matches_history": not connection.execute(
                "SELECT 1 FROM run_event_counters e WHERE e.next_sequence_number <> "
                "COALESCE((SELECT MAX(x.sequence_number)+1 FROM execution_events x "
                "WHERE x.run_id=e.run_id),1)"
            ).fetchall(),
            "repair_approval_and_action_match_plan": connection.execute(
                "SELECT COUNT(*) FROM repair_plans p JOIN repair_approvals a "
                "USING (repair_plan_id,reconciliation_fingerprint) "
                "JOIN repair_actions r USING (repair_plan_id,run_id) "
                "WHERE p.status='applied' AND r.application_status='applied'"
            ).fetchone()[0]
            == 1,
        }
        if not all(invariants.values()):
            raise FrozenFixtureError("The generated fixture violates a cross-row invariant.")
        return {
            "format_version": 1,
            "revision": TARGET_REVISION,
            "files": {
                "schema.sql": {"bytes": len(schema), "sha256": _sha256(schema)},
                "seed.sql": {"bytes": len(seed), "sha256": _sha256(seed)},
            },
            "foreign_key_witnesses": foreign_key_witnesses,
            "invariants": invariants,
            "table_count": len(table_names),
            "tables": tables,
            "logical_rows_sha256": _sha256(
                _json([[name, tables[name]["rows_sha256"]] for name in table_names]).encode("ascii")
            ),
        }
    except sqlite3.Error as error:
        raise FrozenFixtureError("The generated fixture cannot be reconstructed.") from error
    finally:
        connection.close()


def build_fixture(revision_source: str | None = None) -> FrozenFixture:
    """Build deterministic v0001 schema, seed, and manifest bytes."""
    schema = _schema_text(
        revision_source if revision_source is not None else _revision_text()
    ).encode("utf-8")
    seed = _seed_text().encode("utf-8")
    manifest = (_json(_logical_manifest(schema, seed)) + "\n").encode("utf-8")
    return FrozenFixture(schema=schema, seed=seed, manifest=manifest)


def _validate_text_file(name: str, content: bytes) -> None:
    if content.startswith(b"\xef\xbb\xbf"):
        raise FrozenFixtureError(f"{name} must not contain a byte-order mark.")
    try:
        content.decode("utf-8")
    except UnicodeDecodeError as error:
        raise FrozenFixtureError(f"{name} must be valid UTF-8.") from error
    if b"\r" in content or not content.endswith(b"\n"):
        raise FrozenFixtureError(f"{name} must use LF endings and end with a newline.")


def verify_fixture(directory: Path = DEFAULT_FIXTURE_DIRECTORY) -> FrozenFixture:
    """Verify that the tracked v0001 fixture exactly matches reproducible bytes."""
    expected = build_fixture()
    if not directory.is_dir():
        raise FrozenFixtureError(f"Fixture directory does not exist: {directory}")
    actual_names = tuple(sorted(path.name for path in directory.iterdir() if path.is_file()))
    if actual_names != tuple(sorted(_FIXTURE_FILENAMES)):
        raise FrozenFixtureError(
            "The fixture directory must contain only the three released files."
        )
    for name, expected_content in expected.files().items():
        actual_content = (directory / name).read_bytes()
        _validate_text_file(name, actual_content)
        if actual_content != expected_content:
            raise FrozenFixtureError(f"{name} differs from the deterministic v0001 fixture.")
    return expected


def _write_candidate(directory: Path, fixture: FrozenFixture) -> None:
    for name, content in fixture.files().items():
        _validate_text_file(name, content)
        (directory / name).write_bytes(content)


def write_fixture(directory: Path = DEFAULT_FIXTURE_DIRECTORY) -> FrozenFixture:
    """Atomically publish the deterministic v0001 fixture from a verified candidate."""
    fixture = build_fixture()
    parent = directory.parent
    parent.mkdir(parents=True, exist_ok=True)
    candidate = Path(tempfile.mkdtemp(prefix=f".{directory.name}-", dir=parent))
    backup = parent / f".{directory.name}-backup"
    try:
        _write_candidate(candidate, fixture)
        verify_fixture(candidate)
        if backup.exists():
            raise FrozenFixtureError(f"Fixture backup path already exists: {backup}")
        if directory.exists():
            os.replace(directory, backup)
        try:
            os.replace(candidate, directory)
        except BaseException:
            if backup.exists():
                os.replace(backup, directory)
            raise
        if backup.exists():
            shutil.rmtree(backup)
    finally:
        if candidate.exists():
            shutil.rmtree(candidate)
    return fixture


def main(argv: Sequence[str] | None = None) -> int:
    """Check the released fixture by default or rewrite it with explicit consent."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture-directory", type=Path, default=DEFAULT_FIXTURE_DIRECTORY)
    parser.add_argument(
        "--write", action="store_true", help="Write the deterministic fixture files."
    )
    arguments = parser.parse_args(argv)
    try:
        if arguments.write:
            fixture = write_fixture(arguments.fixture_directory)
            action = "Wrote"
        else:
            fixture = verify_fixture(arguments.fixture_directory)
            action = "Verified"
    except (FrozenFixtureError, OSError) as error:
        print(f"Frozen fixture check failed: {error}", file=sys.stderr)
        return 1
    print(
        f"{action} {TARGET_REVISION}: schema={_sha256(fixture.schema)} seed={_sha256(fixture.seed)}"
    )
    return 0
