"""Tests for the migration-owned SQLite integrity trigger catalog."""

from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError

from paritygrid.adapters.persistence.schema import OPERATIONAL_TABLE_NAMES, metadata
from paritygrid.adapters.persistence.triggers import (
    IMMUTABLE_COLUMNS,
    IMMUTABLE_TABLE_NAMES,
    TRIGGER_DECLARATIONS,
    install_integrity_triggers,
)

UTC = "2026-08-12T12:00:00.000000Z"
HASH_A = "a" * 64
HASH_B = "b" * 64

EXPECTED_IMMUTABLE_COLUMNS = {
    "system_metadata": ("key",),
    "pipelines": ("pipeline_id", "created_at"),
    "connectors": ("connector_id", "kind", "created_at"),
    "runs": (
        "run_id",
        "pipeline_id",
        "pipeline_version_number",
        "runner_kind",
        "runner_configuration_json",
        "scenario_seed",
        "created_at",
    ),
    "run_event_counters": ("run_id",),
    "run_nodes": ("run_id", "node_id"),
    "work_items": (
        "work_item_id",
        "run_id",
        "node_id",
        "partition_key",
        "input_reference_json",
        "created_at",
    ),
    "checkpoint_heads": ("run_id", "node_id", "partition_key"),
    "idempotency_records": ("scope", "idempotency_key", "request_sha256", "created_at"),
    "repair_plans": (
        "repair_plan_id",
        "run_id",
        "reconciliation_fingerprint",
        "content_fingerprint",
        "created_at",
    ),
    "repair_actions": (
        "repair_action_id",
        "repair_plan_id",
        "run_id",
        "conflict_id",
        "canonical_key",
        "action_kind",
        "external_idempotency_key",
        "before_sha256",
        "proposed_after_sha256",
        "proposed_record_json",
        "expected_target_record_json",
        "mismatch_evidence_json",
    ),
}


@pytest.fixture
def engine() -> Iterator[Engine]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    yield engine
    engine.dispose()


def _insert_run(connection: Connection) -> None:
    connection.exec_driver_sql(
        "INSERT INTO pipelines "
        "(pipeline_id, display_name, created_at, row_version) VALUES (?, ?, ?, ?)",
        ("pip_alpha", "Pipeline", UTC, 1),
    )
    connection.exec_driver_sql(
        "INSERT INTO pipeline_versions "
        "(pipeline_id, version_number, specification_json, specification_sha256, "
        "planner_format_version, published_at) VALUES (?, ?, ?, ?, ?, ?)",
        ("pip_alpha", 1, "{}", HASH_A, 1, UTC),
    )
    connection.exec_driver_sql(
        "INSERT INTO runs "
        "(run_id, pipeline_id, pipeline_version_number, runner_kind, "
        "runner_configuration_json, state, row_version, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("run_alpha", "pip_alpha", 1, "sequential", "{}", "queued", 1, UTC),
    )


def _insert_work_item(connection: Connection) -> None:
    _insert_run(connection)
    connection.exec_driver_sql(
        "INSERT INTO run_nodes (run_id, node_id, state, row_version) VALUES (?, ?, ?, ?)",
        ("run_alpha", "nod_source", "pending", 1),
    )
    connection.exec_driver_sql(
        "INSERT INTO work_items "
        "(work_item_id, run_id, node_id, partition_key, state, row_version, "
        "completed_attempt_count, expected_checkpoint_version, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("wrk_alpha", "run_alpha", "nod_source", "part-1", "pending", 1, 0, 0, UTC, UTC),
    )


def _insert_reconciliation(connection: Connection) -> None:
    _insert_run(connection)
    connection.exec_driver_sql(
        "INSERT INTO reconciliation_summaries "
        "(run_id, total_count, source_fingerprint, target_fingerprint, "
        "reconciliation_fingerprint, analytical_query_version, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("run_alpha", 0, HASH_A, HASH_A, HASH_A, 1, UTC),
    )


def test_catalog_declares_delete_prohibition_for_every_operational_table() -> None:
    delete_tables = {
        declaration.table_name
        for declaration in TRIGGER_DECLARATIONS
        if declaration.name.endswith("_prohibit_delete")
    }
    assert delete_tables == set(OPERATIONAL_TABLE_NAMES)


def test_catalog_covers_whole_row_and_immutable_column_update_guards() -> None:
    update_prohibitions = {
        declaration.table_name
        for declaration in TRIGGER_DECLARATIONS
        if declaration.name.endswith("_prohibit_update")
    }
    protected_columns = {
        declaration.table_name
        for declaration in TRIGGER_DECLARATIONS
        if declaration.name.endswith("_protect_immutable_columns")
    }
    assert update_prohibitions == set(IMMUTABLE_TABLE_NAMES)
    assert protected_columns == set(IMMUTABLE_COLUMNS)
    assert update_prohibitions.isdisjoint(protected_columns)
    assert dict(IMMUTABLE_COLUMNS) == EXPECTED_IMMUTABLE_COLUMNS
    assert all(
        set(columns) <= set(metadata.tables[table_name].columns.keys())
        for table_name, columns in IMMUTABLE_COLUMNS.items()
    )


def test_trigger_names_and_sql_are_unique_deterministic_and_migration_ready() -> None:
    names = tuple(declaration.name for declaration in TRIGGER_DECLARATIONS)
    category_counts = {
        "delete": sum(name.endswith("_prohibit_delete") for name in names),
        "whole_update": sum(name.endswith("_prohibit_update") for name in names),
        "immutable_columns": sum(name.endswith("_protect_immutable_columns") for name in names),
        "monotonic": sum(name.endswith("_must_increase") for name in names),
        "terminal": sum("_protect_terminal_" in name for name in names),
    }
    assert names == tuple(sorted(names))
    assert len(names) == len(set(names))
    assert category_counts == {
        "delete": 21,
        "whole_update": 10,
        "immutable_columns": 11,
        "monotonic": 2,
        "terminal": 3,
    }
    assert len(names) == sum(category_counts.values())
    assert all(
        declaration.sql.startswith('CREATE TRIGGER "trg_') for declaration in TRIGGER_DECLARATIONS
    )
    assert all("DROP " not in declaration.sql for declaration in TRIGGER_DECLARATIONS)
    assert "trg_checkpoint_heads_current_version_must_increase" in names
    assert "trg_run_event_counters_next_sequence_number_must_increase" in names
    assert "trg_repair_plans_protect_terminal_status" in names


def test_installed_catalog_matches_declarations(engine: Engine) -> None:
    with engine.begin() as connection:
        install_integrity_triggers(connection)
        rows = connection.exec_driver_sql(
            "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger' ORDER BY name"
        ).all()
        assert tuple(rows) == tuple(
            (item.name, item.table_name, item.sql) for item in TRIGGER_DECLARATIONS
        )


def test_immutable_history_trigger_rejects_no_op_update(engine: Engine) -> None:
    with engine.begin() as connection:
        install_integrity_triggers(connection)
        connection.exec_driver_sql(
            "INSERT INTO pipelines "
            "(pipeline_id, display_name, created_at, row_version) VALUES (?, ?, ?, ?)",
            ("pip_example", "Example", UTC, 1),
        )
        connection.exec_driver_sql(
            "INSERT INTO pipeline_versions "
            "(pipeline_id, version_number, specification_json, specification_sha256, "
            "planner_format_version, published_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("pip_example", 1, "{}", HASH_A, 1, UTC),
        )
        with Savepoint(connection), pytest.raises(IntegrityError):
            connection.exec_driver_sql(
                "UPDATE pipeline_versions SET version_number = version_number "
                "WHERE pipeline_id = 'pip_example'"
            )


def test_triggers_allow_named_mutation_but_protect_identity_and_delete(engine: Engine) -> None:
    with engine.begin() as connection:
        install_integrity_triggers(connection)
        connection.exec_driver_sql(
            "INSERT INTO pipelines "
            "(pipeline_id, display_name, created_at, row_version) VALUES (?, ?, ?, ?)",
            ("pip_example", "Example", UTC, 1),
        )
        connection.exec_driver_sql(
            "UPDATE pipelines SET display_name = ?, row_version = ? WHERE pipeline_id = ?",
            ("Renamed", 2, "pip_example"),
        )
        connection.exec_driver_sql(
            "UPDATE pipelines SET pipeline_id = pipeline_id WHERE pipeline_id = 'pip_example'"
        )
        assert (
            connection.exec_driver_sql(
                "SELECT display_name FROM pipelines WHERE pipeline_id = 'pip_example'"
            ).scalar_one()
            == "Renamed"
        )

        with Savepoint(connection), pytest.raises(IntegrityError):
            connection.exec_driver_sql(
                "UPDATE pipelines SET pipeline_id = 'pip_changed' WHERE pipeline_id = 'pip_example'"
            )
        with Savepoint(connection), pytest.raises(IntegrityError):
            connection.exec_driver_sql("DELETE FROM pipelines WHERE pipeline_id = 'pip_example'")


def test_installed_monotonic_guards_reject_stale_values_and_allow_advances(
    engine: Engine,
) -> None:
    with engine.begin() as connection:
        install_integrity_triggers(connection)
        _insert_work_item(connection)
        connection.exec_driver_sql(
            "INSERT INTO checkpoint_heads "
            "(run_id, node_id, partition_key, current_version, updated_at, row_version) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("run_alpha", "nod_source", "part-1", 0, UTC, 1),
        )
        connection.exec_driver_sql(
            "INSERT INTO run_event_counters (run_id, next_sequence_number, row_version) "
            "VALUES (?, ?, ?)",
            ("run_alpha", 1, 1),
        )

        connection.exec_driver_sql(
            "UPDATE checkpoint_heads SET current_version = 1, row_version = 2 "
            "WHERE run_id = 'run_alpha'"
        )
        connection.exec_driver_sql(
            "UPDATE run_event_counters SET next_sequence_number = 8, row_version = 2 "
            "WHERE run_id = 'run_alpha'"
        )

        with Savepoint(connection), pytest.raises(IntegrityError):
            connection.exec_driver_sql(
                "UPDATE checkpoint_heads SET row_version = 3 WHERE run_id = 'run_alpha'"
            )
        with Savepoint(connection), pytest.raises(IntegrityError):
            connection.exec_driver_sql(
                "UPDATE run_event_counters SET row_version = 3 WHERE run_id = 'run_alpha'"
            )

        for version in (1, 0):
            with Savepoint(connection), pytest.raises(IntegrityError):
                connection.exec_driver_sql(
                    "UPDATE checkpoint_heads SET current_version = ? WHERE run_id = 'run_alpha'",
                    (version,),
                )
        for sequence_number in (8, 7):
            with Savepoint(connection), pytest.raises(IntegrityError):
                connection.exec_driver_sql(
                    "UPDATE run_event_counters SET next_sequence_number = ? "
                    "WHERE run_id = 'run_alpha'",
                    (sequence_number,),
                )


@pytest.mark.parametrize("terminal_status", ["completed", "failed"])
def test_installed_idempotency_terminal_guard_rejects_no_op_updates(
    engine: Engine, terminal_status: str
) -> None:
    with engine.begin() as connection:
        install_integrity_triggers(connection)
        connection.exec_driver_sql(
            "INSERT INTO idempotency_records "
            "(scope, idempotency_key, request_sha256, status, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("runs.create", "request-1", HASH_A, "in_progress", UTC, UTC),
        )
        connection.exec_driver_sql(
            "UPDATE idempotency_records SET status = ?, response_schema_version = 1, "
            "response_json = '{}', completed_at = ?, updated_at = ? "
            "WHERE scope = 'runs.create' AND idempotency_key = 'request-1'",
            (terminal_status, UTC, UTC),
        )
        with Savepoint(connection), pytest.raises(IntegrityError):
            connection.exec_driver_sql(
                "UPDATE idempotency_records SET status = status "
                "WHERE scope = 'runs.create' AND idempotency_key = 'request-1'"
            )


@pytest.mark.parametrize(
    ("terminal_status", "timestamp_column"),
    [("applied", "applied_at"), ("rejected", "rejected_at"), ("failed", "failed_at")],
)
def test_installed_repair_plan_terminal_guard_rejects_no_op_updates(
    engine: Engine, terminal_status: str, timestamp_column: str
) -> None:
    with engine.begin() as connection:
        install_integrity_triggers(connection)
        _insert_reconciliation(connection)
        connection.exec_driver_sql(
            "INSERT INTO repair_plans "
            "(repair_plan_id, run_id, reconciliation_fingerprint, content_fingerprint, "
            "status, row_version, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("rpl_alpha", "run_alpha", HASH_A, HASH_B, "proposed", 1, UTC),
        )
        connection.exec_driver_sql(
            "UPDATE repair_plans SET status = 'approved', row_version = 2 "
            "WHERE repair_plan_id = 'rpl_alpha'"
        )
        connection.exec_driver_sql(
            f"UPDATE repair_plans SET status = ?, {timestamp_column} = ?, row_version = 3 "
            "WHERE repair_plan_id = 'rpl_alpha'",
            (terminal_status, UTC),
        )
        with Savepoint(connection), pytest.raises(IntegrityError):
            connection.exec_driver_sql(
                "UPDATE repair_plans SET status = status WHERE repair_plan_id = 'rpl_alpha'"
            )


@pytest.mark.parametrize("terminal_status", ["applied", "failed"])
def test_installed_repair_action_terminal_guard_rejects_no_op_updates(
    engine: Engine, terminal_status: str
) -> None:
    with engine.begin() as connection:
        install_integrity_triggers(connection)
        _insert_reconciliation(connection)
        connection.exec_driver_sql(
            "INSERT INTO reconciliation_conflicts "
            "(conflict_id, run_id, canonical_key, classification, source_references_json, "
            "field_differences_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("cnf_alpha", "run_alpha", "SKU-1", "missing_from_target", "[]", "[]", UTC),
        )
        connection.exec_driver_sql(
            "INSERT INTO repair_plans "
            "(repair_plan_id, run_id, reconciliation_fingerprint, content_fingerprint, "
            "status, row_version, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("rpl_alpha", "run_alpha", HASH_A, HASH_B, "approved", 1, UTC),
        )
        connection.exec_driver_sql(
            "INSERT INTO repair_actions "
            "(repair_action_id, repair_plan_id, run_id, conflict_id, canonical_key, "
            "action_kind, external_idempotency_key, proposed_after_sha256, "
            "proposed_record_json, mismatch_evidence_json, application_status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "rac_alpha",
                "rpl_alpha",
                "run_alpha",
                "cnf_alpha",
                "SKU-1",
                "create_target",
                "effect-1",
                HASH_B,
                "{}",
                "[]",
                "pending",
            ),
        )
        if terminal_status == "applied":
            result_values = (terminal_status, "{}", 1, UTC, "rac_alpha")
            update = (
                "UPDATE repair_actions SET application_status = ?, application_result_json = ?, "
                "target_version = ?, applied_at = ? WHERE repair_action_id = ?"
            )
        else:
            result_values = (terminal_status, "{}", UTC, "rac_alpha")
            update = (
                "UPDATE repair_actions SET application_status = ?, application_result_json = ?, "
                "failed_at = ? WHERE repair_action_id = ?"
            )
        connection.exec_driver_sql(update, result_values)
        with Savepoint(connection), pytest.raises(IntegrityError):
            connection.exec_driver_sql(
                "UPDATE repair_actions SET application_status = application_status "
                "WHERE repair_action_id = 'rac_alpha'"
            )


class Savepoint:
    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._transaction = connection.begin_nested()

    def __enter__(self) -> None:
        return None

    def __exit__(self, *_args: object) -> None:
        self._transaction.rollback()
