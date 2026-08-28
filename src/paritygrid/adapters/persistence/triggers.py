"""Stable SQLite trigger declarations installed by schema migrations."""

from dataclasses import dataclass
from types import MappingProxyType
from typing import Final

from sqlalchemy.engine import Connection

from paritygrid.adapters.persistence.schema import OPERATIONAL_TABLE_NAMES


@dataclass(frozen=True, slots=True)
class TriggerDeclaration:
    """One deterministic SQLite integrity trigger."""

    name: str
    table_name: str
    event: str
    sql: str


_IMMUTABLE_TABLES: Final[frozenset[str]] = frozenset(
    {
        "pipeline_versions",
        "connector_secret_references",
        "work_attempts",
        "artifact_manifests",
        "checkpoints",
        "execution_events",
        "reconciliation_summaries",
        "reconciliation_conflicts",
        "repair_approvals",
        "target_state_verifications",
        "audit_entries",
    }
)

IMMUTABLE_COLUMNS: Final[MappingProxyType[str, tuple[str, ...]]] = MappingProxyType(
    {
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
)


def _prohibition(table_name: str, event: str) -> TriggerDeclaration:
    suffix = event.casefold()
    name = f"trg_{table_name}_prohibit_{suffix}"
    sql = (
        f'CREATE TRIGGER "{name}" BEFORE {event} ON "{table_name}" '
        f"BEGIN SELECT RAISE(ABORT, '{table_name} does not permit {suffix}'); END"
    )
    return TriggerDeclaration(name, table_name, event, sql)


def _immutable_columns(table_name: str, columns: tuple[str, ...]) -> TriggerDeclaration:
    name = f"trg_{table_name}_protect_immutable_columns"
    condition = " OR ".join(f'NEW."{column}" IS NOT OLD."{column}"' for column in columns)
    sql = (
        f'CREATE TRIGGER "{name}" BEFORE UPDATE ON "{table_name}" WHEN {condition} '
        f"BEGIN SELECT RAISE(ABORT, '{table_name} immutable columns cannot change'); END"
    )
    return TriggerDeclaration(name, table_name, "UPDATE", sql)


def _monotonic_column(table_name: str, column: str) -> TriggerDeclaration:
    name = f"trg_{table_name}_{column}_must_increase"
    sql = (
        f'CREATE TRIGGER "{name}" BEFORE UPDATE ON "{table_name}" '
        f'WHEN NEW."{column}" <= OLD."{column}" '
        f"BEGIN SELECT RAISE(ABORT, '{table_name} {column} must increase'); END"
    )
    return TriggerDeclaration(name, table_name, "UPDATE", sql)


def _terminal_status(
    table_name: str, column: str, terminal_values: tuple[str, ...]
) -> TriggerDeclaration:
    name = f"trg_{table_name}_protect_terminal_{column}"
    values = ", ".join(f"'{value}'" for value in terminal_values)
    sql = (
        f'CREATE TRIGGER "{name}" BEFORE UPDATE ON "{table_name}" '
        f'WHEN OLD."{column}" IN ({values}) '
        f"BEGIN SELECT RAISE(ABORT, '{table_name} terminal rows cannot change'); END"
    )
    return TriggerDeclaration(name, table_name, "UPDATE", sql)


_declarations = [
    *(_prohibition(table_name, "DELETE") for table_name in OPERATIONAL_TABLE_NAMES),
    *(_prohibition(table_name, "UPDATE") for table_name in sorted(_IMMUTABLE_TABLES)),
    *(_immutable_columns(table_name, columns) for table_name, columns in IMMUTABLE_COLUMNS.items()),
    _monotonic_column("checkpoint_heads", "current_version"),
    _monotonic_column("run_event_counters", "next_sequence_number"),
    _terminal_status("idempotency_records", "status", ("completed", "failed")),
    _terminal_status("repair_actions", "application_status", ("applied", "failed")),
    _terminal_status("repair_plans", "status", ("applied", "rejected", "failed")),
]

TRIGGER_DECLARATIONS: Final[tuple[TriggerDeclaration, ...]] = tuple(
    sorted(_declarations, key=lambda item: item.name)
)
TRIGGERS_BY_NAME: Final[MappingProxyType[str, TriggerDeclaration]] = MappingProxyType(
    {declaration.name: declaration for declaration in TRIGGER_DECLARATIONS}
)
IMMUTABLE_TABLE_NAMES: Final[frozenset[str]] = _IMMUTABLE_TABLES


def install_integrity_triggers(connection: Connection) -> None:
    """Install declarations through a migration-compatible SQL execution boundary."""
    for declaration in TRIGGER_DECLARATIONS:
        connection.exec_driver_sql(declaration.sql)
