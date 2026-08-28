"""SQLAlchemy metadata for the authoritative SQLite operational schema."""

from collections.abc import Iterator
from typing import Final, cast, overload

from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql.elements import TextClause

from paritygrid.adapters.persistence.values import (
    IdempotencyStatus,
    RepairActionApplicationStatus,
    RepairPlanStatus,
    RunNodeState,
    TargetVerificationVerdict,
    WorkAttemptOutcome,
)
from paritygrid.domain.execution import FailureClassification, RunState, WorkItemState
from paritygrid.domain.reconciliation import ReconciliationClassification
from paritygrid.domain.repair import RepairActionKind

NAMING_CONVENTION: Final[dict[str, str]] = {
    "pk": "pk_%(table_name)s",
    "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
}


class Base(DeclarativeBase):
    """Declarative base shared only by persistence mappings."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


metadata = Base.metadata

_MAX_SEQUENCE = 2_147_483_647
_MAX_DURATION = 31_536_000_000_000


def _values(values: Iterator[str]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def _enum(column: str, values: Iterator[str], name: str) -> CheckConstraint:
    return CheckConstraint(f"{column} IN ({_values(values)})", name=name)


def _positive(column: str, name: str, *, zero: bool = False) -> CheckConstraint:
    lower = 0 if zero else 1
    return CheckConstraint(
        f"typeof({column}) = 'integer' AND {column} BETWEEN {lower} AND {_MAX_SEQUENCE}",
        name=name,
    )


def _nonnegative(column: str, name: str, *, maximum: int | None = None) -> CheckConstraint:
    limit = "" if maximum is None else f" AND {column} <= {maximum}"
    return CheckConstraint(
        f"typeof({column}) = 'integer' AND {column} >= 0{limit}",
        name=name,
    )


def _bounded_text(column: str, maximum: int, name: str) -> CheckConstraint:
    return CheckConstraint(
        f"typeof({column}) = 'text' AND length({column}) BETWEEN 1 AND {maximum}",
        name=name,
    )


def _sha256(column: str, name: str, *, nullable: bool = False) -> CheckConstraint:
    expression = (
        f"typeof({column}) = 'text' AND length({column}) = 64 AND {column} NOT GLOB '*[^0-9a-f]*'"
    )
    if nullable:
        expression = f"{column} IS NULL OR ({expression})"
    return CheckConstraint(expression, name=name)


def _utc(column: str, name: str, *, nullable: bool = False) -> CheckConstraint:
    expression = (
        f"typeof({column}) = 'text' AND length({column}) = 27 "
        f"AND substr({column}, 5, 1) = '-' AND substr({column}, 8, 1) = '-' "
        f"AND substr({column}, 11, 1) = 'T' AND substr({column}, 14, 1) = ':' "
        f"AND substr({column}, 17, 1) = ':' AND substr({column}, 20, 1) = '.' "
        f"AND substr({column}, 27, 1) = 'Z' "
        f"AND substr({column}, 1, 4) NOT GLOB '*[^0-9]*' "
        f"AND substr({column}, 6, 2) NOT GLOB '*[^0-9]*' "
        f"AND substr({column}, 9, 2) NOT GLOB '*[^0-9]*' "
        f"AND substr({column}, 12, 2) NOT GLOB '*[^0-9]*' "
        f"AND substr({column}, 15, 2) NOT GLOB '*[^0-9]*' "
        f"AND substr({column}, 18, 2) NOT GLOB '*[^0-9]*' "
        f"AND substr({column}, 21, 6) NOT GLOB '*[^0-9]*' "
        f"AND substr({column}, 1, 4) BETWEEN '0001' AND '9999' "
        f"AND substr({column}, 6, 2) BETWEEN '01' AND '12' "
        f"AND substr({column}, 9, 2) BETWEEN '01' AND '31' "
        f"AND substr({column}, 12, 2) BETWEEN '00' AND '23' "
        f"AND substr({column}, 15, 2) BETWEEN '00' AND '59' "
        f"AND substr({column}, 18, 2) BETWEEN '00' AND '59'"
    )
    if nullable:
        expression = f"{column} IS NULL OR ({expression})"
    return CheckConstraint(expression, name=name)


def _json(column: str, name: str, *, shape: str, nullable: bool = False) -> CheckConstraint:
    expression = (
        f"typeof({column}) = 'text' AND json_valid({column}) AND json_type({column}) = '{shape}'"
    )
    if nullable:
        expression = f"{column} IS NULL OR ({expression})"
    return CheckConstraint(expression, name=name)


def _id(column: str, prefix: str, name: str, *, nullable: bool = False) -> CheckConstraint:
    maximum = len(prefix) + 1 + 64
    expression = (
        f"typeof({column}) = 'text' AND length({column}) BETWEEN {len(prefix) + 4} "
        f"AND {maximum} AND substr({column}, 1, {len(prefix) + 1}) = '{prefix}_' "
        f"AND substr({column}, {len(prefix) + 2}) NOT GLOB '*[^a-z0-9-]*' "
        f"AND substr({column}, {len(prefix) + 2}) NOT LIKE '-%' "
        f"AND substr({column}, -1) <> '-' AND {column} NOT LIKE '%--%'"
    )
    if nullable:
        expression = f"{column} IS NULL OR ({expression})"
    return CheckConstraint(expression, name=name)


def _pk(table: str, *columns: str) -> PrimaryKeyConstraint:
    return PrimaryKeyConstraint(*columns, name=f"pk_{table}")


def _uq(table: str, *columns: str) -> UniqueConstraint:
    return UniqueConstraint(*columns, name=f"uq_{table}_{'_'.join(columns)}")


def _fk(
    table: str,
    local_columns: list[str],
    remote_columns: list[str],
    referred_table: str,
) -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        local_columns,
        remote_columns,
        name=f"fk_{table}_{'_'.join(local_columns)}_{referred_table}",
    )


def _ix(
    table: str,
    *columns: str,
    sqlite_where: TextClause | None = None,
) -> Index:
    return Index(
        f"ix_{table}_{'_'.join(columns)}",
        *columns,
        sqlite_where=sqlite_where,
    )


@overload
def _column(
    name: str,
    type_: type[Integer],
    *,
    nullable: bool = False,
    default: str | None = None,
) -> Column[int]: ...


@overload
def _column(
    name: str,
    type_: String | type[Text],
    *,
    nullable: bool = False,
    default: str | None = None,
) -> Column[str]: ...


def _column(
    name: str,
    type_: String | type[Integer] | type[Text],
    *,
    nullable: bool = False,
    default: str | None = None,
) -> Column[int] | Column[str]:
    if type_ is Integer:
        return Column(name, Integer, nullable=nullable, server_default=default)
    text_type = cast(String | type[Text], type_)
    return Column(name, text_type, nullable=nullable, server_default=default)


system_metadata = Table(
    "system_metadata",
    metadata,
    _column("key", String(96)),
    _column("value", Text),
    _column("updated_at", String(27)),
    _pk("system_metadata", "key"),
    CheckConstraint(
        "typeof(key) = 'text' AND length(key) BETWEEN 1 AND 96 "
        "AND key GLOB '[a-z]*' AND key NOT GLOB '*[^a-z0-9_.-]*' "
        "AND key NOT GLOB '*[._-][._-]*' "
        "AND substr(key,-1) NOT IN ('.','-','_')",
        name="key_shape",
    ),
    _bounded_text("value", 4096, "value_size"),
    _utc("updated_at", "updated_at_utc"),
)


pipelines = Table(
    "pipelines",
    metadata,
    _column("pipeline_id", String(68)),
    _column("display_name", String(160)),
    _column("description", Text, nullable=True),
    _column("created_at", String(27)),
    _column("archived_at", String(27), nullable=True),
    _column("row_version", Integer, default="1"),
    _pk("pipelines", "pipeline_id"),
    _id("pipeline_id", "pip", "pipeline_id_shape"),
    _bounded_text("display_name", 160, "display_name_size"),
    _utc("created_at", "created_at_utc"),
    _utc("archived_at", "archived_at_utc", nullable=True),
    _positive("row_version", "row_version_range"),
    CheckConstraint("archived_at IS NULL OR archived_at >= created_at", name="archive_order"),
    _ix("pipelines", "archived_at"),
    _ix("pipelines", "display_name"),
)

pipeline_versions = Table(
    "pipeline_versions",
    metadata,
    _column("pipeline_id", String(68)),
    _column("version_number", Integer),
    _column("specification_json", Text),
    _column("specification_sha256", String(64)),
    _column("planner_format_version", Integer),
    _column("published_at", String(27)),
    _pk("pipeline_versions", "pipeline_id", "version_number"),
    _fk("pipeline_versions", ["pipeline_id"], ["pipelines.pipeline_id"], "pipelines"),
    _positive("version_number", "version_number_range"),
    _json("specification_json", "specification_json_object", shape="object"),
    _sha256("specification_sha256", "specification_sha256_shape"),
    _positive("planner_format_version", "planner_format_version_range"),
    _utc("published_at", "published_at_utc"),
    _ix("pipeline_versions", "published_at"),
)

connectors = Table(
    "connectors",
    metadata,
    _column("connector_id", String(68)),
    _column("kind", String(96)),
    _column("display_name", String(160)),
    _column("configuration_json", Text),
    _column("capabilities_json", Text),
    _column("schema_discovery_json", Text, nullable=True),
    _column("revision", Integer, default="1"),
    _column("created_at", String(27)),
    _column("updated_at", String(27)),
    _column("archived_at", String(27), nullable=True),
    _column("row_version", Integer, default="1"),
    _pk("connectors", "connector_id"),
    _id("connector_id", "con", "connector_id_shape"),
    _bounded_text("kind", 96, "kind_size"),
    _bounded_text("display_name", 160, "display_name_size"),
    _json("configuration_json", "configuration_json_object", shape="object"),
    _json("capabilities_json", "capabilities_json_object", shape="object"),
    _json("schema_discovery_json", "schema_discovery_json_object", shape="object", nullable=True),
    _positive("revision", "revision_range"),
    _utc("created_at", "created_at_utc"),
    _utc("updated_at", "updated_at_utc"),
    _utc("archived_at", "archived_at_utc", nullable=True),
    _positive("row_version", "row_version_range"),
    CheckConstraint("updated_at >= created_at", name="updated_at_order"),
    CheckConstraint("archived_at IS NULL OR archived_at >= created_at", name="archive_order"),
    _ix("connectors", "kind"),
    _ix("connectors", "archived_at"),
)

connector_secret_references = Table(
    "connector_secret_references",
    metadata,
    _column("connector_id", String(68)),
    _column("reference_name", String(64)),
    _column("environment_variable_name", String(128)),
    _column("created_at", String(27)),
    _pk("connector_secret_references", "connector_id", "reference_name"),
    _fk(
        "connector_secret_references",
        ["connector_id"],
        ["connectors.connector_id"],
        "connectors",
    ),
    CheckConstraint(
        "typeof(reference_name) = 'text' AND length(reference_name) BETWEEN 1 AND 64 "
        "AND reference_name GLOB '[a-z]*' AND reference_name NOT GLOB '*[^a-z0-9_.-]*' "
        "AND reference_name NOT GLOB '*[._-][._-]*' "
        "AND substr(reference_name,-1) NOT IN ('.','-','_')",
        name="reference_name_shape",
    ),
    CheckConstraint(
        "typeof(environment_variable_name) = 'text' "
        "AND length(environment_variable_name) BETWEEN 1 AND 128 "
        "AND environment_variable_name GLOB '[A-Z_]*' "
        "AND environment_variable_name NOT GLOB '*[^A-Z0-9_]*'",
        name="environment_variable_name_shape",
    ),
    _utc("created_at", "created_at_utc"),
)

runs = Table(
    "runs",
    metadata,
    _column("run_id", String(68)),
    _column("pipeline_id", String(68)),
    _column("pipeline_version_number", Integer),
    _column("runner_kind", String(32)),
    _column("runner_configuration_json", Text),
    _column("state", String(32)),
    _column("row_version", Integer, default="1"),
    _column("scenario_seed", Integer, nullable=True),
    _column("created_at", String(27)),
    _column("started_at", String(27), nullable=True),
    _column("finished_at", String(27), nullable=True),
    _column("cancellation_requested_at", String(27), nullable=True),
    _column("recovery_started_at", String(27), nullable=True),
    _column("recovered_at", String(27), nullable=True),
    _column("execution_evidence_fingerprint", String(64), nullable=True),
    _column("execution_evidence_fingerprint_version", Integer, nullable=True),
    _pk("runs", "run_id"),
    _fk(
        "runs",
        ["pipeline_id", "pipeline_version_number"],
        ["pipeline_versions.pipeline_id", "pipeline_versions.version_number"],
        "pipeline_versions",
    ),
    _id("run_id", "run", "run_id_shape"),
    _positive("pipeline_version_number", "pipeline_version_number_range"),
    _bounded_text("runner_kind", 32, "runner_kind_size"),
    _json("runner_configuration_json", "runner_configuration_json_object", shape="object"),
    _enum("state", (value.value for value in RunState), "state_values"),
    _positive("row_version", "row_version_range"),
    CheckConstraint(
        "scenario_seed IS NULL OR typeof(scenario_seed) = 'integer'",
        name="scenario_seed_storage",
    ),
    _utc("created_at", "created_at_utc"),
    *(
        _utc(column, f"{column}_utc", nullable=True)
        for column in (
            "started_at",
            "finished_at",
            "cancellation_requested_at",
            "recovery_started_at",
            "recovered_at",
        )
    ),
    _sha256(
        "execution_evidence_fingerprint",
        "execution_evidence_fingerprint_shape",
        nullable=True,
    ),
    CheckConstraint(
        "execution_evidence_fingerprint_version IS NULL "
        "OR (typeof(execution_evidence_fingerprint_version) = 'integer' "
        "AND execution_evidence_fingerprint_version BETWEEN 1 AND 2147483647)",
        name="execution_evidence_fingerprint_version_range",
    ),
    CheckConstraint(
        "(execution_evidence_fingerprint IS NULL "
        "AND execution_evidence_fingerprint_version IS NULL) "
        "OR (execution_evidence_fingerprint IS NOT NULL "
        "AND execution_evidence_fingerprint_version IS NOT NULL)",
        name="execution_evidence_fingerprint_pairing",
    ),
    CheckConstraint(
        "state NOT IN ('succeeded','partially_succeeded','failed','cancelled') "
        "OR finished_at IS NOT NULL",
        name="terminal_finish",
    ),
    CheckConstraint(
        "execution_evidence_fingerprint IS NULL OR state IN ('succeeded','partially_succeeded')",
        name="execution_evidence_fingerprint_terminal",
    ),
    CheckConstraint("started_at IS NULL OR started_at >= created_at", name="started_at_order"),
    CheckConstraint("finished_at IS NULL OR finished_at >= created_at", name="finished_at_order"),
    _ix("runs", "state", "created_at"),
    _ix("runs", "pipeline_id", "pipeline_version_number"),
    _ix("runs", "created_at"),
)

run_event_counters = Table(
    "run_event_counters",
    metadata,
    _column("run_id", String(68)),
    _column("next_sequence_number", Integer, default="1"),
    _column("row_version", Integer, default="1"),
    _pk("run_event_counters", "run_id"),
    _fk("run_event_counters", ["run_id"], ["runs.run_id"], "runs"),
    _positive("next_sequence_number", "next_sequence_number_range"),
    _positive("row_version", "row_version_range"),
)

_aggregate_columns = (
    "work_total",
    "work_pending",
    "work_running",
    "work_succeeded",
    "work_quarantined",
    "work_failed",
    "work_cancelled",
    "records_read",
    "records_written",
    "records_quarantined",
    "bytes_read",
    "bytes_written",
    "retry_count",
)
run_nodes = Table(
    "run_nodes",
    metadata,
    _column("run_id", String(68)),
    _column("node_id", String(68)),
    _column("state", String(32)),
    _column("row_version", Integer, default="1"),
    *(_column(name, Integer, default="0") for name in _aggregate_columns),
    _column("duration_microseconds", Integer, default="0"),
    _column("started_at", String(27), nullable=True),
    _column("finished_at", String(27), nullable=True),
    _pk("run_nodes", "run_id", "node_id"),
    _fk("run_nodes", ["run_id"], ["runs.run_id"], "runs"),
    _id("node_id", "nod", "node_id_shape"),
    _enum("state", (value.value for value in RunNodeState), "state_values"),
    _positive("row_version", "row_version_range"),
    *(_nonnegative(name, f"{name}_range") for name in _aggregate_columns),
    _nonnegative("duration_microseconds", "duration_range", maximum=_MAX_DURATION),
    _utc("started_at", "started_at_utc", nullable=True),
    _utc("finished_at", "finished_at_utc", nullable=True),
    CheckConstraint(
        "work_pending + work_running + work_succeeded + work_quarantined "
        "+ work_failed + work_cancelled <= work_total",
        name="work_count_sum",
    ),
    _ix("run_nodes", "run_id", "state"),
)

_durable_work_states = tuple(value for value in WorkItemState if value is not WorkItemState.LEASED)
work_items = Table(
    "work_items",
    metadata,
    _column("work_item_id", String(68)),
    _column("run_id", String(68)),
    _column("node_id", String(68)),
    _column("partition_key", String(128)),
    _column("state", String(32)),
    _column("row_version", Integer, default="1"),
    _column("completed_attempt_count", Integer, default="0"),
    _column("expected_checkpoint_version", Integer, default="0"),
    _column("input_reference_json", Text, nullable=True),
    _column("retry_available_at", String(27), nullable=True),
    _column("lease_owner", String(128), nullable=True),
    _column("lease_expires_at", String(27), nullable=True),
    _column("active_attempt_number", Integer, nullable=True),
    _column("active_attempt_started_at", String(27), nullable=True),
    _column("active_runner_kind", String(32), nullable=True),
    _column("active_worker_identity", String(128), nullable=True),
    _column("created_at", String(27)),
    _column("updated_at", String(27)),
    _pk("work_items", "work_item_id"),
    _uq("work_items", "run_id", "node_id", "partition_key"),
    _fk(
        "work_items",
        ["run_id", "node_id"],
        ["run_nodes.run_id", "run_nodes.node_id"],
        "run_nodes",
    ),
    _id("work_item_id", "wrk", "work_item_id_shape"),
    _bounded_text("partition_key", 128, "partition_key_size"),
    _enum("state", (value.value for value in _durable_work_states), "state_values"),
    _positive("row_version", "row_version_range"),
    _nonnegative("completed_attempt_count", "completed_attempt_count_range", maximum=_MAX_SEQUENCE),
    _nonnegative(
        "expected_checkpoint_version", "expected_checkpoint_version_range", maximum=_MAX_SEQUENCE
    ),
    _json("input_reference_json", "input_reference_json_object", shape="object", nullable=True),
    *(
        _utc(column, f"{column}_utc", nullable=True)
        for column in ("retry_available_at", "lease_expires_at", "active_attempt_started_at")
    ),
    _utc("created_at", "created_at_utc"),
    _utc("updated_at", "updated_at_utc"),
    CheckConstraint("updated_at >= created_at", name="updated_at_order"),
    CheckConstraint(
        "lease_owner IS NULL OR (typeof(lease_owner) = 'text' "
        "AND length(lease_owner) BETWEEN 1 AND 128)",
        name="lease_owner_size",
    ),
    CheckConstraint(
        "active_runner_kind IS NULL OR (typeof(active_runner_kind) = 'text' "
        "AND length(active_runner_kind) BETWEEN 1 AND 32)",
        name="active_runner_kind_size",
    ),
    CheckConstraint(
        "active_worker_identity IS NULL OR (typeof(active_worker_identity) = 'text' "
        "AND length(active_worker_identity) BETWEEN 1 AND 128)",
        name="active_worker_identity_size",
    ),
    CheckConstraint(
        "(state = 'running' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL "
        "AND active_attempt_number IS NOT NULL AND active_attempt_started_at IS NOT NULL "
        "AND active_runner_kind IS NOT NULL AND active_worker_identity IS NOT NULL) OR "
        "(state <> 'running' AND lease_owner IS NULL AND lease_expires_at IS NULL "
        "AND active_attempt_number IS NULL AND active_attempt_started_at IS NULL "
        "AND active_runner_kind IS NULL AND active_worker_identity IS NULL)",
        name="active_attempt_coherence",
    ),
    CheckConstraint(
        "active_attempt_number IS NULL OR active_attempt_number = completed_attempt_count + 1",
        name="active_attempt_number_order",
    ),
    CheckConstraint(
        "lease_expires_at IS NULL OR lease_expires_at > active_attempt_started_at",
        name="lease_time_order",
    ),
    CheckConstraint(
        "(state = 'retry_wait' AND retry_available_at IS NOT NULL) OR "
        "(state <> 'retry_wait' AND retry_available_at IS NULL)",
        name="retry_time_coherence",
    ),
    _ix("work_items", "run_id", "state", "retry_available_at"),
    _ix("work_items", "lease_expires_at", sqlite_where=text("state = 'running'")),
)

work_attempts = Table(
    "work_attempts",
    metadata,
    _column("work_item_id", String(68)),
    _column("attempt_number", Integer),
    _column("started_at", String(27)),
    _column("finished_at", String(27)),
    _column("runner_kind", String(32)),
    _column("worker_identity", String(128)),
    _column("outcome", String(32)),
    _column("failure_classification", String(32), nullable=True),
    _column("redacted_detail", Text, nullable=True),
    _column("result_reference_json", Text, nullable=True),
    _column("records_processed", Integer, default="0"),
    _column("bytes_processed", Integer, default="0"),
    _column("duration_microseconds", Integer, default="0"),
    _pk("work_attempts", "work_item_id", "attempt_number"),
    _fk("work_attempts", ["work_item_id"], ["work_items.work_item_id"], "work_items"),
    _positive("attempt_number", "attempt_number_range"),
    _utc("started_at", "started_at_utc"),
    _utc("finished_at", "finished_at_utc"),
    _bounded_text("runner_kind", 32, "runner_kind_size"),
    _bounded_text("worker_identity", 128, "worker_identity_size"),
    _enum("outcome", (value.value for value in WorkAttemptOutcome), "outcome_values"),
    _enum(
        "failure_classification", (value.value for value in FailureClassification), "failure_values"
    ),
    _json("result_reference_json", "result_reference_json_object", shape="object", nullable=True),
    CheckConstraint(
        "redacted_detail IS NULL OR (typeof(redacted_detail) = 'text' "
        "AND length(redacted_detail) BETWEEN 1 AND 4096)",
        name="redacted_detail_size",
    ),
    *(
        _nonnegative(column, f"{column}_range")
        for column in ("records_processed", "bytes_processed")
    ),
    _nonnegative("duration_microseconds", "duration_range", maximum=_MAX_DURATION),
    CheckConstraint("finished_at >= started_at", name="attempt_time_order"),
    CheckConstraint(
        "(outcome = 'succeeded' AND failure_classification IS NULL) OR "
        "(outcome <> 'succeeded' AND failure_classification IS NOT NULL)",
        name="failure_coherence",
    ),
    CheckConstraint(
        "outcome <> 'lease_expired' OR failure_classification = 'timeout'",
        name="lease_expired_classification",
    ),
    _ix("work_attempts", "finished_at"),
    _ix("work_attempts", "failure_classification"),
)

artifact_manifests = Table(
    "artifact_manifests",
    metadata,
    _column("artifact_id", String(68)),
    _column("run_id", String(68)),
    _column("node_id", String(68)),
    _column("partition_key", String(128)),
    _column("relative_path", Text),
    _column("media_type", String(127)),
    _column("schema_version", Integer),
    _column("byte_size", Integer),
    _column("row_count", Integer),
    _column("sha256", String(64)),
    _column("created_at", String(27)),
    _pk("artifact_manifests", "artifact_id"),
    _fk(
        "artifact_manifests",
        ["run_id", "node_id"],
        ["run_nodes.run_id", "run_nodes.node_id"],
        "run_nodes",
    ),
    _uq("artifact_manifests", "relative_path"),
    _uq("artifact_manifests", "artifact_id", "run_id", "node_id", "partition_key"),
    _id("artifact_id", "art", "artifact_id_shape"),
    _bounded_text("partition_key", 128, "partition_key_size"),
    CheckConstraint(
        "typeof(relative_path) = 'text' AND length(relative_path) BETWEEN 1 AND 1024 "
        "AND substr(relative_path,1,1) NOT IN ('/','\\') "
        "AND instr(relative_path, char(0)) = 0",
        name="relative_path_basic_shape",
    ),
    _bounded_text("media_type", 127, "media_type_size"),
    _positive("schema_version", "schema_version_range"),
    _nonnegative("byte_size", "byte_size_range"),
    _nonnegative("row_count", "row_count_range"),
    _sha256("sha256", "sha256_shape"),
    _utc("created_at", "created_at_utc"),
    _ix("artifact_manifests", "run_id", "node_id"),
    _ix("artifact_manifests", "sha256"),
)

checkpoint_heads = Table(
    "checkpoint_heads",
    metadata,
    _column("run_id", String(68)),
    _column("node_id", String(68)),
    _column("partition_key", String(128)),
    _column("current_version", Integer, default="0"),
    _column("updated_at", String(27)),
    _column("row_version", Integer, default="1"),
    _pk("checkpoint_heads", "run_id", "node_id", "partition_key"),
    _fk(
        "checkpoint_heads",
        ["run_id", "node_id", "partition_key"],
        ["work_items.run_id", "work_items.node_id", "work_items.partition_key"],
        "work_items",
    ),
    _nonnegative("current_version", "current_version_range", maximum=_MAX_SEQUENCE),
    _utc("updated_at", "updated_at_utc"),
    _positive("row_version", "row_version_range"),
)

checkpoints = Table(
    "checkpoints",
    metadata,
    _column("run_id", String(68)),
    _column("node_id", String(68)),
    _column("partition_key", String(128)),
    _column("version", Integer),
    _column("payload_schema_version", Integer),
    _column("source_cursor_json", Text, nullable=True),
    _column("output_position_json", Text, nullable=True),
    _column("artifact_id", String(68), nullable=True),
    _column("committed_at", String(27)),
    _pk("checkpoints", "run_id", "node_id", "partition_key", "version"),
    _fk(
        "checkpoints",
        ["run_id", "node_id", "partition_key"],
        ["checkpoint_heads.run_id", "checkpoint_heads.node_id", "checkpoint_heads.partition_key"],
        "checkpoint_heads",
    ),
    _fk(
        "checkpoints",
        ["artifact_id", "run_id", "node_id", "partition_key"],
        [
            "artifact_manifests.artifact_id",
            "artifact_manifests.run_id",
            "artifact_manifests.node_id",
            "artifact_manifests.partition_key",
        ],
        "artifact_manifests",
    ),
    _positive("version", "version_range"),
    _positive("payload_schema_version", "payload_schema_version_range"),
    _json("source_cursor_json", "source_cursor_json_object", shape="object", nullable=True),
    _json("output_position_json", "output_position_json_object", shape="object", nullable=True),
    _utc("committed_at", "committed_at_utc"),
    _ix("checkpoints", "artifact_id", "run_id", "node_id", "partition_key"),
)

execution_events = Table(
    "execution_events",
    metadata,
    _column("run_id", String(68)),
    _column("sequence_number", Integer),
    _column("event_kind", String(96)),
    _column("occurred_at", String(27)),
    _column("subject_kind", String(48)),
    _column("subject_id", String(128), nullable=True),
    _column("correlation_id", String(96), nullable=True),
    _column("payload_schema_version", Integer),
    _column("payload_json", Text),
    _pk("execution_events", "run_id", "sequence_number"),
    _fk("execution_events", ["run_id"], ["runs.run_id"], "runs"),
    _positive("sequence_number", "sequence_number_range"),
    _bounded_text("event_kind", 96, "event_kind_size"),
    _utc("occurred_at", "occurred_at_utc"),
    _bounded_text("subject_kind", 48, "subject_kind_size"),
    _positive("payload_schema_version", "payload_schema_version_range"),
    _json("payload_json", "payload_json_object", shape="object"),
    CheckConstraint(
        "subject_id IS NULL OR (typeof(subject_id) = 'text' "
        "AND length(subject_id) BETWEEN 1 AND 128)",
        name="subject_id_size",
    ),
    CheckConstraint(
        "correlation_id IS NULL OR (typeof(correlation_id) = 'text' "
        "AND length(correlation_id) BETWEEN 1 AND 96)",
        name="correlation_id_size",
    ),
    _ix("execution_events", "occurred_at"),
    _ix("execution_events", "correlation_id"),
)

idempotency_records = Table(
    "idempotency_records",
    metadata,
    _column("scope", String(96)),
    _column("idempotency_key", String(128)),
    _column("request_sha256", String(64)),
    _column("status", String(32)),
    _column("response_schema_version", Integer, nullable=True),
    _column("response_json", Text, nullable=True),
    _column("created_at", String(27)),
    _column("updated_at", String(27)),
    _column("completed_at", String(27), nullable=True),
    _pk("idempotency_records", "scope", "idempotency_key"),
    _bounded_text("scope", 96, "scope_size"),
    _bounded_text("idempotency_key", 128, "idempotency_key_size"),
    _sha256("request_sha256", "request_sha256_shape"),
    _enum("status", (value.value for value in IdempotencyStatus), "status_values"),
    _json("response_json", "response_json_object", shape="object", nullable=True),
    _utc("created_at", "created_at_utc"),
    _utc("updated_at", "updated_at_utc"),
    _utc("completed_at", "completed_at_utc", nullable=True),
    CheckConstraint(
        "response_schema_version IS NULL OR (typeof(response_schema_version) = 'integer' "
        f"AND response_schema_version BETWEEN 1 AND {_MAX_SEQUENCE})",
        name="response_schema_version_range",
    ),
    CheckConstraint("updated_at >= created_at", name="updated_at_order"),
    CheckConstraint(
        "completed_at IS NULL OR completed_at >= created_at", name="completed_at_order"
    ),
    CheckConstraint(
        "(status = 'in_progress' AND response_schema_version IS NULL AND response_json IS NULL "
        "AND completed_at IS NULL) OR (status IN ('completed','failed') "
        "AND response_schema_version IS NOT NULL AND response_json IS NOT NULL "
        "AND completed_at IS NOT NULL)",
        name="response_coherence",
    ),
    _ix("idempotency_records", "status", "created_at"),
)

_reconciliation_count_columns = (
    "match_count",
    "missing_from_target_count",
    "missing_from_source_count",
    "field_mismatch_count",
    "duplicate_source_count",
    "duplicate_target_count",
    "duplicate_both_count",
)
reconciliation_summaries = Table(
    "reconciliation_summaries",
    metadata,
    _column("run_id", String(68)),
    *(_column(column, Integer, default="0") for column in _reconciliation_count_columns),
    _column("total_count", Integer),
    _column("source_fingerprint", String(64)),
    _column("target_fingerprint", String(64)),
    _column("reconciliation_fingerprint", String(64)),
    _column("analytical_query_version", Integer),
    _column("created_at", String(27)),
    _pk("reconciliation_summaries", "run_id"),
    _uq("reconciliation_summaries", "run_id", "reconciliation_fingerprint"),
    _fk("reconciliation_summaries", ["run_id"], ["runs.run_id"], "runs"),
    *(
        _nonnegative(column, f"{column}_range")
        for column in (*_reconciliation_count_columns, "total_count")
    ),
    CheckConstraint(
        "total_count = match_count + missing_from_target_count + missing_from_source_count "
        "+ field_mismatch_count + duplicate_source_count + duplicate_target_count "
        "+ duplicate_both_count",
        name="total_count_sum",
    ),
    _sha256("source_fingerprint", "source_fingerprint_shape"),
    _sha256("target_fingerprint", "target_fingerprint_shape"),
    _sha256("reconciliation_fingerprint", "reconciliation_fingerprint_shape"),
    _positive("analytical_query_version", "analytical_query_version_range"),
    _utc("created_at", "created_at_utc"),
)

_conflict_values = tuple(
    value
    for value in ReconciliationClassification
    if value is not ReconciliationClassification.MATCH
)
reconciliation_conflicts = Table(
    "reconciliation_conflicts",
    metadata,
    _column("conflict_id", String(68)),
    _column("run_id", String(68)),
    _column("canonical_key", String(64)),
    _column("classification", String(32)),
    _column("source_references_json", Text),
    _column("target_reference_json", Text, nullable=True),
    _column("field_differences_json", Text),
    _column("suggested_resolution", String(32), nullable=True),
    _column("created_at", String(27)),
    _pk("reconciliation_conflicts", "conflict_id"),
    _uq("reconciliation_conflicts", "run_id", "canonical_key"),
    _uq("reconciliation_conflicts", "conflict_id", "run_id", "canonical_key"),
    _fk("reconciliation_conflicts", ["run_id"], ["runs.run_id"], "runs"),
    _id("conflict_id", "cnf", "conflict_id_shape"),
    _bounded_text("canonical_key", 64, "canonical_key_size"),
    _enum("classification", (value.value for value in _conflict_values), "classification_values"),
    _json("source_references_json", "source_references_json_array", shape="array"),
    _json("target_reference_json", "target_reference_json_object", shape="object", nullable=True),
    _json("field_differences_json", "field_differences_json_array", shape="array"),
    CheckConstraint(
        "suggested_resolution IS NULL OR (typeof(suggested_resolution) = 'text' "
        "AND length(suggested_resolution) BETWEEN 1 AND 32)",
        name="suggested_resolution_size",
    ),
    _utc("created_at", "created_at_utc"),
    _ix("reconciliation_conflicts", "run_id", "classification", "canonical_key"),
)

repair_plans = Table(
    "repair_plans",
    metadata,
    _column("repair_plan_id", String(68)),
    _column("run_id", String(68)),
    _column("reconciliation_fingerprint", String(64)),
    _column("content_fingerprint", String(64)),
    _column("status", String(32)),
    _column("row_version", Integer, default="1"),
    _column("created_at", String(27)),
    _column("applying_at", String(27), nullable=True),
    _column("applied_at", String(27), nullable=True),
    _column("rejected_at", String(27), nullable=True),
    _column("failed_at", String(27), nullable=True),
    _column("failure_detail", Text, nullable=True),
    _pk("repair_plans", "repair_plan_id"),
    _uq("repair_plans", "repair_plan_id", "run_id"),
    _uq("repair_plans", "repair_plan_id", "reconciliation_fingerprint"),
    _uq("repair_plans", "run_id", "content_fingerprint"),
    _fk(
        "repair_plans",
        ["run_id", "reconciliation_fingerprint"],
        ["reconciliation_summaries.run_id", "reconciliation_summaries.reconciliation_fingerprint"],
        "reconciliation_summaries",
    ),
    _id("repair_plan_id", "rpl", "repair_plan_id_shape"),
    _sha256("reconciliation_fingerprint", "reconciliation_fingerprint_shape"),
    _sha256("content_fingerprint", "content_fingerprint_shape"),
    _enum("status", (value.value for value in RepairPlanStatus), "status_values"),
    _positive("row_version", "row_version_range"),
    _utc("created_at", "created_at_utc"),
    *(
        _utc(column, f"{column}_utc", nullable=True)
        for column in ("applying_at", "applied_at", "rejected_at", "failed_at")
    ),
    CheckConstraint("status <> 'applied' OR applied_at IS NOT NULL", name="applied_time"),
    CheckConstraint("status <> 'rejected' OR rejected_at IS NOT NULL", name="rejected_time"),
    CheckConstraint("status <> 'failed' OR failed_at IS NOT NULL", name="failed_time"),
    CheckConstraint(
        "failure_detail IS NULL OR (typeof(failure_detail) = 'text' "
        "AND length(failure_detail) BETWEEN 1 AND 4096)",
        name="failure_detail_size",
    ),
    _ix("repair_plans", "run_id", "reconciliation_fingerprint"),
)

repair_approvals = Table(
    "repair_approvals",
    metadata,
    _column("repair_plan_id", String(68)),
    _column("reconciliation_fingerprint", String(64)),
    _column("approved_by", String(128)),
    _column("approved_at", String(27)),
    _column("correlation_id", String(96)),
    _column("approval_schema_version", Integer),
    _column("detail_json", Text),
    _pk("repair_approvals", "repair_plan_id"),
    _fk(
        "repair_approvals",
        ["repair_plan_id", "reconciliation_fingerprint"],
        ["repair_plans.repair_plan_id", "repair_plans.reconciliation_fingerprint"],
        "repair_plans",
    ),
    _sha256("reconciliation_fingerprint", "reconciliation_fingerprint_shape"),
    _bounded_text("approved_by", 128, "approved_by_size"),
    _utc("approved_at", "approved_at_utc"),
    _bounded_text("correlation_id", 96, "correlation_id_size"),
    _positive("approval_schema_version", "approval_schema_version_range"),
    _json("detail_json", "detail_json_object", shape="object"),
    _ix("repair_approvals", "repair_plan_id", "reconciliation_fingerprint"),
)

repair_actions = Table(
    "repair_actions",
    metadata,
    _column("repair_action_id", String(68)),
    _column("repair_plan_id", String(68)),
    _column("run_id", String(68)),
    _column("conflict_id", String(68)),
    _column("canonical_key", String(64)),
    _column("action_kind", String(32)),
    _column("external_idempotency_key", String(128)),
    _column("before_sha256", String(64), nullable=True),
    _column("proposed_after_sha256", String(64)),
    _column("proposed_record_json", Text),
    _column("expected_target_record_json", Text, nullable=True),
    _column("mismatch_evidence_json", Text),
    _column("application_status", String(32), default="'pending'"),
    _column("application_result_json", Text, nullable=True),
    _column("target_version", Integer, nullable=True),
    _column("applied_at", String(27), nullable=True),
    _column("failed_at", String(27), nullable=True),
    _pk("repair_actions", "repair_action_id"),
    _uq("repair_actions", "external_idempotency_key"),
    _uq("repair_actions", "repair_plan_id", "canonical_key", "action_kind"),
    _fk(
        "repair_actions",
        ["repair_plan_id", "run_id"],
        ["repair_plans.repair_plan_id", "repair_plans.run_id"],
        "repair_plans",
    ),
    _fk(
        "repair_actions",
        ["conflict_id", "run_id", "canonical_key"],
        [
            "reconciliation_conflicts.conflict_id",
            "reconciliation_conflicts.run_id",
            "reconciliation_conflicts.canonical_key",
        ],
        "reconciliation_conflicts",
    ),
    _id("repair_action_id", "rac", "repair_action_id_shape"),
    _bounded_text("canonical_key", 64, "canonical_key_size"),
    _enum("action_kind", (value.value for value in RepairActionKind), "action_kind_values"),
    _bounded_text("external_idempotency_key", 128, "external_idempotency_key_size"),
    _sha256("before_sha256", "before_sha256_shape", nullable=True),
    _sha256("proposed_after_sha256", "proposed_after_sha256_shape"),
    _json("proposed_record_json", "proposed_record_json_object", shape="object"),
    _json(
        "expected_target_record_json", "expected_target_json_object", shape="object", nullable=True
    ),
    _json("mismatch_evidence_json", "mismatch_evidence_json_array", shape="array"),
    _enum(
        "application_status",
        (value.value for value in RepairActionApplicationStatus),
        "application_status_values",
    ),
    _json(
        "application_result_json", "application_result_json_object", shape="object", nullable=True
    ),
    _utc("applied_at", "applied_at_utc", nullable=True),
    _utc("failed_at", "failed_at_utc", nullable=True),
    CheckConstraint(
        "target_version IS NULL OR (typeof(target_version) = 'integer' "
        f"AND target_version BETWEEN 1 AND {_MAX_SEQUENCE})",
        name="target_version_range",
    ),
    CheckConstraint(
        "(action_kind = 'create_target' AND before_sha256 IS NULL "
        "AND expected_target_record_json IS NULL) OR "
        "(action_kind = 'update_target' AND before_sha256 IS NOT NULL "
        "AND expected_target_record_json IS NOT NULL)",
        name="action_shape",
    ),
    _ix("repair_actions", "repair_plan_id", "run_id"),
    _ix("repair_actions", "conflict_id", "run_id", "canonical_key"),
    CheckConstraint(
        "(application_status = 'pending' AND application_result_json IS NULL "
        "AND target_version IS NULL AND applied_at IS NULL AND failed_at IS NULL) OR "
        "(application_status = 'applied' AND application_result_json IS NOT NULL "
        "AND target_version >= 1 AND applied_at IS NOT NULL AND failed_at IS NULL) OR "
        "(application_status = 'failed' AND application_result_json IS NOT NULL "
        "AND failed_at IS NOT NULL AND applied_at IS NULL)",
        name="application_result_coherence",
    ),
)

audit_entries = Table(
    "audit_entries",
    metadata,
    _column("sequence_number", Integer),
    _column("actor", String(128)),
    _column("operation", String(96)),
    _column("object_kind", String(48)),
    _column("object_id", String(128), nullable=True),
    _column("correlation_id", String(96)),
    _column("occurred_at", String(27)),
    _column("detail_schema_version", Integer),
    _column("detail_json", Text),
    _pk("audit_entries", "sequence_number"),
    _positive("sequence_number", "sequence_number_range"),
    *(
        _bounded_text(column, maximum, f"{column}_size")
        for column, maximum in (
            ("actor", 128),
            ("operation", 96),
            ("object_kind", 48),
            ("correlation_id", 96),
        )
    ),
    _utc("occurred_at", "occurred_at_utc"),
    _positive("detail_schema_version", "detail_schema_version_range"),
    _json("detail_json", "detail_json_object", shape="object"),
    CheckConstraint(
        "object_id IS NULL OR (typeof(object_id) = 'text' AND length(object_id) BETWEEN 1 AND 128)",
        name="object_id_size",
    ),
    _ix("audit_entries", "occurred_at"),
    _ix("audit_entries", "correlation_id"),
    _ix("audit_entries", "object_kind", "object_id"),
    sqlite_autoincrement=True,
)

target_state_verifications = Table(
    "target_state_verifications",
    metadata,
    _column("verification_id", String(68)),
    _column("run_id", String(68)),
    _column("repair_plan_id", String(68), nullable=True),
    _column("reconciliation_fingerprint", String(64)),
    _column("plan_content_fingerprint", String(64), nullable=True),
    _column("observed_fingerprint", String(64)),
    _column("observed_fingerprint_version", Integer),
    _column("expected_fingerprint", String(64)),
    _column("verdict", String(32)),
    _column("observed_record_count", Integer),
    _column("expected_record_count", Integer),
    _column("observed_target_version", Integer),
    _column("observed_at", String(27)),
    _column("detail_json", Text),
    _pk("target_state_verifications", "verification_id"),
    _fk("target_state_verifications", ["run_id"], ["runs.run_id"], "runs"),
    _fk(
        "target_state_verifications",
        ["run_id", "reconciliation_fingerprint"],
        ["reconciliation_summaries.run_id", "reconciliation_summaries.reconciliation_fingerprint"],
        "reconciliation_summaries",
    ),
    _fk(
        "target_state_verifications",
        ["repair_plan_id"],
        ["repair_plans.repair_plan_id"],
        "repair_plans",
    ),
    _id("verification_id", "tgv", "verification_id_shape"),
    _id("repair_plan_id", "rpl", "repair_plan_id_shape", nullable=True),
    _bounded_text("run_id", 68, "run_id_size"),
    _sha256("reconciliation_fingerprint", "reconciliation_fingerprint_shape"),
    _sha256("plan_content_fingerprint", "plan_content_fingerprint_shape", nullable=True),
    _sha256("observed_fingerprint", "observed_fingerprint_shape"),
    _sha256("expected_fingerprint", "expected_fingerprint_shape"),
    _positive("observed_fingerprint_version", "observed_fingerprint_version_range"),
    _enum(
        "verdict",
        (value.value for value in TargetVerificationVerdict),
        "verdict_values",
    ),
    _nonnegative("observed_record_count", "observed_record_count_range"),
    _nonnegative("expected_record_count", "expected_record_count_range"),
    _nonnegative("observed_target_version", "observed_target_version_range"),
    _utc("observed_at", "observed_at_utc"),
    _json("detail_json", "detail_json_object", shape="object"),
    _ix("target_state_verifications", "run_id", "observed_at"),
    _ix("target_state_verifications", "run_id", "reconciliation_fingerprint"),
    _ix("target_state_verifications", "repair_plan_id"),
)

OPERATIONAL_TABLE_NAMES: Final[tuple[str, ...]] = tuple(metadata.tables)


class SystemMetadataRow(Base):
    """Internal row mapping for system metadata."""

    __table__ = system_metadata


class PipelineRow(Base):
    """Internal row mapping for pipeline identity metadata."""

    __table__ = pipelines


class PipelineVersionRow(Base):
    """Internal row mapping for immutable pipeline specifications."""

    __table__ = pipeline_versions


class ConnectorRow(Base):
    """Internal row mapping for connector definitions."""

    __table__ = connectors


class ConnectorSecretReferenceRow(Base):
    """Internal row mapping for unresolved connector secret references."""

    __table__ = connector_secret_references


class RunRow(Base):
    """Internal row mapping for captured pipeline runs."""

    __table__ = runs


class RunEventCounterRow(Base):
    """Internal row mapping for transactional event sequence allocation."""

    __table__ = run_event_counters


class RunNodeRow(Base):
    """Internal row mapping for per-node run aggregates."""

    __table__ = run_nodes


class WorkItemRow(Base):
    """Internal row mapping for durable work state and active claims."""

    __table__ = work_items


class WorkAttemptRow(Base):
    """Internal row mapping for completed immutable attempt history."""

    __table__ = work_attempts


class ArtifactManifestRow(Base):
    """Internal row mapping for committed artifact identities."""

    __table__ = artifact_manifests


class CheckpointHeadRow(Base):
    """Internal row mapping for the current checkpoint version."""

    __table__ = checkpoint_heads


class CheckpointRow(Base):
    """Internal row mapping for append-only checkpoint history."""

    __table__ = checkpoints


class ExecutionEventRow(Base):
    """Internal row mapping for durable execution events."""

    __table__ = execution_events


class IdempotencyRecordRow(Base):
    """Internal row mapping for command replay state."""

    __table__ = idempotency_records


class ReconciliationSummaryRow(Base):
    """Internal row mapping for final reconciliation counts and fingerprints."""

    __table__ = reconciliation_summaries


class ReconciliationConflictRow(Base):
    """Internal row mapping for immutable conflict evidence."""

    __table__ = reconciliation_conflicts


class RepairPlanRow(Base):
    """Internal row mapping for immutable repair contents and lifecycle state."""

    __table__ = repair_plans


class RepairApprovalRow(Base):
    """Internal row mapping for immutable repair approval facts."""

    __table__ = repair_approvals


class RepairActionRow(Base):
    """Internal row mapping for repair effects and application results."""

    __table__ = repair_actions


class AuditEntryRow(Base):
    """Internal row mapping for append-only administrative audit facts."""

    __table__ = audit_entries


class TargetStateVerificationRow(Base):
    """Internal row mapping for immutable target-state verification facts."""

    __table__ = target_state_verifications


ORM_ROW_TYPES: Final[tuple[type[Base], ...]] = (
    SystemMetadataRow,
    PipelineRow,
    PipelineVersionRow,
    ConnectorRow,
    ConnectorSecretReferenceRow,
    RunRow,
    RunEventCounterRow,
    RunNodeRow,
    WorkItemRow,
    WorkAttemptRow,
    ArtifactManifestRow,
    CheckpointHeadRow,
    CheckpointRow,
    ExecutionEventRow,
    IdempotencyRecordRow,
    ReconciliationSummaryRow,
    ReconciliationConflictRow,
    RepairPlanRow,
    RepairApprovalRow,
    RepairActionRow,
    AuditEntryRow,
    TargetStateVerificationRow,
)
