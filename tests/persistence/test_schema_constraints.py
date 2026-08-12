"""Behavior tests for SQLite storage constraints and relational integrity."""

from collections.abc import Generator, Iterator
from contextlib import contextmanager

import pytest
from sqlalchemy import Connection, create_engine
from sqlalchemy.exc import IntegrityError

from paritygrid.adapters.persistence.schema import metadata
from paritygrid.adapters.persistence.values import CanonicalStorageJson
from paritygrid.domain.models import UtcTimestamp

UTC = "2026-08-12T12:00:00.000000Z"
HASH_A = "a" * 64
HASH_B = "b" * 64


@pytest.fixture
def connection() -> Iterator[Connection]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.connect() as active:
        active.exec_driver_sql("PRAGMA foreign_keys = ON")
        metadata.create_all(active)
        yield active
    engine.dispose()


@contextmanager
def rejected(connection: Connection) -> Generator[None]:
    transaction = connection.begin_nested()
    with pytest.raises(IntegrityError):
        yield
    transaction.rollback()


def _pipeline(connection: Connection, identifier: str = "pip_alpha") -> None:
    connection.exec_driver_sql(
        "INSERT INTO pipelines "
        "(pipeline_id, display_name, created_at, row_version) VALUES (?, ?, ?, ?)",
        (identifier, "Pipeline", UTC, 1),
    )


def _pipeline_version(connection: Connection, identifier: str = "pip_alpha") -> None:
    _pipeline(connection, identifier)
    connection.exec_driver_sql(
        "INSERT INTO pipeline_versions "
        "(pipeline_id, version_number, specification_json, specification_sha256, "
        "planner_format_version, published_at) VALUES (?, ?, ?, ?, ?, ?)",
        (identifier, 1, "{}", HASH_A, 1, UTC),
    )


def _run(
    connection: Connection, identifier: str = "run_alpha", pipeline: str = "pip_alpha"
) -> None:
    _pipeline_version(connection, pipeline)
    connection.exec_driver_sql(
        "INSERT INTO runs "
        "(run_id, pipeline_id, pipeline_version_number, runner_kind, "
        "runner_configuration_json, state, row_version, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (identifier, pipeline, 1, "sequential", "{}", "queued", 1, UTC),
    )


def _run_node(connection: Connection, run_id: str = "run_alpha") -> None:
    _run(connection, run_id)
    connection.exec_driver_sql(
        "INSERT INTO run_nodes (run_id, node_id, state, row_version) VALUES (?, ?, ?, ?)",
        (run_id, "nod_source", "pending", 1),
    )


def _work_item(connection: Connection) -> None:
    _run_node(connection)
    connection.exec_driver_sql(
        "INSERT INTO work_items "
        "(work_item_id, run_id, node_id, partition_key, state, row_version, "
        "completed_attempt_count, expected_checkpoint_version, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("wrk_alpha", "run_alpha", "nod_source", "part-1", "pending", 1, 0, 0, UTC, UTC),
    )


@pytest.mark.parametrize(
    ("sql", "parameters"),
    [
        (
            "INSERT INTO system_metadata (key, value, updated_at) VALUES (?, ?, ?)",
            (b"wrong-storage", "value", UTC),
        ),
        (
            "INSERT INTO pipelines "
            "(pipeline_id, display_name, created_at, row_version) VALUES (?, ?, ?, ?)",
            ("pip_alpha", "Pipeline", "2026-08-12 12:00:00", 1),
        ),
        (
            "INSERT INTO pipelines "
            "(pipeline_id, display_name, created_at, row_version) VALUES (?, ?, ?, ?)",
            ("pip_alpha", "Pipeline", UTC, "one"),
        ),
    ],
)
def test_storage_class_and_timestamp_constraints_reject_permissive_sqlite_values(
    connection: Connection, sql: str, parameters: tuple[object, ...]
) -> None:
    with rejected(connection):
        connection.exec_driver_sql(sql, parameters)


@pytest.mark.parametrize(
    "invalid",
    [
        "0000-08-12T12:00:00.000000Z",
        "2026-00-12T12:00:00.000000Z",
        "2026-13-12T12:00:00.000000Z",
        "2026-08-00T12:00:00.000000Z",
        "2026-08-32T12:00:00.000000Z",
        "2026-08-12T24:00:00.000000Z",
        "2026-08-12T12:60:00.000000Z",
        "2026-08-12T12:00:60.000000Z",
    ],
)
def test_timestamp_constraints_reject_out_of_range_components(
    connection: Connection, invalid: str
) -> None:
    with rejected(connection):
        connection.exec_driver_sql(
            "INSERT INTO pipelines "
            "(pipeline_id, display_name, created_at, row_version) VALUES (?, ?, ?, ?)",
            ("pip_alpha", "Pipeline", invalid, 1),
        )


def test_domain_parser_enforces_calendar_validity_beyond_database_shape(
    connection: Connection,
) -> None:
    invalid_calendar_date = "2026-02-31T12:00:00.000000Z"
    connection.exec_driver_sql(
        "INSERT INTO pipelines "
        "(pipeline_id, display_name, created_at, row_version) VALUES (?, ?, ?, ?)",
        ("pip_alpha", "Pipeline", invalid_calendar_date, 1),
    )
    stored = connection.exec_driver_sql(
        "SELECT created_at FROM pipelines WHERE pipeline_id = 'pip_alpha'"
    ).scalar_one()
    assert stored == invalid_calendar_date
    with pytest.raises(ValueError, match="invalid date"):
        UtcTimestamp.parse(stored)


def test_repository_codec_enforces_canonical_json_beyond_database_shape(
    connection: Connection,
) -> None:
    noncanonical_json = '{"b": 2, "a": 1}'
    _pipeline(connection)
    connection.exec_driver_sql(
        "INSERT INTO pipeline_versions "
        "(pipeline_id, version_number, specification_json, specification_sha256, "
        "planner_format_version, published_at) VALUES (?, ?, ?, ?, ?, ?)",
        ("pip_alpha", 1, noncanonical_json, HASH_A, 1, UTC),
    )
    stored = connection.exec_driver_sql(
        "SELECT specification_json FROM pipeline_versions WHERE pipeline_id = 'pip_alpha'"
    ).scalar_one()
    assert stored == noncanonical_json
    with pytest.raises(ValueError, match="canonical representation"):
        CanonicalStorageJson(stored)


def test_nullable_scenario_seed_rejects_noninteger_storage(connection: Connection) -> None:
    _pipeline_version(connection)
    with rejected(connection):
        connection.exec_driver_sql(
            "INSERT INTO runs "
            "(run_id, pipeline_id, pipeline_version_number, runner_kind, "
            "runner_configuration_json, state, row_version, scenario_seed, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("run_alpha", "pip_alpha", 1, "sequential", "{}", "queued", 1, "seed", UTC),
        )


@pytest.mark.parametrize("invalid", ["A" * 64, "g" * 64, "a" * 63, 7])
def test_sha256_constraints_reject_invalid_storage_values(
    connection: Connection, invalid: object
) -> None:
    _pipeline(connection)
    with rejected(connection):
        connection.exec_driver_sql(
            "INSERT INTO pipeline_versions "
            "(pipeline_id, version_number, specification_json, specification_sha256, "
            "planner_format_version, published_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("pip_alpha", 1, "{}", invalid, 1, UTC),
        )


@pytest.mark.parametrize("invalid", ["[]", '"value"', "not-json", 7])
def test_json_object_constraints_reject_wrong_shape_and_storage_class(
    connection: Connection, invalid: object
) -> None:
    _pipeline(connection)
    with rejected(connection):
        connection.exec_driver_sql(
            "INSERT INTO pipeline_versions "
            "(pipeline_id, version_number, specification_json, specification_sha256, "
            "planner_format_version, published_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("pip_alpha", 1, invalid, HASH_A, 1, UTC),
        )


@pytest.mark.parametrize("invalid", ["{}", '"value"', "not-json", 7])
def test_json_array_constraints_reject_wrong_shape_and_storage_class(
    connection: Connection, invalid: object
) -> None:
    _run(connection)
    with rejected(connection):
        connection.exec_driver_sql(
            "INSERT INTO reconciliation_conflicts "
            "(conflict_id, run_id, canonical_key, classification, source_references_json, "
            "field_differences_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                "cnf_alpha",
                "run_alpha",
                "SKU-1",
                "missing_from_target",
                invalid,
                "[]",
                UTC,
            ),
        )


def test_pipeline_version_composite_foreign_key_rejects_hybrid_parent(
    connection: Connection,
) -> None:
    _pipeline_version(connection, "pip_alpha")
    _pipeline_version(connection, "pip_beta")
    with rejected(connection):
        connection.exec_driver_sql(
            "INSERT INTO runs "
            "(run_id, pipeline_id, pipeline_version_number, runner_kind, "
            "runner_configuration_json, state, row_version, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("run_alpha", "pip_beta", 2, "sequential", "{}", "queued", 1, UTC),
        )


def test_work_and_checkpoint_composite_foreign_keys_reject_hybrid_parents(
    connection: Connection,
) -> None:
    _work_item(connection)
    connection.exec_driver_sql(
        "INSERT INTO run_nodes (run_id, node_id, state, row_version) VALUES (?, ?, ?, ?)",
        ("run_alpha", "nod_other", "pending", 1),
    )
    with rejected(connection):
        connection.exec_driver_sql(
            "INSERT INTO checkpoint_heads "
            "(run_id, node_id, partition_key, current_version, updated_at, row_version) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("run_alpha", "nod_other", "part-1", 0, UTC, 1),
        )


def test_checkpoint_artifact_reference_cannot_cross_execution_partition(
    connection: Connection,
) -> None:
    for suffix in ("alpha", "beta"):
        run_id = f"run_{suffix}"
        pipeline_id = f"pip_{suffix}"
        node_id = f"nod_{suffix}"
        work_item_id = f"wrk_{suffix}"
        partition_key = f"part-{suffix}"
        _run(connection, run_id, pipeline_id)
        connection.exec_driver_sql(
            "INSERT INTO run_nodes (run_id, node_id, state, row_version) VALUES (?, ?, ?, ?)",
            (run_id, node_id, "pending", 1),
        )
        connection.exec_driver_sql(
            "INSERT INTO work_items "
            "(work_item_id, run_id, node_id, partition_key, state, row_version, "
            "completed_attempt_count, expected_checkpoint_version, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (work_item_id, run_id, node_id, partition_key, "pending", 1, 0, 0, UTC, UTC),
        )
        connection.exec_driver_sql(
            "INSERT INTO checkpoint_heads "
            "(run_id, node_id, partition_key, current_version, updated_at, row_version) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (run_id, node_id, partition_key, 0, UTC, 1),
        )
        connection.exec_driver_sql(
            "INSERT INTO artifact_manifests "
            "(artifact_id, run_id, node_id, partition_key, relative_path, media_type, "
            "schema_version, byte_size, row_count, sha256, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f"art_{suffix}",
                run_id,
                node_id,
                partition_key,
                f"runs/{run_id}/{suffix}.json",
                "application/json",
                1,
                2,
                1,
                HASH_A if suffix == "alpha" else HASH_B,
                UTC,
            ),
        )

    with rejected(connection):
        connection.exec_driver_sql(
            "INSERT INTO checkpoints "
            "(run_id, node_id, partition_key, version, payload_schema_version, "
            "artifact_id, committed_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("run_alpha", "nod_alpha", "part-alpha", 1, 1, "art_beta", UTC),
        )


def test_active_attempt_shape_and_transient_leased_state_are_enforced(
    connection: Connection,
) -> None:
    _run_node(connection)
    with rejected(connection):
        connection.exec_driver_sql(
            "INSERT INTO work_items "
            "(work_item_id, run_id, node_id, partition_key, state, row_version, "
            "completed_attempt_count, expected_checkpoint_version, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("wrk_leased", "run_alpha", "nod_source", "part-1", "leased", 1, 0, 0, UTC, UTC),
        )
    with rejected(connection):
        connection.exec_driver_sql(
            "INSERT INTO work_items "
            "(work_item_id, run_id, node_id, partition_key, state, row_version, "
            "completed_attempt_count, expected_checkpoint_version, lease_owner, "
            "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "wrk_partial",
                "run_alpha",
                "nod_source",
                "part-2",
                "running",
                1,
                0,
                0,
                "worker-1",
                UTC,
                UTC,
            ),
        )


def test_exact_reconciliation_fingerprint_binds_plan_and_approval(
    connection: Connection,
) -> None:
    _run(connection)
    connection.exec_driver_sql(
        "INSERT INTO reconciliation_summaries "
        "(run_id, total_count, source_fingerprint, target_fingerprint, "
        "reconciliation_fingerprint, analytical_query_version, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("run_alpha", 0, HASH_A, HASH_A, HASH_A, 1, UTC),
    )
    connection.exec_driver_sql(
        "INSERT INTO repair_plans "
        "(repair_plan_id, run_id, reconciliation_fingerprint, content_fingerprint, "
        "status, row_version, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("rpl_alpha", "run_alpha", HASH_A, HASH_B, "proposed", 1, UTC),
    )
    with rejected(connection):
        connection.exec_driver_sql(
            "INSERT INTO repair_approvals "
            "(repair_plan_id, reconciliation_fingerprint, approved_by, approved_at, "
            "correlation_id, approval_schema_version, detail_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("rpl_alpha", HASH_B, "operator", UTC, "correlation-1", 1, "{}"),
        )


def test_repair_action_composite_keys_reject_cross_run_hybrids(connection: Connection) -> None:
    _run(connection, "run_alpha", "pip_alpha")
    _run(connection, "run_beta", "pip_beta")
    for run_id, plan_id, conflict_id, digest in (
        ("run_alpha", "rpl_alpha", "cnf_alpha", HASH_A),
        ("run_beta", "rpl_beta", "cnf_beta", HASH_B),
    ):
        connection.exec_driver_sql(
            "INSERT INTO reconciliation_summaries "
            "(run_id, total_count, source_fingerprint, target_fingerprint, "
            "reconciliation_fingerprint, analytical_query_version, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (run_id, 0, digest, digest, digest, 1, UTC),
        )
        connection.exec_driver_sql(
            "INSERT INTO reconciliation_conflicts "
            "(conflict_id, run_id, canonical_key, classification, source_references_json, "
            "field_differences_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                conflict_id,
                run_id,
                f"SKU-{run_id[-1].upper()}",
                "missing_from_target",
                "[]",
                "[]",
                UTC,
            ),
        )
        connection.exec_driver_sql(
            "INSERT INTO repair_plans "
            "(repair_plan_id, run_id, reconciliation_fingerprint, content_fingerprint, "
            "status, row_version, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (plan_id, run_id, digest, HASH_B if digest == HASH_A else HASH_A, "proposed", 1, UTC),
        )

    with rejected(connection):
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
                "cnf_beta",
                "SKU-A",
                "create_target",
                "effect-alpha",
                HASH_A,
                "{}",
                "[]",
                "pending",
            ),
        )


def test_repair_action_conflict_reference_cannot_cross_canonical_keys(
    connection: Connection,
) -> None:
    _run(connection)
    connection.exec_driver_sql(
        "INSERT INTO reconciliation_summaries "
        "(run_id, field_mismatch_count, total_count, source_fingerprint, target_fingerprint, "
        "reconciliation_fingerprint, analytical_query_version, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("run_alpha", 2, 2, HASH_A, HASH_A, HASH_A, 1, UTC),
    )
    for suffix in ("A", "B"):
        connection.exec_driver_sql(
            "INSERT INTO reconciliation_conflicts "
            "(conflict_id, run_id, canonical_key, classification, source_references_json, "
            "field_differences_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                f"cnf_{suffix.lower()}xx",
                "run_alpha",
                f"SKU-{suffix}",
                "missing_from_target",
                "[]",
                "[]",
                UTC,
            ),
        )
    connection.exec_driver_sql(
        "INSERT INTO repair_plans "
        "(repair_plan_id, run_id, reconciliation_fingerprint, content_fingerprint, "
        "status, row_version, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("rpl_alpha", "run_alpha", HASH_A, HASH_B, "proposed", 1, UTC),
    )

    with rejected(connection):
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
                "cnf_axx",
                "SKU-B",
                "create_target",
                "effect-alpha",
                HASH_A,
                "{}",
                "[]",
                "pending",
            ),
        )


def test_secret_reference_stores_only_portable_environment_names(connection: Connection) -> None:
    connection.exec_driver_sql(
        "INSERT INTO connectors "
        "(connector_id, kind, display_name, configuration_json, capabilities_json, "
        "revision, created_at, updated_at, row_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("con_alpha", "http.async", "Source", "{}", "{}", 1, UTC, UTC, 1),
    )
    with rejected(connection):
        connection.exec_driver_sql(
            "INSERT INTO connector_secret_references "
            "(connector_id, reference_name, environment_variable_name, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("con_alpha", "api-token", "resolved-secret-value", UTC),
        )


@pytest.mark.parametrize("identifier", ["pip_---", "pip_a--b", "pip_abc-"])
def test_identifier_constraints_reject_noncanonical_grouping(
    connection: Connection, identifier: str
) -> None:
    with rejected(connection):
        _pipeline(connection, identifier)


@pytest.mark.parametrize("key", ["Schema.Version", "schema..version", "schema.-version", "schema-"])
def test_system_metadata_key_requires_canonical_lowercase_grouping(
    connection: Connection, key: str
) -> None:
    with rejected(connection):
        connection.exec_driver_sql(
            "INSERT INTO system_metadata (key, value, updated_at) VALUES (?, ?, ?)",
            (key, "value", UTC),
        )


@pytest.mark.parametrize(
    "reference_name", ["ApiToken", "api..token", "token._primary", "api-token-"]
)
def test_secret_reference_name_requires_canonical_lowercase_grouping(
    connection: Connection, reference_name: str
) -> None:
    connection.exec_driver_sql(
        "INSERT INTO connectors "
        "(connector_id, kind, display_name, configuration_json, capabilities_json, "
        "revision, created_at, updated_at, row_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("con_alpha", "http.async", "Source", "{}", "{}", 1, UTC, UTC, 1),
    )
    with rejected(connection):
        connection.exec_driver_sql(
            "INSERT INTO connector_secret_references "
            "(connector_id, reference_name, environment_variable_name, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("con_alpha", reference_name, "API_TOKEN", UTC),
        )
