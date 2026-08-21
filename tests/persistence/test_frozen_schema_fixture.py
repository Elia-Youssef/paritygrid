"""Released v0001 persistence fixture integration tests."""

import hashlib
import json
import sqlite3
import unicodedata
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import Connection
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from paritygrid.adapters.persistence import SQLiteDatabaseConfig, create_sqlite_engine
from paritygrid.adapters.persistence.migration import (
    HEAD_REVISION,
    MigrationReport,
    upgrade_to_head,
)
from paritygrid.quality.frozen_schema import reconstruct_fixture

FIXTURE_DIRECTORY = Path(__file__).parents[1] / "fixtures/persistence/v0001"
SCHEMA_PATH = FIXTURE_DIRECTORY / "schema.sql"
SEED_PATH = FIXTURE_DIRECTORY / "seed.sql"
MANIFEST_PATH = FIXTURE_DIRECTORY / "manifest.json"

EXPECTED_FILE_HASHES = {
    "manifest.json": "74495cba7d7ce973d5544a0a2acd363acc4ca56b38eb3f5707a81064427dbce0",
    "schema.sql": "50ff2626553f6e5250e217b79f06fc3a957a59ab2ffc3341fbd23b52fdcc243c",
    "seed.sql": "4d8d9221c63c2a38dd85c5b68807a993ca8962a9193fc4d883c901d9374f10d2",
}
EXPECTED_LOGICAL_ROWS_HASH = "a63ab101fb3efbb0d09d6a8e9685ec0d96ff2333c17bb35ffde664d295e37d81"
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
EXPECTED_ROW_COUNTS = {
    "artifact_manifests": 1,
    "audit_entries": 1,
    "checkpoint_heads": 2,
    "checkpoints": 2,
    "connector_secret_references": 1,
    "connectors": 2,
    "execution_events": 4,
    "idempotency_records": 2,
    "pipeline_versions": 1,
    "pipelines": 1,
    "reconciliation_conflicts": 1,
    "reconciliation_summaries": 1,
    "repair_actions": 2,
    "repair_approvals": 1,
    "repair_plans": 2,
    "run_event_counters": 2,
    "run_nodes": 2,
    "runs": 2,
    "system_metadata": 4,
    "work_attempts": 2,
    "work_items": 2,
}
V0001_REVISION = "0001_operational"

WHOLE_TABLE_UPDATE_ATTACKS = (
    (
        "UPDATE artifact_manifests SET artifact_id=artifact_id "
        "WHERE artifact_id='art_inventory-output'"
    ),
    "UPDATE audit_entries SET actor=actor WHERE sequence_number=1",
    "UPDATE checkpoints SET version=version WHERE run_id='run_completed-demo' AND version=1",
    (
        "UPDATE connector_secret_references SET reference_name=reference_name "
        "WHERE connector_id='con_async-source' AND reference_name='api_token'"
    ),
    (
        "UPDATE execution_events SET event_kind=event_kind "
        "WHERE run_id='run_completed-demo' AND sequence_number=1"
    ),
    (
        "UPDATE pipeline_versions SET version_number=version_number "
        "WHERE pipeline_id='pip_inventory-demo' AND version_number=1"
    ),
    (
        "UPDATE reconciliation_conflicts SET canonical_key=canonical_key "
        "WHERE conflict_id='cnf_missing-widget'"
    ),
    (
        "UPDATE reconciliation_summaries SET total_count=total_count "
        "WHERE run_id='run_completed-demo'"
    ),
    (
        "UPDATE repair_approvals SET approved_by=approved_by "
        "WHERE repair_plan_id='rpl_harbor-repair'"
    ),
    (
        "UPDATE work_attempts SET worker_identity=worker_identity "
        "WHERE work_item_id='wrk_completed-partition' AND attempt_number=1"
    ),
)

IMMUTABLE_COLUMN_ATTACKS = (
    (
        "UPDATE checkpoint_heads SET partition_key='catalog-page-9999' "
        "WHERE run_id='run_active-demo' AND node_id='nod_inventory-live'"
    ),
    "UPDATE connectors SET connector_id='con_changed' WHERE connector_id='con_async-source'",
    (
        "UPDATE idempotency_records SET request_sha256='c' || substr(request_sha256,2) "
        "WHERE scope='run:create' AND idempotency_key='fixture-run-active'"
    ),
    "UPDATE pipelines SET pipeline_id='pip_changed' WHERE pipeline_id='pip_inventory-demo'",
    (
        "UPDATE repair_actions SET action_kind='update_target' "
        "WHERE repair_action_id='rac_pending-harbor-lamp'"
    ),
    (
        "UPDATE repair_plans SET content_fingerprint='c' || substr(content_fingerprint,2) "
        "WHERE repair_plan_id='rpl_pending-repair'"
    ),
    (
        "UPDATE run_event_counters SET run_id='run_changed', next_sequence_number=5 "
        "WHERE run_id='run_active-demo'"
    ),
    "UPDATE run_nodes SET node_id='nod_changed' WHERE run_id='run_active-demo'",
    "UPDATE runs SET runner_kind='asyncio' WHERE run_id='run_completed-demo'",
    "UPDATE system_metadata SET key='changed_key' WHERE key='fixture_revision'",
    (
        "UPDATE work_items SET partition_key='catalog-page-9999' "
        "WHERE work_item_id='wrk_active-partition'"
    ),
)

TERMINAL_UPDATE_ATTACKS = (
    (
        "UPDATE idempotency_records SET status=status WHERE scope='run:create' "
        "AND idempotency_key='fixture-run-completed'"
    ),
    (
        "UPDATE repair_actions SET application_status=application_status "
        "WHERE repair_action_id='rac_create-harbor-lamp'"
    ),
    "UPDATE repair_plans SET status=status WHERE repair_plan_id='rpl_harbor-repair'",
)


def _fixture_bytes() -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in (MANIFEST_PATH, SCHEMA_PATH, SEED_PATH)}


def _reconstruct(connection: sqlite3.Connection) -> None:
    reconstruct_fixture(connection, SCHEMA_PATH.read_bytes(), SEED_PATH.read_bytes())


def _snapshot(
    connection: Connection,
    *,
    upgraded_runs: bool = False,
) -> tuple[tuple[str, tuple[tuple[object, ...], ...]], ...]:
    """Read every logical row under its revision-appropriate runs projection.

    After the 0002 upgrade, the runs digest is read under the new storage name
    together with its explicit version, so preservation comparisons stay exact
    instead of depending on dropped v0001 column names.
    """
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    result: list[tuple[str, tuple[tuple[object, ...], ...]]] = []
    for table in sorted(EXPECTED_ROW_COUNTS):
        description = manifest["tables"][table]
        columns = [str(column) for column in description["columns"]]
        primary_key = [str(column) for column in description["primary_key_columns"]]
        order = primary_key or columns
        if table == "runs" and upgraded_runs:
            columns = [
                (
                    "execution_evidence_fingerprint"
                    if column == "final_reconciliation_fingerprint"
                    else column
                )
                for column in columns
            ] + ["execution_evidence_fingerprint_version"]
        columns_sql = ", ".join(f'"{column}"' for column in columns)
        order_sql = ", ".join(f'"{column}"' for column in order)
        rows = tuple(
            tuple(row)
            for row in connection.exec_driver_sql(
                f'SELECT {columns_sql} FROM "{table}" ORDER BY {order_sql}'
            )
        )
        result.append((table, rows))
    return tuple(result)


def _with_upgraded_runs_projection(
    snapshot: tuple[tuple[str, tuple[tuple[object, ...], ...]], ...],
) -> tuple[tuple[str, tuple[tuple[object, ...], ...]], ...]:
    """Map a v0001 snapshot onto the upgraded runs projection.

    Every non-runs table is unchanged. A preserved runs digest gains its
    backfilled version 2, and a null digest stays null without a version.
    """
    mapped: list[tuple[str, tuple[tuple[object, ...], ...]]] = []
    for table, rows in snapshot:
        if table != "runs":
            mapped.append((table, rows))
            continue
        upgraded_rows = tuple((*tuple(row), 2 if row[-1] is not None else None) for row in rows)
        mapped.append((table, upgraded_rows))
    return tuple(mapped)


@pytest.fixture
def reconstructed_engine(tmp_path: Path) -> Iterator[Engine]:
    database = tmp_path / "released-v0001.db"
    raw_connection = sqlite3.connect(database)
    try:
        _reconstruct(raw_connection)
    finally:
        raw_connection.close()
    engine = create_sqlite_engine(SQLiteDatabaseConfig(database_path=database))
    yield engine
    engine.dispose()


def test_committed_files_match_independently_reviewed_hashes_and_manifest() -> None:
    fixture_bytes = _fixture_bytes()
    manifest = json.loads(fixture_bytes["manifest.json"])

    assert set(fixture_bytes) == set(EXPECTED_FILE_HASHES)
    assert {
        name: hashlib.sha256(content).hexdigest() for name, content in fixture_bytes.items()
    } == EXPECTED_FILE_HASHES
    assert manifest["revision"] == "0001_operational"
    assert manifest["table_count"] == 21
    assert manifest["schema_inventory"] == EXPECTED_SCHEMA_INVENTORY
    assert manifest["logical_rows_sha256"] == EXPECTED_LOGICAL_ROWS_HASH
    assert manifest["invariants"] == {
        "checkpoint_head_matches_history": True,
        "event_counter_matches_history": True,
        "repair_approval_and_action_match_plan": True,
    }
    assert len(manifest["foreign_key_witnesses"]) == 18
    assert all(witness["non_null_rows"] >= 1 for witness in manifest["foreign_key_witnesses"])
    assert all(
        witness["matching_rows"] == witness["non_null_rows"]
        and len(witness["columns"]) == len(witness["referenced_columns"])
        and len(witness["witness_values"]) == len(witness["columns"])
        and witness["referenced_table"] in EXPECTED_ROW_COUNTS
        for witness in manifest["foreign_key_witnesses"]
    )
    assert {
        table: description["row_count"] for table, description in manifest["tables"].items()
    } == EXPECTED_ROW_COUNTS
    assert manifest["files"] == {
        "schema.sql": {"bytes": 99010, "sha256": EXPECTED_FILE_HASHES["schema.sql"]},
        "seed.sql": {"bytes": 13866, "sha256": EXPECTED_FILE_HASHES["seed.sql"]},
    }
    assert manifest["sentinel_projections"] == {
        "active_attempt": ["running", 1, "worker-01"],
        "applied_repair": ["applied", "fixture-operator", "applied", 1],
        "secret_environment_name": "PARITYGRID_DEMO_API_TOKEN",
        "unicode_vectors": [["Café — ميناء"], ["Cafe\u0301 — مرسى"]],
    }


def test_fixture_text_contract_is_portable_and_contains_no_runtime_database() -> None:
    for name, content in _fixture_bytes().items():
        assert not content.startswith(b"\xef\xbb\xbf"), name
        assert b"\r" not in content, name
        assert content.endswith(b"\n"), name
        content.decode("utf-8")
    assert not tuple(FIXTURE_DIRECTORY.glob("*.db*"))
    assert not tuple(FIXTURE_DIRECTORY.glob("*.wal"))
    assert not tuple(FIXTURE_DIRECTORY.glob("*.shm"))


def test_reconstruction_integrity_counts_and_sentinel_projections(
    reconstructed_engine: Engine,
) -> None:
    with reconstructed_engine.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA quick_check").all() == [("ok",)]
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
        counts = {
            table: cast(
                int, connection.exec_driver_sql(f'SELECT COUNT(*) FROM "{table}"').scalar_one()
            )
            for table in EXPECTED_ROW_COUNTS
        }
        assert counts == EXPECTED_ROW_COUNTS
        assert (
            connection.exec_driver_sql(
                "SELECT environment_variable_name FROM connector_secret_references"
            ).scalar_one()
            == "PARITYGRID_DEMO_API_TOKEN"
        )
        assert connection.exec_driver_sql(
            "SELECT state, active_attempt_number, active_worker_identity FROM work_items "
            "WHERE work_item_id = 'wrk_active-partition'"
        ).one() == ("running", 1, "worker-01")
        assert connection.exec_driver_sql(
            "SELECT status, applied_at FROM repair_plans WHERE repair_plan_id = 'rpl_harbor-repair'"
        ).one() == ("applied", "2026-08-12T12:10:00.000000Z")
        unicode_rows = {
            cast(str, row[0]): cast(str, row[1])
            for row in connection.exec_driver_sql(
                "SELECT key,value FROM system_metadata WHERE key LIKE 'unicode_%' ORDER BY key"
            ).all()
        }
        assert unicode_rows == {
            "unicode_nfc": "Café — ميناء",
            "unicode_nfd": "Cafe\u0301 — مرسى",
        }
        assert unicodedata.is_normalized("NFC", unicode_rows["unicode_nfc"])
        assert unicodedata.is_normalized("NFD", unicode_rows["unicode_nfd"])
        connection.rollback()


def test_all_foreign_keys_have_non_null_seed_witnesses(reconstructed_engine: Engine) -> None:
    witnesses: list[tuple[str, int, int]] = []
    with reconstructed_engine.connect() as connection:
        for table in sorted(EXPECTED_ROW_COUNTS):
            rows = connection.exec_driver_sql(f'PRAGMA foreign_key_list("{table}")').all()
            for foreign_key_id in sorted({int(row[0]) for row in rows}):
                columns = [str(row[3]) for row in rows if int(row[0]) == foreign_key_id]
                predicate = " AND ".join(f'"{column}" IS NOT NULL' for column in columns)
                count = cast(
                    int,
                    connection.exec_driver_sql(
                        f'SELECT COUNT(*) FROM "{table}" WHERE {predicate}'
                    ).scalar_one(),
                )
                witnesses.append((table, foreign_key_id, count))
        connection.rollback()

    assert len(witnesses) == 18
    assert all(count >= 1 for _, _, count in witnesses)


def test_cross_row_checkpoint_event_and_repair_facts_are_coherent(
    reconstructed_engine: Engine,
) -> None:
    with reconstructed_engine.connect() as connection:
        checkpoint_mismatches = connection.exec_driver_sql(
            "SELECT h.run_id FROM checkpoint_heads h "
            "WHERE h.current_version <> COALESCE((SELECT MAX(c.version) FROM checkpoints c "
            "WHERE c.run_id=h.run_id AND c.node_id=h.node_id "
            "AND c.partition_key=h.partition_key),0)"
        ).all()
        event_mismatches = connection.exec_driver_sql(
            "SELECT e.run_id FROM run_event_counters e WHERE e.next_sequence_number <> "
            "COALESCE((SELECT MAX(sequence_number)+1 FROM execution_events x "
            "WHERE x.run_id=e.run_id),1)"
        ).all()
        repair = connection.exec_driver_sql(
            "SELECT p.status, a.approved_by, r.application_status, r.target_version "
            "FROM repair_plans p JOIN repair_approvals a "
            "USING (repair_plan_id, reconciliation_fingerprint) "
            "JOIN repair_actions r USING (repair_plan_id, run_id)"
        ).one()
        connection.rollback()

    assert checkpoint_mismatches == []
    assert event_mismatches == []
    assert repair == ("applied", "fixture-operator", "applied", 1)


def test_upgrade_repeat_and_reopen_preserve_every_logical_row(
    reconstructed_engine: Engine,
) -> None:
    with reconstructed_engine.connect() as connection:
        before = _snapshot(connection)
        connection.rollback()
        first_report = upgrade_to_head(connection)
        after_first = _snapshot(connection, upgraded_runs=True)
        assert connection.exec_driver_sql("PRAGMA quick_check").all() == [("ok",)]
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
        connection.rollback()
        second_report = upgrade_to_head(connection)
        after_second = _snapshot(connection, upgraded_runs=True)
        assert connection.exec_driver_sql("PRAGMA quick_check").all() == [("ok",)]
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
        connection.rollback()

    reconstructed_engine.dispose()
    with reconstructed_engine.connect() as reopened:
        reopen_report = upgrade_to_head(reopened)
        after_reopen = _snapshot(reopened, upgraded_runs=True)
        assert reopened.exec_driver_sql("PRAGMA quick_check").all() == [("ok",)]
        assert reopened.exec_driver_sql("PRAGMA foreign_key_check").all() == []
        reopened.rollback()

    assert first_report == MigrationReport(V0001_REVISION, HEAD_REVISION, HEAD_REVISION)
    assert second_report == MigrationReport(HEAD_REVISION, HEAD_REVISION, HEAD_REVISION)
    assert reopen_report == MigrationReport(HEAD_REVISION, HEAD_REVISION, HEAD_REVISION)
    assert _with_upgraded_runs_projection(before) == after_first
    assert after_first == after_second == after_reopen


def test_v0001_snapshot_ignores_additive_future_columns(reconstructed_engine: Engine) -> None:
    with reconstructed_engine.connect() as connection:
        before = _snapshot(connection)
        connection.exec_driver_sql("ALTER TABLE pipelines ADD COLUMN future_optional_detail TEXT")
        after = _snapshot(connection)
        connection.rollback()

    assert after == before


def test_released_constraints_and_trigger_guards_remain_active(
    reconstructed_engine: Engine,
) -> None:
    with reconstructed_engine.connect() as connection:
        with pytest.raises(IntegrityError, match="does not permit update"):
            connection.exec_driver_sql(
                "UPDATE work_attempts SET worker_identity='changed' "
                "WHERE work_item_id='wrk_completed-partition' AND attempt_number=1"
            )
        connection.rollback()
        with pytest.raises(IntegrityError, match="terminal rows cannot change"):
            connection.exec_driver_sql(
                "UPDATE idempotency_records SET updated_at='2026-08-12T12:11:00.000000Z' "
                "WHERE scope='run:create' AND idempotency_key='fixture-run-completed'"
            )
        connection.rollback()
        with pytest.raises(IntegrityError, match="does not permit delete"):
            connection.exec_driver_sql("DELETE FROM audit_entries WHERE sequence_number=1")
        connection.rollback()
        with pytest.raises(IntegrityError):
            connection.exec_driver_sql(
                "INSERT INTO execution_events "
                "(run_id,sequence_number,event_kind,occurred_at,subject_kind,"
                "payload_schema_version,payload_json) "
                "VALUES ('run_missing',1,'invalid','2026-08-12T12:00:00.000000Z','run',1,'{}')"
            )
        connection.rollback()


@pytest.mark.parametrize("statement", WHOLE_TABLE_UPDATE_ATTACKS)
def test_every_whole_table_update_guard_is_active(
    reconstructed_engine: Engine, statement: str
) -> None:
    with reconstructed_engine.connect() as connection:
        with pytest.raises(IntegrityError, match="does not permit update"):
            connection.exec_driver_sql(statement)
        connection.rollback()


@pytest.mark.parametrize("statement", IMMUTABLE_COLUMN_ATTACKS)
def test_every_immutable_column_guard_is_active(
    reconstructed_engine: Engine, statement: str
) -> None:
    with reconstructed_engine.connect() as connection:
        with pytest.raises(IntegrityError, match="immutable columns cannot change"):
            connection.exec_driver_sql(statement)
        connection.rollback()


@pytest.mark.parametrize("statement", TERMINAL_UPDATE_ATTACKS)
def test_every_terminal_guard_is_active(reconstructed_engine: Engine, statement: str) -> None:
    with reconstructed_engine.connect() as connection:
        with pytest.raises(IntegrityError, match="terminal rows cannot change"):
            connection.exec_driver_sql(statement)
        connection.rollback()


def test_every_operational_table_delete_guard_is_active(reconstructed_engine: Engine) -> None:
    with reconstructed_engine.connect() as connection:
        trigger_count = connection.exec_driver_sql(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'"
        ).scalar_one()
        assert trigger_count == 47
        for table in sorted(EXPECTED_ROW_COUNTS):
            with pytest.raises(IntegrityError, match="does not permit delete"):
                connection.exec_driver_sql(f'DELETE FROM "{table}"')
            connection.rollback()


def test_monotonic_trigger_guards_remain_active(reconstructed_engine: Engine) -> None:
    with reconstructed_engine.connect() as connection:
        with pytest.raises(IntegrityError, match="current_version must increase"):
            connection.exec_driver_sql(
                "UPDATE checkpoint_heads SET row_version=row_version+1 "
                "WHERE run_id='run_completed-demo' AND node_id='nod_inventory-sync' "
                "AND partition_key='catalog-page-0001'"
            )
        connection.rollback()
        with pytest.raises(IntegrityError, match="next_sequence_number must increase"):
            connection.exec_driver_sql(
                "UPDATE run_event_counters SET row_version=row_version+1 "
                "WHERE run_id='run_completed-demo'"
            )
        connection.rollback()

        connection.exec_driver_sql(
            "UPDATE checkpoint_heads SET current_version=3,row_version=row_version+1 "
            "WHERE run_id='run_completed-demo' AND node_id='nod_inventory-sync' "
            "AND partition_key='catalog-page-0001'"
        )
        assert (
            connection.exec_driver_sql(
                "SELECT current_version FROM checkpoint_heads WHERE run_id='run_completed-demo' "
                "AND node_id='nod_inventory-sync' AND partition_key='catalog-page-0001'"
            ).scalar_one()
            == 3
        )
        connection.rollback()

        connection.exec_driver_sql(
            "UPDATE run_event_counters SET next_sequence_number=5,row_version=row_version+1 "
            "WHERE run_id='run_completed-demo'"
        )
        assert (
            connection.exec_driver_sql(
                "SELECT next_sequence_number FROM run_event_counters "
                "WHERE run_id='run_completed-demo'"
            ).scalar_one()
            == 5
        )
        connection.rollback()


@pytest.mark.parametrize(
    "statement",
    [
        (
            "INSERT INTO work_items "
            "(work_item_id,run_id,node_id,partition_key,state,row_version,"
            "completed_attempt_count,expected_checkpoint_version,input_reference_json,"
            "created_at,updated_at) VALUES "
            "('wrk_hybrid-node','run_completed-demo','nod_inventory-live','hybrid','pending',"
            "1,0,0,'{}','2026-08-12T12:00:00.000000Z','2026-08-12T12:00:00.000000Z')"
        ),
        (
            "INSERT INTO checkpoints "
            "(run_id,node_id,partition_key,version,payload_schema_version,artifact_id,"
            "committed_at) "
            "VALUES ('run_active-demo','nod_inventory-live','catalog-page-0002',1,1,"
            "'art_inventory-output','2026-08-12T12:10:00.000000Z')"
        ),
        (
            "INSERT INTO repair_actions "
            "(repair_action_id,repair_plan_id,run_id,conflict_id,canonical_key,action_kind,"
            "external_idempotency_key,before_sha256,proposed_after_sha256,"
            "proposed_record_json,expected_target_record_json,mismatch_evidence_json,"
            "application_status) VALUES "
            "('rac_hybrid','rpl_harbor-repair','run_active-demo','cnf_missing-widget',"
            "'sku-harbor-lamp','update_target','hybrid-repair-v1',"
            "'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',"
            "'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',"
            "'{}','{}','[]','pending')"
        ),
    ],
)
def test_composite_hybrid_parent_attacks_are_rejected(
    reconstructed_engine: Engine, statement: str
) -> None:
    with reconstructed_engine.connect() as connection:
        with pytest.raises(IntegrityError, match="FOREIGN KEY constraint failed"):
            connection.exec_driver_sql(statement)
        connection.rollback()


@pytest.mark.parametrize(
    "statement",
    [
        (
            "INSERT INTO connectors "
            "(connector_id,kind,display_name,configuration_json,capabilities_json,revision,"
            "created_at,updated_at,row_version) VALUES "
            "('con_bad-json','synthetic','Bad JSON','[]','{}',1,"
            "'2026-08-12T12:00:00.000000Z','2026-08-12T12:00:00.000000Z',1)"
        ),
        (
            "INSERT INTO audit_entries "
            "(actor,operation,object_kind,correlation_id,occurred_at,detail_schema_version,"
            "detail_json) VALUES ('fixture','invalid','run','corr-invalid',"
            "'2026-08-12 12:00:00Z',1,'{}')"
        ),
        (
            "INSERT INTO audit_entries "
            "(actor,operation,object_kind,correlation_id,occurred_at,detail_schema_version,"
            "detail_json) VALUES ('fixture','invalid','run','corr-invalid',"
            "'2026-08-12T12:00:00.000000Z',x'31','{}')"
        ),
        (
            "INSERT INTO pipelines (pipeline_id,display_name,created_at,row_version) VALUES "
            "('pip_bad-','Bad identifier','2026-08-12T12:00:00.000000Z',1)"
        ),
    ],
)
def test_representative_storage_and_shape_constraints_are_active(
    reconstructed_engine: Engine, statement: str
) -> None:
    with reconstructed_engine.connect() as connection:
        with pytest.raises(IntegrityError, match="CHECK constraint failed"):
            connection.exec_driver_sql(statement)
        connection.rollback()
