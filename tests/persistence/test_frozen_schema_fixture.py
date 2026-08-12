"""Released v0001 persistence fixture integration tests."""

import hashlib
import json
import sqlite3
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
    "manifest.json": "c083345f23a186498b1dca69140b7fed0fb21c5dbf82c837a7f3e3f7e1d2861a",
    "schema.sql": "50ff2626553f6e5250e217b79f06fc3a957a59ab2ffc3341fbd23b52fdcc243c",
    "seed.sql": "56fef2edb296c419058fa36665cd9852d344283586f63991a7ddb48eb6d1831a",
}
EXPECTED_LOGICAL_ROWS_HASH = "a1bb59a4b818111fbdef0e97591496302da51a32919b7646f7008ad633aee05c"
EXPECTED_ROW_COUNTS = {
    "artifact_manifests": 1,
    "audit_entries": 1,
    "checkpoint_heads": 2,
    "checkpoints": 2,
    "connector_secret_references": 1,
    "connectors": 2,
    "execution_events": 4,
    "idempotency_records": 1,
    "pipeline_versions": 1,
    "pipelines": 1,
    "reconciliation_conflicts": 1,
    "reconciliation_summaries": 1,
    "repair_actions": 1,
    "repair_approvals": 1,
    "repair_plans": 1,
    "run_event_counters": 2,
    "run_nodes": 2,
    "runs": 2,
    "system_metadata": 2,
    "work_attempts": 2,
    "work_items": 2,
}


def _fixture_bytes() -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in (MANIFEST_PATH, SCHEMA_PATH, SEED_PATH)}


def _reconstruct(connection: sqlite3.Connection) -> None:
    reconstruct_fixture(connection, SCHEMA_PATH.read_bytes(), SEED_PATH.read_bytes())


def _snapshot(connection: Connection) -> tuple[tuple[str, tuple[tuple[object, ...], ...]], ...]:
    tables = sorted(EXPECTED_ROW_COUNTS)
    result: list[tuple[str, tuple[tuple[object, ...], ...]]] = []
    for table in tables:
        info = connection.exec_driver_sql(f'PRAGMA table_info("{table}")').all()
        columns = [str(row[1]) for row in info]
        primary_key = [str(row[1]) for row in sorted(info, key=lambda row: int(row[5])) if row[5]]
        order = primary_key or columns
        order_sql = ", ".join(f'"{column}"' for column in order)
        rows = tuple(
            tuple(row)
            for row in connection.exec_driver_sql(f'SELECT * FROM "{table}" ORDER BY {order_sql}')
        )
        result.append((table, rows))
    return tuple(result)


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
    assert manifest["logical_rows_sha256"] == EXPECTED_LOGICAL_ROWS_HASH
    assert manifest["invariants"] == {
        "checkpoint_head_matches_history": True,
        "event_counter_matches_history": True,
        "repair_approval_and_action_match_plan": True,
    }
    assert len(manifest["foreign_key_witnesses"]) == 18
    assert all(witness["non_null_rows"] >= 1 for witness in manifest["foreign_key_witnesses"])
    assert {
        table: description["row_count"] for table, description in manifest["tables"].items()
    } == EXPECTED_ROW_COUNTS
    assert manifest["files"] == {
        "schema.sql": {"bytes": 99010, "sha256": EXPECTED_FILE_HASHES["schema.sql"]},
        "seed.sql": {"bytes": 12282, "sha256": EXPECTED_FILE_HASHES["seed.sql"]},
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
        after_first = _snapshot(connection)
        connection.rollback()
        second_report = upgrade_to_head(connection)
        after_second = _snapshot(connection)
        connection.rollback()

    reconstructed_engine.dispose()
    with reconstructed_engine.connect() as reopened:
        after_reopen = _snapshot(reopened)
        assert reopened.exec_driver_sql("PRAGMA quick_check").all() == [("ok",)]
        assert reopened.exec_driver_sql("PRAGMA foreign_key_check").all() == []
        reopened.rollback()

    expected_report = MigrationReport(HEAD_REVISION, HEAD_REVISION, HEAD_REVISION)
    assert first_report == expected_report
    assert second_report == expected_report
    assert before == after_first == after_second == after_reopen


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
