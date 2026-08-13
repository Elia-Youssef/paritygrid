"""Abrupt subprocess termination and exact SQLite reopen classification."""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from sqlalchemy import text

from paritygrid.adapters.persistence import SQLiteDatabase, SQLiteDatabaseConfig
from paritygrid.quality.crash_reopen_harness import CrashReopenConfig, run_crash_reopen_case
from paritygrid.quality.crash_reopen_protocol import CrashFailpoint, CrashMarker
from paritygrid.quality.crash_reopen_scenario import (
    CrashDatabaseIntegrityError,
    CrashDatabaseOutcome,
    classify_crash_database,
    prepare_crash_database,
)


@pytest.mark.parametrize("failpoint", tuple(CrashFailpoint))
def test_real_subprocess_crash_matrix_is_exact_and_cleanup_safe(
    tmp_path: Path, failpoint: CrashFailpoint
) -> None:
    case = tmp_path / f"Café % Cafe\u0301 عربي {failpoint.value} {uuid.uuid4().hex}"
    result = run_crash_reopen_case(CrashReopenConfig(case, failpoint))
    assert result.final_outcome is CrashDatabaseOutcome.COMMITTED
    assert result.retried is (result.observed_outcome is CrashDatabaseOutcome.ABSENT)
    assert result.marker_prefix[0].marker is CrashMarker.WORKER_READY
    assert not case.exists()


@pytest.mark.parametrize("repeat", range(4))
def test_commit_entered_ambiguity_uses_database_as_only_authority(
    tmp_path: Path, repeat: int
) -> None:
    case = tmp_path / f"ambiguous-{repeat}-{uuid.uuid4().hex}"
    result = run_crash_reopen_case(CrashReopenConfig(case, CrashFailpoint.COMMIT_AMBIGUOUS))
    assert result.observed_outcome in {
        CrashDatabaseOutcome.ABSENT,
        CrashDatabaseOutcome.COMMITTED,
    }
    assert result.final_outcome is CrashDatabaseOutcome.COMMITTED


def test_classifier_rejects_every_partial_or_duplicate_effect(tmp_path: Path) -> None:
    database_path = tmp_path / "mixed.sqlite3"
    prepare_crash_database(database_path, 8675309)
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(database_path))
    try:
        with database.transaction() as session:
            session.execute(
                text("UPDATE runs SET row_version = 9 WHERE run_id = 'run_crash-reopen'")
            )
    finally:
        database.close()
    with pytest.raises(CrashDatabaseIntegrityError, match="partial or divergent"):
        classify_crash_database(database_path, 8675309)


def test_reopen_classifier_rejects_wrong_seed(tmp_path: Path) -> None:
    database_path = tmp_path / "seed.sqlite3"
    prepare_crash_database(database_path, 11)
    with pytest.raises(CrashDatabaseIntegrityError):
        classify_crash_database(database_path, 12)
