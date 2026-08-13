"""Deterministic verification for the reusable SQLite WAL stress harness."""

# pyright: reportPrivateUsage=false

import json
import shutil
import sqlite3
import subprocess
from dataclasses import replace
from pathlib import Path
from threading import Event
from typing import cast

import pytest
from sqlalchemy.engine import Connection, ExceptionContext
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence import SQLiteDatabase, SQLiteDatabaseConfig
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter
from paritygrid.application.ports.writer import WriterSettings
from paritygrid.quality.wal_stress import (
    WAL_STRESS_REPORT_SCHEMA_VERSION,
    WalStressConfig,
    WalStressError,
    WalStressProfile,
    WalStressReport,
    WalStressWorkload,
    build_scenario,
    run_wal_stress,
    validate_report_destination,
    workload_for,
    write_report_atomic,
)
from paritygrid.quality.wal_stress import runner as stress_runtime


@pytest.fixture(scope="module")
def ci_report(tmp_path_factory: pytest.TempPathFactory) -> tuple[WalStressReport, Path]:
    root = tmp_path_factory.mktemp("wal stress primary")
    database = root / "primary %.db"
    report = run_wal_stress(WalStressConfig(database, WalStressProfile.CI, 17))
    return report, database


def test_ci_profile_sustains_writes_readers_backpressure_and_real_contention(
    ci_report: tuple[WalStressReport, Path],
) -> None:
    report, database = ci_report
    assert report.schema_version == WAL_STRESS_REPORT_SCHEMA_VERSION
    assert report.submitted == report.admitted == report.committed == 98
    assert report.failures == 0
    assert report.writer.accepted == report.writer.completed == 98
    assert report.writer.max_queue_depth == 8
    assert report.writer.max_resident == 9
    assert 0 < report.writer.max_admission_waiters <= 8
    assert report.writer.contention_retries == 1
    assert report.contention_codes == (sqlite3.SQLITE_BUSY_SNAPSHOT,)
    assert report.locked_probe_code == sqlite3.SQLITE_LOCKED
    assert [producer.admitted for producer in report.producers] == [24, 24, 24, 24]
    assert all(reader.last_frontier == 98 for reader in report.readers)
    assert all(reader.last_frontier > reader.first_frontier for reader in report.readers)
    assert report.pinned_reader_start_frontier == report.pinned_reader_end_frontier == 2
    assert report.checkpoints.passive_while_pinned[1] > report.checkpoints.passive_while_pinned[2]
    assert report.checkpoints.truncate_after_release == (0, 0, 0)
    assert report.operational.execution_events == 98
    assert report.operational.next_event_sequence == 99
    assert report.operational.event_counter_row_version == 99
    assert report.operational.run_state == "running"
    assert report.operational.run_row_version == 98
    assert report.operational.work_items == 96
    assert report.operational.checkpoint_heads == 96
    assert report.operational.checkpoints == 0
    assert report.operational.node_work_total == 96
    assert report.operational.node_work_pending == 96
    assert report.operational.node_work_distribution == (24, 24, 24, 24)
    assert report.notifications.offered == 98
    assert report.notifications.accepted == report.notifications.depth == 8
    assert report.notifications.dropped == 90
    assert report.integrity.quick_check == "ok"
    assert report.integrity.foreign_key_violations == 0
    assert report.integrity.pool_checked_out == 0
    assert report.integrity.writer_thread_stopped
    assert report.integrity.reader_threads_stopped
    assert report.integrity.sidecars_absent
    assert database.is_file()
    assert not Path(f"{database}-wal").exists()
    assert not Path(f"{database}-shm").exists()


def test_scenario_manifest_is_stable_and_seeded() -> None:
    workload = workload_for(WalStressProfile.CI)
    first = build_scenario(WalStressProfile.CI, 9, workload)
    second = build_scenario(WalStressProfile.CI, 9, workload)
    other = build_scenario(WalStressProfile.CI, 10, workload)

    assert first.manifest_sha256 == second.manifest_sha256
    assert first.work == second.work
    assert first.owners == second.owners
    assert first.manifest_sha256 != other.manifest_sha256
    assert sorted(first.owners) == [0] * 24 + [1] * 24 + [2] * 24 + [3] * 24
    assert [int(command.event.expected_next_sequence) for command in first.work] == list(
        range(3, 99)
    )


def test_profiles_are_finite_and_invalid_profile_fails_closed() -> None:
    ci = workload_for(WalStressProfile.CI)
    local = workload_for(WalStressProfile.LOCAL)
    assert (ci.producer_count, ci.reader_count, ci.work_commands) == (4, 4, 96)
    assert (local.producer_count, local.reader_count, local.work_commands) == (8, 8, 384)
    with pytest.raises(TypeError, match="profile"):
        workload_for(cast(WalStressProfile, "unknown"))


@pytest.mark.parametrize(
    ("changes", "error"),
    [
        ({"database_path": "bad"}, TypeError),
        ({"database_path": Path("relative.db")}, ValueError),
        ({"profile": "ci"}, TypeError),
        ({"seed": True}, ValueError),
        ({"seed": -1}, ValueError),
        ({"seed": 4_294_967_296}, ValueError),
        ({"create_parent": 1}, TypeError),
    ],
)
def test_config_rejects_invalid_inputs(
    tmp_path: Path, changes: dict[str, object], error: type[Exception]
) -> None:
    values: dict[str, object] = {
        "database_path": tmp_path / "valid.db",
        "profile": WalStressProfile.CI,
        "seed": 1,
        "create_parent": False,
    }
    values.update(changes)
    with pytest.raises(error):
        WalStressConfig(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "changes",
    [
        {"producer_count": 0},
        {"reader_count": True},
        {"work_commands": 0},
        {"queue_capacity": 0},
        {"admission_capacity": 0},
        {"notification_capacity": 0},
        {"max_contention_attempts": 0},
        {"timeout_seconds": 0.0},
        {"timeout_seconds": 1},
        {"total_budget_seconds": 0.0},
        {"total_budget_seconds": 301.0},
    ],
)
def test_workload_rejects_invalid_bounds(changes: dict[str, object]) -> None:
    values: dict[str, object] = {
        "producer_count": 1,
        "reader_count": 1,
        "work_commands": 1,
        "queue_capacity": 1,
        "admission_capacity": 1,
        "notification_capacity": 1,
        "max_contention_attempts": 1,
        "timeout_seconds": 1.0,
        "total_budget_seconds": 1.0,
    }
    values.update(changes)
    with pytest.raises(ValueError, match="invalid"):
        WalStressWorkload(**values)  # type: ignore[arg-type]


def test_report_validation_and_mapping_are_closed(
    ci_report: tuple[WalStressReport, Path],
) -> None:
    report, _ = ci_report
    mapping = report.to_mapping()
    assert mapping["profile"] == "ci"
    assert cast(dict[str, object], mapping["writer"])["state"] == "closed"
    invalid: tuple[dict[str, object], ...] = (
        {"schema_version": 2},
        {"profile": "ci"},
        {"seed": -1},
        {"platform": ""},
        {"python_version": ""},
        {"sqlite_version": ""},
        {"scenario_manifest_sha256": "0" * 63},
        {"scenario_manifest_sha256": "g" * 64},
        {"elapsed_seconds": -1.0},
        {"submitted": -1},
        {"admitted": 1.0},
        {"contention_codes": (-1,)},
        {"contention_codes": (1.0,)},
        {"locked_probe_code": -1},
    )
    for changes in invalid:
        with pytest.raises((TypeError, ValueError)):
            replace(report, **changes)  # type: ignore[arg-type]


def test_atomic_report_is_canonical_utf8_and_never_replaces_existing_file(
    ci_report: tuple[WalStressReport, Path], tmp_path: Path
) -> None:
    report, _ = ci_report
    target = tmp_path / "evidence %.json"
    database = tmp_path / "separate.db"

    write_report_atomic(report, target, database_path=database)
    first = target.read_bytes()
    with pytest.raises(WalStressError, match="new file"):
        write_report_atomic(report, target, database_path=database)

    assert target.read_bytes() == first
    assert first.endswith(b"\n")
    assert not first.startswith(b"\xef\xbb\xbf")
    assert b"\r\n" not in first
    parsed = json.loads(first)
    assert parsed["scenario_manifest_sha256"] == report.scenario_manifest_sha256
    assert list(tmp_path.glob(".*.tmp")) == []


def test_report_destination_rejects_database_alias_existing_and_linked_components(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "database.db"
    with pytest.raises(WalStressError, match="distinct"):
        validate_report_destination(database, database)

    existing = tmp_path / "existing.json"
    existing.write_bytes(b"reserved")
    with pytest.raises(WalStressError, match="new file"):
        validate_report_destination(existing, database)
    assert existing.read_bytes() == b"reserved"

    linked_parent = tmp_path / "linked"
    linked_target = linked_parent / "report.json"
    original_is_symlink = Path.is_symlink

    def identifies_link(candidate: Path) -> bool:
        return candidate == linked_parent or original_is_symlink(candidate)

    monkeypatch.setattr(Path, "is_symlink", identifies_link)
    with pytest.raises(WalStressError, match="symbolic links"):
        validate_report_destination(linked_target, database)


def test_report_destination_rejects_invalid_types_and_inspection_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report = tmp_path / "report.json"
    database = tmp_path / "database.db"
    with pytest.raises(TypeError, match="Path values"):
        validate_report_destination(cast(Path, object()), database)

    original_is_symlink = Path.is_symlink

    def fail_component_inspection(candidate: Path) -> bool:
        if candidate == report:
            raise OSError("component unavailable")
        return original_is_symlink(candidate)

    with monkeypatch.context() as component_context:
        component_context.setattr(Path, "is_symlink", fail_component_inspection)
        with pytest.raises(WalStressError, match="could not be inspected"):
            validate_report_destination(report, database)

    original_resolve = Path.resolve

    def fail_resolution(candidate: Path, *, strict: bool = False) -> Path:
        if candidate == report:
            raise OSError("resolution unavailable")
        return original_resolve(candidate, strict=strict)

    with monkeypatch.context() as resolution_context:
        resolution_context.setattr(Path, "resolve", fail_resolution)
        with pytest.raises(WalStressError, match="could not be inspected"):
            validate_report_destination(report, database)


def test_report_publication_race_preserves_competing_destination(
    ci_report: tuple[WalStressReport, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report, database = ci_report
    target = tmp_path / "raced.json"

    def lose_creation_race(_source: Path, destination: Path) -> None:
        destination.write_bytes(b"winner")
        raise FileExistsError

    monkeypatch.setattr(stress_runtime.os, "link", lose_creation_race)
    with pytest.raises(WalStressError, match="published"):
        write_report_atomic(report, target, database_path=database)
    assert target.read_bytes() == b"winner"
    assert list(tmp_path.glob(".*.tmp")) == []


def test_report_publication_rejects_invalid_targets_and_cleans_candidate(
    ci_report: tuple[WalStressReport, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report, _ = ci_report
    with pytest.raises(TypeError):
        write_report_atomic(
            cast(WalStressReport, object()),
            tmp_path / "report.json",
            database_path=tmp_path / "database.db",
        )
    with pytest.raises(ValueError, match="absolute"):
        write_report_atomic(report, Path("relative.json"), database_path=tmp_path / "database.db")
    with pytest.raises(WalStressError, match="parent"):
        write_report_atomic(
            report,
            tmp_path / "missing" / "report.json",
            database_path=tmp_path / "database.db",
        )

    def fail_link(_source: Path, _target: Path) -> None:
        raise OSError("publication failed")

    monkeypatch.setattr(stress_runtime.os, "link", fail_link)
    with pytest.raises(WalStressError, match="published"):
        write_report_atomic(
            report,
            tmp_path / "failed.json",
            database_path=tmp_path / "database.db",
        )
    assert list(tmp_path.glob(".*.tmp")) == []


def test_runner_rejects_existing_file_and_cleans_partial_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    existing = tmp_path / "existing.db"
    existing.write_bytes(b"reserved")
    with pytest.raises(WalStressError, match="new file"):
        run_wal_stress(WalStressConfig(existing))
    with pytest.raises(TypeError, match="configuration"):
        run_wal_stress(cast(WalStressConfig, object()))

    target = tmp_path / "partial.db"

    def fail_seed(_database: SQLiteDatabase, _scenario: object) -> None:
        raise RuntimeError("seed failed")

    monkeypatch.setattr(stress_runtime, "_seed_pipeline", fail_seed)
    with pytest.raises(RuntimeError, match="seed failed"):
        run_wal_stress(WalStressConfig(target))
    assert not target.exists()
    assert not Path(f"{target}-wal").exists()
    assert not Path(f"{target}-shm").exists()


@pytest.mark.parametrize("directory_name", ["مرحبا space % NFC", "مرحبا space % NFD e\u0301"])
def test_repeated_cycles_support_unicode_space_percent_and_normalization_forms(
    tmp_path: Path, directory_name: str
) -> None:
    root = tmp_path / directory_name
    root.mkdir()
    first = run_wal_stress(WalStressConfig(root / "first.db", seed=31))
    second = run_wal_stress(WalStressConfig(root / "second.db", seed=31))

    assert first.scenario_manifest_sha256 == second.scenario_manifest_sha256
    assert first.operational == second.operational
    assert first.contention_codes == second.contention_codes == (sqlite3.SQLITE_BUSY_SNAPSHOT,)


def test_external_cwd_cli_writes_database_and_canonical_report(tmp_path: Path) -> None:
    external = tmp_path / "external cwd Arabic %"
    external.mkdir()
    database = external / "cli.db"
    report = external / "cli report.json"
    executable = shutil.which("paritygrid")
    assert executable is not None

    completed = subprocess.run(
        [
            executable,
            "stress",
            "wal",
            "--database",
            str(database),
            "--profile",
            "ci",
            "--seed",
            "41",
            "--report",
            str(report),
        ],
        cwd=external,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    assert "WAL stress passed: commands=98, retries=1" in completed.stdout
    assert database.is_file()
    assert json.loads(report.read_text(encoding="utf-8"))["seed"] == 41


def test_raw_locked_probe_and_checkpoint_shape_validation() -> None:
    assert stress_runtime._probe_locked_code() == sqlite3.SQLITE_LOCKED

    class BadResult:
        def one(self) -> tuple[int, int]:
            return (0, 0)

    class BadConnection:
        def exec_driver_sql(self, _statement: str) -> BadResult:
            return BadResult()

    with pytest.raises(WalStressError, match="checkpoint"):
        stress_runtime._checkpoint_tuple(cast(object, BadConnection()), "PASSIVE")  # type: ignore[arg-type]


def test_locked_probe_rejects_wrong_missing_and_unclassified_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = sqlite3.connect(":memory:")
    try:
        with pytest.raises(sqlite3.OperationalError) as captured:
            connection.execute("SELECT * FROM missing_table")
    finally:
        connection.close()
    with pytest.raises(WalStressError, match="unexpected code"):
        stress_runtime._require_locked_error(captured.value)

    locked_connection = sqlite3.connect(":memory:", autocommit=True)
    try:
        locked_connection.execute("CREATE TABLE locked_probe (value INTEGER)")
        locked_connection.executemany("INSERT INTO locked_probe VALUES (?)", ((1,), (2,)))
        cursor = locked_connection.execute("SELECT * FROM locked_probe")
        cursor.fetchone()
        with pytest.raises(sqlite3.OperationalError) as locked:
            locked_connection.execute("DROP TABLE locked_probe")

        def reject_contention(_error: OperationalError) -> bool:
            return False

        monkeypatch.setattr(stress_runtime, "is_sqlite_contention", reject_contention)
        with pytest.raises(WalStressError, match="classified"):
            stress_runtime._require_locked_error(locked.value)
        cursor.close()
    finally:
        locked_connection.close()


def test_locked_probe_detects_missing_lock_and_closes_without_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Cursor:
        def fetchone(self) -> tuple[int]:
            return (1,)

        def close(self) -> None:
            return None

    class NoLockConnection:
        def execute(self, _statement: str) -> Cursor:
            return Cursor()

        def executemany(self, _statement: str, _values: object) -> None:
            return None

        def close(self) -> None:
            return None

    def no_lock_connect(*_args: object, **_kwargs: object) -> object:
        return NoLockConnection()

    monkeypatch.setattr(stress_runtime.sqlite3, "connect", no_lock_connect)
    with pytest.raises(WalStressError, match="did not produce"):
        stress_runtime._probe_locked_code()

    class EarlyFailureConnection(NoLockConnection):
        def execute(self, _statement: str) -> Cursor:
            raise RuntimeError("early failure")

    def early_failure_connect(*_args: object, **_kwargs: object) -> object:
        return EarlyFailureConnection()

    monkeypatch.setattr(stress_runtime.sqlite3, "connect", early_failure_connect)
    with pytest.raises(RuntimeError, match="early failure"):
        stress_runtime._probe_locked_code()


def test_snapshot_and_operational_helpers_reject_incoherence() -> None:
    class Result:
        def __init__(self, value: object) -> None:
            self.value = value

        def scalar_one(self) -> object:
            return self.value

        def one(self) -> object:
            return self.value

        def all(self) -> object:
            return self.value

    class ScriptedConnection:
        def __init__(self, values: list[object]) -> None:
            self.values = values
            self.rolled_back = False

        def exec_driver_sql(self, statement: str, _parameters: object = None) -> Result:
            if statement == "BEGIN":
                return Result(None)
            if statement == "ROLLBACK":
                self.rolled_back = True
                return Result(None)
            return Result(self.values.pop(0))

    snapshot = ScriptedConnection([2, 0, 1, 3, 2, 0])
    with pytest.raises(WalStressError, match="incoherent"):
        stress_runtime._coherent_frontier(cast(Connection, snapshot), "run_test")
    assert snapshot.rolled_back

    operational = ScriptedConnection(
        [
            (2, 1, 1),
            0,
            0,
            (3, 3),
            ("running", 2),
            0,
            [(0, 0, 0, 0, 0, 0, 0)] * 4,
        ]
    )
    with pytest.raises(WalStressError, match="contiguous"):
        stress_runtime._operational_evidence(cast(Connection, operational), "run_test")

    invalid_cardinality = ScriptedConnection(
        [
            (2, 1, 2),
            0,
            0,
            (3, 3),
            ("running", 2),
            0,
            [(0, 0, 0, 0, 0, 0, 0)] * 3,
        ]
    )
    with pytest.raises(WalStressError, match="cardinality"):
        stress_runtime._operational_evidence(cast(Connection, invalid_cardinality), "run_test")

    invalid_state = ScriptedConnection(
        [
            (2, 1, 2),
            0,
            0,
            (3, 3),
            (object(), 2),
            0,
            [(0, 0, 0, 0, 0, 0, 0)] * 4,
        ]
    )
    with pytest.raises(WalStressError, match="run state"):
        stress_runtime._operational_evidence(cast(Connection, invalid_state), "run_test")


def test_pool_helper_requires_file_database_queue_pool() -> None:
    with pytest.raises(WalStressError, match="QueuePool"):
        stress_runtime._checked_out_connections(object())


def test_thread_join_and_failed_file_cleanup_defenses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class StuckThread:
        def join(self, _timeout: float) -> None:
            return None

        def is_alive(self) -> bool:
            return True

    with pytest.raises(WalStressError, match="reader did not stop"):
        stress_runtime._join_threads(
            cast(list[stress_runtime.Thread], [StuckThread()]), 0.0, "reader"
        )

    path = tmp_path / "cleanup.db"
    original_unlink = Path.unlink

    def selective_failure(candidate: Path, *, missing_ok: bool = False) -> None:
        if str(candidate).endswith("-wal"):
            raise OSError("locked sidecar")
        original_unlink(candidate, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", selective_failure)
    stress_runtime._remove_failed_database(path)


def test_report_hard_gate_failures_are_explicit(
    ci_report: tuple[WalStressReport, Path],
) -> None:
    report, _ = ci_report
    command_failures: tuple[dict[str, object], ...] = (
        {"submitted": 97},
        {"admitted": 97},
        {"committed": 97},
        {"failures": 1},
        {"writer": replace(report.writer, accepted=97, completed=97)},
        {"writer": replace(report.writer, contention_retries=0)},
        {"writer": replace(report.writer, max_queue_depth=7)},
        {"writer": replace(report.writer, max_admission_waiters=0)},
        {"close": replace(report.close, drained=False)},
        {"operational": replace(report.operational, execution_events=97)},
        {"operational": replace(report.operational, next_event_sequence=98)},
        {"operational": replace(report.operational, event_counter_row_version=98)},
        {"operational": replace(report.operational, run_state="queued")},
        {"operational": replace(report.operational, run_row_version=97)},
        {"operational": replace(report.operational, work_items=95)},
        {"operational": replace(report.operational, checkpoint_heads=95)},
        {"operational": replace(report.operational, checkpoints=1)},
        {"operational": replace(report.operational, node_work_total=95)},
        {"operational": replace(report.operational, node_work_pending=95)},
        {"operational": replace(report.operational, node_work_distribution=(23, 25, 24, 24))},
        {"notifications": replace(report.notifications, failures=1)},
    )
    for changes in command_failures:
        with pytest.raises(WalStressError, match="command or frontier"):
            stress_runtime._assert_report_gates(replace(report, **changes))  # type: ignore[arg-type]

    failures: tuple[tuple[dict[str, object], str], ...] = (
        ({"contention_codes": ()}, "BUSY"),
        ({"contention_codes": (sqlite3.SQLITE_BUSY,)}, "BUSY"),
        ({"locked_probe_code": 5}, "LOCKED"),
        (
            {
                "producers": (
                    replace(report.producers[0], admitted=0),
                    *report.producers[1:],
                )
            },
            "fairness",
        ),
        (
            {"readers": (replace(report.readers[0], operations=1), *report.readers[1:])},
            "reader progress",
        ),
        (
            {
                "readers": (
                    replace(
                        report.readers[0],
                        last_frontier=report.readers[0].first_frontier,
                    ),
                    *report.readers[1:],
                )
            },
            "reader progress",
        ),
        ({"pinned_reader_end_frontier": 3}, "pinned"),
        (
            {"checkpoints": replace(report.checkpoints, passive_while_pinned=(0, 0, 0))},
            "checkpoint pressure",
        ),
        ({"integrity": replace(report.integrity, journal_mode="delete")}, "integrity"),
        ({"elapsed_seconds": 301.0}, "budget"),
    )
    for changes, message in failures:
        with pytest.raises(WalStressError, match=message):
            stress_runtime._assert_report_gates(replace(report, **changes))  # type: ignore[arg-type]

    linux = replace(report, platform="Linux", elapsed_seconds=21.0)
    with pytest.raises(WalStressError, match="budget"):
        stress_runtime._assert_report_gates(linux)


def test_reader_and_injector_defensive_paths(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "injector defensive.db"
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(database_path))
    try:
        with database.engine.connect() as connection:
            upgrade_to_head(connection)
        ready = stress_runtime.Event()
        injector = stress_runtime._BusySnapshotInjector(
            database,
            database_path,
            ready,
            lambda: stress_runtime.current_thread(),
            0.0,
        )
        injector.arm()
        with pytest.raises(WalStressError, match="resident bound"):
            injector._after_cursor_execute(
                cast(Connection, None),
                None,
                "SELECT * FROM runs",
                None,
                None,
                False,
            )
        injector._after_cursor_execute(
            cast(Connection, None),
            None,
            "SELECT * FROM runs",
            None,
            None,
            False,
        )

        class ErrorContext:
            original_exception = RuntimeError("not SQLite")

        injector._handle_error(cast(ExceptionContext, ErrorContext()))

        with database.engine.connect() as connection:
            connection.exec_driver_sql("BEGIN")
            injector._begin_database_transaction(cast(Session, object()), object(), connection)
            connection.exec_driver_sql("ROLLBACK")
    finally:
        database.close()

    class NeverStart:
        def wait(self, _timeout: float) -> bool:
            return False

    state = stress_runtime._ReaderState([])
    stress_runtime._reader_loop(
        cast(SQLiteDatabase, object()),
        "run_missing",
        cast(Event, NeverStart()),
        stress_runtime.Barrier(1),
        stress_runtime.Event(),
        1,
        stress_runtime.Event(),
        state,
    )
    assert isinstance(state.error, WalStressError)


@pytest.mark.parametrize("reader_has_error", [False, True])
def test_runner_reader_initialization_failures_clean_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reader_has_error: bool
) -> None:
    database = tmp_path / f"reader-init-{reader_has_error}.db"

    def fail_reader(
        _database: object,
        _run_id: str,
        _start: object,
        ready: stress_runtime.Barrier,
        _stop: object,
        _frontier: int,
        final_seen: stress_runtime.Event,
        state: stress_runtime._ReaderState,
    ) -> None:
        if reader_has_error:
            state.error = RuntimeError("reader failed")
        final_seen.set()
        ready.abort()

    monkeypatch.setattr(stress_runtime, "_reader_loop", fail_reader)
    with pytest.raises(WalStressError, match="initialization"):
        run_wal_stress(WalStressConfig(database, seed=88))
    assert not database.exists()


@pytest.mark.parametrize("thread_kind", ["reader", "producer"])
def test_runner_thread_start_failure_stops_started_peers_and_cleans_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, thread_kind: str
) -> None:
    database = tmp_path / f"{thread_kind}-start-failure.db"
    original_start = stress_runtime.Thread.start

    def controlled_start(thread: stress_runtime.Thread) -> None:
        if thread.name == f"paritygrid-wal-{thread_kind}-1":
            raise RuntimeError(f"{thread_kind} start failed")
        original_start(thread)

    monkeypatch.setattr(stress_runtime.Thread, "start", controlled_start)
    with pytest.raises(RuntimeError, match=f"{thread_kind} start failed"):
        run_wal_stress(WalStressConfig(database, seed=87))
    assert not database.exists()
    assert not Path(f"{database}-wal").exists()
    assert not Path(f"{database}-shm").exists()


@pytest.mark.parametrize("thread_kind", ["reader", "producer"])
def test_runner_tracks_threads_when_start_raises_after_launch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, thread_kind: str
) -> None:
    database = tmp_path / f"{thread_kind}-post-launch-failure.db"
    original_start = stress_runtime.Thread.start

    if thread_kind == "reader":

        def stoppable_reader(
            _database: object,
            _run_id: str,
            _start: object,
            _ready: object,
            stop: stress_runtime.Event,
            _frontier: int,
            _final_seen: object,
            _state: object,
        ) -> None:
            assert stop.wait(2.0)

        monkeypatch.setattr(stress_runtime, "_reader_loop", stoppable_reader)

    def launch_then_fail(thread: stress_runtime.Thread) -> None:
        original_start(thread)
        if thread.name == f"paritygrid-wal-{thread_kind}-1":
            raise RuntimeError(f"{thread_kind} post-launch failure")

    monkeypatch.setattr(stress_runtime.Thread, "start", launch_then_fail)
    with pytest.raises(RuntimeError, match=f"{thread_kind} post-launch failure"):
        run_wal_stress(WalStressConfig(database, seed=92))
    assert not database.exists()


def test_runner_expired_cleanup_budget_skips_reclosing_stopped_writer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "expired-cleanup-budget.db"
    writers: list[SQLiteTransactionalWriter] = []
    original_writer_start = SQLiteTransactionalWriter.start
    original_thread_start = stress_runtime.Thread.start
    real_clock = stress_runtime.time.monotonic

    def remember_writer(writer: SQLiteTransactionalWriter) -> None:
        writers.append(writer)
        original_writer_start(writer)

    def stop_writer_then_fail(thread: stress_runtime.Thread) -> None:
        if thread.name == "paritygrid-wal-reader-0":
            assert writers
            assert writers[0].close(timeout_seconds=2.0).drained
            raise RuntimeError("startup failed after cleanup budget")
        original_thread_start(thread)

    monkeypatch.setattr(SQLiteTransactionalWriter, "start", remember_writer)
    monkeypatch.setattr(stress_runtime.Thread, "start", stop_writer_then_fail)
    monkeypatch.setattr(stress_runtime.time, "perf_counter", lambda: real_clock() - 30.0)

    with pytest.raises(RuntimeError, match="startup failed after cleanup budget"):
        run_wal_stress(WalStressConfig(database, seed=93))
    assert not database.exists()


def test_runner_cleanup_failure_chains_the_original_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "cleanup-failure-chain.db"
    original_start = stress_runtime.Thread.start

    def fail_first_reader(thread: stress_runtime.Thread) -> None:
        if thread.name == "paritygrid-wal-reader-0":
            raise RuntimeError("reader launch failed")
        original_start(thread)

    def cannot_prove_stop(_threads: list[stress_runtime.Thread], _deadline: float) -> bool:
        return False

    monkeypatch.setattr(stress_runtime.Thread, "start", fail_first_reader)
    monkeypatch.setattr(stress_runtime, "_join_threads_until", cannot_prove_stop)

    with pytest.raises(WalStressError, match="cleanup could not stop") as captured:
        run_wal_stress(WalStressConfig(database, seed=94))
    assert isinstance(captured.value.__cause__, RuntimeError)
    assert not database.exists()


def test_runner_success_still_requires_cleanup_stop_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "cleanup-verification.db"

    def cannot_prove_stop(_threads: list[stress_runtime.Thread], _deadline: float) -> bool:
        return False

    monkeypatch.setattr(stress_runtime, "_join_threads_until", cannot_prove_stop)

    with pytest.raises(WalStressError, match="cleanup could not stop") as captured:
        run_wal_stress(WalStressConfig(database, seed=95))
    assert captured.value.__cause__ is None
    assert database.exists()


def test_runner_producer_timeout_is_bounded_and_cleans_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "producer timeout.db"
    original_workload = workload_for(WalStressProfile.CI)

    def bounded_workload(_profile: WalStressProfile) -> WalStressWorkload:
        return original_workload

    monkeypatch.setattr(stress_runtime, "workload_for", bounded_workload)
    original_builder = build_scenario

    def invalid_owners(profile: WalStressProfile, seed: int, workload: WalStressWorkload) -> object:
        scenario = original_builder(profile, seed, workload)
        return replace(scenario, owners=(99,) * workload.work_commands)

    monkeypatch.setattr(stress_runtime, "build_scenario", invalid_owners)
    with pytest.raises(WalStressError, match="producer failed"):
        run_wal_stress(WalStressConfig(database, seed=89))
    assert not database.exists()


def test_runner_producer_phase_uses_the_finite_total_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "producer phase budget.db"
    observed: list[tuple[float, str]] = []
    original_join = stress_runtime._join_threads

    def record_join(
        threads: list[stress_runtime.Thread], timeout_seconds: float, subject: str
    ) -> None:
        observed.append((timeout_seconds, subject))
        original_join(threads, timeout_seconds, subject)

    monkeypatch.setattr(stress_runtime, "_join_threads", record_join)
    report = run_wal_stress(WalStressConfig(database, seed=96))

    workload = workload_for(WalStressProfile.CI)
    assert (workload.total_budget_seconds, "producer") in observed
    assert report.committed == 98


def test_runner_command_failure_is_counted_and_cleans_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "command failure.db"
    original_builder = build_scenario

    def duplicate_work(profile: WalStressProfile, seed: int, workload: WalStressWorkload) -> object:
        scenario = original_builder(profile, seed, workload)
        work = list(scenario.work)
        work[1] = work[0]
        return replace(scenario, work=tuple(work))

    monkeypatch.setattr(stress_runtime, "build_scenario", duplicate_work)
    with pytest.raises(WalStressError, match="command failed"):
        run_wal_stress(WalStressConfig(database, seed=90))
    assert not database.exists()


@pytest.mark.parametrize("terminal_error", [False, True])
def test_runner_reader_terminal_failures_are_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, terminal_error: bool
) -> None:
    database = tmp_path / f"reader-terminal-{terminal_error}.db"
    original_workload = workload_for(WalStressProfile.CI)
    operation_timeout = original_workload.timeout_seconds

    def bounded_workload(_profile: WalStressProfile) -> WalStressWorkload:
        return original_workload

    monkeypatch.setattr(stress_runtime, "workload_for", bounded_workload)

    def controlled_reader(
        _database: object,
        _run_id: str,
        start: stress_runtime.Event,
        ready: stress_runtime.Barrier,
        stop: stress_runtime.Event,
        _frontier: int,
        final_seen: stress_runtime.Event,
        state: stress_runtime._ReaderState,
    ) -> None:
        assert start.wait(operation_timeout)
        state.frontiers.append(2)
        ready.wait(operation_timeout)
        if terminal_error:
            final_seen.set()
        assert stop.wait(operation_timeout * 2)
        if terminal_error:
            state.error = RuntimeError("terminal reader failure")

    monkeypatch.setattr(stress_runtime, "_reader_loop", controlled_reader)
    message = "reader failed" if terminal_error else "final frontier"
    with pytest.raises(WalStressError, match=message):
        run_wal_stress(WalStressConfig(database, seed=91))
    assert not database.exists()


def test_real_busy_snapshot_exhaustion_fails_writer_closed(tmp_path: Path) -> None:
    database_path = tmp_path / "exhaust.db"
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(database_path))
    writer: SQLiteTransactionalWriter | None = None
    injector: stress_runtime._BusySnapshotInjector | None = None
    try:
        with database.engine.connect() as connection:
            upgrade_to_head(connection)
        workload = workload_for(WalStressProfile.CI)
        scenario = build_scenario(WalStressProfile.CI, 77, workload)
        stress_runtime._seed_pipeline(database, scenario)
        ready = stress_runtime.Event()
        ready.set()
        writer = SQLiteTransactionalWriter(
            stress_runtime._session_factory(database),
            WriterSettings(
                queue_capacity=2,
                admission_waiter_capacity=2,
                notification_capacity=2,
                max_contention_attempts=2,
                contention_delay_seconds=0.0,
                thread_name="paritygrid-wal-exhaustion",
            ),
        )
        injector = stress_runtime._BusySnapshotInjector(
            database,
            database_path,
            ready,
            lambda: writer.thread,
            2.0,
            fire_count=2,
        )
        injector.install()
        writer.start()
        writer.submit(scenario.create_run, timeout_seconds=1.0).result(timeout_seconds=2.0)
        writer.submit(scenario.start_run, timeout_seconds=1.0).result(timeout_seconds=2.0)
        injector.arm()
        ticket = writer.submit(scenario.work[0], timeout_seconds=1.0)
        with pytest.raises(Exception, match="retry limit"):
            ticket.result(timeout_seconds=3.0)
        assert injector.codes == (
            sqlite3.SQLITE_BUSY_SNAPSHOT,
            sqlite3.SQLITE_BUSY_SNAPSHOT,
        )
        assert writer.snapshot().contention_retries == 2
    finally:
        if writer is not None:
            writer.close(timeout_seconds=2.0)
        if injector is not None:
            injector.remove()
        database.close()
