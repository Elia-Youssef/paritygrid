"""File-based WAL stress orchestration and report publication."""

import json
import os
import platform
import sqlite3
import time
import uuid
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from threading import Barrier, BrokenBarrierError, Condition, Event, Lock, Thread, current_thread
from typing import cast

from sqlalchemy import event
from sqlalchemy.engine import Connection, ExceptionContext
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool

from paritygrid.adapters.persistence import SQLiteDatabase, SQLiteDatabaseConfig
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.repositories import SqlAlchemyPipelineRepository
from paritygrid.adapters.persistence.sqlite import SessionFactory
from paritygrid.adapters.persistence.writer.contention import is_sqlite_contention
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.writer import WriterSettings, WriterTicket

from .models import (
    WAL_STRESS_REPORT_SCHEMA_VERSION,
    IntegrityEvidence,
    OperationalEvidence,
    ProducerEvidence,
    ReaderEvidence,
    WalCheckpointEvidence,
    WalStressConfig,
    WalStressError,
    WalStressReport,
    workload_for,
)
from .scenario import WalStressScenario, build_scenario


@dataclass(slots=True)
class _ReaderState:
    frontiers: list[int]
    maximum_latency: float = 0.0
    error: BaseException | None = None


class _StressSession(Session):
    pass


class _BusySnapshotInjector:
    def __init__(
        self,
        database: SQLiteDatabase,
        database_path: Path,
        resident_ready: Event,
        writer_thread: Callable[[], Thread | None],
        timeout_seconds: float,
        fire_count: int = 1,
    ) -> None:
        self._database = database
        self._database_path = database_path
        self._resident_ready = resident_ready
        self._writer_thread = writer_thread
        self._timeout_seconds = timeout_seconds
        self._fire_count = fire_count
        self._armed = False
        self._fired = 0
        self._codes: list[int] = []
        self._lock = Lock()

    @property
    def codes(self) -> tuple[int, ...]:
        with self._lock:
            return tuple(self._codes)

    def install(self) -> None:
        event.listen(_StressSession, "after_begin", self._begin_database_transaction)
        event.listen(self._database.engine, "after_cursor_execute", self._after_cursor_execute)
        event.listen(self._database.engine, "handle_error", self._handle_error)

    def remove(self) -> None:
        event.remove(_StressSession, "after_begin", self._begin_database_transaction)
        event.remove(self._database.engine, "after_cursor_execute", self._after_cursor_execute)
        event.remove(self._database.engine, "handle_error", self._handle_error)

    def arm(self) -> None:
        self._armed = True

    def _begin_database_transaction(
        self,
        _session: Session,
        _transaction: object,
        connection: Connection,
    ) -> None:
        raw = cast(sqlite3.Connection, connection.connection.driver_connection)
        if not raw.in_transaction:
            connection.exec_driver_sql("BEGIN")

    def _after_cursor_execute(
        self,
        _connection: Connection,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        writer_thread = self._writer_thread()
        if (
            not self._armed
            or self._fired >= self._fire_count
            or writer_thread is None
            or current_thread() is not writer_thread
            or "FROM RUNS" not in statement.upper()
        ):
            return
        self._fired += 1
        independent = sqlite3.connect(
            self._database_path,
            timeout=5.0,
            autocommit=True,
            check_same_thread=False,
        )
        try:
            independent.execute("PRAGMA busy_timeout = 5000")
            independent.execute(
                "INSERT INTO system_metadata (key, value, updated_at) VALUES (?, ?, ?)",
                (
                    f"wal_stress.contention-{self._fired}",
                    f"snapshot invalidation {self._fired}",
                    "2026-08-12T12:00:00.000000Z",
                ),
            )
        finally:
            independent.close()
        if not self._resident_ready.wait(self._timeout_seconds):
            raise WalStressError("writer queue did not reach the required resident bound")

    def _handle_error(self, context: ExceptionContext) -> None:
        code = getattr(context.original_exception, "sqlite_errorcode", None)
        if type(code) is int and code & 0xFF in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
            with self._lock:
                self._codes.append(code)


def _session_factory(database: SQLiteDatabase) -> SessionFactory:
    return sessionmaker(
        bind=database.engine,
        class_=_StressSession,
        autoflush=False,
        expire_on_commit=False,
    )


def _seed_pipeline(database: SQLiteDatabase, scenario: WalStressScenario) -> None:
    with database.transaction() as session:
        repository = SqlAlchemyPipelineRepository(session)
        repository.create(
            pipeline_id=scenario.pipeline_id,
            display_name="WAL stress pipeline",
            description="Synthetic persistence concurrency verification",
            created_at=scenario.create_run.created_at,
        )
        repository.publish_version(
            pipeline_id=scenario.pipeline_id,
            expected_latest_version=None,
            specification=ConfigurationDocument.from_mapping({"nodes": []}),
            planner_format_version=1,
            published_at=scenario.create_run.created_at,
        )


def _coherent_frontier(connection: Connection, run_id: str) -> int:
    connection.exec_driver_sql("BEGIN")
    try:
        event_count = cast(
            int,
            connection.exec_driver_sql(
                "SELECT COUNT(*) FROM execution_events WHERE run_id = ?", (run_id,)
            ).scalar_one(),
        )
        work_count = cast(
            int,
            connection.exec_driver_sql(
                "SELECT COUNT(*) FROM work_items WHERE run_id = ?", (run_id,)
            ).scalar_one(),
        )
        checkpoint_count = cast(
            int,
            connection.exec_driver_sql(
                "SELECT COUNT(*) FROM checkpoint_heads WHERE run_id = ?", (run_id,)
            ).scalar_one(),
        )
        counter = cast(
            int,
            connection.exec_driver_sql(
                "SELECT next_sequence_number FROM run_event_counters WHERE run_id = ?", (run_id,)
            ).scalar_one(),
        )
        run_version = cast(
            int,
            connection.exec_driver_sql(
                "SELECT row_version FROM runs WHERE run_id = ?", (run_id,)
            ).scalar_one(),
        )
        node_total = cast(
            int,
            connection.exec_driver_sql(
                "SELECT COALESCE(SUM(work_total), 0) FROM run_nodes WHERE run_id = ?",
                (run_id,),
            ).scalar_one(),
        )
        if not (
            event_count == work_count + 2
            and checkpoint_count == work_count
            and counter == event_count + 1
            and run_version == event_count
            and node_total == work_count
        ):
            raise WalStressError("a reader observed an incoherent operational snapshot")
        return event_count
    finally:
        connection.exec_driver_sql("ROLLBACK")


def _reader_loop(
    database: SQLiteDatabase,
    run_id: str,
    start: Event,
    ready: Barrier,
    stop: Event,
    final_frontier: int,
    final_seen: Event,
    state: _ReaderState,
) -> None:
    try:
        if not start.wait(5.0):
            raise WalStressError("reader start barrier timed out")
        while not stop.is_set():
            operation_started = time.perf_counter()
            with database.engine.connect() as connection:
                frontier = _coherent_frontier(connection, run_id)
            state.maximum_latency = max(
                state.maximum_latency, time.perf_counter() - operation_started
            )
            state.frontiers.append(frontier)
            if len(state.frontiers) == 1:
                ready.wait(5.0)
            if frontier == final_frontier:
                final_seen.set()
    except BaseException as error:
        state.error = error
        final_seen.set()
        with suppress(BrokenBarrierError):
            ready.abort()


def _probe_locked_code() -> int:
    connection = sqlite3.connect(":memory:", autocommit=True)
    cursor: sqlite3.Cursor | None = None
    try:
        connection.execute("CREATE TABLE probe (value INTEGER NOT NULL)")
        connection.executemany("INSERT INTO probe VALUES (?)", ((1,), (2,)))
        cursor = connection.execute("SELECT value FROM probe")
        cursor.fetchone()
        try:
            connection.execute("DROP TABLE probe")
        except sqlite3.OperationalError as error:
            return _require_locked_error(error)
        raise WalStressError("SQLite LOCKED probe did not produce contention")
    finally:
        if cursor is not None:
            cursor.close()
        connection.close()


def _require_locked_error(error: sqlite3.OperationalError) -> int:
    code = getattr(error, "sqlite_errorcode", None)
    if type(code) is not int or code != sqlite3.SQLITE_LOCKED:
        raise WalStressError("SQLite LOCKED probe returned an unexpected code") from None
    from sqlalchemy.exc import OperationalError

    if not is_sqlite_contention(OperationalError("DROP TABLE", {}, error)):
        raise WalStressError("SQLite LOCKED probe was not classified as contention") from None
    return code


def _checkpoint_tuple(connection: Connection, mode: str) -> tuple[int, int, int]:
    row = tuple(connection.exec_driver_sql(f"PRAGMA wal_checkpoint({mode})").one())
    if len(row) != 3 or any(type(value) is not int or value < 0 for value in row):
        raise WalStressError("SQLite returned an invalid WAL checkpoint result")
    return cast(tuple[int, int, int], row)


def _operational_evidence(connection: Connection, run_id: str) -> OperationalEvidence:
    event_count, minimum, maximum = connection.exec_driver_sql(
        "SELECT COUNT(*), MIN(sequence_number), MAX(sequence_number) "
        "FROM execution_events WHERE run_id = ?",
        (run_id,),
    ).one()
    work_count = connection.exec_driver_sql(
        "SELECT COUNT(*) FROM work_items WHERE run_id = ?", (run_id,)
    ).scalar_one()
    heads = connection.exec_driver_sql(
        "SELECT COUNT(*) FROM checkpoint_heads WHERE run_id = ?", (run_id,)
    ).scalar_one()
    next_sequence = connection.exec_driver_sql(
        "SELECT next_sequence_number FROM run_event_counters WHERE run_id = ?", (run_id,)
    ).scalar_one()
    run_version = connection.exec_driver_sql(
        "SELECT row_version FROM runs WHERE run_id = ?", (run_id,)
    ).scalar_one()
    node_total = connection.exec_driver_sql(
        "SELECT COALESCE(SUM(work_total), 0) FROM run_nodes WHERE run_id = ?", (run_id,)
    ).scalar_one()
    values = (event_count, work_count, heads, next_sequence, run_version, node_total)
    if any(type(value) is not int for value in values) or minimum != 1 or maximum != event_count:
        raise WalStressError("final operational frontiers are not contiguous")
    return OperationalEvidence(
        cast(int, event_count),
        cast(int, next_sequence),
        cast(int, run_version),
        cast(int, work_count),
        cast(int, heads),
        cast(int, node_total),
    )


def _assert_report_gates(report: WalStressReport) -> None:
    expected = report.workload.work_commands + 2
    if not (
        report.submitted == expected
        and report.admitted == expected
        and report.committed == expected
        and report.failures == 0
        and report.writer.accepted == expected
        and report.writer.completed == expected
        and report.writer.contention_retries >= 1
        and report.close.drained
        and report.operational.execution_events == expected
        and report.operational.next_event_sequence == expected + 1
        and report.operational.run_row_version == expected
        and report.operational.work_items == report.workload.work_commands
        and report.operational.checkpoint_heads == report.workload.work_commands
        and report.operational.node_work_total == report.workload.work_commands
    ):
        raise WalStressError("WAL stress command or frontier gates failed")
    if not report.contention_codes or not any(
        code == sqlite3.SQLITE_BUSY_SNAPSHOT for code in report.contention_codes
    ):
        raise WalStressError("WAL stress did not observe SQLITE_BUSY_SNAPSHOT")
    if report.locked_probe_code != sqlite3.SQLITE_LOCKED:
        raise WalStressError("WAL stress did not observe SQLITE_LOCKED")
    if any(producer.admitted == 0 for producer in report.producers):
        raise WalStressError("WAL stress producer fairness gate failed")
    if any(
        reader.operations < 2 or reader.last_frontier <= reader.first_frontier
        for reader in report.readers
    ):
        raise WalStressError("WAL stress reader progress gate failed")
    if report.pinned_reader_start_frontier != report.pinned_reader_end_frontier:
        raise WalStressError("pinned WAL reader snapshot changed")
    integrity = report.integrity
    if not (
        integrity.journal_mode == "wal"
        and integrity.synchronous_level == 2
        and integrity.foreign_keys
        and integrity.busy_timeout_ms == 5_000
        and integrity.quick_check == "ok"
        and integrity.foreign_key_violations == 0
        and integrity.pool_checked_out == 0
        and integrity.writer_thread_stopped
        and integrity.reader_threads_stopped
        and integrity.sidecars_absent
    ):
        raise WalStressError("WAL stress integrity or resource gates failed")
    platform_budget = (
        min(report.workload.total_budget_seconds, 20.0)
        if report.platform == "Linux" and report.profile.value == "ci"
        else report.workload.total_budget_seconds
    )
    if report.elapsed_seconds > platform_budget:
        raise WalStressError("WAL stress exceeded its platform budget")


def _checked_out_connections(pool: object) -> int:
    if not isinstance(pool, QueuePool):
        raise WalStressError("WAL stress requires the file-database QueuePool")
    return pool.checkedout()


def _join_threads(threads: list[Thread], timeout_seconds: float, subject: str) -> None:
    deadline = time.monotonic() + timeout_seconds
    for thread in threads:
        thread.join(max(0.0, deadline - time.monotonic()))
    if any(thread.is_alive() for thread in threads):
        raise WalStressError(f"WAL stress {subject} did not stop within its bound")


def _remove_failed_database(path: Path) -> None:
    for candidate in (Path(f"{path}-wal"), Path(f"{path}-shm"), path):
        try:
            candidate.unlink(missing_ok=True)
        except OSError:
            continue


def run_wal_stress(config: WalStressConfig) -> WalStressReport:
    """Run the bounded WAL concurrency scenario against one new explicit database."""
    if type(config) is not WalStressConfig:
        raise TypeError("WAL stress configuration is invalid")
    path = config.database_path
    if path.exists():
        raise WalStressError("WAL stress database must be a new file")
    workload = workload_for(config.profile)
    scenario = build_scenario(config.profile, config.seed, workload)
    started = time.perf_counter()
    database: SQLiteDatabase | None = None
    writer: SQLiteTransactionalWriter | None = None
    injector: _BusySnapshotInjector | None = None
    pinned: Connection | None = None
    readers: list[Thread] = []
    stop_readers = Event()
    failure: BaseException | None = None
    try:
        database = SQLiteDatabase.open(
            SQLiteDatabaseConfig(path, create_parent=config.create_parent)
        )
        with database.engine.connect() as connection:
            upgrade_to_head(connection)
        _seed_pipeline(database, scenario)
        resident_ready = Event()
        writer = SQLiteTransactionalWriter(
            _session_factory(database),
            WriterSettings(
                queue_capacity=workload.queue_capacity,
                admission_waiter_capacity=workload.admission_capacity,
                notification_capacity=workload.notification_capacity,
                max_contention_attempts=workload.max_contention_attempts,
                contention_delay_seconds=0.0,
                thread_name="paritygrid-wal-stress-writer",
            ),
        )
        injector = _BusySnapshotInjector(
            database, path, resident_ready, lambda: writer.thread, workload.timeout_seconds
        )
        injector.install()
        writer.start()
        initial_tickets = (
            writer.submit(scenario.create_run, timeout_seconds=workload.timeout_seconds),
            writer.submit(scenario.start_run, timeout_seconds=workload.timeout_seconds),
        )
        for ticket in initial_tickets:
            ticket.result(timeout_seconds=workload.timeout_seconds)

        pinned = database.engine.connect()
        pinned.exec_driver_sql("BEGIN")
        pinned_start = cast(
            int,
            pinned.exec_driver_sql(
                "SELECT COUNT(*) FROM execution_events WHERE run_id = ?", (str(scenario.run_id),)
            ).scalar_one(),
        )

        reader_start = Event()
        reader_ready = Barrier(workload.reader_count + 1)
        final_frontier = workload.work_commands + 2
        final_seen = [Event() for _ in range(workload.reader_count)]
        reader_states = [_ReaderState([]) for _ in range(workload.reader_count)]
        for index in range(workload.reader_count):
            thread = Thread(
                target=_reader_loop,
                name=f"paritygrid-wal-reader-{index}",
                args=(
                    database,
                    str(scenario.run_id),
                    reader_start,
                    reader_ready,
                    stop_readers,
                    final_frontier,
                    final_seen[index],
                    reader_states[index],
                ),
                daemon=False,
            )
            readers.append(thread)
            thread.start()
        reader_start.set()
        try:
            reader_ready.wait(workload.timeout_seconds)
        except BrokenBarrierError:
            reader_error = next(
                (state.error for state in reader_states if state.error is not None), None
            )
            if reader_error is not None:
                raise WalStressError("WAL stress reader initialization failed") from reader_error
            raise WalStressError("WAL stress reader initialization timed out") from None

        injector.arm()
        producer_condition = Condition()
        next_index = 0
        producer_tickets: list[WriterTicket | None] = [None] * workload.work_commands
        producer_counts = [0] * workload.producer_count
        producer_wait = [0.0] * workload.producer_count
        producer_errors: list[BaseException] = []

        def produce(producer: int) -> None:
            nonlocal next_index
            try:
                while True:
                    with producer_condition:
                        turn_deadline = time.monotonic() + workload.timeout_seconds / 2
                        while (
                            next_index < workload.work_commands
                            and scenario.owners[next_index] != producer
                        ):
                            remaining = turn_deadline - time.monotonic()
                            if remaining <= 0:
                                raise WalStressError("producer turn wait timed out")
                            producer_condition.wait(remaining)
                        if next_index >= workload.work_commands:
                            return
                        index = next_index
                    admission_started = time.perf_counter()
                    ticket = writer.submit(
                        scenario.work[index], timeout_seconds=workload.timeout_seconds
                    )
                    waited = time.perf_counter() - admission_started
                    with producer_condition:
                        producer_tickets[index] = ticket
                        producer_counts[producer] += 1
                        producer_wait[producer] += waited
                        next_index += 1
                        if next_index >= workload.queue_capacity + 1:
                            resident_ready.set()
                        producer_condition.notify_all()
            except BaseException as error:
                with producer_condition:
                    producer_errors.append(error)
                    next_index = workload.work_commands
                    resident_ready.set()
                    producer_condition.notify_all()

        producers = [
            Thread(
                target=produce,
                name=f"paritygrid-wal-producer-{index}",
                args=(index,),
                daemon=False,
            )
            for index in range(workload.producer_count)
        ]
        for thread in producers:
            thread.start()
        _join_threads(producers, workload.timeout_seconds, "producer")
        if producer_errors:
            raise WalStressError("WAL stress producer failed") from None

        failures = 0
        for ticket in cast(list[WriterTicket], producer_tickets):
            try:
                ticket.result(timeout_seconds=workload.timeout_seconds)
            except BaseException:
                failures += 1
        if failures:
            raise WalStressError("WAL stress command failed")
        for seen in final_seen:
            if not seen.wait(workload.timeout_seconds):
                raise WalStressError("fresh reader did not observe the final frontier")
        stop_readers.set()
        _join_threads(readers, workload.timeout_seconds, "reader")
        if any(state.error is not None for state in reader_states):
            raise WalStressError("WAL stress reader failed")

        pinned_end = cast(
            int,
            pinned.exec_driver_sql(
                "SELECT COUNT(*) FROM execution_events WHERE run_id = ?", (str(scenario.run_id),)
            ).scalar_one(),
        )
        with database.engine.connect() as connection:
            passive = _checkpoint_tuple(connection, "PASSIVE")
        pinned.exec_driver_sql("ROLLBACK")
        pinned.close()
        pinned = None
        with database.engine.connect() as connection:
            truncated = _checkpoint_tuple(connection, "TRUNCATE")
            operational = _operational_evidence(connection, str(scenario.run_id))
            journal_mode = cast(str, connection.exec_driver_sql("PRAGMA journal_mode").scalar_one())
            synchronous = cast(int, connection.exec_driver_sql("PRAGMA synchronous").scalar_one())
            foreign_keys = cast(int, connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one())
            busy_timeout = cast(int, connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one())
            quick_check = cast(str, connection.exec_driver_sql("PRAGMA quick_check").scalar_one())
            foreign_key_violations = len(
                connection.exec_driver_sql("PRAGMA foreign_key_check").all()
            )

        close = writer.close(timeout_seconds=workload.timeout_seconds)
        diagnostics = writer.snapshot()
        notification_stats = writer.notifications.stats()
        locked_code = _probe_locked_code()
        contention_codes = injector.codes
        injector.remove()
        injector = None
        checked_out = _checked_out_connections(database.engine.pool)
        writer_stopped = writer.thread is not None and not writer.thread.is_alive()
        database.close()
        database = None
        sidecars_absent = not Path(f"{path}-wal").exists() and not Path(f"{path}-shm").exists()
        elapsed = time.perf_counter() - started
        report = WalStressReport(
            schema_version=WAL_STRESS_REPORT_SCHEMA_VERSION,
            profile=config.profile,
            seed=config.seed,
            platform=platform.system(),
            python_version=platform.python_version(),
            sqlite_version=sqlite3.sqlite_version,
            scenario_manifest_sha256=scenario.manifest_sha256,
            workload=workload,
            elapsed_seconds=elapsed,
            submitted=workload.work_commands + 2,
            admitted=diagnostics.accepted,
            committed=diagnostics.completed,
            failures=0,
            writer=diagnostics,
            close=close,
            notifications=notification_stats,
            contention_codes=contention_codes,
            locked_probe_code=locked_code,
            producers=tuple(
                ProducerEvidence(index, producer_counts[index], producer_wait[index])
                for index in range(workload.producer_count)
            ),
            readers=tuple(
                ReaderEvidence(
                    index,
                    len(state.frontiers),
                    state.frontiers[0],
                    state.frontiers[-1],
                    state.maximum_latency,
                )
                for index, state in enumerate(reader_states)
            ),
            pinned_reader_start_frontier=pinned_start,
            pinned_reader_end_frontier=pinned_end,
            checkpoints=WalCheckpointEvidence(passive, truncated),
            operational=operational,
            integrity=IntegrityEvidence(
                journal_mode,
                synchronous,
                bool(foreign_keys),
                busy_timeout,
                quick_check,
                foreign_key_violations,
                checked_out,
                writer_stopped,
                not any(thread.is_alive() for thread in readers),
                sidecars_absent,
            ),
        )
        _assert_report_gates(report)
        return report
    except BaseException as error:
        failure = error
        raise
    finally:
        stop_readers.set()
        if pinned is not None:
            with suppress(BaseException):
                pinned.exec_driver_sql("ROLLBACK")
            pinned.close()
        for thread in readers:
            thread.join(1.0)
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        if injector is not None:
            injector.remove()
        if database is not None:
            database.close()
        if failure is not None:
            _remove_failed_database(path)


def write_report_atomic(report: WalStressReport, path: Path) -> None:
    """Publish canonical JSON through an adjacent atomic replacement."""
    if type(report) is not WalStressReport:
        raise TypeError("WAL stress report is invalid")
    path_value = cast(object, path)
    if not isinstance(path_value, Path) or not path.is_absolute():
        raise ValueError("WAL stress report path must be absolute")
    if not path.parent.is_dir():
        raise WalStressError("WAL stress report parent directory does not exist")
    candidate = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    encoded = (
        json.dumps(report.to_mapping(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")
    try:
        with candidate.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(candidate, path)
    except OSError as error:
        candidate.unlink(missing_ok=True)
        raise WalStressError("WAL stress report could not be published") from error


__all__ = ["run_wal_stress", "write_report_atomic"]
