"""Isolated subprocess worker for crash-and-reopen verification."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from threading import Event, Lock, Thread, local
from typing import BinaryIO, ClassVar, TextIO, cast

from sqlalchemy import event
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session, SessionTransaction, sessionmaker

from paritygrid.adapters.persistence import SQLiteDatabase, SQLiteDatabaseConfig
from paritygrid.adapters.persistence.sqlite import SessionFactory
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter
from paritygrid.adapters.persistence.writer.notifications import (
    BoundedCommittedNotificationBuffer,
)
from paritygrid.application.ports.writer import (
    CommittedNotification,
    WriterSettings,
    WriterTicket,
)

from .crash_reopen_protocol import (
    CrashControlEnvelope,
    CrashFailpoint,
    CrashMarker,
    CrashMarkerEmitter,
    CrashReopenProtocolError,
    load_control,
    validate_release_commit,
)
from .crash_reopen_scenario import completion_command


class CrashReopenWorkerError(Exception):
    """The isolated worker could not reach its requested boundary."""


class _InstrumentedSession(Session):
    instrumentation: ClassVar[_WorkerInstrumentation | None] = None

    def close(self) -> None:
        instrumentation = self.instrumentation
        ordinal = self.info.get("crash_command_ordinal")
        if instrumentation is not None and type(ordinal) is int:
            instrumentation.session_close_entered(ordinal)
        super().close()
        if instrumentation is not None and type(ordinal) is int:
            instrumentation.emit(CrashMarker.SESSION_CLOSED, ordinal)


class _InstrumentedNotifications(BoundedCommittedNotificationBuffer):
    def __init__(self, instrumentation: _WorkerInstrumentation) -> None:
        super().__init__(4)
        self._instrumentation = instrumentation

    def offer(self, notification: CommittedNotification) -> bool:
        ordinal = self._instrumentation.current_ordinal()
        self._instrumentation.emit(CrashMarker.RECEIPT_RESOLVED, ordinal)
        self._instrumentation.emit(CrashMarker.NOTIFICATION_ENTERED, ordinal)
        self._instrumentation.hold_after_receipt(ordinal)
        result = super().offer(notification)
        self._instrumentation.emit(CrashMarker.NOTIFICATION_OFFERED, ordinal)
        self._instrumentation.notification_complete.set()
        return result


class _WorkerInstrumentation:
    def __init__(
        self,
        envelope: CrashControlEnvelope,
        emitter: CrashMarkerEmitter,
        stdin: BinaryIO,
    ) -> None:
        self.envelope = envelope
        self.emitter = emitter
        self.stdin = stdin
        self.admission_gates = {ordinal: Event() for ordinal in range(1, 4)}
        self.precommit_boundaries = {ordinal: Event() for ordinal in range(1, 4)}
        self.release_commit = Event()
        self.notification_complete = Event()
        self._ordinal_lock = Lock()
        self._next_ordinal = 1
        self._thread_state = local()
        self._control_thread: Thread | None = None

    def install(self, engine: object) -> None:
        _InstrumentedSession.instrumentation = self
        event.listen(_InstrumentedSession, "after_begin", self.after_begin)
        event.listen(_InstrumentedSession, "before_commit", self.before_commit)
        event.listen(_InstrumentedSession, "after_commit", self.after_commit)
        event.listen(engine, "commit", self.commit_entered)

    def remove(self, engine: object) -> None:
        event.remove(_InstrumentedSession, "after_begin", self.after_begin)
        event.remove(_InstrumentedSession, "before_commit", self.before_commit)
        event.remove(_InstrumentedSession, "after_commit", self.after_commit)
        event.remove(engine, "commit", self.commit_entered)
        _InstrumentedSession.instrumentation = None

    def start_control_reader(self) -> None:
        if self.envelope.failpoint is not CrashFailpoint.COMMIT_AMBIGUOUS:
            return

        def read_control() -> None:
            frame = self.stdin.readline(513)
            try:
                validate_release_commit(frame, self.envelope.invocation_token)
            except CrashReopenProtocolError:
                return
            self.release_commit.set()

        self._control_thread = Thread(
            target=read_control,
            name="paritygrid-crash-control",
            daemon=True,
        )
        self._control_thread.start()

    def admit(self, ordinal: int) -> None:
        self.emit(CrashMarker.COMMAND_ADMITTED, ordinal)
        self.admission_gates[ordinal].set()

    def emit(self, marker: CrashMarker, ordinal: int) -> None:
        self.emitter.emit(marker, ordinal)

    def current_ordinal(self) -> int:
        ordinal = getattr(self._thread_state, "ordinal", None)
        if type(ordinal) is not int:
            raise CrashReopenWorkerError("worker command identity is unavailable")
        return ordinal

    def after_begin(
        self,
        session: Session,
        transaction: SessionTransaction,
        _connection: Connection,
    ) -> None:
        if transaction.parent is not None:
            return
        with self._ordinal_lock:
            ordinal = self._next_ordinal
            self._next_ordinal += 1
        if ordinal not in self.admission_gates:
            raise CrashReopenWorkerError("worker command count exceeded the scenario")
        session.info["crash_command_ordinal"] = ordinal
        self._thread_state.ordinal = ordinal
        if not self.admission_gates[ordinal].wait(self.envelope.hold_timeout_seconds):
            raise CrashReopenWorkerError("worker admission release timed out")

    def before_commit(self, session: Session) -> None:
        ordinal = self._session_ordinal(session)
        self.emit(CrashMarker.PRE_COMMIT, ordinal)
        self.precommit_boundaries[ordinal].set()
        if self.envelope.failpoint is CrashFailpoint.BEFORE_COMMIT and ordinal == 1:
            self._hold_forever()
        if self.envelope.failpoint is CrashFailpoint.SHUTDOWN_DRAIN and ordinal == 2:
            self._hold_forever()

    def commit_entered(self, _connection: Connection) -> None:
        ordinal = self.current_ordinal()
        self.emit(CrashMarker.COMMIT_ENTERED, ordinal)
        if (
            self.envelope.failpoint is CrashFailpoint.COMMIT_AMBIGUOUS
            and ordinal == 1
            and not self.release_commit.wait(self.envelope.hold_timeout_seconds)
        ):
            raise CrashReopenWorkerError("commit release timed out")

    def after_commit(self, session: Session) -> None:
        self.emit(CrashMarker.COMMIT_CONFIRMED, self._session_ordinal(session))

    def session_close_entered(self, ordinal: int) -> None:
        self.emit(CrashMarker.SESSION_CLOSE_ENTERED, ordinal)
        if self.envelope.failpoint is CrashFailpoint.AFTER_COMMIT_BEFORE_RECEIPT and ordinal == 1:
            self._hold_forever()

    def hold_after_receipt(self, ordinal: int) -> None:
        if (
            self.envelope.failpoint is CrashFailpoint.AFTER_RECEIPT_BEFORE_NOTIFICATION
            and ordinal == 1
        ):
            self._hold_forever()

    def _hold_forever(self) -> None:
        if not Event().wait(self.envelope.hold_timeout_seconds):
            raise CrashReopenWorkerError("crash boundary hold timed out")

    def hold_for_parent(self) -> None:
        """Keep the worker live until the parent applies the crash action."""
        self._hold_forever()

    @staticmethod
    def _session_ordinal(session: Session) -> int:
        ordinal = session.info.get("crash_command_ordinal")
        if type(ordinal) is not int:
            raise CrashReopenWorkerError("worker Session identity is unavailable")
        return ordinal


def _session_factory(database: SQLiteDatabase) -> SessionFactory:
    factory = sessionmaker(
        bind=database.engine,
        class_=_InstrumentedSession,
        autoflush=False,
        expire_on_commit=False,
    )
    return cast(SessionFactory, factory)


def _settings() -> WriterSettings:
    return WriterSettings(
        queue_capacity=4,
        admission_waiter_capacity=4,
        notification_capacity=4,
        max_contention_attempts=3,
        contention_delay_seconds=0.0,
        thread_name="paritygrid-crash-writer",
    )


def _submit_target(
    writer: SQLiteTransactionalWriter,
    instrumentation: _WorkerInstrumentation,
    ordinal: int,
) -> WriterTicket:
    ticket = writer.submit(completion_command(ordinal - 1), timeout_seconds=5.0)
    instrumentation.admit(ordinal)
    return ticket


def run_worker(
    control_path: Path,
    *,
    stdin: BinaryIO,
    stdout: BinaryIO,
) -> int:
    """Execute one closed worker case and expose only durable protocol markers."""
    envelope = load_control(control_path)
    emitter = CrashMarkerEmitter(envelope, stdout)
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(envelope.database_path))
    instrumentation = _WorkerInstrumentation(envelope, emitter, stdin)
    notifications = _InstrumentedNotifications(instrumentation)
    instrumentation.install(database.engine)
    instrumentation.start_control_reader()
    writer = SQLiteTransactionalWriter(
        _session_factory(database),
        _settings(),
        notifications=notifications,
    )
    try:
        instrumentation.emit(CrashMarker.WORKER_READY, 0)
        writer.start()
        first = _submit_target(writer, instrumentation, 1)
        first.result(timeout_seconds=envelope.hold_timeout_seconds)
        if not instrumentation.notification_complete.wait(envelope.hold_timeout_seconds):
            raise CrashReopenWorkerError("notification observation timed out")
        instrumentation.emit(CrashMarker.COMMAND_OBSERVED, 1)

        if envelope.failpoint is CrashFailpoint.SHUTDOWN_DRAIN:
            _submit_target(writer, instrumentation, 2)
            if not instrumentation.precommit_boundaries[2].wait(envelope.hold_timeout_seconds):
                raise CrashReopenWorkerError("shutdown in-flight boundary timed out")
            _submit_target(writer, instrumentation, 3)
            instrumentation.emit(CrashMarker.SHUTDOWN_ENTERED, 0)
            writer.close(timeout_seconds=0.001)
            instrumentation.hold_for_parent()

        instrumentation.emit(CrashMarker.SHUTDOWN_ENTERED, 0)
        closed = writer.close(timeout_seconds=envelope.hold_timeout_seconds)
        if not closed.drained:
            raise CrashReopenWorkerError("worker shutdown did not drain")
        instrumentation.emit(CrashMarker.SHUTDOWN_DRAINED, 0)
        return 0
    finally:
        writer.close(timeout_seconds=1.0)
        instrumentation.remove(database.engine)
        database.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--control", required=True)
    return parser


def main(
    arguments: list[str] | None = None,
    *,
    stdin: BinaryIO | None = None,
    stdout: BinaryIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Validate one worker invocation and return a stable process exit code."""
    active_stdin = sys.stdin.buffer if stdin is None else stdin
    active_stdout = sys.stdout.buffer if stdout is None else stdout
    active_stderr = sys.stderr if stderr is None else stderr
    try:
        parsed = _parser().parse_args(arguments)
        return run_worker(Path(parsed.control), stdin=active_stdin, stdout=active_stdout)
    except BaseException:
        active_stderr.write('{"error":"crash_worker_failed"}\n')
        active_stderr.flush()
        return 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["CrashReopenWorkerError", "main", "run_worker"]
