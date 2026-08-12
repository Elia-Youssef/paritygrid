"""Deterministic lifecycle tests for the bounded transactional writer."""

# pyright: reportPrivateUsage=false

import asyncio
import inspect
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from threading import Event, Lock, Thread, current_thread
from typing import cast

import pytest
from sqlalchemy import event, text
from sqlalchemy.orm import Session, sessionmaker

from paritygrid.adapters.persistence import SQLiteDatabaseConfig
from paritygrid.adapters.persistence.sqlite import (
    SessionFactory,
    create_session_factory,
    create_sqlite_engine,
)
from paritygrid.adapters.persistence.writer import core
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter
from paritygrid.adapters.persistence.writer.dispatch import DispatchOutcome
from paritygrid.adapters.persistence.writer.notifications import (
    BoundedCommittedNotificationBuffer,
)
from paritygrid.application.ports.consistency import (
    ConsistencyCorruptionError,
    ConsistencyStateConflictError,
    ConsistencyStorageUnavailableError,
)
from paritygrid.application.ports.execution import (
    ExecutionCorruptionError,
    ExecutionLeaseExpiredError,
    ExecutionLeaseLostError,
    ExecutionLeaseMismatchError,
    ExecutionStateConflictError,
    ExecutionStorageUnavailableError,
)
from paritygrid.application.ports.repair_audit import (
    AuditCorruptionError,
    AuditSequenceConflictError,
    AuditStorageUnavailableError,
    RepairCorruptionError,
    RepairStateConflictError,
    RepairStorageUnavailableError,
)
from paritygrid.application.ports.writer import (
    MAX_WRITER_SUBMISSION_ID,
    CommittedNotification,
    PersistenceContentionError,
    WriterAdmissionTimeoutError,
    WriterClosedError,
    WriterCommand,
    WriterCommandKind,
    WriterCommandResult,
    WriterCommitOutcomeUnknownError,
    WriterDefinitelyNotExecutedError,
    WriterFailedError,
    WriterInvalidRequestError,
    WriterNotStartedError,
    WriterResultTimeoutError,
    WriterSettings,
    WriterState,
    WriterTicket,
)
from paritygrid.domain.models import RunId


@dataclass(frozen=True, slots=True)
class _Command:
    label: str
    run_id: RunId = field(default_factory=lambda: RunId("run_writercore"))

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.FINALIZE_EMPTY_RUN_NODE


@dataclass(frozen=True, slots=True)
class _Result:
    label: str

    @property
    def result_kind(self) -> WriterCommandKind:
        return WriterCommandKind.FINALIZE_EMPTY_RUN_NODE


class _FatalSignal(BaseException):
    pass


class _CloseFailSession(Session):
    def close(self) -> None:
        super().close()
        raise RuntimeError("close failure")


class _BlockingCloseFailSession(Session):
    close_entered = Event()
    close_release = Event()

    def close(self) -> None:
        self.close_entered.set()
        assert self.close_release.wait(2)
        super().close()
        raise RuntimeError("close failure")


class _ExceptionalNotifications(BoundedCommittedNotificationBuffer):
    def offer(self, notification: object) -> bool:
        del notification
        raise RuntimeError("notification failure")


class _RollbackFailure:
    is_active = True

    def rollback(self) -> None:
        raise RuntimeError("rollback failure")


class _CloseFailure:
    def close(self) -> None:
        raise RuntimeError("close failure")


@pytest.fixture
def sessions(tmp_path: Path) -> Iterator[sessionmaker[Session]]:
    engine = create_sqlite_engine(SQLiteDatabaseConfig(tmp_path / "writer core %.db"))
    try:
        yield create_session_factory(engine)
    finally:
        engine.dispose()


@pytest.fixture(autouse=True)
def accept_test_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    def validate(command: WriterCommand) -> WriterCommand:
        return command

    monkeypatch.setattr(core, "validate_command", validate)


def settings(**overrides: object) -> WriterSettings:
    values: dict[str, object] = {
        "queue_capacity": 2,
        "notification_capacity": 2,
        "max_contention_attempts": 3,
        "contention_delay_seconds": 0.0,
        "thread_name": "paritygrid-test-writer",
    }
    values.update(overrides)
    return WriterSettings(**values)  # type: ignore[arg-type]


def command(label: str) -> WriterCommand:
    return cast(WriterCommand, _Command(label))


def successful_dispatch(
    _session: Session,
    submitted: WriterCommand,
) -> DispatchOutcome:
    value = cast(_Command, submitted)
    return DispatchOutcome(cast(WriterCommandResult, _Result(value.label)))


def test_start_is_explicit_single_use_and_close_before_start_is_final(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(core, "dispatch_command", successful_dispatch)
    writer = SQLiteTransactionalWriter(sessions, settings())
    with pytest.raises(WriterNotStartedError, match="started"):
        writer.submit(command("not-started"), timeout_seconds=1.0)

    writer.start()
    assert writer.thread is not None
    assert writer.thread.name == "paritygrid-test-writer"
    assert not writer.thread.daemon
    with pytest.raises(WriterInvalidRequestError, match="single-use"):
        writer.start()
    assert writer.close(timeout_seconds=1.0).drained

    never_started = SQLiteTransactionalWriter(sessions, settings())
    assert never_started.close(timeout_seconds=0.0).drained
    with pytest.raises(WriterClosedError, match="closed"):
        never_started.submit(command("closed"), timeout_seconds=1.0)
    with pytest.raises(WriterInvalidRequestError, match="single-use"):
        never_started.start()


def test_snapshot_is_coherent_across_writer_lifecycle(
    sessions: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(core, "dispatch_command", successful_dispatch)
    writer = SQLiteTransactionalWriter(sessions, settings(queue_capacity=2))
    initial = writer.snapshot()
    assert initial.state is WriterState.NEW
    assert initial.queue_capacity == 2
    assert initial.accepted == initial.completed == 0

    writer.start()
    assert writer.snapshot().state is WriterState.RUNNING
    ticket = writer.submit(command("snapshot"), timeout_seconds=1.0)
    ticket.result(timeout_seconds=1.0)
    assert writer.close(timeout_seconds=1.0).drained

    closed = writer.snapshot()
    assert closed.state is WriterState.CLOSED
    assert closed.accepted == closed.completed == 1
    assert closed.queue_depth == closed.admission_waiters == closed.in_flight == 0
    assert closed.max_resident <= closed.queue_capacity + 1


def test_commit_receipt_and_notification_follow_session_close(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observations: list[tuple[str, object]] = []

    class ObservedSession(Session):
        def close(self) -> None:
            super().close()
            observations.append(("close", current_thread().name))

    observed_sessions = sessionmaker(bind=sessions.kw["bind"], class_=ObservedSession)

    def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
        observations.append(("dispatch", (current_thread().name, session.in_transaction())))
        return successful_dispatch(session, submitted)

    class ObservedNotifications(BoundedCommittedNotificationBuffer):
        def offer(self, notification: object) -> bool:
            observations.append(("notification", notification))
            return super().offer(cast(CommittedNotification, notification))

    notifications = ObservedNotifications(2)
    monkeypatch.setattr(core, "dispatch_command", dispatch)
    writer = SQLiteTransactionalWriter(
        cast(SessionFactory, observed_sessions), settings(), notifications=notifications
    )
    writer.start()
    ticket = writer.submit(command("committed"), timeout_seconds=1.0)
    receipt = ticket.result(timeout_seconds=1.0)
    observations.append(("receipt", receipt))
    assert receipt.submission_id.number == 1
    assert receipt.contention_attempts == 0
    assert receipt.mutated
    assert cast(_Result, receipt.result).label == "committed"
    assert observations[0] == ("dispatch", ("paritygrid-test-writer", True))
    assert observations[1] == ("close", "paritygrid-test-writer")
    assert observations[2][0] == "notification"
    assert observations[3][0] == "receipt"
    assert notifications.take() is not None
    assert notifications.take() is None
    assert writer.close(timeout_seconds=1.0).drained


def test_waiting_capacity_excludes_in_flight_and_admission_timeout_is_not_queued(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = Event()
    release = Event()
    order: list[str] = []

    def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
        value = cast(_Command, submitted)
        order.append(value.label)
        if value.label == "first":
            entered.set()
            assert release.wait(2)
        return successful_dispatch(session, submitted)

    monkeypatch.setattr(core, "dispatch_command", dispatch)
    writer = SQLiteTransactionalWriter(sessions, settings(queue_capacity=1))
    writer.start()
    first = writer.submit(command("first"), timeout_seconds=1.0)
    assert entered.wait(1)
    second = writer.submit(command("second"), timeout_seconds=1.0)
    with pytest.raises(WriterAdmissionTimeoutError, match="before enqueue"):
        writer.submit(command("third"), timeout_seconds=0.01)
    release.set()
    assert first.result(timeout_seconds=1.0).submission_id.number == 1
    assert second.result(timeout_seconds=1.0).submission_id.number == 2
    assert order == ["first", "second"]
    close = writer.close(timeout_seconds=1.0)
    assert (close.accepted, close.completed) == (2, 2)


def test_result_timeout_keeps_ticket_reusable(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = Event()

    def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
        assert release.wait(2)
        return successful_dispatch(session, submitted)

    monkeypatch.setattr(core, "dispatch_command", dispatch)
    writer = SQLiteTransactionalWriter(sessions, settings())
    writer.start()
    ticket = writer.submit(command("slow"), timeout_seconds=1.0)
    with pytest.raises(WriterResultTimeoutError, match="timed out"):
        ticket.result(timeout_seconds=0.01)
    release.set()
    assert cast(_Result, ticket.result(timeout_seconds=1.0).result).label == "slow"
    assert writer.close(timeout_seconds=1.0).drained


def test_contention_retries_whole_command_with_fresh_sessions(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts: list[Session] = []

    def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
        attempts.append(session)
        if len(attempts) < 3:
            raise PersistenceContentionError("Database is temporarily contended.")
        return successful_dispatch(session, submitted)

    monkeypatch.setattr(core, "dispatch_command", dispatch)
    writer = SQLiteTransactionalWriter(sessions, settings(max_contention_attempts=3))
    writer.start()
    receipt = writer.submit(command("retry"), timeout_seconds=1.0).result(timeout_seconds=1.0)
    assert receipt.contention_attempts == 2
    assert len({id(session) for session in attempts}) == 3
    assert all(not session.in_transaction() for session in attempts[:2])
    assert writer.close(timeout_seconds=1.0).drained


def test_contention_exhaustion_fails_closed_and_rejects_queued_ticket(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = Event()
    release = Event()

    def dispatch(_session: Session, _submitted: WriterCommand) -> DispatchOutcome:
        entered.set()
        assert release.wait(2)
        raise PersistenceContentionError("Database is temporarily contended.")

    monkeypatch.setattr(core, "dispatch_command", dispatch)
    writer = SQLiteTransactionalWriter(sessions, settings(max_contention_attempts=1))
    writer.start()
    first = writer.submit(command("first"), timeout_seconds=1.0)
    assert entered.wait(1)
    queued = writer.submit(command("queued"), timeout_seconds=1.0)
    release.set()
    with pytest.raises(WriterFailedError, match="retry limit"):
        first.result(timeout_seconds=1.0)
    with pytest.raises(WriterDefinitelyNotExecutedError, match="not executed"):
        queued.result(timeout_seconds=1.0)
    with pytest.raises(WriterFailedError, match="requires recovery"):
        writer.submit(command("later"), timeout_seconds=1.0)
    assert writer.close(timeout_seconds=1.0).drained


def test_stable_repository_conflict_only_fails_current_ticket(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
        if cast(_Command, submitted).label == "conflict":
            raise ExecutionStateConflictError("Execution state does not allow this operation.")
        return successful_dispatch(session, submitted)

    monkeypatch.setattr(core, "dispatch_command", dispatch)
    writer = SQLiteTransactionalWriter(sessions, settings())
    writer.start()
    failed = writer.submit(command("conflict"), timeout_seconds=1.0)
    succeeded = writer.submit(command("next"), timeout_seconds=1.0)
    with pytest.raises(ExecutionStateConflictError, match="does not allow"):
        failed.result(timeout_seconds=1.0)
    assert cast(_Result, succeeded.result(timeout_seconds=1.0).result).label == "next"
    assert writer.close(timeout_seconds=1.0).drained


def test_precommit_base_exception_fails_closed_and_rolls_back(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = Event()
    release = Event()

    def dispatch(session: Session, _submitted: WriterCommand) -> DispatchOutcome:
        session.execute(text("CREATE TABLE IF NOT EXISTS precommit_guard (value INTEGER)"))
        session.execute(text("INSERT INTO precommit_guard VALUES (1)"))
        entered.set()
        assert release.wait(2)
        raise _FatalSignal()

    monkeypatch.setattr(core, "dispatch_command", dispatch)
    writer = SQLiteTransactionalWriter(sessions, settings())
    writer.start()
    first = writer.submit(command("fatal"), timeout_seconds=1.0)
    assert entered.wait(1)
    queued = writer.submit(command("queued"), timeout_seconds=1.0)
    release.set()
    with pytest.raises(WriterFailedError, match="before commit"):
        first.result(timeout_seconds=1.0)
    with pytest.raises(WriterDefinitelyNotExecutedError):
        queued.result(timeout_seconds=1.0)
    with sessions() as session:
        table = session.execute(
            text("SELECT name FROM sqlite_master WHERE name='precommit_guard'")
        ).scalar_one_or_none()
        if table is not None:
            assert session.execute(text("SELECT COUNT(*) FROM precommit_guard")).scalar_one() == 0
    assert writer.close(timeout_seconds=1.0).drained


def test_commit_exception_is_unknown_not_retried_or_notified(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with sessions.begin() as session:
        session.execute(text("CREATE TABLE commit_guard (value INTEGER NOT NULL)"))

    def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
        session.execute(text("INSERT INTO commit_guard VALUES (7)"))
        return successful_dispatch(session, submitted)

    def fail_after_commit(_session: Session) -> None:
        raise RuntimeError("ack lost")

    event.listen(sessions.class_, "after_commit", fail_after_commit)
    monkeypatch.setattr(core, "dispatch_command", dispatch)
    writer = SQLiteTransactionalWriter(sessions, settings())
    writer.start()
    ticket = writer.submit(command("ambiguous"), timeout_seconds=1.0)
    with pytest.raises(WriterCommitOutcomeUnknownError, match="unknown") as captured:
        ticket.result(timeout_seconds=1.0)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert writer.notifications.take() is None
    event.remove(sessions.class_, "after_commit", fail_after_commit)
    with sessions() as session:
        assert session.execute(text("SELECT value FROM commit_guard")).scalar_one() == 7
    assert writer.close(timeout_seconds=1.0).drained


def test_close_failure_after_commit_preserves_receipt_then_fails_writer(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_fail_sessions = sessionmaker(bind=sessions.kw["bind"], class_=_CloseFailSession)
    entered = Event()
    release = Event()

    def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
        entered.set()
        assert release.wait(2)
        return successful_dispatch(session, submitted)

    monkeypatch.setattr(core, "dispatch_command", dispatch)
    writer = SQLiteTransactionalWriter(cast(SessionFactory, close_fail_sessions), settings())
    writer.start()
    committed = writer.submit(command("committed"), timeout_seconds=1.0)
    assert entered.wait(1)
    queued = writer.submit(command("queued"), timeout_seconds=1.0)
    release.set()
    receipt = committed.result(timeout_seconds=1.0)
    assert cast(_Result, receipt.result).label == "committed"
    with pytest.raises(WriterDefinitelyNotExecutedError):
        queued.result(timeout_seconds=1.0)
    assert writer.notifications.take() is None
    assert writer.close(timeout_seconds=1.0).drained


def test_postcommit_receipt_is_not_visible_until_close_attempt_finishes(
    sessions: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    _BlockingCloseFailSession.close_entered.clear()
    _BlockingCloseFailSession.close_release.clear()
    close_fail_sessions = sessionmaker(bind=sessions.kw["bind"], class_=_BlockingCloseFailSession)
    monkeypatch.setattr(core, "dispatch_command", successful_dispatch)
    writer = SQLiteTransactionalWriter(cast(SessionFactory, close_fail_sessions), settings())
    writer.start()
    committed = writer.submit(command("committed"), timeout_seconds=1.0)
    queued = writer.submit(command("queued"), timeout_seconds=1.0)
    assert _BlockingCloseFailSession.close_entered.wait(1)
    with pytest.raises(WriterResultTimeoutError):
        committed.result(timeout_seconds=0.0)
    _BlockingCloseFailSession.close_release.set()
    assert cast(_Result, committed.result(timeout_seconds=1.0).result).label == "committed"
    with pytest.raises(WriterDefinitelyNotExecutedError):
        queued.result(timeout_seconds=1.0)
    assert writer.notifications.take() is None
    assert writer.close(timeout_seconds=1.0).drained


def test_notifications_are_bounded_and_exceptionally_isolated(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(core, "dispatch_command", successful_dispatch)
    bounded = BoundedCommittedNotificationBuffer(1)
    writer = SQLiteTransactionalWriter(sessions, settings(), notifications=bounded)
    writer.start()
    assert writer.submit(command("one"), timeout_seconds=1.0).result(timeout_seconds=1.0).mutated
    assert writer.submit(command("two"), timeout_seconds=1.0).result(timeout_seconds=1.0).mutated
    assert bounded.stats().dropped == 1
    bounded.reject_new()
    assert writer.submit(command("three"), timeout_seconds=1.0).result(timeout_seconds=1.0).mutated
    assert bounded.stats().rejected == 1
    assert writer.close(timeout_seconds=1.0).drained

    exceptional = _ExceptionalNotifications(1)
    second = SQLiteTransactionalWriter(sessions, settings(), notifications=exceptional)
    second.start()
    assert second.submit(command("safe"), timeout_seconds=1.0).result(timeout_seconds=1.0).mutated
    assert exceptional.stats().failures == 1
    assert second.close(timeout_seconds=1.0).drained


def test_read_only_replay_has_no_notification(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def replay(session: Session, submitted: WriterCommand) -> DispatchOutcome:
        return DispatchOutcome(successful_dispatch(session, submitted).result, mutated=False)

    monkeypatch.setattr(core, "dispatch_command", replay)
    writer = SQLiteTransactionalWriter(sessions, settings())
    writer.start()
    receipt = writer.submit(command("replay"), timeout_seconds=1.0).result(timeout_seconds=1.0)
    assert not receipt.mutated
    assert writer.notifications.stats().offered == 0
    assert writer.close(timeout_seconds=1.0).drained


def test_async_admission_cancellation_never_blocks_loop_or_enqueues(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        entered = Event()
        release = Event()

        def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
            if cast(_Command, submitted).label == "first":
                entered.set()
                assert release.wait(2)
            return successful_dispatch(session, submitted)

        monkeypatch.setattr(core, "dispatch_command", dispatch)
        writer = SQLiteTransactionalWriter(sessions, settings(queue_capacity=1))
        writer.start()
        first = writer.submit(command("first"), timeout_seconds=1.0)
        assert await asyncio.to_thread(entered.wait, 1)
        second = writer.submit(command("second"), timeout_seconds=1.0)
        pending = asyncio.create_task(
            writer.submit_async(command("cancelled"), timeout_seconds=1.0)
        )
        heartbeat = 0
        for _ in range(5):
            await asyncio.sleep(0)
            heartbeat += 1
        assert heartbeat == 5
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        release.set()
        await first.result_async(timeout_seconds=1.0)
        await second.result_async(timeout_seconds=1.0)
        close = await asyncio.to_thread(writer.close, timeout_seconds=1.0)
        assert close.accepted == 2

    asyncio.run(scenario())


def test_async_result_cancellation_does_not_retract_and_ticket_is_reusable(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        release = Event()

        def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
            assert release.wait(2)
            return successful_dispatch(session, submitted)

        monkeypatch.setattr(core, "dispatch_command", dispatch)
        writer = SQLiteTransactionalWriter(sessions, settings())
        writer.start()
        ticket = await writer.submit_async(command("admitted"), timeout_seconds=1.0)
        waiting = asyncio.create_task(ticket.result_async(timeout_seconds=1.0))
        await asyncio.sleep(0)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        release.set()
        receipt = await ticket.result_async(timeout_seconds=1.0)
        assert cast(_Result, receipt.result).label == "admitted"
        assert (await asyncio.to_thread(writer.close, timeout_seconds=1.0)).drained

    asyncio.run(scenario())


def test_drain_timeout_never_cancels_accepted_work(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = Event()
    release = Event()

    def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
        entered.set()
        assert release.wait(2)
        return successful_dispatch(session, submitted)

    monkeypatch.setattr(core, "dispatch_command", dispatch)
    writer = SQLiteTransactionalWriter(sessions, settings())
    writer.start()
    ticket = writer.submit(command("drain"), timeout_seconds=1.0)
    assert entered.wait(1)
    first_close = writer.close(timeout_seconds=0.0)
    assert not first_close.drained
    assert (first_close.queued, first_close.in_flight) == (0, 1)
    with pytest.raises(WriterClosedError):
        writer.submit(command("rejected"), timeout_seconds=1.0)
    release.set()
    assert ticket.result(timeout_seconds=1.0).mutated
    assert writer.close(timeout_seconds=1.0).drained


def test_constructor_and_timeout_boundaries_are_closed(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(core, "dispatch_command", successful_dispatch)
    with pytest.raises(WriterInvalidRequestError, match="factory"):
        SQLiteTransactionalWriter(cast(SessionFactory, object()))
    writer = SQLiteTransactionalWriter(sessions, settings())
    writer.start()
    for invalid in (-0.1, 86_400.1, 1, True):
        with pytest.raises(WriterInvalidRequestError, match="range"):
            writer.submit(command("invalid"), timeout_seconds=cast(float, invalid))
    assert writer.close(timeout_seconds=1.0).drained


def test_concurrent_submissions_receive_fifo_identities_without_gaps(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(core, "dispatch_command", successful_dispatch)
    writer = SQLiteTransactionalWriter(sessions, settings(queue_capacity=8))
    writer.start()
    tickets: list[object] = []
    lock = Lock()

    def submit(index: int) -> None:
        ticket = writer.submit(command(f"command-{index}"), timeout_seconds=1.0)
        with lock:
            tickets.append(ticket)

    threads = [Thread(target=submit, args=(index,)) for index in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    typed_tickets = cast(list[WriterTicket], tickets)
    receipts = [ticket.result(timeout_seconds=1.0) for ticket in typed_tickets]
    assert sorted(receipt.submission_id.number for receipt in receipts) == list(range(1, 9))
    assert writer.close(timeout_seconds=1.0).drained


def test_async_ticket_timeout_completed_error_and_duplicate_resolution() -> None:
    async def scenario() -> None:
        ticket = core._WriterTicket(core.WriterSubmissionId(1))
        with pytest.raises(WriterResultTimeoutError):
            await ticket.result_async(timeout_seconds=0.0)
        error = WriterFailedError("closed")
        ticket.resolve_error(error)
        ticket.resolve_receipt(cast(core.WriterReceipt, object()))
        with pytest.raises(WriterFailedError, match="closed"):
            await ticket.result_async(timeout_seconds=1.0)

        completed = core._WriterTicket(core.WriterSubmissionId(2))
        receipt = core.WriterReceipt(
            core.WriterSubmissionId(2),
            WriterCommandKind.FINALIZE_EMPTY_RUN_NODE,
            RunId("run_writercore"),
            0,
            False,
            cast(WriterCommandResult, _Result("done")),
        )
        completed.resolve_receipt(receipt)
        assert await completed.result_async(timeout_seconds=1.0) is receipt

    asyncio.run(scenario())


def test_internal_cleanup_and_completed_future_guards() -> None:
    rollback_failure = core._rollback_and_close(
        cast(core.Session, _CloseFailure()),
        cast(core.SessionTransaction, _RollbackFailure()),
    )
    assert isinstance(rollback_failure, RuntimeError)
    assert core._close_only(None) is None

    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        admission: asyncio.Future[core._WriterTicket] = loop.create_future()
        admission.cancel()
        core._set_admission_result(admission, core._WriterTicket(core.WriterSubmissionId(1)))
        core._set_admission_error(admission, WriterClosedError("closed"))
        outcome: asyncio.Future[core.WriterReceipt] = loop.create_future()
        outcome.cancel()
        core._set_ticket_outcome(outcome, None, WriterClosedError("closed"))
        assert admission.cancelled()
        assert outcome.cancelled()

    asyncio.run(scenario())


def test_cleanup_failure_during_contention_and_stable_conflict_is_fatal(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broken_sessions = sessionmaker(bind=sessions.kw["bind"], class_=_CloseFailSession)

    def contended(_session: Session, _submitted: WriterCommand) -> DispatchOutcome:
        raise PersistenceContentionError("contended")

    monkeypatch.setattr(core, "dispatch_command", contended)
    writer = SQLiteTransactionalWriter(cast(SessionFactory, broken_sessions), settings())
    writer.start()
    with pytest.raises(WriterFailedError, match="cleanup"):
        writer.submit(command("busy"), timeout_seconds=1.0).result(timeout_seconds=1.0)
    assert writer.close(timeout_seconds=1.0).drained

    def conflict(_session: Session, _submitted: WriterCommand) -> DispatchOutcome:
        raise ExecutionStateConflictError("conflict")

    monkeypatch.setattr(core, "dispatch_command", conflict)
    second = SQLiteTransactionalWriter(cast(SessionFactory, broken_sessions), settings())
    second.start()
    with pytest.raises(WriterFailedError, match="cleanup"):
        second.submit(command("conflict"), timeout_seconds=1.0).result(timeout_seconds=1.0)
    assert second.close(timeout_seconds=1.0).drained


def test_contention_delay_and_writer_thread_close_guard(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def once(session: Session, submitted: WriterCommand) -> DispatchOutcome:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise PersistenceContentionError("contended")
        return successful_dispatch(session, submitted)

    monkeypatch.setattr(core, "dispatch_command", once)
    writer = SQLiteTransactionalWriter(sessions, settings(contention_delay_seconds=0.001))
    writer.start()
    assert (
        writer.submit(command("delayed"), timeout_seconds=1.0)
        .result(timeout_seconds=1.0)
        .contention_attempts
        == 1
    )
    assert writer.close(timeout_seconds=1.0).drained

    guarded: SQLiteTransactionalWriter

    def close_from_writer(_session: Session, _submitted: WriterCommand) -> DispatchOutcome:
        guarded.close(timeout_seconds=0.0)
        raise AssertionError("close guard did not reject")

    monkeypatch.setattr(core, "dispatch_command", close_from_writer)
    guarded = SQLiteTransactionalWriter(sessions, settings())
    guarded.start()
    with pytest.raises(WriterInvalidRequestError, match="join itself"):
        guarded.submit(command("self-close"), timeout_seconds=1.0).result(timeout_seconds=1.0)
    assert guarded.close(timeout_seconds=1.0).drained


def test_invalid_settings_subclass_and_async_waiter_rejected_by_close(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SettingsSubclass(WriterSettings):
        pass

    with pytest.raises(WriterInvalidRequestError, match="settings"):
        SQLiteTransactionalWriter(sessions, SettingsSubclass())

    async def scenario() -> None:
        entered = Event()
        release = Event()

        def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
            if cast(_Command, submitted).label == "first":
                entered.set()
                assert release.wait(2)
            return successful_dispatch(session, submitted)

        monkeypatch.setattr(core, "dispatch_command", dispatch)
        writer = SQLiteTransactionalWriter(sessions, settings(queue_capacity=1))
        writer.start()
        first = writer.submit(command("first"), timeout_seconds=1.0)
        assert await asyncio.to_thread(entered.wait, 1)
        writer.submit(command("second"), timeout_seconds=1.0)
        waiting = asyncio.create_task(writer.submit_async(command("waiting"), timeout_seconds=1.0))
        await asyncio.sleep(0)
        close = await asyncio.to_thread(writer.close, timeout_seconds=0.0)
        assert not close.drained
        with pytest.raises(WriterClosedError):
            await waiting
        release.set()
        await first.result_async(timeout_seconds=1.0)
        assert (await asyncio.to_thread(writer.close, timeout_seconds=1.0)).drained

    asyncio.run(scenario())


def test_sync_waiter_is_rejected_by_close_before_admission(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = Event()
    release = Event()

    def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
        if cast(_Command, submitted).label == "first":
            entered.set()
            assert release.wait(2)
        return successful_dispatch(session, submitted)

    monkeypatch.setattr(core, "dispatch_command", dispatch)
    writer = SQLiteTransactionalWriter(sessions, settings(queue_capacity=1))
    writer.start()
    first = writer.submit(command("first"), timeout_seconds=1.0)
    assert entered.wait(1)
    second = writer.submit(command("second"), timeout_seconds=1.0)
    failure: list[BaseException] = []

    def wait_for_admission() -> None:
        try:
            writer.submit(command("waiting"), timeout_seconds=1.0)
        except BaseException as error:
            failure.append(error)

    waiting = Thread(target=wait_for_admission)
    waiting.start()
    while not writer._admissions:
        pass
    assert not writer.close(timeout_seconds=0.0).drained
    waiting.join(1)
    assert len(failure) == 1
    assert isinstance(failure[0], WriterClosedError)
    release.set()
    assert first.result(timeout_seconds=1.0).mutated
    assert second.result(timeout_seconds=1.0).mutated
    assert writer.close(timeout_seconds=1.0).drained


def test_pending_async_admission_is_woken_on_capacity_and_cancelled_waiters_are_skipped(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        entered = Event()
        release = Event()

        def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
            if cast(_Command, submitted).label == "first":
                entered.set()
                assert release.wait(2)
            return successful_dispatch(session, submitted)

        monkeypatch.setattr(core, "dispatch_command", dispatch)
        writer = SQLiteTransactionalWriter(sessions, settings(queue_capacity=1))
        writer.start()
        first = writer.submit(command("first"), timeout_seconds=1.0)
        assert await asyncio.to_thread(entered.wait, 1)
        second = writer.submit(command("second"), timeout_seconds=1.0)
        pending = asyncio.create_task(writer.submit_async(command("third"), timeout_seconds=1.0))
        await asyncio.sleep(0)
        with writer._condition:
            writer._admissions.appendleft(
                core._AdmissionWaiter(command("cancelled"), cancelled=True)
            )
        release.set()
        third = await pending
        await first.result_async(timeout_seconds=1.0)
        await second.result_async(timeout_seconds=1.0)
        assert (
            cast(_Result, (await third.result_async(timeout_seconds=1.0)).result).label == "third"
        )
        assert (await asyncio.to_thread(writer.close, timeout_seconds=1.0)).drained

    asyncio.run(scenario())


def test_inactive_or_absent_cleanup_is_a_noop() -> None:
    class InactiveTransaction:
        is_active = False

    assert core._rollback_and_close(None, None) is None
    assert (
        core._rollback_and_close(None, cast(core.SessionTransaction, InactiveTransaction())) is None
    )


def test_async_cancellation_races_are_classified_by_admission_state(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        entered = Event()
        release = Event()

        def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
            if cast(_Command, submitted).label == "first":
                entered.set()
                assert release.wait(2)
            return successful_dispatch(session, submitted)

        monkeypatch.setattr(core, "dispatch_command", dispatch)
        writer = SQLiteTransactionalWriter(sessions, settings(queue_capacity=1))
        writer.start()
        first = writer.submit(command("first"), timeout_seconds=1.0)
        assert await asyncio.to_thread(entered.wait, 1)
        writer.submit(command("second"), timeout_seconds=1.0)
        removed = asyncio.create_task(writer.submit_async(command("removed"), timeout_seconds=1.0))
        await asyncio.sleep(0)
        writer.close(timeout_seconds=0.0)
        removed.cancel()
        with pytest.raises(asyncio.CancelledError):
            await removed
        release.set()
        await first.result_async(timeout_seconds=1.0)
        assert (await asyncio.to_thread(writer.close, timeout_seconds=1.0)).drained

        second_entered = Event()
        second_release = Event()

        def second_dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
            if cast(_Command, submitted).label == "first":
                second_entered.set()
                assert second_release.wait(2)
            return successful_dispatch(session, submitted)

        monkeypatch.setattr(core, "dispatch_command", second_dispatch)
        second_writer = SQLiteTransactionalWriter(sessions, settings(queue_capacity=1))
        second_writer.start()
        blocking = second_writer.submit(command("first"), timeout_seconds=1.0)
        assert await asyncio.to_thread(second_entered.wait, 1)
        second_writer.submit(command("second"), timeout_seconds=1.0)
        admitted = asyncio.create_task(
            second_writer.submit_async(command("race"), timeout_seconds=1.0)
        )
        await asyncio.sleep(0)
        with second_writer._condition:
            waiter = second_writer._admissions[0]
            waiter.ticket = core._WriterTicket(core.WriterSubmissionId(3))
            second_writer._admissions.clear()
            admitted.cancel()
        raced_ticket = cast(core._WriterTicket, await admitted)
        raced_ticket.resolve_error(WriterDefinitelyNotExecutedError("test race completed"))
        with pytest.raises(WriterDefinitelyNotExecutedError):
            raced_ticket.result(timeout_seconds=1.0)
        second_release.set()
        await blocking.result_async(timeout_seconds=1.0)
        assert (await asyncio.to_thread(second_writer.close, timeout_seconds=1.0)).drained

    asyncio.run(scenario())


def test_start_failure_is_typed_and_permanently_fail_closed(
    sessions: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_start(_thread: Thread) -> None:
        raise RuntimeError("thread unavailable")

    monkeypatch.setattr(Thread, "start", fail_start)
    writer = SQLiteTransactionalWriter(sessions, settings())
    with pytest.raises(WriterFailedError, match="could not be started"):
        writer.start()
    with pytest.raises(WriterFailedError, match="requires recovery"):
        writer.submit(command("never"), timeout_seconds=0.0)
    assert writer.thread is None
    assert writer.close(timeout_seconds=0.0).drained


def test_public_waits_require_finite_explicit_timeouts() -> None:
    methods = (
        SQLiteTransactionalWriter.submit,
        SQLiteTransactionalWriter.submit_async,
        SQLiteTransactionalWriter.close,
        core._WriterTicket.result,
        core._WriterTicket.result_async,
    )
    for method in methods:
        timeout = inspect.signature(method).parameters["timeout_seconds"]
        assert timeout.default is inspect.Parameter.empty
        assert timeout.kind is inspect.Parameter.KEYWORD_ONLY


def test_admission_waiters_are_bounded_and_async_timeout_cleans_up(
    sessions: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    entered = Event()
    release = Event()

    def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
        if cast(_Command, submitted).label == "first":
            entered.set()
            assert release.wait(2)
        return successful_dispatch(session, submitted)

    monkeypatch.setattr(core, "dispatch_command", dispatch)
    writer = SQLiteTransactionalWriter(
        sessions,
        settings(queue_capacity=1, admission_waiter_capacity=1),
    )
    writer.start()
    first = writer.submit(command("first"), timeout_seconds=1.0)
    assert entered.wait(1)
    second = writer.submit(command("second"), timeout_seconds=1.0)

    async def scenario() -> None:
        waiting = asyncio.create_task(
            writer.submit_async(command("timed-out"), timeout_seconds=0.01)
        )
        await asyncio.sleep(0)
        with pytest.raises(WriterAdmissionTimeoutError, match="capacity"):
            await writer.submit_async(command("excess"), timeout_seconds=1.0)
        with pytest.raises(WriterAdmissionTimeoutError, match="timed out"):
            await waiting
        for _ in range(5):
            await asyncio.sleep(0)

    asyncio.run(scenario())
    with writer._condition:
        assert not writer._admissions
    release.set()
    assert first.result(timeout_seconds=1.0).mutated
    assert second.result(timeout_seconds=1.0).mutated
    assert writer.close(timeout_seconds=1.0).drained


def test_submission_identity_exhaustion_drains_accepted_fifo(
    sessions: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    entered = Event()
    release = Event()

    def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
        if cast(_Command, submitted).label == "first":
            entered.set()
            assert release.wait(2)
        return successful_dispatch(session, submitted)

    monkeypatch.setattr(core, "dispatch_command", dispatch)
    writer = SQLiteTransactionalWriter(sessions, settings(queue_capacity=1))
    writer._next_submission = MAX_WRITER_SUBMISSION_ID - 1
    writer.start()
    first = writer.submit(command("first"), timeout_seconds=1.0)
    assert entered.wait(1)
    last = writer.submit(command("last"), timeout_seconds=1.0)
    failure: list[BaseException] = []

    def wait_for_exhausted_admission() -> None:
        try:
            writer.submit(command("overflow"), timeout_seconds=1.0)
        except BaseException as error:
            failure.append(error)

    producer = Thread(target=wait_for_exhausted_admission)
    producer.start()
    release.set()
    producer.join(2)
    assert not producer.is_alive()
    assert len(failure) == 1
    assert isinstance(failure[0], WriterClosedError)
    assert first.result(timeout_seconds=1.0).submission_id.number == MAX_WRITER_SUBMISSION_ID - 1
    assert last.result(timeout_seconds=1.0).submission_id.number == MAX_WRITER_SUBMISSION_ID
    close = writer.close(timeout_seconds=1.0)
    assert close.drained
    assert close.accepted == close.completed == 2


@pytest.mark.parametrize(
    "fatal_error",
    [
        ExecutionCorruptionError("corrupt"),
        ExecutionStorageUnavailableError("unavailable"),
        ConsistencyCorruptionError("corrupt"),
        ConsistencyStorageUnavailableError("unavailable"),
        RepairCorruptionError("corrupt"),
        RepairStorageUnavailableError("unavailable"),
        AuditCorruptionError("corrupt"),
        AuditStorageUnavailableError("unavailable"),
    ],
)
def test_repository_corruption_and_noncontention_storage_are_fatal(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    fatal_error: BaseException,
) -> None:
    entered = Event()
    release = Event()

    def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
        del session
        if cast(_Command, submitted).label == "fatal":
            entered.set()
            assert release.wait(2)
            raise fatal_error
        return DispatchOutcome(_Result("unexpected"))

    monkeypatch.setattr(core, "dispatch_command", dispatch)
    writer = SQLiteTransactionalWriter(sessions, settings())
    writer.start()
    fatal = writer.submit(command("fatal"), timeout_seconds=1.0)
    assert entered.wait(1)
    queued = writer.submit(command("queued"), timeout_seconds=1.0)
    release.set()
    with pytest.raises(WriterFailedError, match="before commit"):
        fatal.result(timeout_seconds=1.0)
    with pytest.raises(WriterDefinitelyNotExecutedError):
        queued.result(timeout_seconds=1.0)
    with pytest.raises(WriterFailedError, match="requires recovery"):
        writer.submit(command("later"), timeout_seconds=0.0)
    assert writer.close(timeout_seconds=1.0).drained


@pytest.mark.parametrize(
    "recoverable",
    [
        ConsistencyStateConflictError("conflict"),
        RepairStateConflictError("conflict"),
        AuditSequenceConflictError("conflict"),
    ],
)
def test_stable_conflicts_from_each_repository_only_fail_the_current_ticket(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    recoverable: BaseException,
) -> None:
    def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
        if cast(_Command, submitted).label == "conflict":
            raise recoverable
        return successful_dispatch(session, submitted)

    monkeypatch.setattr(core, "dispatch_command", dispatch)
    writer = SQLiteTransactionalWriter(sessions, settings())
    writer.start()
    conflict = writer.submit(command("conflict"), timeout_seconds=1.0)
    following = writer.submit(command("following"), timeout_seconds=1.0)
    with pytest.raises(type(recoverable)):
        conflict.result(timeout_seconds=1.0)
    assert following.result(timeout_seconds=1.0).mutated
    assert writer.close(timeout_seconds=1.0).drained


@pytest.mark.parametrize(
    "lease_error",
    [
        ExecutionLeaseLostError("lease lost"),
        ExecutionLeaseExpiredError("lease expired"),
        ExecutionLeaseMismatchError("lease mismatch"),
    ],
)
def test_lease_races_are_command_local_and_writer_continues(
    sessions: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    lease_error: ExecutionLeaseLostError,
) -> None:
    def dispatch(session: Session, submitted: WriterCommand) -> DispatchOutcome:
        if cast(_Command, submitted).label == "lease-race":
            session.execute(text("CREATE TABLE IF NOT EXISTS lease_guard (value INTEGER)"))
            session.execute(text("INSERT INTO lease_guard VALUES (1)"))
            raise lease_error
        return successful_dispatch(session, submitted)

    monkeypatch.setattr(core, "dispatch_command", dispatch)
    writer = SQLiteTransactionalWriter(sessions, settings())
    writer.start()
    raced = writer.submit(command("lease-race"), timeout_seconds=1.0)
    following = writer.submit(command("following"), timeout_seconds=1.0)
    with pytest.raises(type(lease_error)) as caught:
        raced.result(timeout_seconds=1.0)
    assert caught.value is lease_error
    receipt = following.result(timeout_seconds=1.0)
    assert cast(_Result, receipt.result).label == "following"
    notification = writer.notifications.take()
    assert notification is not None
    assert notification.submission_id == following.submission_id
    assert writer.notifications.take() is None
    assert writer.close(timeout_seconds=1.0).drained
    with sessions() as session:
        count = session.execute(
            text("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='lease_guard'")
        ).scalar_one()
        if count:
            assert session.execute(text("SELECT COUNT(*) FROM lease_guard")).scalar_one() == 0


def test_closed_observer_loops_never_escape_writer_scheduling() -> None:
    loop = asyncio.new_event_loop()
    admission: asyncio.Future[core._WriterTicket] = loop.create_future()
    outcome: asyncio.Future[core.WriterReceipt] = loop.create_future()
    loop.close()
    ticket = core._WriterTicket(core.WriterSubmissionId(1))
    receipt = core.WriterReceipt(
        ticket.submission_id,
        WriterCommandKind.FINALIZE_EMPTY_RUN_NODE,
        RunId("run_closed-loop"),
        0,
        True,
        _Result("done"),
    )
    core._try_schedule_admission_result(loop, admission, ticket)
    core._try_schedule_admission_error(loop, admission, WriterClosedError("closed"))
    core._try_schedule_ticket_outcome(loop, outcome, receipt, None)
    ticket._async_waiters.append((loop, outcome))
    ticket.resolve_receipt(receipt)
    assert ticket.result(timeout_seconds=0.0) is receipt


def test_admission_withdrawal_returns_a_winning_ticket_or_removes_waiter(
    sessions: sessionmaker[Session],
) -> None:
    writer = SQLiteTransactionalWriter(sessions, settings())
    admitted = core._AdmissionWaiter(command("admitted"))
    admitted.ticket = core._WriterTicket(core.WriterSubmissionId(1))
    assert writer._withdraw_admission(admitted) is admitted.ticket
    pending = core._AdmissionWaiter(command("pending"))
    with writer._condition:
        writer._admissions.append(pending)
    assert writer._withdraw_admission(pending) is None
    assert pending.cancelled
    with writer._condition:
        assert pending not in writer._admissions


def test_async_admission_timeout_race_returns_the_winning_ticket(
    sessions: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = SQLiteTransactionalWriter(sessions, settings(queue_capacity=1))
    with writer._condition:
        writer._state = core._State.RUNNING
        occupied = core._WriterTicket(core.WriterSubmissionId(99))
        writer._queue.append(core._QueuedCommand(command("occupied"), occupied))

    async def race() -> None:
        async def timeout_after_admission(_future: object, _timeout: float) -> core._WriterTicket:
            with writer._condition:
                writer._queue.clear()
                writer._admit_waiters()
            raise TimeoutError

        monkeypatch.setattr(asyncio, "wait_for", timeout_after_admission)
        ticket = await writer.submit_async(command("winner"), timeout_seconds=0.01)
        assert ticket.submission_id == core.WriterSubmissionId(1)

    asyncio.run(race())
    with writer._condition:
        writer._queue.clear()
        writer._state = core._State.CLOSED


def test_last_resort_thread_guard_resolves_every_ticket(
    sessions: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    entered = Event()
    release = Event()

    def escape(_writer: SQLiteTransactionalWriter, _queued: object) -> None:
        entered.set()
        assert release.wait(2)
        raise _FatalSignal

    monkeypatch.setattr(SQLiteTransactionalWriter, "_execute", escape)
    writer = SQLiteTransactionalWriter(sessions, settings())
    writer.start()
    active = writer.submit(command("active"), timeout_seconds=1.0)
    assert entered.wait(1)
    queued = writer.submit(command("queued"), timeout_seconds=1.0)
    release.set()
    with pytest.raises(WriterFailedError, match="lifecycle"):
        active.result(timeout_seconds=1.0)
    with pytest.raises(WriterDefinitelyNotExecutedError):
        queued.result(timeout_seconds=1.0)
    assert writer.close(timeout_seconds=1.0).drained


def test_last_resort_guard_never_overwrites_a_known_receipt(
    sessions: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = SQLiteTransactionalWriter(sessions, settings())
    ticket = core._WriterTicket(core.WriterSubmissionId(1))
    receipt = core.WriterReceipt(
        ticket.submission_id,
        WriterCommandKind.FINALIZE_EMPTY_RUN_NODE,
        RunId("run_known-receipt"),
        0,
        True,
        _Result("committed"),
    )
    ticket.resolve_receipt(receipt)
    writer._active = core._QueuedCommand(command("known"), ticket)

    def fail_loop(_writer: SQLiteTransactionalWriter) -> None:
        raise _FatalSignal

    monkeypatch.setattr(SQLiteTransactionalWriter, "_run_loop", fail_loop)
    writer._run()
    assert ticket.result(timeout_seconds=0.0) is receipt
