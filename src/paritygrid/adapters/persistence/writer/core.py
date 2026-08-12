"""Bounded single-thread transactional writer lifecycle."""

import asyncio
import time
from collections import deque
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum, auto
from threading import Condition, Event, Lock, Thread, current_thread
from typing import cast

from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.orm.session import SessionTransaction

from paritygrid.adapters.persistence.sqlite import SessionFactory
from paritygrid.adapters.persistence.writer.dispatch import dispatch_command, validate_command
from paritygrid.adapters.persistence.writer.notifications import (
    BoundedCommittedNotificationBuffer,
)
from paritygrid.application.ports.consistency import ConsistencyRepositoryError
from paritygrid.application.ports.execution import ExecutionRepositoryError
from paritygrid.application.ports.repair_audit import (
    AuditRepositoryError,
    RepairRepositoryError,
)
from paritygrid.application.ports.writer import (
    CommittedNotification,
    PersistenceContentionError,
    TransactionalWriter,
    WriterAdmissionTimeoutError,
    WriterClosedError,
    WriterCloseResult,
    WriterCommand,
    WriterCommitOutcomeUnknownError,
    WriterDefinitelyNotExecutedError,
    WriterFailedError,
    WriterInvalidRequestError,
    WriterNotStartedError,
    WriterReceipt,
    WriterResultTimeoutError,
    WriterSettings,
    WriterSubmissionId,
    WriterTicket,
)


class _State(Enum):
    NEW = auto()
    RUNNING = auto()
    CLOSING = auto()
    CLOSED = auto()
    FAILED = auto()


@dataclass(slots=True)
class _QueuedCommand:
    command: WriterCommand
    ticket: _WriterTicket


@dataclass(slots=True)
class _AdmissionWaiter:
    command: WriterCommand
    loop: asyncio.AbstractEventLoop | None = None
    future: asyncio.Future[_WriterTicket] | None = None
    ticket: _WriterTicket | None = None
    error: BaseException | None = None
    cancelled: bool = False


class _WriterTicket(WriterTicket):
    def __init__(self, submission_id: WriterSubmissionId) -> None:
        self._submission_id = submission_id
        self._condition = Condition()
        self._receipt: WriterReceipt | None = None
        self._error: BaseException | None = None
        self._async_waiters: list[
            tuple[asyncio.AbstractEventLoop, asyncio.Future[WriterReceipt]]
        ] = []

    @property
    def submission_id(self) -> WriterSubmissionId:
        return self._submission_id

    def result(self, timeout_seconds: float | None = None) -> WriterReceipt:
        timeout = _validate_timeout(timeout_seconds, "result timeout")
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while self._receipt is None and self._error is None:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise WriterResultTimeoutError("Writer result wait timed out.")
                self._condition.wait(remaining)
            return self._outcome()

    async def result_async(self, timeout_seconds: float | None = None) -> WriterReceipt:
        timeout = _validate_timeout(timeout_seconds, "result timeout")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[WriterReceipt] = loop.create_future()
        with self._condition:
            if self._receipt is not None or self._error is not None:
                _schedule_ticket_outcome(loop, future, self._receipt, self._error)
            else:
                self._async_waiters.append((loop, future))
        try:
            if timeout is None:
                return await asyncio.shield(future)
            return await asyncio.wait_for(asyncio.shield(future), timeout)
        except TimeoutError:
            self._discard_async_waiter(loop, future)
            raise WriterResultTimeoutError("Writer result wait timed out.") from None
        except asyncio.CancelledError:
            self._discard_async_waiter(loop, future)
            raise

    def resolve_receipt(self, receipt: WriterReceipt) -> None:
        self._resolve(receipt, None)

    def resolve_error(self, error: BaseException) -> None:
        self._resolve(None, error)

    def _resolve(self, receipt: WriterReceipt | None, error: BaseException | None) -> None:
        with self._condition:
            if self._receipt is not None or self._error is not None:
                return
            self._receipt = receipt
            self._error = error
            waiters = tuple(self._async_waiters)
            self._async_waiters.clear()
            self._condition.notify_all()
        for loop, future in waiters:
            _schedule_ticket_outcome(loop, future, receipt, error)

    def _discard_async_waiter(
        self,
        loop: asyncio.AbstractEventLoop,
        future: asyncio.Future[WriterReceipt],
    ) -> None:
        with self._condition, suppress(ValueError):
            self._async_waiters.remove((loop, future))
        future.cancel()

    def _outcome(self) -> WriterReceipt:
        if self._receipt is not None:
            return self._receipt
        assert self._error is not None
        raise self._error


class SQLiteTransactionalWriter(TransactionalWriter):
    """Serialize closed durable commands through one explicitly owned thread."""

    def __init__(
        self,
        session_factory: SessionFactory,
        settings: WriterSettings | None = None,
        *,
        notifications: BoundedCommittedNotificationBuffer | None = None,
    ) -> None:
        factory_value = cast(object, session_factory)
        if not isinstance(factory_value, sessionmaker):
            raise WriterInvalidRequestError("writer session factory is invalid")
        active_settings = WriterSettings() if settings is None else settings
        if type(active_settings) is not WriterSettings:
            raise WriterInvalidRequestError("writer settings are invalid")
        self._session_factory = session_factory
        self._settings = active_settings
        self._notifications = notifications or BoundedCommittedNotificationBuffer(
            active_settings.notification_capacity
        )
        self._condition = Condition(Lock())
        self._state = _State.NEW
        self._queue: deque[_QueuedCommand] = deque()
        self._admissions: deque[_AdmissionWaiter] = deque()
        self._next_submission = 1
        self._accepted = 0
        self._completed = 0
        self._in_flight = 0
        self._thread: Thread | None = None
        self._started = Event()

    @property
    def notifications(self) -> BoundedCommittedNotificationBuffer:
        return self._notifications

    @property
    def thread(self) -> Thread | None:
        """Expose immutable thread identity for lifecycle diagnostics."""
        return self._thread

    def start(self) -> None:
        with self._condition:
            if self._state is not _State.NEW:
                raise WriterInvalidRequestError("writer start is single-use")
            self._state = _State.RUNNING
            thread = Thread(
                target=self._run,
                name=self._settings.thread_name,
                daemon=False,
            )
            self._thread = thread
            thread.start()
        self._started.wait()

    def submit(
        self,
        command: WriterCommand,
        *,
        timeout_seconds: float | None = None,
    ) -> WriterTicket:
        validated = validate_command(command)
        timeout = _validate_timeout(timeout_seconds, "admission timeout")
        deadline = None if timeout is None else time.monotonic() + timeout
        waiter = _AdmissionWaiter(validated)
        with self._condition:
            self._require_admission_open()
            self._admissions.append(waiter)
            self._admit_waiters()
            while waiter.ticket is None and waiter.error is None:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    assert waiter in self._admissions
                    self._admissions.remove(waiter)
                    self._condition.notify_all()
                    raise WriterAdmissionTimeoutError("Writer admission timed out before enqueue.")
                self._condition.wait(remaining)
            if waiter.error is not None:
                raise waiter.error
            assert waiter.ticket is not None
            return waiter.ticket

    async def submit_async(self, command: WriterCommand) -> WriterTicket:
        validated = validate_command(command)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[_WriterTicket] = loop.create_future()
        waiter = _AdmissionWaiter(validated, loop=loop, future=future)
        with self._condition:
            self._require_admission_open()
            self._admissions.append(waiter)
            self._admit_waiters()
            if waiter.ticket is not None:
                future.cancel()
                return waiter.ticket
        try:
            return await future
        except asyncio.CancelledError:
            with self._condition:
                if waiter.ticket is not None:
                    return waiter.ticket
                waiter.cancelled = True
                if waiter in self._admissions:
                    self._admissions.remove(waiter)
                    self._condition.notify_all()
            raise

    def close(self, *, timeout_seconds: float | None = None) -> WriterCloseResult:
        timeout = _validate_timeout(timeout_seconds, "drain timeout")
        with self._condition:
            if self._thread is current_thread():
                raise WriterInvalidRequestError("writer thread cannot join itself")
            if self._state is _State.NEW:
                self._state = _State.CLOSED
                self._reject_admissions(WriterClosedError("Writer is closed."))
            elif self._state is _State.RUNNING:
                self._state = _State.CLOSING
                self._reject_admissions(WriterClosedError("Writer is closed."))
                self._condition.notify_all()
            thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout)
        with self._condition:
            drained = thread is None or not thread.is_alive()
            return WriterCloseResult(
                drained=drained,
                accepted=self._accepted,
                completed=self._completed,
                queued=len(self._queue),
                in_flight=self._in_flight,
            )

    def _run(self) -> None:
        self._started.set()
        while True:
            with self._condition:
                while not self._queue and self._state is _State.RUNNING:
                    self._condition.wait()
                if not self._queue:
                    assert self._state is _State.CLOSING
                    self._state = _State.CLOSED
                    self._condition.notify_all()
                    return
                queued = self._queue.popleft()
                self._in_flight = 1
                self._admit_waiters()
                self._condition.notify_all()
            fatal = self._execute(queued)
            with self._condition:
                self._in_flight = 0
                self._completed += 1
                if fatal is not None:
                    self._state = _State.FAILED
                    self._fail_queued()
                    self._reject_admissions(WriterClosedError("Writer is closed."))
                    self._condition.notify_all()
                    return
                self._condition.notify_all()

    def _execute(self, queued: _QueuedCommand) -> WriterFailedError | None:
        attempts = 0
        while True:
            attempts += 1
            session: Session | None = None
            transaction: SessionTransaction | None = None
            try:
                session = self._session_factory()
                transaction = session.begin()
                outcome = dispatch_command(session, queued.command)
            except PersistenceContentionError:
                cleanup = _rollback_and_close(session, transaction)
                if cleanup is not None:
                    fatal = WriterFailedError("Writer cleanup failed before commit.")
                    queued.ticket.resolve_error(fatal)
                    return fatal
                if attempts >= self._settings.max_contention_attempts:
                    fatal = WriterFailedError("Writer contention retry limit was exhausted.")
                    queued.ticket.resolve_error(fatal)
                    return fatal
                if self._settings.contention_delay_seconds:
                    time.sleep(self._settings.contention_delay_seconds)
                continue
            except (
                ExecutionRepositoryError,
                ConsistencyRepositoryError,
                RepairRepositoryError,
                AuditRepositoryError,
                WriterInvalidRequestError,
            ) as error:
                cleanup = _rollback_and_close(session, transaction)
                if cleanup is not None:
                    fatal = WriterFailedError("Writer cleanup failed before commit.")
                    queued.ticket.resolve_error(fatal)
                    return fatal
                queued.ticket.resolve_error(error)
                return None
            except BaseException:
                _rollback_and_close(session, transaction)
                fatal = WriterFailedError("Writer command failed before commit.")
                queued.ticket.resolve_error(fatal)
                return fatal

            assert session is not None
            assert transaction is not None
            try:
                transaction.commit()
            except BaseException:
                _close_only(session)
                fatal = WriterCommitOutcomeUnknownError(
                    "Writer commit outcome is unknown; recovery is required."
                )
                queued.ticket.resolve_error(fatal)
                return fatal

            receipt = WriterReceipt(
                submission_id=queued.ticket.submission_id,
                command_kind=queued.command.kind,
                run_id=queued.command.run_id,
                contention_attempts=attempts - 1,
                mutated=outcome.mutated,
                result=outcome.result,
            )
            close_error = _close_only(session)
            queued.ticket.resolve_receipt(receipt)
            if close_error is not None:
                return WriterFailedError("Writer Session failed to close after commit.")
            if outcome.mutated:
                self._offer_notification(
                    CommittedNotification(
                        submission_id=queued.ticket.submission_id,
                        command_kind=queued.command.kind,
                        run_id=queued.command.run_id,
                    )
                )
            return None

    def _offer_notification(self, notification: CommittedNotification) -> None:
        try:
            self._notifications.offer(notification)
        except BaseException:
            self._notifications.record_failure()

    def _require_admission_open(self) -> None:
        if self._state is _State.NEW:
            raise WriterNotStartedError("Writer must be started before submission.")
        if self._state in {_State.CLOSING, _State.CLOSED}:
            raise WriterClosedError("Writer is closed.")
        if self._state is _State.FAILED:
            raise WriterFailedError("Writer failed and requires recovery.")

    def _admit_waiters(self) -> None:
        while self._admissions and len(self._queue) < self._settings.queue_capacity:
            waiter = self._admissions.popleft()
            if waiter.cancelled:
                continue
            submission = WriterSubmissionId(self._next_submission)
            self._next_submission += 1
            ticket = _WriterTicket(submission)
            waiter.ticket = ticket
            self._queue.append(_QueuedCommand(waiter.command, ticket))
            self._accepted += 1
            if waiter.loop is not None and waiter.future is not None:
                waiter.loop.call_soon_threadsafe(_set_admission_result, waiter.future, ticket)
            self._condition.notify_all()

    def _reject_admissions(self, error: BaseException) -> None:
        waiters = tuple(self._admissions)
        self._admissions.clear()
        for waiter in waiters:
            waiter.error = error
            if waiter.loop is not None and waiter.future is not None:
                waiter.loop.call_soon_threadsafe(_set_admission_error, waiter.future, error)
        self._condition.notify_all()

    def _fail_queued(self) -> None:
        while self._queue:
            queued = self._queue.popleft()
            queued.ticket.resolve_error(
                WriterDefinitelyNotExecutedError(
                    "Accepted command was not executed because the writer failed."
                )
            )


def _rollback_and_close(
    session: Session | None,
    transaction: SessionTransaction | None,
) -> BaseException | None:
    failure: BaseException | None = None
    if transaction is not None and transaction.is_active:
        try:
            transaction.rollback()
        except BaseException as error:
            failure = error
    close_failure = _close_only(session)
    return failure or close_failure


def _close_only(session: Session | None) -> BaseException | None:
    if session is None:
        return None
    try:
        session.close()
    except BaseException as error:
        return error
    return None


def _validate_timeout(value: float | None, subject: str) -> float | None:
    if value is None:
        return None
    if type(value) is not float or not 0 <= value <= 86_400:
        raise WriterInvalidRequestError(f"{subject} is outside the supported range")
    return value


def _set_admission_result(future: asyncio.Future[_WriterTicket], ticket: _WriterTicket) -> None:
    if not future.done():
        future.set_result(ticket)


def _set_admission_error(future: asyncio.Future[_WriterTicket], error: BaseException) -> None:
    if not future.done():
        future.set_exception(error)


def _schedule_ticket_outcome(
    loop: asyncio.AbstractEventLoop,
    future: asyncio.Future[WriterReceipt],
    receipt: WriterReceipt | None,
    error: BaseException | None,
) -> None:
    loop.call_soon_threadsafe(_set_ticket_outcome, future, receipt, error)


def _set_ticket_outcome(
    future: asyncio.Future[WriterReceipt],
    receipt: WriterReceipt | None,
    error: BaseException | None,
) -> None:
    if future.done():
        return
    if receipt is not None:
        future.set_result(receipt)
        return
    assert error is not None
    future.set_exception(error)


__all__ = ["SQLiteTransactionalWriter"]
