"""Dependency-neutral contracts for serialized durable writes."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from paritygrid.application.ports.consistency import (
    EventSequence,
    PendingExecutionEvent,
)
from paritygrid.domain.models import RunId

MAX_WRITER_SUBMISSION_ID = 9_223_372_036_854_775_807


class PersistenceContentionError(Exception):
    """SQLite write contention prevented a confirmed precommit attempt."""


class WriterError(Exception):
    """Base class for stable transactional-writer failures."""


class WriterInvalidRequestError(WriterError):
    """A writer request violates the public contract."""


class WriterNotStartedError(WriterError):
    """The writer has not been explicitly started."""


class WriterClosedError(WriterError):
    """The writer no longer accepts submissions."""


class WriterAdmissionTimeoutError(WriterError):
    """Queue admission timed out before the command received an identity."""


class WriterResultTimeoutError(WriterError):
    """The wait ended while the accepted command outcome remained unknown."""


class WriterDefinitelyNotExecutedError(WriterError):
    """A fatal writer failure rejected an accepted but undispatched command."""


class WriterFailedError(WriterError):
    """The writer stopped after a fatal precommit or lifecycle failure."""


class WriterCommitOutcomeUnknownError(WriterFailedError):
    """Commit raised, so the durable outcome requires recovery inspection."""


class WriterCommandKind(StrEnum):
    """Closed commands accepted by the transactional writer."""

    CREATE_CAPTURED_RUN = "create_captured_run"
    TRANSITION_RUN = "transition_run"
    BOOTSTRAP_WORK = "bootstrap_work"
    CLAIM_WORK = "claim_work"
    RENEW_WORK_CLAIM = "renew_work_claim"
    COMMIT_WORK_ATTEMPT = "commit_work_attempt"
    COMMIT_WORK_WITH_CHECKPOINT = "commit_work_with_checkpoint"
    RECOVER_EXPIRED_WORK = "recover_expired_work"
    FINALIZE_EMPTY_RUN_NODE = "finalize_empty_run_node"
    CREATE_REPAIR_PLAN = "create_repair_plan"
    APPROVE_REPAIR_PLAN = "approve_repair_plan"
    REJECT_REPAIR_PLAN = "reject_repair_plan"
    BEGIN_REPAIR_APPLICATION = "begin_repair_application"
    RECORD_REPAIR_ACTION_APPLIED = "record_repair_action_applied"
    RECORD_REPAIR_ACTION_FAILED = "record_repair_action_failed"
    COMPLETE_REPAIR_APPLICATION = "complete_repair_application"
    PERSIST_RECONCILIATION = "persist_reconciliation"
    RECORD_TARGET_VERIFICATION = "record_target_verification"


class WriterState(StrEnum):
    """Observable lifecycle states for the transactional writer."""

    NEW = "new"
    RUNNING = "running"
    CLOSING = "closing"
    CLOSED = "closed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class WriterSettings:
    """Explicit queue, retry, notification, and thread bounds."""

    queue_capacity: int = 64
    admission_waiter_capacity: int = 64
    notification_capacity: int = 64
    max_contention_attempts: int = 3
    contention_delay_seconds: float = 0.01
    thread_name: str = "paritygrid-sqlite-writer"

    def __post_init__(self) -> None:
        if type(self.queue_capacity) is not int or not 1 <= self.queue_capacity <= 10_000:
            raise ValueError("writer queue capacity is outside the supported range")
        if (
            type(self.admission_waiter_capacity) is not int
            or not 1 <= self.admission_waiter_capacity <= 10_000
        ):
            raise ValueError("writer admission waiter capacity is outside the supported range")
        if (
            type(self.notification_capacity) is not int
            or not 1 <= self.notification_capacity <= 10_000
        ):
            raise ValueError("notification capacity is outside the supported range")
        if (
            type(self.max_contention_attempts) is not int
            or not 1 <= self.max_contention_attempts <= 10
        ):
            raise ValueError("writer contention attempts are outside the supported range")
        if (
            type(self.contention_delay_seconds) is not float
            or not 0 <= self.contention_delay_seconds <= 1
        ):
            raise ValueError("writer contention delay is outside the supported range")
        if type(self.thread_name) is not str or not 1 <= len(self.thread_name) <= 64:
            raise ValueError("writer thread name is invalid")
        if any(ord(character) < 32 or ord(character) == 127 for character in self.thread_name):
            raise ValueError("writer thread name contains unsupported characters")


@dataclass(frozen=True, slots=True, order=True)
class WriterSubmissionId:
    """A positive identity allocated only after successful FIFO admission."""

    number: int

    def __post_init__(self) -> None:
        if type(self.number) is not int:
            raise TypeError("writer submission identity must be an integer")
        if not 1 <= self.number <= MAX_WRITER_SUBMISSION_ID:
            raise ValueError("writer submission identity is outside the supported range")

    def __int__(self) -> int:
        return self.number


@dataclass(frozen=True, slots=True)
class EventAppendRequest:
    """The exact durable event frontier accompanying one mutation."""

    expected_next_sequence: EventSequence
    expected_counter_row_version: int
    event: PendingExecutionEvent


class WriterCommand(Protocol):
    """Marker shared by the closed immutable writer command set."""

    @property
    def kind(self) -> WriterCommandKind: ...

    @property
    def run_id(self) -> RunId: ...


class WriterCommandResult(Protocol):
    """Marker shared by closed immutable writer result values."""

    @property
    def result_kind(self) -> WriterCommandKind: ...


@dataclass(frozen=True, slots=True)
class WriterReceipt:
    """A truthful result produced only after commit and Session close."""

    submission_id: WriterSubmissionId
    command_kind: WriterCommandKind
    run_id: RunId
    contention_attempts: int
    mutated: bool
    result: WriterCommandResult


@dataclass(frozen=True, slots=True)
class CommittedNotification:
    """Bounded post-commit notification metadata without command payloads."""

    submission_id: WriterSubmissionId
    command_kind: WriterCommandKind
    run_id: RunId


@dataclass(frozen=True, slots=True)
class NotificationBufferStats:
    capacity: int
    depth: int
    offered: int
    accepted: int
    dropped: int
    rejected: int
    failures: int


@dataclass(frozen=True, slots=True)
class WriterCloseResult:
    """Observable state after one bounded close attempt."""

    drained: bool
    accepted: int
    completed: int
    queued: int
    in_flight: int


@dataclass(frozen=True, slots=True)
class WriterDiagnostics:
    """Bounded lifecycle and queue counters captured under one writer lock."""

    state: WriterState
    queue_capacity: int
    admission_capacity: int
    accepted: int
    completed: int
    queue_depth: int
    admission_waiters: int
    in_flight: int
    max_queue_depth: int
    max_admission_waiters: int
    max_resident: int
    contention_retries: int

    def __post_init__(self) -> None:
        if type(self.state) is not WriterState:
            raise TypeError("writer diagnostics state must be a WriterState")
        values = (
            self.queue_capacity,
            self.admission_capacity,
            self.accepted,
            self.completed,
            self.queue_depth,
            self.admission_waiters,
            self.in_flight,
            self.max_queue_depth,
            self.max_admission_waiters,
            self.max_resident,
            self.contention_retries,
        )
        if any(type(value) is not int for value in values):
            raise TypeError("writer diagnostics counters must be integers")
        if not 1 <= self.queue_capacity <= 10_000:
            raise ValueError("writer diagnostics queue capacity is invalid")
        if not 1 <= self.admission_capacity <= 10_000:
            raise ValueError("writer diagnostics admission capacity is invalid")
        if any(value < 0 for value in values[2:]):
            raise ValueError("writer diagnostics counters cannot be negative")
        if self.completed > self.accepted:
            raise ValueError("writer diagnostics completed count exceeds accepted count")
        if self.accepted != self.completed + self.queue_depth + self.in_flight:
            raise ValueError("writer diagnostics accepted command accounting is inconsistent")
        if self.queue_depth > self.queue_capacity:
            raise ValueError("writer diagnostics queue depth exceeds capacity")
        if self.admission_waiters > self.admission_capacity:
            raise ValueError("writer diagnostics admission waiters exceed capacity")
        if self.in_flight not in {0, 1}:
            raise ValueError("writer diagnostics in-flight count is invalid")
        if self.max_queue_depth < self.queue_depth or self.max_queue_depth > self.queue_capacity:
            raise ValueError("writer diagnostics queue high-water mark is invalid")
        if (
            self.max_admission_waiters < self.admission_waiters
            or self.max_admission_waiters > self.admission_capacity
        ):
            raise ValueError("writer diagnostics admission high-water mark is invalid")
        resident = self.queue_depth + self.in_flight
        if self.max_resident < resident or self.max_resident > self.queue_capacity + 1:
            raise ValueError("writer diagnostics resident high-water mark is invalid")
        if self.max_resident < self.max_queue_depth:
            raise ValueError(
                "writer diagnostics resident and queue high-water marks are inconsistent"
            )


class WriterTicket(Protocol):
    """Reusable handle for one accepted durable command."""

    @property
    def submission_id(self) -> WriterSubmissionId: ...

    def result(self, *, timeout_seconds: float) -> WriterReceipt: ...

    async def result_async(self, *, timeout_seconds: float) -> WriterReceipt: ...


class TransactionalWriter(Protocol):
    """Bounded FIFO admission and lifecycle contract."""

    def start(self) -> None: ...

    def submit(
        self,
        command: WriterCommand,
        *,
        timeout_seconds: float,
    ) -> WriterTicket: ...

    async def submit_async(
        self, command: WriterCommand, *, timeout_seconds: float
    ) -> WriterTicket: ...

    def snapshot(self) -> WriterDiagnostics: ...

    def close(self, *, timeout_seconds: float) -> WriterCloseResult: ...


class CommittedNotificationBuffer(Protocol):
    """Nonblocking committed-notification observation contract."""

    def take(self) -> CommittedNotification | None: ...

    def stats(self) -> NotificationBufferStats: ...


__all__ = [
    "MAX_WRITER_SUBMISSION_ID",
    "CommittedNotification",
    "CommittedNotificationBuffer",
    "EventAppendRequest",
    "NotificationBufferStats",
    "PersistenceContentionError",
    "TransactionalWriter",
    "WriterAdmissionTimeoutError",
    "WriterCloseResult",
    "WriterClosedError",
    "WriterCommand",
    "WriterCommandKind",
    "WriterCommandResult",
    "WriterCommitOutcomeUnknownError",
    "WriterDefinitelyNotExecutedError",
    "WriterDiagnostics",
    "WriterError",
    "WriterFailedError",
    "WriterInvalidRequestError",
    "WriterNotStartedError",
    "WriterReceipt",
    "WriterResultTimeoutError",
    "WriterSettings",
    "WriterState",
    "WriterSubmissionId",
    "WriterTicket",
]
