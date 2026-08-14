"""Capability-preserving work-item leasing through the transactional writer."""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass, field
from datetime import timedelta
from threading import Lock
from typing import Protocol, cast, runtime_checkable

from paritygrid.application.ports.consistency import (
    EventSequence,
    ExecutionEventBatch,
    ExecutionEventRecord,
)
from paritygrid.application.ports.execution import (
    RunNodeRecord,
    RunRecord,
    WorkClaim,
)
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    WriterAdmissionTimeoutError,
    WriterCommand,
    WriterCommitOutcomeUnknownError,
    WriterDefinitelyNotExecutedError,
    WriterError,
    WriterReceipt,
    WriterResultTimeoutError,
    WriterSubmissionId,
    WriterTicket,
)
from paritygrid.application.writes import (
    ClaimWork,
    ClaimWorkResult,
    RenewWorkClaim,
    RenewWorkClaimResult,
)
from paritygrid.domain.models import (
    AttemptNumber,
    Duration,
    NodeId,
    RunId,
    UtcTimestamp,
    WorkItemId,
)

MIN_WORK_LEASE_MICROSECONDS = 1_000_000
MAX_WORK_LEASE_MICROSECONDS = 86_400_000_000
MAX_LEASE_OWNER_LENGTH = 128
MAX_RUNNER_KIND_LENGTH = 32
MAX_WORKER_IDENTITY_LENGTH = 128
MAX_LEASE_ROW_VERSION = 2_147_483_647
MAX_LEASE_WRITER_TIMEOUT_SECONDS = 86_400.0
MAX_LEASE_CONTENTION_ATTEMPTS = 9


class WorkLeaseError(RuntimeError):
    """Base failure for work-lease coordination."""


class WorkLeaseInvalidRequestError(WorkLeaseError):
    """A lease request violates the public coordination contract."""


class WorkLeaseBusyError(WorkLeaseError):
    """A work identity already has an active or overlapping lease operation."""


class WorkLeaseOwnershipError(WorkLeaseError):
    """A lease wrapper is reconstructed, stale, foreign, or already retired."""


class WorkLeaseExpiredError(WorkLeaseOwnershipError):
    """A lease expired before a renewal could be submitted."""


class WorkLeaseAdmissionError(WorkLeaseError):
    """Writer admission failed before the lease command received an identity."""


class WorkLeaseWriterError(WorkLeaseError):
    """The writer rejected a lease command with a confirmed non-ambiguous failure."""


class WorkLeaseOutcomeUnknownError(WorkLeaseError):
    """A lease command may have committed and requires durable recovery inspection."""


class WorkLeaseProtocolError(WorkLeaseOutcomeUnknownError):
    """The writer returned malformed or inconsistent lease evidence."""


class WorkLeaseClockError(WorkLeaseError):
    """The injected clock failed or returned an invalid timestamp."""


@runtime_checkable
class WorkLeaseClock(Protocol):
    """Injected deterministic time source for lease acquisition and renewal."""

    def now(self) -> UtcTimestamp:
        """Return the current exact UTC timestamp."""
        ...


@runtime_checkable
class WorkLeaseWriter(Protocol):
    """Borrowed writer surface used without lifecycle ownership."""

    def submit(
        self,
        command: WriterCommand,
        *,
        timeout_seconds: float,
    ) -> WriterTicket:
        """Submit one closed lease command."""
        ...


@dataclass(frozen=True, slots=True)
class WorkLeaseSettings:
    """Bounded duration and wait limits for work-lease operations."""

    lease_duration: Duration = field(default_factory=lambda: Duration(60_000_000))
    admission_timeout_seconds: float = 5.0
    result_timeout_seconds: float = 60.0

    def __post_init__(self) -> None:
        _require_exact(self.lease_duration, Duration, "work lease duration")
        if not (
            MIN_WORK_LEASE_MICROSECONDS
            <= self.lease_duration.microseconds
            <= MAX_WORK_LEASE_MICROSECONDS
        ):
            raise WorkLeaseInvalidRequestError("work lease duration is outside the supported range")
        _validate_timeout(self.admission_timeout_seconds, "lease admission timeout")
        _validate_timeout(self.result_timeout_seconds, "lease result timeout")


@dataclass(frozen=True, slots=True, repr=False)
class AcquireWorkLeaseRequest:
    """Exact durable parents, owner metadata, and event frontier for one claim."""

    run_id: RunId
    node_id: NodeId
    work_item_id: WorkItemId
    expected_attempt_number: AttemptNumber
    expected_work_row_version: int
    expected_node_row_version: int
    expected_run_row_version: int
    lease_owner: str
    runner_kind: str
    worker_identity: str
    event: EventAppendRequest

    def __post_init__(self) -> None:
        _require_exact(self.run_id, RunId, "lease run identity")
        _require_exact(self.node_id, NodeId, "lease node identity")
        _require_exact(self.work_item_id, WorkItemId, "lease work identity")
        _require_exact(self.expected_attempt_number, AttemptNumber, "lease attempt number")
        _validate_row_version(self.expected_work_row_version, "work row version")
        _validate_row_version(self.expected_node_row_version, "node row version")
        _validate_row_version(self.expected_run_row_version, "run row version")
        _validate_text(self.lease_owner, MAX_LEASE_OWNER_LENGTH, "lease owner")
        _validate_text(self.runner_kind, MAX_RUNNER_KIND_LENGTH, "runner kind")
        _validate_text(
            self.worker_identity,
            MAX_WORKER_IDENTITY_LENGTH,
            "worker identity",
        )
        _require_exact(self.event, EventAppendRequest, "lease event request")
        _validate_event_frontier(self.event)

    def __repr__(self) -> str:
        return (
            "AcquireWorkLeaseRequest("
            f"run_id={self.run_id!r}, node_id={self.node_id!r}, "
            f"work_item_id={self.work_item_id!r}, "
            f"expected_attempt_number={self.expected_attempt_number!r}, "
            f"expected_work_row_version={self.expected_work_row_version!r}, "
            f"expected_node_row_version={self.expected_node_row_version!r}, "
            f"expected_run_row_version={self.expected_run_row_version!r}, "
            "lease_owner=<redacted>, runner_kind=<redacted>, "
            "worker_identity=<redacted>, event=<redacted>)"
        )


@dataclass(frozen=True, slots=True, repr=False)
class RenewWorkLeaseRequest:
    """Exact run revision and event frontier for one owned renewal."""

    expected_run_row_version: int
    event: EventAppendRequest

    def __post_init__(self) -> None:
        _validate_row_version(self.expected_run_row_version, "run row version")
        _require_exact(self.event, EventAppendRequest, "lease event request")
        _validate_event_frontier(self.event)

    def __repr__(self) -> str:
        return (
            "RenewWorkLeaseRequest("
            f"expected_run_row_version={self.expected_run_row_version!r}, "
            "event=<redacted>)"
        )


@dataclass(frozen=True, slots=True)
class WorkLeaseServiceSnapshot:
    """Bounded transient ownership counts without owner or worker identities."""

    active: int
    unknown: int
    in_flight: int

    def __post_init__(self) -> None:
        values = (self.active, self.unknown, self.in_flight)
        if any(type(value) is not int for value in values):
            raise TypeError("work lease snapshot counters must be integers")
        if any(value < 0 for value in values):
            raise ValueError("work lease snapshot counters cannot be negative")


_LEASE_CONSTRUCTION_TOKEN = object()


@dataclass(frozen=True, slots=True, repr=False, init=False)
class WorkLease:
    """Identity-bearing wrapper around an exact writer-produced WorkClaim."""

    _claim: WorkClaim
    _node: RunNodeRecord
    _run: RunRecord
    _events: ExecutionEventBatch
    _submission_id: WriterSubmissionId

    def __init__(
        self,
        claim: WorkClaim,
        node: RunNodeRecord,
        run: RunRecord,
        events: ExecutionEventBatch,
        submission_id: WriterSubmissionId,
        *,
        _token: object,
    ) -> None:
        if _token is not _LEASE_CONSTRUCTION_TOKEN:
            raise WorkLeaseOwnershipError("work lease wrappers are service-issued only")
        _require_exact(claim, WorkClaim, "work lease claim")
        _require_exact(node, RunNodeRecord, "work lease node")
        _require_exact(run, RunRecord, "work lease run")
        _require_exact(events, ExecutionEventBatch, "work lease events")
        _require_exact(submission_id, WriterSubmissionId, "work lease submission identity")
        object.__setattr__(self, "_claim", claim)
        object.__setattr__(self, "_node", node)
        object.__setattr__(self, "_run", run)
        object.__setattr__(self, "_events", events)
        object.__setattr__(self, "_submission_id", submission_id)

    @property
    def claim(self) -> WorkClaim:
        """Return the exact writer-produced capability without reconstruction."""
        return self._claim

    @property
    def node(self) -> RunNodeRecord:
        """Return the latest node aggregate observed by this lease operation."""
        return self._node

    @property
    def run(self) -> RunRecord:
        """Return the run revision committed with this lease operation."""
        return self._run

    @property
    def events(self) -> ExecutionEventBatch:
        """Return the durable event batch committed with this lease operation."""
        return self._events

    @property
    def submission_id(self) -> WriterSubmissionId:
        """Return the writer identity that produced this lease evidence."""
        return self._submission_id

    def __repr__(self) -> str:
        return (
            "WorkLease("
            f"work_item_id={self._claim.work_item_id!r}, "
            f"attempt_number={self._claim.attempt_number!r}, "
            f"row_version={self._claim.row_version!r}, "
            f"lease_expires_at={self._claim.lease_expires_at!r}, "
            "owner=<redacted>, worker=<redacted>)"
        )


@dataclass(frozen=True, slots=True)
class _WorkClaimEvidence:
    work_item_id: str
    attempt_number: int
    lease_owner: str
    row_version: int
    started_at: str
    lease_expires_at: str
    runner_kind: str
    worker_identity: str

    @classmethod
    def capture(cls, claim: WorkClaim) -> _WorkClaimEvidence | None:
        if (
            type(claim.work_item_id) is not WorkItemId
            or type(claim.work_item_id.value) is not str
            or type(claim.attempt_number) is not AttemptNumber
            or type(claim.attempt_number.number) is not int
            or type(claim.lease_owner) is not str
            or type(claim.row_version) is not int
            or type(claim.started_at) is not UtcTimestamp
            or type(claim.lease_expires_at) is not UtcTimestamp
            or type(claim.runner_kind) is not str
            or type(claim.worker_identity) is not str
        ):
            return None
        failed = False
        try:
            started_at = str(claim.started_at)
            lease_expires_at = str(claim.lease_expires_at)
        except Exception:
            failed = True
            started_at = None
            lease_expires_at = None
        if failed or started_at is None or lease_expires_at is None:
            return None
        return cls(
            claim.work_item_id.value,
            claim.attempt_number.number,
            claim.lease_owner,
            claim.row_version,
            started_at,
            lease_expires_at,
            claim.runner_kind,
            claim.worker_identity,
        )


@dataclass(frozen=True, slots=True)
class _ActiveWorkLease:
    lease: WorkLease
    claim: WorkClaim
    node: RunNodeRecord
    run: RunRecord
    events: ExecutionEventBatch
    submission_id: WriterSubmissionId
    claim_evidence: _WorkClaimEvidence

    @classmethod
    def capture(cls, lease: WorkLease) -> _ActiveWorkLease:
        claim_evidence = _WorkClaimEvidence.capture(lease.claim)
        if claim_evidence is None:
            raise WorkLeaseProtocolError("work lease claim evidence is invalid")
        return cls(
            lease,
            lease.claim,
            lease.node,
            lease.run,
            lease.events,
            lease.submission_id,
            claim_evidence,
        )

    def matches(self, lease: WorkLease) -> bool:
        claim_evidence = _WorkClaimEvidence.capture(lease.claim)
        return (
            self.lease is lease
            and self.claim is lease.claim
            and self.node is lease.node
            and self.run is lease.run
            and self.events is lease.events
            and self.submission_id is lease.submission_id
            and claim_evidence is not None
            and self.claim_evidence == claim_evidence
        )


class WorkLeaseService:
    """Acquire and renew exact work claims through a borrowed serialized writer."""

    __slots__ = ("_clock", "_in_flight", "_lock", "_settings", "_states", "_writer")

    def __init__(
        self,
        writer: WorkLeaseWriter,
        clock: WorkLeaseClock,
        *,
        settings: WorkLeaseSettings | None = None,
    ) -> None:
        writer_value = cast(object, writer)
        if not isinstance(writer_value, WorkLeaseWriter):
            raise TypeError("work lease writer must provide transactional submit")
        clock_value = cast(object, clock)
        if not isinstance(clock_value, WorkLeaseClock):
            raise TypeError("work lease clock must provide exact UTC time")
        settings_value = WorkLeaseSettings() if settings is None else settings
        _require_exact(settings_value, WorkLeaseSettings, "work lease settings")
        self._writer = writer_value
        self._clock = clock_value
        self._settings = settings_value
        self._lock = Lock()
        self._states: dict[WorkItemId, _ActiveWorkLease | None] = {}
        self._in_flight: set[WorkItemId] = set()

    def acquire(self, request: AcquireWorkLeaseRequest) -> WorkLease:
        """Acquire one exact work claim or fail closed without forging authority."""
        _require_exact(request, AcquireWorkLeaseRequest, "work lease acquisition")
        work_item_id = request.work_item_id
        self._reserve_acquisition(work_item_id)
        outcome_unknown = False
        try:
            started_at = self._now()
            lease_expires_at = self._expires_at(started_at)
            command = ClaimWork(
                run_id=request.run_id,
                node_id=request.node_id,
                work_item_id=work_item_id,
                expected_work_row_version=request.expected_work_row_version,
                expected_node_row_version=request.expected_node_row_version,
                expected_run_row_version=request.expected_run_row_version,
                lease_owner=request.lease_owner,
                started_at=started_at,
                lease_expires_at=lease_expires_at,
                runner_kind=request.runner_kind,
                worker_identity=request.worker_identity,
                event=request.event,
            )
            outcome_unknown = True
            receipt = self._execute(command, work_item_id)
            lease = self._lease_from_claim_receipt(
                receipt,
                command,
                request.expected_attempt_number,
            )
            self._activate(work_item_id, lease)
            outcome_unknown = False
            return lease
        except WorkLeaseAdmissionError, WorkLeaseWriterError:
            outcome_unknown = False
            raise
        except WorkLeaseOutcomeUnknownError:
            outcome_unknown = True
            raise
        finally:
            if outcome_unknown:
                self._mark_unknown(work_item_id)
            else:
                self._release_reservation(work_item_id)

    def renew(self, lease: WorkLease, request: RenewWorkLeaseRequest) -> WorkLease:
        """Extend the current exact service-issued claim and invalidate its wrapper."""
        _require_exact(lease, WorkLease, "work lease renewal capability")
        _require_exact(request, RenewWorkLeaseRequest, "work lease renewal")
        work_item_id = lease.claim.work_item_id
        self._reserve_renewal(lease)
        outcome_unknown = False
        try:
            renewed_at = self._now()
            if renewed_at >= lease.claim.lease_expires_at:
                raise WorkLeaseExpiredError("work lease expired before renewal")
            if lease.claim.row_version >= MAX_LEASE_ROW_VERSION:
                raise WorkLeaseInvalidRequestError("work row version cannot advance")
            lease_expires_at = self._expires_at(renewed_at)
            command = RenewWorkClaim(
                run_id=lease.run.run_id,
                node_id=lease.node.node_id,
                claim=lease.claim,
                expected_run_row_version=request.expected_run_row_version,
                renewed_at=renewed_at,
                lease_expires_at=lease_expires_at,
                event=request.event,
            )
            outcome_unknown = True
            receipt = self._execute(command, work_item_id)
            renewed = self._lease_from_renewal_receipt(receipt, command, lease)
            self._activate(work_item_id, renewed)
            outcome_unknown = False
            return renewed
        except WorkLeaseAdmissionError, WorkLeaseWriterError:
            outcome_unknown = False
            raise
        except WorkLeaseOutcomeUnknownError:
            outcome_unknown = True
            raise
        finally:
            if outcome_unknown:
                self._mark_unknown(work_item_id)
            else:
                self._release_reservation(work_item_id)

    def retire(self, lease: WorkLease) -> None:
        """Forget one exact wrapper after a later durable completion is confirmed."""
        _require_exact(lease, WorkLease, "work lease retirement capability")
        work_item_id = lease.claim.work_item_id
        with self._lock:
            state = self._states.get(work_item_id)
            if work_item_id in self._in_flight or state is None or not state.matches(lease):
                raise WorkLeaseOwnershipError("work lease is not the active service capability")
            del self._states[work_item_id]

    def snapshot(self) -> WorkLeaseServiceSnapshot:
        """Return bounded transient ownership counts under one service lock."""
        with self._lock:
            active = sum(state is not None for state in self._states.values())
            unknown = len(self._states) - active
            return WorkLeaseServiceSnapshot(active, unknown, len(self._in_flight))

    def _reserve_acquisition(self, work_item_id: WorkItemId) -> None:
        with self._lock:
            if work_item_id in self._in_flight or work_item_id in self._states:
                raise WorkLeaseBusyError("work identity already has lease state")
            self._in_flight.add(work_item_id)

    def _reserve_renewal(self, lease: WorkLease) -> None:
        work_item_id = lease.claim.work_item_id
        with self._lock:
            if work_item_id in self._in_flight:
                raise WorkLeaseBusyError("work identity already has a lease operation")
            state = self._states.get(work_item_id)
            if state is None or not state.matches(lease):
                raise WorkLeaseOwnershipError("work lease is not the active service capability")
            self._in_flight.add(work_item_id)

    def _activate(self, work_item_id: WorkItemId, lease: WorkLease) -> None:
        with self._lock:
            if work_item_id not in self._in_flight:
                raise WorkLeaseProtocolError("work lease reservation was lost")
            self._states[work_item_id] = _ActiveWorkLease.capture(lease)
            self._in_flight.remove(work_item_id)

    def _release_reservation(self, work_item_id: WorkItemId) -> None:
        with self._lock:
            self._in_flight.discard(work_item_id)

    def _mark_unknown(self, work_item_id: WorkItemId) -> None:
        with self._lock:
            self._states[work_item_id] = None
            self._in_flight.discard(work_item_id)

    def _now(self) -> UtcTimestamp:
        failed = False
        try:
            value = self._clock.now()
        except Exception:
            failed = True
            value = None
        if failed or type(value) is not UtcTimestamp:
            raise WorkLeaseClockError("work lease clock failed")
        return value

    def _expires_at(self, observed_at: UtcTimestamp) -> UtcTimestamp:
        failed = False
        try:
            value = UtcTimestamp(
                observed_at.to_datetime()
                + timedelta(microseconds=self._settings.lease_duration.microseconds)
            )
        except OverflowError, ValueError:
            failed = True
            value = None
        if failed or value is None:
            raise WorkLeaseClockError("work lease expiry exceeds timestamp bounds")
        return value

    def _execute(self, command: WriterCommand, work_item_id: WorkItemId) -> WriterReceipt:
        admission_failed = False
        writer_failed = False
        unexpected = False
        try:
            ticket = self._writer.submit(
                command,
                timeout_seconds=self._settings.admission_timeout_seconds,
            )
        except WriterAdmissionTimeoutError:
            admission_failed = True
            ticket = None
        except WriterError:
            writer_failed = True
            ticket = None
        except Exception:
            unexpected = True
            ticket = None
        except BaseException:
            self._mark_unknown(work_item_id)
            raise
        if admission_failed:
            raise WorkLeaseAdmissionError("work lease writer admission failed")
        if writer_failed:
            raise WorkLeaseWriterError("work lease writer rejected the command")
        if unexpected or ticket is None:
            raise WorkLeaseProtocolError("work lease writer admission outcome is unknown")
        identity_failed = False
        try:
            submission_id = cast(object, ticket.submission_id)
        except Exception:
            identity_failed = True
            submission_id = None
        except BaseException:
            self._mark_unknown(work_item_id)
            raise
        if identity_failed:
            raise WorkLeaseProtocolError("work lease ticket identity is invalid")
        if type(submission_id) is not WriterSubmissionId:
            raise WorkLeaseProtocolError("work lease ticket identity is invalid")

        ambiguous = False
        definitely_not_executed = False
        writer_failed = False
        unexpected = False
        try:
            receipt = ticket.result(timeout_seconds=self._settings.result_timeout_seconds)
        except WriterResultTimeoutError, WriterCommitOutcomeUnknownError:
            ambiguous = True
            receipt = None
        except WriterDefinitelyNotExecutedError:
            definitely_not_executed = True
            receipt = None
        except WriterError:
            writer_failed = True
            receipt = None
        except Exception:
            unexpected = True
            receipt = None
        except BaseException:
            self._mark_unknown(work_item_id)
            raise
        if ambiguous:
            raise WorkLeaseOutcomeUnknownError("work lease durable outcome is unknown")
        if definitely_not_executed or writer_failed:
            raise WorkLeaseWriterError("work lease command was not committed")
        if unexpected or type(receipt) is not WriterReceipt:
            raise WorkLeaseProtocolError("work lease writer result is invalid")
        if receipt.submission_id != submission_id:
            raise WorkLeaseProtocolError("work lease receipt identity does not match ticket")
        if receipt.command_kind is not command.kind or receipt.run_id != command.run_id:
            raise WorkLeaseProtocolError("work lease receipt does not match command")
        if (
            type(receipt.contention_attempts) is not int
            or not 0 <= receipt.contention_attempts <= MAX_LEASE_CONTENTION_ATTEMPTS
        ):
            raise WorkLeaseProtocolError("work lease receipt contention count is invalid")
        if receipt.mutated is not True:
            raise WorkLeaseProtocolError("work lease command did not report mutation")
        return receipt

    def _lease_from_claim_receipt(
        self,
        receipt: WriterReceipt,
        command: ClaimWork,
        expected_attempt_number: AttemptNumber,
    ) -> WorkLease:
        if type(receipt.result) is not ClaimWorkResult:
            raise WorkLeaseProtocolError("work lease claim result type is invalid")
        result = receipt.result
        _validate_claim_result(result, command, expected_attempt_number)
        return WorkLease(
            result.claim,
            result.node,
            result.run,
            result.events,
            receipt.submission_id,
            _token=_LEASE_CONSTRUCTION_TOKEN,
        )

    def _lease_from_renewal_receipt(
        self,
        receipt: WriterReceipt,
        command: RenewWorkClaim,
        previous: WorkLease,
    ) -> WorkLease:
        if type(receipt.result) is not RenewWorkClaimResult:
            raise WorkLeaseProtocolError("work lease renewal result type is invalid")
        result = receipt.result
        _validate_renewal_result(result, command)
        return WorkLease(
            result.claim,
            previous.node,
            result.run,
            result.events,
            receipt.submission_id,
            _token=_LEASE_CONSTRUCTION_TOKEN,
        )

    def __repr__(self) -> str:
        snapshot = self.snapshot()
        return (
            "WorkLeaseService("
            f"active={snapshot.active}, unknown={snapshot.unknown}, "
            f"in_flight={snapshot.in_flight}, writer=<redacted>, clock=<redacted>)"
        )


def _validate_claim_result(
    result: ClaimWorkResult,
    command: ClaimWork,
    expected_attempt_number: AttemptNumber,
) -> None:
    _require_protocol_exact(result.claim, WorkClaim, "work lease claim result")
    _require_protocol_exact(result.node, RunNodeRecord, "work lease node result")
    _require_protocol_exact(result.run, RunRecord, "work lease run result")
    _require_protocol_exact(result.events, ExecutionEventBatch, "work lease event result")
    _validate_events(result.events, command.run_id, command.event)
    claim = result.claim
    _validate_result_row_version(claim.row_version, "work lease claim row version")
    _validate_result_row_version(result.node.row_version, "work lease node row version")
    _validate_result_row_version(result.run.row_version, "work lease run row version")
    if (
        claim.work_item_id != command.work_item_id
        or claim.attempt_number != expected_attempt_number
        or claim.row_version != command.expected_work_row_version + 1
        or claim.lease_owner != command.lease_owner
        or claim.started_at != command.started_at
        or claim.lease_expires_at != command.lease_expires_at
        or claim.runner_kind != command.runner_kind
        or claim.worker_identity != command.worker_identity
    ):
        raise WorkLeaseProtocolError("work lease claim result does not match command")
    if (
        result.node.run_id != command.run_id
        or result.node.node_id != command.node_id
        or result.node.row_version != command.expected_node_row_version + 1
    ):
        raise WorkLeaseProtocolError("work lease node result does not match command")
    if (
        result.run.run_id != command.run_id
        or result.run.row_version != command.expected_run_row_version + 1
    ):
        raise WorkLeaseProtocolError("work lease run result does not match command")


def _validate_renewal_result(result: RenewWorkClaimResult, command: RenewWorkClaim) -> None:
    _require_protocol_exact(result.claim, WorkClaim, "work lease renewal claim")
    _require_protocol_exact(result.run, RunRecord, "work lease renewal run")
    _require_protocol_exact(result.events, ExecutionEventBatch, "work lease renewal events")
    _validate_events(result.events, command.run_id, command.event)
    previous = command.claim
    claim = result.claim
    _validate_result_row_version(claim.row_version, "work lease renewal row version")
    _validate_result_row_version(result.run.row_version, "work lease run row version")
    if (
        claim.work_item_id != previous.work_item_id
        or claim.attempt_number != previous.attempt_number
        or claim.lease_owner != previous.lease_owner
        or claim.row_version != previous.row_version + 1
        or claim.started_at != previous.started_at
        or claim.lease_expires_at != command.lease_expires_at
        or claim.runner_kind != previous.runner_kind
        or claim.worker_identity != previous.worker_identity
    ):
        raise WorkLeaseProtocolError("work lease renewal result does not preserve claim")
    if (
        result.run.run_id != command.run_id
        or result.run.row_version != command.expected_run_row_version + 1
    ):
        raise WorkLeaseProtocolError("work lease renewal run does not match command")


def _require_exact(value: object, expected: type[object], subject: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{subject} must use {expected.__name__}")


def _require_protocol_exact(value: object, expected: type[object], subject: str) -> None:
    if type(value) is not expected:
        raise WorkLeaseProtocolError(f"{subject} must use {expected.__name__}")


def _validate_events(
    batch: ExecutionEventBatch,
    run_id: RunId,
    request: EventAppendRequest,
) -> None:
    if (
        type(batch.items) is not tuple
        or len(batch.items) != 1
        or type(batch.items[0]) is not ExecutionEventRecord
        or type(batch.next_sequence) is not EventSequence
        or type(batch.counter_row_version) is not int
        or not 1 <= batch.counter_row_version <= MAX_LEASE_ROW_VERSION
    ):
        raise WorkLeaseProtocolError("work lease event result shape is invalid")
    record = batch.items[0]
    pending = request.event
    if (
        record.run_id != run_id
        or record.sequence != request.expected_next_sequence
        or record.event_kind != pending.event_kind
        or record.occurred_at != pending.occurred_at
        or record.subject_kind is not pending.subject_kind
        or record.subject_id != pending.subject_id
        or record.correlation_id != pending.correlation_id
        or record.payload_schema_version != pending.payload_schema_version
        or record.payload != pending.payload
        or int(batch.next_sequence) != int(request.expected_next_sequence) + 1
        or batch.counter_row_version != request.expected_counter_row_version + 1
    ):
        raise WorkLeaseProtocolError("work lease event result does not match command")


def _validate_row_version(value: object, subject: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    if not 1 <= value < MAX_LEASE_ROW_VERSION:
        raise WorkLeaseInvalidRequestError(f"{subject} is outside the supported range")


def _validate_result_row_version(value: object, subject: str) -> None:
    if type(value) is not int or not 1 <= value <= MAX_LEASE_ROW_VERSION:
        raise WorkLeaseProtocolError(f"{subject} is outside the supported range")


def _validate_event_frontier(request: EventAppendRequest) -> None:
    _require_exact(request.expected_next_sequence, EventSequence, "lease event sequence")
    if int(request.expected_next_sequence) >= MAX_LEASE_ROW_VERSION:
        raise WorkLeaseInvalidRequestError("lease event sequence cannot advance")
    _validate_row_version(request.expected_counter_row_version, "lease event counter row version")


def _validate_timeout(value: object, subject: str) -> None:
    if type(value) is not float:
        raise TypeError(f"{subject} must be a float")
    if not 0 <= value <= MAX_LEASE_WRITER_TIMEOUT_SECONDS:
        raise WorkLeaseInvalidRequestError(f"{subject} is outside the supported range")


def _validate_text(value: object, maximum: int, subject: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{subject} must be text")
    text = value
    if not 1 <= len(text) <= maximum:
        raise WorkLeaseInvalidRequestError(f"{subject} is outside the supported range")
    if unicodedata.normalize("NFC", text) != text:
        raise WorkLeaseInvalidRequestError(f"{subject} must use normalized Unicode")


__all__ = [
    "MAX_LEASE_CONTENTION_ATTEMPTS",
    "MAX_LEASE_OWNER_LENGTH",
    "MAX_LEASE_ROW_VERSION",
    "MAX_LEASE_WRITER_TIMEOUT_SECONDS",
    "MAX_RUNNER_KIND_LENGTH",
    "MAX_WORKER_IDENTITY_LENGTH",
    "MAX_WORK_LEASE_MICROSECONDS",
    "MIN_WORK_LEASE_MICROSECONDS",
    "AcquireWorkLeaseRequest",
    "RenewWorkLeaseRequest",
    "WorkLease",
    "WorkLeaseAdmissionError",
    "WorkLeaseBusyError",
    "WorkLeaseClock",
    "WorkLeaseClockError",
    "WorkLeaseError",
    "WorkLeaseExpiredError",
    "WorkLeaseInvalidRequestError",
    "WorkLeaseOutcomeUnknownError",
    "WorkLeaseOwnershipError",
    "WorkLeaseProtocolError",
    "WorkLeaseService",
    "WorkLeaseServiceSnapshot",
    "WorkLeaseSettings",
    "WorkLeaseWriter",
    "WorkLeaseWriterError",
]
