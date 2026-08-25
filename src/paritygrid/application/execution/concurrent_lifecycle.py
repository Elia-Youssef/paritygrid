"""Concurrent pause, resume, and cancellation lifecycle coordination (P7.10).

The sequential coordinators in :mod:`paritygrid.application.execution.pause`
and :mod:`paritygrid.application.execution.cancellation` own the durable
pause and cancellation arrows for the node-sequential reference.  This
module applies the same durable discipline to concurrent runs: pause
acknowledgement and pause-abort are one compare-and-set with exactly one
winner, every durable arrow re-reads the current run and event frontier
before submission, and the work-lease admission gate is reserved for the
whole pause window so no new claim can race the stable boundary.

The compare-and-set lives on :class:`ConcurrentPauseSignal`: a pause
request carries the scheduler's control generation, and the stable
boundary either acknowledges that exact generation (the run is durably
paused and requires an explicit resume) or an abort wins first (the run
keeps admitting under a bumped generation).  No live concurrency object
— thread, task, queue, or future — ever enters a durable transition or
a recovery fact.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum
from threading import Lock
from typing import Protocol, cast, runtime_checkable

from paritygrid.application.execution.leasing import (
    WorkLeasePauseReservation,
    WorkLeaseService,
)
from paritygrid.application.execution.pause import (
    PAUSE_EVENT_PAYLOAD_SCHEMA_VERSION,
    PauseDurableState,
)
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    WriterCommand,
    WriterSubmissionId,
    WriterTicket,
)
from paritygrid.application.writes.execution import TransitionRun
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import RunId, UtcTimestamp

CONCURRENT_LIFECYCLE_VERSION = 1
MAX_LIFECYCLE_CORRELATION_ID_LENGTH = 96
MAX_LIFECYCLE_TIMEOUT_SECONDS = 86_400.0

_EVENT_KINDS: dict[RunState, str] = {
    RunState.RUNNING: "run_started",
    RunState.PAUSING: "run_pausing",
    RunState.PAUSED: "run_paused",
    RunState.RESUMING: "run_resuming",
    RunState.CANCELLING: "run_cancelling",
    RunState.CANCELLED: "run_cancelled",
}


class ConcurrentLifecycleError(RuntimeError):
    """Base failure for concurrent lifecycle coordination."""


class ConcurrentLifecycleInvalidRequestError(ConcurrentLifecycleError):
    """A lifecycle request violated the concurrent contract."""


class ConcurrentLifecycleStateReadError(ConcurrentLifecycleError):
    """The durable run state could not be read for a lifecycle decision."""


class ConcurrentLifecycleRejectedError(ConcurrentLifecycleError):
    """The durable run state rejected the requested lifecycle arrow."""


class ConcurrentLifecycleAdmissionError(ConcurrentLifecycleError):
    """The writer refused a lifecycle command without a durable effect."""


class ConcurrentLifecycleOutcomeUnknownError(ConcurrentLifecycleError):
    """A lifecycle writer outcome stayed unknown and needs recovery."""


class ConcurrentLifecycleIncompleteError(ConcurrentLifecycleError):
    """The first lifecycle arrow committed but the second did not."""


class ConcurrentLifecycleBusyError(ConcurrentLifecycleError):
    """A pause or cancellation is already pending for the run."""


@runtime_checkable
class ConcurrentLifecycleClock(Protocol):
    """Injected clock contract for lifecycle transitions."""

    def now(self) -> UtcTimestamp: ...


@runtime_checkable
class ConcurrentLifecycleStateReader(Protocol):
    """Durable run-state reader reused from the accepted pause contract."""

    def read(self, run_id: RunId, /) -> PauseDurableState: ...


@runtime_checkable
class ConcurrentLifecycleWriter(Protocol):
    """Borrowed transactional-writer surface for lifecycle arrows."""

    def submit(self, command: WriterCommand, *, timeout_seconds: float) -> WriterTicket: ...


@dataclass(frozen=True, slots=True)
class ConcurrentLifecycleSettings:
    """Bounded timeouts for concurrent lifecycle coordination."""

    admission_timeout_seconds: float = 5.0
    result_timeout_seconds: float = 60.0

    def __post_init__(self) -> None:
        for name in ("admission_timeout_seconds", "result_timeout_seconds"):
            value = getattr(self, name)
            if type(value) is not float:
                raise TypeError(f"{name} must be a float second count")
            if not math.isfinite(value) or not 0.0 < value <= MAX_LIFECYCLE_TIMEOUT_SECONDS:
                raise ConcurrentLifecycleInvalidRequestError(
                    f"{name} is outside the supported range"
                )


class ConcurrentPauseSignal:
    """Compare-and-set pause signal keyed by the scheduler control generation.

    ``request`` installs one unacknowledged generation.  Exactly one of
    ``try_acknowledge`` or ``try_abort`` can win that generation; the
    loser observes ``False`` deterministically.  ``clear`` retires an
    acknowledged generation after an explicit resume.
    """

    __slots__ = ("_generation", "_lock", "_state")

    def __init__(self) -> None:
        self._lock = Lock()
        self._generation: int | None = None
        self._state = _SignalState.IDLE

    @property
    def is_requested(self) -> bool:
        with self._lock:
            return self._state is _SignalState.REQUESTED

    @property
    def requested_generation(self) -> int | None:
        with self._lock:
            return self._generation if self._state is _SignalState.REQUESTED else None

    def request(self, generation: int) -> None:
        """Install one unacknowledged pause generation."""
        _require_generation(generation)
        with self._lock:
            if self._state is _SignalState.REQUESTED:
                raise ConcurrentLifecycleBusyError("a pause request is already pending")
            if self._state is _SignalState.ACKNOWLEDGED:
                raise ConcurrentLifecycleBusyError(
                    "an acknowledged pause awaits an explicit resume"
                )
            self._generation = generation
            self._state = _SignalState.REQUESTED

    def try_acknowledge(self, generation: int) -> bool:
        """Claim the pending generation for a durable pause, exactly once."""
        _require_generation(generation)
        with self._lock:
            if self._state is _SignalState.REQUESTED and self._generation == generation:
                self._state = _SignalState.ACKNOWLEDGED
                return True
            return False

    def try_abort(self, generation: int) -> bool:
        """Abort the pending generation before any acknowledgement wins."""
        _require_generation(generation)
        with self._lock:
            if self._state is _SignalState.REQUESTED and self._generation == generation:
                self._generation = None
                self._state = _SignalState.IDLE
                return True
            return False

    def clear(self, generation: int) -> bool:
        """Retire an acknowledged generation after an explicit resume."""
        _require_generation(generation)
        with self._lock:
            if self._state is _SignalState.ACKNOWLEDGED and self._generation == generation:
                self._generation = None
                self._state = _SignalState.IDLE
                return True
            return False


class _SignalState(StrEnum):
    IDLE = "idle"
    REQUESTED = "requested"
    ACKNOWLEDGED = "acknowledged"


@dataclass(frozen=True, slots=True)
class ConcurrentPausedProof:
    """Coordinator-issued proof that one run is durably paused."""

    run_id: str
    generation: int
    control_state: str

    def __post_init__(self) -> None:
        if type(self.run_id) is not str or not self.run_id:
            raise ValueError("paused proof run identity must be text")
        _require_generation(self.generation)
        if self.control_state != "paused":
            raise ValueError("paused proof must record the paused control state")


@dataclass(frozen=True, slots=True)
class ConcurrentLifecycleReport:
    """One durable lifecycle outcome with its submission evidence."""

    action: str
    from_state: RunState
    to_state: RunState
    submission_ids: tuple[WriterSubmissionId, ...]

    def __post_init__(self) -> None:
        if type(self.action) is not str or not self.action:
            raise ValueError("lifecycle action must be text")
        if type(self.submission_ids) is not tuple:
            raise TypeError("lifecycle submission ids must be a tuple")


class ConcurrentLifecycleCoordinator:
    """Own durable pause, resume, and cancellation arrows for one engine.

    The coordinator reserves the work-lease admission gate for the whole
    pause window, re-reads the durable run and event frontier before
    every arrow, and submits only exact ``TransitionRun`` commands.  The
    pause compare-and-set is decided on the signal before any durable
    arrow: an acknowledged pause writes the two arrows and returns a
    proof an explicit resume must consume; an aborted pause writes
    nothing.
    """

    __slots__ = (
        "_clock",
        "_correlation_id",
        "_lock",
        "_reader",
        "_settings",
        "_uncertain",
        "_writer",
    )

    def __init__(
        self,
        writer: ConcurrentLifecycleWriter,
        reader: ConcurrentLifecycleStateReader,
        clock: ConcurrentLifecycleClock,
        *,
        settings: ConcurrentLifecycleSettings | None = None,
        correlation_id: str | None = None,
    ) -> None:
        writer_value = cast(object, writer)
        if not isinstance(writer_value, ConcurrentLifecycleWriter):
            raise TypeError("lifecycle writer must implement the writer protocol")
        reader_value = cast(object, reader)
        if not isinstance(reader_value, ConcurrentLifecycleStateReader):
            raise TypeError("lifecycle reader must implement the state reader protocol")
        clock_value = cast(object, clock)
        if not isinstance(clock_value, ConcurrentLifecycleClock):
            raise TypeError("lifecycle clock must implement the clock protocol")
        if settings is not None and type(settings) is not ConcurrentLifecycleSettings:
            raise TypeError("lifecycle settings must use ConcurrentLifecycleSettings")
        self._writer = writer_value
        self._reader = reader_value
        self._clock = clock_value
        self._settings = settings or ConcurrentLifecycleSettings()
        self._correlation_id = _require_correlation_id(correlation_id)
        self._lock = Lock()
        self._uncertain = False

    @property
    def is_uncertain(self) -> bool:
        """Report whether any lifecycle writer outcome stayed unknown."""
        return self._uncertain

    def read_state(self, run_id: RunId) -> PauseDurableState:
        """Return the durable run and event frontier for one run."""
        _require_run_id(run_id)
        try:
            state = self._reader.read(run_id)
        except ConcurrentLifecycleError:
            raise
        except Exception as error:
            raise ConcurrentLifecycleStateReadError(
                "durable lifecycle state could not be read"
            ) from error
        if type(state) is not PauseDurableState:
            raise ConcurrentLifecycleStateReadError(
                "durable lifecycle state must use PauseDurableState"
            )
        return state

    def complete_pause(
        self,
        run_id: RunId,
        *,
        lease_service: WorkLeaseService,
        reservation: WorkLeasePauseReservation,
        signal: ConcurrentPauseSignal,
        generation: int,
    ) -> ConcurrentPausedProof:
        """Write the durable pause arrows after the signal acknowledged.

        The caller reaches a stable boundary first (no admission, every
        in-flight result durably committed or explicitly recoverable);
        only then may the signal be acknowledged.  An acknowledged pause
        writes ``RUNNING``→``PAUSING``→``PAUSED`` and hands the lease
        reservation to the returned proof: an explicit resume consumes
        both.
        """

        _require_run_id(run_id)
        if type(lease_service) is not WorkLeaseService:
            raise TypeError("pause completion must use WorkLeaseService")
        if type(reservation) is not WorkLeasePauseReservation:
            raise TypeError("pause completion must hold the lease pause reservation")
        if type(signal) is not ConcurrentPauseSignal:
            raise TypeError("pause completion must use ConcurrentPauseSignal")
        _require_generation(generation)
        if not signal.try_acknowledge(generation):
            raise ConcurrentLifecycleRejectedError(
                "pause acknowledgement lost the compare-and-set; the pause was aborted"
            )
        with self._lock:
            self._transition_pair(run_id, (RunState.PAUSING, RunState.PAUSED))
        return ConcurrentPausedProof(
            run_id=str(run_id),
            generation=generation,
            control_state="paused",
        )

    def resume(
        self,
        proof: ConcurrentPausedProof,
        *,
        lease_service: WorkLeaseService,
        reservation: WorkLeasePauseReservation,
        signal: ConcurrentPauseSignal,
    ) -> ConcurrentLifecycleReport:
        """Resume one durably paused run and release the admission gate."""

        if type(proof) is not ConcurrentPausedProof:
            raise TypeError("resume must consume ConcurrentPausedProof")
        if type(lease_service) is not WorkLeaseService:
            raise TypeError("resume must use WorkLeaseService")
        if type(reservation) is not WorkLeasePauseReservation:
            raise TypeError("resume must hold the lease pause reservation")
        if type(signal) is not ConcurrentPauseSignal:
            raise TypeError("resume must use ConcurrentPauseSignal")
        run_id = RunId(proof.run_id)
        with self._lock:
            from_state, to_state = self._transition_pair(
                run_id, (RunState.RESUMING, RunState.RUNNING)
            )
        signal.clear(proof.generation)
        lease_service.release_pause(reservation)
        return ConcurrentLifecycleReport(
            action="resumed",
            from_state=from_state,
            to_state=to_state,
            submission_ids=(),
        )

    def begin_cancellation(
        self,
        run_id: RunId,
    ) -> ConcurrentLifecycleReport:
        """Write the durable cancellation request arrow for one run.

        ``QUEUED`` runs cancel before start; ``RUNNING`` runs enter
        ``CANCELLING`` and the caller drains owned work before
        :meth:`finish_cancellation`.
        """

        _require_run_id(run_id)
        with self._lock:
            state = self.read_state(run_id)
            current = state.run.state
            if current is RunState.QUEUED:
                target = RunState.CANCELLED
            elif current is RunState.RUNNING:
                target = RunState.CANCELLING
            elif current is RunState.CANCELLING:
                return ConcurrentLifecycleReport(
                    action="cancelling",
                    from_state=current,
                    to_state=current,
                    submission_ids=(),
                )
            else:
                raise ConcurrentLifecycleRejectedError(
                    "run state does not admit a cancellation request"
                )
            submission = self._transition(run_id, target)
            return ConcurrentLifecycleReport(
                action="cancellation_begun",
                from_state=current,
                to_state=target,
                submission_ids=(submission,),
            )

    def finish_cancellation(self, run_id: RunId) -> ConcurrentLifecycleReport:
        """Write the terminal cancelled arrow after owned work drained."""

        _require_run_id(run_id)
        with self._lock:
            state = self.read_state(run_id)
            current = state.run.state
            if current is RunState.CANCELLED:
                return ConcurrentLifecycleReport(
                    action="already_cancelled",
                    from_state=current,
                    to_state=current,
                    submission_ids=(),
                )
            if current is not RunState.CANCELLING:
                raise ConcurrentLifecycleRejectedError(
                    "only a cancelling run can finish cancellation"
                )
            submission = self._transition(run_id, RunState.CANCELLED)
            return ConcurrentLifecycleReport(
                action="cancelled",
                from_state=current,
                to_state=RunState.CANCELLED,
                submission_ids=(submission,),
            )

    def _transition_pair(
        self,
        run_id: RunId,
        targets: tuple[RunState, RunState],
    ) -> tuple[RunState, RunState]:
        self._transition(run_id, targets[0])
        self._transition(run_id, targets[1])
        return targets[0], targets[1]

    def _transition(self, run_id: RunId, target: RunState) -> WriterSubmissionId:
        state = self.read_state(run_id)
        current = state.run.state
        if not current.can_transition_to(target):
            raise ConcurrentLifecycleRejectedError(
                f"durable run state {current.value} rejects {target.value}"
            )
        transitioned_at = self._clock.now()
        if type(transitioned_at) is not UtcTimestamp:
            raise ConcurrentLifecycleStateReadError("lifecycle clock returned an invalid timestamp")
        command = TransitionRun(
            run_id=run_id,
            expected_run_row_version=state.run.row_version,
            target_state=target,
            transitioned_at=transitioned_at,
            execution_evidence_fingerprint=None,
            execution_evidence_fingerprint_version=None,
            event=EventAppendRequest(
                EventSequence(state.next_event_sequence.number),
                state.event_counter_row_version,
                PendingExecutionEvent(
                    _EVENT_KINDS[target],
                    transitioned_at,
                    EventSubjectKind.RUN,
                    run_id,
                    self._correlation_id,
                    PAUSE_EVENT_PAYLOAD_SCHEMA_VERSION,
                    RedactedDocument.from_mapping(
                        {"from_state": current.value, "to_state": target.value}
                    ),
                ),
            ),
        )
        try:
            ticket = self._writer.submit(
                command, timeout_seconds=self._settings.admission_timeout_seconds
            )
            receipt = ticket.result(timeout_seconds=self._settings.result_timeout_seconds)
        except ConcurrentLifecycleError:
            raise
        except Exception as error:
            self._uncertain = True
            raise ConcurrentLifecycleOutcomeUnknownError(
                "lifecycle writer outcome is unknown"
            ) from error
        submission = receipt.submission_id
        if type(submission) is not WriterSubmissionId:
            self._uncertain = True
            raise ConcurrentLifecycleOutcomeUnknownError(
                "lifecycle writer receipt identity is invalid"
            )
        return submission


def _require_generation(value: object) -> None:
    if type(value) is not int or not 1 <= value <= 2_147_483_647:
        raise TypeError("control generation must be an integer in the supported range")


def _require_run_id(value: object) -> RunId:
    if type(value) is not RunId:
        raise TypeError("lifecycle run identity must use RunId")
    return value


def _require_correlation_id(value: object) -> str | None:
    if value is None:
        return None
    if type(value) is not str:
        raise TypeError("lifecycle correlation id must be text or None")
    text = value
    if not 1 <= len(text) <= MAX_LIFECYCLE_CORRELATION_ID_LENGTH:
        raise ConcurrentLifecycleInvalidRequestError(
            "lifecycle correlation id length is outside the range"
        )
    for character in text:
        if not "\x20" <= character <= "\x7e":
            raise ConcurrentLifecycleInvalidRequestError(
                "lifecycle correlation id must use printable ASCII"
            )
    return text


__all__ = [
    "CONCURRENT_LIFECYCLE_VERSION",
    "MAX_LIFECYCLE_CORRELATION_ID_LENGTH",
    "MAX_LIFECYCLE_TIMEOUT_SECONDS",
    "ConcurrentLifecycleAdmissionError",
    "ConcurrentLifecycleBusyError",
    "ConcurrentLifecycleClock",
    "ConcurrentLifecycleCoordinator",
    "ConcurrentLifecycleError",
    "ConcurrentLifecycleIncompleteError",
    "ConcurrentLifecycleInvalidRequestError",
    "ConcurrentLifecycleOutcomeUnknownError",
    "ConcurrentLifecycleRejectedError",
    "ConcurrentLifecycleReport",
    "ConcurrentLifecycleSettings",
    "ConcurrentLifecycleStateReadError",
    "ConcurrentLifecycleStateReader",
    "ConcurrentLifecycleWriter",
    "ConcurrentPauseSignal",
    "ConcurrentPausedProof",
]
