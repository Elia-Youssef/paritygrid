"""Stable-boundary pause and resume coordination for sequential execution."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from threading import Lock
from typing import Protocol, cast, runtime_checkable

from paritygrid.application.execution.leasing import (
    WorkLeaseError,
    WorkLeasePauseReservation,
    WorkLeaseService,
    WorkLeaseServiceSnapshot,
)
from paritygrid.application.execution.scheduler import (
    ScheduledNode,
    ScheduledNodeStatus,
    SchedulerState,
    SchedulerStatus,
)
from paritygrid.application.planner import PlanFingerprint
from paritygrid.application.ports.configuration import (
    ConfigurationDocument,
    DocumentArray,
    DocumentValue,
    NestedDocumentObject,
)
from paritygrid.application.ports.consistency import (
    MAX_CONSISTENCY_SEQUENCE,
    ConsistencyRepositoryError,
    EventSequence,
    EventSubjectKind,
    ExecutionEventBatch,
    ExecutionEventRecord,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.execution import (
    ExecutionRepositoryError,
    RunRecord,
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
from paritygrid.application.writes import TransitionRun, TransitionRunResult
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    StateFingerprint,
    UtcTimestamp,
)

PAUSE_EVENT_PAYLOAD_SCHEMA_VERSION = 1
MAX_PAUSE_CORRELATION_ID_LENGTH = 96
MAX_PAUSE_TIMEOUT_SECONDS = 86_400.0
MAX_PAUSE_CONTENTION_ATTEMPTS = 9

_PORTABLE_IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]*", flags=re.ASCII)
_PAUSED_RUN_TOKEN = object()
_PAUSE_ACKNOWLEDGEMENT_TOKEN = object()
_PAUSE_RUNNER_TOKEN = object()
_DEFINITELY_NOT_EXECUTED = (
    WriterDefinitelyNotExecutedError,
    ExecutionRepositoryError,
    ConsistencyRepositoryError,
)


class PauseCoordinatorError(RuntimeError):
    """Base failure for stable-boundary pause coordination."""


class PauseCoordinatorBusyError(PauseCoordinatorError):
    """An overlapping control operation or incompatible request was rejected."""


class PauseCoordinatorInvalidRequestError(PauseCoordinatorError):
    """Pause evidence or lifecycle state is not an admissible boundary."""


class PauseCoordinatorNotReadyError(PauseCoordinatorError):
    """The admission gate is installed but work has not fully drained."""


class PauseCoordinatorClockError(PauseCoordinatorError):
    """The injected clock did not produce one exact safe timestamp."""


class PauseCoordinatorStateReadError(PauseCoordinatorError):
    """A fresh durable run/event frontier could not be read safely."""


class PauseCoordinatorAdmissionError(PauseCoordinatorError):
    """Writer admission failed before a durable command identity was allocated."""


class PauseCoordinatorRejectedError(PauseCoordinatorError):
    """The first transition was proven not to have executed."""


class PauseCoordinatorIncompleteError(PauseCoordinatorError):
    """The first arrow committed but the lifecycle pair did not complete."""


class PauseCoordinatorOutcomeUnknownError(PauseCoordinatorError):
    """An admitted transition has no proven durable outcome."""


class PauseCoordinatorProtocolError(PauseCoordinatorOutcomeUnknownError):
    """Borrowed collaborator evidence was malformed or inconsistent."""


class PauseAction(StrEnum):
    """Closed successful control actions."""

    PAUSED = "paused"
    RESUMED = "resumed"


@dataclass(frozen=True, slots=True, repr=False, init=False)
class PauseAcknowledgement:
    """Runner-issued proof of one exact stable scheduler frontier."""

    _scheduler_state: SchedulerState
    _generation: int

    def __init__(
        self,
        scheduler_state: SchedulerState,
        generation: int,
        *,
        _token: object,
    ) -> None:
        if _token is not _PAUSE_ACKNOWLEDGEMENT_TOKEN:
            raise PauseCoordinatorInvalidRequestError(
                "pause acknowledgements are runner-issued only"
            )
        object.__setattr__(self, "_scheduler_state", scheduler_state)
        object.__setattr__(self, "_generation", generation)

    @property
    def scheduler_state(self) -> SchedulerState:
        """Return the acknowledged stable scheduler frontier."""
        return self._scheduler_state

    @property
    def generation(self) -> int:
        """Return the exact pause generation carried by this opaque proof."""
        return self._generation

    def __repr__(self) -> str:
        return "PauseAcknowledgement(authority=<redacted>)"


class PauseToken:
    """Thread-safe pause signal whose clearing is generation guarded."""

    __slots__ = (
        "_acknowledged_state",
        "_acknowledgement",
        "_generation",
        "_lock",
        "_requested",
        "_runner_authority",
    )

    def __init__(self) -> None:
        self._lock = Lock()
        self._generation = 0
        self._requested = False
        self._acknowledgement: PauseAcknowledgement | None = None
        self._acknowledged_state: SchedulerState | None = None
        self._runner_authority: object | None = None

    def _bind_runner(self, *, _token: object) -> object:
        """Bind this signal to one exact sequential-runner instance."""
        if _token is not _PAUSE_RUNNER_TOKEN:
            raise PauseCoordinatorInvalidRequestError(
                "pause tokens bind only to sequential runners"
            )
        with self._lock:
            if self._runner_authority is not None:
                raise PauseCoordinatorBusyError(
                    "pause token is already bound to a sequential runner"
                )
            authority = object()
            self._runner_authority = authority
            return authority

    @property
    def is_requested(self) -> bool:
        """Return whether the sequential runner must stop at its next boundary."""
        with self._lock:
            return self._requested

    def request_for_coordinator(self) -> int:
        """Signal one coordinator-owned generation."""
        with self._lock:
            self._generation += 1
            self._requested = True
            self._acknowledgement = None
            self._acknowledged_state = None
            return self._generation

    def _acknowledge_for_runner(
        self,
        state: SchedulerState,
        *,
        authority: object,
        _token: object,
    ) -> PauseAcknowledgement | None:
        """Claim the requested pause generation or lose to a coordinator abort.

        The claim succeeds only while the pause generation is still requested,
        so an abort that cleared the signal first makes this compare-and-set
        lose without raising: exactly one side wins, and no pause-coordinator
        failure can leak through the runner.
        """
        if _token is not _PAUSE_RUNNER_TOKEN:
            raise PauseCoordinatorInvalidRequestError(
                "pause acknowledgements are sequential-runner-issued only"
            )
        with self._lock:
            if authority is not self._runner_authority:
                raise PauseCoordinatorInvalidRequestError(
                    "pause acknowledgement runner authority is foreign"
                )
        clean = _snapshot_scheduler(state)
        if clean.active_node_id is not None or clean.status is not SchedulerStatus.ACTIVE:
            raise PauseCoordinatorInvalidRequestError(
                "runner pause acknowledgement requires a stable active scheduler"
            )
        with self._lock:
            if authority is not self._runner_authority:
                raise PauseCoordinatorInvalidRequestError(
                    "pause acknowledgement runner authority is foreign"
                )
            if not self._requested:
                return None
            acknowledgement = PauseAcknowledgement(
                clean,
                self._generation,
                _token=_PAUSE_ACKNOWLEDGEMENT_TOKEN,
            )
            self._acknowledgement = acknowledgement
            self._acknowledged_state = _snapshot_scheduler(clean)
            return acknowledgement

    def snapshot_acknowledgement(
        self,
        acknowledgement: PauseAcknowledgement,
        generation: int,
    ) -> SchedulerState | None:
        """Return a detached frontier only for the current runner-issued proof."""
        with self._lock:
            if (
                not self._requested
                or self._generation != generation
                or self._acknowledgement is not acknowledgement
                or type(acknowledgement) is not PauseAcknowledgement
                or type(acknowledgement.generation) is not int
                or acknowledgement.generation != generation
                or self._acknowledged_state is None
            ):
                return None
            invalid = False
            try:
                observed = _snapshot_scheduler(acknowledgement.scheduler_state)
            except Exception:
                invalid = True
                observed = None
            if invalid or observed != self._acknowledged_state:
                return None
            return _snapshot_scheduler(self._acknowledged_state)

    def clear_for_coordinator(self, generation: int) -> bool:
        """Clear only the exact generation that completed durable resume."""
        with self._lock:
            if self._generation != generation:
                return False
            self._requested = False
            self._acknowledgement = None
            self._acknowledged_state = None
            return True

    def abort_for_coordinator(self, generation: int) -> bool:
        """Abort only while the exact generation remains unacknowledged.

        This is the coordinator side of the compare-and-set that
        ``_acknowledge_for_runner`` claims: exactly one of an abort or a
        runner acknowledgement can win the same unacknowledged generation.
        """
        with self._lock:
            if self._generation != generation or self._acknowledgement is not None:
                return False
            self._requested = False
            self._acknowledgement = None
            self._acknowledged_state = None
            return True

    def __repr__(self) -> str:
        return f"PauseToken(requested={self.is_requested!r})"


@dataclass(frozen=True, slots=True)
class PauseCoordinatorSettings:
    """Bounded admission and result waits for each lifecycle arrow."""

    admission_timeout_seconds: float = 5.0
    result_timeout_seconds: float = 60.0

    def __post_init__(self) -> None:
        _validate_timeout(self.admission_timeout_seconds, "pause admission timeout")
        _validate_timeout(self.result_timeout_seconds, "pause result timeout")


@dataclass(frozen=True, slots=True, repr=False)
class PauseDurableState:
    """One transactionally read run and event allocation frontier."""

    run: RunRecord
    next_event_sequence: EventSequence
    event_counter_row_version: int
    active_work_count: int = 0

    def __post_init__(self) -> None:
        if type(self.run) is not RunRecord:
            raise TypeError("pause durable state run must use RunRecord")
        if type(self.next_event_sequence) is not EventSequence:
            raise TypeError("pause event frontier must use EventSequence")
        if type(self.event_counter_row_version) is not int:
            raise TypeError("pause event counter row version must be an integer")
        if not 1 <= self.event_counter_row_version <= MAX_CONSISTENCY_SEQUENCE:
            raise ValueError("pause event counter row version is outside the supported range")
        if type(self.active_work_count) is not int:
            raise TypeError("pause active work count must be an integer")
        if not 0 <= self.active_work_count <= MAX_CONSISTENCY_SEQUENCE:
            raise ValueError("pause active work count is outside the supported range")

    def __repr__(self) -> str:
        return (
            "PauseDurableState("
            f"run_id={self.run.run_id!r}, state={self.run.state.value!r}, "
            f"run_row_version={self.run.row_version!r}, "
            f"next_event_sequence={self.next_event_sequence.number!r}, "
            f"event_counter_row_version={self.event_counter_row_version!r}, "
            f"active_work_count={self.active_work_count!r})"
        )


@runtime_checkable
class PauseClock(Protocol):
    """Injected exact UTC clock used before durable transition admission."""

    def now(self) -> UtcTimestamp:
        """Return the current exact UTC timestamp."""
        ...


@runtime_checkable
class PauseStateReader(Protocol):
    """Borrowed short-transaction reader for a run and its event frontier."""

    def read(self, run_id: RunId, /) -> PauseDurableState:
        """Read one coherent durable pause frontier."""
        ...


@runtime_checkable
class PauseWriter(Protocol):
    """Borrowed transactional-writer surface without lifecycle ownership."""

    def submit(
        self,
        command: WriterCommand,
        *,
        timeout_seconds: float,
    ) -> WriterTicket:
        """Submit one exact run transition."""
        ...


@dataclass(frozen=True, slots=True, repr=False, init=False)
class PausedRun:
    """Service-issued proof that a run reached one stable PAUSED boundary."""

    _run: RunRecord
    _scheduler_state: SchedulerState
    _events: ExecutionEventBatch
    _submission_ids: tuple[WriterSubmissionId, WriterSubmissionId]

    def __init__(
        self,
        run: RunRecord,
        scheduler_state: SchedulerState,
        events: ExecutionEventBatch,
        submission_ids: tuple[WriterSubmissionId, WriterSubmissionId],
        *,
        _token: object,
    ) -> None:
        if _token is not _PAUSED_RUN_TOKEN:
            raise PauseCoordinatorInvalidRequestError("paused-run proofs are coordinator-issued")
        object.__setattr__(self, "_run", run)
        object.__setattr__(self, "_scheduler_state", scheduler_state)
        object.__setattr__(self, "_events", events)
        object.__setattr__(self, "_submission_ids", submission_ids)

    @property
    def run(self) -> RunRecord:
        return self._run

    @property
    def scheduler_state(self) -> SchedulerState:
        return self._scheduler_state

    @property
    def events(self) -> ExecutionEventBatch:
        return self._events

    @property
    def submission_ids(self) -> tuple[WriterSubmissionId, WriterSubmissionId]:
        return self._submission_ids

    def __repr__(self) -> str:
        return (
            "PausedRun("
            f"run_id={self._run.run_id!r}, run_row_version={self._run.row_version!r}, "
            f"scheduler_status={self._scheduler_state.status.value!r}, "
            "authority=<redacted>)"
        )


@dataclass(frozen=True, slots=True, repr=False)
class PauseCoordinatorReport:
    """Proven successful durable pair with its stable scheduler frontier."""

    action: PauseAction
    run: RunRecord
    scheduler_state: SchedulerState
    events: ExecutionEventBatch
    submission_ids: tuple[WriterSubmissionId, WriterSubmissionId]

    def __repr__(self) -> str:
        return (
            "PauseCoordinatorReport("
            f"action={self.action.value!r}, run_id={self.run.run_id!r}, "
            f"run_row_version={self.run.row_version!r}, "
            f"scheduler_status={self.scheduler_state.status.value!r})"
        )


class PauseCoordinator:
    """Converge a run at a stable checkpoint boundary through two exact arrows."""

    __slots__ = (
        "_clock",
        "_generation",
        "_intermediate",
        "_lease_service",
        "_lifecycle_lock",
        "_operation_lock",
        "_paused",
        "_paused_evidence",
        "_reader",
        "_reservation",
        "_run_id",
        "_settings",
        "_token",
        "_uncertain",
        "_writer",
    )

    def __init__(
        self,
        writer: PauseWriter,
        reader: PauseStateReader,
        lease_service: WorkLeaseService,
        clock: PauseClock,
        *,
        settings: PauseCoordinatorSettings | None = None,
    ) -> None:
        writer_value = cast(object, writer)
        reader_value = cast(object, reader)
        clock_value = cast(object, clock)
        if not isinstance(writer_value, PauseWriter):
            raise TypeError("pause writer must provide transactional submit")
        if not isinstance(reader_value, PauseStateReader):
            raise TypeError("pause state reader must provide a coherent read")
        if type(lease_service) is not WorkLeaseService:
            raise TypeError("pause lease service must use WorkLeaseService")
        if not isinstance(clock_value, PauseClock):
            raise TypeError("pause clock must provide exact UTC time")
        selected_settings = PauseCoordinatorSettings() if settings is None else settings
        if type(selected_settings) is not PauseCoordinatorSettings:
            raise TypeError("pause settings must use PauseCoordinatorSettings")
        self._writer = writer_value
        self._reader = reader_value
        self._lease_service = lease_service
        self._clock = clock_value
        self._settings = selected_settings
        self._token = PauseToken()
        self._lifecycle_lock = Lock()
        self._operation_lock = Lock()
        self._reservation: WorkLeasePauseReservation | None = None
        self._run_id: RunId | None = None
        self._generation: int | None = None
        self._paused: PausedRun | None = None
        self._paused_evidence: object | None = None
        self._intermediate = False
        self._uncertain = False

    @property
    def token(self) -> PauseToken:
        """Return the pause signal shared with the sequential runner."""
        return self._token

    def request_pause(self, run_id: RunId) -> None:
        """Close run-scoped acquisition admission before signaling the runner."""
        clean_run_id = _snapshot_run_id(run_id)
        if not self._operation_lock.acquire(blocking=False):
            raise PauseCoordinatorBusyError("pause coordinator already has an active operation")
        try:
            with self._lifecycle_lock:
                if self._reservation is not None:
                    if self._run_id == clean_run_id and self._paused is None:
                        return
                    raise PauseCoordinatorBusyError("pause coordinator already owns a request")
                reservation_failed = False
                try:
                    reservation = self._lease_service.reserve_pause(clean_run_id)
                except WorkLeaseError:
                    reservation_failed = True
                    reservation = None
                if reservation_failed or reservation is None:
                    raise PauseCoordinatorBusyError("pause admission gate could not be installed")
                try:
                    generation = self._token.request_for_coordinator()
                except BaseException:
                    _suppress_base_exception(lambda: self._lease_service.release_pause(reservation))
                    raise
                self._reservation = reservation
                self._run_id = clean_run_id
                self._generation = generation
        finally:
            self._operation_lock.release()

    def pause(
        self,
        acknowledgement: PauseAcknowledgement,
        *,
        correlation_id: str | None = None,
    ) -> tuple[PausedRun, PauseCoordinatorReport]:
        """Persist RUNNING -> PAUSING -> PAUSED after one stable boundary."""
        correlation = _validate_correlation_id(correlation_id)
        if type(acknowledgement) is not PauseAcknowledgement:
            raise PauseCoordinatorInvalidRequestError(
                "pause requires a runner-issued acknowledgement"
            )
        if not self._operation_lock.acquire(blocking=False):
            raise PauseCoordinatorBusyError("pause coordinator already has an active operation")
        try:
            reservation, run_id = self._active_request()
            with self._lifecycle_lock:
                if self._paused is not None:
                    raise PauseCoordinatorBusyError("run is already paused")
                if self._uncertain or self._intermediate:
                    raise PauseCoordinatorOutcomeUnknownError(
                        "pause lifecycle requires durable recovery inspection"
                    )
                generation = self._generation
            if generation is None:
                raise PauseCoordinatorProtocolError("pause generation evidence is invalid")
            scheduler = self._token.snapshot_acknowledgement(
                acknowledgement,
                generation,
            )
            if scheduler is None:
                raise PauseCoordinatorInvalidRequestError(
                    "pause acknowledgement is stale or foreign"
                )
            self._require_stable_scheduler(scheduler)
            self._require_drained(reservation)
            durable = self._read_state(run_id)
            self._require_durable_drained(durable)
            if durable.run.state is not RunState.RUNNING:
                raise PauseCoordinatorInvalidRequestError("pause requires a running durable run")
            _require_pair_headroom(durable)
            transitioned_at = self._now(durable.run)
            first_command = _transition_command(
                durable,
                RunState.PAUSING,
                transitioned_at,
                correlation,
            )
            first_expected = _transition_command(
                durable,
                RunState.PAUSING,
                transitioned_at,
                correlation,
            )
            try:
                first_run, first_events, first_id = self._execute(
                    first_command,
                    first_expected,
                    durable.run,
                    mark_intermediate=True,
                )
            except PauseCoordinatorRejectedError:
                raise
            except PauseCoordinatorAdmissionError:
                raise
            except PauseCoordinatorOutcomeUnknownError:
                with self._lifecycle_lock:
                    self._uncertain = True
                raise
            second_state = PauseDurableState(
                first_run,
                first_events.next_sequence,
                first_events.counter_row_version,
                0,
            )
            second_command = _transition_command(
                second_state,
                RunState.PAUSED,
                transitioned_at,
                correlation,
            )
            second_expected = _transition_command(
                second_state,
                RunState.PAUSED,
                transitioned_at,
                correlation,
            )
            second_not_executed = False
            try:
                second_run, second_events, second_id = self._execute(
                    second_command,
                    second_expected,
                    first_run,
                )
            except PauseCoordinatorRejectedError, PauseCoordinatorAdmissionError:
                second_not_executed = True
                second_run = None
                second_events = None
                second_id = None
            except PauseCoordinatorOutcomeUnknownError:
                with self._lifecycle_lock:
                    self._uncertain = True
                raise
            if second_not_executed:
                raise PauseCoordinatorIncompleteError(
                    "run remains durably pausing after the second arrow failed"
                )
            assert second_run is not None
            assert second_events is not None
            assert second_id is not None
            combined = _combine_events(first_events, second_events)
            paused = PausedRun(
                second_run,
                scheduler,
                combined,
                (first_id, second_id),
                _token=_PAUSED_RUN_TOKEN,
            )
            evidence = _paused_evidence(paused)
            with self._lifecycle_lock:
                self._paused = paused
                self._paused_evidence = evidence
                self._intermediate = False
                self._uncertain = False
            report = PauseCoordinatorReport(
                PauseAction.PAUSED,
                _snapshot_run(second_run),
                _snapshot_scheduler(scheduler),
                _snapshot_event_batch(combined),
                (_snapshot_submission_id(first_id), _snapshot_submission_id(second_id)),
            )
            return paused, report
        finally:
            self._operation_lock.release()

    def resume(
        self,
        paused: PausedRun,
        *,
        correlation_id: str | None = None,
    ) -> PauseCoordinatorReport:
        """Persist PAUSED -> RESUMING -> RUNNING before reopening admission."""
        correlation = _validate_correlation_id(correlation_id)
        if type(paused) is not PausedRun:
            raise PauseCoordinatorInvalidRequestError("resume requires a paused-run proof")
        if not self._operation_lock.acquire(blocking=False):
            raise PauseCoordinatorBusyError("pause coordinator already has an active operation")
        try:
            reservation, run_id = self._active_request()
            with self._lifecycle_lock:
                if self._paused is not paused:
                    raise PauseCoordinatorInvalidRequestError("paused-run proof is not active")
                evidence = _paused_evidence(paused)
                if evidence != self._paused_evidence:
                    raise PauseCoordinatorInvalidRequestError("paused-run proof was changed")
                if self._uncertain or self._intermediate:
                    raise PauseCoordinatorOutcomeUnknownError(
                        "pause lifecycle requires durable recovery inspection"
                    )
            scheduler = _snapshot_scheduler(paused.scheduler_state)
            self._require_stable_scheduler(scheduler)
            self._require_drained(reservation)
            durable = self._read_state(run_id)
            self._require_durable_drained(durable)
            expected_paused_run = _snapshot_run(paused.run)
            if durable.run != expected_paused_run or durable.run.state is not RunState.PAUSED:
                raise PauseCoordinatorInvalidRequestError("paused-run proof is stale")
            _require_pair_headroom(durable)
            transitioned_at = self._now(durable.run)
            first_command = _transition_command(
                durable,
                RunState.RESUMING,
                transitioned_at,
                correlation,
            )
            first_expected = _transition_command(
                durable,
                RunState.RESUMING,
                transitioned_at,
                correlation,
            )
            try:
                first_run, first_events, first_id = self._execute(
                    first_command,
                    first_expected,
                    durable.run,
                    mark_intermediate=True,
                )
            except PauseCoordinatorOutcomeUnknownError:
                with self._lifecycle_lock:
                    self._uncertain = True
                raise
            second_state = PauseDurableState(
                first_run,
                first_events.next_sequence,
                first_events.counter_row_version,
                0,
            )
            second_command = _transition_command(
                second_state,
                RunState.RUNNING,
                transitioned_at,
                correlation,
            )
            second_expected = _transition_command(
                second_state,
                RunState.RUNNING,
                transitioned_at,
                correlation,
            )
            second_not_executed = False
            try:
                second_run, second_events, second_id = self._execute(
                    second_command,
                    second_expected,
                    first_run,
                )
            except PauseCoordinatorRejectedError, PauseCoordinatorAdmissionError:
                second_not_executed = True
                second_run = None
                second_events = None
                second_id = None
            except PauseCoordinatorOutcomeUnknownError:
                with self._lifecycle_lock:
                    self._uncertain = True
                raise
            if second_not_executed:
                raise PauseCoordinatorIncompleteError(
                    "run remains durably resuming after the second arrow failed"
                )
            assert second_run is not None
            assert second_events is not None
            assert second_id is not None
            combined = _combine_events(first_events, second_events)
            generation = self._generation
            if generation is None or not self._token.clear_for_coordinator(generation):
                raise PauseCoordinatorIncompleteError(
                    "run resumed but its pause signal could not be cleared"
                )
            release_failed = False
            try:
                self._lease_service.release_pause(reservation)
            except WorkLeaseError:
                release_failed = True
            if release_failed:
                raise PauseCoordinatorIncompleteError(
                    "run resumed but acquisition admission remains closed"
                )
            with self._lifecycle_lock:
                self._reservation = None
                self._run_id = None
                self._generation = None
                self._paused = None
                self._paused_evidence = None
                self._intermediate = False
                self._uncertain = False
            return PauseCoordinatorReport(
                PauseAction.RESUMED,
                _snapshot_run(second_run),
                _snapshot_scheduler(scheduler),
                _snapshot_event_batch(combined),
                (_snapshot_submission_id(first_id), _snapshot_submission_id(second_id)),
            )
        finally:
            self._operation_lock.release()

    def abort_pause(self) -> None:
        """Release a pre-transition request whose durable state is still untouched."""
        if not self._operation_lock.acquire(blocking=False):
            raise PauseCoordinatorBusyError("pause coordinator already has an active operation")
        try:
            reservation, _run_id = self._active_request()
            with self._lifecycle_lock:
                if self._paused is not None or self._intermediate or self._uncertain:
                    raise PauseCoordinatorInvalidRequestError(
                        "durable or ambiguous pause state cannot be aborted"
                    )
                generation = self._generation
            if generation is None:
                raise PauseCoordinatorProtocolError("pause signal evidence is invalid")
            if not self._token.abort_for_coordinator(generation):
                raise PauseCoordinatorInvalidRequestError(
                    "acknowledged pause generation cannot be aborted; an explicit "
                    "resume is required"
                )
            release_failed = False
            try:
                self._lease_service.release_pause(reservation)
            except WorkLeaseError:
                release_failed = True
            if release_failed:
                raise PauseCoordinatorProtocolError("pause admission gate release failed")
            with self._lifecycle_lock:
                self._reservation = None
                self._run_id = None
                self._generation = None
        finally:
            self._operation_lock.release()

    def _active_request(self) -> tuple[WorkLeasePauseReservation, RunId]:
        with self._lifecycle_lock:
            if self._reservation is None or self._run_id is None:
                raise PauseCoordinatorInvalidRequestError("pause has not been requested")
            return self._reservation, self._run_id

    @staticmethod
    def _require_stable_scheduler(state: SchedulerState) -> None:
        if state.active_node_id is not None:
            raise PauseCoordinatorInvalidRequestError(
                "pause scheduler boundary still has an active node"
            )

    @staticmethod
    def _require_durable_drained(state: PauseDurableState) -> None:
        if state.active_work_count != 0:
            raise PauseCoordinatorNotReadyError("pause durable boundary still has running work")

    def _require_drained(self, reservation: WorkLeasePauseReservation) -> None:
        failed = False
        try:
            snapshot = self._lease_service.snapshot_pause(reservation)
        except WorkLeaseError:
            failed = True
            snapshot = None
        if failed or type(snapshot) is not WorkLeaseServiceSnapshot:
            raise PauseCoordinatorProtocolError("pause lease snapshot is invalid")
        if (snapshot.active, snapshot.unknown, snapshot.in_flight) != (0, 0, 0):
            raise PauseCoordinatorNotReadyError("pause boundary still has lease ownership")

    def _read_state(self, run_id: RunId) -> PauseDurableState:
        failed = False
        try:
            value = self._reader.read(run_id)
        except Exception:
            failed = True
            value = None
        if failed:
            raise PauseCoordinatorStateReadError("pause durable frontier read failed")
        invalid = False
        try:
            clean = _snapshot_durable_state(value)
        except Exception:
            invalid = True
            clean = None
        if invalid or clean is None:
            raise PauseCoordinatorProtocolError("pause durable frontier is invalid")
        return clean

    def _now(self, run: RunRecord) -> UtcTimestamp:
        failed = False
        try:
            value = self._clock.now()
        except Exception:
            failed = True
            value = None
        if failed:
            raise PauseCoordinatorClockError("pause clock failed")
        invalid = False
        try:
            timestamp = _snapshot_timestamp(value)
        except Exception:
            invalid = True
            timestamp = None
        if invalid or timestamp is None:
            raise PauseCoordinatorClockError("pause clock returned an invalid time")
        evidence = tuple(
            item
            for item in (
                run.created_at,
                run.started_at,
                run.cancellation_requested_at,
                run.recovery_started_at,
                run.recovered_at,
            )
            if item is not None
        )
        if evidence and timestamp < max(evidence):
            raise PauseCoordinatorClockError("pause clock is behind durable run time")
        return timestamp

    def _execute(
        self,
        command: TransitionRun,
        expected_command: TransitionRun,
        previous_run: RunRecord,
        *,
        mark_intermediate: bool = False,
    ) -> tuple[RunRecord, ExecutionEventBatch, WriterSubmissionId]:
        admission_failed = False
        rejected = False
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
            rejected = True
            ticket = None
        except Exception:
            unexpected = True
            ticket = None
        except BaseException:
            self._mark_uncertain()
            raise
        if admission_failed or rejected:
            raise PauseCoordinatorAdmissionError("pause writer admission failed")
        if unexpected or ticket is None:
            raise PauseCoordinatorProtocolError("pause writer admission outcome is unknown")
        try:
            submission_id = _ticket_identity(ticket)
        except BaseException:
            self._mark_uncertain()
            raise
        definitely_not_executed = False
        unknown = False
        unexpected = False
        try:
            receipt = ticket.result(timeout_seconds=self._settings.result_timeout_seconds)
        except _DEFINITELY_NOT_EXECUTED:
            definitely_not_executed = True
            receipt = None
        except WriterResultTimeoutError, WriterCommitOutcomeUnknownError, WriterError:
            unknown = True
            receipt = None
        except Exception:
            unexpected = True
            receipt = None
        except BaseException:
            self._mark_uncertain()
            raise
        if definitely_not_executed:
            raise PauseCoordinatorRejectedError("pause transition was not committed")
        if unknown or unexpected:
            raise PauseCoordinatorOutcomeUnknownError("pause transition durable outcome is unknown")
        invalid_receipt = False
        try:
            validated = _validate_receipt(
                receipt,
                submission_id,
                expected_command,
                previous_run,
            )
        except Exception:
            invalid_receipt = True
            validated = None
        except BaseException:
            self._mark_uncertain()
            raise
        if invalid_receipt or validated is None:
            raise PauseCoordinatorProtocolError("pause writer receipt is invalid")
        if mark_intermediate:
            try:
                with self._lifecycle_lock:
                    self._intermediate = True
            except BaseException:
                self._mark_uncertain()
                raise
        return validated

    def _mark_uncertain(self) -> None:
        try:
            with self._lifecycle_lock:
                self._uncertain = True
        except BaseException:
            self._uncertain = True


def _transition_command(
    state: PauseDurableState,
    target: RunState,
    transitioned_at: UtcTimestamp,
    correlation_id: str | None,
) -> TransitionRun:
    previous = state.run.state
    event_kind = {
        RunState.PAUSING: "run_pausing",
        RunState.PAUSED: "run_paused",
        RunState.RESUMING: "run_resuming",
        RunState.RUNNING: "run_started",
    }[target]
    event = PendingExecutionEvent(
        event_kind,
        _snapshot_timestamp(transitioned_at),
        EventSubjectKind.RUN,
        _snapshot_run_id(state.run.run_id),
        correlation_id,
        PAUSE_EVENT_PAYLOAD_SCHEMA_VERSION,
        RedactedDocument.from_mapping({"from_state": previous.value, "to_state": target.value}),
    )
    return TransitionRun(
        _snapshot_run_id(state.run.run_id),
        state.run.row_version,
        target,
        _snapshot_timestamp(transitioned_at),
        None,
        EventAppendRequest(
            EventSequence(state.next_event_sequence.number),
            state.event_counter_row_version,
            event,
        ),
    )


def _ticket_identity(ticket: WriterTicket) -> WriterSubmissionId:
    failed = False
    try:
        identity = cast(object, ticket.submission_id)
        if type(identity) is not WriterSubmissionId or type(identity.number) is not int:
            failed = True
            clean = None
        else:
            clean = WriterSubmissionId(identity.number)
    except Exception:
        failed = True
        clean = None
    if failed or clean is None:
        raise PauseCoordinatorProtocolError("pause writer ticket identity is invalid")
    return clean


def _validate_receipt(
    receipt: object,
    submission_id: WriterSubmissionId,
    command: TransitionRun,
    previous_run: RunRecord,
) -> tuple[RunRecord, ExecutionEventBatch, WriterSubmissionId]:
    if type(receipt) is not WriterReceipt:
        raise PauseCoordinatorProtocolError("pause writer receipt type is invalid")
    clean_id = _snapshot_submission_id(receipt.submission_id)
    clean_run_id = _snapshot_run_id(receipt.run_id)
    if (
        clean_id != submission_id
        or receipt.command_kind is not command.kind
        or clean_run_id != command.run_id
        or type(receipt.contention_attempts) is not int
        or not 0 <= receipt.contention_attempts <= MAX_PAUSE_CONTENTION_ATTEMPTS
        or receipt.mutated is not True
        or type(receipt.result) is not TransitionRunResult
    ):
        raise PauseCoordinatorProtocolError("pause writer receipt does not match command")
    clean_run = _snapshot_run(receipt.result.run)
    clean_events = _snapshot_event_batch(receipt.result.events)
    expected_run = _expected_run(previous_run, command)
    expected_events = _expected_events(command)
    if clean_run != expected_run or clean_events != expected_events:
        raise PauseCoordinatorProtocolError("pause writer receipt evidence is inconsistent")
    return clean_run, clean_events, clean_id


def _expected_run(previous: RunRecord, command: TransitionRun) -> RunRecord:
    clean = _snapshot_run(previous)
    return RunRecord(
        clean.run_id,
        clean.pipeline_id,
        clean.pipeline_version,
        clean.runner_kind,
        clean.runner_configuration,
        command.target_state,
        clean.row_version + 1,
        clean.scenario_seed,
        clean.created_at,
        clean.started_at,
        clean.finished_at,
        clean.cancellation_requested_at,
        clean.recovery_started_at,
        clean.recovered_at,
        clean.final_reconciliation_fingerprint,
    )


def _expected_events(command: TransitionRun) -> ExecutionEventBatch:
    request = command.event
    event = request.event
    record = ExecutionEventRecord(
        command.run_id,
        request.expected_next_sequence,
        event.event_kind,
        event.occurred_at,
        event.subject_kind,
        event.subject_id,
        event.correlation_id,
        event.payload_schema_version,
        event.payload,
    )
    return ExecutionEventBatch(
        (record,),
        request.expected_next_sequence.advance(1),
        request.expected_counter_row_version + 1,
    )


def _combine_events(
    first: ExecutionEventBatch,
    second: ExecutionEventBatch,
) -> ExecutionEventBatch:
    if second.items[0].sequence != first.next_sequence:
        raise PauseCoordinatorProtocolError("pause event pairs are not contiguous")
    return ExecutionEventBatch(
        first.items + second.items,
        second.next_sequence,
        second.counter_row_version,
    )


def _require_pair_headroom(state: PauseDurableState) -> None:
    maximum = MAX_CONSISTENCY_SEQUENCE - 2
    if (
        state.run.row_version > maximum
        or state.next_event_sequence.number > maximum
        or state.event_counter_row_version > maximum
    ):
        raise PauseCoordinatorInvalidRequestError(
            "pause lifecycle frontier cannot advance by two arrows"
        )


def _snapshot_durable_state(value: object) -> PauseDurableState:
    if type(value) is not PauseDurableState:
        raise TypeError("pause durable state has an invalid type")
    return PauseDurableState(
        _snapshot_run(value.run),
        _snapshot_event_sequence(value.next_event_sequence),
        _bounded_positive(value.event_counter_row_version, "event counter row version"),
        _bounded_nonnegative(value.active_work_count, "active work count"),
    )


def _snapshot_run(value: object) -> RunRecord:
    if type(value) is not RunRecord:
        raise TypeError("pause run evidence must use RunRecord")
    return RunRecord(
        _snapshot_run_id(value.run_id),
        _snapshot_pipeline_id(value.pipeline_id),
        _snapshot_pipeline_version(value.pipeline_version),
        _exact_text(value.runner_kind, "runner kind"),
        _snapshot_document(value.runner_configuration),
        _exact_enum(value.state, RunState, "run state"),
        _bounded_positive(value.row_version, "run row version"),
        _optional_integer(value.scenario_seed, "scenario seed"),
        _snapshot_timestamp(value.created_at),
        _optional_timestamp(value.started_at),
        _optional_timestamp(value.finished_at),
        _optional_timestamp(value.cancellation_requested_at),
        _optional_timestamp(value.recovery_started_at),
        _optional_timestamp(value.recovered_at),
        _optional_fingerprint(value.final_reconciliation_fingerprint),
    )


def _snapshot_scheduler(value: object) -> SchedulerState:
    if type(value) is not SchedulerState or type(value.nodes) is not tuple:
        raise PauseCoordinatorInvalidRequestError("pause scheduler evidence is invalid")
    nodes: list[ScheduledNode] = []
    invalid = False
    try:
        for item in value.nodes:
            if type(item) is not ScheduledNode or type(item.remaining_dependency_ids) is not tuple:
                raise TypeError
            dependencies = tuple(
                _snapshot_node_id(node_id) for node_id in item.remaining_dependency_ids
            )
            nodes.append(
                ScheduledNode(
                    _snapshot_node_id(item.node_id),
                    _exact_enum(item.status, ScheduledNodeStatus, "scheduled node status"),
                    dependencies,
                )
            )
        clean = SchedulerState(
            _exact_enum(value.status, SchedulerStatus, "scheduler status"),
            tuple(nodes),
            _snapshot_plan_fingerprint(value.plan_fingerprint),
            _exact_integer(value.version, "scheduler version"),
        )
    except Exception:
        invalid = True
        clean = None
    if invalid or clean is None:
        raise PauseCoordinatorInvalidRequestError("pause scheduler evidence is invalid")
    return clean


def _snapshot_event_batch(value: object) -> ExecutionEventBatch:
    if type(value) is not ExecutionEventBatch or type(value.items) is not tuple:
        raise TypeError("pause event batch is invalid")
    return ExecutionEventBatch(
        tuple(_snapshot_event_record(item) for item in value.items),
        _snapshot_event_sequence(value.next_sequence),
        _bounded_positive(value.counter_row_version, "event counter row version"),
    )


def _snapshot_event_record(value: object) -> ExecutionEventRecord:
    if type(value) is not ExecutionEventRecord:
        raise TypeError("pause event record is invalid")
    subject_kind = _exact_enum(value.subject_kind, EventSubjectKind, "event subject kind")
    if subject_kind is not EventSubjectKind.RUN:
        raise TypeError("pause event subject must identify a run")
    return ExecutionEventRecord(
        _snapshot_run_id(value.run_id),
        _snapshot_event_sequence(value.sequence),
        _exact_text(value.event_kind, "event kind"),
        _snapshot_timestamp(value.occurred_at),
        subject_kind,
        _snapshot_run_id(value.subject_id),
        _validate_correlation_id(value.correlation_id),
        _bounded_positive(value.payload_schema_version, "event payload schema version"),
        _snapshot_redacted_document(value.payload),
    )


def _snapshot_document(value: object) -> ConfigurationDocument:
    if type(value) is not ConfigurationDocument or type(value.items) is not tuple:
        raise TypeError("configuration document evidence is invalid")
    return ConfigurationDocument(tuple(_snapshot_document_pair(item) for item in value.items))


def _snapshot_document_pair(value: object) -> tuple[str, DocumentValue]:
    if type(value) is not tuple:
        raise TypeError("configuration document entry is invalid")
    pair = cast(tuple[object, ...], value)
    if len(pair) != 2:
        raise TypeError("configuration document entry is invalid")
    return _exact_text(pair[0], "configuration key"), _snapshot_document_value(pair[1])


def _snapshot_document_value(value: object) -> DocumentValue:
    if value is None or type(value) in (bool, int, str):
        return cast(DocumentValue, value)
    if type(value) is DocumentArray and type(value.values) is tuple:
        return DocumentArray(tuple(_snapshot_document_value(item) for item in value.values))
    if type(value) is NestedDocumentObject and type(value.items) is tuple:
        return NestedDocumentObject(tuple(_snapshot_document_pair(item) for item in value.items))
    raise TypeError("configuration document value is invalid")


def _snapshot_redacted_document(value: object) -> RedactedDocument:
    if type(value) is not RedactedDocument:
        raise TypeError("redacted document evidence is invalid")
    return RedactedDocument(_snapshot_document(value.document))


def _paused_evidence(value: PausedRun) -> object:
    try:
        return (
            _snapshot_run(value.run),
            _snapshot_scheduler(value.scheduler_state),
            _snapshot_event_batch(value.events),
            tuple(_snapshot_submission_id(item) for item in value.submission_ids),
        )
    except Exception:
        return None


def _snapshot_run_id(value: object) -> RunId:
    if type(value) is not RunId or type(value.value) is not str:
        raise TypeError("run identity evidence is invalid")
    return RunId(value.value)


def _snapshot_node_id(value: object) -> NodeId:
    if type(value) is not NodeId or type(value.value) is not str:
        raise TypeError("node identity evidence is invalid")
    return NodeId(value.value)


def _snapshot_pipeline_id(value: object) -> PipelineId:
    if type(value) is not PipelineId or type(value.value) is not str:
        raise TypeError("pipeline identity evidence is invalid")
    return PipelineId(value.value)


def _snapshot_pipeline_version(value: object) -> PipelineVersion:
    if type(value) is not PipelineVersion or type(value.number) is not int:
        raise TypeError("pipeline version evidence is invalid")
    return PipelineVersion(value.number)


def _snapshot_plan_fingerprint(value: object) -> PlanFingerprint:
    if type(value) is not PlanFingerprint or type(value.value) is not str:
        raise TypeError("plan fingerprint evidence is invalid")
    return PlanFingerprint(value.value)


def _snapshot_timestamp(value: object) -> UtcTimestamp:
    if (
        type(value) is not UtcTimestamp
        or type(value.value) is not datetime
        or value.value.tzinfo is not UTC
    ):
        raise TypeError("timestamp evidence is invalid")
    return UtcTimestamp(value.value)


def _optional_timestamp(value: object) -> UtcTimestamp | None:
    return None if value is None else _snapshot_timestamp(value)


def _optional_fingerprint(value: object) -> StateFingerprint | None:
    if value is None:
        return None
    if type(value) is not StateFingerprint or type(value.value) is not str:
        raise TypeError("state fingerprint evidence is invalid")
    return StateFingerprint(value.value)


def _snapshot_event_sequence(value: object) -> EventSequence:
    if type(value) is not EventSequence or type(value.number) is not int:
        raise TypeError("event sequence evidence is invalid")
    return EventSequence(value.number)


def _snapshot_submission_id(value: object) -> WriterSubmissionId:
    if type(value) is not WriterSubmissionId or type(value.number) is not int:
        raise TypeError("writer submission evidence is invalid")
    return WriterSubmissionId(value.number)


def _exact_enum[T: StrEnum](value: object, expected: type[T], subject: str) -> T:
    if type(value) is not expected:
        raise TypeError(f"{subject} is invalid")
    return cast(T, value)


def _exact_text(value: object, subject: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{subject} must be text")
    return value


def _exact_integer(value: object, subject: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    return value


def _bounded_positive(value: object, subject: str) -> int:
    integer = _exact_integer(value, subject)
    if not 1 <= integer <= MAX_CONSISTENCY_SEQUENCE:
        raise ValueError(f"{subject} is outside the supported range")
    return integer


def _bounded_nonnegative(value: object, subject: str) -> int:
    integer = _exact_integer(value, subject)
    if not 0 <= integer <= MAX_CONSISTENCY_SEQUENCE:
        raise ValueError(f"{subject} is outside the supported range")
    return integer


def _optional_integer(value: object, subject: str) -> int | None:
    return None if value is None else _exact_integer(value, subject)


def _validate_correlation_id(value: object) -> str | None:
    if value is None:
        return None
    if (
        type(value) is not str
        or not 1 <= len(value) <= MAX_PAUSE_CORRELATION_ID_LENGTH
        or _PORTABLE_IDENTITY.fullmatch(value) is None
    ):
        raise PauseCoordinatorInvalidRequestError("pause correlation identifier is invalid")
    return value


def _validate_timeout(value: object, subject: str) -> None:
    if type(value) is not float:
        raise TypeError(f"{subject} must be a float")
    if not 0 <= value <= MAX_PAUSE_TIMEOUT_SECONDS:
        raise ValueError(f"{subject} is outside the supported range")


def _suppress_base_exception(operation: Callable[[], object]) -> None:
    try:
        operation()
    except BaseException:
        return


__all__ = [
    "MAX_PAUSE_CONTENTION_ATTEMPTS",
    "MAX_PAUSE_CORRELATION_ID_LENGTH",
    "MAX_PAUSE_TIMEOUT_SECONDS",
    "PAUSE_EVENT_PAYLOAD_SCHEMA_VERSION",
    "PauseAcknowledgement",
    "PauseAction",
    "PauseClock",
    "PauseCoordinator",
    "PauseCoordinatorAdmissionError",
    "PauseCoordinatorBusyError",
    "PauseCoordinatorClockError",
    "PauseCoordinatorError",
    "PauseCoordinatorIncompleteError",
    "PauseCoordinatorInvalidRequestError",
    "PauseCoordinatorNotReadyError",
    "PauseCoordinatorOutcomeUnknownError",
    "PauseCoordinatorProtocolError",
    "PauseCoordinatorRejectedError",
    "PauseCoordinatorReport",
    "PauseCoordinatorSettings",
    "PauseCoordinatorStateReadError",
    "PauseDurableState",
    "PauseStateReader",
    "PauseToken",
    "PauseWriter",
    "PausedRun",
]
