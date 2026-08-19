"""Prompt bounded cancellation for sequential runs and their owned resources."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from threading import Lock
from typing import Protocol, cast, runtime_checkable

from paritygrid.application.execution.attempt_events import (
    AttemptCancelled,
    AttemptEventContext,
    RedactedAttemptDetail,
)
from paritygrid.application.execution.leasing import (
    WorkLease,
    WorkLeaseError,
    WorkLeasePauseReservation,
    WorkLeaseService,
    WorkLeaseServiceSnapshot,
)
from paritygrid.application.execution.result_sink import (
    ResultMetrics,
    ResultSinkOutcome,
    ResultSubmission,
    UnsuccessfulWorkResult,
    submit_work_result,
)
from paritygrid.application.execution.runner import CancellationToken
from paritygrid.application.planner import PlannerRunnerKind
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
from paritygrid.application.ports.result_sink import ResultSink
from paritygrid.application.ports.run_aggregates import WorkMetricDelta
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
    PipelineId,
    PipelineVersion,
    RunId,
    StateFingerprint,
    UtcTimestamp,
)

CANCELLATION_EVENT_PAYLOAD_SCHEMA_VERSION = 1
MAX_CANCELLATION_CORRELATION_ID_LENGTH = 96
MAX_CANCELLATION_TIMEOUT_SECONDS = 86_400.0
MAX_CANCELLATION_CONTENTION_ATTEMPTS = 9
MAX_CANCELLATION_RESOURCES = 64
MAX_CANCELLATION_DETAIL_LENGTH = 4_096

_PORTABLE_IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]*", flags=re.ASCII)
_DEFINITELY_NOT_EXECUTED = (
    WriterDefinitelyNotExecutedError,
    ExecutionRepositoryError,
    ConsistencyRepositoryError,
)
# Work items that never left PENDING or RETRY_WAIT cannot reach CANCELLED
# through the accepted lifecycle: only an owned RUNNING claim can complete as
# cancelled, and no claim is admitted once the run leaves RUNNING. Those rows
# stay untouched as durable evidence of work that was never attempted.
_PAUSE_BOUND_STATES = frozenset({RunState.PAUSING, RunState.PAUSED, RunState.RESUMING})


class CancellationCoordinatorError(RuntimeError):
    """Base failure for bounded run cancellation."""


class CancellationCoordinatorBusyError(CancellationCoordinatorError):
    """An overlapping control operation or incompatible request was rejected."""


class CancellationCoordinatorInvalidRequestError(CancellationCoordinatorError):
    """Cancellation evidence or lifecycle state is not admissible."""


class CancellationCoordinatorNotReadyError(CancellationCoordinatorError):
    """Active work or lease ownership still has to reach a stable boundary."""


class CancellationCoordinatorClockError(CancellationCoordinatorError):
    """The injected clock did not produce one exact safe timestamp."""


class CancellationCoordinatorStateReadError(CancellationCoordinatorError):
    """A fresh durable run/event frontier could not be read safely."""


class CancellationCoordinatorAdmissionError(CancellationCoordinatorError):
    """Writer admission failed before a durable command identity was allocated."""


class CancellationCoordinatorRejectedError(CancellationCoordinatorError):
    """The transition was proven not to have executed."""


class CancellationCoordinatorIncompleteError(CancellationCoordinatorError):
    """An arrow committed but the lifecycle pair did not complete."""


class CancellationCoordinatorOutcomeUnknownError(CancellationCoordinatorError):
    """An admitted transition has no proven durable outcome."""


class CancellationCoordinatorProtocolError(CancellationCoordinatorOutcomeUnknownError):
    """Borrowed collaborator evidence was malformed or inconsistent."""


class CancellationCleanupError(CancellationCoordinatorError):
    """The run cancelled durably but an owned resource failed bounded cleanup."""


class CancellationAction(StrEnum):
    """Closed successful cancellation outcomes."""

    CANCELLED = "cancelled"
    CANCELLED_BEFORE_START = "cancelled_before_start"
    ALREADY_CANCELLED = "already_cancelled"


@dataclass(frozen=True, slots=True)
class CancellationCoordinatorSettings:
    """Bounded admission, result, and per-resource cleanup waits."""

    admission_timeout_seconds: float = 5.0
    result_timeout_seconds: float = 60.0
    cleanup_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        _validate_timeout(self.admission_timeout_seconds, "cancellation admission timeout")
        _validate_timeout(self.result_timeout_seconds, "cancellation result timeout")
        _validate_timeout(self.cleanup_timeout_seconds, "cancellation cleanup timeout")


@dataclass(frozen=True, slots=True, repr=False)
class CancellationDurableState:
    """One transactionally read run and event allocation frontier."""

    run: RunRecord
    next_event_sequence: EventSequence
    event_counter_row_version: int
    active_work_count: int = 0

    def __post_init__(self) -> None:
        if type(self.run) is not RunRecord:
            raise TypeError("cancellation durable state run must use RunRecord")
        if type(self.next_event_sequence) is not EventSequence:
            raise TypeError("cancellation event frontier must use EventSequence")
        if type(self.event_counter_row_version) is not int:
            raise TypeError("cancellation event counter row version must be an integer")
        if not 1 <= self.event_counter_row_version <= MAX_CONSISTENCY_SEQUENCE:
            raise ValueError("cancellation event counter row version is outside the range")
        if type(self.active_work_count) is not int:
            raise TypeError("cancellation active work count must be an integer")
        if not 0 <= self.active_work_count <= MAX_CONSISTENCY_SEQUENCE:
            raise ValueError("cancellation active work count is outside the range")

    def __repr__(self) -> str:
        return (
            "CancellationDurableState("
            f"run_id={self.run.run_id!r}, state={self.run.state.value!r}, "
            f"run_row_version={self.run.row_version!r}, "
            f"next_event_sequence={self.next_event_sequence.number!r}, "
            f"event_counter_row_version={self.event_counter_row_version!r}, "
            f"active_work_count={self.active_work_count!r})"
        )


@runtime_checkable
class CancellationClock(Protocol):
    """Injected exact UTC clock used before durable transition admission."""

    def now(self) -> UtcTimestamp:
        """Return the current exact UTC timestamp."""
        ...


@runtime_checkable
class CancellationStateReader(Protocol):
    """Borrowed short-transaction reader for a run and its event frontier."""

    def read(self, run_id: RunId, /) -> CancellationDurableState:
        """Read one coherent durable cancellation frontier."""
        ...


@runtime_checkable
class CancellationWriter(Protocol):
    """Borrowed transactional-writer surface without lifecycle ownership."""

    def submit(
        self,
        command: WriterCommand,
        *,
        timeout_seconds: float,
    ) -> WriterTicket:
        """Submit one exact run transition."""
        ...


@runtime_checkable
class CancellationResource(Protocol):
    """Owned resource whose release must honor one explicit time bound."""

    def close(self, *, timeout_seconds: float) -> None:
        """Release the resource within the given bound or fail."""
        ...


@dataclass(frozen=True, slots=True, repr=False)
class CancellationReport:
    """Proven terminal cancellation evidence and its bounded cleanup result."""

    action: CancellationAction
    run: RunRecord
    events: ExecutionEventBatch
    submission_ids: tuple[WriterSubmissionId, ...]
    cleanup_closed: int

    def __post_init__(self) -> None:
        _exact_enum(self.action, CancellationAction, "cancellation action")
        _snapshot_run(self.run)
        _snapshot_event_batch(self.events)
        submissions = self.submission_ids
        if type(submissions) is not tuple or any(
            type(item) is not WriterSubmissionId for item in submissions
        ):
            raise TypeError("cancellation submissions must use WriterSubmissionId")
        if (
            type(self.cleanup_closed) is not int
            or not 0 <= self.cleanup_closed <= MAX_CANCELLATION_RESOURCES
        ):
            raise ValueError("cancellation cleanup count is outside the supported range")

    def __repr__(self) -> str:
        return (
            "CancellationReport("
            f"action={self.action.value!r}, run_id={self.run.run_id!r}, "
            f"run_row_version={self.run.row_version!r}, "
            f"events={len(self.events.items)}, submissions={len(self.submission_ids)}, "
            f"cleanup_closed={self.cleanup_closed!r})"
        )


class CancellationCoordinator:
    """Converge one run on a terminal CANCELLED state with bounded cleanup.

    The accepted work lifecycle deliberately cancels only an owned RUNNING
    claim (through the result sink); PENDING and RETRY_WAIT items of a
    cancelled run can never be claimed again because claims require a RUNNING
    run, so they remain untouched evidence rather than being rewritten.
    """

    __slots__ = (
        "_clock",
        "_completed",
        "_intermediate",
        "_lease_service",
        "_lifecycle_lock",
        "_operation_lock",
        "_reader",
        "_reservation",
        "_resources",
        "_run_id",
        "_settings",
        "_sink",
        "_token",
        "_uncertain",
        "_writer",
    )

    def __init__(
        self,
        writer: CancellationWriter,
        reader: CancellationStateReader,
        lease_service: WorkLeaseService,
        sink: ResultSink,
        clock: CancellationClock,
        *,
        settings: CancellationCoordinatorSettings | None = None,
    ) -> None:
        writer_value = cast(object, writer)
        reader_value = cast(object, reader)
        clock_value = cast(object, clock)
        sink_value = cast(object, sink)
        if not isinstance(writer_value, CancellationWriter):
            raise TypeError("cancellation writer must provide transactional submit")
        if not isinstance(reader_value, CancellationStateReader):
            raise TypeError("cancellation state reader must provide a coherent read")
        if type(lease_service) is not WorkLeaseService:
            raise TypeError("cancellation lease service must use WorkLeaseService")
        if not isinstance(sink_value, ResultSink):
            raise TypeError("cancellation sink must implement ResultSink")
        if not isinstance(clock_value, CancellationClock):
            raise TypeError("cancellation clock must provide exact UTC time")
        selected_settings = CancellationCoordinatorSettings() if settings is None else settings
        if type(selected_settings) is not CancellationCoordinatorSettings:
            raise TypeError("cancellation settings must use CancellationCoordinatorSettings")
        self._writer = writer_value
        self._reader = reader_value
        self._lease_service = lease_service
        self._sink = sink_value
        self._clock = clock_value
        self._settings = selected_settings
        self._token = CancellationToken()
        self._lifecycle_lock = Lock()
        self._operation_lock = Lock()
        self._reservation: WorkLeasePauseReservation | None = None
        self._run_id: RunId | None = None
        self._resources: tuple[CancellationResource, ...] = ()
        self._completed = False
        self._intermediate = False
        self._uncertain = False

    @property
    def token(self) -> CancellationToken:
        """Return the cancellation signal shared with the sequential runner."""
        return self._token

    def register(self, resource: CancellationResource) -> None:
        """Register one owned resource for bounded release after cancellation."""
        resource_value = cast(object, resource)
        if not isinstance(resource_value, CancellationResource):
            raise CancellationCoordinatorInvalidRequestError(
                "cancellation resources must provide bounded close"
            )
        with self._lifecycle_lock:
            if self._completed:
                raise CancellationCoordinatorInvalidRequestError(
                    "cancellation already completed its cleanup"
                )
            if len(self._resources) >= MAX_CANCELLATION_RESOURCES:
                raise CancellationCoordinatorInvalidRequestError(
                    "cancellation resource limit reached"
                )
            self._resources = (*self._resources, resource_value)

    def request_cancellation(self, run_id: RunId) -> None:
        """Close run-scoped acquisition admission before signalling work."""
        clean_run_id = _snapshot_run_id(run_id)
        if not self._operation_lock.acquire(blocking=False):
            raise CancellationCoordinatorBusyError(
                "cancellation coordinator already has an active operation"
            )
        try:
            with self._lifecycle_lock:
                if self._completed:
                    raise CancellationCoordinatorInvalidRequestError(
                        "completed cancellation coordinator cannot be reused"
                    )
                if self._reservation is not None:
                    if self._run_id == clean_run_id:
                        return
                    raise CancellationCoordinatorBusyError(
                        "cancellation coordinator already owns a request"
                    )
                reservation_failed = False
                try:
                    reservation = self._lease_service.reserve_pause(clean_run_id)
                except WorkLeaseError:
                    reservation_failed = True
                    reservation = None
                if reservation_failed or reservation is None:
                    raise CancellationCoordinatorBusyError(
                        "cancellation admission gate could not be installed"
                    )
                self._token.request()
                self._reservation = reservation
                self._run_id = clean_run_id
        finally:
            self._operation_lock.release()

    def cancel_work(
        self,
        lease: WorkLease,
        *,
        finished_at: UtcTimestamp,
        detail: str | None = None,
    ) -> ResultSinkOutcome:
        """Commit one owned RUNNING claim as durably cancelled without retry."""
        if type(lease) is not WorkLease:
            raise CancellationCoordinatorInvalidRequestError(
                "work cancellation requires a service-issued lease"
            )
        timestamp = _snapshot_timestamp(finished_at)
        if detail is not None:
            _validate_detail(detail)
        if not self._operation_lock.acquire(blocking=False):
            raise CancellationCoordinatorBusyError(
                "cancellation coordinator already has an active operation"
            )
        try:
            run_id = self._active_request()[1]
            with self._lifecycle_lock:
                if self._completed:
                    raise CancellationCoordinatorInvalidRequestError(
                        "cancellation already completed"
                    )
                if self._uncertain or self._intermediate:
                    raise CancellationCoordinatorOutcomeUnknownError(
                        "cancellation lifecycle requires durable recovery inspection"
                    )
            claim = lease.claim
            if lease.run.run_id != run_id:
                raise CancellationCoordinatorInvalidRequestError(
                    "work cancellation lease belongs to another run"
                )
            runner_kind = _runner_kind(claim.runner_kind)
            context = AttemptEventContext(
                lease.run.run_id,
                lease.node.node_id,
                claim.work_item_id,
                claim.attempt_number,
                _snapshot_timestamp(claim.started_at),
                runner_kind,
                claim.worker_identity,
            )
            terminal: AttemptCancelled = AttemptCancelled(
                context,
                timestamp,
                None if detail is None else _redacted_detail(detail),
            )
            result = UnsuccessfulWorkResult(
                terminal,
                None,
                ResultMetrics(0, 0, WorkMetricDelta(0, 0, 0, 0, 0)),
            )
            submission = ResultSubmission(lease, result)
            return submit_work_result(
                self._sink,
                submission,
                lease_service=self._lease_service,
            )
        finally:
            self._operation_lock.release()

    def cancel(self, *, correlation_id: str | None = None) -> CancellationReport:
        """Persist the exact accepted cancellation arrows and clean up."""
        correlation = _validate_correlation_id(correlation_id)
        if not self._operation_lock.acquire(blocking=False):
            raise CancellationCoordinatorBusyError(
                "cancellation coordinator already has an active operation"
            )
        try:
            reservation, run_id = self._active_request()
            with self._lifecycle_lock:
                completed = self._completed
                if self._uncertain or self._intermediate:
                    raise CancellationCoordinatorOutcomeUnknownError(
                        "cancellation lifecycle requires durable recovery inspection"
                    )
            durable = self._read_state(run_id)
            if completed or durable.run.state is RunState.CANCELLED:
                return self._finish_already_cancelled(reservation, durable)
            if durable.run.state in _PAUSE_BOUND_STATES:
                raise CancellationCoordinatorInvalidRequestError(
                    "run must leave its pause lifecycle before cancellation"
                )
            if durable.run.state.is_terminal:
                raise CancellationCoordinatorInvalidRequestError("terminal run cannot be cancelled")
            self._require_drained(reservation)
            self._require_durable_drained(durable)
            if durable.run.state is RunState.QUEUED:
                return self._cancel_from_queued(durable, correlation, reservation)
            assert durable.run.state in {RunState.RUNNING, RunState.CANCELLING}
            return self._cancel_from_active(durable, correlation, reservation)
        finally:
            self._operation_lock.release()

    def _cancel_from_queued(
        self,
        durable: CancellationDurableState,
        correlation: str | None,
        reservation: WorkLeasePauseReservation,
    ) -> CancellationReport:
        _require_headroom(durable, 1)
        transitioned_at = self._now(durable.run)
        run, events, submission_id = self._execute_arrow(
            durable,
            RunState.CANCELLED,
            transitioned_at,
            correlation,
        )
        return self._finish_terminal(
            reservation,
            CancellationAction.CANCELLED_BEFORE_START,
            run,
            events,
            (submission_id,),
        )

    def _cancel_from_active(
        self,
        durable: CancellationDurableState,
        correlation: str | None,
        reservation: WorkLeasePauseReservation,
    ) -> CancellationReport:
        if durable.run.state is RunState.CANCELLING:
            _require_headroom(durable, 1)
            transitioned_at = self._now(durable.run)
            run, events, submission_id = self._execute_arrow(
                durable,
                RunState.CANCELLED,
                transitioned_at,
                correlation,
            )
            return self._finish_terminal(
                reservation,
                CancellationAction.CANCELLED,
                run,
                events,
                (submission_id,),
            )
        _require_headroom(durable, 2)
        transitioned_at = self._now(durable.run)
        first_run, first_events, first_id = self._execute_arrow(
            durable,
            RunState.CANCELLING,
            transitioned_at,
            correlation,
            mark_intermediate=True,
        )
        second_state = CancellationDurableState(
            first_run,
            first_events.next_sequence,
            first_events.counter_row_version,
            0,
        )
        second_not_executed = False
        try:
            second_run, second_events, second_id = self._execute_arrow(
                second_state,
                RunState.CANCELLED,
                transitioned_at,
                correlation,
            )
        except CancellationCoordinatorRejectedError, CancellationCoordinatorAdmissionError:
            second_not_executed = True
            second_run = None
            second_events = None
            second_id = None
        if second_not_executed:
            raise CancellationCoordinatorIncompleteError(
                "run remains durably cancelling after the second arrow failed"
            )
        assert second_run is not None
        assert second_events is not None
        assert second_id is not None
        combined = _combine_events(first_events, second_events)
        return self._finish_terminal(
            reservation,
            CancellationAction.CANCELLED,
            second_run,
            combined,
            (first_id, second_id),
        )

    def _finish_already_cancelled(
        self,
        reservation: WorkLeasePauseReservation,
        durable: CancellationDurableState,
    ) -> CancellationReport:
        empty_events = ExecutionEventBatch(
            (),
            durable.next_event_sequence,
            durable.event_counter_row_version,
        )
        self._release_request(reservation)
        cleanup_closed = self._run_cleanup()
        return CancellationReport(
            CancellationAction.ALREADY_CANCELLED,
            _snapshot_run(durable.run),
            empty_events,
            (),
            cleanup_closed,
        )

    def _finish_terminal(
        self,
        reservation: WorkLeasePauseReservation,
        action: CancellationAction,
        run: RunRecord,
        events: ExecutionEventBatch,
        submission_ids: tuple[WriterSubmissionId, ...],
    ) -> CancellationReport:
        with self._lifecycle_lock:
            self._completed = True
            self._intermediate = False
        self._release_request(reservation)
        cleanup_closed = self._run_cleanup()
        return CancellationReport(
            action,
            _snapshot_run(run),
            _snapshot_event_batch(events),
            tuple(_snapshot_submission_id(item) for item in submission_ids),
            cleanup_closed,
        )

    def _release_request(self, reservation: WorkLeasePauseReservation) -> None:
        release_failed = False
        try:
            self._lease_service.release_pause(reservation)
        except WorkLeaseError:
            release_failed = True
        if release_failed:
            raise CancellationCoordinatorIncompleteError(
                "run cancelled but acquisition admission remains closed"
            )
        with self._lifecycle_lock:
            self._reservation = None
            self._run_id = None
            self._intermediate = False

    def _run_cleanup(self) -> int:
        with self._lifecycle_lock:
            resources = self._resources
            self._resources = ()
        closed = 0
        first_failure: Exception | None = None
        for resource in resources:
            # Every registered resource gets its bounded close attempt even
            # after an earlier failure, so one bad resource never leaks the rest.
            try:
                resource.close(timeout_seconds=self._settings.cleanup_timeout_seconds)
                closed += 1
            except Exception as error:
                if first_failure is None:
                    first_failure = error
        if first_failure is not None:
            raise CancellationCleanupError(
                "run cancelled but an owned resource failed bounded cleanup"
            ) from first_failure
        return closed

    def _active_request(self) -> tuple[WorkLeasePauseReservation, RunId]:
        with self._lifecycle_lock:
            if self._reservation is None or self._run_id is None:
                raise CancellationCoordinatorInvalidRequestError(
                    "cancellation has not been requested"
                )
            return self._reservation, self._run_id

    def _require_drained(self, reservation: WorkLeasePauseReservation) -> None:
        failed = False
        try:
            snapshot = self._lease_service.snapshot_pause(reservation)
        except WorkLeaseError:
            failed = True
            snapshot = None
        if failed or type(snapshot) is not WorkLeaseServiceSnapshot:
            raise CancellationCoordinatorProtocolError("cancellation lease snapshot is invalid")
        if (snapshot.active, snapshot.unknown, snapshot.in_flight) != (0, 0, 0):
            raise CancellationCoordinatorNotReadyError(
                "cancellation boundary still has lease ownership"
            )

    @staticmethod
    def _require_durable_drained(state: CancellationDurableState) -> None:
        if state.active_work_count != 0:
            raise CancellationCoordinatorNotReadyError(
                "cancellation durable boundary still has running work"
            )

    def _read_state(self, run_id: RunId) -> CancellationDurableState:
        failed = False
        try:
            value = self._reader.read(run_id)
        except Exception:
            failed = True
            value = None
        if failed:
            raise CancellationCoordinatorStateReadError("cancellation durable frontier read failed")
        invalid = False
        try:
            clean = _snapshot_durable_state(value)
        except Exception:
            invalid = True
            clean = None
        if invalid or clean is None:
            raise CancellationCoordinatorProtocolError("cancellation durable frontier is invalid")
        return clean

    def _now(self, run: RunRecord) -> UtcTimestamp:
        failed = False
        try:
            value = self._clock.now()
        except Exception:
            failed = True
            value = None
        if failed:
            raise CancellationCoordinatorClockError("cancellation clock failed")
        invalid = False
        try:
            timestamp = _snapshot_timestamp(value)
        except Exception:
            invalid = True
            timestamp = None
        if invalid or timestamp is None:
            raise CancellationCoordinatorClockError("cancellation clock returned an invalid time")
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
            raise CancellationCoordinatorClockError("cancellation clock is behind durable run time")
        return timestamp

    def _execute_arrow(
        self,
        state: CancellationDurableState,
        target: RunState,
        transitioned_at: UtcTimestamp,
        correlation: str | None,
        *,
        mark_intermediate: bool = False,
    ) -> tuple[RunRecord, ExecutionEventBatch, WriterSubmissionId]:
        command = _transition_command(state, target, transitioned_at, correlation)
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
            raise CancellationCoordinatorAdmissionError("cancellation writer admission failed")
        if unexpected or ticket is None:
            self._mark_uncertain()
            raise CancellationCoordinatorProtocolError(
                "cancellation writer admission outcome is unknown"
            )
        try:
            submission_id = _ticket_identity(ticket)
        except BaseException:
            self._mark_uncertain()
            raise
        definitely_not_executed = False
        unknown = False
        try:
            receipt = ticket.result(timeout_seconds=self._settings.result_timeout_seconds)
        except _DEFINITELY_NOT_EXECUTED:
            definitely_not_executed = True
            receipt = None
        except WriterResultTimeoutError, WriterCommitOutcomeUnknownError, WriterError:
            unknown = True
            receipt = None
        except Exception:
            unknown = True
            receipt = None
        except BaseException:
            self._mark_uncertain()
            raise
        if definitely_not_executed:
            raise CancellationCoordinatorRejectedError("cancellation transition was not committed")
        if unknown:
            self._mark_uncertain()
            raise CancellationCoordinatorOutcomeUnknownError(
                "cancellation transition durable outcome is unknown"
            )
        invalid_receipt = False
        try:
            validated = _validate_receipt(receipt, submission_id, command, state.run)
        except Exception:
            invalid_receipt = True
            validated = None
        except BaseException:
            self._mark_uncertain()
            raise
        if invalid_receipt or validated is None:
            self._mark_uncertain()
            raise CancellationCoordinatorProtocolError("cancellation writer receipt is invalid")
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
    state: CancellationDurableState,
    target: RunState,
    transitioned_at: UtcTimestamp,
    correlation_id: str | None,
) -> TransitionRun:
    previous = state.run.state
    event_kind = {
        RunState.CANCELLING: "run_cancelling",
        RunState.CANCELLED: "run_cancelled",
    }[target]
    event = PendingExecutionEvent(
        event_kind,
        _snapshot_timestamp(transitioned_at),
        EventSubjectKind.RUN,
        _snapshot_run_id(state.run.run_id),
        correlation_id,
        CANCELLATION_EVENT_PAYLOAD_SCHEMA_VERSION,
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
        raise CancellationCoordinatorProtocolError("cancellation writer ticket identity is invalid")
    return clean


def _validate_receipt(
    receipt: object,
    submission_id: WriterSubmissionId,
    command: TransitionRun,
    previous_run: RunRecord,
) -> tuple[RunRecord, ExecutionEventBatch, WriterSubmissionId]:
    if type(receipt) is not WriterReceipt:
        raise CancellationCoordinatorProtocolError("cancellation writer receipt type is invalid")
    clean_id = _snapshot_submission_id(receipt.submission_id)
    clean_run_id = _snapshot_run_id(receipt.run_id)
    if (
        clean_id != submission_id
        or receipt.command_kind is not command.kind
        or clean_run_id != command.run_id
        or type(receipt.contention_attempts) is not int
        or not 0 <= receipt.contention_attempts <= MAX_CANCELLATION_CONTENTION_ATTEMPTS
        or receipt.mutated is not True
        or type(receipt.result) is not TransitionRunResult
    ):
        raise CancellationCoordinatorProtocolError(
            "cancellation writer receipt does not match command"
        )
    clean_run = _snapshot_run(receipt.result.run)
    clean_events = _snapshot_event_batch(receipt.result.events)
    expected_run = _expected_run(previous_run, command)
    expected_events = _expected_events(command)
    if clean_run != expected_run or clean_events != expected_events:
        raise CancellationCoordinatorProtocolError(
            "cancellation writer receipt evidence is inconsistent"
        )
    return clean_run, clean_events, clean_id


def _expected_run(previous: RunRecord, command: TransitionRun) -> RunRecord:
    clean = _snapshot_run(previous)
    target = command.target_state
    timestamp = command.transitioned_at
    cancellation_requested_at = clean.cancellation_requested_at
    finished_at = clean.finished_at
    if target is RunState.CANCELLING or (
        clean.state is RunState.QUEUED and target is RunState.CANCELLED
    ):
        cancellation_requested_at = timestamp
    if target.is_terminal:
        finished_at = timestamp
    return RunRecord(
        clean.run_id,
        clean.pipeline_id,
        clean.pipeline_version,
        clean.runner_kind,
        clean.runner_configuration,
        target,
        clean.row_version + 1,
        clean.scenario_seed,
        clean.created_at,
        clean.started_at,
        finished_at,
        cancellation_requested_at,
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
        raise CancellationCoordinatorProtocolError("cancellation event pairs are not contiguous")
    return ExecutionEventBatch(
        first.items + second.items,
        second.next_sequence,
        second.counter_row_version,
    )


def _require_headroom(state: CancellationDurableState, arrows: int) -> None:
    maximum = MAX_CONSISTENCY_SEQUENCE - arrows
    if (
        state.run.row_version > maximum
        or state.next_event_sequence.number > maximum
        or state.event_counter_row_version > maximum
    ):
        raise CancellationCoordinatorInvalidRequestError(
            "cancellation lifecycle frontier cannot advance its arrows"
        )


def _snapshot_durable_state(value: object) -> CancellationDurableState:
    if type(value) is not CancellationDurableState:
        raise TypeError("cancellation durable state has an invalid type")
    return CancellationDurableState(
        _snapshot_run(value.run),
        _snapshot_event_sequence(value.next_event_sequence),
        _bounded_positive(value.event_counter_row_version, "event counter row version"),
        _bounded_nonnegative(value.active_work_count, "active work count"),
    )


def _snapshot_run(value: object) -> RunRecord:
    if type(value) is not RunRecord:
        raise TypeError("cancellation run evidence must use RunRecord")
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


def _snapshot_event_batch(value: object) -> ExecutionEventBatch:
    if type(value) is not ExecutionEventBatch or type(value.items) is not tuple:
        raise TypeError("cancellation event batch is invalid")
    return ExecutionEventBatch(
        tuple(_snapshot_event_record(item) for item in value.items),
        _snapshot_event_sequence(value.next_sequence),
        _bounded_positive(value.counter_row_version, "event counter row version"),
    )


def _snapshot_event_record(value: object) -> ExecutionEventRecord:
    if type(value) is not ExecutionEventRecord:
        raise TypeError("cancellation event record is invalid")
    subject_kind = _exact_enum(value.subject_kind, EventSubjectKind, "event subject kind")
    if subject_kind is not EventSubjectKind.RUN:
        raise TypeError("cancellation event subject must identify a run")
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


def _snapshot_run_id(value: object) -> RunId:
    if type(value) is not RunId or type(value.value) is not str:
        raise TypeError("run identity evidence is invalid")
    return RunId(value.value)


def _snapshot_pipeline_id(value: object) -> PipelineId:
    if type(value) is not PipelineId or type(value.value) is not str:
        raise TypeError("pipeline identity evidence is invalid")
    return PipelineId(value.value)


def _snapshot_pipeline_version(value: object) -> PipelineVersion:
    if type(value) is not PipelineVersion or type(value.number) is not int:
        raise TypeError("pipeline version evidence is invalid")
    return PipelineVersion(value.number)


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


def _runner_kind(value: object) -> PlannerRunnerKind:
    if type(value) is not str:
        raise CancellationCoordinatorInvalidRequestError(
            "work cancellation runner kind must be text"
        )
    try:
        return PlannerRunnerKind(value)
    except ValueError:
        raise CancellationCoordinatorInvalidRequestError(
            "work cancellation runner kind is not registered"
        ) from None


def _redacted_detail(value: str) -> RedactedAttemptDetail:
    return RedactedAttemptDetail(value)


def _exact_enum[T: StrEnum](value: object, expected: type[T], subject: str) -> T:
    if type(value) is not expected:
        raise TypeError(f"{subject} is invalid")
    return cast(T, value)


def _exact_text(value: object, subject: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{subject} must be text")
    return value


def _bounded_positive(value: object, subject: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    if not 1 <= value <= MAX_CONSISTENCY_SEQUENCE:
        raise ValueError(f"{subject} is outside the supported range")
    return value


def _bounded_nonnegative(value: object, subject: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    if not 0 <= value <= MAX_CONSISTENCY_SEQUENCE:
        raise ValueError(f"{subject} is outside the supported range")
    return value


def _optional_integer(value: object, subject: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    return value


def _validate_correlation_id(value: object) -> str | None:
    if value is None:
        return None
    if (
        type(value) is not str
        or not 1 <= len(value) <= MAX_CANCELLATION_CORRELATION_ID_LENGTH
        or _PORTABLE_IDENTITY.fullmatch(value) is None
    ):
        raise CancellationCoordinatorInvalidRequestError(
            "cancellation correlation identifier is invalid"
        )
    return value


def _validate_detail(value: object) -> None:
    if type(value) is not str:
        raise CancellationCoordinatorInvalidRequestError("cancellation detail must be text or None")
    if not 1 <= len(value) <= MAX_CANCELLATION_DETAIL_LENGTH:
        raise CancellationCoordinatorInvalidRequestError(
            "cancellation detail is outside the supported range"
        )


def _validate_timeout(value: object, subject: str) -> None:
    if type(value) is not float:
        raise TypeError(f"{subject} must be a float")
    if not 0 <= value <= MAX_CANCELLATION_TIMEOUT_SECONDS:
        raise ValueError(f"{subject} is outside the supported range")


__all__ = [
    "CANCELLATION_EVENT_PAYLOAD_SCHEMA_VERSION",
    "MAX_CANCELLATION_CONTENTION_ATTEMPTS",
    "MAX_CANCELLATION_CORRELATION_ID_LENGTH",
    "MAX_CANCELLATION_DETAIL_LENGTH",
    "MAX_CANCELLATION_RESOURCES",
    "MAX_CANCELLATION_TIMEOUT_SECONDS",
    "CancellationAction",
    "CancellationCleanupError",
    "CancellationClock",
    "CancellationCoordinator",
    "CancellationCoordinatorAdmissionError",
    "CancellationCoordinatorBusyError",
    "CancellationCoordinatorClockError",
    "CancellationCoordinatorError",
    "CancellationCoordinatorIncompleteError",
    "CancellationCoordinatorInvalidRequestError",
    "CancellationCoordinatorNotReadyError",
    "CancellationCoordinatorOutcomeUnknownError",
    "CancellationCoordinatorProtocolError",
    "CancellationCoordinatorRejectedError",
    "CancellationCoordinatorSettings",
    "CancellationCoordinatorStateReadError",
    "CancellationDurableState",
    "CancellationReport",
    "CancellationResource",
    "CancellationStateReader",
    "CancellationWriter",
]
