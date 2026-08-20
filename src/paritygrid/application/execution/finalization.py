"""Deterministic terminal run finalization verified from durable evidence."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256
from threading import Lock
from typing import Protocol, cast, runtime_checkable

from paritygrid.application.planner import PlanFingerprint
from paritygrid.application.ports.configuration import (
    ConfigurationDocument,
    DocumentArray,
    DocumentValue,
    NestedDocumentObject,
)
from paritygrid.application.ports.consistency import (
    MAX_CONSISTENCY_SEQUENCE,
    ConsistencyCorruptionError,
    ConsistencyRepositoryError,
    EventSequence,
    EventSubjectKind,
    ExecutionEventBatch,
    ExecutionEventRecord,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.execution import (
    AttemptOutcome,
    ExecutionCorruptionError,
    ExecutionRepositoryError,
    RunNodeRecord,
    RunNodeStatus,
    RunRecord,
    WorkAttemptRecord,
    WorkItemRecord,
    WorkItemState,
)
from paritygrid.application.ports.run_statistics import (
    RunStatisticsError,
    RunStatisticsQuerySnapshot,
    RunStatisticsSourceSnapshot,
    RunStatisticsSummary,
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
    FinalizeEmptyRunNode,
    FinalizeEmptyRunNodeResult,
    TransitionRun,
    TransitionRunResult,
)
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    Duration,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    StateFingerprint,
    UtcTimestamp,
    WorkItemId,
)

FINALIZATION_EVENT_PAYLOAD_SCHEMA_VERSION = 1
FINALIZATION_FINGERPRINT_DOMAIN = b"paritygrid:run-finalization:v2\0"
EMPTY_NODE_EVENT_PAYLOAD_SCHEMA_VERSION = 1
MAX_FINALIZATION_CORRELATION_ID_LENGTH = 96
MAX_FINALIZATION_TIMEOUT_SECONDS = 86_400.0
MAX_FINALIZATION_CONTENTION_ATTEMPTS = 9
MAX_FINALIZATION_NODES = 256
_LENGTH_BYTES = 8

_PORTABLE_IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]*", flags=re.ASCII)
_DEFINITELY_NOT_EXECUTED = (
    WriterDefinitelyNotExecutedError,
    ExecutionRepositoryError,
    ConsistencyRepositoryError,
)
_PAUSE_BOUND_STATES = frozenset({RunState.PAUSING, RunState.PAUSED, RunState.RESUMING})
_TERMINAL_WORK_STATES = frozenset(
    {
        WorkItemState.SUCCEEDED,
        WorkItemState.QUARANTINED,
        WorkItemState.FAILED,
        WorkItemState.CANCELLED,
    }
)
_SUCCESS_OUTCOMES = frozenset({RunState.SUCCEEDED, RunState.PARTIALLY_SUCCEEDED})
_RUN_EVENT_KINDS = {
    RunState.SUCCEEDED: "run_succeeded",
    RunState.PARTIALLY_SUCCEEDED: "run_partially_succeeded",
    RunState.FAILED: "run_failed",
}


class FinalizationError(RuntimeError):
    """Base failure for terminal run finalization."""


class FinalizationBusyError(FinalizationError):
    """An overlapping finalization operation was rejected."""


class FinalizationInvalidRequestError(FinalizationError):
    """Finalization evidence or lifecycle state is not admissible."""


class FinalizationNotReadyError(FinalizationError):
    """The execution graph has not reached a stable terminal boundary."""


class FinalizationConflictError(FinalizationError):
    """Durable evidence contradicts the requested or recorded finalization."""


class FinalizationClockError(FinalizationError):
    """The injected clock did not produce one exact safe timestamp."""


class FinalizationStateReadError(FinalizationError):
    """A coherent durable finalization frontier could not be read safely."""


class FinalizationAdmissionError(FinalizationError):
    """Writer admission failed before a durable command identity was allocated."""


class FinalizationRejectedError(FinalizationError):
    """A command was proven not to have executed."""


class FinalizationOutcomeUnknownError(FinalizationError):
    """An admitted command has no proven durable outcome."""


class FinalizationProtocolError(FinalizationOutcomeUnknownError):
    """Borrowed collaborator evidence was malformed or inconsistent."""


class FinalizationVerificationError(FinalizationOutcomeUnknownError):
    """Final verification could not prove the expected terminal evidence."""


class FinalizationAnalyticsError(FinalizationError):
    """The accepted analytical boundary failed before any run mutation."""


class FinalizationAction(StrEnum):
    """Closed successful finalization outcomes."""

    FINALIZED = "finalized"
    ALREADY_FINALIZED = "already_finalized"


class FinalizationOutcome(StrEnum):
    """Closed terminal run outcomes derived from durable evidence."""

    SUCCEEDED = "succeeded"
    PARTIALLY_SUCCEEDED = "partially_succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class FinalizationSettings:
    """Bounded writer admission and result waits."""

    admission_timeout_seconds: float = 5.0
    result_timeout_seconds: float = 60.0

    def __post_init__(self) -> None:
        _validate_timeout(self.admission_timeout_seconds, "finalization admission timeout")
        _validate_timeout(self.result_timeout_seconds, "finalization result timeout")


@dataclass(frozen=True, slots=True, repr=False)
class FinalizationEvidence:
    """One transactionally read terminal finalization frontier."""

    run: RunRecord
    next_event_sequence: EventSequence
    event_counter_row_version: int
    nodes: tuple[RunNodeRecord, ...]
    work: tuple[WorkItemRecord, ...]
    attempts: tuple[WorkAttemptRecord, ...]
    checkpoint_versions: tuple[tuple[WorkItemId, int], ...]

    def __post_init__(self) -> None:
        if type(self.run) is not RunRecord:
            raise TypeError("finalization run must use RunRecord")
        if type(self.next_event_sequence) is not EventSequence:
            raise TypeError("finalization event frontier must use EventSequence")
        if type(self.event_counter_row_version) is not int:
            raise TypeError("finalization counter row version must be an integer")
        if not 1 <= self.event_counter_row_version <= MAX_CONSISTENCY_SEQUENCE:
            raise ValueError("finalization counter row version is outside the range")
        for collection, subject in (
            (self.nodes, "finalization nodes"),
            (self.work, "finalization work"),
            (self.attempts, "finalization attempts"),
        ):
            if type(collection) is not tuple:
                raise TypeError(f"{subject} must be a tuple")
        if type(self.checkpoint_versions) is not tuple:
            raise TypeError("finalization checkpoint frontier must be a tuple")
        if not self.nodes:
            raise ValueError("finalization evidence requires run nodes")
        if len(self.nodes) > MAX_FINALIZATION_NODES:
            raise ValueError("finalization evidence exceeds the node limit")
        for entry in self.checkpoint_versions:
            if type(entry) is not tuple or len(entry) != 2:
                raise TypeError("finalization checkpoint entry is invalid")

    def __repr__(self) -> str:
        return (
            "FinalizationEvidence("
            f"run_id={self.run.run_id!r}, state={self.run.state.value!r}, "
            f"run_row_version={self.run.row_version!r}, nodes={len(self.nodes)}, "
            f"work={len(self.work)}, attempts={len(self.attempts)}, "
            f"checkpoints={len(self.checkpoint_versions)}, "
            f"next_event_sequence={self.next_event_sequence.number!r})"
        )


@runtime_checkable
class FinalizationAnalytics(Protocol):
    """Borrowed run-statistics analytical boundary used without ownership."""

    def rebuild(self, source: RunStatisticsSourceSnapshot) -> RunStatisticsQuerySnapshot:
        """Rebuild disposable analytical views from one authoritative source."""
        ...

    def get_summary(self, snapshot: RunStatisticsQuerySnapshot) -> RunStatisticsSummary:
        """Return the exact run-level metric projection of one snapshot."""
        ...


@runtime_checkable
class FinalizationClock(Protocol):
    """Injected exact UTC clock used before durable transition admission."""

    def now(self) -> UtcTimestamp:
        """Return the current exact UTC timestamp."""
        ...


@runtime_checkable
class FinalizationStateReader(Protocol):
    """Borrowed short-transaction reader for one terminal frontier."""

    def read(self, run_id: RunId, /) -> FinalizationEvidence:
        """Read one coherent durable finalization frontier."""
        ...


@runtime_checkable
class FinalizationWriter(Protocol):
    """Borrowed transactional-writer surface without lifecycle ownership."""

    def submit(
        self,
        command: WriterCommand,
        *,
        timeout_seconds: float,
    ) -> WriterTicket:
        """Submit one exact finalization command."""
        ...


@dataclass(frozen=True, slots=True, repr=False)
class FinalizationReport:
    """Proven terminal finalization evidence for one exact run."""

    action: FinalizationAction
    outcome: FinalizationOutcome
    run: RunRecord
    events: ExecutionEventBatch
    submission_ids: tuple[WriterSubmissionId, ...]
    fingerprint: StateFingerprint | None
    source_sha256: str

    def __repr__(self) -> str:
        return (
            "FinalizationReport("
            f"action={self.action.value!r}, outcome={self.outcome.value!r}, "
            f"run_id={self.run.run_id!r}, run_row_version={self.run.row_version!r}, "
            f"events={len(self.events.items)}, submissions={len(self.submission_ids)}, "
            f"fingerprint={self.fingerprint is not None!r})"
        )


class RunFinalizer:
    """Derive one terminal run outcome from exact durable evidence.

    The final fingerprint is computed through the accepted run-statistics
    analytical boundary before any run mutation and covers the captured plan
    plus logical terminal node, work-partition, checkpoint, and metric evidence.
    Run-scoped identities, runner choice, attempt history, and timing remain in
    the diagnostic projection digest but cannot break runner equivalence.
    Replaying an exact finalization is read-only; divergent replay is a typed
    conflict.
    """

    __slots__ = (
        "_analytics",
        "_clock",
        "_lifecycle_lock",
        "_operation_lock",
        "_reader",
        "_settings",
        "_uncertain",
        "_writer",
    )

    def __init__(
        self,
        writer: FinalizationWriter,
        reader: FinalizationStateReader,
        analytics: FinalizationAnalytics,
        clock: FinalizationClock,
        *,
        settings: FinalizationSettings | None = None,
    ) -> None:
        writer_value = cast(object, writer)
        reader_value = cast(object, reader)
        analytics_value = cast(object, analytics)
        clock_value = cast(object, clock)
        if not isinstance(writer_value, FinalizationWriter):
            raise TypeError("finalization writer must provide transactional submit")
        if not isinstance(reader_value, FinalizationStateReader):
            raise TypeError("finalization reader must provide a coherent read")
        if not isinstance(analytics_value, FinalizationAnalytics):
            raise TypeError("finalization analytics must implement the statistics boundary")
        if not isinstance(clock_value, FinalizationClock):
            raise TypeError("finalization clock must provide exact UTC time")
        selected_settings = FinalizationSettings() if settings is None else settings
        if type(selected_settings) is not FinalizationSettings:
            raise TypeError("finalization settings must use FinalizationSettings")
        self._writer = writer_value
        self._reader = reader_value
        self._analytics = analytics_value
        self._clock = clock_value
        self._settings = selected_settings
        self._lifecycle_lock = Lock()
        self._operation_lock = Lock()
        self._uncertain = False

    def finalize(
        self,
        run_id: RunId,
        *,
        plan_nodes: tuple[NodeId, ...],
        plan_fingerprint: PlanFingerprint,
        correlation_id: str | None = None,
    ) -> FinalizationReport:
        """Derive, verify, and persist one terminal run outcome."""
        correlation = _validate_correlation_id(correlation_id)
        _validate_plan_nodes(plan_nodes)
        if type(plan_fingerprint) is not PlanFingerprint:
            raise FinalizationInvalidRequestError("finalization requires an exact plan fingerprint")
        if not self._operation_lock.acquire(blocking=False):
            raise FinalizationBusyError("finalizer already has an active operation")
        try:
            with self._lifecycle_lock:
                if self._uncertain:
                    raise FinalizationOutcomeUnknownError(
                        "finalization requires durable recovery inspection"
                    )
            evidence = self._read_evidence(run_id)
            self._require_plan_nodes(evidence, plan_nodes)
            state = evidence.run.state
            if state in _SUCCESS_OUTCOMES:
                return self._replay_success(run_id, evidence, plan_fingerprint)
            if state is RunState.FAILED:
                return self._replay_failure(run_id, evidence)
            if state is RunState.CANCELLED:
                return self._replay_cancelled(run_id, evidence)
            if state is RunState.CANCELLING:
                raise FinalizationNotReadyError(
                    "run cancellation must complete before finalization"
                )
            if state in _PAUSE_BOUND_STATES:
                raise FinalizationInvalidRequestError(
                    "run must leave its pause lifecycle before finalization"
                )
            if state is RunState.QUEUED:
                raise FinalizationInvalidRequestError("run has not started executing")
            assert state is RunState.RUNNING
            return self._finalize_running(
                run_id,
                evidence,
                plan_nodes,
                plan_fingerprint,
                correlation,
            )
        finally:
            self._operation_lock.release()

    def _replay_success(
        self,
        run_id: RunId,
        evidence: FinalizationEvidence,
        plan_fingerprint: PlanFingerprint,
    ) -> FinalizationReport:
        stored = evidence.run.final_reconciliation_fingerprint
        if stored is None:
            raise FinalizationConflictError("finalized run is missing its fingerprint")
        _require_work_terminal(evidence)
        _require_checkpoint_frontier(evidence)
        _require_nodes_terminal(evidence)
        summary = self._analytics_projection(evidence)
        fingerprint = _final_fingerprint(
            plan_fingerprint,
            evidence,
            summary,
        )
        if fingerprint != stored:
            raise FinalizationConflictError("replay evidence diverges from the stored run")
        return FinalizationReport(
            FinalizationAction.ALREADY_FINALIZED,
            FinalizationOutcome(evidence.run.state.value),
            _snapshot_run(evidence.run),
            ExecutionEventBatch(
                (),
                evidence.next_event_sequence,
                evidence.event_counter_row_version,
            ),
            (),
            stored,
            summary.source_sha256,
        )

    def _replay_failure(
        self,
        run_id: RunId,
        evidence: FinalizationEvidence,
    ) -> FinalizationReport:
        del run_id
        _require_work_terminal(evidence)
        outcome = _derive_outcome(evidence)
        if outcome is not FinalizationOutcome.FAILED:
            raise FinalizationConflictError("replay evidence diverges from the stored run")
        if evidence.run.final_reconciliation_fingerprint is not None:
            raise FinalizationConflictError("failed run must not store a final fingerprint")
        _require_checkpoint_frontier(evidence)
        _require_nodes_terminal(evidence)
        self._require_aggregate_consistency(evidence)
        return FinalizationReport(
            FinalizationAction.ALREADY_FINALIZED,
            FinalizationOutcome.FAILED,
            _snapshot_run(evidence.run),
            ExecutionEventBatch(
                (),
                evidence.next_event_sequence,
                evidence.event_counter_row_version,
            ),
            (),
            None,
            _projection_digest(evidence),
        )

    def _replay_cancelled(
        self,
        run_id: RunId,
        evidence: FinalizationEvidence,
    ) -> FinalizationReport:
        del run_id
        _require_work_terminal(evidence)
        _require_checkpoint_frontier(evidence)
        self._require_aggregate_consistency(evidence)
        if evidence.run.final_reconciliation_fingerprint is not None:
            raise FinalizationConflictError("cancelled run must not store a final fingerprint")
        return FinalizationReport(
            FinalizationAction.ALREADY_FINALIZED,
            FinalizationOutcome.CANCELLED,
            _snapshot_run(evidence.run),
            ExecutionEventBatch(
                (),
                evidence.next_event_sequence,
                evidence.event_counter_row_version,
            ),
            (),
            None,
            _projection_digest(evidence),
        )

    def _finalize_running(
        self,
        run_id: RunId,
        evidence: FinalizationEvidence,
        plan_nodes: tuple[NodeId, ...],
        plan_fingerprint: PlanFingerprint,
        correlation: str | None,
    ) -> FinalizationReport:
        del plan_nodes
        _require_work_terminal(evidence)
        _require_checkpoint_frontier(evidence)
        _require_headroom(evidence, 1 + _empty_node_count(evidence))
        self._require_aggregate_consistency(evidence)
        # One timestamp covers the whole mutation phase so node finished_at
        # values can never precede each other or the run transition.
        transitioned_at = self._now(evidence.run)
        projected = _project_empty_nodes(evidence, correlation, transitioned_at)
        _require_nodes_terminal(projected)
        summary = self._analytics_projection(projected)
        outcome = _derive_outcome(projected)
        _require_derivation_agreement(projected, summary, outcome)
        fingerprint = (
            None
            if outcome is FinalizationOutcome.FAILED
            else _final_fingerprint(plan_fingerprint, projected, summary)
        )
        submission_ids: list[WriterSubmissionId] = []
        evidence = self._finalize_empty_nodes(
            evidence, correlation, submission_ids, transitioned_at
        )
        if evidence != projected:
            self._mark_uncertain()
            raise FinalizationVerificationError(
                "empty-node commits diverged from their validated projection"
            )
        target = _target_state(outcome)
        command = _transition_command(evidence, target, transitioned_at, fingerprint, correlation)
        run, events, submission_id = self._execute(command, command, evidence.run)
        submission_ids.append(submission_id)
        del run_id
        return FinalizationReport(
            FinalizationAction.FINALIZED,
            outcome,
            run,
            events,
            tuple(submission_ids),
            fingerprint,
            summary.source_sha256,
        )

    def _finalize_empty_nodes(
        self,
        evidence: FinalizationEvidence,
        correlation: str | None,
        submission_ids: list[WriterSubmissionId],
        transitioned_at: UtcTimestamp,
    ) -> FinalizationEvidence:
        empty = tuple(
            node
            for node in evidence.nodes
            if node.work_total == 0 and node.status is RunNodeStatus.PENDING
        )
        run_row_version = evidence.run.row_version
        for node in empty:
            command = _empty_node_command(
                evidence,
                node,
                run_row_version,
                transitioned_at,
                correlation,
            )
            node_record, node_events, submission_id = self._execute_empty(command, command)
            submission_ids.append(submission_id)
            run_row_version += 1
            evidence = FinalizationEvidence(
                _advanced_run(evidence.run, run_row_version),
                node_events.next_sequence,
                node_events.counter_row_version,
                tuple(
                    replaced if replaced.node_id != node_record.node_id else node_record
                    for replaced in evidence.nodes
                ),
                evidence.work,
                evidence.attempts,
                evidence.checkpoint_versions,
            )
        return evidence

    def _require_aggregate_consistency(self, evidence: FinalizationEvidence) -> None:
        """Validate stored aggregates against work and attempt history in memory."""
        try:
            RunStatisticsSourceSnapshot(
                evidence.run,
                evidence.nodes,
                evidence.work,
                evidence.attempts,
            )
        except RunStatisticsError:
            raise FinalizationConflictError("node aggregates diverge from durable work") from None
        except Exception:
            raise FinalizationProtocolError("finalization analytics input is invalid") from None

    def _analytics_projection(self, evidence: FinalizationEvidence) -> _AnalyticsProjection:
        self._require_aggregate_consistency(evidence)
        snapshot = RunStatisticsSourceSnapshot(
            evidence.run,
            evidence.nodes,
            evidence.work,
            evidence.attempts,
        )
        analytics_failed = False
        summary: RunStatisticsSummary | None = None
        try:
            query = self._analytics.rebuild(snapshot)
            summary = self._analytics.get_summary(query)
        except Exception:
            analytics_failed = True
        if analytics_failed or summary is None:
            raise FinalizationAnalyticsError("finalization analytics boundary failed")
        return _AnalyticsProjection(snapshot.source_sha256, summary)

    def _read_evidence(self, run_id: RunId) -> FinalizationEvidence:
        corrupt = False
        failed = False
        try:
            value = self._reader.read(run_id)
        except ExecutionCorruptionError, ConsistencyCorruptionError:
            corrupt = True
            value = None
        except Exception:
            failed = True
            value = None
        if corrupt:
            raise FinalizationConflictError("durable finalization frontier is corrupt")
        if failed:
            raise FinalizationStateReadError("finalization frontier read failed")
        invalid = False
        try:
            clean = _snapshot_evidence(value)
        except Exception:
            invalid = True
            clean = None
        if invalid or clean is None:
            raise FinalizationProtocolError("finalization frontier is invalid")
        return clean

    def _now(self, run: RunRecord) -> UtcTimestamp:
        failed = False
        try:
            value = self._clock.now()
        except Exception:
            failed = True
            value = None
        if failed:
            raise FinalizationClockError("finalization clock failed")
        invalid = False
        try:
            timestamp = _snapshot_timestamp(value)
        except Exception:
            invalid = True
            timestamp = None
        if invalid or timestamp is None:
            raise FinalizationClockError("finalization clock returned an invalid time")
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
            raise FinalizationClockError("finalization clock is behind durable run time")
        return timestamp

    def _execute(
        self,
        command: TransitionRun,
        expected_command: TransitionRun,
        previous_run: RunRecord,
    ) -> tuple[RunRecord, ExecutionEventBatch, WriterSubmissionId]:
        del expected_command
        ticket = self._submit(command)
        submission_id = _ticket_identity(ticket)
        receipt = self._await_result(ticket)
        invalid_receipt = False
        try:
            validated = _validate_receipt(receipt, submission_id, command, previous_run)
        except FinalizationVerificationError:
            self._mark_uncertain()
            raise
        except Exception:
            invalid_receipt = True
            validated = None
        if invalid_receipt or validated is None:
            self._mark_uncertain()
            raise FinalizationProtocolError("finalization writer receipt is invalid")
        return validated

    def _execute_empty(
        self,
        command: FinalizeEmptyRunNode,
        expected_command: FinalizeEmptyRunNode,
    ) -> tuple[RunNodeRecord, ExecutionEventBatch, WriterSubmissionId]:
        del expected_command
        ticket = self._submit(command)
        submission_id = _ticket_identity(ticket)
        receipt = self._await_result(ticket)
        invalid_receipt = False
        try:
            validated = _validate_empty_receipt(receipt, submission_id, command)
        except Exception:
            invalid_receipt = True
            validated = None
        if invalid_receipt or validated is None:
            self._mark_uncertain()
            raise FinalizationProtocolError("empty-node receipt is invalid")
        return validated

    def _submit(self, command: WriterCommand) -> WriterTicket:
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
            self._mark_uncertain()
            raise
        if admission_failed or writer_failed:
            raise FinalizationAdmissionError("finalization writer admission failed")
        if unexpected or ticket is None:
            self._mark_uncertain()
            raise FinalizationProtocolError("finalization admission outcome is unknown")
        return ticket

    def _await_result(self, ticket: WriterTicket) -> WriterReceipt:
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
            raise FinalizationRejectedError("finalization command was not committed")
        if unknown:
            self._mark_uncertain()
            raise FinalizationOutcomeUnknownError("finalization durable outcome is unknown")
        return cast(WriterReceipt, receipt)

    def _mark_uncertain(self) -> None:
        try:
            with self._lifecycle_lock:
                self._uncertain = True
        except BaseException:
            self._uncertain = True

    @staticmethod
    def _require_plan_nodes(evidence: FinalizationEvidence, plan_nodes: tuple[NodeId, ...]) -> None:
        durable = tuple(sorted(str(node.node_id) for node in evidence.nodes))
        planned = tuple(sorted(str(node_id) for node_id in plan_nodes))
        if durable != planned:
            raise FinalizationConflictError("durable nodes do not match the captured plan")


@dataclass(frozen=True, slots=True)
class _AnalyticsProjection:
    source_sha256: str
    summary: RunStatisticsSummary


def _projection_digest(evidence: FinalizationEvidence) -> str:
    return sha256(
        json.dumps(
            {
                "attempt_count": len(evidence.attempts),
                "node_count": len(evidence.nodes),
                "run_id": str(evidence.run.run_id),
                "work_count": len(evidence.work),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _final_fingerprint(
    plan_fingerprint: PlanFingerprint,
    evidence: FinalizationEvidence,
    projection: _AnalyticsProjection,
) -> StateFingerprint:
    document = _evidence_document(plan_fingerprint, evidence, projection.summary)
    encoded = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    digest = sha256(
        FINALIZATION_FINGERPRINT_DOMAIN + _frame(plan_fingerprint.to_bytes()) + _frame(encoded)
    ).digest()
    return StateFingerprint(digest.hex())


_SUMMARY_LOGICAL_FIELDS = (
    "bytes_read",
    "bytes_written",
    "node_count",
    "records_quarantined",
    "records_read",
    "records_written",
    "work_cancelled",
    "work_failed",
    "work_quarantined",
    "work_succeeded",
    "work_total",
)


def _evidence_document(
    plan_fingerprint: PlanFingerprint,
    evidence: FinalizationEvidence,
    summary: RunStatisticsSummary,
) -> dict[str, object]:
    run = evidence.run
    summary_document: dict[str, object] = {}
    for field in _SUMMARY_LOGICAL_FIELDS:
        summary_document[field] = getattr(summary, field)
    nodes_document = [
        {
            "bytes_read": node.bytes_read,
            "bytes_written": node.bytes_written,
            "node_id": str(node.node_id),
            "records_quarantined": node.records_quarantined,
            "records_read": node.records_read,
            "records_written": node.records_written,
            "status": node.status.value,
            "work_cancelled": node.work_cancelled,
            "work_failed": node.work_failed,
            "work_quarantined": node.work_quarantined,
            "work_succeeded": node.work_succeeded,
            "work_total": node.work_total,
        }
        for node in sorted(evidence.nodes, key=lambda item: str(item.node_id))
    ]
    checkpoints = dict(evidence.checkpoint_versions)
    work_document = [
        {
            "checkpoint_version": checkpoints.get(work.work_item_id),
            "node_id": str(work.node_id),
            "partition_key": str(work.partition_key),
            "state": work.state.value,
        }
        for work in sorted(
            evidence.work,
            key=lambda item: (str(item.node_id), str(item.partition_key)),
        )
    ]
    return {
        "finalization_version": 2,
        "nodes": nodes_document,
        "plan_fingerprint": str(plan_fingerprint),
        "run": {
            "pipeline_id": str(run.pipeline_id),
            "pipeline_version": run.pipeline_version.number,
            "scenario_seed": run.scenario_seed,
        },
        "summary": summary_document,
        "work": work_document,
    }


def _derive_outcome(evidence: FinalizationEvidence) -> FinalizationOutcome:
    failed = any(work.state is WorkItemState.FAILED for work in evidence.work)
    if failed:
        return FinalizationOutcome.FAILED
    if evidence.work:
        pure = all(work.state is WorkItemState.SUCCEEDED for work in evidence.work)
        return FinalizationOutcome.SUCCEEDED if pure else FinalizationOutcome.PARTIALLY_SUCCEEDED
    return FinalizationOutcome.SUCCEEDED


def _target_state(outcome: FinalizationOutcome) -> RunState:
    if outcome is FinalizationOutcome.SUCCEEDED:
        return RunState.SUCCEEDED
    if outcome is FinalizationOutcome.PARTIALLY_SUCCEEDED:
        return RunState.PARTIALLY_SUCCEEDED
    return RunState.FAILED


def _require_derivation_agreement(
    evidence: FinalizationEvidence,
    projection: _AnalyticsProjection,
    outcome: FinalizationOutcome,
) -> None:
    summary = projection.summary
    counts = (
        (summary.work_failed, WorkItemState.FAILED),
        (summary.work_succeeded, WorkItemState.SUCCEEDED),
        (summary.work_quarantined, WorkItemState.QUARANTINED),
        (summary.work_cancelled, WorkItemState.CANCELLED),
    )
    for observed, state in counts:
        expected = sum(work.state is state for work in evidence.work)
        if observed != expected:
            raise FinalizationProtocolError("finalization analytics counts are inconsistent")


def _require_work_terminal(evidence: FinalizationEvidence) -> None:
    for work in evidence.work:
        if work.state not in _TERMINAL_WORK_STATES:
            raise FinalizationNotReadyError("finalization boundary still has non-terminal work")


def _require_nodes_terminal(evidence: FinalizationEvidence) -> None:
    for node in evidence.nodes:
        if node.status in {RunNodeStatus.PENDING, RunNodeStatus.RUNNING}:
            raise FinalizationNotReadyError("finalization boundary still has a non-terminal node")


def _require_checkpoint_frontier(evidence: FinalizationEvidence) -> None:
    frontier = dict(evidence.checkpoint_versions)
    for work in evidence.work:
        version = frontier.get(work.work_item_id)
        if version is None:
            raise FinalizationConflictError("checkpoint frontier is missing durable evidence")
        if version != work.expected_checkpoint_version:
            raise FinalizationConflictError("checkpoint frontier diverges from work evidence")
        if work.state is WorkItemState.SUCCEEDED and version < 1:
            raise FinalizationConflictError("successful work is missing its checkpoint")


def _empty_node_count(evidence: FinalizationEvidence) -> int:
    return sum(
        node.work_total == 0 and node.status is RunNodeStatus.PENDING for node in evidence.nodes
    )


def _project_empty_nodes(
    evidence: FinalizationEvidence,
    correlation: str | None,
    transitioned_at: UtcTimestamp,
) -> FinalizationEvidence:
    """Project every empty-node commit before admitting any mutation."""
    run_row_version = evidence.run.row_version
    for node in tuple(
        item
        for item in evidence.nodes
        if item.work_total == 0 and item.status is RunNodeStatus.PENDING
    ):
        command = _empty_node_command(
            evidence,
            node,
            run_row_version,
            transitioned_at,
            correlation,
        )
        node_record = _expected_empty_node(command)
        run_row_version += 1
        evidence = FinalizationEvidence(
            _advanced_run(evidence.run, run_row_version),
            EventSequence(evidence.next_event_sequence.number + 1),
            evidence.event_counter_row_version + 1,
            tuple(
                replaced if replaced.node_id != node_record.node_id else node_record
                for replaced in evidence.nodes
            ),
            evidence.work,
            evidence.attempts,
            evidence.checkpoint_versions,
        )
    return evidence


def _empty_node_command(
    evidence: FinalizationEvidence,
    node: RunNodeRecord,
    run_row_version: int,
    transitioned_at: UtcTimestamp,
    correlation: str | None,
) -> FinalizeEmptyRunNode:
    return FinalizeEmptyRunNode(
        evidence.run.run_id,
        node.node_id,
        node.row_version,
        run_row_version,
        transitioned_at,
        EventAppendRequest(
            EventSequence(evidence.next_event_sequence.number),
            evidence.event_counter_row_version,
            PendingExecutionEvent(
                "run_node_succeeded",
                transitioned_at,
                EventSubjectKind.RUN,
                _snapshot_run_id(evidence.run.run_id),
                correlation,
                EMPTY_NODE_EVENT_PAYLOAD_SCHEMA_VERSION,
                RedactedDocument.from_mapping({"node_id": str(node.node_id)}),
            ),
        ),
    )


def _require_headroom(evidence: FinalizationEvidence, arrows: int = 1) -> None:
    maximum = MAX_CONSISTENCY_SEQUENCE - arrows
    if (
        evidence.run.row_version > maximum
        or evidence.next_event_sequence.number > maximum
        or evidence.event_counter_row_version > maximum
    ):
        raise FinalizationInvalidRequestError("finalization frontier cannot advance its arrows")


def _advanced_run(run: RunRecord, row_version: int) -> RunRecord:
    clean = _snapshot_run(run)
    return RunRecord(
        clean.run_id,
        clean.pipeline_id,
        clean.pipeline_version,
        clean.runner_kind,
        clean.runner_configuration,
        clean.state,
        row_version,
        clean.scenario_seed,
        clean.created_at,
        clean.started_at,
        clean.finished_at,
        clean.cancellation_requested_at,
        clean.recovery_started_at,
        clean.recovered_at,
        clean.final_reconciliation_fingerprint,
    )


def _transition_command(
    evidence: FinalizationEvidence,
    target: RunState,
    transitioned_at: UtcTimestamp,
    fingerprint: StateFingerprint | None,
    correlation_id: str | None,
) -> TransitionRun:
    previous = evidence.run.state
    event = PendingExecutionEvent(
        _RUN_EVENT_KINDS[target],
        _snapshot_timestamp(transitioned_at),
        EventSubjectKind.RUN,
        _snapshot_run_id(evidence.run.run_id),
        correlation_id,
        FINALIZATION_EVENT_PAYLOAD_SCHEMA_VERSION,
        RedactedDocument.from_mapping(
            {
                "final_fingerprint": None if fingerprint is None else str(fingerprint),
                "from_state": previous.value,
                "to_state": target.value,
            }
        ),
    )
    return TransitionRun(
        _snapshot_run_id(evidence.run.run_id),
        evidence.run.row_version,
        target,
        _snapshot_timestamp(transitioned_at),
        fingerprint,
        EventAppendRequest(
            EventSequence(evidence.next_event_sequence.number),
            evidence.event_counter_row_version,
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
        raise FinalizationProtocolError("finalization ticket identity is invalid")
    return clean


def _validate_receipt(
    receipt: object,
    submission_id: WriterSubmissionId,
    command: TransitionRun,
    previous_run: RunRecord,
) -> tuple[RunRecord, ExecutionEventBatch, WriterSubmissionId]:
    if type(receipt) is not WriterReceipt:
        raise FinalizationVerificationError("finalization receipt type is invalid")
    clean_id = _snapshot_submission_id(receipt.submission_id)
    clean_run_id = _snapshot_run_id(receipt.run_id)
    if (
        clean_id != submission_id
        or receipt.command_kind is not command.kind
        or clean_run_id != command.run_id
        or type(receipt.contention_attempts) is not int
        or not 0 <= receipt.contention_attempts <= MAX_FINALIZATION_CONTENTION_ATTEMPTS
        or receipt.mutated is not True
        or type(receipt.result) is not TransitionRunResult
    ):
        raise FinalizationVerificationError("finalization receipt does not match command")
    clean_run = _snapshot_run(receipt.result.run)
    clean_events = _snapshot_event_batch(receipt.result.events)
    expected_run = _expected_final_run(previous_run, command)
    expected_events = _expected_events(command)
    if clean_run != expected_run or clean_events != expected_events:
        raise FinalizationVerificationError(
            "final verification could not prove the terminal evidence"
        )
    return clean_run, clean_events, clean_id


def _validate_empty_receipt(
    receipt: object,
    submission_id: WriterSubmissionId,
    command: FinalizeEmptyRunNode,
) -> tuple[RunNodeRecord, ExecutionEventBatch, WriterSubmissionId]:
    if type(receipt) is not WriterReceipt:
        raise FinalizationVerificationError("empty-node receipt type is invalid")
    clean_id = _snapshot_submission_id(receipt.submission_id)
    if (
        clean_id != submission_id
        or receipt.command_kind is not command.kind
        or receipt.run_id != command.run_id
        or type(receipt.contention_attempts) is not int
        or not 0 <= receipt.contention_attempts <= MAX_FINALIZATION_CONTENTION_ATTEMPTS
        or receipt.mutated is not True
        or type(receipt.result) is not FinalizeEmptyRunNodeResult
    ):
        raise FinalizationVerificationError("empty-node receipt does not match command")
    result = receipt.result
    expected_node = _expected_empty_node(command)
    clean_node = _snapshot_node(result.node)
    clean_events = _snapshot_event_batch(result.events)
    _require_expected_events(clean_events, command)
    if clean_node != expected_node:
        raise FinalizationVerificationError("empty-node evidence is inconsistent")
    return clean_node, clean_events, clean_id


def _expected_empty_node(command: FinalizeEmptyRunNode) -> RunNodeRecord:
    return RunNodeRecord(
        command.run_id,
        command.node_id,
        RunNodeStatus.SUCCEEDED,
        command.expected_node_row_version + 1,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        Duration(0),
        command.finalized_at,
        command.finalized_at,
    )


def _require_expected_events(
    events: ExecutionEventBatch,
    command: FinalizeEmptyRunNode,
) -> None:
    pending = command.event.event
    expected = ExecutionEventRecord(
        command.run_id,
        command.event.expected_next_sequence,
        pending.event_kind,
        pending.occurred_at,
        pending.subject_kind,
        pending.subject_id,
        pending.correlation_id,
        pending.payload_schema_version,
        pending.payload,
    )
    expected_batch = ExecutionEventBatch(
        (expected,),
        command.event.expected_next_sequence.advance(1),
        command.event.expected_counter_row_version + 1,
    )
    if events != expected_batch:
        raise FinalizationVerificationError("empty-node event evidence is inconsistent")


def _expected_final_run(previous: RunRecord, command: TransitionRun) -> RunRecord:
    clean = _snapshot_run(previous)
    target = command.target_state
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
        command.transitioned_at,
        clean.cancellation_requested_at,
        clean.recovery_started_at,
        clean.recovered_at,
        command.final_reconciliation_fingerprint,
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


def snapshot_finalization_evidence(value: object) -> FinalizationEvidence:
    """Return one detached exact copy of a finalization frontier."""
    return _snapshot_evidence(value)


def _snapshot_evidence(value: object) -> FinalizationEvidence:
    if type(value) is not FinalizationEvidence:
        raise TypeError("finalization evidence has an invalid type")
    checkpoints: list[tuple[WorkItemId, int]] = []
    for entry in value.checkpoint_versions:
        pair = cast(tuple[object, object], entry)
        identity = pair[0]
        version = pair[1]
        if type(identity) is not WorkItemId or type(identity.value) is not str:
            raise TypeError("checkpoint entry identity is invalid")
        if type(version) is not int:
            raise TypeError("checkpoint entry version is invalid")
        checkpoints.append((WorkItemId(identity.value), version))
    return FinalizationEvidence(
        _snapshot_run(value.run),
        _snapshot_event_sequence(value.next_event_sequence),
        _exact_positive_integer(value.event_counter_row_version, "counter row version"),
        tuple(_snapshot_node(node) for node in value.nodes),
        tuple(_snapshot_work(work) for work in value.work),
        tuple(_snapshot_attempt(attempt) for attempt in value.attempts),
        tuple(checkpoints),
    )


def _snapshot_run(value: object) -> RunRecord:
    if type(value) is not RunRecord:
        raise TypeError("finalization run evidence must use RunRecord")
    return RunRecord(
        _snapshot_run_id(value.run_id),
        _snapshot_pipeline_id(value.pipeline_id),
        _snapshot_pipeline_version(value.pipeline_version),
        _exact_text(value.runner_kind, "runner kind"),
        _snapshot_document(value.runner_configuration),
        _exact_enum(value.state, RunState, "run state"),
        _exact_positive_integer(value.row_version, "run row version"),
        _optional_integer(value.scenario_seed, "scenario seed"),
        _snapshot_timestamp(value.created_at),
        _optional_timestamp(value.started_at),
        _optional_timestamp(value.finished_at),
        _optional_timestamp(value.cancellation_requested_at),
        _optional_timestamp(value.recovery_started_at),
        _optional_timestamp(value.recovered_at),
        _optional_fingerprint(value.final_reconciliation_fingerprint),
    )


def _snapshot_node(value: object) -> RunNodeRecord:
    from paritygrid.domain.models import Duration

    if type(value) is not RunNodeRecord:
        raise TypeError("finalization node evidence must use RunNodeRecord")
    node = value
    return RunNodeRecord(
        _snapshot_run_id(node.run_id),
        _snapshot_node_id(node.node_id),
        _exact_enum(node.status, RunNodeStatus, "node status"),
        _exact_positive_integer(node.row_version, "node row version"),
        node.work_total,
        node.work_pending,
        node.work_running,
        node.work_succeeded,
        node.work_quarantined,
        node.work_failed,
        node.work_cancelled,
        node.records_read,
        node.records_written,
        node.records_quarantined,
        node.bytes_read,
        node.bytes_written,
        node.retry_count,
        Duration(node.duration.microseconds),
        _optional_timestamp(node.started_at),
        _optional_timestamp(node.finished_at),
    )


def _snapshot_work(value: object) -> WorkItemRecord:
    if type(value) is not WorkItemRecord:
        raise TypeError("finalization work evidence must use WorkItemRecord")
    from paritygrid.domain.models import AttemptNumber
    from paritygrid.domain.pipeline import PartitionKey

    work = value
    return WorkItemRecord(
        _snapshot_work_item_id(work.work_item_id),
        _snapshot_run_id(work.run_id),
        _snapshot_node_id(work.node_id),
        PartitionKey(work.partition_key.value),
        _exact_enum(work.state, WorkItemState, "work state"),
        _exact_positive_integer(work.row_version, "work row version"),
        work.completed_attempt_count,
        work.expected_checkpoint_version,
        None if work.input_reference is None else _snapshot_document(work.input_reference),
        _optional_timestamp(work.retry_available_at),
        None if work.lease_owner is None else _exact_text(work.lease_owner, "lease owner"),
        _optional_timestamp(work.lease_expires_at),
        None
        if work.active_attempt_number is None
        else AttemptNumber(work.active_attempt_number.number),
        _optional_timestamp(work.active_attempt_started_at),
        None
        if work.active_runner_kind is None
        else _exact_text(work.active_runner_kind, "active runner kind"),
        None
        if work.active_worker_identity is None
        else _exact_text(work.active_worker_identity, "active worker identity"),
        _snapshot_timestamp(work.created_at),
        _snapshot_timestamp(work.updated_at),
    )


def _snapshot_attempt(value: object) -> WorkAttemptRecord:
    from paritygrid.domain.models import AttemptNumber, Duration

    if type(value) is not WorkAttemptRecord:
        raise TypeError("finalization attempt evidence must use WorkAttemptRecord")
    attempt = value
    return WorkAttemptRecord(
        _snapshot_work_item_id(attempt.work_item_id),
        AttemptNumber(attempt.attempt_number.number),
        _snapshot_timestamp(attempt.started_at),
        _snapshot_timestamp(attempt.finished_at),
        _exact_text(attempt.runner_kind, "attempt runner kind"),
        _exact_text(attempt.worker_identity, "attempt worker identity"),
        _exact_enum(attempt.outcome, AttemptOutcome, "attempt outcome"),
        None if attempt.failure_classification is None else attempt.failure_classification,
        None if attempt.redacted_detail is None else _exact_text(attempt.redacted_detail, "detail"),
        None if attempt.result_reference is None else _snapshot_document(attempt.result_reference),
        attempt.records_processed,
        attempt.bytes_processed,
        Duration(attempt.duration.microseconds),
    )


def _snapshot_event_batch(value: object) -> ExecutionEventBatch:
    if type(value) is not ExecutionEventBatch or type(value.items) is not tuple:
        raise TypeError("finalization event batch is invalid")
    return ExecutionEventBatch(
        tuple(_snapshot_event_record(item) for item in value.items),
        _snapshot_event_sequence(value.next_sequence),
        _exact_positive_integer(value.counter_row_version, "counter row version"),
    )


def _snapshot_event_record(value: object) -> ExecutionEventRecord:
    if type(value) is not ExecutionEventRecord:
        raise TypeError("finalization event record is invalid")
    record = value
    subject_kind = _exact_enum(record.subject_kind, EventSubjectKind, "event subject kind")
    if subject_kind is not EventSubjectKind.RUN:
        raise TypeError("finalization event subject must identify a run")
    return ExecutionEventRecord(
        _snapshot_run_id(record.run_id),
        _snapshot_event_sequence(record.sequence),
        _exact_text(record.event_kind, "event kind"),
        _snapshot_timestamp(record.occurred_at),
        subject_kind,
        _snapshot_run_id(record.subject_id),
        _validate_correlation_id(record.correlation_id),
        _exact_positive_integer(record.payload_schema_version, "payload schema version"),
        _snapshot_redacted_document(record.payload),
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


def _snapshot_node_id(value: object) -> NodeId:
    if type(value) is not NodeId or type(value.value) is not str:
        raise TypeError("node identity evidence is invalid")
    return NodeId(value.value)


def _snapshot_work_item_id(value: object) -> WorkItemId:
    if type(value) is not WorkItemId or type(value.value) is not str:
        raise TypeError("work identity evidence is invalid")
    return WorkItemId(value.value)


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


def _exact_enum[T: StrEnum](value: object, expected: type[T], subject: str) -> T:
    if type(value) is not expected:
        raise TypeError(f"{subject} is invalid")
    return cast(T, value)


def _exact_text(value: object, subject: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{subject} must be text")
    return value


def _exact_positive_integer(value: object, subject: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    if not 1 <= value <= MAX_CONSISTENCY_SEQUENCE:
        raise ValueError(f"{subject} is outside the supported range")
    return value


def _optional_integer(value: object, subject: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    return value


def _validate_plan_nodes(plan_nodes: object) -> None:
    if type(plan_nodes) is not tuple or not plan_nodes:
        raise FinalizationInvalidRequestError("finalization requires the captured plan nodes")
    values = cast(tuple[object, ...], plan_nodes)
    if any(type(node_id) is not NodeId for node_id in values):
        raise FinalizationInvalidRequestError("plan node identities are invalid")
    identities = [str(node_id) for node_id in values]
    if len(set(identities)) != len(identities) or len(identities) > MAX_FINALIZATION_NODES:
        raise FinalizationInvalidRequestError("plan node identities must be unique and bounded")


def _validate_correlation_id(value: object) -> str | None:
    if value is None:
        return None
    if (
        type(value) is not str
        or not 1 <= len(value) <= MAX_FINALIZATION_CORRELATION_ID_LENGTH
        or _PORTABLE_IDENTITY.fullmatch(value) is None
    ):
        raise FinalizationInvalidRequestError("finalization correlation identifier is invalid")
    return value


def _validate_timeout(value: object, subject: str) -> None:
    if type(value) is not float:
        raise TypeError(f"{subject} must be a float")
    if not 0 <= value <= MAX_FINALIZATION_TIMEOUT_SECONDS:
        raise ValueError(f"{subject} is outside the supported range")


def _frame(value: bytes) -> bytes:
    return len(value).to_bytes(_LENGTH_BYTES, byteorder="big") + value


__all__ = [
    "EMPTY_NODE_EVENT_PAYLOAD_SCHEMA_VERSION",
    "FINALIZATION_EVENT_PAYLOAD_SCHEMA_VERSION",
    "FINALIZATION_FINGERPRINT_DOMAIN",
    "MAX_FINALIZATION_CONTENTION_ATTEMPTS",
    "MAX_FINALIZATION_CORRELATION_ID_LENGTH",
    "MAX_FINALIZATION_NODES",
    "MAX_FINALIZATION_TIMEOUT_SECONDS",
    "FinalizationAction",
    "FinalizationAdmissionError",
    "FinalizationAnalytics",
    "FinalizationAnalyticsError",
    "FinalizationBusyError",
    "FinalizationClock",
    "FinalizationClockError",
    "FinalizationConflictError",
    "FinalizationError",
    "FinalizationEvidence",
    "FinalizationInvalidRequestError",
    "FinalizationNotReadyError",
    "FinalizationOutcome",
    "FinalizationOutcomeUnknownError",
    "FinalizationProtocolError",
    "FinalizationRejectedError",
    "FinalizationReport",
    "FinalizationSettings",
    "FinalizationStateReadError",
    "FinalizationStateReader",
    "FinalizationVerificationError",
    "FinalizationWriter",
    "RunFinalizer",
    "snapshot_finalization_evidence",
]
