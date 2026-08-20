"""Startup recovery classification from durable SQLite and artifact evidence."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from threading import Lock
from typing import Protocol, cast, runtime_checkable

from paritygrid.application.execution.finalization import FinalizationEvidence
from paritygrid.application.ports.artifact_integrity import ArtifactIntegrityIssue
from paritygrid.application.ports.artifacts import ArtifactManifestRecord
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
    IdempotencyRecord,
    IdempotencyStatus,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.execution import (
    AttemptOutcome,
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
    RunStatisticsSourceSnapshot,
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
from paritygrid.application.writes import RecoverExpiredWork, RecoverExpiredWorkResult
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    NodeId,
    RunId,
    UtcTimestamp,
    WorkItemId,
)

RECOVERY_EVENT_PAYLOAD_SCHEMA_VERSION = 1
RECOVERY_LEASE_EVENT_KIND = "work_lease_expired"
MAX_RECOVERY_CORRELATION_ID_LENGTH = 96
MAX_RECOVERY_TIMEOUT_SECONDS = 86_400.0
MAX_RECOVERY_CONTENTION_ATTEMPTS = 9
MAX_RECOVERY_FINDINGS = 10_000
MAX_RECOVERY_STRANDED_IDEMPOTENCY = 10_000
_TERMINAL_RUN_STATES = frozenset(
    {
        RunState.SUCCEEDED,
        RunState.PARTIALLY_SUCCEEDED,
        RunState.FAILED,
        RunState.CANCELLED,
    }
)
_DEFINITELY_NOT_EXECUTED = (
    WriterDefinitelyNotExecutedError,
    ExecutionRepositoryError,
    ConsistencyRepositoryError,
)
_PORTABLE_IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]*", flags=re.ASCII)


class RecoveryError(RuntimeError):
    """Base failure for startup recovery classification and action."""


class RecoveryBusyError(RecoveryError):
    """An overlapping recovery operation was rejected."""


class RecoveryInvalidRequestError(RecoveryError):
    """Recovery evidence or request shape is not admissible."""


class RecoveryStateReadError(RecoveryError):
    """A coherent durable recovery frontier could not be read safely."""


class RecoveryCorruptionError(RecoveryError):
    """Durable storage or artifact evidence is corrupt or inconsistent."""


class RecoveryAmbiguousError(RecoveryError):
    """Ambiguous evidence fails closed and prevents unsafe scheduling."""


class RecoveryAdmissionError(RecoveryError):
    """Writer admission failed before a durable command identity was allocated."""


class RecoveryRejectedError(RecoveryError):
    """A recovery command was proven not to have executed."""


class RecoveryOutcomeUnknownError(RecoveryError):
    """An admitted recovery command has no proven durable outcome."""


class RecoveryProtocolError(RecoveryOutcomeUnknownError):
    """Borrowed collaborator evidence was malformed or inconsistent."""


class RecoveryClockError(RecoveryError):
    """The injected clock did not produce one exact safe timestamp."""


class RecoveryStatus(StrEnum):
    """Closed run-level recovery classification."""

    HEALTHY = "healthy"
    ACTIVE = "active"
    RECOVERABLE = "recoverable"
    AMBIGUOUS = "ambiguous"


class RecoveryFindingKind(StrEnum):
    """Closed finding kinds with stable sorted reporting."""

    WORK_PENDING = "work_pending"
    WORK_RETRY_WAITING = "work_retry_waiting"
    WORK_COMMITTED = "work_committed"
    WORK_ACTIVE_LEASE = "work_active_lease"
    WORK_EXPIRED_NO_EFFECT = "work_expired_no_effect"
    WORK_EXPIRED_WITH_COMMITTED_ARTIFACT = "work_expired_with_committed_artifact"
    WORK_RUNNING_WITHOUT_LEASE = "work_running_without_lease"
    RUN_QUEUED = "run_queued"
    RUN_PAUSED = "run_paused"
    RUN_TERMINAL = "run_terminal"
    RUN_TERMINAL_WITH_ACTIVE_WORK = "run_terminal_with_active_work"
    CHECKPOINT_FRONTIER_MISMATCH = "checkpoint_frontier_mismatch"
    ATTEMPT_HISTORY_MISMATCH = "attempt_history_mismatch"
    AGGREGATE_MISMATCH = "aggregate_mismatch"
    EVENT_FRONTIER_CORRUPT = "event_frontier_corrupt"
    INTEGRITY_MISSING_FILE = "integrity_missing_file"
    INTEGRITY_ORPHAN_FILE = "integrity_orphan_file"
    INTEGRITY_CHANGED_FILE = "integrity_changed_file"
    INTEGRITY_INVALID_FILE = "integrity_invalid_file"
    INTEGRITY_UNSAFE_ENTRY = "integrity_unsafe_entry"
    STRANDED_IDEMPOTENCY = "stranded_idempotency"


_RECOVERABLE_KINDS = frozenset(
    {
        RecoveryFindingKind.WORK_EXPIRED_NO_EFFECT,
        RecoveryFindingKind.WORK_EXPIRED_WITH_COMMITTED_ARTIFACT,
    }
)
_CORRUPT_KINDS = frozenset(
    {
        RecoveryFindingKind.WORK_RUNNING_WITHOUT_LEASE,
        RecoveryFindingKind.RUN_TERMINAL_WITH_ACTIVE_WORK,
        RecoveryFindingKind.CHECKPOINT_FRONTIER_MISMATCH,
        RecoveryFindingKind.ATTEMPT_HISTORY_MISMATCH,
        RecoveryFindingKind.AGGREGATE_MISMATCH,
        RecoveryFindingKind.EVENT_FRONTIER_CORRUPT,
        RecoveryFindingKind.INTEGRITY_MISSING_FILE,
        RecoveryFindingKind.INTEGRITY_ORPHAN_FILE,
        RecoveryFindingKind.INTEGRITY_CHANGED_FILE,
        RecoveryFindingKind.INTEGRITY_INVALID_FILE,
        RecoveryFindingKind.INTEGRITY_UNSAFE_ENTRY,
    }
)
_INTEGRITY_KIND_MAP = {
    "missing_file": RecoveryFindingKind.INTEGRITY_MISSING_FILE,
    "orphan_file": RecoveryFindingKind.INTEGRITY_ORPHAN_FILE,
    "invalid_file": RecoveryFindingKind.INTEGRITY_CHANGED_FILE,
    "unsafe_entry": RecoveryFindingKind.INTEGRITY_UNSAFE_ENTRY,
}


@dataclass(frozen=True, slots=True)
class RecoverySettings:
    """Bounded writer waits and finding limits."""

    admission_timeout_seconds: float = 5.0
    result_timeout_seconds: float = 60.0
    max_findings: int = MAX_RECOVERY_FINDINGS

    def __post_init__(self) -> None:
        _validate_timeout(self.admission_timeout_seconds, "recovery admission timeout")
        _validate_timeout(self.result_timeout_seconds, "recovery result timeout")
        if (
            type(self.max_findings) is not int
            or not 1 <= self.max_findings <= MAX_RECOVERY_FINDINGS
        ):
            raise ValueError("recovery finding limit is outside the supported range")


@dataclass(frozen=True, slots=True, repr=False)
class RecoveryFinding:
    """One immutable recovery observation without machine paths."""

    kind: RecoveryFindingKind
    run_id: RunId
    node_id: NodeId | None = None
    work_item_id: WorkItemId | None = None
    artifact_id: str | None = None
    detail: str | None = None

    def __post_init__(self) -> None:
        if type(self.kind) is not RecoveryFindingKind:
            raise TypeError("recovery finding kind is invalid")
        _snapshot_run_id(self.run_id)
        if self.node_id is not None:
            _snapshot_node_id(self.node_id)
        if self.work_item_id is not None:
            _snapshot_work_item_id(self.work_item_id)
        if self.artifact_id is not None:
            _exact_text(self.artifact_id, "recovery artifact identity")
        if self.detail is not None:
            _validate_detail(self.detail)

    @property
    def order_key(self) -> tuple[str, str, str, str]:
        """Return the stable deterministic sort key for reporting."""
        return (
            self.kind.value,
            "" if self.node_id is None else str(self.node_id),
            "" if self.work_item_id is None else str(self.work_item_id),
            self.artifact_id or "",
        )

    def __repr__(self) -> str:
        return (
            "RecoveryFinding("
            f"kind={self.kind.value!r}, run_id={self.run_id!r}, "
            f"node_id={self.node_id!r}, work_item_id={self.work_item_id!r})"
        )


@dataclass(frozen=True, slots=True, repr=False)
class RecoveryEvidence:
    """One transactionally read durable recovery frontier."""

    frontier: FinalizationEvidence
    artifacts: tuple[ArtifactManifestRecord, ...]
    integrity_issues: tuple[ArtifactIntegrityIssue, ...]
    idempotency_in_progress: tuple[IdempotencyRecord, ...]

    def __post_init__(self) -> None:
        if type(self.frontier) is not FinalizationEvidence:
            raise TypeError("recovery frontier must use FinalizationEvidence")
        for collection, subject in (
            (self.artifacts, "recovery artifacts"),
            (self.integrity_issues, "recovery integrity issues"),
            (self.idempotency_in_progress, "recovery idempotency records"),
        ):
            if type(collection) is not tuple:
                raise TypeError(f"{subject} must be a tuple")
        if len(self.idempotency_in_progress) > MAX_RECOVERY_STRANDED_IDEMPOTENCY:
            raise RecoveryInvalidRequestError(
                "recovery idempotency evidence exceeds the bounded limit"
            )

    def __repr__(self) -> str:
        return (
            "RecoveryEvidence("
            f"run_id={self.frontier.run.run_id!r}, state={self.frontier.run.state.value!r}, "
            f"work={len(self.frontier.work)}, attempts={len(self.frontier.attempts)}, "
            f"artifacts={len(self.artifacts)}, issues={len(self.integrity_issues)}, "
            f"idempotency={len(self.idempotency_in_progress)})"
        )


@dataclass(frozen=True, slots=True, repr=False)
class RecoveryScan:
    """Immutable classification of one durable recovery frontier."""

    run_id: RunId
    observed_at: UtcTimestamp
    status: RecoveryStatus
    findings: tuple[RecoveryFinding, ...]

    def __post_init__(self) -> None:
        _snapshot_run_id(self.run_id)
        _snapshot_timestamp(self.observed_at)
        if type(self.status) is not RecoveryStatus:
            raise TypeError("recovery status is invalid")
        if type(self.findings) is not tuple:
            raise TypeError("recovery findings must be a tuple")
        keys = [finding.order_key for finding in self.findings]
        if keys != sorted(keys):
            raise ValueError("recovery findings must be deterministically ordered")

    @property
    def recoverable_findings(self) -> tuple[RecoveryFinding, ...]:
        """Return only the findings carrying durable recovery authority."""
        return tuple(finding for finding in self.findings if finding.kind in _RECOVERABLE_KINDS)

    def __repr__(self) -> str:
        return (
            "RecoveryScan("
            f"run_id={self.run_id!r}, status={self.status.value!r}, "
            f"findings={len(self.findings)})"
        )


@dataclass(frozen=True, slots=True, repr=False)
class RecoveryReport:
    """Proven recovery outcome after classification and bounded mutation."""

    before: RecoveryScan
    after: RecoveryScan
    applied: int
    submission_ids: tuple[WriterSubmissionId, ...]

    def __repr__(self) -> str:
        return (
            "RecoveryReport("
            f"run_id={self.before.run_id!r}, applied={self.applied!r}, "
            f"before={self.before.status.value!r}, after={self.after.status.value!r})"
        )


@runtime_checkable
class RecoveryClock(Protocol):
    """Injected exact UTC clock used for lease observation."""

    def now(self) -> UtcTimestamp:
        """Return the current exact UTC timestamp."""
        ...


@runtime_checkable
class RecoveryStateReader(Protocol):
    """Borrowed short-transaction reader for one recovery frontier."""

    def read(self, run_id: RunId, /) -> RecoveryEvidence:
        """Read one coherent durable recovery frontier."""
        ...


@runtime_checkable
class RecoveryWriter(Protocol):
    """Borrowed transactional-writer surface without lifecycle ownership."""

    def submit(
        self,
        command: WriterCommand,
        *,
        timeout_seconds: float,
    ) -> WriterTicket:
        """Submit one closed recovery command."""
        ...


class StartupRecoveryScanner:
    """Classify durable states and apply bounded, restart-safe recovery.

    Classification reads only durable SQLite and artifact evidence; process
    exit, memory, notification, and acknowledgement state are never consulted.
    Scan is read-only and deterministic; ``recover`` applies at most the
    expired-lease findings through the transactional writer, and ambiguous
    evidence fails closed to prevent unsafe scheduling.
    """

    __slots__ = (
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
        writer: RecoveryWriter,
        reader: RecoveryStateReader,
        clock: RecoveryClock,
        *,
        settings: RecoverySettings | None = None,
    ) -> None:
        writer_value = cast(object, writer)
        reader_value = cast(object, reader)
        clock_value = cast(object, clock)
        if not isinstance(writer_value, RecoveryWriter):
            raise TypeError("recovery writer must provide transactional submit")
        if not isinstance(reader_value, RecoveryStateReader):
            raise TypeError("recovery reader must provide a coherent read")
        if not isinstance(clock_value, RecoveryClock):
            raise TypeError("recovery clock must provide exact UTC time")
        selected_settings = RecoverySettings() if settings is None else settings
        if type(selected_settings) is not RecoverySettings:
            raise TypeError("recovery settings must use RecoverySettings")
        self._writer = writer_value
        self._reader = reader_value
        self._clock = clock_value
        self._settings = selected_settings
        self._lifecycle_lock = Lock()
        self._operation_lock = Lock()
        self._uncertain = False

    def scan(self, run_id: RunId) -> RecoveryScan:
        """Classify one durable frontier without mutating anything."""
        clean_run_id = _snapshot_run_id(run_id)
        if not self._operation_lock.acquire(blocking=False):
            raise RecoveryBusyError("recovery scanner already has an active operation")
        try:
            with self._lifecycle_lock:
                if self._uncertain:
                    raise RecoveryOutcomeUnknownError(
                        "recovery requires durable outcome inspection"
                    )
            evidence = self._read_evidence(clean_run_id)
            observed_at = self._now(evidence.frontier.run)
            scan = _classify(evidence, observed_at, self._settings.max_findings)
            return scan
        finally:
            self._operation_lock.release()

    def recover(self, run_id: RunId, *, correlation_id: str | None = None) -> RecoveryReport:
        """Classify and apply the bounded recovery actions for one run."""
        correlation = _validate_correlation_id(correlation_id)
        clean_run_id = _snapshot_run_id(run_id)
        if not self._operation_lock.acquire(blocking=False):
            raise RecoveryBusyError("recovery scanner already has an active operation")
        try:
            with self._lifecycle_lock:
                if self._uncertain:
                    raise RecoveryOutcomeUnknownError(
                        "recovery requires durable outcome inspection"
                    )
            evidence = self._read_evidence(clean_run_id)
            observed_at = self._now(evidence.frontier.run)
            before = _classify(evidence, observed_at, self._settings.max_findings)
            if before.status is RecoveryStatus.AMBIGUOUS:
                raise RecoveryAmbiguousError(
                    "ambiguous recovery evidence prevents unsafe scheduling"
                )
            _require_headroom(evidence.frontier, len(before.recoverable_findings))
            submission_ids: list[WriterSubmissionId] = []
            frontier = evidence.frontier
            work_by_id = {work.work_item_id: work for work in frontier.work}
            applied = 0
            for finding in before.recoverable_findings:
                work_item_id = finding.work_item_id
                assert work_item_id is not None
                work = work_by_id.get(work_item_id)
                node = (
                    next(
                        (item for item in frontier.nodes if item.node_id == work.node_id),
                        None,
                    )
                    if work is not None
                    else None
                )
                if work is None or node is None:
                    raise RecoveryProtocolError("recovery finding lacks durable parents")
                command = _recovery_command(
                    frontier,
                    work,
                    node,
                    observed_at,
                    correlation,
                )
                _submission_id = self._execute(command)
                submission_ids.append(_submission_id)
                applied += 1
                frontier = _advanced_frontier(frontier, node.node_id)
            after_evidence = self._read_evidence(clean_run_id)
            after = _classify(after_evidence, observed_at, self._settings.max_findings)
            return RecoveryReport(before, after, applied, tuple(submission_ids))
        finally:
            self._operation_lock.release()

    def _read_evidence(self, run_id: RunId) -> RecoveryEvidence:
        try:
            value = self._reader.read(run_id)
        except RecoveryCorruptionError:
            raise
        except (ExecutionRepositoryError, ConsistencyRepositoryError) as error:
            raise RecoveryCorruptionError("recovery evidence is corrupt") from error
        except Exception:
            raise RecoveryStateReadError("recovery evidence read failed") from None
        invalid = False
        try:
            clean = _snapshot_evidence(value)
        except Exception:
            invalid = True
            clean = None
        if invalid or clean is None:
            raise RecoveryProtocolError("recovery evidence is invalid")
        return clean

    def _now(self, run: RunRecord) -> UtcTimestamp:
        failed = False
        try:
            value = self._clock.now()
        except Exception:
            failed = True
            value = None
        if failed:
            raise RecoveryClockError("recovery clock failed")
        invalid = False
        try:
            timestamp = _snapshot_timestamp(value)
        except Exception:
            invalid = True
            timestamp = None
        if invalid or timestamp is None:
            raise RecoveryClockError("recovery clock returned an invalid time")
        evidence = tuple(
            item
            for item in (
                run.created_at,
                run.started_at,
                run.cancellation_requested_at,
                run.recovery_started_at,
                run.recovered_at,
                run.finished_at,
            )
            if item is not None
        )
        if evidence and timestamp < max(evidence):
            raise RecoveryClockError("recovery clock is behind durable run time")
        return timestamp

    def _execute(self, command: RecoverExpiredWork) -> WriterSubmissionId:
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
            raise RecoveryAdmissionError("recovery writer admission failed")
        if unexpected or ticket is None:
            self._mark_uncertain()
            raise RecoveryProtocolError("recovery admission outcome is unknown")
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
            raise RecoveryRejectedError("recovery command was not committed")
        if unknown:
            self._mark_uncertain()
            raise RecoveryOutcomeUnknownError("recovery durable outcome is unknown")
        invalid = False
        try:
            _validate_receipt(receipt, submission_id, command)
        except Exception:
            invalid = True
        if invalid:
            self._mark_uncertain()
            raise RecoveryProtocolError("recovery writer receipt is invalid")
        return submission_id

    def _mark_uncertain(self) -> None:
        try:
            with self._lifecycle_lock:
                self._uncertain = True
        except BaseException:
            self._uncertain = True


def _classify(
    evidence: RecoveryEvidence,
    observed_at: UtcTimestamp,
    max_findings: int,
) -> RecoveryScan:
    findings: list[RecoveryFinding] = []
    run = evidence.frontier.run
    run_id = run.run_id
    status = RecoveryStatus.HEALTHY
    artifact_keys = {
        (str(record.node_id), str(record.partition_key)) for record in evidence.artifacts
    }
    heads = dict(evidence.frontier.checkpoint_versions)
    attempts_by_work: dict[WorkItemId, list[WorkAttemptRecord]] = {}
    for attempt in evidence.frontier.attempts:
        attempts_by_work.setdefault(attempt.work_item_id, []).append(attempt)
    for work in evidence.frontier.work:
        head_version = heads.get(work.work_item_id)
        if head_version is None or head_version != work.expected_checkpoint_version:
            findings.append(
                RecoveryFinding(
                    RecoveryFindingKind.CHECKPOINT_FRONTIER_MISMATCH,
                    run_id,
                    work.node_id,
                    work.work_item_id,
                )
            )
            status = _worse(status, RecoveryStatus.AMBIGUOUS)
        attempts = attempts_by_work.get(work.work_item_id, [])
        if len(attempts) != work.completed_attempt_count:
            findings.append(
                RecoveryFinding(
                    RecoveryFindingKind.ATTEMPT_HISTORY_MISMATCH,
                    run_id,
                    work.node_id,
                    work.work_item_id,
                )
            )
            status = _worse(status, RecoveryStatus.AMBIGUOUS)
        if work.state is WorkItemState.PENDING:
            findings.append(
                RecoveryFinding(
                    RecoveryFindingKind.WORK_PENDING, run_id, work.node_id, work.work_item_id
                )
            )
        elif work.state is WorkItemState.RETRY_WAIT:
            findings.append(
                RecoveryFinding(
                    RecoveryFindingKind.WORK_RETRY_WAITING, run_id, work.node_id, work.work_item_id
                )
            )
        elif work.state is WorkItemState.RUNNING:
            if work.lease_expires_at is None or work.active_attempt_number is None:
                findings.append(
                    RecoveryFinding(
                        RecoveryFindingKind.WORK_RUNNING_WITHOUT_LEASE,
                        run_id,
                        work.node_id,
                        work.work_item_id,
                    )
                )
                status = _worse(status, RecoveryStatus.AMBIGUOUS)
            elif work.lease_expires_at.value > observed_at.value:
                findings.append(
                    RecoveryFinding(
                        RecoveryFindingKind.WORK_ACTIVE_LEASE,
                        run_id,
                        work.node_id,
                        work.work_item_id,
                    )
                )
                status = _worse(status, RecoveryStatus.ACTIVE)
            else:
                has_artifact = (str(work.node_id), str(work.partition_key)) in artifact_keys
                if head_version is not None and head_version >= 1:
                    findings.append(
                        RecoveryFinding(
                            RecoveryFindingKind.CHECKPOINT_FRONTIER_MISMATCH,
                            run_id,
                            work.node_id,
                            work.work_item_id,
                        )
                    )
                    status = _worse(status, RecoveryStatus.AMBIGUOUS)
                elif has_artifact:
                    findings.append(
                        RecoveryFinding(
                            RecoveryFindingKind.WORK_EXPIRED_WITH_COMMITTED_ARTIFACT,
                            run_id,
                            work.node_id,
                            work.work_item_id,
                            detail="committed artifact identity prevents duplicate effects",
                        )
                    )
                    status = _worse(status, RecoveryStatus.RECOVERABLE)
                else:
                    findings.append(
                        RecoveryFinding(
                            RecoveryFindingKind.WORK_EXPIRED_NO_EFFECT,
                            run_id,
                            work.node_id,
                            work.work_item_id,
                        )
                    )
                    status = _worse(status, RecoveryStatus.RECOVERABLE)
        else:
            if work.state is WorkItemState.SUCCEEDED and (head_version is None or head_version < 1):
                findings.append(
                    RecoveryFinding(
                        RecoveryFindingKind.CHECKPOINT_FRONTIER_MISMATCH,
                        run_id,
                        work.node_id,
                        work.work_item_id,
                    )
                )
                status = _worse(status, RecoveryStatus.AMBIGUOUS)
            findings.append(
                RecoveryFinding(
                    RecoveryFindingKind.WORK_COMMITTED, run_id, work.node_id, work.work_item_id
                )
            )

    if run.state in _TERMINAL_RUN_STATES:
        if run.state is RunState.CANCELLED:
            # Cancellation deliberately preserves work that was never admitted
            # (PENDING/RETRY_WAIT) as inert durable evidence. Only a surviving
            # owned claim is active after the terminal cancellation arrow.
            active = any(work.state is WorkItemState.RUNNING for work in evidence.frontier.work)
        else:
            active = any(
                work.state not in _TERMINAL_WORK for work in evidence.frontier.work
            ) or any(
                node.status in {RunNodeStatus.PENDING, RunNodeStatus.RUNNING}
                for node in evidence.frontier.nodes
            )
        if active:
            findings.append(
                RecoveryFinding(RecoveryFindingKind.RUN_TERMINAL_WITH_ACTIVE_WORK, run_id)
            )
            status = _worse(status, RecoveryStatus.AMBIGUOUS)
        else:
            findings.append(RecoveryFinding(RecoveryFindingKind.RUN_TERMINAL, run_id))
    elif run.state is RunState.QUEUED:
        findings.append(RecoveryFinding(RecoveryFindingKind.RUN_QUEUED, run_id))
    elif run.state in {RunState.PAUSING, RunState.PAUSED, RunState.RESUMING}:
        findings.append(RecoveryFinding(RecoveryFindingKind.RUN_PAUSED, run_id))

    try:
        RunStatisticsSourceSnapshot(
            run,
            evidence.frontier.nodes,
            evidence.frontier.work,
            evidence.frontier.attempts,
        )
    except RunStatisticsError:
        findings.append(RecoveryFinding(RecoveryFindingKind.AGGREGATE_MISMATCH, run_id))
        status = _worse(status, RecoveryStatus.AMBIGUOUS)
    except Exception:
        raise RecoveryProtocolError("recovery aggregate validation input is invalid") from None

    for issue in evidence.integrity_issues:
        mapped = _INTEGRITY_KIND_MAP[issue.kind.value]
        findings.append(
            RecoveryFinding(
                mapped,
                run_id,
                None,
                None,
                None if issue.artifact_id is None else str(issue.artifact_id),
                None if issue.relative_path is None else str(issue.relative_path.value),
            )
        )
        status = _worse(status, RecoveryStatus.AMBIGUOUS)

    for record in evidence.idempotency_in_progress:
        if record.status is not IdempotencyStatus.IN_PROGRESS:
            findings.append(RecoveryFinding(RecoveryFindingKind.EVENT_FRONTIER_CORRUPT, run_id))
            status = _worse(status, RecoveryStatus.AMBIGUOUS)
            continue
        findings.append(
            RecoveryFinding(
                RecoveryFindingKind.STRANDED_IDEMPOTENCY,
                run_id,
                detail="in-progress reservation preserved as evidence",
            )
        )

    ordered = tuple(sorted(findings, key=lambda finding: finding.order_key))
    if len(ordered) > max_findings:
        raise RecoveryInvalidRequestError("recovery findings exceed the bounded limit")
    return RecoveryScan(run_id, observed_at, status, ordered)


_TERMINAL_WORK = frozenset(
    {
        WorkItemState.SUCCEEDED,
        WorkItemState.QUARANTINED,
        WorkItemState.FAILED,
        WorkItemState.CANCELLED,
    }
)


def _require_headroom(frontier: FinalizationEvidence, arrows: int) -> None:
    """Reject frontiers that cannot advance every recovery command at once."""
    if arrows <= 0:
        return
    maximum = MAX_CONSISTENCY_SEQUENCE - arrows
    if (
        frontier.run.row_version > maximum
        or frontier.next_event_sequence.number > maximum
        or frontier.event_counter_row_version > maximum
    ):
        raise RecoveryInvalidRequestError("recovery frontier cannot advance its commands")


def _worse(current: RecoveryStatus, candidate: RecoveryStatus) -> RecoveryStatus:
    order = {
        RecoveryStatus.HEALTHY: 0,
        RecoveryStatus.ACTIVE: 1,
        RecoveryStatus.RECOVERABLE: 2,
        RecoveryStatus.AMBIGUOUS: 3,
    }
    return candidate if order[candidate] > order[current] else current


def _recovery_command(
    frontier: FinalizationEvidence,
    work: WorkItemRecord,
    node: RunNodeRecord,
    observed_at: UtcTimestamp,
    correlation_id: str | None,
) -> RecoverExpiredWork:
    attempt = work.active_attempt_number
    assert attempt is not None
    event = PendingExecutionEvent(
        RECOVERY_LEASE_EVENT_KIND,
        observed_at,
        EventSubjectKind.WORK_ITEM,
        work.work_item_id,
        correlation_id,
        RECOVERY_EVENT_PAYLOAD_SCHEMA_VERSION,
        RedactedDocument.from_mapping(
            {
                "attempt_number": attempt.number,
                "lease_expires_at": str(work.lease_expires_at),
                "node_id": str(work.node_id),
                "runner_kind": work.active_runner_kind or "",
            }
        ),
    )
    return RecoverExpiredWork(
        frontier.run.run_id,
        work.node_id,
        work.work_item_id,
        work.row_version,
        attempt,
        observed_at,
        observed_at,
        None,
        node.row_version,
        frontier.run.row_version,
        EventAppendRequest(
            EventSequence(frontier.next_event_sequence.number),
            frontier.event_counter_row_version,
            event,
        ),
    )


def _advanced_run(run: RunRecord, row_version: int) -> RunRecord:
    return RunRecord(
        run.run_id,
        run.pipeline_id,
        run.pipeline_version,
        run.runner_kind,
        run.runner_configuration,
        run.state,
        row_version,
        run.scenario_seed,
        run.created_at,
        run.started_at,
        run.finished_at,
        run.cancellation_requested_at,
        run.recovery_started_at,
        run.recovered_at,
        run.final_reconciliation_fingerprint,
    )


def _advanced_frontier(frontier: FinalizationEvidence, node_id: NodeId) -> FinalizationEvidence:
    return FinalizationEvidence(
        _advanced_run(frontier.run, frontier.run.row_version + 1),
        EventSequence(frontier.next_event_sequence.number + 1),
        frontier.event_counter_row_version + 1,
        tuple(node if node.node_id != node_id else _advanced_node(node) for node in frontier.nodes),
        frontier.work,
        frontier.attempts,
        frontier.checkpoint_versions,
    )


def _advanced_node(node: RunNodeRecord) -> RunNodeRecord:
    return RunNodeRecord(
        node.run_id,
        node.node_id,
        node.status,
        node.row_version + 1,
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
        node.duration,
        node.started_at,
        node.finished_at,
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
        raise RecoveryProtocolError("recovery ticket identity is invalid")
    return clean


def _validate_receipt(
    receipt: object,
    submission_id: WriterSubmissionId,
    command: RecoverExpiredWork,
) -> None:
    if type(receipt) is not WriterReceipt:
        raise RecoveryProtocolError("recovery receipt type is invalid")
    result = receipt.result
    if (
        receipt.submission_id != submission_id
        or receipt.command_kind is not command.kind
        or receipt.run_id != command.run_id
        or type(receipt.contention_attempts) is not int
        or not 0 <= receipt.contention_attempts <= MAX_RECOVERY_CONTENTION_ATTEMPTS
        or receipt.mutated is not True
        or type(result) is not RecoverExpiredWorkResult
    ):
        raise RecoveryProtocolError("recovery receipt does not match command")
    work = result.completed.work_item
    attempt = result.completed.attempt
    if (
        work.work_item_id != command.work_item_id
        or work.state is not WorkItemState.RETRY_WAIT
        or work.row_version != command.expected_work_row_version + 1
        or attempt.attempt_number != command.expected_attempt_number
        or attempt.outcome is not AttemptOutcome.LEASE_EXPIRED
        or result.run.run_id != command.run_id
        or result.run.row_version != command.expected_run_row_version + 1
    ):
        raise RecoveryProtocolError("recovery receipt evidence is inconsistent")
    pending = command.event.event
    record = ExecutionEventRecord(
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
    expected = ExecutionEventBatch(
        (record,),
        command.event.expected_next_sequence.advance(1),
        command.event.expected_counter_row_version + 1,
    )
    if result.events != expected:
        raise RecoveryProtocolError("recovery event evidence is inconsistent")


def _snapshot_evidence(value: object) -> RecoveryEvidence:
    if type(value) is not RecoveryEvidence:
        raise TypeError("recovery evidence has an invalid type")
    from paritygrid.application.execution.finalization import (
        snapshot_finalization_evidence,
    )

    return RecoveryEvidence(
        snapshot_finalization_evidence(value.frontier),
        tuple(_snapshot_manifest(record) for record in value.artifacts),
        tuple(_snapshot_issue(issue) for issue in value.integrity_issues),
        tuple(_snapshot_idempotency(record) for record in value.idempotency_in_progress),
    )


def _snapshot_manifest(value: object) -> ArtifactManifestRecord:
    from paritygrid.application.ports.artifacts import ArtifactRelativePath
    from paritygrid.domain.models import ArtifactId
    from paritygrid.domain.pipeline import PartitionKey

    if type(value) is not ArtifactManifestRecord:
        raise TypeError("recovery manifest evidence is invalid")
    manifest = value
    return ArtifactManifestRecord(
        artifact_id=ArtifactId(manifest.artifact_id.value),
        run_id=_snapshot_run_id(manifest.run_id),
        node_id=_snapshot_node_id(manifest.node_id),
        partition_key=PartitionKey(manifest.partition_key.value),
        relative_path=ArtifactRelativePath(manifest.relative_path.value),
        media_type=_exact_text(manifest.media_type, "manifest media type"),
        schema_version=manifest.schema_version,
        byte_size=manifest.byte_size,
        row_count=manifest.row_count,
        sha256=_exact_text(manifest.sha256, "manifest digest"),
        created_at=_snapshot_timestamp(manifest.created_at),
    )


def _snapshot_issue(value: object) -> ArtifactIntegrityIssue:
    from paritygrid.application.ports.artifact_integrity import ArtifactIntegrityIssueKind
    from paritygrid.application.ports.artifacts import ArtifactRelativePath
    from paritygrid.domain.models import ArtifactId

    if type(value) is not ArtifactIntegrityIssue:
        raise TypeError("recovery integrity evidence is invalid")
    issue = value
    kind = issue.kind
    if type(kind) is not ArtifactIntegrityIssueKind:
        raise TypeError("recovery integrity kind is invalid")
    relative_path = issue.relative_path
    artifact_id = issue.artifact_id
    return ArtifactIntegrityIssue(
        kind,
        None
        if relative_path is None
        else ArtifactRelativePath(_exact_text(relative_path.value, "integrity path")),
        None if artifact_id is None else ArtifactId(artifact_id.value),
        None
        if issue.observed_path_sha256 is None
        else _exact_text(issue.observed_path_sha256, "integrity digest"),
    )


def _snapshot_idempotency(value: object) -> IdempotencyRecord:
    if type(value) is not IdempotencyRecord:
        raise TypeError("recovery idempotency evidence is invalid")
    record = value
    return IdempotencyRecord(
        _exact_text(record.scope, "idempotency scope"),
        _exact_text(record.key, "idempotency key"),
        record.status,
        record.response_schema_version,
        None if record.response is None else _snapshot_redacted_document(record.response),
        _snapshot_timestamp(record.created_at),
        _snapshot_timestamp(record.updated_at),
        None if record.completed_at is None else _snapshot_timestamp(record.completed_at),
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


def _snapshot_timestamp(value: object) -> UtcTimestamp:
    if (
        type(value) is not UtcTimestamp
        or type(value.value) is not datetime
        or value.value.tzinfo is not UTC
    ):
        raise TypeError("timestamp evidence is invalid")
    return UtcTimestamp(value.value)


def _exact_text(value: object, subject: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{subject} must be text")
    return value


def _validate_detail(value: object) -> None:
    if type(value) is not str:
        raise TypeError("recovery detail must be text")
    if not 1 <= len(value) <= 256:
        raise ValueError("recovery detail is outside the supported range")


def _validate_correlation_id(value: object) -> str | None:
    if value is None:
        return None
    if (
        type(value) is not str
        or not 1 <= len(value) <= MAX_RECOVERY_CORRELATION_ID_LENGTH
        or _PORTABLE_IDENTITY.fullmatch(value) is None
    ):
        raise RecoveryInvalidRequestError("recovery correlation identifier is invalid")
    return value


def _validate_timeout(value: object, subject: str) -> None:
    if type(value) is not float:
        raise TypeError(f"{subject} must be a float")
    if not 0 <= value <= MAX_RECOVERY_TIMEOUT_SECONDS:
        raise ValueError(f"{subject} is outside the supported range")


__all__ = [
    "MAX_RECOVERY_CONTENTION_ATTEMPTS",
    "MAX_RECOVERY_CORRELATION_ID_LENGTH",
    "MAX_RECOVERY_FINDINGS",
    "MAX_RECOVERY_STRANDED_IDEMPOTENCY",
    "MAX_RECOVERY_TIMEOUT_SECONDS",
    "RECOVERY_EVENT_PAYLOAD_SCHEMA_VERSION",
    "RECOVERY_LEASE_EVENT_KIND",
    "RecoveryAdmissionError",
    "RecoveryAmbiguousError",
    "RecoveryBusyError",
    "RecoveryClock",
    "RecoveryClockError",
    "RecoveryCorruptionError",
    "RecoveryError",
    "RecoveryEvidence",
    "RecoveryFinding",
    "RecoveryFindingKind",
    "RecoveryInvalidRequestError",
    "RecoveryOutcomeUnknownError",
    "RecoveryProtocolError",
    "RecoveryRejectedError",
    "RecoveryReport",
    "RecoveryScan",
    "RecoverySettings",
    "RecoveryStateReadError",
    "RecoveryStateReader",
    "RecoveryStatus",
    "RecoveryWriter",
    "StartupRecoveryScanner",
]
