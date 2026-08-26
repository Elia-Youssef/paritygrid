"""Durable concurrent-run recovery and frontier reconstruction (P7.10).

Startup recovery for a concurrent run reconstructs
:class:`SchedulerFrontierV2` exclusively from durable evidence — the
SQLite run and node records, work-item states and lease facts, the
event frontier, in-progress idempotency reservations, and
artifact-integrity findings.  No thread, task, queue, future, or any
other live concurrency object is ever a recovery source or enters a
recovery fact.

Expired claims are resolved through the closed ``RecoverExpiredWork``
writer command with exact row-version fencing.  Non-expired leases are
never assumed to have vanished: recovery reports them explicitly and
stays recovery-required until they expire or are resolved, according
to the captured durable policy.  Integrity ambiguity fails closed.
One service instance performs recovery under a non-blocking exclusive
lock and issues a fresh ownership token so the next coordinator can
fence late results from earlier ownership.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from threading import Lock
from typing import Protocol, cast, runtime_checkable

from paritygrid.application.execution.concurrent_scheduler import (
    SCHEDULER_FRONTIER_VERSION,
    FrontierRetryWait,
    FrontierWorkState,
    SchedulerFrontierV2,
    WorkIdentity,
)
from paritygrid.application.execution.runner_contract import (
    ContractLifecycleState,
    ControlGeneration,
)
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.execution import RunNodeRecord, RunRecord
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    WriterCommand,
    WriterSubmissionId,
    WriterTicket,
)
from paritygrid.application.writes.execution import (
    RecoverExpiredWork,
    RecoverExpiredWorkResult,
)
from paritygrid.domain.execution import RunState, WorkItemState
from paritygrid.domain.models import (
    AttemptNumber,
    NodeId,
    RunId,
    UtcTimestamp,
    WorkItemId,
)

CONCURRENT_RECOVERY_VERSION = 1
RECOVERY_EXPIRED_EVENT_KIND = "work_lease_expired"
RECOVERY_EVENT_PAYLOAD_SCHEMA_VERSION = 1
MAX_RECOVERY_WORK_ITEMS = 65_536
MAX_RECOVERY_TIMEOUT_SECONDS = 86_400.0
_MAX_RECOVERY_MICROS = 86_400_000_000
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

_WORK_STATE_MAP: dict[WorkItemState, FrontierWorkState] = {
    WorkItemState.PENDING: FrontierWorkState.BLOCKED,
    WorkItemState.RUNNING: FrontierWorkState.ADMITTED,
    WorkItemState.RETRY_WAIT: FrontierWorkState.RETRY_WAIT,
    WorkItemState.SUCCEEDED: FrontierWorkState.SUCCEEDED,
    WorkItemState.QUARANTINED: FrontierWorkState.QUARANTINED,
    WorkItemState.FAILED: FrontierWorkState.FAILED,
    WorkItemState.CANCELLED: FrontierWorkState.CANCELLED,
}
_RECOVERY_RUN_STATES = frozenset(
    {
        RunState.RUNNING,
        RunState.PAUSING,
        RunState.PAUSED,
        RunState.RESUMING,
        RunState.CANCELLING,
    }
)


class ConcurrentRecoveryError(RuntimeError):
    """Base failure for concurrent recovery."""


class ConcurrentRecoveryInvalidRequestError(ConcurrentRecoveryError):
    """A recovery request violated the durable contract."""


class ConcurrentRecoveryStateReadError(ConcurrentRecoveryError):
    """Durable recovery evidence could not be read."""


class ConcurrentRecoveryAmbiguousError(ConcurrentRecoveryError):
    """Durable evidence is ambiguous and fails closed."""


class ConcurrentRecoveryBusyError(ConcurrentRecoveryError):
    """Another recovery owns the exclusive coordinator slot."""


class ConcurrentRecoveryNonExpiredLeaseError(ConcurrentRecoveryError):
    """Non-expired leases stayed unresolved under the durable policy."""


class ConcurrentRecoveryOutcomeUnknownError(ConcurrentRecoveryError):
    """A recovery writer outcome stayed unknown."""


class NonExpiredLeasePolicy(StrEnum):
    """Explicit durable policy for leases that have not expired."""

    WAIT_FOR_EXPIRY = "wait_for_expiry"
    REMAIN_RECOVERY_REQUIRED = "remain_recovery_required"


@dataclass(frozen=True, slots=True)
class RecoveryWorkEvidence:
    """Durable facts for one work item as recovery read them."""

    node_id: str
    partition_key: str
    work_item_id: str
    state: str
    row_version: int
    completed_attempt_count: int
    lease_owner: str | None
    lease_expires_at_micros: int | None
    retry_available_at_micros: int | None
    active_attempt_number: int | None

    def __post_init__(self) -> None:
        for field in ("node_id", "partition_key", "work_item_id"):
            value = getattr(self, field)
            if type(value) is not str or not 1 <= len(value) <= 128:
                raise ConcurrentRecoveryInvalidRequestError(f"recovery work {field} is invalid")
        if type(self.row_version) is not int or not 1 <= self.row_version <= 2_147_483_647:
            raise ConcurrentRecoveryInvalidRequestError("recovery row version is invalid")
        if (
            type(self.completed_attempt_count) is not int
            or not 0 <= self.completed_attempt_count <= 2_147_483_647
        ):
            raise ConcurrentRecoveryInvalidRequestError("recovery attempt count is invalid")
        for optional in (self.lease_expires_at_micros, self.retry_available_at_micros):
            if optional is not None and (type(optional) is not int or optional < 0):
                raise ConcurrentRecoveryInvalidRequestError(
                    "recovery optional micros evidence is invalid"
                )
        if self.active_attempt_number is not None and (
            type(self.active_attempt_number) is not int
            or not 1 <= self.active_attempt_number <= 2_147_483_647
        ):
            raise ConcurrentRecoveryInvalidRequestError("recovery attempt evidence is invalid")


@dataclass(frozen=True, slots=True)
class ConcurrentRecoveryEvidence:
    """One coherent durable snapshot for a concurrent run."""

    run: RunRecord
    nodes: tuple[RunNodeRecord, ...]
    work: tuple[RecoveryWorkEvidence, ...]
    next_event_sequence: int
    event_counter_row_version: int
    in_progress_idempotency: int
    integrity_issue_count: int

    def __post_init__(self) -> None:
        if type(self.run) is not RunRecord:
            raise ConcurrentRecoveryStateReadError("recovery run must use RunRecord")
        if type(self.nodes) is not tuple or type(self.work) is not tuple:
            raise TypeError("recovery evidence collections must be tuples")
        if len(self.work) > MAX_RECOVERY_WORK_ITEMS:
            raise ConcurrentRecoveryInvalidRequestError(
                "recovery work evidence exceeds the supported bound"
            )
        for counter in (self.in_progress_idempotency, self.integrity_issue_count):
            if type(counter) is not int or not 0 <= counter <= MAX_RECOVERY_WORK_ITEMS:
                raise ConcurrentRecoveryInvalidRequestError(
                    "recovery counters are outside the supported range"
                )


@runtime_checkable
class ConcurrentRecoveryClock(Protocol):
    """Injected clock contract for expiry decisions."""

    def now(self) -> UtcTimestamp: ...


@runtime_checkable
class ConcurrentRecoveryStateReader(Protocol):
    """Durable evidence reader for one concurrent run."""

    def read(self, run_id: str, /) -> ConcurrentRecoveryEvidence: ...


@runtime_checkable
class ConcurrentRecoveryWriter(Protocol):
    """Borrowed transactional-writer surface for recovery commands."""

    def submit(self, command: WriterCommand, *, timeout_seconds: float) -> WriterTicket: ...


@dataclass(frozen=True, slots=True)
class ConcurrentRecoverySettings:
    """Bounded timeouts for recovery commands."""

    admission_timeout_seconds: float = 5.0
    result_timeout_seconds: float = 60.0

    def __post_init__(self) -> None:
        for name in ("admission_timeout_seconds", "result_timeout_seconds"):
            value = getattr(self, name)
            if type(value) is not float:
                raise TypeError(f"{name} must be a float second count")
            if not math.isfinite(value) or not 0.0 < value <= MAX_RECOVERY_TIMEOUT_SECONDS:
                raise ConcurrentRecoveryInvalidRequestError(f"{name} is outside the range")


@dataclass(frozen=True, slots=True)
class LeasedWorkClassification:
    """Classified lease evidence for one in-flight work item."""

    identity: WorkIdentity
    work_item_id: str
    row_version: int
    attempt_number: int
    lease_owner: str
    expires_at_micros: int

    def __post_init__(self) -> None:
        if type(self.identity) is not WorkIdentity:
            raise TypeError("lease classification must use WorkIdentity")


@dataclass(frozen=True, slots=True)
class ConcurrentRecoveryScan:
    """Read-only classification of one concurrent run's durable state."""

    run_id: str
    plan_fingerprint: str
    run_state: str
    expired_leases: tuple[LeasedWorkClassification, ...]
    non_expired_leases: tuple[LeasedWorkClassification, ...]
    ambiguous: bool
    ambiguity_reason: str | None
    owner_token: int

    def __post_init__(self) -> None:
        if type(self.owner_token) is not int or self.owner_token < 1:
            raise ConcurrentRecoveryInvalidRequestError("recovery owner token is invalid")
        for field in ("expired_leases", "non_expired_leases"):
            items = getattr(self, field)
            if tuple(sorted(items, key=_leased_order_key)) != items:
                raise ConcurrentRecoveryInvalidRequestError(
                    f"recovery {field} must be deterministically sorted"
                )
        if self.ambiguous and not self.ambiguity_reason:
            raise ConcurrentRecoveryInvalidRequestError("ambiguous recovery requires a reason")


@dataclass(frozen=True, slots=True)
class ConcurrentRecoveryReport:
    """Outcome of one exclusive recovery pass."""

    scan: ConcurrentRecoveryScan
    recovered_work: tuple[WorkIdentity, ...]
    frontier: SchedulerFrontierV2
    submission_ids: tuple[WriterSubmissionId, ...]

    def __post_init__(self) -> None:
        if type(self.frontier) is not SchedulerFrontierV2:
            raise TypeError("recovery report frontier must use SchedulerFrontierV2")
        if type(self.recovered_work) is not tuple or type(self.submission_ids) is not tuple:
            raise TypeError("recovery report collections must be tuples")


class ConcurrentRecoveryService:
    """Exclusive durable recovery for one concurrent run at a time."""

    __slots__ = ("_clock", "_lock", "_owner_counter", "_reader", "_settings", "_writer")

    def __init__(
        self,
        writer: ConcurrentRecoveryWriter,
        reader: ConcurrentRecoveryStateReader,
        clock: ConcurrentRecoveryClock,
        *,
        settings: ConcurrentRecoverySettings | None = None,
    ) -> None:
        writer_value = cast(object, writer)
        if not isinstance(writer_value, ConcurrentRecoveryWriter):
            raise TypeError("recovery writer must implement the writer protocol")
        reader_value = cast(object, reader)
        if not isinstance(reader_value, ConcurrentRecoveryStateReader):
            raise TypeError("recovery reader must implement the evidence reader protocol")
        clock_value = cast(object, clock)
        if not isinstance(clock_value, ConcurrentRecoveryClock):
            raise TypeError("recovery clock must implement the clock protocol")
        if settings is not None and type(settings) is not ConcurrentRecoverySettings:
            raise TypeError("recovery settings must use ConcurrentRecoverySettings")
        self._writer = writer_value
        self._reader = reader_value
        self._clock = clock_value
        self._settings = settings or ConcurrentRecoverySettings()
        self._lock = Lock()
        self._owner_counter = 0

    @property
    def owner_counter(self) -> int:
        """Return the latest issued exclusive ownership token."""
        with self._lock:
            return self._owner_counter

    def scan(
        self,
        *,
        run_id: str,
        plan_fingerprint: str,
        node_order: tuple[str, ...],
        edges: tuple[tuple[str, str], ...],
        partitions_by_node: dict[str, tuple[str, ...]],
    ) -> ConcurrentRecoveryScan:
        """Classify durable evidence read-only under exclusive ownership."""
        _require_topology(node_order, edges, partitions_by_node)
        owner = self._claim_ownership()
        evidence = self._read_evidence(run_id)
        _require_evidence_covers_plan(run_id, node_order, partitions_by_node, evidence)
        expired, non_expired, ambiguous, reason = _classify_leases(evidence, self._now_micros())
        return ConcurrentRecoveryScan(
            run_id=run_id,
            plan_fingerprint=plan_fingerprint,
            run_state=evidence.run.state.value,
            expired_leases=expired,
            non_expired_leases=non_expired,
            ambiguous=ambiguous,
            ambiguity_reason=reason,
            owner_token=owner,
        )

    def _claim_ownership(self) -> int:
        """Issue one exclusive ownership token, refusing overlap."""
        if not self._lock.acquire(blocking=False):
            raise ConcurrentRecoveryBusyError("another recovery owns this coordinator")
        try:
            self._owner_counter += 1
            return self._owner_counter
        finally:
            self._lock.release()

    def recover(
        self,
        *,
        run_id: str,
        plan_fingerprint: str,
        node_order: tuple[str, ...],
        edges: tuple[tuple[str, str], ...],
        partitions_by_node: dict[str, tuple[str, ...]],
        control_generation: int,
        non_expired_policy: NonExpiredLeasePolicy = (
            NonExpiredLeasePolicy.REMAIN_RECOVERY_REQUIRED
        ),
        retry_delay_micros: int = 0,
    ) -> ConcurrentRecoveryReport:
        """Resolve expired claims durably and rebuild the scheduler frontier.

        Expired claims are fenced and resolved through
        ``RecoverExpiredWork``; non-expired leases follow the explicit
        policy and never vanish silently; ambiguity fails closed.  The
        returned frontier is rebuilt from post-command durable evidence
        only.
        """

        if type(control_generation) is not int or not 1 <= control_generation <= 2_147_483_647:
            raise ConcurrentRecoveryInvalidRequestError("recovery control generation is invalid")
        if (
            type(retry_delay_micros) is not int
            or not 0 <= retry_delay_micros <= _MAX_RECOVERY_MICROS
        ):
            raise ConcurrentRecoveryInvalidRequestError("recovery retry delay is invalid")
        if type(non_expired_policy) is not NonExpiredLeasePolicy:
            raise TypeError("recovery lease policy must use NonExpiredLeasePolicy")
        _require_topology(node_order, edges, partitions_by_node)
        if not self._lock.acquire(blocking=False):
            raise ConcurrentRecoveryBusyError("another recovery owns this coordinator")
        try:
            self._owner_counter += 1
            owner = self._owner_counter
            evidence = self._read_evidence(run_id)
            _require_evidence_covers_plan(run_id, node_order, partitions_by_node, evidence)
            if evidence.run.state not in _RECOVERY_RUN_STATES:
                raise ConcurrentRecoveryInvalidRequestError(
                    "recovery requires an active or paused run"
                )
            expired, non_expired, ambiguous, reason = _classify_leases(evidence, self._now_micros())
            if ambiguous:
                raise ConcurrentRecoveryAmbiguousError(
                    f"durable recovery evidence is ambiguous: {reason}"
                )
            if non_expired and non_expired_policy is NonExpiredLeasePolicy.REMAIN_RECOVERY_REQUIRED:
                raise ConcurrentRecoveryNonExpiredLeaseError(
                    "non-expired leases stay recovery-required under the durable policy"
                )
            submissions: list[WriterSubmissionId] = []
            recovered: list[WorkIdentity] = []
            node_versions = {str(node.node_id): node.row_version for node in evidence.nodes}
            run_row_version = evidence.run.row_version
            sequence = evidence.next_event_sequence
            counter_version = evidence.event_counter_row_version
            for leased in expired:
                run_row_version, sequence, counter_version, submission = self._resolve_expired(
                    leased=leased,
                    retry_delay_micros=retry_delay_micros,
                    node_versions=node_versions,
                    run_row_version=run_row_version,
                    sequence=sequence,
                    counter_version=counter_version,
                )
                submissions.append(submission)
                recovered.append(leased.identity)
            final = self._read_evidence(run_id)
            _require_evidence_covers_plan(run_id, node_order, partitions_by_node, final)
            frontier = _rebuild_frontier(
                run_id=run_id,
                plan_fingerprint=plan_fingerprint,
                node_order=node_order,
                edges=edges,
                partitions_by_node=partitions_by_node,
                evidence=final,
                control_generation=control_generation,
                ambiguous_reason=None,
            )
            scan = ConcurrentRecoveryScan(
                run_id=run_id,
                plan_fingerprint=plan_fingerprint,
                run_state=final.run.state.value,
                expired_leases=expired,
                non_expired_leases=(),
                ambiguous=False,
                ambiguity_reason=None,
                owner_token=owner,
            )
            return ConcurrentRecoveryReport(
                scan=scan,
                recovered_work=tuple(recovered),
                frontier=frontier,
                submission_ids=tuple(submissions),
            )
        finally:
            self._lock.release()

    def _resolve_expired(
        self,
        *,
        leased: LeasedWorkClassification,
        retry_delay_micros: int,
        node_versions: dict[str, int],
        run_row_version: int,
        sequence: int,
        counter_version: int,
    ) -> tuple[int, int, int, WriterSubmissionId]:
        observed_at = self._require_now()
        retry_available_at = _from_micros(_to_micros(observed_at) + retry_delay_micros)
        command = RecoverExpiredWork(
            run_id=RunId(leased.identity.run_id),
            node_id=NodeId(leased.identity.node_id),
            work_item_id=WorkItemId(leased.work_item_id),
            expected_work_row_version=leased.row_version,
            expected_attempt_number=AttemptNumber(leased.attempt_number),
            observed_at=observed_at,
            retry_available_at=retry_available_at,
            redacted_detail=None,
            expected_node_row_version=node_versions[leased.identity.node_id],
            expected_run_row_version=run_row_version,
            event=EventAppendRequest(
                EventSequence(sequence),
                counter_version,
                PendingExecutionEvent(
                    RECOVERY_EXPIRED_EVENT_KIND,
                    observed_at,
                    EventSubjectKind.WORK_ITEM,
                    WorkItemId(leased.work_item_id),
                    None,
                    RECOVERY_EVENT_PAYLOAD_SCHEMA_VERSION,
                    RedactedDocument.from_mapping(
                        {
                            "attempt_number": leased.attempt_number,
                            "node_id": leased.identity.node_id,
                            "retry_available_at": str(retry_available_at),
                        }
                    ),
                ),
            ),
        )
        try:
            ticket = self._writer.submit(
                command, timeout_seconds=self._settings.admission_timeout_seconds
            )
            receipt = ticket.result(timeout_seconds=self._settings.result_timeout_seconds)
        except ConcurrentRecoveryError:
            raise
        except Exception as error:
            raise ConcurrentRecoveryOutcomeUnknownError(
                "recovery writer outcome is unknown"
            ) from error
        result = receipt.result
        if type(result) is not RecoverExpiredWorkResult:
            raise ConcurrentRecoveryOutcomeUnknownError(
                "recovery writer receipt is not a recovery result"
            )
        node_versions[leased.identity.node_id] = result.node.row_version
        return (
            result.run.row_version,
            result.events.next_sequence.number,
            result.events.counter_row_version,
            receipt.submission_id,
        )

    def _read_evidence(self, run_id: str) -> ConcurrentRecoveryEvidence:
        if type(run_id) is not str or not run_id:
            raise ConcurrentRecoveryInvalidRequestError("recovery run identity is invalid")
        try:
            evidence = self._reader.read(run_id)
        except ConcurrentRecoveryError:
            raise
        except Exception as error:
            raise ConcurrentRecoveryStateReadError(
                "durable recovery evidence could not be read"
            ) from error
        if type(evidence) is not ConcurrentRecoveryEvidence:
            raise ConcurrentRecoveryStateReadError(
                "durable recovery evidence must use ConcurrentRecoveryEvidence"
            )
        return evidence

    def _require_now(self) -> UtcTimestamp:
        now = self._clock.now()
        if type(now) is not UtcTimestamp:
            raise ConcurrentRecoveryStateReadError("recovery clock returned an invalid timestamp")
        return now

    def _now_micros(self) -> int:
        return _to_micros(self._require_now())


def _leased_order_key(leased: LeasedWorkClassification) -> tuple[str, str, str]:
    return WorkIdentity.sort_key(leased.identity)


def _to_micros(value: UtcTimestamp) -> int:
    delta = value.to_datetime() - _EPOCH
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def _from_micros(value: int) -> UtcTimestamp:
    return UtcTimestamp(_EPOCH + timedelta(microseconds=value))


def _classify_leases(
    evidence: ConcurrentRecoveryEvidence,
    now_micros: int,
) -> tuple[
    tuple[LeasedWorkClassification, ...],
    tuple[LeasedWorkClassification, ...],
    bool,
    str | None,
]:
    expired: list[LeasedWorkClassification] = []
    non_expired: list[LeasedWorkClassification] = []
    ambiguous = evidence.integrity_issue_count > 0
    reason = "artifact integrity findings" if ambiguous else None
    if evidence.in_progress_idempotency > 0:
        ambiguous = True
        reason = reason or "in-progress idempotency reservations"
    for item in evidence.work:
        if item.state != WorkItemState.RUNNING.value:
            continue
        if (
            item.lease_owner is None
            or item.lease_expires_at_micros is None
            or item.active_attempt_number is None
        ):
            ambiguous = True
            reason = reason or "running work without complete lease evidence"
            continue
        classification = LeasedWorkClassification(
            identity=WorkIdentity(
                run_id=str(evidence.run.run_id),
                node_id=item.node_id,
                partition_key=item.partition_key,
            ),
            work_item_id=item.work_item_id,
            row_version=item.row_version,
            attempt_number=item.active_attempt_number,
            lease_owner=item.lease_owner,
            expires_at_micros=item.lease_expires_at_micros,
        )
        if item.lease_expires_at_micros <= now_micros:
            expired.append(classification)
        else:
            non_expired.append(classification)
    ordered_expired = tuple(sorted(expired, key=_leased_order_key))
    ordered_non = tuple(sorted(non_expired, key=_leased_order_key))
    return ordered_expired, ordered_non, ambiguous, reason


def _require_topology(
    node_order: tuple[str, ...],
    edges: tuple[tuple[str, str], ...],
    partitions_by_node: dict[str, tuple[str, ...]],
) -> None:
    if type(node_order) is not tuple or not node_order:
        raise ConcurrentRecoveryInvalidRequestError("recovery node order is invalid")
    if type(edges) is not tuple:
        raise ConcurrentRecoveryInvalidRequestError("recovery edges are invalid")
    if type(partitions_by_node) is not dict:
        raise TypeError("recovery partitions must use a mapping")
    if len(set(node_order)) != len(node_order):
        raise ConcurrentRecoveryInvalidRequestError("recovery nodes must be unique")
    for node in node_order:
        partitions = partitions_by_node.get(node)
        if type(partitions) is not tuple or not partitions:
            raise ConcurrentRecoveryInvalidRequestError("recovery partitions must cover every node")


def _require_evidence_covers_plan(
    run_id: str,
    node_order: tuple[str, ...],
    partitions_by_node: dict[str, tuple[str, ...]],
    evidence: ConcurrentRecoveryEvidence,
) -> None:
    if str(evidence.run.run_id) != run_id:
        raise ConcurrentRecoveryInvalidRequestError(
            "recovery evidence run does not match the request"
        )
    observed = {(item.node_id, item.partition_key) for item in evidence.work}
    expected = {(node, partition) for node in node_order for partition in partitions_by_node[node]}
    if observed != expected:
        raise ConcurrentRecoveryInvalidRequestError(
            "durable work evidence does not exactly cover the plan partitions"
        )


def _rebuild_frontier(
    *,
    run_id: str,
    plan_fingerprint: str,
    node_order: tuple[str, ...],
    edges: tuple[tuple[str, str], ...],
    partitions_by_node: dict[str, tuple[str, ...]],
    evidence: ConcurrentRecoveryEvidence,
    control_generation: int,
    ambiguous_reason: str | None,
) -> SchedulerFrontierV2:
    item_by_key = {(item.node_id, item.partition_key): item for item in evidence.work}
    successors: dict[str, list[str]] = defaultdict(list)
    for source, target in edges:
        successors[source].append(target)
    admitted_states = _node_admission_states(node_order, edges, item_by_key)
    states: dict[WorkIdentity, FrontierWorkState] = {}
    fences: dict[WorkIdentity, int] = {}
    retry_waits: list[FrontierRetryWait] = []
    for node in node_order:
        for partition in sorted(partitions_by_node[node]):
            item = item_by_key[(node, partition)]
            identity = WorkIdentity(run_id, node, partition)
            durable_state = _state_value(item.state)
            frontier_state = _WORK_STATE_MAP[durable_state]
            if durable_state is WorkItemState.PENDING and not admitted_states[node]:
                frontier_state = FrontierWorkState.BLOCKED
            states[identity] = frontier_state
            if frontier_state is FrontierWorkState.ADMITTED:
                fences[identity] = item.row_version
            if frontier_state is FrontierWorkState.RETRY_WAIT:
                retry_waits.append(
                    FrontierRetryWait(
                        identity,
                        item.retry_available_at_micros or 0,
                        "recovery_retry_wait",
                    )
                )
    run_state = evidence.run.state
    if ambiguous_reason is not None:
        control_state = ContractLifecycleState.RECOVERY_REQUIRED
    elif run_state is RunState.PAUSED:
        control_state = ContractLifecycleState.PAUSED
    elif run_state is RunState.CANCELLING:
        control_state = ContractLifecycleState.CANCELLING
    else:
        control_state = ContractLifecycleState.RUNNING
    reason = ambiguous_reason if control_state is ContractLifecycleState.RECOVERY_REQUIRED else None
    return SchedulerFrontierV2(
        version=SCHEDULER_FRONTIER_VERSION,
        plan_fingerprint=plan_fingerprint,
        control_generation=ControlGeneration(control_generation),
        run_id=run_id,
        node_order=node_order,
        edges=tuple(sorted(edges)),
        work_states=tuple(sorted(states.items(), key=lambda item: WorkIdentity.sort_key(item[0]))),
        lease_fences=tuple(sorted(fences.items(), key=lambda item: WorkIdentity.sort_key(item[0]))),
        retry_waits=tuple(
            sorted(retry_waits, key=lambda wait: WorkIdentity.sort_key(wait.identity))
        ),
        control_state=control_state,
        recovery_required_reason=reason,
    )


def _node_admission_states(
    node_order: tuple[str, ...],
    edges: tuple[tuple[str, str], ...],
    item_by_key: dict[tuple[str, str], RecoveryWorkEvidence],
) -> dict[str, bool]:
    """Compute which nodes permit new admission from durable work states."""
    predecessors: dict[str, list[str]] = defaultdict(list)
    for source, target in edges:
        predecessors[target].append(source)
    resolved: dict[str, bool] = {}

    def permits(node: str) -> bool:
        cached = resolved.get(node)
        if cached is not None:
            return cached
        resolved[node] = False
        permits_value = all(_node_all_succeeded(pred, item_by_key) for pred in predecessors[node])
        resolved[node] = permits_value
        return permits_value

    return {node: permits(node) for node in node_order}


def _node_all_succeeded(
    node: str,
    item_by_key: dict[tuple[str, str], RecoveryWorkEvidence],
) -> bool:
    """Report whether every durable work item of one node succeeded."""
    items = [item for key, item in item_by_key.items() if key[0] == node]
    return all(item.state == WorkItemState.SUCCEEDED.value for item in items)


def _state_value(value: str) -> WorkItemState:
    try:
        return WorkItemState(value)
    except ValueError as error:
        raise ConcurrentRecoveryStateReadError(
            "durable work state is not a known work item state"
        ) from error


__all__ = [
    "CONCURRENT_RECOVERY_VERSION",
    "RECOVERY_EXPIRED_EVENT_KIND",
    "ConcurrentRecoveryAmbiguousError",
    "ConcurrentRecoveryBusyError",
    "ConcurrentRecoveryClock",
    "ConcurrentRecoveryError",
    "ConcurrentRecoveryEvidence",
    "ConcurrentRecoveryInvalidRequestError",
    "ConcurrentRecoveryNonExpiredLeaseError",
    "ConcurrentRecoveryOutcomeUnknownError",
    "ConcurrentRecoveryReport",
    "ConcurrentRecoveryScan",
    "ConcurrentRecoveryService",
    "ConcurrentRecoverySettings",
    "ConcurrentRecoveryStateReadError",
    "ConcurrentRecoveryStateReader",
    "ConcurrentRecoveryWriter",
    "LeasedWorkClassification",
    "NonExpiredLeasePolicy",
    "RecoveryWorkEvidence",
]
