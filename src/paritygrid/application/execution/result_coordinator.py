"""Parent-side concurrent result coordination (P7.9).

Workers return immutable ``WorkResultV1`` envelopes through the bounded
result channel. This module is the single parent-side coordinator that
turns those envelopes into one serialized durable commit per result. It
owns the only writer port in the concurrent execution boundary: no
thread, task, process, or interpreter worker ever sees the writer, the
reader, the scheduler, or the capacity ledger, and this module itself
imports no database driver — every durable effect flows through the
borrowed writer protocol below.

Admission is fully validated before any writer interaction. A result
rejected pre-admission submits zero writer commands and retains the
active lease: the registration stays intact and one corrected bounded
resubmission is possible. At serialized writer admission the coordinator
never trusts assignment-time row versions: it re-reads the current
run/node/event evidence through the reader protocol and builds the
commit intent from those rebased versions only, so deliberately
reversed completions still commit correct aggregates and contiguous
per-run event sequences without stale global row-version failures.

Writer failures follow the checkpoint-sink classification. A known
rollback (admission timeout, writer rejection, or a proven
definitely-not-executed command) raises a retryable error and commits
nothing. An unknown outcome — an unexpected admission failure, a
malformed ticket or receipt, or an unproven commit result — stops
admission entirely, marks the scheduler recovery-required, closes
producer flow on the result channel, and records the ambiguous
identity. Nothing is committed, released, or guessed for an ambiguous
identity; already accepted channel messages stay drainable and durable
recovery (P7.10) resolves the ambiguity.
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import Protocol, cast, runtime_checkable

from paritygrid.application.execution.capacity import ScheduledWorkLimiters
from paritygrid.application.execution.channels import (
    CHANNEL_KIND_RESULT,
    BoundedChannel,
    ChannelClosedError,
    ChannelTimeoutError,
)
from paritygrid.application.execution.concurrent_scheduler import (
    ConcurrentScheduler,
    FrontierWorkState,
    WorkIdentity,
)
from paritygrid.application.execution.runner_contract import (
    MAX_CONTRACT_LIST_ITEMS,
    MAX_CONTRACT_STRING_LENGTH,
    MAX_METRIC_VALUE,
    ContractOutcome,
    RunnerContractError,
    WorkResultV1,
)
from paritygrid.application.execution.telemetry import (
    TelemetryRecord,
    service_duration_record,
)
from paritygrid.application.ports.consistency import MAX_CONSISTENCY_SEQUENCE
from paritygrid.application.ports.writer import (
    WriterAdmissionTimeoutError,
    WriterDefinitelyNotExecutedError,
    WriterError,
)
from paritygrid.domain.execution import FailureClassification

RESULT_COORDINATOR_VERSION = 1
MAX_IN_FLIGHT_RESULTS = 256
COORDINATOR_ADMISSION_TIMEOUT_SECONDS = 5.0
COORDINATOR_RESULT_TIMEOUT_SECONDS = 60.0

_MAX_COORDINATOR_TIMEOUT_SECONDS = 86_400.0
_MAX_COORDINATOR_TEXT_LENGTH = 128
_MAX_COORDINATOR_INT = 2**63 - 1
# Retry eligibility uses the absolute injected-clock microsecond frame, so the
# bound must cover epoch instants while staying finite and validated.
_MAX_RETRY_ELIGIBILITY_MICROS = 2**53 - 1
_UNKNOWN_OUTCOME_REASON = "result_writer_outcome_unknown"
_TELEMETRY_OPERATION = "result_commit"

_COORDINATOR_FINGERPRINT_PATTERN: re.Pattern[str] = re.compile(r"[0-9a-f]{64}")
_REBASE_ATTEMPT_STATES: frozenset[str] = frozenset({"running", "awaiting_result", "expired"})
_OWNING_ATTEMPT_STATES: frozenset[str] = frozenset({"running", "awaiting_result"})
_COMMITTABLE_OUTCOMES: frozenset[str] = frozenset(outcome.value for outcome in ContractOutcome)
_TERMINAL_WORK_STATES: dict[ContractOutcome, FrontierWorkState] = {
    ContractOutcome.SUCCEEDED: FrontierWorkState.SUCCEEDED,
    ContractOutcome.QUARANTINED: FrontierWorkState.QUARANTINED,
    ContractOutcome.FAILED: FrontierWorkState.FAILED,
    ContractOutcome.CANCELLED: FrontierWorkState.CANCELLED,
}
_COMMITTABLE_CLASSIFICATIONS: frozenset[str] = frozenset(
    classification.value for classification in FailureClassification
)


class ResultCoordinatorError(RuntimeError):
    """Base failure for concurrent result coordination."""


class ResultValidationRejection(ResultCoordinatorError):  # noqa: N818 - task-mandated name
    """A result or coordination value failed validation before admission.

    Nothing was submitted to the writer, so the active lease is retained
    by construction and one corrected bounded resubmission is possible.
    """


class ResultStaleRejection(ResultCoordinatorError):  # noqa: N818 - task-mandated name
    """A result is stale, duplicated, or no longer owns its work.

    Stale results fail closed: wrong attempt, lease fence, lease owner,
    control generation, coordinator owner, identity, run, or a lease the
    rebased durable evidence no longer honors.
    """


class ResultForgedReferenceRejection(ResultCoordinatorError):  # noqa: N818 - task-mandated name
    """A result referenced an artifact not registered for the assignment."""


class ResultWriterRetryableError(ResultCoordinatorError):
    """The writer proved a known rollback; nothing was committed.

    A bounded persistence retry may resubmit the still-registered
    assignment.
    """


class ResultOutcomeUnknownError(ResultCoordinatorError):
    """The durable writer outcome is unknown; admission is stopped.

    Recovery (P7.10) must resolve the ambiguity from durable evidence
    before any further admission.
    """


class ResultCoordinatorClosedError(ResultCoordinatorError):
    """The coordinator no longer admits results."""


def _require_text(value: object, subject: str, maximum: int = _MAX_COORDINATOR_TEXT_LENGTH) -> str:
    if type(value) is not str:
        raise TypeError(f"{subject} must be text")
    text = value
    if not 1 <= len(text) <= maximum:
        raise ResultValidationRejection(f"{subject} length is outside the supported range")
    for character in text:
        if not "\x20" <= character <= "\x7e":
            raise ResultValidationRejection(f"{subject} must use printable ASCII characters")
    return text


def _require_int(value: object, minimum: int, maximum: int, subject: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    number = value
    if not minimum <= number <= maximum:
        raise ResultValidationRejection(f"{subject} is outside the supported range")
    return number


def _require_fingerprint(value: object) -> str:
    if type(value) is not str:
        raise TypeError("coordinator plan fingerprint must be text")
    if _COORDINATOR_FINGERPRINT_PATTERN.fullmatch(value) is None:
        raise ResultValidationRejection(
            "coordinator plan fingerprint must use 64 lowercase hexadecimal characters"
        )
    return value


def _require_timeout_seconds(value: object, subject: str) -> float:
    if type(value) is not float:
        raise TypeError(f"{subject} must be a float second count")
    seconds = value
    if not math.isfinite(seconds) or not 0.0 <= seconds <= _MAX_COORDINATOR_TIMEOUT_SECONDS:
        raise ResultValidationRejection(f"{subject} is outside the supported range")
    return seconds


def _require_reference_tuple(value: object, subject: str) -> tuple[str, ...]:
    if type(value) is not tuple:
        raise TypeError(f"{subject} must be a tuple")
    items = cast(tuple[object, ...], value)
    if len(items) > MAX_CONTRACT_LIST_ITEMS:
        raise ResultValidationRejection(f"{subject} exceeds the supported item bound")
    for item in items:
        if type(item) is not str:
            raise TypeError(f"{subject} items must be text")
        _require_text(item, f"{subject} item", MAX_CONTRACT_STRING_LENGTH)
    return cast(tuple[str, ...], items)


def _require_allowed_artifact_ids(value: object) -> tuple[str, ...]:
    identifiers = list(_require_reference_tuple(value, "allowed artifact identifiers"))
    if len(set(identifiers)) != len(identifiers):
        raise ResultValidationRejection("allowed artifact identifiers must be unique")
    if identifiers != sorted(identifiers):
        raise ResultValidationRejection("allowed artifact identifiers must be sorted")
    return tuple(identifiers)


def _require_receive_timeout(value: object) -> None:
    if value is None:
        return
    if type(value) is not float and type(value) is not int:
        raise TypeError("result receive timeout must be a finite non-negative second count")
    seconds: float = value
    if not math.isfinite(seconds) or seconds < 0:
        raise ResultValidationRejection(
            "result receive timeout must be a finite non-negative second count"
        )


@dataclass(frozen=True, slots=True, repr=False)
class RegisteredAssignment:
    """Admission-time facts for one in-flight work item.

    The registration captures only the work-local lease facts and the
    closed set of artifact identifiers the assignment may reference. It
    deliberately carries no run, node, or event row versions: those are
    never authoritative across out-of-order completions and are re-read
    at serialized writer admission instead.
    """

    identity: WorkIdentity
    work_item_id: str
    attempt_number: int
    lease_fence: int
    lease_owner: str
    control_generation: int
    deadline_micros: int
    allowed_artifact_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.identity) is not WorkIdentity:
            raise TypeError("registered assignment identity must use WorkIdentity")
        _require_text(self.work_item_id, "registered assignment work item identity")
        _require_int(self.attempt_number, 1, MAX_METRIC_VALUE, "registered assignment attempt")
        _require_int(self.lease_fence, 1, MAX_METRIC_VALUE, "registered assignment lease fence")
        _require_text(self.lease_owner, "registered assignment lease owner")
        _require_int(
            self.control_generation,
            1,
            MAX_METRIC_VALUE,
            "registered assignment control generation",
        )
        _require_int(
            self.deadline_micros,
            0,
            _MAX_COORDINATOR_INT,
            "registered assignment deadline",
        )
        _require_allowed_artifact_ids(self.allowed_artifact_ids)

    def __repr__(self) -> str:
        return (
            "RegisteredAssignment("
            f"identity={self.identity!r}, work_item_id={self.work_item_id!r}, "
            f"attempt_number={self.attempt_number!r}, lease_fence={self.lease_fence!r}, "
            "lease_owner=<redacted>, "
            f"control_generation={self.control_generation!r}, "
            f"deadline_micros={self.deadline_micros!r}, "
            f"allowed_artifact_ids={len(self.allowed_artifact_ids)})"
        )


@dataclass(frozen=True, slots=True)
class RebasedFrontier:
    """Current durable evidence re-read at serialized writer admission.

    This is never the assignment-time capture: the reader port supplies
    the current run and node row versions, the next contiguous per-run
    event sequence with its counter row version, and the attempt state
    as durable truth. ``attempt_state`` uses the closed set
    ``running``/``awaiting_result`` (the attempt still owns the work)
    plus ``expired`` (the durable lease policy no longer honors the
    attempt, so the result is stale).
    """

    run_id: str
    run_row_version: int
    node_id: str
    node_row_version: int
    work_item_id: str
    attempt_number: int
    lease_fence: int
    lease_owner: str
    next_event_sequence: int
    event_counter_row_version: int
    attempt_state: str
    observed_at_micros: int
    expires_at_micros: int
    runner_kind: str = "sequential"
    worker_identity: str = "engine-default"
    started_at_micros: int = 0

    def __post_init__(self) -> None:
        _require_text(self.run_id, "rebased frontier run identity")
        _require_int(
            self.run_row_version,
            0,
            _MAX_COORDINATOR_INT,
            "rebased frontier run row version",
        )
        _require_text(self.node_id, "rebased frontier node identity")
        _require_int(
            self.node_row_version,
            0,
            _MAX_COORDINATOR_INT,
            "rebased frontier node row version",
        )
        _require_text(self.work_item_id, "rebased frontier work item identity")
        _require_int(
            self.attempt_number,
            1,
            MAX_METRIC_VALUE,
            "rebased frontier attempt",
        )
        _require_int(
            self.lease_fence,
            1,
            MAX_METRIC_VALUE,
            "rebased frontier lease fence",
        )
        _require_text(self.lease_owner, "rebased frontier lease owner")
        _require_int(
            self.next_event_sequence,
            1,
            MAX_CONSISTENCY_SEQUENCE,
            "rebased frontier event sequence",
        )
        _require_int(
            self.event_counter_row_version,
            0,
            _MAX_COORDINATOR_INT,
            "rebased frontier event counter row version",
        )
        if type(self.attempt_state) is not str:
            raise TypeError("rebased frontier attempt state must be text")
        if self.attempt_state not in _REBASE_ATTEMPT_STATES:
            raise ResultValidationRejection("rebased frontier attempt state is unknown")
        _require_int(
            self.observed_at_micros,
            0,
            _MAX_COORDINATOR_INT,
            "rebased frontier observation time",
        )
        _require_int(
            self.expires_at_micros,
            0,
            _MAX_COORDINATOR_INT,
            "rebased frontier lease expiry",
        )
        _require_text(self.runner_kind, "rebased frontier runner kind", 32)
        _require_text(self.worker_identity, "rebased frontier worker identity")
        _require_int(
            self.started_at_micros,
            0,
            _MAX_COORDINATOR_INT,
            "rebased frontier attempt start time",
        )


@dataclass(frozen=True, slots=True, repr=False)
class CommitIntent:
    """One durable commit intent carrying only rebased versions.

    The intent is the exact object submitted to the serialized writer
    port: the rebased run and node row versions, the rebased contiguous
    event frontier, and the result's terminal facts. Doubles record it
    and acknowledge it through ``committed_intent`` equality.
    """

    run_id: str
    plan_fingerprint: str
    node_id: str
    partition_key: str
    work_item_id: str
    attempt_number: int
    lease_fence: int
    lease_owner: str
    observed_at_micros: int
    lease_expires_at_micros: int
    outcome: str
    expected_run_row_version: int
    expected_node_row_version: int
    next_event_sequence: int
    event_counter_row_version: int
    checkpoint_proposed: bool
    artifact_ids: tuple[str, ...]
    result: WorkResultV1
    runner_kind: str = "sequential"
    worker_identity: str = "engine-default"
    started_at_micros: int = 0
    retry_eligible_at_micros: int = 0
    failure_classification: str | None = None

    def __post_init__(self) -> None:
        _require_text(self.run_id, "commit intent run identity")
        _require_fingerprint(self.plan_fingerprint)
        _require_text(self.node_id, "commit intent node identity")
        _require_text(self.partition_key, "commit intent partition key")
        _require_text(self.work_item_id, "commit intent work item identity")
        _require_int(self.attempt_number, 1, MAX_METRIC_VALUE, "commit intent attempt")
        _require_int(self.lease_fence, 1, MAX_METRIC_VALUE, "commit intent lease fence")
        _require_text(self.lease_owner, "commit intent lease owner")
        _require_int(
            self.observed_at_micros,
            0,
            _MAX_COORDINATOR_INT,
            "commit intent observation time",
        )
        _require_int(
            self.lease_expires_at_micros,
            0,
            _MAX_COORDINATOR_INT,
            "commit intent lease expiry",
        )
        if type(self.outcome) is not str:
            raise TypeError("commit intent outcome must be text")
        if self.outcome not in _COMMITTABLE_OUTCOMES:
            raise ResultValidationRejection("commit intent outcome is unknown")
        _require_int(
            self.expected_run_row_version,
            0,
            _MAX_COORDINATOR_INT,
            "commit intent run row version",
        )
        _require_int(
            self.expected_node_row_version,
            0,
            _MAX_COORDINATOR_INT,
            "commit intent node row version",
        )
        _require_int(
            self.next_event_sequence,
            1,
            MAX_CONSISTENCY_SEQUENCE,
            "commit intent event sequence",
        )
        _require_int(
            self.event_counter_row_version,
            0,
            _MAX_COORDINATOR_INT,
            "commit intent event counter row version",
        )
        if type(self.checkpoint_proposed) is not bool:
            raise TypeError("commit intent checkpoint proposal must be a boolean")
        _require_reference_tuple(self.artifact_ids, "commit intent artifact identifiers")
        if type(self.result) is not WorkResultV1:
            raise TypeError("commit intent result must use WorkResultV1")
        _revalidate_envelope(self.result)
        result_facts = (
            self.result.run_id,
            self.result.plan_fingerprint,
            self.result.node_id,
            self.result.partition_key,
            self.result.work_item_id,
            self.result.attempt_number,
            self.result.lease_fence,
            self.result.lease_owner,
            self.result.outcome.value,
            self.result.checkpoint_proposal,
            self.result.artifact_references,
        )
        intent_facts = (
            self.run_id,
            self.plan_fingerprint,
            self.node_id,
            self.partition_key,
            self.work_item_id,
            self.attempt_number,
            self.lease_fence,
            self.lease_owner,
            self.outcome,
            self.checkpoint_proposed,
            self.artifact_ids,
        )
        if result_facts != intent_facts:
            raise ResultValidationRejection(
                "commit intent result does not match its validated commit facts"
            )
        _require_text(self.runner_kind, "commit intent runner kind", 32)
        _require_text(self.worker_identity, "commit intent worker identity")
        _require_int(
            self.started_at_micros,
            0,
            _MAX_COORDINATOR_INT,
            "commit intent attempt start time",
        )
        _require_int(
            self.retry_eligible_at_micros,
            0,
            _MAX_RETRY_ELIGIBILITY_MICROS,
            "commit intent retry eligibility",
        )
        classification = self.failure_classification
        if classification is not None:
            if type(classification) is not str:
                raise TypeError("commit intent failure classification must be text or None")
            if classification not in _COMMITTABLE_CLASSIFICATIONS:
                raise ResultValidationRejection("commit intent failure classification is unknown")

    def __repr__(self) -> str:
        return (
            "CommitIntent("
            f"run_id={self.run_id!r}, plan_fingerprint=<redacted>, "
            f"node_id={self.node_id!r}, partition_key={self.partition_key!r}, "
            f"work_item_id={self.work_item_id!r}, attempt_number={self.attempt_number!r}, "
            f"lease_fence={self.lease_fence!r}, lease_owner=<redacted>, "
            f"outcome={self.outcome!r}, checkpoint_proposed={self.checkpoint_proposed!r}, "
            f"artifact_ids={len(self.artifact_ids)}, result=<redacted>)"
        )


@runtime_checkable
class ResultCoordinatorReader(Protocol):
    """Parent-side durable evidence re-read at serialized admission."""

    def rebase(
        self,
        run_id: str,
        node_id: str,
        partition_key: str,
        work_item_id: str,
    ) -> RebasedFrontier:
        """Return the current durable frontier evidence for one work item."""
        ...


@runtime_checkable
class ResultCoordinatorWriter(Protocol):
    """Borrowed serialized-writer surface used without lifecycle ownership.

    ``submit`` returns an admission ticket exposing ``submission_id``
    and ``result(timeout_seconds=...)``; a later adapter can adapt the
    transactional writer to this shape without any coordinator change.
    """

    def submit(self, command: object, *, timeout_seconds: float) -> object:
        """Submit one commit intent and return its admission ticket."""
        ...


def _ticket_admits(ticket: object) -> bool:
    if ticket is None:
        return False
    try:
        return getattr(ticket, "submission_id", None) is not None
    except Exception:
        return False


def _receipt_acknowledges(receipt: object, intent: CommitIntent) -> bool:
    try:
        committed = getattr(receipt, "committed", None)
        committed_intent = getattr(receipt, "committed_intent", None)
    except Exception:
        return False
    return type(committed) is bool and committed is True and committed_intent == intent


def _revalidate_envelope(result: WorkResultV1) -> None:
    """Re-run the closed contract invariants over one received envelope.

    The envelope validated itself at construction, so this re-construction
    catches any post-construction tampering and converts every contract
    failure into a pre-admission rejection.
    """

    try:
        WorkResultV1(
            protocol=result.protocol,
            contract_version=result.contract_version,
            plan_fingerprint=result.plan_fingerprint,
            run_id=result.run_id,
            node_id=result.node_id,
            partition_key=result.partition_key,
            work_item_id=result.work_item_id,
            attempt_number=result.attempt_number,
            lease_fence=result.lease_fence,
            lease_owner=result.lease_owner,
            control_generation=result.control_generation,
            outcome=result.outcome,
            metrics=result.metrics,
            artifact_references=result.artifact_references,
            checkpoint_proposal=result.checkpoint_proposal,
            failure_detail=result.failure_detail,
            cleanup=result.cleanup,
        )
    except (RunnerContractError, TypeError, ValueError) as error:
        raise ResultValidationRejection(
            "work result envelope failed contract validation"
        ) from error


class ConcurrentResultCoordinator:
    """Parent-side result coordinator owning the only writer port.

    One internal lock serializes the whole validated path — pre-admission
    validation, the durable rebase, writer admission, and the durable
    acknowledgement — so exactly one commit intent is in flight at any
    instant. Capacity is retained from acceptance until durable commit
    acknowledgement and released only after it; a known rollback keeps
    the assignment registered for one bounded retry; an unknown outcome
    stops admission until durable recovery.
    """

    __slots__ = (
        "_admission_stopped",
        "_admission_timeout_seconds",
        "_ambiguous",
        "_assignments",
        "_capacity",
        "_closed",
        "_committed_count",
        "_control_generation",
        "_lock",
        "_plan_fingerprint",
        "_reader",
        "_rejected_count",
        "_result_channel",
        "_result_timeout_seconds",
        "_run_id",
        "_scheduler",
        "_telemetry_sequence",
        "_telemetry_sink",
        "_writer",
    )

    def __init__(
        self,
        *,
        run_id: str,
        plan_fingerprint: str,
        control_generation: int,
        reader: ResultCoordinatorReader,
        writer: ResultCoordinatorWriter,
        result_channel: BoundedChannel,
        scheduler: ConcurrentScheduler,
        capacity: ScheduledWorkLimiters,
        admission_timeout_seconds: float = COORDINATOR_ADMISSION_TIMEOUT_SECONDS,
        result_timeout_seconds: float = COORDINATOR_RESULT_TIMEOUT_SECONDS,
        telemetry_sink: Callable[[TelemetryRecord], None] | None = None,
    ) -> None:
        _require_text(run_id, "coordinator run identity")
        _require_fingerprint(plan_fingerprint)
        _require_int(control_generation, 1, MAX_METRIC_VALUE, "coordinator control generation")
        reader_value = cast(object, reader)
        if not isinstance(reader_value, ResultCoordinatorReader):
            raise TypeError("result coordinator reader must implement ResultCoordinatorReader")
        writer_value = cast(object, writer)
        if not isinstance(writer_value, ResultCoordinatorWriter):
            raise TypeError("result coordinator writer must implement ResultCoordinatorWriter")
        if type(result_channel) is not BoundedChannel:
            raise TypeError("result channel must use BoundedChannel")
        if result_channel.kind != CHANNEL_KIND_RESULT:
            raise ResultValidationRejection("result channel must use the result channel kind")
        if type(scheduler) is not ConcurrentScheduler:
            raise TypeError("scheduler must use ConcurrentScheduler")
        if type(capacity) is not ScheduledWorkLimiters:
            raise TypeError("capacity must use ScheduledWorkLimiters")
        _require_timeout_seconds(admission_timeout_seconds, "coordinator admission timeout")
        _require_timeout_seconds(result_timeout_seconds, "coordinator result timeout")
        if telemetry_sink is not None and not callable(telemetry_sink):
            raise TypeError("telemetry sink must be callable")

        self._run_id = run_id
        self._plan_fingerprint = plan_fingerprint
        self._control_generation = control_generation
        self._reader = reader_value
        self._writer = writer_value
        self._result_channel = result_channel
        self._scheduler = scheduler
        self._capacity = capacity
        self._admission_timeout_seconds = admission_timeout_seconds
        self._result_timeout_seconds = result_timeout_seconds
        self._telemetry_sink = telemetry_sink
        self._lock = Lock()
        self._assignments: dict[WorkIdentity, RegisteredAssignment] = {}
        self._ambiguous: list[WorkIdentity] = []
        self._closed = False
        self._admission_stopped = False
        self._committed_count = 0
        self._rejected_count = 0
        self._telemetry_sequence = 0

    def __repr__(self) -> str:
        with self._lock:
            return (
                "ConcurrentResultCoordinator("
                f"run_id={self._run_id!r}, registered={len(self._assignments)}, "
                f"committed={self._committed_count}, rejected={self._rejected_count}, "
                f"ambiguous={len(self._ambiguous)}, "
                f"admission_stopped={self._admission_stopped!r}, closed={self._closed!r})"
            )

    @property
    def registered_identities(self) -> tuple[WorkIdentity, ...]:
        """Return every currently registered identity in deterministic order."""
        with self._lock:
            return tuple(sorted(self._assignments, key=WorkIdentity.sort_key))

    @property
    def ambiguous_identities(self) -> tuple[WorkIdentity, ...]:
        """Return identities whose durable writer outcome stayed unknown."""
        with self._lock:
            return tuple(self._ambiguous)

    @property
    def is_admission_stopped(self) -> bool:
        """Return whether an unknown outcome stopped result admission."""
        with self._lock:
            return self._admission_stopped

    @property
    def committed_count(self) -> int:
        """Return how many results reached a durable commit acknowledgement."""
        with self._lock:
            return self._committed_count

    @property
    def rejected_count(self) -> int:
        """Return how many results were rejected before or at rebase admission."""
        with self._lock:
            return self._rejected_count

    def register_assignment(self, assignment: RegisteredAssignment) -> None:
        """Register the admission-time facts for one in-flight work item.

        Registration validates only uniqueness and run ownership: the
        scheduler stays decoupled so admission ordering remains the
        caller's durable decision.
        """

        if type(assignment) is not RegisteredAssignment:
            raise TypeError("registered assignment must use RegisteredAssignment")
        with self._lock:
            if self._closed or self._admission_stopped:
                raise ResultCoordinatorClosedError("result coordinator no longer admits results")
            identity = assignment.identity
            if identity.run_id != self._run_id:
                raise ResultStaleRejection(
                    "registered assignment must reference the coordinator run"
                )
            current_generation = self._scheduler.frontier.control_generation.value
            if assignment.control_generation != current_generation:
                raise ResultStaleRejection(
                    "registered assignment control generation is not current"
                )
            if identity in self._assignments:
                raise ResultStaleRejection("work identity is already registered")
            if len(self._assignments) >= MAX_IN_FLIGHT_RESULTS:
                raise ResultValidationRejection(
                    "registered in-flight results exceed the supported bound"
                )
            self._assignments[identity] = assignment

    def submit_result(
        self,
        result: WorkResultV1,
        *,
        retry_eligible_at_micros: int = 0,
        failure_classification: str | None = None,
    ) -> None:
        """Validate, rebase, and durably commit one received result envelope.

        Raises a pre-admission rejection (zero writer commands, lease
        retained), a retryable rollback error, or an unknown-outcome
        error that stops admission; only a durable acknowledgement
        returns normally.
        """

        _require_int(
            retry_eligible_at_micros,
            0,
            _MAX_RETRY_ELIGIBILITY_MICROS,
            "retry eligibility",
        )
        if failure_classification is not None:
            if type(failure_classification) is not str:
                raise TypeError("failure classification must be text or None")
            if failure_classification not in _COMMITTABLE_CLASSIFICATIONS:
                raise ResultValidationRejection("failure classification is unknown")
        with self._lock:
            if self._closed or self._admission_stopped:
                raise ResultCoordinatorClosedError("result coordinator no longer admits results")
            try:
                identity, assignment = self._pre_admission_checks(result)
                self._mark_result_received(identity)
                intent = self._rebase_intent(
                    identity,
                    assignment,
                    result,
                    retry_eligible_at_micros,
                    failure_classification,
                )
                ticket = self._admit_intent(identity, intent)
                self._await_commit(identity, ticket, intent)
            except (
                ResultValidationRejection,
                ResultStaleRejection,
                ResultForgedReferenceRejection,
            ):
                self._rejected_count += 1
                raise
            self._complete_commit(identity, assignment, result, retry_eligible_at_micros)

    def receive_next(self, *, timeout: float | None = 0.5) -> None:
        """Pull one result envelope from the bounded channel and submit it.

        A timeout or a foreign-closed drained channel is a no-op; a
        drained channel after this coordinator closed raises
        ``ResultCoordinatorClosedError``.
        """

        _require_receive_timeout(timeout)
        try:
            message = self._result_channel.recv(timeout=timeout)
        except ChannelTimeoutError:
            return
        except ChannelClosedError:
            if self._closed:
                raise ResultCoordinatorClosedError(
                    "result channel is closed and fully drained"
                ) from None
            return
        self.submit_result(cast(WorkResultV1, message))

    def close(self) -> None:
        """Stop admitting results idempotently without closing the channel.

        The channel set owns channel lifecycle; closing here only refuses
        further submissions, registrations, and channel-driven receives.
        """

        with self._lock:
            self._closed = True

    def _pre_admission_checks(
        self,
        result: WorkResultV1,
    ) -> tuple[WorkIdentity, RegisteredAssignment]:
        if type(result) is not WorkResultV1:
            raise ResultValidationRejection("work result must use the exact WorkResultV1 envelope")
        _revalidate_envelope(result)
        if result.run_id != self._run_id:
            raise ResultStaleRejection("work result run does not match the coordinator run")
        if result.plan_fingerprint != self._plan_fingerprint:
            raise ResultStaleRejection(
                "work result plan fingerprint does not match the coordinator plan"
            )
        identity = WorkIdentity(
            run_id=result.run_id,
            node_id=result.node_id,
            partition_key=result.partition_key,
        )
        assignment = self._assignments.get(identity)
        if assignment is None:
            raise ResultStaleRejection("work result identity is not a registered assignment")
        if result.work_item_id != assignment.work_item_id:
            raise ResultStaleRejection(
                "work result item identity does not match the registered assignment"
            )
        if result.attempt_number != assignment.attempt_number:
            raise ResultStaleRejection(
                "work result attempt does not match the registered assignment"
            )
        if result.lease_fence != assignment.lease_fence:
            raise ResultStaleRejection(
                "work result lease fence does not match the registered assignment"
            )
        if result.lease_owner != assignment.lease_owner:
            raise ResultStaleRejection(
                "work result lease owner does not match the registered assignment"
            )
        if result.control_generation.value != assignment.control_generation:
            raise ResultStaleRejection(
                "work result control generation does not match the registered assignment"
            )
        allowed = frozenset(assignment.allowed_artifact_ids)
        for reference in result.artifact_references:
            if reference not in allowed:
                raise ResultForgedReferenceRejection(
                    "work result references an unregistered artifact"
                )
        return identity, assignment

    def _mark_result_received(self, identity: WorkIdentity) -> None:
        state = dict(self._scheduler.frontier.work_states).get(identity)
        if (
            state is not FrontierWorkState.ADMITTED
            and state is not FrontierWorkState.AWAITING_COMMIT
        ):
            raise ResultStaleRejection("scheduler frontier no longer holds the work in flight")
        if state is FrontierWorkState.ADMITTED:
            self._scheduler.mark_result_received(identity)

    def _rebase_intent(
        self,
        identity: WorkIdentity,
        assignment: RegisteredAssignment,
        result: WorkResultV1,
        retry_eligible_at_micros: int,
        failure_classification: str | None,
    ) -> CommitIntent:
        frontier_value = self._reader.rebase(
            self._run_id,
            identity.node_id,
            identity.partition_key,
            assignment.work_item_id,
        )
        if type(frontier_value) is not RebasedFrontier:
            raise ResultValidationRejection("rebased evidence must use RebasedFrontier")
        frontier = frontier_value
        if frontier.run_id != self._run_id or frontier.node_id != identity.node_id:
            raise ResultStaleRejection("rebased evidence does not match the work identity")
        if frontier.work_item_id != assignment.work_item_id:
            raise ResultStaleRejection("rebased evidence does not match the registered work item")
        if frontier.attempt_number != assignment.attempt_number:
            raise ResultStaleRejection("rebased evidence does not match the registered attempt")
        if frontier.lease_fence != assignment.lease_fence:
            raise ResultStaleRejection("rebased evidence does not match the registered lease fence")
        if frontier.lease_owner != assignment.lease_owner:
            raise ResultStaleRejection("rebased evidence does not match the registered lease owner")
        if frontier.attempt_state not in _OWNING_ATTEMPT_STATES:
            raise ResultStaleRejection("rebased evidence shows the attempt no longer owns the work")
        if frontier.observed_at_micros > frontier.expires_at_micros:
            raise ResultStaleRejection("rebased evidence shows the durable lease expired")
        if frontier.observed_at_micros > assignment.deadline_micros:
            raise ResultStaleRejection("work result arrived after the assignment deadline")
        return CommitIntent(
            run_id=self._run_id,
            plan_fingerprint=self._plan_fingerprint,
            node_id=identity.node_id,
            partition_key=identity.partition_key,
            work_item_id=assignment.work_item_id,
            attempt_number=frontier.attempt_number,
            lease_fence=frontier.lease_fence,
            lease_owner=frontier.lease_owner,
            observed_at_micros=frontier.observed_at_micros,
            lease_expires_at_micros=frontier.expires_at_micros,
            outcome=result.outcome.value,
            expected_run_row_version=frontier.run_row_version,
            expected_node_row_version=frontier.node_row_version,
            next_event_sequence=frontier.next_event_sequence,
            event_counter_row_version=frontier.event_counter_row_version,
            checkpoint_proposed=result.checkpoint_proposal,
            artifact_ids=result.artifact_references,
            result=result,
            runner_kind=frontier.runner_kind,
            worker_identity=frontier.worker_identity,
            started_at_micros=frontier.started_at_micros,
            retry_eligible_at_micros=retry_eligible_at_micros,
            failure_classification=failure_classification,
        )

    def _admit_intent(self, identity: WorkIdentity, intent: CommitIntent) -> object:
        admission_rollback = False
        unknown = False
        ticket: object = None
        try:
            ticket = self._writer.submit(
                intent,
                timeout_seconds=self._admission_timeout_seconds,
            )
        except ResultValidationRejection:
            raise
        except WriterAdmissionTimeoutError:
            admission_rollback = True
        except WriterError:
            admission_rollback = True
        except Exception:
            unknown = True
        if admission_rollback:
            raise ResultWriterRetryableError(
                "writer admission rolled back without a durable commit"
            )
        if unknown or not _ticket_admits(ticket):
            self._enter_unknown_path(identity)
            raise ResultOutcomeUnknownError("writer admission outcome is unknown")
        return ticket

    def _await_commit(self, identity: WorkIdentity, ticket: object, intent: CommitIntent) -> None:
        proven_rollback = False
        unknown = False
        receipt: object = None
        try:
            ticket_result = getattr(ticket, "result", None)
            if not callable(ticket_result):
                unknown = True
            else:
                receipt = ticket_result(timeout_seconds=self._result_timeout_seconds)
        except WriterDefinitelyNotExecutedError:
            proven_rollback = True
        except WriterError:
            unknown = True
        except Exception:
            unknown = True
        if proven_rollback:
            raise ResultWriterRetryableError("writer proved the admitted command was not executed")
        if unknown or not _receipt_acknowledges(receipt, intent):
            self._enter_unknown_path(identity)
            raise ResultOutcomeUnknownError("writer durable commit outcome is unknown")

    def _complete_commit(
        self,
        identity: WorkIdentity,
        assignment: RegisteredAssignment,
        result: WorkResultV1,
        retry_eligible_at_micros: int,
    ) -> None:
        del self._assignments[identity]
        if result.outcome is ContractOutcome.RETRY_WAIT:
            self._scheduler.schedule_retry(
                identity,
                retry_eligible_at_micros,
                f"attempt_{assignment.attempt_number}_retry_wait",
            )
        else:
            self._scheduler.commit_result(
                identity,
                _TERMINAL_WORK_STATES[result.outcome].value,
            )
        self._capacity.release(assignment.lease_owner, identity.node_id)
        self._committed_count += 1
        sink = self._telemetry_sink
        if sink is None:
            return
        self._telemetry_sequence += 1
        try:
            sink(
                service_duration_record(
                    run_id=self._run_id,
                    observed_at_micros=self._telemetry_sequence,
                    operation=_TELEMETRY_OPERATION,
                    duration_micros=0,
                )
            )
        except Exception:
            # Telemetry is best-effort and non-authoritative: a failing
            # sink can never fail or un-commit a durable result.
            return

    def _enter_unknown_path(self, identity: WorkIdentity) -> None:
        """Stop admission after an unknown durable writer outcome.

        The scheduler becomes recovery-required, producer flow on the
        result channel closes while accepted messages stay drainable,
        the ambiguous identity is recorded, and nothing is committed,
        unregistered, or released for it.
        """

        self._scheduler.mark_recovery_required(_UNKNOWN_OUTCOME_REASON)
        self._result_channel.mark_unknown_outcome(_UNKNOWN_OUTCOME_REASON)
        self._ambiguous.append(identity)
        self._admission_stopped = True


__all__ = [
    "COORDINATOR_ADMISSION_TIMEOUT_SECONDS",
    "COORDINATOR_RESULT_TIMEOUT_SECONDS",
    "MAX_IN_FLIGHT_RESULTS",
    "RESULT_COORDINATOR_VERSION",
    "CommitIntent",
    "ConcurrentResultCoordinator",
    "RebasedFrontier",
    "RegisteredAssignment",
    "ResultCoordinatorClosedError",
    "ResultCoordinatorError",
    "ResultCoordinatorReader",
    "ResultCoordinatorWriter",
    "ResultForgedReferenceRejection",
    "ResultOutcomeUnknownError",
    "ResultStaleRejection",
    "ResultValidationRejection",
    "ResultWriterRetryableError",
]
