# pyright: reportPrivateUsage=false

"""Deterministic controlled-double tests for the P7.9 result coordinator.

Every test builds a real ``ConcurrentScheduler``, ``ScheduledWorkLimiters```,
and ``BoundedChannel`` plus scripted reader/writer doubles. No test touches
a real database: the doubles record the commit intents and acknowledge or
fail them exactly as scripted, and a source scan at the bottom of this file
proves neither the module nor this test file mentions any direct database
driver. The runner-side double receives only the result channel and never
the parent-owned writer, reader, scheduler, or capacity ports.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Event, Thread
from typing import Any, cast

import pytest

from paritygrid.application.execution import result_coordinator
from paritygrid.application.execution.capacity import (
    CAPACITY_CATEGORY_GLOBAL,
    CAPACITY_CATEGORY_NODE,
    ScheduledWorkLimiters,
)
from paritygrid.application.execution.channels import (
    CHANNEL_KIND_RESULT,
    CHANNEL_KIND_TELEMETRY,
    BoundedChannel,
    ChannelUnknownOutcomeError,
)
from paritygrid.application.execution.clock_policy import ManualClock
from paritygrid.application.execution.concurrency_settings import CapturedConcurrencySettings
from paritygrid.application.execution.concurrent_scheduler import (
    ConcurrentScheduler,
    FrontierWorkState,
    WorkIdentity,
)
from paritygrid.application.execution.result_coordinator import (
    COORDINATOR_ADMISSION_TIMEOUT_SECONDS,
    COORDINATOR_RESULT_TIMEOUT_SECONDS,
    MAX_IN_FLIGHT_RESULTS,
    RESULT_COORDINATOR_VERSION,
    CommitIntent,
    ConcurrentResultCoordinator,
    RebasedFrontier,
    RegisteredAssignment,
    ResultCoordinatorClosedError,
    ResultCoordinatorError,
    ResultCoordinatorReader,
    ResultCoordinatorWriter,
    ResultForgedReferenceRejection,
    ResultOutcomeUnknownError,
    ResultStaleRejection,
    ResultValidationRejection,
    ResultWriterRetryableError,
)
from paritygrid.application.execution.runner_contract import (
    RUNNER_CONTRACT_VERSION,
    WORK_RESULT_PROTOCOL,
    ContractCleanupEvidence,
    ContractCleanupStatus,
    ContractMetric,
    ContractOutcome,
    ControlGeneration,
    WorkResultV1,
)
from paritygrid.application.execution.telemetry import (
    TelemetryMetricKind,
    TelemetryRecord,
)
from paritygrid.application.ports.writer import (
    WriterAdmissionTimeoutError,
    WriterClosedError,
    WriterCommitOutcomeUnknownError,
    WriterDefinitelyNotExecutedError,
    WriterError,
    WriterResultTimeoutError,
)
from paritygrid.domain.models import UtcTimestamp

RUN_ID = "run_p79"
OTHER_RUN_ID = "run_other"
FINGERPRINT = "f" * 64
NODE_A = "node_a"
NODE_B = "node_b"
NODE_C = "node_c"
PART_0 = "p0"
PART_1 = "p1"
WORK_A = "work_a"
WORK_B = "work_b"
OWNER_A = "worker_a"
OWNER_B = "worker_b"

_DEFAULT_SINK = object()

# Built by concatenation so this file never contains the contiguous marker.
_FORBIDDEN_DRIVER_MARKERS: tuple[str, ...] = (
    "sql" + "ite3",
    "sqlal" + "chemy",
    "create" + "_engine",
)


def _now() -> UtcTimestamp:
    return UtcTimestamp(datetime(2025, 1, 1, tzinfo=UTC))


def _identity(
    node_id: str = NODE_A,
    partition_key: str = PART_0,
    run_id: str = RUN_ID,
) -> WorkIdentity:
    return WorkIdentity(run_id=run_id, node_id=node_id, partition_key=partition_key)


def _assignment(
    *,
    node_id: str = NODE_A,
    partition_key: str = PART_0,
    run_id: str = RUN_ID,
    work_item_id: str = WORK_A,
    attempt_number: int = 1,
    lease_fence: int = 7,
    lease_owner: str = OWNER_A,
    control_generation: int = 1,
    allowed_artifacts: tuple[str, ...] = (),
) -> RegisteredAssignment:
    return RegisteredAssignment(
        identity=_identity(node_id=node_id, partition_key=partition_key, run_id=run_id),
        work_item_id=work_item_id,
        attempt_number=attempt_number,
        lease_fence=lease_fence,
        lease_owner=lease_owner,
        control_generation=control_generation,
        deadline_micros=1_000_000,
        allowed_artifact_ids=allowed_artifacts,
    )


def _result(
    *,
    node_id: str = NODE_A,
    partition_key: str = PART_0,
    run_id: str = RUN_ID,
    work_item_id: str = WORK_A,
    attempt_number: int = 1,
    lease_fence: int = 7,
    lease_owner: str = OWNER_A,
    generation: int = 1,
    outcome: ContractOutcome = ContractOutcome.SUCCEEDED,
    artifacts: tuple[str, ...] = (),
    checkpoint_proposal: bool = True,
) -> WorkResultV1:
    return WorkResultV1(
        protocol=WORK_RESULT_PROTOCOL,
        contract_version=RUNNER_CONTRACT_VERSION,
        run_id=run_id,
        node_id=node_id,
        partition_key=partition_key,
        work_item_id=work_item_id,
        attempt_number=attempt_number,
        lease_fence=lease_fence,
        lease_owner=lease_owner,
        control_generation=ControlGeneration(generation),
        outcome=outcome,
        metrics=(ContractMetric(name="records", value=1),),
        artifact_references=artifacts,
        checkpoint_proposal=checkpoint_proposal,
        failure_detail=None,
        cleanup=ContractCleanupEvidence(
            status=ContractCleanupStatus.COMPLETED,
            actions=(),
            idempotency_key="cleanup-1",
        ),
    )


def _frontier(
    *,
    run_id: str = RUN_ID,
    node_id: str = NODE_A,
    run_row_version: int = 5,
    node_row_version: int = 3,
    next_event_sequence: int = 5,
    event_counter_row_version: int = 2,
    attempt_state: str = "running",
    expires_at_micros: int = 2_000_000,
) -> RebasedFrontier:
    return RebasedFrontier(
        run_id=run_id,
        run_row_version=run_row_version,
        node_id=node_id,
        node_row_version=node_row_version,
        next_event_sequence=next_event_sequence,
        event_counter_row_version=event_counter_row_version,
        attempt_state=attempt_state,
        expires_at_micros=expires_at_micros,
    )


class _ReaderDouble:
    """Scripted parent-side rebase evidence provider."""

    __slots__ = ("_script", "calls")

    def __init__(self) -> None:
        self._script: dict[tuple[str, str], list[RebasedFrontier]] = {}
        self.calls: list[tuple[str, str, str, str]] = []

    def plan(self, node_id: str, partition_key: str, frontier: RebasedFrontier) -> None:
        self._script.setdefault((node_id, partition_key), []).append(frontier)

    def rebase(
        self,
        run_id: str,
        node_id: str,
        partition_key: str,
        work_item_id: str,
    ) -> RebasedFrontier:
        self.calls.append((run_id, node_id, partition_key, work_item_id))
        queue = self._script.get((node_id, partition_key))
        if not queue:
            raise AssertionError(f"no scripted frontier for {(node_id, partition_key)}")
        return queue.pop(0)


@dataclass(frozen=True, slots=True)
class _ReceiptDouble:
    committed: bool
    committed_intent: object


class _TicketDouble:
    """Scripted admission ticket recording its wait bound."""

    __slots__ = ("_receipt", "_result_error", "_submission_id", "result_timeouts")

    def __init__(self, number: int, result_error: Exception | None, receipt: object) -> None:
        self._submission_id = number
        self._result_error = result_error
        self._receipt = receipt
        self.result_timeouts: list[float] = []

    @property
    def submission_id(self) -> int:
        return self._submission_id

    def result(self, *, timeout_seconds: float) -> object:
        self.result_timeouts.append(timeout_seconds)
        if self._result_error is not None:
            raise self._result_error
        return self._receipt


class _ExplodingTicket:
    __slots__ = ()

    @property
    def submission_id(self) -> object:
        raise RuntimeError("ticket identity exploded")


class _NoIdentityTicket:
    __slots__ = ()

    @property
    def submission_id(self) -> object:
        return None


class _AdmissionOnlyTicket:
    __slots__ = ()

    @property
    def submission_id(self) -> object:
        return 99


class _ExplodingReceipt:
    __slots__ = ()

    @property
    def committed(self) -> object:
        raise RuntimeError("receipt exploded")


class _WriterDouble:
    """Scripted serialized-writer double recording accepted commit intents."""

    __slots__ = (
        "admission_errors",
        "commands",
        "gate",
        "receipt_overrides",
        "result_errors",
        "submitted",
        "ticket_overrides",
        "tickets",
    )

    def __init__(self) -> None:
        self.commands: list[CommitIntent] = []
        self.admission_errors: list[Exception] = []
        self.result_errors: list[Exception] = []
        self.receipt_overrides: list[object] = []
        self.ticket_overrides: list[object] = []
        self.tickets: list[_TicketDouble] = []
        self.submitted = Event()
        self.gate: Event | None = None

    def submit(self, command: object, *, timeout_seconds: float) -> object:
        self.submitted.set()
        gate = self.gate
        if gate is not None:
            gate.wait()
        if self.admission_errors:
            raise self.admission_errors.pop(0)
        if self.ticket_overrides:
            return self.ticket_overrides.pop(0)
        intent = cast(CommitIntent, command)
        self.commands.append(intent)
        result_error = self.result_errors.pop(0) if self.result_errors else None
        receipt = (
            self.receipt_overrides.pop(0)
            if self.receipt_overrides
            else _ReceiptDouble(True, intent)
        )
        ticket = _TicketDouble(len(self.commands), result_error, receipt)
        self.tickets.append(ticket)
        return ticket


class _RunnerDouble:
    """Runner-side worker double: it receives only the result channel."""

    __slots__ = ("_channel",)

    def __init__(self, channel: BoundedChannel) -> None:
        self._channel = channel

    def deliver(self, result: WorkResultV1) -> None:
        self._channel.send(result)


class _Harness:
    """One full parent-side boundary built from real coordinator collaborators."""

    def __init__(
        self,
        *,
        node_ids: tuple[str, ...] = (NODE_A,),
        edges: tuple[tuple[str, str], ...] = (),
        partitions: dict[str, tuple[str, ...]] | None = None,
        telemetry_sink: object = _DEFAULT_SINK,
        admission_timeout_seconds: float | None = None,
        result_timeout_seconds: float | None = None,
    ) -> None:
        self.scheduler = ConcurrentScheduler(
            run_id=RUN_ID,
            plan_fingerprint=FINGERPRINT,
            node_order=node_ids,
            edges=edges,
            partitions_by_node=dict.fromkeys(node_ids, (PART_0,))
            if partitions is None
            else partitions,
            control_generation=ControlGeneration(1),
        )
        self.clock = ManualClock(_now())
        self.capacity = ScheduledWorkLimiters(
            CapturedConcurrencySettings(),
            strategy_id="sequential",
            node_ids=tuple(sorted(node_ids)),
            clock=self.clock,
        )
        self.channel = BoundedChannel(kind=CHANNEL_KIND_RESULT, capacity=8)
        self.reader = _ReaderDouble()
        self.writer = _WriterDouble()
        self.telemetry: list[TelemetryRecord] = []
        if telemetry_sink is _DEFAULT_SINK:
            sink: Callable[[TelemetryRecord], None] | None = self.telemetry.append
        elif telemetry_sink is None:
            sink = None
        else:
            sink = cast(Callable[[TelemetryRecord], None], telemetry_sink)
        options: dict[str, Any] = {}
        if admission_timeout_seconds is not None:
            options["admission_timeout_seconds"] = admission_timeout_seconds
        if result_timeout_seconds is not None:
            options["result_timeout_seconds"] = result_timeout_seconds
        self.coordinator = ConcurrentResultCoordinator(
            run_id=RUN_ID,
            plan_fingerprint=FINGERPRINT,
            control_generation=1,
            reader=self.reader,
            writer=self.writer,
            result_channel=self.channel,
            scheduler=self.scheduler,
            capacity=self.capacity,
            telemetry_sink=sink,
            **options,
        )

    def admit(self, assignment: RegisteredAssignment) -> None:
        """Admit one assignment through the scheduler, capacity, and registration."""
        self.scheduler.register_admission(assignment.identity, assignment.lease_fence)
        self.capacity.acquire(assignment.lease_owner, assignment.identity.node_id)
        self.coordinator.register_assignment(assignment)

    def expect(
        self,
        assignment: RegisteredAssignment,
        *,
        frontier: RebasedFrontier | None = None,
    ) -> None:
        self.reader.plan(
            assignment.identity.node_id,
            assignment.identity.partition_key,
            _frontier(node_id=assignment.identity.node_id) if frontier is None else frontier,
        )

    def work_states(self) -> dict[WorkIdentity, FrontierWorkState]:
        return dict(self.scheduler.frontier.work_states)


def _raising_sink(_: TelemetryRecord) -> None:
    raise RuntimeError("telemetry sink exploded")


# ---------------------------------------------------------------------------
# Value objects: RegisteredAssignment
# ---------------------------------------------------------------------------


def test_registered_assignment_accepts_minimal_facts() -> None:
    assignment = _assignment()
    assert assignment.identity == _identity()
    assert assignment.allowed_artifact_ids == ()
    assert "lease_owner=<redacted>" in repr(assignment)


def test_registered_assignment_rejects_non_exact_identity() -> None:
    with pytest.raises(TypeError):
        RegisteredAssignment(
            identity=cast(WorkIdentity, object()),
            work_item_id=WORK_A,
            attempt_number=1,
            lease_fence=7,
            lease_owner=OWNER_A,
            control_generation=1,
            deadline_micros=1,
            allowed_artifact_ids=(),
        )


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (123, TypeError),
        ("", ResultValidationRejection),
        ("x" * 129, ResultValidationRejection),
        ("bad\nid", ResultValidationRejection),
    ],
)
def test_registered_assignment_rejects_invalid_work_item_ids(
    value: object,
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        _assignment(work_item_id=cast(str, value))


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (0, ResultValidationRejection),
        (2**31, ResultValidationRejection),
        ("1", TypeError),
        (True, TypeError),
    ],
)
def test_registered_assignment_rejects_invalid_attempts(
    value: object,
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        _assignment(attempt_number=cast(int, value))


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (0, ResultValidationRejection),
        (2**31, ResultValidationRejection),
        (None, TypeError),
    ],
)
def test_registered_assignment_rejects_invalid_lease_fences(
    value: object,
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        _assignment(lease_fence=cast(int, value))


@pytest.mark.parametrize(
    ("value", "error"),
    [
        ("", ResultValidationRejection),
        ("owner with spaces is fine but this is too long" + "x" * 128, ResultValidationRejection),
        (31, TypeError),
    ],
)
def test_registered_assignment_rejects_invalid_lease_owners(
    value: object,
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        _assignment(lease_owner=cast(str, value))


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (0, ResultValidationRejection),
        (-1, ResultValidationRejection),
        ("1", TypeError),
    ],
)
def test_registered_assignment_rejects_invalid_control_generations(
    value: object,
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        _assignment(control_generation=cast(int, value))


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (-1, ResultValidationRejection),
        (2**63, ResultValidationRejection),
        ("1000", TypeError),
    ],
)
def test_registered_assignment_rejects_invalid_deadlines(
    value: object,
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        RegisteredAssignment(
            identity=_identity(),
            work_item_id=WORK_A,
            attempt_number=1,
            lease_fence=7,
            lease_owner=OWNER_A,
            control_generation=1,
            deadline_micros=cast(int, value),
            allowed_artifact_ids=(),
        )


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (["artifact-1"], TypeError),
        (("b", "a"), ResultValidationRejection),
        (("a", "a"), ResultValidationRejection),
        ((1,), TypeError),
        (("x" * 257,), ResultValidationRejection),
        (tuple(f"artifact-{index}" for index in range(65)), ResultValidationRejection),
        (("bad\nname",), ResultValidationRejection),
    ],
)
def test_registered_assignment_rejects_invalid_allowed_artifacts(
    value: object,
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        _assignment(allowed_artifacts=cast(tuple[str, ...], value))


# ---------------------------------------------------------------------------
# Value objects: RebasedFrontier
# ---------------------------------------------------------------------------


def test_rebased_frontier_accepts_current_evidence() -> None:
    frontier = _frontier()
    assert frontier.run_row_version == 5
    assert frontier.attempt_state == "running"


@pytest.mark.parametrize("attempt_state", ["running", "awaiting_result", "expired"])
def test_rebased_frontier_accepts_closed_attempt_states(attempt_state: str) -> None:
    assert _frontier(attempt_state=attempt_state).attempt_state == attempt_state


@pytest.mark.parametrize(
    ("value", "error"),
    [
        ("", ResultValidationRejection),
        ("x" * 129, ResultValidationRejection),
        (7, TypeError),
    ],
)
@pytest.mark.parametrize("field", ["run_id", "node_id"])
def test_rebased_frontier_rejects_invalid_identity_text(
    field: str,
    value: object,
    error: type[Exception],
) -> None:
    options = {field: value}
    with pytest.raises(error):
        _frontier(**cast(Any, options))


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (-1, ResultValidationRejection),
        (2**63, ResultValidationRejection),
        ("5", TypeError),
        (True, TypeError),
    ],
)
@pytest.mark.parametrize(
    "field",
    ["run_row_version", "node_row_version", "event_counter_row_version", "expires_at_micros"],
)
def test_rebased_frontier_rejects_invalid_versions(
    field: str,
    value: object,
    error: type[Exception],
) -> None:
    options = {field: value}
    with pytest.raises(error):
        _frontier(**cast(Any, options))


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (0, ResultValidationRejection),
        (2**31, ResultValidationRejection),
        ("5", TypeError),
    ],
)
def test_rebased_frontier_rejects_invalid_event_sequences(
    value: object,
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        _frontier(next_event_sequence=cast(int, value))


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (123, TypeError),
        ("pending", ResultValidationRejection),
        ("expired2", ResultValidationRejection),
        ("RUNNING", ResultValidationRejection),
    ],
)
def test_rebased_frontier_rejects_invalid_attempt_states(
    value: object,
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        _frontier(attempt_state=cast(str, value))


# ---------------------------------------------------------------------------
# Value objects: CommitIntent
# ---------------------------------------------------------------------------


def test_commit_intent_carries_rebased_facts() -> None:
    intent = CommitIntent(
        run_id=RUN_ID,
        node_id=NODE_A,
        partition_key=PART_0,
        work_item_id=WORK_A,
        outcome="succeeded",
        expected_run_row_version=9,
        expected_node_row_version=4,
        next_event_sequence=7,
        event_counter_row_version=3,
        checkpoint_proposed=True,
        artifact_ids=("artifact-1",),
    )
    assert intent.outcome == "succeeded"
    assert intent.expected_run_row_version == 9


@pytest.mark.parametrize(
    ("value", "error"),
    [
        ("", ResultValidationRejection),
        ("detonated", ResultValidationRejection),
        (5, TypeError),
    ],
)
def test_commit_intent_rejects_invalid_outcomes(value: object, error: type[Exception]) -> None:
    with pytest.raises(error):
        CommitIntent(
            run_id=RUN_ID,
            node_id=NODE_A,
            partition_key=PART_0,
            work_item_id=WORK_A,
            outcome=cast(str, value),
            expected_run_row_version=1,
            expected_node_row_version=1,
            next_event_sequence=1,
            event_counter_row_version=1,
            checkpoint_proposed=False,
            artifact_ids=(),
        )


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"expected_run_row_version": -1}, ResultValidationRejection),
        ({"expected_node_row_version": 2**63}, ResultValidationRejection),
        ({"next_event_sequence": 0}, ResultValidationRejection),
        ({"next_event_sequence": 2**31}, ResultValidationRejection),
        ({"event_counter_row_version": "1"}, TypeError),
        ({"checkpoint_proposed": 1}, TypeError),
        ({"checkpoint_proposed": None}, TypeError),
        ({"artifact_ids": ["artifact-1"]}, TypeError),
        ({"artifact_ids": ("x" * 257,)}, ResultValidationRejection),
        ({"run_id": ""}, ResultValidationRejection),
        ({"partition_key": 3}, TypeError),
        ({"work_item_id": None}, TypeError),
    ],
)
def test_commit_intent_rejects_invalid_fields(
    overrides: dict[str, object],
    error: type[Exception],
) -> None:
    fields: dict[str, object] = {
        "run_id": RUN_ID,
        "node_id": NODE_A,
        "partition_key": PART_0,
        "work_item_id": WORK_A,
        "outcome": "succeeded",
        "expected_run_row_version": 1,
        "expected_node_row_version": 1,
        "next_event_sequence": 1,
        "event_counter_row_version": 1,
        "checkpoint_proposed": False,
        "artifact_ids": (),
    }
    fields.update(overrides)
    with pytest.raises(error):
        CommitIntent(**cast(Any, fields))


# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------


def test_constructor_accepts_defaults_and_starts_idle() -> None:
    harness = _Harness()
    assert harness.coordinator.registered_identities == ()
    assert harness.coordinator.ambiguous_identities == ()
    assert harness.coordinator.is_admission_stopped is False
    assert harness.coordinator.committed_count == 0
    assert harness.coordinator.rejected_count == 0
    assert RESULT_COORDINATOR_VERSION == 1
    assert MAX_IN_FLIGHT_RESULTS == 256
    assert COORDINATOR_ADMISSION_TIMEOUT_SECONDS == 5.0
    assert COORDINATOR_RESULT_TIMEOUT_SECONDS == 60.0
    assert issubclass(ResultCoordinatorError, RuntimeError)


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (123, TypeError),
        ("", ResultValidationRejection),
        ("x" * 129, ResultValidationRejection),
    ],
)
def test_constructor_rejects_invalid_run_ids(value: object, error: type[Exception]) -> None:
    harness = _Harness()
    with pytest.raises(error):
        ConcurrentResultCoordinator(
            run_id=cast(str, value),
            plan_fingerprint=FINGERPRINT,
            control_generation=1,
            reader=harness.reader,
            writer=harness.writer,
            result_channel=harness.channel,
            scheduler=harness.scheduler,
            capacity=harness.capacity,
        )


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (123, TypeError),
        ("", ResultValidationRejection),
        ("z" * 64, ResultValidationRejection),
        ("f" * 63, ResultValidationRejection),
    ],
)
def test_constructor_rejects_invalid_plan_fingerprints(
    value: object,
    error: type[Exception],
) -> None:
    harness = _Harness()
    with pytest.raises(error):
        ConcurrentResultCoordinator(
            run_id=RUN_ID,
            plan_fingerprint=cast(str, value),
            control_generation=1,
            reader=harness.reader,
            writer=harness.writer,
            result_channel=harness.channel,
            scheduler=harness.scheduler,
            capacity=harness.capacity,
        )


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (0, ResultValidationRejection),
        (-1, ResultValidationRejection),
        ("1", TypeError),
    ],
)
def test_constructor_rejects_invalid_control_generations(
    value: object,
    error: type[Exception],
) -> None:
    harness = _Harness()
    with pytest.raises(error):
        ConcurrentResultCoordinator(
            run_id=RUN_ID,
            plan_fingerprint=FINGERPRINT,
            control_generation=cast(int, value),
            reader=harness.reader,
            writer=harness.writer,
            result_channel=harness.channel,
            scheduler=harness.scheduler,
            capacity=harness.capacity,
        )


def test_constructor_rejects_reader_without_rebase() -> None:
    harness = _Harness()
    with pytest.raises(TypeError):
        ConcurrentResultCoordinator(
            run_id=RUN_ID,
            plan_fingerprint=FINGERPRINT,
            control_generation=1,
            reader=cast(ResultCoordinatorReader, object()),
            writer=harness.writer,
            result_channel=harness.channel,
            scheduler=harness.scheduler,
            capacity=harness.capacity,
        )


def test_constructor_rejects_writer_without_submit() -> None:
    harness = _Harness()
    with pytest.raises(TypeError):
        ConcurrentResultCoordinator(
            run_id=RUN_ID,
            plan_fingerprint=FINGERPRINT,
            control_generation=1,
            reader=harness.reader,
            writer=cast(ResultCoordinatorWriter, object()),
            result_channel=harness.channel,
            scheduler=harness.scheduler,
            capacity=harness.capacity,
        )


def test_constructor_rejects_non_channel_result_channel() -> None:
    harness = _Harness()
    with pytest.raises(TypeError):
        ConcurrentResultCoordinator(
            run_id=RUN_ID,
            plan_fingerprint=FINGERPRINT,
            control_generation=1,
            reader=harness.reader,
            writer=harness.writer,
            result_channel=cast(BoundedChannel, object()),
            scheduler=harness.scheduler,
            capacity=harness.capacity,
        )


def test_constructor_rejects_wrong_kind_result_channel() -> None:
    harness = _Harness()
    telemetry_channel = BoundedChannel(kind=CHANNEL_KIND_TELEMETRY, capacity=4)
    with pytest.raises(ResultValidationRejection):
        ConcurrentResultCoordinator(
            run_id=RUN_ID,
            plan_fingerprint=FINGERPRINT,
            control_generation=1,
            reader=harness.reader,
            writer=harness.writer,
            result_channel=telemetry_channel,
            scheduler=harness.scheduler,
            capacity=harness.capacity,
        )


def test_constructor_rejects_wrong_scheduler_type() -> None:
    harness = _Harness()
    with pytest.raises(TypeError):
        ConcurrentResultCoordinator(
            run_id=RUN_ID,
            plan_fingerprint=FINGERPRINT,
            control_generation=1,
            reader=harness.reader,
            writer=harness.writer,
            result_channel=harness.channel,
            scheduler=cast(ConcurrentScheduler, harness.scheduler.frontier),
            capacity=harness.capacity,
        )


def test_constructor_rejects_wrong_capacity_type() -> None:
    harness = _Harness()
    with pytest.raises(TypeError):
        ConcurrentResultCoordinator(
            run_id=RUN_ID,
            plan_fingerprint=FINGERPRINT,
            control_generation=1,
            reader=harness.reader,
            writer=harness.writer,
            result_channel=harness.channel,
            scheduler=harness.scheduler,
            capacity=cast(ScheduledWorkLimiters, object()),
        )


@pytest.mark.parametrize("field", ["admission_timeout_seconds", "result_timeout_seconds"])
@pytest.mark.parametrize(
    ("value", "error"),
    [
        ("1.0", TypeError),
        (1, TypeError),
        (True, TypeError),
        (-0.1, ResultValidationRejection),
        (float("inf"), ResultValidationRejection),
        (float("nan"), ResultValidationRejection),
        (86_400.5, ResultValidationRejection),
    ],
)
def test_constructor_rejects_invalid_timeouts(
    field: str,
    value: object,
    error: type[Exception],
) -> None:
    options = {field: value}
    harness = _Harness()
    with pytest.raises(error):
        ConcurrentResultCoordinator(
            run_id=RUN_ID,
            plan_fingerprint=FINGERPRINT,
            control_generation=1,
            reader=harness.reader,
            writer=harness.writer,
            result_channel=harness.channel,
            scheduler=harness.scheduler,
            capacity=harness.capacity,
            **cast(Any, options),
        )


def test_constructor_rejects_non_callable_telemetry_sink() -> None:
    harness = _Harness()
    with pytest.raises(TypeError):
        ConcurrentResultCoordinator(
            run_id=RUN_ID,
            plan_fingerprint=FINGERPRINT,
            control_generation=1,
            reader=harness.reader,
            writer=harness.writer,
            result_channel=harness.channel,
            scheduler=harness.scheduler,
            capacity=harness.capacity,
            telemetry_sink=cast("Callable[[TelemetryRecord], None] | None", 123),
        )


def test_coordinator_repr_reports_bounded_counts() -> None:
    harness = _Harness()
    assert "run_p79" in repr(harness.coordinator)
    assert "registered=0" in repr(harness.coordinator)


# ---------------------------------------------------------------------------
# register_assignment
# ---------------------------------------------------------------------------


def test_register_assignment_records_identity() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.coordinator.register_assignment(assignment)
    assert harness.coordinator.registered_identities == (assignment.identity,)


def test_register_assignment_rejects_non_exact_type() -> None:
    harness = _Harness()
    with pytest.raises(TypeError):
        harness.coordinator.register_assignment(cast(RegisteredAssignment, object()))


def test_register_assignment_rejects_duplicate_identity() -> None:
    harness = _Harness()
    harness.coordinator.register_assignment(_assignment())
    with pytest.raises(ResultStaleRejection):
        harness.coordinator.register_assignment(_assignment(lease_fence=8))


def test_register_assignment_rejects_foreign_run() -> None:
    harness = _Harness()
    with pytest.raises(ResultStaleRejection):
        harness.coordinator.register_assignment(_assignment(run_id=OTHER_RUN_ID))


def test_register_assignment_refuses_after_close() -> None:
    harness = _Harness()
    harness.coordinator.close()
    with pytest.raises(ResultCoordinatorClosedError):
        harness.coordinator.register_assignment(_assignment())


def test_register_assignment_enforces_in_flight_bound() -> None:
    harness = _Harness()
    for index in range(MAX_IN_FLIGHT_RESULTS):
        harness.coordinator.register_assignment(
            _assignment(partition_key=f"part-{index:04d}"),
        )
    with pytest.raises(ResultValidationRejection):
        harness.coordinator.register_assignment(
            _assignment(partition_key=f"part-{MAX_IN_FLIGHT_RESULTS:04d}"),
        )


def test_registered_identities_are_sorted_deterministically() -> None:
    harness = _Harness(node_ids=(NODE_A, NODE_B))
    second = _assignment(node_id=NODE_B)
    first = _assignment()
    harness.coordinator.register_assignment(second)
    harness.coordinator.register_assignment(first)
    assert harness.coordinator.registered_identities == (first.identity, second.identity)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_happy_path_commits_one_rebased_result() -> None:
    harness = _Harness()
    assignment = _assignment(allowed_artifacts=("artifact-1", "artifact-2"))
    harness.admit(assignment)
    harness.expect(assignment)
    result = _result(artifacts=("artifact-1",))
    harness.channel.send(result)

    harness.coordinator.receive_next(timeout=0)

    assert len(harness.writer.commands) == 1
    assert harness.coordinator.committed_count == 1
    assert harness.coordinator.rejected_count == 0
    assert harness.coordinator.registered_identities == ()
    assert harness.work_states()[assignment.identity] is FrontierWorkState.SUCCEEDED
    assert harness.capacity.in_use(CAPACITY_CATEGORY_GLOBAL) == 0
    assert harness.capacity.in_use(CAPACITY_CATEGORY_NODE, NODE_A) == 0
    assert len(harness.telemetry) == 1


def test_writer_command_carries_every_rebased_fact() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(
        assignment,
        frontier=_frontier(
            run_row_version=9,
            node_row_version=4,
            next_event_sequence=7,
            event_counter_row_version=3,
        ),
    )
    harness.coordinator.submit_result(
        _result(outcome=ContractOutcome.QUARANTINED, checkpoint_proposal=False)
    )
    intent = harness.writer.commands[0]
    assert intent == CommitIntent(
        run_id=RUN_ID,
        node_id=NODE_A,
        partition_key=PART_0,
        work_item_id=WORK_A,
        outcome="quarantined",
        expected_run_row_version=9,
        expected_node_row_version=4,
        next_event_sequence=7,
        event_counter_row_version=3,
        checkpoint_proposed=False,
        artifact_ids=(),
    )


def test_rebase_is_called_once_with_registered_facts() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.coordinator.submit_result(_result())
    assert harness.reader.calls == [(RUN_ID, NODE_A, PART_0, WORK_A)]


def test_writer_timeouts_carry_configured_bounds() -> None:
    harness = _Harness(admission_timeout_seconds=1.5, result_timeout_seconds=2.5)
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.coordinator.submit_result(_result())
    assert len(harness.writer.tickets) == 1
    assert harness.writer.tickets[0].result_timeouts == [2.5]
    harness_two = _Harness()
    assignment_two = _assignment()
    harness_two.admit(assignment_two)
    harness_two.expect(assignment_two)
    harness_two.coordinator.submit_result(_result())
    assert harness_two.writer.tickets[0].result_timeouts == [60.0]


@pytest.mark.parametrize(
    ("outcome", "expected_state"),
    [
        (ContractOutcome.SUCCEEDED, FrontierWorkState.SUCCEEDED),
        (ContractOutcome.QUARANTINED, FrontierWorkState.QUARANTINED),
        (ContractOutcome.FAILED, FrontierWorkState.FAILED),
        (ContractOutcome.CANCELLED, FrontierWorkState.CANCELLED),
    ],
)
def test_terminal_outcomes_map_to_scheduler_states(
    outcome: ContractOutcome,
    expected_state: FrontierWorkState,
) -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.coordinator.submit_result(_result(outcome=outcome))
    assert harness.work_states()[assignment.identity] is expected_state
    assert harness.capacity.in_use(CAPACITY_CATEGORY_GLOBAL) == 0


def test_retry_wait_outcome_schedules_durable_retry() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.coordinator.submit_result(
        _result(outcome=ContractOutcome.RETRY_WAIT, checkpoint_proposal=False),
        retry_eligible_at_micros=500,
    )
    states = harness.work_states()
    assert states[assignment.identity] is FrontierWorkState.RETRY_WAIT
    (wait,) = harness.scheduler.frontier.retry_waits
    assert wait.identity == assignment.identity
    assert wait.eligible_at_micros == 500
    assert wait.reason == "attempt_1_retry_wait"
    assert harness.capacity.in_use(CAPACITY_CATEGORY_GLOBAL) == 0


def test_retry_wait_default_eligibility_is_zero() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.coordinator.submit_result(_result(outcome=ContractOutcome.RETRY_WAIT))
    (wait,) = harness.scheduler.frontier.retry_waits
    assert wait.eligible_at_micros == 0


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (-1, ResultValidationRejection),
        (2**31, ResultValidationRejection),
        ("500", TypeError),
    ],
)
def test_invalid_retry_eligibility_is_rejected_before_any_command(
    value: object,
    error: type[Exception],
) -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    with pytest.raises(error):
        harness.coordinator.submit_result(
            _result(),
            retry_eligible_at_micros=cast(int, value),
        )
    assert harness.writer.commands == []


def test_artifact_subset_of_allowed_set_commits() -> None:
    harness = _Harness()
    assignment = _assignment(allowed_artifacts=("artifact-1", "artifact-2"))
    harness.admit(assignment)
    harness.expect(assignment)
    harness.coordinator.submit_result(_result(artifacts=("artifact-2", "artifact-1")))
    assert harness.writer.commands[0].artifact_ids == ("artifact-2", "artifact-1")


# ---------------------------------------------------------------------------
# Pre-admission stale and validation matrix
# ---------------------------------------------------------------------------


def _rejected_without_writer(
    harness: _Harness,
    assignment: RegisteredAssignment,
    result: WorkResultV1,
    error: type[Exception],
) -> None:
    """Assert one rejected result submits nothing and retains everything."""
    frontier_before = harness.scheduler.frontier
    with pytest.raises(error):
        harness.coordinator.submit_result(result)
    assert harness.writer.commands == []
    assert harness.reader.calls == []
    assert harness.coordinator.registered_identities == (assignment.identity,)
    assert harness.scheduler.frontier == frontier_before
    assert harness.coordinator.committed_count == 0
    assert harness.coordinator.rejected_count == 1
    assert harness.capacity.in_use(CAPACITY_CATEGORY_GLOBAL) == 1


def test_wrong_attempt_number_is_stale() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    _rejected_without_writer(
        harness,
        assignment,
        _result(attempt_number=2),
        ResultStaleRejection,
    )


def test_wrong_lease_fence_is_stale() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    _rejected_without_writer(harness, assignment, _result(lease_fence=8), ResultStaleRejection)


def test_wrong_lease_owner_is_stale() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    _rejected_without_writer(
        harness,
        assignment,
        _result(lease_owner=OWNER_B),
        ResultStaleRejection,
    )


def test_wrong_control_generation_is_stale() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    _rejected_without_writer(harness, assignment, _result(generation=2), ResultStaleRejection)


def test_wrong_work_item_id_is_stale() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    _rejected_without_writer(
        harness, assignment, _result(work_item_id=WORK_B), ResultStaleRejection
    )


def test_unregistered_identity_is_stale() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    other = _result(node_id=NODE_B)
    with pytest.raises(ResultStaleRejection):
        harness.coordinator.submit_result(other)
    assert harness.writer.commands == []
    assert harness.coordinator.registered_identities == (assignment.identity,)
    assert harness.coordinator.rejected_count == 1


def test_foreign_run_result_is_stale() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    _rejected_without_writer(
        harness,
        assignment,
        _result(run_id=OTHER_RUN_ID),
        ResultStaleRejection,
    )


def test_duplicate_terminal_result_is_stale() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.scheduler.commit_result(assignment.identity, "succeeded")
    _rejected_without_writer(harness, assignment, _result(), ResultStaleRejection)


def test_scheduler_without_in_flight_work_is_stale() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.capacity.acquire(assignment.lease_owner, assignment.identity.node_id)
    harness.coordinator.register_assignment(assignment)
    _rejected_without_writer(harness, assignment, _result(), ResultStaleRejection)


def test_forged_artifact_reference_is_rejected() -> None:
    harness = _Harness()
    assignment = _assignment(allowed_artifacts=("artifact-1",))
    harness.admit(assignment)
    _rejected_without_writer(
        harness,
        assignment,
        _result(artifacts=("artifact-1", "forged-2")),
        ResultForgedReferenceRejection,
    )


class _EnvelopeSubclass(WorkResultV1):
    """A non-exact envelope type that still passes value validation."""


def test_non_exact_envelope_type_is_rejected() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    envelope = _EnvelopeSubclass(
        protocol=WORK_RESULT_PROTOCOL,
        contract_version=RUNNER_CONTRACT_VERSION,
        run_id=RUN_ID,
        node_id=NODE_A,
        partition_key=PART_0,
        work_item_id=WORK_A,
        attempt_number=1,
        lease_fence=7,
        lease_owner=OWNER_A,
        control_generation=ControlGeneration(1),
        outcome=ContractOutcome.SUCCEEDED,
        metrics=(),
        artifact_references=(),
        checkpoint_proposal=True,
        failure_detail=None,
        cleanup=ContractCleanupEvidence(
            status=ContractCleanupStatus.PENDING,
            actions=(),
            idempotency_key=None,
        ),
    )
    _rejected_without_writer(harness, assignment, envelope, ResultValidationRejection)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("outcome", "detonated"),
        ("contract_version", 2),
        ("lease_owner", "bad\nowner"),
        ("attempt_number", 0),
    ],
)
def test_tampered_envelope_fails_contract_revalidation(
    field: str,
    value: object,
) -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    envelope = _result()
    object.__setattr__(envelope, field, value)
    _rejected_without_writer(harness, assignment, envelope, ResultValidationRejection)


def test_corrected_resubmission_after_pre_admission_rejection() -> None:
    harness = _Harness()
    assignment = _assignment(control_generation=3)
    harness.admit(assignment)
    harness.expect(assignment)
    with pytest.raises(ResultStaleRejection):
        harness.coordinator.submit_result(_result(generation=2))
    assert harness.writer.commands == []
    assert harness.coordinator.registered_identities == (assignment.identity,)
    harness.coordinator.submit_result(_result(generation=3))
    assert harness.coordinator.committed_count == 1
    assert len(harness.writer.commands) == 1
    assert harness.coordinator.registered_identities == ()


# ---------------------------------------------------------------------------
# Rebased evidence
# ---------------------------------------------------------------------------


def test_non_exact_rebased_evidence_is_rejected() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.reader.plan(NODE_A, PART_0, cast(RebasedFrontier, object()))
    with pytest.raises(ResultValidationRejection):
        harness.coordinator.submit_result(_result())
    assert harness.writer.commands == []
    assert harness.work_states()[assignment.identity] is FrontierWorkState.AWAITING_COMMIT
    assert harness.coordinator.registered_identities == (assignment.identity,)
    assert harness.coordinator.rejected_count == 1


def test_rebased_run_mismatch_is_stale() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment, frontier=_frontier(run_id=OTHER_RUN_ID))
    with pytest.raises(ResultStaleRejection):
        harness.coordinator.submit_result(_result())
    assert harness.writer.commands == []


def test_rebased_node_mismatch_is_stale() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment, frontier=_frontier(node_id=NODE_B))
    with pytest.raises(ResultStaleRejection):
        harness.coordinator.submit_result(_result())
    assert harness.writer.commands == []


def test_rebased_expired_attempt_is_stale() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment, frontier=_frontier(attempt_state="expired"))
    with pytest.raises(ResultStaleRejection):
        harness.coordinator.submit_result(_result())
    assert harness.writer.commands == []


def test_rebased_awaiting_result_state_still_commits() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment, frontier=_frontier(attempt_state="awaiting_result"))
    harness.coordinator.submit_result(_result())
    assert harness.coordinator.committed_count == 1


def test_writer_command_uses_rebased_versions_not_capture_time_values() -> None:
    harness = _Harness(node_ids=(NODE_A, NODE_B))
    assignment_a = _assignment(lease_owner=OWNER_A)
    assignment_b = _assignment(
        node_id=NODE_B,
        work_item_id=WORK_B,
        lease_fence=8,
        lease_owner=OWNER_B,
    )
    harness.admit(assignment_a)
    harness.admit(assignment_b)
    harness.expect(
        assignment_a,
        frontier=_frontier(
            node_id=NODE_A,
            run_row_version=11,
            node_row_version=6,
            next_event_sequence=9,
            event_counter_row_version=5,
        ),
    )
    harness.expect(
        assignment_b,
        frontier=_frontier(
            node_id=NODE_B,
            run_row_version=2,
            node_row_version=1,
            next_event_sequence=2,
            event_counter_row_version=1,
        ),
    )
    harness.coordinator.submit_result(_result())
    harness.coordinator.submit_result(
        _result(node_id=NODE_B, work_item_id=WORK_B, lease_fence=8, lease_owner=OWNER_B)
    )
    assert [intent.expected_run_row_version for intent in harness.writer.commands] == [11, 2]
    assert [intent.next_event_sequence for intent in harness.writer.commands] == [9, 2]
    assert [intent.expected_node_row_version for intent in harness.writer.commands] == [6, 1]


# ---------------------------------------------------------------------------
# Reversed completion order (core P7.9 acceptance)
# ---------------------------------------------------------------------------


def test_reversed_completion_commits_contiguous_events_and_releases_successors() -> None:
    harness = _Harness(
        node_ids=(NODE_A, NODE_B, NODE_C),
        edges=((NODE_A, NODE_C), (NODE_B, NODE_C)),
    )
    assignment_a = _assignment(lease_owner=OWNER_A)
    assignment_b = _assignment(
        node_id=NODE_B,
        work_item_id=WORK_B,
        lease_fence=8,
        lease_owner=OWNER_B,
    )
    harness.admit(assignment_a)
    harness.admit(assignment_b)
    # B completes FIRST against the current global frontier; A completes
    # SECOND against the frontier B's commit already advanced.
    harness.expect(
        assignment_b,
        frontier=_frontier(node_id=NODE_B, run_row_version=5, next_event_sequence=5),
    )
    harness.expect(
        assignment_a,
        frontier=_frontier(node_id=NODE_A, run_row_version=6, next_event_sequence=6),
    )
    successor = WorkIdentity(run_id=RUN_ID, node_id=NODE_C, partition_key=PART_0)

    harness.coordinator.submit_result(
        _result(node_id=NODE_B, work_item_id=WORK_B, lease_fence=8, lease_owner=OWNER_B)
    )
    assert harness.work_states()[successor] is FrontierWorkState.BLOCKED
    assert harness.capacity.in_use(CAPACITY_CATEGORY_GLOBAL) == 1

    harness.coordinator.submit_result(_result())
    states = harness.work_states()
    assert states[assignment_a.identity] is FrontierWorkState.SUCCEEDED
    assert states[assignment_b.identity] is FrontierWorkState.SUCCEEDED
    assert states[successor] is FrontierWorkState.READY
    sequences = [intent.next_event_sequence for intent in harness.writer.commands]
    assert sequences == [5, 6]
    assert [intent.expected_run_row_version for intent in harness.writer.commands] == [5, 6]
    assert harness.coordinator.committed_count == 2
    assert harness.capacity.in_use(CAPACITY_CATEGORY_GLOBAL) == 0


def test_same_node_out_of_order_partitions_advance_the_node_aggregate() -> None:
    harness = _Harness(node_ids=(NODE_A,), partitions={NODE_A: (PART_0, PART_1)})
    assignment_first = _assignment(partition_key=PART_0, lease_owner=OWNER_A)
    assignment_second = _assignment(
        partition_key=PART_1,
        work_item_id=WORK_B,
        lease_fence=8,
        lease_owner=OWNER_B,
    )
    harness.admit(assignment_first)
    harness.admit(assignment_second)
    # The p1 partition completes FIRST against the current node frontier;
    # p0 completes SECOND against the frontier p1's commit advanced.
    harness.expect(assignment_second, frontier=_frontier(run_row_version=4, next_event_sequence=5))
    harness.expect(assignment_first, frontier=_frontier(run_row_version=5, next_event_sequence=6))
    harness.coordinator.submit_result(
        _result(partition_key=PART_1, work_item_id=WORK_B, lease_fence=8, lease_owner=OWNER_B)
    )
    assert harness.capacity.in_use(CAPACITY_CATEGORY_NODE, NODE_A) == 1
    harness.coordinator.submit_result(_result(partition_key=PART_0))
    states = harness.work_states()
    assert states[assignment_first.identity] is FrontierWorkState.SUCCEEDED
    assert states[assignment_second.identity] is FrontierWorkState.SUCCEEDED
    assert [intent.next_event_sequence for intent in harness.writer.commands] == [5, 6]
    assert [intent.expected_node_row_version for intent in harness.writer.commands] == [3, 3]
    assert harness.capacity.in_use(CAPACITY_CATEGORY_NODE, NODE_A) == 0
    assert harness.scheduler.is_finished


# ---------------------------------------------------------------------------
# Writer failure classification
# ---------------------------------------------------------------------------


def _retryable_failure_then_success(
    harness: _Harness,
    assignment: RegisteredAssignment,
    *,
    command_accepted: bool = False,
) -> None:
    with pytest.raises(ResultWriterRetryableError):
        harness.coordinator.submit_result(_result())
    accepted = 1 if command_accepted else 0
    assert len(harness.writer.commands) == accepted
    assert harness.work_states()[assignment.identity] is FrontierWorkState.AWAITING_COMMIT
    assert harness.coordinator.registered_identities == (assignment.identity,)
    assert harness.capacity.in_use(CAPACITY_CATEGORY_GLOBAL) == 1
    assert harness.coordinator.committed_count == 0
    assert harness.coordinator.rejected_count == 0
    harness.expect(assignment)
    harness.coordinator.submit_result(_result())
    assert harness.coordinator.committed_count == 1
    assert len(harness.writer.commands) == accepted + 1
    assert harness.capacity.in_use(CAPACITY_CATEGORY_GLOBAL) == 0


def test_writer_admission_timeout_is_retryable() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.writer.admission_errors.append(WriterAdmissionTimeoutError("admission timeout"))
    _retryable_failure_then_success(harness, assignment)


def test_writer_closed_at_admission_is_retryable_with_zero_commits() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.writer.admission_errors.append(WriterClosedError("writer shut down"))
    _retryable_failure_then_success(harness, assignment)


def test_generic_writer_admission_error_is_retryable() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.writer.admission_errors.append(WriterError("writer rejected"))
    _retryable_failure_then_success(harness, assignment)


def test_definitely_not_executed_result_is_retryable() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.writer.result_errors.append(WriterDefinitelyNotExecutedError("never dispatched"))
    _retryable_failure_then_success(harness, assignment, command_accepted=True)


def _assert_unknown_path(harness: _Harness, assignment: RegisteredAssignment) -> None:
    assert harness.coordinator.is_admission_stopped is True
    assert harness.coordinator.ambiguous_identities == (assignment.identity,)
    assert harness.coordinator.committed_count == 0
    assert harness.coordinator.rejected_count == 0
    assert harness.coordinator.registered_identities == (assignment.identity,)
    assert harness.scheduler.frontier.is_recovery_required
    assert harness.scheduler.frontier.recovery_required_reason == "result_writer_outcome_unknown"
    assert harness.work_states()[assignment.identity] is FrontierWorkState.AWAITING_COMMIT
    assert harness.capacity.in_use(CAPACITY_CATEGORY_GLOBAL) == 1
    assert harness.channel.is_recovery_closed
    with pytest.raises(ChannelUnknownOutcomeError):
        harness.channel.send(_result())
    with pytest.raises(ResultCoordinatorClosedError):
        harness.coordinator.submit_result(_result())
    with pytest.raises(ResultCoordinatorClosedError):
        harness.coordinator.register_assignment(_assignment(partition_key="p9"))


@pytest.mark.parametrize(
    "error",
    [
        WriterResultTimeoutError("result timeout"),
        WriterCommitOutcomeUnknownError("commit outcome unknown"),
        WriterError("generic writer failure"),
        RuntimeError("unexpected writer crash"),
    ],
)
def test_unproven_ticket_results_stop_admission(error: Exception) -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.writer.result_errors.append(error)
    with pytest.raises(ResultOutcomeUnknownError):
        harness.coordinator.submit_result(_result())
    _assert_unknown_path(harness, assignment)


def test_unexpected_admission_exception_is_unknown() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.writer.admission_errors.append(RuntimeError("writer exploded"))
    with pytest.raises(ResultOutcomeUnknownError):
        harness.coordinator.submit_result(_result())
    _assert_unknown_path(harness, assignment)


def test_none_ticket_is_unknown() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.writer.ticket_overrides.append(None)
    with pytest.raises(ResultOutcomeUnknownError):
        harness.coordinator.submit_result(_result())
    _assert_unknown_path(harness, assignment)


def test_ticket_without_identity_is_unknown() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.writer.ticket_overrides.append(_NoIdentityTicket())
    with pytest.raises(ResultOutcomeUnknownError):
        harness.coordinator.submit_result(_result())
    _assert_unknown_path(harness, assignment)


def test_ticket_with_exploding_identity_is_unknown() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.writer.ticket_overrides.append(_ExplodingTicket())
    with pytest.raises(ResultOutcomeUnknownError):
        harness.coordinator.submit_result(_result())
    _assert_unknown_path(harness, assignment)


def test_ticket_without_result_callable_is_unknown() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.writer.ticket_overrides.append(_AdmissionOnlyTicket())
    with pytest.raises(ResultOutcomeUnknownError):
        harness.coordinator.submit_result(_result())
    _assert_unknown_path(harness, assignment)


@pytest.mark.parametrize(
    "receipt",
    [
        _ReceiptDouble(False, None),
        _ReceiptDouble(True, "wrong intent"),
        object(),
        _ExplodingReceipt(),
    ],
)
def test_receipts_that_do_not_acknowledge_the_intent_are_unknown(receipt: object) -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.writer.receipt_overrides.append(receipt)
    with pytest.raises(ResultOutcomeUnknownError):
        harness.coordinator.submit_result(_result())
    _assert_unknown_path(harness, assignment)


def test_unknown_path_keeps_channel_drainable_and_receive_is_noop() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.channel.send(_result())
    later_result = _result(node_id=NODE_B)
    harness.channel.send(later_result)
    harness.writer.result_errors.append(WriterResultTimeoutError("result timeout"))
    with pytest.raises(ResultOutcomeUnknownError):
        harness.coordinator.receive_next(timeout=0)
    assert harness.channel.queued == 1
    drained = harness.channel.drain()
    assert drained == (later_result,)
    assert harness.coordinator.receive_next(timeout=0) is None
    assert len(harness.reader.calls) == 1


# ---------------------------------------------------------------------------
# Backpressure and serialized admission
# ---------------------------------------------------------------------------


def test_second_submission_parks_on_admission_lock_while_writer_blocks() -> None:
    harness = _Harness(node_ids=(NODE_A,), partitions={NODE_A: (PART_0, PART_1)})
    assignment_a = _assignment(work_item_id=WORK_A, lease_owner=OWNER_A)
    assignment_b = _assignment(
        partition_key=PART_1,
        work_item_id=WORK_B,
        lease_fence=8,
        lease_owner=OWNER_B,
    )
    harness.admit(assignment_a)
    harness.admit(assignment_b)
    harness.expect(assignment_a)
    harness.expect(assignment_b)
    harness.writer.gate = Event()
    failures: list[Exception] = []

    def submit_first() -> None:
        try:
            harness.coordinator.submit_result(_result())
        except Exception as error:
            failures.append(error)

    first = Thread(target=submit_first)
    first.start()
    assert harness.writer.submitted.wait(5.0)

    def submit_second() -> None:
        try:
            harness.coordinator.submit_result(
                _result(
                    partition_key=PART_1, work_item_id=WORK_B, lease_fence=8, lease_owner=OWNER_B
                )
            )
        except Exception as error:
            failures.append(error)

    second = Thread(target=submit_second)
    second.start()
    second.join(0.2)
    assert second.is_alive()
    assert harness.writer.commands == []
    assert harness.capacity.in_use(CAPACITY_CATEGORY_GLOBAL) == 2

    harness.writer.gate.set()
    first.join(5.0)
    second.join(5.0)
    assert not first.is_alive()
    assert not second.is_alive()
    assert failures == []
    assert [intent.work_item_id for intent in harness.writer.commands] == [WORK_A, WORK_B]
    assert harness.coordinator.committed_count == 2
    assert harness.capacity.in_use(CAPACITY_CATEGORY_GLOBAL) == 0


def test_capacity_held_through_rejection_and_rollback_and_released_only_on_commit() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    assert harness.capacity.in_use(CAPACITY_CATEGORY_GLOBAL) == 1
    with pytest.raises(ResultStaleRejection):
        harness.coordinator.submit_result(_result(lease_fence=99))
    assert harness.capacity.in_use(CAPACITY_CATEGORY_GLOBAL) == 1
    harness.writer.admission_errors.append(WriterError("rollback"))
    with pytest.raises(ResultWriterRetryableError):
        harness.coordinator.submit_result(_result())
    assert harness.capacity.in_use(CAPACITY_CATEGORY_GLOBAL) == 1
    harness.expect(assignment)
    harness.coordinator.submit_result(_result())
    assert harness.capacity.in_use(CAPACITY_CATEGORY_GLOBAL) == 0


# ---------------------------------------------------------------------------
# receive_next and close
# ---------------------------------------------------------------------------


def test_receive_next_on_empty_channel_is_a_noop() -> None:
    harness = _Harness()
    assert harness.coordinator.receive_next(timeout=0) is None
    assert harness.coordinator.committed_count == 0


def test_receive_next_pulls_and_submits_one_envelope() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    runner = _RunnerDouble(harness.channel)
    runner.deliver(_result())
    harness.coordinator.receive_next(timeout=0)
    assert harness.coordinator.committed_count == 1
    assert harness.channel.queued == 0


def test_receive_next_after_coordinator_close_on_drained_channel_raises() -> None:
    harness = _Harness()
    harness.coordinator.close()
    harness.channel.close()
    with pytest.raises(ResultCoordinatorClosedError):
        harness.coordinator.receive_next(timeout=0)


def test_receive_next_after_foreign_close_is_a_noop() -> None:
    harness = _Harness()
    harness.channel.close()
    assert harness.coordinator.receive_next(timeout=0) is None


def test_receive_next_without_timeout_returns_queued_message() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.channel.send(_result())
    harness.coordinator.receive_next()
    assert harness.coordinator.committed_count == 1


def test_receive_next_accepts_explicit_none_timeout_for_blocking_receive() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.channel.send(_result())
    harness.coordinator.receive_next(timeout=None)
    assert harness.coordinator.committed_count == 1


def test_receive_next_rejects_non_envelope_messages() -> None:
    harness = _Harness()
    harness.channel.send("not-an-envelope")
    with pytest.raises(ResultValidationRejection):
        harness.coordinator.receive_next(timeout=0)


@pytest.mark.parametrize(
    ("value", "error"),
    [
        ("0.5", TypeError),
        (-0.5, ResultValidationRejection),
        (float("inf"), ResultValidationRejection),
    ],
)
def test_receive_next_rejects_invalid_timeouts(
    value: object,
    error: type[Exception],
) -> None:
    harness = _Harness()
    with pytest.raises(error):
        harness.coordinator.receive_next(timeout=cast(float, value))


def test_close_is_idempotent_and_does_not_close_the_channel() -> None:
    harness = _Harness()
    harness.coordinator.close()
    harness.coordinator.close()
    assert harness.channel.is_closed is False
    harness.channel.send(_result())
    assert harness.channel.queued == 1


def test_submit_after_close_is_refused() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.coordinator.close()
    with pytest.raises(ResultCoordinatorClosedError):
        harness.coordinator.submit_result(_result())
    assert harness.writer.commands == []


def test_receive_next_after_close_drains_then_refuses_submission() -> None:
    harness = _Harness()
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.channel.send(_result())
    harness.coordinator.close()
    with pytest.raises(ResultCoordinatorClosedError):
        harness.coordinator.receive_next(timeout=0)
    assert harness.writer.commands == []


# ---------------------------------------------------------------------------
# Telemetry
# ---------------------------------------------------------------------------


def test_telemetry_sink_receives_one_service_duration_record_per_commit() -> None:
    harness = _Harness(node_ids=(NODE_A,), partitions={NODE_A: (PART_0, PART_1)})
    first = _assignment(partition_key=PART_0, lease_owner=OWNER_A)
    second = _assignment(
        partition_key=PART_1,
        work_item_id=WORK_B,
        lease_fence=8,
        lease_owner=OWNER_B,
    )
    harness.admit(first)
    harness.admit(second)
    harness.expect(first)
    harness.expect(second)
    harness.coordinator.submit_result(_result())
    harness.coordinator.submit_result(
        _result(partition_key=PART_1, work_item_id=WORK_B, lease_fence=8, lease_owner=OWNER_B)
    )
    assert len(harness.telemetry) == 2
    for record in harness.telemetry:
        assert record.run_id == RUN_ID
        (metric,) = record.metrics
        assert metric.kind is TelemetryMetricKind.SERVICE_DURATION
        assert metric.name == "result_commit_service_duration"
    assert [record.observed_at_micros for record in harness.telemetry] == [1, 2]


def test_no_telemetry_is_emitted_without_a_sink() -> None:
    harness = _Harness(telemetry_sink=None)
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.coordinator.submit_result(_result())
    assert harness.coordinator.committed_count == 1
    assert harness.telemetry == []


def test_failing_telemetry_sink_is_swallowed() -> None:
    harness = _Harness(telemetry_sink=_raising_sink)
    assignment = _assignment()
    harness.admit(assignment)
    harness.expect(assignment)
    harness.coordinator.submit_result(_result())
    assert harness.coordinator.committed_count == 1
    assert harness.work_states()[assignment.identity] is FrontierWorkState.SUCCEEDED
    assert harness.capacity.in_use(CAPACITY_CATEGORY_GLOBAL) == 0


# ---------------------------------------------------------------------------
# Zero direct database driver proof and runner isolation
# ---------------------------------------------------------------------------


def test_sources_never_mention_direct_database_drivers() -> None:
    module_path = Path(str(result_coordinator.__file__))
    test_path = Path(__file__)
    module_source = module_path.read_text(encoding="utf-8")
    test_source = test_path.read_text(encoding="utf-8")
    for marker in _FORBIDDEN_DRIVER_MARKERS:
        assert marker not in module_source
        assert marker not in test_source


def test_runner_double_only_ever_receives_the_result_channel() -> None:
    harness = _Harness()
    runner = _RunnerDouble(harness.channel)
    parameters = list(inspect.signature(_RunnerDouble.__init__).parameters)
    assert parameters == ["self", "channel"]
    for forbidden in (
        "writer",
        "reader",
        "scheduler",
        "capacity",
        "_writer",
        "_reader",
        "_scheduler",
        "_capacity",
    ):
        assert not hasattr(runner, forbidden)
    runner.deliver(_result())
    assert harness.channel.queued == 1
