"""Contract and adversarial tests for transactional work leasing."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from collections.abc import Callable
from dataclasses import FrozenInstanceError, dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from threading import Event, Thread
from traceback import format_exception
from typing import Any, cast

import pytest

import paritygrid.application.execution.leasing as leasing_module
from paritygrid.application.execution import (
    MAX_LEASE_CONTENTION_ATTEMPTS,
    MAX_LEASE_OWNER_LENGTH,
    MAX_LEASE_ROW_VERSION,
    MAX_LEASE_WRITER_TIMEOUT_SECONDS,
    MAX_RUNNER_KIND_LENGTH,
    MAX_WORK_LEASE_MICROSECONDS,
    MAX_WORKER_IDENTITY_LENGTH,
    MIN_WORK_LEASE_MICROSECONDS,
    AcquireWorkLeaseRequest,
    RenewWorkLeaseRequest,
    WorkLease,
    WorkLeaseAdmissionError,
    WorkLeaseBusyError,
    WorkLeaseClockError,
    WorkLeaseExpiredError,
    WorkLeaseInvalidRequestError,
    WorkLeaseOutcomeUnknownError,
    WorkLeaseOwnershipError,
    WorkLeaseProtocolError,
    WorkLeaseService,
    WorkLeaseServiceSnapshot,
    WorkLeaseSettings,
    WorkLeaseWriterError,
)
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    ConsistencyStaleRowVersionError,
    EventSequence,
    EventSubjectKind,
    ExecutionEventBatch,
    ExecutionEventRecord,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.execution import (
    ExecutionStaleRowVersionError,
    RunNodeRecord,
    RunNodeStatus,
    RunRecord,
    WorkClaim,
)
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    WriterAdmissionTimeoutError,
    WriterClosedError,
    WriterCommand,
    WriterCommandKind,
    WriterCommitOutcomeUnknownError,
    WriterDefinitelyNotExecutedError,
    WriterReceipt,
    WriterResultTimeoutError,
    WriterSubmissionId,
)
from paritygrid.application.writes import (
    WORK_LEASE_EVENT_PAYLOAD_SCHEMA_VERSION,
    ClaimWork,
    ClaimWorkResult,
    RenewWorkClaim,
    RenewWorkClaimResult,
)
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    AttemptNumber,
    Duration,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)

RUN_ID = RunId("run_work-leasing")
NODE_ID = NodeId("nod_work-leasing")
WORK_ID = WorkItemId("wrk_work-leasing")
PIPELINE_ID = PipelineId("pip_work-leasing")
_BASE = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)


class _Fatal(BaseException):
    pass


def _timestamp(second: int) -> UtcTimestamp:
    return UtcTimestamp(_BASE + timedelta(seconds=second))


def _document(**values: object) -> ConfigurationDocument:
    return ConfigurationDocument.from_mapping(values)


def _event(sequence: int, kind: str, second: int) -> EventAppendRequest:
    pending = PendingExecutionEvent(
        event_kind=kind,
        occurred_at=_timestamp(second),
        subject_kind=EventSubjectKind.WORK_ITEM,
        subject_id=WORK_ID,
        correlation_id="corr-work-leasing",
        payload_schema_version=1,
        payload=RedactedDocument.from_mapping({"kind": kind}),
    )
    return EventAppendRequest(EventSequence(sequence), sequence, pending)


def _event_batch(request: EventAppendRequest, *, run_id: RunId = RUN_ID) -> ExecutionEventBatch:
    pending = request.event
    record = ExecutionEventRecord(
        run_id=run_id,
        sequence=request.expected_next_sequence,
        event_kind=pending.event_kind,
        occurred_at=pending.occurred_at,
        subject_kind=pending.subject_kind,
        subject_id=pending.subject_id,
        correlation_id=pending.correlation_id,
        payload_schema_version=pending.payload_schema_version,
        payload=pending.payload,
    )
    return ExecutionEventBatch(
        (record,),
        request.expected_next_sequence.advance(1),
        request.expected_counter_row_version + 1,
    )


def _run(row_version: int) -> RunRecord:
    return RunRecord(
        run_id=RUN_ID,
        pipeline_id=PIPELINE_ID,
        pipeline_version=PipelineVersion(1),
        runner_kind="sequential",
        runner_configuration=_document(max_concurrency=1),
        state=RunState.RUNNING,
        row_version=row_version,
        scenario_seed=None,
        created_at=_timestamp(0),
        started_at=_timestamp(1),
        finished_at=None,
        cancellation_requested_at=None,
        recovery_started_at=None,
        recovered_at=None,
        final_reconciliation_fingerprint=None,
    )


def _node(row_version: int) -> RunNodeRecord:
    return RunNodeRecord(
        run_id=RUN_ID,
        node_id=NODE_ID,
        status=RunNodeStatus.RUNNING,
        row_version=row_version,
        work_total=1,
        work_pending=0,
        work_running=1,
        work_succeeded=0,
        work_quarantined=0,
        work_failed=0,
        work_cancelled=0,
        records_read=0,
        records_written=0,
        records_quarantined=0,
        bytes_read=0,
        bytes_written=0,
        retry_count=0,
        duration=Duration(0),
        started_at=_timestamp(3),
        finished_at=None,
    )


def _claim(command: ClaimWork) -> WorkClaim:
    return WorkClaim(
        work_item_id=command.work_item_id,
        attempt_number=AttemptNumber(1),
        lease_owner=command.lease_owner,
        row_version=command.expected_work_row_version + 1,
        started_at=command.started_at,
        lease_expires_at=command.lease_expires_at,
        runner_kind=command.runner_kind,
        worker_identity=command.worker_identity,
    )


def _claim_result(command: ClaimWork) -> ClaimWorkResult:
    return ClaimWorkResult(
        _claim(command),
        _node(command.expected_node_row_version + 1),
        _event_batch(command.event),
        _run(command.expected_run_row_version + 1),
    )


def _renewal_result(command: RenewWorkClaim) -> RenewWorkClaimResult:
    previous = command.claim
    claim = WorkClaim(
        work_item_id=previous.work_item_id,
        attempt_number=previous.attempt_number,
        lease_owner=previous.lease_owner,
        row_version=previous.row_version + 1,
        started_at=previous.started_at,
        lease_expires_at=command.lease_expires_at,
        runner_kind=previous.runner_kind,
        worker_identity=previous.worker_identity,
    )
    return RenewWorkClaimResult(
        claim,
        _event_batch(command.event),
        _run(command.expected_run_row_version + 1),
    )


def _valid_receipt(command: WriterCommand, submission_id: WriterSubmissionId) -> WriterReceipt:
    if type(command) is ClaimWork:
        result = _claim_result(command)
    elif type(command) is RenewWorkClaim:
        result = _renewal_result(command)
    else:  # pragma: no cover - the closed service never emits another command
        raise AssertionError("unexpected work lease command")
    return WriterReceipt(submission_id, command.kind, command.run_id, 0, True, result)


class _Ticket:
    def __init__(self, submission_id: object, result: object) -> None:
        self._submission_id = submission_id
        self._result = result
        self.result_timeouts: list[float] = []

    @property
    def submission_id(self) -> WriterSubmissionId:
        if isinstance(self._submission_id, BaseException):
            raise self._submission_id
        return cast(WriterSubmissionId, self._submission_id)

    def result(self, *, timeout_seconds: float) -> WriterReceipt:
        self.result_timeouts.append(timeout_seconds)
        if isinstance(self._result, BaseException):
            raise self._result
        return cast(WriterReceipt, self._result)

    async def result_async(self, *, timeout_seconds: float) -> WriterReceipt:
        return self.result(timeout_seconds=timeout_seconds)


_TicketFactory = Callable[[WriterCommand, WriterSubmissionId], _Ticket]


class _Writer:
    def __init__(self, factory: _TicketFactory | BaseException | None = None) -> None:
        self.factory = factory
        self.commands: list[WriterCommand] = []
        self.admission_timeouts: list[float] = []
        self.tickets: list[_Ticket] = []

    def submit(self, command: WriterCommand, *, timeout_seconds: float) -> _Ticket:
        self.commands.append(command)
        self.admission_timeouts.append(timeout_seconds)
        if isinstance(self.factory, BaseException):
            raise self.factory
        submission_id = WriterSubmissionId(len(self.commands))
        if self.factory is None:
            ticket = _Ticket(submission_id, _valid_receipt(command, submission_id))
        else:
            ticket = self.factory(command, submission_id)
        self.tickets.append(ticket)
        return ticket


class _Clock:
    def __init__(self, *values: object) -> None:
        self.values = list(values or (_timestamp(3),))

    def now(self) -> UtcTimestamp:
        value = self.values.pop(0)
        if isinstance(value, BaseException):
            raise value
        return cast(UtcTimestamp, value)


def _acquire_request() -> AcquireWorkLeaseRequest:
    return AcquireWorkLeaseRequest(
        run_id=RUN_ID,
        node_id=NODE_ID,
        work_item_id=WORK_ID,
        expected_attempt_number=AttemptNumber(1),
        expected_work_row_version=1,
        expected_node_row_version=2,
        expected_run_row_version=3,
        lease_owner="scheduler-secret-owner",
        runner_kind="sequential",
        worker_identity="machine-secret-worker",
        event=_event(4, "work_claimed", 3),
    )


def _renew_request() -> RenewWorkLeaseRequest:
    return RenewWorkLeaseRequest(4, _event(5, "work_claim_renewed", 4))


def _service(
    writer: _Writer | None = None,
    clock: _Clock | None = None,
    settings: WorkLeaseSettings | None = None,
) -> tuple[WorkLeaseService, _Writer]:
    selected_writer = writer or _Writer()
    selected_clock = clock or _Clock(_timestamp(3), _timestamp(4))
    return WorkLeaseService(selected_writer, selected_clock, settings=settings), selected_writer


def test_settings_requests_snapshots_and_reprs_are_exact_bounded_and_redacted() -> None:
    assert WorkLeaseSettings() == WorkLeaseSettings(Duration(60_000_000), 5.0, 60.0)
    assert (
        WorkLeaseSettings(
            Duration(MIN_WORK_LEASE_MICROSECONDS),
            0.0,
            MAX_LEASE_WRITER_TIMEOUT_SECONDS,
        ).lease_duration.microseconds
        == MIN_WORK_LEASE_MICROSECONDS
    )
    assert WorkLeaseSettings(Duration(MAX_WORK_LEASE_MICROSECONDS)).lease_duration.microseconds == (
        MAX_WORK_LEASE_MICROSECONDS
    )
    request = _acquire_request()
    renewal = _renew_request()
    request_repr = repr(request)
    assert "scheduler-secret-owner" not in request_repr
    assert "machine-secret-worker" not in request_repr
    assert request_repr.count("<redacted>") == 4
    assert repr(renewal).endswith("event=<redacted>)")
    snapshot = WorkLeaseServiceSnapshot(1, 2, 3)
    assert snapshot == WorkLeaseServiceSnapshot(active=1, unknown=2, in_flight=3)
    with pytest.raises(AttributeError):
        request.lease_owner = "changed"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("settings", "error", "message"),
    [
        (lambda: WorkLeaseSettings(cast(Any, 1)), TypeError, "Duration"),
        (
            lambda: WorkLeaseSettings(Duration(MIN_WORK_LEASE_MICROSECONDS - 1)),
            WorkLeaseInvalidRequestError,
            "supported range",
        ),
        (
            lambda: WorkLeaseSettings(Duration(MAX_WORK_LEASE_MICROSECONDS + 1)),
            WorkLeaseInvalidRequestError,
            "supported range",
        ),
        (lambda: WorkLeaseSettings(admission_timeout_seconds=cast(Any, 1)), TypeError, "float"),
        (
            lambda: WorkLeaseSettings(admission_timeout_seconds=-0.1),
            WorkLeaseInvalidRequestError,
            "supported range",
        ),
        (
            lambda: WorkLeaseSettings(
                result_timeout_seconds=MAX_LEASE_WRITER_TIMEOUT_SECONDS + 0.1
            ),
            WorkLeaseInvalidRequestError,
            "supported range",
        ),
    ],
)
def test_settings_reject_invalid_values(
    settings: Callable[[], object], error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message):
        settings()


@pytest.mark.parametrize(
    ("field", "value", "error", "message"),
    [
        ("run_id", "run", TypeError, "RunId"),
        ("node_id", "node", TypeError, "NodeId"),
        ("work_item_id", "work", TypeError, "WorkItemId"),
        ("expected_attempt_number", 1, TypeError, "AttemptNumber"),
        ("expected_work_row_version", True, TypeError, "integer"),
        ("expected_node_row_version", 0, WorkLeaseInvalidRequestError, "supported range"),
        (
            "expected_run_row_version",
            MAX_LEASE_ROW_VERSION,
            WorkLeaseInvalidRequestError,
            "supported range",
        ),
        ("lease_owner", 1, TypeError, "text"),
        ("lease_owner", "", WorkLeaseInvalidRequestError, "supported range"),
        ("runner_kind", "r" * (MAX_RUNNER_KIND_LENGTH + 1), WorkLeaseInvalidRequestError, "range"),
        (
            "worker_identity",
            "e\N{COMBINING ACUTE ACCENT}",
            WorkLeaseInvalidRequestError,
            "normalized",
        ),
        ("event", object(), TypeError, "EventAppendRequest"),
    ],
)
def test_acquisition_request_rejects_invalid_values(
    field: str,
    value: object,
    error: type[Exception],
    message: str,
) -> None:
    values: dict[str, object] = {
        "run_id": RUN_ID,
        "node_id": NODE_ID,
        "work_item_id": WORK_ID,
        "expected_attempt_number": AttemptNumber(1),
        "expected_work_row_version": 1,
        "expected_node_row_version": 2,
        "expected_run_row_version": 3,
        "lease_owner": "owner",
        "runner_kind": "sequential",
        "worker_identity": "worker",
        "event": _event(4, "work_claimed", 3),
    }
    values[field] = value
    with pytest.raises(error, match=message):
        AcquireWorkLeaseRequest(**cast(Any, values))


def test_request_text_upper_bounds_and_renewal_validation() -> None:
    request = replace(
        _acquire_request(),
        lease_owner="o" * MAX_LEASE_OWNER_LENGTH,
        runner_kind="r" * MAX_RUNNER_KIND_LENGTH,
        worker_identity="w" * MAX_WORKER_IDENTITY_LENGTH,
    )
    assert len(request.lease_owner) == MAX_LEASE_OWNER_LENGTH
    with pytest.raises(TypeError, match="integer"):
        RenewWorkLeaseRequest(cast(Any, True), _event(5, "work_claim_renewed", 4))
    with pytest.raises(TypeError, match="EventAppendRequest"):
        RenewWorkLeaseRequest(4, cast(Any, object()))


@pytest.mark.parametrize(
    "event",
    [
        replace(
            _event(4, "work_claimed", 3),
            expected_next_sequence=cast(Any, object()),
        ),
        replace(
            _event(4, "work_claimed", 3),
            expected_next_sequence=EventSequence(MAX_LEASE_ROW_VERSION),
        ),
        replace(
            _event(4, "work_claimed", 3),
            expected_counter_row_version=MAX_LEASE_ROW_VERSION,
        ),
    ],
)
def test_requests_reject_non_incrementable_event_frontiers(event: EventAppendRequest) -> None:
    error = (
        TypeError
        if type(event.expected_next_sequence) is not EventSequence
        else WorkLeaseInvalidRequestError
    )
    with pytest.raises(error):
        replace(_acquire_request(), event=event)


@pytest.mark.parametrize("values", [(True, 0, 0), (0, -1, 0), (0, 0, -1)])
def test_snapshot_rejects_non_integer_or_negative_counters(
    values: tuple[object, object, object],
) -> None:
    error = TypeError if any(type(value) is not int for value in values) else ValueError
    with pytest.raises(error):
        WorkLeaseServiceSnapshot(*cast(Any, values))


def test_service_requires_structural_writer_clock_and_exact_settings() -> None:
    with pytest.raises(TypeError, match="writer"):
        WorkLeaseService(cast(Any, object()), _Clock())
    with pytest.raises(TypeError, match="clock"):
        WorkLeaseService(_Writer(), cast(Any, object()))
    with pytest.raises(TypeError, match="WorkLeaseSettings"):
        WorkLeaseService(_Writer(), _Clock(), settings=cast(Any, object()))
    service = WorkLeaseService(_Writer(), _Clock())
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 0, 0)


def test_acquire_renew_retire_preserve_exact_writer_capability_and_bounds() -> None:
    settings = WorkLeaseSettings(Duration(5_000_000), 0.25, 0.75)
    service, writer = _service(settings=settings)
    request = _acquire_request()
    request = replace(
        request,
        event=replace(
            request.event,
            event=PendingExecutionEvent(
                event_kind="caller_untrusted",
                occurred_at=_timestamp(0),
                subject_kind=EventSubjectKind.RUN,
                subject_id=RUN_ID,
                correlation_id="corr-work-leasing",
                payload_schema_version=99,
                payload=RedactedDocument.from_mapping(
                    {"message": "credential-canary-must-not-persist"}
                ),
            ),
        ),
    )
    lease = service.acquire(request)
    assert service.snapshot() == WorkLeaseServiceSnapshot(1, 0, 0)
    assert (
        lease.claim
        is cast(ClaimWorkResult, cast(WriterReceipt, writer.tickets[0]._result).result).claim
    )
    assert lease.node.row_version == 3
    assert lease.run.row_version == 4
    assert lease.events.counter_row_version == 5
    assert lease.submission_id == WriterSubmissionId(1)
    assert "scheduler-secret-owner" not in repr(lease)
    assert "machine-secret-worker" not in repr(lease)
    command = cast(ClaimWork, writer.commands[0])
    assert command.started_at == _timestamp(3)
    assert command.lease_expires_at == _timestamp(8)
    assert command.expected_attempt_number == AttemptNumber(1)
    assert command.event.event.event_kind == "work_claimed"
    assert command.event.event.occurred_at == command.started_at
    assert command.event.event.payload_schema_version == WORK_LEASE_EVENT_PAYLOAD_SCHEMA_VERSION
    assert command.event.event.payload.to_mapping() == {
        "attempt_number": 1,
        "lease_expires_at": str(command.lease_expires_at),
        "node_id": str(NODE_ID),
        "runner_kind": "sequential",
    }
    assert "scheduler-secret-owner" not in repr(command.event.event)
    assert "machine-secret-worker" not in repr(command.event.event)
    assert "credential-canary-must-not-persist" not in repr(command.event.event)
    assert "credential-canary-must-not-persist" not in str(command.event.event.payload.to_mapping())
    assert writer.admission_timeouts == [0.25]
    assert writer.tickets[0].result_timeouts == [0.75]
    renewed = service.renew(lease, _renew_request())
    assert (
        renewed.claim
        is cast(RenewWorkClaimResult, cast(WriterReceipt, writer.tickets[1]._result).result).claim
    )
    assert renewed.node is lease.node
    assert renewed.run.row_version == 5
    assert renewed.claim.row_version == 3
    assert renewed.claim.lease_expires_at == _timestamp(9)
    renewal = cast(RenewWorkClaim, writer.commands[1])
    assert renewal.event.event.event_kind == "work_claim_renewed"
    assert renewal.event.event.occurred_at == renewal.renewed_at
    assert renewal.event.event.payload_schema_version == WORK_LEASE_EVENT_PAYLOAD_SCHEMA_VERSION
    assert renewal.event.event.payload.to_mapping() == {
        "attempt_number": 1,
        "lease_expires_at": str(renewal.lease_expires_at),
        "node_id": str(NODE_ID),
        "runner_kind": "sequential",
    }
    with pytest.raises(WorkLeaseOwnershipError, match="active"):
        service.renew(lease, _renew_request())
    with pytest.raises(WorkLeaseOwnershipError, match="active"):
        service.retire(lease)
    service.retire(renewed)
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 0, 0)
    with pytest.raises(WorkLeaseOwnershipError, match="active"):
        service.retire(renewed)
    assert "writer=<redacted>" in repr(service)


def test_work_lease_constructor_is_service_only_and_validates_exact_inputs() -> None:
    command = ClaimWork(
        RUN_ID,
        NODE_ID,
        WORK_ID,
        AttemptNumber(1),
        1,
        2,
        3,
        "owner",
        _timestamp(3),
        _timestamp(8),
        "sequential",
        "worker",
        _event(4, "work_claimed", 3),
    )
    result = _claim_result(command)
    with pytest.raises(WorkLeaseOwnershipError, match="service-issued"):
        WorkLease(
            result.claim,
            result.node,
            result.run,
            result.events,
            WriterSubmissionId(1),
            _token=object(),
        )
    with pytest.raises(TypeError, match="WorkClaim"):
        WorkLease(
            cast(Any, object()),
            result.node,
            result.run,
            result.events,
            WriterSubmissionId(1),
            _token=leasing_module._LEASE_CONSTRUCTION_TOKEN,
        )


def test_issued_lease_is_frozen_and_internal_identity_defeats_reflective_reconstruction() -> None:
    service, _ = _service()
    lease = service.acquire(_acquire_request())
    original = lease.claim
    reconstructed = replace(original)
    assert reconstructed == original
    assert reconstructed is not original
    with pytest.raises(FrozenInstanceError):
        lease._claim = reconstructed  # type: ignore[misc]
    object.__setattr__(lease, "_claim", reconstructed)
    with pytest.raises(WorkLeaseOwnershipError, match="active"):
        service.renew(lease, _renew_request())


def test_in_place_reflective_claim_mutation_cannot_reuse_issued_authority() -> None:
    service, writer = _service()
    lease = service.acquire(_acquire_request())
    issued = lease.claim
    object.__setattr__(issued, "lease_owner", "forged-owner")
    with pytest.raises(WorkLeaseOwnershipError, match="active"):
        service.renew(lease, _renew_request())
    assert len(writer.commands) == 1
    assert service.snapshot() == WorkLeaseServiceSnapshot(1, 0, 0)


def test_renewal_revalidates_capability_after_external_clock_boundary() -> None:
    other_work_id = WorkItemId("wrk_work-leasing-other")
    service, writer = _service(clock=_Clock(_timestamp(3), _timestamp(3)))
    first = service.acquire(_acquire_request())
    second = service.acquire(
        replace(
            _acquire_request(),
            work_item_id=other_work_id,
            lease_owner="other-owner",
            worker_identity="other-worker",
        )
    )

    class MutatingClock:
        def now(self) -> UtcTimestamp:
            object.__setattr__(first.claim, "work_item_id", other_work_id)
            return _timestamp(4)

    object.__setattr__(service, "_clock", MutatingClock())
    with pytest.raises(WorkLeaseOwnershipError, match="changed"):
        service.renew(first, _renew_request())
    assert len(writer.commands) == 2
    assert service.snapshot() == WorkLeaseServiceSnapshot(2, 0, 0)

    object.__setattr__(service, "_clock", _Clock(_timestamp(4)))
    renewed = service.renew(second, _renew_request())
    assert renewed.claim.work_item_id == other_work_id
    assert len(writer.commands) == 3


def test_invalid_reflective_claim_shape_cannot_be_recaptured_or_renewed() -> None:
    service, writer = _service()
    lease = service.acquire(_acquire_request())
    object.__setattr__(lease.claim, "lease_owner", object())
    with pytest.raises(WorkLeaseProtocolError, match="evidence"):
        leasing_module._ActiveWorkLease.capture(lease)
    with pytest.raises(WorkLeaseOwnershipError, match="active"):
        service.renew(lease, _renew_request())
    assert len(writer.commands) == 1


def test_invalid_reflective_timestamp_shape_cannot_reuse_issued_authority() -> None:
    service, writer = _service()
    lease = service.acquire(_acquire_request())
    object.__setattr__(lease.claim.started_at, "value", object())
    with pytest.raises(WorkLeaseOwnershipError, match="active"):
        service.renew(lease, _renew_request())
    assert len(writer.commands) == 1


def test_non_utc_nested_claim_timestamp_cannot_reuse_issued_authority() -> None:
    service, writer = _service()
    lease = service.acquire(_acquire_request())
    object.__setattr__(
        lease.claim.started_at,
        "value",
        datetime(2026, 8, 12, 12, 0, 3),
    )
    with pytest.raises(WorkLeaseOwnershipError, match="active"):
        service.renew(lease, _renew_request())
    assert len(writer.commands) == 1


def test_non_utc_nested_node_timestamp_cannot_reuse_issued_authority() -> None:
    service, writer = _service()
    lease = service.acquire(_acquire_request())
    assert lease.node.started_at is not None
    object.__setattr__(
        lease.node.started_at,
        "value",
        datetime(2026, 8, 12, 12, 0, 3),
    )
    with pytest.raises(WorkLeaseOwnershipError, match="active"):
        service.renew(lease, _renew_request())
    assert len(writer.commands) == 1


def test_arbitrary_nested_dataclass_cannot_execute_during_authority_check() -> None:
    calls: list[str] = []

    @dataclass
    class EvilDataclass:
        value: int

        def __getattribute__(self, name: str) -> object:
            if name == "value":
                calls.append("attribute")
            return object.__getattribute__(self, name)

    service, writer = _service()
    lease = service.acquire(_acquire_request())
    object.__setattr__(lease.node, "duration", EvilDataclass(1))
    with pytest.raises(WorkLeaseOwnershipError, match="active"):
        service.renew(lease, _renew_request())
    assert calls == []
    assert len(writer.commands) == 1


def test_arbitrary_nested_enum_cannot_execute_during_authority_check() -> None:
    calls: list[str] = []

    class EvilEnum(Enum):
        VALUE = "value"

        @property
        def microseconds(self) -> int:
            calls.append("property")
            return 0

    service, writer = _service()
    lease = service.acquire(_acquire_request())
    object.__setattr__(lease.node, "duration", EvilEnum.VALUE)
    with pytest.raises(WorkLeaseOwnershipError, match="active"):
        service.renew(lease, _renew_request())
    assert calls == []
    assert len(writer.commands) == 1


def test_acquire_and_renew_require_exact_public_contracts() -> None:
    service, _ = _service()
    with pytest.raises(TypeError, match="AcquireWorkLeaseRequest"):
        service.acquire(cast(Any, object()))
    lease = service.acquire(_acquire_request())
    with pytest.raises(TypeError, match="WorkLease"):
        service.renew(cast(Any, object()), _renew_request())
    with pytest.raises(TypeError, match="RenewWorkLeaseRequest"):
        service.renew(lease, cast(Any, object()))
    with pytest.raises(TypeError, match="WorkLease"):
        service.retire(cast(Any, object()))


def test_acquisition_requires_the_exact_expected_attempt_number() -> None:
    service, _ = _service()
    request = replace(_acquire_request(), expected_attempt_number=AttemptNumber(2))
    with pytest.raises(WorkLeaseProtocolError, match="claim result"):
        service.acquire(request)
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 1, 0)


def test_active_and_overlapping_operations_fail_closed_per_work_identity() -> None:
    entered = Event()
    release = Event()

    def blocking_factory(command: WriterCommand, identity: WriterSubmissionId) -> _Ticket:
        receipt = _valid_receipt(command, identity)

        class _BlockingTicket(_Ticket):
            def result(self, *, timeout_seconds: float) -> WriterReceipt:
                entered.set()
                assert release.wait(5)
                return super().result(timeout_seconds=timeout_seconds)

        return _BlockingTicket(identity, receipt)

    service, _ = _service(_Writer(blocking_factory))
    result: list[WorkLease] = []
    thread = Thread(target=lambda: result.append(service.acquire(_acquire_request())))
    thread.start()
    assert entered.wait(5)
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 0, 1)
    with pytest.raises(WorkLeaseBusyError, match="lease state"):
        service.acquire(_acquire_request())
    release.set()
    thread.join(5)
    assert not thread.is_alive()
    lease = result[0]
    with pytest.raises(WorkLeaseBusyError, match="lease state"):
        service.acquire(_acquire_request())

    entered.clear()
    release.clear()
    renew_result: list[WorkLease] = []
    renew_thread = Thread(
        target=lambda: renew_result.append(service.renew(lease, _renew_request()))
    )
    renew_thread.start()
    assert entered.wait(5)
    assert service.snapshot() == WorkLeaseServiceSnapshot(1, 0, 1)
    with pytest.raises(WorkLeaseBusyError, match="lease operation"):
        service.renew(lease, _renew_request())
    with pytest.raises(WorkLeaseOwnershipError, match="active"):
        service.retire(lease)
    release.set()
    renew_thread.join(5)
    assert not renew_thread.is_alive()
    assert service.snapshot() == WorkLeaseServiceSnapshot(1, 0, 0)


def test_foreign_and_expired_renewals_never_reach_the_writer() -> None:
    first, _ = _service(clock=_Clock(_timestamp(3)))
    lease = first.acquire(_acquire_request())
    second_writer = _Writer()
    second = WorkLeaseService(second_writer, _Clock(_timestamp(4)))
    with pytest.raises(WorkLeaseOwnershipError, match="active"):
        second.renew(lease, _renew_request())
    assert second_writer.commands == []

    expired_writer = _Writer()
    expired = WorkLeaseService(
        expired_writer,
        _Clock(_timestamp(3), _timestamp(4)),
        settings=WorkLeaseSettings(Duration(1_000_000)),
    )
    expired_lease = expired.acquire(_acquire_request())
    with pytest.raises(WorkLeaseExpiredError, match="expired"):
        expired.renew(expired_lease, _renew_request())
    assert len(expired_writer.commands) == 1
    assert expired.snapshot() == WorkLeaseServiceSnapshot(1, 0, 0)


def test_non_incrementable_claim_cannot_be_renewed() -> None:
    def factory(command: WriterCommand, identity: WriterSubmissionId) -> _Ticket:
        receipt = _valid_receipt(command, identity)
        result = cast(ClaimWorkResult, receipt.result)
        claim = replace(result.claim, row_version=MAX_LEASE_ROW_VERSION)
        node = replace(result.node, row_version=MAX_LEASE_ROW_VERSION)
        run = replace(result.run, row_version=MAX_LEASE_ROW_VERSION)
        return _Ticket(
            identity, replace(receipt, result=replace(result, claim=claim, node=node, run=run))
        )

    service, writer = _service(
        _Writer(factory),
        _Clock(_timestamp(3), _timestamp(4), _timestamp(5)),
    )
    request = replace(
        _acquire_request(),
        expected_work_row_version=MAX_LEASE_ROW_VERSION - 1,
        expected_node_row_version=MAX_LEASE_ROW_VERSION - 1,
        expected_run_row_version=MAX_LEASE_ROW_VERSION - 1,
    )
    lease = service.acquire(request)
    with pytest.raises(WorkLeaseInvalidRequestError, match="cannot advance"):
        service.renew(lease, _renew_request())
    assert len(writer.commands) == 1
    assert service.snapshot() == WorkLeaseServiceSnapshot(1, 0, 0)


def test_safe_renewal_failure_preserves_the_previous_active_capability() -> None:
    call = 0

    def factory(command: WriterCommand, identity: WriterSubmissionId) -> _Ticket:
        nonlocal call
        call += 1
        if call == 2:
            return _Ticket(identity, WriterDefinitelyNotExecutedError("safe"))
        return _Ticket(identity, _valid_receipt(command, identity))

    service, writer = _service(
        _Writer(factory),
        _Clock(_timestamp(3), _timestamp(4), _timestamp(5)),
    )
    lease = service.acquire(_acquire_request())
    with pytest.raises(WorkLeaseWriterError, match="not committed"):
        service.renew(lease, _renew_request())
    assert service.snapshot() == WorkLeaseServiceSnapshot(1, 0, 0)
    assert service.renew(lease, _renew_request()).claim.row_version == 3
    assert len(writer.commands) == 3


@pytest.mark.parametrize("clock_value", [RuntimeError("clock /secret"), object()])
def test_clock_failures_are_typed_redacted_and_retryable(clock_value: object) -> None:
    service, writer = _service(clock=_Clock(clock_value, _timestamp(3)))
    with pytest.raises(WorkLeaseClockError, match="clock failed") as captured:
        service.acquire(_acquire_request())
    rendered = "".join(format_exception(captured.value))
    assert "/secret" not in rendered
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 0, 0)
    assert writer.commands == []
    assert service.acquire(_acquire_request()).claim.work_item_id == WORK_ID


def test_clock_base_exception_propagates_before_admission_without_poisoning() -> None:
    service, writer = _service(clock=_Clock(_Fatal("stop")))
    with pytest.raises(_Fatal, match="stop"):
        service.acquire(_acquire_request())
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 0, 0)
    assert writer.commands == []


def test_timestamp_overflow_is_typed_and_retryable() -> None:
    maximum = UtcTimestamp(datetime.max.replace(tzinfo=UTC))
    service, writer = _service(clock=_Clock(maximum))
    with pytest.raises(WorkLeaseClockError, match="expiry"):
        service.acquire(_acquire_request())
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 0, 0)
    assert writer.commands == []


@pytest.mark.parametrize(
    ("failure", "error"),
    [
        (WriterAdmissionTimeoutError("credential=secret"), WorkLeaseAdmissionError),
        (WriterClosedError("C:/private/writer.db"), WorkLeaseWriterError),
    ],
)
def test_pre_admission_writer_failures_are_typed_redacted_and_retryable(
    failure: BaseException, error: type[Exception]
) -> None:
    service, _ = _service(_Writer(failure), _Clock(_timestamp(3), _timestamp(3)))
    with pytest.raises(error) as captured:
        service.acquire(_acquire_request())
    rendered = "".join(format_exception(captured.value))
    assert "credential=secret" not in rendered
    assert "C:/private" not in rendered
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 0, 0)
    service._writer = _Writer()  # type: ignore[assignment]
    assert service.acquire(_acquire_request()).claim.work_item_id == WORK_ID


@pytest.mark.parametrize(
    ("failure", "error", "unknown"),
    [
        (WriterResultTimeoutError("secret"), WorkLeaseOutcomeUnknownError, True),
        (WriterCommitOutcomeUnknownError("secret"), WorkLeaseOutcomeUnknownError, True),
        (WriterDefinitelyNotExecutedError("secret"), WorkLeaseWriterError, False),
        (WriterClosedError("secret"), WorkLeaseWriterError, False),
        (ExecutionStaleRowVersionError("secret"), WorkLeaseWriterError, False),
        (ConsistencyStaleRowVersionError("secret"), WorkLeaseWriterError, False),
        (RuntimeError("secret"), WorkLeaseProtocolError, True),
    ],
)
def test_accepted_ticket_failures_distinguish_safe_from_ambiguous(
    failure: BaseException,
    error: type[Exception],
    unknown: bool,
) -> None:
    def factory(_command: WriterCommand, identity: WriterSubmissionId) -> _Ticket:
        return _Ticket(identity, failure)

    service, _ = _service(_Writer(factory), _Clock(_timestamp(3), _timestamp(3)))
    with pytest.raises(error) as captured:
        service.acquire(_acquire_request())
    assert "secret" not in "".join(format_exception(captured.value))
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    expected = WorkLeaseServiceSnapshot(0, 1 if unknown else 0, 0)
    assert service.snapshot() == expected
    if unknown:
        with pytest.raises(WorkLeaseBusyError):
            service.acquire(_acquire_request())
    else:
        service._writer = _Writer()  # type: ignore[assignment]
        assert service.acquire(_acquire_request()).claim.work_item_id == WORK_ID


@pytest.mark.parametrize("location", ["submit", "identity", "result"])
def test_base_exceptions_propagate_and_poison_after_possible_admission(location: str) -> None:
    fatal = _Fatal("fatal")
    if location == "submit":
        writer = _Writer(fatal)
    elif location == "identity":
        writer = _Writer(
            lambda command, identity: _Ticket(fatal, _valid_receipt(command, identity))
        )
    else:
        writer = _Writer(lambda _command, identity: _Ticket(identity, fatal))
    service, _ = _service(writer)
    with pytest.raises(_Fatal, match="fatal"):
        service.acquire(_acquire_request())
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 1, 0)


@pytest.mark.parametrize("location", ["submit", "identity"])
def test_unexpected_admission_or_identity_exceptions_are_protocol_unknown(location: str) -> None:
    if location == "submit":
        writer = _Writer(RuntimeError("machine path"))
    else:
        writer = _Writer(
            lambda command, identity: _Ticket(
                RuntimeError("machine path"), _valid_receipt(command, identity)
            )
        )
    service, _ = _service(writer)
    with pytest.raises(WorkLeaseProtocolError) as captured:
        service.acquire(_acquire_request())
    assert "machine path" not in "".join(format_exception(captured.value))
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 1, 0)


def test_none_ticket_is_protocol_unknown() -> None:
    class _NoneWriter:
        def submit(self, command: WriterCommand, *, timeout_seconds: float) -> Any:
            del command, timeout_seconds
            return None

    service = WorkLeaseService(_NoneWriter(), _Clock(_timestamp(3)))
    with pytest.raises(WorkLeaseProtocolError, match="outcome"):
        service.acquire(_acquire_request())
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 1, 0)


def test_interrupt_after_committed_acquisition_receipt_poisoned_before_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = leasing_module.WorkLeaseService._execute

    def commit_then_interrupt(
        service: WorkLeaseService,
        command: WriterCommand,
        work_item_id: WorkItemId,
    ) -> WriterReceipt:
        original(service, command, work_item_id)
        raise _Fatal("post-commit")

    monkeypatch.setattr(leasing_module.WorkLeaseService, "_execute", commit_then_interrupt)
    service, writer = _service()
    with pytest.raises(_Fatal, match="post-commit"):
        service.acquire(_acquire_request())
    assert len(writer.commands) == 1
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 1, 0)


def test_interrupt_after_committed_renewal_receipt_invalidates_previous_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, writer = _service()
    lease = service.acquire(_acquire_request())
    original = leasing_module.WorkLeaseService._execute

    def commit_then_interrupt(
        current: WorkLeaseService,
        command: WriterCommand,
        work_item_id: WorkItemId,
    ) -> WriterReceipt:
        original(current, command, work_item_id)
        raise _Fatal("post-commit")

    monkeypatch.setattr(leasing_module.WorkLeaseService, "_execute", commit_then_interrupt)
    with pytest.raises(_Fatal, match="post-commit"):
        service.renew(lease, _renew_request())
    assert len(writer.commands) == 2
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 1, 0)
    with pytest.raises(WorkLeaseOwnershipError):
        service.renew(lease, _renew_request())


@pytest.mark.parametrize("identity", [object(), "submission-1"])
def test_invalid_ticket_identity_is_protocol_unknown(identity: object) -> None:
    writer = _Writer(lambda command, valid: _Ticket(identity, _valid_receipt(command, valid)))
    service, _ = _service(writer)
    with pytest.raises(WorkLeaseProtocolError, match="ticket identity"):
        service.acquire(_acquire_request())
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 1, 0)


def _receipt_factory(
    transform: Callable[[WriterReceipt, WriterCommand], object],
) -> _TicketFactory:
    def factory(command: WriterCommand, identity: WriterSubmissionId) -> _Ticket:
        receipt = _valid_receipt(command, identity)
        return _Ticket(identity, transform(receipt, command))

    return factory


_ReceiptTransform = Callable[[WriterReceipt, WriterCommand], object]
_RECEIPT_CASES: list[tuple[_ReceiptTransform, str]] = [
    (lambda _receipt, _command: object(), "writer result"),
    (
        lambda receipt, _command: replace(
            receipt, submission_id=WriterSubmissionId(receipt.submission_id.number + 1)
        ),
        "identity",
    ),
    (
        lambda receipt, _command: replace(receipt, command_kind=WriterCommandKind.RENEW_WORK_CLAIM),
        "command",
    ),
    (lambda receipt, _command: replace(receipt, run_id=RunId("run_other")), "command"),
    (lambda receipt, _command: replace(receipt, contention_attempts=-1), "contention"),
    (
        lambda receipt, _command: replace(
            receipt, contention_attempts=MAX_LEASE_CONTENTION_ATTEMPTS + 1
        ),
        "contention",
    ),
    (lambda receipt, _command: replace(receipt, contention_attempts=True), "contention"),
    (lambda receipt, _command: replace(receipt, mutated=False), "mutation"),
    (lambda receipt, _command: replace(receipt, result=object()), "claim result type"),
]


@pytest.mark.parametrize(
    ("transform", "message"),
    _RECEIPT_CASES,
)
def test_malformed_writer_receipts_are_protocol_unknown(
    transform: Callable[[WriterReceipt, WriterCommand], object], message: str
) -> None:
    service, _ = _service(_Writer(_receipt_factory(transform)))
    with pytest.raises(WorkLeaseProtocolError, match=message):
        service.acquire(_acquire_request())
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 1, 0)


def _replace_claim_result(
    receipt: WriterReceipt,
    result_transform: Callable[[ClaimWorkResult, ClaimWork], ClaimWorkResult],
    command: WriterCommand,
) -> WriterReceipt:
    return replace(
        receipt,
        result=result_transform(cast(ClaimWorkResult, receipt.result), cast(ClaimWork, command)),
    )


_ClaimResultTransform = Callable[[ClaimWorkResult, ClaimWork], ClaimWorkResult]
_CLAIM_RESULT_CASES: list[tuple[_ClaimResultTransform, str]] = [
    (
        lambda result, _command: replace(result, claim=cast(Any, object())),
        "WorkClaim",
    ),
    (
        lambda result, _command: replace(
            result,
            claim=replace(result.claim, lease_owner="different"),
        ),
        "claim result",
    ),
    (
        lambda result, _command: replace(
            result,
            claim=replace(result.claim, row_version=MAX_LEASE_ROW_VERSION + 1),
        ),
        "claim row version",
    ),
    (
        lambda result, _command: replace(result, node=cast(Any, object())),
        "RunNodeRecord",
    ),
    (
        lambda result, _command: replace(
            result, node=replace(result.node, node_id=NodeId("nod_other"))
        ),
        "node result",
    ),
    (
        lambda result, _command: replace(
            result,
            node=replace(result.node, row_version=MAX_LEASE_ROW_VERSION + 1),
        ),
        "node row version",
    ),
    (
        lambda result, _command: replace(result, run=cast(Any, object())),
        "RunRecord",
    ),
    (
        lambda result, _command: replace(result, run=replace(result.run, row_version=99)),
        "run result",
    ),
    (
        lambda result, _command: replace(
            result,
            run=replace(result.run, row_version=MAX_LEASE_ROW_VERSION + 1),
        ),
        "run row version",
    ),
    (
        lambda result, _command: replace(result, events=cast(Any, object())),
        "ExecutionEventBatch",
    ),
    (
        lambda result, _command: replace(
            result, events=ExecutionEventBatch((), EventSequence(5), 5)
        ),
        "event result shape",
    ),
    (
        lambda result, _command: replace(
            result,
            events=replace(
                result.events,
                counter_row_version=MAX_LEASE_ROW_VERSION + 1,
            ),
        ),
        "event result shape",
    ),
    (
        lambda result, _command: replace(
            result,
            events=replace(result.events, counter_row_version=99),
        ),
        "event result does not match",
    ),
]


@pytest.mark.parametrize(
    ("result_transform", "message"),
    _CLAIM_RESULT_CASES,
)
def test_malformed_claim_evidence_is_protocol_unknown(
    result_transform: Callable[[ClaimWorkResult, ClaimWork], ClaimWorkResult],
    message: str,
) -> None:
    def transform(receipt: WriterReceipt, command: WriterCommand) -> WriterReceipt:
        return _replace_claim_result(receipt, result_transform, command)

    service, _ = _service(_Writer(_receipt_factory(transform)))
    with pytest.raises(WorkLeaseProtocolError, match=message):
        service.acquire(_acquire_request())
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 1, 0)


def test_malformed_event_item_and_content_are_protocol_unknown() -> None:
    def malformed_item(receipt: WriterReceipt, _command: WriterCommand) -> WriterReceipt:
        result = cast(ClaimWorkResult, receipt.result)
        events = replace(result.events, items=cast(Any, (object(),)))
        return replace(receipt, result=replace(result, events=events))

    service, _ = _service(_Writer(_receipt_factory(malformed_item)))
    with pytest.raises(WorkLeaseProtocolError, match="shape"):
        service.acquire(_acquire_request())

    def malformed_content(receipt: WriterReceipt, _command: WriterCommand) -> WriterReceipt:
        result = cast(ClaimWorkResult, receipt.result)
        record = replace(result.events.items[0], event_kind="different")
        return replace(
            receipt,
            result=replace(result, events=replace(result.events, items=(record,))),
        )

    other, _ = _service(_Writer(_receipt_factory(malformed_content)))
    with pytest.raises(WorkLeaseProtocolError, match="does not match"):
        other.acquire(_acquire_request())


def test_malformed_renewal_evidence_invalidates_the_previous_capability() -> None:
    call = 0

    def factory(command: WriterCommand, identity: WriterSubmissionId) -> _Ticket:
        nonlocal call
        call += 1
        receipt = _valid_receipt(command, identity)
        if call == 2:
            result = cast(RenewWorkClaimResult, receipt.result)
            receipt = replace(
                receipt, result=replace(result, claim=replace(result.claim, row_version=99))
            )
        return _Ticket(identity, receipt)

    service, _ = _service(_Writer(factory))
    lease = service.acquire(_acquire_request())
    with pytest.raises(WorkLeaseProtocolError, match="preserve claim"):
        service.renew(lease, _renew_request())
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 1, 0)
    with pytest.raises(WorkLeaseOwnershipError):
        service.renew(lease, _renew_request())


def test_malformed_renewal_result_type_run_and_events_are_protocol_unknown() -> None:
    mutations: list[Callable[[RenewWorkClaimResult], object]] = [
        lambda _result: object(),
        lambda result: replace(result, run=cast(Any, object())),
        lambda result: replace(result, run=replace(result.run, run_id=RunId("run_other"))),
        lambda result: replace(result, events=cast(Any, object())),
        lambda result: replace(
            result,
            claim=replace(result.claim, row_version=MAX_LEASE_ROW_VERSION + 1),
        ),
        lambda result: replace(
            result,
            run=replace(result.run, row_version=MAX_LEASE_ROW_VERSION + 1),
        ),
    ]
    messages = [
        "renewal result type",
        "RunRecord",
        "renewal run",
        "ExecutionEventBatch",
        "renewal row version",
        "run row version",
    ]
    for mutation, message in zip(mutations, messages, strict=True):
        call = 0

        def factory(
            command: WriterCommand,
            identity: WriterSubmissionId,
            *,
            mutation: Callable[[RenewWorkClaimResult], object] = mutation,
        ) -> _Ticket:
            nonlocal call
            call += 1
            receipt = _valid_receipt(command, identity)
            if call == 2:
                receipt = replace(
                    receipt, result=mutation(cast(RenewWorkClaimResult, receipt.result))
                )
            return _Ticket(identity, receipt)

        service, _ = _service(_Writer(factory))
        lease = service.acquire(_acquire_request())
        with pytest.raises(WorkLeaseProtocolError, match=message):
            service.renew(lease, _renew_request())
        assert service.snapshot() == WorkLeaseServiceSnapshot(0, 1, 0)


def test_lost_internal_reservation_is_protocol_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    original = leasing_module.WorkLeaseService._activate

    def lose_reservation(
        service: WorkLeaseService, work_item_id: WorkItemId, lease: WorkLease
    ) -> None:
        service._release_reservation(work_item_id)
        original(service, work_item_id, lease)

    monkeypatch.setattr(leasing_module.WorkLeaseService, "_activate", lose_reservation)
    service, _ = _service()
    with pytest.raises(WorkLeaseProtocolError, match="reservation"):
        service.acquire(_acquire_request())
    assert service.snapshot() == WorkLeaseServiceSnapshot(0, 1, 0)
