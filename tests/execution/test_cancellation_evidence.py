# pyright: reportPrivateUsage=false
"""Adversarial evidence-corruption tests for the cancellation coordinator."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from paritygrid.application.execution import (
    CancellationCoordinator,
    CancellationCoordinatorAdmissionError,
    CancellationCoordinatorBusyError,
    CancellationCoordinatorClockError,
    CancellationCoordinatorOutcomeUnknownError,
    CancellationCoordinatorSettings,
    CancellationDurableState,
    WorkLeaseService,
    WorkLeaseSettings,
)
from paritygrid.application.ports.configuration import (
    ConfigurationDocument,
    DocumentArray,
    NestedDocumentObject,
)
from paritygrid.application.ports.consistency import (
    EventSequence,
    ExecutionEventBatch,
)
from paritygrid.application.ports.execution import RunRecord
from paritygrid.application.ports.writer import (
    WriterCommand,
    WriterCommandKind,
    WriterReceipt,
    WriterSubmissionId,
)
from paritygrid.application.writes import TransitionRun, TransitionRunResult
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)

RUN_ID = RunId("run_cancel-test")
OTHER_RUN_ID = RunId("run_other-test")
PIPELINE_ID = PipelineId("pip_cancel-test")
WORK_ID = WorkItemId("wrk_cancel-1")
BASE_TIME = UtcTimestamp(datetime(2025, 1, 1, 0, 0, 5, tzinfo=UTC))
CREATED = UtcTimestamp(datetime(2025, 1, 1, 0, 0, 1, tzinfo=UTC))
STARTED = UtcTimestamp(datetime(2025, 1, 1, 0, 0, 2, tzinfo=UTC))


def _run() -> RunRecord:
    return RunRecord(
        RUN_ID,
        PIPELINE_ID,
        PipelineVersion(1),
        "sequential",
        ConfigurationDocument(()),
        RunState.RUNNING,
        4,
        None,
        CREATED,
        STARTED,
        None,
        None,
        None,
        None,
        None,
    )


class _Clock:
    def __init__(self, value: object = None) -> None:
        self.value = BASE_TIME if value is None else value

    def now(self) -> UtcTimestamp:
        if isinstance(self.value, BaseException):
            raise self.value
        return cast(UtcTimestamp, self.value)


class _Sink:
    def submit(self, submission: Any, /) -> Any:
        raise AssertionError("sink must not be reached")


class _Ticket:
    def __init__(self, submission_id: WriterSubmissionId, outcome: object) -> None:
        self._submission_id = submission_id
        self._outcome = outcome

    @property
    def submission_id(self) -> WriterSubmissionId:
        return self._submission_id

    def result(self, *, timeout_seconds: float) -> WriterReceipt:
        assert timeout_seconds == 60.0
        if isinstance(self._outcome, BaseException):
            raise self._outcome
        return cast(WriterReceipt, cast(Any, self._outcome)())

    async def result_async(self, *, timeout_seconds: float) -> WriterReceipt:
        return self.result(timeout_seconds=timeout_seconds)


class _Writer:
    def __init__(self, state: CancellationDurableState) -> None:
        self.state = state
        self.commands: list[WriterCommand] = []
        self.result_failures: dict[int, BaseException] = {}
        self.submit_failures: dict[int, BaseException] = {}
        self.ticket_overrides: dict[int, object] = {}
        self.receipt_mutators: dict[int, Any] = {}

    def submit(self, command: WriterCommand, *, timeout_seconds: float) -> _Ticket:
        assert timeout_seconds == 5.0
        index = len(self.commands) + 1
        failure = self.submit_failures.get(index)
        if failure is not None:
            raise failure
        self.commands.append(command)
        override = self.ticket_overrides.get(index)
        if override is not None:
            return cast(Any, override)
        submission_id = WriterSubmissionId(index)
        result_failure = self.result_failures.get(index)
        if result_failure is not None:
            return _Ticket(submission_id, result_failure)

        def commit() -> WriterReceipt:
            selected = cast(TransitionRun, command)
            previous = self.state.run
            cancellation_requested_at = previous.cancellation_requested_at
            finished_at = previous.finished_at
            if selected.target_state is RunState.CANCELLING or (
                previous.state is RunState.QUEUED and selected.target_state is RunState.CANCELLED
            ):
                cancellation_requested_at = selected.transitioned_at
            if selected.target_state.is_terminal:
                finished_at = selected.transitioned_at
            run = replace(
                previous,
                state=selected.target_state,
                row_version=previous.row_version + 1,
                cancellation_requested_at=cancellation_requested_at,
                finished_at=finished_at,
            )
            from paritygrid.application.ports.consistency import ExecutionEventRecord

            pending = selected.event.event
            record = ExecutionEventRecord(
                selected.run_id,
                selected.event.expected_next_sequence,
                pending.event_kind,
                pending.occurred_at,
                pending.subject_kind,
                pending.subject_id,
                pending.correlation_id,
                pending.payload_schema_version,
                pending.payload,
            )
            events = ExecutionEventBatch(
                (record,),
                selected.event.expected_next_sequence.advance(1),
                selected.event.expected_counter_row_version + 1,
            )
            self.state = CancellationDurableState(
                run,
                events.next_sequence,
                events.counter_row_version,
            )
            receipt = WriterReceipt(
                submission_id,
                WriterCommandKind.TRANSITION_RUN,
                selected.run_id,
                0,
                True,
                TransitionRunResult(run, events),
            )
            mutator = self.receipt_mutators.get(index)
            if mutator is not None:
                return cast(WriterReceipt, mutator(receipt))
            return receipt

        return _Ticket(submission_id, commit)


class _Reader:
    def __init__(self, writer: _Writer) -> None:
        self.writer = writer
        self.failure: BaseException | None = None
        self.override: object | None = None

    def read(self, run_id: RunId, /) -> CancellationDurableState:
        assert run_id == RUN_ID
        if self.failure is not None:
            raise self.failure
        if self.override is not None:
            return cast(CancellationDurableState, self.override)
        return self.writer.state


def _coordinator(
    *,
    state: CancellationDurableState | None = None,
    clock: _Clock | None = None,
) -> tuple[CancellationCoordinator, _Writer, _Reader, WorkLeaseService, _Sink]:
    durable = state or CancellationDurableState(_run(), EventSequence(5), 5)
    writer = _Writer(durable)
    reader = _Reader(writer)
    selected_clock = clock or _Clock()
    leases = WorkLeaseService(writer, selected_clock, settings=WorkLeaseSettings())
    sink = _Sink()
    coordinator = CancellationCoordinator(
        writer,
        reader,
        leases,
        sink,
        selected_clock,
        settings=CancellationCoordinatorSettings(),
    )
    return coordinator, writer, reader, leases, sink


def test_operations_reject_overlapping_invocations() -> None:
    coordinator, _writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    operation_lock = cast(Any, coordinator)._operation_lock
    operation_lock.acquire()
    try:
        with pytest.raises(CancellationCoordinatorBusyError, match="active operation"):
            coordinator.request_cancellation(RUN_ID)
        with pytest.raises(CancellationCoordinatorBusyError, match="active operation"):
            coordinator.cancel_work(_lease(), finished_at=BASE_TIME)
        with pytest.raises(CancellationCoordinatorBusyError, match="active operation"):
            coordinator.cancel()
    finally:
        operation_lock.release()


def test_cancel_reports_invalid_lease_snapshots() -> None:
    coordinator, _writer, _reader, leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    reservation = cast(Any, coordinator)._reservation
    leases.release_pause(reservation)
    with pytest.raises(CancellationCoordinatorOutcomeUnknownError, match="lease snapshot"):
        coordinator.cancel()


def test_cancel_rejects_invalid_clock_values() -> None:
    clock = _Clock()
    coordinator, _writer, _reader, _leases, _sink = _coordinator(clock=clock)
    coordinator.request_cancellation(RUN_ID)
    clock.value = "not a timestamp"
    with pytest.raises(CancellationCoordinatorClockError, match="invalid time"):
        coordinator.cancel()


class _ExplodingTicket:
    @property
    def submission_id(self) -> WriterSubmissionId:
        raise KeyboardInterrupt("identity interrupted")

    def result(self, *, timeout_seconds: float) -> WriterReceipt:
        raise AssertionError("result must not be reached")

    async def result_async(self, *, timeout_seconds: float) -> WriterReceipt:
        raise AssertionError("result must not be reached")


class _InvalidTicket:
    @property
    def submission_id(self) -> WriterSubmissionId:
        return cast(Any, "not an identity")

    def result(self, *, timeout_seconds: float) -> WriterReceipt:
        raise AssertionError("result must not be reached")

    async def result_async(self, *, timeout_seconds: float) -> WriterReceipt:
        raise AssertionError("result must not be reached")


def test_unexpected_admission_and_ticket_failures_are_typed() -> None:
    coordinator, writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    writer.submit_failures[1] = RuntimeError("credential=secret C:\\machine")
    with pytest.raises(CancellationCoordinatorOutcomeUnknownError, match="admission outcome"):
        coordinator.cancel()
    with pytest.raises(CancellationCoordinatorOutcomeUnknownError, match="inspection"):
        coordinator.cancel()

    other, other_writer, _other_reader, _other_leases, _other_sink = _coordinator()
    other.request_cancellation(RUN_ID)
    other_writer.submit_failures[1] = KeyboardInterrupt("interrupted submit")
    with pytest.raises(KeyboardInterrupt):
        other.cancel()

    ticketed, ticketed_writer, _ticketed_reader, _ticketed_leases, _ticketed_sink = _coordinator()
    ticketed.request_cancellation(RUN_ID)
    ticketed_writer.ticket_overrides[1] = _ExplodingTicket()
    with pytest.raises(KeyboardInterrupt):
        ticketed.cancel()

    invalid, invalid_writer, _invalid_reader, _invalid_leases, _invalid_sink = _coordinator()
    invalid.request_cancellation(RUN_ID)
    invalid_writer.ticket_overrides[1] = _InvalidTicket()
    with pytest.raises(CancellationCoordinatorOutcomeUnknownError, match="ticket identity"):
        invalid.cancel()


def test_unexpected_result_and_receipt_failures_are_typed() -> None:
    import paritygrid.application.execution.cancellation as cancellation_module

    coordinator, writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    writer.result_failures[1] = RuntimeError("unexpected result failure")
    with pytest.raises(CancellationCoordinatorOutcomeUnknownError, match="durable outcome"):
        coordinator.cancel()

    fatal, _fatal_writer, _fatal_reader, _fatal_leases, _fatal_sink = _coordinator()
    fatal.request_cancellation(RUN_ID)
    original = cancellation_module._validate_receipt

    def _raise_receipt(
        receipt: object,
        submission_id: WriterSubmissionId,
        command: TransitionRun,
        previous_run: RunRecord,
    ) -> tuple[RunRecord, ExecutionEventBatch, WriterSubmissionId]:
        raise KeyboardInterrupt("receipt interrupted")

    cancellation_module._validate_receipt = _raise_receipt  # type: ignore[assignment]
    try:
        with pytest.raises(KeyboardInterrupt):
            fatal.cancel()
    finally:
        cancellation_module._validate_receipt = original  # type: ignore[assignment]


def test_noncontiguous_event_pairs_are_rejected() -> None:
    import paritygrid.application.execution.cancellation as cancellation_module

    coordinator, _writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    original = cancellation_module._validate_receipt

    def _shifted(
        receipt: object,
        submission_id: WriterSubmissionId,
        command: TransitionRun,
        previous_run: RunRecord,
    ) -> tuple[RunRecord, ExecutionEventBatch, WriterSubmissionId]:
        run, events, identity = original(receipt, submission_id, command, previous_run)
        if command.target_state is not RunState.CANCELLED:
            return run, events, identity
        from paritygrid.application.ports.consistency import EventSequence as _Sequence

        shifted_items = (
            replace(events.items[0], sequence=_Sequence(events.items[0].sequence.number + 1)),
        )
        return run, replace(events, items=shifted_items), identity

    cancellation_module._validate_receipt = _shifted  # type: ignore[assignment]
    try:
        with pytest.raises(CancellationCoordinatorOutcomeUnknownError, match="contiguous"):
            coordinator.cancel()
    finally:
        cancellation_module._validate_receipt = original  # type: ignore[assignment]


def test_intermediate_lock_failures_poison_lifecycle() -> None:
    coordinator, _writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)

    class _FlakyLock:
        def __init__(self) -> None:
            self.entered = 0

        def __enter__(self) -> None:
            self.entered += 1
            if self.entered > 2:
                raise KeyboardInterrupt("lock interrupted")

        def __exit__(self, *_args: object) -> None:
            return None

    cast(Any, coordinator)._lifecycle_lock = _FlakyLock()
    with pytest.raises(KeyboardInterrupt):
        coordinator.cancel()
    assert cast(Any, coordinator)._uncertain is True


_DURABLE_CORRUPTIONS: list[str] = [
    "run_type",
    "run_identity",
    "run_pipeline",
    "run_pipeline_version",
    "run_state",
    "run_row_version",
    "run_scenario_seed",
    "run_created_at",
    "run_started_at",
    "run_finished_at",
    "run_cancellation_requested_at",
    "run_recovery_started_at",
    "run_recovered_at",
    "run_fingerprint",
    "run_runner_kind",
    "run_configuration",
    "document_array_values",
    "configuration_pair",
    "configuration_pair_length",
    "configuration_array",
    "configuration_nested",
    "configuration_value",
    "sequence_type",
    "counter_type",
    "counter_bool",
    "counter_range",
    "active_type",
    "active_range",
]


def _corrupt_document_array() -> ConfigurationDocument:
    array = DocumentArray(())
    object.__setattr__(array, "values", "not a tuple")
    return _corrupt_document((("key", array),))


def _corrupt_durable(kind: str) -> CancellationDurableState:
    state = CancellationDurableState(_run(), EventSequence(5), 5)
    if kind == "run_type":
        object.__setattr__(state, "run", object())
    elif kind == "sequence_type":
        object.__setattr__(state, "next_event_sequence", object())
    elif kind == "counter_type":
        object.__setattr__(state, "event_counter_row_version", "5")
    elif kind == "counter_bool":
        object.__setattr__(state, "event_counter_row_version", True)
    elif kind == "counter_range":
        object.__setattr__(state, "event_counter_row_version", 0)
    elif kind == "active_type":
        object.__setattr__(state, "active_work_count", "1")
    elif kind == "active_range":
        object.__setattr__(state, "active_work_count", -1)
    else:
        run = _run()
        fields: dict[str, tuple[str, object]] = {
            "run_identity": ("run_id", object()),
            "run_pipeline": ("pipeline_id", object()),
            "run_pipeline_version": ("pipeline_version", object()),
            "run_state": ("state", "running"),
            "run_row_version": ("row_version", 4.0),
            "run_scenario_seed": ("scenario_seed", "seed"),
            "run_created_at": ("created_at", object()),
            "run_started_at": ("started_at", object()),
            "run_finished_at": ("finished_at", object()),
            "run_cancellation_requested_at": ("cancellation_requested_at", object()),
            "run_recovery_started_at": ("recovery_started_at", object()),
            "run_recovered_at": ("recovered_at", object()),
            "run_fingerprint": ("execution_evidence_fingerprint", object()),
            "run_runner_kind": ("runner_kind", 42),
            "run_configuration": ("runner_configuration", object()),
            "document_array_values": ("runner_configuration", _corrupt_document_array()),
            "configuration_pair": (
                "runner_configuration",
                _corrupt_document((("key", ("pair",)),)),
            ),
            "configuration_pair_length": (
                "runner_configuration",
                _corrupt_document((("only-key",),)),
            ),
            "configuration_array": (
                "runner_configuration",
                _corrupt_document((("key", DocumentArray((cast(Any, object()),))),)),
            ),
            "configuration_nested": (
                "runner_configuration",
                _corrupt_document(
                    (
                        (
                            "key",
                            NestedDocumentObject((("inner", cast(Any, object())),)),
                        )
                    ),
                ),
            ),
            "configuration_value": (
                "runner_configuration",
                _corrupt_document((("key", 1.5),)),
            ),
        }
        field, value = fields[kind]
        object.__setattr__(run, field, value)
        object.__setattr__(state, "run", run)
    return state


def _corrupt_document(items: tuple[Any, ...]) -> ConfigurationDocument:
    document = ConfigurationDocument(())
    object.__setattr__(document, "items", items)
    return document


def _lease() -> Any:
    from paritygrid.application.execution import WorkLease
    from paritygrid.application.execution.leasing import _LEASE_CONSTRUCTION_TOKEN
    from paritygrid.application.ports.execution import RunNodeRecord, RunNodeStatus, WorkClaim
    from paritygrid.domain.models import AttemptNumber, Duration

    claim = WorkClaim(
        WORK_ID,
        AttemptNumber(1),
        "lease-owner",
        2,
        BASE_TIME,
        UtcTimestamp(datetime(2025, 1, 1, 0, 1, 0, tzinfo=UTC)),
        "sequential",
        "reference-worker",
    )
    node = RunNodeRecord(
        RUN_ID,
        NodeId("nod_cancel-a"),
        RunNodeStatus.RUNNING,
        2,
        1,
        0,
        1,
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
        STARTED,
        None,
    )
    return WorkLease(
        claim,
        node,
        _run(),
        ExecutionEventBatch((), EventSequence(6), 6),
        WriterSubmissionId(3),
        _token=_LEASE_CONSTRUCTION_TOKEN,
    )


@pytest.mark.parametrize("kind", _DURABLE_CORRUPTIONS)
def test_durable_evidence_corruption_fails_closed(kind: str) -> None:
    coordinator, _writer, reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    reader.override = _corrupt_durable(kind)
    with pytest.raises(CancellationCoordinatorOutcomeUnknownError) as captured:
        coordinator.cancel()
    assert "secret" not in str(captured.value)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


_RECEIPT_CORRUPTIONS: list[str] = [
    "receipt_type",
    "submission_identity",
    "submission_type",
    "command_kind",
    "run_identity",
    "contention_type",
    "contention_range",
    "result_type",
    "run_mismatch",
    "subject_kind",
    "payload_document",
    "event_record_type",
]


@pytest.mark.parametrize("kind", _RECEIPT_CORRUPTIONS)
def test_receipt_evidence_corruption_fails_closed(kind: str) -> None:
    coordinator, writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    writer.receipt_mutators[1] = _mutate_receipt(kind)
    with pytest.raises(CancellationCoordinatorOutcomeUnknownError, match="receipt"):
        coordinator.cancel()


def _mutate_receipt(kind: str) -> Any:
    def mutate(receipt: WriterReceipt) -> WriterReceipt:
        from paritygrid.application.ports.consistency import EventSubjectKind, ExecutionEventRecord

        if kind == "receipt_type":
            return cast(Any, object())
        if kind == "submission_identity":
            return replace(receipt, submission_id=WriterSubmissionId(99))
        if kind == "submission_type":
            return replace(receipt, submission_id=cast(Any, 5))
        if kind == "command_kind":
            return replace(receipt, command_kind=cast(Any, WriterCommandKind.CLAIM_WORK))
        if kind == "run_identity":
            return replace(receipt, run_id=OTHER_RUN_ID)
        if kind == "contention_type":
            return replace(receipt, contention_attempts=cast(Any, True))
        if kind == "contention_range":
            return replace(receipt, contention_attempts=10)
        if kind == "result_type":
            return replace(receipt, result=cast(Any, object()))
        if kind in {"subject_kind", "payload_document", "event_record_type"}:
            result = cast(TransitionRunResult, receipt.result)
            batch = result.events
            record = batch.items[0]
            if kind == "subject_kind":
                record = replace(record, subject_kind=EventSubjectKind.WORK_ITEM)
            elif kind == "payload_document":
                record = replace(record, payload=cast(Any, object()))
            else:
                record = cast(ExecutionEventRecord, object())
            mutated_batch = ExecutionEventBatch(
                (record, *batch.items[1:]),
                batch.next_sequence,
                batch.counter_row_version,
            )
            return replace(
                receipt,
                result=TransitionRunResult(result.run, mutated_batch),
            )
        return replace(
            receipt,
            result=TransitionRunResult(
                replace(cast(TransitionRunResult, receipt.result).run, row_version=99),
                cast(TransitionRunResult, receipt.result).events,
            ),
        )

    return mutate


def test_receipt_event_evidence_corruption_fails_closed() -> None:
    coordinator, writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)

    def _corrupt_events(receipt: WriterReceipt) -> WriterReceipt:
        result = cast(TransitionRunResult, receipt.result)
        corrupted = replace(result, events=cast(Any, object()))
        return replace(receipt, result=corrupted)

    writer.receipt_mutators[1] = _corrupt_events
    with pytest.raises(CancellationCoordinatorOutcomeUnknownError, match="receipt"):
        coordinator.cancel()

    event_corruptor, event_writer, _event_reader, _event_leases, _event_sink = _coordinator()
    event_corruptor.request_cancellation(RUN_ID)

    def _corrupt_event_record(receipt: WriterReceipt) -> WriterReceipt:
        result = cast(TransitionRunResult, receipt.result)
        events = result.events
        record = events.items[0]

        corrupted_record = replace(record, subject_id=WORK_ID)
        corrupted_batch = ExecutionEventBatch(
            (corrupted_record,),
            events.next_sequence,
            events.counter_row_version,
        )
        return replace(receipt, result=TransitionRunResult(result.run, corrupted_batch))

    event_writer.receipt_mutators[1] = _corrupt_event_record
    with pytest.raises(CancellationCoordinatorOutcomeUnknownError, match="receipt"):
        event_corruptor.cancel()


def test_durable_state_validates_exact_types() -> None:
    with pytest.raises(TypeError, match="run must use RunRecord"):
        CancellationDurableState(cast(Any, object()), EventSequence(5), 5)
    with pytest.raises(TypeError, match="event frontier"):
        CancellationDurableState(_run(), cast(Any, object()), 5)
    with pytest.raises(TypeError, match="event counter"):
        CancellationDurableState(_run(), EventSequence(5), cast(Any, "5"))
    with pytest.raises(ValueError, match="event counter"):
        CancellationDurableState(_run(), EventSequence(5), 0)
    with pytest.raises(TypeError, match="active work count"):
        CancellationDurableState(_run(), EventSequence(5), 5, cast(Any, "1"))
    with pytest.raises(ValueError, match="active work count"):
        CancellationDurableState(_run(), EventSequence(5), 5, -1)


def test_submit_rejection_before_admission_is_not_uncertain() -> None:
    from paritygrid.application.ports.writer import WriterClosedError

    coordinator, writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    writer.submit_failures[1] = WriterClosedError("writer closed")
    with pytest.raises(CancellationCoordinatorAdmissionError, match="admission failed"):
        coordinator.cancel()


def test_ticket_identity_generic_failures_are_typed() -> None:
    coordinator, writer, _reader, _leases, _sink = _coordinator()

    class _RaisingTicket:
        @property
        def submission_id(self) -> WriterSubmissionId:
            raise RuntimeError("identity unavailable")

        def result(self, *, timeout_seconds: float) -> WriterReceipt:
            raise AssertionError("result must not be reached")

        async def result_async(self, *, timeout_seconds: float) -> WriterReceipt:
            raise AssertionError("result must not be reached")

    coordinator.request_cancellation(RUN_ID)
    writer.ticket_overrides[1] = _RaisingTicket()
    with pytest.raises(CancellationCoordinatorOutcomeUnknownError, match="ticket identity"):
        coordinator.cancel()


def test_run_evidence_with_seed_and_fingerprint_cancels_exactly() -> None:
    from paritygrid.domain.models import StateFingerprint

    seeded = replace(
        _run(),
        scenario_seed=42,
        execution_evidence_fingerprint=StateFingerprint("0" * 64),
        execution_evidence_fingerprint_version=2,
        runner_configuration=ConfigurationDocument(
            (
                ("array", DocumentArray((1, "two"))),
                ("nested", NestedDocumentObject((("inner", True),))),
            )
        ),
    )
    coordinator, writer, reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    seeded_state = CancellationDurableState(seeded, EventSequence(5), 5)
    reader.override = seeded_state
    writer.state = seeded_state
    report = coordinator.cancel()
    assert report.run.scenario_seed == 42
    assert report.run.execution_evidence_fingerprint == StateFingerprint("0" * 64)


def test_cancel_work_rejects_non_text_runner_kind_and_detail() -> None:
    from paritygrid.application.execution import CancellationCoordinatorInvalidRequestError

    coordinator, _writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    unknown_runner = _lease()
    claim = replace(unknown_runner.claim, runner_kind=42)
    object.__setattr__(unknown_runner, "_claim", claim)
    with pytest.raises(CancellationCoordinatorInvalidRequestError, match="text"):
        coordinator.cancel_work(unknown_runner, finished_at=BASE_TIME)
    with pytest.raises(CancellationCoordinatorInvalidRequestError, match="detail"):
        coordinator.cancel_work(_lease(), finished_at=BASE_TIME, detail=42)  # type: ignore[arg-type]


def test_cancel_work_rejects_leases_without_active_service_capability() -> None:
    from paritygrid.application.execution import ResultSinkInvalidResultError

    coordinator, _writer, _reader, _leases, _sink = _coordinator()
    coordinator.request_cancellation(RUN_ID)
    with pytest.raises(ResultSinkInvalidResultError, match="active service capability"):
        coordinator.cancel_work(_lease(), finished_at=BASE_TIME)
