# pyright: reportPrivateUsage=false
"""Adversarial evidence-corruption tests for terminal run finalization."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from paritygrid.application.execution import (
    FinalizationAdmissionError,
    FinalizationAnalyticsError,
    FinalizationConflictError,
    FinalizationEvidence,
    FinalizationInvalidRequestError,
    FinalizationNotReadyError,
    FinalizationOutcomeUnknownError,
    FinalizationProtocolError,
    FinalizationSettings,
    FinalizationVerificationError,
    RunFinalizer,
)
from paritygrid.application.planner import PlanFingerprint
from paritygrid.application.ports.configuration import (
    ConfigurationDocument,
    DocumentArray,
    NestedDocumentObject,
)
from paritygrid.application.ports.consistency import (
    EventSequence,
    ExecutionEventBatch,
    ExecutionEventRecord,
)
from paritygrid.application.ports.execution import (
    AttemptOutcome,
    RunNodeRecord,
    RunNodeStatus,
    RunRecord,
    WorkAttemptRecord,
    WorkItemRecord,
    WorkItemState,
)
from paritygrid.application.ports.run_statistics import (
    RunStatisticsSourceSnapshot,
    RunStatisticsSummary,
)
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    WriterCommand,
    WriterCommandKind,
    WriterReceipt,
    WriterSubmissionId,
)
from paritygrid.application.writes import (
    FinalizeEmptyRunNode,
    FinalizeEmptyRunNodeResult,
    TransitionRun,
    TransitionRunResult,
)
from paritygrid.domain.execution import FailureClassification, RunState
from paritygrid.domain.models import (
    AttemptNumber,
    Duration,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    StateFingerprint,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

RUN_ID = RunId("run_final-test")
NODE_A = NodeId("nod_final-a")
NODE_EMPTY = NodeId("nod_final-empty")
PLAN_NODES = (NODE_A, NODE_EMPTY)
PLAN_FINGERPRINT = PlanFingerprint("1" * 64)


def _time(second: int) -> UtcTimestamp:
    return UtcTimestamp(
        datetime(2025, 2, 1, 0, 0, second % 60, tzinfo=UTC) + timedelta(minutes=second // 60)
    )


def _run(
    *,
    state: RunState = RunState.RUNNING,
    row_version: int = 8,
    fingerprint: StateFingerprint | None = None,
) -> RunRecord:
    return RunRecord(
        RUN_ID,
        PipelineId("pip_final-test"),
        PipelineVersion(1),
        "sequential",
        ConfigurationDocument(()),
        state,
        row_version,
        42,
        _time(1),
        _time(2) if state is not RunState.QUEUED else None,
        None,
        None,
        None,
        None,
        fingerprint,
    )


def _success_work() -> WorkItemRecord:
    return WorkItemRecord(
        WorkItemId("wrk_final-a"),
        RUN_ID,
        NODE_A,
        PartitionKey("part-a"),
        WorkItemState.SUCCEEDED,
        3,
        1,
        1,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        _time(3),
        _time(4),
    )


def _success_attempt() -> WorkAttemptRecord:
    return WorkAttemptRecord(
        WorkItemId("wrk_final-a"),
        AttemptNumber(1),
        _time(4),
        _time(5),
        "sequential",
        "final-worker",
        AttemptOutcome.SUCCEEDED,
        None,
        None,
        None,
        1,
        2,
        Duration(1_000_000),
    )


def _success_node() -> RunNodeRecord:
    return RunNodeRecord(
        RUN_ID,
        NODE_A,
        RunNodeStatus.SUCCEEDED,
        2,
        1,
        0,
        0,
        1,
        0,
        0,
        0,
        1,
        1,
        0,
        1,
        2,
        0,
        Duration(1_000_000),
        _time(2),
        _time(5),
    )


def _empty_node(node_id: NodeId = NODE_EMPTY) -> RunNodeRecord:
    return RunNodeRecord(
        RUN_ID,
        node_id,
        RunNodeStatus.PENDING,
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
        0,
        0,
        0,
        Duration(0),
        None,
        None,
    )


def _evidence(
    *,
    run: RunRecord | None = None,
    fingerprint: StateFingerprint | None = None,
    with_work: bool = True,
    finished_run: bool = False,
) -> FinalizationEvidence:
    selected_run = run or _run(
        fingerprint=fingerprint,
        state=RunState.SUCCEEDED if finished_run else RunState.RUNNING,
        row_version=9 if finished_run else 8,
    )
    return FinalizationEvidence(
        selected_run,
        EventSequence(9),
        9,
        (_success_node(), _empty_node()),
        (_success_work(),) if with_work else (),
        (_success_attempt(),) if with_work else (),
        ((WorkItemId("wrk_final-a"), 1),) if with_work else (),
    )


class _Clock:
    def __init__(self, value: object = None) -> None:
        self.value = _time(20) if value is None else value

    def now(self) -> UtcTimestamp:
        if isinstance(self.value, BaseException):
            raise self.value
        return cast(UtcTimestamp, self.value)


class _Analytics:
    def __init__(
        self, *, lie: dict[str, int] | None = None, failure: BaseException | None = None
    ) -> None:
        self.lie = lie
        self.failure = failure

    def rebuild(self, source: RunStatisticsSourceSnapshot) -> object:
        if self.failure is not None:
            raise self.failure
        return source

    def get_summary(self, snapshot: object) -> RunStatisticsSummary:
        if self.failure is not None:
            raise self.failure
        source = cast(RunStatisticsSourceSnapshot, snapshot)
        counts = dict.fromkeys(WorkItemState, 0)
        for work in source.work_items:
            counts[work.state] += 1
        retry = sum(
            attempt.outcome in {AttemptOutcome.RETRY_SCHEDULED, AttemptOutcome.LEASE_EXPIRED}
            for attempt in source.attempts
        )
        duration = sum(attempt.duration.microseconds for attempt in source.attempts)
        summary = RunStatisticsSummary(
            RUN_ID,
            source.run.row_version,
            source.run.state,
            len(source.nodes),
            len(source.work_items),
            counts[WorkItemState.PENDING] + counts[WorkItemState.RETRY_WAIT],
            counts[WorkItemState.RUNNING],
            counts[WorkItemState.SUCCEEDED],
            counts[WorkItemState.QUARANTINED],
            counts[WorkItemState.FAILED],
            counts[WorkItemState.CANCELLED],
            len(source.attempts),
            retry,
            len(source.attempts),
            len(source.attempts) * 2,
            counts[WorkItemState.QUARANTINED],
            len(source.attempts) * 2,
            len(source.attempts) * 4,
            duration,
            None,
            None,
            None,
            source.run.started_at,
            source.run.finished_at,
        )
        if self.lie is not None:
            return cast(RunStatisticsSummary, replace(cast(Any, summary), **self.lie))
        return summary


class _Ticket:
    def __init__(self, submission_id: WriterSubmissionId, outcome: object) -> None:
        self._submission_id = submission_id
        self._outcome = outcome

    @property
    def submission_id(self) -> WriterSubmissionId:
        if isinstance(self._outcome, _IdentityFailure):
            raise self._outcome.failure
        return self._submission_id

    def result(self, *, timeout_seconds: float) -> WriterReceipt:
        assert timeout_seconds == 60.0
        if isinstance(self._outcome, BaseException):
            raise self._outcome
        return cast(WriterReceipt, self._outcome)

    async def result_async(self, *, timeout_seconds: float) -> WriterReceipt:
        return self.result(timeout_seconds=timeout_seconds)


class _IdentityFailure:
    def __init__(self, failure: BaseException) -> None:
        self.failure = failure


class _InvalidIdentityTicket:
    @property
    def submission_id(self) -> WriterSubmissionId:
        return cast(Any, "not an identity")

    def result(self, *, timeout_seconds: float) -> WriterReceipt:
        raise AssertionError("result must not be reached")

    async def result_async(self, *, timeout_seconds: float) -> WriterReceipt:
        raise AssertionError("result must not be reached")


class _Writer:
    def __init__(self, run_template: RunRecord | None = None) -> None:
        self.run_template = run_template
        self.commands: list[WriterCommand] = []
        self.result_failures: dict[int, BaseException] = {}
        self.admission_failures: dict[int, BaseException] = {}
        self.ticket_overrides: dict[int, object] = {}
        self.receipt_mutators: dict[int, Any] = {}

    def submit(self, command: WriterCommand, *, timeout_seconds: float) -> Any:
        assert timeout_seconds == 5.0
        index = len(self.commands) + 1
        failure = self.admission_failures.get(index)
        if failure is not None:
            raise failure
        self.commands.append(command)
        override = self.ticket_overrides.get(index)
        if override is not None:
            return override
        submission_id = WriterSubmissionId(index)
        result_failure = self.result_failures.get(index)
        if result_failure is not None:
            return _Ticket(submission_id, result_failure)
        return _Ticket(submission_id, self._receipt(command, submission_id, index))

    def _receipt(
        self, command: WriterCommand, submission_id: WriterSubmissionId, index: int
    ) -> WriterReceipt:
        if isinstance(command, FinalizeEmptyRunNode):
            node = replace(
                _empty_node(command.node_id),
                status=RunNodeStatus.SUCCEEDED,
                row_version=command.expected_node_row_version + 1,
                started_at=command.finalized_at,
                finished_at=command.finalized_at,
            )
            events = _events(command.event)
            advanced = replace(_run(), row_version=command.expected_run_row_version + 1)
            receipt = WriterReceipt(
                submission_id,
                WriterCommandKind.FINALIZE_EMPTY_RUN_NODE,
                command.run_id,
                0,
                True,
                FinalizeEmptyRunNodeResult(node, events, advanced),
            )
        else:
            selected = cast(TransitionRun, command)
            template = self.run_template or _run()
            run = replace(
                template,
                state=selected.target_state,
                row_version=selected.expected_run_row_version + 1,
                finished_at=selected.transitioned_at,
            )
            if selected.final_reconciliation_fingerprint is not None:
                run = replace(
                    run, final_reconciliation_fingerprint=selected.final_reconciliation_fingerprint
                )
            events = _events(selected.event)
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


def _events(request: EventAppendRequest) -> ExecutionEventBatch:
    pending = request.event
    record = ExecutionEventRecord(
        RUN_ID,
        request.expected_next_sequence,
        pending.event_kind,
        pending.occurred_at,
        pending.subject_kind,
        pending.subject_id,
        pending.correlation_id,
        pending.payload_schema_version,
        pending.payload,
    )
    return ExecutionEventBatch(
        (record,),
        request.expected_next_sequence.advance(1),
        request.expected_counter_row_version + 1,
    )


class _Reader:
    def __init__(self, evidence: FinalizationEvidence) -> None:
        self.evidence = evidence
        self.failure: BaseException | None = None

    def read(self, run_id: RunId, /) -> FinalizationEvidence:
        assert run_id == RUN_ID
        if self.failure is not None:
            raise self.failure
        return self.evidence


def _finalizer(
    evidence: FinalizationEvidence,
    *,
    clock: _Clock | None = None,
    analytics: _Analytics | None = None,
) -> tuple[RunFinalizer, _Writer, _Reader, _Analytics]:
    writer = _Writer(run_template=cast(Any, evidence).run)
    reader = _Reader(evidence)
    selected_analytics = analytics or _Analytics()
    selected_clock = clock or _Clock()
    finalizer = RunFinalizer(
        writer,
        reader,
        cast(Any, selected_analytics),
        selected_clock,
        settings=FinalizationSettings(),
    )
    return finalizer, writer, reader, selected_analytics


def test_finalized_success_replay_without_fingerprint_is_a_conflict() -> None:
    finalizer, writer, _reader, _analytics = _finalizer(
        _evidence(finished_run=True, fingerprint=None)
    )
    with pytest.raises(FinalizationConflictError, match="missing its fingerprint"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
    assert writer.commands == []


def test_failed_replay_divergence_is_a_conflict() -> None:
    divergent = _evidence(with_work=False)
    object.__setattr__(
        divergent,
        "run",
        replace(_run(state=RunState.FAILED, row_version=9), finished_at=_time(19)),
    )
    finalizer, writer, _reader, _analytics = _finalizer(divergent)
    with pytest.raises(FinalizationConflictError, match="diverges"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
    assert writer.commands == []


def test_failed_replay_with_fingerprint_is_a_conflict() -> None:
    evidence = _evidence(with_work=False)
    object.__setattr__(
        evidence,
        "run",
        replace(
            _run(state=RunState.FAILED, row_version=9),
            finished_at=_time(19),
            final_reconciliation_fingerprint=StateFingerprint("4" * 64),
        ),
    )
    # Terminal failed replay requires work evidence that derives FAILED; give it one.
    failed_work = replace(
        _success_work(), state=WorkItemState.FAILED, expected_checkpoint_version=0
    )
    failed_attempt = replace(
        _success_attempt(),
        outcome=AttemptOutcome.FAILED,
        failure_classification=FailureClassification.UNKNOWN,
    )
    failed_node = replace(
        _success_node(),
        status=RunNodeStatus.FAILED,
        work_succeeded=0,
        work_failed=1,
    )
    object.__setattr__(evidence, "work", (failed_work,))
    object.__setattr__(evidence, "attempts", (failed_attempt,))
    object.__setattr__(evidence, "nodes", (failed_node, _empty_node()))
    object.__setattr__(evidence, "checkpoint_versions", ((failed_work.work_item_id, 0),))
    finalizer, _writer, _reader, _analytics = _finalizer(evidence)
    with pytest.raises(FinalizationConflictError, match="must not store"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)


def test_analytics_count_lies_are_protocol_failures() -> None:
    coherent_lies: tuple[dict[str, int], ...] = (
        {"work_succeeded": 0, "work_failed": 1},
        {"work_succeeded": 0, "work_quarantined": 1},
        {"work_succeeded": 0, "work_cancelled": 1},
    )
    for lie in coherent_lies:
        finalizer, _writer, _reader, _analytics = _finalizer(
            _evidence(), analytics=_Analytics(lie=lie)
        )
        with pytest.raises(FinalizationOutcomeUnknownError, match="inconsistent"):
            finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)


def test_clock_failures_are_typed() -> None:
    clock = _Clock()
    finalizer, writer, _reader, _analytics = _finalizer(_evidence(), clock=clock)
    clock.value = RuntimeError("clock unavailable")
    with pytest.raises(Exception, match="clock failed"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
    clock.value = _time(1)
    with pytest.raises(Exception, match="behind durable"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
    clock.value = "not a timestamp"
    with pytest.raises(Exception, match="invalid time"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
    assert writer.commands == []


def test_empty_node_clock_runs_before_transition() -> None:
    clock = _Clock(_time(20))
    finalizer, _writer, _reader, _analytics = _finalizer(_evidence(), clock=clock)
    report = finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
    assert report.action.value == "finalized"
    empty_command = cast(FinalizeEmptyRunNode, _writer.commands[0])
    assert empty_command.finalized_at == _time(20)


def test_unexpected_admission_and_ticket_failures_are_typed() -> None:
    finalizer, writer, _reader, _analytics = _finalizer(_evidence())
    writer.admission_failures[1] = RuntimeError("credential=secret C:\\machine")
    with pytest.raises(FinalizationOutcomeUnknownError, match="admission outcome"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)

    fatal_writer_case, fatal_writer, _r2, _a2 = _finalizer(_evidence())
    fatal_writer.admission_failures[1] = KeyboardInterrupt("interrupted submit")
    with pytest.raises(KeyboardInterrupt):
        fatal_writer_case.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)

    identity_case, identity_writer, _r3, _a3 = _finalizer(_evidence())
    identity_writer.ticket_overrides[1] = _InvalidIdentityTicket()
    with pytest.raises(FinalizationOutcomeUnknownError, match="ticket identity"):
        identity_case.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)


def test_unexpected_result_failures_are_typed() -> None:
    finalizer, writer, _reader, _analytics = _finalizer(_evidence())
    writer.result_failures[1] = RuntimeError("unexpected result failure")
    with pytest.raises(FinalizationOutcomeUnknownError, match="durable outcome"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)


_RECEIPT_CORRUPTIONS: list[str] = [
    "receipt_type",
    "submission_identity",
    "submission_type",
    "command_kind",
    "run_identity",
    "contention_type",
    "contention_range",
    "mutated_false",
    "result_type",
    "run_mismatch",
    "finished_at_mismatch",
    "fingerprint_missing",
    "events_type",
    "event_record_type",
    "event_payload",
    "event_subject",
    "event_subject_identity",
]


def _work_only_evidence() -> FinalizationEvidence:
    evidence = _evidence()
    object.__setattr__(evidence, "nodes", (_success_node(),))
    return evidence


@pytest.mark.parametrize("kind", _RECEIPT_CORRUPTIONS)
def test_receipt_evidence_corruption_fails_verification(kind: str) -> None:
    finalizer, writer, _reader, _analytics = _finalizer(_work_only_evidence())
    writer.receipt_mutators[1] = _mutate_receipt(kind)
    with pytest.raises(FinalizationOutcomeUnknownError) as captured:
        finalizer.finalize(RUN_ID, plan_nodes=(NODE_A,), plan_fingerprint=PLAN_FINGERPRINT)
    assert "secret" not in str(captured.value)
    with pytest.raises(FinalizationOutcomeUnknownError, match="recovery inspection"):
        finalizer.finalize(RUN_ID, plan_nodes=(NODE_A,), plan_fingerprint=PLAN_FINGERPRINT)


def _mutate_receipt(kind: str) -> Any:
    def mutate(receipt: WriterReceipt) -> WriterReceipt:
        if kind == "receipt_type":
            return cast(Any, object())
        if kind == "submission_identity":
            return replace(receipt, submission_id=WriterSubmissionId(99))
        if kind == "submission_type":
            return replace(receipt, submission_id=cast(Any, 5))
        if kind == "command_kind":
            return replace(receipt, command_kind=cast(Any, WriterCommandKind.CLAIM_WORK))
        if kind == "run_identity":
            return replace(receipt, run_id=RunId("run_other"))
        if kind == "contention_type":
            return replace(receipt, contention_attempts=cast(Any, True))
        if kind == "contention_range":
            return replace(receipt, contention_attempts=10)
        if kind == "mutated_false":
            return replace(receipt, mutated=False)
        if kind == "result_type":
            return replace(receipt, result=cast(Any, object()))
        result = cast(TransitionRunResult, receipt.result)
        if kind == "run_mismatch":
            return replace(
                receipt,
                result=TransitionRunResult(replace(result.run, row_version=99), result.events),
            )
        if kind == "finished_at_mismatch":
            return replace(
                receipt,
                result=TransitionRunResult(
                    replace(result.run, finished_at=_time(19)), result.events
                ),
            )
        if kind == "fingerprint_missing":
            return replace(
                receipt,
                result=TransitionRunResult(
                    replace(result.run, final_reconciliation_fingerprint=None), result.events
                ),
            )
        if kind == "events_type":
            return replace(receipt, result=TransitionRunResult(result.run, cast(Any, object())))
        record = result.events.items[0]
        if kind == "event_record_type":
            batch = ExecutionEventBatch(
                (cast(Any, object()),),
                result.events.next_sequence,
                result.events.counter_row_version,
            )
            return replace(receipt, result=TransitionRunResult(result.run, batch))
        if kind == "event_payload":
            corrupted_payload = replace(record, payload=cast(Any, object()))
            batch = ExecutionEventBatch(
                (corrupted_payload,),
                result.events.next_sequence,
                result.events.counter_row_version,
            )
            return replace(receipt, result=TransitionRunResult(result.run, batch))
        if kind == "event_subject_identity":
            corrupted = replace(record, subject_id=WorkItemId("wrk_final-a"))
        else:
            from paritygrid.application.ports.consistency import EventSubjectKind

            corrupted = replace(record, subject_kind=EventSubjectKind.WORK_ITEM)
        batch = ExecutionEventBatch(
            (corrupted,),
            result.events.next_sequence,
            result.events.counter_row_version,
        )
        return replace(receipt, result=TransitionRunResult(result.run, batch))

    return mutate


def test_empty_node_receipt_corruption_fails_verification() -> None:
    evidence = FinalizationEvidence(
        _run(),
        EventSequence(9),
        9,
        (_empty_node(NODE_A), _empty_node(NODE_EMPTY)),
        (),
        (),
        (),
    )
    finalizer, writer, _reader, _analytics = _finalizer(evidence)

    def _corrupt(receipt: WriterReceipt) -> WriterReceipt:
        result = cast(FinalizeEmptyRunNodeResult, receipt.result)
        return replace(
            receipt,
            result=FinalizeEmptyRunNodeResult(
                replace(result.node, status=RunNodeStatus.PENDING), result.events, result.run
            ),
        )

    writer.receipt_mutators[1] = _corrupt
    with pytest.raises(FinalizationOutcomeUnknownError):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)


_DURABLE_CORRUPTIONS: list[str] = [
    "run_type",
    "sequence_type",
    "counter_type",
    "counter_range",
    "nodes_type",
    "work_type",
    "attempts_type",
    "checkpoints_type",
    "nodes_empty",
    "checkpoint_entry_type",
    "checkpoint_entry_pair",
    "checkpoint_identity",
    "checkpoint_version",
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
    "configuration_pair",
    "configuration_pair_length",
    "configuration_array",
    "configuration_nested",
    "configuration_value",
    "node_type",
    "node_identity",
    "node_status",
    "node_row_version",
    "node_started_at",
    "node_finished_at",
    "work_identity",
    "work_state",
    "work_row_version",
    "work_created_at",
    "work_updated_at",
    "work_retry_available_at",
    "attempt_type",
    "attempt_identity",
    "attempt_number",
    "attempt_started_at",
    "attempt_finished_at",
    "attempt_runner_kind",
    "attempt_outcome",
]


@pytest.mark.parametrize("kind", _DURABLE_CORRUPTIONS)
def test_durable_evidence_corruption_fails_closed(kind: str) -> None:
    finalizer, _writer, reader, _analytics = _finalizer(_evidence())
    reader.evidence = _corrupt_evidence(kind)
    with pytest.raises((FinalizationOutcomeUnknownError, FinalizationNotReadyError)) as captured:
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
    assert "secret" not in str(captured.value)


def _corrupt_evidence(kind: str) -> FinalizationEvidence:
    evidence = _evidence()
    if kind == "run_type":
        object.__setattr__(evidence, "run", object())
    elif kind == "sequence_type":
        object.__setattr__(evidence, "next_event_sequence", object())
    elif kind == "counter_type":
        object.__setattr__(evidence, "event_counter_row_version", "9")
    elif kind == "counter_range":
        object.__setattr__(evidence, "event_counter_row_version", 0)
    elif kind == "nodes_type":
        object.__setattr__(evidence, "nodes", (object(),))
    elif kind == "work_type":
        object.__setattr__(evidence, "work", (object(),))
    elif kind == "attempts_type":
        object.__setattr__(evidence, "attempts", (object(),))
    elif kind == "checkpoints_type":
        object.__setattr__(evidence, "checkpoint_versions", (object(),))
    elif kind == "nodes_empty":
        object.__setattr__(evidence, "nodes", ())
    elif kind == "checkpoint_entry_type":
        object.__setattr__(evidence, "checkpoint_versions", (object(),))
    elif kind == "checkpoint_entry_pair":
        object.__setattr__(evidence, "checkpoint_versions", ((WorkItemId("wrk_final-a"),),))
    elif kind == "checkpoint_identity":
        object.__setattr__(
            evidence,
            "checkpoint_versions",
            ((cast(Any, "wrk"), 1),),
        )
    elif kind == "checkpoint_version":
        object.__setattr__(
            evidence,
            "checkpoint_versions",
            ((WorkItemId("wrk_final-a"), cast(Any, "1")),),
        )
    elif kind in {
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
        "configuration_pair",
        "configuration_pair_length",
        "configuration_array",
        "configuration_nested",
        "configuration_value",
    }:
        fields: dict[str, tuple[str, object]] = {
            "run_identity": ("run_id", object()),
            "run_pipeline": ("pipeline_id", object()),
            "run_pipeline_version": ("pipeline_version", object()),
            "run_state": ("state", "running"),
            "run_row_version": ("row_version", 8.0),
            "run_scenario_seed": ("scenario_seed", "seed"),
            "run_created_at": ("created_at", object()),
            "run_started_at": ("started_at", object()),
            "run_finished_at": ("finished_at", object()),
            "run_cancellation_requested_at": ("cancellation_requested_at", object()),
            "run_recovery_started_at": ("recovery_started_at", object()),
            "run_recovered_at": ("recovered_at", object()),
            "run_fingerprint": ("final_reconciliation_fingerprint", object()),
            "run_runner_kind": ("runner_kind", 42),
            "run_configuration": ("runner_configuration", object()),
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
        object.__setattr__(evidence.run, field, value)
    elif kind in {
        "node_type",
        "node_identity",
        "node_status",
        "node_row_version",
        "node_started_at",
        "node_finished_at",
    }:
        if kind == "node_type":
            object.__setattr__(evidence, "nodes", (object(), _empty_node()))
        else:
            node_fields: dict[str, tuple[str, object]] = {
                "node_identity": ("node_id", object()),
                "node_status": ("status", "succeeded"),
                "node_row_version": ("row_version", 2.0),
                "node_started_at": ("started_at", object()),
                "node_finished_at": ("finished_at", object()),
            }
            field, value = node_fields[kind]
            object.__setattr__(evidence.nodes[0], field, value)
    elif kind.startswith(("work_", "attempt_")):
        _corrupt_nested(evidence, kind)
    return evidence


def _corrupt_nested(evidence: FinalizationEvidence, kind: str) -> None:
    if kind == "work_type":
        object.__setattr__(evidence, "work", (object(),))
    elif kind == "attempt_type":
        object.__setattr__(evidence, "attempts", (object(),))
    else:
        work_fields = {
            "work_identity": ("work_item_id", object()),
            "work_state": ("state", "succeeded"),
            "work_row_version": ("row_version", 3.0),
            "work_created_at": ("created_at", object()),
            "work_updated_at": ("updated_at", object()),
            "work_retry_available_at": ("retry_available_at", object()),
        }
        attempt_fields = {
            "attempt_identity": ("work_item_id", object()),
            "attempt_number": ("attempt_number", cast(Any, object())),
            "attempt_started_at": ("started_at", object()),
            "attempt_finished_at": ("finished_at", object()),
            "attempt_runner_kind": ("runner_kind", 42),
            "attempt_outcome": ("outcome", "succeeded"),
        }
        if kind in work_fields:
            field, value = work_fields[kind]
            object.__setattr__(evidence.work[0], field, value)
        elif kind in attempt_fields:
            field, value = attempt_fields[kind]
            object.__setattr__(evidence.attempts[0], field, value)


def _corrupt_document(items: tuple[Any, ...]) -> ConfigurationDocument:
    document = ConfigurationDocument(())
    object.__setattr__(document, "items", items)
    return document


def test_finalize_rejects_evidence_without_nodes_before_any_command() -> None:
    finalizer, writer, _reader, _analytics = _finalizer(_evidence())
    object.__setattr__(finalizer, "_reader", _Reader(cast(Any, object())))
    with pytest.raises(FinalizationOutcomeUnknownError, match="frontier is invalid"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
    assert writer.commands == []


def test_successful_work_without_checkpoint_version_is_a_conflict() -> None:
    evidence = _evidence()
    object.__setattr__(
        evidence,
        "checkpoint_versions",
        ((WorkItemId("wrk_final-a"), 0),),
    )
    object.__setattr__(evidence.work[0], "expected_checkpoint_version", 0)
    finalizer, writer, _reader, _analytics = _finalizer(evidence)
    with pytest.raises(FinalizationConflictError, match="missing its checkpoint"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
    assert writer.commands == []


def test_analytics_rebuild_rejection_is_an_analytics_failure() -> None:
    from paritygrid.application.ports.run_statistics import (
        RunStatisticsInvalidError,
        RunStatisticsQuerySnapshot,
    )

    analytics = _Analytics()

    def _failing_rebuild(source: RunStatisticsSourceSnapshot) -> RunStatisticsQuerySnapshot:
        raise RunStatisticsInvalidError("invalid source")

    analytics.rebuild = _failing_rebuild  # type: ignore[method-assign]
    finalizer, writer, _reader, _analytics = _finalizer(_evidence(), analytics=analytics)
    with pytest.raises(FinalizationAnalyticsError, match="analytics boundary failed"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
    assert all(type(command).__name__ == "FinalizeEmptyRunNode" for command in writer.commands)


def test_verification_error_is_outcome_unknown() -> None:
    assert issubclass(FinalizationVerificationError, FinalizationOutcomeUnknownError)
    assert issubclass(FinalizationProtocolError, FinalizationOutcomeUnknownError)


def test_evidence_document_covers_seed_and_fingerprint() -> None:
    from paritygrid.application.execution.finalization import (
        _AnalyticsProjection,
        _final_fingerprint,
    )

    finalizer, _writer, _reader, analytics = _finalizer(_evidence())
    source = RunStatisticsSourceSnapshot(
        _evidence().run,
        _evidence().nodes,
        _evidence().work,
        _evidence().attempts,
    )
    summary = analytics.get_summary(analytics.rebuild(source))
    projection = _AnalyticsProjection(source.source_sha256, summary)
    first = _final_fingerprint(PLAN_FINGERPRINT, _evidence(), projection)
    assert first == _final_fingerprint(PLAN_FINGERPRINT, _evidence(), projection)
    del finalizer


def test_evidence_constructor_validates_exact_types() -> None:
    with pytest.raises(TypeError, match="must use RunRecord"):
        FinalizationEvidence(
            cast(Any, object()), EventSequence(9), 9, (_success_node(),), (), (), ()
        )
    with pytest.raises(TypeError, match="EventSequence"):
        FinalizationEvidence(_run(), cast(Any, object()), 9, (_success_node(),), (), (), ())
    with pytest.raises(TypeError, match="counter row version"):
        FinalizationEvidence(
            _run(), EventSequence(9), cast(Any, "9"), (_success_node(),), (), (), ()
        )
    with pytest.raises(ValueError, match="counter row version"):
        FinalizationEvidence(_run(), EventSequence(9), 0, (_success_node(),), (), (), ())
    with pytest.raises(TypeError, match="finalization nodes must be a tuple"):
        FinalizationEvidence(_run(), EventSequence(9), 9, cast(Any, []), (), (), ())
    with pytest.raises(TypeError, match="checkpoint frontier must be a tuple"):
        FinalizationEvidence(_run(), EventSequence(9), 9, (_success_node(),), (), (), cast(Any, []))
    with pytest.raises(ValueError, match="requires run nodes"):
        FinalizationEvidence(_run(), EventSequence(9), 9, (), (), (), ())
    with pytest.raises(ValueError, match="node limit"):
        FinalizationEvidence(
            _run(),
            EventSequence(9),
            9,
            tuple(_success_node() for _ in range(257)),
            (),
            (),
            (),
        )
    with pytest.raises(TypeError, match="checkpoint entry is invalid"):
        FinalizationEvidence(
            _run(),
            EventSequence(9),
            9,
            (_success_node(),),
            (),
            (),
            cast(Any, (object(),)),
        )


def test_frontier_headroom_is_rejected_before_admission() -> None:
    crowded = _evidence()
    object.__setattr__(crowded, "run", replace(_run(), row_version=2_147_483_647))
    finalizer, writer, _reader, _analytics = _finalizer(crowded)
    with pytest.raises(FinalizationInvalidRequestError, match="cannot advance"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
    assert writer.commands == []


def test_writer_closed_admission_is_rejected_without_uncertainty() -> None:
    from paritygrid.application.ports.writer import WriterClosedError

    finalizer, writer, _reader, _analytics = _finalizer(_evidence())
    writer.admission_failures[1] = WriterClosedError("closed")
    with pytest.raises(FinalizationAdmissionError, match="admission failed"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)


def test_ticket_identity_generic_failure_is_typed() -> None:
    finalizer, writer, _reader, _analytics = _finalizer(_evidence())

    class _RaisingTicket:
        @property
        def submission_id(self) -> WriterSubmissionId:
            raise RuntimeError("identity unavailable")

        def result(self, *, timeout_seconds: float) -> WriterReceipt:
            raise AssertionError("result must not be reached")

        async def result_async(self, *, timeout_seconds: float) -> WriterReceipt:
            raise AssertionError("result must not be reached")

    writer.ticket_overrides[1] = _RaisingTicket()
    with pytest.raises(FinalizationOutcomeUnknownError, match="ticket identity"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)


def test_empty_node_event_corruption_fails_verification() -> None:
    evidence = FinalizationEvidence(
        _run(),
        EventSequence(9),
        9,
        (_success_node(), _empty_node()),
        (_success_work(),),
        (_success_attempt(),),
        ((WorkItemId("wrk_final-a"), 1),),
    )
    finalizer, writer, _reader, _analytics = _finalizer(evidence)

    def _corrupt_events(receipt: WriterReceipt) -> WriterReceipt:
        result = cast(FinalizeEmptyRunNodeResult, receipt.result)
        events = result.events
        record = events.items[0]
        from paritygrid.application.ports.consistency import RedactedDocument

        corrupted = replace(record, payload=RedactedDocument.from_mapping({"other": "1"}))
        batch = ExecutionEventBatch((corrupted,), events.next_sequence, events.counter_row_version)
        return replace(receipt, result=FinalizeEmptyRunNodeResult(result.node, batch, result.run))

    writer.receipt_mutators[1] = _corrupt_events
    with pytest.raises(FinalizationOutcomeUnknownError):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)


def test_lifecycle_lock_failures_poison_via_fallback() -> None:
    finalizer, _writer, _reader, _analytics = _finalizer(_work_only_evidence())
    cast(Any, finalizer)._writer.result_failures[1] = RuntimeError("result broke")

    class _FlakyLock:
        def __init__(self) -> None:
            self.entered = 0

        def __enter__(self) -> None:
            self.entered += 1
            if self.entered > 1:
                raise RuntimeError("lock unavailable")

        def __exit__(self, *_args: object) -> None:
            return None

    cast(Any, finalizer)._lifecycle_lock = _FlakyLock()
    with pytest.raises(FinalizationOutcomeUnknownError):
        finalizer.finalize(RUN_ID, plan_nodes=(NODE_A,), plan_fingerprint=PLAN_FINGERPRINT)
    assert cast(Any, finalizer)._uncertain is True


def test_fingerprint_covers_nested_configuration_documents() -> None:
    evidence = _evidence()
    configured_run = replace(
        _run(),
        runner_configuration=ConfigurationDocument(
            (
                ("array", DocumentArray((1, "two"))),
                ("nested", NestedDocumentObject((("inner", True),))),
            )
        ),
    )
    object.__setattr__(evidence, "run", configured_run)
    finalizer, _writer, _reader, _analytics = _finalizer(evidence)
    report = finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
    assert report.fingerprint is not None


def test_aggregate_precheck_generic_failure_is_a_protocol_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import paritygrid.application.execution.finalization as finalization_module

    finalizer, writer, _reader, _analytics = _finalizer(_evidence())

    def _explode(*_args: object) -> object:
        raise RuntimeError("snapshot construction broke")

    monkeypatch.setattr(finalization_module, "RunStatisticsSourceSnapshot", _explode)
    with pytest.raises(FinalizationOutcomeUnknownError, match="analytics input is invalid"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
    assert writer.commands == []


def test_evidence_repr_is_bounded() -> None:
    evidence = _evidence()
    assert "FinalizationEvidence(" in repr(evidence)
    assert "final-test" in repr(evidence)


def test_run_without_scenario_seed_finalizes_exactly() -> None:
    evidence = _work_only_evidence()
    object.__setattr__(evidence, "run", replace(_run(), scenario_seed=None))
    finalizer, _writer, _reader, _analytics = _finalizer(evidence)
    report = finalizer.finalize(RUN_ID, plan_nodes=(NODE_A,), plan_fingerprint=PLAN_FINGERPRINT)
    assert report.fingerprint is not None


def test_plan_nodes_rejects_non_node_identities() -> None:
    finalizer, _writer, _reader, _analytics = _finalizer(_evidence())
    with pytest.raises(FinalizationInvalidRequestError, match="plan node identities"):
        finalizer.finalize(
            RUN_ID,
            plan_nodes=(NODE_A, cast(Any, "nod_final-empty")),  # type: ignore[arg-type]
            plan_fingerprint=PLAN_FINGERPRINT,
        )


def test_empty_node_receipt_shape_corruption_fails_verification() -> None:
    evidence = FinalizationEvidence(
        _run(),
        EventSequence(9),
        9,
        (_success_node(), _empty_node()),
        (_success_work(),),
        (_success_attempt(),),
        ((WorkItemId("wrk_final-a"), 1),),
    )

    def _object_receipt(receipt: WriterReceipt) -> WriterReceipt:
        return cast(Any, object())

    def _unmutated_receipt(receipt: WriterReceipt) -> WriterReceipt:
        return replace(receipt, mutated=False)

    for mutator in (_object_receipt, _unmutated_receipt):
        finalizer, writer, _reader, _analytics = _finalizer(evidence)
        writer.receipt_mutators[1] = mutator
        with pytest.raises(FinalizationOutcomeUnknownError):
            finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
