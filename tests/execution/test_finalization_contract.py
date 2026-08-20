# pyright: reportPrivateUsage=false
"""Contract tests for deterministic terminal run finalization."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from paritygrid.application.execution import (
    FinalizationAction,
    FinalizationAnalyticsError,
    FinalizationConflictError,
    FinalizationEvidence,
    FinalizationInvalidRequestError,
    FinalizationNotReadyError,
    FinalizationOutcome,
    FinalizationOutcomeUnknownError,
    FinalizationRejectedError,
    FinalizationSettings,
    FinalizationStateReadError,
    RunFinalizer,
)
from paritygrid.application.planner import PlanFingerprint
from paritygrid.application.ports.configuration import ConfigurationDocument
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
    WriterAdmissionTimeoutError,
    WriterCommand,
    WriterCommandKind,
    WriterDefinitelyNotExecutedError,
    WriterReceipt,
    WriterResultTimeoutError,
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
PIPELINE_ID = PipelineId("pip_final-test")
NODE_A = NodeId("nod_final-a")
NODE_B = NodeId("nod_final-b")
PLAN_NODES = (NODE_A, NODE_B)
PLAN_FINGERPRINT = PlanFingerprint("1" * 64)
CORRELATION = "corr-final-test"


def _time(second: int) -> UtcTimestamp:
    return UtcTimestamp(
        datetime(2025, 2, 1, 0, 0, second % 60, tzinfo=UTC) + timedelta(minutes=second // 60)
    )


def _run(
    *,
    state: RunState = RunState.RUNNING,
    row_version: int = 8,
    fingerprint: StateFingerprint | None = None,
    finished_at: UtcTimestamp | None = None,
) -> RunRecord:
    return RunRecord(
        RUN_ID,
        PIPELINE_ID,
        PipelineVersion(1),
        "sequential",
        ConfigurationDocument(()),
        state,
        row_version,
        42,
        _time(1),
        _time(2),
        finished_at,
        None,
        None,
        None,
        fingerprint,
    )


class _WorkSpec:
    def __init__(self, work_id: WorkItemId, node_id: NodeId, state: WorkItemState) -> None:
        self.work_id = work_id
        self.node_id = node_id
        self.state = state


def _work_record(spec: _WorkSpec) -> WorkItemRecord:
    checkpoint_version = 1 if spec.state is WorkItemState.SUCCEEDED else 0
    return WorkItemRecord(
        spec.work_id,
        RUN_ID,
        spec.node_id,
        PartitionKey(f"part-{spec.work_id.value}"),
        spec.state,
        3,
        1,
        checkpoint_version,
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


def _attempt_record(spec: _WorkSpec) -> WorkAttemptRecord:
    outcome = {
        WorkItemState.SUCCEEDED: AttemptOutcome.SUCCEEDED,
        WorkItemState.QUARANTINED: AttemptOutcome.QUARANTINED,
        WorkItemState.FAILED: AttemptOutcome.FAILED,
        WorkItemState.CANCELLED: AttemptOutcome.CANCELLED,
        WorkItemState.RETRY_WAIT: AttemptOutcome.RETRY_SCHEDULED,
        WorkItemState.RUNNING: AttemptOutcome.SUCCEEDED,
        WorkItemState.PENDING: AttemptOutcome.SUCCEEDED,
    }[spec.state]
    classification = (
        None if spec.state is WorkItemState.SUCCEEDED else FailureClassification.UNKNOWN
    )
    return WorkAttemptRecord(
        spec.work_id,
        AttemptNumber(1),
        _time(4),
        _time(5),
        "sequential",
        "final-worker",
        outcome,
        classification,
        None,
        None,
        1,
        2,
        Duration(1_000_000),
    )


def _node_record(
    node_id: NodeId,
    specs: tuple[_WorkSpec, ...],
    *,
    status: RunNodeStatus | None = None,
) -> RunNodeRecord:
    counts = dict.fromkeys(WorkItemState, 0)
    for spec in specs:
        counts[spec.state] += 1
    succeeded = counts[WorkItemState.SUCCEEDED]
    quarantined = counts[WorkItemState.QUARANTINED]
    failed = counts[WorkItemState.FAILED]
    cancelled = counts[WorkItemState.CANCELLED]
    derived = (
        RunNodeStatus.FAILED
        if failed
        else RunNodeStatus.CANCELLED
        if cancelled and not (succeeded or quarantined)
        else RunNodeStatus.PARTIALLY_SUCCEEDED
        if quarantined or cancelled
        else RunNodeStatus.SUCCEEDED
        if specs
        else RunNodeStatus.PENDING
    )
    selected = derived if status is None else status
    return RunNodeRecord(
        RUN_ID,
        node_id,
        selected,
        2,
        len(specs),
        0,
        0,
        succeeded,
        quarantined,
        failed,
        cancelled,
        len(specs),
        len(specs) * 2,
        quarantined,
        len(specs) * 2,
        len(specs) * 4,
        0,
        Duration(len(specs) * 1_000_000),
        _time(2) if specs else None,
        _time(5) if specs else None,
    )


def _evidence(
    *specs: _WorkSpec,
    node_a_status: RunNodeStatus | None = None,
    run: RunRecord | None = None,
    extra_node: bool = False,
) -> FinalizationEvidence:
    node_a_specs = tuple(spec for spec in specs if spec.node_id is NODE_A)
    node_b_specs = tuple(spec for spec in specs if spec.node_id is NODE_B)
    nodes = [_node_record(NODE_A, node_a_specs, status=node_a_status)]
    if extra_node or node_b_specs:
        nodes.append(_node_record(NODE_B, node_b_specs))
    return FinalizationEvidence(
        run or _run(),
        EventSequence(9),
        9,
        tuple(nodes),
        tuple(_work_record(spec) for spec in specs),
        tuple(_attempt_record(spec) for spec in specs),
        tuple(
            (
                spec.work_id,
                1 if spec.state is WorkItemState.SUCCEEDED else 0,
            )
            for spec in specs
        ),
    )


class _Clock:
    def __init__(self, value: object = None) -> None:
        self.value = _time(20) if value is None else value

    def now(self) -> UtcTimestamp:
        if isinstance(self.value, BaseException):
            raise self.value
        return cast(UtcTimestamp, self.value)


class _Analytics:
    def __init__(self) -> None:
        self.failure: BaseException | None = None
        self.sources: list[RunStatisticsSourceSnapshot] = []

    def rebuild(self, source: RunStatisticsSourceSnapshot) -> object:
        self.sources.append(source)
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
        return RunStatisticsSummary(
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
        return cast(WriterReceipt, self._outcome)

    async def result_async(self, *, timeout_seconds: float) -> WriterReceipt:
        return self.result(timeout_seconds=timeout_seconds)


class _Writer:
    def __init__(self) -> None:
        self.commands: list[WriterCommand] = []
        self.result_failures: dict[int, BaseException] = {}
        self.admission_failures: dict[int, BaseException] = {}
        self.receipt_mutators: dict[int, Any] = {}

    def submit(self, command: WriterCommand, *, timeout_seconds: float) -> _Ticket:
        assert timeout_seconds == 5.0
        index = len(self.commands) + 1
        failure = self.admission_failures.get(index)
        if failure is not None:
            raise failure
        self.commands.append(command)
        submission_id = WriterSubmissionId(index)
        result_failure = self.result_failures.get(index)
        if result_failure is not None:
            return _Ticket(submission_id, result_failure)
        return _Ticket(submission_id, self._receipt(command, submission_id, index))

    def _receipt(
        self, command: WriterCommand, submission_id: WriterSubmissionId, index: int
    ) -> WriterReceipt:
        if isinstance(command, FinalizeEmptyRunNode):
            from paritygrid.domain.models import Duration as _Duration

            node = RunNodeRecord(
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
                _Duration(0),
                command.finalized_at,
                command.finalized_at,
            )
            events = _events(command.event)
            advanced_run = replace(
                _run(),
                row_version=command.expected_run_row_version + 1,
                scenario_seed=42,
            )
            receipt = WriterReceipt(
                submission_id,
                WriterCommandKind.FINALIZE_EMPTY_RUN_NODE,
                command.run_id,
                0,
                True,
                FinalizeEmptyRunNodeResult(node, events, advanced_run),
            )
        else:
            selected = cast(TransitionRun, command)
            previous_fingerprint = selected.final_reconciliation_fingerprint
            run = replace(
                _run(
                    state=selected.target_state,
                    fingerprint=previous_fingerprint,
                    finished_at=selected.transitioned_at,
                ),
                row_version=selected.expected_run_row_version + 1,
                scenario_seed=42,
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
    def __init__(self) -> None:
        self.evidence: FinalizationEvidence | None = None
        self.failure: BaseException | None = None

    def read(self, run_id: RunId, /) -> FinalizationEvidence:
        assert run_id == RUN_ID
        if self.failure is not None:
            raise self.failure
        if self.evidence is None:
            raise AssertionError("reader evidence was not prepared")
        return self.evidence


def _finalizer(
    evidence: FinalizationEvidence | None = None,
    *,
    clock: _Clock | None = None,
) -> tuple[RunFinalizer, _Writer, _Reader, _Analytics, _Clock]:
    writer = _Writer()
    reader = _Reader()
    if evidence is not None:
        reader.evidence = evidence
    analytics = _Analytics()
    selected_clock = clock or _Clock()
    finalizer = RunFinalizer(
        writer,
        reader,
        cast(Any, analytics),
        selected_clock,
        settings=FinalizationSettings(),
    )
    return finalizer, writer, reader, analytics, selected_clock


_SUCCESS_A = _WorkSpec(WorkItemId("wrk_final-a"), NODE_A, WorkItemState.SUCCEEDED)
_QUARANTINED_B = _WorkSpec(WorkItemId("wrk_final-b"), NODE_B, WorkItemState.QUARANTINED)
_FAILED_B = _WorkSpec(WorkItemId("wrk_final-b"), NODE_B, WorkItemState.FAILED)


def test_clean_success_finalizes_with_exact_fingerprint_and_event() -> None:
    finalizer, writer, _reader, analytics, _clock = _finalizer(_evidence(_SUCCESS_A))
    report = finalizer.finalize(
        RUN_ID,
        plan_nodes=PLAN_NODES[:1],
        plan_fingerprint=PLAN_FINGERPRINT,
        correlation_id=CORRELATION,
    )
    assert report.action is FinalizationAction.FINALIZED
    assert report.outcome is FinalizationOutcome.SUCCEEDED
    assert report.run.state is RunState.SUCCEEDED
    assert report.run.finished_at == _time(20)
    assert report.run.row_version == 9
    assert report.fingerprint is not None
    assert report.fingerprint == report.run.final_reconciliation_fingerprint
    assert report.submission_ids == (WriterSubmissionId(1),)
    assert [item.event_kind for item in report.events.items] == ["run_succeeded"]
    assert report.events.items[0].payload.to_mapping() == {
        "final_fingerprint": str(report.fingerprint),
        "from_state": "running",
        "to_state": "succeeded",
    }
    assert len(writer.commands) == 1
    assert len(analytics.sources) == 1


def test_partial_success_with_quarantine_derives_partial_outcome() -> None:
    finalizer, _writer, _reader, _analytics, _clock = _finalizer(
        _evidence(_SUCCESS_A, _QUARANTINED_B)
    )
    report = finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
    assert report.outcome is FinalizationOutcome.PARTIALLY_SUCCEEDED
    assert report.run.state is RunState.PARTIALLY_SUCCEEDED
    assert report.fingerprint is not None
    assert [item.event_kind for item in report.events.items] == ["run_partially_succeeded"]


def test_terminal_failure_derives_failed_without_fingerprint() -> None:
    finalizer, _writer, _reader, _analytics, _clock = _finalizer(_evidence(_SUCCESS_A, _FAILED_B))
    report = finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
    assert report.outcome is FinalizationOutcome.FAILED
    assert report.run.state is RunState.FAILED
    assert report.fingerprint is None
    assert report.run.final_reconciliation_fingerprint is None
    assert [item.event_kind for item in report.events.items] == ["run_failed"]


def test_empty_terminal_graph_finalizes_nodes_and_run() -> None:
    empty = FinalizationEvidence(
        _run(),
        EventSequence(3),
        3,
        (_node_record(NODE_A, ()), _node_record(NODE_B, ())),
        (),
        (),
        (),
    )
    finalizer, writer, _reader, analytics, _clock = _finalizer(empty)
    report = finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
    assert report.outcome is FinalizationOutcome.SUCCEEDED
    assert report.action is FinalizationAction.FINALIZED
    assert len(writer.commands) == 3
    assert [command.kind for command in writer.commands] == [
        WriterCommandKind.FINALIZE_EMPTY_RUN_NODE,
        WriterCommandKind.FINALIZE_EMPTY_RUN_NODE,
        WriterCommandKind.TRANSITION_RUN,
    ]
    assert report.submission_ids == (
        WriterSubmissionId(1),
        WriterSubmissionId(2),
        WriterSubmissionId(3),
    )
    assert len(analytics.sources) == 1


@pytest.mark.parametrize(
    "state",
    [WorkItemState.PENDING, WorkItemState.RETRY_WAIT, WorkItemState.RUNNING],
)
def test_non_terminal_work_rejects_finalization(state: WorkItemState) -> None:
    spec = _WorkSpec(WorkItemId("wrk_final-x"), NODE_A, state)
    finalizer, writer, _reader, analytics, _clock = _finalizer(_evidence(spec))
    with pytest.raises(FinalizationNotReadyError, match="non-terminal work"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)
    assert writer.commands == []
    assert analytics.sources == []


def test_active_node_status_rejects_finalization() -> None:
    finalizer, _writer, _reader, _analytics, _clock = _finalizer(
        _evidence(_SUCCESS_A, node_a_status=RunNodeStatus.RUNNING)
    )
    with pytest.raises(FinalizationNotReadyError, match="non-terminal node"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)


def test_checkpoint_frontier_mismatch_is_a_typed_conflict() -> None:
    evidence = _evidence(_SUCCESS_A)
    corrupted = FinalizationEvidence(
        evidence.run,
        evidence.next_event_sequence,
        evidence.event_counter_row_version,
        evidence.nodes,
        evidence.work,
        evidence.attempts,
        ((evidence.work[0].work_item_id, 0),),
    )
    finalizer, writer, _reader, _analytics, _clock = _finalizer(corrupted)
    with pytest.raises(FinalizationConflictError, match="frontier diverges"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)
    assert writer.commands == []


def test_missing_checkpoint_evidence_is_a_typed_conflict() -> None:
    evidence = _evidence(_SUCCESS_A)
    finalizer, _writer, _reader, _analytics, _clock = _finalizer(
        replace(evidence, checkpoint_versions=())
    )
    with pytest.raises(FinalizationConflictError, match="missing durable evidence"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)


def test_aggregate_mismatch_is_a_typed_conflict() -> None:
    evidence = _evidence(_SUCCESS_A)
    drifted_node = replace(evidence.nodes[0], work_succeeded=0)
    finalizer, _writer, _reader, _analytics, _clock = _finalizer(
        replace(evidence, nodes=(drifted_node,))
    )
    with pytest.raises(FinalizationConflictError, match="aggregates diverge"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)


def test_plan_node_mismatch_is_a_typed_conflict() -> None:
    finalizer, _writer, _reader, _analytics, _clock = _finalizer(_evidence(_SUCCESS_A))
    with pytest.raises(FinalizationConflictError, match="captured plan"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)


def test_analytics_failure_leaves_no_mutation_and_is_typed() -> None:
    finalizer, writer, reader, analytics, _clock = _finalizer(_evidence(_SUCCESS_A))
    analytics.failure = RuntimeError("credential=secret C:\\machine")
    with pytest.raises(FinalizationAnalyticsError) as captured:
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)
    assert "secret" not in str(captured.value)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert writer.commands == []
    del reader


def test_empty_graph_analytics_failure_leaves_no_mutation() -> None:
    empty = FinalizationEvidence(
        _run(),
        EventSequence(3),
        3,
        (_node_record(NODE_A, ()), _node_record(NODE_B, ())),
        (),
        (),
        (),
    )
    finalizer, writer, _reader, analytics, _clock = _finalizer(empty)
    analytics.failure = RuntimeError("analytics unavailable")

    with pytest.raises(FinalizationAnalyticsError):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)

    assert writer.commands == []


def test_exact_replay_is_read_only() -> None:
    finalizer, _writer, _reader, _analytics, _clock = _finalizer(_evidence(_SUCCESS_A))
    first = finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)
    replay_evidence = _evidence(
        _SUCCESS_A,
        run=_run(
            state=RunState.SUCCEEDED,
            row_version=9,
            fingerprint=first.fingerprint,
            finished_at=_time(20),
        ),
    )
    replay_finalizer, replay_writer, _replay_reader, replay_analytics, _clock2 = _finalizer(
        replay_evidence
    )
    replay = replay_finalizer.finalize(
        RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT
    )
    assert replay.action is FinalizationAction.ALREADY_FINALIZED
    assert replay.outcome is FinalizationOutcome.SUCCEEDED
    assert replay.fingerprint == first.fingerprint
    assert replay.events.items == ()
    assert replay.submission_ids == ()
    assert replay_writer.commands == []
    assert len(replay_analytics.sources) == 1


def test_divergent_replay_is_a_typed_conflict() -> None:
    evidence = _evidence(
        _SUCCESS_A,
        run=_run(
            state=RunState.SUCCEEDED,
            row_version=9,
            fingerprint=StateFingerprint("2" * 64),
            finished_at=_time(20),
        ),
    )
    finalizer, writer, _reader, _analytics, _clock = _finalizer(evidence)
    with pytest.raises(FinalizationConflictError, match="diverges"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)
    assert writer.commands == []


def test_cancelled_and_failed_replays_are_read_only() -> None:
    cancelled = _evidence(
        _WorkSpec(WorkItemId("wrk_final-c"), NODE_A, WorkItemState.CANCELLED),
        run=_run(state=RunState.CANCELLED, finished_at=_time(19)),
    )
    finalizer, writer, _reader, analytics, _clock = _finalizer(cancelled)
    report = finalizer.finalize(
        RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT
    )
    assert report.action is FinalizationAction.ALREADY_FINALIZED
    assert report.outcome is FinalizationOutcome.CANCELLED
    assert writer.commands == []
    assert analytics.sources == []

    failed = _evidence(
        _FAILED_B,
        node_a_status=RunNodeStatus.SUCCEEDED,
        run=_run(state=RunState.FAILED, finished_at=_time(19)),
    )
    failed_finalizer, failed_writer, _failed_reader, _failed_analytics, _clock2 = _finalizer(failed)
    failed_report = failed_finalizer.finalize(
        RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT
    )
    assert failed_report.action is FinalizationAction.ALREADY_FINALIZED
    assert failed_report.outcome is FinalizationOutcome.FAILED
    assert failed_report.fingerprint is None
    assert failed_writer.commands == []


def test_failed_replay_revalidates_nodes_checkpoints_and_aggregates() -> None:
    active_node = _evidence(
        _FAILED_B,
        run=_run(state=RunState.FAILED, finished_at=_time(19)),
    )
    active_finalizer = _finalizer(active_node)[0]
    with pytest.raises(FinalizationNotReadyError, match="non-terminal node"):
        active_finalizer.finalize(
            RUN_ID,
            plan_nodes=PLAN_NODES,
            plan_fingerprint=PLAN_FINGERPRINT,
        )

    complete = _evidence(
        _SUCCESS_A,
        _FAILED_B,
        run=_run(state=RunState.FAILED, finished_at=_time(19)),
    )
    missing_checkpoint = replace(
        complete,
        checkpoint_versions=((_FAILED_B.work_id, 0),),
    )
    checkpoint_finalizer = _finalizer(missing_checkpoint)[0]
    with pytest.raises(FinalizationConflictError, match="missing durable evidence"):
        checkpoint_finalizer.finalize(
            RUN_ID,
            plan_nodes=PLAN_NODES,
            plan_fingerprint=PLAN_FINGERPRINT,
        )

    drifted = replace(
        complete,
        nodes=(replace(complete.nodes[0], work_succeeded=0), complete.nodes[1]),
    )
    aggregate_finalizer = _finalizer(drifted)[0]
    with pytest.raises(FinalizationConflictError, match="aggregates diverge"):
        aggregate_finalizer.finalize(
            RUN_ID,
            plan_nodes=PLAN_NODES,
            plan_fingerprint=PLAN_FINGERPRINT,
        )


def test_cancelled_replay_rejects_a_stored_final_fingerprint() -> None:
    evidence = _evidence(
        _WorkSpec(WorkItemId("wrk_final-c"), NODE_A, WorkItemState.CANCELLED),
        run=_run(
            state=RunState.CANCELLED,
            fingerprint=StateFingerprint("3" * 64),
            finished_at=_time(19),
        ),
    )
    finalizer = _finalizer(evidence)[0]

    with pytest.raises(FinalizationConflictError, match="must not store"):
        finalizer.finalize(
            RUN_ID,
            plan_nodes=PLAN_NODES[:1],
            plan_fingerprint=PLAN_FINGERPRINT,
        )


def test_cancellation_pause_and_queued_states_are_rejected() -> None:
    cancelling = _finalizer(
        _evidence(
            _WorkSpec(WorkItemId("wrk_final-c"), NODE_A, WorkItemState.CANCELLED),
            run=_run(state=RunState.CANCELLING),
        )
    )[0]
    with pytest.raises(FinalizationNotReadyError, match="cancellation must complete"):
        cancelling.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)
    paused = _finalizer(_evidence(_SUCCESS_A, run=_run(state=RunState.PAUSED)))[0]
    with pytest.raises(FinalizationInvalidRequestError, match="pause lifecycle"):
        paused.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)
    queued = _finalizer(_evidence(_SUCCESS_A, run=_run(state=RunState.QUEUED)))[0]
    with pytest.raises(FinalizationInvalidRequestError, match="not started"):
        queued.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)


def test_deterministic_fingerprint_across_logical_ordering_variations() -> None:
    first, _writer, _reader, _analytics, _clock = _finalizer(_evidence(_SUCCESS_A, _QUARANTINED_B))
    report = first.finalize(RUN_ID, plan_nodes=PLAN_NODES, plan_fingerprint=PLAN_FINGERPRINT)
    reordered_evidence = FinalizationEvidence(
        _run(),
        EventSequence(9),
        9,
        tuple(reversed(_evidence(_SUCCESS_A, _QUARANTINED_B).nodes)),
        tuple(reversed(_evidence(_SUCCESS_A, _QUARANTINED_B).work)),
        tuple(reversed(_evidence(_SUCCESS_A, _QUARANTINED_B).attempts)),
        tuple(reversed(_evidence(_SUCCESS_A, _QUARANTINED_B).checkpoint_versions)),
    )
    second, _writer2, _reader2, _analytics2, _clock2 = _finalizer(reordered_evidence)
    reordered = second.finalize(
        RUN_ID, plan_nodes=(NODE_B, NODE_A), plan_fingerprint=PLAN_FINGERPRINT
    )
    assert reordered.outcome is FinalizationOutcome.PARTIALLY_SUCCEEDED
    assert reordered.fingerprint == report.fingerprint


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (WriterAdmissionTimeoutError(), FinalizationRejectedError.__mro__[1]),
        (WriterDefinitelyNotExecutedError(), FinalizationRejectedError),
        (WriterResultTimeoutError(), FinalizationOutcomeUnknownError),
    ],
)
def test_writer_failure_classification(
    failure: BaseException,
    expected: type[BaseException],
) -> None:
    finalizer, writer, _reader, _analytics, _clock = _finalizer(_evidence(_SUCCESS_A))
    if isinstance(failure, WriterAdmissionTimeoutError):
        writer.admission_failures[1] = failure
        with pytest.raises(FinalizationRejectedError.__mro__[3], match="admission"):
            finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)
    else:
        writer.result_failures[1] = failure
        with pytest.raises(expected):
            finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)


def test_stale_run_row_version_is_rejected_and_retryable() -> None:
    finalizer, writer, _reader, _analytics, _clock = _finalizer(_evidence(_SUCCESS_A))
    writer.result_failures[1] = WriterDefinitelyNotExecutedError()
    with pytest.raises(FinalizationRejectedError):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)
    writer.result_failures.clear()
    report = finalizer.finalize(
        RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT
    )
    assert report.action is FinalizationAction.FINALIZED


def test_final_verification_failure_fails_closed() -> None:
    finalizer, writer, _reader, _analytics, _clock = _finalizer(_evidence(_SUCCESS_A))

    def _unmutated(receipt: WriterReceipt) -> WriterReceipt:
        return replace(receipt, mutated=False)

    writer.receipt_mutators[1] = _unmutated
    with pytest.raises(FinalizationOutcomeUnknownError):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)
    with pytest.raises(FinalizationOutcomeUnknownError, match="recovery inspection"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)


def test_base_exceptions_propagate_and_poison() -> None:
    finalizer, writer, _reader, _analytics, _clock = _finalizer(_evidence(_SUCCESS_A))
    fatal = KeyboardInterrupt("interrupted")
    writer.result_failures[1] = fatal
    with pytest.raises(KeyboardInterrupt) as captured:
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)
    assert captured.value is fatal
    with pytest.raises(FinalizationOutcomeUnknownError, match="recovery inspection"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)


def test_state_read_failures_are_typed_and_redacted() -> None:
    finalizer, _writer, reader, _analytics, _clock = _finalizer(_evidence(_SUCCESS_A))
    reader.failure = RuntimeError("credential=secret C:\\machine")
    with pytest.raises(FinalizationStateReadError, match="read failed"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)
    reader.failure = None
    reader.evidence = cast(Any, object())
    with pytest.raises(FinalizationOutcomeUnknownError, match="frontier is invalid"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)


def test_corrupt_frontier_read_is_a_typed_conflict() -> None:
    from paritygrid.application.ports.consistency import ConsistencyCorruptionError

    finalizer, _writer, reader, _analytics, _clock = _finalizer(_evidence(_SUCCESS_A))
    reader.failure = ConsistencyCorruptionError("event history is not contiguous")
    with pytest.raises(FinalizationConflictError, match="corrupt"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)


def test_clock_failures_are_typed() -> None:
    clock = _Clock()
    finalizer, _writer, _reader, _analytics, _clock3 = _finalizer(
        _evidence(_SUCCESS_A), clock=clock
    )
    clock.value = RuntimeError("clock broke")
    with pytest.raises(Exception, match="clock failed"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)
    clock.value = _time(1)
    with pytest.raises(Exception, match="behind durable"):
        finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)


def test_request_and_correlation_validation() -> None:
    finalizer, _writer, _reader, _analytics, _clock = _finalizer(_evidence(_SUCCESS_A))
    with pytest.raises(FinalizationInvalidRequestError, match="plan nodes"):
        finalizer.finalize(RUN_ID, plan_nodes=(), plan_fingerprint=PLAN_FINGERPRINT)
    with pytest.raises(FinalizationInvalidRequestError, match="unique"):
        finalizer.finalize(
            RUN_ID,
            plan_nodes=(NODE_A, NODE_A),  # type: ignore[arg-type]
            plan_fingerprint=PLAN_FINGERPRINT,
        )
    with pytest.raises(FinalizationInvalidRequestError, match="plan fingerprint"):
        finalizer.finalize(
            RUN_ID,
            plan_nodes=PLAN_NODES[:1],
            plan_fingerprint=cast(Any, "fingerprint"),
        )
    with pytest.raises(FinalizationInvalidRequestError, match="correlation"):
        finalizer.finalize(
            RUN_ID,
            plan_nodes=PLAN_NODES[:1],
            plan_fingerprint=PLAN_FINGERPRINT,
            correlation_id="not portable!",
        )


def test_overlapping_operations_are_rejected() -> None:
    finalizer, _writer, _reader, _analytics, _clock = _finalizer(_evidence(_SUCCESS_A))
    operation_lock = cast(Any, finalizer)._operation_lock
    operation_lock.acquire()
    try:
        with pytest.raises(Exception, match="active operation"):
            finalizer.finalize(RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT)
    finally:
        operation_lock.release()


def test_constructor_validates_collaborators() -> None:
    writer = _Writer()
    reader = _Reader()
    analytics = _Analytics()
    clock = _Clock()
    with pytest.raises(TypeError, match="writer"):
        RunFinalizer(object(), reader, analytics, clock)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="reader"):
        RunFinalizer(writer, object(), analytics, clock)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="analytics"):
        RunFinalizer(writer, reader, object(), clock)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="clock"):
        RunFinalizer(writer, reader, analytics, object())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="settings"):
        RunFinalizer(writer, reader, analytics, clock, settings=object())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="float"):
        FinalizationSettings(admission_timeout_seconds=1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="supported range"):
        FinalizationSettings(result_timeout_seconds=86_400.5)


def test_finalization_report_repr_is_bounded() -> None:
    finalizer, _writer, _reader, _analytics, _clock = _finalizer(_evidence(_SUCCESS_A))
    report = finalizer.finalize(
        RUN_ID, plan_nodes=PLAN_NODES[:1], plan_fingerprint=PLAN_FINGERPRINT
    )
    assert "FinalizationReport(" in repr(report)
    assert "final-test" in repr(report)
