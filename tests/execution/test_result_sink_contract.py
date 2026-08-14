"""Contract and adversarial tests for closed worker results and result sinks."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from traceback import format_exception
from typing import Any, cast

import pytest

from paritygrid.application.execution import (
    MAX_RESULT_CHECKPOINT_SCHEMA_VERSION,
    RESULT_SINK_SCHEMA_VERSION,
    AcquireWorkLeaseRequest,
    AttemptCancelled,
    AttemptEventContext,
    AttemptFailed,
    AttemptSucceeded,
    Http429RetryDelay,
    RedactedAttemptDetail,
    ResultCheckpoint,
    ResultMetrics,
    ResultRejectionReason,
    ResultSink,
    ResultSinkAdmissionError,
    ResultSinkCommitted,
    ResultSinkError,
    ResultSinkInvalidResultError,
    ResultSinkOutcome,
    ResultSinkOutcomeKind,
    ResultSinkOutcomeUnknownError,
    ResultSinkProtocolError,
    ResultSinkRejected,
    ResultSubmission,
    RetryPolicyName,
    RetryScheduledDecision,
    RetryStoppedDecision,
    SuccessfulWorkResult,
    UnsuccessfulWorkResult,
    WorkLease,
    WorkLeaseBusyError,
    WorkLeaseCompletionDisposition,
    WorkLeaseCompletionReservation,
    WorkLeaseOwnershipError,
    WorkLeaseService,
    WorkResult,
    WorkResultKind,
    snapshot_result_submission,
    snapshot_work_result,
    submit_work_result,
)
from paritygrid.application.planner import PlannerRunnerKind
from paritygrid.application.ports import ResultSink as PortResultSink
from paritygrid.application.ports.artifacts import (
    ArtifactManifestRecord,
    ArtifactRelativePath,
    ArtifactWriteReceipt,
)
from paritygrid.application.ports.configuration import (
    ConfigurationDocument,
    DocumentArray,
    NestedDocumentObject,
)
from paritygrid.application.ports.consistency import (
    CheckpointVersion,
    EventSequence,
    EventSubjectKind,
    ExecutionEventBatch,
    ExecutionEventRecord,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.execution import (
    RunNodeRecord,
    RunNodeStatus,
    RunRecord,
    WorkClaim,
)
from paritygrid.application.ports.run_aggregates import MAX_WORK_METRIC, WorkMetricDelta
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    WriterCommand,
    WriterReceipt,
    WriterSubmissionId,
)
from paritygrid.application.writes import ClaimWork, ClaimWorkResult
from paritygrid.domain.execution import (
    FailureClassification,
    FailureDisposition,
    RunState,
    WorkItemState,
    disposition_for,
)
from paritygrid.domain.models import (
    ArtifactId,
    AttemptNumber,
    Duration,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

RUN_ID = RunId("run_result-sink")
NODE_ID = NodeId("nod_result-sink")
WORK_ID = WorkItemId("wrk_result-sink")
PIPELINE_ID = PipelineId("pip_result-sink")
PARTITION = PartitionKey("partition-00000000")
ARTIFACT_ID = ArtifactId("art_result-sink")
_BASE = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
ATTEMPT_ONE = AttemptNumber(1)


class _Fatal(BaseException):
    pass


class _InterruptingAddSet(set[WorkItemId]):
    def __init__(self, failure: BaseException) -> None:
        super().__init__()
        self.failure = failure

    def add(self, element: WorkItemId) -> None:
        super().add(element)
        raise self.failure

    def discard(self, element: object) -> None:
        super().discard(cast(WorkItemId, element))
        raise self.failure


class _StubbornInterruptingAddSet(_InterruptingAddSet):
    def discard(self, element: object) -> None:
        raise self.failure


class _ExplodingContainsAddSet(_InterruptingAddSet):
    def __init__(self, failure: BaseException) -> None:
        super().__init__(failure)
        self.interrupted = False

    def add(self, element: WorkItemId) -> None:
        super(_InterruptingAddSet, self).add(element)
        self.interrupted = True
        raise self.failure

    def __contains__(self, element: object) -> bool:
        if self.interrupted:
            raise self.failure
        return super().__contains__(element)


class _UnclearableAddSet(_ExplodingContainsAddSet):
    def __iter__(self) -> Iterator[WorkItemId]:
        if self.interrupted:
            raise self.failure
        return super().__iter__()


class _InterruptingRemoveSet(set[WorkItemId]):
    def __init__(self, values: set[WorkItemId], failure: BaseException) -> None:
        super().__init__(values)
        self.failure = failure

    def remove(self, element: WorkItemId) -> None:
        super().remove(element)
        raise self.failure


class _InterruptingDeleteDict(dict[Any, Any]):
    def __init__(self, values: dict[Any, Any], failure: BaseException) -> None:
        super().__init__(values)
        self.failure = failure

    def __delitem__(self, key: Any) -> None:
        super().__delitem__(key)
        raise self.failure


class _InterruptingGetDict(dict[Any, Any]):
    def __init__(self, values: dict[Any, Any], failure: BaseException) -> None:
        super().__init__(values)
        self.failure = failure

    def get(self, key: Any, default: Any = None) -> Any:
        raise self.failure


def _timestamp(second: int) -> UtcTimestamp:
    return UtcTimestamp(_BASE + timedelta(seconds=second))


def _document(**values: object) -> ConfigurationDocument:
    return ConfigurationDocument.from_mapping(values)


def _event(sequence: int) -> EventAppendRequest:
    pending = PendingExecutionEvent(
        event_kind="work_claim_requested",
        occurred_at=_timestamp(3),
        subject_kind=EventSubjectKind.WORK_ITEM,
        subject_id=WORK_ID,
        correlation_id="corr-result-sink",
        payload_schema_version=1,
        payload=RedactedDocument.from_mapping({"kind": "claim"}),
    )
    return EventAppendRequest(EventSequence(sequence), sequence, pending)


def _event_batch(request: EventAppendRequest) -> ExecutionEventBatch:
    pending = request.event
    record = ExecutionEventRecord(
        run_id=RUN_ID,
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
        runner_kind=PlannerRunnerKind.SEQUENTIAL.value,
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
        attempt_number=command.expected_attempt_number,
        lease_owner=command.lease_owner,
        row_version=command.expected_work_row_version + 1,
        started_at=command.started_at,
        lease_expires_at=command.lease_expires_at,
        runner_kind=command.runner_kind,
        worker_identity=command.worker_identity,
    )


class _Ticket:
    def __init__(self, receipt: WriterReceipt) -> None:
        self._receipt = receipt

    @property
    def submission_id(self) -> WriterSubmissionId:
        return self._receipt.submission_id

    def result(self, *, timeout_seconds: float) -> WriterReceipt:
        assert timeout_seconds > 0
        return self._receipt

    async def result_async(self, *, timeout_seconds: float) -> WriterReceipt:
        return self.result(timeout_seconds=timeout_seconds)


class _Writer:
    def __init__(self) -> None:
        self.commands: list[WriterCommand] = []

    def submit(self, command: WriterCommand, *, timeout_seconds: float) -> _Ticket:
        assert timeout_seconds > 0
        self.commands.append(command)
        assert type(command) is ClaimWork
        submission_id = WriterSubmissionId(len(self.commands))
        result = ClaimWorkResult(
            _claim(command),
            _node(command.expected_node_row_version + 1),
            _event_batch(command.event),
            _run(command.expected_run_row_version + 1),
        )
        return _Ticket(WriterReceipt(submission_id, command.kind, command.run_id, 0, True, result))


class _Clock:
    def now(self) -> UtcTimestamp:
        return _timestamp(3)


_LEASE_SERVICES: dict[int, WorkLeaseService] = {}


def _issued_lease() -> WorkLease:
    service = WorkLeaseService(_Writer(), _Clock())
    lease = service.acquire(
        AcquireWorkLeaseRequest(
            run_id=RUN_ID,
            node_id=NODE_ID,
            work_item_id=WORK_ID,
            expected_attempt_number=AttemptNumber(1),
            expected_work_row_version=1,
            expected_node_row_version=1,
            expected_run_row_version=1,
            lease_owner="result-sink-owner",
            runner_kind=PlannerRunnerKind.SEQUENTIAL.value,
            worker_identity="result-sink-worker",
            event=_event(1),
        )
    )
    _LEASE_SERVICES[id(lease)] = service
    return lease


def _service_for(lease: WorkLease) -> WorkLeaseService:
    return _LEASE_SERVICES[id(lease)]


def _context(
    lease: WorkLease,
    *,
    run_id: RunId = RUN_ID,
    node_id: NodeId = NODE_ID,
    work_item_id: WorkItemId = WORK_ID,
    attempt_number: AttemptNumber = ATTEMPT_ONE,
    started_at: UtcTimestamp | None = None,
    runner_kind: PlannerRunnerKind = PlannerRunnerKind.SEQUENTIAL,
    worker_identity: str = "result-sink-worker",
    correlation_id: str | None = "corr-result-sink",
) -> AttemptEventContext:
    return AttemptEventContext(
        run_id,
        node_id,
        work_item_id,
        attempt_number,
        lease.claim.started_at if started_at is None else started_at,
        runner_kind,
        worker_identity,
        correlation_id,
    )


def _manifest(
    *,
    run_id: RunId = RUN_ID,
    node_id: NodeId = NODE_ID,
    partition_key: PartitionKey = PARTITION,
    created_at: UtcTimestamp | None = None,
) -> ArtifactManifestRecord:
    return ArtifactManifestRecord(
        artifact_id=ARTIFACT_ID,
        run_id=run_id,
        node_id=node_id,
        partition_key=partition_key,
        relative_path=ArtifactRelativePath("result-sink/output.parquet"),
        media_type="application/vnd.apache.parquet",
        schema_version=1,
        byte_size=12,
        row_count=2,
        sha256="a" * 64,
        created_at=_timestamp(4) if created_at is None else created_at,
    )


def _metrics() -> ResultMetrics:
    return ResultMetrics(
        2,
        12,
        WorkMetricDelta(
            records_read=2,
            records_written=2,
            records_quarantined=0,
            bytes_read=11,
            bytes_written=12,
        ),
    )


def _checkpoint(artifact: ArtifactManifestRecord | None = None) -> ResultCheckpoint:
    return ResultCheckpoint(
        PARTITION,
        1,
        _document(offset=2),
        _document(rows=2),
        artifact,
    )


def _success(
    lease: WorkLease, *, artifact: ArtifactManifestRecord | None = None
) -> SuccessfulWorkResult:
    return SuccessfulWorkResult(
        AttemptSucceeded(_context(lease), _timestamp(5)),
        _checkpoint(artifact),
        _metrics(),
    )


def _stopped(
    lease: WorkLease,
    classification: FailureClassification,
    disposition: FailureDisposition,
    *,
    attempt_number: AttemptNumber = ATTEMPT_ONE,
    exhausted: bool = False,
) -> RetryStoppedDecision:
    return RetryStoppedDecision(
        RetryPolicyName.BOUNDED_EXPONENTIAL_V1,
        lease.claim.work_item_id,
        attempt_number,
        classification,
        _timestamp(5),
        disposition,
        exhausted,
    )


def _scheduled(lease: WorkLease) -> RetryScheduledDecision:
    return RetryScheduledDecision(
        RetryPolicyName.BOUNDED_EXPONENTIAL_V1,
        lease.claim.work_item_id,
        lease.claim.attempt_number,
        FailureClassification.TIMEOUT,
        _timestamp(5),
        _timestamp(5),
        None,
        Duration(0),
        Duration(1_000_000),
        _timestamp(6),
    )


def _failure(
    lease: WorkLease,
    classification: FailureClassification,
    decision: RetryScheduledDecision | RetryStoppedDecision,
    *,
    detail: RedactedAttemptDetail | None = None,
) -> UnsuccessfulWorkResult:
    return UnsuccessfulWorkResult(
        AttemptFailed(_context(lease), _timestamp(5), classification, detail),
        decision,
        _metrics(),
    )


def _cancelled(lease: WorkLease) -> UnsuccessfulWorkResult:
    return UnsuccessfulWorkResult(
        AttemptCancelled(
            _context(lease, correlation_id=None),
            _timestamp(5),
            RedactedAttemptDetail("cancelled by user"),
        ),
        None,
        _metrics(),
    )


def _outcome_for(
    result: WorkResult,
    *,
    committed: bool = True,
    reason: ResultRejectionReason = ResultRejectionReason.STALE_CAPABILITY,
) -> ResultSinkOutcome:
    context = result.terminal.context
    if committed:
        return ResultSinkCommitted(
            WriterSubmissionId(7),
            result.kind,
            context.run_id,
            context.node_id,
            context.work_item_id,
            context.attempt_number,
            CheckpointVersion(1) if result.kind is WorkResultKind.SUCCEEDED else None,
        )
    return ResultSinkRejected(
        WriterSubmissionId(7),
        result.kind,
        context.run_id,
        context.node_id,
        context.work_item_id,
        context.attempt_number,
        reason,
    )


class _Sink:
    def __init__(
        self,
        outcome: ResultSinkOutcome | BaseException | Callable[[ResultSubmission], object],
    ) -> None:
        self.outcome = outcome
        self.submissions: list[ResultSubmission] = []

    def submit(self, submission: ResultSubmission, /) -> ResultSinkOutcome:
        self.submissions.append(submission)
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        if callable(self.outcome):
            return cast(ResultSinkOutcome, self.outcome(submission))
        return self.outcome


def test_public_contract_exports_constants_and_closed_axes() -> None:
    assert ResultSink is PortResultSink
    assert RESULT_SINK_SCHEMA_VERSION == 1
    assert MAX_RESULT_CHECKPOINT_SCHEMA_VERSION == 2_147_483_647
    assert tuple(WorkResultKind) == (
        WorkResultKind.SUCCEEDED,
        WorkResultKind.RETRY_WAIT,
        WorkResultKind.QUARANTINED,
        WorkResultKind.FAILED,
        WorkResultKind.CANCELLED,
    )
    assert tuple(ResultSinkOutcomeKind) == (
        ResultSinkOutcomeKind.COMMITTED,
        ResultSinkOutcomeKind.REJECTED,
    )
    assert tuple(ResultRejectionReason) == (
        ResultRejectionReason.STALE_CAPABILITY,
        ResultRejectionReason.STATE_CONFLICT,
        ResultRejectionReason.DEFINITELY_NOT_EXECUTED,
    )
    assert issubclass(ResultSinkProtocolError, ResultSinkOutcomeUnknownError)


def test_successful_result_supports_exact_manifest_and_artifact_free_checkpoint() -> None:
    lease = _issued_lease()
    with_artifact = _success(lease, artifact=_manifest())
    without_artifact = _success(lease)

    assert with_artifact.kind is WorkResultKind.SUCCEEDED
    assert with_artifact.checkpoint.artifact == _manifest()
    assert with_artifact.checkpoint.artifact is not _manifest()
    assert without_artifact.checkpoint.artifact is None
    assert with_artifact.metrics == _metrics()
    assert "output.parquet" not in repr(with_artifact)
    assert "a" * 64 not in repr(with_artifact)
    assert "offset" not in repr(with_artifact.checkpoint)
    assert repr(with_artifact) == (
        "SuccessfulWorkResult("
        "work_item_id=WorkItemId(value='wrk_result-sink'), "
        "attempt_number=AttemptNumber(number=1), has_artifact=True, "
        "schema_version=1, payload=<redacted>)"
    )


_ResultFactory = Callable[[WorkLease], UnsuccessfulWorkResult]
_RESULT_MATRIX: tuple[
    tuple[_ResultFactory, WorkResultKind, WorkItemState, FailureDisposition], ...
] = (
    (
        lambda lease: _failure(
            lease,
            FailureClassification.TIMEOUT,
            _scheduled(lease),
        ),
        WorkResultKind.RETRY_WAIT,
        WorkItemState.RETRY_WAIT,
        FailureDisposition.RETRY,
    ),
    (
        lambda lease: _failure(
            lease,
            FailureClassification.VALIDATION,
            _stopped(
                lease,
                FailureClassification.VALIDATION,
                FailureDisposition.QUARANTINE,
            ),
        ),
        WorkResultKind.QUARANTINED,
        WorkItemState.QUARANTINED,
        FailureDisposition.QUARANTINE,
    ),
    (
        lambda lease: _failure(
            lease,
            FailureClassification.IDEMPOTENCY_CONFLICT,
            _stopped(
                lease,
                FailureClassification.IDEMPOTENCY_CONFLICT,
                FailureDisposition.CONFLICT,
            ),
        ),
        WorkResultKind.FAILED,
        WorkItemState.FAILED,
        FailureDisposition.CONFLICT,
    ),
    (
        lambda lease: _failure(
            lease,
            FailureClassification.HTTP_4XX,
            _stopped(
                lease,
                FailureClassification.HTTP_4XX,
                FailureDisposition.PERMANENT,
            ),
        ),
        WorkResultKind.FAILED,
        WorkItemState.FAILED,
        FailureDisposition.PERMANENT,
    ),
    (
        _cancelled,
        WorkResultKind.CANCELLED,
        WorkItemState.CANCELLED,
        FailureDisposition.CANCEL,
    ),
)


@pytest.mark.parametrize(
    ("result_factory", "kind", "target", "disposition"),
    _RESULT_MATRIX,
)
def test_unsuccessful_result_matrix_is_exhaustive_and_separate_from_sink_rejection(
    result_factory: Callable[[WorkLease], UnsuccessfulWorkResult],
    kind: WorkResultKind,
    target: WorkItemState,
    disposition: FailureDisposition,
) -> None:
    result = result_factory(_issued_lease())
    assert result.kind is kind
    assert result.target_state is target
    assert result.disposition is disposition
    assert "payload=<redacted>" in repr(result)


def test_exhausted_retry_becomes_failed_without_reusing_policy_arithmetic() -> None:
    lease = _issued_lease()
    context = _context(lease, attempt_number=AttemptNumber(3))
    decision = _stopped(
        lease,
        FailureClassification.TIMEOUT,
        FailureDisposition.PERMANENT,
        attempt_number=AttemptNumber(3),
        exhausted=True,
    )
    result = UnsuccessfulWorkResult(
        AttemptFailed(context, _timestamp(5), FailureClassification.TIMEOUT),
        decision,
        _metrics(),
    )
    assert result.kind is WorkResultKind.FAILED


def test_rejected_result_requires_exact_matching_retry_evidence() -> None:
    lease = _issued_lease()
    terminal = AttemptFailed(
        _context(lease),
        _timestamp(5),
        FailureClassification.VALIDATION,
    )
    valid = _stopped(
        lease,
        FailureClassification.VALIDATION,
        FailureDisposition.QUARANTINE,
    )
    mismatches = [
        None,
        replace(valid, work_item_id=WorkItemId("wrk_other-result")),
        replace(valid, attempt_number=AttemptNumber(2)),
        _stopped(
            lease,
            FailureClassification.HTTP_4XX,
            FailureDisposition.PERMANENT,
        ),
        replace(valid, failed_at=_timestamp(4)),
    ]
    for decision in mismatches:
        with pytest.raises(ResultSinkInvalidResultError, match=r"does not match|requires"):
            UnsuccessfulWorkResult(terminal, decision, _metrics())

    with pytest.raises(ResultSinkInvalidResultError, match="cancelled"):
        UnsuccessfulWorkResult(
            AttemptCancelled(_context(lease), _timestamp(5)),
            valid,
            _metrics(),
        )


@pytest.mark.parametrize(
    ("run_id", "node_id", "created_at", "message"),
    [
        (RunId("run_other-result"), NODE_ID, _timestamp(4), "parent"),
        (RUN_ID, NodeId("nod_other-result"), _timestamp(4), "parent"),
        (RUN_ID, NODE_ID, _timestamp(2), "interval"),
        (RUN_ID, NODE_ID, _timestamp(6), "interval"),
    ],
)
def test_result_artifact_is_exact_verified_parent_evidence(
    run_id: RunId,
    node_id: NodeId,
    created_at: UtcTimestamp,
    message: str,
) -> None:
    lease = _issued_lease()
    with pytest.raises(ResultSinkInvalidResultError, match=message):
        _success(
            lease,
            artifact=_manifest(run_id=run_id, node_id=node_id, created_at=created_at),
        )

    with pytest.raises(TypeError, match="ArtifactManifestRecord"):
        replace(
            _checkpoint(),
            artifact=cast(
                Any,
                ArtifactWriteReceipt(
                    ArtifactRelativePath("result-sink/output.parquet"), 12, "a" * 64
                ),
            ),
        )


@pytest.mark.parametrize("value", [True, -1, MAX_WORK_METRIC + 1])
def test_result_metrics_are_exact_and_bounded(value: object) -> None:
    with pytest.raises((TypeError, ResultSinkInvalidResultError)):
        ResultMetrics(cast(int, value), 0, WorkMetricDelta())
    with pytest.raises((TypeError, ResultSinkInvalidResultError)):
        ResultMetrics(0, cast(int, value), WorkMetricDelta())


def test_result_metrics_snapshot_nested_aggregate_values() -> None:
    delta = WorkMetricDelta(1, 2, 3, 4, 5)
    metrics = ResultMetrics(6, 7, delta)
    assert metrics.aggregate_delta == delta
    assert metrics.aggregate_delta is not delta
    object.__setattr__(delta, "records_read", 99)
    assert metrics.aggregate_delta.records_read == 1
    with pytest.raises(FrozenInstanceError):
        metrics.records_processed = 1  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("payload_schema_version", True, TypeError),
        ("payload_schema_version", 0, ResultSinkInvalidResultError),
        (
            "payload_schema_version",
            MAX_RESULT_CHECKPOINT_SCHEMA_VERSION + 1,
            ResultSinkInvalidResultError,
        ),
        ("source_cursor", {}, TypeError),
        ("output_position", {}, TypeError),
    ],
)
def test_checkpoint_payload_is_exact_versioned_and_bounded(
    field: str,
    value: object,
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        replace(_checkpoint(), **{field: value})


def test_checkpoint_documents_are_deeply_detached() -> None:
    source = ConfigurationDocument(
        (
            ("array", DocumentArray((1, "two"))),
            ("nested", NestedDocumentObject((("ok", True),))),
        )
    )
    checkpoint = ResultCheckpoint(PARTITION, 1, source, None, None)
    assert checkpoint.source_cursor == source
    assert checkpoint.source_cursor is not source
    object.__setattr__(source, "items", (("mutated", 1),))
    assert checkpoint.source_cursor == ConfigurationDocument(
        (
            ("array", DocumentArray((1, "two"))),
            ("nested", NestedDocumentObject((("ok", True),))),
        )
    )


def test_checkpoint_rejects_artifact_from_a_different_partition() -> None:
    foreign_partition = PartitionKey("partition-00000001")
    with pytest.raises(ResultSinkInvalidResultError, match="checkpoint partition"):
        ResultCheckpoint(
            PARTITION,
            1,
            _document(offset=2),
            _document(rows=2),
            _manifest(partition_key=foreign_partition),
        )


def test_result_schema_and_closed_outer_types_are_exact() -> None:
    lease = _issued_lease()
    result = _success(lease)
    with pytest.raises(TypeError, match="schema version"):
        replace(result, schema_version=True)
    with pytest.raises(ResultSinkInvalidResultError, match="not supported"):
        replace(result, schema_version=2)
    with pytest.raises(TypeError, match="closed WorkResult"):
        snapshot_work_result(cast(Any, object()))
    with pytest.raises(TypeError, match="AttemptSucceeded"):
        SuccessfulWorkResult(
            cast(Any, AttemptCancelled(_context(lease), _timestamp(5))), _checkpoint(), _metrics()
        )
    with pytest.raises(TypeError, match="AttemptFailed or AttemptCancelled"):
        UnsuccessfulWorkResult(
            cast(Any, AttemptSucceeded(_context(lease), _timestamp(5))), None, _metrics()
        )


def test_result_submission_requires_service_issued_matching_lease() -> None:
    lease = _issued_lease()
    submission = ResultSubmission(lease, _success(lease, artifact=_manifest()))
    assert submission.has_current_lease_evidence()
    assert "result-sink-owner" not in repr(submission)
    assert "result-sink-worker" not in repr(submission)

    with pytest.raises(WorkLeaseOwnershipError, match="service-issued"):
        WorkLease(
            lease.claim,
            lease.node,
            lease.run,
            lease.events,
            lease.submission_id,
            _token=object(),
        )
    with pytest.raises(TypeError, match="WorkLease"):
        ResultSubmission(cast(Any, object()), _success(lease))


def test_result_submission_rejects_reflectively_forged_lease_wrapper() -> None:
    lease = _issued_lease()
    service = _service_for(lease)
    forged = object.__new__(WorkLease)
    for name in ("_claim", "_node", "_run", "_events", "_submission_id"):
        object.__setattr__(forged, name, getattr(lease, name))

    submission = ResultSubmission(forged, _success(lease))
    sink = _Sink(_outcome_for(submission.result))
    with pytest.raises(ResultSinkInvalidResultError, match="active service capability"):
        submit_work_result(sink, submission, lease_service=service)
    assert sink.submissions == []


_ContextTransform = Callable[[AttemptEventContext], AttemptEventContext]
_CONTEXT_TRANSFORMS: tuple[_ContextTransform, ...] = (
    lambda context: replace(context, run_id=RunId("run_other-result")),
    lambda context: replace(context, node_id=NodeId("nod_other-result")),
    lambda context: replace(context, work_item_id=WorkItemId("wrk_other-result")),
    lambda context: replace(context, attempt_number=AttemptNumber(2)),
    lambda context: replace(context, started_at=_timestamp(2)),
    lambda context: replace(context, runner_kind=PlannerRunnerKind.THREADED),
    lambda context: replace(context, worker_identity="other-worker"),
)


@pytest.mark.parametrize(
    "context_transform",
    _CONTEXT_TRANSFORMS,
)
def test_submission_binds_every_attempt_identity_to_lease_capability(
    context_transform: Callable[[AttemptEventContext], AttemptEventContext],
) -> None:
    lease = _issued_lease()
    terminal = AttemptSucceeded(context_transform(_context(lease)), _timestamp(5))
    with pytest.raises(ResultSinkInvalidResultError, match="active lease"):
        ResultSubmission(
            lease,
            SuccessfulWorkResult(terminal, _checkpoint(), _metrics()),
        )


def test_submission_snapshots_result_and_detects_reflective_lease_mutation() -> None:
    lease = _issued_lease()
    result = _success(lease, artifact=_manifest())
    submission = ResultSubmission(lease, result)
    original = submission.result
    object.__setattr__(result, "schema_version", 99)
    assert submission.result.schema_version == 1
    assert submission.result is not original or submission.result is not result

    object.__setattr__(lease.claim, "worker_identity", "forged-worker")
    assert not submission.has_current_lease_evidence()
    with pytest.raises(ResultSinkInvalidResultError, match="evidence changed"):
        snapshot_result_submission(submission)


def test_submission_rejects_inconsistent_lease_parents_and_expired_result() -> None:
    lease = _issued_lease()
    result = _success(lease)
    object.__setattr__(lease.node, "run_id", RunId("run_other-result"))
    with pytest.raises(ResultSinkInvalidResultError, match="parents"):
        ResultSubmission(lease, result)

    fresh = _issued_lease()
    terminal = AttemptSucceeded(_context(fresh), fresh.claim.lease_expires_at)
    with pytest.raises(ResultSinkInvalidResultError, match="before lease expiry"):
        ResultSubmission(
            fresh,
            SuccessfulWorkResult(terminal, _checkpoint(), _metrics()),
        )


def test_snapshot_submission_rejects_replaced_claim_and_invalid_outer_type() -> None:
    lease = _issued_lease()
    submission = ResultSubmission(lease, _success(lease))
    object.__setattr__(lease, "_claim", replace(lease.claim))
    with pytest.raises(ResultSinkInvalidResultError, match="evidence changed"):
        snapshot_result_submission(submission)
    with pytest.raises(TypeError, match="ResultSubmission"):
        snapshot_result_submission(cast(Any, object()))


def test_committed_and_rejected_sink_outcomes_are_distinct_and_exact() -> None:
    lease = _issued_lease()
    success = _success(lease)
    failed = _failure(
        lease,
        FailureClassification.VALIDATION,
        _stopped(
            lease,
            FailureClassification.VALIDATION,
            FailureDisposition.QUARANTINE,
        ),
    )
    committed_success = _outcome_for(success)
    committed_failure = _outcome_for(failed)
    rejected = _outcome_for(failed, committed=False)
    assert committed_success.kind is ResultSinkOutcomeKind.COMMITTED
    assert committed_failure.kind is ResultSinkOutcomeKind.COMMITTED
    assert rejected.kind is ResultSinkOutcomeKind.REJECTED
    assert cast(ResultSinkCommitted, committed_success).checkpoint_version == CheckpointVersion(1)
    assert cast(ResultSinkCommitted, committed_failure).checkpoint_version is None
    assert cast(ResultSinkRejected, rejected).reason is ResultRejectionReason.STALE_CAPABILITY


@pytest.mark.parametrize(
    ("kind", "checkpoint"),
    [
        (WorkResultKind.SUCCEEDED, None),
        (WorkResultKind.FAILED, CheckpointVersion(1)),
        (WorkResultKind.SUCCEEDED, CheckpointVersion(0)),
    ],
)
def test_committed_outcome_checkpoint_shape_is_exact(
    kind: WorkResultKind,
    checkpoint: CheckpointVersion | None,
) -> None:
    with pytest.raises(ResultSinkInvalidResultError, match="checkpoint"):
        ResultSinkCommitted(
            WriterSubmissionId(1),
            kind,
            RUN_ID,
            NODE_ID,
            WORK_ID,
            AttemptNumber(1),
            checkpoint,
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("submission_id", 1, "WriterSubmissionId"),
        ("result_kind", "succeeded", "WorkResultKind"),
        ("run_id", "run_result-sink", "RunId"),
        ("node_id", "nod_result-sink", "NodeId"),
        ("work_item_id", "wrk_result-sink", "WorkItemId"),
        ("attempt_number", 1, "AttemptNumber"),
        ("reason", "stale_capability", "ResultRejectionReason"),
    ],
)
def test_rejected_outcome_requires_exact_public_types(
    field: str,
    value: object,
    message: str,
) -> None:
    lease = _issued_lease()
    outcome = cast(ResultSinkRejected, _outcome_for(_cancelled(lease), committed=False))
    with pytest.raises(TypeError, match=message):
        replace(outcome, **{field: value})


def test_submit_work_result_accepts_committed_and_confirmed_rejected_outcomes() -> None:
    lease = _issued_lease()
    service = _service_for(lease)
    submission = ResultSubmission(lease, _success(lease, artifact=_manifest()))
    committed = _outcome_for(submission.result)
    committed_sink = _Sink(committed)
    returned = submit_work_result(committed_sink, submission, lease_service=service)
    assert returned == committed
    assert returned is not committed
    assert committed_sink.submissions[0] is not submission
    assert committed_sink.submissions[0].result is not submission.result
    assert service.snapshot().active == 0

    rejected_lease = _issued_lease()
    rejected_service = _service_for(rejected_lease)
    rejected_submission = ResultSubmission(rejected_lease, _success(rejected_lease))
    rejected = _outcome_for(rejected_submission.result, committed=False)
    assert (
        submit_work_result(
            _Sink(rejected),
            rejected_submission,
            lease_service=rejected_service,
        )
        == rejected
    )
    assert rejected_service.snapshot().unknown == 1

    retained_lease = _issued_lease()
    retained_service = _service_for(retained_lease)
    retained_submission = ResultSubmission(retained_lease, _success(retained_lease))
    definitely_rejected = _outcome_for(
        retained_submission.result,
        committed=False,
        reason=ResultRejectionReason.DEFINITELY_NOT_EXECUTED,
    )
    assert (
        submit_work_result(
            _Sink(definitely_rejected),
            retained_submission,
            lease_service=retained_service,
        )
        == definitely_rejected
    )
    assert retained_service.snapshot().active == 1
    assert retained_service.snapshot().in_flight == 0


def test_result_submission_reservation_blocks_overlapping_authority_changes() -> None:
    lease = _issued_lease()
    service = _service_for(lease)
    submission = ResultSubmission(lease, _success(lease))
    committed = _outcome_for(submission.result)

    def overlap(selected: ResultSubmission) -> ResultSinkOutcome:
        with pytest.raises(WorkLeaseOwnershipError, match="active service capability"):
            service.retire(selected.lease)
        with pytest.raises(WorkLeaseBusyError, match="lease operation"):
            service.reserve_completion(selected.lease)
        assert service.snapshot().in_flight == 1
        return committed

    assert submit_work_result(_Sink(overlap), submission, lease_service=service) == committed
    assert service.snapshot().active == 0
    assert service.snapshot().in_flight == 0


def test_completion_reservation_rejects_forgery_and_closed_disposition_values() -> None:
    lease = _issued_lease()
    service = _service_for(lease)
    malformed = object.__new__(WorkLease)
    with pytest.raises(WorkLeaseOwnershipError, match="identity evidence"):
        service.reserve_completion(malformed)
    with pytest.raises(WorkLeaseOwnershipError, match="service-issued"):
        WorkLeaseCompletionReservation(lease, _token=object())
    reservation = service.reserve_completion(lease)
    assert "redacted" in repr(reservation)
    forged = object.__new__(WorkLeaseCompletionReservation)
    object.__setattr__(forged, "_lease", reservation.lease)
    object.__setattr__(forged, "_work_item_id", lease.claim.work_item_id)
    with pytest.raises(WorkLeaseOwnershipError, match="not active"):
        service.finalize_completion(
            forged,
            WorkLeaseCompletionDisposition.RETAIN_ACTIVE,
        )
    missing_forged = object.__new__(WorkLeaseCompletionReservation)
    object.__setattr__(missing_forged, "_lease", reservation.lease)
    with pytest.raises(WorkLeaseOwnershipError, match="not active"):
        service.finalize_completion(
            missing_forged,
            WorkLeaseCompletionDisposition.RETAIN_ACTIVE,
        )
    with pytest.raises(TypeError, match="completion disposition"):
        service.finalize_completion(reservation, cast(Any, "retain_active"))
    service.finalize_completion(
        reservation,
        WorkLeaseCompletionDisposition.RETAIN_ACTIVE,
    )
    assert service.snapshot().active == 1
    assert service.snapshot().in_flight == 0

    corrupt_lease = _issued_lease()
    corrupt_service = _service_for(corrupt_lease)
    corrupt_reservation = corrupt_service.reserve_completion(corrupt_lease)
    object.__setattr__(
        corrupt_reservation,
        "_work_item_id",
        WorkItemId("wrk_other-result"),
    )
    with pytest.raises(WorkLeaseOwnershipError, match="active lease"):
        corrupt_service.finalize_completion(
            corrupt_reservation,
            WorkLeaseCompletionDisposition.RETAIN_ACTIVE,
        )
    assert corrupt_service.snapshot().unknown == 1
    assert corrupt_service.snapshot().in_flight == 0

    missing_lease = _issued_lease()
    missing_service = _service_for(missing_lease)
    missing_reservation = missing_service.reserve_completion(missing_lease)
    object.__delattr__(missing_reservation, "_work_item_id")
    with pytest.raises(WorkLeaseOwnershipError, match="evidence is invalid"):
        missing_service.finalize_completion(
            missing_reservation,
            WorkLeaseCompletionDisposition.RETAIN_ACTIVE,
        )
    assert missing_service.snapshot().unknown == 1

    missing_wrapper_lease = _issued_lease()
    missing_wrapper_service = _service_for(missing_wrapper_lease)
    missing_wrapper_reservation = missing_wrapper_service.reserve_completion(missing_wrapper_lease)
    object.__delattr__(missing_wrapper_reservation, "_lease")
    with pytest.raises(WorkLeaseOwnershipError, match="active lease"):
        missing_wrapper_service.finalize_completion(
            missing_wrapper_reservation,
            WorkLeaseCompletionDisposition.RETAIN_ACTIVE,
        )
    assert missing_wrapper_service.snapshot().unknown == 1

    malformed_lease = _issued_lease()
    malformed_service = _service_for(malformed_lease)
    malformed_reservation = malformed_service.reserve_completion(malformed_lease)
    object.__setattr__(malformed_lease, "_claim", object())
    with pytest.raises(WorkLeaseOwnershipError, match="active lease"):
        malformed_service.finalize_completion(
            malformed_reservation,
            WorkLeaseCompletionDisposition.RETAIN_ACTIVE,
        )
    assert malformed_service.snapshot().unknown == 1

    inconsistent_lease = _issued_lease()
    inconsistent_service = _service_for(inconsistent_lease)
    inconsistent_reservation = inconsistent_service.reserve_completion(inconsistent_lease)
    inconsistent_service._in_flight.remove(inconsistent_lease.claim.work_item_id)
    with pytest.raises(WorkLeaseOwnershipError, match="not active"):
        inconsistent_service.finalize_completion(
            inconsistent_reservation,
            WorkLeaseCompletionDisposition.RETIRE_COMMITTED,
        )
    assert inconsistent_service._completions == {}
    assert inconsistent_service.snapshot().unknown == 1
    assert inconsistent_service.snapshot().in_flight == 0


@pytest.mark.parametrize(
    "interrupting_set_type",
    [_InterruptingAddSet, _StubbornInterruptingAddSet, _ExplodingContainsAddSet],
)
def test_completion_reservation_interruption_restores_active_lease_without_orphan(
    interrupting_set_type: type[_InterruptingAddSet],
) -> None:
    lease = _issued_lease()
    service = _service_for(lease)
    fatal = _Fatal()
    object.__setattr__(service, "_in_flight", interrupting_set_type(fatal))

    with pytest.raises(_Fatal) as raised:
        service.reserve_completion(lease)
    assert raised.value is fatal
    assert service._completions == {}
    assert service.snapshot().active == 1
    assert service.snapshot().unknown == 0
    assert service.snapshot().in_flight == 0

    object.__setattr__(service, "_in_flight", set())
    reservation = service.reserve_completion(lease)
    service.finalize_completion(
        reservation,
        WorkLeaseCompletionDisposition.RETAIN_ACTIVE,
    )


def test_unclearable_reservation_interruption_poison_authority() -> None:
    lease = _issued_lease()
    service = _service_for(lease)
    fatal = _Fatal()
    object.__setattr__(service, "_in_flight", _UnclearableAddSet(fatal))

    with pytest.raises(_Fatal) as raised:
        service.reserve_completion(lease)
    assert raised.value is fatal
    assert service._completions == {}
    assert service.snapshot().active == 0
    assert service.snapshot().unknown == 1
    assert service.snapshot().in_flight == 0


@pytest.mark.parametrize(
    "boundary",
    ["state_lookup", "reservation_delete", "in_flight_remove", "active_delete"],
)
def test_committed_finalization_interruption_poison_authority_at_every_boundary(
    boundary: str,
) -> None:
    lease = _issued_lease()
    service = _service_for(lease)
    submission = ResultSubmission(lease, _success(lease))
    committed = _outcome_for(submission.result)
    fatal = _Fatal()

    def interrupt_finalization(_selected: ResultSubmission) -> ResultSinkOutcome:
        if boundary == "state_lookup":
            object.__setattr__(
                service,
                "_states",
                _InterruptingGetDict(dict(service._states), fatal),
            )
        elif boundary == "reservation_delete":
            object.__setattr__(
                service,
                "_completions",
                _InterruptingDeleteDict(dict(service._completions), fatal),
            )
        elif boundary == "in_flight_remove":
            object.__setattr__(
                service,
                "_in_flight",
                _InterruptingRemoveSet(set(service._in_flight), fatal),
            )
        else:
            object.__setattr__(
                service,
                "_states",
                _InterruptingDeleteDict(dict(service._states), fatal),
            )
        return committed

    with pytest.raises(_Fatal) as raised:
        submit_work_result(
            _Sink(interrupt_finalization),
            submission,
            lease_service=service,
        )
    assert raised.value is fatal
    assert service._completions == {}
    assert service.snapshot().active == 0
    assert service.snapshot().unknown == 1
    assert service.snapshot().in_flight == 0
    object.__setattr__(service, "_states", dict(service._states))
    with pytest.raises(WorkLeaseOwnershipError, match="active service capability"):
        service.reserve_completion(lease)


def test_submit_work_result_rejects_non_sink_and_malformed_outcome() -> None:
    lease = _issued_lease()
    service = _service_for(lease)
    submission = ResultSubmission(lease, _success(lease))
    with pytest.raises(TypeError, match="implement ResultSink"):
        submit_work_result(cast(Any, object()), submission, lease_service=service)
    assert service.snapshot().in_flight == 0
    with pytest.raises(ResultSinkProtocolError, match="invalid outcome"):
        submit_work_result(
            _Sink(lambda _submission: object()),
            submission,
            lease_service=service,
        )
    assert service.snapshot().unknown == 1


_OutcomeTransform = Callable[[ResultSinkCommitted], ResultSinkCommitted]
_OUTCOME_TRANSFORMS: tuple[_OutcomeTransform, ...] = (
    lambda outcome: replace(
        outcome,
        result_kind=WorkResultKind.FAILED,
        checkpoint_version=None,
    ),
    lambda outcome: replace(outcome, run_id=RunId("run_other-result")),
    lambda outcome: replace(outcome, node_id=NodeId("nod_other-result")),
    lambda outcome: replace(outcome, work_item_id=WorkItemId("wrk_other-result")),
    lambda outcome: replace(outcome, attempt_number=AttemptNumber(2)),
)


@pytest.mark.parametrize(
    "transform",
    _OUTCOME_TRANSFORMS,
)
def test_submit_work_result_rejects_mismatched_post_commit_evidence(
    transform: Callable[[ResultSinkCommitted], ResultSinkCommitted],
) -> None:
    lease = _issued_lease()
    service = _service_for(lease)
    submission = ResultSubmission(lease, _success(lease))
    outcome = cast(ResultSinkCommitted, _outcome_for(submission.result))
    with pytest.raises(ResultSinkProtocolError, match="invalid outcome"):
        submit_work_result(
            _Sink(transform(outcome)),
            submission,
            lease_service=service,
        )
    assert service.snapshot().unknown == 1


@pytest.mark.parametrize("path", ["admission", "malformed", "committed"])
def test_submit_work_result_fails_unknown_when_reservation_finalization_is_corrupted(
    path: str,
) -> None:
    lease = _issued_lease()
    service = _service_for(lease)
    submission = ResultSubmission(lease, _success(lease))
    committed = _outcome_for(submission.result)

    def corrupt(_selected: ResultSubmission) -> object:
        object.__setattr__(lease.claim, "worker_identity", "corrupted-worker")
        if path == "admission":
            raise ResultSinkAdmissionError("sensitive admission")
        if path == "malformed":
            return object()
        return committed

    with pytest.raises(ResultSinkOutcomeUnknownError, match="completion state is unknown"):
        submit_work_result(_Sink(corrupt), submission, lease_service=service)
    assert service.snapshot().unknown == 1
    assert service.snapshot().in_flight == 0


@pytest.mark.parametrize(
    ("failure", "expected_type", "expected_unknown"),
    [
        (
            ResultSinkAdmissionError("sensitive admission"),
            ResultSinkAdmissionError,
            False,
        ),
        (
            ResultSinkInvalidResultError("sensitive invalid"),
            ResultSinkInvalidResultError,
            True,
        ),
        (ResultSinkProtocolError("sensitive protocol"), ResultSinkProtocolError, True),
        (
            ResultSinkOutcomeUnknownError("sensitive unknown"),
            ResultSinkOutcomeUnknownError,
            True,
        ),
        (ResultSinkError("sensitive generic"), ResultSinkOutcomeUnknownError, True),
    ],
)
def test_submit_work_result_redacts_typed_sink_errors(
    failure: ResultSinkError,
    expected_type: type[ResultSinkError],
    expected_unknown: bool,
) -> None:
    lease = _issued_lease()
    service = _service_for(lease)
    submission = ResultSubmission(lease, _success(lease))
    with pytest.raises(expected_type) as raised:
        submit_work_result(_Sink(failure), submission, lease_service=service)
    assert raised.value is not failure
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    rendered = "".join(format_exception(raised.value))
    assert "sensitive" not in rendered
    snapshot = service.snapshot()
    assert snapshot.unknown == int(expected_unknown)
    assert snapshot.active == int(not expected_unknown)
    assert snapshot.in_flight == 0


def test_submit_work_result_translates_exception_and_propagates_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease = _issued_lease()
    service = _service_for(lease)
    submission = ResultSubmission(lease, _success(lease))

    sensitive = "credential=" + "secret" + " C:\\machine\\path"
    with pytest.raises(ResultSinkOutcomeUnknownError) as unknown:
        submit_work_result(
            _Sink(RuntimeError(sensitive)),
            submission,
            lease_service=service,
        )
    assert unknown.value.__cause__ is None
    assert unknown.value.__context__ is None
    rendered = "".join(format_exception(unknown.value))
    assert "secret" not in rendered
    assert "machine" not in rendered
    assert service.snapshot().unknown == 1

    fatal_lease = _issued_lease()
    fatal_service = _service_for(fatal_lease)
    fatal_submission = ResultSubmission(fatal_lease, _success(fatal_lease))
    fatal = _Fatal()
    with pytest.raises(_Fatal) as raised:
        submit_work_result(_Sink(fatal), fatal_submission, lease_service=fatal_service)
    assert raised.value is fatal
    assert fatal_service.snapshot().unknown == 1

    unmasked_lease = _issued_lease()
    unmasked_submission = ResultSubmission(unmasked_lease, _success(unmasked_lease))
    unmasked = _Fatal()

    def fail_finalization(
        _service: WorkLeaseService,
        _reservation: WorkLeaseCompletionReservation,
        _disposition: WorkLeaseCompletionDisposition,
    ) -> None:
        raise RuntimeError("sensitive finalization failure")

    monkeypatch.setattr(WorkLeaseService, "finalize_completion", fail_finalization)
    with pytest.raises(_Fatal) as unmasked_raised:
        submit_work_result(
            _Sink(unmasked),
            unmasked_submission,
            lease_service=_service_for(unmasked_lease),
        )
    assert unmasked_raised.value is unmasked


def test_http_429_retry_evidence_is_detached_without_parsing_headers() -> None:
    lease = _issued_lease()
    delay = Http429RetryDelay(Duration(2_000_000))
    decision = RetryScheduledDecision(
        RetryPolicyName.BOUNDED_EXPONENTIAL_V1,
        WORK_ID,
        AttemptNumber(1),
        FailureClassification.HTTP_429,
        _timestamp(5),
        _timestamp(5),
        delay,
        Duration(0),
        Duration(2_000_000),
        _timestamp(7),
    )
    result = _failure(lease, FailureClassification.HTTP_429, decision)
    assert cast(RetryScheduledDecision, result.decision).http_429_delay == delay
    assert cast(RetryScheduledDecision, result.decision).http_429_delay is not delay


def test_redacted_detail_and_configuration_payload_never_appear_in_repr() -> None:
    lease = _issued_lease()
    secret = "credential=top-secret C:\\machine\\private"
    result = _failure(
        lease,
        FailureClassification.VALIDATION,
        _stopped(
            lease,
            FailureClassification.VALIDATION,
            FailureDisposition.QUARANTINE,
        ),
        detail=RedactedAttemptDetail(secret),
    )
    assert secret not in repr(result)
    assert secret not in repr(ResultSubmission(lease, result))


def test_manifest_and_result_inputs_reject_bare_or_inexact_nested_values() -> None:
    lease = _issued_lease()
    manifest = _manifest()
    object.__setattr__(manifest.artifact_id, "value", cast(Any, 1))
    with pytest.raises(TypeError, match="artifact identity text"):
        _success(lease, artifact=manifest)

    delta = WorkMetricDelta()
    object.__setattr__(delta, "records_read", cast(Any, True))
    with pytest.raises(TypeError, match="aggregate metric"):
        ResultMetrics(0, 0, delta)


def test_document_snapshot_rejects_reflectively_malformed_structure() -> None:
    bad_outer = _document(ok=True)
    object.__setattr__(bad_outer, "items", cast(Any, []))
    with pytest.raises(TypeError, match="object must be a tuple"):
        ResultCheckpoint(PARTITION, 1, bad_outer, None, None)

    bad_entry = _document(ok=True)
    object.__setattr__(bad_entry, "items", cast(Any, (("only-key",),)))
    with pytest.raises(TypeError, match="key-value"):
        ResultCheckpoint(PARTITION, 1, bad_entry, None, None)

    non_tuple_entry = _document(ok=True)
    object.__setattr__(non_tuple_entry, "items", cast(Any, (["key", 1],)))
    with pytest.raises(TypeError, match="key-value"):
        ResultCheckpoint(PARTITION, 1, non_tuple_entry, None, None)

    bad_array = _document(ok=True)
    object.__setattr__(bad_array, "items", cast(Any, (("items", DocumentArray((1,))),)))
    object.__setattr__(cast(DocumentArray, bad_array.items[0][1]), "values", cast(Any, []))
    with pytest.raises(TypeError, match="array values"):
        ResultCheckpoint(PARTITION, 1, bad_array, None, None)

    unsupported = _document(ok=True)
    object.__setattr__(unsupported, "items", cast(Any, (("bad", object()),)))
    with pytest.raises(TypeError, match="unsupported value"):
        ResultCheckpoint(PARTITION, 1, unsupported, None, None)


def test_lease_evidence_failure_is_closed_and_receipt_nested_values_are_snapshotted() -> None:
    lease = _issued_lease()
    submission = ResultSubmission(lease, _success(lease))
    object.__setattr__(lease.claim.started_at, "value", cast(Any, object()))
    assert not submission.has_current_lease_evidence()

    clean_lease = _issued_lease()
    result = _success(clean_lease)
    submission_id = WriterSubmissionId(9)
    committed = cast(ResultSinkCommitted, _outcome_for(result))
    copied = replace(committed, submission_id=submission_id)
    object.__setattr__(submission_id, "number", 99)
    assert copied.submission_id == WriterSubmissionId(9)


def test_reflectively_malformed_retry_and_sequence_scalars_fail_closed() -> None:
    lease = _issued_lease()
    terminal = AttemptFailed(
        _context(lease),
        _timestamp(5),
        FailureClassification.VALIDATION,
    )
    with pytest.raises(TypeError, match="closed RetryDecision"):
        UnsuccessfulWorkResult(terminal, cast(Any, object()), _metrics())

    stopped = _stopped(
        lease,
        FailureClassification.VALIDATION,
        FailureDisposition.QUARANTINE,
    )
    object.__setattr__(stopped, "exhausted", cast(Any, 0))
    with pytest.raises(TypeError, match="boolean"):
        UnsuccessfulWorkResult(terminal, stopped, _metrics())

    attempt = AttemptNumber(1)
    object.__setattr__(attempt, "number", cast(Any, True))
    with pytest.raises(TypeError, match="integer"):
        ResultSinkRejected(
            WriterSubmissionId(1),
            WorkResultKind.FAILED,
            RUN_ID,
            NODE_ID,
            WORK_ID,
            attempt,
            ResultRejectionReason.STATE_CONFLICT,
        )


def test_failure_disposition_helper_agrees_with_result_contract() -> None:
    assert disposition_for(FailureClassification.VALIDATION) is FailureDisposition.QUARANTINE
    assert disposition_for(FailureClassification.USER_CANCELLATION) is FailureDisposition.CANCEL
