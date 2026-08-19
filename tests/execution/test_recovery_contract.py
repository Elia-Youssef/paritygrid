# pyright: reportPrivateUsage=false
"""Contract tests for durable startup recovery classification."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from paritygrid.application.execution import (
    FinalizationEvidence,
    RecoveryAmbiguousError,
    RecoveryBusyError,
    RecoveryClockError,
    RecoveryCorruptionError,
    RecoveryEvidence,
    RecoveryFinding,
    RecoveryFindingKind,
    RecoveryInvalidRequestError,
    RecoveryOutcomeUnknownError,
    RecoveryRejectedError,
    RecoveryScan,
    RecoverySettings,
    RecoveryStateReadError,
    RecoveryStatus,
    StartupRecoveryScanner,
)
from paritygrid.application.ports.artifact_integrity import (
    ArtifactIntegrityIssue,
    ArtifactIntegrityIssueKind,
)
from paritygrid.application.ports.artifacts import ArtifactManifestRecord
from paritygrid.application.ports.consistency import (
    EventSequence,
    IdempotencyRecord,
    IdempotencyStatus,
    RedactedDocument,
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
from paritygrid.application.ports.writer import (
    WriterAdmissionTimeoutError,
    WriterCommand,
    WriterCommandKind,
    WriterDefinitelyNotExecutedError,
    WriterReceipt,
    WriterResultTimeoutError,
    WriterSubmissionId,
)
from paritygrid.application.writes import RecoverExpiredWork, RecoverExpiredWorkResult
from paritygrid.domain.execution import FailureClassification, RunState
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
from paritygrid.domain.pipeline import PartitionKey

RUN_ID = RunId("run_recov-test")
PIPELINE_ID = PipelineId("pip_recov-test")
NODE_A = NodeId("nod_recov-a")
WORK_A = WorkItemId("wrk_recov-a")
WORK_B = WorkItemId("wrk_recov-b")
OBSERVED = UtcTimestamp(datetime(2025, 3, 1, 0, 0, 30, tzinfo=UTC))
CLAIMED = UtcTimestamp(datetime(2025, 3, 1, 0, 0, 10, tzinfo=UTC))
EXPIRED = UtcTimestamp(datetime(2025, 3, 1, 0, 0, 20, tzinfo=UTC))
LIVE = UtcTimestamp(datetime(2025, 3, 1, 0, 0, 40, tzinfo=UTC))


def _time(second: int) -> UtcTimestamp:
    return UtcTimestamp(
        datetime(2025, 3, 1, 0, 0, second % 60, tzinfo=UTC) + timedelta(minutes=second // 60)
    )


def _run(*, state: RunState = RunState.RUNNING, row_version: int = 6) -> RunRecord:
    return RunRecord(
        RUN_ID,
        PIPELINE_ID,
        PipelineVersion(1),
        "sequential",
        _configuration(),
        state,
        row_version,
        None,
        _time(1),
        _time(2) if state is not RunState.QUEUED else None,
        None,
        None,
        None,
        None,
        None,
    )


def _configuration() -> Any:
    from paritygrid.application.ports.configuration import ConfigurationDocument

    return ConfigurationDocument(())


def _work(
    *,
    state: WorkItemState = WorkItemState.RUNNING,
    work_id: WorkItemId = WORK_A,
    expected_checkpoint: int = 0,
    lease_expires_at: UtcTimestamp | None = EXPIRED,
    completed_attempts: int = 0,
) -> WorkItemRecord:
    active = state is WorkItemState.RUNNING
    return WorkItemRecord(
        work_id,
        RUN_ID,
        NODE_A,
        PartitionKey(f"part-{work_id.value}"),
        state,
        2,
        completed_attempts,
        expected_checkpoint,
        None,
        None,
        "recov-owner" if active else None,
        lease_expires_at if active else None,
        AttemptNumber(completed_attempts + 1) if active else None,
        CLAIMED if active else None,
        "sequential" if active else None,
        "recov-worker" if active else None,
        _time(3),
        CLAIMED if active else _time(3),
    )


def _node(
    *,
    work_states: tuple[WorkItemState, ...] = (WorkItemState.RUNNING,),
    row_version: int = 3,
) -> RunNodeRecord:
    counts = dict.fromkeys(WorkItemState, 0)
    for state in work_states:
        counts[state] += 1
    return RunNodeRecord(
        RUN_ID,
        NODE_A,
        RunNodeStatus.RUNNING
        if WorkItemState.RUNNING in work_states or WorkItemState.PENDING in work_states
        else RunNodeStatus.SUCCEEDED,
        row_version,
        len(work_states),
        counts[WorkItemState.PENDING] + counts[WorkItemState.RETRY_WAIT],
        counts[WorkItemState.RUNNING],
        counts[WorkItemState.SUCCEEDED],
        counts[WorkItemState.QUARANTINED],
        counts[WorkItemState.FAILED],
        counts[WorkItemState.CANCELLED],
        0,
        0,
        0,
        0,
        0,
        0,
        Duration(0),
        _time(2),
        None,
    )


def _attempt(work_id: WorkItemId, number: int = 1) -> WorkAttemptRecord:
    return WorkAttemptRecord(
        work_id,
        AttemptNumber(number),
        CLAIMED,
        EXPIRED,
        "sequential",
        "recov-worker",
        AttemptOutcome.SUCCEEDED,
        None,
        None,
        None,
        0,
        0,
        Duration(0),
    )


def _evidence(
    *,
    run: RunRecord | None = None,
    work: tuple[WorkItemRecord, ...] = (),
    work_states: tuple[WorkItemState, ...] | None = None,
    attempts: tuple[WorkAttemptRecord, ...] = (),
    checkpoints: tuple[tuple[WorkItemId, int], ...] = (),
    artifacts: tuple[ArtifactManifestRecord, ...] = (),
    integrity: tuple[ArtifactIntegrityIssue, ...] = (),
    idempotency: tuple[IdempotencyRecord, ...] = (),
) -> RecoveryEvidence:
    states = work_states
    if states is None:
        states = tuple(item.state for item in work)
    frontier = FinalizationEvidence(
        run or _run(),
        EventSequence(7),
        7,
        (_node(work_states=states),),
        work,
        attempts,
        checkpoints
        or tuple((item.work_item_id, item.expected_checkpoint_version) for item in work),
    )
    return RecoveryEvidence(frontier, artifacts, integrity, idempotency)


class _Clock:
    def __init__(self, value: object = OBSERVED) -> None:
        self.value = value

    def now(self) -> UtcTimestamp:
        if isinstance(self.value, BaseException):
            raise self.value
        return cast(UtcTimestamp, self.value)


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
        return _Ticket(submission_id, self._receipt(command, submission_id))

    def _receipt(self, command: WriterCommand, submission_id: WriterSubmissionId) -> WriterReceipt:
        selected = cast(RecoverExpiredWork, command)
        from paritygrid.application.ports.consistency import (
            ExecutionEventBatch,
            ExecutionEventRecord,
        )

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
        recovered_work = replace(
            _work(state=WorkItemState.RETRY_WAIT, work_id=selected.work_item_id),
            row_version=selected.expected_work_row_version + 1,
            completed_attempt_count=selected.expected_attempt_number.number,
        )
        from paritygrid.application.ports.execution import CompletedWork

        attempt = WorkAttemptRecord(
            selected.work_item_id,
            selected.expected_attempt_number,
            CLAIMED,
            selected.observed_at,
            "sequential",
            "recov-worker",
            AttemptOutcome.LEASE_EXPIRED,
            FailureClassification.TIMEOUT,
            None,
            None,
            0,
            0,
            Duration(10_000_000),
        )
        return WriterReceipt(
            submission_id,
            WriterCommandKind.RECOVER_EXPIRED_WORK,
            selected.run_id,
            0,
            True,
            cast(
                Any,
                RecoverExpiredWorkResult(
                    CompletedWork(recovered_work, attempt),
                    _node(work_states=(WorkItemState.RETRY_WAIT,), row_version=4),
                    events,
                    replace(_run(), row_version=selected.expected_run_row_version + 1),
                ),
            ),
        )


class _Reader:
    def __init__(self, evidence: RecoveryEvidence | None = None) -> None:
        self.evidence = evidence
        self.failure: BaseException | None = None

    def read(self, run_id: RunId, /) -> RecoveryEvidence:
        assert run_id == RUN_ID
        if self.failure is not None:
            raise self.failure
        if self.evidence is None:
            raise AssertionError("recovery evidence was not prepared")
        return self.evidence


def _scanner(
    evidence: RecoveryEvidence,
    *,
    clock: _Clock | None = None,
    writer: _Writer | None = None,
) -> tuple[StartupRecoveryScanner, _Writer, _Reader]:
    selected_writer = writer or _Writer()
    reader = _Reader(evidence)
    selected_clock = clock or _Clock()
    scanner = StartupRecoveryScanner(
        selected_writer,
        reader,
        selected_clock,
        settings=RecoverySettings(),
    )
    return scanner, selected_writer, reader


def test_clean_terminal_shutdown_is_healthy_and_committed() -> None:
    evidence = _evidence(
        run=_run(state=RunState.SUCCEEDED),
        work=(
            _work(
                state=WorkItemState.SUCCEEDED,
                expected_checkpoint=1,
                lease_expires_at=None,
                completed_attempts=1,
            ),
        ),
        attempts=(_attempt(WORK_A),),
    )
    scanner, writer, _reader = _scanner(evidence)
    scan = scanner.scan(RUN_ID)
    assert scan.status is RecoveryStatus.HEALTHY
    kinds = {finding.kind for finding in scan.findings}
    assert RecoveryFindingKind.WORK_COMMITTED in kinds
    assert RecoveryFindingKind.RUN_TERMINAL in kinds
    report = scanner.recover(RUN_ID)
    assert report.applied == 0
    assert writer.commands == []


def test_queued_run_reports_awaiting_start() -> None:
    evidence = _evidence(
        run=_run(state=RunState.QUEUED),
        work=(_work(state=WorkItemState.PENDING, lease_expires_at=None),),
        work_states=(WorkItemState.PENDING,),
    )
    scanner, _writer, _reader = _scanner(evidence)
    scan = scanner.scan(RUN_ID)
    assert scan.status is RecoveryStatus.HEALTHY
    assert RecoveryFindingKind.RUN_QUEUED in {finding.kind for finding in scan.findings}


def test_running_run_without_active_work_is_healthy() -> None:
    evidence = _evidence(
        work=(_work(state=WorkItemState.PENDING, lease_expires_at=None),),
        work_states=(WorkItemState.PENDING,),
    )
    scanner, _writer, _reader = _scanner(evidence)
    scan = scanner.scan(RUN_ID)
    assert scan.status is RecoveryStatus.HEALTHY
    assert RecoveryFindingKind.WORK_PENDING in {finding.kind for finding in scan.findings}


def test_valid_active_lease_waits_without_recovery() -> None:
    evidence = _evidence(work=(_work(lease_expires_at=LIVE),))
    scanner, writer, _reader = _scanner(evidence)
    scan = scanner.scan(RUN_ID)
    assert scan.status is RecoveryStatus.ACTIVE
    assert RecoveryFindingKind.WORK_ACTIVE_LEASE in {finding.kind for finding in scan.findings}
    report = scanner.recover(RUN_ID)
    assert report.applied == 0
    assert writer.commands == []


def test_expired_lease_without_effect_is_recoverable_and_applied() -> None:
    evidence = _evidence(work=(_work(),))
    scanner, writer, _reader = _scanner(evidence)
    scan = scanner.scan(RUN_ID)
    assert scan.status is RecoveryStatus.RECOVERABLE
    assert RecoveryFindingKind.WORK_EXPIRED_NO_EFFECT in {finding.kind for finding in scan.findings}
    report = scanner.recover(RUN_ID, correlation_id="recov:test-1")
    assert report.applied == 1
    assert report.before.status is RecoveryStatus.RECOVERABLE
    assert len(writer.commands) == 1
    command = cast(RecoverExpiredWork, writer.commands[0])
    assert command.observed_at == OBSERVED
    assert command.retry_available_at == OBSERVED
    assert command.expected_attempt_number == AttemptNumber(1)
    assert report.submission_ids == (WriterSubmissionId(1),)


def test_expired_lease_with_committed_artifact_notes_idempotency() -> None:
    from paritygrid.application.ports.artifacts import ArtifactRelativePath
    from paritygrid.domain.models import ArtifactId

    manifest = ArtifactManifestRecord(
        ArtifactId("art_recov-1"),
        RUN_ID,
        NODE_A,
        PartitionKey("part-wrk_recov-a"),
        ArtifactRelativePath("runs/run/artifact.parquet"),
        "application/vnd.apache.parquet",
        1,
        10,
        1,
        "0" * 64,
        CLAIMED,
    )
    evidence = _evidence(work=(_work(),), artifacts=(manifest,))
    scanner, _writer, _reader = _scanner(evidence)
    scan = scanner.scan(RUN_ID)
    assert scan.status is RecoveryStatus.RECOVERABLE
    finding = next(
        finding
        for finding in scan.findings
        if finding.kind is RecoveryFindingKind.WORK_EXPIRED_WITH_COMMITTED_ARTIFACT
    )
    assert finding.detail == "committed artifact identity prevents duplicate effects"


def _artifact_id() -> Any:
    from paritygrid.domain.models import ArtifactId

    return ArtifactId("art_recov-1")


def test_committed_checkpoint_with_stale_acknowledgement_is_committed() -> None:
    evidence = _evidence(
        work=(
            _work(
                state=WorkItemState.SUCCEEDED,
                expected_checkpoint=1,
                lease_expires_at=None,
                completed_attempts=1,
            ),
        ),
        attempts=(_attempt(WORK_A),),
    )
    scanner, writer, _reader = _scanner(evidence)
    scan = scanner.scan(RUN_ID)
    assert scan.status is RecoveryStatus.HEALTHY
    assert RecoveryFindingKind.WORK_COMMITTED in {finding.kind for finding in scan.findings}
    assert scanner.recover(RUN_ID).applied == 0
    assert writer.commands == []


def test_retry_wait_work_is_healthy_waiting() -> None:
    evidence = _evidence(
        work=(_work(state=WorkItemState.RETRY_WAIT, lease_expires_at=None),),
        work_states=(WorkItemState.RETRY_WAIT,),
    )
    scanner, _writer, _reader = _scanner(evidence)
    scan = scanner.scan(RUN_ID)
    assert scan.status is RecoveryStatus.HEALTHY
    assert RecoveryFindingKind.WORK_RETRY_WAITING in {finding.kind for finding in scan.findings}


@pytest.mark.parametrize(
    ("kind", "work_kwargs", "extra"),
    [
        (
            RecoveryFindingKind.RUN_TERMINAL_WITH_ACTIVE_WORK,
            {"state": WorkItemState.RUNNING},
            {"run": _run(state=RunState.CANCELLED)},
        ),
        (RecoveryFindingKind.WORK_RUNNING_WITHOUT_LEASE, {"lease_expires_at": None}, {}),
        (
            RecoveryFindingKind.CHECKPOINT_FRONTIER_MISMATCH,
            {
                "state": WorkItemState.SUCCEEDED,
                "expected_checkpoint": 1,
                "lease_expires_at": None,
                "completed_attempts": 1,
            },
            {"checkpoints": ((WORK_A, 5),)},
        ),
        (RecoveryFindingKind.ATTEMPT_HISTORY_MISMATCH, {}, {"attempts": (_attempt(WORK_A),)}),
    ],
)
def test_mixed_states_fail_closed_as_ambiguous(
    kind: RecoveryFindingKind,
    work_kwargs: dict[str, Any],
    extra: dict[str, Any],
) -> None:
    work = _work(**work_kwargs)
    if work.state is WorkItemState.SUCCEEDED:
        evidence = _evidence(
            work=(work,),
            attempts=(_attempt(WORK_A),) if not extra.get("checkpoints") else (),
            **extra,
        )
    else:
        evidence = _evidence(work=(work,), **extra)
    scanner, writer, _reader = _scanner(evidence)
    scan = scanner.scan(RUN_ID)
    assert scan.status is RecoveryStatus.AMBIGUOUS
    assert kind in {finding.kind for finding in scan.findings}
    with pytest.raises(RecoveryAmbiguousError, match="prevents unsafe scheduling"):
        scanner.recover(RUN_ID)
    assert writer.commands == []


@pytest.mark.parametrize(
    ("issue_kind", "finding_kind"),
    [
        (ArtifactIntegrityIssueKind.MISSING_FILE, RecoveryFindingKind.INTEGRITY_MISSING_FILE),
        (ArtifactIntegrityIssueKind.ORPHAN_FILE, RecoveryFindingKind.INTEGRITY_ORPHAN_FILE),
        (ArtifactIntegrityIssueKind.INVALID_FILE, RecoveryFindingKind.INTEGRITY_CHANGED_FILE),
        (ArtifactIntegrityIssueKind.UNSAFE_ENTRY, RecoveryFindingKind.INTEGRITY_UNSAFE_ENTRY),
    ],
)
def test_artifact_integrity_findings_fail_closed(
    issue_kind: ArtifactIntegrityIssueKind,
    finding_kind: RecoveryFindingKind,
) -> None:
    from paritygrid.application.ports.artifacts import ArtifactRelativePath

    if issue_kind is ArtifactIntegrityIssueKind.UNSAFE_ENTRY:
        issue = ArtifactIntegrityIssue(issue_kind, None, None, "1" * 64)
    elif issue_kind is ArtifactIntegrityIssueKind.ORPHAN_FILE:
        issue = ArtifactIntegrityIssue(
            issue_kind, ArtifactRelativePath("runs/orphan.parquet"), None, None
        )
    else:
        issue = ArtifactIntegrityIssue(
            issue_kind, ArtifactRelativePath("runs/run/artifact.parquet"), _artifact_id(), None
        )
    evidence = _evidence(integrity=(issue,))
    scanner, _writer, _reader = _scanner(evidence)
    scan = scanner.scan(RUN_ID)
    assert scan.status is RecoveryStatus.AMBIGUOUS
    assert finding_kind in {finding.kind for finding in scan.findings}


def test_stranded_idempotency_is_reported_preserved() -> None:
    record = IdempotencyRecord(
        "scope",
        "key",
        IdempotencyStatus.IN_PROGRESS,
        None,
        None,
        CLAIMED,
        CLAIMED,
        None,
    )
    evidence = _evidence(
        work=(_work(state=WorkItemState.PENDING, lease_expires_at=None),),
        work_states=(WorkItemState.PENDING,),
        idempotency=(record,),
    )
    scanner, writer, _reader = _scanner(evidence)
    scan = scanner.scan(RUN_ID)
    assert scan.status is RecoveryStatus.HEALTHY
    finding = next(
        finding
        for finding in scan.findings
        if finding.kind is RecoveryFindingKind.STRANDED_IDEMPOTENCY
    )
    assert finding.detail == "in-progress reservation preserved as evidence"
    assert scanner.recover(RUN_ID).applied == 0
    assert writer.commands == []


def test_non_progress_idempotency_evidence_is_corrupt() -> None:
    record = IdempotencyRecord(
        "scope",
        "key",
        IdempotencyStatus.COMPLETED,
        1,
        RedactedDocument.from_mapping({"done": True}),
        CLAIMED,
        EXPIRED,
        EXPIRED,
    )
    evidence = _evidence(
        idempotency=(record,),
        work=(_work(state=WorkItemState.PENDING, lease_expires_at=None),),
        work_states=(WorkItemState.PENDING,),
    )
    scanner, _writer, _reader = _scanner(evidence)
    scan = scanner.scan(RUN_ID)
    assert scan.status is RecoveryStatus.AMBIGUOUS
    assert RecoveryFindingKind.EVENT_FRONTIER_CORRUPT in {finding.kind for finding in scan.findings}


def test_repeat_scan_is_deterministic_and_recovery_is_idempotent() -> None:
    evidence = _evidence(work=(_work(),))
    scanner, writer, reader = _scanner(evidence)
    first = scanner.scan(RUN_ID)
    second = scanner.scan(RUN_ID)
    assert first == second
    scanner.recover(RUN_ID)
    recovered = _evidence(
        work=(_work(state=WorkItemState.RETRY_WAIT, lease_expires_at=None),),
        work_states=(WorkItemState.RETRY_WAIT,),
    )
    reader.evidence = recovered
    report = scanner.recover(RUN_ID)
    assert report.applied == 0
    assert len(writer.commands) == 1
    assert report.after.status is RecoveryStatus.HEALTHY


def test_paused_run_reports_pause_boundary() -> None:
    evidence = _evidence(
        run=_run(state=RunState.PAUSED),
        work=(_work(state=WorkItemState.PENDING, lease_expires_at=None),),
        work_states=(WorkItemState.PENDING,),
    )
    scanner, _writer, _reader = _scanner(evidence)
    scan = scanner.scan(RUN_ID)
    assert scan.status is RecoveryStatus.HEALTHY
    assert RecoveryFindingKind.RUN_PAUSED in {finding.kind for finding in scan.findings}


def test_aggregate_drift_fails_closed() -> None:
    evidence = _evidence(work=(_work(),))
    drifted_node = replace(_node(work_states=(WorkItemState.PENDING,)), work_total=9)
    object.__setattr__(evidence.frontier, "nodes", (drifted_node,))
    scanner, _writer, _reader = _scanner(evidence)
    scan = scanner.scan(RUN_ID)
    assert scan.status is RecoveryStatus.AMBIGUOUS
    assert RecoveryFindingKind.AGGREGATE_MISMATCH in {finding.kind for finding in scan.findings}


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (WriterAdmissionTimeoutError(), RecoveryRejectedError.__mro__[3]),
        (WriterDefinitelyNotExecutedError(), RecoveryRejectedError),
        (WriterResultTimeoutError(), RecoveryOutcomeUnknownError),
    ],
)
def test_writer_failure_classification(
    failure: BaseException, expected: type[BaseException]
) -> None:
    evidence = _evidence(work=(_work(),))
    scanner, writer, _reader = _scanner(evidence)
    if isinstance(failure, WriterAdmissionTimeoutError):
        writer.admission_failures[1] = failure
        with pytest.raises(RecoveryRejectedError.__mro__[3], match="admission"):
            scanner.recover(RUN_ID)
    else:
        writer.result_failures[1] = failure
        with pytest.raises(expected):
            scanner.recover(RUN_ID)


def test_base_exceptions_propagate_and_poison() -> None:
    evidence = _evidence(work=(_work(),))
    scanner, writer, _reader = _scanner(evidence)
    fatal = KeyboardInterrupt("interrupted")
    writer.result_failures[1] = fatal
    with pytest.raises(KeyboardInterrupt) as captured:
        scanner.recover(RUN_ID)
    assert captured.value is fatal
    with pytest.raises(RecoveryOutcomeUnknownError, match="outcome inspection"):
        scanner.scan(RUN_ID)


def test_clock_failures_are_typed() -> None:
    clock = _Clock()
    evidence = _evidence(
        work=(_work(state=WorkItemState.PENDING, lease_expires_at=None),),
        work_states=(WorkItemState.PENDING,),
    )
    scanner, _writer, _reader = _scanner(evidence, clock=clock)
    clock.value = RuntimeError("clock broke")
    with pytest.raises(RecoveryClockError, match="clock failed"):
        scanner.scan(RUN_ID)
    clock.value = _time(1)
    with pytest.raises(RecoveryClockError, match="behind durable"):
        scanner.scan(RUN_ID)


def test_state_read_failures_are_typed() -> None:
    evidence = _evidence(
        work=(_work(state=WorkItemState.PENDING, lease_expires_at=None),),
        work_states=(WorkItemState.PENDING,),
    )
    scanner, _writer, reader = _scanner(evidence)
    reader.failure = RuntimeError("credential=secret C:\\machine")
    with pytest.raises(RecoveryStateReadError, match="read failed"):
        scanner.scan(RUN_ID)
    from paritygrid.application.ports.execution import ExecutionCorruptionError

    reader.failure = ExecutionCorruptionError("corrupt rows")
    with pytest.raises(RecoveryCorruptionError, match="corrupt"):
        scanner.scan(RUN_ID)
    reader.failure = None
    reader.evidence = cast(Any, object())
    with pytest.raises(RecoveryOutcomeUnknownError, match="evidence is invalid"):
        scanner.scan(RUN_ID)


def test_overlapping_operations_are_rejected() -> None:
    evidence = _evidence(
        work=(_work(state=WorkItemState.PENDING, lease_expires_at=None),),
        work_states=(WorkItemState.PENDING,),
    )
    scanner, _writer, _reader = _scanner(evidence)
    lock = cast(Any, scanner)._operation_lock
    lock.acquire()
    try:
        with pytest.raises(RecoveryBusyError, match="active operation"):
            scanner.scan(RUN_ID)
        with pytest.raises(RecoveryBusyError, match="active operation"):
            scanner.recover(RUN_ID)
    finally:
        lock.release()


def test_constructor_and_settings_validation() -> None:
    writer = _Writer()
    reader = _Reader(_evidence())
    clock = _Clock()
    with pytest.raises(TypeError, match="writer"):
        StartupRecoveryScanner(object(), reader, clock)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="reader"):
        StartupRecoveryScanner(writer, object(), clock)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="clock"):
        StartupRecoveryScanner(writer, reader, object())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="settings"):
        StartupRecoveryScanner(writer, reader, clock, settings=object())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="supported range"):
        RecoverySettings(max_findings=0)
    with pytest.raises(TypeError, match="float"):
        RecoverySettings(admission_timeout_seconds=1)  # type: ignore[arg-type]


def test_finding_and_scan_validation() -> None:
    with pytest.raises(TypeError, match="kind"):
        RecoveryFinding(cast(Any, "expired"), RUN_ID)
    with pytest.raises(TypeError, match="run identity"):
        RecoveryFinding(RecoveryFindingKind.WORK_PENDING, cast(Any, "run"))
    with pytest.raises(ValueError, match="detail"):
        RecoveryFinding(RecoveryFindingKind.WORK_PENDING, RUN_ID, detail="x" * 257)
    findings = (
        RecoveryFinding(RecoveryFindingKind.WORK_PENDING, RUN_ID, NODE_A, WORK_A),
        RecoveryFinding(RecoveryFindingKind.WORK_COMMITTED, RUN_ID, NODE_A, WORK_A),
    )
    with pytest.raises(ValueError, match="deterministically ordered"):
        RecoveryScan(RUN_ID, OBSERVED, RecoveryStatus.HEALTHY, findings)
    scan = RecoveryScan(RUN_ID, OBSERVED, RecoveryStatus.HEALTHY, tuple(reversed(findings)))
    assert scan.recoverable_findings == ()
    assert "RecoveryScan(" in repr(scan)


def test_evidence_validation_and_repr() -> None:
    evidence = _evidence()
    assert "RecoveryEvidence(" in repr(evidence)
    with pytest.raises(TypeError, match="frontier"):
        RecoveryEvidence(cast(Any, object()), (), (), ())
    with pytest.raises(TypeError, match="artifacts"):
        RecoveryEvidence(evidence.frontier, cast(Any, []), (), ())
    with pytest.raises(ValueError, match="idempotency"):
        RecoveryEvidence(
            evidence.frontier,
            (),
            (),
            cast(Any, tuple(range(10_001))),
        )


def test_recovery_requires_exact_run_identity() -> None:
    evidence = _evidence(
        work=(_work(state=WorkItemState.PENDING, lease_expires_at=None),),
        work_states=(WorkItemState.PENDING,),
    )
    scanner, _writer, _reader = _scanner(evidence)
    with pytest.raises(Exception, match="run identity"):
        scanner.scan(cast(Any, "run_recov-test"))
    with pytest.raises(RecoveryInvalidRequestError, match="correlation"):
        scanner.recover(RUN_ID, correlation_id="not portable!")
