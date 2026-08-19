# pyright: reportPrivateUsage=false
"""Adversarial evidence and failure-path tests for startup recovery."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from paritygrid.application.execution import (
    FinalizationEvidence,
    RecoveryEvidence,
    RecoveryFinding,
    RecoveryFindingKind,
    RecoveryOutcomeUnknownError,
    RecoveryRejectedError,
    RecoveryScan,
    RecoverySettings,
    RecoveryStatus,
    StartupRecoveryScanner,
)
from paritygrid.application.execution.recovery import _INTEGRITY_KIND_MAP
from paritygrid.application.ports.artifact_integrity import ArtifactIntegrityIssueKind
from paritygrid.application.ports.consistency import EventSequence
from paritygrid.application.ports.execution import (
    RunNodeRecord,
    RunNodeStatus,
    RunRecord,
    WorkItemRecord,
    WorkItemState,
)
from paritygrid.application.ports.writer import (
    WriterClosedError,
    WriterCommandKind,
    WriterSubmissionId,
)
from paritygrid.application.writes import RecoverExpiredWorkResult
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

RUN_ID = RunId("run_recov-test")
NODE_A = NodeId("nod_recov-a")
WORK_A = WorkItemId("wrk_recov-a")
OBSERVED = UtcTimestamp(datetime(2025, 3, 1, 0, 0, 30, tzinfo=UTC))
EXPIRED = UtcTimestamp(datetime(2025, 3, 1, 0, 0, 20, tzinfo=UTC))
CLAIMED = UtcTimestamp(datetime(2025, 3, 1, 0, 0, 10, tzinfo=UTC))
WORK_CREATED = UtcTimestamp(datetime(2025, 3, 1, 0, 0, 3, tzinfo=UTC))


def _time(second: int) -> UtcTimestamp:
    return UtcTimestamp(
        datetime(2025, 3, 1, 0, 0, second % 60, tzinfo=UTC) + timedelta(minutes=second // 60)
    )


def _run() -> RunRecord:
    from paritygrid.application.ports.configuration import ConfigurationDocument

    return RunRecord(
        RUN_ID,
        PipelineId("pip_recov-test"),
        PipelineVersion(1),
        "sequential",
        ConfigurationDocument(()),
        RunState.RUNNING,
        6,
        None,
        _time(1),
        _time(2),
        None,
        None,
        None,
        None,
        None,
    )


def _running_work() -> WorkItemRecord:
    from paritygrid.domain.models import AttemptNumber

    return WorkItemRecord(
        WORK_A,
        RUN_ID,
        NODE_A,
        PartitionKey("part-wrk_recov-a"),
        WorkItemState.RUNNING,
        2,
        0,
        0,
        None,
        None,
        "recov-owner",
        EXPIRED,
        AttemptNumber(1),
        CLAIMED,
        "sequential",
        "recov-worker",
        _time(3),
        CLAIMED,
    )


def _node() -> RunNodeRecord:
    from paritygrid.domain.models import Duration

    return RunNodeRecord(
        RUN_ID,
        NODE_A,
        RunNodeStatus.RUNNING,
        3,
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
        _time(2),
        None,
    )


def _evidence() -> RecoveryEvidence:
    work = _running_work()
    return RecoveryEvidence(
        FinalizationEvidence(
            _run(),
            EventSequence(7),
            7,
            (_node(),),
            (work,),
            (),
            ((WORK_A, 0),),
        ),
        (),
        (),
        (),
    )


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
        if isinstance(self._outcome, _IdentityFailure):
            raise self._outcome.failure
        return self._submission_id

    def result(self, *, timeout_seconds: float) -> Any:
        assert timeout_seconds == 60.0
        if isinstance(self._outcome, BaseException):
            raise self._outcome
        if callable(self._outcome):
            return self._outcome()
        return self._outcome

    async def result_async(self, *, timeout_seconds: float) -> Any:
        return self.result(timeout_seconds=timeout_seconds)


class _IdentityFailure:
    def __init__(self, failure: BaseException) -> None:
        self.failure = failure


class _RaisingIdentityTicket:
    @property
    def submission_id(self) -> WriterSubmissionId:
        raise RuntimeError("identity unavailable")

    def result(self, *, timeout_seconds: float) -> Any:
        raise AssertionError("result must not be reached")

    async def result_async(self, *, timeout_seconds: float) -> Any:
        raise AssertionError("result must not be reached")


class _Writer:
    def __init__(self) -> None:
        self.commands: list[Any] = []
        self.result_failures: dict[int, BaseException] = {}
        self.admission_failures: dict[int, BaseException] = {}
        self.ticket_overrides: dict[int, object] = {}
        self.receipt_mutators: dict[int, Any] = {}

    def submit(self, command: Any, *, timeout_seconds: float) -> Any:
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

    def _receipt(self, command: Any, submission_id: WriterSubmissionId, index: int) -> Any:
        from paritygrid.application.ports.consistency import (
            ExecutionEventBatch,
            ExecutionEventRecord,
        )
        from paritygrid.application.ports.execution import CompletedWork
        from paritygrid.application.writes import RecoverExpiredWorkResult
        from paritygrid.domain.execution import FailureClassification
        from paritygrid.domain.models import Duration

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
        events = ExecutionEventBatch(
            (record,),
            command.event.expected_next_sequence.advance(1),
            command.event.expected_counter_row_version + 1,
        )
        recovered = replace(
            _running_work(),
            state=WorkItemState.RETRY_WAIT,
            row_version=command.expected_work_row_version + 1,
            completed_attempt_count=1,
            lease_owner=None,
            lease_expires_at=None,
            active_attempt_number=None,
            active_attempt_started_at=None,
            active_runner_kind=None,
            active_worker_identity=None,
        )
        attempt_record = replace(
            recovered,
            work_item_id=recovered.work_item_id,
        )
        del attempt_record
        from paritygrid.application.ports.execution import (
            AttemptOutcome,
            WorkAttemptRecord,
        )

        attempt = WorkAttemptRecord(
            command.work_item_id,
            command.expected_attempt_number,
            CLAIMED,
            command.observed_at,
            "sequential",
            "recov-worker",
            AttemptOutcome.LEASE_EXPIRED,
            FailureClassification.TIMEOUT,
            None,
            None,
            0,
            0,
            Duration(0),
        )
        node = replace(
            _node(),
            status=RunNodeStatus.RUNNING,
            row_version=command.expected_node_row_version + 1,
            work_running=0,
            work_pending=1,
        )
        run = replace(_run(), row_version=command.expected_run_row_version + 1)
        receipt = _Receipt(
            submission_id,
            RecoverExpiredWorkResult(CompletedWork(recovered, attempt), node, events, run),
        )
        mutator = self.receipt_mutators.get(index)
        if mutator is not None:
            return mutator(receipt.value)
        return receipt.value


class _Receipt:
    def __init__(self, submission_id: WriterSubmissionId, result: Any) -> None:
        from paritygrid.application.ports.writer import WriterCommandKind, WriterReceipt

        self.value = WriterReceipt(
            submission_id,
            WriterCommandKind.RECOVER_EXPIRED_WORK,
            RUN_ID,
            0,
            True,
            result,
        )


class _Reader:
    def __init__(self, evidence: RecoveryEvidence) -> None:
        self.evidence = evidence
        self.failure: BaseException | None = None

    def read(self, run_id: RunId, /) -> RecoveryEvidence:
        assert run_id == RUN_ID
        if self.failure is not None:
            raise self.failure
        return self.evidence


def _scanner(
    evidence: RecoveryEvidence | None = None,
    *,
    clock: _Clock | None = None,
    writer: _Writer | None = None,
) -> tuple[StartupRecoveryScanner, _Writer, _Reader]:
    selected_writer = writer or _Writer()
    reader = _Reader(evidence or _evidence())
    selected_clock = clock or _Clock()
    scanner = StartupRecoveryScanner(
        selected_writer,
        reader,
        selected_clock,
        settings=RecoverySettings(),
    )
    return scanner, selected_writer, reader


def test_integrity_map_covers_the_closed_issue_kinds() -> None:
    assert set(_INTEGRITY_KIND_MAP) == {kind.value for kind in ArtifactIntegrityIssueKind}


def test_writer_closed_admission_is_rejected_without_poisoning() -> None:
    scanner, writer, _reader = _scanner()
    writer.admission_failures[1] = WriterClosedError("closed")
    with pytest.raises(RecoveryRejectedError.__mro__[3], match="admission"):
        scanner.recover(RUN_ID)


def test_unexpected_admission_and_ticket_failures_are_typed() -> None:
    scanner, writer, _reader = _scanner()
    writer.admission_failures[1] = RuntimeError("credential=secret C:\\machine")
    with pytest.raises(RecoveryOutcomeUnknownError, match="admission outcome"):
        scanner.recover(RUN_ID)

    fatal_scanner, fatal_writer, _r2 = _scanner()
    fatal_writer.admission_failures[1] = KeyboardInterrupt("interrupted submit")
    with pytest.raises(KeyboardInterrupt):
        fatal_scanner.recover(RUN_ID)

    identity_scanner, identity_writer, _r3 = _scanner()
    identity_writer.ticket_overrides[1] = _RaisingIdentityTicket()
    with pytest.raises(RecoveryOutcomeUnknownError, match="ticket identity"):
        identity_scanner.recover(RUN_ID)


def test_unexpected_result_failure_is_unknown() -> None:
    scanner, writer, _reader = _scanner()
    writer.result_failures[1] = RuntimeError("result broke")
    with pytest.raises(RecoveryOutcomeUnknownError, match="durable outcome"):
        scanner.recover(RUN_ID)


_RECEIPT_CORRUPTIONS: list[str] = [
    "receipt_type",
    "submission_identity",
    "command_kind",
    "run_identity",
    "contention_type",
    "contention_range",
    "mutated_false",
    "result_type",
    "work_state",
    "work_row_version",
    "attempt_outcome",
    "run_row_version",
    "events_mismatch",
]


@pytest.mark.parametrize("kind", _RECEIPT_CORRUPTIONS)
def test_receipt_evidence_corruption_fails_closed(kind: str) -> None:
    scanner, writer, _reader = _scanner()
    writer.receipt_mutators[1] = _mutate_receipt(kind)
    with pytest.raises(RecoveryOutcomeUnknownError, match="receipt"):
        scanner.recover(RUN_ID)


def _mutate_receipt(kind: str) -> Any:
    from dataclasses import replace as _replace

    def mutate(receipt: Any) -> Any:
        if kind == "receipt_type":
            return object()
        if kind == "submission_identity":
            return _replace(receipt, submission_id=WriterSubmissionId(99))
        if kind == "command_kind":
            return _replace(receipt, command_kind=cast(Any, WriterCommandKind.CLAIM_WORK))
        if kind == "run_identity":
            return _replace(receipt, run_id=RunId("run_other"))
        if kind == "contention_type":
            return _replace(receipt, contention_attempts=cast(Any, True))
        if kind == "contention_range":
            return _replace(receipt, contention_attempts=10)
        if kind == "mutated_false":
            return _replace(receipt, mutated=False)
        if kind == "result_type":
            return _replace(receipt, result=cast(Any, object()))
        result = receipt.result
        work = result.completed.work_item
        if kind == "work_state":
            from paritygrid.application.ports.execution import WorkItemState

            work = _replace(work, state=WorkItemState.PENDING)
        elif kind == "work_row_version":
            work = _replace(work, row_version=99)
        elif kind == "attempt_outcome":
            from paritygrid.application.ports.execution import AttemptOutcome

            attempt = _replace(result.completed.attempt, outcome=AttemptOutcome.FAILED)
            from paritygrid.application.ports.execution import CompletedWork

            return _replace(
                receipt,
                result=RecoverExpiredWorkResult(
                    CompletedWork(work, attempt), result.node, result.events, result.run
                ),
            )
        elif kind == "run_row_version":
            run = _replace(result.run, row_version=99)
            from paritygrid.application.ports.execution import CompletedWork

            return _replace(
                receipt,
                result=RecoverExpiredWorkResult(result.completed, result.node, result.events, run),
            )
        elif kind == "events_mismatch":
            from paritygrid.application.ports.consistency import RedactedDocument

            events = _replace(
                result.events,
                items=(
                    _replace(
                        result.events.items[0], payload=RedactedDocument.from_mapping({"x": 1})
                    ),
                ),
            )
            from paritygrid.application.ports.execution import CompletedWork

            return _replace(
                receipt,
                result=RecoverExpiredWorkResult(result.completed, result.node, events, result.run),
            )
        from paritygrid.application.ports.execution import CompletedWork

        return _replace(
            receipt,
            result=RecoverExpiredWorkResult(
                CompletedWork(work, result.completed.attempt),
                result.node,
                result.events,
                result.run,
            ),
        )

    return mutate


_DURABLE_CORRUPTIONS: list[str] = [
    "frontier_type",
    "artifacts_type",
    "integrity_type",
    "idempotency_type",
    "run_type",
    "run_identity",
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
    "sequence_type",
    "counter_type",
    "counter_range",
    "nodes_type",
    "node_type",
    "node_identity",
    "node_status",
    "node_row_version",
    "node_started_at",
    "node_finished_at",
    "work_type",
    "work_identity",
    "work_state",
    "work_row_version",
    "work_created_at",
    "work_updated_at",
    "work_retry_available_at",
    "work_lease_owner",
    "work_lease_expires_at",
    "work_active_attempt",
    "work_active_started_at",
    "work_active_runner",
    "work_active_worker",
    "attempts_type",
    "checkpoints_type",
]


@pytest.mark.parametrize("kind", _DURABLE_CORRUPTIONS)
def test_durable_evidence_corruption_fails_closed(kind: str) -> None:
    scanner, _writer, reader = _scanner()
    reader.evidence = _corrupt_evidence(kind)
    with pytest.raises(RecoveryOutcomeUnknownError) as captured:
        scanner.scan(RUN_ID)
    assert "secret" not in str(captured.value)


def _corrupt_evidence(kind: str) -> RecoveryEvidence:
    evidence = _evidence()
    if kind == "frontier_type":
        object.__setattr__(evidence, "frontier", object())
    elif kind == "run_type":
        object.__setattr__(evidence.frontier, "run", object())
    elif kind == "artifacts_type":
        object.__setattr__(evidence, "artifacts", (object(),))
    elif kind == "integrity_type":
        object.__setattr__(evidence, "integrity_issues", (object(),))
    elif kind == "idempotency_type":
        object.__setattr__(evidence, "idempotency_in_progress", (object(),))
    else:
        fields: dict[str, tuple[str, object]] = {
            "run_identity": ("run_id", object()),
            "run_state": ("state", "running"),
            "run_row_version": ("row_version", 6.0),
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
        }
        if kind in fields:
            field, value = fields[kind]
            object.__setattr__(evidence.frontier.run, field, value)
        elif kind == "sequence_type":
            object.__setattr__(evidence.frontier, "next_event_sequence", object())
        elif kind == "counter_type":
            object.__setattr__(evidence.frontier, "event_counter_row_version", "7")
        elif kind == "counter_range":
            object.__setattr__(evidence.frontier, "event_counter_row_version", 0)
        elif kind == "nodes_type" or kind == "node_type":
            object.__setattr__(evidence.frontier, "nodes", (object(),))
        elif kind.startswith("node_"):
            node_fields: dict[str, tuple[str, object]] = {
                "node_identity": ("node_id", object()),
                "node_status": ("status", "running"),
                "node_row_version": ("row_version", 3.0),
                "node_started_at": ("started_at", object()),
                "node_finished_at": ("finished_at", object()),
            }
            field, value = node_fields[kind]
            object.__setattr__(evidence.frontier.nodes[0], field, value)
        elif kind == "work_type":
            object.__setattr__(evidence.frontier, "work", (object(),))
        elif kind.startswith("work_"):
            work_fields: dict[str, tuple[str, object]] = {
                "work_identity": ("work_item_id", object()),
                "work_state": ("state", "running"),
                "work_row_version": ("row_version", 2.0),
                "work_created_at": ("created_at", object()),
                "work_updated_at": ("updated_at", object()),
                "work_retry_available_at": ("retry_available_at", object()),
                "work_lease_owner": ("lease_owner", 42),
                "work_lease_expires_at": ("lease_expires_at", object()),
                "work_active_attempt": ("active_attempt_number", object()),
                "work_active_started_at": ("active_attempt_started_at", object()),
                "work_active_runner": ("active_runner_kind", 42),
                "work_active_worker": ("active_worker_identity", 42),
            }
            field, value = work_fields[kind]
            object.__setattr__(evidence.frontier.work[0], field, value)
        elif kind == "attempts_type":
            object.__setattr__(evidence.frontier, "attempts", (object(),))
        elif kind == "checkpoints_type":
            object.__setattr__(evidence.frontier, "checkpoint_versions", (object(),))
    return evidence


def test_clock_invalid_value_is_typed() -> None:
    clock = _Clock()
    scanner, _writer, _reader = _scanner(clock=clock)
    clock.value = "not a timestamp"
    with pytest.raises(Exception, match="invalid time"):
        scanner.scan(RUN_ID)


def test_recovering_foreign_finding_parents_is_a_protocol_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import paritygrid.application.execution.recovery as recovery_module

    scanner, writer, _reader = _scanner()

    def _orphan_finding(
        evidence: RecoveryEvidence,
        observed_at: Any,
        max_findings: int,
    ) -> Any:
        from paritygrid.application.execution import RecoveryScan

        finding = RecoveryFinding(
            RecoveryFindingKind.WORK_EXPIRED_NO_EFFECT,
            RUN_ID,
            NODE_A,
            WorkItemId("wrk_recov-missing"),
        )
        return RecoveryScan(RUN_ID, observed_at, RecoveryStatus.RECOVERABLE, (finding,))

    monkeypatch.setattr(recovery_module, "_classify", _orphan_finding)
    assert scanner.scan(RUN_ID).status is RecoveryStatus.RECOVERABLE
    with pytest.raises(RecoveryOutcomeUnknownError, match="lacks durable parents"):
        scanner.recover(RUN_ID)
    assert writer.commands == []


def test_lifecycle_lock_failure_poisons_via_fallback() -> None:
    scanner, writer, _reader = _scanner()
    writer.result_failures[1] = RuntimeError("result broke")

    class _FlakyLock:
        def __init__(self) -> None:
            self.entered = 0

        def __enter__(self) -> None:
            self.entered += 1
            if self.entered > 1:
                raise RuntimeError("lock unavailable")

        def __exit__(self, *_args: object) -> None:
            return None

    cast(Any, scanner)._lifecycle_lock = _FlakyLock()
    with pytest.raises(RecoveryOutcomeUnknownError):
        scanner.recover(RUN_ID)
    assert cast(Any, scanner)._uncertain is True


def test_finding_detail_and_artifact_validation() -> None:
    with pytest.raises(TypeError, match="artifact identity"):
        RecoveryFinding(
            RecoveryFindingKind.INTEGRITY_MISSING_FILE,
            RUN_ID,
            artifact_id=cast(Any, 42),
        )
    with pytest.raises(TypeError, match="node identity"):
        RecoveryFinding(RecoveryFindingKind.WORK_PENDING, RUN_ID, node_id=cast(Any, 42))
    with pytest.raises(TypeError, match="work identity"):
        RecoveryFinding(RecoveryFindingKind.WORK_PENDING, RUN_ID, work_item_id=cast(Any, 42))
    with pytest.raises(TypeError, match="detail"):
        RecoveryFinding(RecoveryFindingKind.WORK_PENDING, RUN_ID, detail=cast(Any, 42))


def _healthy_evidence() -> RecoveryEvidence:
    work = replace(
        _running_work(),
        state=WorkItemState.PENDING,
        lease_owner=None,
        lease_expires_at=None,
        active_attempt_number=None,
        active_attempt_started_at=None,
        active_runner_kind=None,
        active_worker_identity=None,
        updated_at=WORK_CREATED,
    )
    node = replace(_node(), work_running=0, work_pending=1)
    return RecoveryEvidence(
        FinalizationEvidence(_run(), EventSequence(7), 7, (node,), (work,), (), ((WORK_A, 0),)),
        (),
        (),
        (),
    )


def test_scan_and_report_reprs_are_bounded() -> None:
    evidence = _evidence()
    assert "RecoveryEvidence(" in repr(evidence)
    scanner, _writer, reader = _scanner()
    assert "RecoveryScan(" in repr(scanner.scan(RUN_ID))
    reader.evidence = _healthy_evidence()
    report = scanner.recover(RUN_ID)
    assert "RecoveryReport(" in repr(report)
    assert "applied=" in repr(report)


def test_scan_post_init_validates_status_and_findings() -> None:
    findings = (RecoveryFinding(RecoveryFindingKind.WORK_PENDING, RUN_ID),)
    with pytest.raises(TypeError, match="status"):
        RecoveryScan(RUN_ID, OBSERVED, cast(Any, "healthy"), findings)
    with pytest.raises(TypeError, match="findings"):
        RecoveryScan(RUN_ID, OBSERVED, RecoveryStatus.HEALTHY, cast(Any, list(findings)))


def test_recover_refuses_poisoned_scanner() -> None:
    scanner, writer, _reader = _scanner()
    writer.result_failures[1] = RuntimeError("result broke")
    object.__setattr__(cast(Any, scanner), "_uncertain", True)
    with pytest.raises(RecoveryOutcomeUnknownError, match="outcome inspection"):
        scanner.recover(RUN_ID)


def test_reader_corruption_error_reraises_verbatim() -> None:
    from paritygrid.application.execution import RecoveryCorruptionError

    scanner, _writer, reader = _scanner()
    reader.failure = RecoveryCorruptionError("storage reported corruption")
    with pytest.raises(RecoveryCorruptionError, match="storage reported corruption"):
        scanner.scan(RUN_ID)


def test_running_work_with_committed_checkpoint_is_ambiguous() -> None:
    evidence = _evidence()
    object.__setattr__(evidence.frontier, "checkpoint_versions", ((WORK_A, 1),))
    object.__setattr__(evidence.frontier.work[0], "expected_checkpoint_version", 1)
    scanner, _writer, _reader = _scanner(evidence)
    scan = scanner.scan(RUN_ID)
    assert scan.status is RecoveryStatus.AMBIGUOUS
    assert RecoveryFindingKind.CHECKPOINT_FRONTIER_MISMATCH in {f.kind for f in scan.findings}


def test_succeeded_work_without_checkpoint_is_ambiguous() -> None:
    work = replace(
        _running_work(),
        state=WorkItemState.SUCCEEDED,
        completed_attempt_count=1,
        lease_owner=None,
        lease_expires_at=None,
        active_attempt_number=None,
        active_attempt_started_at=None,
        active_runner_kind=None,
        active_worker_identity=None,
    )
    node = replace(
        _node(),
        work_running=0,
        work_succeeded=1,
        status=RunNodeStatus.SUCCEEDED,
        finished_at=OBSERVED,
    )
    evidence = RecoveryEvidence(
        FinalizationEvidence(_run(), EventSequence(7), 7, (node,), (work,), (), ((WORK_A, 0),)),
        (),
        (),
        (),
    )
    scanner, _writer, _reader = _scanner(evidence)
    scan = scanner.scan(RUN_ID)
    assert scan.status is RecoveryStatus.AMBIGUOUS
    assert RecoveryFindingKind.CHECKPOINT_FRONTIER_MISMATCH in {f.kind for f in scan.findings}


def test_aggregate_snapshot_generic_failure_is_a_protocol_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import paritygrid.application.execution.recovery as recovery_module

    scanner, _writer, _reader = _scanner()

    def _explode(*_args: object) -> object:
        raise RuntimeError("snapshot construction broke")

    monkeypatch.setattr(recovery_module, "RunStatisticsSourceSnapshot", _explode)
    with pytest.raises(RecoveryOutcomeUnknownError, match="aggregate validation input"):
        scanner.scan(RUN_ID)


def test_finding_limit_is_enforced() -> None:
    evidence = _healthy_evidence()
    object.__setattr__(
        evidence.frontier, "run", replace(_run(), state=RunState.QUEUED, started_at=None)
    )
    scanner, _writer, _reader = _scanner(evidence)
    object.__setattr__(cast(Any, scanner), "_settings", RecoverySettings(max_findings=1))
    with pytest.raises(Exception, match="exceed the bounded limit"):
        scanner.scan(RUN_ID)


def test_integrity_kind_corruption_fails_closed() -> None:
    from paritygrid.application.ports.artifact_integrity import (
        ArtifactIntegrityIssue,
        ArtifactIntegrityIssueKind,
    )
    from paritygrid.application.ports.artifacts import ArtifactRelativePath
    from paritygrid.domain.models import ArtifactId

    issue = ArtifactIntegrityIssue(
        ArtifactIntegrityIssueKind.MISSING_FILE,
        ArtifactRelativePath("runs/run/artifact.parquet"),
        ArtifactId("art_recov-1"),
        None,
    )
    object.__setattr__(issue, "kind", cast(Any, "missing_file"))
    evidence = _evidence()
    object.__setattr__(evidence, "integrity_issues", (issue,))
    scanner, _writer, _reader = _scanner(evidence)
    with pytest.raises(RecoveryOutcomeUnknownError, match="evidence is invalid"):
        scanner.scan(RUN_ID)


_IDEMPOTENCY_DOCUMENT_CORRUPTIONS: list[str] = [
    "document_type",
    "document_pair",
    "document_pair_length",
    "document_array",
    "document_nested",
    "document_value",
    "redacted_type",
]


@pytest.mark.parametrize("kind", _IDEMPOTENCY_DOCUMENT_CORRUPTIONS)
def test_idempotency_document_corruption_fails_closed(kind: str) -> None:
    from paritygrid.application.ports.configuration import (
        ConfigurationDocument,
        DocumentArray,
        NestedDocumentObject,
    )
    from paritygrid.application.ports.consistency import (
        IdempotencyRecord,
        IdempotencyStatus,
        RedactedDocument,
    )

    # Corrupt after construction so the value constructors stay satisfied.
    document = ConfigurationDocument(())
    response: Any = RedactedDocument(document)
    if kind == "document_type":
        object.__setattr__(document, "items", object())
    elif kind == "document_pair":
        object.__setattr__(document, "items", (("key", ("pair",)),))
    elif kind == "document_pair_length":
        object.__setattr__(document, "items", (("key",),))
    elif kind == "document_array":
        array = DocumentArray(())
        object.__setattr__(array, "values", (object(),))
        object.__setattr__(document, "items", (("key", array),))
    elif kind == "document_nested":
        nested = NestedDocumentObject(())
        object.__setattr__(nested, "items", (("inner", object()),))
        object.__setattr__(document, "items", (("key", nested),))
    elif kind == "document_value":
        object.__setattr__(document, "items", (("key", 1.5),))
    elif kind == "redacted_type":
        response = object()
    record = IdempotencyRecord(
        "scope",
        "key",
        IdempotencyStatus.IN_PROGRESS,
        1,
        response,
        CLAIMED,
        CLAIMED,
        None,
    )
    evidence = _evidence()
    object.__setattr__(evidence, "idempotency_in_progress", (record,))
    scanner, _writer, _reader = _scanner(evidence)
    with pytest.raises(RecoveryOutcomeUnknownError, match="evidence is invalid"):
        scanner.scan(RUN_ID)


def test_valid_idempotency_documents_snapshot_exactly() -> None:
    from paritygrid.application.ports.configuration import (
        ConfigurationDocument,
        DocumentArray,
        NestedDocumentObject,
    )
    from paritygrid.application.ports.consistency import (
        IdempotencyRecord,
        IdempotencyStatus,
        RedactedDocument,
    )

    record = IdempotencyRecord(
        "scope",
        "key",
        IdempotencyStatus.IN_PROGRESS,
        1,
        RedactedDocument(
            ConfigurationDocument(
                (
                    ("array", DocumentArray((1, "two"))),
                    ("nested", NestedDocumentObject((("inner", True),))),
                )
            )
        ),
        CLAIMED,
        CLAIMED,
        None,
    )
    evidence = _evidence()
    object.__setattr__(evidence, "idempotency_in_progress", (record,))
    scanner, _writer, _reader = _scanner(evidence)
    scan = scanner.scan(RUN_ID)
    assert RecoveryFindingKind.STRANDED_IDEMPOTENCY in {f.kind for f in scan.findings}


def test_counter_above_supported_range_fails_closed() -> None:
    evidence = _evidence()
    object.__setattr__(evidence.frontier, "event_counter_row_version", 2_147_483_648)
    scanner, _writer, _reader = _scanner(evidence)
    with pytest.raises(RecoveryOutcomeUnknownError, match="evidence is invalid"):
        scanner.scan(RUN_ID)


def test_finding_repr_is_bounded() -> None:
    finding = RecoveryFinding(RecoveryFindingKind.WORK_PENDING, RUN_ID, NODE_A, WORK_A)
    assert "RecoveryFinding(" in repr(finding)
    assert "recov-test" in repr(finding)


def test_ticket_with_malformed_identity_is_rejected() -> None:
    scanner, writer, _reader = _scanner()

    class _BadNumberTicket:
        @property
        def submission_id(self) -> Any:
            from paritygrid.application.ports.writer import WriterSubmissionId

            identity = WriterSubmissionId(1)
            object.__setattr__(identity, "number", cast(Any, "1"))
            return identity

        def result(self, *, timeout_seconds: float) -> Any:
            raise AssertionError("result must not be reached")

        async def result_async(self, *, timeout_seconds: float) -> Any:
            raise AssertionError("result must not be reached")

    writer.ticket_overrides[1] = _BadNumberTicket()
    with pytest.raises(RecoveryOutcomeUnknownError, match="ticket identity"):
        scanner.recover(RUN_ID)


def test_document_entry_non_tuple_corruption_fails_closed() -> None:
    from paritygrid.application.ports.configuration import ConfigurationDocument
    from paritygrid.application.ports.consistency import (
        IdempotencyRecord,
        IdempotencyStatus,
        RedactedDocument,
    )

    document = ConfigurationDocument(())
    record = IdempotencyRecord(
        "scope",
        "key",
        IdempotencyStatus.IN_PROGRESS,
        1,
        RedactedDocument(document),
        CLAIMED,
        CLAIMED,
        None,
    )
    object.__setattr__(document, "items", ("not-a-tuple",))
    evidence = _evidence()
    object.__setattr__(evidence, "idempotency_in_progress", (record,))
    scanner, _writer, _reader = _scanner(evidence)
    with pytest.raises(RecoveryOutcomeUnknownError, match="evidence is invalid"):
        scanner.scan(RUN_ID)


def test_settings_timeout_upper_bound_is_enforced() -> None:
    with pytest.raises(ValueError, match="supported range"):
        RecoverySettings(result_timeout_seconds=86_400.5)
