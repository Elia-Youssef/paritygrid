# pyright: reportPrivateUsage=false
"""Edge and defensive-branch coverage for the repair services."""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import cast

import pytest

from paritygrid.adapters.persistence.repair_workflow import SQLiteRepairWorkflowReader
from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter
from paritygrid.adapters.persistence.writer.dispatch import dispatch_command
from paritygrid.application.ports.connectors import (
    ConnectorCallContext,
    ConnectorCancelledError,
    ConnectorError,
    ConnectorRetryableError,
    ConnectorTimeoutError,
    TargetConnector,
    TargetRecord,
    TargetStateSnapshot,
    TargetWriteOutcome,
    TargetWriteRequest,
)
from paritygrid.application.ports.reconciliation_persistence import (
    ReconciliationResultRecord,
    TargetVerificationRecord,
)
from paritygrid.application.ports.repair_audit import (
    RepairPlanAggregate,
    RepairPlanStatus,
)
from paritygrid.application.ports.writer import (
    TransactionalWriter,
    WriterCommand,
    WriterCommandKind,
)
from paritygrid.application.reconciliation.analysis import ReconciliationAnalysis
from paritygrid.application.repair import (
    AppliedEffectEvidence,
    GeneratedRepairPlan,
    ObservedTargetPayload,
    ReconciliationResultService,
    RepairApplicationPolicy,
    RepairApplicationService,
    RepairApprovalRequest,
    RepairApprovalService,
    RepairPlanMismatchError,
    RepairPlanningService,
    RepairPlanStateError,
    RepairReconciliationMissingError,
    RepairReconciliationStaleError,
    RepairWorkflowEvidence,
    RepairWorkflowReader,
)
from paritygrid.application.repair.applier import _reconstruct_reservation
from paritygrid.application.repair.companions import (
    RepairCompanions,
    build_companions,
    frontier_from_evidence,
    submit_command,
)
from paritygrid.application.repair.errors import (
    RepairApprovalConflictError,
    RepairWriterOutcomeUnknownError,
)
from paritygrid.application.repair.errors import (
    RepairReconciliationStaleError as WorkflowStaleError,
)
from paritygrid.application.repair.identities import derive_plan_id
from paritygrid.application.repair.payloads import parse_observed_payload
from paritygrid.domain.models import (
    RepairPlanId,
    RunId,
    TargetVerificationId,
    UtcTimestamp,
)
from tests.repair.conftest import (
    RUN_ID,
    DeterministicClock,
    seed_terminal_run,
    wire_payload,
)
from tests.repair.test_applier import _IdempotentFakeTarget, _no_sleep
from tests.repair.test_service_branches import _result, _UnknownOutcomeProxy

pytestmark = pytest.mark.anyio


class _CancelOnWriteTarget(_IdempotentFakeTarget):
    """Cancel the call token from inside the write itself."""

    def __init__(self) -> None:
        super().__init__()
        from paritygrid.application.ports.connectors import EventCancellationToken

        self.token = EventCancellationToken()

    async def write_record_async(
        self, request: TargetWriteRequest, context: ConnectorCallContext
    ) -> TargetWriteOutcome:
        self.writes.append(request)
        self.token.cancel()
        raise ConnectorCancelledError("cancelled mid-write")


class _TransientTarget(_IdempotentFakeTarget):
    """Fail with a retryable connect error a fixed number of times."""

    def __init__(self, failures_remaining: int, error: BaseException) -> None:
        super().__init__()
        self._remaining = failures_remaining
        self._error = error

    async def write_record_async(
        self, request: TargetWriteRequest, context: ConnectorCallContext
    ) -> TargetWriteOutcome:
        self.writes.append(request)
        if self._remaining > 0:
            self._remaining -= 1
            raise self._error
        return await super().write_record_async(request, context)


class _GarbageReadTarget(_IdempotentFakeTarget):
    """Return an unparsable payload for every read."""

    async def read_record_async(
        self, sku: str, context: ConnectorCallContext
    ) -> TargetRecord | None:
        return TargetRecord(
            sku=sku,
            payload={"sku": sku, "unexpected": True},
            record_version=1,
            target_version=1,
        )


class _BareConnectorError(ConnectorError):
    """A connector failure outside every specific transport classification."""

    def __init__(self) -> None:
        super().__init__("unclassified connector failure")


class _CancelOnReadTarget(_IdempotentFakeTarget):
    """Cancel during the per-key observation loop."""

    def __init__(self) -> None:
        super().__init__()
        from paritygrid.application.ports.connectors import EventCancellationToken

        self.token = EventCancellationToken()

    async def read_record_async(
        self, sku: str, context: ConnectorCallContext
    ) -> TargetRecord | None:
        self.token.cancel()
        raise ConnectorCancelledError("cancelled mid-read")


class _FailingSnapshotTarget(_IdempotentFakeTarget):
    """Fail the snapshot after a successful constructor call."""

    def __init__(self, error: BaseException) -> None:
        super().__init__()
        self._error = error

    async def state_snapshot_async(self, context: ConnectorCallContext) -> TargetStateSnapshot:
        raise self._error


async def _approved(
    database: SQLiteDatabase,
    writer: SQLiteTransactionalWriter,
    reader: SQLiteRepairWorkflowReader,
    clock: DeterministicClock,
) -> ReconciliationAnalysis:
    from paritygrid.application.ports.consistency import RedactedDocument

    seed_terminal_run(database)
    result = _result()
    ReconciliationResultService(writer, reader, now=clock.now).persist(
        run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr"
    )
    created = RepairPlanningService(writer, reader, now=clock.now).create(
        run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr"
    )
    assert created.aggregate is not None
    RepairApprovalService(writer, reader, now=clock.now).approve(
        RepairApprovalRequest(
            run_id=RUN_ID,
            repair_plan_id=created.aggregate.plan.repair_plan_id,
            approved_by="approver-1",
            correlation_id="corr",
            approved_content_fingerprint=created.aggregate.plan.content_fingerprint,
            approved_reconciliation_fingerprint=result.summary.fingerprint,
            detail=RedactedDocument.from_mapping({"decision": "ok"}),
        )
    )
    return result


def _applier(
    writer: TransactionalWriter,
    reader: RepairWorkflowReader,
    clock: DeterministicClock,
    *,
    policy: RepairApplicationPolicy | None = None,
) -> RepairApplicationService:
    return RepairApplicationService(
        writer,
        reader,
        now=clock.now,
        policy=policy
        if policy is not None
        else RepairApplicationPolicy(delay_seconds=0.0, timeout_seconds=10.0),
        sleep=_no_sleep,
    )


class TestApplierDefensiveBranches:
    async def test_untyped_identities_and_missing_summary_are_rejected(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        seed_terminal_run(database)
        service = _applier(writer, reader, clock)
        target = cast(TargetConnector, _IdempotentFakeTarget())
        plan_id = derive_plan_id(RUN_ID, _result().summary.fingerprint)
        with pytest.raises(TypeError):
            await service.apply(
                run_id="run_text",  # type: ignore[arg-type]
                repair_plan_id=plan_id,
                target=target,
                context_id="corr",
            )
        with pytest.raises(RepairReconciliationMissingError):
            await service.apply(
                run_id=RUN_ID, repair_plan_id=plan_id, target=target, context_id="corr"
            )

    async def test_stale_reconciliation_at_entry_is_rejected(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        await _approved(database, writer, reader, clock)
        stale_reader = _StaleSummaryReader(reader)
        service = _applier(writer, stale_reader, clock)
        with pytest.raises(WorkflowStaleError):
            await service.apply(
                run_id=RUN_ID,
                repair_plan_id=derive_plan_id(RUN_ID, _result().summary.fingerprint),
                target=cast(TargetConnector, _IdempotentFakeTarget()),
                context_id="corr",
            )

    async def test_a_failed_action_under_an_applying_plan_is_rejected(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        # The repository mapping refuses to materialize this inconsistent
        # state, so the defensive applier branch is exercised through a
        # hand-built aggregate that carries it.
        await _approved(database, writer, reader, clock)
        plan_id = derive_plan_id(RUN_ID, _result().summary.fingerprint)
        corrupt_reader = _FailedActionReader(reader, plan_id)
        with pytest.raises(RepairPlanStateError, match="failed action"):
            await _applier(writer, corrupt_reader, clock).apply(
                run_id=RUN_ID,
                repair_plan_id=plan_id,
                target=cast(TargetConnector, _IdempotentFakeTarget()),
                context_id="corr",
            )

    async def test_cancelled_write_interrupts_without_recording(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        await _approved(database, writer, reader, clock)
        fake = _CancelOnWriteTarget()
        report = await _applier(writer, reader, clock).apply(
            run_id=RUN_ID,
            repair_plan_id=derive_plan_id(RUN_ID, _result().summary.fingerprint),
            target=cast(TargetConnector, fake),
            context_id="corr",
            cancellation=fake.token,
        )
        assert report.disposition.value == "interrupted"

    async def test_retryable_failure_retries_then_succeeds(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        await _approved(database, writer, reader, clock)
        fake = _TransientTarget(
            1, ConnectorRetryableError("connect refused", retry_after_seconds=None)
        )
        report = await _applier(writer, reader, clock).apply(
            run_id=RUN_ID,
            repair_plan_id=derive_plan_id(RUN_ID, _result().summary.fingerprint),
            target=cast(TargetConnector, fake),
            context_id="corr",
        )
        assert report.disposition.value == "completed"
        assert report.effects[0].attempts == 2

    async def test_timeout_exhaustion_records_a_terminal_failure(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        from paritygrid.application.repair.errors import TargetApplicationError

        await _approved(database, writer, reader, clock)
        fake = _TransientTarget(99, ConnectorTimeoutError("connect deadline exceeded"))
        with pytest.raises(TargetApplicationError, match="retry_exhausted"):
            await _applier(writer, reader, clock).apply(
                run_id=RUN_ID,
                repair_plan_id=derive_plan_id(RUN_ID, _result().summary.fingerprint),
                target=cast(TargetConnector, fake),
                context_id="corr",
            )

    async def test_unclassified_connector_error_suspends(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        await _approved(database, writer, reader, clock)
        fake = _TransientTarget(99, _BareConnectorError())
        report = await _applier(writer, reader, clock).apply(
            run_id=RUN_ID,
            repair_plan_id=derive_plan_id(RUN_ID, _result().summary.fingerprint),
            target=cast(TargetConnector, fake),
            context_id="corr",
        )
        assert report.disposition.value == "unresolved"

    def test_corrupt_applying_state_is_rejected(self) -> None:
        aggregate = _aggregate_stub(status=RepairPlanStatus.APPLYING, applying_at=None)
        with pytest.raises(RepairPlanStateError, match="corrupt"):
            _reconstruct_reservation(aggregate)

    async def test_effect_evidence_validates_its_shape(self) -> None:
        from paritygrid.domain.models import RepairActionId

        action = RepairActionId("rac_edge-case")
        valid = AppliedEffectEvidence(
            action_id=action,
            canonical_key="GRID-0001",
            outcome="applied",
            attempts=1,
            target_version=1,
        )
        assert valid.outcome == "applied"
        with pytest.raises(ValueError, match="outcome"):
            AppliedEffectEvidence(
                action_id=action,
                canonical_key="GRID-0001",
                outcome="",
                attempts=1,
                target_version=1,
            )
        with pytest.raises(ValueError, match="target version"):
            AppliedEffectEvidence(
                action_id=action,
                canonical_key="GRID-0001",
                outcome="applied",
                attempts=1,
                target_version=0,
            )


class _CompetingWriterProxy:
    """Pre-dispatch a competing winner for chosen command kinds."""

    def __init__(
        self,
        real: SQLiteTransactionalWriter,
        database: SQLiteDatabase,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
        *,
        compete_on: dict[WriterCommandKind, Callable[[WriterCommand, _CompetingWriterProxy], None]],
    ) -> None:
        self._real = real
        self._database = database
        self._reader = reader
        self._clock = clock
        self._compete_on = compete_on
        self._done: set[WriterCommandKind] = set()
        self.captured: list[WriterCommand] = []

    def start(self) -> None:
        return self._real.start()

    def close(self, *, timeout_seconds: float) -> object:
        return self._real.close(timeout_seconds=timeout_seconds)

    def submit(self, command: WriterCommand, *, timeout_seconds: float) -> object:
        self.captured.append(command)
        kind = command.kind
        if kind in self._compete_on and kind not in self._done:
            self._done.add(kind)
            builder = self._compete_on[kind]
            builder(command, self)
        return self._real.submit(command, timeout_seconds=timeout_seconds)

    async def submit_async(self, command: WriterCommand, *, timeout_seconds: float) -> object:
        return self.submit(command, timeout_seconds=timeout_seconds)

    def snapshot(self) -> object:
        return self._real.snapshot()

    def dispatch_competing(self, command: WriterCommand) -> None:
        with self._database.transaction() as session:
            dispatch_command(session, command)


def _companions_for(
    reader: SQLiteRepairWorkflowReader,
    run_id: RunId,
    operation: str,
    object_kind: str,
    object_id: str,
    occurred_at: UtcTimestamp,
    payload: dict[str, object],
) -> RepairCompanions:
    return build_companions(
        frontier=frontier_from_evidence(reader.load(run_id)),
        run_id=run_id,
        operation=operation,
        object_kind=object_kind,
        object_id=object_id,
        actor="competing-actor",
        correlation_id="corr-competing",
        occurred_at=occurred_at,
        payload=payload,
    )


class TestCompetingApplicationRaces:
    async def test_a_competing_begin_fences_the_loser(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        await _approved(database, writer, reader, clock)
        result = _result()
        plan_id = derive_plan_id(RUN_ID, result.summary.fingerprint)
        aggregate = reader.load_plan(plan_id)
        assert aggregate is not None

        def compete(_command: WriterCommand, proxy: _CompetingWriterProxy) -> None:
            from paritygrid.application.writes.repairs import BeginRepairApplication

            moment = clock.now()
            proxy.dispatch_competing(
                BeginRepairApplication(
                    run_id=RUN_ID,
                    repair_plan_id=plan_id,
                    expected_plan_row_version=aggregate.plan.row_version,
                    current_reconciliation_fingerprint=result.summary.fingerprint,
                    applying_at=moment,
                    companions=_companions_for(
                        reader,
                        RUN_ID,
                        "repair_application_started",
                        "repair_plan",
                        plan_id.value,
                        moment,
                        {"action_count": len(aggregate.actions)},
                    ),
                )
            )

        proxy = _CompetingWriterProxy(
            writer,
            database,
            reader,
            clock,
            compete_on={WriterCommandKind.BEGIN_REPAIR_APPLICATION: compete},
        )
        with pytest.raises(RepairPlanStateError, match="concurrent application"):
            await _applier(
                writer=cast("TransactionalWriter", proxy), reader=reader, clock=clock
            ).apply(
                run_id=RUN_ID,
                repair_plan_id=plan_id,
                target=cast(TargetConnector, _IdempotentFakeTarget()),
                context_id="corr",
            )

    async def test_a_competing_record_fences_the_loser(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        await _approved(database, writer, reader, clock)
        result = _result()
        plan_id = derive_plan_id(RUN_ID, result.summary.fingerprint)
        aggregate = reader.load_plan(plan_id)
        assert aggregate is not None
        assert aggregate.plan.applying_at is None

        # Begin first so the plan is applying with a durable reservation.
        from paritygrid.application.writes.repairs import BeginRepairApplication

        begin_moment = clock.now()
        with database.transaction() as session:
            dispatch_command(
                session,
                BeginRepairApplication(
                    run_id=RUN_ID,
                    repair_plan_id=plan_id,
                    expected_plan_row_version=aggregate.plan.row_version,
                    current_reconciliation_fingerprint=result.summary.fingerprint,
                    applying_at=begin_moment,
                    companions=_companions_for(
                        reader,
                        RUN_ID,
                        "repair_application_started",
                        "repair_plan",
                        plan_id.value,
                        begin_moment,
                        {"action_count": len(aggregate.actions)},
                    ),
                ),
            )
        begun = reader.load_plan(plan_id)
        assert begun is not None
        reservation = _reconstruct_reservation(begun)
        action = begun.actions[0]

        def compete(command: WriterCommand, proxy: _CompetingWriterProxy) -> None:
            from paritygrid.application.writes.repairs import RecordRepairActionApplied

            competing = cast("RecordRepairActionApplied", command)
            moment = clock.now()
            proxy.dispatch_competing(
                RecordRepairActionApplied(
                    run_id=RUN_ID,
                    reservation=competing.reservation,
                    repair_action_id=action.effect.action_id,
                    result=competing.result,
                    target_version=competing.target_version,
                    applied_at=moment,
                    companions=_companions_for(
                        reader,
                        RUN_ID,
                        "repair_action_applied",
                        "repair_action",
                        action.effect.action_id.value,
                        moment,
                        {"canonical_key": action.effect.proposed.sku},
                    ),
                )
            )

        proxy = _CompetingWriterProxy(
            writer,
            database,
            reader,
            clock,
            compete_on={WriterCommandKind.RECORD_REPAIR_ACTION_APPLIED: compete},
        )
        del reservation
        with pytest.raises(RepairPlanStateError, match="concurrent application"):
            await _applier(
                writer=cast("TransactionalWriter", proxy), reader=reader, clock=clock
            ).apply(
                run_id=RUN_ID,
                repair_plan_id=plan_id,
                target=cast(TargetConnector, _IdempotentFakeTarget()),
                context_id="corr",
            )


class TestApprovalDefensiveBranches:
    def test_approve_rejects_foreign_requests(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        seed_terminal_run(database)
        service = RepairApprovalService(writer, reader, now=clock.now)
        with pytest.raises(TypeError):
            service.approve(cast("RepairApprovalRequest", object()))
        from paritygrid.application.ports.consistency import RedactedDocument
        from paritygrid.domain.models import RepairPlanId, StateFingerprint

        request = RepairApprovalRequest(
            run_id=RUN_ID,
            repair_plan_id=RepairPlanId("rpl_missing"),
            approved_by="approver-1",
            correlation_id="corr",
            approved_content_fingerprint=StateFingerprint("1" * 64),
            approved_reconciliation_fingerprint=StateFingerprint("2" * 64),
            detail=RedactedDocument.from_mapping({"decision": "ok"}),
        )
        with pytest.raises(RepairReconciliationMissingError):
            service.approve(request)

    def test_reject_rejects_foreign_identities(
        self,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        service = RepairApprovalService(writer, reader, now=clock.now)
        with pytest.raises(TypeError):
            service.reject(
                run_id="run_text",  # type: ignore[arg-type]
                repair_plan_id=None,  # type: ignore[arg-type]
                correlation_id="corr",
            )

    async def test_a_competing_decision_fences_the_loser(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        seed_terminal_run(database)
        result = _result()
        ReconciliationResultService(writer, reader, now=clock.now).persist(
            run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr"
        )
        created = RepairPlanningService(writer, reader, now=clock.now).create(
            run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr"
        )
        assert created.aggregate is not None
        plan_id = created.aggregate.plan.repair_plan_id

        def compete(_command: WriterCommand, proxy: _CompetingWriterProxy) -> None:
            from paritygrid.application.writes.repairs import RejectRepairPlan

            moment = clock.now()
            proxy.dispatch_competing(
                RejectRepairPlan(
                    run_id=RUN_ID,
                    repair_plan_id=plan_id,
                    expected_plan_row_version=created.aggregate.plan.row_version
                    if created.aggregate is not None
                    else 1,
                    rejected_at=moment,
                    companions=_companions_for(
                        reader,
                        RUN_ID,
                        "repair_plan_rejected",
                        "repair_plan",
                        plan_id.value,
                        moment,
                        {},
                    ),
                )
            )

        proxy = _CompetingWriterProxy(
            writer,
            database,
            reader,
            clock,
            compete_on={WriterCommandKind.APPROVE_REPAIR_PLAN: compete},
        )
        from paritygrid.application.ports.consistency import RedactedDocument
        from paritygrid.domain.models import StateFingerprint

        with pytest.raises((RepairPlanStateError, RepairApprovalConflictError)):
            RepairApprovalService(
                cast("TransactionalWriter", proxy), reader, now=clock.now
            ).approve(
                RepairApprovalRequest(
                    run_id=RUN_ID,
                    repair_plan_id=plan_id,
                    approved_by="approver-1",
                    correlation_id="corr",
                    approved_content_fingerprint=StateFingerprint(
                        created.aggregate.plan.content_fingerprint.value
                    ),
                    approved_reconciliation_fingerprint=result.summary.fingerprint,
                    detail=RedactedDocument.from_mapping({"decision": "ok"}),
                )
            )

    def test_another_runs_plan_cannot_be_approved(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        from paritygrid.application.ports.consistency import RedactedDocument
        from paritygrid.domain.models import RepairPlanId, RunId, StateFingerprint

        result = _result()
        seed_terminal_run(database)
        ReconciliationResultService(writer, reader, now=clock.now).persist(
            run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr"
        )
        created = RepairPlanningService(writer, reader, now=clock.now).create(
            run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr"
        )
        assert created.aggregate is not None
        other = RunId("run_phase11-foreign")
        seed_terminal_run(database, other, seed_pipeline=False)
        ReconciliationResultService(writer, reader, now=clock.now).persist(
            run_id=other, analysis=result, actor="operator-1", correlation_id="corr"
        )
        service = RepairApprovalService(writer, reader, now=clock.now)
        with pytest.raises(RepairPlanMismatchError, match="another run"):
            service.approve(
                RepairApprovalRequest(
                    run_id=other,
                    repair_plan_id=created.aggregate.plan.repair_plan_id,
                    approved_by="approver-1",
                    correlation_id="corr",
                    approved_content_fingerprint=StateFingerprint(
                        created.aggregate.plan.content_fingerprint.value
                    ),
                    approved_reconciliation_fingerprint=result.summary.fingerprint,
                    detail=RedactedDocument.from_mapping({"decision": "ok"}),
                )
            )
        del RepairPlanId


class _StaleSummaryReader:
    """A reader wrapper that reports a foreign current reconciliation identity."""

    def __init__(self, inner: SQLiteRepairWorkflowReader) -> None:
        self._inner = inner

    def load(self, run_id: RunId) -> RepairWorkflowEvidence:
        from paritygrid.domain.models import StateFingerprint

        evidence = self._inner.load(run_id)
        if evidence.summary is None:
            return evidence
        from dataclasses import replace

        return replace(
            evidence,
            summary=replace(
                evidence.summary,
                reconciliation_fingerprint=StateFingerprint("f" * 64),
            ),
        )

    def load_plan(self, repair_plan_id: RepairPlanId) -> RepairPlanAggregate | None:
        return self._inner.load_plan(repair_plan_id)

    def load_reconciliation_result(self, run_id: RunId) -> ReconciliationResultRecord | None:
        return self._inner.load_reconciliation_result(run_id)

    def load_target_verification(
        self, verification_id: TargetVerificationId
    ) -> TargetVerificationRecord | None:
        return self._inner.load_target_verification(verification_id)


class TestVerificationDefensiveBranches:
    def test_expected_inventory_validates_its_shape(self) -> None:
        from paritygrid.application.repair import ExpectedInventory
        from tests.repair.conftest import record_for

        records = (record_for("GRID-0002"), record_for("GRID-0001"))
        with pytest.raises(ValueError, match="sorted unique"):
            ExpectedInventory(records=records, absent_keys=(), ambiguous_keys=())
        with pytest.raises(ValueError, match="fingerprint bound"):
            ExpectedInventory(
                records=tuple(record_for(f"GRID-{index:05d}") for index in range(10_001)),
                absent_keys=(),
                ambiguous_keys=(),
            )

    def test_observed_report_validates_its_shape(self) -> None:
        from paritygrid.application.repair import (
            TargetObservationDisposition,
            TargetVerificationReport,
        )
        from paritygrid.domain.models import StateFingerprint, UtcTimestamp

        moment = UtcTimestamp(datetime(2026, 8, 27, 12, 0, 0, tzinfo=UTC))
        with pytest.raises(ValueError, match="verdict and identity"):
            TargetVerificationReport(
                disposition=TargetObservationDisposition.OBSERVED,
                verdict=None,
                observed=None,
                expected_fingerprint=StateFingerprint("1" * 64),
                expected_record_count=0,
                observed_record_count=0,
                divergences=(),
                observed_target_version=0,
                observed_at=moment,
                detail=None,
            )

    async def test_observation_edges(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        from paritygrid.application.repair import (
            TargetObservationDisposition,
            TargetParityVerifier,
            build_expected_inventory,
        )

        result = await _approved(database, writer, reader, clock)
        inventory = build_expected_inventory(result, None)
        verifier = TargetParityVerifier(now=clock.now)
        garbage = cast(TargetConnector, _GarbageReadTarget())
        report = await verifier.verify(target=garbage, inventory=inventory, context_id="corr")
        assert report.verdict is not None
        assert report.verdict.value == "parity_divergent"
        assert any(
            divergence.reason == "target record is unparsable" for divergence in report.divergences
        )

        cancel_read = _CancelOnReadTarget()
        cancelled = await verifier.verify(
            target=cast(TargetConnector, cancel_read),
            inventory=inventory,
            context_id="corr",
            cancellation=cancel_read.token,
        )
        assert cancelled.disposition is TargetObservationDisposition.INTERRUPTED

        failing = _FailingSnapshotTarget(_BareConnectorError())
        failed = await verifier.verify(
            target=cast(TargetConnector, failing), inventory=inventory, context_id="corr"
        )
        assert failed.disposition is TargetObservationDisposition.OBSERVATION_FAILED

    async def test_unexpected_present_record_breaks_parity(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        from paritygrid.application.repair import TargetParityVerifier, build_expected_inventory

        result = await _approved(database, writer, reader, clock)
        inventory = build_expected_inventory(result, None)
        assert inventory.absent_keys
        fake = _IdempotentFakeTarget()
        verifier = TargetParityVerifier(now=clock.now)
        for key in inventory.absent_keys:
            await fake.write_record_async(
                TargetWriteRequest(
                    sku=key,
                    payload=wire_payload(key),
                    idempotency_key=f"unexpected-{key}",
                ),
                ConnectorCallContext(correlation_id="corr"),
            )
        report = await verifier.verify(
            target=cast(TargetConnector, fake),
            inventory=inventory,
            context_id="corr",
        )
        assert any(
            divergence.reason == "unexpected target record is present"
            for divergence in report.divergences
        )

    async def test_recording_validates_its_inputs_and_fences(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        from paritygrid.application.ports.consistency import RedactedDocument
        from paritygrid.application.repair import (
            TargetObservationDisposition,
            TargetParityVerifier,
            TargetVerificationService,
            build_expected_inventory,
        )
        from paritygrid.domain.models import RepairPlanId, StateFingerprint

        result = await _approved(database, writer, reader, clock)
        service = TargetVerificationService(writer, reader, now=clock.now)
        report = await TargetParityVerifier(now=clock.now).verify(
            target=cast(TargetConnector, _IdempotentFakeTarget()),
            inventory=build_expected_inventory(result, None),
            context_id="corr",
        )
        with pytest.raises(TypeError):
            service.record(
                run_id="run_text",  # type: ignore[arg-type]
                report=report,
                reconciliation_fingerprint=result.summary.fingerprint,
                repair_plan_id=None,
                plan_content_fingerprint=None,
                actor="a",
                correlation_id="c",
            )
        with pytest.raises(TypeError):
            service.record(
                run_id=RUN_ID,
                report=cast("object", report) if False else object(),  # type: ignore[arg-type]
                reconciliation_fingerprint=result.summary.fingerprint,
                repair_plan_id=None,
                plan_content_fingerprint=None,
                actor="a",
                correlation_id="c",
            )
        with pytest.raises(RepairReconciliationStaleError):
            service.record(
                run_id=RUN_ID,
                report=report,
                reconciliation_fingerprint=StateFingerprint("f" * 64),
                repair_plan_id=None,
                plan_content_fingerprint=None,
                actor="a",
                correlation_id="c",
            )
        with pytest.raises(RepairPlanMismatchError, match="does not exist"):
            service.record(
                run_id=RUN_ID,
                report=report,
                reconciliation_fingerprint=result.summary.fingerprint,
                repair_plan_id=RepairPlanId("rpl_missing"),
                plan_content_fingerprint=None,
                actor="a",
                correlation_id="c",
            )
        record = service.record(
            run_id=RUN_ID,
            report=report,
            reconciliation_fingerprint=result.summary.fingerprint,
            repair_plan_id=None,
            plan_content_fingerprint=None,
            actor="a",
            correlation_id="c",
        )
        assert record.verdict.value == "parity_divergent"
        # The same derived identity with different evidence is a conflict.
        from dataclasses import replace as dataclass_replace

        divergent_copy = dataclass_replace(
            report,
            expected_fingerprint=StateFingerprint("e" * 64),
            expected_record_count=report.expected_record_count + 1,
        )
        with pytest.raises(Exception, match="differs from durable state"):
            service.record(
                run_id=RUN_ID,
                report=divergent_copy,
                reconciliation_fingerprint=result.summary.fingerprint,
                repair_plan_id=None,
                plan_content_fingerprint=None,
                actor="a",
                correlation_id="c",
            )
        del RedactedDocument, TargetObservationDisposition


class TestPlanningDefensiveBranches:
    def test_generated_plan_validates_its_shape(self) -> None:
        from paritygrid.application.repair import RepairPlanBinding

        binding = RepairPlanBinding(
            run_id=RUN_ID,
            reconciliation_fingerprint=_result().summary.fingerprint,
            source_input_identity="1" * 64,
            target_input_identity="2" * 64,
            policy_version=1,
            generation_version=1,
            rules_version=1,
            analysis_version=1,
            analytical_query_version=1,
            action_count=5,
        )
        with pytest.raises(ValueError, match="present together"):
            GeneratedRepairPlan(
                plan=None,
                content_fingerprint=_result().summary.fingerprint,
                action_keys=None,
                binding=binding,
                repairable_keys=(),
                review_only_keys=(),
            )
        with pytest.raises(ValueError, match="cover every repairable key"):
            GeneratedRepairPlan(
                plan=None,
                content_fingerprint=None,
                action_keys=None,
                binding=binding,
                repairable_keys=("GRID-0001",),
                review_only_keys=(),
            )

    def test_observed_payload_validates_its_shape(self) -> None:
        record = parse_observed_payload(0, wire_payload("GRID-0001")).record
        quarantined = parse_observed_payload(0, None).quarantined
        with pytest.raises(ValueError, match="exactly one"):
            ObservedTargetPayload(record=record, quarantined=quarantined)
        with pytest.raises(ValueError, match="exactly one"):
            ObservedTargetPayload(record=None, quarantined=None)

    def test_frontier_builder_rejects_foreign_evidence(self) -> None:
        with pytest.raises(TypeError):
            frontier_from_evidence(cast("RepairWorkflowEvidence", object()))

    def test_companion_submission_re_raises_typed_failures(self) -> None:
        from paritygrid.application.repair.errors import RepairWriterUnavailableError

        class _TypedFailureWriter:
            def submit(self, command: object, *, timeout_seconds: float) -> object:
                raise RepairWriterUnavailableError("already typed")

        with pytest.raises(RepairWriterUnavailableError):
            submit_command(
                cast("TransactionalWriter", _TypedFailureWriter()),
                cast("WriterCommand", object()),
                timeout_seconds=1.0,
            )

    def test_companion_wait_passes_typed_rejections_through(self) -> None:
        class _BrokenTicket:
            def result(self, *, timeout_seconds: float) -> object:
                raise ValueError("boom")

        class _BrokenWriter:
            def submit(self, command: object, *, timeout_seconds: float) -> object:
                return _BrokenTicket()

        # Typed rejections from the repositories are not writer failures;
        # they propagate so the service fences can translate them.
        with pytest.raises(ValueError, match="boom"):
            submit_command(
                cast("TransactionalWriter", _BrokenWriter()),
                cast("WriterCommand", object()),
                timeout_seconds=1.0,
            )

    def test_created_plan_validates_its_shape(self) -> None:
        from paritygrid.application.repair import CreatedRepairPlan, RepairPlanBinding

        binding = RepairPlanBinding(
            run_id=RUN_ID,
            reconciliation_fingerprint=_result().summary.fingerprint,
            source_input_identity="1" * 64,
            target_input_identity="2" * 64,
            policy_version=1,
            generation_version=1,
            rules_version=1,
            analysis_version=1,
            analytical_query_version=1,
            action_count=0,
        )
        empty = GeneratedRepairPlan(
            plan=None,
            content_fingerprint=None,
            action_keys=None,
            binding=binding,
            repairable_keys=(),
            review_only_keys=(),
        )
        with pytest.raises(ValueError, match="empty generation"):
            CreatedRepairPlan(
                generated=empty,
                aggregate=_aggregate_stub(),
                binding=binding,
                replayed=False,
            )
        from paritygrid.application.repair.planning_service import (
            _require_matching_plan,
        )  # pyright: ignore[reportPrivateUsage]

        with pytest.raises(RepairPlanMismatchError, match="no plan"):
            _require_matching_plan(_aggregate_stub(), empty)

    def test_persisting_unknown_outcomes_eventually_surfaces(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        seed_terminal_run(database)
        proxy = _UnknownOutcomeProxy(
            writer, lose_receipts={WriterCommandKind.PERSIST_RECONCILIATION: 99}
        )
        with pytest.raises(RepairWriterOutcomeUnknownError):
            ReconciliationResultService(
                cast("TransactionalWriter", proxy), reader, now=clock.now
            ).persist(
                run_id=RUN_ID,
                analysis=_result(),
                actor="operator-1",
                correlation_id="corr",
            )


class _FailedActionReader:
    """Wrap the real reader with one aggregate carrying a failed action."""

    def __init__(self, inner: SQLiteRepairWorkflowReader, plan_id: RepairPlanId) -> None:
        self._inner = inner
        self._plan_id = plan_id

    def load(self, run_id: RunId) -> RepairWorkflowEvidence:
        return self._inner.load(run_id)

    def load_plan(self, repair_plan_id: RepairPlanId) -> RepairPlanAggregate | None:
        aggregate = self._inner.load_plan(repair_plan_id)
        if aggregate is None or repair_plan_id != self._plan_id:
            return aggregate
        from dataclasses import replace as dataclass_replace

        from paritygrid.application.ports.repair_audit import RepairActionStatus

        actions = list(aggregate.actions)
        if actions:
            actions[0] = dataclass_replace(actions[0], status=RepairActionStatus.FAILED)
        return dataclass_replace(aggregate, actions=tuple(actions))

    def load_reconciliation_result(self, run_id: RunId) -> ReconciliationResultRecord | None:
        return self._inner.load_reconciliation_result(run_id)

    def load_target_verification(
        self, verification_id: TargetVerificationId
    ) -> TargetVerificationRecord | None:
        return self._inner.load_target_verification(verification_id)


def _aggregate_stub(
    *,
    status: RepairPlanStatus = RepairPlanStatus.PROPOSED,
    applying_at: UtcTimestamp | None = None,
) -> RepairPlanAggregate:
    from datetime import UTC, datetime

    from paritygrid.application.ports.repair_audit import (
        RepairActionRecord,
        RepairPlanRecord,
    )

    moment = UtcTimestamp(datetime(2026, 8, 27, 12, 0, 0, tzinfo=UTC))
    plan = RepairPlanRecord(
        repair_plan_id=derive_plan_id(RUN_ID, _result().summary.fingerprint),
        run_id=RUN_ID,
        reconciliation_fingerprint=_result().summary.fingerprint,
        content_fingerprint=_result().summary.fingerprint,
        status=status,
        row_version=1,
        created_at=moment,
        applying_at=applying_at,
        applied_at=None,
        rejected_at=None,
        failed_at=None,
        failure=None,
    )
    return RepairPlanAggregate(
        plan=plan, approval=None, actions=cast("tuple[RepairActionRecord, ...]", ())
    )
