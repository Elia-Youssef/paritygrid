# pyright: reportPrivateUsage=false
"""Idempotent repair application over the real writer and target (P11.3)."""

from typing import cast

import pytest
from sqlalchemy import func, select

from paritygrid.adapters.persistence.repair_workflow import SQLiteRepairWorkflowReader
from paritygrid.adapters.persistence.schema import audit_entries, execution_events
from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter
from paritygrid.application.ports.connectors import (
    ConnectorAmbiguousError,
    ConnectorCallContext,
    ConnectorCapabilitiesV1,
    ConnectorCapability,
    ConnectorCapabilitySet,
    ConnectorKind,
    ConnectorPermanentError,
    ConnectorState,
    ConnectorValidationError,
    EventCancellationToken,
    TargetConnector,
    TargetEffectOutcome,
    TargetRecord,
    TargetRecordPage,
    TargetStateSnapshot,
    TargetWriteOutcome,
    TargetWriteRequest,
)
from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.application.ports.repair_audit import RepairPlanStatus
from paritygrid.application.reconciliation.analysis import ReconciliationAnalysis
from paritygrid.application.repair import (
    ReconciliationResultService,
    RepairApplicationPolicy,
    RepairApplicationReport,
    RepairApplicationService,
    RepairApprovalRequest,
    RepairApprovalService,
    RepairPlanningService,
)
from paritygrid.application.repair.errors import (
    RepairPlanStateError,
    TargetApplicationError,
)
from paritygrid.demo.failures import FailureScript, ScriptedFailure, ScriptedFailureKind
from paritygrid.demo.simulators.warehouse import SimulatedWarehouse
from paritygrid.domain.models import RepairPlanId
from tests.repair.conftest import (
    RUN_ID,
    DeterministicClock,
    analysis,
    open_target,
    seed_terminal_run,
    wire_payload,
)

pytestmark = pytest.mark.anyio


def _analysis() -> ReconciliationAnalysis:
    return analysis(
        [
            wire_payload("GRID-0001"),
            wire_payload("GRID-0002", quantity=9),
            wire_payload("GRID-0003", name="Only Source"),
        ],
        [wire_payload("GRID-0001", name="Different")],
    )


async def _prepare(
    database: SQLiteDatabase,
    writer: SQLiteTransactionalWriter,
    reader: SQLiteRepairWorkflowReader,
    clock: DeterministicClock,
) -> None:
    seed_terminal_run(database)
    result = _analysis()
    ReconciliationResultService(writer, reader, now=clock.now).persist(
        run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr-11"
    )
    created = RepairPlanningService(writer, reader, now=clock.now).create(
        run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr-11"
    )
    assert created.aggregate is not None
    RepairApprovalService(writer, reader, now=clock.now).approve(
        RepairApprovalRequest(
            run_id=RUN_ID,
            repair_plan_id=created.aggregate.plan.repair_plan_id,
            approved_by="approver-1",
            correlation_id="corr-approve",
            approved_content_fingerprint=created.aggregate.plan.content_fingerprint,
            approved_reconciliation_fingerprint=result.summary.fingerprint,
            detail=RedactedDocument.from_mapping({"decision": "reviewed"}),
        )
    )


def _derived_plan_id(reader: SQLiteRepairWorkflowReader) -> RepairPlanId:
    from paritygrid.application.repair.identities import derive_plan_id

    summary = reader.load(RUN_ID).summary
    assert summary is not None
    return derive_plan_id(RUN_ID, summary.reconciliation_fingerprint)


def _service(
    writer: SQLiteTransactionalWriter,
    reader: SQLiteRepairWorkflowReader,
    clock: DeterministicClock,
) -> RepairApplicationService:
    return RepairApplicationService(
        writer,
        reader,
        now=clock.now,
        policy=RepairApplicationPolicy(
            max_attempts_per_action=3,
            max_ambiguous_replays=2,
            delay_seconds=0.0,
            timeout_seconds=10.0,
        ),
        sleep=_no_sleep,
    )


async def _no_sleep(_seconds: float) -> None:
    return None


async def _apply(
    writer: SQLiteTransactionalWriter,
    reader: SQLiteRepairWorkflowReader,
    clock: DeterministicClock,
    target: TargetConnector,
    *,
    cancellation: object | None = None,
) -> RepairApplicationReport:
    service = _service(writer, reader, clock)
    if cancellation is None:
        return await service.apply(
            run_id=RUN_ID,
            repair_plan_id=_derived_plan_id(reader),
            target=target,
            context_id="corr-apply",
        )
    return await service.apply(
        run_id=RUN_ID,
        repair_plan_id=_derived_plan_id(reader),
        target=target,
        context_id="corr-apply",
        cancellation=cast("EventCancellationToken", cancellation),
    )


class _IdempotentFakeTarget:
    """A faithful in-memory double of the warehouse idempotency contract."""

    def __init__(
        self,
        *,
        failures: dict[str, BaseException] | None = None,
        cancel_after_writes: int | None = None,
    ) -> None:
        self._failures = dict(failures or {})
        self._cancel_after = cancel_after_writes
        self.records: dict[str, tuple[dict[str, object], int]] = {}
        self.registry: dict[str, TargetWriteOutcome] = {}
        self.writes: list[TargetWriteRequest] = []
        self.target_version = 0
        self.token = EventCancellationToken() if cancel_after_writes is not None else None

    def capabilities(self) -> ConnectorCapabilitiesV1:
        return ConnectorCapabilitiesV1(
            protocol="paritygrid.connector.capabilities.v1",
            contract_version=1,
            kind=ConnectorKind.WAREHOUSE_TARGET,
            capabilities=ConnectorCapabilitySet(
                values=(
                    ConnectorCapability.READ,
                    ConnectorCapability.WRITE,
                    ConnectorCapability.IDEMPOTENCY,
                )
            ),
            max_page_records=200,
            supports_cursors=True,
        )

    def state(self) -> ConnectorState:
        return ConnectorState.OPEN

    async def open_async(self) -> None:
        return None

    async def write_record_async(
        self, request: TargetWriteRequest, context: ConnectorCallContext
    ) -> TargetWriteOutcome:
        self.writes.append(request)
        if (
            self.token is not None
            and self._cancel_after is not None
            and len(self.writes) >= (self._cancel_after)
        ):
            self.token.cancel()
        failure = self._failures.get(request.sku)
        if failure is not None:
            raise failure
        recorded = self.registry.get(request.idempotency_key)
        if recorded is not None:
            return TargetWriteOutcome(
                outcome=TargetEffectOutcome.REPLAYED,
                record_version=recorded.record_version,
                target_version=self.target_version,
                request_count=1,
            )
        stored = self.records.get(request.sku)
        if stored is not None and stored[0] != dict(request.payload):
            from paritygrid.application.ports.connectors import ConnectorConflictError

            raise ConnectorConflictError("the idempotency key was reused with a different request")
        self.target_version += 1
        record_version = 1 if stored is None else stored[1] + 1
        self.records[request.sku] = (dict(request.payload), record_version)
        outcome = TargetWriteOutcome(
            outcome=TargetEffectOutcome.APPLIED,
            record_version=record_version,
            target_version=self.target_version,
            request_count=1,
        )
        self.registry[request.idempotency_key] = outcome
        return outcome

    async def read_record_async(
        self, sku: str, context: ConnectorCallContext
    ) -> TargetRecord | None:
        stored = self.records.get(sku)
        if stored is None:
            return None
        return TargetRecord(
            sku=sku, payload=stored[0], record_version=stored[1], target_version=self.target_version
        )

    async def list_records_async(
        self, cursor: str | None, context: ConnectorCallContext
    ) -> TargetRecordPage:
        if cursor is not None:
            raise AssertionError("fake target has one exhaustive page")
        return TargetRecordPage(
            records=tuple(
                TargetRecord(
                    sku=sku,
                    payload=payload,
                    record_version=version,
                    target_version=self.target_version,
                )
                for sku, (payload, version) in sorted(self.records.items())
            ),
            next_cursor=None,
            request_count=1,
            byte_count=0,
        )

    async def state_snapshot_async(self, context: ConnectorCallContext) -> TargetStateSnapshot:
        return TargetStateSnapshot(
            record_count=len(self.records),
            target_version=self.target_version,
            content_fingerprint="0" * 64,
            capacity=10_000,
        )

    async def aclose(self) -> None:
        return None


def _fake(
    *, failures: dict[str, BaseException] | None = None, cancel_after_writes: int | None = None
) -> TargetConnector:
    return cast(
        TargetConnector,
        _IdempotentFakeTarget(failures=failures, cancel_after_writes=cancel_after_writes),
    )


class TestHappyApplication:
    async def test_applies_each_action_once_and_completes(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            await _prepare(database, writer, reader, clock)
            target = await open_target(warehouse)
            try:
                report = await _apply(writer, reader, clock, target)
            finally:
                await target.aclose()
            assert report.disposition.value == "completed"
            assert report.aggregate.plan.status is RepairPlanStatus.APPLIED
            assert len(report.effects) == 3
            for effect in report.effects:
                assert effect.outcome == "applied"
                assert effect.attempts == 1
            behavior = warehouse.behavior
            assert behavior.record_count == 3
            assert behavior.target_version == 3
            with database.transaction() as session:
                assert session.scalar(select(func.count()).select_from(audit_entries)) == 8
                kinds = session.execute(select(execution_events.c.event_kind)).scalars().all()
                assert kinds.count("repair_application_started") == 1
                assert kinds.count("repair_action_applied") == 3
                assert kinds.count("repair_application_completed") == 1
        finally:
            await warehouse.aclose()

    async def test_reapply_after_completion_is_a_no_op(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            await _prepare(database, writer, reader, clock)
            target = await open_target(warehouse)
            try:
                await _apply(writer, reader, clock, target)
                requests_before = warehouse.request_count()
                second = await _apply(writer, reader, clock, target)
            finally:
                await target.aclose()
            assert second.disposition.value == "already_applied"
            assert second.effects == ()
            assert warehouse.request_count() == requests_before
            assert warehouse.behavior.target_version == 3
        finally:
            await warehouse.aclose()

    async def test_unapproved_plan_cannot_be_applied(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        seed_terminal_run(database)
        result = _analysis()
        ReconciliationResultService(writer, reader, now=clock.now).persist(
            run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr-11"
        )
        RepairPlanningService(writer, reader, now=clock.now).create(
            run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr-11"
        )
        with pytest.raises(RepairPlanStateError, match="approval"):
            await _apply(writer, reader, clock, _fake())


class TestInterruptionAndReplay:
    async def test_interruption_mid_application_resumes_without_duplicate_effects(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        await _prepare(database, writer, reader, clock)
        fake = _IdempotentFakeTarget(cancel_after_writes=1)
        target = cast(TargetConnector, fake)
        interrupted = await _apply(writer, reader, clock, target, cancellation=fake.token)
        assert interrupted.disposition.value == "interrupted"
        aggregate = reader.load_plan(_derived_plan_id(reader))
        assert aggregate is not None
        assert aggregate.plan.status is RepairPlanStatus.APPLYING
        applied_so_far = sum(1 for action in aggregate.actions if action.status.value == "applied")
        assert applied_so_far == 1
        fresh_token = EventCancellationToken()
        resumed = await _apply(writer, reader, clock, target, cancellation=fresh_token)
        assert resumed.resumed
        assert resumed.disposition.value == "completed"
        # Three logical effects total regardless of interruption; the two
        # pending actions apply after the resume and none repeat.
        assert fake.target_version == 3
        assert len(fake.records) == 3
        keys = [write.idempotency_key for write in fake.writes]
        assert len(keys) == len(set(keys)) == 3

    async def test_ambiguous_write_resolves_by_same_key_replay(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse(
            FailureScript.from_entries(
                (
                    ScriptedFailure(
                        sequence=1,
                        kind=ScriptedFailureKind.CONNECTION_LOSS,
                        partial_bytes=8,
                    ),
                )
            )
        )
        await warehouse.start()
        try:
            await _prepare(database, writer, reader, clock)
            target = await open_target(warehouse)
            try:
                report = await _apply(writer, reader, clock, target)
            finally:
                await target.aclose()
            assert report.disposition.value == "completed"
            assert report.effects[0].attempts == 2
            assert report.effects[0].outcome in {"applied", "replayed"}
            assert warehouse.behavior.target_version == 3
            assert warehouse.behavior.record_count == 3
        finally:
            await warehouse.aclose()

    async def test_rate_limited_retry_then_success_applies_once(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse(
            FailureScript.from_entries(
                (
                    ScriptedFailure(
                        sequence=1,
                        kind=ScriptedFailureKind.RATE_LIMIT,
                        retry_after_seconds=1,
                    ),
                )
            )
        )
        await warehouse.start()
        try:
            await _prepare(database, writer, reader, clock)
            target = await open_target(warehouse)
            try:
                report = await _apply(writer, reader, clock, target)
            finally:
                await target.aclose()
            assert report.disposition.value == "completed"
            assert report.effects[0].attempts == 2
            assert warehouse.behavior.target_version == 3
        finally:
            await warehouse.aclose()

    async def test_retry_exhaustion_terminalizes_the_plan_as_failed(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        script = FailureScript.from_entries(
            tuple(
                ScriptedFailure(
                    sequence=sequence,
                    kind=ScriptedFailureKind.RATE_LIMIT,
                    retry_after_seconds=1,
                )
                for sequence in range(1, 12)
            )
        )
        warehouse = SimulatedWarehouse(script)
        await warehouse.start()
        try:
            await _prepare(database, writer, reader, clock)
            target = await open_target(warehouse)
            try:
                with pytest.raises(TargetApplicationError):
                    await _apply(writer, reader, clock, target)
            finally:
                await target.aclose()
            aggregate = reader.load_plan(_derived_plan_id(reader))
            assert aggregate is not None
            assert aggregate.plan.status is RepairPlanStatus.FAILED
            assert warehouse.behavior.target_version == 0
            fresh = await open_target(warehouse)
            try:
                with pytest.raises(RepairPlanStateError):
                    await _apply(writer, reader, clock, fresh)
            finally:
                await fresh.aclose()
        finally:
            await warehouse.aclose()


class TestFailureMatrix:
    async def test_invalid_request_fails_before_dispatch(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        await _prepare(database, writer, reader, clock)
        fake = _IdempotentFakeTarget(failures={"GRID-0001": ConnectorValidationError("bad")})
        with pytest.raises(TargetApplicationError, match="invalid_request"):
            await _apply(writer, reader, clock, cast(TargetConnector, fake))
        aggregate = reader.load_plan(_derived_plan_id(reader))
        assert aggregate is not None
        assert aggregate.plan.status is RepairPlanStatus.FAILED
        assert len(fake.writes) == 1

    async def test_permanent_target_rejection_terminalizes_the_plan(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        await _prepare(database, writer, reader, clock)
        target = _fake(failures={"GRID-0001": ConnectorPermanentError("no")})
        with pytest.raises(TargetApplicationError, match="target_rejected"):
            await _apply(writer, reader, clock, target)
        aggregate = reader.load_plan(_derived_plan_id(reader))
        assert aggregate is not None
        assert aggregate.plan.status is RepairPlanStatus.FAILED

    async def test_unresolved_ambiguity_suspends_without_a_false_terminal_state(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        await _prepare(database, writer, reader, clock)
        target = _fake(failures={"GRID-0001": ConnectorAmbiguousError("unknown after timeout")})
        report = await _apply(writer, reader, clock, target)
        assert report.disposition.value == "unresolved"
        assert report.unresolved_action is not None
        aggregate = reader.load_plan(_derived_plan_id(reader))
        assert aggregate is not None
        assert aggregate.plan.status is RepairPlanStatus.APPLYING
        assert aggregate.actions[0].status.value == "pending"

    async def test_unknown_outcome_resolves_after_the_target_recovers(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        await _prepare(database, writer, reader, clock)
        fake = _IdempotentFakeTarget(failures={"GRID-0001": ConnectorAmbiguousError("unknown")})
        target = cast(TargetConnector, fake)
        first = await _apply(writer, reader, clock, target)
        assert first.disposition.value == "unresolved"
        fake._failures = {}
        second = await _apply(writer, reader, clock, target)
        assert second.resumed
        assert second.disposition.value == "completed"
        assert fake.target_version == 3
        keys = [write.idempotency_key for write in fake.writes]
        # Three ambiguous replays share one identity; the fourth write is the
        # resolving replay after recovery, still on that same identity.
        assert keys.count(keys[0]) == 4
        assert len(set(keys)) == 3

    async def test_stale_reservation_loses_its_race(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:

        from paritygrid.adapters.persistence.writer.dispatch import dispatch_command
        from paritygrid.application.ports.repair_audit import (
            RepairApplicationConflictError,
            RepairApplicationResult,
        )
        from paritygrid.application.repair.companions import (
            build_companions,
            frontier_from_evidence,
        )
        from paritygrid.application.writes.repairs import RecordRepairActionApplied

        await _prepare(database, writer, reader, clock)
        service = _service(writer, reader, clock)
        begun = await service.apply(
            run_id=RUN_ID,
            repair_plan_id=_derived_plan_id(reader),
            target=_fake(),
            context_id="corr-begin",
        )
        assert begun.disposition.value == "completed"
        aggregate = reader.load_plan(_derived_plan_id(reader))
        assert aggregate is not None
        plan = aggregate.plan
        assert plan.applying_at is not None
        from paritygrid.application.ports.repair_audit import (
            RepairApplicationReservation,
        )

        forged = RepairApplicationReservation(
            repair_plan_id=plan.repair_plan_id,
            run_id=plan.run_id,
            reconciliation_fingerprint=plan.reconciliation_fingerprint,
            content_fingerprint=plan.content_fingerprint,
            applying_at=plan.applying_at,
            row_version=plan.row_version + 5,
        )
        evidence = reader.load(RUN_ID)
        action = aggregate.actions[0]
        late_at = clock.now()
        companions = build_companions(
            frontier=frontier_from_evidence(evidence),
            run_id=RUN_ID,
            operation="repair_action_applied",
            object_kind="repair_action",
            object_id=action.effect.action_id.value,
            actor="late-worker",
            correlation_id="corr-late",
            occurred_at=late_at,
            payload={"canonical_key": action.effect.proposed.sku},
        )
        command = RecordRepairActionApplied(
            run_id=RUN_ID,
            reservation=forged,
            repair_action_id=action.effect.action_id,
            result=RepairApplicationResult(
                1, RedactedDocument.from_mapping({"outcome": "applied"})
            ),
            target_version=99,
            applied_at=late_at,
            companions=companions,
        )
        with (
            pytest.raises(RepairApplicationConflictError),
            database.transaction() as session,
        ):
            dispatch_command(session, command)
        # The durable applied state is unchanged by the fenced late write.
        after = reader.load_plan(_derived_plan_id(reader))
        assert after is not None
        assert after.actions[0].target_version == action.target_version
