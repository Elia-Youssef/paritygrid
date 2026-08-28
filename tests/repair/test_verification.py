# pyright: reportPrivateUsage=false
"""Independent target observation and parity verification (P11.4)."""

import contextlib
from dataclasses import replace
from typing import cast

import pytest
from sqlalchemy import func, select

from paritygrid.adapters.connectors import WarehouseTargetConfig, WarehouseTargetConnector
from paritygrid.adapters.persistence.repair_workflow import SQLiteRepairWorkflowReader
from paritygrid.adapters.persistence.schema import target_state_verifications
from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter
from paritygrid.application.ports.connectors import (
    ConnectorCallBounds,
    ConnectorCallContext,
    ConnectorPermanentError,
    ConnectorState,
    EventCancellationToken,
    TargetConnector,
    TargetRecordPage,
    TargetWriteRequest,
)
from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.application.ports.reconciliation_persistence import (
    TargetVerificationVerdict,
)
from paritygrid.application.ports.repair_audit import RepairPlanAggregate
from paritygrid.application.reconciliation.analysis import ReconciliationAnalysis
from paritygrid.application.repair import (
    ExpectedInventory,
    ReconciliationResultService,
    RepairApplicationPolicy,
    RepairApplicationService,
    RepairApprovalRequest,
    RepairApprovalService,
    RepairPlanningService,
    TargetObservationDisposition,
    TargetParityVerifier,
    TargetVerificationReport,
    TargetVerificationService,
    build_expected_inventory,
)
from paritygrid.application.repair.errors import (
    RepairPlanMismatchError,
    RepairReconciliationMissingError,
)
from paritygrid.demo.simulators.warehouse import SimulatedWarehouse
from paritygrid.domain.models import StateFingerprint
from paritygrid.domain.repair import RepairPlan
from tests.repair.conftest import (
    RUN_ID,
    DeterministicClock,
    analysis,
    open_target,
    record_for,
    seed_terminal_run,
    wire_payload,
)
from tests.repair.test_applier import _IdempotentFakeTarget, _no_sleep

pytestmark = pytest.mark.anyio


def _analysis() -> ReconciliationAnalysis:
    return analysis(
        [
            wire_payload("GRID-0001"),
            wire_payload("GRID-0002", quantity=9),
            wire_payload("GRID-0003", name="Only Source"),
        ],
        [
            wire_payload("GRID-0001", name="Different"),
            wire_payload("GRID-0004", name="Target Only"),
        ],
    )


async def _seed_target(warehouse: SimulatedWarehouse, result: ReconciliationAnalysis) -> None:
    """Load the real target with the analysis's target-side observations."""

    from paritygrid.application.ports.connectors import TargetWriteRequest
    from paritygrid.application.repair.payloads import render_target_payload

    target = await open_target(warehouse)
    try:
        for key in result.classification.keys:
            for record in key.outcome.target_records:
                await target.write_record_async(
                    TargetWriteRequest(
                        sku=record.sku,
                        payload=render_target_payload(record),
                        idempotency_key=f"seed-{record.sku}",
                    ),
                    ConnectorCallContext(correlation_id="seed"),
                )
    finally:
        await target.aclose()


async def _applied_state(
    database: SQLiteDatabase,
    writer: SQLiteTransactionalWriter,
    reader: SQLiteRepairWorkflowReader,
    clock: DeterministicClock,
    warehouse: SimulatedWarehouse,
) -> tuple[ReconciliationAnalysis, RepairPlan]:
    seed_terminal_run(database)
    result = _analysis()
    await _seed_target(warehouse, result)
    ReconciliationResultService(writer, reader, now=clock.now).persist(
        run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr-11"
    )
    created = RepairPlanningService(writer, reader, now=clock.now).create(
        run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr-11"
    )
    assert created.generated.plan is not None
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
    service = RepairApplicationService(
        writer,
        reader,
        now=clock.now,
        policy=RepairApplicationPolicy(delay_seconds=0.0, timeout_seconds=10.0),
        sleep=_no_sleep,
    )
    target = await open_target(warehouse)
    try:
        report = await service.apply(
            run_id=RUN_ID,
            repair_plan_id=created.aggregate.plan.repair_plan_id,
            target=target,
            context_id="corr-apply",
        )
    finally:
        await target.aclose()
    assert report.disposition.value == "completed"
    return result, created.generated.plan


def _verifier(clock: DeterministicClock) -> TargetParityVerifier:
    return TargetParityVerifier(now=clock.now)


async def _verify(
    clock: DeterministicClock,
    target: TargetConnector,
    inventory: ExpectedInventory,
) -> TargetVerificationReport:
    return await _verifier(clock).verify(
        target=target, inventory=inventory, context_id="corr-verify"
    )


async def _open_paged_target(
    warehouse: SimulatedWarehouse, *, max_page_records: int
) -> WarehouseTargetConnector:
    target = WarehouseTargetConnector(
        WarehouseTargetConfig(
            warehouse.base_url,
            bounds=ConnectorCallBounds(max_page_records=max_page_records),
        )
    )
    await target.open_async()
    return target


class _CancelAfterFirstPageTarget:
    """Cancel the shared token after receiving a first nonterminal page."""

    def __init__(self, inner: WarehouseTargetConnector, token: EventCancellationToken) -> None:
        self._inner = inner
        self._token = token
        self._cancelled = False

    def __getattr__(self, name: str) -> object:
        return getattr(self._inner, name)

    async def list_records_async(
        self, cursor: str | None, context: ConnectorCallContext
    ) -> TargetRecordPage:
        page = await self._inner.list_records_async(cursor, context)
        if not self._cancelled and page.next_cursor is not None:
            self._cancelled = True
            self._token.cancel()
        return page


class _MutationDuringEnumerationTarget:
    """Introduce an out-of-band target mutation after the opening page."""

    def __init__(self, inner: WarehouseTargetConnector) -> None:
        self._inner = inner
        self._mutated = False

    def __getattr__(self, name: str) -> object:
        return getattr(self._inner, name)

    async def list_records_async(
        self, cursor: str | None, context: ConnectorCallContext
    ) -> TargetRecordPage:
        page = await self._inner.list_records_async(cursor, context)
        if not self._mutated:
            self._mutated = True
            await self._inner.write_record_async(
                TargetWriteRequest(
                    sku="GRID-0001",
                    payload=wire_payload("GRID-0001", name="Changed during enumeration"),
                    idempotency_key="enumeration-race",
                ),
                ConnectorCallContext(correlation_id="enumeration-race"),
            )
        return page


class TestExpectedInventory:
    def test_expected_inventory_covers_every_canonical_key(self) -> None:
        result = _analysis()
        inventory = build_expected_inventory(result, None)
        # Without a plan the expected present keys are exactly the keys the
        # reconciliation observed on the target; source-only keys stay absent.
        keys = {record.sku for record in inventory.records}
        assert keys == {"GRID-0001", "GRID-0004"}
        assert inventory.absent_keys == ("GRID-0002", "GRID-0003")

    def test_plan_effects_replace_their_keys(self) -> None:
        result = _analysis()
        from paritygrid.application.repair import generate_repair_plan

        generated = generate_repair_plan(run_id=RUN_ID, analysis=result)
        assert generated.plan is not None
        inventory = build_expected_inventory(result, generated.plan)
        repaired = {
            record.sku: record
            for record in inventory.records
            if record.sku in {"GRID-0001", "GRID-0002", "GRID-0003"}
        }
        actions = {action.sku: action for action in generated.plan.actions}
        for sku, record in repaired.items():
            assert record == replace(
                actions[sku].proposed_record,
                source_record_key=f"repair:{sku.lower()}",
            )
        target_only = [record for record in inventory.records if record.sku == "GRID-0004"]
        assert len(target_only) == 1

    def test_distinct_duplicate_target_content_is_flagged_ambiguous(self) -> None:
        result = analysis(
            [wire_payload("GRID-0002")],
            [
                wire_payload("GRID-0001", name="First"),
                wire_payload("GRID-0001", name="Second"),
            ],
        )
        inventory = build_expected_inventory(result, None)
        assert inventory.ambiguous_keys == ("GRID-0001",)


class TestParityVerification:
    async def test_parity_holds_after_application_with_matching_fingerprints(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            result, plan = await _applied_state(database, writer, reader, clock, warehouse)
            inventory = build_expected_inventory(result, plan)
            target = await open_target(warehouse)
            try:
                report = await _verify(clock, target, inventory)
            finally:
                await target.aclose()
            assert report.disposition.value == "observed"
            assert report.verdict is TargetVerificationVerdict.PARITY_HOLDING
            assert report.observed is not None
            assert report.observed.fingerprint == report.expected_fingerprint
            assert report.observed_record_count == 4
            # Two seed writes plus three repair effects advance the target
            # version exactly five times.
            assert report.observed_target_version == 5
            assert report.divergences == ()
        finally:
            await warehouse.aclose()

    async def test_changed_target_data_changes_the_fingerprint_and_breaks_parity(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            result, plan = await _applied_state(database, writer, reader, clock, warehouse)
            inventory = build_expected_inventory(result, plan)
            target = await open_target(warehouse)
            try:
                before = await _verify(clock, target, inventory)
                # An out-of-band mutation of one stored record must change the
                # observed fingerprint and force a divergent verdict.
                mutated = dict(target_records(warehouse)["GRID-0002"])
                mutated["quantity"] = 42
                from paritygrid.application.ports.connectors import TargetWriteRequest

                await target.write_record_async(
                    TargetWriteRequest(
                        sku="GRID-0002",
                        payload=mutated,
                        idempotency_key="out-of-band-mutation",
                    ),
                    ConnectorCallContext(correlation_id="out-of-band"),
                )
                after = await _verify(clock, target, inventory)
            finally:
                await target.aclose()
            assert before.verdict is TargetVerificationVerdict.PARITY_HOLDING
            assert after.verdict is TargetVerificationVerdict.PARITY_DIVERGENT
            assert after.observed is not None
            assert before.observed is not None
            assert after.observed.fingerprint != before.observed.fingerprint
            assert any(divergence.canonical_key == "GRID-0002" for divergence in after.divergences)
        finally:
            await warehouse.aclose()

    async def test_missing_target_record_breaks_parity(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            result, plan = await _applied_state(database, writer, reader, clock, warehouse)
            inventory = build_expected_inventory(result, plan)
            # Expect one extra record the target never received.
            from tests.repair.conftest import record_for

            mutated_records = (*inventory.records, record_for("GRID-9999"))
            from dataclasses import replace as dataclass_replace

            stretched = dataclass_replace(inventory, records=mutated_records)
            target = await open_target(warehouse)
            try:
                report = await _verify(clock, target, stretched)
            finally:
                await target.aclose()
            assert report.verdict is TargetVerificationVerdict.PARITY_DIVERGENT
            assert any(
                divergence.reason == "target record is missing" for divergence in report.divergences
            )
        finally:
            await warehouse.aclose()

    async def test_extra_target_record_breaks_parity_through_the_count(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            result, plan = await _applied_state(database, writer, reader, clock, warehouse)
            inventory = build_expected_inventory(result, plan)
            target = await open_target(warehouse)
            try:
                await target.write_record_async(
                    TargetWriteRequest(
                        sku="GRID-0005",
                        payload=wire_payload("GRID-0005"),
                        idempotency_key="extra-record",
                    ),
                    ConnectorCallContext(correlation_id="extra"),
                )
                report = await _verify(clock, target, inventory)
            finally:
                await target.aclose()
            assert report.verdict is TargetVerificationVerdict.PARITY_DIVERGENT
            assert report.observed is not None
            assert report.observed.record_count == 5
            assert report.observed.fingerprint != report.expected_fingerprint
        finally:
            await warehouse.aclose()

    async def test_extra_record_content_is_fingerprinted_by_full_inventory_enumeration(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            result, plan = await _applied_state(database, writer, reader, clock, warehouse)
            inventory = build_expected_inventory(result, plan)
            target = await open_target(warehouse)
            try:
                await target.write_record_async(
                    TargetWriteRequest(
                        sku="GRID-0005",
                        payload=wire_payload("GRID-0005", name="Unexpected first"),
                        idempotency_key="extra-first",
                    ),
                    ConnectorCallContext(correlation_id="extra"),
                )
                first = await _verify(clock, target, inventory)
                await target.write_record_async(
                    TargetWriteRequest(
                        sku="GRID-0005",
                        payload=wire_payload("GRID-0005", name="Unexpected second"),
                        idempotency_key="extra-second",
                    ),
                    ConnectorCallContext(correlation_id="extra"),
                )
                second = await _verify(clock, target, inventory)
            finally:
                await target.aclose()
            assert first.verdict is TargetVerificationVerdict.PARITY_DIVERGENT
            assert second.verdict is TargetVerificationVerdict.PARITY_DIVERGENT
            assert first.observed is not None
            assert second.observed is not None
            assert first.observed.record_count == second.observed.record_count == 5
            assert first.observed.fingerprint != second.observed.fingerprint
            assert any(item.canonical_key == "GRID-0005" for item in second.divergences)
        finally:
            await warehouse.aclose()

    async def test_same_count_missing_extra_replacement_is_divergent(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        from tests.repair.conftest import record_for

        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            result, plan = await _applied_state(database, writer, reader, clock, warehouse)
            inventory = build_expected_inventory(result, plan)
            replacement = (
                *(record for record in inventory.records if record.sku != "GRID-0004"),
                record_for("GRID-9999"),
            )
            swapped = replace(
                inventory, records=tuple(sorted(replacement, key=lambda item: item.sku))
            )
            target = await open_target(warehouse)
            try:
                report = await _verify(clock, target, swapped)
            finally:
                await target.aclose()
            assert report.verdict is TargetVerificationVerdict.PARITY_DIVERGENT
            assert report.expected_record_count == report.observed_record_count == 4
            assert {item.canonical_key for item in report.divergences} >= {
                "GRID-0004",
                "GRID-9999",
            }
            assert {item.reason for item in report.divergences} >= {
                "target record is missing",
                "unexpected target record is present",
            }
        finally:
            await warehouse.aclose()

    async def test_pagination_enumerates_every_page_before_accepting_parity(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            result, plan = await _applied_state(database, writer, reader, clock, warehouse)
            inventory = build_expected_inventory(result, plan)
            target = await _open_paged_target(warehouse, max_page_records=1)
            try:
                report = await _verify(clock, target, inventory)
            finally:
                await target.aclose()
            assert report.verdict is TargetVerificationVerdict.PARITY_HOLDING
            assert report.observed_record_count == 4
        finally:
            await warehouse.aclose()

    async def test_cancellation_between_target_pages_is_interrupted(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            result, plan = await _applied_state(database, writer, reader, clock, warehouse)
            inventory = build_expected_inventory(result, plan)
            inner = await _open_paged_target(warehouse, max_page_records=1)
            token = EventCancellationToken()
            target = _CancelAfterFirstPageTarget(inner, token)
            try:
                report = await _verifier(clock).verify(
                    target=cast(TargetConnector, target),
                    inventory=inventory,
                    context_id="corr-verify",
                    cancellation=token,
                )
            finally:
                await inner.aclose()
            assert report.disposition.value == "interrupted"
        finally:
            await warehouse.aclose()

    async def test_mutation_during_inventory_observation_fails_closed(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            result, plan = await _applied_state(database, writer, reader, clock, warehouse)
            inventory = build_expected_inventory(result, plan)
            inner = await _open_paged_target(warehouse, max_page_records=1)
            target = _MutationDuringEnumerationTarget(inner)
            try:
                report = await _verify(clock, cast(TargetConnector, target), inventory)
            finally:
                await inner.aclose()
            assert report.disposition.value == "observation_failed"
            assert report.verdict is None
            assert report.detail is not None
            assert "changed while" in report.detail
        finally:
            await warehouse.aclose()

    async def test_observation_failure_is_reported_not_guessed(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            result, plan = await _applied_state(database, writer, reader, clock, warehouse)
            inventory = build_expected_inventory(result, plan)
            await warehouse.aclose()
            dead = cast(TargetConnector, _IdempotentFakeTarget())
            dead.state = ConnectorState.CLOSED  # type: ignore[method-assign]

            async def _fail(context: object) -> object:
                raise ConnectorPermanentError("the target is unavailable")

            dead.state_snapshot_async = _fail  # type: ignore[method-assign]
            report = await _verify(clock, dead, inventory)
            assert report.disposition.value == "observation_failed"
            assert report.verdict is None
            assert report.observed is None
            assert report.detail is not None
            assert "unavailable" in report.detail
        finally:
            with contextlib.suppress(Exception):
                await warehouse.aclose()

    async def test_cancelled_observation_is_interrupted_and_unrecordable(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            result, plan = await _applied_state(database, writer, reader, clock, warehouse)
            inventory = build_expected_inventory(result, plan)
            target = await open_target(warehouse)
            try:
                token = EventCancellationToken()
                token.cancel()
                report = await _verifier(clock).verify(
                    target=target,
                    inventory=inventory,
                    context_id="corr-verify",
                    cancellation=token,
                )
            finally:
                await target.aclose()
            assert report.disposition.value == "interrupted"
            with pytest.raises(RepairPlanMismatchError, match="cancelled"):
                TargetVerificationService(writer, reader, now=clock.now).record(
                    run_id=RUN_ID,
                    report=report,
                    reconciliation_fingerprint=result.summary.fingerprint,
                    repair_plan_id=None,
                    plan_content_fingerprint=None,
                    actor="operator-1",
                    correlation_id="corr-verify",
                )
        finally:
            await warehouse.aclose()

    async def test_observed_fingerprint_is_not_derived_from_other_identities(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            result, plan = await _applied_state(database, writer, reader, clock, warehouse)
            inventory = build_expected_inventory(result, plan)
            target = await open_target(warehouse)
            try:
                report = await _verify(clock, target, inventory)
            finally:
                await target.aclose()
            observed = report.observed
            assert observed is not None
            assert observed.fingerprint != result.summary.fingerprint
            evidence = reader.load(RUN_ID).run.execution_evidence_fingerprint
            assert evidence is not None
            assert observed.fingerprint != evidence
            from paritygrid.domain.canonical import FingerprintScope, fingerprint_state

            assert observed.fingerprint != fingerprint_state(
                (plan,), scope=FingerprintScope.REPAIR_PLAN_CONTENT
            )
        finally:
            await warehouse.aclose()


class TestVerificationRecording:
    async def test_direct_parity_holding_report_is_rejected_even_with_exact_plan(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            result, plan = await _applied_state(database, writer, reader, clock, warehouse)
            inventory = build_expected_inventory(result, plan)
            target = await open_target(warehouse)
            try:
                forged = await _verify(clock, target, inventory)
            finally:
                await target.aclose()
            assert forged.verdict is TargetVerificationVerdict.PARITY_HOLDING
            aggregate = reader.load_plan(plan.plan_id)
            assert aggregate is not None
            with pytest.raises(RepairPlanMismatchError, match="must use verify_and_record"):
                TargetVerificationService(writer, reader, now=clock.now).record(
                    run_id=RUN_ID,
                    report=forged,
                    reconciliation_fingerprint=result.summary.fingerprint,
                    repair_plan_id=aggregate.plan.repair_plan_id,
                    plan_content_fingerprint=aggregate.plan.content_fingerprint,
                    actor="operator-1",
                    correlation_id="corr-forged",
                )
        finally:
            await warehouse.aclose()


class TestVerificationEnumerationEdges:
    def test_record_rejects_runs_without_a_reconciliation_snapshot(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        seed_terminal_run(database)
        report = TargetVerificationReport(
            disposition=TargetObservationDisposition.OBSERVATION_FAILED,
            verdict=None,
            observed=None,
            expected_fingerprint=StateFingerprint("a" * 64),
            expected_record_count=0,
            observed_record_count=None,
            divergences=(),
            observed_target_version=None,
            observed_at=clock.now(),
            detail="target unavailable",
        )
        with pytest.raises(RepairReconciliationMissingError, match="no reconciliation snapshot"):
            TargetVerificationService(writer, reader, now=clock.now).record(
                run_id=RUN_ID,
                report=report,
                reconciliation_fingerprint=StateFingerprint("a" * 64),
                repair_plan_id=None,
                plan_content_fingerprint=None,
                actor="operator-1",
                correlation_id="corr-no-summary",
            )

    def test_expected_inventory_rejects_overlapping_or_unordered_keys(self) -> None:
        with pytest.raises(ValueError, match="sorted unique"):
            ExpectedInventory(records=(), absent_keys=("B", "A"), ambiguous_keys=())
        with pytest.raises(ValueError, match="must not overlap"):
            ExpectedInventory(
                records=(record_for("GRID-1"),), absent_keys=("GRID-1",), ambiguous_keys=()
            )

    async def test_large_target_and_repeated_cursor_fail_closed(
        self, clock: DeterministicClock
    ) -> None:
        from paritygrid.application.ports.connectors import TargetStateSnapshot
        from paritygrid.application.repair import ExpectedInventory

        class _LargeTarget(_IdempotentFakeTarget):
            async def state_snapshot_async(
                self, context: ConnectorCallContext
            ) -> TargetStateSnapshot:
                return TargetStateSnapshot(100_001, 1, "a" * 64, 100_001)

        inventory = ExpectedInventory(records=(), absent_keys=(), ambiguous_keys=())
        report = await _verifier(clock).verify(
            target=cast(TargetConnector, _LargeTarget()), inventory=inventory, context_id="large"
        )
        assert report.disposition.value == "observation_failed"

        class _RepeatedCursorTarget(_IdempotentFakeTarget):
            async def state_snapshot_async(
                self, context: ConnectorCallContext
            ) -> TargetStateSnapshot:
                return TargetStateSnapshot(0, 1, "a" * 64, 10)

            async def list_records_async(
                self, cursor: str | None, context: ConnectorCallContext
            ) -> TargetRecordPage:
                return TargetRecordPage(
                    records=(), next_cursor="again", request_count=1, byte_count=0
                )

        repeated = await _verifier(clock).verify(
            target=cast(TargetConnector, _RepeatedCursorTarget()),
            inventory=inventory,
            context_id="cursor",
        )
        assert repeated.disposition.value == "observation_failed"

    @pytest.mark.parametrize("payload", [{"sku": "GRID-1"}, {"not": "inventory"}])
    async def test_enumeration_rejects_duplicate_or_unparseable_target_records(
        self, clock: DeterministicClock, payload: dict[str, object]
    ) -> None:
        from paritygrid.application.ports.connectors import TargetRecord

        class _BadRecords(_IdempotentFakeTarget):
            async def list_records_async(
                self, cursor: str | None, context: ConnectorCallContext
            ) -> TargetRecordPage:
                records = (
                    (TargetRecord("GRID-1", payload, 1, 1),)
                    if "not" in payload
                    else (
                        TargetRecord("GRID-1", wire_payload("GRID-1"), 1, 1),
                        TargetRecord("GRID-1", wire_payload("GRID-1"), 1, 1),
                    )
                )
                return TargetRecordPage(records, None, 1, 0)

        with pytest.raises(RuntimeError):
            await _verifier(clock)._observe_inventory(  # pyright: ignore[reportPrivateUsage]
                cast(TargetConnector, _BadRecords()), ConnectorCallContext()
            )

    @pytest.mark.parametrize("changed_field", ["record_count", "target_version", "fingerprint"])
    async def test_changed_snapshot_component_fails_the_observation(
        self, clock: DeterministicClock, changed_field: str
    ) -> None:
        from paritygrid.application.ports.connectors import TargetStateSnapshot

        class _ChangingSnapshotTarget(_IdempotentFakeTarget):
            def __init__(self) -> None:
                super().__init__()
                self._calls = 0

            async def state_snapshot_async(
                self, context: ConnectorCallContext
            ) -> TargetStateSnapshot:
                self._calls += 1
                changed = {
                    "record_count": (1, 1, "a" * 64),
                    "target_version": (0, 2, "a" * 64),
                    "fingerprint": (0, 1, "b" * 64),
                }[changed_field]
                if self._calls == 1:
                    return TargetStateSnapshot(0, 1, "a" * 64, 10)
                return TargetStateSnapshot(*changed, capacity=10)

            async def list_records_async(
                self, cursor: str | None, context: ConnectorCallContext
            ) -> TargetRecordPage:
                return TargetRecordPage((), None, 1, 0)

        report = await _verifier(clock).verify(
            target=cast(TargetConnector, _ChangingSnapshotTarget()),
            inventory=ExpectedInventory(records=(), absent_keys=(), ambiguous_keys=()),
            context_id="snapshot-change",
        )
        assert report.disposition.value == "observation_failed"

    async def test_closing_snapshot_failure_fails_the_observation(
        self, clock: DeterministicClock
    ) -> None:
        from paritygrid.application.ports.connectors import TargetStateSnapshot

        class _ClosingFailureTarget(_IdempotentFakeTarget):
            def __init__(self) -> None:
                super().__init__()
                self._snapshots = 0

            async def state_snapshot_async(
                self, context: ConnectorCallContext
            ) -> TargetStateSnapshot:
                self._snapshots += 1
                if self._snapshots > 1:
                    raise RuntimeError("target state became unavailable")
                return TargetStateSnapshot(0, 1, "a" * 64, 10)

            async def list_records_async(
                self, cursor: str | None, context: ConnectorCallContext
            ) -> TargetRecordPage:
                return TargetRecordPage((), None, 1, 0)

        report = await _verifier(clock).verify(
            target=cast(TargetConnector, _ClosingFailureTarget()),
            inventory=ExpectedInventory(records=(), absent_keys=(), ambiguous_keys=()),
            context_id="closing-failure",
        )
        assert report.disposition.value == "observation_failed"

    async def test_verify_and_record_requires_an_applied_exact_plan(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        seed_terminal_run(database)
        result = _analysis()
        ReconciliationResultService(writer, reader, now=clock.now).persist(
            run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr-preapply"
        )
        created = RepairPlanningService(writer, reader, now=clock.now).create(
            run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr-preapply"
        )
        assert created.aggregate is not None
        assert created.generated.plan is not None
        service = TargetVerificationService(writer, reader, now=clock.now)
        inventory = build_expected_inventory(result, created.generated.plan)
        target = _IdempotentFakeTarget()
        observed_report = await _verify(clock, cast(TargetConnector, target), inventory)
        assert observed_report.verdict is TargetVerificationVerdict.PARITY_DIVERGENT
        with pytest.raises(RepairPlanMismatchError, match="plan content requires"):
            service.record(
                run_id=RUN_ID,
                report=observed_report,
                reconciliation_fingerprint=result.summary.fingerprint,
                repair_plan_id=None,
                plan_content_fingerprint=StateFingerprint("d" * 64),
                actor="operator-1",
                correlation_id="corr-orphan-content",
            )
        with pytest.raises(RepairPlanMismatchError, match="requires its content"):
            service.record(
                run_id=RUN_ID,
                report=observed_report,
                reconciliation_fingerprint=result.summary.fingerprint,
                repair_plan_id=created.aggregate.plan.repair_plan_id,
                plan_content_fingerprint=None,
                actor="operator-1",
                correlation_id="corr-missing-content",
            )
        with pytest.raises(RepairPlanMismatchError, match="has not been applied"):
            await service.verify_and_record(
                run_id=RUN_ID,
                target=cast(TargetConnector, target),
                inventory=inventory,
                reconciliation_fingerprint=result.summary.fingerprint,
                repair_plan_id=created.aggregate.plan.repair_plan_id,
                plan_content_fingerprint=created.aggregate.plan.content_fingerprint,
                actor="operator-1",
                correlation_id="corr-preapply",
            )
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
        applied = await RepairApplicationService(
            writer,
            reader,
            now=clock.now,
            policy=RepairApplicationPolicy(delay_seconds=0.0, timeout_seconds=10.0),
            sleep=_no_sleep,
        ).apply(
            run_id=RUN_ID,
            repair_plan_id=created.aggregate.plan.repair_plan_id,
            target=cast(TargetConnector, target),
            context_id="corr-apply",
        )
        assert applied.disposition.value == "completed"
        aggregate = reader.load_plan(created.aggregate.plan.repair_plan_id)
        assert aggregate is not None
        with pytest.raises(RepairPlanMismatchError, match="contents do not match"):
            await service.verify_and_record(
                run_id=RUN_ID,
                target=cast(TargetConnector, target),
                inventory=inventory,
                reconciliation_fingerprint=result.summary.fingerprint,
                repair_plan_id=aggregate.plan.repair_plan_id,
                plan_content_fingerprint=StateFingerprint("e" * 64),
                actor="operator-1",
                correlation_id="corr-wrong-content",
            )
        stale_plan = replace(
            aggregate.plan,
            reconciliation_fingerprint=StateFingerprint("f" * 64),
        )

        def load_stale_plan(
            _reader: SQLiteRepairWorkflowReader, _plan_id: object
        ) -> RepairPlanAggregate:
            return replace(aggregate, plan=stale_plan)

        monkeypatch.setattr(
            SQLiteRepairWorkflowReader,
            "load_plan",
            load_stale_plan,
        )
        with pytest.raises(RepairPlanMismatchError, match="another reconciliation"):
            service.record(
                run_id=RUN_ID,
                report=observed_report,
                reconciliation_fingerprint=result.summary.fingerprint,
                repair_plan_id=aggregate.plan.repair_plan_id,
                plan_content_fingerprint=aggregate.plan.content_fingerprint,
                actor="operator-1",
                correlation_id="corr-wrong-reconciliation",
            )

    async def test_recording_persists_the_immutable_fact_and_replays(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            result, plan = await _applied_state(database, writer, reader, clock, warehouse)
            inventory = build_expected_inventory(result, plan)
            content = reader.load_plan(plan.plan_id)
            assert content is not None
            service = TargetVerificationService(writer, reader, now=clock.now)
            target = await open_target(warehouse)
            try:
                record = await service.verify_and_record(
                    run_id=RUN_ID,
                    target=target,
                    inventory=inventory,
                    reconciliation_fingerprint=result.summary.fingerprint,
                    repair_plan_id=content.plan.repair_plan_id,
                    plan_content_fingerprint=content.plan.content_fingerprint,
                    actor="operator-1",
                    correlation_id="corr-verify",
                )
                replayed = await service.verify_and_record(
                    run_id=RUN_ID,
                    target=target,
                    inventory=inventory,
                    reconciliation_fingerprint=result.summary.fingerprint,
                    repair_plan_id=content.plan.repair_plan_id,
                    plan_content_fingerprint=content.plan.content_fingerprint,
                    actor="operator-1",
                    correlation_id="corr-verify",
                )
            finally:
                await target.aclose()
            assert replayed == record
            with database.transaction() as session:
                assert (
                    session.scalar(select(func.count()).select_from(target_state_verifications))
                    == 1
                )
        finally:
            await warehouse.aclose()

    async def test_divergent_observation_records_a_distinct_fact(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            result, plan = await _applied_state(database, writer, reader, clock, warehouse)
            inventory = build_expected_inventory(result, plan)
            target = await open_target(warehouse)
            service = TargetVerificationService(writer, reader, now=clock.now)
            try:
                content = reader.load_plan(plan.plan_id)
                assert content is not None
                first = await service.verify_and_record(
                    run_id=RUN_ID,
                    target=target,
                    inventory=inventory,
                    reconciliation_fingerprint=result.summary.fingerprint,
                    repair_plan_id=content.plan.repair_plan_id,
                    plan_content_fingerprint=content.plan.content_fingerprint,
                    actor="operator-1",
                    correlation_id="corr-verify",
                )
                await target.write_record_async(
                    TargetWriteRequest(
                        sku="GRID-0006",
                        payload=wire_payload("GRID-0006"),
                        idempotency_key="extra-record",
                    ),
                    ConnectorCallContext(correlation_id="extra"),
                )
                divergent = await _verify(clock, target, inventory)
            finally:
                await target.aclose()
            second = service.record(
                run_id=RUN_ID,
                report=divergent,
                reconciliation_fingerprint=result.summary.fingerprint,
                repair_plan_id=None,
                plan_content_fingerprint=None,
                actor="operator-1",
                correlation_id="corr-verify",
            )
            assert first.verdict is TargetVerificationVerdict.PARITY_HOLDING
            assert second.verdict is TargetVerificationVerdict.PARITY_DIVERGENT
            assert first.verification_id != second.verification_id
            with database.transaction() as session:
                assert (
                    session.scalar(select(func.count()).select_from(target_state_verifications))
                    == 2
                )
        finally:
            await warehouse.aclose()


def target_records(warehouse: SimulatedWarehouse) -> dict[str, dict[str, object]]:
    snapshot = warehouse.behavior.state_snapshot()
    records = cast("dict[str, dict[str, object]]", snapshot["records"])
    return {sku: cast("dict[str, object]", value["payload"]) for sku, value in records.items()}
