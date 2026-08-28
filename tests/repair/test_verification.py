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
from paritygrid.application.reconciliation.analysis import ReconciliationAnalysis
from paritygrid.application.repair import (
    ExpectedInventory,
    ReconciliationResultService,
    RepairApplicationPolicy,
    RepairApplicationService,
    RepairApprovalRequest,
    RepairApprovalService,
    RepairPlanningService,
    TargetParityVerifier,
    TargetVerificationReport,
    TargetVerificationService,
    build_expected_inventory,
)
from paritygrid.application.repair.errors import RepairPlanMismatchError
from paritygrid.demo.simulators.warehouse import SimulatedWarehouse
from paritygrid.domain.repair import RepairPlan
from tests.repair.conftest import (
    RUN_ID,
    DeterministicClock,
    analysis,
    open_target,
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
            assert record is actions[sku].proposed_record
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
            target = await open_target(warehouse)
            try:
                report = await _verify(clock, target, inventory)
            finally:
                await target.aclose()
            from paritygrid.application.repair.identities import derive_plan_id

            plan_id = derive_plan_id(RUN_ID, result.summary.fingerprint)
            content = reader.load_plan(plan_id)
            assert content is not None
            service = TargetVerificationService(writer, reader, now=clock.now)
            record = service.record(
                run_id=RUN_ID,
                report=report,
                reconciliation_fingerprint=result.summary.fingerprint,
                repair_plan_id=plan_id,
                plan_content_fingerprint=content.plan.content_fingerprint,
                actor="operator-1",
                correlation_id="corr-verify",
            )
            replayed = service.record(
                run_id=RUN_ID,
                report=report,
                reconciliation_fingerprint=result.summary.fingerprint,
                repair_plan_id=plan_id,
                plan_content_fingerprint=content.plan.content_fingerprint,
                actor="operator-1",
                correlation_id="corr-verify",
            )
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
            try:
                holding = await _verify(clock, target, inventory)
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
            service = TargetVerificationService(writer, reader, now=clock.now)
            first = service.record(
                run_id=RUN_ID,
                report=holding,
                reconciliation_fingerprint=result.summary.fingerprint,
                repair_plan_id=None,
                plan_content_fingerprint=None,
                actor="operator-1",
                correlation_id="corr-verify",
            )
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
