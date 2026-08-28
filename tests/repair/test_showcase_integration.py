"""Showcase-scale reconciliation-to-verification integration proof (P11.5).

One bounded scenario drives the complete Phase 11 workflow over a seeded
Phase 8 dataset: persist the Phase 10 reconciliation, generate and durably
create the plan, approve it exactly, apply every effect idempotently
through the Phase 9 connector against the Phase 8 warehouse, then verify
parity by independent observation and record the immutable verification
fact. Exact counts, fingerprints, audit and event evidence, and target
request bounds are asserted end to end.
"""

from typing import cast

import pytest
from sqlalchemy import func, select

from paritygrid.adapters.persistence.repair_workflow import SQLiteRepairWorkflowReader
from paritygrid.adapters.persistence.schema import (
    audit_entries,
    execution_events,
    reconciliation_conflicts,
    reconciliation_summaries,
    repair_actions,
    repair_plans,
    target_state_verifications,
)
from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter
from paritygrid.application.ports.connectors import (
    ConnectorCallContext,
    TargetConnector,
    TargetWriteRequest,
)
from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.application.ports.reconciliation_persistence import (
    TargetVerificationVerdict,
)
from paritygrid.application.reconciliation.analysis import (
    ReconciliationAnalysis,
    ReconciliationAnalysisRequest,
    analyze_reconciliation,
)
from paritygrid.application.repair import (
    ReconciliationResultService,
    RepairApplicationPolicy,
    RepairApplicationService,
    RepairApprovalRequest,
    RepairApprovalService,
    RepairPlanningService,
    TargetParityVerifier,
    TargetVerificationService,
    build_expected_inventory,
)
from paritygrid.application.repair.payloads import render_target_payload
from paritygrid.demo.datasets import (
    DatasetProfile,
    ScenarioSeed,
    ScenarioVersion,
    SyntheticDataset,
    WireRow,
    generate_dataset,
)
from paritygrid.demo.simulators.warehouse import SimulatedWarehouse
from paritygrid.domain.reconciliation import (
    ReconciliationClassification,
    SourceObservation,
)
from tests.repair.conftest import (
    RUN_ID,
    SOURCE_CONNECTOR,
    TARGET_CONNECTOR,
    DeterministicClock,
    open_target,
    seed_terminal_run,
)

pytestmark = pytest.mark.anyio

SCENARIO_SEED = 9104
SHOWCASE_RECORDS = 500


def _dataset() -> SyntheticDataset:
    return generate_dataset(
        ScenarioSeed(SCENARIO_SEED),
        ScenarioVersion(1),
        DatasetProfile(
            record_count=SHOWCASE_RECORDS,
            malformed_count=12,
            boundary_count=8,
            duplicate_count=40,
        ),
    )


def _wire_payload(row: WireRow) -> dict[str, object]:
    payload: dict[str, object] = dict(row.payload)
    return payload


def _showcase_analysis(dataset: SyntheticDataset) -> tuple[ReconciliationAnalysis, int, int]:
    """Reconcile the seeded source against a derived divergent target.

    Every dataset row feeds the source side (malformed rows exercise
    quarantine); the target side is derived from the first record of each
    canonical key, with a deterministic subset dropped and tweaked so the
    expected classification counts stay exactly assertable.
    """
    request_source = tuple(
        SourceObservation(position=index, connector_id=SOURCE_CONNECTOR, payload=_wire_payload(row))
        for index, row in enumerate(dataset.rows)
    )
    key_counts: dict[str, int] = {}
    first_by_key: dict[str, dict[str, object]] = {}
    for row in dataset.rows:
        if row.role.value == "malformed":
            continue
        payload = _wire_payload(row)
        sku = cast(str, payload.get("sku"))
        key_counts[sku] = key_counts.get(sku, 0) + 1
        first_by_key.setdefault(sku, payload)
    target_payloads: list[dict[str, object]] = []
    # Keys with repeated source rows classify as duplicate-source regardless
    # of the derived target, so only unduplicated keys count toward the
    # expected missing and mismatch totals.
    dropped = 0
    tweaked = 0
    for index, (sku, payload) in enumerate(first_by_key.items()):
        if key_counts[sku] > 1:
            continue
        if index % 7 == 3:
            dropped += 1
            continue
        candidate = payload
        if index % 11 == 5:
            tweaked += 1
            candidate = dict(payload)
            candidate["quantity"] = int(cast(int, payload.get("quantity", 1))) + 3
        target_payloads.append(candidate)
    request_target = tuple(
        SourceObservation(
            position=100_000 + index,
            connector_id=TARGET_CONNECTOR,
            payload=payload,
        )
        for index, payload in enumerate(target_payloads)
    )
    return (
        analyze_reconciliation(_request(request_source, request_target, dataset)),
        dropped,
        tweaked,
    )


def _request(
    source: tuple[SourceObservation, ...],
    target: tuple[SourceObservation, ...],
    dataset: SyntheticDataset,
) -> ReconciliationAnalysisRequest:
    return ReconciliationAnalysisRequest(
        source_observations=source,
        target_observations=target,
        source_input_identity=dataset.manifest.dataset_id,
        target_input_identity=dataset.manifest.dataset_id,
    )


async def _load_target(target: TargetConnector, analysis: ReconciliationAnalysis) -> None:
    """Load the warehouse with the reconciliation's target-side records."""
    for key in analysis.classification.keys:
        for record in key.outcome.target_records:
            await target.write_record_async(
                TargetWriteRequest(
                    sku=record.sku,
                    payload=render_target_payload(record),
                    idempotency_key=f"showcase-seed-{record.sku}",
                ),
                ConnectorCallContext(correlation_id="showcase-seed"),
            )


class TestShowcaseIntegration:
    async def test_complete_workflow_proves_parity_end_to_end(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        dataset = _dataset()
        analysis, dropped, tweaked = _showcase_analysis(dataset)
        seed_terminal_run(database)
        warehouse = SimulatedWarehouse()
        await warehouse.start()
        try:
            target = await open_target(warehouse)
            try:
                await _load_target(target, analysis)
                seed_writes = warehouse.request_count()

                persisted = ReconciliationResultService(writer, reader, now=clock.now).persist(
                    run_id=RUN_ID,
                    analysis=analysis,
                    actor="operator-1",
                    correlation_id="showcase",
                )
                assert not persisted.replayed
                counts = dict(analysis.summary.counts.by_classification)
                missing = counts[ReconciliationClassification.MISSING_FROM_TARGET]
                mismatch = counts[ReconciliationClassification.FIELD_MISMATCH]
                assert missing == dropped
                assert mismatch == tweaked
                assert missing + mismatch > 50

                created = RepairPlanningService(writer, reader, now=clock.now).create(
                    run_id=RUN_ID,
                    analysis=analysis,
                    actor="operator-1",
                    correlation_id="showcase",
                )
                assert created.aggregate is not None
                assert created.generated.plan is not None
                action_count = len(created.generated.plan.actions)
                assert action_count == missing + mismatch

                approved = RepairApprovalService(writer, reader, now=clock.now).approve(
                    RepairApprovalRequest(
                        run_id=RUN_ID,
                        repair_plan_id=created.aggregate.plan.repair_plan_id,
                        approved_by="approver-1",
                        correlation_id="showcase-approve",
                        approved_content_fingerprint=(created.aggregate.plan.content_fingerprint),
                        approved_reconciliation_fingerprint=analysis.summary.fingerprint,
                        detail=RedactedDocument.from_mapping({"decision": "showcase approval"}),
                    )
                )
                assert not approved.replayed

                applied = await RepairApplicationService(
                    writer,
                    reader,
                    now=clock.now,
                    policy=RepairApplicationPolicy(delay_seconds=0.0, timeout_seconds=30.0),
                ).apply(
                    run_id=RUN_ID,
                    repair_plan_id=created.aggregate.plan.repair_plan_id,
                    target=target,
                    context_id="showcase-apply",
                )
                assert applied.disposition.value == "completed"
                repair_writes = warehouse.request_count() - seed_writes
                assert repair_writes == action_count

                inventory = build_expected_inventory(analysis, created.generated.plan)
                report = await TargetParityVerifier(now=clock.now).verify(
                    target=target,
                    inventory=inventory,
                    context_id="showcase-verify",
                )
                assert report.verdict is TargetVerificationVerdict.PARITY_HOLDING
                assert report.observed is not None
                # Every canonical key that the target must hold after repairs:
                # the review-only duplicate-source keys stay absent by design.
                assert report.observed.record_count == len(inventory.records)
                assert report.observed.record_count == (
                    analysis.summary.counts.canonical_key_count - len(inventory.absent_keys)
                )
                # The warehouse counts mutating requests only, so the read
                # bound is structural: one bounded read per expected record.

                recorded = TargetVerificationService(writer, reader, now=clock.now).record(
                    run_id=RUN_ID,
                    report=report,
                    reconciliation_fingerprint=analysis.summary.fingerprint,
                    repair_plan_id=created.aggregate.plan.repair_plan_id,
                    plan_content_fingerprint=created.aggregate.plan.content_fingerprint,
                    actor="operator-1",
                    correlation_id="showcase-verify",
                )
                assert recorded.verdict is TargetVerificationVerdict.PARITY_HOLDING

                # Re-application and re-verification are bounded no-ops that
                # reproduce the identical fingerprint without new effects.
                requests_before = warehouse.request_count()
                replayed = await RepairApplicationService(
                    writer,
                    reader,
                    now=clock.now,
                    policy=RepairApplicationPolicy(delay_seconds=0.0, timeout_seconds=30.0),
                ).apply(
                    run_id=RUN_ID,
                    repair_plan_id=created.aggregate.plan.repair_plan_id,
                    target=target,
                    context_id="showcase-reapply",
                )
                assert replayed.disposition.value == "already_applied"
                assert warehouse.request_count() == requests_before
                second = await TargetParityVerifier(now=clock.now).verify(
                    target=target,
                    inventory=inventory,
                    context_id="showcase-reverify",
                )
                assert second.observed is not None
                assert report.observed is not None
                assert second.observed.fingerprint == report.observed.fingerprint
            finally:
                await target.aclose()

            with database.transaction() as session:
                assert (
                    session.scalar(select(func.count()).select_from(reconciliation_summaries)) == 1
                )
                assert session.scalar(
                    select(func.count()).select_from(reconciliation_conflicts)
                ) == sum(
                    count
                    for classification, count in (analysis.summary.counts.by_classification)
                    if classification is not ReconciliationClassification.MATCH
                )
                assert session.scalar(select(func.count()).select_from(repair_plans)) == 1
                assert (
                    session.scalar(select(func.count()).select_from(repair_actions)) == action_count
                )
                assert (
                    session.scalar(select(func.count()).select_from(target_state_verifications))
                    == 1
                )
                audit_total = session.scalar(select(func.count()).select_from(audit_entries))
                # persist, plan creation, approval, begin, complete, verify.
                assert audit_total == 6 + action_count
                events_total = session.scalar(select(func.count()).select_from(execution_events))
                assert events_total == 6 + action_count
                kinds = session.execute(select(execution_events.c.event_kind)).scalars().all()
                for kind in (
                    "reconciliation_persisted",
                    "repair_plan_created",
                    "repair_plan_approved",
                    "repair_application_started",
                    "repair_action_applied",
                    "repair_application_completed",
                    "target_state_verified",
                ):
                    assert kind in kinds
            # The warehouse honored its capacity and version budget: exactly
            # one version advance per logical effect.
            assert warehouse.behavior.target_version == (seed_writes + action_count)
        finally:
            await warehouse.aclose()
