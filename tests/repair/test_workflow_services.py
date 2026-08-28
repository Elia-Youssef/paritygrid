"""Durable reconciliation persistence, planning, and approval fences (P11.1/P11.2)."""

import pytest
from sqlalchemy import func, select

from paritygrid.adapters.persistence.repair_workflow import SQLiteRepairWorkflowReader
from paritygrid.adapters.persistence.schema import (
    audit_entries,
    execution_events,
    reconciliation_conflicts,
    reconciliation_summaries,
    repair_approvals,
    repair_plans,
    runs,
)
from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter
from paritygrid.application.ports.reconciliation_persistence import (
    ReconciliationInvalidRequestError,
    ReconciliationResultConflictError,
)
from paritygrid.application.ports.repair_audit import RepairPlanStatus
from paritygrid.application.reconciliation.analysis import ReconciliationAnalysis
from paritygrid.application.repair import (
    PersistedReconciliationOutcome,
    ReconciliationResultService,
    RepairApprovalRequest,
    RepairApprovalService,
    RepairPlanningService,
    build_persisted_conflicts,
)
from paritygrid.application.repair.errors import (
    RepairApprovalConflictError,
    RepairPlanMismatchError,
    RepairPlanStateError,
    RepairReconciliationMissingError,
    RepairReconciliationStaleError,
)
from paritygrid.domain.models import RepairPlanId, RunId, StateFingerprint
from tests.repair.conftest import (
    RUN_ID,
    DeterministicClock,
    analysis,
    seed_terminal_run,
    wire_payload,
)


def _mismatch_analysis() -> ReconciliationAnalysis:
    return analysis(
        [wire_payload("GRID-0001"), wire_payload("GRID-0002", quantity=9)],
        [wire_payload("GRID-0001", name="Different")],
    )


def _persist(
    database: SQLiteDatabase,
    writer: SQLiteTransactionalWriter,
    reader: SQLiteRepairWorkflowReader,
    clock: DeterministicClock,
    result: ReconciliationAnalysis,
) -> PersistedReconciliationOutcome:
    service = ReconciliationResultService(writer, reader, now=clock.now)
    return service.persist(
        run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr-11"
    )


class TestReconciliationPersistence:
    def test_persists_summary_conflicts_audit_and_event_atomically(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        seed_terminal_run(database)
        result = _mismatch_analysis()
        outcome = _persist(database, writer, reader, clock, result)
        assert not outcome.replayed
        assert outcome.record.summary.reconciliation_fingerprint == (result.summary.fingerprint)
        with database.transaction() as session:
            assert session.scalar(select(func.count()).select_from(reconciliation_summaries)) == 1
            assert session.scalar(select(func.count()).select_from(reconciliation_conflicts)) == 2
            assert session.scalar(select(func.count()).select_from(audit_entries)) == 1
            assert session.scalar(select(func.count()).select_from(execution_events)) == 1
            run_row = session.execute(select(runs.c.row_version)).scalar_one()
            assert run_row == 4
            event = session.execute(select(execution_events.c.event_kind)).scalars().all()
            assert "reconciliation_persisted" in event

    def test_exact_replay_returns_the_stored_fact_without_new_evidence(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        seed_terminal_run(database)
        result = _mismatch_analysis()
        first = _persist(database, writer, reader, clock, result)
        second = _persist(database, writer, reader, clock, result)
        assert second.replayed
        assert second.record == first.record
        with database.transaction() as session:
            assert session.scalar(select(func.count()).select_from(audit_entries)) == 1
            assert session.scalar(select(func.count()).select_from(execution_events)) == 1

    def test_a_different_analysis_for_one_run_is_rejected(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        seed_terminal_run(database)
        _persist(database, writer, reader, clock, _mismatch_analysis())
        different = analysis([wire_payload("GRID-0001")], [wire_payload("GRID-0001", quantity=8)])
        with pytest.raises(ReconciliationResultConflictError):
            _persist(database, writer, reader, clock, different)

    def test_requires_a_terminal_run(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        seed_terminal_run(database, terminal=False)
        service = ReconciliationResultService(writer, reader, now=clock.now)
        from paritygrid.application.ports.reconciliation_persistence import (
            ReconciliationInvalidRequestError,
        )

        with pytest.raises(ReconciliationInvalidRequestError, match="completed run"):
            service.persist(
                run_id=RUN_ID,
                analysis=_mismatch_analysis(),
                actor="operator-1",
                correlation_id="corr-11",
            )

    def test_unknown_run_is_rejected(
        self,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        from paritygrid.application.ports.execution import ExecutionRecordNotFoundError

        service = ReconciliationResultService(writer, reader, now=clock.now)
        with pytest.raises(ExecutionRecordNotFoundError):
            service.persist(
                run_id=RunId("run_missing"),
                analysis=_mismatch_analysis(),
                actor="operator-1",
                correlation_id="corr-11",
            )

    def test_conflicts_must_cover_the_summary_counts(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        seed_terminal_run(database)
        result = _mismatch_analysis()
        created = clock.now()
        conflicts = build_persisted_conflicts(RUN_ID, result, created)
        # A projection that drops one conflict must be refused by the
        # repository's count validation, exercised through the writer.
        from paritygrid.adapters.persistence.writer.dispatch import dispatch_command
        from paritygrid.application.repair.companions import (
            build_companions,
            frontier_from_evidence,
        )
        from paritygrid.application.writes.reconciliation import PersistReconciliation

        evidence = reader.load(RUN_ID)
        companions = build_companions(
            frontier=frontier_from_evidence(evidence),
            run_id=RUN_ID,
            operation="reconciliation_persisted",
            object_kind="reconciliation_summary",
            object_id=RUN_ID.value,
            actor="operator-1",
            correlation_id="corr-11",
            occurred_at=created,
            payload={"reconciliation_fingerprint": result.summary.fingerprint.value},
        )
        command = PersistReconciliation(
            run_id=RUN_ID,
            summary=result.summary,
            conflicts=conflicts[:1],
            created_at=created,
            companions=companions,
        )
        with (
            pytest.raises(ReconciliationInvalidRequestError, match="must cover the summary"),
            database.transaction() as session,
        ):
            dispatch_command(session, command)
        with database.transaction() as session:
            assert session.scalar(select(func.count()).select_from(reconciliation_summaries)) == 0


class TestPlanningService:
    def test_creates_the_plan_durably_with_all_evidence(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        seed_terminal_run(database)
        result = _mismatch_analysis()
        _persist(database, writer, reader, clock, result)
        service = RepairPlanningService(writer, reader, now=clock.now)
        created = service.create(
            run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr-11"
        )
        assert not created.replayed
        assert created.aggregate is not None
        assert created.aggregate.plan.status is RepairPlanStatus.PROPOSED
        assert created.aggregate.plan.reconciliation_fingerprint == (result.summary.fingerprint)
        assert len(created.aggregate.actions) == 2
        with database.transaction() as session:
            assert session.scalar(select(func.count()).select_from(repair_plans)) == 1
            assert session.scalar(select(func.count()).select_from(audit_entries)) == 2
            kinds = session.execute(select(execution_events.c.event_kind)).scalars().all()
            assert kinds.count("repair_plan_created") == 1

    def test_regeneration_replays_the_identical_plan(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        seed_terminal_run(database)
        result = _mismatch_analysis()
        _persist(database, writer, reader, clock, result)
        service = RepairPlanningService(writer, reader, now=clock.now)
        first = service.create(
            run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr-11"
        )
        second = service.create(
            run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr-11"
        )
        assert second.replayed
        assert second.aggregate is not None
        assert first.aggregate is not None
        assert second.aggregate.plan.repair_plan_id == first.aggregate.plan.repair_plan_id
        with database.transaction() as session:
            assert session.scalar(select(func.count()).select_from(repair_plans)) == 1

    def test_regeneration_after_approval_returns_the_durable_winner(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        from paritygrid.application.ports.consistency import RedactedDocument

        seed_terminal_run(database)
        result = _mismatch_analysis()
        _persist(database, writer, reader, clock, result)
        service = RepairPlanningService(writer, reader, now=clock.now)
        first = service.create(
            run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr-11"
        )
        assert first.aggregate is not None
        approval = RepairApprovalService(writer, reader, now=clock.now).approve(
            RepairApprovalRequest(
                run_id=RUN_ID,
                repair_plan_id=first.aggregate.plan.repair_plan_id,
                approved_by="approver-1",
                correlation_id="corr-approve",
                approved_content_fingerprint=first.aggregate.plan.content_fingerprint,
                approved_reconciliation_fingerprint=result.summary.fingerprint,
                detail=RedactedDocument.from_mapping({"decision": "ok"}),
            )
        )
        after = service.create(
            run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr-11"
        )
        assert after.replayed
        assert after.aggregate is not None
        assert after.aggregate.plan.repair_plan_id == first.aggregate.plan.repair_plan_id
        # The durable lifecycle the approver created is untouched by the
        # regeneration: still approved, still one immutable approval fact.
        assert after.aggregate.plan.status is RepairPlanStatus.APPROVED
        assert approval.aggregate.approval is not None
        assert after.aggregate.approval == approval.aggregate.approval
        with database.transaction() as session:
            assert session.scalar(select(func.count()).select_from(repair_plans)) == 1

    def test_requires_a_persisted_reconciliation(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        seed_terminal_run(database)
        service = RepairPlanningService(writer, reader, now=clock.now)
        with pytest.raises(RepairReconciliationMissingError):
            service.create(
                run_id=RUN_ID,
                analysis=_mismatch_analysis(),
                actor="operator-1",
                correlation_id="corr-11",
            )

    def test_rejects_an_analysis_that_is_not_the_durable_snapshot(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        seed_terminal_run(database)
        _persist(database, writer, reader, clock, _mismatch_analysis())
        service = RepairPlanningService(writer, reader, now=clock.now)
        with pytest.raises(RepairReconciliationStaleError):
            service.create(
                run_id=RUN_ID,
                analysis=analysis([wire_payload("GRID-0009")], []),
                actor="operator-1",
                correlation_id="corr-11",
            )

    def test_empty_generation_creates_no_durable_plan(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        seed_terminal_run(database)
        clean = analysis([wire_payload("GRID-0001")], [wire_payload("GRID-0001")])
        _persist(database, writer, reader, clock, clean)
        service = RepairPlanningService(writer, reader, now=clock.now)
        created = service.create(
            run_id=RUN_ID, analysis=clean, actor="operator-1", correlation_id="corr-11"
        )
        assert created.aggregate is None
        with database.transaction() as session:
            assert session.scalar(select(func.count()).select_from(repair_plans)) == 0
            assert session.scalar(select(func.count()).select_from(execution_events)) == 1


class TestApprovalService:
    def _prepared(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> tuple[ReconciliationAnalysis, RepairPlanId, StateFingerprint]:
        seed_terminal_run(database)
        result = _mismatch_analysis()
        _persist(database, writer, reader, clock, result)
        created = RepairPlanningService(writer, reader, now=clock.now).create(
            run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr-11"
        )
        assert created.aggregate is not None
        return (
            result,
            created.aggregate.plan.repair_plan_id,
            (created.aggregate.plan.content_fingerprint),
        )

    def _request(
        self,
        plan_id: RepairPlanId,
        result: ReconciliationAnalysis,
        content: StateFingerprint,
        *,
        actor: str = "approver-1",
        correlation: str = "corr-approve",
    ) -> RepairApprovalRequest:
        from paritygrid.application.ports.consistency import RedactedDocument

        return RepairApprovalRequest(
            run_id=RUN_ID,
            repair_plan_id=plan_id,
            approved_by=actor,
            correlation_id=correlation,
            approved_content_fingerprint=content,
            approved_reconciliation_fingerprint=result.summary.fingerprint,
            detail=RedactedDocument.from_mapping({"decision": "approved after review"}),
        )

    def test_approval_of_the_exact_current_plan_persists_an_immutable_fact(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        result, plan_id, content = self._prepared(database, writer, reader, clock)
        service = RepairApprovalService(writer, reader, now=clock.now)
        outcome = service.approve(self._request(plan_id, result, content))
        assert not outcome.replayed
        assert outcome.aggregate.plan.status is RepairPlanStatus.APPROVED
        assert outcome.aggregate.approval is not None
        assert outcome.aggregate.approval.approved_by == "approver-1"
        with database.transaction() as session:
            assert session.scalar(select(func.count()).select_from(repair_approvals)) == 1
            kinds = session.execute(select(execution_events.c.event_kind)).scalars().all()
            assert kinds.count("repair_plan_approved") == 1

    def test_exact_approval_retry_replays_the_immutable_fact(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        result, plan_id, content = self._prepared(database, writer, reader, clock)
        service = RepairApprovalService(writer, reader, now=clock.now)
        service.approve(self._request(plan_id, result, content))
        retry = service.approve(self._request(plan_id, result, content))
        assert retry.replayed
        assert retry.aggregate.plan.status is RepairPlanStatus.APPROVED
        with database.transaction() as session:
            assert session.scalar(select(func.count()).select_from(repair_approvals)) == 1

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("actor", "approver-2"),
            ("correlation", "corr-divergent"),
        ],
    )
    def test_divergent_approval_replay_is_rejected(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
        field: str,
        value: str,
    ) -> None:
        result, plan_id, content = self._prepared(database, writer, reader, clock)
        service = RepairApprovalService(writer, reader, now=clock.now)
        service.approve(self._request(plan_id, result, content))
        divergent = self._request(plan_id, result, content, **{field: value})
        from paritygrid.application.repair.errors import (
            RepairApprovalConflictError as WorkflowConflict,
        )

        with pytest.raises(WorkflowConflict):
            service.approve(divergent)

    def test_approval_with_a_mismatched_content_fingerprint_is_rejected(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        result, plan_id, _content = self._prepared(database, writer, reader, clock)
        service = RepairApprovalService(writer, reader, now=clock.now)
        with pytest.raises(RepairPlanMismatchError, match="contents"):
            service.approve(self._request(plan_id, result, StateFingerprint("e" * 64)))

    def test_approval_with_a_stale_reconciliation_fingerprint_is_rejected(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        _, plan_id, content = self._prepared(database, writer, reader, clock)
        stale = analysis([wire_payload("GRID-0007")], [wire_payload("GRID-0007", quantity=3)])
        service = RepairApprovalService(writer, reader, now=clock.now)
        with pytest.raises(RepairPlanMismatchError, match="reconciliation"):
            service.approve(self._request(plan_id, stale, content))

    def test_an_already_approved_plan_cannot_be_reapproved_with_new_content(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        result, plan_id, content = self._prepared(database, writer, reader, clock)
        service = RepairApprovalService(writer, reader, now=clock.now)
        service.approve(self._request(plan_id, result, content))
        with pytest.raises(RepairApprovalConflictError, match="immutable durable approval"):
            service.approve(
                self._request(
                    plan_id,
                    result,
                    content,
                    actor="approver-2",
                    correlation="corr-second",
                )
            )

    def test_rejected_plan_cannot_be_approved_afterwards(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        result, plan_id, content = self._prepared(database, writer, reader, clock)
        service = RepairApprovalService(writer, reader, now=clock.now)
        service.reject(run_id=RUN_ID, repair_plan_id=plan_id, correlation_id="corr-reject")
        with pytest.raises(RepairPlanStateError):
            service.approve(self._request(plan_id, result, content))

    def test_unknown_or_foreign_plan_is_rejected(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        result, _plan_id, content = self._prepared(database, writer, reader, clock)
        service = RepairApprovalService(writer, reader, now=clock.now)
        with pytest.raises(RepairPlanMismatchError, match="does not exist"):
            service.approve(self._request(RepairPlanId("rpl_missing"), result, content))

    def test_no_durable_state_changes_when_the_approval_is_rejected(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        result, plan_id, _content = self._prepared(database, writer, reader, clock)
        service = RepairApprovalService(writer, reader, now=clock.now)
        before = reader.load(RUN_ID)
        with pytest.raises(RepairPlanMismatchError):
            service.approve(self._request(plan_id, result, StateFingerprint("e" * 64)))
        after = reader.load(RUN_ID)
        aggregate = reader.load_plan(plan_id)
        assert aggregate is not None
        assert aggregate.plan.status is RepairPlanStatus.PROPOSED
        assert after.run.row_version == before.run.row_version
        with database.transaction() as session:
            assert session.scalar(select(func.count()).select_from(repair_approvals)) == 0
