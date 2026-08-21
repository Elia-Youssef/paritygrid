"""Behavioral tests for repair and audit repository invariants."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from threading import Barrier, Lock
from types import MethodType
from typing import NoReturn, cast

import pytest
from sqlalchemy import event as sqlalchemy_event
from sqlalchemy import insert, select, text
from sqlalchemy import update as sql_update
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence import SQLiteDatabase, SQLiteDatabaseConfig
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyAuditRepository,
    SqlAlchemyPipelineRepository,
    SqlAlchemyRepairRepository,
    SqlAlchemyRunRepository,
)
from paritygrid.adapters.persistence.repositories import repairs as repair_module
from paritygrid.adapters.persistence.repositories.repair_audit_common import (
    effect_content_fingerprint,
    encode_application_result,
    plan_content_fingerprint,
)
from paritygrid.adapters.persistence.schema import (
    audit_entries,
    reconciliation_conflicts,
    reconciliation_summaries,
    repair_actions,
    runs,
)
from paritygrid.application.ports import ConfigurationDocument
from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.application.ports.repair_audit import (
    MAX_PERSISTED_INTEGER,
    AuditCorruptionError,
    AuditSequence,
    AuditSequenceConflictError,
    AuditStorageError,
    PendingAuditEntry,
    RepairActionCursor,
    RepairActionEffect,
    RepairActionKeyMap,
    RepairActionStatus,
    RepairApplicationBeginDisposition,
    RepairApplicationBeginResult,
    RepairApplicationConflictError,
    RepairApplicationResult,
    RepairApprovalConflictError,
    RepairCorruptionError,
    RepairDuplicateError,
    RepairInvalidRequestError,
    RepairPlanAggregate,
    RepairPlanContentConflictError,
    RepairPlanCursor,
    RepairPlanStatus,
    RepairRecordNotFoundError,
    RepairStaleRowVersionError,
    RepairStateConflictError,
)
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    ConflictId,
    ConnectorId,
    CurrencyCode,
    InventoryRecord,
    Money,
    NodeId,
    PipelineId,
    PipelineVersion,
    RepairActionId,
    RepairPlanId,
    RunId,
    StateFingerprint,
    UtcTimestamp,
)
from paritygrid.domain.repair import RepairAction, RepairActionKind, RepairPlan

PIPELINE_ID = PipelineId("pip_repair-tests")
RUN_ID = RunId("run_repair-tests")
PLAN_ID = RepairPlanId("rpl_repair-tests")
ACTION_ID = RepairActionId("rac_repair-tests")
CONFLICT_ID = ConflictId("cnf_repair-tests")
RECONCILIATION = StateFingerprint("4" * 64)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[SQLiteDatabase]:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "répair state %25.db"))
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    try:
        yield database
    finally:
        database.close()


def timestamp(second: int) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 12, 12, 0, second, tzinfo=UTC))


def redacted(**values: object) -> RedactedDocument:
    return RedactedDocument.from_mapping(values)


def record(*, sku: str = "HARBOR-LAMP", name: str = "Harbor Lamp") -> InventoryRecord:
    return InventoryRecord.create(
        sku=sku,
        name=name,
        quantity=7,
        unit_price=Money(Decimal("45.99"), CurrencyCode("USD"), 2),
        updated_at=timestamp(1),
        connector_id=ConnectorId("con_repair-source"),
        source_record_key="source-record-1",
        attributes={"finish": "Brass"},
    )


def plan() -> RepairPlan:
    return RepairPlan(
        plan_id=PLAN_ID,
        state_fingerprint=RECONCILIATION,
        actions=(
            RepairAction(
                action_id=ACTION_ID,
                conflict_id=CONFLICT_ID,
                state_fingerprint=RECONCILIATION,
                kind=RepairActionKind.CREATE_TARGET,
                proposed_record=record(),
            ),
        ),
    )


def seed_reconciliation(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PIPELINE_ID,
            display_name="Repair tests",
            description=None,
            created_at=timestamp(0),
        )
        pipelines.publish_version(
            pipeline_id=PIPELINE_ID,
            expected_latest_version=None,
            specification=ConfigurationDocument.from_mapping({"nodes": []}),
            planner_format_version=1,
            published_at=timestamp(0),
        )
        run_repository = SqlAlchemyRunRepository(session)
        run_repository.create(
            run_id=RUN_ID,
            pipeline_id=PIPELINE_ID,
            pipeline_version=PipelineVersion(1),
            runner_kind="sequential",
            runner_configuration=ConfigurationDocument.from_mapping({}),
            scenario_seed=None,
            node_ids=(NodeId("nod_repair-node"),),
            created_at=timestamp(0),
        )
        run_repository.transition(
            RUN_ID,
            expected_row_version=1,
            target_state=RunState.RUNNING,
            transitioned_at=timestamp(0),
        )
        run_repository.transition(
            RUN_ID,
            expected_row_version=2,
            target_state=RunState.SUCCEEDED,
            transitioned_at=timestamp(1),
            execution_evidence_fingerprint=RECONCILIATION,
            execution_evidence_fingerprint_version=2,
        )
        session.execute(
            insert(reconciliation_summaries).values(
                run_id=RUN_ID.value,
                match_count=0,
                missing_from_target_count=1,
                missing_from_source_count=0,
                field_mismatch_count=0,
                duplicate_source_count=0,
                duplicate_target_count=0,
                duplicate_both_count=0,
                total_count=1,
                source_fingerprint="1" * 64,
                target_fingerprint="2" * 64,
                reconciliation_fingerprint=RECONCILIATION.value,
                analytical_query_version=1,
                created_at=str(timestamp(1)),
            )
        )
        session.execute(
            insert(reconciliation_conflicts).values(
                conflict_id=CONFLICT_ID.value,
                run_id=RUN_ID.value,
                canonical_key="HARBOR-LAMP",
                classification="missing_from_target",
                source_references_json="[]",
                target_reference_json=None,
                field_differences_json="[]",
                suggested_resolution="create_target",
                created_at=str(timestamp(1)),
            )
        )
        session.execute(
            insert(reconciliation_conflicts),
            (
                {
                    "conflict_id": "cnf_second-repair",
                    "run_id": RUN_ID.value,
                    "canonical_key": "SECOND-LAMP",
                    "classification": "missing_from_target",
                    "source_references_json": "[]",
                    "target_reference_json": None,
                    "field_differences_json": "[]",
                    "suggested_resolution": "create_target",
                    "created_at": str(timestamp(1)),
                },
                {
                    "conflict_id": "cnf_update-repair",
                    "run_id": RUN_ID.value,
                    "canonical_key": "UPDATE-LAMP",
                    "classification": "field_mismatch",
                    "source_references_json": "[]",
                    "target_reference_json": "{}",
                    "field_differences_json": "[]",
                    "suggested_resolution": "update_target",
                    "created_at": str(timestamp(1)),
                },
            ),
        )


def create_plan(repository: SqlAlchemyRepairRepository) -> RepairPlanAggregate:
    return repository.create_plan(
        run_id=RUN_ID,
        plan=plan(),
        action_keys=RepairActionKeyMap.from_mapping({ACTION_ID: "repair-harbor-lamp-v1"}),
        created_at=timestamp(2),
    )


def approve(repository: SqlAlchemyRepairRepository) -> RepairPlanAggregate:
    return repository.approve(
        PLAN_ID,
        expected_row_version=1,
        current_reconciliation_fingerprint=RECONCILIATION,
        approved_by="operator-1",
        approved_at=timestamp(3),
        correlation_id="corr-repair-1",
        schema_version=1,
        detail=redacted(reason="Reviewed synthetic evidence"),
    )


def second_plan() -> RepairPlan:
    return RepairPlan(
        RepairPlanId("rpl_second-repair"),
        RECONCILIATION,
        (
            RepairAction(
                RepairActionId("rac_second-repair"),
                ConflictId("cnf_second-repair"),
                RECONCILIATION,
                RepairActionKind.CREATE_TARGET,
                record(sku="SECOND-LAMP", name="Second Lamp"),
            ),
        ),
    )


def update_plan() -> RepairPlan:
    return RepairPlan(
        RepairPlanId("rpl_update-repair"),
        RECONCILIATION,
        (
            RepairAction(
                RepairActionId("rac_update-repair"),
                ConflictId("cnf_update-repair"),
                RECONCILIATION,
                RepairActionKind.UPDATE_TARGET,
                record(sku="UPDATE-LAMP", name="New Lamp"),
                record(sku="UPDATE-LAMP", name="Old Lamp"),
            ),
        ),
    )


def multi_plan() -> RepairPlan:
    return RepairPlan(
        RepairPlanId("rpl_multi-repair"),
        RECONCILIATION,
        (plan().actions[0], second_plan().actions[0]),
    )


def test_canonical_effect_projection_matches_original_plan_protocol() -> None:
    domain_plan = plan()
    effect = RepairActionEffect.from_action(domain_plan.actions[0])
    assert effect_content_fingerprint(PLAN_ID, RECONCILIATION, (effect,)) == (
        plan_content_fingerprint(domain_plan)
    )
    assert plan_content_fingerprint(domain_plan).value == (
        "cdf2fb7065338c116f6ebbf6cc3f19189743c1e42578f7d7d3b611ee813bcaf9"
    )


def test_repair_plan_full_application_and_exact_replays(database: SQLiteDatabase) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        proposed = create_plan(repository)
        replay = create_plan(repository)
        assert replay == proposed
        assert proposed.plan.status is RepairPlanStatus.PROPOSED
        assert proposed.plan.row_version == 1
        approved = approve(repository)
        assert approved.plan.status is RepairPlanStatus.APPROVED
        assert approved.plan.row_version == 2
        begun = repository.begin_application(
            PLAN_ID,
            expected_row_version=2,
            current_reconciliation_fingerprint=RECONCILIATION,
            applying_at=timestamp(4),
        )
        assert begun.disposition is RepairApplicationBeginDisposition.STARTED
        assert begun.reservation is not None
        observed = repository.begin_application(
            PLAN_ID,
            expected_row_version=2,
            current_reconciliation_fingerprint=RECONCILIATION,
            applying_at=timestamp(4),
        )
        assert observed.disposition is RepairApplicationBeginDisposition.IN_PROGRESS_REPLAY
        assert observed.reservation is None
        applied_action = repository.record_action_applied(
            begun.reservation,
            ACTION_ID,
            result=RepairApplicationResult(1, redacted(effect="created")),
            target_version=1,
            applied_at=timestamp(5),
        )
        assert applied_action.action.status is RepairActionStatus.APPLIED
        assert applied_action.reservation.row_version == 4
        retried = repository.record_action_applied(
            begun.reservation,
            ACTION_ID,
            result=RepairApplicationResult(1, redacted(effect="created")),
            target_version=1,
            applied_at=timestamp(5),
        )
        assert retried == applied_action
        completed = repository.complete_application(
            applied_action.reservation, applied_at=timestamp(6)
        )
        assert completed.plan.status is RepairPlanStatus.APPLIED
        assert completed.plan.row_version == 5
        terminal_observation = repository.begin_application(
            PLAN_ID,
            expected_row_version=2,
            current_reconciliation_fingerprint=StateFingerprint("9" * 64),
            applying_at=timestamp(4),
        )
        assert terminal_observation.disposition is RepairApplicationBeginDisposition.APPLIED_REPLAY
        assert (
            repository.complete_application(applied_action.reservation, applied_at=timestamp(6))
            == completed
        )
        assert approve(repository) == completed


def test_failed_action_atomically_terminalizes_plan(database: SQLiteDatabase) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        create_plan(repository)
        approve(repository)
        begun = repository.begin_application(
            PLAN_ID,
            expected_row_version=2,
            current_reconciliation_fingerprint=RECONCILIATION,
            applying_at=timestamp(4),
        )
        assert begun.reservation is not None
        failed = repository.record_action_failed(
            begun.reservation,
            ACTION_ID,
            result=RepairApplicationResult(1, redacted(outcome="failed")),
            failed_at=timestamp(5),
            plan_failure=redacted(reason="target_conflict"),
        )
        assert failed.plan.status is RepairPlanStatus.FAILED
        assert failed.plan.row_version == 4
        assert failed.actions[0].status is RepairActionStatus.FAILED
        terminal_observation = repository.begin_application(
            PLAN_ID,
            expected_row_version=2,
            current_reconciliation_fingerprint=StateFingerprint("9" * 64),
            applying_at=timestamp(4),
        )
        assert terminal_observation.disposition is RepairApplicationBeginDisposition.FAILED_REPLAY
        assert (
            repository.record_action_failed(
                begun.reservation,
                ACTION_ID,
                result=RepairApplicationResult(1, redacted(outcome="failed")),
                failed_at=timestamp(5),
                plan_failure=redacted(reason="target_conflict"),
            )
            == failed
        )


def test_fresh_fingerprint_is_required_for_approval(database: SQLiteDatabase) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        create_plan(repository)
        with pytest.raises(RepairStateConflictError):
            repository.approve(
                PLAN_ID,
                expected_row_version=1,
                current_reconciliation_fingerprint=StateFingerprint("9" * 64),
                approved_by="operator-1",
                approved_at=timestamp(3),
                correlation_id="corr-repair-1",
                schema_version=1,
                detail=redacted(reason="Reviewed"),
            )


def test_repair_creation_rejects_a_nonterminal_run_parent(database: SQLiteDatabase) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        session.execute(
            sql_update(runs)
            .where(runs.c.run_id == RUN_ID.value)
            .values(
                state=RunState.QUEUED.value,
                execution_evidence_fingerprint=None,
                execution_evidence_fingerprint_version=None,
            )
        )
        with pytest.raises(RepairStateConflictError, match="has not completed"):
            create_plan(SqlAlchemyRepairRepository(session))


@pytest.mark.parametrize("operation", ["approve", "begin"])
def test_freshness_rejects_run_and_summary_fingerprint_divergence(
    database: SQLiteDatabase, operation: str
) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        create_plan(repository)
        if operation == "begin":
            approve(repository)
        session.execute(
            sql_update(runs)
            .where(runs.c.run_id == RUN_ID.value)
            .values(execution_evidence_fingerprint="9" * 64)
        )
        if operation == "approve":
            with pytest.raises(RepairCorruptionError, match="fingerprints diverge"):
                approve(repository)
        else:
            with pytest.raises(RepairCorruptionError, match="fingerprints diverge"):
                repository.begin_application(
                    PLAN_ID,
                    expected_row_version=2,
                    current_reconciliation_fingerprint=RECONCILIATION,
                    applying_at=timestamp(4),
                )


def test_repair_get_and_bounded_list_paths(database: SQLiteDatabase) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        first = create_plan(repository)
        second = repository.create_plan(
            run_id=RUN_ID,
            plan=second_plan(),
            action_keys=RepairActionKeyMap.from_mapping(
                {RepairActionId("rac_second-repair"): "repair-second-lamp-v1"}
            ),
            created_at=timestamp(3),
        )
        update = repository.create_plan(
            run_id=RUN_ID,
            plan=update_plan(),
            action_keys=RepairActionKeyMap.from_mapping(
                {RepairActionId("rac_update-repair"): "repair-update-lamp-v1"}
            ),
            created_at=timestamp(4),
        )
        assert repository.get(PLAN_ID) == first
        assert repository.get(RepairPlanId("rpl_missing")) is None
        assert repository.get_action(ACTION_ID) == first.actions[0]
        assert repository.get_action(RepairActionId("rac_missing")) is None
        page = repository.list_for_run(RUN_ID, limit=1)
        assert page.items == (first.plan,)
        assert page.next_cursor == RepairPlanCursor(timestamp(2), PLAN_ID)
        second_page = repository.list_for_run(RUN_ID, limit=10, after=page.next_cursor)
        assert second_page.items == (second.plan, update.plan)
        assert second_page.next_cursor is None
        action_page = repository.list_actions(PLAN_ID, limit=1)
        assert action_page.items == first.actions
        assert action_page.next_cursor is None
        empty = repository.list_actions(
            PLAN_ID,
            limit=1,
            after=RepairActionCursor("ZZZZ", ACTION_ID),
        )
        assert empty.items == ()


@pytest.mark.parametrize(
    ("column", "value"),
    [("classification", "field_mismatch"), ("suggested_resolution", "update_target")],
)
def test_strict_repair_reads_revalidate_the_conflict_relationship(
    database: SQLiteDatabase, column: str, value: str
) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        create_plan(repository)
        session.execute(text('DROP TRIGGER "trg_reconciliation_conflicts_prohibit_update"'))
        session.execute(
            sql_update(reconciliation_conflicts)
            .where(reconciliation_conflicts.c.conflict_id == CONFLICT_ID.value)
            .values({column: value})
        )
        with pytest.raises(RepairCorruptionError, match="conflict relationship"):
            repository.get(PLAN_ID)


def test_rejection_and_conflicting_replays(database: SQLiteDatabase) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        create_plan(repository)
        with pytest.raises(RepairStaleRowVersionError):
            repository.reject(PLAN_ID, expected_row_version=2, rejected_at=timestamp(3))
        rejected = repository.reject(PLAN_ID, expected_row_version=1, rejected_at=timestamp(3))
        assert rejected.plan.status is RepairPlanStatus.REJECTED
        assert (
            repository.reject(PLAN_ID, expected_row_version=1, rejected_at=timestamp(3)) == rejected
        )
        with pytest.raises(RepairStateConflictError):
            repository.reject(PLAN_ID, expected_row_version=1, rejected_at=timestamp(4))
        with pytest.raises(RepairStateConflictError, match="lifecycle"):
            repository.approve(
                PLAN_ID,
                expected_row_version=2,
                current_reconciliation_fingerprint=RECONCILIATION,
                approved_by="operator-1",
                approved_at=timestamp(4),
                correlation_id="corr-repair-1",
                schema_version=1,
                detail=redacted(reason="Too late"),
            )
        with pytest.raises(RepairStateConflictError):
            repository.begin_application(
                PLAN_ID,
                expected_row_version=2,
                current_reconciliation_fingerprint=RECONCILIATION,
                applying_at=timestamp(4),
            )


def test_plan_creation_classifies_invalid_and_duplicate_requests(
    database: SQLiteDatabase,
) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        with pytest.raises(Exception, match="exactly match"):
            repository.create_plan(
                run_id=RUN_ID,
                plan=plan(),
                action_keys=RepairActionKeyMap(()),
                created_at=timestamp(2),
            )
        with pytest.raises(Exception, match="not monotonic"):
            repository.create_plan(
                run_id=RUN_ID,
                plan=plan(),
                action_keys=RepairActionKeyMap.from_mapping({ACTION_ID: "repair-key"}),
                created_at=timestamp(0),
            )
        create_plan(repository)
        with pytest.raises(RepairPlanContentConflictError):
            repository.create_plan(
                run_id=RUN_ID,
                plan=plan(),
                action_keys=RepairActionKeyMap.from_mapping({ACTION_ID: "repair-harbor-lamp-v1"}),
                created_at=timestamp(3),
            )
        alternate_identity = RepairPlan(
            RepairPlanId("rpl_same-content"), RECONCILIATION, plan().actions
        )
        with pytest.raises(RepairPlanContentConflictError):
            repository.create_plan(
                run_id=RUN_ID,
                plan=alternate_identity,
                action_keys=RepairActionKeyMap.from_mapping({ACTION_ID: "repair-harbor-lamp-v1"}),
                created_at=timestamp(2),
            )


def test_approval_and_application_conflict_paths(database: SQLiteDatabase) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        create_plan(repository)
        with pytest.raises(RepairStaleRowVersionError):
            repository.approve(
                PLAN_ID,
                expected_row_version=2,
                current_reconciliation_fingerprint=RECONCILIATION,
                approved_by="operator-1",
                approved_at=timestamp(3),
                correlation_id="corr-repair-1",
                schema_version=1,
                detail=redacted(reason="Reviewed synthetic evidence"),
            )
        approve(repository)
        with pytest.raises(RepairApprovalConflictError):
            repository.approve(
                PLAN_ID,
                expected_row_version=1,
                current_reconciliation_fingerprint=StateFingerprint("9" * 64),
                approved_by="operator-2",
                approved_at=timestamp(3),
                correlation_id="corr-repair-1",
                schema_version=1,
                detail=redacted(reason="Different"),
            )
        with pytest.raises(Exception, match="not monotonic"):
            repository.begin_application(
                PLAN_ID,
                expected_row_version=2,
                current_reconciliation_fingerprint=RECONCILIATION,
                applying_at=timestamp(2),
            )
        begun = repository.begin_application(
            PLAN_ID,
            expected_row_version=2,
            current_reconciliation_fingerprint=RECONCILIATION,
            applying_at=timestamp(4),
        )
        assert begun.reservation is not None
        with pytest.raises(RepairStateConflictError):
            repository.complete_application(begun.reservation, applied_at=timestamp(5))
        with pytest.raises(RepairRecordNotFoundError):
            repository.record_action_applied(
                begun.reservation,
                RepairActionId("rac_missing"),
                result=RepairApplicationResult(1, redacted(effect="created")),
                target_version=1,
                applied_at=timestamp(5),
            )
        applied = repository.record_action_applied(
            begun.reservation,
            ACTION_ID,
            result=RepairApplicationResult(1, redacted(effect="created")),
            target_version=1,
            applied_at=timestamp(5),
        )
        with pytest.raises(RepairApplicationConflictError):
            repository.record_action_applied(
                begun.reservation,
                ACTION_ID,
                result=RepairApplicationResult(1, redacted(effect="different")),
                target_version=1,
                applied_at=timestamp(5),
            )
        with pytest.raises(Exception, match="not monotonic"):
            repository.complete_application(applied.reservation, applied_at=timestamp(4))


def audit(second: int, operation: str = "repair_plan_approved") -> PendingAuditEntry:
    return PendingAuditEntry(
        actor="operator-1",
        operation=operation,
        object_kind="repair_plan",
        object_id=PLAN_ID.value,
        correlation_id="corr-repair-1",
        occurred_at=timestamp(second),
        detail_schema_version=1,
        detail=redacted(synthetic=True),
    )


def test_audit_autoincrement_pagination_gaps_and_nonmonotonic_time(
    database: SQLiteDatabase,
) -> None:
    with database.transaction() as session:
        repository = SqlAlchemyAuditRepository(session)
        first = repository.append(audit(8))
        second = repository.append(audit(7, "repair_plan_applied"))
        assert first.sequence == AuditSequence(1)
        assert second.sequence == AuditSequence(2)
        assert repository.get(first.sequence) == first
        page = repository.list_after(after=None, limit=1)
        assert page.items == (first,)
        assert page.next_cursor == first.sequence
        assert repository.list_after(after=page.next_cursor, limit=10).items == (second,)
        session.execute(
            insert(audit_entries).values(
                sequence_number=10,
                actor="operator-1",
                operation="repair_plan_recovered",
                object_kind="repair_plan",
                object_id=PLAN_ID.value,
                correlation_id="corr-repair-1",
                occurred_at=str(timestamp(6)),
                detail_schema_version=1,
                detail_json='{"synthetic":true}',
            )
        )
        eleventh = repository.append(audit(5, "repair_plan_verified"))
        assert eleventh.sequence == AuditSequence(11)


def test_rolled_back_audit_sequence_can_be_reused(database: SQLiteDatabase) -> None:
    def append_then_fail() -> None:
        with database.transaction() as session:
            record_value = SqlAlchemyAuditRepository(session).append(audit(1))
            assert record_value.sequence == AuditSequence(1)
            raise RuntimeError("rollback")

    with pytest.raises(RuntimeError):
        append_then_fail()
    with database.transaction() as session:
        assert SqlAlchemyAuditRepository(session).append(audit(2)).sequence == AuditSequence(1)


def test_repositories_require_caller_owned_transaction(database: SQLiteDatabase) -> None:
    session = Session(database.engine)
    try:
        with pytest.raises(Exception, match="caller-owned transaction"):
            SqlAlchemyRepairRepository(session).get(PLAN_ID)
        with pytest.raises(Exception, match="caller-owned transaction"):
            SqlAlchemyAuditRepository(session).list_after(after=None, limit=10)
    finally:
        session.close()


def test_reopen_preserves_repair_and_audit(database: SQLiteDatabase) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        create_plan(SqlAlchemyRepairRepository(session))
        SqlAlchemyAuditRepository(session).append(audit(2))
    database.engine.dispose()
    with database.transaction() as session:
        assert SqlAlchemyRepairRepository(session).get(PLAN_ID) is not None
        assert len(SqlAlchemyAuditRepository(session).list_after(after=None, limit=10).items) == 1
        assert session.execute(select(audit_entries.c.sequence_number)).scalar_one() == 1


def test_multi_action_pagination_and_typed_effect_key_collision(
    database: SQLiteDatabase,
) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        aggregate = repository.create_plan(
            run_id=RUN_ID,
            plan=multi_plan(),
            action_keys=RepairActionKeyMap.from_mapping(
                {
                    ACTION_ID: "repair-harbor-lamp-v1",
                    RepairActionId("rac_second-repair"): "repair-second-lamp-v1",
                }
            ),
            created_at=timestamp(2),
        )
        first = repository.list_actions(aggregate.plan.repair_plan_id, limit=1)
        assert len(first.items) == 1
        assert first.next_cursor is not None
        second = repository.list_actions(
            aggregate.plan.repair_plan_id, limit=1, after=first.next_cursor
        )
        assert len(second.items) == 1
        assert second.next_cursor is None

    with pytest.raises(RepairDuplicateError), database.transaction() as session:
        SqlAlchemyRepairRepository(session).create_plan(
            run_id=RUN_ID,
            plan=update_plan(),
            action_keys=RepairActionKeyMap.from_mapping(
                {RepairActionId("rac_update-repair"): "repair-harbor-lamp-v1"}
            ),
            created_at=timestamp(3),
        )
    with database.transaction() as session:
        assert SqlAlchemyRepairRepository(session).get(RepairPlanId("rpl_update-repair")) is None


def test_missing_and_mismatched_reconciliation_parents_are_typed(
    database: SQLiteDatabase,
) -> None:
    with database.transaction() as session, pytest.raises(RepairRecordNotFoundError):
        create_plan(SqlAlchemyRepairRepository(session))
    seed_reconciliation(database)
    missing_conflict_plan = RepairPlan(
        RepairPlanId("rpl_missing-conflict"),
        RECONCILIATION,
        (
            RepairAction(
                RepairActionId("rac_missing-conflict"),
                ConflictId("cnf_not-present"),
                RECONCILIATION,
                RepairActionKind.CREATE_TARGET,
                record(sku="MISSING-LAMP"),
            ),
        ),
    )
    mismatched_conflict_plan = RepairPlan(
        RepairPlanId("rpl_wrong-conflict"),
        RECONCILIATION,
        (
            RepairAction(
                RepairActionId("rac_wrong-conflict"),
                ConflictId("cnf_update-repair"),
                RECONCILIATION,
                RepairActionKind.CREATE_TARGET,
                record(),
            ),
        ),
    )
    stale_fingerprint = StateFingerprint("8" * 64)
    stale_plan = RepairPlan(
        RepairPlanId("rpl_stale-repair"),
        stale_fingerprint,
        (
            RepairAction(
                RepairActionId("rac_stale-repair"),
                CONFLICT_ID,
                stale_fingerprint,
                RepairActionKind.CREATE_TARGET,
                record(),
            ),
        ),
    )
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        with pytest.raises(RepairRecordNotFoundError):
            repository.create_plan(
                run_id=RUN_ID,
                plan=missing_conflict_plan,
                action_keys=RepairActionKeyMap.from_mapping(
                    {RepairActionId("rac_missing-conflict"): "missing-conflict-key"}
                ),
                created_at=timestamp(2),
            )
        with pytest.raises(RepairStateConflictError):
            repository.create_plan(
                run_id=RUN_ID,
                plan=mismatched_conflict_plan,
                action_keys=RepairActionKeyMap.from_mapping(
                    {RepairActionId("rac_wrong-conflict"): "wrong-conflict-key"}
                ),
                created_at=timestamp(2),
            )
        with pytest.raises(RepairStateConflictError):
            repository.create_plan(
                run_id=RUN_ID,
                plan=stale_plan,
                action_keys=RepairActionKeyMap.from_mapping(
                    {RepairActionId("rac_stale-repair"): "stale-repair-key"}
                ),
                created_at=timestamp(2),
            )


def test_reservation_mismatch_stale_and_terminal_conflicts(database: SQLiteDatabase) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        create_plan(repository)
        approve(repository)
        begun = repository.begin_application(
            PLAN_ID,
            expected_row_version=2,
            current_reconciliation_fingerprint=RECONCILIATION,
            applying_at=timestamp(4),
        )
        assert begun.reservation is not None
        forged = replace(begun.reservation, run_id=RunId("run_other"))
        with pytest.raises(RepairApplicationConflictError, match="does not match"):
            repository.record_action_applied(
                forged,
                ACTION_ID,
                result=RepairApplicationResult(1, redacted(effect="created")),
                target_version=1,
                applied_at=timestamp(5),
            )
        applied = repository.record_action_applied(
            begun.reservation,
            ACTION_ID,
            result=RepairApplicationResult(1, redacted(effect="created")),
            target_version=1,
            applied_at=timestamp(5),
        )
        with pytest.raises(RepairApplicationConflictError, match="stale"):
            repository.record_action_failed(
                begun.reservation,
                ACTION_ID,
                result=RepairApplicationResult(1, redacted(outcome="failed")),
                failed_at=timestamp(6),
                plan_failure=redacted(reason="late_failure"),
            )
        completed = repository.complete_application(applied.reservation, applied_at=timestamp(6))
        with pytest.raises(RepairApplicationConflictError, match="differs"):
            repository.complete_application(applied.reservation, applied_at=timestamp(7))
        terminal_claim = replace(applied.reservation, row_version=completed.plan.row_version)
        with pytest.raises(RepairApplicationConflictError, match="not applying"):
            repository.record_action_failed(
                terminal_claim,
                ACTION_ID,
                result=RepairApplicationResult(1, redacted(outcome="failed")),
                failed_at=timestamp(7),
                plan_failure=redacted(reason="late_failure"),
            )


def test_every_application_path_validates_the_complete_reservation(
    database: SQLiteDatabase,
) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        create_plan(repository)
        approve(repository)
        begun = repository.begin_application(
            PLAN_ID,
            expected_row_version=2,
            current_reconciliation_fingerprint=RECONCILIATION,
            applying_at=timestamp(4),
        )
        assert begun.reservation is not None
        forged = replace(
            begun.reservation,
            content_fingerprint=StateFingerprint("9" * 64),
        )
        with pytest.raises(RepairApplicationConflictError, match="does not match"):
            repository.record_action_failed(
                forged,
                ACTION_ID,
                result=RepairApplicationResult(1, redacted(outcome="failed")),
                failed_at=timestamp(5),
                plan_failure=redacted(reason="target_conflict"),
            )
        with pytest.raises(RepairApplicationConflictError, match="does not match"):
            repository.complete_application(forged, applied_at=timestamp(5))
        malformed = replace(begun.reservation, run_id=cast(RunId, "run_invalid"))
        with pytest.raises(RepairInvalidRequestError, match="reservation run identifier"):
            repository.record_action_applied(
                malformed,
                ACTION_ID,
                result=RepairApplicationResult(1, redacted(outcome="applied")),
                target_version=1,
                applied_at=timestamp(5),
            )


def test_nested_cursor_values_and_row_version_capacity_fail_before_sql(
    database: SQLiteDatabase,
) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        create_plan(repository)
        with pytest.raises(RepairInvalidRequestError, match="cursor creation time"):
            repository.list_for_run(
                RUN_ID,
                limit=10,
                after=RepairPlanCursor(cast(UtcTimestamp, "invalid"), PLAN_ID),
            )
        for invalid_key in ("", "lowercase"):
            with pytest.raises(RepairInvalidRequestError, match="canonical key"):
                repository.list_actions(
                    PLAN_ID,
                    limit=10,
                    after=RepairActionCursor(invalid_key, ACTION_ID),
                )
        with pytest.raises(RepairStateConflictError, match="supported maximum"):
            repository._advance_plan(
                PLAN_ID,
                MAX_PERSISTED_INTEGER,
                RepairPlanStatus.PROPOSED,
                status="approved",
            )


def test_divergent_failure_retry_and_missing_plan_list(database: SQLiteDatabase) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        assert repository.list_for_run(RunId("run_missing"), limit=10).items == ()
        with pytest.raises(RepairRecordNotFoundError):
            repository.list_actions(RepairPlanId("rpl_missing"), limit=10)
        create_plan(repository)
        approve(repository)
        begun = repository.begin_application(
            PLAN_ID,
            expected_row_version=2,
            current_reconciliation_fingerprint=RECONCILIATION,
            applying_at=timestamp(4),
        )
        assert begun.reservation is not None
        repository.record_action_failed(
            begun.reservation,
            ACTION_ID,
            result=RepairApplicationResult(1, redacted(outcome="failed")),
            failed_at=timestamp(5),
            plan_failure=redacted(reason="target_conflict"),
        )
        with pytest.raises(RepairApplicationConflictError, match="differs"):
            repository.record_action_failed(
                begun.reservation,
                ACTION_ID,
                result=RepairApplicationResult(1, redacted(outcome="different")),
                failed_at=timestamp(5),
                plan_failure=redacted(reason="target_conflict"),
            )
        with pytest.raises(RepairApplicationConflictError, match="differs"):
            repository.record_action_failed(
                begun.reservation,
                ACTION_ID,
                result=RepairApplicationResult(1, redacted(outcome="failed")),
                failed_at=timestamp(5),
                plan_failure=redacted(reason="different_failure"),
            )


@pytest.mark.parametrize(
    ("stored", "expected"),
    [
        ("bad", AuditCorruptionError),
        (2_147_483_647, AuditSequenceConflictError),
    ],
)
def test_audit_sequence_preflight_rejects_corruption_and_exhaustion(
    database: SQLiteDatabase, stored: object, expected: type[Exception]
) -> None:
    with database.transaction() as session:
        SqlAlchemyAuditRepository(session).append(audit(1))
    with database.transaction() as session:
        session.execute(
            text("UPDATE sqlite_sequence SET seq = :seq WHERE name = 'audit_entries'"),
            {"seq": stored},
        )
    with pytest.raises(expected), database.transaction() as session:
        SqlAlchemyAuditRepository(session).append(audit(2))


def test_audit_capacity_race_has_one_maximum_winner_and_one_typed_loser(
    database: SQLiteDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    with database.transaction() as session:
        SqlAlchemyAuditRepository(session).append(audit(1))
        session.execute(
            text("UPDATE sqlite_sequence SET seq = :seq WHERE name = 'audit_entries'"),
            {"seq": MAX_PERSISTED_INTEGER - 1},
        )
    barrier = Barrier(2)
    lock = Lock()
    checked: set[int] = set()
    original = SqlAlchemyAuditRepository._preflight_sequence

    def synchronize_first_preflight(repository: SqlAlchemyAuditRepository) -> None:
        original(repository)
        with lock:
            first = id(repository) not in checked
            checked.add(id(repository))
        if first:
            barrier.wait(timeout=10)

    monkeypatch.setattr(
        SqlAlchemyAuditRepository,
        "_preflight_sequence",
        synchronize_first_preflight,
    )

    def append_at_frontier(second: int) -> AuditSequence | type[Exception]:
        try:
            with database.transaction() as session:
                return SqlAlchemyAuditRepository(session).append(audit(second)).sequence
        except Exception as error:
            return type(error)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = {
            future.result(timeout=15)
            for future in (
                executor.submit(append_at_frontier, 2),
                executor.submit(append_at_frontier, 3),
            )
        }
    assert results == {
        AuditSequence(MAX_PERSISTED_INTEGER),
        AuditSequenceConflictError,
    }


def test_noncapacity_audit_integrity_failure_remains_a_storage_failure(
    database: SQLiteDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    with database.transaction() as session:
        repository = SqlAlchemyAuditRepository(session)
        monkeypatch.setattr(repository, "_preflight_sequence", lambda: None)

        def fail_execute(_session: object, _statement: object) -> NoReturn:
            raise IntegrityError("audit insert", {}, ValueError("constraint"))

        monkeypatch.setattr(session, "execute", MethodType(fail_execute, session))
        with pytest.raises(AuditStorageError):
            repository.append(audit(1))


def test_defensive_repair_cas_classification_paths(  # pyright: ignore[reportPrivateUsage]
    database: SQLiteDatabase,
) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        aggregate = create_plan(repository)
        with pytest.raises(RepairRecordNotFoundError):
            repository._raise_plan_cas(RepairPlanId("rpl_missing"), 1, RepairPlanStatus.PROPOSED)
        with pytest.raises(RepairStaleRowVersionError):
            repository._raise_plan_cas(PLAN_ID, 2, RepairPlanStatus.PROPOSED)
        with pytest.raises(RepairStateConflictError, match="lifecycle"):
            repository._raise_plan_cas(PLAN_ID, 1, RepairPlanStatus.APPROVED)
        with pytest.raises(RepairStateConflictError, match="rejected"):
            repository._raise_plan_cas(PLAN_ID, 1, RepairPlanStatus.PROPOSED)
        with pytest.raises(RepairStaleRowVersionError):
            repository._advance_plan(PLAN_ID, 2, RepairPlanStatus.PROPOSED, status="approved")
        with pytest.raises(RepairCorruptionError, match="summary is missing"):
            repository._require_fresh(
                replace(aggregate.plan, run_id=RunId("run_missing")), RECONCILIATION
            )
        with pytest.raises(RepairCorruptionError, match="relationship"):
            repository._require_fresh(
                replace(
                    aggregate.plan,
                    reconciliation_fingerprint=StateFingerprint("9" * 64),
                ),
                RECONCILIATION,
            )
        for row in (
            {"run_state": 1, "execution_evidence_fingerprint": RECONCILIATION.value},
            {"run_state": "unknown", "execution_evidence_fingerprint": RECONCILIATION.value},
            {"run_state": RunState.SUCCEEDED.value, "execution_evidence_fingerprint": None},
        ):
            with pytest.raises(RepairCorruptionError):
                repository._validate_run_fingerprint(row, RECONCILIATION)
        repository._validate_run_fingerprint(
            {
                "run_state": RunState.PARTIALLY_SUCCEEDED.value,
                "execution_evidence_fingerprint": RECONCILIATION.value,
            },
            RECONCILIATION,
        )
        with pytest.raises(RepairDuplicateError):
            repository._classify_create_replay(
                RUN_ID,
                second_plan(),
                {RepairActionId("rac_second-repair"): "unused-key"},
                timestamp(2),
                (RepairActionEffect.from_action(second_plan().actions[0]),),
                StateFingerprint("f" * 64),
            )
        with pytest.raises(RepairCorruptionError, match="reservation state"):
            repair_module._reservation(aggregate.plan)
        assert not repair_module._matches_applied(aggregate.actions[0], "{}", 1, timestamp(3))
        assert not repair_module._matches_failed(aggregate.actions[0], "{}", timestamp(3))


@pytest.mark.parametrize("failure", [False, True])
def test_action_zero_row_after_parent_fence_is_typed_and_rollback_clean(
    database: SQLiteDatabase, monkeypatch: pytest.MonkeyPatch, failure: bool
) -> None:
    seed_reconciliation(database)

    def steal_then_apply() -> None:
        with database.transaction() as session:
            repository = SqlAlchemyRepairRepository(session)
            create_plan(repository)
            approve(repository)
            begun = repository.begin_application(
                PLAN_ID,
                expected_row_version=2,
                current_reconciliation_fingerprint=RECONCILIATION,
                applying_at=timestamp(4),
            )
            assert begun.reservation is not None

            def steal_action(*_args: object, **_kwargs: object) -> None:
                if failure:
                    values: dict[str, object] = {
                        "application_status": "failed",
                        "application_result_json": encode_application_result(
                            RepairApplicationResult(1, redacted(outcome="stolen"))
                        ).text,
                        "failed_at": str(timestamp(5)),
                    }
                else:
                    values = {
                        "application_status": "applied",
                        "application_result_json": encode_application_result(
                            RepairApplicationResult(1, redacted(outcome="stolen"))
                        ).text,
                        "target_version": 1,
                        "applied_at": str(timestamp(5)),
                    }
                session.execute(
                    sql_update(repair_actions)
                    .where(repair_actions.c.repair_action_id == ACTION_ID.value)
                    .values(**values)
                )

            monkeypatch.setattr(repository, "_advance_plan", steal_action)
            if failure:
                repository.record_action_failed(
                    begun.reservation,
                    ACTION_ID,
                    result=RepairApplicationResult(1, redacted(outcome="failed")),
                    failed_at=timestamp(5),
                    plan_failure=redacted(reason="target_conflict"),
                )
            else:
                repository.record_action_applied(
                    begun.reservation,
                    ACTION_ID,
                    result=RepairApplicationResult(1, redacted(outcome="applied")),
                    target_version=1,
                    applied_at=timestamp(5),
                )

    with pytest.raises(RepairApplicationConflictError, match="lost its race"):
        steal_then_apply()
    with database.transaction() as session:
        assert SqlAlchemyRepairRepository(session).get(PLAN_ID) is None


class _FatalWrite(BaseException):
    """A non-Exception failure used to prove transaction cleanup."""


@pytest.mark.parametrize("failure", [RuntimeError("write failed"), _FatalWrite()])
def test_internal_plan_and_approval_failpoints_roll_back_all_rows(
    database: SQLiteDatabase,
    monkeypatch: pytest.MonkeyPatch,
    failure: BaseException,
) -> None:
    seed_reconciliation(database)

    def fail_actions(*_args: object, **_kwargs: object) -> None:
        raise failure

    def create_with_failure() -> None:
        with database.transaction() as session:
            repository = SqlAlchemyRepairRepository(session)
            monkeypatch.setattr(repository, "_insert_actions", fail_actions)
            create_plan(repository)

    with pytest.raises(type(failure)):
        create_with_failure()
    monkeypatch.undo()
    with database.transaction() as session:
        assert SqlAlchemyRepairRepository(session).get(PLAN_ID) is None

    with database.transaction() as session:
        create_plan(SqlAlchemyRepairRepository(session))

    def fail_approval(*_args: object, **_kwargs: object) -> None:
        raise failure

    def approve_with_failure() -> None:
        with database.transaction() as session:
            repository = SqlAlchemyRepairRepository(session)
            monkeypatch.setattr(repository, "_insert_approval", fail_approval)
            approve(repository)

    with pytest.raises(type(failure)):
        approve_with_failure()
    with database.transaction() as session:
        stored = SqlAlchemyRepairRepository(session).get(PLAN_ID)
        assert stored is not None
        assert stored.plan.status is RepairPlanStatus.PROPOSED
        assert stored.plan.row_version == 1
        assert stored.approval is None


@pytest.mark.parametrize("failure", [RuntimeError("write failed"), _FatalWrite()])
def test_failure_after_parent_action_fence_rolls_back_parent_and_action(
    database: SQLiteDatabase,
    monkeypatch: pytest.MonkeyPatch,
    failure: BaseException,
) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        create_plan(repository)
        approve(repository)

    def apply_with_failure() -> None:
        with database.transaction() as session:
            repository = SqlAlchemyRepairRepository(session)
            begun = repository.begin_application(
                PLAN_ID,
                expected_row_version=2,
                current_reconciliation_fingerprint=RECONCILIATION,
                applying_at=timestamp(4),
            )
            assert begun.reservation is not None
            original = repository._advance_plan

            def advance_then_fail(*args: object, **kwargs: object) -> None:
                original(*args, **kwargs)  # type: ignore[arg-type]
                raise failure

            monkeypatch.setattr(repository, "_advance_plan", advance_then_fail)
            repository.record_action_applied(
                begun.reservation,
                ACTION_ID,
                result=RepairApplicationResult(1, redacted(outcome="applied")),
                target_version=1,
                applied_at=timestamp(5),
            )

    with pytest.raises(type(failure)):
        apply_with_failure()
    with database.transaction() as session:
        stored = SqlAlchemyRepairRepository(session).get(PLAN_ID)
        assert stored is not None
        assert stored.plan.status is RepairPlanStatus.APPROVED
        assert stored.plan.row_version == 2
        assert stored.actions[0].status is RepairActionStatus.PENDING


def test_concurrent_audit_append_allocates_two_unique_sequences(
    database: SQLiteDatabase,
) -> None:
    def append_entry(second: int) -> AuditSequence:
        with database.transaction() as session:
            return SqlAlchemyAuditRepository(session).append(audit(second)).sequence

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = (executor.submit(append_entry, 1), executor.submit(append_entry, 2))
        sequences = {future.result(timeout=10) for future in futures}
    assert sequences == {AuditSequence(1), AuditSequence(2)}
    with database.transaction() as session:
        connection = session.connection()
        assert connection.exec_driver_sql("PRAGMA quick_check").scalar_one() == "ok"
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []


def test_two_session_approval_and_application_have_one_capability_winner(
    database: SQLiteDatabase,
) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        create_plan(SqlAlchemyRepairRepository(session))

    def first_approval() -> RepairPlanStatus:
        with database.transaction() as session:
            return approve(SqlAlchemyRepairRepository(session)).plan.status

    def divergent_approval() -> type[Exception]:
        try:
            with database.transaction() as session:
                SqlAlchemyRepairRepository(session).approve(
                    PLAN_ID,
                    expected_row_version=1,
                    current_reconciliation_fingerprint=RECONCILIATION,
                    approved_by="operator-2",
                    approved_at=timestamp(3),
                    correlation_id="corr-repair-2",
                    schema_version=1,
                    detail=redacted(reason="Different approval"),
                )
        except Exception as error:
            return type(error)
        raise AssertionError("divergent approval unexpectedly succeeded")

    with ThreadPoolExecutor(max_workers=2) as executor:
        winner = executor.submit(first_approval)
        assert winner.result(timeout=10) is RepairPlanStatus.APPROVED
        loser = executor.submit(divergent_approval)
        assert loser.result(timeout=10) is RepairApprovalConflictError

    def begin_application() -> RepairApplicationBeginResult:
        with database.transaction() as session:
            return SqlAlchemyRepairRepository(session).begin_application(
                PLAN_ID,
                expected_row_version=2,
                current_reconciliation_fingerprint=RECONCILIATION,
                applying_at=timestamp(4),
            )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(begin_application).result(timeout=10)
        second = executor.submit(begin_application).result(timeout=10)
    assert first.reservation is not None
    assert second.reservation is None
    assert second.disposition is RepairApplicationBeginDisposition.IN_PROGRESS_REPLAY


def test_repair_plan_page_uses_constant_three_queries(database: SQLiteDatabase) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        repository = SqlAlchemyRepairRepository(session)
        create_plan(repository)
        repository.create_plan(
            run_id=RUN_ID,
            plan=second_plan(),
            action_keys=RepairActionKeyMap.from_mapping(
                {RepairActionId("rac_second-repair"): "repair-second-lamp-v1"}
            ),
            created_at=timestamp(3),
        )
    statements = 0

    def count_statement(
        _connection: Connection,
        _cursor: object,
        _statement: str,
        _parameters: object,
        _context: object,
        _many: bool,
    ) -> None:
        nonlocal statements
        statements += 1

    sqlalchemy_event.listen(database.engine, "before_cursor_execute", count_statement)
    try:
        with database.transaction() as session:
            page = SqlAlchemyRepairRepository(session).list_for_run(RUN_ID, limit=100)
            assert len(page.items) == 2
    finally:
        sqlalchemy_event.remove(database.engine, "before_cursor_execute", count_statement)
    assert statements == 3
