"""Atomic integration tests for repair writer dispatch."""

# pyright: reportPrivateUsage=false

from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import func, insert, select, update

from paritygrid.adapters.persistence import SQLiteDatabase, SQLiteDatabaseConfig
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyPipelineRepository,
    SqlAlchemyRepairRepository,
    SqlAlchemyRunRepository,
)
from paritygrid.adapters.persistence.schema import (
    audit_entries,
    execution_events,
    reconciliation_conflicts,
    reconciliation_summaries,
    repair_actions,
    repair_plans,
    run_event_counters,
    runs,
)
from paritygrid.adapters.persistence.writer import dispatch as dispatch_runtime
from paritygrid.adapters.persistence.writer.dispatch import dispatch_command, validate_command
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    ConsistencyRepositoryError,
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.repair_audit import (
    AuditCorruptionError,
    AuditSequenceConflictError,
    PendingAuditEntry,
    RepairActionKeyMap,
    RepairApplicationBeginDisposition,
    RepairApplicationReservation,
    RepairApplicationResult,
    RepairPlanStatus,
)
from paritygrid.application.ports.writer import EventAppendRequest, WriterInvalidRequestError
from paritygrid.application.writes.repairs import (
    ApproveRepairPlan,
    BeginRepairApplication,
    BeginRepairApplicationResult,
    CompleteRepairApplication,
    CreateRepairPlan,
    RecordRepairActionApplied,
    RecordRepairActionFailed,
    RejectRepairPlan,
    RepairActionAppliedResult,
    RepairCompanions,
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

PIPELINE_ID = PipelineId("pip_writerrepair")
RUN_ID = RunId("run_writerrepair")
PLAN_ID = RepairPlanId("rpl_writerrepair")
ACTION_ID = RepairActionId("rac_writerrepair")
CONFLICT_ID = ConflictId("cnf_writerrepair")
FINGERPRINT = StateFingerprint("6" * 64)
EVIDENCE = StateFingerprint("7" * 64)


class _CompanionFailure(BaseException):
    pass


@pytest.fixture
def database(tmp_path: Path) -> Iterator[SQLiteDatabase]:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "writer repair %.db"))
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


def repair_plan() -> RepairPlan:
    record = InventoryRecord.create(
        sku="WRITER-LAMP",
        name="Writer Lamp",
        quantity=4,
        unit_price=Money(Decimal("21.25"), CurrencyCode("USD"), 2),
        updated_at=timestamp(1),
        connector_id=ConnectorId("con_writersource"),
        source_record_key="writer-source-1",
        attributes={"finish": "Brass"},
    )
    return RepairPlan(
        PLAN_ID,
        FINGERPRINT,
        (
            RepairAction(
                ACTION_ID,
                CONFLICT_ID,
                FINGERPRINT,
                RepairActionKind.CREATE_TARGET,
                record,
            ),
        ),
    )


def seed_reconciliation(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PIPELINE_ID,
            display_name="Writer repair pipeline",
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
        runs_repo = SqlAlchemyRunRepository(session)
        runs_repo.create(
            run_id=RUN_ID,
            pipeline_id=PIPELINE_ID,
            pipeline_version=PipelineVersion(1),
            runner_kind="sequential",
            runner_configuration=ConfigurationDocument.from_mapping({}),
            scenario_seed=None,
            node_ids=(NodeId("nod_writerrepair"),),
            created_at=timestamp(0),
        )
        runs_repo.transition(
            RUN_ID,
            expected_row_version=1,
            target_state=RunState.RUNNING,
            transitioned_at=timestamp(0),
        )
        runs_repo.transition(
            RUN_ID,
            expected_row_version=2,
            target_state=RunState.SUCCEEDED,
            transitioned_at=timestamp(1),
            execution_evidence_fingerprint=EVIDENCE,
            execution_evidence_fingerprint_version=2,
        )
        session.execute(
            insert(reconciliation_summaries).values(
                run_id=str(RUN_ID),
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
                reconciliation_fingerprint=str(FINGERPRINT),
                analytical_query_version=1,
                created_at=str(timestamp(1)),
            )
        )
        session.execute(
            insert(reconciliation_conflicts).values(
                conflict_id=str(CONFLICT_ID),
                run_id=str(RUN_ID),
                canonical_key="WRITER-LAMP",
                classification="missing_from_target",
                source_references_json="[]",
                target_reference_json=None,
                field_differences_json="[]",
                suggested_resolution="create_target",
                created_at=str(timestamp(1)),
            )
        )


def companions(sequence: int, operation: str, second: int) -> RepairCompanions:
    object_kind = "repair_action" if "action" in operation else "repair_plan"
    object_id = ACTION_ID if object_kind == "repair_action" else PLAN_ID
    event = EventAppendRequest(
        EventSequence(sequence),
        sequence,
        PendingExecutionEvent(
            event_kind=operation,
            occurred_at=timestamp(second),
            subject_kind=EventSubjectKind.RUN,
            subject_id=RUN_ID,
            correlation_id="corr-writer-repair",
            payload_schema_version=1,
            payload=redacted(operation=operation),
        ),
    )
    audit = PendingAuditEntry(
        actor="operator-1",
        operation=operation,
        object_kind=object_kind,
        object_id=str(object_id),
        correlation_id="corr-writer-repair",
        occurred_at=timestamp(second),
        detail_schema_version=1,
        detail=redacted(operation=operation),
    )
    return RepairCompanions(audit, event, sequence + 2)


def create_command() -> CreateRepairPlan:
    return CreateRepairPlan(
        RUN_ID,
        repair_plan(),
        RepairActionKeyMap.from_mapping({ACTION_ID: "writer-lamp-v1"}),
        timestamp(2),
        companions(1, "repair_plan_created", 2),
    )


def approve_command() -> ApproveRepairPlan:
    return ApproveRepairPlan(
        RUN_ID,
        PLAN_ID,
        1,
        FINGERPRINT,
        "operator-1",
        timestamp(3),
        "corr-writer-repair",
        1,
        redacted(reason="reviewed"),
        companions(2, "repair_plan_approved", 3),
    )


def test_full_application_and_exact_replays_have_no_duplicate_companions(
    database: SQLiteDatabase,
) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        created = dispatch_command(session, create_command())
    assert created.mutated
    with database.transaction() as session:
        replayed_create = dispatch_command(session, create_command())
    assert not replayed_create.mutated

    with database.transaction() as session:
        approved = dispatch_command(session, approve_command())
    assert approved.mutated
    with database.transaction() as session:
        replayed_approval = dispatch_command(session, approve_command())
    assert not replayed_approval.mutated

    begin_command = BeginRepairApplication(
        RUN_ID,
        PLAN_ID,
        2,
        FINGERPRINT,
        timestamp(4),
        companions(3, "repair_application_started", 4),
    )
    with database.transaction() as session:
        begun = dispatch_command(session, begin_command)
    operation = cast(BeginRepairApplicationResult, begun.result).operation
    assert operation.disposition is RepairApplicationBeginDisposition.STARTED
    reservation = cast(RepairApplicationReservation, operation.reservation)
    with database.transaction() as session:
        replayed_begin = dispatch_command(session, begin_command)
    assert not replayed_begin.mutated

    applied_command = RecordRepairActionApplied(
        RUN_ID,
        reservation,
        ACTION_ID,
        RepairApplicationResult(1, redacted(outcome="created")),
        1,
        timestamp(5),
        companions(4, "repair_action_applied", 5),
    )
    with database.transaction() as session:
        applied = dispatch_command(session, applied_command)
    next_reservation = cast(RepairActionAppliedResult, applied.result).operation.reservation
    operation = cast(RepairActionAppliedResult, applied.result).operation
    with pytest.raises(WriterInvalidRequestError, match="another run"):
        dispatch_runtime._require_repair_action_parent(
            replace(operation, action=replace(operation.action, run_id=RunId("run_other"))),
            RUN_ID,
        )
    with database.transaction() as session:
        replayed_action = dispatch_command(session, applied_command)
    assert not replayed_action.mutated

    complete_command = CompleteRepairApplication(
        RUN_ID,
        next_reservation,
        timestamp(6),
        companions(5, "repair_application_completed", 6),
    )
    with database.transaction() as session:
        completed = dispatch_command(session, complete_command)
    assert completed.result.aggregate.plan.status is RepairPlanStatus.APPLIED  # type: ignore[attr-defined]
    with database.transaction() as session:
        replayed_complete = dispatch_command(session, complete_command)
        assert not replayed_complete.mutated
        assert session.scalar(select(func.count()).select_from(audit_entries)) == 5
        assert session.scalar(select(func.count()).select_from(execution_events)) == 5
        assert session.scalar(select(runs.c.row_version).where(runs.c.run_id == str(RUN_ID))) == 8
        counter = session.execute(select(run_event_counters)).mappings().one()
        assert (counter["next_sequence_number"], counter["row_version"]) == (6, 6)


def test_repair_commands_reject_cross_run_plan_hybrids_and_roll_back(
    database: SQLiteDatabase,
) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        dispatch_command(session, create_command())
    other_run = RunId("run_writerrepair-other")

    def for_other_run(value: RepairCompanions) -> RepairCompanions:
        event = replace(value.event.event, subject_id=other_run)
        return replace(value, event=replace(value.event, event=event))

    wrong_approval = replace(
        approve_command(),
        run_id=other_run,
        companions=for_other_run(companions(2, "repair_plan_approved", 3)),
    )
    wrong_rejection = RejectRepairPlan(
        other_run,
        PLAN_ID,
        1,
        timestamp(3),
        for_other_run(companions(2, "repair_plan_rejected", 3)),
    )
    for wrong in (wrong_approval, wrong_rejection):
        with (
            pytest.raises(WriterInvalidRequestError, match="another run"),
            database.transaction() as session,
        ):
            dispatch_command(session, wrong)
    with database.transaction() as session:
        plan = session.execute(select(repair_plans)).mappings().one()
        assert (plan["status"], plan["row_version"]) == (RepairPlanStatus.PROPOSED.value, 1)
        assert session.scalar(select(func.count()).select_from(audit_entries)) == 1
        assert session.scalar(select(func.count()).select_from(execution_events)) == 1
        dispatch_command(session, approve_command())

    wrong_begin = BeginRepairApplication(
        other_run,
        PLAN_ID,
        2,
        FINGERPRINT,
        timestamp(4),
        for_other_run(companions(3, "repair_application_started", 4)),
    )
    with (
        pytest.raises(WriterInvalidRequestError, match="another run"),
        database.transaction() as session,
    ):
        dispatch_command(session, wrong_begin)
    with database.transaction() as session:
        plan = session.execute(select(repair_plans)).mappings().one()
        assert (plan["status"], plan["row_version"]) == (RepairPlanStatus.APPROVED.value, 2)
        assert session.scalar(select(func.count()).select_from(audit_entries)) == 2


def test_repair_replay_requires_byte_exact_immediate_companions(
    database: SQLiteDatabase,
) -> None:
    seed_reconciliation(database)
    original = create_command()
    with database.transaction() as session:
        dispatch_command(session, original)

    altered_audit = replace(
        original,
        companions=replace(
            original.companions,
            audit=replace(original.companions.audit, detail=redacted(operation="altered")),
        ),
    )
    with (
        pytest.raises(AuditSequenceConflictError, match="differs"),
        database.transaction() as session,
    ):
        dispatch_command(session, altered_audit)

    altered_event = replace(
        original,
        companions=replace(
            original.companions,
            event=replace(
                original.companions.event,
                event=replace(
                    original.companions.event.event,
                    payload=redacted(operation="altered"),
                ),
            ),
        ),
    )
    with (
        pytest.raises(ConsistencyRepositoryError),
        database.transaction() as session,
    ):
        dispatch_command(session, altered_event)
    with database.transaction() as session:
        replay = dispatch_command(session, original)
        assert not replay.mutated
        assert session.scalar(select(func.count()).select_from(audit_entries)) == 1
        assert session.scalar(select(func.count()).select_from(execution_events)) == 1


def test_approval_replay_rejects_a_later_companion_frontier(
    database: SQLiteDatabase,
) -> None:
    seed_reconciliation(database)
    approval = approve_command()
    with database.transaction() as session:
        dispatch_command(session, create_command())
        dispatch_command(session, approval)
        immediate = dispatch_command(session, approval)
        assert not immediate.mutated
        dispatch_command(
            session,
            BeginRepairApplication(
                RUN_ID,
                PLAN_ID,
                2,
                FINGERPRINT,
                timestamp(4),
                companions(3, "repair_application_started", 4),
            ),
        )
    with (
        pytest.raises(ConsistencyRepositoryError),
        database.transaction() as session,
    ):
        dispatch_command(session, approval)


def test_repair_replay_requires_the_preexisting_audit_fact(
    database: SQLiteDatabase,
) -> None:
    seed_reconciliation(database)
    original = create_command()
    with database.transaction() as session:
        SqlAlchemyRepairRepository(session).create_plan(
            run_id=RUN_ID,
            plan=original.plan,
            action_keys=original.action_keys,
            created_at=original.created_at,
        )
    with (
        pytest.raises(AuditSequenceConflictError, match="does not exist"),
        database.transaction() as session,
    ):
        dispatch_command(session, original)
    with database.transaction() as session:
        assert session.scalar(select(func.count()).select_from(audit_entries)) == 0
        assert session.scalar(select(func.count()).select_from(execution_events)) == 0


def test_repair_replay_rejects_ambiguous_audit_and_later_run_revision(
    database: SQLiteDatabase,
) -> None:
    seed_reconciliation(database)
    original = create_command()
    with database.transaction() as session:
        dispatch_command(session, original)
        row = dict(session.execute(select(audit_entries)).mappings().one())
        row.pop("sequence_number")
        session.execute(insert(audit_entries).values(**row))
    with (
        pytest.raises(AuditCorruptionError, match="ambiguous"),
        database.transaction() as session,
    ):
        dispatch_command(session, original)


def test_repair_replay_rejects_run_revision_without_matching_companion(
    database: SQLiteDatabase,
) -> None:
    seed_reconciliation(database)
    original = create_command()
    with database.transaction() as session:
        dispatch_command(session, original)
        session.execute(
            update(runs)
            .where(runs.c.run_id == str(RUN_ID), runs.c.row_version == 4)
            .values(row_version=5)
        )
    with (
        pytest.raises(WriterInvalidRequestError, match="immediate run revision"),
        database.transaction() as session,
    ):
        dispatch_command(session, original)


def test_reject_and_failed_action_paths_are_atomic(database: SQLiteDatabase) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        dispatch_command(session, create_command())
        rejected = dispatch_command(
            session,
            RejectRepairPlan(
                RUN_ID,
                PLAN_ID,
                1,
                timestamp(3),
                companions(2, "repair_plan_rejected", 3),
            ),
        )
    assert rejected.result.aggregate.plan.status is RepairPlanStatus.REJECTED  # type: ignore[attr-defined]

    second_database_path = database.engine.url.database
    assert second_database_path is not None


def test_failed_action_terminalizes_plan_with_companions(database: SQLiteDatabase) -> None:
    seed_reconciliation(database)
    with database.transaction() as session:
        dispatch_command(session, create_command())
        dispatch_command(session, approve_command())
        begun = dispatch_command(
            session,
            BeginRepairApplication(
                RUN_ID,
                PLAN_ID,
                2,
                FINGERPRINT,
                timestamp(4),
                companions(3, "repair_application_started", 4),
            ),
        )
    reservation = cast(
        RepairApplicationReservation,
        cast(BeginRepairApplicationResult, begun.result).operation.reservation,
    )
    failed_command = RecordRepairActionFailed(
        RUN_ID,
        reservation,
        ACTION_ID,
        RepairApplicationResult(1, redacted(outcome="failed")),
        timestamp(5),
        redacted(reason="target_conflict"),
        companions(4, "repair_action_failed", 5),
    )
    with database.transaction() as session:
        failed = dispatch_command(session, failed_command)
    assert failed.result.aggregate.plan.status is RepairPlanStatus.FAILED  # type: ignore[attr-defined]
    with database.transaction() as session:
        replay = dispatch_command(session, failed_command)
        assert not replay.mutated
        assert session.scalar(select(func.count()).select_from(audit_entries)) == 4


def test_companion_base_exception_rolls_back_repair_mutation(
    database: SQLiteDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_reconciliation(database)

    def fail_companions(*_args: object, **_kwargs: object) -> object:
        raise _CompanionFailure

    monkeypatch.setattr(dispatch_runtime, "_repair_companions", fail_companions)
    with pytest.raises(_CompanionFailure), database.transaction() as session:
        dispatch_command(session, create_command())
    with database.transaction() as session:
        assert session.scalar(select(func.count()).select_from(repair_plans)) == 0
        assert session.scalar(select(func.count()).select_from(repair_actions)) == 0
        assert session.scalar(select(func.count()).select_from(audit_entries)) == 0
        assert session.scalar(select(func.count()).select_from(execution_events)) == 0
        assert session.scalar(select(runs.c.row_version).where(runs.c.run_id == str(RUN_ID))) == 3


def test_repair_validation_rejects_companion_mismatches_before_sql(
    database: SQLiteDatabase,
) -> None:
    seed_reconciliation(database)
    mismatched = CreateRepairPlan(
        RUN_ID,
        repair_plan(),
        RepairActionKeyMap.from_mapping({ACTION_ID: "writer-lamp-v1"}),
        timestamp(2),
        companions(1, "repair_plan_approved", 2),
    )
    with pytest.raises(WriterInvalidRequestError), database.transaction() as session:
        dispatch_command(session, mismatched)
    with database.transaction() as session:
        assert session.scalar(select(func.count()).select_from(repair_plans)) == 0


def test_repair_validation_error_matrix_is_fail_fast() -> None:
    valid = create_command()
    assert validate_command(valid) is valid
    with pytest.raises(WriterInvalidRequestError, match="companions"):
        validate_command(replace(valid, companions=cast(RepairCompanions, object())))

    valid_companions = valid.companions
    with pytest.raises(WriterInvalidRequestError, match="companion type"):
        dispatch_runtime._validate_repair_companions(
            cast(RepairCompanions, object()),
            RUN_ID,
            "repair_plan_created",
            "repair_plan_created",
            "repair_plan",
            PLAN_ID,
            timestamp(2),
        )
    invalid_audit = replace(valid_companions, audit=cast(PendingAuditEntry, object()))
    with pytest.raises(WriterInvalidRequestError, match="audit type"):
        dispatch_runtime._validate_repair_companions(
            invalid_audit,
            RUN_ID,
            "repair_plan_created",
            "repair_plan_created",
            "repair_plan",
            PLAN_ID,
            timestamp(2),
        )
    wrong_correlation = replace(
        valid_companions,
        audit=replace(valid_companions.audit, correlation_id="corr-other"),
    )
    with pytest.raises(WriterInvalidRequestError, match="correlation"):
        validate_command(replace(valid, companions=wrong_correlation))
    wrong_time_event = replace(
        valid_companions.event,
        event=replace(valid_companions.event.event, occurred_at=timestamp(3)),
    )
    with pytest.raises(WriterInvalidRequestError, match="time"):
        validate_command(
            replace(valid, companions=replace(valid_companions, event=wrong_time_event))
        )

    reservation = RepairApplicationReservation(
        PLAN_ID,
        RunId("run_otherrepair"),
        FINGERPRINT,
        FINGERPRINT,
        timestamp(4),
        3,
    )
    action_command = RecordRepairActionApplied(
        RUN_ID,
        reservation,
        ACTION_ID,
        RepairApplicationResult(1, redacted(outcome="created")),
        1,
        timestamp(5),
        companions(4, "repair_action_applied", 5),
    )
    with pytest.raises(WriterInvalidRequestError, match="another run"):
        validate_command(action_command)
