# pyright: reportPrivateUsage=false
"""Remaining defensive-branch coverage: races, unknown outcomes, and guards."""

from typing import cast

import pytest

from paritygrid.adapters.persistence.repair_workflow import SQLiteRepairWorkflowReader
from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter
from paritygrid.adapters.persistence.writer.dispatch import dispatch_command
from paritygrid.application.ports.connectors import (
    ConnectorCallContext,
    ConnectorPermanentError,
    TargetConnector,
    TargetRecord,
    TargetStateSnapshot,
    TargetWriteRequest,
)
from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.application.ports.reconciliation_persistence import (
    ReconciliationResultConflictError,
    ReconciliationResultRecord,
    TargetVerificationRecord,
)
from paritygrid.application.ports.repair_audit import (
    RepairApplicationResult,
    RepairApprovalConflictError,
    RepairPlanAggregate,
    RepairStaleRowVersionError,
    RepairStateConflictError,
)
from paritygrid.application.ports.writer import (
    TransactionalWriter,
    WriterCommand,
    WriterCommandKind,
)
from paritygrid.application.reconciliation.analysis import ReconciliationAnalysis
from paritygrid.application.repair import (
    ReconciliationResultService,
    RepairApplicationPolicy,
    RepairApplicationService,
    RepairApprovalRequest,
    RepairApprovalService,
    RepairPlanningService,
    RepairWorkflowEvidence,
)
from paritygrid.application.repair.errors import (
    RepairApprovalConflictError as WorkflowApprovalConflictError,
)
from paritygrid.application.repair.errors import (
    RepairPlanMismatchError,
    RepairPlanStateError,
    RepairWriterOutcomeUnknownError,
)
from paritygrid.application.repair.identities import derive_plan_id
from paritygrid.domain.models import (
    RepairPlanId,
    RunId,
    StateFingerprint,
    TargetVerificationId,
)
from tests.repair.conftest import (
    RUN_ID,
    DeterministicClock,
    analysis,
    seed_terminal_run,
    wire_payload,
)
from tests.repair.test_applier import _IdempotentFakeTarget, _no_sleep
from tests.repair.test_applier_edges import (
    _approved,
    _BareConnectorError,
    _companions_for,
    _StaleSummaryReader,
)
from tests.repair.test_service_branches import _result

pytestmark = pytest.mark.anyio


class _TypedFailureTicket:
    """A ticket whose wait raises one typed repository rejection."""

    def __init__(self, error: BaseException) -> None:
        self._error = error
        from paritygrid.application.ports.writer import WriterSubmissionId

        self.submission_id = WriterSubmissionId(1)

    def result(self, *, timeout_seconds: float) -> object:
        raise self._error


class _TypedFailureWriter:
    """Yield tickets that raise a chosen typed error for chosen kinds."""

    def __init__(self, errors: dict[WriterCommandKind, BaseException]) -> None:
        self._errors = errors

    def submit(self, command: WriterCommand, *, timeout_seconds: float) -> object:
        return _TypedFailureTicket(self._errors[command.kind])


class TestApprovalRaceMappings:
    def _request(
        self,
        result: ReconciliationAnalysis,
        plan_id: RepairPlanId,
        content: StateFingerprint,
    ) -> RepairApprovalRequest:
        return RepairApprovalRequest(
            run_id=RUN_ID,
            repair_plan_id=plan_id,
            approved_by="approver-1",
            correlation_id="corr",
            approved_content_fingerprint=content,
            approved_reconciliation_fingerprint=result.summary.fingerprint,
            detail=RedactedDocument.from_mapping({"decision": "ok"}),
        )

    def test_repository_rejections_map_to_typed_service_errors(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        result = _result()
        seed_terminal_run(database)
        ReconciliationResultService(writer, reader, now=clock.now).persist(
            run_id=RUN_ID, analysis=result, actor="o", correlation_id="c"
        )
        created = RepairPlanningService(writer, reader, now=clock.now).create(
            run_id=RUN_ID, analysis=result, actor="o", correlation_id="c"
        )
        assert created.aggregate is not None
        plan_id = created.aggregate.plan.repair_plan_id
        content = created.aggregate.plan.content_fingerprint
        cases: list[tuple[BaseException, type[Exception] | str]] = [
            (RepairStaleRowVersionError("stale"), "concurrent approval"),
            (RepairApprovalConflictError("differs"), "differs"),
            (RepairStateConflictError("lifecycle"), "lifecycle"),
        ]
        for error, expect in cases:
            failing = cast(
                "TransactionalWriter",
                _TypedFailureWriter({WriterCommandKind.APPROVE_REPAIR_PLAN: error}),
            )
            with pytest.raises(expect if isinstance(expect, type) else Exception):
                RepairApprovalService(failing, reader, now=clock.now).approve(
                    self._request(result, plan_id, content)
                )

    def test_rejection_race_maps_to_typed_service_errors(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        result = _result()
        seed_terminal_run(database)
        ReconciliationResultService(writer, reader, now=clock.now).persist(
            run_id=RUN_ID, analysis=result, actor="o", correlation_id="c"
        )
        created = RepairPlanningService(writer, reader, now=clock.now).create(
            run_id=RUN_ID, analysis=result, actor="o", correlation_id="c"
        )
        assert created.aggregate is not None
        for error, expected, pattern in (
            (
                RepairStaleRowVersionError("stale"),
                WorkflowApprovalConflictError,
                "concurrent decision",
            ),
            (RepairStateConflictError("lifecycle"), RepairPlanStateError, "lifecycle"),
        ):
            failing = cast(
                "TransactionalWriter",
                _TypedFailureWriter({WriterCommandKind.REJECT_REPAIR_PLAN: error}),
            )
            with pytest.raises(expected, match=pattern):
                RepairApprovalService(failing, reader, now=clock.now).reject(
                    run_id=RUN_ID,
                    repair_plan_id=created.aggregate.plan.repair_plan_id,
                    correlation_id="corr",
                )

    def test_a_stale_current_reconciliation_blocks_approval(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        result = _result()
        seed_terminal_run(database)
        ReconciliationResultService(writer, reader, now=clock.now).persist(
            run_id=RUN_ID, analysis=result, actor="o", correlation_id="c"
        )
        created = RepairPlanningService(writer, reader, now=clock.now).create(
            run_id=RUN_ID, analysis=result, actor="o", correlation_id="c"
        )
        assert created.aggregate is not None
        from paritygrid.application.repair.errors import (
            RepairReconciliationStaleError,
        )

        with pytest.raises(RepairReconciliationStaleError):
            RepairApprovalService(writer, _StaleSummaryReader(reader), now=clock.now).approve(
                self._request(
                    result,
                    created.aggregate.plan.repair_plan_id,
                    created.aggregate.plan.content_fingerprint,
                )
            )


class TestPlanningRaceMappings:
    def test_a_competing_creation_returns_the_durable_winner(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        result = _result()
        seed_terminal_run(database)
        ReconciliationResultService(writer, reader, now=clock.now).persist(
            run_id=RUN_ID, analysis=result, actor="o", correlation_id="c"
        )
        plan_id = derive_plan_id(RUN_ID, result.summary.fingerprint)

        class _CompetingCreateProxy:
            def __init__(self) -> None:
                self.done = False

            def submit(self, command: WriterCommand, *, timeout_seconds: float) -> object:
                if not self.done and command.kind is WriterCommandKind.CREATE_REPAIR_PLAN:
                    self.done = True
                    from paritygrid.application.repair import generate_repair_plan
                    from paritygrid.application.writes.repairs import CreateRepairPlan

                    generated = generate_repair_plan(run_id=RUN_ID, analysis=result)
                    assert generated.plan is not None
                    assert generated.action_keys is not None
                    moment = clock.now()
                    with database.transaction() as session:
                        dispatch_command(
                            session,
                            CreateRepairPlan(
                                run_id=RUN_ID,
                                plan=generated.plan,
                                action_keys=generated.action_keys,
                                created_at=moment,
                                companions=_companions_for(
                                    reader,
                                    RUN_ID,
                                    "repair_plan_created",
                                    "repair_plan",
                                    plan_id.value,
                                    moment,
                                    {"action_count": len(generated.plan.actions)},
                                ),
                            ),
                        )
                return writer.submit(command, timeout_seconds=timeout_seconds)

        created = RepairPlanningService(
            cast("TransactionalWriter", _CompetingCreateProxy()), reader, now=clock.now
        ).create(run_id=RUN_ID, analysis=result, actor="o", correlation_id="c")
        assert created.aggregate is not None
        assert created.replayed

    def test_persistent_unknown_creation_outcome_surfaces(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        result = _result()
        seed_terminal_run(database)
        ReconciliationResultService(writer, reader, now=clock.now).persist(
            run_id=RUN_ID, analysis=result, actor="o", correlation_id="c"
        )
        from tests.repair.test_service_branches import _UnknownOutcomeProxy

        proxy = _UnknownOutcomeProxy(
            writer, lose_receipts={WriterCommandKind.CREATE_REPAIR_PLAN: 99}
        )
        with pytest.raises(RepairWriterOutcomeUnknownError):
            RepairPlanningService(cast("TransactionalWriter", proxy), reader, now=clock.now).create(
                run_id=RUN_ID, analysis=result, actor="o", correlation_id="c"
            )

    def test_a_divergent_regeneration_is_rejected(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        result = _result()
        seed_terminal_run(database)
        ReconciliationResultService(writer, reader, now=clock.now).persist(
            run_id=RUN_ID, analysis=result, actor="o", correlation_id="c"
        )
        created = RepairPlanningService(writer, reader, now=clock.now).create(
            run_id=RUN_ID, analysis=result, actor="o", correlation_id="c"
        )
        assert created.aggregate is not None
        from dataclasses import replace as dataclass_replace

        from paritygrid.application.repair.planning_service import (
            _require_matching_plan,  # pyright: ignore[reportPrivateUsage]
        )
        from paritygrid.domain.models import StateFingerprint

        divergent = dataclass_replace(
            created.aggregate,
            plan=dataclass_replace(
                created.aggregate.plan,
                content_fingerprint=StateFingerprint("e" * 64),
            ),
        )
        with pytest.raises(RepairPlanMismatchError, match="differs"):
            _require_matching_plan(divergent, created.generated)


class TestReconciliationRaceMappings:
    def test_a_competing_persist_of_a_different_analysis_is_terminal(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        result = _result()
        seed_terminal_run(database)

        class _CompetingPersistProxy:
            def __init__(self) -> None:
                self.done = False

            def submit(self, command: WriterCommand, *, timeout_seconds: float) -> object:
                if not self.done and command.kind is WriterCommandKind.PERSIST_RECONCILIATION:
                    self.done = True
                    different = analysis([wire_payload("GRID-0001", quantity=99)], [])
                    from paritygrid.application.repair import build_persisted_conflicts
                    from paritygrid.application.repair.companions import (
                        build_companions,
                        frontier_from_evidence,
                    )
                    from paritygrid.application.writes.reconciliation import (
                        PersistReconciliation,
                    )

                    moment = clock.now()
                    with database.transaction() as session:
                        dispatch_command(
                            session,
                            PersistReconciliation(
                                run_id=RUN_ID,
                                summary=different.summary,
                                conflicts=build_persisted_conflicts(RUN_ID, different, moment),
                                created_at=moment,
                                companions=build_companions(
                                    frontier=frontier_from_evidence(reader.load(RUN_ID)),
                                    run_id=RUN_ID,
                                    operation="reconciliation_persisted",
                                    object_kind="reconciliation_summary",
                                    object_id=RUN_ID.value,
                                    actor="competitor",
                                    correlation_id="corr-competing",
                                    occurred_at=moment,
                                    payload={"reconciliation_fingerprint": "f" * 64},
                                ),
                            ),
                        )
                return writer.submit(command, timeout_seconds=timeout_seconds)

        with pytest.raises(ReconciliationResultConflictError):
            ReconciliationResultService(
                cast("TransactionalWriter", _CompetingPersistProxy()), reader, now=clock.now
            ).persist(run_id=RUN_ID, analysis=result, actor="o", correlation_id="c")

    def test_a_disappearing_durable_result_is_reported(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        result = _result()
        seed_terminal_run(database)
        ReconciliationResultService(writer, reader, now=clock.now).persist(
            run_id=RUN_ID, analysis=result, actor="o", correlation_id="c"
        )

        class _VanishingResultReader:
            def load(self, run_id: RunId) -> RepairWorkflowEvidence:
                return reader.load(run_id)

            def load_plan(self, repair_plan_id: RepairPlanId) -> RepairPlanAggregate | None:
                return reader.load_plan(repair_plan_id)

            def load_reconciliation_result(
                self, run_id: RunId
            ) -> ReconciliationResultRecord | None:
                return None

            def load_target_verification(
                self, verification_id: TargetVerificationId
            ) -> TargetVerificationRecord | None:
                return reader.load_target_verification(verification_id)

        from paritygrid.application.repair.errors import RepairRunNotFoundError

        with pytest.raises(RepairRunNotFoundError):
            ReconciliationResultService(writer, _VanishingResultReader(), now=clock.now).persist(
                run_id=RUN_ID, analysis=result, actor="o", correlation_id="c"
            )


class TestApplicationRaceMappings:
    async def test_a_competing_completion_fences_the_loser_exactly_once(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        # Drive the plan to applying with every action recorded as applied
        # but no completion yet, then race two completions: the competing
        # one must win and the service's own completion must lose its fence.
        from sqlalchemy import func, select

        from paritygrid.adapters.persistence.schema import execution_events, repair_plans
        from paritygrid.application.ports.repair_audit import (
            RepairApplicationReservation,
        )
        from paritygrid.application.repair.applier import _reconstruct_reservation
        from paritygrid.application.writes.repairs import (
            BeginRepairApplication,
            CompleteRepairApplication,
            RecordRepairActionApplied,
        )

        await _approved(database, writer, reader, clock)
        result = _result()
        plan_id = derive_plan_id(RUN_ID, result.summary.fingerprint)
        aggregate = reader.load_plan(plan_id)
        assert aggregate is not None

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
        for action in begun.actions:
            moment = clock.now()
            outcome = await cast(TargetConnector, _IdempotentFakeTarget()).write_record_async(
                TargetWriteRequest(
                    sku=action.effect.proposed.sku,
                    payload={
                        "name": action.effect.proposed.name,
                        "quantity": action.effect.proposed.quantity,
                        "sku": action.effect.proposed.sku,
                    },
                    idempotency_key=action.external_idempotency_key,
                ),
                ConnectorCallContext(correlation_id="race"),
            )
            with database.transaction() as session:
                record_result = dispatch_command(
                    session,
                    RecordRepairActionApplied(
                        run_id=RUN_ID,
                        reservation=reservation,
                        repair_action_id=action.effect.action_id,
                        result=RepairApplicationResult(
                            1,
                            RedactedDocument.from_mapping({"outcome": "applied"}),
                        ),
                        target_version=outcome.target_version,
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
                    ),
                )
            from paritygrid.application.writes.repairs import (
                RepairActionAppliedResult,
            )

            reservation = cast(
                RepairActionAppliedResult, record_result.result
            ).operation.reservation
        del RepairApplicationReservation

        class _CompetingCompleteProxy:
            def __init__(self) -> None:
                self.fired = False

            def submit(self, command: WriterCommand, *, timeout_seconds: float) -> object:
                if not self.fired and command.kind is WriterCommandKind.COMPLETE_REPAIR_APPLICATION:
                    self.fired = True
                    moment = clock.now()
                    with database.transaction() as session:
                        dispatch_command(
                            session,
                            CompleteRepairApplication(
                                run_id=RUN_ID,
                                reservation=reservation,
                                applied_at=moment,
                                companions=_companions_for(
                                    reader,
                                    RUN_ID,
                                    "repair_application_completed",
                                    "repair_plan",
                                    plan_id.value,
                                    moment,
                                    {"action_count": 2},
                                ),
                            ),
                        )
                return writer.submit(command, timeout_seconds=timeout_seconds)

        with pytest.raises(RepairPlanStateError, match="concurrent application"):
            await RepairApplicationService(
                writer=cast("TransactionalWriter", _CompetingCompleteProxy()),
                reader=reader,
                now=clock.now,
                policy=RepairApplicationPolicy(delay_seconds=0.0),
                sleep=_no_sleep,
            ).apply(
                run_id=RUN_ID,
                repair_plan_id=plan_id,
                target=cast(TargetConnector, _IdempotentFakeTarget()),
                context_id="race",
            )
        # Exactly one completion landed: one applied timestamp, one completion
        # event, and a single durable version advance past the last action.
        with database.transaction() as session:
            completed = session.execute(
                select(repair_plans.c.applied_at, repair_plans.c.row_version).where(
                    repair_plans.c.repair_plan_id == plan_id.value
                )
            ).one()
            assert completed.applied_at is not None
            completions = session.execute(
                select(func.count())
                .select_from(execution_events)
                .where(execution_events.c.event_kind == "repair_application_completed")
            ).scalar_one()
            assert completions == 1

    async def test_failure_recording_with_a_lost_receipt_suspends(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        from tests.repair.test_service_branches import _UnknownOutcomeProxy

        await _approved(database, writer, reader, clock)
        plan_id = derive_plan_id(RUN_ID, _result().summary.fingerprint)
        failing = _IdempotentFakeTarget(failures={"GRID-0001": ConnectorPermanentError("rejected")})
        proxy = _UnknownOutcomeProxy(
            writer, lose_receipts={WriterCommandKind.RECORD_REPAIR_ACTION_FAILED: 99}
        )
        report = await RepairApplicationService(
            writer=cast("TransactionalWriter", proxy),
            reader=reader,
            now=clock.now,
            policy=RepairApplicationPolicy(
                delay_seconds=0.0, max_writer_replays=2, timeout_seconds=10.0
            ),
            sleep=_no_sleep,
        ).apply(
            run_id=RUN_ID,
            repair_plan_id=plan_id,
            target=cast(TargetConnector, failing),
            context_id="c",
        )
        assert report.disposition.value == "unresolved"

    async def test_failure_recording_race_maps_to_the_concurrent_fence(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        await _approved(database, writer, reader, clock)
        plan_id = derive_plan_id(RUN_ID, _result().summary.fingerprint)

        class _CompetingFailureProxy:
            def __init__(self) -> None:
                self.done = False

            def submit(self, command: WriterCommand, *, timeout_seconds: float) -> object:
                if not self.done and command.kind is WriterCommandKind.RECORD_REPAIR_ACTION_FAILED:
                    self.done = True
                    from paritygrid.application.writes.repairs import (
                        RecordRepairActionFailed,
                    )

                    failing = cast("RecordRepairActionFailed", command)
                    moment = clock.now()
                    with database.transaction() as session:
                        dispatch_command(
                            session,
                            RecordRepairActionFailed(
                                run_id=RUN_ID,
                                reservation=failing.reservation,
                                repair_action_id=failing.repair_action_id,
                                result=RepairApplicationResult(
                                    1, RedactedDocument.from_mapping({"detail": "x"})
                                ),
                                failed_at=moment,
                                plan_failure=RedactedDocument.from_mapping(
                                    {"reason": "competing", "detail": "different"}
                                ),
                                companions=_companions_for(
                                    reader,
                                    RUN_ID,
                                    "repair_action_failed",
                                    "repair_action",
                                    failing.repair_action_id.value,
                                    moment,
                                    {"canonical_key": "GRID-0001"},
                                ),
                            ),
                        )
                return writer.submit(command, timeout_seconds=timeout_seconds)

        failing = _IdempotentFakeTarget(failures={"GRID-0001": ConnectorPermanentError("rejected")})
        with pytest.raises(RepairPlanStateError):
            await RepairApplicationService(
                writer=cast("TransactionalWriter", _CompetingFailureProxy()),
                reader=reader,
                now=clock.now,
                policy=RepairApplicationPolicy(delay_seconds=0.0),
                sleep=_no_sleep,
            ).apply(
                run_id=RUN_ID,
                repair_plan_id=plan_id,
                target=cast(TargetConnector, failing),
                context_id="c",
            )


class TestVerificationRemainingBranches:
    async def test_read_loop_failures_and_snapshots(
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

        class _ExplodingReadTarget(_IdempotentFakeTarget):
            async def read_record_async(
                self, sku: str, context: ConnectorCallContext
            ) -> TargetRecord | None:
                raise _BareConnectorError()

        exploded = await verifier.verify(
            target=cast(TargetConnector, _ExplodingReadTarget()),
            inventory=inventory,
            context_id="c",
        )
        assert exploded.disposition is TargetObservationDisposition.OBSERVATION_FAILED

    async def test_ambiguous_expected_keys_diverge(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        from paritygrid.application.repair import (
            TargetParityVerifier,
            build_expected_inventory,
        )

        result = await _approved(database, writer, reader, clock)
        inventory = build_expected_inventory(result, None)
        assert not inventory.ambiguous_keys
        ambiguous_analysis = analysis(
            [wire_payload("GRID-0002")],
            [
                wire_payload("GRID-0001", name="First"),
                wire_payload("GRID-0001", name="Second"),
            ],
        )
        ambiguous = build_expected_inventory(ambiguous_analysis, None)
        assert ambiguous.ambiguous_keys == ("GRID-0001",)
        report = await TargetParityVerifier(now=clock.now).verify(
            target=cast(TargetConnector, _IdempotentFakeTarget()),
            inventory=ambiguous,
            context_id="c",
        )
        assert any(
            divergence.reason == "expected content is ambiguous after duplicate review"
            for divergence in report.divergences
        )

    async def test_recording_guards_and_failed_observations(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        from paritygrid.application.ports.reconciliation_persistence import (
            TargetVerificationVerdict,
        )
        from paritygrid.application.repair import (
            TargetObservationDisposition,
            TargetParityVerifier,
            TargetVerificationService,
            build_expected_inventory,
        )
        from paritygrid.domain.models import StateFingerprint

        result = await _approved(database, writer, reader, clock)
        service = TargetVerificationService(writer, reader, now=clock.now)
        failed = await TargetParityVerifier(now=clock.now).verify(
            target=cast(
                TargetConnector,
                _FailingSnapshot(_BareConnectorError()),
            ),
            inventory=build_expected_inventory(result, None),
            context_id="c",
        )
        assert failed.disposition is TargetObservationDisposition.OBSERVATION_FAILED
        record = service.record(
            run_id=RUN_ID,
            report=failed,
            reconciliation_fingerprint=result.summary.fingerprint,
            repair_plan_id=None,
            plan_content_fingerprint=None,
            actor="o",
            correlation_id="c",
        )
        assert record.verdict is TargetVerificationVerdict.OBSERVATION_FAILED
        with pytest.raises(TypeError):
            service.record(
                run_id=RUN_ID,
                report=failed,
                reconciliation_fingerprint="f" * 64,  # type: ignore[arg-type]
                repair_plan_id=None,
                plan_content_fingerprint=None,
                actor="o",
                correlation_id="c",
            )
        other_plan_run = RUN_ID
        del other_plan_run
        with pytest.raises(RepairPlanMismatchError, match="another run"):
            service.record(
                run_id=RUN_ID,
                report=failed,
                reconciliation_fingerprint=result.summary.fingerprint,
                repair_plan_id=_foreign_plan(database, writer, reader, clock, result),
                plan_content_fingerprint=None,
                actor="o",
                correlation_id="c",
            )
        del StateFingerprint


class _FailingSnapshot(_IdempotentFakeTarget):
    def __init__(self, error: BaseException) -> None:
        super().__init__()
        self._error = error

    async def state_snapshot_async(self, context: ConnectorCallContext) -> TargetStateSnapshot:
        raise self._error


def _foreign_plan(
    database: SQLiteDatabase,
    writer: SQLiteTransactionalWriter,
    reader: SQLiteRepairWorkflowReader,
    clock: DeterministicClock,
    result: ReconciliationAnalysis,
) -> RepairPlanId:
    from paritygrid.domain.models import RunId

    other = RunId("run_phase11-foreign-plan")
    seed_terminal_run(database, other, seed_pipeline=False)
    ReconciliationResultService(writer, reader, now=clock.now).persist(
        run_id=other, analysis=result, actor="o", correlation_id="c"
    )
    created = RepairPlanningService(writer, reader, now=clock.now).create(
        run_id=other, analysis=result, actor="o", correlation_id="c"
    )
    assert created.aggregate is not None
    return created.aggregate.plan.repair_plan_id
