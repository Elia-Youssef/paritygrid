# pyright: reportPrivateUsage=false
"""Branch coverage for repair-service failure and recovery boundaries."""

from dataclasses import replace
from typing import cast

import pytest

from paritygrid.adapters.persistence.repair_workflow import SQLiteRepairWorkflowReader
from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
from paritygrid.adapters.persistence.writer.core import (
    SQLiteTransactionalWriter,
)
from paritygrid.application.ports.connectors import TargetConnector
from paritygrid.application.ports.execution import RunRecord
from paritygrid.application.ports.repair_audit import RepairPlanStatus
from paritygrid.application.ports.writer import (
    TransactionalWriter,
    WriterCommand,
    WriterCommandKind,
    WriterCommitOutcomeUnknownError,
    WriterSubmissionId,
    WriterTicket,
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
    build_expected_inventory,
)
from paritygrid.application.repair import planning as planning_module
from paritygrid.application.repair.companions import (
    MutationFrontier,
    submit_command,
)
from paritygrid.application.repair.errors import (
    RepairPlanMismatchError,
    RepairPlanStateError,
    RepairWriterOutcomeUnknownError,
    RepairWriterUnavailableError,
)
from paritygrid.application.repair.evidence import RepairWorkflowReader
from paritygrid.application.repair.identities import (
    derive_action_id,
    derive_action_idempotency_key,
    derive_conflict_id,
    derive_plan_id,
    derive_verification_id,
)
from paritygrid.application.repair.payloads import (
    parse_observed_payload,
    render_effect_payload,
    render_target_payload,
)
from paritygrid.application.repair.planning import validate_safe_action_matrix
from paritygrid.domain.models import RepairPlanId, StateFingerprint
from tests.repair.conftest import (
    RUN_ID,
    DeterministicClock,
    analysis,
    seed_terminal_run,
    wire_payload,
)
from tests.repair.test_applier import _IdempotentFakeTarget, _no_sleep

pytestmark = pytest.mark.anyio


class _RaisingTicket:
    """A ticket whose wait always reports an unknown commit outcome."""

    def __init__(self) -> None:
        self._submission_id = WriterSubmissionId(1)

    @property
    def submission_id(self) -> WriterSubmissionId:
        return self._submission_id

    def result(self, *, timeout_seconds: float) -> object:
        raise WriterCommitOutcomeUnknownError("The durable outcome requires recovery inspection.")

    async def result_async(self, *, timeout_seconds: float) -> object:
        raise WriterCommitOutcomeUnknownError("The durable outcome requires recovery inspection.")


class _UnknownOutcomeProxy:
    """Delegate to the real writer, then lose the receipt a fixed number of times."""

    def __init__(
        self,
        real: SQLiteTransactionalWriter,
        *,
        lose_receipts: dict[WriterCommandKind, int],
    ) -> None:
        self._real = real
        self._lose_receipts = dict(lose_receipts)
        self._attempts: dict[WriterCommandKind, int] = {}
        self._real_tickets: list[WriterTicket] = []

    def start(self) -> None:
        return self._real.start()

    def close(self, *, timeout_seconds: float) -> object:
        return self._real.close(timeout_seconds=timeout_seconds)

    def submit(self, command: WriterCommand, *, timeout_seconds: float) -> WriterTicket:
        kind = command.kind
        seen = self._attempts.get(kind, 0) + 1
        self._attempts[kind] = seen
        ticket = self._real.submit(command, timeout_seconds=timeout_seconds)
        if seen <= self._lose_receipts.get(kind, 0):
            # The command executed and committed; only its receipt is lost.
            self._real_tickets.append(ticket)
            return cast(WriterTicket, _RaisingTicket())
        return ticket

    async def submit_async(self, command: WriterCommand, *, timeout_seconds: float) -> WriterTicket:
        return self.submit(command, timeout_seconds=timeout_seconds)

    def snapshot(self) -> object:
        return self._real.snapshot()


def _result() -> ReconciliationAnalysis:
    return analysis(
        [wire_payload("GRID-0001"), wire_payload("GRID-0002", quantity=4)],
        [wire_payload("GRID-0001", name="Different")],
    )


async def _prepared(
    database: SQLiteDatabase,
    writer: TransactionalWriter,
    reader: RepairWorkflowReader,
    clock: DeterministicClock,
) -> None:
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


class TestUnknownWriterOutcomes:
    async def test_lost_begin_receipt_resolves_by_identical_replay(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        await _prepared(database, writer, reader, clock)
        proxy = _UnknownOutcomeProxy(
            writer, lose_receipts={WriterCommandKind.BEGIN_REPAIR_APPLICATION: 1}
        )
        plan_id = derive_plan_id(RUN_ID, _result().summary.fingerprint)
        report = await RepairApplicationService(
            writer=cast("TransactionalWriter", proxy),
            reader=reader,
            now=clock.now,
            policy=RepairApplicationPolicy(delay_seconds=0.0),
            sleep=_no_sleep,
        ).apply(
            run_id=RUN_ID,
            repair_plan_id=plan_id,
            target=cast(TargetConnector, _IdempotentFakeTarget()),
            context_id="corr",
        )
        assert report.disposition.value in {"already_applied", "completed"}
        aggregate = reader.load_plan(plan_id)
        assert aggregate is not None

    async def test_persisting_an_unknown_receipt_replays_identically(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        seed_terminal_run(database)
        proxy = _UnknownOutcomeProxy(
            writer, lose_receipts={WriterCommandKind.PERSIST_RECONCILIATION: 1}
        )
        outcome = ReconciliationResultService(
            cast("TransactionalWriter", proxy), reader, now=clock.now
        ).persist(
            run_id=RUN_ID,
            analysis=_result(),
            actor="operator-1",
            correlation_id="corr",
        )
        assert not outcome.replayed
        assert outcome.record.summary.reconciliation_fingerprint == (_result().summary.fingerprint)

    async def test_plan_creation_with_a_lost_receipt_replays_identically(
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
        proxy = _UnknownOutcomeProxy(
            writer, lose_receipts={WriterCommandKind.CREATE_REPAIR_PLAN: 1}
        )
        created = RepairPlanningService(
            cast("TransactionalWriter", proxy), reader, now=clock.now
        ).create(run_id=RUN_ID, analysis=result, actor="operator-1", correlation_id="corr")
        assert created.aggregate is not None
        assert created.replayed

    async def test_persistent_unknown_outcome_on_a_record_suspends(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        await _prepared(database, writer, reader, clock)
        proxy = _UnknownOutcomeProxy(
            writer,
            lose_receipts={WriterCommandKind.RECORD_REPAIR_ACTION_APPLIED: 99},
        )
        plan_id = derive_plan_id(RUN_ID, _result().summary.fingerprint)
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
            target=cast(TargetConnector, _IdempotentFakeTarget()),
            context_id="corr",
        )
        assert report.disposition.value == "unresolved"
        aggregate = reader.load_plan(plan_id)
        assert aggregate is not None
        assert aggregate.plan.status is RepairPlanStatus.APPLYING


class TestEntryStateFences:
    async def test_rejected_plan_cannot_be_applied(
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
        RepairApprovalService(writer, reader, now=clock.now).reject(
            run_id=RUN_ID,
            repair_plan_id=created.aggregate.plan.repair_plan_id,
            correlation_id="corr-reject",
        )
        with pytest.raises(RepairPlanStateError, match="rejected"):
            await RepairApplicationService(writer, reader, now=clock.now, sleep=_no_sleep).apply(
                run_id=RUN_ID,
                repair_plan_id=created.aggregate.plan.repair_plan_id,
                target=cast(TargetConnector, _IdempotentFakeTarget()),
                context_id="corr",
            )

    async def test_unknown_plan_and_foreign_plan_are_rejected(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        from paritygrid.domain.models import RepairPlanId, RunId
        from tests.repair.conftest import RUN_ID as PRIMARY

        await _prepared(database, writer, reader, clock)
        service = RepairApplicationService(writer, reader, now=clock.now, sleep=_no_sleep)
        target = cast(TargetConnector, _idempotent_fake_for_foreign())
        with pytest.raises(RepairPlanMismatchError, match="does not exist"):
            await service.apply(
                run_id=PRIMARY,
                repair_plan_id=RepairPlanId("rpl_missing"),
                target=target,
                context_id="corr",
            )
        # A plan created for a different run can never be applied to this one.
        other_run = RunId("run_phase11-second")
        seed_terminal_run(database, other_run, seed_pipeline=False)
        other = _result()
        ReconciliationResultService(writer, reader, now=clock.now).persist(
            run_id=other_run, analysis=other, actor="operator-1", correlation_id="corr"
        )
        created = RepairPlanningService(writer, reader, now=clock.now).create(
            run_id=other_run, analysis=other, actor="operator-1", correlation_id="corr"
        )
        assert created.aggregate is not None
        with pytest.raises(RepairPlanMismatchError, match="another run"):
            await service.apply(
                run_id=PRIMARY,
                repair_plan_id=created.aggregate.plan.repair_plan_id,
                target=target,
                context_id="corr",
            )


class TestApprovalRejection:
    def test_reject_returns_the_rejected_plan_and_replays(
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
        service = RepairApprovalService(writer, reader, now=clock.now)
        rejected = service.reject(
            run_id=RUN_ID,
            repair_plan_id=created.aggregate.plan.repair_plan_id,
            correlation_id="corr-reject",
        )
        assert rejected.plan.status is RepairPlanStatus.REJECTED
        replayed = service.reject(
            run_id=RUN_ID,
            repair_plan_id=created.aggregate.plan.repair_plan_id,
            correlation_id="corr-reject",
        )
        assert replayed.plan.status is RepairPlanStatus.REJECTED

    def test_an_approved_plan_cannot_be_rejected(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        _, plan_id, _content = _approval_fixture(database, writer, reader, clock)
        service = RepairApprovalService(writer, reader, now=clock.now)
        with pytest.raises(RepairPlanStateError, match="cannot be rejected"):
            service.reject(run_id=RUN_ID, repair_plan_id=plan_id, correlation_id="corr")


def _approval_fixture(
    database: SQLiteDatabase,
    writer: SQLiteTransactionalWriter,
    reader: SQLiteRepairWorkflowReader,
    clock: DeterministicClock,
) -> tuple[ReconciliationAnalysis, RepairPlanId, StateFingerprint]:
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
    return (
        result,
        created.aggregate.plan.repair_plan_id,
        (created.aggregate.plan.content_fingerprint),
    )


class TestTypedInputFences:
    def test_services_reject_foreign_identity_types(
        self,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        service = ReconciliationResultService(writer, reader, now=clock.now)
        with pytest.raises(TypeError):
            service.persist(
                run_id="run_text",  # type: ignore[arg-type]
                analysis=_result(),
                actor="a",
                correlation_id="c",
            )
        with pytest.raises(TypeError):
            service.persist(
                run_id=RUN_ID,
                analysis=object(),  # type: ignore[arg-type]
                actor="a",
                correlation_id="c",
            )
        planning = RepairPlanningService(writer, reader, now=clock.now)
        with pytest.raises(TypeError):
            planning.create(
                run_id="run_text",  # type: ignore[arg-type]
                analysis=_result(),
                actor="a",
                correlation_id="c",
            )
        with pytest.raises(TypeError):
            planning.create(
                run_id=RUN_ID,
                analysis=object(),  # type: ignore[arg-type]
                actor="a",
                correlation_id="c",
            )

    def test_approval_request_validates_its_shape(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        reader: SQLiteRepairWorkflowReader,
        clock: DeterministicClock,
    ) -> None:
        from paritygrid.application.ports.consistency import RedactedDocument
        from paritygrid.domain.models import StateFingerprint

        result, plan_id, content = _approval_fixture(database, writer, reader, clock)
        base = RepairApprovalRequest(
            run_id=RUN_ID,
            repair_plan_id=plan_id,
            approved_by="approver-1",
            correlation_id="corr",
            approved_content_fingerprint=content,
            approved_reconciliation_fingerprint=result.summary.fingerprint,
            detail=RedactedDocument.from_mapping({"decision": "ok"}),
        )
        for field, value in (
            ("run_id", "run_text"),
            ("repair_plan_id", "rpl_text"),
            ("approved_by", ""),
            ("correlation_id", ""),
            ("approved_content_fingerprint", "f" * 64),
            ("approved_reconciliation_fingerprint", "e" * 64),
            ("detail", object()),
        ):
            with pytest.raises((TypeError, ValueError)):
                replace(base, **{field: value})
        assert base.approved_by == "approver-1"
        assert StateFingerprint(content.value) == content

    def test_frontier_rejects_invalid_values(self) -> None:
        with pytest.raises(ValueError, match="run_row_version is outside"):
            MutationFrontier(run_row_version=0, next_event_sequence=1, event_counter_row_version=1)
        with pytest.raises(ValueError, match="next_event_sequence is outside"):
            MutationFrontier(
                run_row_version=1,
                next_event_sequence="2",  # type: ignore[arg-type]
                event_counter_row_version=1,
            )


class TestPolicyValidation:
    def test_policy_bounds_are_enforced(self) -> None:
        from paritygrid.application.repair import RepairApplicationPolicy

        with pytest.raises(ValueError, match="max_attempts_per_action must be between"):
            RepairApplicationPolicy(max_attempts_per_action=0)
        with pytest.raises(ValueError, match="max_ambiguous_replays must be between"):
            RepairApplicationPolicy(max_ambiguous_replays=True)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="delay_seconds must be a bounded"):
            RepairApplicationPolicy(delay_seconds=-0.1)
        with pytest.raises(ValueError, match="timeout_seconds must be a bounded"):
            RepairApplicationPolicy(timeout_seconds="30")  # type: ignore[arg-type]
        assert RepairApplicationPolicy(delay_seconds=0.0).max_attempts_per_action == 4

    def test_safe_action_matrix_detects_policy_divergence(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from paritygrid.domain.reconciliation import ReconciliationClassification

        validate_safe_action_matrix()
        monkeypatch.setattr(
            planning_module,
            "REPAIRABLE_CLASSIFICATIONS",
            frozenset({ReconciliationClassification.MISSING_FROM_SOURCE}),
        )
        with pytest.raises(RuntimeError, match="policy"):
            validate_safe_action_matrix()


class TestPayloadAndIdentityEdges:
    def test_renderers_reject_foreign_types(self) -> None:
        with pytest.raises(TypeError):
            render_target_payload(object())  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            render_effect_payload(object())  # type: ignore[arg-type]

    def test_malformed_observed_payload_is_quarantined(self) -> None:
        parsed = parse_observed_payload(0, None)
        assert parsed.record is None
        assert parsed.quarantined is not None
        with pytest.raises(TypeError):
            parse_observed_payload(-1, {})  # type: ignore[arg-type]

    def test_identity_slugs_truncate_with_a_digest(self) -> None:
        from paritygrid.domain.models import RunId, StateFingerprint

        long_run = RunId("run_" + "a" * 60)
        fingerprint = StateFingerprint("b" * 64)
        conflict = derive_conflict_id(long_run, "GRID-" + "9" * 40)
        assert len(conflict.value) <= 68
        assert conflict.value.startswith("cnf_")
        assert "-" in conflict.value.removeprefix("cnf_")
        plan = derive_plan_id(long_run, fingerprint)
        assert len(plan.value) <= 68
        action = derive_action_id(long_run, fingerprint, "GRID-1")
        assert action.value.startswith("rac_")
        verification = derive_verification_id(long_run, fingerprint, fingerprint)
        assert verification.value.startswith("tgv_")
        key = derive_action_idempotency_key(long_run, fingerprint, "GRID-" + "9" * 80)
        assert len(key) <= 128

    def test_expected_inventory_rejects_foreign_inputs(self) -> None:
        with pytest.raises(TypeError):
            build_expected_inventory(object(), None)  # type: ignore[arg-type]
        result = _result()
        with pytest.raises(TypeError):
            build_expected_inventory(result, object())  # type: ignore[arg-type]


class TestEvidenceReaderEdges:
    def test_evidence_frontier_properties_expose_the_counter(self) -> None:
        from paritygrid.application.ports.execution import RunEventCounterRecord

        counter = RunEventCounterRecord(run_id=RUN_ID, next_sequence_number=5, row_version=5)
        evidence = RepairWorkflowEvidence(
            run=_run_record_stub(),
            event_counter=counter,
            summary=None,
        )
        assert evidence.next_event_sequence == 5
        assert evidence.event_counter_row_version == 5


def _run_record_stub() -> RunRecord:
    from datetime import UTC, datetime

    from paritygrid.application.ports.configuration import ConfigurationDocument
    from paritygrid.domain.execution import RunState
    from paritygrid.domain.models import (
        PipelineId,
        PipelineVersion,
        UtcTimestamp,
    )

    return RunRecord(
        run_id=RUN_ID,
        pipeline_id=PipelineId("pip_stub"),
        pipeline_version=PipelineVersion(1),
        runner_kind="sequential",
        runner_configuration=ConfigurationDocument.from_mapping({}),
        state=RunState.SUCCEEDED,
        row_version=3,
        scenario_seed=None,
        created_at=UtcTimestamp(datetime(2026, 8, 27, 8, 0, 0, tzinfo=UTC)),
        started_at=None,
        finished_at=None,
        cancellation_requested_at=None,
        recovery_started_at=None,
        recovered_at=None,
        execution_evidence_fingerprint=None,
    )


def _idempotent_fake_for_foreign() -> object:
    return _IdempotentFakeTarget()


class TestCompanionSubmissionErrors:
    def test_writer_failures_map_to_typed_service_errors(self) -> None:
        class _ClosedWriter:
            def submit(self, command: object, *, timeout_seconds: float) -> object:
                from paritygrid.application.ports.writer import WriterClosedError

                raise WriterClosedError("the writer is closed")

        with pytest.raises(RepairWriterUnavailableError):
            submit_command(
                cast("TransactionalWriter", _ClosedWriter()),
                cast("WriterCommand", object()),
                timeout_seconds=1.0,
            )

    def test_unknown_result_maps_to_the_unknown_outcome_error(self) -> None:
        class _UnknownTicket:
            def result(self, *, timeout_seconds: float) -> object:
                raise WriterCommitOutcomeUnknownError("unknown")

        class _UnknownWriter:
            def submit(self, command: object, *, timeout_seconds: float) -> object:
                return _UnknownTicket()

        with pytest.raises(RepairWriterOutcomeUnknownError):
            submit_command(
                cast("TransactionalWriter", _UnknownWriter()),
                cast("WriterCommand", object()),
                timeout_seconds=1.0,
            )

    def test_invalid_receipt_is_rejected(self) -> None:
        class _BadTicket:
            def result(self, *, timeout_seconds: float) -> object:
                return object()

        class _BadWriter:
            def submit(self, command: object, *, timeout_seconds: float) -> object:
                return _BadTicket()

        with pytest.raises(RepairWriterUnavailableError, match="invalid receipt"):
            submit_command(
                cast("TransactionalWriter", _BadWriter()),
                cast("WriterCommand", object()),
                timeout_seconds=1.0,
            )
