"""Closed repair commands accepted by the transactional writer."""

from dataclasses import dataclass

from paritygrid.application.ports.consistency import ExecutionEventBatch, RedactedDocument
from paritygrid.application.ports.execution import RunRecord
from paritygrid.application.ports.repair_audit import (
    AppliedRepairAction,
    AuditEntryRecord,
    PendingAuditEntry,
    RepairActionKeyMap,
    RepairApplicationBeginResult,
    RepairApplicationReservation,
    RepairApplicationResult,
    RepairPlanAggregate,
)
from paritygrid.application.ports.writer import EventAppendRequest, WriterCommandKind
from paritygrid.domain.models import (
    RepairActionId,
    RepairPlanId,
    RunId,
    StateFingerprint,
    UtcTimestamp,
)
from paritygrid.domain.repair import RepairPlan


@dataclass(frozen=True, slots=True, repr=False)
class RepairCompanions:
    """Required audit and durable-event facts for one repair mutation."""

    audit: PendingAuditEntry
    event: EventAppendRequest
    expected_run_row_version: int


@dataclass(frozen=True, slots=True, repr=False)
class CreateRepairPlan:
    run_id: RunId
    plan: RepairPlan
    action_keys: RepairActionKeyMap
    created_at: UtcTimestamp
    companions: RepairCompanions

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.CREATE_REPAIR_PLAN


@dataclass(frozen=True, slots=True, repr=False)
class ApproveRepairPlan:
    run_id: RunId
    repair_plan_id: RepairPlanId
    expected_plan_row_version: int
    current_reconciliation_fingerprint: StateFingerprint
    approved_by: str
    approved_at: UtcTimestamp
    correlation_id: str
    schema_version: int
    detail: RedactedDocument
    companions: RepairCompanions

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.APPROVE_REPAIR_PLAN


@dataclass(frozen=True, slots=True, repr=False)
class RejectRepairPlan:
    run_id: RunId
    repair_plan_id: RepairPlanId
    expected_plan_row_version: int
    rejected_at: UtcTimestamp
    companions: RepairCompanions

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.REJECT_REPAIR_PLAN


@dataclass(frozen=True, slots=True, repr=False)
class BeginRepairApplication:
    run_id: RunId
    repair_plan_id: RepairPlanId
    expected_plan_row_version: int
    current_reconciliation_fingerprint: StateFingerprint
    applying_at: UtcTimestamp
    companions: RepairCompanions

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.BEGIN_REPAIR_APPLICATION


@dataclass(frozen=True, slots=True, repr=False)
class RecordRepairActionApplied:
    run_id: RunId
    reservation: RepairApplicationReservation
    repair_action_id: RepairActionId
    result: RepairApplicationResult
    target_version: int
    applied_at: UtcTimestamp
    companions: RepairCompanions

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.RECORD_REPAIR_ACTION_APPLIED


@dataclass(frozen=True, slots=True, repr=False)
class RecordRepairActionAttempt:
    """Append bounded ambiguous-attempt evidence without terminalizing an action."""

    run_id: RunId
    reservation: RepairApplicationReservation
    repair_action_id: RepairActionId
    attempted_at: UtcTimestamp
    companions: RepairCompanions

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.RECORD_REPAIR_ACTION_ATTEMPT


@dataclass(frozen=True, slots=True, repr=False)
class RecordRepairActionFailed:
    run_id: RunId
    reservation: RepairApplicationReservation
    repair_action_id: RepairActionId
    result: RepairApplicationResult
    failed_at: UtcTimestamp
    plan_failure: RedactedDocument
    companions: RepairCompanions

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.RECORD_REPAIR_ACTION_FAILED


@dataclass(frozen=True, slots=True, repr=False)
class CompleteRepairApplication:
    run_id: RunId
    reservation: RepairApplicationReservation
    applied_at: UtcTimestamp
    companions: RepairCompanions

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.COMPLETE_REPAIR_APPLICATION


@dataclass(frozen=True, slots=True)
class RepairMutationResult:
    result_kind: WriterCommandKind
    aggregate: RepairPlanAggregate
    audit: AuditEntryRecord | None
    events: ExecutionEventBatch | None
    run: RunRecord | None


@dataclass(frozen=True, slots=True)
class BeginRepairApplicationResult:
    operation: RepairApplicationBeginResult
    audit: AuditEntryRecord | None
    events: ExecutionEventBatch | None
    run: RunRecord | None

    @property
    def result_kind(self) -> WriterCommandKind:
        return WriterCommandKind.BEGIN_REPAIR_APPLICATION


@dataclass(frozen=True, slots=True)
class RepairActionAppliedResult:
    operation: AppliedRepairAction
    audit: AuditEntryRecord | None
    events: ExecutionEventBatch | None
    run: RunRecord | None

    @property
    def result_kind(self) -> WriterCommandKind:
        return WriterCommandKind.RECORD_REPAIR_ACTION_APPLIED


__all__ = [
    "ApproveRepairPlan",
    "BeginRepairApplication",
    "BeginRepairApplicationResult",
    "CompleteRepairApplication",
    "CreateRepairPlan",
    "RecordRepairActionApplied",
    "RecordRepairActionAttempt",
    "RecordRepairActionFailed",
    "RejectRepairPlan",
    "RepairActionAppliedResult",
    "RepairCompanions",
    "RepairMutationResult",
]
