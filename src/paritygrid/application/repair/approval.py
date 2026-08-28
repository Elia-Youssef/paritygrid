"""Explicit approval and rejection of one exact durable repair plan.

An approval is valid only for the exact current plan: the same plan
identity, the same canonical content fingerprint, and the same still-current
reconciliation fingerprint the approver reviewed. The approval fact is
immutable; an exact retry replays it, while any divergence (actor,
correlation, detail, fingerprints, plan identity, or state) is rejected.
Approval advances the plan from proposed to approved in the same
transaction that persists the audit fact and durable event.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.application.ports.repair_audit import (
    RepairApprovalConflictError,
    RepairPlanAggregate,
    RepairPlanStatus,
    RepairStaleRowVersionError,
    RepairStateConflictError,
)
from paritygrid.application.ports.writer import TransactionalWriter
from paritygrid.application.repair.companions import (
    build_companions,
    frontier_from_evidence,
    submit_command,
)
from paritygrid.application.repair.errors import (
    RepairApprovalConflictError as WorkflowApprovalConflictError,
)
from paritygrid.application.repair.errors import (
    RepairPlanMismatchError,
    RepairPlanStateError,
    RepairReconciliationMissingError,
    RepairReconciliationStaleError,
)
from paritygrid.application.repair.evidence import RepairWorkflowReader
from paritygrid.application.writes.repairs import (
    ApproveRepairPlan,
    RejectRepairPlan,
    RepairMutationResult,
)
from paritygrid.domain.models import RepairPlanId, RunId, StateFingerprint, UtcTimestamp

APPROVAL_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class RepairApprovalRequest:
    """One explicit approval decision addressing the exact current plan."""

    run_id: RunId
    repair_plan_id: RepairPlanId
    approved_by: str
    correlation_id: str
    approved_content_fingerprint: StateFingerprint
    approved_reconciliation_fingerprint: StateFingerprint
    detail: RedactedDocument

    def __post_init__(self) -> None:
        if type(self.run_id) is not RunId:
            raise TypeError("approval request requires RunId")
        if type(self.repair_plan_id) is not RepairPlanId:
            raise TypeError("approval request requires RepairPlanId")
        if type(self.approved_by) is not str or not 1 <= len(self.approved_by) <= 128:
            raise ValueError("approval actor is invalid")
        if type(self.correlation_id) is not str or not 1 <= len(self.correlation_id) <= 96:
            raise ValueError("approval correlation is invalid")
        for name in ("approved_content_fingerprint", "approved_reconciliation_fingerprint"):
            if type(getattr(self, name)) is not StateFingerprint:
                raise TypeError(f"approval request {name} is invalid")
        if type(self.detail) is not RedactedDocument:
            raise TypeError("approval detail must be a RedactedDocument")


@dataclass(frozen=True, slots=True)
class RepairApprovalOutcome:
    """The durable result of one approval attempt."""

    aggregate: RepairPlanAggregate
    replayed: bool


class RepairApprovalService:
    """Approve or reject repair plans through the fenced durable boundary."""

    def __init__(
        self,
        writer: TransactionalWriter,
        reader: RepairWorkflowReader,
        *,
        now: Callable[[], UtcTimestamp],
        timeout_seconds: float = 30.0,
    ) -> None:
        self._writer = writer
        self._reader = reader
        self._now = now
        self._timeout_seconds = timeout_seconds

    def approve(self, request: RepairApprovalRequest) -> RepairApprovalOutcome:
        """Approve the exact current plan or replay its immutable approval."""
        if type(request) is not RepairApprovalRequest:
            raise TypeError("approval requires RepairApprovalRequest")
        evidence = self._reader.load(request.run_id)
        if evidence.summary is None:
            raise RepairReconciliationMissingError("the run has no reconciliation snapshot")
        aggregate = self._require_plan(request)
        plan = aggregate.plan
        if aggregate.approval is not None:
            return self._classify_replay(request, aggregate)
        if plan.status is not RepairPlanStatus.PROPOSED:
            raise RepairPlanStateError(
                f"repair plan is already {plan.status.value} and cannot be approved"
            )
        if request.approved_content_fingerprint != plan.content_fingerprint:
            raise RepairPlanMismatchError("approval addresses different repair-plan contents")
        if request.approved_reconciliation_fingerprint != plan.reconciliation_fingerprint:
            raise RepairPlanMismatchError(
                "approval addresses a different reconciliation snapshot than the plan"
            )
        current = evidence.summary.reconciliation_fingerprint
        if current != plan.reconciliation_fingerprint:
            raise RepairReconciliationStaleError(
                expected=current.value, actual=plan.reconciliation_fingerprint.value
            )
        approved_at = self._now()
        companions = build_companions(
            frontier=frontier_from_evidence(evidence),
            run_id=request.run_id,
            operation="repair_plan_approved",
            object_kind="repair_plan",
            object_id=request.repair_plan_id.value,
            actor=request.approved_by,
            correlation_id=request.correlation_id,
            occurred_at=approved_at,
            payload={
                "approved_by": request.approved_by,
                "content_fingerprint": plan.content_fingerprint.value,
                "reconciliation_fingerprint": plan.reconciliation_fingerprint.value,
            },
        )
        command = ApproveRepairPlan(
            run_id=request.run_id,
            repair_plan_id=request.repair_plan_id,
            expected_plan_row_version=plan.row_version,
            current_reconciliation_fingerprint=current,
            approved_by=request.approved_by,
            approved_at=approved_at,
            correlation_id=request.correlation_id,
            schema_version=APPROVAL_SCHEMA_VERSION,
            detail=request.detail,
            companions=companions,
        )
        try:
            _, result, _mutated = submit_command(
                self._writer, command, timeout_seconds=self._timeout_seconds
            )
        except RepairStaleRowVersionError as error:
            raise WorkflowApprovalConflictError(
                "a concurrent approval changed the durable plan"
            ) from error
        except RepairApprovalConflictError as error:
            raise WorkflowApprovalConflictError(str(error)) from error
        except RepairStateConflictError as error:
            raise RepairPlanStateError(str(error)) from error
        return RepairApprovalOutcome(aggregate=_aggregate_of(result), replayed=False)

    def reject(
        self,
        *,
        run_id: RunId,
        repair_plan_id: RepairPlanId,
        correlation_id: str,
    ) -> RepairPlanAggregate:
        """Reject a proposed plan; a rejected plan can never be approved later."""
        if type(run_id) is not RunId or type(repair_plan_id) is not RepairPlanId:
            raise TypeError("rejection requires typed plan and run identities")
        evidence = self._reader.load(run_id)
        aggregate = self._require_plan_by_id(run_id, repair_plan_id)
        plan = aggregate.plan
        if plan.status is RepairPlanStatus.REJECTED:
            return aggregate
        if plan.status is not RepairPlanStatus.PROPOSED:
            raise RepairPlanStateError(
                f"repair plan is already {plan.status.value} and cannot be rejected"
            )
        rejected_at = self._now()
        companions = build_companions(
            frontier=frontier_from_evidence(evidence),
            run_id=run_id,
            operation="repair_plan_rejected",
            object_kind="repair_plan",
            object_id=repair_plan_id.value,
            actor="repair-operator",
            correlation_id=correlation_id,
            occurred_at=rejected_at,
            payload={
                "content_fingerprint": plan.content_fingerprint.value,
                "reconciliation_fingerprint": plan.reconciliation_fingerprint.value,
            },
        )
        command = RejectRepairPlan(
            run_id=run_id,
            repair_plan_id=repair_plan_id,
            expected_plan_row_version=plan.row_version,
            rejected_at=rejected_at,
            companions=companions,
        )
        try:
            _, result, _mutated = submit_command(
                self._writer, command, timeout_seconds=self._timeout_seconds
            )
        except RepairStaleRowVersionError as error:
            raise WorkflowApprovalConflictError(
                "a concurrent decision changed the durable plan"
            ) from error
        except RepairStateConflictError as error:
            raise RepairPlanStateError(str(error)) from error
        return _aggregate_of(result)

    def _require_plan(self, request: RepairApprovalRequest) -> RepairPlanAggregate:
        return self._require_plan_by_id(request.run_id, request.repair_plan_id)

    def _require_plan_by_id(
        self, run_id: RunId, repair_plan_id: RepairPlanId
    ) -> RepairPlanAggregate:
        aggregate = self._reader.load_plan(repair_plan_id)
        if aggregate is None:
            raise RepairPlanMismatchError("repair plan does not exist")
        if aggregate.plan.run_id != run_id:
            raise RepairPlanMismatchError("repair plan belongs to another run")
        return aggregate

    def _classify_replay(
        self, request: RepairApprovalRequest, aggregate: RepairPlanAggregate
    ) -> RepairApprovalOutcome:
        approval = aggregate.approval
        if approval is None:
            raise RepairPlanStateError("repair plan approval state is corrupt")
        if (
            approval.approved_by == request.approved_by
            and approval.correlation_id == request.correlation_id
            and approval.reconciliation_fingerprint == request.approved_reconciliation_fingerprint
            and aggregate.plan.content_fingerprint == request.approved_content_fingerprint
            and approval.detail.to_mapping() == request.detail.to_mapping()
        ):
            return RepairApprovalOutcome(aggregate=aggregate, replayed=True)
        raise WorkflowApprovalConflictError(
            "repair approval differs from the immutable durable approval"
        )


def _aggregate_of(result: object) -> RepairPlanAggregate:
    return cast(RepairMutationResult, result).aggregate


__all__ = [
    "APPROVAL_SCHEMA_VERSION",
    "RepairApprovalOutcome",
    "RepairApprovalRequest",
    "RepairApprovalService",
]
