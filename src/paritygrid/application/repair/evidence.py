"""Read-side evidence and ports for the repair workflow services."""

from dataclasses import dataclass
from typing import Protocol

from paritygrid.application.ports.execution import RunEventCounterRecord, RunRecord
from paritygrid.application.ports.reconciliation_persistence import (
    ReconciliationResultRecord,
    ReconciliationSummaryRecord,
    TargetVerificationRecord,
)
from paritygrid.application.ports.repair_audit import RepairPlanAggregate
from paritygrid.domain.models import RepairPlanId, RunId, TargetVerificationId


@dataclass(frozen=True, slots=True)
class RepairWorkflowEvidence:
    """One coherent read of the durable facts a repair decision needs."""

    run: RunRecord
    event_counter: RunEventCounterRecord
    summary: ReconciliationSummaryRecord | None

    @property
    def next_event_sequence(self) -> int:
        return self.event_counter.next_sequence_number

    @property
    def event_counter_row_version(self) -> int:
        return self.event_counter.row_version


class RepairWorkflowReader(Protocol):
    """Read one run's repair frontier and plan aggregates without mutation."""

    def load(self, run_id: RunId) -> RepairWorkflowEvidence: ...

    def load_plan(self, repair_plan_id: RepairPlanId) -> RepairPlanAggregate | None: ...

    def load_reconciliation_result(self, run_id: RunId) -> ReconciliationResultRecord | None: ...

    def load_target_verification(
        self, verification_id: TargetVerificationId
    ) -> TargetVerificationRecord | None: ...


__all__ = ["RepairWorkflowEvidence", "RepairWorkflowReader"]
