"""Short-transaction SQLite reader for the repair workflow services."""

from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyReconciliationResultRepository,
    SqlAlchemyRepairRepository,
    SqlAlchemyRunRepository,
    SqlAlchemyTargetVerificationRepository,
)
from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
from paritygrid.application.ports.execution import (
    ExecutionRecordNotFoundError,
    RunEventCounterRecord,
)
from paritygrid.application.ports.reconciliation_persistence import (
    ReconciliationResultRecord,
    TargetVerificationRecord,
)
from paritygrid.application.ports.repair_audit import RepairPlanAggregate
from paritygrid.application.repair.evidence import RepairWorkflowEvidence
from paritygrid.domain.models import RepairPlanId, RunId, TargetVerificationId


class SQLiteRepairWorkflowReader:
    """Read one run's repair frontier and plan aggregates without mutation."""

    __slots__ = ("_database",)

    def __init__(self, database: SQLiteDatabase) -> None:
        if type(database) is not SQLiteDatabase:
            raise TypeError("repair workflow reader requires SQLiteDatabase")
        self._database = database

    def load(self, run_id: RunId) -> RepairWorkflowEvidence:
        """Return one coherent run frontier from one short transaction."""
        if type(run_id) is not RunId:
            raise TypeError("repair workflow reader requires RunId")
        with self._database.transaction() as session:
            runs = SqlAlchemyRunRepository(session)
            run = runs.get(run_id)
            counter = runs.get_event_counter(run_id)
            if run is None or counter is None:
                raise ExecutionRecordNotFoundError("repair workflow run does not exist")
            summary = SqlAlchemyReconciliationResultRepository(session).get_summary(run_id)
            return RepairWorkflowEvidence(
                run=run,
                event_counter=RunEventCounterRecord(
                    run_id=counter.run_id,
                    next_sequence_number=counter.next_sequence_number,
                    row_version=counter.row_version,
                ),
                summary=summary,
            )

    def load_plan(self, repair_plan_id: RepairPlanId) -> RepairPlanAggregate | None:
        """Return one repair plan aggregate or ``None`` without mutation."""
        if type(repair_plan_id) is not RepairPlanId:
            raise TypeError("repair workflow reader requires RepairPlanId")
        with self._database.transaction() as session:
            return SqlAlchemyRepairRepository(session).get(repair_plan_id)

    def load_reconciliation_result(self, run_id: RunId) -> ReconciliationResultRecord | None:
        """Return one durable reconciliation result or ``None`` without mutation."""
        if type(run_id) is not RunId:
            raise TypeError("repair workflow reader requires RunId")
        with self._database.transaction() as session:
            return SqlAlchemyReconciliationResultRepository(session).get_result(run_id)

    def load_target_verification(
        self, verification_id: TargetVerificationId
    ) -> TargetVerificationRecord | None:
        """Return one durable verification fact or ``None`` without mutation."""
        if type(verification_id) is not TargetVerificationId:
            raise TypeError("repair workflow reader requires TargetVerificationId")
        with self._database.transaction() as session:
            return SqlAlchemyTargetVerificationRepository(session).get(verification_id)


__all__ = ["SQLiteRepairWorkflowReader"]
