"""Durable creation of repair plans from persisted reconciliation snapshots."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

from paritygrid.application.ports.repair_audit import (
    RepairPlanAggregate,
    RepairPlanContentConflictError,
)
from paritygrid.application.ports.writer import TransactionalWriter
from paritygrid.application.reconciliation.analysis import ReconciliationAnalysis
from paritygrid.application.repair.companions import (
    build_companions,
    frontier_from_evidence,
    submit_command,
)
from paritygrid.application.repair.errors import (
    RepairPlanMismatchError,
    RepairReconciliationMissingError,
    RepairReconciliationStaleError,
    RepairWriterOutcomeUnknownError,
)
from paritygrid.application.repair.evidence import RepairWorkflowReader
from paritygrid.application.repair.planning import (
    GeneratedRepairPlan,
    RepairPlanBinding,
    generate_repair_plan,
)
from paritygrid.application.writes.repairs import CreateRepairPlan, RepairMutationResult
from paritygrid.domain.models import RunId, UtcTimestamp


@dataclass(frozen=True, slots=True)
class CreatedRepairPlan:
    """The durable result of one plan-creation attempt."""

    generated: GeneratedRepairPlan
    aggregate: RepairPlanAggregate | None
    binding: RepairPlanBinding
    replayed: bool

    def __post_init__(self) -> None:
        empty = self.generated.plan is None
        if empty != (self.aggregate is None):
            raise ValueError("an empty generation cannot carry a durable plan")


class RepairPlanningService:
    """Generate and durably create the one safe plan for a reconciliation."""

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

    def create(
        self,
        *,
        run_id: RunId,
        analysis: ReconciliationAnalysis,
        actor: str,
        correlation_id: str,
    ) -> CreatedRepairPlan:
        """Create the plan for one analysis; regenerate identically on replay."""
        if type(run_id) is not RunId:
            raise TypeError("repair planning requires RunId")
        if type(analysis) is not ReconciliationAnalysis:
            raise TypeError("repair planning requires ReconciliationAnalysis")
        evidence = self._reader.load(run_id)
        if evidence.summary is None:
            raise RepairReconciliationMissingError(
                "persist the reconciliation result before planning repairs"
            )
        stored = evidence.summary
        if stored.reconciliation_fingerprint != analysis.summary.fingerprint:
            raise RepairReconciliationStaleError(
                expected=stored.reconciliation_fingerprint.value,
                actual=analysis.summary.fingerprint.value,
            )
        generated = generate_repair_plan(run_id=run_id, analysis=analysis)
        plan = generated.plan
        content = generated.content_fingerprint
        keys = generated.action_keys
        if plan is None or content is None or keys is None:
            return CreatedRepairPlan(
                generated=generated, aggregate=None, binding=generated.binding, replayed=False
            )
        existing = self._reader.load_plan(plan.plan_id)
        if existing is not None:
            # A regeneration of the same snapshot returns the durable plan
            # rather than submitting a second creation with a new timestamp,
            # which the immutable plan contents would reject.
            _require_matching_plan(existing, generated)
            return CreatedRepairPlan(
                generated=generated, aggregate=existing, binding=generated.binding, replayed=True
            )
        occurred_at = self._now()
        companions = build_companions(
            frontier=frontier_from_evidence(evidence),
            run_id=run_id,
            operation="repair_plan_created",
            object_kind="repair_plan",
            object_id=plan.plan_id.value,
            actor=actor,
            correlation_id=correlation_id,
            occurred_at=occurred_at,
            payload={
                "action_count": len(plan.actions),
                "content_fingerprint": content.value,
                "reconciliation_fingerprint": analysis.summary.fingerprint.value,
            },
        )
        command = CreateRepairPlan(
            run_id=run_id,
            plan=plan,
            action_keys=keys,
            created_at=occurred_at,
            companions=companions,
        )
        try:
            _, result, mutated = self._submit_with_replay(command)
        except RepairPlanContentConflictError as error:
            winner = self._reader.load_plan(plan.plan_id)
            if winner is not None:
                _require_matching_plan(winner, generated)
                return CreatedRepairPlan(
                    generated=generated,
                    aggregate=winner,
                    binding=generated.binding,
                    replayed=True,
                )
            raise RepairPlanMismatchError(str(error)) from error
        aggregate = cast(RepairMutationResult, result).aggregate
        return CreatedRepairPlan(
            generated=generated,
            aggregate=aggregate,
            binding=generated.binding,
            replayed=not mutated,
        )

    def _submit_with_replay(self, command: CreateRepairPlan) -> tuple[object, object, bool]:
        """Submit the creation, resubmitting the identical command on unknown outcome."""
        attempts = 0
        while True:
            attempts += 1
            try:
                return submit_command(self._writer, command, timeout_seconds=self._timeout_seconds)
            except RepairWriterOutcomeUnknownError:
                if attempts >= 3:
                    raise
                continue


def _require_matching_plan(existing: RepairPlanAggregate, generated: GeneratedRepairPlan) -> None:
    plan = generated.plan
    content = generated.content_fingerprint
    if plan is None or content is None:
        raise RepairPlanMismatchError("regeneration produced no plan for a durable identity")
    if (
        existing.plan.reconciliation_fingerprint != plan.state_fingerprint
        or existing.plan.content_fingerprint != content
        or len(existing.actions) != len(plan.actions)
    ):
        raise RepairPlanMismatchError(
            "regenerated plan content differs from the durable plan identity"
        )


__all__ = ["CreatedRepairPlan", "RepairPlanningService"]
