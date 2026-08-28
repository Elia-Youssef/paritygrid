"""Deterministic repair-plan generation from one reconciliation analysis.

The generator is pure: it reads exactly one :class:`ReconciliationAnalysis`
and emits the closed set of safe actions the governing policy permits.
Only ``missing_from_target`` (create) and ``field_mismatch`` (update)
outcomes are repairable; every other classification is human-review-only
and never becomes a target effect. Deletion is not expressible anywhere in
the policy, the action kind set, or the plan values.
"""

from dataclasses import dataclass, replace

from paritygrid.application.ports.repair_audit import RepairActionKeyMap
from paritygrid.application.reconciliation.analysis import ReconciliationAnalysis
from paritygrid.application.repair.identities import (
    derive_action_id,
    derive_action_idempotency_key,
    derive_conflict_id,
    derive_plan_id,
)
from paritygrid.domain.canonical import FingerprintScope, fingerprint_state
from paritygrid.domain.models import (
    RepairActionId,
    RunId,
    StateFingerprint,
)
from paritygrid.domain.reconciliation import (
    ReconciliationClassification,
    SuggestedResolution,
    suggested_resolution_for,
)
from paritygrid.domain.repair import RepairAction, RepairActionKind, RepairPlan

REPAIR_GENERATION_POLICY_VERSION = 1
REPAIR_GENERATION_VERSION = 1
REPAIRABLE_CLASSIFICATIONS: frozenset[ReconciliationClassification] = frozenset(
    {ReconciliationClassification.MISSING_FROM_TARGET, ReconciliationClassification.FIELD_MISMATCH}
)
_REPAIRABLE_ACTION_KINDS: frozenset[RepairActionKind] = frozenset(
    {RepairActionKind.CREATE_TARGET, RepairActionKind.UPDATE_TARGET}
)


@dataclass(frozen=True, slots=True)
class RepairPlanBinding:
    """Every identity a generated plan is bound to."""

    run_id: RunId
    reconciliation_fingerprint: StateFingerprint
    source_input_identity: str
    target_input_identity: str
    policy_version: int
    generation_version: int
    rules_version: int
    analysis_version: int
    analytical_query_version: int
    action_count: int


@dataclass(frozen=True, slots=True)
class GeneratedRepairPlan:
    """The generation result for one reconciliation snapshot."""

    plan: RepairPlan | None
    content_fingerprint: StateFingerprint | None
    action_keys: RepairActionKeyMap | None
    binding: RepairPlanBinding
    repairable_keys: tuple[str, ...]
    review_only_keys: tuple[str, ...]

    def __post_init__(self) -> None:
        empty = self.plan is None
        if empty != (self.content_fingerprint is None) or empty != (self.action_keys is None):
            raise ValueError("generated plan parts must be present together")
        if self.binding.action_count != len(self.repairable_keys):
            raise ValueError("generated plan binding must cover every repairable key")


def generate_repair_plan(
    *,
    run_id: RunId,
    analysis: ReconciliationAnalysis,
) -> GeneratedRepairPlan:
    """Generate the one safe plan permitted for this reconciliation snapshot."""
    if type(run_id) is not RunId:
        raise TypeError("repair generation requires RunId")
    if type(analysis) is not ReconciliationAnalysis:
        raise TypeError("repair generation requires ReconciliationAnalysis")
    summary = analysis.summary
    binding = RepairPlanBinding(
        run_id=run_id,
        reconciliation_fingerprint=summary.fingerprint,
        source_input_identity=summary.source_input_identity,
        target_input_identity=summary.target_input_identity,
        policy_version=REPAIR_GENERATION_POLICY_VERSION,
        generation_version=REPAIR_GENERATION_VERSION,
        rules_version=summary.rules_version,
        analysis_version=summary.analysis_version,
        analytical_query_version=summary.analytical_query_version,
        action_count=0,
    )
    repairable: list[RepairAction] = []
    repairable_keys: list[str] = []
    review_only: list[str] = []
    for key in analysis.classification.keys:
        outcome = key.outcome
        if outcome.classification not in REPAIRABLE_CLASSIFICATIONS:
            review_only.append(outcome.sku)
            continue
        repairable_keys.append(outcome.sku)
        repairable.append(
            RepairAction.from_outcome(
                action_id=derive_action_id(run_id, summary.fingerprint, outcome.sku),
                conflict_id=derive_conflict_id(run_id, outcome.sku),
                state_fingerprint=summary.fingerprint,
                outcome=outcome,
            )
        )
    if not repairable:
        return GeneratedRepairPlan(
            plan=None,
            content_fingerprint=None,
            action_keys=None,
            binding=replace_count(binding, 0),
            repairable_keys=(),
            review_only_keys=tuple(sorted(review_only)),
        )
    plan = RepairPlan(
        plan_id=derive_plan_id(run_id, summary.fingerprint),
        state_fingerprint=summary.fingerprint,
        actions=tuple(repairable),
    )
    content = fingerprint_state((plan,), scope=FingerprintScope.REPAIR_PLAN_CONTENT)
    keys: dict[RepairActionId, str] = {
        action.action_id: derive_action_idempotency_key(run_id, content, action.sku)
        for action in plan.actions
    }
    return GeneratedRepairPlan(
        plan=plan,
        content_fingerprint=content,
        action_keys=RepairActionKeyMap.from_mapping(keys),
        binding=replace_count(binding, len(plan.actions)),
        repairable_keys=tuple(sorted(repairable_keys)),
        review_only_keys=tuple(sorted(review_only)),
    )


def validate_safe_action_matrix() -> None:
    """Prove the policy set matches the domain's own safe-resolution mapping."""
    for classification in ReconciliationClassification:
        resolution = suggested_resolution_for(classification)
        if classification in REPAIRABLE_CLASSIFICATIONS:
            if resolution not in {
                SuggestedResolution.CREATE_TARGET,
                SuggestedResolution.UPDATE_TARGET,
            }:
                raise RuntimeError("repair policy diverges from the safe resolution mapping")
        elif resolution in {
            SuggestedResolution.CREATE_TARGET,
            SuggestedResolution.UPDATE_TARGET,
        }:
            raise RuntimeError("repair policy permits an unsafe classification")


def repairable_action_kinds() -> frozenset[RepairActionKind]:
    """Return the closed set of action kinds the policy can ever produce."""
    return _REPAIRABLE_ACTION_KINDS


def replace_count(binding: RepairPlanBinding, count: int) -> RepairPlanBinding:
    """Return the binding with an exact action count."""
    return replace(binding, action_count=count)


__all__ = [
    "REPAIRABLE_CLASSIFICATIONS",
    "REPAIR_GENERATION_POLICY_VERSION",
    "REPAIR_GENERATION_VERSION",
    "GeneratedRepairPlan",
    "RepairPlanBinding",
    "generate_repair_plan",
    "repairable_action_kinds",
    "validate_safe_action_matrix",
]
