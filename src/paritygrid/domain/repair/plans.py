"""Safe repair actions bound to an exact reconciliation state."""

import re
from collections.abc import Hashable, Iterable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import ClassVar, cast

from paritygrid.domain.errors import StaleRepairPlanError
from paritygrid.domain.models import (
    ConflictId,
    InventoryRecord,
    RepairActionId,
    RepairPlanId,
    RunId,
    StateFingerprint,
)
from paritygrid.domain.reconciliation import (
    FieldMismatch,
    ReconciliationClassification,
    ReconciliationOutcome,
    differences_between,
)

_SHA256 = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True, slots=True)
class RepairPlanBinding:
    """Immutable source, target, and policy identity for one repair plan."""

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

    def __post_init__(self) -> None:
        if (
            type(self.run_id) is not RunId
            or type(self.reconciliation_fingerprint) is not StateFingerprint
        ):
            raise TypeError("repair-plan binding identities are invalid")
        for value in (self.source_input_identity, self.target_input_identity):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise ValueError("repair-plan binding input identity is invalid")
        for value in (
            self.policy_version,
            self.generation_version,
            self.rules_version,
            self.analysis_version,
            self.analytical_query_version,
        ):
            if type(value) is not int or value < 1:
                raise ValueError("repair-plan binding version is invalid")
        if (
            type(self.action_count) is not int
            or not 0 <= self.action_count <= RepairPlan.MAX_ACTIONS
        ):
            raise ValueError("repair-plan binding action count is invalid")


class RepairActionKind(StrEnum):
    """Closed set of supported non-destructive target effects."""

    CREATE_TARGET = "create_target"
    UPDATE_TARGET = "update_target"


@dataclass(frozen=True, slots=True)
class RepairAction:
    """One idempotent create or update guarded by exact prior state."""

    action_id: RepairActionId
    conflict_id: ConflictId
    state_fingerprint: StateFingerprint
    kind: RepairActionKind
    proposed_record: InventoryRecord
    expected_target_record: InventoryRecord | None = None
    mismatches: tuple[FieldMismatch, ...] = field(init=False)

    def __post_init__(self) -> None:
        _require_type(self.action_id, RepairActionId, field_name="action_id")
        _require_type(self.conflict_id, ConflictId, field_name="conflict_id")
        _require_type(
            self.state_fingerprint,
            StateFingerprint,
            field_name="state_fingerprint",
        )
        _require_type(self.kind, RepairActionKind, field_name="kind")
        _require_type(self.proposed_record, InventoryRecord, field_name="proposed_record")

        if self.kind is RepairActionKind.CREATE_TARGET:
            if self.expected_target_record is not None:
                raise ValueError("create action requires the target record to be absent")
            mismatches: tuple[FieldMismatch, ...] = ()
        else:
            if type(self.expected_target_record) is not InventoryRecord:
                raise TypeError("update action requires an expected target InventoryRecord")
            mismatches = differences_between(self.proposed_record, self.expected_target_record)
            if not mismatches:
                raise ValueError("update action requires at least one field mismatch")
        object.__setattr__(self, "mismatches", mismatches)

    @classmethod
    def from_outcome(
        cls,
        *,
        action_id: RepairActionId,
        conflict_id: ConflictId,
        state_fingerprint: StateFingerprint,
        outcome: object,
    ) -> RepairAction:
        """Build the only safe action for a repairable reconciliation outcome."""
        if type(outcome) is not ReconciliationOutcome:
            raise TypeError("repair action outcome must be a ReconciliationOutcome")
        if outcome.classification is ReconciliationClassification.MISSING_FROM_TARGET:
            return cls(
                action_id=action_id,
                conflict_id=conflict_id,
                state_fingerprint=state_fingerprint,
                kind=RepairActionKind.CREATE_TARGET,
                proposed_record=outcome.source_records[0],
            )
        if outcome.classification is ReconciliationClassification.FIELD_MISMATCH:
            return cls(
                action_id=action_id,
                conflict_id=conflict_id,
                state_fingerprint=state_fingerprint,
                kind=RepairActionKind.UPDATE_TARGET,
                proposed_record=outcome.source_records[0],
                expected_target_record=outcome.target_records[0],
            )
        raise ValueError(f"{outcome.classification.value} outcome is not safely repairable")

    @property
    def sku(self) -> str:
        """Return the canonical inventory key affected by the action."""
        return self.proposed_record.sku


@dataclass(frozen=True, slots=True)
class RepairPlan:
    """An immutable set of safe actions for one exact reconciliation state."""

    MAX_ACTIONS: ClassVar[int] = 10_000

    plan_id: RepairPlanId
    state_fingerprint: StateFingerprint
    actions: tuple[RepairAction, ...]
    binding: RepairPlanBinding | None = None

    def __post_init__(self) -> None:
        _require_type(self.plan_id, RepairPlanId, field_name="plan_id")
        _require_type(
            self.state_fingerprint,
            StateFingerprint,
            field_name="state_fingerprint",
        )
        actions_value = cast(object, self.actions)
        if not isinstance(actions_value, tuple):
            raise TypeError("repair plan actions must be a tuple")
        action_values = cast(tuple[object, ...], actions_value)
        if len(action_values) > self.MAX_ACTIONS:
            raise ValueError(f"repair plan must contain at most {self.MAX_ACTIONS} actions")
        if not action_values:
            raise ValueError("repair plan requires at least one action")
        if any(type(action) is not RepairAction for action in action_values):
            raise TypeError("repair plan actions must contain only RepairAction values")
        actions = cast(tuple[RepairAction, ...], action_values)
        if any(action.state_fingerprint != self.state_fingerprint for action in actions):
            raise ValueError("every repair action must reference the plan state fingerprint")
        _require_unique((action.action_id for action in actions), subject="action identities")
        _require_unique((action.conflict_id for action in actions), subject="conflict identities")
        _require_unique((action.sku for action in actions), subject="inventory keys")
        if self.binding is not None:
            _require_type(self.binding, RepairPlanBinding, field_name="binding")
            if (
                self.binding.reconciliation_fingerprint != self.state_fingerprint
                or self.binding.action_count != len(actions)
            ):
                raise ValueError("repair-plan binding does not match plan contents")
        object.__setattr__(self, "actions", tuple(sorted(actions, key=_action_order_key)))

    def is_current(self, current_fingerprint: object) -> bool:
        """Report whether this plan still describes the supplied state."""
        if type(current_fingerprint) is not StateFingerprint:
            raise TypeError("current fingerprint must be a StateFingerprint")
        return self.state_fingerprint == current_fingerprint

    def require_current(self, current_fingerprint: object) -> None:
        """Reject use when the current reconciliation state has changed."""
        if type(current_fingerprint) is not StateFingerprint:
            raise TypeError("current fingerprint must be a StateFingerprint")
        if not self.is_current(current_fingerprint):
            raise StaleRepairPlanError(
                expected=self.state_fingerprint,
                actual=current_fingerprint,
            )


def _require_type(value: object, expected_type: type[object], *, field_name: str) -> None:
    if type(value) is not expected_type:
        raise TypeError(f"{field_name} must be a {expected_type.__name__}")


def _require_unique(values: Iterable[Hashable], *, subject: str) -> None:
    items = tuple(values)
    if len(set(items)) != len(items):
        raise ValueError(f"repair plan {subject} must be unique")


def _action_order_key(action: RepairAction) -> tuple[str, str, str]:
    return action.sku, action.kind.value, action.action_id.value
