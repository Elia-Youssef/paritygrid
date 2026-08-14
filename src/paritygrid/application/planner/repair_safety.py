"""Dependency-neutral contracts for approval-before-effect safety."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from paritygrid.domain.models import NodeId

MAX_REPAIR_SAFETY_NODES = 256


class RepairSafetyError(ValueError):
    """Base failure for an unsafe repair path."""


class UnapprovedRepairEffectError(RepairSafetyError):
    """A repair effect is reachable along a path without prior approval."""


@dataclass(frozen=True, slots=True, repr=False)
class RepairSafetySummary:
    """Stable identities of approvals and guarded repair effects."""

    approval_node_ids: tuple[NodeId, ...]
    repair_effect_node_ids: tuple[NodeId, ...]

    def __post_init__(self) -> None:
        approvals = _validate_node_ids(self.approval_node_ids, "repair approval nodes")
        effects = _validate_node_ids(self.repair_effect_node_ids, "repair effect nodes")
        if len(approvals) + len(effects) > MAX_REPAIR_SAFETY_NODES:
            raise RepairSafetyError("repair safety summary exceeds the node limit")
        if frozenset(approvals) & frozenset(effects):
            raise RepairSafetyError("repair approval and effect nodes must be disjoint")
        object.__setattr__(self, "approval_node_ids", tuple(sorted(approvals, key=str)))
        object.__setattr__(self, "repair_effect_node_ids", tuple(sorted(effects, key=str)))

    def __repr__(self) -> str:
        return (
            "RepairSafetySummary("
            f"approvals={len(self.approval_node_ids)}, "
            f"effects={len(self.repair_effect_node_ids)})"
        )


def _validate_node_ids(value: object, subject: str) -> tuple[NodeId, ...]:
    if type(value) is not tuple:
        raise TypeError(f"{subject} must be a tuple")
    values = cast(tuple[object, ...], value)
    if any(type(item) is not NodeId for item in values):
        raise TypeError(f"{subject} contains an invalid value")
    if len(set(values)) != len(values):
        raise RepairSafetyError(f"{subject} must be unique")
    return cast(tuple[NodeId, ...], values)
