"""Dependency-neutral contracts for approval-before-effect safety."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from paritygrid.application.planner.documents import PipelineDocument
from paritygrid.application.planner.graph import topological_node_order
from paritygrid.application.planner.registry import NodeRole, registered_node_definition
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


def validate_repair_safety(document: PipelineDocument) -> RepairSafetySummary:
    """Reject every repair effect with an approval-free incoming path."""
    if type(document) is not PipelineDocument:
        raise TypeError("pipeline document must use PipelineDocument")
    ordered_node_ids = topological_node_order(document).node_ids
    definitions = {
        node.node_id: registered_node_definition(node.kind, node.configuration_version)
        for node in document.nodes
    }
    incoming: dict[NodeId, list[NodeId]] = {node_id: [] for node_id in ordered_node_ids}
    for edge in document.edges:
        incoming[edge.target_node_id].append(edge.source_node_id)

    has_unapproved_path: dict[NodeId, bool] = {}
    for node_id in ordered_node_ids:
        role = definitions[node_id].role
        parents = incoming[node_id]
        has_unapproved_path[node_id] = role is not NodeRole.APPROVAL and (
            not parents or any(has_unapproved_path[parent] for parent in parents)
        )
        if role is NodeRole.REPAIR_EFFECT and has_unapproved_path[node_id]:
            raise UnapprovedRepairEffectError(
                "repair effect requires prior approval on every incoming path"
            )

    return RepairSafetySummary(
        tuple(
            node_id
            for node_id in ordered_node_ids
            if definitions[node_id].role is NodeRole.APPROVAL
        ),
        tuple(
            node_id
            for node_id in ordered_node_ids
            if definitions[node_id].role is NodeRole.REPAIR_EFFECT
        ),
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
