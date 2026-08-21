"""Dependency-neutral state and ordering for the sequential scheduler."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from paritygrid.application.planner import (
    MAX_EXECUTION_PLAN_NODES,
    ExecutionPlan,
    ExecutionPlanNode,
    PlanFingerprint,
    fingerprint_execution_plan,
)
from paritygrid.domain.models import NodeId

SCHEDULER_STATE_VERSION = 1
MAX_SCHEDULER_DEPENDENCIES = MAX_EXECUTION_PLAN_NODES - 1


class SchedulerError(ValueError):
    """Base failure for transient scheduler state and transitions."""


class SchedulerInvalidStateError(SchedulerError):
    """A scheduler snapshot is malformed or inconsistent with its plan."""


class SchedulerTransitionError(SchedulerError):
    """The requested scheduler transition is not valid at the current frontier."""


class SchedulerUnknownNodeError(SchedulerTransitionError):
    """The requested node is not part of the scheduler's execution plan."""


class SchedulerDeadlockError(SchedulerInvalidStateError):
    """Active scheduler state has no running or dependency-ready node."""


class SchedulerStatus(StrEnum):
    """Closed transient status of one dependency tracker."""

    ACTIVE = "active"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        """Return whether the tracker admits no later node transition."""
        return self is not SchedulerStatus.ACTIVE


class ScheduledNodeStatus(StrEnum):
    """Closed transient status of one planned node."""

    BLOCKED = "blocked"
    READY = "ready"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        """Return whether the node has a final scheduler outcome."""
        return self in {ScheduledNodeStatus.SUCCEEDED, ScheduledNodeStatus.FAILED}


@dataclass(frozen=True, slots=True, repr=False)
class ScheduledNode:
    """One immutable node status and its direct unsatisfied dependencies."""

    node_id: NodeId
    status: ScheduledNodeStatus
    remaining_dependency_ids: tuple[NodeId, ...]

    def __post_init__(self) -> None:
        _require_exact(self.node_id, NodeId, "scheduled node identity")
        _require_exact(self.status, ScheduledNodeStatus, "scheduled node status")
        remaining = _require_exact_tuple(
            self.remaining_dependency_ids,
            NodeId,
            "remaining scheduler dependencies",
        )
        if len(set(remaining)) != len(remaining):
            raise SchedulerInvalidStateError("remaining scheduler dependencies must be unique")
        if len(remaining) > MAX_SCHEDULER_DEPENDENCIES:
            raise SchedulerInvalidStateError("scheduled node exceeds the dependency limit")
        if self.node_id in remaining:
            raise SchedulerInvalidStateError("a scheduled node cannot depend on itself")
        if self.status is ScheduledNodeStatus.BLOCKED and not remaining:
            raise SchedulerInvalidStateError("a blocked scheduler node requires a dependency")
        if self.status is not ScheduledNodeStatus.BLOCKED and remaining:
            raise SchedulerInvalidStateError(
                "an unblocked scheduler node cannot retain a dependency"
            )
        object.__setattr__(
            self,
            "remaining_dependency_ids",
            tuple(sorted(remaining, key=str)),
        )

    def __repr__(self) -> str:
        return (
            "ScheduledNode("
            f"node_id={self.node_id!r}, status={self.status.value!r}, "
            f"remaining_dependencies={len(self.remaining_dependency_ids)})"
        )


@dataclass(frozen=True, slots=True, repr=False)
class SchedulerState:
    """A complete immutable scheduling frontier for one execution plan."""

    status: SchedulerStatus
    nodes: tuple[ScheduledNode, ...]
    plan_fingerprint: PlanFingerprint
    version: int = SCHEDULER_STATE_VERSION

    def __post_init__(self) -> None:
        _require_exact(self.status, SchedulerStatus, "scheduler status")
        nodes = _require_exact_tuple(self.nodes, ScheduledNode, "scheduled nodes")
        _require_exact(
            self.plan_fingerprint,
            PlanFingerprint,
            "scheduler plan fingerprint",
        )
        version = cast(object, self.version)
        if type(version) is not int:
            raise TypeError("scheduler state version must be an integer")
        if version != SCHEDULER_STATE_VERSION:
            raise SchedulerInvalidStateError("scheduler state version is unsupported")
        if not nodes:
            raise SchedulerInvalidStateError("scheduler state requires at least one node")
        if len(nodes) > MAX_EXECUTION_PLAN_NODES:
            raise SchedulerInvalidStateError("scheduler state exceeds the node limit")
        node_ids = tuple(node.node_id for node in nodes)
        if len(set(node_ids)) != len(node_ids):
            raise SchedulerInvalidStateError("scheduled node identities must be unique")
        known_node_ids = frozenset(node_ids)
        if any(
            dependency_id not in known_node_ids
            for node in nodes
            for dependency_id in node.remaining_dependency_ids
        ):
            raise SchedulerInvalidStateError(
                "scheduler dependency must identify a known scheduled node"
            )

        running = tuple(node for node in nodes if node.status is ScheduledNodeStatus.RUNNING)
        failed = tuple(node for node in nodes if node.status is ScheduledNodeStatus.FAILED)
        if len(running) > 1:
            raise SchedulerInvalidStateError("sequential scheduler permits one running node")
        if self.status is SchedulerStatus.ACTIVE:
            if failed:
                raise SchedulerInvalidStateError("active scheduler state cannot contain failure")
            if all(node.status is ScheduledNodeStatus.SUCCEEDED for node in nodes):
                raise SchedulerInvalidStateError("completed scheduler state must be succeeded")
            if not running and not any(node.status is ScheduledNodeStatus.READY for node in nodes):
                raise SchedulerDeadlockError("active scheduler state has no running or ready node")
        elif self.status is SchedulerStatus.SUCCEEDED:
            if any(node.status is not ScheduledNodeStatus.SUCCEEDED for node in nodes):
                raise SchedulerInvalidStateError(
                    "succeeded scheduler state requires every node to succeed"
                )
        elif len(failed) != 1 or running:
            raise SchedulerInvalidStateError(
                "failed scheduler state requires one failed node and no running node"
            )

    @property
    def active_node_id(self) -> NodeId | None:
        """Return the sole running node identity, when present."""
        return next(
            (node.node_id for node in self.nodes if node.status is ScheduledNodeStatus.RUNNING),
            None,
        )

    @property
    def ready_node_ids(self) -> tuple[NodeId, ...]:
        """Return dependency-ready node identities in deterministic plan order."""
        return tuple(
            node.node_id for node in self.nodes if node.status is ScheduledNodeStatus.READY
        )

    @property
    def succeeded_node_ids(self) -> tuple[NodeId, ...]:
        """Return completed node identities in deterministic plan order."""
        return tuple(
            node.node_id for node in self.nodes if node.status is ScheduledNodeStatus.SUCCEEDED
        )

    @property
    def failed_node_id(self) -> NodeId | None:
        """Return the sole failed node identity, when present."""
        return next(
            (node.node_id for node in self.nodes if node.status is ScheduledNodeStatus.FAILED),
            None,
        )

    def __repr__(self) -> str:
        return (
            "SchedulerState("
            f"version={self.version!r}, status={self.status.value!r}, "
            f"nodes={len(self.nodes)}, ready={len(self.ready_node_ids)}, "
            f"active={self.active_node_id is not None}, plan_fingerprint=<redacted>)"
        )


class DependencyTracker:
    """Advance one execution plan through a strict sequential node frontier."""

    __slots__ = (
        "_dependencies",
        "_nodes_by_id",
        "_order",
        "_plan_fingerprint",
        "_state",
    )

    def __init__(
        self,
        plan: ExecutionPlan,
        *,
        state: SchedulerState | None = None,
    ) -> None:
        if type(plan) is not ExecutionPlan:
            raise TypeError("scheduler plan must use ExecutionPlan")
        if state is not None and type(state) is not SchedulerState:
            raise TypeError("restored scheduler state must use SchedulerState or None")

        self._order = tuple(node.node_id for node in plan.nodes)
        self._nodes_by_id = {node.node_id: node for node in plan.nodes}
        self._plan_fingerprint = fingerprint_execution_plan(plan)
        dependency_sets: dict[NodeId, set[NodeId]] = {node_id: set() for node_id in self._order}
        for edge in plan.edges:
            dependency_sets[edge.target_node_id].add(edge.source_node_id)
        self._dependencies = {
            node_id: tuple(sorted(dependency_sets[node_id], key=str)) for node_id in self._order
        }

        if state is None:
            nodes = tuple(
                ScheduledNode(
                    node_id=node_id,
                    status=(
                        ScheduledNodeStatus.BLOCKED
                        if self._dependencies[node_id]
                        else ScheduledNodeStatus.READY
                    ),
                    remaining_dependency_ids=self._dependencies[node_id],
                )
                for node_id in self._order
            )
            self._state = SchedulerState(
                SchedulerStatus.ACTIVE,
                nodes,
                self._plan_fingerprint,
            )
        else:
            self._validate_restored_state(state)
            self._state = state

    @property
    def state(self) -> SchedulerState:
        """Return the current immutable scheduler state."""
        return self._state

    def node(self, node_id: NodeId) -> ScheduledNode:
        """Return one current node snapshot or reject an unknown identity."""
        self._require_known_node(node_id)
        return next(node for node in self._state.nodes if node.node_id == node_id)

    def next_ready_node(self) -> ExecutionPlanNode | None:
        """Return the next admissible plan node, or none while active or terminal."""
        if self._state.status is not SchedulerStatus.ACTIVE:
            return None
        if self._state.active_node_id is not None:
            return None
        return self._nodes_by_id[self._state.ready_node_ids[0]]

    def start(self, node_id: NodeId) -> SchedulerState:
        """Start exactly the deterministic next node at the sequential frontier."""
        self._require_known_node(node_id)
        if self._state.status is not SchedulerStatus.ACTIVE:
            raise SchedulerTransitionError("terminal scheduler state cannot start a node")
        if self._state.active_node_id is not None:
            raise SchedulerTransitionError("sequential scheduler already has a running node")
        next_node = self.next_ready_node()
        if next_node is None or next_node.node_id != node_id:
            raise SchedulerTransitionError("scheduler node is not next in deterministic order")
        self._state = self._replace_status(node_id, ScheduledNodeStatus.RUNNING)
        return self._state

    def succeed(self, node_id: NodeId) -> SchedulerState:
        """Complete the active node and release each newly satisfied dependency."""
        self._require_active_node(node_id)
        self._state = self._replace_status(node_id, ScheduledNodeStatus.SUCCEEDED)
        return self._state

    def fail(self, node_id: NodeId) -> SchedulerState:
        """Fail the active node and close the scheduler without admitting more work."""
        self._require_active_node(node_id)
        self._state = self._replace_status(node_id, ScheduledNodeStatus.FAILED)
        return self._state

    def pause(self, node_id: NodeId) -> SchedulerState:
        """Return one active sequential node to READY at a proven checkpoint boundary."""
        _require_exact(node_id, NodeId, "paused scheduler node")
        identity = node_id
        if self._state.status is not SchedulerStatus.ACTIVE:
            raise SchedulerTransitionError("terminal scheduler cannot pause a node")
        if self._state.active_node_id != identity:
            raise SchedulerTransitionError("only the active scheduler node can pause")
        self._state = SchedulerState(
            SchedulerStatus.ACTIVE,
            tuple(
                ScheduledNode(
                    node.node_id,
                    (ScheduledNodeStatus.READY if node.node_id == identity else node.status),
                    node.remaining_dependency_ids,
                )
                for node in self._state.nodes
            ),
            self._plan_fingerprint,
        )
        return self._state

    def _replace_status(
        self,
        node_id: NodeId,
        target: ScheduledNodeStatus,
    ) -> SchedulerState:
        statuses = {node.node_id: node.status for node in self._state.nodes}
        statuses[node_id] = target
        nodes: list[ScheduledNode] = []
        for current_id in self._order:
            current_status = statuses[current_id]
            remaining = tuple(
                dependency_id
                for dependency_id in self._dependencies[current_id]
                if statuses[dependency_id] is not ScheduledNodeStatus.SUCCEEDED
            )
            if current_status in {ScheduledNodeStatus.BLOCKED, ScheduledNodeStatus.READY}:
                current_status = (
                    ScheduledNodeStatus.BLOCKED if remaining else ScheduledNodeStatus.READY
                )
            nodes.append(
                ScheduledNode(
                    node_id=current_id,
                    status=current_status,
                    remaining_dependency_ids=(
                        remaining if current_status is ScheduledNodeStatus.BLOCKED else ()
                    ),
                )
            )
        if target is ScheduledNodeStatus.FAILED:
            status = SchedulerStatus.FAILED
        elif all(node.status is ScheduledNodeStatus.SUCCEEDED for node in nodes):
            status = SchedulerStatus.SUCCEEDED
        else:
            status = SchedulerStatus.ACTIVE
        return SchedulerState(
            status=status,
            nodes=tuple(nodes),
            plan_fingerprint=self._plan_fingerprint,
        )

    def _require_known_node(self, node_id: NodeId) -> None:
        if type(node_id) is not NodeId:
            raise TypeError("scheduler node identity must use NodeId")
        if node_id not in self._nodes_by_id:
            raise SchedulerUnknownNodeError("scheduler node is unknown")

    def _require_active_node(self, node_id: NodeId) -> None:
        self._require_known_node(node_id)
        if self._state.status is not SchedulerStatus.ACTIVE:
            raise SchedulerTransitionError("terminal scheduler state cannot complete a node")
        if self._state.active_node_id != node_id:
            raise SchedulerTransitionError("scheduler completion does not match the running node")

    def _validate_restored_state(self, state: SchedulerState) -> None:
        if state.plan_fingerprint != self._plan_fingerprint:
            raise SchedulerInvalidStateError(
                "restored scheduler plan fingerprint does not match execution plan"
            )
        if tuple(node.node_id for node in state.nodes) != self._order:
            raise SchedulerInvalidStateError(
                "restored scheduler nodes must exactly match execution-plan order"
            )
        statuses = {node.node_id: node.status for node in state.nodes}
        seen_frontier = False
        for node in state.nodes:
            if node.status is ScheduledNodeStatus.SUCCEEDED:
                if seen_frontier:
                    raise SchedulerInvalidStateError(
                        "restored scheduler successes must form a deterministic prefix"
                    )
            else:
                seen_frontier = True
            remaining = tuple(
                dependency_id
                for dependency_id in self._dependencies[node.node_id]
                if statuses[dependency_id] is not ScheduledNodeStatus.SUCCEEDED
            )
            expected_status = ScheduledNodeStatus.BLOCKED if remaining else node.status
            if remaining and node.status is not expected_status:
                raise SchedulerInvalidStateError(
                    "restored scheduler node violates dependency completion"
                )
            expected_remaining = tuple(sorted(remaining, key=str))
            if node.remaining_dependency_ids != expected_remaining:
                raise SchedulerInvalidStateError(
                    "restored scheduler dependencies do not match execution plan"
                )

        frontier = len(state.succeeded_node_ids)
        if frontier == len(self._order):
            return
        frontier_status = state.nodes[frontier].status
        if state.active_node_id is not None and frontier_status is not ScheduledNodeStatus.RUNNING:
            raise SchedulerInvalidStateError(
                "restored running node must be the deterministic frontier"
            )
        if state.failed_node_id is not None and frontier_status is not ScheduledNodeStatus.FAILED:
            raise SchedulerInvalidStateError(
                "restored failed node must be the deterministic frontier"
            )

    def __repr__(self) -> str:
        return (
            "DependencyTracker("
            f"status={self._state.status.value!r}, nodes={len(self._order)}, "
            f"completed={len(self._state.succeeded_node_ids)})"
        )


def _require_exact(value: object, expected: type[object], subject: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{subject} must use {expected.__name__}")


def _require_exact_tuple[T](value: object, item_type: type[T], subject: str) -> tuple[T, ...]:
    if type(value) is not tuple:
        raise TypeError(f"{subject} must be a tuple")
    values = cast(tuple[object, ...], value)
    if any(type(item) is not item_type for item in values):
        raise TypeError(f"{subject} contains an invalid value")
    return cast(tuple[T, ...], values)
