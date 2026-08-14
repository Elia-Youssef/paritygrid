"""Ordering, transition, and restoration tests for the dependency tracker."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, cast

import pytest

from paritygrid.application.execution import (
    DependencyTracker,
    ScheduledNode,
    ScheduledNodeStatus,
    SchedulerInvalidStateError,
    SchedulerState,
    SchedulerStatus,
    SchedulerTransitionError,
    SchedulerUnknownNodeError,
)
from paritygrid.application.planner import (
    ConnectorBindingSnapshot,
    ConnectorCapability,
    ConnectorCapabilitySet,
    ConnectorRequirement,
    ExecutionPlan,
    ExecutionPlanNode,
    NodeRole,
    PlannerRunnerKind,
    ResourcePolicy,
    RetryBehavior,
    fingerprint_execution_plan,
)
from paritygrid.application.ports import ConfigurationDocument
from paritygrid.domain.models import ConnectorId, NodeId
from paritygrid.domain.pipeline import NodeKind, PipelineEdge, PortName


def _id(value: str) -> NodeId:
    return NodeId(f"nod_sched-{value}")


def _plan_node(value: str) -> ExecutionPlanNode:
    return ExecutionPlanNode(
        node_id=_id(value),
        kind=NodeKind("transform.normalize"),
        configuration_version=1,
        configuration=ConfigurationDocument.from_mapping({}),
        connector_id=None,
        role=NodeRole.TRANSFORM,
        connector_requirement=ConnectorRequirement.NONE,
        supported_runners=(PlannerRunnerKind.SEQUENTIAL,),
        retry_behavior=RetryBehavior.NEVER,
        requires_idempotency=False,
    )


def _edge(source: str, target: str, suffix: str = "records") -> PipelineEdge:
    return PipelineEdge(
        _id(source),
        PortName(f"out-{suffix}"),
        _id(target),
        PortName(f"in-{suffix}"),
    )


def _plan(
    nodes: tuple[str, ...],
    edges: tuple[tuple[str, str], ...],
) -> ExecutionPlan:
    return ExecutionPlan(
        nodes=tuple(_plan_node(node) for node in nodes),
        edges=tuple(
            _edge(source, target, str(index)) for index, (source, target) in enumerate(edges)
        ),
        resource_policy=ResourcePolicy(),
        connector_bindings=(),
    )


def _diamond_plan() -> ExecutionPlan:
    return _plan(
        ("a", "b", "c", "d"),
        (("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")),
    )


def test_initial_state_tracks_exact_direct_dependencies() -> None:
    plan = _diamond_plan()
    tracker = DependencyTracker(plan)
    assert repr(tracker) == "DependencyTracker(status='active', nodes=4, completed=0)"
    assert tracker.state.status is SchedulerStatus.ACTIVE
    assert tracker.state.plan_fingerprint == fingerprint_execution_plan(plan)
    assert tracker.state.ready_node_ids == (_id("a"),)
    assert tracker.node(_id("a")).status is ScheduledNodeStatus.READY
    assert tracker.node(_id("b")).remaining_dependency_ids == (_id("a"),)
    assert tracker.node(_id("c")).remaining_dependency_ids == (_id("a"),)
    assert tracker.node(_id("d")).remaining_dependency_ids == (_id("b"), _id("c"))
    assert tracker.next_ready_node() == _plan_node("a")


def test_parallel_edges_collapse_to_one_node_dependency() -> None:
    plan = ExecutionPlan(
        nodes=(_plan_node("a"), _plan_node("b")),
        edges=(_edge("a", "b", "first"), _edge("a", "b", "second")),
        resource_policy=ResourcePolicy(),
        connector_bindings=(),
    )
    tracker = DependencyTracker(plan)
    assert tracker.node(_id("b")).remaining_dependency_ids == (_id("a"),)


def test_tracker_advances_diamond_in_plan_order() -> None:
    tracker = DependencyTracker(_diamond_plan())
    assert tracker.start(_id("a")).active_node_id == _id("a")
    assert tracker.next_ready_node() is None
    state = tracker.succeed(_id("a"))
    assert state.ready_node_ids == (_id("b"), _id("c"))
    assert tracker.next_ready_node() == _plan_node("b")

    tracker.start(_id("b"))
    state = tracker.succeed(_id("b"))
    assert state.ready_node_ids == (_id("c"),)
    assert tracker.node(_id("d")).remaining_dependency_ids == (_id("c"),)

    tracker.start(_id("c"))
    state = tracker.succeed(_id("c"))
    assert state.ready_node_ids == (_id("d"),)
    assert tracker.node(_id("d")).remaining_dependency_ids == ()

    tracker.start(_id("d"))
    state = tracker.succeed(_id("d"))
    assert state.status is SchedulerStatus.SUCCEEDED
    assert state.succeeded_node_ids == (_id("a"), _id("b"), _id("c"), _id("d"))
    assert tracker.next_ready_node() is None


def test_independent_nodes_still_use_exact_plan_order() -> None:
    tracker = DependencyTracker(_plan(("a", "b", "c"), ()))
    assert tracker.state.ready_node_ids == (_id("a"), _id("b"), _id("c"))
    with pytest.raises(SchedulerTransitionError, match="deterministic order"):
        tracker.start(_id("b"))
    tracker.start(_id("a"))
    tracker.succeed(_id("a"))
    assert tracker.next_ready_node() == _plan_node("b")


def test_failure_is_terminal_and_admits_no_later_ready_node() -> None:
    tracker = DependencyTracker(_plan(("a", "b"), ()))
    tracker.start(_id("a"))
    state = tracker.fail(_id("a"))
    assert state.status is SchedulerStatus.FAILED
    assert state.failed_node_id == _id("a")
    assert state.ready_node_ids == (_id("b"),)
    assert tracker.next_ready_node() is None
    with pytest.raises(SchedulerTransitionError, match="terminal"):
        tracker.start(_id("b"))
    with pytest.raises(SchedulerTransitionError, match="terminal"):
        tracker.succeed(_id("a"))


def test_tracker_rejects_unknown_invalid_and_stale_transitions() -> None:
    tracker = DependencyTracker(_plan(("a", "b"), (("a", "b"),)))
    with pytest.raises(TypeError, match="ExecutionPlan"):
        DependencyTracker(cast(Any, {}))
    with pytest.raises(TypeError, match="NodeId"):
        tracker.node(cast(Any, "nod_sched-a"))
    with pytest.raises(SchedulerUnknownNodeError, match="unknown"):
        tracker.node(_id("missing"))
    with pytest.raises(SchedulerTransitionError, match="does not match"):
        tracker.succeed(_id("a"))
    tracker.start(_id("a"))
    with pytest.raises(SchedulerTransitionError, match="already has"):
        tracker.start(_id("a"))
    with pytest.raises(SchedulerTransitionError, match="does not match"):
        tracker.fail(_id("b"))
    tracker.succeed(_id("a"))
    with pytest.raises(SchedulerTransitionError, match="does not match"):
        tracker.fail(_id("a"))


def test_exact_state_can_be_restored_and_continued() -> None:
    first = DependencyTracker(_diamond_plan())
    first.start(_id("a"))
    state = first.succeed(_id("a"))
    restored = DependencyTracker(_diamond_plan(), state=state)
    assert restored.state is state
    restored.start(_id("b"))
    assert restored.state.active_node_id == _id("b")


def test_completed_state_can_be_restored_without_reopening_admission() -> None:
    plan = _plan(("a",), ())
    first = DependencyTracker(plan)
    first.start(_id("a"))
    state = first.succeed(_id("a"))
    restored = DependencyTracker(plan, state=state)
    assert restored.state.status is SchedulerStatus.SUCCEEDED
    assert restored.next_ready_node() is None


def test_restoration_requires_exact_state_and_plan_node_order() -> None:
    plan = _plan(("a", "b"), ())
    tracker = DependencyTracker(plan)
    with pytest.raises(TypeError, match="SchedulerState"):
        DependencyTracker(plan, state=cast(Any, {}))
    reversed_state = replace(tracker.state, nodes=tuple(reversed(tracker.state.nodes)))
    with pytest.raises(SchedulerInvalidStateError, match="execution-plan order"):
        DependencyTracker(plan, state=reversed_state)


def test_restoration_rejects_dependency_and_success_prefix_corruption() -> None:
    plan = _plan(("a", "b", "c"), (("a", "b"),))
    state = DependencyTracker(plan).state
    wrong_dependency = replace(
        state.nodes[1],
        remaining_dependency_ids=(_id("c"),),
    )
    with pytest.raises(SchedulerInvalidStateError, match="do not match"):
        DependencyTracker(
            plan,
            state=replace(
                state,
                nodes=(state.nodes[0], wrong_dependency, state.nodes[2]),
            ),
        )

    corrupt = SchedulerState(
        SchedulerStatus.ACTIVE,
        (
            ScheduledNode(_id("a"), ScheduledNodeStatus.READY, ()),
            ScheduledNode(_id("b"), ScheduledNodeStatus.SUCCEEDED, ()),
            ScheduledNode(_id("c"), ScheduledNodeStatus.READY, ()),
        ),
        fingerprint_execution_plan(plan),
    )
    with pytest.raises(SchedulerInvalidStateError, match="deterministic prefix"):
        DependencyTracker(plan, state=corrupt)


@pytest.mark.parametrize("terminal", [ScheduledNodeStatus.RUNNING, ScheduledNodeStatus.FAILED])
def test_restoration_rejects_active_or_failed_node_beyond_frontier(
    terminal: ScheduledNodeStatus,
) -> None:
    plan = _plan(("a", "b"), ())
    status = (
        SchedulerStatus.ACTIVE
        if terminal is ScheduledNodeStatus.RUNNING
        else SchedulerStatus.FAILED
    )
    state = SchedulerState(
        status,
        (
            ScheduledNode(_id("a"), ScheduledNodeStatus.READY, ()),
            ScheduledNode(_id("b"), terminal, ()),
        ),
        fingerprint_execution_plan(plan),
    )
    message = "running" if terminal is ScheduledNodeStatus.RUNNING else "failed"
    with pytest.raises(SchedulerInvalidStateError, match=message):
        DependencyTracker(plan, state=state)


def test_restoration_rejects_completed_node_with_unsatisfied_dependency() -> None:
    plan = _plan(("a", "b", "c"), (("a", "b"),))
    state = SchedulerState(
        SchedulerStatus.ACTIVE,
        (
            ScheduledNode(_id("a"), ScheduledNodeStatus.READY, ()),
            ScheduledNode(_id("b"), ScheduledNodeStatus.RUNNING, ()),
            ScheduledNode(_id("c"), ScheduledNodeStatus.READY, ()),
        ),
        fingerprint_execution_plan(plan),
    )
    with pytest.raises(SchedulerInvalidStateError, match="dependency completion"):
        DependencyTracker(plan, state=state)


@pytest.mark.parametrize("change", ["configuration", "edge", "resource_policy"])
def test_restoration_rejects_divergent_plan_content(change: str) -> None:
    original = _plan(("a", "b"), (("a", "b"),))
    tracker = DependencyTracker(original)
    tracker.start(_id("a"))
    restored_state = tracker.succeed(_id("a"))
    nodes = original.nodes
    edges = original.edges
    resource_policy = original.resource_policy
    if change == "configuration":
        nodes = (
            replace(
                original.nodes[0],
                configuration=ConfigurationDocument.from_mapping({"revision": 2}),
            ),
            original.nodes[1],
        )
    elif change == "edge":
        edges = (replace(original.edges[0], source_port=PortName("out-changed")),)
    else:
        resource_policy = replace(original.resource_policy, max_concurrency=2)
    changed = ExecutionPlan(
        nodes=nodes,
        edges=edges,
        resource_policy=resource_policy,
        connector_bindings=original.connector_bindings,
    )
    assert fingerprint_execution_plan(changed) != restored_state.plan_fingerprint
    with pytest.raises(SchedulerInvalidStateError, match="plan fingerprint"):
        DependencyTracker(changed, state=restored_state)


def test_restoration_rejects_divergent_connector_snapshot() -> None:
    connector_id = ConnectorId("con_sched-source")
    node = replace(
        _plan_node("a"),
        kind=NodeKind("source.csv"),
        connector_id=connector_id,
        role=NodeRole.SOURCE,
        connector_requirement=ConnectorRequirement.SOURCE,
        retry_behavior=RetryBehavior.CONNECTOR,
    )
    binding = ConnectorBindingSnapshot(
        connector_id=connector_id,
        kind="csv-local",
        revision=1,
        configuration=ConfigurationDocument.from_mapping({"path": "inventory.csv"}),
        capabilities=ConnectorCapabilitySet((ConnectorCapability.READ,)),
        schema_discovery=None,
        secret_references=(),
    )
    original = ExecutionPlan(
        nodes=(node,),
        edges=(),
        resource_policy=ResourcePolicy(),
        connector_bindings=(binding,),
    )
    tracker = DependencyTracker(original)
    tracker.start(_id("a"))
    restored_state = tracker.succeed(_id("a"))
    changed = replace(
        original,
        connector_bindings=(replace(binding, revision=2),),
    )
    assert fingerprint_execution_plan(changed) != restored_state.plan_fingerprint
    with pytest.raises(SchedulerInvalidStateError, match="plan fingerprint"):
        DependencyTracker(changed, state=restored_state)
