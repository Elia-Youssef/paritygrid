"""Frozen contract tests for logical execution plans."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from typing import cast

import pytest

from paritygrid.application.planner import (
    EXECUTION_PLAN_VERSION,
    MAX_EXECUTION_PLAN_EDGES,
    MAX_EXECUTION_PLAN_NODES,
    ConnectorBindingSnapshot,
    ConnectorCapability,
    ConnectorCapabilitySet,
    ConnectorRequirement,
    ExecutionPlan,
    ExecutionPlanError,
    ExecutionPlanNode,
    InvalidExecutionPlanError,
    NodeRole,
    PlannerRunnerKind,
    ResourcePolicy,
    RetryBehavior,
)
from paritygrid.application.ports import ConfigurationDocument
from paritygrid.domain.models import ConnectorId, NodeId
from paritygrid.domain.pipeline import NodeKind, PipelineEdge, PortName


def _node(
    index: int,
    *,
    kind: str = "transform.normalize",
    connector_id: ConnectorId | None = None,
    requirement: ConnectorRequirement = ConnectorRequirement.NONE,
    role: NodeRole = NodeRole.TRANSFORM,
) -> ExecutionPlanNode:
    return ExecutionPlanNode(
        node_id=NodeId(f"nod_step-{index:03d}"),
        kind=NodeKind(kind),
        configuration_version=1,
        configuration=ConfigurationDocument.from_mapping({"index": index}),
        connector_id=connector_id,
        role=role,
        connector_requirement=requirement,
        supported_runners=(PlannerRunnerKind.THREADED, PlannerRunnerKind.SEQUENTIAL),
        retry_behavior=RetryBehavior.NEVER,
        requires_idempotency=False,
    )


def _edge(source: int, target: int, suffix: str = "records") -> PipelineEdge:
    return PipelineEdge(
        NodeId(f"nod_step-{source:03d}"),
        PortName(suffix),
        NodeId(f"nod_step-{target:03d}"),
        PortName(suffix),
    )


def _binding(connector_id: ConnectorId) -> ConnectorBindingSnapshot:
    return ConnectorBindingSnapshot(
        connector_id=connector_id,
        kind="csv-local",
        revision=1,
        configuration=ConfigurationDocument.from_mapping({"path": "inventory.csv"}),
        capabilities=ConnectorCapabilitySet((ConnectorCapability.READ,)),
        schema_discovery=None,
        secret_references=(),
    )


def _plan() -> ExecutionPlan:
    connector_id = ConnectorId("con_source-001")
    source = _node(
        1,
        kind="source.csv",
        connector_id=connector_id,
        requirement=ConnectorRequirement.SOURCE,
        role=NodeRole.SOURCE,
    )
    export = _node(2, kind="export.parquet", role=NodeRole.EXPORT)
    return ExecutionPlan(
        (source, export),
        (_edge(1, 2),),
        ResourcePolicy(),
        (_binding(connector_id),),
    )


def test_execution_plan_constants_and_error_family_are_frozen() -> None:
    assert EXECUTION_PLAN_VERSION == 1
    assert MAX_EXECUTION_PLAN_NODES == 256
    assert MAX_EXECUTION_PLAN_EDGES == 4_096
    assert issubclass(InvalidExecutionPlanError, ExecutionPlanError)


def test_node_sorts_runners_and_maps_exact_logical_metadata() -> None:
    node = _node(1)
    assert node.supported_runners == (
        PlannerRunnerKind.SEQUENTIAL,
        PlannerRunnerKind.THREADED,
    )
    assert node.to_mapping() == {
        "configuration": {"index": 1},
        "configuration_version": 1,
        "connector_id": None,
        "connector_requirement": "none",
        "id": "nod_step-001",
        "kind": "transform.normalize",
        "partition_strategy": {"kind": "single", "partition_count": 1, "version": 1},
        "requires_idempotency": False,
        "retry_behavior": "never",
        "role": "transform",
        "supported_runners": ["sequential", "threaded"],
    }
    assert "index" not in repr(node)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("node_id", "nod_step-001", "NodeId"),
        ("kind", "transform.normalize", "NodeKind"),
        ("configuration_version", True, "integer"),
        ("configuration", {}, "ConfigurationDocument"),
        ("connector_id", "con_source-001", "ConnectorId"),
        ("role", "transform", "NodeRole"),
        ("connector_requirement", "none", "ConnectorRequirement"),
        ("supported_runners", [], "tuple"),
        ("supported_runners", ("sequential",), "invalid value"),
        ("retry_behavior", "never", "RetryBehavior"),
        ("requires_idempotency", 1, "boolean"),
        ("partition_strategy", {}, "PartitionStrategy"),
    ],
)
def test_node_requires_exact_public_types(field: str, value: object, message: str) -> None:
    with pytest.raises(TypeError, match=message):
        replace(_node(1), **{field: value})


@pytest.mark.parametrize("version", [0, 2_147_483_648])
def test_node_rejects_configuration_versions_outside_bounds(version: int) -> None:
    with pytest.raises(InvalidExecutionPlanError, match="outside"):
        replace(_node(1), configuration_version=version)


def test_node_requires_one_unique_runner() -> None:
    with pytest.raises(InvalidExecutionPlanError, match="supported runner"):
        replace(_node(1), supported_runners=())
    with pytest.raises(InvalidExecutionPlanError, match="unique"):
        replace(
            _node(1),
            supported_runners=(PlannerRunnerKind.SEQUENTIAL, PlannerRunnerKind.SEQUENTIAL),
        )


def test_node_connector_presence_matches_requirement() -> None:
    with pytest.raises(InvalidExecutionPlanError, match="does not permit"):
        replace(_node(1), connector_id=ConnectorId("con_source-001"))
    with pytest.raises(InvalidExecutionPlanError, match="requires a connector"):
        replace(_node(1), connector_requirement=ConnectorRequirement.SOURCE)


def test_plan_maps_topological_logical_content_without_layout() -> None:
    plan = _plan()
    mapping = plan.to_mapping()
    assert [node["id"] for node in cast(list[dict[str, object]], mapping["nodes"])] == [
        "nod_step-001",
        "nod_step-002",
    ]
    assert "layout" not in mapping
    assert mapping["version"] == 1
    assert repr(plan) == "ExecutionPlan(version=1, nodes=2, edges=1, connector_bindings=1)"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("nodes", [], "tuple"),
        ("nodes", (object(),), "invalid value"),
        ("edges", [], "tuple"),
        ("edges", (object(),), "invalid value"),
        ("resource_policy", {}, "ResourcePolicy"),
        ("connector_bindings", [], "tuple"),
        ("connector_bindings", (object(),), "invalid value"),
        ("version", True, "integer"),
    ],
)
def test_plan_requires_exact_public_types(field: str, value: object, message: str) -> None:
    with pytest.raises(TypeError, match=message):
        replace(_plan(), **{field: value})


def test_plan_rejects_unsupported_version_and_empty_nodes() -> None:
    with pytest.raises(InvalidExecutionPlanError, match="unsupported"):
        replace(_plan(), version=2)
    with pytest.raises(InvalidExecutionPlanError, match="at least one"):
        ExecutionPlan((), (), ResourcePolicy(), ())


def test_plan_enforces_node_and_edge_limits() -> None:
    nodes = tuple(_node(index) for index in range(MAX_EXECUTION_PLAN_NODES + 1))
    with pytest.raises(InvalidExecutionPlanError, match="node limit"):
        ExecutionPlan(nodes, (), ResourcePolicy(), ())
    edge = _edge(1, 2)
    edges = tuple(
        PipelineEdge(
            edge.source_node_id, PortName(f"p-{index}"), edge.target_node_id, edge.target_port
        )
        for index in range(MAX_EXECUTION_PLAN_EDGES + 1)
    )
    with pytest.raises(InvalidExecutionPlanError, match="edge limit"):
        ExecutionPlan((_node(1), _node(2)), edges, ResourcePolicy(), ())


def test_plan_rejects_duplicate_nodes_edges_and_unknown_endpoints() -> None:
    node = _node(1)
    with pytest.raises(InvalidExecutionPlanError, match="node identities"):
        ExecutionPlan((node, node), (), ResourcePolicy(), ())
    edge = _edge(1, 2)
    with pytest.raises(InvalidExecutionPlanError, match="edges must be unique"):
        ExecutionPlan((_node(1), _node(2)), (edge, edge), ResourcePolicy(), ())
    with pytest.raises(InvalidExecutionPlanError, match="unknown node"):
        ExecutionPlan((_node(1),), (edge,), ResourcePolicy(), ())


def test_plan_requires_topological_order() -> None:
    with pytest.raises(InvalidExecutionPlanError, match="topologically"):
        ExecutionPlan((_node(2), _node(1)), (_edge(1, 2),), ResourcePolicy(), ())


def test_plan_bindings_are_unique_exact_and_sorted() -> None:
    connector_id = ConnectorId("con_source-001")
    binding = _binding(connector_id)
    with pytest.raises(InvalidExecutionPlanError, match="unique"):
        replace(_plan(), connector_bindings=(binding, binding))
    with pytest.raises(InvalidExecutionPlanError, match="exactly match"):
        replace(_plan(), connector_bindings=())


def test_plan_is_immutable() -> None:
    plan = _plan()
    with pytest.raises(FrozenInstanceError):
        plan.nodes = ()  # type: ignore[misc]
