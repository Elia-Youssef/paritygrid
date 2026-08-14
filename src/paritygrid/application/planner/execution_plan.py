"""Dependency-neutral contracts for deterministic logical execution plans."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from paritygrid.application.planner.connectors import (
    ConnectorBindingSnapshot,
    ConnectorCapability,
    ConnectorValidationError,
    InvalidConnectorSnapshotError,
    MissingConnectorCapabilityError,
    MissingConnectorError,
    required_connector_capabilities,
)
from paritygrid.application.planner.graph import topological_node_order, validate_acyclic_graph
from paritygrid.application.planner.port_validation import validate_typed_ports
from paritygrid.application.planner.publication import PublishedPipelineSpecification
from paritygrid.application.planner.reachability import validate_graph_reachability
from paritygrid.application.planner.registry import (
    ConnectorRequirement,
    NodeRole,
    PlannerRunnerKind,
    RetryBehavior,
    registered_node_definition,
    validate_registered_nodes,
)
from paritygrid.application.planner.repair_safety import validate_repair_safety
from paritygrid.application.planner.resources import ResourcePolicy, validate_resource_policy
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.domain.models import ConnectorId, NodeId
from paritygrid.domain.pipeline import NodeKind, PipelineEdge

EXECUTION_PLAN_VERSION = 1
MAX_EXECUTION_PLAN_NODES = 256
MAX_EXECUTION_PLAN_EDGES = 4_096


class ExecutionPlanError(ValueError):
    """Base failure for an invalid or unbuildable logical plan."""


class InvalidExecutionPlanError(ExecutionPlanError):
    """A logical plan violates the frozen structural contract."""


@dataclass(frozen=True, slots=True, repr=False)
class ExecutionPlanNode:
    """One logical node with frozen runner and connector semantics."""

    node_id: NodeId
    kind: NodeKind
    configuration_version: int
    configuration: ConfigurationDocument
    connector_id: ConnectorId | None
    role: NodeRole
    connector_requirement: ConnectorRequirement
    supported_runners: tuple[PlannerRunnerKind, ...]
    retry_behavior: RetryBehavior
    requires_idempotency: bool

    def __post_init__(self) -> None:
        _require_exact(self.node_id, NodeId, "execution-plan node identity")
        _require_exact(self.kind, NodeKind, "execution-plan node kind")
        version = cast(object, self.configuration_version)
        if type(version) is not int:
            raise TypeError("execution-plan node configuration version must be an integer")
        if not 1 <= version <= 2_147_483_647:
            raise InvalidExecutionPlanError(
                "execution-plan node configuration version is outside the supported range"
            )
        _require_exact(
            self.configuration,
            ConfigurationDocument,
            "execution-plan node configuration",
        )
        connector = cast(object, self.connector_id)
        if connector is not None and type(connector) is not ConnectorId:
            raise TypeError("execution-plan node connector must use ConnectorId or None")
        _require_exact(self.role, NodeRole, "execution-plan node role")
        _require_exact(
            self.connector_requirement,
            ConnectorRequirement,
            "execution-plan node connector requirement",
        )
        runners = _require_exact_tuple(
            self.supported_runners,
            PlannerRunnerKind,
            "execution-plan node supported runners",
        )
        if not runners:
            raise InvalidExecutionPlanError("execution-plan node requires a supported runner")
        if len(set(runners)) != len(runners):
            raise InvalidExecutionPlanError("execution-plan node runners must be unique")
        _require_exact(self.retry_behavior, RetryBehavior, "execution-plan node retry behavior")
        if type(self.requires_idempotency) is not bool:
            raise TypeError("execution-plan node idempotency marker must be boolean")
        if self.connector_requirement is ConnectorRequirement.NONE and connector is not None:
            raise InvalidExecutionPlanError("execution-plan node does not permit a connector")
        if self.connector_requirement is not ConnectorRequirement.NONE and connector is None:
            raise InvalidExecutionPlanError("execution-plan node requires a connector")
        object.__setattr__(
            self,
            "supported_runners",
            tuple(sorted(runners, key=lambda runner: runner.value)),
        )

    def to_mapping(self) -> dict[str, object]:
        """Return the exact logical node representation."""
        return {
            "configuration": self.configuration.to_mapping(),
            "configuration_version": self.configuration_version,
            "connector_id": None if self.connector_id is None else str(self.connector_id),
            "connector_requirement": self.connector_requirement.value,
            "id": str(self.node_id),
            "kind": str(self.kind),
            "requires_idempotency": self.requires_idempotency,
            "retry_behavior": self.retry_behavior.value,
            "role": self.role.value,
            "supported_runners": [runner.value for runner in self.supported_runners],
        }

    def __repr__(self) -> str:
        return (
            "ExecutionPlanNode("
            f"node_id={self.node_id!r}, kind={self.kind!r}, "
            f"role={self.role.value!r}, configuration=<redacted>)"
        )


@dataclass(frozen=True, slots=True, repr=False)
class ExecutionPlan:
    """A complete topologically ordered logical plan without visual layout."""

    nodes: tuple[ExecutionPlanNode, ...]
    edges: tuple[PipelineEdge, ...]
    resource_policy: ResourcePolicy
    connector_bindings: tuple[ConnectorBindingSnapshot, ...]
    version: int = EXECUTION_PLAN_VERSION

    def __post_init__(self) -> None:
        nodes = _require_exact_tuple(self.nodes, ExecutionPlanNode, "execution-plan nodes")
        edges = _require_exact_tuple(self.edges, PipelineEdge, "execution-plan edges")
        bindings = _require_exact_tuple(
            self.connector_bindings,
            ConnectorBindingSnapshot,
            "execution-plan connector bindings",
        )
        _require_exact(self.resource_policy, ResourcePolicy, "execution-plan resource policy")
        version = cast(object, self.version)
        if type(version) is not int:
            raise TypeError("execution-plan version must be an integer")
        if version != EXECUTION_PLAN_VERSION:
            raise InvalidExecutionPlanError("execution-plan version is unsupported")
        if not nodes:
            raise InvalidExecutionPlanError("execution plan requires at least one node")
        if len(nodes) > MAX_EXECUTION_PLAN_NODES:
            raise InvalidExecutionPlanError("execution plan exceeds the node limit")
        if len(edges) > MAX_EXECUTION_PLAN_EDGES:
            raise InvalidExecutionPlanError("execution plan exceeds the edge limit")
        node_ids = tuple(node.node_id for node in nodes)
        if len(set(node_ids)) != len(node_ids):
            raise InvalidExecutionPlanError("execution-plan node identities must be unique")
        if len(set(edges)) != len(edges):
            raise InvalidExecutionPlanError("execution-plan edges must be unique")
        known_nodes = frozenset(node_ids)
        if any(
            edge.source_node_id not in known_nodes or edge.target_node_id not in known_nodes
            for edge in edges
        ):
            raise InvalidExecutionPlanError("execution-plan edge references an unknown node")
        positions = {node_id: index for index, node_id in enumerate(node_ids)}
        if any(positions[edge.source_node_id] >= positions[edge.target_node_id] for edge in edges):
            raise InvalidExecutionPlanError("execution-plan nodes are not topologically ordered")
        binding_ids = tuple(binding.connector_id for binding in bindings)
        if len(set(binding_ids)) != len(binding_ids):
            raise InvalidExecutionPlanError("execution-plan connector bindings must be unique")
        referenced_ids = frozenset(
            node.connector_id for node in nodes if node.connector_id is not None
        )
        if frozenset(binding_ids) != referenced_ids:
            raise InvalidExecutionPlanError(
                "execution-plan connector bindings must exactly match node references"
            )
        object.__setattr__(self, "edges", tuple(sorted(edges, key=_edge_key)))
        object.__setattr__(
            self,
            "connector_bindings",
            tuple(sorted(bindings, key=lambda binding: str(binding.connector_id))),
        )

    def to_mapping(self) -> dict[str, object]:
        """Return the exact logical plan representation, excluding visual layout."""
        return {
            "connector_bindings": [binding.to_mapping() for binding in self.connector_bindings],
            "edges": [edge.to_primitive() for edge in self.edges],
            "nodes": [node.to_mapping() for node in self.nodes],
            "resource_policy": self.resource_policy.to_mapping(),
            "version": self.version,
        }

    def __repr__(self) -> str:
        return (
            "ExecutionPlan("
            f"version={self.version!r}, nodes={len(self.nodes)}, "
            f"edges={len(self.edges)}, connector_bindings={len(self.connector_bindings)})"
        )


def compile_execution_plan(specification: PublishedPipelineSpecification) -> ExecutionPlan:
    """Compile one immutable publication into a deterministic logical plan."""
    if type(specification) is not PublishedPipelineSpecification:
        raise TypeError("published specification must use PublishedPipelineSpecification")
    pipeline = specification.pipeline
    validate_registered_nodes(pipeline)
    validate_typed_ports(pipeline)
    validate_acyclic_graph(pipeline)
    validate_graph_reachability(pipeline)
    resource_policy = validate_resource_policy(pipeline)
    validate_repair_safety(pipeline)
    _validate_embedded_connector_capabilities(specification)

    nodes_by_id = {node.node_id: node for node in pipeline.nodes}
    plan_nodes: list[ExecutionPlanNode] = []
    for node_id in topological_node_order(pipeline):
        node = nodes_by_id[node_id]
        definition = registered_node_definition(node.kind, node.configuration_version)
        plan_nodes.append(
            ExecutionPlanNode(
                node_id=node.node_id,
                kind=node.kind,
                configuration_version=node.configuration_version,
                configuration=node.configuration,
                connector_id=node.connector_id,
                role=definition.role,
                connector_requirement=definition.connector_requirement,
                supported_runners=definition.supported_runners,
                retry_behavior=definition.retry_behavior,
                requires_idempotency=definition.requires_idempotency,
            )
        )
    return ExecutionPlan(
        nodes=tuple(plan_nodes),
        edges=pipeline.edges,
        resource_policy=resource_policy,
        connector_bindings=specification.connector_bindings,
    )


def _validate_embedded_connector_capabilities(
    specification: PublishedPipelineSpecification,
) -> None:
    bindings = {binding.connector_id: binding for binding in specification.connector_bindings}
    for binding in specification.connector_bindings:
        if binding.schema_discovery is not None and not binding.capabilities.supports(
            ConnectorCapability.SCHEMA_DISCOVERY
        ):
            raise InvalidConnectorSnapshotError(
                "connector schema metadata requires schema discovery capability"
            )
    for node in specification.pipeline.nodes:
        definition = registered_node_definition(node.kind, node.configuration_version)
        if definition.connector_requirement is ConnectorRequirement.NONE:
            if node.connector_id is not None:
                raise ConnectorValidationError("pipeline node does not permit a connector binding")
            continue
        if node.connector_id is None:
            raise MissingConnectorError("pipeline node requires a connector binding")
        binding = bindings[node.connector_id]
        required = required_connector_capabilities(node.kind)
        if any(not binding.capabilities.supports(capability) for capability in required):
            raise MissingConnectorCapabilityError("pipeline connector lacks a required capability")


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


def _edge_key(edge: PipelineEdge) -> tuple[str, str, str, str]:
    return (
        str(edge.source_node_id),
        str(edge.source_port),
        str(edge.target_node_id),
        str(edge.target_port),
    )
