"""Fail-closed validation for registered typed pipeline ports."""

from __future__ import annotations

from collections import Counter

from paritygrid.application.planner.documents import PipelineDocument
from paritygrid.application.planner.ports import (
    InputPortDefinition,
    InvalidPortConnectionError,
    OutputPortDefinition,
)
from paritygrid.application.planner.registry import registered_node_definition
from paritygrid.domain.models import NodeId
from paritygrid.domain.pipeline import PortName


def ports_are_compatible(
    source: OutputPortDefinition,
    target: InputPortDefinition,
) -> bool:
    """Return exact closed-type compatibility for one output and input."""
    if type(source) is not OutputPortDefinition:
        raise TypeError("source port must use OutputPortDefinition")
    if type(target) is not InputPortDefinition:
        raise TypeError("target port must use InputPortDefinition")
    return target.accepts(source.value_type)


def validate_typed_ports(document: PipelineDocument) -> None:
    """Validate declarations, types, fan-in, and required inputs for one draft."""
    if type(document) is not PipelineDocument:
        raise TypeError("pipeline document must use PipelineDocument")
    definitions = {
        node.node_id: registered_node_definition(node.kind, node.configuration_version)
        for node in document.nodes
    }
    incoming: Counter[tuple[NodeId, PortName]] = Counter()
    for edge in document.edges:
        source_schema = definitions[edge.source_node_id].port_schema
        target_schema = definitions[edge.target_node_id].port_schema
        source = source_schema.output(edge.source_port)
        if source is None:
            raise InvalidPortConnectionError("pipeline edge source port is not declared")
        target = target_schema.input(edge.target_port)
        if target is None:
            raise InvalidPortConnectionError("pipeline edge target port is not declared")
        if not ports_are_compatible(source, target):
            raise InvalidPortConnectionError("pipeline edge port types are incompatible")
        key = (edge.target_node_id, edge.target_port)
        incoming[key] += 1
        if incoming[key] > target.maximum_connections:
            raise InvalidPortConnectionError("pipeline input exceeds its connection limit")

    missing = tuple(
        (node.node_id, port.name)
        for node in document.nodes
        for port in definitions[node.node_id].port_schema.inputs
        if port.required and incoming[(node.node_id, port.name)] == 0
    )
    if missing:
        raise InvalidPortConnectionError("pipeline node is missing a required input connection")
