"""Dependency-neutral contracts for pipeline graph reachability."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from paritygrid.application.planner.documents import PipelineDocument
from paritygrid.application.planner.graph import topological_node_order
from paritygrid.application.planner.registry import NodeRole, registered_node_definition
from paritygrid.domain.models import NodeId

MAX_REACHABILITY_ENDPOINTS = 256


class PipelineReachabilityError(ValueError):
    """Base failure for invalid source-to-terminal graph reachability."""


class DisconnectedPipelineError(PipelineReachabilityError):
    """A pipeline contains a node outside its connected logical graph."""


class InvalidPipelineTerminalError(PipelineReachabilityError):
    """A pipeline has no allowed terminal path or contains a dead end."""


@dataclass(frozen=True, slots=True, repr=False)
class GraphReachabilitySummary:
    """Canonical source and terminal identities for one valid graph."""

    source_node_ids: tuple[NodeId, ...]
    terminal_node_ids: tuple[NodeId, ...]

    def __post_init__(self) -> None:
        sources = _validate_node_ids(self.source_node_ids, "source")
        terminals = _validate_node_ids(self.terminal_node_ids, "terminal")
        if set(sources) & set(terminals):
            raise PipelineReachabilityError("source and terminal identities must be disjoint")
        object.__setattr__(self, "source_node_ids", tuple(sorted(sources, key=str)))
        object.__setattr__(self, "terminal_node_ids", tuple(sorted(terminals, key=str)))

    def __repr__(self) -> str:
        return (
            "GraphReachabilitySummary("
            f"sources={len(self.source_node_ids)}, terminals={len(self.terminal_node_ids)})"
        )


def _validate_node_ids(value: object, subject: str) -> tuple[NodeId, ...]:
    if not isinstance(value, tuple):
        raise TypeError(f"{subject} node identities must be a tuple")
    values = cast(tuple[object, ...], value)
    if any(type(item) is not NodeId for item in values):
        raise TypeError(f"{subject} node identities contain an invalid value")
    if not values:
        raise PipelineReachabilityError(f"reachability requires at least one {subject} node")
    if len(values) > MAX_REACHABILITY_ENDPOINTS:
        raise PipelineReachabilityError(f"{subject} node identities exceed the limit")
    if len(set(values)) != len(values):
        raise PipelineReachabilityError(f"{subject} node identities must be unique")
    return cast(tuple[NodeId, ...], values)


_TERMINAL_ROLES = frozenset({NodeRole.EXPORT, NodeRole.VERIFICATION})


def validate_graph_reachability(document: PipelineDocument) -> GraphReachabilitySummary:
    """Require one connected acyclic graph from source roots to allowed sinks."""
    if type(document) is not PipelineDocument:
        raise TypeError("pipeline document must use PipelineDocument")
    topological_node_order(document)
    node_ids = tuple(node.node_id for node in document.nodes)
    definitions = {
        node.node_id: registered_node_definition(node.kind, node.configuration_version)
        for node in document.nodes
    }
    outgoing: dict[NodeId, set[NodeId]] = {node_id: set() for node_id in node_ids}
    incoming: dict[NodeId, set[NodeId]] = {node_id: set() for node_id in node_ids}
    adjacent: dict[NodeId, set[NodeId]] = {node_id: set() for node_id in node_ids}
    for edge in document.edges:
        outgoing[edge.source_node_id].add(edge.target_node_id)
        incoming[edge.target_node_id].add(edge.source_node_id)
        adjacent[edge.source_node_id].add(edge.target_node_id)
        adjacent[edge.target_node_id].add(edge.source_node_id)

    sources = tuple(node_id for node_id in node_ids if definitions[node_id].role is NodeRole.SOURCE)
    if not sources:
        raise DisconnectedPipelineError("pipeline graph requires at least one source node")
    if any(incoming[node_id] for node_id in sources):
        raise DisconnectedPipelineError("pipeline source nodes cannot have incoming dependencies")
    roots = tuple(node_id for node_id in node_ids if not incoming[node_id])
    if any(definitions[node_id].role is not NodeRole.SOURCE for node_id in roots):
        raise DisconnectedPipelineError("pipeline graph roots must be source nodes")

    connected = _walk((node_ids[0],), adjacent)
    if len(connected) != len(node_ids):
        raise DisconnectedPipelineError("pipeline graph contains disconnected components")

    sinks = tuple(node_id for node_id in node_ids if not outgoing[node_id])
    if any(definitions[node_id].role not in _TERMINAL_ROLES for node_id in sinks):
        raise InvalidPipelineTerminalError("pipeline graph contains a non-terminal dead end")
    return GraphReachabilitySummary(sources, sinks)


def _walk(starts: tuple[NodeId, ...], adjacency: dict[NodeId, set[NodeId]]) -> set[NodeId]:
    visited: set[NodeId] = set()
    pending = list(reversed(starts))
    while pending:
        node_id = pending.pop()
        if node_id in visited:
            continue
        visited.add(node_id)
        pending.extend(sorted(adjacency[node_id] - visited, key=str, reverse=True))
    return visited
