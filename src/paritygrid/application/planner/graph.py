"""Dependency-neutral contracts for deterministic pipeline graph ordering."""

from __future__ import annotations

import heapq
from collections.abc import Iterator
from dataclasses import dataclass
from typing import cast

from paritygrid.application.planner.documents import PipelineDocument
from paritygrid.domain.models import NodeId

MAX_TOPOLOGICAL_NODES = 256


class PipelineGraphError(ValueError):
    """Base failure for invalid logical pipeline graphs."""


class PipelineCycleError(PipelineGraphError):
    """A pipeline graph contains at least one directed cycle."""


@dataclass(frozen=True, slots=True, repr=False)
class TopologicalOrder:
    """One complete deterministic order of unique pipeline node identities."""

    node_ids: tuple[NodeId, ...]

    def __post_init__(self) -> None:
        node_ids = cast(object, self.node_ids)
        if not isinstance(node_ids, tuple):
            raise TypeError("topological node identities must be a tuple")
        values = cast(tuple[object, ...], node_ids)
        if any(type(value) is not NodeId for value in values):
            raise TypeError("topological node identities contain an invalid value")
        if not values:
            raise PipelineGraphError("topological order requires at least one node")
        if len(values) > MAX_TOPOLOGICAL_NODES:
            raise PipelineGraphError("topological order exceeds the node limit")
        if len(set(values)) != len(values):
            raise PipelineGraphError("topological node identities must be unique")

    def __iter__(self) -> Iterator[NodeId]:
        return iter(self.node_ids)

    def __len__(self) -> int:
        return len(self.node_ids)

    def __repr__(self) -> str:
        return f"TopologicalOrder(nodes={len(self.node_ids)})"


def topological_node_order(document: PipelineDocument) -> TopologicalOrder:
    """Return the deterministic node order or fail when any cycle remains."""
    if type(document) is not PipelineDocument:
        raise TypeError("pipeline document must use PipelineDocument")
    node_ids = tuple(node.node_id for node in document.nodes)
    incoming = dict.fromkeys(node_ids, 0)
    outgoing: dict[NodeId, list[NodeId]] = {node_id: [] for node_id in node_ids}
    for edge in document.edges:
        outgoing[edge.source_node_id].append(edge.target_node_id)
        incoming[edge.target_node_id] += 1

    ready = [(str(node_id), node_id) for node_id in node_ids if incoming[node_id] == 0]
    heapq.heapify(ready)
    ordered: list[NodeId] = []
    while ready:
        _, node_id = heapq.heappop(ready)
        ordered.append(node_id)
        for target_id in outgoing[node_id]:
            incoming[target_id] -= 1
            if incoming[target_id] == 0:
                heapq.heappush(ready, (str(target_id), target_id))
    if len(ordered) != len(node_ids):
        raise PipelineCycleError("pipeline graph contains a directed cycle")
    return TopologicalOrder(tuple(ordered))


def validate_acyclic_graph(document: PipelineDocument) -> None:
    """Reject cyclic documents through the deterministic ordering contract."""
    topological_node_order(document)
