"""Dependency-neutral contracts for deterministic pipeline graph ordering."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import cast

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
