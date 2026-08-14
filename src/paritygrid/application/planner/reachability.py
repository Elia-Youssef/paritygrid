"""Dependency-neutral contracts for pipeline graph reachability."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

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
