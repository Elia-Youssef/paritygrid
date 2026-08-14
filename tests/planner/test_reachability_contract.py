"""Frozen contracts for pipeline reachability results and failures."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.application.planner import (
    MAX_REACHABILITY_ENDPOINTS,
    GraphReachabilitySummary,
    PipelineReachabilityError,
)
from paritygrid.application.planner import reachability as contract
from paritygrid.domain.models import NodeId


def _node(index: int, prefix: str = "endpoint") -> NodeId:
    return NodeId(f"nod_{prefix}-{index:03d}")


def test_reachability_contract_is_dependency_neutral_and_bounded() -> None:
    assert MAX_REACHABILITY_ENDPOINTS == 256
    source = Path(contract.__file__).read_text(encoding="utf-8")
    assert "sqlalchemy" not in source
    assert "fastapi" not in source
    assert "pydantic" not in source


def test_reachability_summary_is_canonical_immutable_and_redacted() -> None:
    summary = GraphReachabilitySummary(
        (_node(1, "source"), _node(0, "source")),
        (_node(1, "terminal"), _node(0, "terminal")),
    )
    assert summary.source_node_ids == (_node(0, "source"), _node(1, "source"))
    assert summary.terminal_node_ids == (_node(0, "terminal"), _node(1, "terminal"))
    assert repr(summary) == "GraphReachabilitySummary(sources=2, terminals=2)"
    with pytest.raises(FrozenInstanceError):
        summary.source_node_ids = ()  # type: ignore[misc]


@pytest.mark.parametrize(
    ("sources", "terminals", "error", "message"),
    [
        (cast(Any, []), (_node(0, "terminal"),), TypeError, "source.*tuple"),
        (cast(Any, ("nod_source-000",)), (_node(0, "terminal"),), TypeError, "source.*invalid"),
        ((), (_node(0, "terminal"),), PipelineReachabilityError, "source node"),
        (
            tuple(_node(index, "source") for index in range(MAX_REACHABILITY_ENDPOINTS + 1)),
            (_node(0, "terminal"),),
            PipelineReachabilityError,
            "source.*limit",
        ),
        (
            (_node(0, "source"), _node(0, "source")),
            (_node(0, "terminal"),),
            PipelineReachabilityError,
            "source.*unique",
        ),
        ((_node(0, "source"),), cast(Any, []), TypeError, "terminal.*tuple"),
        ((_node(0, "source"),), cast(Any, (object(),)), TypeError, "terminal.*invalid"),
        ((_node(0, "source"),), (), PipelineReachabilityError, "terminal node"),
        (
            (_node(0, "source"),),
            tuple(_node(index, "terminal") for index in range(MAX_REACHABILITY_ENDPOINTS + 1)),
            PipelineReachabilityError,
            "terminal.*limit",
        ),
        (
            (_node(0, "source"),),
            (_node(0, "terminal"), _node(0, "terminal")),
            PipelineReachabilityError,
            "terminal.*unique",
        ),
        (
            (_node(0),),
            (_node(0),),
            PipelineReachabilityError,
            "disjoint",
        ),
    ],
)
def test_reachability_summary_rejects_invalid_endpoints(
    sources: tuple[NodeId, ...],
    terminals: tuple[NodeId, ...],
    error: type[Exception],
    message: str,
) -> None:
    with pytest.raises(error, match=message):
        GraphReachabilitySummary(sources, terminals)
