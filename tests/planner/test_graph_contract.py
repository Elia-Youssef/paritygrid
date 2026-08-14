"""Frozen contracts for deterministic pipeline graph ordering."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.application.planner import (
    MAX_TOPOLOGICAL_NODES,
    PipelineGraphError,
    TopologicalOrder,
)
from paritygrid.application.planner import graph as contract
from paritygrid.domain.models import NodeId


def _node(index: int) -> NodeId:
    return NodeId(f"nod_graph-{index:03d}")


def test_graph_contract_is_dependency_neutral_and_bounded() -> None:
    assert MAX_TOPOLOGICAL_NODES == 256
    source = Path(contract.__file__).read_text(encoding="utf-8")
    assert "sqlalchemy" not in source
    assert "fastapi" not in source
    assert "pydantic" not in source


def test_topological_order_is_exact_immutable_iterable_and_redacted() -> None:
    order = TopologicalOrder((_node(0), _node(1)))
    assert tuple(order) == (_node(0), _node(1))
    assert len(order) == 2
    assert repr(order) == "TopologicalOrder(nodes=2)"
    with pytest.raises(FrozenInstanceError):
        order.node_ids = ()  # type: ignore[misc]


@pytest.mark.parametrize(
    ("node_ids", "error", "message"),
    [
        (cast(Any, []), TypeError, "tuple"),
        (cast(Any, ("nod_graph-000",)), TypeError, "invalid"),
        ((), PipelineGraphError, "at least one"),
        ((_node(0), _node(0)), PipelineGraphError, "unique"),
        (
            tuple(_node(index) for index in range(MAX_TOPOLOGICAL_NODES + 1)),
            PipelineGraphError,
            "node limit",
        ),
    ],
)
def test_topological_order_rejects_invalid_values(
    node_ids: tuple[NodeId, ...],
    error: type[Exception],
    message: str,
) -> None:
    with pytest.raises(error, match=message):
        TopologicalOrder(node_ids)
