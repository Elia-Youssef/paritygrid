"""Generated and adversarial tests for deterministic DAG validation."""

from __future__ import annotations

from typing import Any, cast

import pytest
from hypothesis import given
from hypothesis import strategies as st

from paritygrid.application.planner import (
    PipelineCycleError,
    PipelineDocument,
    topological_node_order,
    validate_acyclic_graph,
)


def _node(index: int) -> dict[str, object]:
    return {
        "configuration": {},
        "configuration_version": 1,
        "connector_id": None,
        "id": f"nod_graph-{index:03d}",
        "kind": "source.csv",
    }


def _edge(source: int, target: int, sequence: int = 0) -> dict[str, str]:
    return {
        "source_node_id": f"nod_graph-{source:03d}",
        "source_port": f"out-{source}-{target}-{sequence}",
        "target_node_id": f"nod_graph-{target:03d}",
        "target_port": f"in-{source}-{target}-{sequence}",
    }


def _document(
    size: int,
    edges: list[dict[str, str]],
    *,
    reverse: bool = False,
) -> PipelineDocument:
    nodes = [_node(index) for index in range(size)]
    values = list(edges)
    if reverse:
        nodes.reverse()
        values.reverse()
    return PipelineDocument.from_mapping(
        {
            "canonical_format_version": 1,
            "edges": values,
            "layout": [],
            "nodes": nodes,
            "resource_policy": {},
            "schema_version": 1,
        }
    )


@st.composite
def _dag_cases(draw: st.DrawFn) -> tuple[int, frozenset[tuple[int, int]]]:
    size = draw(st.integers(min_value=1, max_value=8))
    candidates = [(source, target) for source in range(size) for target in range(source + 1, size)]
    if not candidates:
        return size, frozenset()
    edges = draw(st.frozensets(st.sampled_from(candidates), max_size=len(candidates)))
    return size, edges


@given(_dag_cases())
def test_generated_dags_have_complete_stable_topological_orders(
    case: tuple[int, frozenset[tuple[int, int]]],
) -> None:
    size, pairs = case
    edges = [_edge(source, target) for source, target in pairs]
    first = topological_node_order(_document(size, edges))
    second = topological_node_order(_document(size, edges, reverse=True))
    assert first == second
    assert len(first) == size
    positions = {str(node_id): index for index, node_id in enumerate(first)}
    for source, target in pairs:
        assert positions[f"nod_graph-{source:03d}"] < positions[f"nod_graph-{target:03d}"]


@given(st.integers(min_value=1, max_value=12))
def test_generated_directed_rings_are_rejected(size: int) -> None:
    edges = [_edge(index, (index + 1) % size) for index in range(size)]
    with pytest.raises(PipelineCycleError, match="directed cycle"):
        topological_node_order(_document(size, edges))


def test_independent_nodes_use_stable_identity_order() -> None:
    document = _document(4, [])
    order = topological_node_order(document)
    assert tuple(str(node_id) for node_id in order) == (
        "nod_graph-000",
        "nod_graph-001",
        "nod_graph-002",
        "nod_graph-003",
    )
    validate_acyclic_graph(document)


def test_parallel_dependencies_are_counted_without_false_cycles() -> None:
    document = _document(2, [_edge(0, 1, 0), _edge(0, 1, 1)])
    assert tuple(str(node_id) for node_id in topological_node_order(document)) == (
        "nod_graph-000",
        "nod_graph-001",
    )


def test_cycle_validator_rejects_self_loop_and_partial_cycle() -> None:
    with pytest.raises(PipelineCycleError):
        validate_acyclic_graph(_document(1, [_edge(0, 0)]))
    with pytest.raises(PipelineCycleError):
        validate_acyclic_graph(_document(4, [_edge(0, 1), _edge(1, 2), _edge(2, 1)]))


def test_cycle_validator_requires_exact_document_contract() -> None:
    with pytest.raises(TypeError, match="PipelineDocument"):
        topological_node_order(cast(Any, {}))
    with pytest.raises(TypeError, match="PipelineDocument"):
        validate_acyclic_graph(cast(Any, {}))
