"""Disconnected-graph and terminal-path tests for pipeline reachability."""

from __future__ import annotations

from typing import Any, cast

import pytest

from paritygrid.application.planner import (
    DisconnectedPipelineError,
    InvalidPipelineTerminalError,
    PipelineCycleError,
    PipelineDocument,
    validate_graph_reachability,
)


def _node(index: int, kind: str) -> dict[str, object]:
    return {
        "configuration": {},
        "configuration_version": 1,
        "connector_id": None,
        "id": f"nod_reach-{index:03d}",
        "kind": kind,
    }


def _edge(source: int, target: int) -> dict[str, str]:
    return {
        "source_node_id": f"nod_reach-{source:03d}",
        "source_port": f"out-{source}-{target}",
        "target_node_id": f"nod_reach-{target:03d}",
        "target_port": f"in-{source}-{target}",
    }


def _document(
    nodes: list[dict[str, object]],
    edges: list[dict[str, str]],
    *,
    reverse: bool = False,
) -> PipelineDocument:
    node_values = list(nodes)
    edge_values = list(edges)
    if reverse:
        node_values.reverse()
        edge_values.reverse()
    return PipelineDocument.from_mapping(
        {
            "canonical_format_version": 1,
            "edges": edge_values,
            "layout": [],
            "nodes": node_values,
            "resource_policy": {},
            "schema_version": 1,
        }
    )


def test_connected_source_to_export_graph_has_canonical_summary() -> None:
    nodes = [
        _node(0, "source.csv"),
        _node(1, "transform.normalize"),
        _node(2, "transform.validate"),
        _node(3, "export.parquet"),
    ]
    edges = [_edge(0, 1), _edge(1, 2), _edge(2, 3)]
    summary = validate_graph_reachability(_document(nodes, edges))
    assert tuple(str(item) for item in summary.source_node_ids) == ("nod_reach-000",)
    assert tuple(str(item) for item in summary.terminal_node_ids) == ("nod_reach-003",)
    assert summary == validate_graph_reachability(_document(nodes, edges, reverse=True))


def test_connected_repair_graph_may_terminate_at_verification() -> None:
    nodes = [
        _node(0, "source.jsonl"),
        _node(1, "reconcile.target"),
        _node(2, "repair.generate"),
        _node(3, "repair.approval"),
        _node(4, "repair.apply"),
        _node(5, "verify.target"),
    ]
    edges = [_edge(index, index + 1) for index in range(5)]
    summary = validate_graph_reachability(_document(nodes, edges))
    assert tuple(str(item) for item in summary.terminal_node_ids) == ("nod_reach-005",)


def test_multiple_sources_may_converge_into_one_connected_terminal_path() -> None:
    nodes = [
        _node(0, "source.csv"),
        _node(1, "source.jsonl"),
        _node(2, "transform.normalize"),
        _node(3, "export.parquet"),
    ]
    edges = [_edge(0, 2), _edge(1, 2), _edge(2, 3)]
    summary = validate_graph_reachability(_document(nodes, edges))
    assert tuple(str(item) for item in summary.source_node_ids) == (
        "nod_reach-000",
        "nod_reach-001",
    )


def test_connected_diamond_handles_duplicate_traversal_frontiers() -> None:
    nodes = [
        _node(0, "source.csv"),
        _node(1, "transform.normalize"),
        _node(2, "transform.validate"),
        _node(3, "export.parquet"),
    ]
    edges = [_edge(0, 1), _edge(0, 2), _edge(1, 3), _edge(2, 3)]
    summary = validate_graph_reachability(_document(nodes, edges))
    assert tuple(str(item) for item in summary.terminal_node_ids) == ("nod_reach-003",)


def test_disconnected_source_to_terminal_components_are_rejected() -> None:
    nodes = [
        _node(0, "source.csv"),
        _node(1, "export.parquet"),
        _node(2, "source.jsonl"),
        _node(3, "export.parquet"),
    ]
    with pytest.raises(DisconnectedPipelineError, match="disconnected"):
        validate_graph_reachability(_document(nodes, [_edge(0, 1), _edge(2, 3)]))


def test_graph_requires_a_source_and_only_source_roots() -> None:
    with pytest.raises(DisconnectedPipelineError, match="source node"):
        validate_graph_reachability(_document([_node(0, "export.parquet")], []))
    nodes = [
        _node(0, "source.csv"),
        _node(1, "transform.normalize"),
        _node(2, "export.parquet"),
    ]
    with pytest.raises(DisconnectedPipelineError, match="roots"):
        validate_graph_reachability(_document(nodes, [_edge(0, 2), _edge(1, 2)]))


def test_source_nodes_cannot_have_incoming_dependencies() -> None:
    nodes = [
        _node(0, "transform.normalize"),
        _node(1, "source.csv"),
        _node(2, "export.parquet"),
    ]
    with pytest.raises(DisconnectedPipelineError, match="source nodes cannot"):
        validate_graph_reachability(_document(nodes, [_edge(0, 1), _edge(1, 2)]))


def test_unreachable_node_and_non_terminal_dead_end_are_rejected() -> None:
    unreachable_nodes = [
        _node(0, "source.csv"),
        _node(1, "transform.normalize"),
        _node(2, "export.parquet"),
    ]
    with pytest.raises(DisconnectedPipelineError, match="roots"):
        validate_graph_reachability(_document(unreachable_nodes, [_edge(0, 2), _edge(1, 2)]))
    dead_end_nodes = [_node(0, "source.csv"), _node(1, "transform.normalize")]
    with pytest.raises(InvalidPipelineTerminalError, match="dead end"):
        validate_graph_reachability(_document(dead_end_nodes, [_edge(0, 1)]))


def test_cycle_failure_precedes_reachability_analysis() -> None:
    nodes = [_node(0, "source.csv"), _node(1, "export.parquet")]
    with pytest.raises(PipelineCycleError):
        validate_graph_reachability(_document(nodes, [_edge(0, 1), _edge(1, 0)]))


def test_reachability_validator_requires_exact_document() -> None:
    with pytest.raises(TypeError, match="PipelineDocument"):
        validate_graph_reachability(cast(Any, {}))
