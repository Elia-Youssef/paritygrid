"""Adversarial graph tests for approval-before-effect validation."""

from __future__ import annotations

from typing import Any, cast

import pytest

from paritygrid.application.planner import (
    PipelineDocument,
    RepairSafetySummary,
    UnapprovedRepairEffectError,
    UnknownNodeKindError,
    validate_repair_safety,
)
from paritygrid.domain.models import NodeId


def _node_id(index: int) -> str:
    return f"nod_step-{index:03d}"


def _document(
    kinds: tuple[str, ...],
    edges: tuple[tuple[int, int], ...],
) -> PipelineDocument:
    nodes: list[dict[str, object]] = []
    for index, kind in enumerate(kinds, start=1):
        nodes.append(
            {
                "configuration": {},
                "configuration_version": 1,
                "connector_id": None,
                "id": _node_id(index),
                "kind": kind,
            }
        )
    edge_values: list[dict[str, object]] = []
    for index, (source, target) in enumerate(edges, start=1):
        edge_values.append(
            {
                "source_node_id": _node_id(source),
                "source_port": f"output-{index:03d}",
                "target_node_id": _node_id(target),
                "target_port": f"input-{index:03d}",
            }
        )
    value: dict[str, object] = {
        "canonical_format_version": 1,
        "edges": edge_values,
        "layout": [],
        "nodes": nodes,
        "resource_policy": {},
        "schema_version": 1,
    }
    return PipelineDocument.from_mapping(value)


def test_valid_repair_chain_requires_approval_before_effect() -> None:
    document = _document(
        (
            "source.csv",
            "reconcile.target",
            "repair.generate",
            "repair.approval",
            "repair.apply",
            "verify.target",
        ),
        ((1, 2), (2, 3), (3, 4), (4, 5), (5, 6)),
    )
    assert validate_repair_safety(document) == RepairSafetySummary(
        (NodeId(_node_id(4)),),
        (NodeId(_node_id(5)),),
    )


def test_pipeline_without_repair_nodes_is_safe() -> None:
    document = _document(("source.csv", "export.parquet"), ((1, 2),))
    assert validate_repair_safety(document) == RepairSafetySummary((), ())


@pytest.mark.parametrize(
    ("kinds", "edges"),
    [
        (("source.csv", "repair.apply"), ((1, 2),)),
        (("repair.apply",), ()),
        (
            ("source.csv", "repair.approval", "repair.apply"),
            ((1, 3), (1, 2)),
        ),
        (
            ("source.csv", "repair.approval", "repair.apply"),
            ((1, 2), (2, 3), (1, 3)),
        ),
    ],
)
def test_bypass_root_off_path_and_mixed_paths_fail_closed(
    kinds: tuple[str, ...],
    edges: tuple[tuple[int, int], ...],
) -> None:
    with pytest.raises(UnapprovedRepairEffectError, match="every incoming path"):
        validate_repair_safety(_document(kinds, edges))


def test_all_branches_may_cross_independent_approvals() -> None:
    document = _document(
        (
            "source.csv",
            "repair.approval",
            "source.jsonl",
            "repair.approval",
            "repair.apply",
        ),
        ((1, 2), (2, 5), (3, 4), (4, 5)),
    )
    assert validate_repair_safety(document) == RepairSafetySummary(
        (NodeId(_node_id(2)), NodeId(_node_id(4))),
        (NodeId(_node_id(5)),),
    )


def test_validator_requires_exact_document_contract() -> None:
    with pytest.raises(TypeError, match="PipelineDocument"):
        validate_repair_safety(cast(Any, {}))


def test_unknown_node_kind_is_never_treated_as_safe() -> None:
    with pytest.raises(UnknownNodeKindError):
        validate_repair_safety(_document(("source.csv", "repair.magic"), ((1, 2),)))
