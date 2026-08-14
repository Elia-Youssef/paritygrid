"""Frozen contract tests for approval-before-effect safety."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Any, cast

import pytest

from paritygrid.application.planner import (
    MAX_REPAIR_SAFETY_NODES,
    RepairSafetyError,
    RepairSafetySummary,
    UnapprovedRepairEffectError,
)
from paritygrid.domain.models import NodeId


def _node(index: int, role: str) -> NodeId:
    return NodeId(f"nod_{role}-{index:03d}")


def test_repair_safety_error_family_and_frozen_limit() -> None:
    assert issubclass(UnapprovedRepairEffectError, RepairSafetyError)
    assert MAX_REPAIR_SAFETY_NODES == 256


def test_summary_sorts_exact_unique_node_identities() -> None:
    summary = RepairSafetySummary(
        (_node(2, "approval"), _node(1, "approval")),
        (_node(2, "effect"), _node(1, "effect")),
    )
    assert summary.approval_node_ids == (
        _node(1, "approval"),
        _node(2, "approval"),
    )
    assert summary.repair_effect_node_ids == (
        _node(1, "effect"),
        _node(2, "effect"),
    )
    assert repr(summary) == "RepairSafetySummary(approvals=2, effects=2)"


def test_summary_allows_pipeline_without_repair_nodes() -> None:
    assert RepairSafetySummary((), ()) == RepairSafetySummary((), ())


@pytest.mark.parametrize("field", ["approval_node_ids", "repair_effect_node_ids"])
def test_summary_requires_exact_tuples(field: str) -> None:
    values: dict[str, object] = {
        "approval_node_ids": (),
        "repair_effect_node_ids": (),
    }
    values[field] = []
    with pytest.raises(TypeError, match="tuple"):
        RepairSafetySummary(**cast(Any, values))


@pytest.mark.parametrize("field", ["approval_node_ids", "repair_effect_node_ids"])
def test_summary_requires_exact_node_id_values(field: str) -> None:
    values: dict[str, object] = {
        "approval_node_ids": (),
        "repair_effect_node_ids": (),
    }
    values[field] = ("nod_wrong-001",)
    with pytest.raises(TypeError, match="invalid value"):
        RepairSafetySummary(**cast(Any, values))


@pytest.mark.parametrize("field", ["approval_node_ids", "repair_effect_node_ids"])
def test_summary_rejects_duplicate_identities(field: str) -> None:
    node_id = _node(1, "approval" if field == "approval_node_ids" else "effect")
    values: dict[str, object] = {
        "approval_node_ids": (),
        "repair_effect_node_ids": (),
    }
    values[field] = (node_id, node_id)
    with pytest.raises(RepairSafetyError, match="unique"):
        RepairSafetySummary(**cast(Any, values))


def test_summary_rejects_overlapping_roles() -> None:
    node_id = _node(1, "shared")
    with pytest.raises(RepairSafetyError, match="disjoint"):
        RepairSafetySummary((node_id,), (node_id,))


def test_summary_rejects_more_than_one_document_of_nodes() -> None:
    approvals = tuple(_node(index, "approval") for index in range(MAX_REPAIR_SAFETY_NODES))
    with pytest.raises(RepairSafetyError, match="node limit"):
        RepairSafetySummary(approvals, (_node(1, "effect"),))


def test_summary_is_immutable() -> None:
    summary = RepairSafetySummary((), ())
    with pytest.raises(FrozenInstanceError):
        summary.approval_node_ids = (_node(1, "approval"),)  # type: ignore[misc]
