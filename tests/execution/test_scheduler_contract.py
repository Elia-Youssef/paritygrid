"""Frozen contracts for dependency-neutral sequential scheduler state."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.application.execution import (
    MAX_SCHEDULER_DEPENDENCIES,
    SCHEDULER_STATE_VERSION,
    ScheduledNode,
    ScheduledNodeStatus,
    SchedulerDeadlockError,
    SchedulerError,
    SchedulerInvalidStateError,
    SchedulerState,
    SchedulerStatus,
    SchedulerTransitionError,
    SchedulerUnknownNodeError,
)
from paritygrid.application.execution import scheduler as contract
from paritygrid.application.planner import MAX_EXECUTION_PLAN_NODES, PlanFingerprint
from paritygrid.domain.models import NodeId


def _id(value: str) -> NodeId:
    return NodeId(f"nod_sched-{value}")


def _fingerprint() -> PlanFingerprint:
    return PlanFingerprint("a" * 64)


def _node(
    value: str,
    status: ScheduledNodeStatus,
    remaining: tuple[str, ...] = (),
) -> ScheduledNode:
    return ScheduledNode(_id(value), status, tuple(_id(item) for item in remaining))


def test_scheduler_contract_is_dependency_neutral_versioned_and_closed() -> None:
    assert SCHEDULER_STATE_VERSION == 1
    assert MAX_SCHEDULER_DEPENDENCIES == 255
    source = Path(contract.__file__).read_text(encoding="utf-8").lower()
    for forbidden in ("sqlalchemy", "sqlite", "fastapi", "pydantic", "duckdb"):
        assert forbidden not in source
    assert issubclass(SchedulerInvalidStateError, SchedulerError)
    assert issubclass(SchedulerTransitionError, SchedulerError)
    assert issubclass(SchedulerUnknownNodeError, SchedulerTransitionError)
    assert issubclass(SchedulerDeadlockError, SchedulerInvalidStateError)
    assert set(SchedulerStatus) == {
        SchedulerStatus.ACTIVE,
        SchedulerStatus.SUCCEEDED,
        SchedulerStatus.FAILED,
    }
    assert set(ScheduledNodeStatus) == {
        ScheduledNodeStatus.BLOCKED,
        ScheduledNodeStatus.READY,
        ScheduledNodeStatus.RUNNING,
        ScheduledNodeStatus.SUCCEEDED,
        ScheduledNodeStatus.FAILED,
    }


def test_status_terminal_classification_is_exhaustive() -> None:
    assert not SchedulerStatus.ACTIVE.is_terminal
    assert SchedulerStatus.SUCCEEDED.is_terminal
    assert SchedulerStatus.FAILED.is_terminal
    assert not ScheduledNodeStatus.BLOCKED.is_terminal
    assert not ScheduledNodeStatus.READY.is_terminal
    assert not ScheduledNodeStatus.RUNNING.is_terminal
    assert ScheduledNodeStatus.SUCCEEDED.is_terminal
    assert ScheduledNodeStatus.FAILED.is_terminal


def test_scheduled_node_is_exact_immutable_sorted_and_bounded_in_repr() -> None:
    node = ScheduledNode(
        _id("c"),
        ScheduledNodeStatus.BLOCKED,
        (_id("b"), _id("a")),
    )
    assert node.remaining_dependency_ids == (_id("a"), _id("b"))
    assert repr(node) == (
        "ScheduledNode(node_id=NodeId(value='nod_sched-c'), status='blocked', "
        "remaining_dependencies=2)"
    )
    with pytest.raises(FrozenInstanceError):
        node.status = ScheduledNodeStatus.READY  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("node_id", "nod_sched-a", "NodeId"),
        ("status", "ready", "ScheduledNodeStatus"),
        ("remaining_dependency_ids", [], "tuple"),
        ("remaining_dependency_ids", ("nod_sched-b",), "invalid"),
    ],
)
def test_scheduled_node_requires_exact_public_types(
    field: str,
    value: object,
    message: str,
) -> None:
    values: dict[str, object] = {
        "node_id": _id("a"),
        "status": ScheduledNodeStatus.READY,
        "remaining_dependency_ids": (),
    }
    values[field] = value
    with pytest.raises(TypeError, match=message):
        ScheduledNode(**cast(Any, values))


def test_scheduled_node_rejects_incoherent_dependencies() -> None:
    with pytest.raises(SchedulerInvalidStateError, match="requires a dependency"):
        _node("a", ScheduledNodeStatus.BLOCKED)
    with pytest.raises(SchedulerInvalidStateError, match="cannot retain"):
        _node("a", ScheduledNodeStatus.READY, ("b",))
    with pytest.raises(SchedulerInvalidStateError, match="unique"):
        _node("a", ScheduledNodeStatus.BLOCKED, ("b", "b"))
    with pytest.raises(SchedulerInvalidStateError, match="itself"):
        _node("a", ScheduledNodeStatus.BLOCKED, ("a",))
    dependencies = tuple(
        NodeId(f"nod_dependency-{index:03d}") for index in range(MAX_SCHEDULER_DEPENDENCIES + 1)
    )
    with pytest.raises(SchedulerInvalidStateError, match="dependency limit"):
        ScheduledNode(_id("a"), ScheduledNodeStatus.BLOCKED, dependencies)


def test_scheduler_state_exposes_exact_frontier_and_redacted_repr() -> None:
    state = SchedulerState(
        SchedulerStatus.ACTIVE,
        (
            _node("a", ScheduledNodeStatus.SUCCEEDED),
            _node("b", ScheduledNodeStatus.RUNNING),
            _node("c", ScheduledNodeStatus.READY),
        ),
        _fingerprint(),
    )
    assert state.active_node_id == _id("b")
    assert state.ready_node_ids == (_id("c"),)
    assert state.succeeded_node_ids == (_id("a"),)
    assert state.failed_node_id is None
    assert repr(state) == (
        "SchedulerState(version=1, status='active', nodes=3, ready=1, active=True, "
        "plan_fingerprint=<redacted>)"
    )
    with pytest.raises(FrozenInstanceError):
        state.nodes = ()  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("status", "active", "SchedulerStatus"),
        ("nodes", [], "tuple"),
        ("nodes", (object(),), "invalid"),
        ("plan_fingerprint", "a" * 64, "PlanFingerprint"),
        ("version", True, "integer"),
    ],
)
def test_scheduler_state_requires_exact_public_types(
    field: str,
    value: object,
    message: str,
) -> None:
    values: dict[str, object] = {
        "status": SchedulerStatus.ACTIVE,
        "nodes": (_node("a", ScheduledNodeStatus.READY),),
        "plan_fingerprint": _fingerprint(),
        "version": 1,
    }
    values[field] = value
    with pytest.raises(TypeError, match=message):
        SchedulerState(**cast(Any, values))


def test_scheduler_state_rejects_version_size_and_identity_corruption() -> None:
    with pytest.raises(SchedulerInvalidStateError, match="unsupported"):
        SchedulerState(
            SchedulerStatus.ACTIVE,
            (_node("a", ScheduledNodeStatus.READY),),
            _fingerprint(),
            version=2,
        )
    with pytest.raises(SchedulerInvalidStateError, match="at least one"):
        SchedulerState(SchedulerStatus.ACTIVE, (), _fingerprint())
    with pytest.raises(SchedulerInvalidStateError, match="unique"):
        SchedulerState(
            SchedulerStatus.ACTIVE,
            (
                _node("a", ScheduledNodeStatus.READY),
                _node("a", ScheduledNodeStatus.READY),
            ),
            _fingerprint(),
        )
    with pytest.raises(SchedulerInvalidStateError, match="known scheduled node"):
        SchedulerState(
            SchedulerStatus.ACTIVE,
            (
                _node("a", ScheduledNodeStatus.READY),
                _node("b", ScheduledNodeStatus.BLOCKED, ("foreign",)),
            ),
            _fingerprint(),
        )
    nodes = tuple(
        ScheduledNode(
            NodeId(f"nod_sched-{index:03d}"),
            ScheduledNodeStatus.READY,
            (),
        )
        for index in range(MAX_EXECUTION_PLAN_NODES + 1)
    )
    with pytest.raises(SchedulerInvalidStateError, match="node limit"):
        SchedulerState(SchedulerStatus.ACTIVE, nodes, _fingerprint())


def test_scheduler_state_rejects_sequential_and_global_status_corruption() -> None:
    with pytest.raises(SchedulerInvalidStateError, match="one running"):
        SchedulerState(
            SchedulerStatus.ACTIVE,
            (
                _node("a", ScheduledNodeStatus.RUNNING),
                _node("b", ScheduledNodeStatus.RUNNING),
            ),
            _fingerprint(),
        )
    with pytest.raises(SchedulerInvalidStateError, match="cannot contain failure"):
        SchedulerState(
            SchedulerStatus.ACTIVE,
            (_node("a", ScheduledNodeStatus.FAILED),),
            _fingerprint(),
        )
    with pytest.raises(SchedulerInvalidStateError, match="must be succeeded"):
        SchedulerState(
            SchedulerStatus.ACTIVE,
            (_node("a", ScheduledNodeStatus.SUCCEEDED),),
            _fingerprint(),
        )
    with pytest.raises(SchedulerInvalidStateError, match="every node"):
        SchedulerState(
            SchedulerStatus.SUCCEEDED,
            (_node("a", ScheduledNodeStatus.READY),),
            _fingerprint(),
        )
    with pytest.raises(SchedulerInvalidStateError, match="one failed"):
        SchedulerState(
            SchedulerStatus.FAILED,
            (_node("a", ScheduledNodeStatus.READY),),
            _fingerprint(),
        )
    with pytest.raises(SchedulerInvalidStateError, match="one failed"):
        SchedulerState(
            SchedulerStatus.FAILED,
            (
                _node("a", ScheduledNodeStatus.FAILED),
                _node("b", ScheduledNodeStatus.RUNNING),
            ),
            _fingerprint(),
        )


def test_scheduler_state_rejects_no_progress_frontier() -> None:
    with pytest.raises(SchedulerDeadlockError, match="no running or ready"):
        SchedulerState(
            SchedulerStatus.ACTIVE,
            (
                _node("a", ScheduledNodeStatus.BLOCKED, ("b",)),
                _node("b", ScheduledNodeStatus.BLOCKED, ("a",)),
            ),
            _fingerprint(),
        )
