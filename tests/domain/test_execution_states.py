"""Exhaustive verification of execution lifecycle transitions."""

from itertools import product

import pytest
from hypothesis import given
from hypothesis import strategies as st

from paritygrid.domain.errors import DomainError, DomainErrorCode, InvalidTransitionError
from paritygrid.domain.execution import (
    RUN_TRANSITIONS,
    WORK_ITEM_TRANSITIONS,
    RunState,
    WorkItemState,
)

RUN_ARROWS = frozenset(
    {
        (RunState.QUEUED, RunState.RUNNING),
        (RunState.QUEUED, RunState.CANCELLED),
        (RunState.RUNNING, RunState.PAUSING),
        (RunState.RUNNING, RunState.SUCCEEDED),
        (RunState.RUNNING, RunState.PARTIALLY_SUCCEEDED),
        (RunState.RUNNING, RunState.FAILED),
        (RunState.RUNNING, RunState.CANCELLING),
        (RunState.PAUSING, RunState.PAUSED),
        (RunState.PAUSED, RunState.RESUMING),
        (RunState.RESUMING, RunState.RUNNING),
        (RunState.CANCELLING, RunState.CANCELLED),
    }
)
WORK_ITEM_ARROWS = frozenset(
    {
        (WorkItemState.PENDING, WorkItemState.LEASED),
        (WorkItemState.LEASED, WorkItemState.RUNNING),
        (WorkItemState.RUNNING, WorkItemState.SUCCEEDED),
        (WorkItemState.RUNNING, WorkItemState.RETRY_WAIT),
        (WorkItemState.RUNNING, WorkItemState.QUARANTINED),
        (WorkItemState.RUNNING, WorkItemState.FAILED),
        (WorkItemState.RUNNING, WorkItemState.CANCELLED),
        (WorkItemState.RETRY_WAIT, WorkItemState.LEASED),
    }
)


@pytest.mark.parametrize(("current", "target"), tuple(product(RunState, repeat=2)))
def test_every_run_state_pair_matches_the_documented_arrows(
    current: RunState, target: RunState
) -> None:
    expected = (current, target) in RUN_ARROWS

    assert current.can_transition_to(target) is expected
    if expected:
        assert current.transition_to(target) is target
    else:
        with pytest.raises(InvalidTransitionError) as captured:
            current.transition_to(target)
        assert isinstance(captured.value, DomainError)
        assert captured.value.lifecycle == "run"
        assert captured.value.current_state == current.value
        assert captured.value.target_state == target.value
        assert captured.value.code is DomainErrorCode.INVALID_TRANSITION
        message = f"invalid run transition from {current.value!r} to {target.value!r}"
        assert str(captured.value) == message
        assert captured.value.args == (message,)


@pytest.mark.parametrize(("current", "target"), tuple(product(WorkItemState, repeat=2)))
def test_every_work_item_state_pair_matches_the_documented_arrows(
    current: WorkItemState, target: WorkItemState
) -> None:
    expected = (current, target) in WORK_ITEM_ARROWS

    assert current.can_transition_to(target) is expected
    if expected:
        assert current.transition_to(target) is target
    else:
        with pytest.raises(InvalidTransitionError) as captured:
            current.transition_to(target)
        assert captured.value.lifecycle == "work item"
        assert captured.value.current_state == current.value
        assert captured.value.target_state == target.value
        assert captured.value.code is DomainErrorCode.INVALID_TRANSITION
        message = f"invalid work item transition from {current.value!r} to {target.value!r}"
        assert str(captured.value) == message
        assert captured.value.args == (message,)


def test_transition_tables_are_exhaustive_and_match_the_expected_arrows() -> None:
    assert set(RUN_TRANSITIONS) == set(RunState)
    assert set(WORK_ITEM_TRANSITIONS) == set(WorkItemState)
    assert {
        (current, target) for current, targets in RUN_TRANSITIONS.items() for target in targets
    } == RUN_ARROWS
    assert {
        (current, target)
        for current, targets in WORK_ITEM_TRANSITIONS.items()
        for target in targets
    } == WORK_ITEM_ARROWS


def test_terminal_states_have_no_outgoing_transitions() -> None:
    assert {state for state in RunState if state.is_terminal} == {
        RunState.SUCCEEDED,
        RunState.PARTIALLY_SUCCEEDED,
        RunState.FAILED,
        RunState.CANCELLED,
    }
    assert {state for state in WorkItemState if state.is_terminal} == {
        WorkItemState.SUCCEEDED,
        WorkItemState.QUARANTINED,
        WorkItemState.FAILED,
        WorkItemState.CANCELLED,
    }


def test_transition_methods_reject_values_from_another_lifecycle() -> None:
    with pytest.raises(TypeError, match="RunState"):
        RunState.QUEUED.transition_to(WorkItemState.LEASED)
    with pytest.raises(TypeError, match="WorkItemState"):
        WorkItemState.PENDING.transition_to(RunState.RUNNING)


def test_transition_queries_reject_raw_strings_and_other_lifecycles() -> None:
    assert not RunState.QUEUED.can_transition_to("running")
    assert not RunState.QUEUED.can_transition_to(WorkItemState.RUNNING)
    assert not WorkItemState.PENDING.can_transition_to("leased")
    assert not WorkItemState.PENDING.can_transition_to(RunState.RUNNING)


def test_invalid_transition_message_is_stable_and_descriptive() -> None:
    error = InvalidTransitionError(
        lifecycle="run",
        current_state="queued",
        target_state="failed",
    )

    assert str(error) == "invalid run transition from 'queued' to 'failed'"


@given(st.sampled_from(tuple(RunState)), st.sampled_from(tuple(RunState)))
def test_run_transition_property_covers_the_closed_state_space(
    current: RunState, target: RunState
) -> None:
    assert current.can_transition_to(target) is ((current, target) in RUN_ARROWS)


@given(st.sampled_from(tuple(WorkItemState)), st.sampled_from(tuple(WorkItemState)))
def test_work_item_transition_property_covers_the_closed_state_space(
    current: WorkItemState, target: WorkItemState
) -> None:
    assert current.can_transition_to(target) is ((current, target) in WORK_ITEM_ARROWS)
