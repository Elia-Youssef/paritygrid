"""Exact run and work-item lifecycle state machines."""

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType

from paritygrid.domain.errors import InvalidTransitionError


class RunState(StrEnum):
    """State of a captured pipeline run."""

    QUEUED = "queued"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    RESUMING = "resuming"
    SUCCEEDED = "succeeded"
    PARTIALLY_SUCCEEDED = "partially_succeeded"
    FAILED = "failed"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        """Report whether no later state is valid."""
        return not RUN_TRANSITIONS[self]

    def can_transition_to(self, target: object) -> bool:
        """Report whether the requested transition is an exact lifecycle arrow."""
        return isinstance(target, RunState) and target in RUN_TRANSITIONS[self]

    def transition_to(self, target: object) -> RunState:
        """Return the target state or reject an invalid lifecycle move."""
        if not isinstance(target, RunState):
            raise TypeError("target must be a RunState")
        if not self.can_transition_to(target):
            raise InvalidTransitionError(
                lifecycle="run",
                current_state=self.value,
                target_state=target.value,
            )
        return target


RUN_TRANSITIONS: Mapping[RunState, frozenset[RunState]] = MappingProxyType(
    {
        RunState.QUEUED: frozenset({RunState.RUNNING, RunState.CANCELLED}),
        RunState.RUNNING: frozenset(
            {
                RunState.PAUSING,
                RunState.SUCCEEDED,
                RunState.PARTIALLY_SUCCEEDED,
                RunState.FAILED,
                RunState.CANCELLING,
            }
        ),
        RunState.PAUSING: frozenset({RunState.PAUSED}),
        RunState.PAUSED: frozenset({RunState.RESUMING}),
        RunState.RESUMING: frozenset({RunState.RUNNING}),
        RunState.SUCCEEDED: frozenset(),
        RunState.PARTIALLY_SUCCEEDED: frozenset(),
        RunState.FAILED: frozenset(),
        RunState.CANCELLING: frozenset({RunState.CANCELLED}),
        RunState.CANCELLED: frozenset(),
    }
)


class WorkItemState(StrEnum):
    """State of one immutable work item across its attempts."""

    PENDING = "pending"
    LEASED = "leased"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    RETRY_WAIT = "retry_wait"
    QUARANTINED = "quarantined"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        """Report whether no later state is valid."""
        return not WORK_ITEM_TRANSITIONS[self]

    def can_transition_to(self, target: object) -> bool:
        """Report whether the requested transition is an exact lifecycle arrow."""
        return isinstance(target, WorkItemState) and target in WORK_ITEM_TRANSITIONS[self]

    def transition_to(self, target: object) -> WorkItemState:
        """Return the target state or reject an invalid lifecycle move."""
        if not isinstance(target, WorkItemState):
            raise TypeError("target must be a WorkItemState")
        if not self.can_transition_to(target):
            raise InvalidTransitionError(
                lifecycle="work item",
                current_state=self.value,
                target_state=target.value,
            )
        return target


WORK_ITEM_TRANSITIONS: Mapping[WorkItemState, frozenset[WorkItemState]] = MappingProxyType(
    {
        WorkItemState.PENDING: frozenset({WorkItemState.LEASED}),
        WorkItemState.LEASED: frozenset({WorkItemState.RUNNING}),
        WorkItemState.RUNNING: frozenset(
            {
                WorkItemState.SUCCEEDED,
                WorkItemState.RETRY_WAIT,
                WorkItemState.QUARANTINED,
                WorkItemState.FAILED,
                WorkItemState.CANCELLED,
            }
        ),
        WorkItemState.SUCCEEDED: frozenset(),
        WorkItemState.RETRY_WAIT: frozenset({WorkItemState.LEASED}),
        WorkItemState.QUARANTINED: frozenset(),
        WorkItemState.FAILED: frozenset(),
        WorkItemState.CANCELLED: frozenset(),
    }
)
