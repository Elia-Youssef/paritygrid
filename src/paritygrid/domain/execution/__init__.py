"""Execution lifecycle and failure-classification values."""

from paritygrid.domain.execution.failures import (
    FAILURE_DISPOSITIONS,
    FailureClassification,
    FailureDisposition,
    disposition_for,
)
from paritygrid.domain.execution.states import (
    RUN_TRANSITIONS,
    WORK_ITEM_TRANSITIONS,
    RunState,
    WorkItemState,
)

__all__ = [
    "FAILURE_DISPOSITIONS",
    "RUN_TRANSITIONS",
    "WORK_ITEM_TRANSITIONS",
    "FailureClassification",
    "FailureDisposition",
    "RunState",
    "WorkItemState",
    "disposition_for",
]
