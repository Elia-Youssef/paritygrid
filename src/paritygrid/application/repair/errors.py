"""Typed failures for the Phase 11 repair and verification workflow."""


class RepairWorkflowError(Exception):
    """Base class for repair workflow failures.

    These are application-workflow failures, not domain-rule failures, so
    they deliberately stay outside the closed domain error registry.
    """


class RepairRunNotFoundError(RepairWorkflowError):
    """The referenced run does not exist."""


class RepairReconciliationMissingError(RepairWorkflowError):
    """No durable reconciliation snapshot exists for the run."""


class RepairReconciliationStaleError(RepairWorkflowError):
    """The supplied reconciliation identity is not the current snapshot."""

    def __init__(self, expected: str, actual: str) -> None:
        super().__init__("the reconciliation snapshot changed; regenerate the repair plan")
        self.expected = expected
        self.actual = actual


class RepairPlanMismatchError(RepairWorkflowError):
    """A request does not address the exact durable plan."""


class RepairPlanStateError(RepairWorkflowError):
    """The durable plan lifecycle state rejects the requested operation."""


class RepairApprovalConflictError(RepairWorkflowError):
    """An approval attempt conflicts with the immutable approval fact."""


class RepairWriterUnavailableError(RepairWorkflowError):
    """The durable writer rejected or could not execute a repair command."""


class RepairWriterOutcomeUnknownError(RepairWorkflowError):
    """The durable outcome of a repair command is unknown."""


class TargetApplicationError(RepairWorkflowError):
    """A repair effect failed terminally at the target boundary."""


class TargetApplicationUnresolvedError(RepairWorkflowError):
    """A repair effect outcome is unresolved and requires recovery."""


class TargetApplicationInterruptedError(RepairWorkflowError):
    """Cooperative cancellation stopped repair application."""


__all__ = [
    "RepairApprovalConflictError",
    "RepairPlanMismatchError",
    "RepairPlanStateError",
    "RepairReconciliationMissingError",
    "RepairReconciliationStaleError",
    "RepairRunNotFoundError",
    "RepairWorkflowError",
    "RepairWriterOutcomeUnknownError",
    "RepairWriterUnavailableError",
    "TargetApplicationError",
    "TargetApplicationInterruptedError",
    "TargetApplicationUnresolvedError",
]
