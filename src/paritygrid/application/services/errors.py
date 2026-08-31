"""Typed failures for the operational application services.

These failures compose accepted repository and writer errors into the small
set of use-case outcomes the HTTP boundary maps onto Problem Details.  They
deliberately extend :class:`Exception` rather than the closed domain error
registry, mirroring the Phase 11 repair workflow error style.
"""

from dataclasses import dataclass


class OperationalServiceError(Exception):
    """Base class for operational use-case failures."""


class OperationalRequestError(OperationalServiceError):
    """A request value violates a use-case rule before any durable work."""

    def __init__(self, message: str, *, field: str | None = None) -> None:
        super().__init__(message)
        self.field = field


class OperationalRecordNotFoundError(OperationalServiceError):
    """A addressed record does not exist."""

    def __init__(self, resource: str, identifier: str) -> None:
        super().__init__(f"{resource} {identifier!r} does not exist")
        self.resource = resource
        self.identifier = identifier


class OperationalConflictError(OperationalServiceError):
    """Durable state conflicts with the requested transition."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


class RunInvalidTransitionError(OperationalConflictError):
    """The addressed run cannot perform the requested lifecycle transition."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="run_invalid_transition")


class OperationalUnavailableError(OperationalServiceError):
    """A backing store or writer cannot currently complete the request."""


class IdempotencyBoundaryError(OperationalServiceError):
    """Base class for command-idempotency boundary failures."""


class IdempotencyKeyConflictError(IdempotencyBoundaryError):
    """The idempotency key was reused with a different canonical request."""


class IdempotencyInProgressError(IdempotencyBoundaryError):
    """Another request owns the reservation and its lease has not expired."""


class IdempotencyReplayConflictError(IdempotencyBoundaryError):
    """Durable replay evidence conflicts with the attempted command."""


@dataclass(frozen=True, slots=True)
class ProblemOutcome:
    """A bounded transport-shaped failure produced by a use case.

    Services raise this when a deterministic application outcome is already
    expressible as a Problem Details response: the API stores it as the
    logical idempotent response instead of inventing its own mapping.
    """

    status_code: int
    code: str
    detail: str
