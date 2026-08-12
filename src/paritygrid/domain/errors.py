"""Errors raised when domain invariants are violated."""


class DomainError(Exception):
    """Base class for errors caused by invalid domain operations."""


class InvalidTransitionError(DomainError):
    """Raised when a lifecycle cannot move between two states."""

    lifecycle: str
    current_state: str
    target_state: str

    def __init__(self, *, lifecycle: str, current_state: str, target_state: str) -> None:
        self.lifecycle = lifecycle
        self.current_state = current_state
        self.target_state = target_state
        super().__init__(
            f"invalid {lifecycle} transition from {current_state!r} to {target_state!r}"
        )
