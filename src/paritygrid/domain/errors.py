"""Typed failures raised by pure domain operations."""

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import ClassVar

from paritygrid.domain.models.fingerprints import StateFingerprint


class DomainErrorCode(StrEnum):
    """Stable codes for the closed set of operational domain failures."""

    INVALID_TRANSITION = "invalid_transition"
    STALE_REPAIR_PLAN = "stale_repair_plan"
    CANONICAL_ENCODING = "canonical_encoding"


class CanonicalErrorCode(StrEnum):
    """Stable reasons why a trusted value cannot be canonically encoded."""

    UNSUPPORTED_CANONICAL_VERSION = "unsupported_canonical_version"
    UNSUPPORTED_CANONICAL_TYPE = "unsupported_canonical_type"
    INVALID_CANONICAL_VALUE = "invalid_canonical_value"


class DomainError(Exception):
    """Base class for errors caused by invalid domain operations."""

    code: ClassVar[DomainErrorCode]


class InvalidTransitionError(DomainError):
    """Raised when a lifecycle cannot move between two states."""

    __slots__ = ("_current_state", "_lifecycle", "_target_state")

    code = DomainErrorCode.INVALID_TRANSITION

    def __init__(self, *, lifecycle: str, current_state: str, target_state: str) -> None:
        self._lifecycle = _bounded_error_text(lifecycle, field_name="lifecycle")
        self._current_state = _bounded_error_text(current_state, field_name="current_state")
        self._target_state = _bounded_error_text(target_state, field_name="target_state")
        super().__init__(
            f"invalid {self.lifecycle} transition from {self.current_state!r} "
            f"to {self.target_state!r}"
        )

    @property
    def lifecycle(self) -> str:
        """Return the lifecycle whose transition was rejected."""
        return self._lifecycle

    @property
    def current_state(self) -> str:
        """Return the state from which the transition was attempted."""
        return self._current_state

    @property
    def target_state(self) -> str:
        """Return the requested destination state."""
        return self._target_state


class StaleRepairPlanError(DomainError):
    """Raised when a repair plan no longer describes the current state."""

    __slots__ = ("_actual", "_expected")

    code = DomainErrorCode.STALE_REPAIR_PLAN

    def __init__(self, *, expected: StateFingerprint, actual: StateFingerprint) -> None:
        self._expected = _require_fingerprint(expected, field_name="expected")
        self._actual = _require_fingerprint(actual, field_name="actual")
        super().__init__(f"repair plan expects state {expected}, but current state is {actual}")

    @property
    def expected(self) -> StateFingerprint:
        """Return the reconciliation state captured by the plan."""
        return self._expected

    @property
    def actual(self) -> StateFingerprint:
        """Return the reconciliation state observed at use time."""
        return self._actual


class CanonicalEncodingError(DomainError):
    """Raised when a trusted value has no supported canonical representation."""

    __slots__ = ("_reason", "_subject_type", "_version")

    code = DomainErrorCode.CANONICAL_ENCODING

    def __init__(
        self,
        *,
        reason: CanonicalErrorCode,
        subject_type: str | None = None,
        version: int | None = None,
    ) -> None:
        self._reason = _require_canonical_reason(reason)
        self._subject_type = _canonical_subject(reason, subject_type)
        self._version = _canonical_version(reason, version)
        super().__init__(_canonical_message(reason, self._subject_type, self._version))

    @property
    def reason(self) -> CanonicalErrorCode:
        """Return the closed canonical-encoding failure reason."""
        return self._reason

    @property
    def subject_type(self) -> str | None:
        """Return the bounded type name associated with the failure, if any."""
        return self._subject_type

    @property
    def version(self) -> int | None:
        """Return the unsupported encoding version, if any."""
        return self._version


DOMAIN_ERROR_TYPES: Mapping[DomainErrorCode, type[DomainError]] = MappingProxyType(
    {
        DomainErrorCode.INVALID_TRANSITION: InvalidTransitionError,
        DomainErrorCode.STALE_REPAIR_PLAN: StaleRepairPlanError,
        DomainErrorCode.CANONICAL_ENCODING: CanonicalEncodingError,
    }
)


def _bounded_error_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be text")
    if not 1 <= len(value) <= 96:
        raise ValueError(f"{field_name} must contain between 1 and 96 characters")
    if not value.isascii() or not value.isprintable():
        raise ValueError(f"{field_name} must contain printable ASCII")
    return value


def _require_fingerprint(value: object, *, field_name: str) -> StateFingerprint:
    if not isinstance(value, StateFingerprint):
        raise TypeError(f"{field_name} fingerprint must be a StateFingerprint")
    return value


def _require_canonical_reason(value: object) -> CanonicalErrorCode:
    if not isinstance(value, CanonicalErrorCode):
        raise TypeError("canonical error reason must be a CanonicalErrorCode")
    return value


def _canonical_subject(reason: CanonicalErrorCode, value: object) -> str | None:
    if reason is CanonicalErrorCode.UNSUPPORTED_CANONICAL_VERSION:
        if value is not None:
            raise ValueError("unsupported canonical version errors do not accept a subject type")
        return None
    if value is None:
        raise ValueError("canonical type and value errors require a subject type")
    return _bounded_error_text(value, field_name="subject_type")


def _canonical_version(reason: CanonicalErrorCode, value: object) -> int | None:
    if reason is CanonicalErrorCode.UNSUPPORTED_CANONICAL_VERSION:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("unsupported canonical version must be an integer")
        if not 0 <= value <= 2_147_483_647:
            raise ValueError("unsupported canonical version is outside the supported range")
        return value
    if value is not None:
        raise ValueError("canonical type and value errors do not accept a version")
    return None


def _canonical_message(
    reason: CanonicalErrorCode,
    subject_type: str | None,
    version: int | None,
) -> str:
    if reason is CanonicalErrorCode.UNSUPPORTED_CANONICAL_VERSION:
        return f"canonical encoding version {version} is unsupported"
    if reason is CanonicalErrorCode.UNSUPPORTED_CANONICAL_TYPE:
        return f"canonical encoding does not support type {subject_type!r}"
    return f"canonical value for type {subject_type!r} violates encoding invariants"
