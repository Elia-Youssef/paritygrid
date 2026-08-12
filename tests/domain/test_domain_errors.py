"""Exhaustive verification of the operational domain failure contract."""

from types import MappingProxyType

import pytest

from paritygrid.domain.errors import (
    DOMAIN_ERROR_TYPES,
    CanonicalEncodingError,
    CanonicalErrorCode,
    DomainError,
    DomainErrorCode,
    InvalidTransitionError,
    StaleRepairPlanError,
)
from paritygrid.domain.models import StateFingerprint

CURRENT = StateFingerprint("1" * 64)
STALE = StateFingerprint("2" * 64)


def test_domain_error_registry_is_closed_complete_and_immutable() -> None:
    expected = {
        DomainErrorCode.INVALID_TRANSITION: InvalidTransitionError,
        DomainErrorCode.STALE_REPAIR_PLAN: StaleRepairPlanError,
        DomainErrorCode.CANONICAL_ENCODING: CanonicalEncodingError,
    }

    assert isinstance(DOMAIN_ERROR_TYPES, MappingProxyType)
    assert set(DOMAIN_ERROR_TYPES) == set(DomainErrorCode)
    assert dict(DOMAIN_ERROR_TYPES) == expected
    assert len(set(DOMAIN_ERROR_TYPES.values())) == len(expected)
    assert set(DomainError.__subclasses__()) == set(expected.values())
    assert all(error_type.code is code for code, error_type in DOMAIN_ERROR_TYPES.items())
    assert all(issubclass(error_type, DomainError) for error_type in DOMAIN_ERROR_TYPES.values())


def test_invalid_transition_preserves_its_public_contract_and_identity_semantics() -> None:
    first = InvalidTransitionError(
        lifecycle="run",
        current_state="queued",
        target_state="failed",
    )
    second = InvalidTransitionError(
        lifecycle="run",
        current_state="queued",
        target_state="failed",
    )

    assert first.code is DomainErrorCode.INVALID_TRANSITION
    assert first.lifecycle == "run"
    assert first.current_state == "queued"
    assert first.target_state == "failed"
    assert str(first) == "invalid run transition from 'queued' to 'failed'"
    assert first.args == ("invalid run transition from 'queued' to 'failed'",)
    assert first is not second
    assert first != second
    with pytest.raises(AttributeError):
        first.lifecycle = "work item"  # type: ignore[misc]


@pytest.mark.parametrize("field_name", ["lifecycle", "current_state", "target_state"])
def test_invalid_transition_rejects_unbounded_or_untrusted_context(field_name: str) -> None:
    values: dict[str, object] = {
        "lifecycle": "run",
        "current_state": "queued",
        "target_state": "failed",
    }
    values[field_name] = "x" * 97
    with pytest.raises(ValueError, match=field_name):
        InvalidTransitionError(**values)  # type: ignore[arg-type]
    values[field_name] = 1
    with pytest.raises(TypeError, match=field_name):
        InvalidTransitionError(**values)  # type: ignore[arg-type]
    values[field_name] = "queued\n"
    with pytest.raises(ValueError, match="printable ASCII"):
        InvalidTransitionError(**values)  # type: ignore[arg-type]


def test_stale_repair_plan_error_has_exact_typed_read_only_context() -> None:
    error = StaleRepairPlanError(expected=CURRENT, actual=STALE)

    assert error.code is DomainErrorCode.STALE_REPAIR_PLAN
    assert error.expected is CURRENT
    assert error.actual is STALE
    assert str(error) == f"repair plan expects state {CURRENT}, but current state is {STALE}"
    assert error.args == (str(error),)
    with pytest.raises(AttributeError):
        error.actual = CURRENT  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field_name", "value"),
    [("expected", "1" * 64), ("actual", "2" * 64)],
)
def test_stale_repair_plan_error_requires_fingerprint_values(
    field_name: str, value: object
) -> None:
    arguments: dict[str, object] = {"expected": CURRENT, "actual": STALE}
    arguments[field_name] = value

    with pytest.raises(TypeError, match="StateFingerprint"):
        StaleRepairPlanError(**arguments)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("reason", "arguments", "message"),
    [
        (
            CanonicalErrorCode.UNSUPPORTED_CANONICAL_VERSION,
            {"version": 2},
            "canonical encoding version 2 is unsupported",
        ),
        (
            CanonicalErrorCode.UNSUPPORTED_CANONICAL_TYPE,
            {"subject_type": "builtins.float"},
            "canonical encoding does not support type 'builtins.float'",
        ),
        (
            CanonicalErrorCode.INVALID_CANONICAL_VALUE,
            {"subject_type": "inventory.attributes"},
            "canonical value for type 'inventory.attributes' violates encoding invariants",
        ),
    ],
)
def test_every_canonical_error_reason_has_stable_typed_context(
    reason: CanonicalErrorCode,
    arguments: dict[str, object],
    message: str,
) -> None:
    error = CanonicalEncodingError(reason=reason, **arguments)  # type: ignore[arg-type]

    assert error.code is DomainErrorCode.CANONICAL_ENCODING
    assert error.reason is reason
    assert str(error) == message
    assert error.args == (message,)
    assert error.version == arguments.get("version")
    assert error.subject_type == arguments.get("subject_type")
    with pytest.raises(AttributeError):
        error.reason = CanonicalErrorCode.INVALID_CANONICAL_VALUE  # type: ignore[misc]


def test_canonical_error_reason_space_is_exhaustive() -> None:
    assert set(CanonicalErrorCode) == {
        CanonicalErrorCode.UNSUPPORTED_CANONICAL_VERSION,
        CanonicalErrorCode.UNSUPPORTED_CANONICAL_TYPE,
        CanonicalErrorCode.INVALID_CANONICAL_VALUE,
    }


def test_canonical_error_rejects_invalid_reason_and_context_combinations() -> None:
    with pytest.raises(TypeError, match="CanonicalErrorCode"):
        CanonicalEncodingError(reason="unsupported_canonical_type", subject_type="float")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="do not accept a subject"):
        CanonicalEncodingError(
            reason=CanonicalErrorCode.UNSUPPORTED_CANONICAL_VERSION,
            version=2,
            subject_type="inventory.record",
        )
    with pytest.raises(TypeError, match="integer"):
        CanonicalEncodingError(
            reason=CanonicalErrorCode.UNSUPPORTED_CANONICAL_VERSION,
            version=True,
        )
    with pytest.raises(ValueError, match="outside"):
        CanonicalEncodingError(
            reason=CanonicalErrorCode.UNSUPPORTED_CANONICAL_VERSION,
            version=-1,
        )
    with pytest.raises(ValueError, match="require a subject"):
        CanonicalEncodingError(reason=CanonicalErrorCode.UNSUPPORTED_CANONICAL_TYPE)
    with pytest.raises(ValueError, match="do not accept a version"):
        CanonicalEncodingError(
            reason=CanonicalErrorCode.INVALID_CANONICAL_VALUE,
            subject_type="inventory.record",
            version=1,
        )
    with pytest.raises(ValueError, match="between"):
        CanonicalEncodingError(
            reason=CanonicalErrorCode.UNSUPPORTED_CANONICAL_TYPE,
            subject_type="x" * 97,
        )
