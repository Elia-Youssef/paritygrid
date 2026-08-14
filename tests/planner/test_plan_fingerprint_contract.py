"""Frozen public contracts for versioned logical-plan fingerprints."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from paritygrid.application.planner import (
    PLAN_FINGERPRINT_ALGORITHM,
    PLAN_FINGERPRINT_HEX_LENGTH,
    PLAN_FINGERPRINT_VERSION,
    InvalidPlanFingerprintError,
    PlanFingerprint,
    PlanFingerprintError,
)


def test_plan_fingerprint_constants_and_error_family_are_frozen() -> None:
    assert PLAN_FINGERPRINT_VERSION == 1
    assert PLAN_FINGERPRINT_ALGORITHM == "sha256"
    assert PLAN_FINGERPRINT_HEX_LENGTH == 64
    assert issubclass(InvalidPlanFingerprintError, PlanFingerprintError)


def test_plan_fingerprint_round_trips_exact_lowercase_hex() -> None:
    text = "abcdef01" * 8
    fingerprint = PlanFingerprint.parse(text)
    assert str(fingerprint) == text
    assert bytes(fingerprint) == text.encode("ascii")
    assert fingerprint.to_bytes() == text.encode("ascii")
    assert PlanFingerprint.from_bytes(bytes(fingerprint)) == fingerprint
    assert hash(PlanFingerprint(text)) == hash(fingerprint)


@pytest.mark.parametrize(
    "value",
    [
        "",
        "0" * 63,
        "0" * 65,
        "A" * 64,
        "g" * 64,
        "0" * 63 + "é",
    ],
)
def test_plan_fingerprint_rejects_noncanonical_text(value: str) -> None:
    with pytest.raises(InvalidPlanFingerprintError, match="64 lowercase"):
        PlanFingerprint.parse(value)


def test_plan_fingerprint_rejects_wrong_public_types_and_non_ascii_bytes() -> None:
    with pytest.raises(TypeError, match="text"):
        PlanFingerprint.parse(42)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="bytes"):
        PlanFingerprint.from_bytes(bytearray(b"0" * 64))  # type: ignore[arg-type]
    with pytest.raises(InvalidPlanFingerprintError, match="ASCII"):
        PlanFingerprint.from_bytes("é".encode() * 64)


def test_plan_fingerprint_is_immutable_and_orderable() -> None:
    first = PlanFingerprint("0" * 64)
    second = PlanFingerprint("1" * 64)
    assert first < second
    with pytest.raises(FrozenInstanceError):
        first.value = "2" * 64  # type: ignore[misc]
