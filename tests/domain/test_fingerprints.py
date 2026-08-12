"""Verification for opaque state fingerprint values."""

from dataclasses import FrozenInstanceError

import pytest
from hypothesis import given
from hypothesis import strategies as st

from paritygrid.domain.models import RepairActionId, StateFingerprint


def test_state_fingerprint_round_trips_without_computing_a_digest() -> None:
    text = "0123456789abcdef" * 4
    fingerprint = StateFingerprint.parse(text)

    assert str(fingerprint) == text
    assert bytes(fingerprint) == text.encode("ascii")
    assert StateFingerprint.from_bytes(bytes(fingerprint)) == fingerprint
    assert hash(StateFingerprint(text)) == hash(fingerprint)


@pytest.mark.parametrize(
    "value",
    [
        "",
        "a" * 63,
        "a" * 65,
        "A" * 64,
        "g" * 64,
        "0" * 63 + "-",
        "0" * 63 + "é",
    ],
)
def test_state_fingerprint_rejects_noncanonical_text(value: str) -> None:
    with pytest.raises(ValueError, match="64 lowercase hexadecimal"):
        StateFingerprint.parse(value)


def test_state_fingerprint_rejects_wrong_input_types() -> None:
    with pytest.raises(TypeError, match="text"):
        StateFingerprint.parse(42)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="bytes"):
        StateFingerprint.from_bytes(bytearray(b"0" * 64))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="ASCII"):
        StateFingerprint.from_bytes("é".encode() * 64)


def test_state_fingerprint_is_immutable_and_orderable() -> None:
    first = StateFingerprint("0" * 64)
    second = StateFingerprint("1" * 64)

    assert first < second
    with pytest.raises(FrozenInstanceError):
        first.value = "1" * 64  # type: ignore[misc]


def test_repair_action_identifier_is_typed_and_stable() -> None:
    action_id = RepairActionId.parse("rac_create-sku-001")

    assert str(action_id) == "rac_create-sku-001"
    assert RepairActionId.from_bytes(bytes(action_id)) == action_id


@given(st.binary(min_size=32, max_size=32))
def test_every_sha256_sized_byte_value_has_a_valid_hex_representation(value: bytes) -> None:
    fingerprint = StateFingerprint(value.hex())

    assert StateFingerprint.from_bytes(bytes(fingerprint)) == fingerprint
