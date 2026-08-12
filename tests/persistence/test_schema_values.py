"""Tests for persistence-only values and deterministic JSON storage."""

import unicodedata
from collections.abc import Callable
from enum import StrEnum

import pytest

from paritygrid.adapters.persistence import (
    CanonicalStorageJson,
    EnvironmentVariableName,
    IdempotencyStatus,
    RepairActionApplicationStatus,
    RepairPlanStatus,
    RunNodeState,
    SecretReferenceName,
    Sha256Digest,
    WorkAttemptOutcome,
)


@pytest.mark.parametrize(
    ("enum_type", "expected"),
    [
        (
            RunNodeState,
            {"pending", "running", "succeeded", "partially_succeeded", "failed", "cancelled"},
        ),
        (
            WorkAttemptOutcome,
            {
                "succeeded",
                "retry_scheduled",
                "quarantined",
                "failed",
                "cancelled",
                "lease_expired",
            },
        ),
        (IdempotencyStatus, {"in_progress", "completed", "failed"}),
        (
            RepairPlanStatus,
            {"proposed", "approved", "applying", "applied", "rejected", "failed"},
        ),
        (RepairActionApplicationStatus, {"pending", "applied", "failed"}),
    ],
)
def test_closed_values_match_persistence_contract(
    enum_type: type[StrEnum], expected: set[str]
) -> None:
    assert {value.value for value in enum_type} == expected


@pytest.mark.parametrize("value", ["0" * 64, "0123456789abcdef" * 4])
def test_sha256_digest_accepts_exact_lowercase_hex(value: str) -> None:
    assert str(Sha256Digest(value)) == value
    assert Sha256Digest.parse(value) == Sha256Digest(value)


@pytest.mark.parametrize("value", ["", "0" * 63, "0" * 65, "G" * 64, "A" * 64, 7])
def test_sha256_digest_rejects_noncanonical_values(value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        Sha256Digest(value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("factory", "valid"),
    [
        (SecretReferenceName, "api-token.primary"),
        (EnvironmentVariableName, "PARITYGRID_API_TOKEN"),
    ],
)
def test_secret_reference_values_accept_portable_names(
    factory: Callable[[str], object], valid: str
) -> None:
    assert factory(valid) is not None


@pytest.mark.parametrize(
    ("factory", "invalid"),
    [
        (SecretReferenceName, "ApiToken"),
        (SecretReferenceName, "token..primary"),
        (EnvironmentVariableName, "api_token"),
        (EnvironmentVariableName, "9TOKEN"),
    ],
)
def test_secret_reference_values_reject_nonportable_names(
    factory: Callable[[str], object], invalid: str
) -> None:
    with pytest.raises(ValueError, match="must"):
        factory(invalid)


@pytest.mark.parametrize(
    ("factory", "invalid"),
    [
        (SecretReferenceName, 7),
        (SecretReferenceName, "x" * 65),
        (EnvironmentVariableName, 7),
        (EnvironmentVariableName, "X" * 129),
    ],
)
def test_secret_reference_values_reject_wrong_types_and_sizes(
    factory: Callable[[str], object], invalid: object
) -> None:
    with pytest.raises((TypeError, ValueError), match="must"):
        factory(invalid)  # type: ignore[arg-type]


def test_storage_json_is_deterministic_and_detached() -> None:
    encoded = CanonicalStorageJson.encode({"z": [2, 1], "a": {"ok": True}})
    assert str(encoded) == '{"a":{"ok":true},"z":[2,1]}'
    assert encoded.decode() == {"a": {"ok": True}, "z": [2, 1]}


def test_storage_json_rejects_non_text_runtime_input() -> None:
    with pytest.raises(TypeError, match="must be text"):
        CanonicalStorageJson(7)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "value",
    [
        {"amount": 1.5},
        {"name": unicodedata.normalize("NFD", "Café")},
        {1: "wrong-key"},
        ("not", "json"),
    ],
)
def test_storage_json_rejects_ambiguous_values(value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        CanonicalStorageJson.encode(value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "text",
    ['{"value":1.0}', '{"value":NaN}', '{"b":1,"a":2}', '{"name":"Cafe\\u0301"}'],
)
def test_storage_json_rejects_noncanonical_text(text: str) -> None:
    with pytest.raises(ValueError, match="storage JSON"):
        CanonicalStorageJson(text)


def test_storage_json_rejects_malformed_text() -> None:
    with pytest.raises(ValueError, match="not valid"):
        CanonicalStorageJson("{")
