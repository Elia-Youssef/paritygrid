"""Property verification for canonical domain identifiers."""

import re

import pytest
from hypothesis import given
from hypothesis import strategies as st

from paritygrid.domain.models import AttemptNumber, PipelineVersion, RunId

_CANONICAL_PAYLOAD = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*", flags=re.ASCII)
_PAYLOAD_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"


@st.composite
def canonical_payloads(draw: st.DrawFn) -> str:
    """Build canonical payloads without relying on filtered examples."""
    groups = draw(
        st.lists(
            st.text(alphabet=_PAYLOAD_ALPHABET, min_size=1, max_size=16),
            min_size=1,
            max_size=4,
        )
    )
    payload = "-".join(groups)
    if len(payload) < 3:
        payload += "a" * (3 - len(payload))
    return payload[:64].rstrip("-")


@given(canonical_payloads())
def test_valid_identifier_round_trip_is_stable(payload: str) -> None:
    text = f"run_{payload}"

    identifier = RunId.parse(text)

    assert str(identifier) == text
    assert RunId.parse(str(identifier)) == identifier
    assert RunId.from_bytes(identifier.to_bytes()) == identifier


@given(
    st.text(
        alphabet=st.characters(min_codepoint=128, max_codepoint=0x10FFFF),
        min_size=1,
        max_size=16,
    )
)
def test_non_ascii_payload_is_always_rejected(payload: str) -> None:
    with pytest.raises(ValueError, match="ASCII"):
        RunId.parse(f"run_{payload}")


@given(st.sampled_from(["_", ".", "/", "\\", " ", "%", ":", "\x00"]))
def test_unsafe_ascii_character_is_always_rejected(character: str) -> None:
    with pytest.raises(ValueError, match="canonical"):
        RunId.parse(f"run_abc{character}def")


@given(st.integers(min_value=3, max_value=64))
def test_identifier_accepts_each_supported_payload_length(length: int) -> None:
    identifier = RunId.parse(f"run_{'a' * length}")

    assert len(identifier.value.removeprefix("run_")) == length


@given(st.integers(min_value=0, max_value=2).map(lambda length: "a" * length))
def test_identifier_rejects_every_undersized_payload(payload: str) -> None:
    with pytest.raises(ValueError, match="between"):
        RunId.parse(f"run_{payload}")


@given(st.integers(min_value=65, max_value=256))
def test_identifier_rejects_oversized_payloads(length: int) -> None:
    with pytest.raises(ValueError, match="between"):
        RunId.parse(f"run_{'a' * length}")


@given(st.integers(min_value=1, max_value=2_147_483_647))
def test_sequence_values_round_trip_canonical_decimal(number: int) -> None:
    version = PipelineVersion(number=number)
    attempt = AttemptNumber(number=number)

    assert PipelineVersion.parse(str(version)) == version
    assert AttemptNumber.from_bytes(attempt.to_bytes()) == attempt
    assert int(version) == number


@given(st.integers().filter(lambda number: not 1 <= number <= 2_147_483_647))
def test_sequence_values_reject_all_out_of_range_integers(number: int) -> None:
    with pytest.raises(ValueError, match="between"):
        PipelineVersion(number=number)
