"""Validation, ordering, serialization, and determinism of failure scripts."""

import json

import pytest
from hypothesis import given
from hypothesis import settings as hypothesis_settings
from hypothesis import strategies as st

from paritygrid.demo.failures import (
    SCRIPTED_FAILURE_VERSION,
    AppliedFailure,
    FailureScript,
    FailureScriptError,
    ScriptedFailure,
    ScriptedFailureKind,
    require_transport_script,
)


def _rate_limit(sequence: int = 1) -> ScriptedFailure:
    return ScriptedFailure(
        sequence=sequence, kind=ScriptedFailureKind.RATE_LIMIT, retry_after_seconds=5
    )


def _timeout(sequence: int = 2) -> ScriptedFailure:
    return ScriptedFailure(
        sequence=sequence, kind=ScriptedFailureKind.TIMEOUT, delay_microseconds=250_000
    )


def test_scripts_are_stored_in_sequence_order() -> None:
    script = FailureScript.from_entries([_timeout(7), _rate_limit(2), _timeout(4)])
    assert [failure.sequence for failure in script.failures] == [2, 4, 7]


def test_duplicate_sequences_are_rejected() -> None:
    with pytest.raises(FailureScriptError, match="more than once"):
        FailureScript.from_entries(
            [
                _rate_limit(3),
                ScriptedFailure(
                    sequence=3, kind=ScriptedFailureKind.HANG, delay_microseconds=1_000
                ),
            ]
        )


@pytest.mark.parametrize("sequence", [0, -1, 2**31])
def test_out_of_range_sequences_are_rejected(sequence: int) -> None:
    with pytest.raises(FailureScriptError):
        ScriptedFailure(sequence=sequence, kind=ScriptedFailureKind.TRANSIENT_ERROR)


@pytest.mark.parametrize("kind", list(ScriptedFailureKind))
def test_every_kind_is_constructible_with_its_required_parameters(
    kind: ScriptedFailureKind,
) -> None:
    kwargs: dict[str, int] = {}
    if kind is ScriptedFailureKind.RATE_LIMIT:
        kwargs["retry_after_seconds"] = 2
    if kind in (ScriptedFailureKind.TIMEOUT, ScriptedFailureKind.HANG):
        kwargs["delay_microseconds"] = 1_500_000
    if kind is ScriptedFailureKind.CONNECTION_LOSS:
        kwargs["partial_bytes"] = 64
    failure = ScriptedFailure(sequence=1, kind=kind, **kwargs)
    assert failure.kind is kind
    assert failure.describe()["kind"] == kind.value


def test_rate_limit_requires_bounded_retry_after() -> None:
    with pytest.raises(FailureScriptError, match="retry_after_seconds"):
        ScriptedFailure(sequence=1, kind=ScriptedFailureKind.RATE_LIMIT)
    with pytest.raises(FailureScriptError, match="retry_after_seconds"):
        ScriptedFailure(sequence=1, kind=ScriptedFailureKind.RATE_LIMIT, retry_after_seconds=61)
    accepted = ScriptedFailure(
        sequence=1, kind=ScriptedFailureKind.RATE_LIMIT, retry_after_seconds=60
    )
    assert accepted.retry_after_seconds == 60


def test_timeout_requires_bounded_delay() -> None:
    with pytest.raises(FailureScriptError, match="delay_microseconds"):
        ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TIMEOUT)
    with pytest.raises(FailureScriptError, match="delay_microseconds"):
        ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TIMEOUT, delay_microseconds=60_000_001)


def test_connection_loss_requires_bounded_partial_bytes() -> None:
    with pytest.raises(FailureScriptError, match="partial_bytes"):
        ScriptedFailure(sequence=1, kind=ScriptedFailureKind.CONNECTION_LOSS)
    with pytest.raises(FailureScriptError, match="partial_bytes"):
        ScriptedFailure(sequence=1, kind=ScriptedFailureKind.CONNECTION_LOSS, partial_bytes=65_537)


def test_parameters_are_rejected_on_irrelevant_kinds() -> None:
    with pytest.raises(FailureScriptError, match="only applies"):
        ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TRANSIENT_ERROR, retry_after_seconds=1)
    with pytest.raises(FailureScriptError, match="only applies"):
        ScriptedFailure(
            sequence=1, kind=ScriptedFailureKind.MALFORMED_RESPONSE, delay_microseconds=1
        )
    with pytest.raises(FailureScriptError, match="only applies"):
        ScriptedFailure(sequence=1, kind=ScriptedFailureKind.TRANSIENT_ERROR, partial_bytes=8)


def test_failure_for_matches_exact_sequences_only() -> None:
    script = FailureScript.from_entries([_rate_limit(2), _timeout(5)])
    assert script.failure_for(2) is script.failures[0]
    assert script.failure_for(5) is script.failures[1]
    for sequence in (1, 3, 4, 6, 100):
        assert script.failure_for(sequence) is None


def test_empty_script_never_fails() -> None:
    script = FailureScript.empty()
    assert script.entry_count() == 0
    assert script.failure_for(1) is None
    assert script.to_canonical_bytes() == (
        b'{"failures":[],"format":"paritygrid-scripted-failure","version":1}'
    )


def test_canonical_bytes_round_trip_is_exact() -> None:
    script = FailureScript.from_entries(
        [
            _rate_limit(3),
            ScriptedFailure(
                sequence=8,
                kind=ScriptedFailureKind.CONNECTION_LOSS,
                partial_bytes=128,
            ),
            _timeout(11),
        ]
    )
    parsed = FailureScript.from_canonical_bytes(script.to_canonical_bytes())
    assert parsed == script
    assert parsed.to_canonical_bytes() == script.to_canonical_bytes()


def test_canonical_bytes_are_inspectable_sorted_documents() -> None:
    script = FailureScript.from_entries([_rate_limit(4), _timeout(2)])
    text = script.to_canonical_bytes().decode("ascii")
    document = json.loads(text)
    assert document["format"] == "paritygrid-scripted-failure"
    assert document["version"] == SCRIPTED_FAILURE_VERSION
    assert [entry["sequence"] for entry in document["failures"]] == [2, 4]
    assert json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True) == text


def test_from_canonical_bytes_rejects_unknown_kinds_and_versions() -> None:
    good = json.loads(FailureScript.from_entries([_rate_limit(1)]).to_canonical_bytes())
    unknown_kind = dict(good, failures=[{"sequence": 1, "kind": "explode"}])
    with pytest.raises(FailureScriptError, match="unknown scripted failure kind"):
        FailureScript.from_canonical_bytes(_canonical(unknown_kind))
    wrong_version = dict(good, version=99)
    with pytest.raises(FailureScriptError, match="unsupported version"):
        FailureScript.from_canonical_bytes(_canonical(wrong_version))
    wrong_format = dict(good, format="other")
    with pytest.raises(FailureScriptError, match="unknown format"):
        FailureScript.from_canonical_bytes(_canonical(wrong_format))


def test_from_canonical_bytes_rejects_malformed_payloads() -> None:
    with pytest.raises(FailureScriptError, match="not valid JSON"):
        FailureScript.from_canonical_bytes(b"{not json")
    good = json.loads(FailureScript.from_entries([_rate_limit(1)]).to_canonical_bytes())
    non_int_sequence = dict(good, failures=[{"sequence": "one", "kind": "rate_limit"}])
    with pytest.raises(FailureScriptError, match="must be an integer"):
        FailureScript.from_canonical_bytes(_canonical(non_int_sequence))
    missing_failures = {key: value for key, value in good.items() if key != "failures"}
    with pytest.raises(FailureScriptError, match="failures list"):
        FailureScript.from_canonical_bytes(_canonical(missing_failures))


def test_transport_restriction_rejects_response_shape_kinds() -> None:
    malformed = FailureScript.from_entries(
        [ScriptedFailure(sequence=1, kind=ScriptedFailureKind.MALFORMED_RESPONSE)]
    )
    with pytest.raises(FailureScriptError, match="warehouse scripts may not use"):
        require_transport_script(malformed, subject="warehouse")
    duplicates = FailureScript.from_entries(
        [ScriptedFailure(sequence=1, kind=ScriptedFailureKind.DUPLICATE_RECORDS)]
    )
    with pytest.raises(FailureScriptError):
        require_transport_script(duplicates, subject="warehouse")
    transport_only = FailureScript.from_entries([_rate_limit(1), _timeout(2)])
    assert require_transport_script(transport_only, subject="warehouse") is transport_only


def test_from_entries_rejects_text_input() -> None:
    with pytest.raises(FailureScriptError, match="iterable"):
        FailureScript.from_entries("rate-limit")  # type: ignore[arg-type]


def test_applied_failure_records_sequence_and_kind() -> None:
    applied = AppliedFailure(sequence=4, kind=ScriptedFailureKind.RATE_LIMIT)
    assert applied.sequence == 4
    assert applied.kind is ScriptedFailureKind.RATE_LIMIT


@hypothesis_settings(max_examples=40, deadline=None)
@given(
    sequences=st.lists(
        st.integers(min_value=1, max_value=999), min_size=0, max_size=20, unique=True
    ),
    retry=st.integers(min_value=1, max_value=60),
)
def test_script_serialization_is_deterministic_and_order_independent(
    sequences: list[int], retry: int
) -> None:
    entries = [
        ScriptedFailure(
            sequence=sequence, kind=ScriptedFailureKind.RATE_LIMIT, retry_after_seconds=retry
        )
        for sequence in sequences
    ]
    forward = FailureScript.from_entries(entries)
    backward = FailureScript.from_entries(reversed(entries))
    assert forward == backward
    assert forward.to_canonical_bytes() == backward.to_canonical_bytes()
    reparsed = FailureScript.from_canonical_bytes(forward.to_canonical_bytes())
    assert reparsed == forward
    assert [failure.sequence for failure in reparsed.failures] == sorted(sequences)


def _canonical(document: dict[str, object]) -> bytes:
    return json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode(
        "ascii"
    )
