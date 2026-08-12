"""Exhaustive verification of failure classification dispositions."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from paritygrid.domain.execution import (
    FAILURE_DISPOSITIONS,
    FailureClassification,
    FailureDisposition,
    disposition_for,
)

EXPECTED_DISPOSITIONS = {
    FailureClassification.CONNECTION: FailureDisposition.RETRY,
    FailureClassification.TIMEOUT: FailureDisposition.RETRY,
    FailureClassification.HTTP_429: FailureDisposition.RETRY,
    FailureClassification.HTTP_5XX: FailureDisposition.RETRY,
    FailureClassification.HTTP_4XX: FailureDisposition.PERMANENT,
    FailureClassification.VALIDATION: FailureDisposition.QUARANTINE,
    FailureClassification.IDEMPOTENCY_CONFLICT: FailureDisposition.CONFLICT,
    FailureClassification.SQLITE_CONTENTION: FailureDisposition.RETRY,
    FailureClassification.USER_CANCELLATION: FailureDisposition.CANCEL,
    FailureClassification.UNKNOWN: FailureDisposition.PERMANENT,
}


@pytest.mark.parametrize(
    ("classification", "expected"),
    EXPECTED_DISPOSITIONS.items(),
)
def test_each_failure_classification_has_the_expected_disposition(
    classification: FailureClassification, expected: FailureDisposition
) -> None:
    assert disposition_for(classification) is expected
    assert FAILURE_DISPOSITIONS[classification] is expected


def test_failure_mapping_is_complete_for_the_closed_classification() -> None:
    assert set(FAILURE_DISPOSITIONS) == set(FailureClassification)
    assert dict(FAILURE_DISPOSITIONS) == EXPECTED_DISPOSITIONS
    assert set(FAILURE_DISPOSITIONS.values()) == set(FailureDisposition)


def test_failure_classification_and_disposition_have_stable_values() -> None:
    assert FailureClassification("http_429") is FailureClassification.HTTP_429
    assert FailureClassification("http_5xx") is FailureClassification.HTTP_5XX
    assert FailureClassification("http_4xx") is FailureClassification.HTTP_4XX
    assert FailureDisposition("quarantine") is FailureDisposition.QUARANTINE


def test_disposition_rejects_an_unclassified_value() -> None:
    with pytest.raises(TypeError, match="FailureClassification"):
        disposition_for("timeout")


@given(st.sampled_from(tuple(FailureClassification)))
def test_every_generated_classification_has_exactly_one_disposition(
    classification: FailureClassification,
) -> None:
    assert disposition_for(classification) is EXPECTED_DISPOSITIONS[classification]
