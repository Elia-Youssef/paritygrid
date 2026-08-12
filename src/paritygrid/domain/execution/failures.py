"""Closed failure classifications and their execution dispositions."""

from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType


class FailureClassification(StrEnum):
    """Stable classification of a failed execution attempt."""

    CONNECTION = "connection"
    TIMEOUT = "timeout"
    HTTP_429 = "http_429"
    HTTP_5XX = "http_5xx"
    HTTP_4XX = "http_4xx"
    VALIDATION = "validation"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    SQLITE_CONTENTION = "sqlite_contention"
    USER_CANCELLATION = "user_cancellation"
    UNKNOWN = "unknown"


class FailureDisposition(StrEnum):
    """Stable scheduler-level outcome for a classified failure."""

    RETRY = "retry"
    QUARANTINE = "quarantine"
    CONFLICT = "conflict"
    CANCEL = "cancel"
    PERMANENT = "permanent"


FAILURE_DISPOSITIONS: Mapping[FailureClassification, FailureDisposition] = MappingProxyType(
    {
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
)


def disposition_for(classification: object) -> FailureDisposition:
    """Return the execution disposition for a closed failure classification."""
    if not isinstance(classification, FailureClassification):
        raise TypeError("classification must be a FailureClassification")
    return FAILURE_DISPOSITIONS[classification]
