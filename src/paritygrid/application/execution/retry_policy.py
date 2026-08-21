"""Named deterministic retry decisions for failed work attempts."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from typing import Protocol, cast, runtime_checkable

from paritygrid.domain.execution import (
    FailureClassification,
    FailureDisposition,
    disposition_for,
)
from paritygrid.domain.models import AttemptNumber, Duration, UtcTimestamp, WorkItemId

RETRY_POLICY_VERSION = 1
MAX_RETRY_ATTEMPTS = 3
STANDARD_RETRY_INITIAL_MICROSECONDS = 1_000_000
STANDARD_RETRY_MAX_MICROSECONDS = 60_000_000
SQLITE_RETRY_INITIAL_MICROSECONDS = 10_000
SQLITE_RETRY_MAX_MICROSECONDS = 1_000_000
MAX_HTTP_429_RETRY_DELAY_MICROSECONDS = 86_400_000_000
RETRY_JITTER_DIVISOR = 4
MAX_RETRY_JITTER_SEED = 9_223_372_036_854_775_807


class RetryPolicyError(Exception):
    """Base class for typed retry-policy failures."""


class RetryPolicyInvalidRequestError(RetryPolicyError, ValueError):
    """Raised when exact retry inputs violate the named policy contract."""


class RetryPolicyClockError(RetryPolicyError):
    """Raised when retry scheduling cannot obtain a trustworthy instant."""


class RetryPolicyJitterError(RetryPolicyError):
    """Raised when a jitter source fails or returns invalid evidence."""


class RetryPolicyName(StrEnum):
    """Stable names for closed retry behavior."""

    BOUNDED_EXPONENTIAL_V1 = "bounded_exponential_v1"


class RetryDecisionKind(StrEnum):
    """Closed outcomes returned by a named retry policy."""

    SCHEDULED = "scheduled"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class Http429RetryDelay:
    """A parsed and validated HTTP 429 minimum delay.

    P7.5 owns parsing raw ``Retry-After`` values. This type is the only
    rate-limit input accepted by the P6.5 policy.
    """

    duration: Duration

    def __post_init__(self) -> None:
        duration = _snapshot_duration(self.duration, "HTTP 429 retry delay")
        if duration.microseconds > MAX_HTTP_429_RETRY_DELAY_MICROSECONDS:
            raise RetryPolicyInvalidRequestError("HTTP 429 retry delay exceeds the policy bound")


@dataclass(frozen=True, slots=True, repr=False)
class RetryPolicyRequest:
    """Exact evidence needed to decide the next action for one failed attempt."""

    work_item_id: WorkItemId
    attempt_number: AttemptNumber
    classification: FailureClassification
    failed_at: UtcTimestamp
    http_429_delay: Http429RetryDelay | None = None

    def __post_init__(self) -> None:
        _validate_request_fields(
            self.work_item_id,
            self.attempt_number,
            self.classification,
            self.failed_at,
            self.http_429_delay,
        )

    def __repr__(self) -> str:
        return (
            "RetryPolicyRequest("
            f"work_item_id={self.work_item_id!r}, attempt_number={self.attempt_number!r}, "
            f"classification={self.classification!r}, failed_at={self.failed_at!r}, "
            f"http_429_delay={self.http_429_delay!r})"
        )


@dataclass(frozen=True, slots=True, repr=False)
class RetryScheduledDecision:
    """A bounded future retry derived from exact policy evidence."""

    policy_name: RetryPolicyName
    work_item_id: WorkItemId
    attempt_number: AttemptNumber
    classification: FailureClassification
    failed_at: UtcTimestamp
    observed_at: UtcTimestamp
    http_429_delay: Http429RetryDelay | None
    jitter: Duration
    delay: Duration
    retry_available_at: UtcTimestamp

    def __post_init__(self) -> None:
        attempt_number, classification, failed_at = _validate_decision_identity(
            self.policy_name,
            self.work_item_id,
            self.attempt_number,
            self.classification,
            self.failed_at,
        )
        observed_at = _snapshot_timestamp(self.observed_at, "retry observation time")
        http_429_delay = _snapshot_http_429_delay(self.http_429_delay, classification)
        jitter = _snapshot_duration(self.jitter, "retry jitter")
        delay = _snapshot_duration(self.delay, "retry delay")
        available_at = _snapshot_timestamp(
            self.retry_available_at,
            "retry availability time",
        )
        if disposition_for(classification) is not FailureDisposition.RETRY:
            raise RetryPolicyInvalidRequestError("scheduled retry classification is not retryable")
        if int(attempt_number) >= MAX_RETRY_ATTEMPTS:
            raise RetryPolicyInvalidRequestError("scheduled retry exceeds the attempt bound")
        if observed_at < failed_at:
            raise RetryPolicyInvalidRequestError("retry observation cannot precede failure")
        base, jitter_bound = _retry_envelope(
            classification,
            attempt_number,
            http_429_delay,
        )
        if jitter.microseconds > jitter_bound:
            raise RetryPolicyInvalidRequestError("retry jitter exceeds the v1 policy envelope")
        if delay.microseconds != base + jitter.microseconds:
            raise RetryPolicyInvalidRequestError(
                "scheduled retry delay violates v1 policy arithmetic"
            )
        if _add_delay(observed_at, delay) != available_at:
            raise RetryPolicyInvalidRequestError(
                "retry availability must equal observation time plus delay"
            )

    @property
    def kind(self) -> RetryDecisionKind:
        return RetryDecisionKind.SCHEDULED

    @property
    def disposition(self) -> FailureDisposition:
        return FailureDisposition.RETRY

    @property
    def exhausted(self) -> bool:
        return False

    def __repr__(self) -> str:
        return (
            "RetryScheduledDecision("
            f"policy_name={self.policy_name!r}, work_item_id={self.work_item_id!r}, "
            f"attempt_number={self.attempt_number!r}, classification={self.classification!r}, "
            f"failed_at={self.failed_at!r}, observed_at={self.observed_at!r}, "
            f"http_429_delay={self.http_429_delay!r}, jitter={self.jitter!r}, "
            f"delay={self.delay!r}, retry_available_at={self.retry_available_at!r})"
        )


@dataclass(frozen=True, slots=True, repr=False)
class RetryStoppedDecision:
    """A terminal policy decision for an ineligible or exhausted failure."""

    policy_name: RetryPolicyName
    work_item_id: WorkItemId
    attempt_number: AttemptNumber
    classification: FailureClassification
    failed_at: UtcTimestamp
    disposition: FailureDisposition
    exhausted: bool

    def __post_init__(self) -> None:
        attempt_number, classification, _ = _validate_decision_identity(
            self.policy_name,
            self.work_item_id,
            self.attempt_number,
            self.classification,
            self.failed_at,
        )
        disposition = _require_exact(
            self.disposition,
            FailureDisposition,
            "stopped retry disposition",
        )
        if type(self.exhausted) is not bool:
            raise TypeError("retry exhaustion marker must be a boolean")
        base_disposition = disposition_for(classification)
        if base_disposition is FailureDisposition.RETRY:
            if (
                not self.exhausted
                or disposition is not FailureDisposition.PERMANENT
                or int(attempt_number) < MAX_RETRY_ATTEMPTS
            ):
                raise RetryPolicyInvalidRequestError(
                    "retryable failure may stop only after attempt exhaustion"
                )
        elif self.exhausted or disposition is not base_disposition:
            raise RetryPolicyInvalidRequestError(
                "non-retryable failure must preserve its domain disposition"
            )

    @property
    def kind(self) -> RetryDecisionKind:
        return RetryDecisionKind.STOPPED

    def __repr__(self) -> str:
        return (
            "RetryStoppedDecision("
            f"policy_name={self.policy_name!r}, work_item_id={self.work_item_id!r}, "
            f"attempt_number={self.attempt_number!r}, classification={self.classification!r}, "
            f"failed_at={self.failed_at!r}, disposition={self.disposition!r}, "
            f"exhausted={self.exhausted!r})"
        )


type RetryDecision = RetryScheduledDecision | RetryStoppedDecision


@runtime_checkable
class RetryClock(Protocol):
    """Borrowed clock used only while deriving a scheduled retry."""

    def now(self) -> UtcTimestamp:
        """Return the current exact UTC instant."""
        ...


@runtime_checkable
class RetryJitterSource(Protocol):
    """Borrowed deterministic keyed jitter source."""

    def sample(
        self,
        *,
        work_item_id: WorkItemId,
        classification: FailureClassification,
        attempt_number: AttemptNumber,
        upper_bound: Duration,
    ) -> Duration:
        """Return a duration in the inclusive range ``0..upper_bound``."""
        ...


@runtime_checkable
class NamedRetryPolicy(Protocol):
    """Borrowed interface for a stable named retry policy."""

    @property
    def name(self) -> RetryPolicyName:
        """Return the stable policy name."""
        ...

    def decide(self, request: RetryPolicyRequest) -> RetryDecision:
        """Return one closed retry decision without performing side effects."""
        ...


@dataclass(frozen=True, slots=True, repr=False)
class SeededRetryJitterSource:
    """Call-order-independent SHA-256 jitter for reproducible executions."""

    seed: int

    def __post_init__(self) -> None:
        _validate_jitter_seed(self.seed)

    def sample(
        self,
        *,
        work_item_id: WorkItemId,
        classification: FailureClassification,
        attempt_number: AttemptNumber,
        upper_bound: Duration,
    ) -> Duration:
        seed = _validate_jitter_seed(self.seed)
        identity = _snapshot_work_item_id(work_item_id)
        selected_classification = _require_exact(
            classification,
            FailureClassification,
            "retry jitter classification",
        )
        selected_attempt = _snapshot_attempt_number(attempt_number)
        bound = _snapshot_duration(upper_bound, "retry jitter upper bound")
        payload = b"\x00".join(
            (
                b"paritygrid-retry-jitter-v1",
                str(seed).encode("ascii"),
                bytes(identity),
                selected_classification.value.encode("ascii"),
                bytes(selected_attempt),
                str(bound.microseconds).encode("ascii"),
            )
        )
        value = int.from_bytes(sha256(payload).digest()[:8], "big") % (bound.microseconds + 1)
        return Duration(value)

    def __repr__(self) -> str:
        return "SeededRetryJitterSource(seed=<redacted>)"


class BoundedExponentialRetryPolicy:
    """Reference P6.5 policy with fixed, versioned retry behavior."""

    __slots__ = ("_clock", "_jitter_source")

    def __init__(self, clock: RetryClock, jitter_source: RetryJitterSource) -> None:
        clock_value = cast(object, clock)
        jitter_value = cast(object, jitter_source)
        if not isinstance(clock_value, RetryClock):
            raise TypeError("retry clock must implement RetryClock")
        if not isinstance(jitter_value, RetryJitterSource):
            raise TypeError("retry jitter source must implement RetryJitterSource")
        self._clock = clock
        self._jitter_source = jitter_source

    @property
    def name(self) -> RetryPolicyName:
        return RetryPolicyName.BOUNDED_EXPONENTIAL_V1

    def decide(self, request: RetryPolicyRequest) -> RetryDecision:
        selected = _snapshot_request(request)
        base_disposition = disposition_for(selected.classification)
        if base_disposition is not FailureDisposition.RETRY:
            return RetryStoppedDecision(
                self.name,
                selected.work_item_id,
                selected.attempt_number,
                selected.classification,
                selected.failed_at,
                base_disposition,
                False,
            )
        if int(selected.attempt_number) >= MAX_RETRY_ATTEMPTS:
            return RetryStoppedDecision(
                self.name,
                selected.work_item_id,
                selected.attempt_number,
                selected.classification,
                selected.failed_at,
                FailureDisposition.PERMANENT,
                True,
            )

        observed_at = self._now(selected.failed_at)
        delay, jitter = self._delay(selected)
        return RetryScheduledDecision(
            self.name,
            selected.work_item_id,
            selected.attempt_number,
            selected.classification,
            selected.failed_at,
            observed_at,
            selected.http_429_delay,
            jitter,
            delay,
            self._available_at(observed_at, delay),
        )

    def _now(self, failed_at: UtcTimestamp) -> UtcTimestamp:
        failed = False
        try:
            value = self._clock.now()
        except Exception:
            failed = True
            value = None
        if failed or type(value) is not UtcTimestamp:
            raise RetryPolicyClockError("retry policy clock failed")
        observed_at = _snapshot_clock_timestamp(value)
        if observed_at < failed_at:
            raise RetryPolicyClockError("retry policy clock precedes the failed attempt")
        return observed_at

    def _delay(self, request: RetryPolicyRequest) -> tuple[Duration, Duration]:
        base, jitter_bound = _retry_envelope(
            request.classification,
            request.attempt_number,
            request.http_429_delay,
        )
        jitter = self._jitter(
            request.work_item_id,
            request.classification,
            request.attempt_number,
            Duration(jitter_bound),
        )
        return Duration(base + jitter.microseconds), jitter

    def _jitter(
        self,
        work_item_id: WorkItemId,
        classification: FailureClassification,
        attempt_number: AttemptNumber,
        upper_bound: Duration,
    ) -> Duration:
        failed = False
        try:
            source_work_item_id = _snapshot_work_item_id(work_item_id)
            source_attempt_number = _snapshot_attempt_number(attempt_number)
            source_upper_bound = _snapshot_duration(upper_bound, "retry jitter upper bound")
            value = self._jitter_source.sample(
                work_item_id=source_work_item_id,
                classification=classification,
                attempt_number=source_attempt_number,
                upper_bound=source_upper_bound,
            )
        except Exception:
            failed = True
            value = None
        if failed or type(value) is not Duration:
            raise RetryPolicyJitterError("retry jitter source failed")
        invalid = False
        try:
            inner_value = value.microseconds
            if type(inner_value) is not int:
                raise TypeError
            jitter = Duration(inner_value)
        except Exception:
            invalid = True
            jitter = None
        if invalid or jitter is None:
            raise RetryPolicyJitterError("retry jitter source failed")
        if jitter > upper_bound:
            raise RetryPolicyJitterError("retry jitter exceeds its requested bound")
        return jitter

    @staticmethod
    def _available_at(observed_at: UtcTimestamp, delay: Duration) -> UtcTimestamp:
        try:
            return _add_delay(observed_at, delay)
        except RetryPolicyInvalidRequestError:
            raise RetryPolicyClockError("retry availability exceeds timestamp bounds") from None

    def __repr__(self) -> str:
        return (
            "BoundedExponentialRetryPolicy("
            f"name={self.name!r}, clock=<redacted>, jitter_source=<redacted>)"
        )


def _snapshot_request(value: object) -> RetryPolicyRequest:
    request = _require_exact(value, RetryPolicyRequest, "retry policy request")
    identity = _snapshot_work_item_id(request.work_item_id)
    attempt = _snapshot_attempt_number(request.attempt_number)
    classification = _require_exact(
        request.classification,
        FailureClassification,
        "retry failure classification",
    )
    failed_at = _snapshot_request_timestamp(request.failed_at)
    delay = _snapshot_http_429_delay(request.http_429_delay, classification)
    return RetryPolicyRequest(identity, attempt, classification, failed_at, delay)


def _validate_request_fields(
    work_item_id: object,
    attempt_number: object,
    classification: object,
    failed_at: object,
    http_429_delay: object,
) -> None:
    _snapshot_work_item_id(work_item_id)
    _snapshot_attempt_number(attempt_number)
    selected = _require_exact(
        classification,
        FailureClassification,
        "retry failure classification",
    )
    _snapshot_request_timestamp(failed_at)
    _snapshot_http_429_delay(http_429_delay, selected)


def _validate_decision_identity(
    policy_name: object,
    work_item_id: object,
    attempt_number: object,
    classification: object,
    failed_at: object,
) -> tuple[AttemptNumber, FailureClassification, UtcTimestamp]:
    _require_exact(policy_name, RetryPolicyName, "retry policy name")
    _snapshot_work_item_id(work_item_id)
    attempt = _snapshot_attempt_number(attempt_number)
    selected_classification = _require_exact(
        classification,
        FailureClassification,
        "retry failure classification",
    )
    failure_time = _snapshot_request_timestamp(failed_at)
    return attempt, selected_classification, failure_time


def _profile_for(classification: FailureClassification) -> tuple[int, int]:
    if classification is FailureClassification.SQLITE_CONTENTION:
        return SQLITE_RETRY_INITIAL_MICROSECONDS, SQLITE_RETRY_MAX_MICROSECONDS
    return STANDARD_RETRY_INITIAL_MICROSECONDS, STANDARD_RETRY_MAX_MICROSECONDS


def _retry_envelope(
    classification: FailureClassification,
    attempt_number: AttemptNumber,
    http_429_delay: Http429RetryDelay | None,
) -> tuple[int, int]:
    initial, profile_cap = _profile_for(classification)
    exponent = int(attempt_number) - 1
    base = min(profile_cap, initial * (1 << exponent))
    overall_cap = profile_cap
    if http_429_delay is not None:
        base = max(base, http_429_delay.duration.microseconds)
        overall_cap = MAX_HTTP_429_RETRY_DELAY_MICROSECONDS
    return base, min(overall_cap - base, base // RETRY_JITTER_DIVISOR)


def _add_delay(observed_at: UtcTimestamp, delay: Duration) -> UtcTimestamp:
    try:
        return UtcTimestamp(observed_at.to_datetime() + timedelta(microseconds=delay.microseconds))
    except OverflowError, TypeError, ValueError:
        raise RetryPolicyInvalidRequestError(
            "retry availability exceeds timestamp bounds"
        ) from None


def _snapshot_work_item_id(value: object) -> WorkItemId:
    selected = _require_exact(value, WorkItemId, "retry work-item identity")
    try:
        inner_value = selected.value
        if type(inner_value) is not str:
            raise TypeError
        return WorkItemId(inner_value)
    except Exception:
        raise RetryPolicyInvalidRequestError("retry work-item identity is invalid") from None


def _snapshot_attempt_number(value: object) -> AttemptNumber:
    selected = _require_exact(value, AttemptNumber, "retry attempt number")
    try:
        inner_value = selected.number
        if type(inner_value) is not int:
            raise TypeError
        return AttemptNumber(inner_value)
    except Exception:
        raise RetryPolicyInvalidRequestError("retry attempt number is invalid") from None


def _snapshot_duration(value: object, subject: str) -> Duration:
    selected = _require_exact(value, Duration, subject)
    try:
        inner_value = selected.microseconds
        if type(inner_value) is not int:
            raise TypeError
        return Duration(inner_value)
    except Exception:
        raise RetryPolicyInvalidRequestError(f"{subject} is invalid") from None


def _snapshot_request_timestamp(value: object) -> UtcTimestamp:
    return _snapshot_timestamp(value, "retry failure time")


def _snapshot_timestamp(value: object, subject: str) -> UtcTimestamp:
    selected = _require_exact(value, UtcTimestamp, subject)
    try:
        inner_value = selected.value
        if type(inner_value) is not datetime:
            raise TypeError
        return UtcTimestamp(inner_value)
    except Exception:
        raise RetryPolicyInvalidRequestError(f"{subject} is invalid") from None


def _snapshot_clock_timestamp(value: UtcTimestamp) -> UtcTimestamp:
    try:
        inner_value = value.value
        if type(inner_value) is not datetime:
            raise TypeError
        return UtcTimestamp(inner_value)
    except Exception:
        raise RetryPolicyClockError("retry policy clock failed") from None


def _snapshot_http_429_delay(
    value: object,
    classification: FailureClassification,
) -> Http429RetryDelay | None:
    if value is None:
        return None
    delay = _require_exact(value, Http429RetryDelay, "HTTP 429 retry delay")
    if classification is not FailureClassification.HTTP_429:
        raise RetryPolicyInvalidRequestError(
            "HTTP 429 retry delay requires the HTTP_429 classification"
        )
    return Http429RetryDelay(_snapshot_duration(delay.duration, "HTTP 429 retry delay"))


def _validate_jitter_seed(value: object) -> int:
    seed = value
    if type(seed) is not int:
        raise TypeError("retry jitter seed must be an integer")
    if not 0 <= seed <= MAX_RETRY_JITTER_SEED:
        raise RetryPolicyInvalidRequestError("retry jitter seed is outside policy bounds")
    return seed


def _require_exact[T](value: object, expected: type[T], subject: str) -> T:
    if type(value) is not expected:
        raise TypeError(f"{subject} must use {expected.__name__}")
    return cast(T, value)


__all__ = [
    "MAX_HTTP_429_RETRY_DELAY_MICROSECONDS",
    "MAX_RETRY_ATTEMPTS",
    "MAX_RETRY_JITTER_SEED",
    "RETRY_JITTER_DIVISOR",
    "RETRY_POLICY_VERSION",
    "SQLITE_RETRY_INITIAL_MICROSECONDS",
    "SQLITE_RETRY_MAX_MICROSECONDS",
    "STANDARD_RETRY_INITIAL_MICROSECONDS",
    "STANDARD_RETRY_MAX_MICROSECONDS",
    "BoundedExponentialRetryPolicy",
    "Http429RetryDelay",
    "NamedRetryPolicy",
    "RetryClock",
    "RetryDecision",
    "RetryDecisionKind",
    "RetryJitterSource",
    "RetryPolicyClockError",
    "RetryPolicyError",
    "RetryPolicyInvalidRequestError",
    "RetryPolicyJitterError",
    "RetryPolicyName",
    "RetryPolicyRequest",
    "RetryScheduledDecision",
    "RetryStoppedDecision",
    "SeededRetryJitterSource",
]
