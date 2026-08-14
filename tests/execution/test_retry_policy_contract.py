"""Exhaustive public-contract tests for the named P6.5 retry policy."""

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from paritygrid.application.execution import (
    MAX_HTTP_429_RETRY_DELAY_MICROSECONDS,
    MAX_RETRY_ATTEMPTS,
    MAX_RETRY_JITTER_SEED,
    RETRY_JITTER_DIVISOR,
    RETRY_POLICY_VERSION,
    SQLITE_RETRY_INITIAL_MICROSECONDS,
    SQLITE_RETRY_MAX_MICROSECONDS,
    STANDARD_RETRY_INITIAL_MICROSECONDS,
    STANDARD_RETRY_MAX_MICROSECONDS,
    BoundedExponentialRetryPolicy,
    Http429RetryDelay,
    NamedRetryPolicy,
    RetryClock,
    RetryDecisionKind,
    RetryJitterSource,
    RetryPolicyClockError,
    RetryPolicyInvalidRequestError,
    RetryPolicyJitterError,
    RetryPolicyName,
    RetryPolicyRequest,
    RetryScheduledDecision,
    RetryStoppedDecision,
    SeededRetryJitterSource,
)
from paritygrid.domain.execution import (
    FailureClassification,
    FailureDisposition,
    disposition_for,
)
from paritygrid.domain.models import AttemptNumber, Duration, UtcTimestamp, WorkItemId

_BASE = datetime(2026, 8, 14, 12, tzinfo=UTC)
_WORK_ITEM_ID = WorkItemId("wrk_retry-item")
_RETRYABLE = tuple(
    classification
    for classification in FailureClassification
    if disposition_for(classification) is FailureDisposition.RETRY
)
_NON_RETRYABLE = tuple(
    classification
    for classification in FailureClassification
    if disposition_for(classification) is not FailureDisposition.RETRY
)


class _Fatal(BaseException):
    pass


class _Clock:
    def __init__(self, value: object) -> None:
        self.value = value
        self.calls = 0

    def now(self) -> UtcTimestamp:
        self.calls += 1
        if isinstance(self.value, BaseException):
            raise self.value
        return cast(UtcTimestamp, self.value)


class _Jitter:
    def __init__(self, value: object = Duration(0), *, return_upper: bool = False) -> None:
        self.value = value
        self.return_upper = return_upper
        self.calls: list[tuple[WorkItemId, FailureClassification, AttemptNumber, Duration]] = []

    def sample(
        self,
        *,
        work_item_id: WorkItemId,
        classification: FailureClassification,
        attempt_number: AttemptNumber,
        upper_bound: Duration,
    ) -> Duration:
        self.calls.append((work_item_id, classification, attempt_number, upper_bound))
        if isinstance(self.value, BaseException):
            raise self.value
        if self.return_upper:
            return upper_bound
        return cast(Duration, self.value)


class _MutatingJitter:
    def sample(
        self,
        *,
        work_item_id: WorkItemId,
        classification: FailureClassification,
        attempt_number: AttemptNumber,
        upper_bound: Duration,
    ) -> Duration:
        del classification
        object.__setattr__(work_item_id, "value", "credential=forged")
        object.__setattr__(attempt_number, "number", 99)
        object.__setattr__(upper_bound, "microseconds", 99_000_000)
        return Duration(0)


class _ClockCanary:
    def now(self) -> UtcTimestamp:
        return _timestamp()

    def __repr__(self) -> str:
        return "credential=should-not-appear"


class _JitterCanary:
    def sample(
        self,
        *,
        work_item_id: WorkItemId,
        classification: FailureClassification,
        attempt_number: AttemptNumber,
        upper_bound: Duration,
    ) -> Duration:
        del work_item_id, classification, attempt_number, upper_bound
        return Duration(0)

    def __repr__(self) -> str:
        return "credential=should-not-appear"


def _timestamp(microseconds: int = 0) -> UtcTimestamp:
    return UtcTimestamp(_BASE + timedelta(microseconds=microseconds))


def _request(
    classification: FailureClassification = FailureClassification.CONNECTION,
    *,
    attempt: int = 1,
    failed_at: UtcTimestamp | None = None,
    http_429_delay: Http429RetryDelay | None = None,
) -> RetryPolicyRequest:
    return RetryPolicyRequest(
        WorkItemId("wrk_retry-item"),
        AttemptNumber(attempt),
        classification,
        failed_at or _timestamp(),
        http_429_delay,
    )


def _policy(
    *,
    clock: _Clock | None = None,
    jitter: _Jitter | None = None,
) -> tuple[BoundedExponentialRetryPolicy, _Clock, _Jitter]:
    selected_clock = clock or _Clock(_timestamp(5_000_000))
    selected_jitter = jitter or _Jitter()
    return (
        BoundedExponentialRetryPolicy(selected_clock, selected_jitter),
        selected_clock,
        selected_jitter,
    )


def test_named_policy_constants_protocols_and_redacted_repr_are_stable() -> None:
    assert RETRY_POLICY_VERSION == 1
    assert MAX_RETRY_ATTEMPTS == 3
    assert STANDARD_RETRY_INITIAL_MICROSECONDS == 1_000_000
    assert STANDARD_RETRY_MAX_MICROSECONDS == 60_000_000
    assert SQLITE_RETRY_INITIAL_MICROSECONDS == 10_000
    assert SQLITE_RETRY_MAX_MICROSECONDS == 1_000_000
    assert MAX_HTTP_429_RETRY_DELAY_MICROSECONDS == 86_400_000_000
    assert RETRY_JITTER_DIVISOR == 4
    assert MAX_RETRY_JITTER_SEED == 9_223_372_036_854_775_807
    policy, clock, jitter = _policy()
    assert policy.name is RetryPolicyName.BOUNDED_EXPONENTIAL_V1
    assert isinstance(policy, NamedRetryPolicy)
    assert isinstance(clock, RetryClock)
    assert isinstance(jitter, RetryJitterSource)
    assert repr(policy) == (
        "BoundedExponentialRetryPolicy("
        "name=<RetryPolicyName.BOUNDED_EXPONENTIAL_V1: 'bounded_exponential_v1'>, "
        "clock=<redacted>, jitter_source=<redacted>)"
    )
    canary_policy = BoundedExponentialRetryPolicy(
        _ClockCanary(),
        _JitterCanary(),
    )
    assert "credential" not in repr(canary_policy)


@pytest.mark.parametrize("classification", tuple(FailureClassification))
def test_policy_exhaustively_uses_the_authoritative_classification_mapping(
    classification: FailureClassification,
) -> None:
    policy, clock, jitter = _policy()
    decision = policy.decide(_request(classification))
    base_disposition = disposition_for(classification)
    if base_disposition is FailureDisposition.RETRY:
        assert type(decision) is RetryScheduledDecision
        assert decision.kind is RetryDecisionKind.SCHEDULED
        assert decision.disposition is FailureDisposition.RETRY
        assert decision.exhausted is False
        expected = (
            SQLITE_RETRY_INITIAL_MICROSECONDS
            if classification is FailureClassification.SQLITE_CONTENTION
            else STANDARD_RETRY_INITIAL_MICROSECONDS
        )
        assert decision.delay == Duration(expected)
        assert decision.retry_available_at == _timestamp(5_000_000 + expected)
        assert clock.calls == 1
        assert len(jitter.calls) == 1
    else:
        assert type(decision) is RetryStoppedDecision
        assert decision.kind is RetryDecisionKind.STOPPED
        assert decision.disposition is base_disposition
        assert decision.exhausted is False
        assert clock.calls == 0
        assert jitter.calls == []


@pytest.mark.parametrize("classification", _RETRYABLE)
def test_retryable_classifications_stop_permanently_at_attempt_exhaustion(
    classification: FailureClassification,
) -> None:
    policy, clock, jitter = _policy()
    decision = policy.decide(_request(classification, attempt=MAX_RETRY_ATTEMPTS))
    assert decision == RetryStoppedDecision(
        RetryPolicyName.BOUNDED_EXPONENTIAL_V1,
        _WORK_ITEM_ID,
        AttemptNumber(MAX_RETRY_ATTEMPTS),
        classification,
        _timestamp(),
        FailureDisposition.PERMANENT,
        True,
    )
    assert clock.calls == 0
    assert jitter.calls == []


def test_very_large_attempt_stops_before_exponential_arithmetic_or_ports() -> None:
    policy, clock, jitter = _policy()
    decision = policy.decide(_request(attempt=2_147_483_647))
    assert type(decision) is RetryStoppedDecision
    assert decision.exhausted is True
    assert clock.calls == 0
    assert jitter.calls == []


@pytest.mark.parametrize(
    ("classification", "http_delay", "expected_base", "expected_jitter"),
    [
        (FailureClassification.CONNECTION, None, 2_000_000, 500_000),
        (FailureClassification.TIMEOUT, None, 2_000_000, 500_000),
        (FailureClassification.HTTP_5XX, None, 2_000_000, 500_000),
        (FailureClassification.SQLITE_CONTENTION, None, 20_000, 5_000),
        (
            FailureClassification.HTTP_429,
            Http429RetryDelay(Duration(10_000_000)),
            10_000_000,
            2_500_000,
        ),
    ],
)
def test_second_attempt_uses_exact_integer_backoff_and_maximum_additive_jitter(
    classification: FailureClassification,
    http_delay: Http429RetryDelay | None,
    expected_base: int,
    expected_jitter: int,
) -> None:
    jitter = _Jitter(return_upper=True)
    policy, _, _ = _policy(jitter=jitter)
    decision = policy.decide(_request(classification, attempt=2, http_429_delay=http_delay))
    assert type(decision) is RetryScheduledDecision
    assert decision.delay == Duration(expected_base + expected_jitter)
    assert jitter.calls == [
        (
            _WORK_ITEM_ID,
            classification,
            AttemptNumber(2),
            Duration(expected_jitter),
        )
    ]


def test_http_429_maximum_delay_leaves_zero_bounded_jitter_headroom() -> None:
    jitter = _Jitter(return_upper=True)
    policy, _, _ = _policy(jitter=jitter)
    delay = Http429RetryDelay(Duration(MAX_HTTP_429_RETRY_DELAY_MICROSECONDS))
    decision = policy.decide(_request(FailureClassification.HTTP_429, http_429_delay=delay))
    assert type(decision) is RetryScheduledDecision
    assert decision.delay == delay.duration
    assert jitter.calls[0][3] == Duration(0)


def test_http_429_delay_is_typed_bounded_and_classification_specific() -> None:
    assert Http429RetryDelay(Duration(0)).duration == Duration(0)
    assert Http429RetryDelay(Duration(MAX_HTTP_429_RETRY_DELAY_MICROSECONDS)).duration == Duration(
        MAX_HTTP_429_RETRY_DELAY_MICROSECONDS
    )
    with pytest.raises(RetryPolicyInvalidRequestError, match="exceeds"):
        Http429RetryDelay(Duration(MAX_HTTP_429_RETRY_DELAY_MICROSECONDS + 1))
    with pytest.raises(TypeError, match="Duration"):
        Http429RetryDelay(cast(Any, 3))
    with pytest.raises(RetryPolicyInvalidRequestError, match="HTTP_429"):
        _request(
            FailureClassification.CONNECTION,
            http_429_delay=Http429RetryDelay(Duration(1)),
        )
    with pytest.raises(TypeError, match="Http429RetryDelay"):
        RetryPolicyRequest(
            _WORK_ITEM_ID,
            AttemptNumber(1),
            FailureClassification.HTTP_429,
            _timestamp(),
            cast(Any, "120"),
        )


@pytest.mark.parametrize(
    "factory",
    [
        lambda: RetryPolicyRequest(
            cast(Any, "wrk_retry-item"),
            AttemptNumber(1),
            FailureClassification.CONNECTION,
            _timestamp(),
        ),
        lambda: RetryPolicyRequest(
            _WORK_ITEM_ID,
            cast(Any, 1),
            FailureClassification.CONNECTION,
            _timestamp(),
        ),
        lambda: RetryPolicyRequest(
            _WORK_ITEM_ID,
            AttemptNumber(1),
            cast(Any, "connection"),
            _timestamp(),
        ),
        lambda: RetryPolicyRequest(
            _WORK_ITEM_ID,
            AttemptNumber(1),
            FailureClassification.CONNECTION,
            cast(Any, _BASE),
        ),
    ],
)
def test_request_rejects_substituted_runtime_types(factory: Any) -> None:
    with pytest.raises(TypeError):
        factory()


def test_request_is_frozen_and_has_a_bounded_payload_free_repr() -> None:
    request = _request(
        FailureClassification.HTTP_429,
        http_429_delay=Http429RetryDelay(Duration(4_000_000)),
    )
    text = repr(request)
    assert "RetryPolicyRequest" in text
    assert "http_429_delay" in text
    with pytest.raises(FrozenInstanceError):
        request.failed_at = _timestamp(1)  # type: ignore[misc]


def test_policy_snapshots_and_rejects_reflectively_corrupted_request_values() -> None:
    policy, _, _ = _policy()

    bad_identity = _request()
    object.__setattr__(bad_identity.work_item_id, "value", "credential=canary")
    with pytest.raises(RetryPolicyInvalidRequestError, match="identity") as identity_error:
        policy.decide(bad_identity)
    assert "credential" not in str(identity_error.value)

    bad_attempt = _request()
    object.__setattr__(bad_attempt.attempt_number, "number", "credential=canary")
    with pytest.raises(RetryPolicyInvalidRequestError, match="attempt") as attempt_error:
        policy.decide(bad_attempt)
    assert "credential" not in str(attempt_error.value)

    bad_classification = _request()
    object.__setattr__(bad_classification, "classification", "credential=canary")
    with pytest.raises(TypeError, match="FailureClassification"):
        policy.decide(bad_classification)

    bad_time = _request()
    object.__setattr__(bad_time.failed_at, "value", "credential=canary")
    with pytest.raises(RetryPolicyInvalidRequestError, match="failure time") as time_error:
        policy.decide(bad_time)
    assert "credential" not in str(time_error.value)

    bad_delay_type = _request(FailureClassification.HTTP_429)
    object.__setattr__(bad_delay_type, "http_429_delay", "credential=canary")
    with pytest.raises(TypeError, match="Http429RetryDelay"):
        policy.decide(bad_delay_type)

    bad_delay = _request(
        FailureClassification.HTTP_429,
        http_429_delay=Http429RetryDelay(Duration(1)),
    )
    assert bad_delay.http_429_delay is not None
    object.__setattr__(bad_delay.http_429_delay.duration, "microseconds", "credential=canary")
    with pytest.raises(RetryPolicyInvalidRequestError, match="HTTP 429") as delay_error:
        policy.decide(bad_delay)
    assert "credential" not in str(delay_error.value)


def test_seeded_jitter_is_keyed_call_order_independent_bounded_and_redacted() -> None:
    source = SeededRetryJitterSource(42)
    upper_bound = Duration(500_000)
    first = source.sample(
        work_item_id=_WORK_ITEM_ID,
        classification=FailureClassification.TIMEOUT,
        attempt_number=AttemptNumber(2),
        upper_bound=upper_bound,
    )
    unrelated = source.sample(
        work_item_id=WorkItemId("wrk_other-item"),
        classification=FailureClassification.HTTP_5XX,
        attempt_number=AttemptNumber(1),
        upper_bound=Duration(10),
    )
    assert unrelated <= Duration(10)
    assert (
        source.sample(
            work_item_id=_WORK_ITEM_ID,
            classification=FailureClassification.TIMEOUT,
            attempt_number=AttemptNumber(2),
            upper_bound=upper_bound,
        )
        == first
    )
    assert Duration(0) <= first <= upper_bound
    assert source.sample(
        work_item_id=_WORK_ITEM_ID,
        classification=FailureClassification.TIMEOUT,
        attempt_number=AttemptNumber(2),
        upper_bound=Duration(0),
    ) == Duration(0)
    assert repr(source) == "SeededRetryJitterSource(seed=<redacted>)"
    with pytest.raises(FrozenInstanceError):
        source.seed = 43  # type: ignore[misc]


@pytest.mark.parametrize("seed", [True, "1"])
def test_seeded_jitter_rejects_non_integer_seed(seed: object) -> None:
    with pytest.raises(TypeError, match="integer"):
        SeededRetryJitterSource(cast(Any, seed))


@pytest.mark.parametrize("seed", [-1, MAX_RETRY_JITTER_SEED + 1])
def test_seeded_jitter_rejects_out_of_range_seed(seed: int) -> None:
    with pytest.raises(RetryPolicyInvalidRequestError, match="bounds"):
        SeededRetryJitterSource(seed)


@pytest.mark.parametrize(
    "arguments",
    [
        {
            "work_item_id": "wrk_retry-item",
            "classification": FailureClassification.CONNECTION,
            "attempt_number": AttemptNumber(1),
            "upper_bound": Duration(1),
        },
        {
            "work_item_id": _WORK_ITEM_ID,
            "classification": "connection",
            "attempt_number": AttemptNumber(1),
            "upper_bound": Duration(1),
        },
        {
            "work_item_id": _WORK_ITEM_ID,
            "classification": FailureClassification.CONNECTION,
            "attempt_number": 1,
            "upper_bound": Duration(1),
        },
        {
            "work_item_id": _WORK_ITEM_ID,
            "classification": FailureClassification.CONNECTION,
            "attempt_number": AttemptNumber(1),
            "upper_bound": 1,
        },
    ],
)
def test_seeded_jitter_rejects_substituted_sample_types(arguments: dict[str, object]) -> None:
    with pytest.raises(TypeError):
        SeededRetryJitterSource(0).sample(**cast(Any, arguments))


def test_seeded_jitter_revalidates_a_reflectively_mutated_seed() -> None:
    source = SeededRetryJitterSource(1)
    object.__setattr__(source, "seed", "credential=canary")
    with pytest.raises(TypeError, match="integer") as captured:
        source.sample(
            work_item_id=_WORK_ITEM_ID,
            classification=FailureClassification.CONNECTION,
            attempt_number=AttemptNumber(1),
            upper_bound=Duration(1),
        )
    assert "credential" not in str(captured.value)


def test_policy_requires_exact_borrowed_ports() -> None:
    with pytest.raises(TypeError, match="RetryClock"):
        BoundedExponentialRetryPolicy(cast(Any, object()), _Jitter())
    with pytest.raises(TypeError, match="RetryJitterSource"):
        BoundedExponentialRetryPolicy(_Clock(_timestamp()), cast(Any, object()))


@pytest.mark.parametrize(
    "clock_value",
    [RuntimeError("credential=canary"), "2026-08-14T12:00:00Z"],
)
def test_clock_exception_or_malformed_return_is_typed_and_redacted(
    clock_value: object,
) -> None:
    policy, _, jitter = _policy(clock=_Clock(clock_value))
    with pytest.raises(RetryPolicyClockError, match="clock failed") as captured:
        policy.decide(_request())
    assert "credential" not in str(captured.value)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert jitter.calls == []


def test_clock_must_not_precede_failure_and_corruption_is_redacted() -> None:
    policy, _, jitter = _policy(clock=_Clock(_timestamp()))
    with pytest.raises(RetryPolicyClockError, match="precedes"):
        policy.decide(_request(failed_at=_timestamp(1)))
    assert jitter.calls == []

    corrupt = _timestamp(2)
    object.__setattr__(corrupt, "value", "credential=canary")
    policy, _, _ = _policy(clock=_Clock(corrupt))
    with pytest.raises(RetryPolicyClockError, match="clock failed") as captured:
        policy.decide(_request())
    assert "credential" not in str(captured.value)


def test_clock_base_exception_propagates_without_conversion() -> None:
    fatal = _Fatal("stop")
    policy, _, _ = _policy(clock=_Clock(fatal))
    with pytest.raises(_Fatal) as captured:
        policy.decide(_request())
    assert captured.value is fatal


@pytest.mark.parametrize(
    "jitter_value",
    [RuntimeError("credential=canary"), 1],
)
def test_jitter_exception_or_malformed_return_is_typed_and_redacted(
    jitter_value: object,
) -> None:
    policy, clock, _ = _policy(jitter=_Jitter(jitter_value))
    with pytest.raises(RetryPolicyJitterError, match="jitter source failed") as captured:
        policy.decide(_request())
    assert "credential" not in str(captured.value)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert clock.calls == 1


def test_jitter_must_not_exceed_bound_and_corruption_is_redacted() -> None:
    policy, _, _ = _policy(jitter=_Jitter(Duration(250_001)))
    with pytest.raises(RetryPolicyJitterError, match="exceeds"):
        policy.decide(_request())

    corrupt = Duration(0)
    object.__setattr__(corrupt, "microseconds", "credential=canary")
    policy, _, _ = _policy(jitter=_Jitter(corrupt))
    with pytest.raises(RetryPolicyJitterError, match="source failed") as captured:
        policy.decide(_request())
    assert "credential" not in str(captured.value)


def test_jitter_base_exception_propagates_without_conversion() -> None:
    fatal = _Fatal("stop")
    policy, _, _ = _policy(jitter=_Jitter(fatal))
    with pytest.raises(_Fatal) as captured:
        policy.decide(_request())
    assert captured.value is fatal


def test_borrowed_jitter_cannot_mutate_decision_identity_or_bound() -> None:
    policy = BoundedExponentialRetryPolicy(
        _Clock(_timestamp(5_000_000)),
        _MutatingJitter(),
    )
    decision = policy.decide(_request())
    assert type(decision) is RetryScheduledDecision
    assert decision.work_item_id == _WORK_ITEM_ID
    assert decision.attempt_number == AttemptNumber(1)
    assert decision.delay == Duration(STANDARD_RETRY_INITIAL_MICROSECONDS)
    assert "credential" not in repr(decision)


def test_retry_availability_overflow_is_a_typed_clock_failure() -> None:
    maximum = UtcTimestamp(datetime.max.replace(tzinfo=UTC))
    policy, _, jitter = _policy(clock=_Clock(maximum))
    with pytest.raises(RetryPolicyClockError, match="availability") as captured:
        policy.decide(_request(failed_at=maximum))
    assert captured.value.__cause__ is None
    assert jitter.calls


def _scheduled(**changes: object) -> RetryScheduledDecision:
    values: dict[str, object] = {
        "policy_name": RetryPolicyName.BOUNDED_EXPONENTIAL_V1,
        "work_item_id": _WORK_ITEM_ID,
        "attempt_number": AttemptNumber(1),
        "classification": FailureClassification.CONNECTION,
        "failed_at": _timestamp(),
        "observed_at": _timestamp(1_000_000),
        "delay": Duration(1_000_000),
        "retry_available_at": _timestamp(2_000_000),
    }
    values.update(changes)
    return RetryScheduledDecision(**cast(Any, values))


def test_scheduled_decision_is_frozen_self_validating_and_payload_free() -> None:
    decision = _scheduled()
    assert decision.kind is RetryDecisionKind.SCHEDULED
    assert decision.disposition is FailureDisposition.RETRY
    assert decision.exhausted is False
    assert "RetryScheduledDecision" in repr(decision)
    with pytest.raises(FrozenInstanceError):
        decision.delay = Duration(2)  # type: ignore[misc]


@pytest.mark.parametrize(
    "changes",
    [
        {"policy_name": "bounded_exponential_v1"},
        {"work_item_id": "wrk_retry-item"},
        {"attempt_number": 1},
        {"classification": "connection"},
        {"failed_at": _BASE},
        {"observed_at": _BASE},
        {"delay": 1_000_000},
        {"retry_available_at": _BASE},
    ],
)
def test_scheduled_decision_rejects_substituted_types(changes: dict[str, object]) -> None:
    with pytest.raises(TypeError):
        _scheduled(**changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"classification": FailureClassification.HTTP_4XX},
        {"attempt_number": AttemptNumber(MAX_RETRY_ATTEMPTS)},
        {"observed_at": _timestamp(), "failed_at": _timestamp(1)},
        {"delay": Duration(0), "retry_available_at": _timestamp(1_000_000)},
        {
            "delay": Duration(STANDARD_RETRY_MAX_MICROSECONDS + 1),
            "retry_available_at": _timestamp(1_000_000 + STANDARD_RETRY_MAX_MICROSECONDS + 1),
        },
        {
            "classification": FailureClassification.SQLITE_CONTENTION,
            "delay": Duration(SQLITE_RETRY_MAX_MICROSECONDS + 1),
            "retry_available_at": _timestamp(2_000_001),
        },
        {"retry_available_at": _timestamp(2_000_001)},
    ],
)
def test_scheduled_decision_rejects_incoherent_values(changes: dict[str, object]) -> None:
    with pytest.raises(RetryPolicyInvalidRequestError):
        _scheduled(**changes)


def test_scheduled_decision_accepts_http_and_sqlite_delay_bounds() -> None:
    assert _scheduled(
        classification=FailureClassification.HTTP_429,
        delay=Duration(MAX_HTTP_429_RETRY_DELAY_MICROSECONDS),
        retry_available_at=_timestamp(1_000_000 + MAX_HTTP_429_RETRY_DELAY_MICROSECONDS),
    ).delay == Duration(MAX_HTTP_429_RETRY_DELAY_MICROSECONDS)
    assert _scheduled(
        classification=FailureClassification.SQLITE_CONTENTION,
        delay=Duration(SQLITE_RETRY_MAX_MICROSECONDS),
        retry_available_at=_timestamp(1_000_000 + SQLITE_RETRY_MAX_MICROSECONDS),
    ).delay == Duration(SQLITE_RETRY_MAX_MICROSECONDS)


def test_direct_scheduled_decision_rejects_timestamp_overflow() -> None:
    maximum = UtcTimestamp(datetime.max.replace(tzinfo=UTC))
    with pytest.raises(RetryPolicyInvalidRequestError, match="availability"):
        _scheduled(
            failed_at=maximum,
            observed_at=maximum,
            retry_available_at=maximum,
        )


def _stopped(**changes: object) -> RetryStoppedDecision:
    values: dict[str, object] = {
        "policy_name": RetryPolicyName.BOUNDED_EXPONENTIAL_V1,
        "work_item_id": _WORK_ITEM_ID,
        "attempt_number": AttemptNumber(1),
        "classification": FailureClassification.HTTP_4XX,
        "failed_at": _timestamp(),
        "disposition": FailureDisposition.PERMANENT,
        "exhausted": False,
    }
    values.update(changes)
    return RetryStoppedDecision(**cast(Any, values))


def test_stopped_decision_is_frozen_and_preserves_terminal_disposition() -> None:
    decision = _stopped()
    assert decision.kind is RetryDecisionKind.STOPPED
    assert decision.disposition is disposition_for(decision.classification)
    assert decision.exhausted is False
    assert "RetryStoppedDecision" in repr(decision)
    with pytest.raises(FrozenInstanceError):
        decision.exhausted = True  # type: ignore[misc]


@pytest.mark.parametrize(
    "changes",
    [
        {"disposition": "permanent"},
        {"exhausted": 1},
    ],
)
def test_stopped_decision_rejects_substituted_types(changes: dict[str, object]) -> None:
    with pytest.raises(TypeError):
        _stopped(**changes)


@pytest.mark.parametrize(
    "changes",
    [
        {
            "classification": FailureClassification.CONNECTION,
            "attempt_number": AttemptNumber(MAX_RETRY_ATTEMPTS),
            "disposition": FailureDisposition.PERMANENT,
            "exhausted": False,
        },
        {
            "classification": FailureClassification.CONNECTION,
            "attempt_number": AttemptNumber(MAX_RETRY_ATTEMPTS),
            "disposition": FailureDisposition.RETRY,
            "exhausted": True,
        },
        {
            "classification": FailureClassification.CONNECTION,
            "attempt_number": AttemptNumber(1),
            "disposition": FailureDisposition.PERMANENT,
            "exhausted": True,
        },
        {"exhausted": True},
        {"disposition": FailureDisposition.QUARANTINE},
    ],
)
def test_stopped_decision_rejects_incoherent_values(changes: dict[str, object]) -> None:
    with pytest.raises(RetryPolicyInvalidRequestError):
        _stopped(**changes)


@pytest.mark.parametrize("classification", _NON_RETRYABLE)
def test_stopped_decision_accepts_every_non_retry_domain_disposition(
    classification: FailureClassification,
) -> None:
    decision = _stopped(
        classification=classification,
        disposition=disposition_for(classification),
    )
    assert decision.disposition is disposition_for(classification)
