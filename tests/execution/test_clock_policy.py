"""Injected-clock delay, rate-policy, and seeded schedule tests for P7.5."""

from __future__ import annotations

import threading
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

from paritygrid.application.execution import (
    CLOCK_POLICY_VERSION,
    MAX_BURST,
    MAX_DELAY_MICROSECONDS,
    MAX_RATE_PER_SECOND,
    MIN_RATE_PER_SECOND,
    MIN_TOKEN_MICROSECONDS,
    CapturedConcurrencySettings,
    ClockPolicyError,
    ClockPolicyVersionError,
    ClockProtocolError,
    DelayPolicyExceededError,
    DelayValue,
    DelayValueError,
    ManualClock,
    PolicyClock,
    RateLimitPolicy,
    RatePolicyError,
    SeededJitterSchedule,
    TokenBucket,
    clock_policy,
    validate_delay_against_policy,
)
from paritygrid.domain.models import Duration, UtcTimestamp

_BASE = datetime(2026, 8, 24, 12, tzinfo=UTC)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_UNITS = ("microseconds", "milliseconds", "seconds", "minutes")
_MAX_TIMESTAMP = UtcTimestamp(datetime.max.replace(tzinfo=UTC))


def _timestamp(microseconds: int = 0) -> UtcTimestamp:
    return UtcTimestamp(_BASE + timedelta(microseconds=microseconds))


def _epoch_micros(value: UtcTimestamp) -> int:
    delta = value.to_datetime() - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def _settings(
    cleanup_timeout_seconds: float = 60.0,
    shutdown_timeout_seconds: float = 30.0,
) -> CapturedConcurrencySettings:
    return CapturedConcurrencySettings(
        cleanup_timeout_seconds=cleanup_timeout_seconds,
        shutdown_timeout_seconds=shutdown_timeout_seconds,
    )


def _expected_eligibility(seed: int, attempt: int, base_micros: int, cap_micros: int) -> int:
    digest = sha256(
        b"\x00".join(
            (
                b"paritygrid-clock-jitter-v1",
                str(seed).encode("ascii"),
                str(attempt).encode("ascii"),
            )
        )
    ).digest()
    fraction = (int.from_bytes(digest[:8], "big") >> 11) / float(1 << 53)
    shift = min(attempt - 1, 16)
    return min(round(base_micros * (1 << shift) * (1.0 + fraction)), cap_micros)


def test_module_constants_are_pinned() -> None:
    assert CLOCK_POLICY_VERSION == 1
    assert MAX_DELAY_MICROSECONDS == 86_400_000_000
    assert MAX_RATE_PER_SECOND == 1_000.0
    assert MIN_RATE_PER_SECOND == 0.001
    assert MAX_BURST == 1_000
    assert MIN_TOKEN_MICROSECONDS == 1


def test_module_all_exports_deliberate_public_names() -> None:
    assert clock_policy.__all__ == [
        "CLOCK_POLICY_VERSION",
        "MAX_BURST",
        "MAX_DELAY_MICROSECONDS",
        "MAX_RATE_PER_SECOND",
        "MIN_RATE_PER_SECOND",
        "MIN_TOKEN_MICROSECONDS",
        "ClockPolicyError",
        "ClockPolicyVersionError",
        "ClockProtocolError",
        "DelayPolicyExceededError",
        "DelayValue",
        "DelayValueError",
        "ManualClock",
        "PolicyClock",
        "RateLimitPolicy",
        "RatePolicyError",
        "SeededJitterSchedule",
        "TokenBucket",
        "validate_delay_against_policy",
    ]


def test_module_source_contains_no_ambient_clock_usage() -> None:
    module_file = clock_policy.__file__
    assert module_file is not None
    source = Path(module_file).read_text(encoding="utf-8")
    for forbidden in ("time.time", "datetime.now", "utcnow", "perf_counter", "monotonic("):
        assert forbidden not in source


def test_policy_clock_protocol_separates_clocks_from_non_clocks() -> None:
    assert isinstance(ManualClock(_timestamp()), PolicyClock)
    assert not isinstance(object(), PolicyClock)


def test_clock_policy_errors_share_one_module_base() -> None:
    for error in (
        DelayValueError,
        DelayPolicyExceededError,
        RatePolicyError,
        ClockProtocolError,
        ClockPolicyVersionError,
    ):
        assert issubclass(error, ClockPolicyError)


# --------------------------------------------------------------------------------------
# ManualClock
# --------------------------------------------------------------------------------------


def test_manual_clock_returns_exact_initial_value() -> None:
    clock = ManualClock(_timestamp(1_500_000))
    assert clock.now() == _timestamp(1_500_000)


def test_manual_clock_advance_accumulates_exactly() -> None:
    clock = ManualClock(_timestamp())
    clock.advance(500)
    assert clock.now() == _timestamp(500)
    clock.advance(1_500_000)
    assert clock.now() == _timestamp(1_500_500)
    clock.advance(0)
    assert clock.now() == _timestamp(1_500_500)


@pytest.mark.parametrize("microseconds", [-1, -1_000_000])
def test_manual_clock_rejects_negative_advance(microseconds: int) -> None:
    clock = ManualClock(_timestamp())
    with pytest.raises(DelayValueError):
        clock.advance(microseconds)
    assert clock.now() == _timestamp()


@pytest.mark.parametrize("microseconds", [True, 1.0, "100", None])
def test_manual_clock_rejects_non_exact_integer_advance(microseconds: object) -> None:
    clock = ManualClock(_timestamp())
    with pytest.raises(DelayValueError):
        clock.advance(cast(int, microseconds))
    assert clock.now() == _timestamp()


def test_manual_clock_rejects_overflowing_advance() -> None:
    clock = ManualClock(_MAX_TIMESTAMP)
    with pytest.raises(DelayValueError):
        clock.advance(1)
    assert clock.now() == _MAX_TIMESTAMP


def test_manual_clock_advance_to_moves_forward_or_stays() -> None:
    clock = ManualClock(_timestamp())
    clock.advance_to(_timestamp(5_000_000))
    assert clock.now() == _timestamp(5_000_000)
    clock.advance_to(_timestamp(5_000_000))
    assert clock.now() == _timestamp(5_000_000)


def test_manual_clock_rejects_backwards_advance_to() -> None:
    clock = ManualClock(_timestamp(1_000_000))
    with pytest.raises(ClockProtocolError):
        clock.advance_to(_timestamp(999_999))
    assert clock.now() == _timestamp(1_000_000)


@pytest.mark.parametrize("initial", [_BASE, 5, None])
def test_manual_clock_rejects_non_exact_initial_values(initial: object) -> None:
    with pytest.raises(TypeError):
        ManualClock(cast(UtcTimestamp, initial))


def test_manual_clock_advance_to_rejects_non_exact_timestamp() -> None:
    clock = ManualClock(_timestamp())
    with pytest.raises(TypeError):
        clock.advance_to(cast(UtcTimestamp, _BASE))


def test_manual_clock_advance_is_thread_safe() -> None:
    clock = ManualClock(_timestamp())
    barrier = threading.Barrier(2)

    def worker() -> None:
        barrier.wait()
        for _ in range(1_000):
            clock.advance(100)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert clock.now() == _timestamp(200_000)


# --------------------------------------------------------------------------------------
# DelayValue construction
# --------------------------------------------------------------------------------------


def test_delay_value_accepts_bounds_and_reports_version() -> None:
    assert DelayValue(0).to_microseconds() == 0
    assert DelayValue(MAX_DELAY_MICROSECONDS).to_microseconds() == MAX_DELAY_MICROSECONDS
    assert DelayValue(5).version == CLOCK_POLICY_VERSION


@pytest.mark.parametrize("microseconds", [True, 1.0, "5", None, b"5"])
def test_delay_value_rejects_non_exact_integers(microseconds: object) -> None:
    with pytest.raises(TypeError):
        DelayValue(cast(int, microseconds))


@pytest.mark.parametrize("microseconds", [-1, MAX_DELAY_MICROSECONDS + 1])
def test_delay_value_rejects_out_of_range_microseconds(microseconds: int) -> None:
    with pytest.raises(DelayValueError):
        DelayValue(microseconds)


@pytest.mark.parametrize("version", [0, 2, True, "1"])
def test_delay_value_rejects_unknown_versions(version: object) -> None:
    with pytest.raises(ClockPolicyVersionError):
        DelayValue(5, version=cast(int, version))


def test_delay_value_is_frozen() -> None:
    delay = DelayValue(5)
    with pytest.raises(FrozenInstanceError):
        delay.microseconds = 6  # type: ignore[misc]


# --------------------------------------------------------------------------------------
# DelayValue.parse
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "unit", "expected"),
    [
        (0, "microseconds", 0),
        (1, "microseconds", 1),
        (MAX_DELAY_MICROSECONDS, "microseconds", MAX_DELAY_MICROSECONDS),
        (1, "milliseconds", 1_000),
        (250, "milliseconds", 250_000),
        (86_400_000, "milliseconds", MAX_DELAY_MICROSECONDS),
        (2, "seconds", 2_000_000),
        (86_400, "seconds", MAX_DELAY_MICROSECONDS),
        (1, "minutes", 60_000_000),
        (1_440, "minutes", MAX_DELAY_MICROSECONDS),
        (2.5, "seconds", 2_500_000),
        (0.001, "seconds", 1_000),
        (1.5, "seconds", 1_500_000),
        (0.25, "milliseconds", 250),
        (1.0, "minutes", 60_000_000),
        (500.0, "microseconds", 500),
    ],
)
def test_delay_parse_converts_units_exactly(value: object, unit: str, expected: int) -> None:
    assert DelayValue.parse(value, unit=unit).to_microseconds() == expected


@pytest.mark.parametrize("unit", _UNITS)
def test_delay_parse_rejects_nan_for_every_unit(unit: str) -> None:
    with pytest.raises(DelayValueError):
        DelayValue.parse(float("nan"), unit=unit)


@pytest.mark.parametrize("unit", _UNITS)
def test_delay_parse_rejects_infinities_for_every_unit(unit: str) -> None:
    with pytest.raises(DelayValueError):
        DelayValue.parse(float("inf"), unit=unit)
    with pytest.raises(DelayValueError):
        DelayValue.parse(float("-inf"), unit=unit)


@pytest.mark.parametrize("unit", _UNITS)
def test_delay_parse_rejects_negative_values_for_every_unit(unit: str) -> None:
    with pytest.raises(DelayValueError):
        DelayValue.parse(-1, unit=unit)
    with pytest.raises(DelayValueError):
        DelayValue.parse(-0.5, unit=unit)


@pytest.mark.parametrize(
    ("value", "unit"),
    [
        (MAX_DELAY_MICROSECONDS + 1, "microseconds"),
        (86_400_001, "milliseconds"),
        (86_401, "seconds"),
        (1_441, "minutes"),
        (86_401.0, "seconds"),
        (1e30, "seconds"),
    ],
)
def test_delay_parse_rejects_overflow_in_every_unit(value: object, unit: str) -> None:
    with pytest.raises(DelayValueError):
        DelayValue.parse(value, unit=unit)


@pytest.mark.parametrize(
    ("value", "unit"),
    [
        (1.5, "microseconds"),
        (1.25, "microseconds"),
        (0.5, "microseconds"),
        (0.0000015, "seconds"),
        (1.0000005, "seconds"),
    ],
)
def test_delay_parse_rejects_fractional_microseconds(value: float, unit: str) -> None:
    with pytest.raises(DelayValueError):
        DelayValue.parse(value, unit=unit)


@pytest.mark.parametrize("value", ["10", b"10", None, True, False, [10], (10,)])
def test_delay_parse_rejects_malformed_values(value: object) -> None:
    with pytest.raises(DelayValueError):
        DelayValue.parse(value, unit="seconds")


@pytest.mark.parametrize(
    "unit",
    [
        "min",
        "minute",
        "MINUTES",
        "Microseconds",
        "us",
        "ms",
        "s",
        "",
        " seconds",
        "seconds ",
        "sec",
    ],
)
def test_delay_parse_rejects_ambiguous_units(unit: str) -> None:
    with pytest.raises(DelayValueError):
        DelayValue.parse(5, unit=unit)


@pytest.mark.parametrize("unit", [b"seconds", 1, None, True])
def test_delay_parse_rejects_non_string_units(unit: object) -> None:
    with pytest.raises(DelayValueError):
        DelayValue.parse(5, unit=cast(str, unit))


# --------------------------------------------------------------------------------------
# DelayValue.parse_text
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("0us", 0),
        ("500us", 500),
        ("250ms", 250_000),
        ("86400000ms", MAX_DELAY_MICROSECONDS),
        ("5s", 5_000_000),
        ("1.5s", 1_500_000),
        ("0.001s", 1_000),
        ("1.000001s", 1_000_001),
        ("86400s", MAX_DELAY_MICROSECONDS),
    ],
)
def test_delay_parse_text_accepts_canonical_forms(text: str, expected: int) -> None:
    assert DelayValue.parse_text(text).to_microseconds() == expected


@pytest.mark.parametrize(
    "text",
    [
        "",
        " ",
        "us",
        "ms",
        "s",
        "1 m",
        "1min",
        "1 s",
        "1.5us",
        "1.5ms",
        "-5s",
        "-5us",
        "+5s",
        "1e3s",
        "nan",
        "inf",
        "1US",
        "1Ms",
        ".5s",
        "5.",
        "1us2",
        " 1s",
        "1s\n",
        "1s ",
        "0x10s",
        "1_000s",
        "1440min",
        "1second",
        "1.5.5s",
        "\u0661s",  # arabic-indic digit one must not pass as a magnitude
    ],
)
def test_delay_parse_text_rejects_non_canonical_forms(text: str) -> None:
    with pytest.raises(DelayValueError):
        DelayValue.parse_text(text)


@pytest.mark.parametrize(
    "text",
    ["86401s", "86401.0s", "86400001ms", "86400000001us", "99999999999999999999us"],
)
def test_delay_parse_text_rejects_overflow(text: str) -> None:
    with pytest.raises(DelayValueError):
        DelayValue.parse_text(text)


@pytest.mark.parametrize("value", [b"500us", None, 500, 500.0])
def test_delay_parse_text_rejects_non_string_values(value: object) -> None:
    with pytest.raises(DelayValueError):
        DelayValue.parse_text(cast(str, value))


# --------------------------------------------------------------------------------------
# DelayValue arithmetic
# --------------------------------------------------------------------------------------


def test_delay_plus_adds_exactly() -> None:
    assert DelayValue(5).plus(DelayValue(10)).to_microseconds() == 15
    assert DelayValue(0).plus(DelayValue(0)).to_microseconds() == 0
    assert (
        DelayValue(MAX_DELAY_MICROSECONDS - 5).plus(DelayValue(5)).to_microseconds()
        == MAX_DELAY_MICROSECONDS
    )


def test_delay_plus_rejects_overflow() -> None:
    with pytest.raises(DelayValueError):
        DelayValue(MAX_DELAY_MICROSECONDS).plus(DelayValue(1))


@pytest.mark.parametrize("value", [Duration(1), 1, None])
def test_delay_plus_rejects_non_delay_values(value: object) -> None:
    with pytest.raises(TypeError):
        DelayValue(1).plus(cast(DelayValue, value))


def test_delay_plus_rejects_tampered_operand() -> None:
    operand = DelayValue(5)
    object.__setattr__(operand, "microseconds", -1)
    with pytest.raises(DelayValueError):
        DelayValue(1).plus(operand)


def test_delay_against_adds_to_a_timestamp() -> None:
    assert DelayValue(1_500_000).against(_timestamp()) == _timestamp(1_500_000)
    assert DelayValue(0).against(_timestamp(42)) == _timestamp(42)


def test_delay_against_rejects_overflow() -> None:
    with pytest.raises(DelayValueError):
        DelayValue(1).against(_MAX_TIMESTAMP)


def test_delay_against_rejects_non_timestamp() -> None:
    with pytest.raises(TypeError):
        DelayValue(1).against(cast(UtcTimestamp, _BASE))


# --------------------------------------------------------------------------------------
# validate_delay_against_policy
# --------------------------------------------------------------------------------------


def test_policy_validation_accepts_delay_at_the_ceiling_unchanged() -> None:
    delay = DelayValue(60_000_000)
    assert validate_delay_against_policy(delay, _settings()) is delay


def test_policy_validation_rejects_one_microsecond_above_the_ceiling() -> None:
    with pytest.raises(DelayPolicyExceededError):
        validate_delay_against_policy(DelayValue(60_000_001), _settings())


def test_policy_validation_uses_the_larger_shutdown_ceiling() -> None:
    settings = _settings(cleanup_timeout_seconds=30.0, shutdown_timeout_seconds=90.0)
    accepted = validate_delay_against_policy(DelayValue(90_000_000), settings)
    assert accepted.to_microseconds() == 90_000_000
    with pytest.raises(DelayPolicyExceededError):
        validate_delay_against_policy(DelayValue(90_000_001), settings)


def test_policy_validation_supports_fractional_ceilings() -> None:
    settings = _settings(cleanup_timeout_seconds=0.001, shutdown_timeout_seconds=0.001)
    accepted = validate_delay_against_policy(DelayValue(1_000), settings)
    assert accepted.to_microseconds() == 1_000
    with pytest.raises(DelayPolicyExceededError):
        validate_delay_against_policy(DelayValue(1_001), settings)


def test_policy_validation_rejects_non_delay_value() -> None:
    with pytest.raises(TypeError):
        validate_delay_against_policy(cast(DelayValue, Duration(1)), _settings())


def test_policy_validation_rejects_non_settings() -> None:
    with pytest.raises(TypeError):
        validate_delay_against_policy(DelayValue(1), cast(CapturedConcurrencySettings, object()))


def test_policy_exceeded_error_is_distinct_from_value_error() -> None:
    assert not issubclass(DelayPolicyExceededError, DelayValueError)


# --------------------------------------------------------------------------------------
# RateLimitPolicy
# --------------------------------------------------------------------------------------


def test_rate_policy_defaults_to_single_token_burst() -> None:
    policy = RateLimitPolicy(rate_per_second=10.0)
    assert policy.burst == 1
    assert policy.version == CLOCK_POLICY_VERSION


def test_rate_policy_per_second_builder() -> None:
    policy = RateLimitPolicy.per_second(25.0, burst=5)
    assert policy.rate_per_second == 25.0
    assert policy.burst == 5
    assert RateLimitPolicy.per_second(1.0).burst == 1


@pytest.mark.parametrize("rate", [MIN_RATE_PER_SECOND, MAX_RATE_PER_SECOND, 1.0, 123.456])
def test_rate_policy_accepts_bounded_rates(rate: float) -> None:
    assert RateLimitPolicy(rate_per_second=rate).rate_per_second == rate


@pytest.mark.parametrize("rate", [0.0009, 1_000.001, -1.0])
def test_rate_policy_rejects_out_of_range_rates(rate: float) -> None:
    with pytest.raises(RatePolicyError):
        RateLimitPolicy(rate_per_second=rate)


@pytest.mark.parametrize("rate", [float("nan"), float("inf"), float("-inf")])
def test_rate_policy_rejects_non_finite_rates(rate: float) -> None:
    with pytest.raises(RatePolicyError):
        RateLimitPolicy(rate_per_second=rate)


@pytest.mark.parametrize("rate", [10, "10", True, None])
def test_rate_policy_rejects_non_float_rates(rate: object) -> None:
    with pytest.raises(RatePolicyError):
        RateLimitPolicy(rate_per_second=cast(float, rate))


@pytest.mark.parametrize("burst", [1, 17, MAX_BURST])
def test_rate_policy_accepts_bounded_bursts(burst: int) -> None:
    assert RateLimitPolicy(rate_per_second=1.0, burst=burst).burst == burst


@pytest.mark.parametrize("burst", [0, -1, MAX_BURST + 1])
def test_rate_policy_rejects_out_of_range_bursts(burst: int) -> None:
    with pytest.raises(RatePolicyError):
        RateLimitPolicy(rate_per_second=1.0, burst=burst)


@pytest.mark.parametrize("burst", [True, 1.0, "1", None])
def test_rate_policy_rejects_non_exact_bursts(burst: object) -> None:
    with pytest.raises(RatePolicyError):
        RateLimitPolicy(rate_per_second=1.0, burst=cast(int, burst))


@pytest.mark.parametrize("version", [0, 2, True, "1"])
def test_rate_policy_rejects_unknown_versions(version: object) -> None:
    with pytest.raises(ClockPolicyVersionError):
        RateLimitPolicy(rate_per_second=1.0, version=cast(int, version))


@pytest.mark.parametrize(
    ("rate", "expected"),
    [
        (1_000.0, 1_000),
        (100.0, 10_000),
        (4.0, 250_000),
        (2.5, 400_000),
        (1.0, 1_000_000),
        (3.0, 333_333),
        (0.001, 1_000_000_000),
    ],
)
def test_rate_policy_token_interval_math(rate: float, expected: int) -> None:
    interval = RateLimitPolicy(rate_per_second=rate).token_interval_micros()
    assert interval == expected
    assert interval >= MIN_TOKEN_MICROSECONDS


def test_rate_policy_is_frozen() -> None:
    policy = RateLimitPolicy(rate_per_second=1.0)
    with pytest.raises(FrozenInstanceError):
        policy.burst = 2  # type: ignore[misc]


# --------------------------------------------------------------------------------------
# TokenBucket
# --------------------------------------------------------------------------------------


def test_token_bucket_initial_state_is_full() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=3))
    now = _timestamp()
    assert bucket.state(now) == (3, _epoch_micros(now))


def test_token_bucket_supports_empty_start() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=3), initial_tokens=0)
    now = _timestamp()
    assert bucket.state(now) == (0, _epoch_micros(now))


@pytest.mark.parametrize("initial_tokens", [-1, 4, True, 1.0, "1"])
def test_token_bucket_rejects_invalid_initial_tokens(initial_tokens: object) -> None:
    with pytest.raises(RatePolicyError):
        TokenBucket(
            RateLimitPolicy(rate_per_second=1.0, burst=3),
            cast(int, initial_tokens),
        )


def test_token_bucket_rejects_non_rate_policy() -> None:
    with pytest.raises(TypeError):
        TokenBucket(cast(RateLimitPolicy, object()))


def test_token_bucket_state_is_pure_and_establishes_no_baseline() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=2), initial_tokens=0)
    first = bucket.state(_timestamp(1_000_000))
    second = bucket.state(_timestamp(1_000_000))
    assert first == second == (0, _epoch_micros(_timestamp(1_000_000)))
    assert bucket.try_acquire(_timestamp(0)) is False


def test_token_bucket_try_acquire_consumes_tokens() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=2))
    now = _timestamp()
    assert bucket.try_acquire(now) is True
    assert bucket.state(now) == (1, _epoch_micros(now))
    assert bucket.try_acquire(now) is True
    assert bucket.try_acquire(now) is False
    assert bucket.state(now) == (0, _epoch_micros(now))


def test_token_bucket_try_acquire_denies_without_blocking() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=1), initial_tokens=0)
    now = _timestamp()
    assert bucket.try_acquire(now) is False
    assert bucket.state(now) == (0, _epoch_micros(now))


def test_token_bucket_refills_after_exactly_one_interval() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=1), initial_tokens=0)
    assert bucket.try_acquire(_timestamp()) is False
    assert bucket.try_acquire(_timestamp(999_999)) is False
    assert bucket.try_acquire(_timestamp(1_000_000)) is True


def test_token_bucket_partial_refill_preserves_the_remainder() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=3), initial_tokens=0)
    assert bucket.try_acquire(_timestamp(), tokens=3) is False
    later = _timestamp(2_500_000)
    assert bucket.state(later) == (2, _epoch_micros(later))
    assert bucket.try_acquire(later, tokens=3) is False
    assert bucket.try_acquire(later, tokens=3) is False
    assert bucket.state(later) == (2, _epoch_micros(later))
    assert bucket.try_acquire(_timestamp(2_999_999), tokens=3) is False
    assert bucket.try_acquire(_timestamp(3_000_000), tokens=3) is True
    assert bucket.state(_timestamp(3_000_000)) == (0, _epoch_micros(_timestamp(3_000_000)))


def test_token_bucket_refill_saturates_at_burst() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=2), initial_tokens=0)
    assert bucket.try_acquire(_timestamp()) is False
    assert bucket.state(_timestamp(10_000_000)) == (2, _epoch_micros(_timestamp(10_000_000)))
    assert bucket.state(_timestamp(100_000_000)) == (2, _epoch_micros(_timestamp(100_000_000)))


def test_token_bucket_burst_bounds_simultaneous_acquisition() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=3))
    now = _timestamp()
    assert bucket.try_acquire(now, tokens=3) is True
    assert bucket.try_acquire(now) is False


def test_token_bucket_try_acquire_above_burst_returns_false() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=2))
    now = _timestamp()
    assert bucket.try_acquire(now, tokens=3) is False
    with pytest.raises(RatePolicyError):
        bucket.acquire_at(now, tokens=3)


@pytest.mark.parametrize("tokens", [0, -1, True, 1.0, "1", None])
def test_token_bucket_rejects_invalid_token_counts(tokens: object) -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=2))
    now = _timestamp()
    with pytest.raises(RatePolicyError):
        bucket.try_acquire(now, tokens=cast(int, tokens))
    with pytest.raises(RatePolicyError):
        bucket.acquire_at(now, tokens=cast(int, tokens))


def test_token_bucket_rejects_non_timestamp_instants() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0))
    with pytest.raises(TypeError):
        bucket.state(cast(UtcTimestamp, _BASE))
    with pytest.raises(TypeError):
        bucket.try_acquire(cast(UtcTimestamp, _BASE))
    with pytest.raises(TypeError):
        bucket.acquire_at(cast(UtcTimestamp, _BASE))


def test_token_bucket_rejects_non_monotonic_injection() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=1), initial_tokens=0)
    assert bucket.try_acquire(_timestamp()) is False
    with pytest.raises(RatePolicyError):
        bucket.try_acquire(_timestamp(-1))
    with pytest.raises(RatePolicyError):
        bucket.state(_timestamp(-1))
    with pytest.raises(RatePolicyError):
        bucket.acquire_at(_timestamp(-1))


def test_token_bucket_same_now_calls_are_idempotent_in_refill() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=4), initial_tokens=0)
    assert bucket.try_acquire(_timestamp()) is False
    later = _timestamp(2_500_000)
    assert bucket.state(later) == (2, _epoch_micros(later))
    assert bucket.state(later) == (2, _epoch_micros(later))
    assert bucket.try_acquire(later, tokens=4) is False
    assert bucket.try_acquire(later, tokens=4) is False
    assert bucket.state(later) == (2, _epoch_micros(later))


def test_token_bucket_acquire_at_returns_now_when_available() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=1))
    now = _timestamp(500_000)
    assert bucket.acquire_at(now) == now


def test_token_bucket_acquire_at_returns_the_next_interval_when_empty() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=1), initial_tokens=0)
    assert bucket.try_acquire(_timestamp()) is False
    ready = bucket.acquire_at(_timestamp(100_000))
    assert ready == _timestamp(1_000_000)
    assert bucket.try_acquire(_timestamp(999_999)) is False
    assert bucket.try_acquire(ready) is True


def test_token_bucket_acquire_at_multi_token_exact_math() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=4.0, burst=10), initial_tokens=0)
    assert bucket.try_acquire(_timestamp()) is False
    ready = bucket.acquire_at(_timestamp(50_000), tokens=3)
    assert ready == _timestamp(750_000)
    assert bucket.try_acquire(ready, tokens=3) is True


def test_token_bucket_acquire_at_aligns_to_token_boundaries() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=1))
    assert bucket.try_acquire(_timestamp()) is True
    assert bucket.acquire_at(_timestamp(500_000)) == _timestamp(1_000_000)
    assert bucket.acquire_at(_timestamp(900_000)) == _timestamp(1_000_000)


def test_token_bucket_acquire_at_does_not_mutate_or_set_a_baseline() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=2.0, burst=4), initial_tokens=1)
    now = _timestamp()
    before = bucket.state(now)
    first = bucket.acquire_at(now, tokens=4)
    second = bucket.acquire_at(now, tokens=4)
    assert first == second == _timestamp(1_500_000)
    assert bucket.state(now) == before
    assert bucket.try_acquire(_timestamp(0)) is True


def test_token_bucket_acquire_at_overflows_timestamp_bounds() -> None:
    near_max = UtcTimestamp(datetime.max.replace(tzinfo=UTC) - timedelta(seconds=2))
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=10), initial_tokens=0)
    assert bucket.try_acquire(near_max) is False
    with pytest.raises(RatePolicyError):
        bucket.acquire_at(
            UtcTimestamp(datetime.max.replace(tzinfo=UTC) - timedelta(seconds=1)),
            tokens=3,
        )


def test_token_bucket_try_acquire_is_thread_safe() -> None:
    bucket = TokenBucket(RateLimitPolicy(rate_per_second=1.0, burst=2), initial_tokens=2)
    now = _timestamp()
    barrier = threading.Barrier(4)
    results: list[bool] = []
    results_lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        acquired = bucket.try_acquire(now)
        with results_lock:
            results.append(acquired)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sum(results) == 2
    assert bucket.state(now) == (0, _epoch_micros(now))


# --------------------------------------------------------------------------------------
# SeededJitterSchedule
# --------------------------------------------------------------------------------------


def test_seeded_schedule_repeats_identical_eligibility() -> None:
    schedule = SeededJitterSchedule(seed=5150)
    first = schedule.eligibility(_timestamp(), 3, DelayValue(1_000_000), DelayValue(30_000_000))
    for _ in range(5):
        repeated = schedule.eligibility(
            _timestamp(), 3, DelayValue(1_000_000), DelayValue(30_000_000)
        )
        assert repeated == first


def test_seeded_schedule_is_call_order_independent() -> None:
    schedule = SeededJitterSchedule(seed=12345)
    base = DelayValue(1_000_000)
    cap = DelayValue(60_000_000)
    failed_at = _timestamp()
    forward = {
        attempt: schedule.eligibility(failed_at, attempt, base, cap) for attempt in range(1, 8)
    }
    backward = {
        attempt: schedule.eligibility(failed_at, attempt, base, cap)
        for attempt in reversed(range(1, 8))
    }
    assert forward == backward
    for attempt in range(1, 8):
        expected = _expected_eligibility(12345, attempt, 1_000_000, 60_000_000)
        assert forward[attempt] == _timestamp(expected)


def test_seeded_schedule_attempt_one_scales_base_directly_by_jitter() -> None:
    schedule = SeededJitterSchedule(seed=987654321)
    base = DelayValue(2_500_000)
    cap = DelayValue(60_000_000)
    expected = _expected_eligibility(987654321, 1, 2_500_000, 60_000_000)
    assert 2_500_000 <= expected < 5_000_000
    assert schedule.eligibility(_timestamp(), 1, base, cap) == _timestamp(expected)


def test_seeded_schedule_different_seeds_differ() -> None:
    base = DelayValue(1_000_000)
    cap = DelayValue(60_000_000)
    eligibilities = [
        SeededJitterSchedule(seed=seed).eligibility(_timestamp(), 1, base, cap)
        for seed in range(10)
    ]
    assert len(set(eligibilities)) == 10


def test_seeded_schedule_delays_grow_monotonically_until_cap() -> None:
    schedule = SeededJitterSchedule(seed=20260824)
    base = DelayValue(1_000_000)
    cap = DelayValue(60_000_000)
    failed_at = _timestamp()
    delays = [
        _epoch_micros(schedule.eligibility(failed_at, attempt, base, cap))
        - _epoch_micros(failed_at)
        for attempt in range(1, 21)
    ]
    assert delays == sorted(delays)
    assert delays[-1] == 60_000_000


def test_seeded_schedule_saturates_at_cap_for_every_attempt() -> None:
    schedule = SeededJitterSchedule(seed=7)
    base = DelayValue(10_000_000)
    cap = DelayValue(10_000_000)
    for attempt in range(1, 10):
        assert schedule.eligibility(_timestamp(), attempt, base, cap) == _timestamp(10_000_000)


def test_seeded_schedule_caps_backoff_shift_at_sixteen() -> None:
    schedule = SeededJitterSchedule(seed=42)
    base = DelayValue(1)
    cap = DelayValue(MAX_DELAY_MICROSECONDS)
    for attempt in (17, 18, 40):
        expected = _expected_eligibility(42, attempt, 1, MAX_DELAY_MICROSECONDS)
        assert 65_536 <= expected <= 131_072
        assert schedule.eligibility(_timestamp(), attempt, base, cap) == _timestamp(expected)


def test_seeded_schedule_zero_base_returns_the_failure_instant() -> None:
    schedule = SeededJitterSchedule(seed=3)
    failed_at = _timestamp(123_456)
    eligibility = schedule.eligibility(failed_at, 5, DelayValue(0), DelayValue(1_000_000))
    assert eligibility == failed_at


def test_seeded_schedule_accepts_the_maximum_seed() -> None:
    schedule = SeededJitterSchedule(seed=2**63 - 1)
    assert schedule.eligibility(_timestamp(), 1, DelayValue(0), DelayValue(1)) == _timestamp()


@pytest.mark.parametrize("seed", [-1, -(2**63), 2**63])
def test_seeded_schedule_rejects_out_of_bounds_seeds(seed: int) -> None:
    with pytest.raises(DelayValueError):
        SeededJitterSchedule(seed=seed)


@pytest.mark.parametrize("seed", [True, 1.0, "1", None])
def test_seeded_schedule_rejects_non_exact_seeds(seed: object) -> None:
    with pytest.raises(TypeError):
        SeededJitterSchedule(seed=cast(int, seed))


@pytest.mark.parametrize("version", [0, 2, True, "1"])
def test_seeded_schedule_rejects_unknown_versions(version: object) -> None:
    with pytest.raises(ClockPolicyVersionError):
        SeededJitterSchedule(seed=1, version=cast(int, version))


@pytest.mark.parametrize("attempt", [0, -1])
def test_seeded_schedule_rejects_non_positive_attempts(attempt: int) -> None:
    with pytest.raises(DelayValueError):
        SeededJitterSchedule(seed=1).eligibility(
            _timestamp(), attempt, DelayValue(1), DelayValue(1)
        )


@pytest.mark.parametrize("attempt", [True, 1.0, "1", None])
def test_seeded_schedule_rejects_non_exact_attempts(attempt: object) -> None:
    with pytest.raises(TypeError):
        SeededJitterSchedule(seed=1).eligibility(
            _timestamp(), cast(int, attempt), DelayValue(1), DelayValue(1)
        )


def test_seeded_schedule_rejects_raw_datetime_failure_time() -> None:
    with pytest.raises(TypeError):
        SeededJitterSchedule(seed=1).eligibility(
            cast(UtcTimestamp, _BASE), 1, DelayValue(1), DelayValue(1)
        )


@pytest.mark.parametrize("value", [Duration(5), 5, None])
def test_seeded_schedule_rejects_non_delay_value_inputs(value: object) -> None:
    schedule = SeededJitterSchedule(seed=1)
    delay = cast(DelayValue, value)
    with pytest.raises(TypeError):
        schedule.eligibility(_timestamp(), 1, delay, DelayValue(1_000_000))
    with pytest.raises(TypeError):
        schedule.eligibility(_timestamp(), 1, DelayValue(1_000_000), delay)


def test_seeded_schedule_rejects_cap_below_base() -> None:
    with pytest.raises(DelayValueError):
        SeededJitterSchedule(seed=1).eligibility(
            _timestamp(), 1, DelayValue(2_000_000), DelayValue(1_999_999)
        )


def test_seeded_schedule_rejects_eligibility_overflow() -> None:
    schedule = SeededJitterSchedule(seed=1)
    with pytest.raises(DelayValueError):
        schedule.eligibility(_MAX_TIMESTAMP, 1, DelayValue(1_000_000), DelayValue(1_000_000))


def test_seeded_schedule_is_frozen_and_redacts_the_seed() -> None:
    schedule = SeededJitterSchedule(seed=99)
    with pytest.raises(FrozenInstanceError):
        schedule.seed = 100  # type: ignore[misc]
    assert repr(schedule) == "SeededJitterSchedule(seed=<redacted>)"


def test_seeded_schedule_revalidates_a_tampered_seed() -> None:
    schedule = SeededJitterSchedule(seed=1)
    object.__setattr__(schedule, "seed", -1)
    with pytest.raises(DelayValueError):
        schedule.eligibility(_timestamp(), 1, DelayValue(1), DelayValue(1))
