"""Injected-clock delay values, rate policies, and seeded jitter schedules."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from math import isfinite
from threading import Lock
from typing import Protocol, cast, runtime_checkable

from paritygrid.application.execution.concurrency_settings import CapturedConcurrencySettings
from paritygrid.domain.models import UtcTimestamp

CLOCK_POLICY_VERSION = 1
MAX_DELAY_MICROSECONDS = 86_400_000_000
MAX_RATE_PER_SECOND = 1_000.0
MIN_RATE_PER_SECOND = 0.001
MAX_BURST = 1_000
MIN_TOKEN_MICROSECONDS = 1

_MAX_JITTER_SEED = 9_223_372_036_854_775_807
_MAX_BACKOFF_SHIFT = 16
_MAX_CONVERSION_ERROR_MICROSECONDS = 1e-6
_MICROSECONDS_PER_DAY = 86_400_000_000

_UNIT_FACTORS: dict[str, int] = {
    "microseconds": 1,
    "milliseconds": 1_000,
    "seconds": 1_000_000,
    "minutes": 60_000_000,
}

_DELAY_TEXT_PATTERN: re.Pattern[str] = re.compile(
    r"(?P<integer>[0-9]+)(?:\.(?P<fraction>[0-9]+))?(?P<unit>us|ms|s)",
    flags=re.ASCII,
)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class ClockPolicyError(RuntimeError):
    """Base class for injected-clock policy failures."""


class DelayValueError(ClockPolicyError):
    """A delay input is negative, non-finite, overflowed, malformed, or ambiguous."""


class DelayPolicyExceededError(ClockPolicyError):
    """A structurally valid delay exceeds the captured policy ceiling."""


class RatePolicyError(ClockPolicyError):
    """A rate or burst configuration, or a refill request, is invalid."""


class ClockProtocolError(ClockPolicyError):
    """An injected clock accepted or produced an invalid instant."""


class ClockPolicyVersionError(ClockPolicyError):
    """A clock-policy value object uses an unknown version."""


@runtime_checkable
class PolicyClock(Protocol):
    """Borrowed clock behind every delay, refill, and eligibility decision."""

    def now(self) -> UtcTimestamp:
        """Return the current exact UTC instant."""
        ...


class ManualClock:
    """Deterministic clock advanced only through explicit scenario steps."""

    __slots__ = (
        "_current",
        "_lock",
    )

    def __init__(self, initial: UtcTimestamp) -> None:
        self._current = _require_exact(initial, UtcTimestamp, "manual clock initial value")
        self._lock = Lock()

    def now(self) -> UtcTimestamp:
        """Return the current value without consulting any ambient clock."""
        with self._lock:
            return self._current

    def advance(self, microseconds: int) -> None:
        """Move the clock forward by an exact non-negative microsecond count."""
        if type(microseconds) is not int:
            raise DelayValueError("manual clock advance must be an exact integer")
        if microseconds < 0:
            raise DelayValueError("manual clock advance must be non-negative")
        with self._lock:
            try:
                candidate = self._current.to_datetime() + timedelta(microseconds=microseconds)
            except OverflowError, TypeError, ValueError:
                raise DelayValueError(
                    "manual clock advance overflows the timestamp bounds"
                ) from None
            self._current = UtcTimestamp(candidate)

    def advance_to(self, timestamp: UtcTimestamp) -> None:
        """Move the clock forward to an equal or later exact instant."""
        target = _require_exact(timestamp, UtcTimestamp, "manual clock target")
        with self._lock:
            if target < self._current:
                raise ClockProtocolError("manual clock cannot move backwards")
            self._current = target


@dataclass(frozen=True, slots=True)
class DelayValue:
    """A validated non-negative delay bounded to the 24-hour policy maximum."""

    microseconds: int
    version: int = CLOCK_POLICY_VERSION

    def __post_init__(self) -> None:
        if type(self.microseconds) is not int:
            raise TypeError("delay microseconds must be an integer number of microseconds")
        if self.microseconds < 0:
            raise DelayValueError("delay microseconds must be non-negative")
        if self.microseconds > MAX_DELAY_MICROSECONDS:
            raise DelayValueError("delay overflows the supported maximum")
        if type(self.version) is not int or self.version != CLOCK_POLICY_VERSION:
            raise ClockPolicyVersionError(f"delay version must equal {CLOCK_POLICY_VERSION}")

    @classmethod
    def parse(cls, value: object, *, unit: str) -> DelayValue:
        """Parse an exact magnitude in one unambiguous unit, failing closed."""
        factor = _unit_factor(unit)
        if type(value) is int:
            if value < 0:
                raise DelayValueError("delay integer must be non-negative")
            micros = value * factor
            if micros > MAX_DELAY_MICROSECONDS:
                raise DelayValueError("delay conversion overflows the supported maximum")
            return cls(microseconds=micros)
        if type(value) is float:
            if not isfinite(value):
                raise DelayValueError("delay float must be finite")
            if value < 0.0:
                raise DelayValueError("delay float must be non-negative")
            scaled = value * factor
            rounded = round(scaled)
            if abs(scaled - rounded) > _MAX_CONVERSION_ERROR_MICROSECONDS:
                raise DelayValueError("delay float must convert to whole microseconds")
            if rounded > MAX_DELAY_MICROSECONDS:
                raise DelayValueError("delay conversion overflows the supported maximum")
            return cls(microseconds=rounded)
        raise DelayValueError("delay value must be an exact integer or float")

    @classmethod
    def parse_text(cls, value: str) -> DelayValue:
        """Parse exactly the canonical ``<int>us``, ``<int>ms``, or ``<float>s`` forms."""
        if type(value) is not str:
            raise DelayValueError("delay text must be an exact string")
        match = _DELAY_TEXT_PATTERN.fullmatch(value)
        if match is None:
            raise DelayValueError(
                "delay text must use canonical <int>us, <int>ms, or <float>s form"
            )
        unit = match.group("unit")
        integer = match.group("integer")
        fraction = match.group("fraction")
        if fraction is not None and unit != "s":
            raise DelayValueError("delay text accepts fractional magnitudes only for seconds")
        if unit == "s":
            return cls.parse(float(f"{integer}.{fraction or '0'}"), unit="seconds")
        if unit == "us":
            return cls.parse(int(integer), unit="microseconds")
        return cls.parse(int(integer), unit="milliseconds")

    def plus(self, other: DelayValue) -> DelayValue:
        """Add two validated delays, failing closed on overflow past the maximum."""
        total = _delay_microseconds(self, "delay addition base") + _delay_microseconds(
            other, "delay addition operand"
        )
        if total > MAX_DELAY_MICROSECONDS:
            raise DelayValueError("delay addition overflows the supported maximum")
        return DelayValue(microseconds=total)

    def against(self, clock_now: UtcTimestamp) -> UtcTimestamp:
        """Return the exact instant reached by applying this delay to a base instant."""
        base = _require_exact(clock_now, UtcTimestamp, "delay base timestamp")
        return _add_microseconds(base, _delay_microseconds(self, "delay value"))

    def to_microseconds(self) -> int:
        """Return the exact validated microsecond count."""
        return self.microseconds


def validate_delay_against_policy(
    delay: DelayValue,
    settings: CapturedConcurrencySettings,
) -> DelayValue:
    """Accept a delay unchanged only when it fits the captured timeout ceiling."""
    micros = _delay_microseconds(delay, "policy delay")
    captured = _require_exact(
        settings,
        CapturedConcurrencySettings,
        "captured concurrency settings",
    )
    ceiling_micros = round(
        max(captured.cleanup_timeout_seconds, captured.shutdown_timeout_seconds) * 1_000_000
    )
    if micros > ceiling_micros:
        raise DelayPolicyExceededError("delay exceeds the captured timeout policy ceiling")
    return delay


@dataclass(frozen=True, slots=True)
class RateLimitPolicy:
    """A bounded token-refill rate policy with fail-closed versioning."""

    rate_per_second: float
    burst: int = 1
    version: int = CLOCK_POLICY_VERSION

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != CLOCK_POLICY_VERSION:
            raise ClockPolicyVersionError(f"rate policy version must equal {CLOCK_POLICY_VERSION}")
        if type(self.rate_per_second) is not float:
            raise RatePolicyError("rate must be an exact float")
        if not isfinite(self.rate_per_second):
            raise RatePolicyError("rate must be finite")
        if not MIN_RATE_PER_SECOND <= self.rate_per_second <= MAX_RATE_PER_SECOND:
            raise RatePolicyError("rate is outside the supported range")
        if type(self.burst) is not int:
            raise RatePolicyError("burst must be an exact integer")
        if not 1 <= self.burst <= MAX_BURST:
            raise RatePolicyError("burst is outside the supported range")

    @classmethod
    def per_second(cls, rate: float, *, burst: int = 1) -> RateLimitPolicy:
        """Build one policy from a per-second rate and optional burst."""
        return cls(rate_per_second=rate, burst=burst)

    def token_interval_micros(self) -> int:
        """Return the whole-microsecond spacing between refilled tokens."""
        return max(round(1_000_000 / self.rate_per_second), MIN_TOKEN_MICROSECONDS)


class TokenBucket:
    """Deterministic token bucket refilled only from injected instants."""

    __slots__ = (
        "_burst",
        "_interval_micros",
        "_last_seen_micros",
        "_lock",
        "_policy",
        "_tokens",
    )

    def __init__(self, policy: RateLimitPolicy, initial_tokens: int | None = None) -> None:
        selected = _require_exact(policy, RateLimitPolicy, "token bucket policy")
        if initial_tokens is None:
            tokens = selected.burst
        else:
            if type(initial_tokens) is not int:
                raise RatePolicyError("token bucket initial tokens must be an exact integer")
            if not 0 <= initial_tokens <= selected.burst:
                raise RatePolicyError("token bucket initial tokens are outside the burst bound")
            tokens = initial_tokens
        self._policy = selected
        self._burst = selected.burst
        self._interval_micros = selected.token_interval_micros()
        self._tokens = tokens
        self._last_seen_micros: int | None = None
        self._lock = Lock()

    def state(self, now: UtcTimestamp) -> tuple[int, int]:
        """Return the pure refill projection at one injected instant."""
        instant = _require_exact(now, UtcTimestamp, "token bucket instant")
        now_micros = _epoch_micros(instant)
        with self._lock:
            tokens, _advanced = self._refilled(now_micros)
        return tokens, now_micros

    def try_acquire(self, now: UtcTimestamp, tokens: int = 1) -> bool:
        """Refill from the injected instant and acquire without ever blocking."""
        instant = _require_exact(now, UtcTimestamp, "token bucket instant")
        requested = _validate_token_count(tokens)
        now_micros = _epoch_micros(instant)
        with self._lock:
            available, advanced = self._refilled(now_micros)
            if available >= requested:
                self._tokens = available - requested
                acquired = True
            else:
                self._tokens = available
                acquired = False
            self._last_seen_micros = advanced
        return acquired

    def acquire_at(self, now: UtcTimestamp, tokens: int = 1) -> UtcTimestamp:
        """Return the earliest injected instant at which the acquisition would succeed."""
        instant = _require_exact(now, UtcTimestamp, "token bucket instant")
        requested = _validate_token_count(tokens)
        if requested > self._burst:
            raise RatePolicyError("token request exceeds the burst bound and can never succeed")
        now_micros = _epoch_micros(instant)
        with self._lock:
            available, advanced = self._refilled(now_micros)
            if available >= requested:
                return instant
            target_micros = advanced + (requested - available) * self._interval_micros
        return _timestamp_from_epoch_micros(target_micros)

    def _refilled(self, now_micros: int) -> tuple[int, int]:
        last_seen = self._last_seen_micros
        if last_seen is None:
            return self._tokens, now_micros
        if now_micros < last_seen:
            raise RatePolicyError("token bucket instant precedes the last observed instant")
        interval = self._interval_micros
        refills = (now_micros - last_seen) // interval
        return min(self._burst, self._tokens + refills), last_seen + refills * interval


@dataclass(frozen=True, slots=True, repr=False)
class SeededJitterSchedule:
    """Call-order-independent seeded schedule producing exact eligibility instants."""

    seed: int
    version: int = CLOCK_POLICY_VERSION

    def __post_init__(self) -> None:
        _validate_schedule_seed(self.seed)
        if type(self.version) is not int or self.version != CLOCK_POLICY_VERSION:
            raise ClockPolicyVersionError(
                f"jitter schedule version must equal {CLOCK_POLICY_VERSION}"
            )

    def eligibility(
        self,
        failed_at: UtcTimestamp,
        attempt_number: int,
        base: DelayValue,
        cap: DelayValue,
    ) -> UtcTimestamp:
        """Return the exact retry eligibility instant for one failed attempt."""
        seed = _validate_schedule_seed(self.seed)
        failure_time = _require_exact(failed_at, UtcTimestamp, "jitter failure time")
        attempt = _validate_attempt_number(attempt_number)
        base_micros = _delay_microseconds(base, "jitter base delay")
        cap_micros = _delay_microseconds(cap, "jitter cap delay")
        if cap_micros < base_micros:
            raise DelayValueError("jitter cap must not be below the base delay")
        jitter = _jitter_fraction(seed, attempt)
        shift = min(attempt - 1, _MAX_BACKOFF_SHIFT)
        scaled = round(base_micros * (1 << shift) * (1.0 + jitter))
        return _add_microseconds(failure_time, min(scaled, cap_micros))

    def __repr__(self) -> str:
        return "SeededJitterSchedule(seed=<redacted>)"


def _require_exact[T](value: object, expected: type[T], subject: str) -> T:
    if type(value) is not expected:
        raise TypeError(f"{subject} must use {expected.__name__}")
    return cast(T, value)


def _unit_factor(unit: object) -> int:
    if type(unit) is not str:
        raise DelayValueError("delay unit must be an exact string")
    factor = _UNIT_FACTORS.get(unit)
    if factor is None:
        raise DelayValueError("delay unit is ambiguous")
    return factor


def _delay_microseconds(value: object, subject: str) -> int:
    selected = _require_exact(value, DelayValue, subject)
    micros = selected.microseconds
    if type(micros) is not int or not 0 <= micros <= MAX_DELAY_MICROSECONDS:
        raise DelayValueError(f"{subject} carries invalid microseconds")
    return micros


def _add_microseconds(base: UtcTimestamp, micros: int) -> UtcTimestamp:
    try:
        return UtcTimestamp(base.to_datetime() + timedelta(microseconds=micros))
    except OverflowError, TypeError, ValueError:
        raise DelayValueError("delay addition overflows the timestamp bounds") from None


def _epoch_micros(instant: UtcTimestamp) -> int:
    elapsed = instant.to_datetime() - _EPOCH
    return (elapsed.days * 86_400 + elapsed.seconds) * 1_000_000 + elapsed.microseconds


def _timestamp_from_epoch_micros(micros: int) -> UtcTimestamp:
    days, remainder = divmod(micros, _MICROSECONDS_PER_DAY)
    try:
        return UtcTimestamp(_EPOCH + timedelta(days=days, microseconds=remainder))
    except OverflowError, TypeError, ValueError:
        raise RatePolicyError("token acquisition time exceeds the timestamp bounds") from None


def _validate_token_count(value: object) -> int:
    if type(value) is not int:
        raise RatePolicyError("token count must be an exact integer")
    if value < 1:
        raise RatePolicyError("token count must be at least one token")
    return value


def _validate_schedule_seed(value: object) -> int:
    if type(value) is not int:
        raise TypeError("jitter schedule seed must be an integer")
    if not 0 <= value <= _MAX_JITTER_SEED:
        raise DelayValueError("jitter schedule seed is outside policy bounds")
    return value


def _validate_attempt_number(value: object) -> int:
    if type(value) is not int:
        raise TypeError("jitter attempt number must be an integer")
    if value < 1:
        raise DelayValueError("jitter attempt number must be at least one")
    return value


def _jitter_fraction(seed: int, attempt_number: int) -> float:
    payload = b"\x00".join(
        (
            b"paritygrid-clock-jitter-v1",
            str(seed).encode("ascii"),
            str(attempt_number).encode("ascii"),
        )
    )
    digest = sha256(payload).digest()
    return (int.from_bytes(digest[:8], "big") >> 11) / float(1 << 53)


__all__ = [
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
