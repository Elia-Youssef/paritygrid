"""Bounded temporal values with canonical UTC representations."""

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from typing import ClassVar, Self

_TIMESTAMP_PATTERN = re.compile(
    r"(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})"
    r"T(?P<hour>[0-9]{2}):(?P<minute>[0-9]{2}):(?P<second>[0-9]{2})"
    r"(?:\.(?P<fraction>[0-9]{1,6}))?(?P<offset>Z|[+-][0-9]{2}:[0-9]{2})",
    flags=re.ASCII,
)
_DURATION_PATTERN = re.compile(
    r"PT(?P<seconds>0|[1-9][0-9]*)\.(?P<microseconds>[0-9]{6})S",
    flags=re.ASCII,
)
_MICROSECONDS_PER_SECOND = 1_000_000
_SECONDS_PER_DAY = 86_400


@dataclass(frozen=True, slots=True, order=True)
class UtcTimestamp:
    """An aware instant normalized to UTC with microsecond precision."""

    value: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _normalize_utc(self.value))

    @classmethod
    def parse(cls, value: str) -> Self:
        """Parse a bounded ISO-8601 instant and normalize its offset to UTC."""
        text = _require_text(value, subject="timestamp")
        match = _TIMESTAMP_PATTERN.fullmatch(text)
        if match is None:
            raise ValueError("timestamp must use an ISO-8601 date, time, and explicit offset")

        fraction = (match.group("fraction") or "").ljust(6, "0")
        offset = match.group("offset")
        try:
            parsed = datetime(
                year=int(match.group("year")),
                month=int(match.group("month")),
                day=int(match.group("day")),
                hour=int(match.group("hour")),
                minute=int(match.group("minute")),
                second=int(match.group("second")),
                microsecond=int(fraction or "0"),
                tzinfo=UTC if offset == "Z" else _parse_offset(offset),
            )
        except ValueError as error:
            raise ValueError("timestamp contains an invalid date, time, or offset") from error
        return cls(value=parsed)

    @classmethod
    def from_bytes(cls, value: bytes) -> Self:
        """Parse an ASCII ISO-8601 representation."""
        return cls.parse(_decode_ascii(value, subject="timestamp"))

    def to_datetime(self) -> datetime:
        """Return the immutable UTC datetime value."""
        return self.value

    def to_bytes(self) -> bytes:
        """Return the canonical UTC representation as ASCII."""
        return str(self).encode("ascii")

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def __str__(self) -> str:
        value = self.value
        return (
            f"{value.year:04d}-{value.month:02d}-{value.day:02d}"
            f"T{value.hour:02d}:{value.minute:02d}:{value.second:02d}"
            f".{value.microsecond:06d}Z"
        )


@dataclass(frozen=True, slots=True, order=True)
class Duration:
    """A nonnegative microsecond duration bounded to one year."""

    MAX_MICROSECONDS: ClassVar[int] = 365 * _SECONDS_PER_DAY * _MICROSECONDS_PER_SECOND

    microseconds: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "microseconds", _validate_duration(self.microseconds))

    @classmethod
    def parse(cls, value: str) -> Self:
        """Parse the canonical fixed-microsecond ISO-8601 duration form."""
        text = _require_text(value, subject="duration")
        match = _DURATION_PATTERN.fullmatch(text)
        if match is None:
            raise ValueError("duration must use canonical PT<seconds>.<microseconds>S form")
        seconds_text = match.group("seconds")
        if len(seconds_text) > len(str(cls.MAX_MICROSECONDS // _MICROSECONDS_PER_SECOND)):
            raise ValueError("duration exceeds the supported maximum")
        microseconds = int(seconds_text) * _MICROSECONDS_PER_SECOND + int(
            match.group("microseconds")
        )
        return cls(microseconds=microseconds)

    @classmethod
    def from_timedelta(cls, value: timedelta) -> Self:
        """Create a duration without converting through floating-point seconds."""
        value = _require_timedelta(value)
        microseconds = (
            value.days * _SECONDS_PER_DAY + value.seconds
        ) * _MICROSECONDS_PER_SECOND + value.microseconds
        return cls(microseconds=microseconds)

    @classmethod
    def from_bytes(cls, value: bytes) -> Self:
        """Parse canonical ASCII duration bytes."""
        return cls.parse(_decode_ascii(value, subject="duration"))

    def to_timedelta(self) -> timedelta:
        """Return the exact standard-library duration."""
        return timedelta(microseconds=self.microseconds)

    def to_bytes(self) -> bytes:
        """Return the canonical ASCII duration representation."""
        return str(self).encode("ascii")

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def __str__(self) -> str:
        seconds, microseconds = divmod(self.microseconds, _MICROSECONDS_PER_SECOND)
        return f"PT{seconds}.{microseconds:06d}S"


def _normalize_utc(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("timestamp value must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include an explicit UTC offset")
    try:
        return value.astimezone(UTC)
    except OverflowError as error:
        raise ValueError("timestamp offset exceeds the supported datetime range") from error


def _parse_offset(value: str) -> timezone:
    if value == "-00:00":
        raise ValueError("timestamp offset must represent a known UTC relationship")
    sign = 1 if value[0] == "+" else -1
    hours = int(value[1:3])
    minutes = int(value[4:6])
    if hours > 23 or minutes > 59:
        raise ValueError("timestamp offset is outside the supported range")
    return timezone(sign * timedelta(hours=hours, minutes=minutes))


def _validate_duration(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("duration must be an integer number of microseconds")
    if not 0 <= value <= Duration.MAX_MICROSECONDS:
        raise ValueError("duration must be between zero and one year")
    return value


def _require_text(value: object, *, subject: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{subject} representation must be text")
    return value


def _require_timedelta(value: object) -> timedelta:
    if not isinstance(value, timedelta):
        raise TypeError("duration source must be a timedelta")
    return value


def _decode_ascii(value: object, *, subject: str) -> str:
    if not isinstance(value, bytes):
        raise TypeError(f"{subject} encoding must be bytes")
    try:
        return value.decode("ascii")
    except UnicodeDecodeError as error:
        raise ValueError(f"{subject} encoding must contain only ASCII") from error
