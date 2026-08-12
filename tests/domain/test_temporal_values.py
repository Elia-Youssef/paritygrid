"""Example-based verification of temporal domain values."""

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone

import pytest

from paritygrid.domain.models import Duration, UtcTimestamp


def test_timestamp_normalizes_an_explicit_offset_to_utc() -> None:
    timestamp = UtcTimestamp(
        datetime(2026, 8, 12, 15, 45, 3, 1200, tzinfo=timezone(timedelta(hours=3)))
    )

    assert timestamp.to_datetime() == datetime(2026, 8, 12, 12, 45, 3, 1200, tzinfo=UTC)
    assert str(timestamp) == "2026-08-12T12:45:03.001200Z"


@pytest.mark.parametrize(
    ("source", "canonical"),
    [
        ("2024-02-29T23:59:59Z", "2024-02-29T23:59:59.000000Z"),
        ("2024-01-01T00:00:00.1+02:30", "2023-12-31T21:30:00.100000Z"),
        ("2024-01-01T00:00:00.123456-05:15", "2024-01-01T05:15:00.123456Z"),
    ],
)
def test_timestamp_parsing_has_a_stable_utc_round_trip(source: str, canonical: str) -> None:
    timestamp = UtcTimestamp.parse(source)

    assert str(timestamp) == canonical
    assert bytes(timestamp) == canonical.encode("ascii")
    assert UtcTimestamp.parse(str(timestamp)) == timestamp
    assert UtcTimestamp.from_bytes(timestamp.to_bytes()) == timestamp


def test_timestamp_accepts_supported_datetime_boundaries() -> None:
    minimum = UtcTimestamp(datetime.min.replace(tzinfo=UTC))
    maximum = UtcTimestamp(datetime.max.replace(tzinfo=UTC))

    assert str(minimum) == "0001-01-01T00:00:00.000000Z"
    assert str(maximum) == "9999-12-31T23:59:59.999999Z"
    assert minimum < maximum


def test_timestamp_is_immutable_and_hashable() -> None:
    timestamp = UtcTimestamp.parse("2026-01-01T00:00:00Z")

    assert hash(timestamp) == hash(UtcTimestamp.parse("2026-01-01T00:00:00+00:00"))
    with pytest.raises(FrozenInstanceError):
        timestamp.value = datetime.now(UTC)  # type: ignore[misc]


@pytest.mark.parametrize(
    "value",
    [
        "2024-01-01 00:00:00Z",
        "2024-01-01T00:00:00z",
        "2024-01-01T00:00:00",
        "2024-01-01T00:00:00.1234567Z",
        "2023-02-29T00:00:00Z",
        "2024-13-01T00:00:00Z",
        "2024-01-01T24:00:00Z",
        "2024-01-01T23:59:60Z",
        "2024-01-01T00:00:00+24:00",
        "2024-01-01T00:00:00+01:60",
        "2024-01-01T00:00:00-00:00",
    ],
)
def test_timestamp_rejects_noncanonical_or_invalid_input(value: str) -> None:
    with pytest.raises(ValueError, match="timestamp"):
        UtcTimestamp.parse(value)


def test_timestamp_rejects_naive_datetime_and_wrong_runtime_type() -> None:
    with pytest.raises(ValueError, match="offset"):
        UtcTimestamp(datetime(2026, 1, 1))
    with pytest.raises(TypeError, match="datetime"):
        UtcTimestamp("2026-01-01T00:00:00Z")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="text"):
        UtcTimestamp.parse(42)  # type: ignore[arg-type]


def test_timestamp_rejects_offset_normalization_beyond_datetime_bounds() -> None:
    with pytest.raises(ValueError, match="range"):
        UtcTimestamp(datetime.min.replace(tzinfo=timezone(timedelta(hours=1))))
    with pytest.raises(ValueError, match="range"):
        UtcTimestamp(datetime.max.replace(tzinfo=timezone(timedelta(hours=-1))))


def test_timestamp_bytes_reject_wrong_type_and_non_ascii() -> None:
    with pytest.raises(TypeError, match="bytes"):
        UtcTimestamp.from_bytes(bytearray(b"2024-01-01T00:00:00Z"))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="ASCII"):
        UtcTimestamp.from_bytes("2024-01-01T00:00:00é".encode())


@pytest.mark.parametrize(
    ("microseconds", "canonical"),
    [
        (0, "PT0.000000S"),
        (1, "PT0.000001S"),
        (1_000_000, "PT1.000000S"),
        (61_000_042, "PT61.000042S"),
        (Duration.MAX_MICROSECONDS, "PT31536000.000000S"),
    ],
)
def test_duration_round_trips_supported_boundaries(microseconds: int, canonical: str) -> None:
    duration = Duration(microseconds=microseconds)

    assert str(duration) == canonical
    assert bytes(duration) == canonical.encode("ascii")
    assert Duration.parse(canonical) == duration
    assert Duration.from_bytes(duration.to_bytes()) == duration
    assert Duration.from_timedelta(duration.to_timedelta()) == duration


def test_duration_converts_timedelta_without_floating_point() -> None:
    duration = Duration.from_timedelta(timedelta(days=2, seconds=3, microseconds=4))

    assert duration.microseconds == 172_803_000_004


@pytest.mark.parametrize("value", [-1, Duration.MAX_MICROSECONDS + 1])
def test_duration_rejects_out_of_range_values(value: int) -> None:
    with pytest.raises(ValueError, match="between"):
        Duration(microseconds=value)


@pytest.mark.parametrize("value", [True, 1.5, "1"])
def test_duration_rejects_non_integer_values(value: object) -> None:
    with pytest.raises(TypeError, match="integer"):
        Duration(microseconds=value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "value",
    [
        "",
        "P0D",
        "PT1S",
        "PT1.0S",
        "PT01.000000S",
        "PT-1.000000S",
        "pt1.000000s",
        "PT31536000.000001S",
        "PT999999999999999999.000000S",
    ],
)
def test_duration_rejects_noncanonical_or_out_of_range_text(value: str) -> None:
    with pytest.raises(ValueError, match="duration"):
        Duration.parse(value)


def test_duration_rejects_wrong_parse_and_conversion_types() -> None:
    with pytest.raises(TypeError, match="text"):
        Duration.parse(1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="timedelta"):
        Duration.from_timedelta(1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="bytes"):
        Duration.from_bytes(bytearray(b"PT0.000000S"))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="ASCII"):
        Duration.from_bytes("PT0.00000éS".encode())
