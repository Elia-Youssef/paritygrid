"""Property verification for bounded temporal values."""

from datetime import datetime, timedelta, timezone

from hypothesis import given
from hypothesis import strategies as st

from paritygrid.domain.models import Duration, UtcTimestamp


@given(
    st.datetimes(
        min_value=datetime(2, 1, 2),
        max_value=datetime(9998, 12, 30, 23, 59, 59, 999999),
    ),
    st.integers(min_value=-1_439, max_value=1_439),
)
def test_timestamp_normalization_round_trip_is_stable(naive: datetime, offset_minutes: int) -> None:
    aware = naive.replace(tzinfo=timezone(timedelta(minutes=offset_minutes)))

    timestamp = UtcTimestamp(aware)

    assert UtcTimestamp.parse(str(timestamp)) == timestamp
    assert UtcTimestamp.from_bytes(bytes(timestamp)) == timestamp
    assert timestamp.value.utcoffset() == timedelta(0)


@given(st.integers(min_value=0, max_value=Duration.MAX_MICROSECONDS))
def test_duration_round_trip_is_exact(microseconds: int) -> None:
    duration = Duration(microseconds=microseconds)

    assert Duration.parse(str(duration)) == duration
    assert Duration.from_timedelta(duration.to_timedelta()) == duration
