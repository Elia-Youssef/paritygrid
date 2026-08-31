"""Bounded ephemeral telemetry fan-out for live operational channels.

The hub is deliberately lossy: publishing never blocks, every subscriber
queue is bounded with drop-oldest sampling, and no record is ever persisted
or treated as authoritative execution state.  Durable truth lives entirely
in the committed run, event, and verification stores.
"""

from collections import deque
from collections.abc import Callable
from threading import Lock

from paritygrid.application.execution.telemetry import (
    TELEMETRY_SCHEMA_VERSION,
    TelemetryMetric,
    TelemetryMetricKind,
    TelemetryRecord,
)
from paritygrid.application.ports.writer import WriterDiagnostics
from paritygrid.domain.models import UtcTimestamp

MAX_TELEMETRY_QUEUE_CAPACITY = 1_024
MAX_TELEMETRY_SUBSCRIBERS_PER_RUN = 16


class TelemetrySubscriberLimitError(Exception):
    """One run already holds the maximum number of live subscribers."""


class TelemetrySubscription:
    """One bounded drop-oldest queue owned by a single live consumer."""

    def __init__(self, *, run_id: str, capacity: int) -> None:
        self._run_id = run_id
        self._records: deque[TelemetryRecord] = deque(maxlen=capacity)
        self._dropped = 0
        self._closed = False

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def dropped(self) -> int:
        return self._dropped

    @property
    def closed(self) -> bool:
        return self._closed

    def offer(self, record: TelemetryRecord) -> None:
        """Retain one record, sampling the oldest when the queue is full."""
        if self._closed:
            return
        if len(self._records) == self._records.maxlen:
            self._dropped += 1
        self._records.append(record)

    def drain(self) -> tuple[TelemetryRecord, ...]:
        """Return and clear the retained records in arrival order."""
        drained = tuple(self._records)
        self._records.clear()
        return drained

    def close(self) -> None:
        """Release the queue; later offers are ignored."""
        self._closed = True
        self._records.clear()


class LiveTelemetryHub:
    """Fan out bounded telemetry records to per-run live subscribers."""

    def __init__(
        self,
        *,
        queue_capacity: int = 256,
        max_subscribers_per_run: int = MAX_TELEMETRY_SUBSCRIBERS_PER_RUN,
    ) -> None:
        if type(queue_capacity) is not int or not 1 <= queue_capacity <= (
            MAX_TELEMETRY_QUEUE_CAPACITY
        ):
            raise ValueError("telemetry queue capacity is outside the supported range")
        if type(max_subscribers_per_run) is not int or not 1 <= max_subscribers_per_run <= 128:
            raise ValueError("telemetry subscriber limit is outside the supported range")
        self._queue_capacity = queue_capacity
        self._max_subscribers = max_subscribers_per_run
        self._subscribers: dict[str, list[TelemetrySubscription]] = {}
        self._lock = Lock()
        self._closed = False

    @property
    def queue_capacity(self) -> int:
        return self._queue_capacity

    @property
    def max_subscribers_per_run(self) -> int:
        return self._max_subscribers

    def publish(self, record: TelemetryRecord) -> None:
        """Offer one record to every live subscriber without blocking."""
        if self._closed:
            return
        with self._lock:
            subscribers = tuple(self._subscribers.get(record.run_id, ()))
        for subscription in subscribers:
            subscription.offer(record)

    def subscribe(self, run_id: str) -> TelemetrySubscription:
        """Register one bounded subscriber queue for a run."""
        if type(run_id) is not str or not run_id:
            raise ValueError("telemetry subscription requires a run identity")
        subscription = TelemetrySubscription(run_id=run_id, capacity=self._queue_capacity)
        with self._lock:
            if self._closed:
                raise TelemetrySubscriberLimitError("the telemetry hub is closed")
            live = self._subscribers.setdefault(run_id, [])
            live = [item for item in live if not item.closed]
            if len(live) >= self._max_subscribers:
                self._subscribers[run_id] = live
                raise TelemetrySubscriberLimitError(
                    "the run already holds the maximum number of live subscribers"
                )
            live.append(subscription)
            self._subscribers[run_id] = live
        return subscription

    def subscriber_count(self, run_id: str) -> int:
        """Return the number of live subscribers for one run."""
        with self._lock:
            return sum(1 for item in self._subscribers.get(run_id, ()) if not item.closed)

    def close(self) -> None:
        """Close every subscriber so live channels terminate deterministically."""
        with self._lock:
            self._closed = True
            subscribers = tuple(
                subscription for live in self._subscribers.values() for subscription in live
            )
            self._subscribers.clear()
        for subscription in subscribers:
            subscription.close()

    @property
    def closed(self) -> bool:
        return self._closed


def snapshot_record(
    *,
    run_id: str,
    observed_at_micros: int,
    queue_depth: int,
    queue_capacity: int,
) -> TelemetryRecord:
    """Render one bounded writer-queue snapshot as a telemetry record."""
    return TelemetryRecord(
        schema_version=TELEMETRY_SCHEMA_VERSION,
        observed_at_micros=observed_at_micros,
        run_id=run_id,
        metrics=(
            TelemetryMetric(
                name="writer_queue_depth",
                kind=TelemetryMetricKind.QUEUE_DEPTH,
                value=queue_depth,
                labels=(("channel", "writer"),),
            ),
            TelemetryMetric(
                name="writer_queue_capacity",
                kind=TelemetryMetricKind.ACTIVE_CAPACITY,
                value=queue_capacity,
                labels=(("channel", "writer"),),
            ),
        ),
    )


class LiveTelemetryChannel:
    """Compose the hub, writer snapshot, and clock for the live transport."""

    def __init__(
        self,
        *,
        hub: LiveTelemetryHub,
        writer_snapshot: Callable[[], WriterDiagnostics],
        clock: Callable[[], UtcTimestamp],
        send_timeout_seconds: float = 10.0,
        poll_seconds: float = 0.25,
    ) -> None:
        if type(send_timeout_seconds) is not float or not 0.1 <= send_timeout_seconds <= 60.0:
            raise ValueError("telemetry send timeout is outside the supported range")
        if type(poll_seconds) is not float or not 0.05 <= poll_seconds <= 30.0:
            raise ValueError("telemetry poll interval is outside the supported range")
        self.hub = hub
        self._writer_snapshot = writer_snapshot
        self._clock = clock
        self._send_timeout_seconds = send_timeout_seconds
        self._poll_seconds = poll_seconds

    @property
    def send_timeout_seconds(self) -> float:
        return self._send_timeout_seconds

    @property
    def poll_seconds(self) -> float:
        return self._poll_seconds

    def snapshot_for(self, *, run_id: str) -> TelemetryRecord:
        """Return one bounded connection snapshot for a run."""
        diagnostics = self._writer_snapshot()
        return snapshot_record(
            run_id=run_id,
            observed_at_micros=_observed_at_micros(self._clock()),
            queue_depth=diagnostics.queue_depth,
            queue_capacity=diagnostics.queue_capacity,
        )

    def publish_snapshot(self, *, run_id: str) -> None:
        """Publish one current writer snapshot without affecting durable state.

        The live transport calls this on its bounded poll cadence.  It is a
        production source of sampled operational telemetry, not a test hook:
        publication only reads the writer diagnostics and offers the result to
        lossy subscriber queues.
        """
        self.hub.publish(self.snapshot_for(run_id=run_id))


def _observed_at_micros(timestamp: UtcTimestamp) -> int:
    moment = timestamp.to_datetime()
    epoch = moment.timestamp()
    return int(epoch * 1_000_000)


__all__ = [
    "MAX_TELEMETRY_QUEUE_CAPACITY",
    "MAX_TELEMETRY_SUBSCRIBERS_PER_RUN",
    "LiveTelemetryChannel",
    "LiveTelemetryHub",
    "TelemetrySubscriberLimitError",
    "TelemetrySubscription",
    "snapshot_record",
]
