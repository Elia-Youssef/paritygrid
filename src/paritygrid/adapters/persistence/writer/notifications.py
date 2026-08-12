"""Bounded nonblocking storage for committed notification metadata."""

from collections import deque
from threading import Lock

from paritygrid.application.ports.writer import (
    CommittedNotification,
    CommittedNotificationBuffer,
    NotificationBufferStats,
)


class BoundedCommittedNotificationBuffer(CommittedNotificationBuffer):
    """Retain a bounded FIFO of post-commit facts without blocking writes."""

    def __init__(self, capacity: int) -> None:
        if type(capacity) is not int or not 1 <= capacity <= 10_000:
            raise ValueError("notification capacity is outside the supported range")
        self._capacity = capacity
        self._items: deque[CommittedNotification] = deque()
        self._lock = Lock()
        self._accepting = True
        self._offered = 0
        self._accepted = 0
        self._dropped = 0
        self._rejected = 0
        self._failures = 0

    def offer(self, notification: CommittedNotification) -> bool:
        """Try to retain one notification without waiting for capacity."""
        if type(notification) is not CommittedNotification:
            raise TypeError("committed notification has an invalid type")
        with self._lock:
            self._offered += 1
            if not self._accepting:
                self._rejected += 1
                return False
            if len(self._items) >= self._capacity:
                self._dropped += 1
                return False
            self._items.append(notification)
            self._accepted += 1
            return True

    def take(self) -> CommittedNotification | None:
        with self._lock:
            return None if not self._items else self._items.popleft()

    def stats(self) -> NotificationBufferStats:
        with self._lock:
            return NotificationBufferStats(
                capacity=self._capacity,
                depth=len(self._items),
                offered=self._offered,
                accepted=self._accepted,
                dropped=self._dropped,
                rejected=self._rejected,
                failures=self._failures,
            )

    def reject_new(self) -> None:
        """Reject later offers while preserving buffered notifications."""
        with self._lock:
            self._accepting = False

    def record_failure(self) -> None:
        """Record an exceptional offer isolated by the writer."""
        with self._lock:
            self._failures += 1


__all__ = ["BoundedCommittedNotificationBuffer"]
