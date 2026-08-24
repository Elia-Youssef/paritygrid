"""Bounded closeable message channels for the execution boundary (P7.7).

Every queue and channel in the execution model is explicitly finite and
closeable: a deliberately blocked downstream consumer stops upstream
progress at the configured bound instead of growing memory. A
``BoundedChannel`` is a thread-safe FIFO of opaque immutable message
objects; message typing stays the producer's contract, and only ``None``
is rejected so a non-blocking receive can distinguish an empty channel
from a delivered message.

Waiting is bounded. The synchronous ``send`` and ``recv`` block only
while a peer keeps the channel full or empty, and every explicit
``timeout`` is a relative second count enforced through
``threading.Condition.wait``. The ambient monotonic clock is used only
for that condition timeout arithmetic: it is never a delay or rate policy
value and never becomes durable evidence (the injected policy clock of
P7.5 owns those).

Close is idempotent and releases every blocked sender and receiver with
a typed error, and accepted messages are never silently lost: a closed
channel still delivers or drains exactly what it accepted, while a sender
that was still parked at close time learns through
``ChannelClosedError.accepted_remaining`` how many accepted messages
remain drainable. An unknown writer outcome marks the channel
recovery-closed: producer admission then fails closed with
``ChannelUnknownOutcomeError`` while accepted messages stay drainable
(the parent coordinator of P7.9 injects that transition through its own
hook).

The synchronous entry points refuse to run inside an active event loop,
mirroring ``runner_contract``. The cooperative ``send_async`` and
``recv_async`` wrappers instead retry the non-blocking operations a
bounded number of times, yielding to the caller's event loop between
attempts; they never start or drive a nested event loop and never block a
loop thread. The production strategies (P7.12/P7.13) will replace this
cooperation budget with real waiting primitives.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from threading import Condition

CHANNEL_KIND_ASSIGNMENT = "assignment"
CHANNEL_KIND_RESULT = "result"
CHANNEL_KIND_TELEMETRY = "telemetry"
CHANNEL_KIND_WRITER = "writer"
MIN_CHANNEL_CAPACITY = 1
MAX_CHANNEL_CAPACITY = 65_536

_MAX_UNKNOWN_OUTCOME_REASON_LENGTH = 256

_CHANNEL_KINDS: tuple[str, ...] = (
    CHANNEL_KIND_ASSIGNMENT,
    CHANNEL_KIND_RESULT,
    CHANNEL_KIND_TELEMETRY,
    CHANNEL_KIND_WRITER,
)
_CHANNEL_KIND_SET = frozenset(_CHANNEL_KINDS)


class ChannelError(RuntimeError):
    """Base class for bounded channel failures."""


class ChannelValidationError(ChannelError):
    """A channel input has an unsupported type, kind, capacity, or bound."""


class ChannelClosedError(ChannelError):
    """An operation touched a closed channel and carries the drainable count."""

    def __init__(
        self,
        message: str = "channel is closed",
        *,
        accepted_remaining: int = 0,
    ) -> None:
        super().__init__(message)
        self.accepted_remaining = accepted_remaining


class ChannelTimeoutError(ChannelError):
    """A bounded channel wait elapsed before the operation could finish."""


class ChannelLoopError(ChannelError):
    """A blocking channel entry point was used inside a running event loop."""


class ChannelUnknownOutcomeError(ChannelClosedError):
    """Producer admission stopped after an unknown writer outcome.

    The channel fails closed for senders while every already accepted
    message remains deliverable or drainable.
    """


def _reject_running_loop(subject: str) -> None:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    raise ChannelLoopError(f"{subject} must not block while an event loop is active")


def _validate_kind(value: object) -> str:
    if type(value) is not str or value not in _CHANNEL_KIND_SET:
        raise ChannelValidationError("channel kind must name a known channel kind")
    return value


def _validate_capacity(value: object) -> int:
    if type(value) is not int or not MIN_CHANNEL_CAPACITY <= value <= MAX_CHANNEL_CAPACITY:
        raise ChannelValidationError("channel capacity is outside the supported range")
    return value


def _validate_message(value: object) -> None:
    if value is None:
        raise ChannelValidationError("channel message must be a concrete object, not None")


def _validate_timeout(value: object, subject: str) -> None:
    if value is None:
        return
    if type(value) is int or type(value) is float:
        seconds: float = value
    else:
        raise ChannelValidationError(f"{subject} must be a finite non-negative second count")
    if not math.isfinite(seconds) or seconds < 0:
        raise ChannelValidationError(f"{subject} must be a finite non-negative second count")


def _validate_iterations(value: object) -> int:
    if type(value) is not int or not 1 <= value <= MAX_CHANNEL_CAPACITY:
        raise ChannelValidationError("cooperation iteration count is outside the supported range")
    return value


def _validate_unknown_reason(value: object) -> str:
    if type(value) is not str:
        raise ChannelValidationError("unknown-outcome reason must be text")
    reason = value
    if not 1 <= len(reason) <= _MAX_UNKNOWN_OUTCOME_REASON_LENGTH:
        raise ChannelValidationError("unknown-outcome reason length is outside the supported range")
    for character in reason:
        if not "\x20" <= character <= "\x7e":
            raise ChannelValidationError(
                "unknown-outcome reason must use printable ASCII characters"
            )
    return reason


class BoundedChannel:
    """One generic thread-safe bounded closeable FIFO channel of messages.

    Messages are opaque immutable objects stored by reference; validation
    of message shape is the producer's contract. The channel never grows
    beyond ``capacity`` accepted-but-undelivered messages, so a blocked
    downstream consumer stops upstream senders at the bound instead of
    growing memory. A ``None`` timeout blocks only until a peer makes
    progress or the channel closes, which keeps every wait bounded by
    capacity plus the close contract.
    """

    __slots__ = (
        "_accepted_count",
        "_buffer",
        "_capacity",
        "_closed",
        "_condition",
        "_kind",
        "_max_observed_queued",
        "_recovery_closed",
    )

    def __init__(self, *, kind: str, capacity: int) -> None:
        self._kind = _validate_kind(kind)
        self._capacity = _validate_capacity(capacity)
        self._buffer: deque[object] = deque()
        self._condition = Condition()
        self._accepted_count = 0
        self._max_observed_queued = 0
        self._closed = False
        self._recovery_closed = False

    def __repr__(self) -> str:
        with self._condition:
            return (
                f"BoundedChannel(kind={self._kind!r}, capacity={self._capacity!r}, "
                f"queued={len(self._buffer)!r}, closed={self._closed!r}, "
                f"recovery_closed={self._recovery_closed!r})"
            )

    @property
    def kind(self) -> str:
        """Return the closed channel kind name."""
        return self._kind

    @property
    def capacity(self) -> int:
        """Return the explicit finite capacity of the bounded buffer."""
        return self._capacity

    @property
    def accepted_count(self) -> int:
        """Return how many messages the channel has ever accepted."""
        with self._condition:
            return self._accepted_count

    @property
    def queued(self) -> int:
        """Return the current count of accepted-but-undelivered messages."""
        with self._condition:
            return len(self._buffer)

    @property
    def is_closed(self) -> bool:
        """Return whether the channel has been closed."""
        with self._condition:
            return self._closed

    @property
    def is_recovery_closed(self) -> bool:
        """Return whether an unknown writer outcome stopped producer admission."""
        with self._condition:
            return self._recovery_closed

    @property
    def max_observed_queued(self) -> int:
        """Return the peak queued count, bounded by the capacity."""
        with self._condition:
            return self._max_observed_queued

    def send(self, message: object, *, timeout: float | None = None) -> None:
        """Accept one message, blocking while the bounded buffer is full.

        ``timeout`` is a relative second count; ``None`` blocks until space
        appears or the channel closes or is marked recovery-closed, both of
        which release the sender with a typed error.
        """
        _reject_running_loop("channel send")
        _validate_message(message)
        _validate_timeout(timeout, "channel send timeout")
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while True:
                if self._try_send_locked(message):
                    return
                if deadline is None:
                    self._condition.wait()
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ChannelTimeoutError(
                        "channel send timed out while the bounded buffer stayed full"
                    )
                self._condition.wait(remaining)

    def try_send(self, message: object) -> bool:
        """Try once, without waiting, to accept one message.

        Returns ``False`` when the bounded buffer is full; raises
        ``ChannelClosedError`` when the channel is closed and
        ``ChannelUnknownOutcomeError`` when producer admission stopped.
        """
        _reject_running_loop("channel try_send")
        _validate_message(message)
        return self._attempt_send(message)

    def recv(self, *, timeout: float | None = None) -> object:
        """Return the oldest accepted message, blocking while the buffer is empty.

        When the channel closes with messages still accepted, ``recv``
        keeps delivering them; only a closed and fully drained channel
        raises ``ChannelClosedError``.
        """
        _reject_running_loop("channel recv")
        _validate_timeout(timeout, "channel recv timeout")
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while True:
                item = self._try_recv_locked()
                if item is not None:
                    return item
                if deadline is None:
                    self._condition.wait()
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ChannelTimeoutError(
                        "channel receive timed out while the channel stayed empty"
                    )
                self._condition.wait(remaining)

    def try_recv(self) -> object | None:
        """Try once, without waiting, to take the oldest accepted message.

        Returns ``None`` when the buffer is empty and the channel can
        still deliver more; raises ``ChannelClosedError`` only when the
        channel is terminally empty.
        """
        _reject_running_loop("channel try_recv")
        return self._attempt_recv()

    def close(self) -> None:
        """Close the channel idempotently, releasing every blocked peer.

        Blocked senders raise ``ChannelClosedError`` because their
        messages were never accepted; blocked receivers on an empty
        channel raise ``ChannelClosedError`` as well. Closing never
        blocks and is safe from inside a running event loop so shutdown
        paths can always release waiters.
        """
        with self._condition:
            if self._closed:
                return
            self._closed = True
            self._condition.notify_all()

    def drain(self) -> tuple[object, ...]:
        """Return every accepted-but-undelivered message in FIFO order.

        Draining empties the bounded buffer, which frees space for parked
        senders, and works before or after close. No accepted message is
        ever silently lost: it is either delivered by ``recv`` or
        surfaced here.
        """
        _reject_running_loop("channel drain")
        with self._condition:
            items = tuple(self._buffer)
            self._buffer.clear()
            self._condition.notify_all()
            return items

    def mark_unknown_outcome(self, reason: str) -> None:
        """Stop producer admission after an unknown writer outcome.

        Subsequent sends fail closed with ``ChannelUnknownOutcomeError``
        while every accepted message stays deliverable and drainable.
        The transition is idempotent, wakes every blocked peer, and is
        safe from inside a running event loop. Closing afterwards is
        still allowed.
        """
        _validate_unknown_reason(reason)
        with self._condition:
            if self._recovery_closed:
                return
            self._recovery_closed = True
            self._condition.notify_all()

    async def send_async(self, message: object, *, iterations: int = 8) -> None:
        """Accept one message cooperatively without blocking the event loop.

        The wrapper retries the non-blocking send up to ``iterations``
        times, yielding with ``asyncio.sleep(0)`` between attempts so the
        caller's event loop can run the peer that frees space, and then
        raises ``ChannelTimeoutError``. It never starts a nested loop.
        """
        selected = _validate_iterations(iterations)
        _validate_message(message)
        for _ in range(selected):
            if self._attempt_send(message):
                return
            await asyncio.sleep(0)
        raise ChannelTimeoutError(
            "channel send_async exhausted its cooperation budget while the buffer stayed full"
        )

    async def recv_async(self, *, iterations: int = 8) -> object:
        """Return the oldest message cooperatively without blocking the loop.

        The wrapper retries the non-blocking receive up to ``iterations``
        times, yielding with ``asyncio.sleep(0)`` between attempts so the
        caller's event loop can run the peer that sends, and then raises
        ``ChannelTimeoutError``. It never starts a nested loop.
        """
        selected = _validate_iterations(iterations)
        for _ in range(selected):
            item = self._attempt_recv()
            if item is not None:
                return item
            await asyncio.sleep(0)
        raise ChannelTimeoutError(
            "channel recv_async exhausted its cooperation budget while the channel stayed empty"
        )

    def _attempt_send(self, message: object) -> bool:
        with self._condition:
            return self._try_send_locked(message)

    def _attempt_recv(self) -> object | None:
        with self._condition:
            return self._try_recv_locked()

    def _try_send_locked(self, message: object) -> bool:
        if self._closed:
            raise ChannelClosedError(
                "channel is closed to sends",
                accepted_remaining=len(self._buffer),
            )
        if self._recovery_closed:
            raise ChannelUnknownOutcomeError(
                "channel admission stopped after an unknown writer outcome",
                accepted_remaining=len(self._buffer),
            )
        if len(self._buffer) >= self._capacity:
            return False
        self._buffer.append(message)
        self._accepted_count += 1
        if len(self._buffer) > self._max_observed_queued:
            self._max_observed_queued = len(self._buffer)
        self._condition.notify_all()
        return True

    def _try_recv_locked(self) -> object | None:
        if self._buffer:
            item = self._buffer.popleft()
            self._condition.notify_all()
            return item
        if self._closed or self._recovery_closed:
            raise ChannelClosedError(
                "channel is closed and fully drained",
                accepted_remaining=0,
            )
        return None


class ChannelSet:
    """The four bounded channels for one run boundary with a fixed close order.

    ``close_all`` closes the writer channel first so the downstream
    writer stops before every producer, then results, then assignments,
    and telemetry last because it is advisory and may still emit during
    shutdown. The order cannot deadlock: each close only wakes waiters
    and never blocks, and draining before or after close never loses an
    accepted message.
    """

    __slots__ = ("_close_order", "_report_order")

    def __init__(
        self,
        *,
        assignment_capacity: int,
        result_capacity: int,
        telemetry_capacity: int,
        writer_capacity: int,
    ) -> None:
        assignment = BoundedChannel(kind=CHANNEL_KIND_ASSIGNMENT, capacity=assignment_capacity)
        result = BoundedChannel(kind=CHANNEL_KIND_RESULT, capacity=result_capacity)
        telemetry = BoundedChannel(kind=CHANNEL_KIND_TELEMETRY, capacity=telemetry_capacity)
        writer = BoundedChannel(kind=CHANNEL_KIND_WRITER, capacity=writer_capacity)
        self._report_order: tuple[BoundedChannel, ...] = (assignment, result, telemetry, writer)
        self._close_order: tuple[BoundedChannel, ...] = (writer, result, assignment, telemetry)

    def __repr__(self) -> str:
        parts = [f"{channel.kind}={channel.queued}" for channel in self._report_order]
        return "ChannelSet(" + ", ".join(parts) + ")"

    @property
    def assignment(self) -> BoundedChannel:
        """Return the bounded assignment channel."""
        return self._report_order[0]

    @property
    def result(self) -> BoundedChannel:
        """Return the bounded result channel."""
        return self._report_order[1]

    @property
    def telemetry(self) -> BoundedChannel:
        """Return the bounded telemetry channel."""
        return self._report_order[2]

    @property
    def writer(self) -> BoundedChannel:
        """Return the bounded writer-facing channel."""
        return self._report_order[3]

    def close_all(self) -> None:
        """Close every channel idempotently in the deadlock-free order."""
        for channel in self._close_order:
            channel.close()

    def drain_results(self) -> tuple[object, ...]:
        """Return every accepted-but-undelivered result message in FIFO order."""
        return self._report_order[1].drain()

    def drain_writer(self) -> tuple[object, ...]:
        """Return every accepted-but-undelivered writer message in FIFO order."""
        return self._report_order[3].drain()

    def mark_writer_unknown_outcome(self, reason: str) -> None:
        """Stop writer-channel producer admission after an unknown outcome."""
        self._report_order[3].mark_unknown_outcome(reason)

    def snapshots(self) -> tuple[tuple[str, int, int, int], ...]:
        """Return (kind, capacity, queued, max observed) per channel in order.

        The order is assignment, result, telemetry, writer and every count
        is a bounded observability fact, never authoritative progress.
        """
        return tuple(
            (channel.kind, channel.capacity, channel.queued, channel.max_observed_queued)
            for channel in self._report_order
        )


__all__ = [
    "CHANNEL_KIND_ASSIGNMENT",
    "CHANNEL_KIND_RESULT",
    "CHANNEL_KIND_TELEMETRY",
    "CHANNEL_KIND_WRITER",
    "MAX_CHANNEL_CAPACITY",
    "MIN_CHANNEL_CAPACITY",
    "BoundedChannel",
    "ChannelClosedError",
    "ChannelError",
    "ChannelLoopError",
    "ChannelSet",
    "ChannelTimeoutError",
    "ChannelUnknownOutcomeError",
    "ChannelValidationError",
]
