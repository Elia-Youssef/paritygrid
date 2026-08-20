"""Closed non-authoritative attempt events shared by execution runners."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from itertools import islice
from typing import Protocol, cast, runtime_checkable

from paritygrid.application.planner import PlannerRunnerKind
from paritygrid.domain.execution import FailureClassification
from paritygrid.domain.models import (
    AttemptNumber,
    Duration,
    NodeId,
    RunId,
    UtcTimestamp,
    WorkItemId,
)

ATTEMPT_EVENT_SCHEMA_VERSION = 1
MAX_ATTEMPT_EVENT_CORRELATION_ID_LENGTH = 96
MAX_ATTEMPT_EVENT_DETAIL_LENGTH = 4_096
MAX_ATTEMPT_EVENT_TRACE_LENGTH = 2
MAX_ATTEMPT_EVENT_WORKER_IDENTITY_LENGTH = 128

_CORRELATION_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]*", flags=re.ASCII)


class AttemptEventError(ValueError):
    """Base failure for normalized attempt-event values and traces."""


class AttemptEventInvalidRequestError(AttemptEventError):
    """An attempt event violates the normalized value contract."""


class AttemptEventSequenceError(AttemptEventError):
    """An attempt-event trace violates the closed lifecycle order."""


class AttemptEventUnsupportedVersionError(AttemptEventError):
    """An attempt event uses a schema version this runtime cannot interpret."""


class AttemptEventKind(StrEnum):
    """Closed completed-fact names observed by every execution runner."""

    STARTED = "attempt_started"
    SUCCEEDED = "attempt_succeeded"
    FAILED = "attempt_failed"
    CANCELLED = "attempt_cancelled"


@dataclass(frozen=True, slots=True, repr=False)
class RedactedAttemptDetail:
    """Bounded diagnostic text already redacted before event construction."""

    text: str

    def __post_init__(self) -> None:
        _validate_text(
            self.text,
            maximum=MAX_ATTEMPT_EVENT_DETAIL_LENGTH,
            subject="attempt-event detail",
        )

    def __repr__(self) -> str:
        return "RedactedAttemptDetail(content=<redacted>)"


@dataclass(frozen=True, slots=True, repr=False)
class AttemptEventContext:
    """Exact execution identity shared by the events of one attempt."""

    run_id: RunId
    node_id: NodeId
    work_item_id: WorkItemId
    attempt_number: AttemptNumber
    started_at: UtcTimestamp
    runner_kind: PlannerRunnerKind
    worker_identity: str
    correlation_id: str | None = None
    schema_version: int = ATTEMPT_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_exact(self.run_id, RunId, "attempt-event run identity")
        _require_exact(self.node_id, NodeId, "attempt-event node identity")
        _require_exact(self.work_item_id, WorkItemId, "attempt-event work identity")
        _require_exact(self.attempt_number, AttemptNumber, "attempt-event attempt number")
        _require_exact(self.started_at, UtcTimestamp, "attempt-event start time")
        _require_exact(self.runner_kind, PlannerRunnerKind, "attempt-event runner kind")
        _validate_text(
            self.worker_identity,
            maximum=MAX_ATTEMPT_EVENT_WORKER_IDENTITY_LENGTH,
            subject="attempt-event worker identity",
        )
        _validate_correlation_id(self.correlation_id)
        if type(self.schema_version) is not int:
            raise TypeError("attempt-event schema version must be an integer")
        if self.schema_version != ATTEMPT_EVENT_SCHEMA_VERSION:
            raise AttemptEventUnsupportedVersionError(
                "attempt-event schema version is not supported"
            )

    def __repr__(self) -> str:
        return (
            "AttemptEventContext("
            f"run_id={self.run_id!r}, node_id={self.node_id!r}, "
            f"work_item_id={self.work_item_id!r}, "
            f"attempt_number={self.attempt_number!r}, started_at={self.started_at!r}, "
            f"runner_kind={self.runner_kind.value!r}, schema_version={self.schema_version!r}, "
            "worker_identity=<redacted>, correlation_id=<redacted>)"
        )


@dataclass(frozen=True, slots=True)
class AttemptStarted:
    """One exact attempt began executing under its immutable context."""

    context: AttemptEventContext

    def __post_init__(self) -> None:
        _require_exact(self.context, AttemptEventContext, "attempt-start context")

    @property
    def kind(self) -> AttemptEventKind:
        return AttemptEventKind.STARTED

    @property
    def occurred_at(self) -> UtcTimestamp:
        return self.context.started_at


@dataclass(frozen=True, slots=True)
class AttemptSucceeded:
    """One exact attempt finished successfully without carrying result authority."""

    context: AttemptEventContext
    finished_at: UtcTimestamp
    duration: Duration = field(init=False)

    def __post_init__(self) -> None:
        _require_exact(self.context, AttemptEventContext, "attempt-success context")
        object.__setattr__(self, "duration", _terminal_duration(self.context, self.finished_at))

    @property
    def kind(self) -> AttemptEventKind:
        return AttemptEventKind.SUCCEEDED

    @property
    def occurred_at(self) -> UtcTimestamp:
        return self.finished_at


@dataclass(frozen=True, slots=True, repr=False)
class AttemptFailed:
    """One exact attempt finished with a classified non-cancellation failure."""

    context: AttemptEventContext
    finished_at: UtcTimestamp
    failure_classification: FailureClassification
    detail: RedactedAttemptDetail | None = None
    duration: Duration = field(init=False)

    def __post_init__(self) -> None:
        _require_exact(self.context, AttemptEventContext, "attempt-failure context")
        _require_exact(
            self.failure_classification,
            FailureClassification,
            "attempt failure classification",
        )
        if self.failure_classification is FailureClassification.USER_CANCELLATION:
            raise AttemptEventInvalidRequestError("user cancellation must use AttemptCancelled")
        if self.detail is not None:
            _require_exact(self.detail, RedactedAttemptDetail, "attempt failure detail")
        object.__setattr__(self, "duration", _terminal_duration(self.context, self.finished_at))

    @property
    def kind(self) -> AttemptEventKind:
        return AttemptEventKind.FAILED

    @property
    def occurred_at(self) -> UtcTimestamp:
        return self.finished_at

    def __repr__(self) -> str:
        return (
            "AttemptFailed("
            f"context={self.context!r}, finished_at={self.finished_at!r}, "
            f"failure_classification={self.failure_classification!r}, "
            f"duration={self.duration!r}, detail=<redacted>)"
        )


@dataclass(frozen=True, slots=True, repr=False)
class AttemptCancelled:
    """One exact attempt stopped after cooperative user cancellation."""

    context: AttemptEventContext
    finished_at: UtcTimestamp
    detail: RedactedAttemptDetail | None = None
    duration: Duration = field(init=False)

    def __post_init__(self) -> None:
        _require_exact(self.context, AttemptEventContext, "attempt-cancellation context")
        if self.detail is not None:
            _require_exact(self.detail, RedactedAttemptDetail, "attempt cancellation detail")
        object.__setattr__(self, "duration", _terminal_duration(self.context, self.finished_at))

    @property
    def kind(self) -> AttemptEventKind:
        return AttemptEventKind.CANCELLED

    @property
    def occurred_at(self) -> UtcTimestamp:
        return self.finished_at

    @property
    def failure_classification(self) -> FailureClassification:
        return FailureClassification.USER_CANCELLATION

    def __repr__(self) -> str:
        return (
            "AttemptCancelled("
            f"context={self.context!r}, finished_at={self.finished_at!r}, "
            f"duration={self.duration!r}, detail=<redacted>)"
        )


type TerminalAttemptEvent = AttemptSucceeded | AttemptFailed | AttemptCancelled
type NormalizedAttemptEvent = AttemptStarted | TerminalAttemptEvent


@runtime_checkable
class AttemptEventObserver(Protocol):
    """Borrowed non-authoritative observer used by runner-owned execution code."""

    def emit(self, event: NormalizedAttemptEvent, /) -> None:
        """Observe one normalized fact without committing durable state."""
        ...


@dataclass(frozen=True, slots=True)
class AttemptEventTrace:
    """One active or complete attempt trace in exact lifecycle order."""

    items: tuple[NormalizedAttemptEvent, ...]

    def __post_init__(self) -> None:
        values = cast(object, self.items)
        if type(values) is not tuple:
            raise TypeError("attempt-event trace items must be a tuple")
        events = cast(tuple[object, ...], values)
        if not 1 <= len(events) <= MAX_ATTEMPT_EVENT_TRACE_LENGTH:
            raise AttemptEventSequenceError(
                "attempt-event trace must contain a start and at most one terminal event"
            )
        if type(events[0]) is not AttemptStarted:
            raise AttemptEventSequenceError("attempt-event trace must begin with AttemptStarted")
        started = events[0]
        if len(events) == 2:
            terminal = events[1]
            if type(terminal) not in _TERMINAL_EVENT_TYPES:
                raise AttemptEventSequenceError(
                    "attempt-event trace must end with one closed terminal variant"
                )
            typed_terminal = cast(TerminalAttemptEvent, terminal)
            if typed_terminal.context != started.context:
                raise AttemptEventSequenceError(
                    "attempt-event trace variants must share one exact context"
                )

    @property
    def context(self) -> AttemptEventContext:
        return self.items[0].context

    @property
    def is_complete(self) -> bool:
        return len(self.items) == MAX_ATTEMPT_EVENT_TRACE_LENGTH

    @property
    def terminal(self) -> TerminalAttemptEvent | None:
        if not self.is_complete:
            return None
        return cast(TerminalAttemptEvent, self.items[1])

    def require_complete(self) -> TerminalAttemptEvent:
        """Return the terminal fact or reject an active-only trace."""
        terminal = self.terminal
        if terminal is None:
            raise AttemptEventSequenceError("attempt-event trace has no terminal event")
        return terminal


_TERMINAL_EVENT_TYPES = (AttemptSucceeded, AttemptFailed, AttemptCancelled)


def validate_attempt_event_trace(events: Sequence[NormalizedAttemptEvent]) -> AttemptEventTrace:
    """Copy a bounded event sequence into an immutable validated trace."""
    if isinstance(events, (str, bytes, bytearray)):
        raise TypeError("attempt-event trace source must be an event sequence")
    bounded = tuple(islice(events, MAX_ATTEMPT_EVENT_TRACE_LENGTH + 1))
    return AttemptEventTrace(bounded)


def emit_attempt_event(
    observer: AttemptEventObserver,
    event: NormalizedAttemptEvent,
) -> None:
    """Emit one exact closed variant through a borrowed observer."""
    observer_value = cast(object, observer)
    if not isinstance(observer_value, AttemptEventObserver):
        raise TypeError("attempt-event observer must implement AttemptEventObserver")
    if type(event) not in _ALL_EVENT_TYPES:
        raise TypeError("attempt event must use a closed NormalizedAttemptEvent variant")
    observer_value.emit(event)


_ALL_EVENT_TYPES = (AttemptStarted, *_TERMINAL_EVENT_TYPES)


def _terminal_duration(context: AttemptEventContext, value: object) -> Duration:
    finished_at = _require_exact(value, UtcTimestamp, "attempt-event finish time")
    if finished_at < context.started_at:
        raise AttemptEventInvalidRequestError("attempt-event finish time cannot precede its start")
    try:
        return Duration.from_timedelta(finished_at.to_datetime() - context.started_at.to_datetime())
    except ValueError:
        raise AttemptEventInvalidRequestError(
            "attempt-event duration is outside the supported range"
        ) from None


def _validate_correlation_id(value: object) -> None:
    if value is None:
        return
    if type(value) is not str:
        raise TypeError("attempt-event correlation identifier must be text or None")
    if (
        not 1 <= len(value) <= MAX_ATTEMPT_EVENT_CORRELATION_ID_LENGTH
        or _CORRELATION_ID_PATTERN.fullmatch(value) is None
    ):
        raise AttemptEventInvalidRequestError(
            "attempt-event correlation identifier must use bounded portable ASCII"
        )


def _validate_text(value: object, *, maximum: int, subject: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{subject} must be text")
    if not 1 <= len(value) <= maximum:
        raise AttemptEventInvalidRequestError(f"{subject} is outside the supported range")
    if unicodedata.normalize("NFC", value) != value:
        raise AttemptEventInvalidRequestError(f"{subject} must use normalized Unicode")


def _require_exact[T](value: object, expected: type[T], subject: str) -> T:
    if type(value) is not expected:
        raise TypeError(f"{subject} must use {expected.__name__}")
    return cast(T, value)


__all__ = [
    "ATTEMPT_EVENT_SCHEMA_VERSION",
    "MAX_ATTEMPT_EVENT_CORRELATION_ID_LENGTH",
    "MAX_ATTEMPT_EVENT_DETAIL_LENGTH",
    "MAX_ATTEMPT_EVENT_TRACE_LENGTH",
    "MAX_ATTEMPT_EVENT_WORKER_IDENTITY_LENGTH",
    "AttemptCancelled",
    "AttemptEventContext",
    "AttemptEventError",
    "AttemptEventInvalidRequestError",
    "AttemptEventKind",
    "AttemptEventObserver",
    "AttemptEventSequenceError",
    "AttemptEventTrace",
    "AttemptEventUnsupportedVersionError",
    "AttemptFailed",
    "AttemptStarted",
    "AttemptSucceeded",
    "NormalizedAttemptEvent",
    "RedactedAttemptDetail",
    "TerminalAttemptEvent",
    "emit_attempt_event",
    "validate_attempt_event_trace",
]
