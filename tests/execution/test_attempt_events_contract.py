"""Exhaustive tests for the normalized runner attempt-event union."""

from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from paritygrid.application.execution import (
    ATTEMPT_EVENT_SCHEMA_VERSION,
    MAX_ATTEMPT_EVENT_CORRELATION_ID_LENGTH,
    MAX_ATTEMPT_EVENT_DETAIL_LENGTH,
    MAX_ATTEMPT_EVENT_TRACE_LENGTH,
    MAX_ATTEMPT_EVENT_WORKER_IDENTITY_LENGTH,
    AttemptCancelled,
    AttemptEventContext,
    AttemptEventInvalidRequestError,
    AttemptEventKind,
    AttemptEventSequenceError,
    AttemptEventTrace,
    AttemptEventUnsupportedVersionError,
    AttemptFailed,
    AttemptStarted,
    AttemptSucceeded,
    RedactedAttemptDetail,
    emit_attempt_event,
    validate_attempt_event_trace,
)
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

RUN_ID = RunId("run_attempt-events")
NODE_ID = NodeId("nod_attempt-events")
WORK_ID = WorkItemId("wrk_attempt-events")
CORRELATION_ID = "request:attempt-01"
SECRET_WORKER = "machine-secret-worker"
SECRET_DETAIL = "redacted diagnostic canary"


def _timestamp(day: int = 1, second: int = 0) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, day, 12, 0, second, tzinfo=UTC))


def _context(
    *,
    work_item_id: WorkItemId = WORK_ID,
    started_at: UtcTimestamp | None = None,
) -> AttemptEventContext:
    return AttemptEventContext(
        run_id=RUN_ID,
        node_id=NODE_ID,
        work_item_id=work_item_id,
        attempt_number=AttemptNumber(2),
        started_at=started_at or _timestamp(second=1),
        runner_kind=PlannerRunnerKind.SEQUENTIAL,
        worker_identity=SECRET_WORKER,
        correlation_id=CORRELATION_ID,
    )


def _terminal_events(
    context: AttemptEventContext | None = None,
) -> tuple[AttemptSucceeded | AttemptFailed | AttemptCancelled, ...]:
    shared = context or _context()
    finished = _timestamp(second=6)
    detail = RedactedAttemptDetail(SECRET_DETAIL)
    return (
        AttemptSucceeded(shared, finished),
        AttemptFailed(shared, finished, FailureClassification.TIMEOUT, detail),
        AttemptCancelled(shared, finished, detail),
    )


def test_closed_event_union_has_one_start_and_three_terminal_facts() -> None:
    context = _context()
    started = AttemptStarted(context)
    succeeded, failed, cancelled = _terminal_events(context)

    assert tuple(AttemptEventKind) == (
        AttemptEventKind.STARTED,
        AttemptEventKind.SUCCEEDED,
        AttemptEventKind.FAILED,
        AttemptEventKind.CANCELLED,
    )
    assert started.kind is AttemptEventKind.STARTED
    assert started.occurred_at == context.started_at
    assert succeeded.kind is AttemptEventKind.SUCCEEDED
    assert failed.kind is AttemptEventKind.FAILED
    assert cancelled.kind is AttemptEventKind.CANCELLED
    assert all(
        event.occurred_at == _timestamp(second=6) for event in (succeeded, failed, cancelled)
    )
    assert all(event.duration == Duration(5_000_000) for event in (succeeded, failed, cancelled))
    assert cast(AttemptCancelled, cancelled).failure_classification is (
        FailureClassification.USER_CANCELLATION
    )
    assert MAX_ATTEMPT_EVENT_TRACE_LENGTH == 2


@pytest.mark.parametrize(
    "classification",
    tuple(
        classification
        for classification in FailureClassification
        if classification is not FailureClassification.USER_CANCELLATION
    ),
)
def test_failed_variant_accepts_every_non_cancellation_classification(
    classification: FailureClassification,
) -> None:
    event = AttemptFailed(_context(), _timestamp(second=2), classification)
    assert event.failure_classification is classification
    assert event.duration == Duration(1_000_000)


def test_user_cancellation_has_exactly_one_variant() -> None:
    with pytest.raises(AttemptEventInvalidRequestError, match="AttemptCancelled"):
        AttemptFailed(
            _context(),
            _timestamp(second=2),
            FailureClassification.USER_CANCELLATION,
        )
    event = AttemptCancelled(_context(), _timestamp(second=2))
    assert event.failure_classification is FailureClassification.USER_CANCELLATION


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("run_id", "run_attempt-events", "run identity"),
        ("node_id", "nod_attempt-events", "node identity"),
        ("work_item_id", "wrk_attempt-events", "work identity"),
        ("attempt_number", 2, "attempt number"),
        ("started_at", "now", "start time"),
        ("runner_kind", "sequential", "runner kind"),
        ("worker_identity", 1, "worker identity"),
        ("correlation_id", 1, "correlation"),
        ("schema_version", True, "schema version"),
    ],
)
def test_context_requires_exact_types(field: str, value: object, message: str) -> None:
    values: dict[str, object] = {
        "run_id": RUN_ID,
        "node_id": NODE_ID,
        "work_item_id": WORK_ID,
        "attempt_number": AttemptNumber(2),
        "started_at": _timestamp(second=1),
        "runner_kind": PlannerRunnerKind.SEQUENTIAL,
        "worker_identity": SECRET_WORKER,
        "correlation_id": CORRELATION_ID,
        "schema_version": ATTEMPT_EVENT_SCHEMA_VERSION,
    }
    values[field] = value
    with pytest.raises(TypeError, match=message):
        AttemptEventContext(**cast(Any, values))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("worker_identity", "", "outside"),
        (
            "worker_identity",
            "w" * (MAX_ATTEMPT_EVENT_WORKER_IDENTITY_LENGTH + 1),
            "outside",
        ),
        ("worker_identity", "e\u0301", "normalized"),
        ("correlation_id", "", "portable ASCII"),
        (
            "correlation_id",
            "c" * (MAX_ATTEMPT_EVENT_CORRELATION_ID_LENGTH + 1),
            "portable ASCII",
        ),
        ("correlation_id", "not portable", "portable ASCII"),
        ("correlation_id", "café", "portable ASCII"),
    ],
)
def test_context_rejects_noncanonical_bounded_text(
    field: str,
    value: str,
    message: str,
) -> None:
    values: dict[str, object] = {
        "run_id": RUN_ID,
        "node_id": NODE_ID,
        "work_item_id": WORK_ID,
        "attempt_number": AttemptNumber(2),
        "started_at": _timestamp(second=1),
        "runner_kind": PlannerRunnerKind.SEQUENTIAL,
        "worker_identity": SECRET_WORKER,
        "correlation_id": CORRELATION_ID,
    }
    values[field] = value
    with pytest.raises(AttemptEventInvalidRequestError, match=message):
        AttemptEventContext(**cast(Any, values))


def test_context_rejects_future_schema_with_typed_fallback() -> None:
    with pytest.raises(AttemptEventUnsupportedVersionError, match="not supported"):
        replace(_context(), schema_version=ATTEMPT_EVENT_SCHEMA_VERSION + 1)


def test_context_accepts_an_absent_correlation_identifier() -> None:
    assert replace(_context(), correlation_id=None).correlation_id is None


@pytest.mark.parametrize(
    ("value", "error", "message"),
    [
        (1, TypeError, "must be text"),
        ("", AttemptEventInvalidRequestError, "outside"),
        (
            "d" * (MAX_ATTEMPT_EVENT_DETAIL_LENGTH + 1),
            AttemptEventInvalidRequestError,
            "outside",
        ),
        ("e\u0301", AttemptEventInvalidRequestError, "normalized"),
    ],
)
def test_redacted_detail_is_exact_normalized_and_bounded(
    value: object,
    error: type[Exception],
    message: str,
) -> None:
    with pytest.raises(error, match=message):
        RedactedAttemptDetail(cast(Any, value))


def test_variant_fields_require_exact_types() -> None:
    context = _context()
    with pytest.raises(TypeError, match="context"):
        AttemptStarted(cast(Any, object()))
    with pytest.raises(TypeError, match="context"):
        AttemptSucceeded(cast(Any, object()), _timestamp(second=2))
    with pytest.raises(TypeError, match="finish time"):
        AttemptSucceeded(context, cast(Any, object()))
    with pytest.raises(TypeError, match="classification"):
        AttemptFailed(context, _timestamp(second=2), cast(Any, "timeout"))
    with pytest.raises(TypeError, match="failure detail"):
        AttemptFailed(
            context,
            _timestamp(second=2),
            FailureClassification.TIMEOUT,
            cast(Any, "detail"),
        )
    with pytest.raises(TypeError, match="context"):
        AttemptCancelled(cast(Any, object()), _timestamp(second=2))
    with pytest.raises(TypeError, match="cancellation detail"):
        AttemptCancelled(context, _timestamp(second=2), cast(Any, "detail"))


def test_terminal_time_can_equal_start_but_cannot_precede_it() -> None:
    context = _context()
    assert AttemptSucceeded(context, context.started_at).duration == Duration(0)
    for constructor in (
        lambda: AttemptSucceeded(context, _timestamp()),
        lambda: AttemptFailed(context, _timestamp(), FailureClassification.TIMEOUT),
        lambda: AttemptCancelled(context, _timestamp()),
    ):
        with pytest.raises(AttemptEventInvalidRequestError, match="cannot precede"):
            constructor()


def test_terminal_duration_is_bounded_to_one_year() -> None:
    context = _context(started_at=UtcTimestamp(datetime(2026, 1, 1, tzinfo=UTC)))
    boundary = UtcTimestamp(datetime(2027, 1, 1, tzinfo=UTC))
    assert AttemptSucceeded(context, boundary).duration == Duration(Duration.MAX_MICROSECONDS)
    with pytest.raises(AttemptEventInvalidRequestError, match="duration"):
        AttemptSucceeded(context, UtcTimestamp(datetime(2027, 1, 2, tzinfo=UTC)))


@pytest.mark.parametrize("terminal", _terminal_events())
def test_trace_accepts_start_followed_by_each_terminal_variant(
    terminal: AttemptSucceeded | AttemptFailed | AttemptCancelled,
) -> None:
    started = AttemptStarted(terminal.context)
    trace = AttemptEventTrace((started, terminal))
    assert trace.context == terminal.context
    assert trace.is_complete
    assert trace.terminal is terminal
    assert trace.require_complete() is terminal


def test_trace_accepts_active_start_and_can_require_completion() -> None:
    started = AttemptStarted(_context())
    trace = validate_attempt_event_trace([started])
    assert trace.items == (started,)
    assert not trace.is_complete
    assert trace.terminal is None
    with pytest.raises(AttemptEventSequenceError, match="no terminal"):
        trace.require_complete()


@pytest.mark.parametrize(
    "events",
    [
        (),
        (_terminal_events()[0],),
        (AttemptStarted(_context()), AttemptStarted(_context())),
        (AttemptStarted(_context()), cast(Any, object())),
        (
            AttemptStarted(_context()),
            _terminal_events()[0],
            _terminal_events()[1],
        ),
        (
            AttemptStarted(_context()),
            AttemptSucceeded(
                _context(work_item_id=WorkItemId("wrk_attempt-other")),
                _timestamp(second=6),
            ),
        ),
    ],
)
def test_trace_rejects_empty_reordered_duplicate_unknown_or_mismatched_events(
    events: tuple[Any, ...],
) -> None:
    with pytest.raises(AttemptEventSequenceError):
        AttemptEventTrace(cast(Any, events))


def test_trace_requires_tuple_but_sequence_validator_copies_bounded_sequences() -> None:
    started = AttemptStarted(_context())
    with pytest.raises(TypeError, match="must be a tuple"):
        AttemptEventTrace(cast(Any, [started]))
    trace = validate_attempt_event_trace([started])
    assert trace.items == (started,)
    with pytest.raises(TypeError, match="event sequence"):
        validate_attempt_event_trace(cast(Any, "attempt_started"))


class _Observer:
    def __init__(self) -> None:
        self.events: list[object] = []

    def emit(self, event: object, /) -> None:
        self.events.append(event)


class _WrongObserver:
    pass


def test_borrowed_observer_receives_each_exact_closed_variant_without_lifecycle_ownership() -> None:
    observer = _Observer()
    events = (AttemptStarted(_context()), *_terminal_events())
    for event in events:
        emit_attempt_event(observer, event)
    assert observer.events == list(events)
    assert not hasattr(observer, "close")


def test_emit_rejects_invalid_observer_and_unknown_variant() -> None:
    with pytest.raises(TypeError, match="observer"):
        emit_attempt_event(cast(Any, _WrongObserver()), AttemptStarted(_context()))
    with pytest.raises(TypeError, match="closed"):
        emit_attempt_event(_Observer(), cast(Any, object()))


def test_values_are_frozen_and_public_reprs_redact_sensitive_context_and_detail() -> None:
    context = _context()
    detail = RedactedAttemptDetail(SECRET_DETAIL)
    failed = AttemptFailed(
        context,
        _timestamp(second=2),
        FailureClassification.UNKNOWN,
        detail,
    )
    cancelled = AttemptCancelled(context, _timestamp(second=2), detail)
    for value in (context, detail, failed, cancelled):
        rendered = repr(value)
        assert SECRET_WORKER not in rendered
        assert CORRELATION_ID not in rendered
        assert SECRET_DETAIL not in rendered
        assert "<redacted>" in rendered
    with pytest.raises(FrozenInstanceError):
        cast(Any, context).worker_identity = "changed"
