"""Bounded reads of committed durable events for resumable streaming."""

from dataclasses import dataclass
from threading import Event

from paritygrid.application.ports.consistency import (
    EventSequence,
    ExecutionEventPage,
    validate_consistency_page_limit,
)
from paritygrid.application.ports.execution import RunRecord
from paritygrid.application.ports.operations import OperationalUnitOfWork
from paritygrid.application.services.errors import (
    OperationalRecordNotFoundError,
    OperationalRequestError,
    OperationalUnavailableError,
)
from paritygrid.domain.models import RunId

_PAGE_SIZE = 100
MAX_EVENT_SEQUENCE = 2_147_483_647


@dataclass(frozen=True, slots=True)
class DurableEventPageView:
    """One committed event page with the run's coherent row version."""

    run: RunRecord
    page: ExecutionEventPage


class DurableEventHistoryGapError(OperationalUnavailableError):
    """Committed event history cannot prove a contiguous replay frontier.

    Durable events are append-only and never pruned.  A missing acknowledged
    sequence or a discontinuity after a supplied cursor therefore indicates
    unavailable/corrupt durable history, never a condition a stream may skip.
    """


class DurableEventStreamService:
    """Read committed events in sequence order without owning subscriptions.

    Every read is one short transaction, so a stalled subscriber can never
    hold a database owner; durable history remains the recovery and audit
    truth and is never derived from any live channel.
    """

    def __init__(
        self,
        *,
        unit_of_work: OperationalUnitOfWork,
        heartbeat_seconds: float = 15.0,
        poll_seconds: float = 0.25,
    ) -> None:
        if type(heartbeat_seconds) is not float or not 0.1 <= heartbeat_seconds <= 300.0:
            raise ValueError("stream heartbeat interval is outside the supported range")
        if type(poll_seconds) is not float or not 0.05 <= poll_seconds <= 30.0:
            raise ValueError("stream poll interval is outside the supported range")
        self._unit_of_work = unit_of_work
        self._heartbeat_seconds = heartbeat_seconds
        self._poll_seconds = poll_seconds
        self._stopping = Event()

    @property
    def heartbeat_seconds(self) -> float:
        return self._heartbeat_seconds

    @property
    def poll_seconds(self) -> float:
        return self._poll_seconds

    def read_page(self, run_id: str, *, after: int) -> DurableEventPageView:
        """Return one page of committed events strictly after a sequence.

        ``after`` is the last sequence the consumer already holds; ``0``
        replays the full retained history from the first committed event.
        """
        if type(after) is not int or not 0 <= after <= MAX_EVENT_SEQUENCE:
            raise OperationalRequestError(
                "event sequences must be nonnegative 31-bit integers", field="after"
            )
        identity = _run_id(run_id)
        page_limit = validate_consistency_page_limit(_PAGE_SIZE)
        cursor = None if after == 0 else _sequence(after)
        with self._unit_of_work.transaction() as repositories:
            run = repositories.runs.get(identity)
            if run is None:
                raise OperationalRecordNotFoundError("run", run_id)
            counter = repositories.runs.get_event_counter(identity)
            if counter is None:
                raise DurableEventHistoryGapError(
                    "the durable event frontier is unavailable for this run"
                )
            frontier = counter.next_sequence_number - 1
            if after > frontier:
                raise OperationalRequestError(
                    "event sequence is ahead of the durable frontier", field="after"
                )
            if cursor is not None and repositories.events.get(identity, cursor) is None:
                raise DurableEventHistoryGapError(
                    "the acknowledged durable event is unavailable; replay cannot continue safely"
                )
            page = repositories.events.list_after(identity, after=cursor, limit=page_limit)
            _require_contiguous_page(page, after=after, frontier=frontier)
        return DurableEventPageView(run=run, page=page)

    def frontier(self, run_id: str) -> tuple[RunRecord, int]:
        """Return the run record and its last committed sequence number."""
        identity = _run_id(run_id)
        with self._unit_of_work.transaction() as repositories:
            run = repositories.runs.get(identity)
            if run is None:
                raise OperationalRecordNotFoundError("run", run_id)
            counter = repositories.runs.get_event_counter(identity)
        frontier = 0 if counter is None else counter.next_sequence_number - 1
        return run, frontier

    def stop(self) -> None:
        """Signal every active subscriber to terminate promptly."""
        self._stopping.set()

    def is_stopping(self) -> bool:
        """Return whether shutdown has been requested."""
        return self._stopping.is_set()


def _sequence(value: int) -> EventSequence:
    if type(value) is not int or not 1 <= value <= MAX_EVENT_SEQUENCE:
        raise OperationalRequestError(
            "event sequences must be positive 31-bit integers",
            field="after",
        )
    return EventSequence(value)


def _require_contiguous_page(page: ExecutionEventPage, *, after: int, frontier: int) -> None:
    """Reject any unavailable, pruned, or gapped event range before emission."""
    expected = after + 1
    if not page.items:
        if expected <= frontier:
            raise DurableEventHistoryGapError(
                "the durable event range is incomplete; replay cannot continue safely"
            )
        return
    for record in page.items:
        if record.sequence.number != expected:
            raise DurableEventHistoryGapError(
                "the durable event range is not contiguous; replay cannot continue safely"
            )
        expected += 1


def _run_id(run_id: str) -> RunId:
    try:
        return RunId.parse(run_id)
    except ValueError as error:
        raise OperationalRequestError(
            "run identity must use the canonical run format",
            field="run_id",
        ) from error


__all__ = [
    "MAX_EVENT_SEQUENCE",
    "DurableEventHistoryGapError",
    "DurableEventPageView",
    "DurableEventStreamService",
]
