"""SQLAlchemy repository for contiguous durable execution events."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import NoReturn

from sqlalchemy import func, insert, select, update
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence.repositories.consistency_common import (
    encode_redacted_document,
    event_kind,
    portable_identity,
    positive_int,
    require_event_sequence,
    require_event_subject_kind,
    require_incrementable,
    require_redacted_document,
    require_run_id,
    require_timestamp,
    require_work_item_id,
    stored_run_id,
    stored_timestamp,
    translate_consistency_storage_errors,
    validate_events,
)
from paritygrid.adapters.persistence.repositories.consistency_mapping import (
    EventCounterState,
    event_counter_from_row,
    execution_event_from_row,
)
from paritygrid.adapters.persistence.schema import (
    execution_events,
    run_event_counters,
    runs,
    work_items,
)
from paritygrid.application.ports.consistency import (
    ConsistencyCorruptionError,
    ConsistencyInvalidRequestError,
    ConsistencyRecordNotFoundError,
    ConsistencyStaleRowVersionError,
    EventSequence,
    EventSequenceConflictError,
    EventSubjectKind,
    ExecutionEventBatch,
    ExecutionEventPage,
    ExecutionEventRecord,
    ExecutionEventRepository,
    PendingExecutionEvent,
    validate_consistency_page_limit,
)
from paritygrid.domain.models import RunId, UtcTimestamp, WorkItemId


@dataclass(frozen=True, slots=True)
class _EventState:
    run_id: RunId
    run_created_at: UtcTimestamp
    counter: EventCounterState


@dataclass(frozen=True, slots=True)
class _EncodedEvent:
    event_kind: str
    occurred_at: UtcTimestamp
    subject_kind: EventSubjectKind
    subject_id: RunId | WorkItemId
    correlation_id: str | None
    payload_schema_version: int
    payload_json: str

    def matches(self, record: ExecutionEventRecord) -> bool:
        return (
            record.event_kind == self.event_kind
            and record.occurred_at == self.occurred_at
            and record.subject_kind is self.subject_kind
            and record.subject_id == self.subject_id
            and record.correlation_id == self.correlation_id
            and record.payload_schema_version == self.payload_schema_version
            and encode_redacted_document(record.payload, "stored event payload").text
            == self.payload_json
        )


class SqlAlchemyExecutionEventRepository(ExecutionEventRepository):
    """Allocate and append event ranges in a caller-owned transaction."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @translate_consistency_storage_errors
    def append(
        self,
        run_id: RunId,
        *,
        expected_next_sequence: EventSequence,
        expected_counter_row_version: int,
        events: Sequence[PendingExecutionEvent],
    ) -> ExecutionEventBatch:
        self._require_transaction()
        identity = require_run_id(run_id)
        expected_next = require_event_sequence(expected_next_sequence)
        expected_row = positive_int(expected_counter_row_version, "expected counter row version")
        require_incrementable(expected_row, "event counter row version")
        pending = validate_events(events)
        new_next = expected_next.advance(len(pending))
        state = _load_event_state(self._session, identity)
        if state is None:
            raise ConsistencyRecordNotFoundError("event run does not exist")
        encoded = tuple(_encode_event(item, state) for item in pending)
        _validate_subjects(self._session, identity, tuple(item.subject_id for item in encoded))
        if state.counter.next_sequence == new_next:
            return self._replay_append(
                state,
                expected_next=expected_next,
                expected_row=expected_row,
                encoded=encoded,
            )
        if state.counter.row_version != expected_row:
            raise ConsistencyStaleRowVersionError("event counter row version is stale")
        if state.counter.next_sequence != expected_next:
            raise EventSequenceConflictError("event sequence frontier has changed")
        counter_row = (
            self._session.execute(
                update(run_event_counters)
                .where(
                    run_event_counters.c.run_id == str(identity),
                    run_event_counters.c.next_sequence_number == int(expected_next),
                    run_event_counters.c.row_version == expected_row,
                )
                .values(
                    next_sequence_number=int(new_next),
                    row_version=expected_row + 1,
                )
                .returning(*run_event_counters.c)
            )
            .mappings()
            .one_or_none()
        )
        if counter_row is None:
            self._raise_counter_cas_failure(identity, expected_next, expected_row)
        rows = (
            self._session.execute(
                insert(execution_events).returning(*execution_events.c),
                [
                    {
                        "run_id": str(identity),
                        "sequence_number": int(expected_next) + offset,
                        "event_kind": item.event_kind,
                        "occurred_at": str(item.occurred_at),
                        "subject_kind": item.subject_kind.value,
                        "subject_id": str(item.subject_id),
                        "correlation_id": item.correlation_id,
                        "payload_schema_version": item.payload_schema_version,
                        "payload_json": item.payload_json,
                    }
                    for offset, item in enumerate(encoded)
                ],
            )
            .mappings()
            .all()
        )
        records = tuple(execution_event_from_row(row) for row in rows)
        if len(records) != len(encoded):
            raise ConsistencyCorruptionError("event append result is incomplete")
        for offset, (requested, record) in enumerate(zip(encoded, records, strict=True)):
            if (
                record.run_id != identity
                or record.sequence != EventSequence(int(expected_next) + offset)
                or not requested.matches(record)
            ):
                raise ConsistencyCorruptionError("event append result is corrupt")
        counter = event_counter_from_row(counter_row)
        if (
            counter.run_id != identity
            or counter.next_sequence != new_next
            or counter.row_version != expected_row + 1
        ):
            raise ConsistencyCorruptionError("event counter append result is corrupt")
        return ExecutionEventBatch(records, new_next, counter.row_version)

    @translate_consistency_storage_errors
    def get(self, run_id: RunId, sequence: EventSequence) -> ExecutionEventRecord | None:
        self._require_transaction()
        identity = require_run_id(run_id)
        requested = require_event_sequence(sequence)
        state = _load_event_state(self._session, identity)
        if state is None:
            raise ConsistencyRecordNotFoundError("event run does not exist")
        row = (
            self._session.execute(
                select(execution_events).where(
                    execution_events.c.run_id == str(identity),
                    execution_events.c.sequence_number == int(requested),
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        record = execution_event_from_row(row)
        _validate_event_records(self._session, state, (record,))
        return record

    @translate_consistency_storage_errors
    def list_after(
        self,
        run_id: RunId,
        *,
        after: EventSequence | None,
        limit: int,
    ) -> ExecutionEventPage:
        self._require_transaction()
        identity = require_run_id(run_id)
        cursor = None if after is None else require_event_sequence(after)
        page_limit = validate_consistency_page_limit(limit)
        state = _load_event_state(self._session, identity)
        if state is None:
            raise ConsistencyRecordNotFoundError("event run does not exist")
        statement = select(execution_events).where(execution_events.c.run_id == str(identity))
        if cursor is not None:
            statement = statement.where(execution_events.c.sequence_number > int(cursor))
        rows = (
            self._session.execute(
                statement.order_by(execution_events.c.sequence_number).limit(page_limit + 1)
            )
            .mappings()
            .all()
        )
        records = tuple(execution_event_from_row(row) for row in rows[:page_limit])
        _validate_event_records(self._session, state, records)
        next_cursor = records[-1].sequence if len(rows) > page_limit else None
        return ExecutionEventPage(records, next_cursor)

    def _replay_append(
        self,
        state: _EventState,
        *,
        expected_next: EventSequence,
        expected_row: int,
        encoded: tuple[_EncodedEvent, ...],
    ) -> ExecutionEventBatch:
        if state.counter.row_version != expected_row + 1:
            raise ConsistencyStaleRowVersionError("event append frontier is stale")
        rows = (
            self._session.execute(
                select(execution_events)
                .where(
                    execution_events.c.run_id == str(state.run_id),
                    execution_events.c.sequence_number >= int(expected_next),
                    execution_events.c.sequence_number < int(state.counter.next_sequence),
                )
                .order_by(execution_events.c.sequence_number)
            )
            .mappings()
            .all()
        )
        records = tuple(execution_event_from_row(row) for row in rows)
        if len(records) != len(encoded):
            raise ConsistencyCorruptionError("event replay range is incomplete")
        _validate_event_records(self._session, state, records)
        if any(
            record.sequence != EventSequence(int(expected_next) + offset)
            or not requested.matches(record)
            for offset, (requested, record) in enumerate(zip(encoded, records, strict=True))
        ):
            raise EventSequenceConflictError("event append conflicts with durable history")
        return ExecutionEventBatch(records, state.counter.next_sequence, state.counter.row_version)

    def _raise_counter_cas_failure(
        self, run_id: RunId, expected_next: EventSequence, expected_row: int
    ) -> NoReturn:
        state = _load_event_state(self._session, run_id)
        if state is None:
            raise ConsistencyRecordNotFoundError("event run does not exist")
        if state.counter.row_version != expected_row:
            raise ConsistencyStaleRowVersionError("event counter row version is stale")
        if state.counter.next_sequence != expected_next:
            raise EventSequenceConflictError("event sequence frontier has changed")
        raise EventSequenceConflictError("event counter update was rejected")

    def _require_transaction(self) -> None:
        if not self._session.in_transaction():
            raise ConsistencyInvalidRequestError("repository requires a caller-owned transaction")


def _encode_event(event: PendingExecutionEvent, state: _EventState) -> _EncodedEvent:
    kind = event_kind(event.event_kind)
    occurred = require_timestamp(event.occurred_at, "event occurrence time")
    subject_kind = require_event_subject_kind(event.subject_kind)
    if subject_kind is EventSubjectKind.RUN:
        subject_id: RunId | WorkItemId = require_run_id(event.subject_id)
        if subject_id != state.run_id:
            raise ConsistencyInvalidRequestError("event subject does not belong to its run")
    else:
        subject_id = require_work_item_id(event.subject_id)
    correlation = (
        None
        if event.correlation_id is None
        else portable_identity(event.correlation_id, "correlation identifier", 96)
    )
    schema_version = positive_int(event.payload_schema_version, "event payload schema version")
    payload = encode_redacted_document(
        require_redacted_document(event.payload, "event payload"), "event payload"
    )
    if occurred < state.run_created_at:
        raise ConsistencyInvalidRequestError("event occurrence cannot precede its run")
    return _EncodedEvent(
        kind,
        occurred,
        subject_kind,
        subject_id,
        correlation,
        schema_version,
        payload.text,
    )


def _load_event_state(session: Session, run_id: RunId) -> _EventState | None:
    run_row = (
        session.execute(select(runs).where(runs.c.run_id == str(run_id))).mappings().one_or_none()
    )
    counter_row = (
        session.execute(
            select(run_event_counters).where(run_event_counters.c.run_id == str(run_id))
        )
        .mappings()
        .one_or_none()
    )
    if run_row is None and counter_row is None:
        return None
    if run_row is None or counter_row is None:
        raise ConsistencyCorruptionError("event run and counter relationship is incomplete")
    stored_identity = stored_run_id(run_row["run_id"])
    counter = event_counter_from_row(counter_row)
    if stored_identity != run_id or counter.run_id != run_id:
        raise ConsistencyCorruptionError("event run identity is corrupt")
    if counter.row_version > int(counter.next_sequence):
        raise ConsistencyCorruptionError("event counter row version is corrupt")
    run_created = stored_timestamp(run_row["created_at"], "run creation time")
    _validate_event_frontier(session, run_id, counter.next_sequence)
    return _EventState(run_id, run_created, counter)


def _validate_event_frontier(session: Session, run_id: RunId, next_sequence: EventSequence) -> None:
    count, minimum, maximum = session.execute(
        select(
            func.count(execution_events.c.sequence_number),
            func.min(execution_events.c.sequence_number),
            func.max(execution_events.c.sequence_number),
        ).where(execution_events.c.run_id == str(run_id))
    ).one()
    expected_count = int(next_sequence) - 1
    if expected_count == 0:
        if count != 0 or minimum is not None or maximum is not None:
            raise ConsistencyCorruptionError("event history exceeds its counter")
        return
    if count != expected_count or minimum != 1 or maximum != expected_count:
        raise ConsistencyCorruptionError("event history is not contiguous")


def _validate_subjects(
    session: Session, run_id: RunId, subject_ids: tuple[RunId | WorkItemId, ...]
) -> None:
    work_ids = tuple(
        sorted({str(subject) for subject in subject_ids if type(subject) is WorkItemId})
    )
    if not work_ids:
        return
    rows = session.execute(
        select(work_items.c.work_item_id, work_items.c.run_id).where(
            work_items.c.work_item_id.in_(work_ids)
        )
    ).all()
    parents = {str(row[0]): stored_run_id(row[1]) for row in rows}
    if set(parents) != set(work_ids) or any(parent != run_id for parent in parents.values()):
        raise ConsistencyInvalidRequestError("event subject does not belong to its run")


def _validate_event_records(
    session: Session, state: _EventState, records: tuple[ExecutionEventRecord, ...]
) -> None:
    for record in records:
        if record.run_id != state.run_id:
            raise ConsistencyCorruptionError("event identity is corrupt")
        if record.occurred_at < state.run_created_at:
            raise ConsistencyCorruptionError("event chronology is corrupt")
        if record.subject_kind is EventSubjectKind.RUN and record.subject_id != state.run_id:
            raise ConsistencyCorruptionError("event run subject is corrupt")
    work_ids = tuple(
        record.subject_id for record in records if record.subject_kind is EventSubjectKind.WORK_ITEM
    )
    try:
        _validate_subjects(session, state.run_id, work_ids)
    except ConsistencyInvalidRequestError as error:
        raise ConsistencyCorruptionError("event work subject is corrupt") from error
