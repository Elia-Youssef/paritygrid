# pyright: reportAttributeAccessIssue=false, reportPrivateUsage=false
# pyright: reportUnknownArgumentType=false, reportUnknownLambdaType=false
"""Adversarial branch tests for consistency repository classifiers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import NoReturn, cast
from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence.repositories import checkpoints as checkpoint_module
from paritygrid.adapters.persistence.repositories import execution_events as event_module
from paritygrid.adapters.persistence.repositories import idempotency as idempotency_module
from paritygrid.adapters.persistence.repositories.checkpoints import (
    SqlAlchemyCheckpointRepository,
)
from paritygrid.adapters.persistence.repositories.consistency_mapping import (
    EventCounterState,
    StoredIdempotencyRecord,
)
from paritygrid.adapters.persistence.repositories.execution_events import (
    SqlAlchemyExecutionEventRepository,
)
from paritygrid.adapters.persistence.repositories.idempotency import (
    SqlAlchemyIdempotencyRepository,
)
from paritygrid.application.ports import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    CheckpointConflictError,
    CheckpointHeadRecord,
    CheckpointRecord,
    CheckpointVersion,
    ConsistencyCorruptionError,
    ConsistencyInvalidRequestError,
    ConsistencyRecordNotFoundError,
    ConsistencyStaleRowVersionError,
    EventSequence,
    EventSequenceConflictError,
    EventSubjectKind,
    ExecutionEventRecord,
    IdempotencyConflictError,
    IdempotencyRecord,
    IdempotencyReservation,
    IdempotencyStatus,
    PendingExecutionEvent,
    RedactedDocument,
    UpdatedWorkCheckpoint,
)
from paritygrid.domain.models import ArtifactId, NodeId, RunId, UtcTimestamp, WorkItemId
from paritygrid.domain.pipeline import PartitionKey

RUN_ID = RunId("run_internal")
NODE_ID = NodeId("nod_internal")
WORK_ID = WorkItemId("wrk_internal")
PARTITION = PartitionKey("internal")
NOW = UtcTimestamp(datetime(2026, 8, 12, 12, 0, tzinfo=UTC))
LATER = UtcTimestamp(datetime(2026, 8, 12, 12, 0, 1, tzinfo=UTC))


def document(**values: object) -> ConfigurationDocument:
    return ConfigurationDocument.from_mapping(values)


def redacted(**values: object) -> RedactedDocument:
    return RedactedDocument.from_mapping(values)


def reservation(digest: str = "a" * 64) -> IdempotencyReservation:
    return IdempotencyReservation("scope", "key", digest, NOW, NOW)


def result(
    *,
    one_or_none: object = None,
    one: object = None,
    all_rows: object = (),
) -> MagicMock:
    value = MagicMock()
    value.mappings.return_value.one_or_none.return_value = one_or_none
    value.mappings.return_value.one.return_value = one
    value.mappings.return_value.all.return_value = all_rows
    value.one.return_value = one
    value.all.return_value = all_rows
    return value


def checkpoint_head(
    *, current: int = 0, row_version: int = 1, updated_at: UtcTimestamp = NOW
) -> CheckpointHeadRecord:
    return CheckpointHeadRecord(
        RUN_ID,
        NODE_ID,
        PARTITION,
        CheckpointVersion(current),
        updated_at,
        row_version,
    )


def work_checkpoint(*, current: int = 0, row_version: int = 1) -> UpdatedWorkCheckpoint:
    return UpdatedWorkCheckpoint(
        WORK_ID,
        RUN_ID,
        NODE_ID,
        PARTITION,
        CheckpointVersion(current),
        row_version,
    )


def checkpoint_state(
    *, current: int = 0, head_row: int = 1, work_row: int = 1
) -> checkpoint_module._CheckpointState:
    return checkpoint_module._CheckpointState(
        checkpoint_head(current=current, row_version=head_row),
        work_checkpoint(current=current, row_version=work_row),
        NOW,
        NOW,
    )


def checkpoint_record(*, version: int = 1) -> CheckpointRecord:
    return CheckpointRecord(
        RUN_ID,
        NODE_ID,
        PARTITION,
        CheckpointVersion(version),
        1,
        None,
        None,
        None,
        LATER,
    )


def pending_event() -> PendingExecutionEvent:
    return PendingExecutionEvent(
        "run_started",
        NOW,
        EventSubjectKind.RUN,
        RUN_ID,
        None,
        1,
        redacted(safe=True),
    )


def event_state(*, next_sequence: int = 1, row_version: int = 1) -> event_module._EventState:
    return event_module._EventState(
        RUN_ID,
        NOW,
        EventCounterState(RUN_ID, EventSequence(next_sequence), row_version),
    )


def encoded_event() -> event_module._EncodedEvent:
    return event_module._EncodedEvent(
        "run_started",
        NOW,
        EventSubjectKind.RUN,
        RUN_ID,
        None,
        1,
        '{"safe":true}',
    )


def event_record(
    *,
    run_id: RunId = RUN_ID,
    occurred_at: UtcTimestamp = NOW,
    subject_id: RunId | WorkItemId = RUN_ID,
    subject_kind: EventSubjectKind = EventSubjectKind.RUN,
) -> ExecutionEventRecord:
    return ExecutionEventRecord(
        run_id,
        EventSequence(1),
        "run_started",
        occurred_at,
        subject_kind,
        subject_id,
        None,
        1,
        redacted(safe=True),
    )


def stored_idempotency(
    *,
    status: IdempotencyStatus = IdempotencyStatus.IN_PROGRESS,
    digest: str = "a" * 64,
    updated_at: UtcTimestamp = NOW,
    response: RedactedDocument | None = None,
) -> StoredIdempotencyRecord:
    completed = updated_at if status is not IdempotencyStatus.IN_PROGRESS else None
    return StoredIdempotencyRecord(
        IdempotencyRecord(
            "scope",
            "key",
            status,
            None if response is None else 1,
            response,
            NOW,
            updated_at,
            completed,
        ),
        digest,
    )


def test_checkpoint_state_integrity_classifiers(monkeypatch: pytest.MonkeyPatch) -> None:
    key = (RUN_ID, NODE_ID, PARTITION)
    session = cast(Session, MagicMock())
    session.execute.side_effect = [result(one_or_none={}), result(one_or_none=None)]
    with pytest.raises(ConsistencyCorruptionError, match="incomplete"):
        checkpoint_module._load_checkpoint_state(session, key)

    for work, head, match in (
        (
            UpdatedWorkCheckpoint(
                WORK_ID,
                RunId("run_other"),
                NODE_ID,
                PARTITION,
                CheckpointVersion(0),
                1,
            ),
            checkpoint_head(),
            "work identity",
        ),
        (
            work_checkpoint(),
            CheckpointHeadRecord(
                RUN_ID,
                NodeId("nod_other"),
                PARTITION,
                CheckpointVersion(0),
                NOW,
                1,
            ),
            "head identity",
        ),
        (work_checkpoint(current=1), checkpoint_head(), "frontiers diverge"),
        (work_checkpoint(), checkpoint_head(updated_at=LATER), "chronology"),
    ):
        work_row = {"created_at": str(NOW), "updated_at": str(NOW)}
        session.execute.side_effect = [
            result(one_or_none=work_row),
            result(one_or_none={}),
        ]
        monkeypatch.setattr(
            checkpoint_module,
            "updated_work_checkpoint_from_row",
            lambda _row, work=work: work,
        )
        monkeypatch.setattr(
            checkpoint_module,
            "checkpoint_head_from_row",
            lambda _row, head=head: head,
        )
        with pytest.raises(ConsistencyCorruptionError, match=match):
            checkpoint_module._load_checkpoint_state(session, key)


@pytest.mark.parametrize("aggregate", [(1, 1, 1), (1, 1, 2)])
def test_checkpoint_history_frontier_rejects_excess_or_gap(aggregate: tuple[int, int, int]) -> None:
    session = cast(Session, MagicMock())
    session.execute.return_value = result(one=aggregate)
    version = CheckpointVersion(0 if aggregate == (1, 1, 1) else 2)
    with pytest.raises(ConsistencyCorruptionError):
        checkpoint_module._validate_history_frontier(session, (RUN_ID, NODE_ID, PARTITION), version)


def test_checkpoint_expected_frontier_classifier_is_exhaustive() -> None:
    with pytest.raises(ConsistencyStaleRowVersionError, match="work-item"):
        checkpoint_module._require_expected_frontier(
            checkpoint_state(work_row=2),
            expected_version=CheckpointVersion(0),
            expected_head_row=1,
            expected_work_row=1,
        )
    with pytest.raises(CheckpointConflictError, match="frontier"):
        checkpoint_module._require_expected_frontier(
            checkpoint_state(current=1),
            expected_version=CheckpointVersion(0),
            expected_head_row=1,
            expected_work_row=1,
        )


def test_checkpoint_result_identity_and_artifact_classifiers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = (RUN_ID, NODE_ID, PARTITION)
    record = checkpoint_record()
    checkpoint_module._validate_checkpoint_identity(record, key)
    with pytest.raises(ConsistencyCorruptionError, match="identity"):
        checkpoint_module._validate_checkpoint_identity(
            CheckpointRecord(
                RunId("run_other"),
                NODE_ID,
                PARTITION,
                CheckpointVersion(1),
                1,
                None,
                None,
                None,
                LATER,
            ),
            key,
        )
    checkpoint_module._validate_commit_result(
        checkpoint_head(current=1, row_version=2),
        work_checkpoint(current=1, row_version=2),
        record,
        key,
        CheckpointVersion(1),
    )
    with pytest.raises(ConsistencyCorruptionError, match="append result"):
        checkpoint_module._validate_commit_result(
            checkpoint_head(),
            work_checkpoint(),
            record,
            key,
            CheckpointVersion(1),
        )

    session = cast(Session, MagicMock())
    session.execute.return_value = result(one_or_none={})
    monkeypatch.setattr(
        checkpoint_module,
        "artifact_key_from_row",
        lambda _row: (
            checkpoint_module.ArtifactId("art_internal"),
            RunId("run_other"),
            NODE_ID,
            PARTITION,
            NOW,
        ),
    )
    with pytest.raises(CheckpointConflictError, match="another"):
        checkpoint_module._require_checkpoint_artifact(
            session,
            key,
            checkpoint_module.ArtifactId("art_internal"),
            LATER,
        )


def test_checkpoint_stored_artifact_batch_corruption_is_exhaustive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = cast(Session, MagicMock())
    artifact_id = ArtifactId("art_internal")
    stored = CheckpointRecord(
        RUN_ID,
        NODE_ID,
        PARTITION,
        CheckpointVersion(1),
        1,
        None,
        None,
        artifact_id,
        LATER,
    )
    session.execute.return_value = result(all_rows=())
    with pytest.raises(ConsistencyCorruptionError, match="missing"):
        checkpoint_module._validate_checkpoint_artifacts(session, (stored,))

    session.execute.return_value = result(all_rows=({},))
    relationships = (
        (artifact_id, RUN_ID, NODE_ID, PARTITION, NOW),
        (artifact_id, RunId("run_other"), NODE_ID, PARTITION, NOW),
        (
            artifact_id,
            RUN_ID,
            NODE_ID,
            PARTITION,
            UtcTimestamp(datetime(2026, 8, 12, 12, 0, 2, tzinfo=UTC)),
        ),
    )
    for relationship, match in zip(
        relationships, (None, "relationship is corrupt", "chronology"), strict=True
    ):
        monkeypatch.setattr(
            checkpoint_module,
            "artifact_key_from_row",
            lambda _row, relationship=relationship: relationship,
        )
        if match is None:
            checkpoint_module._validate_checkpoint_artifacts(session, (checkpoint_record(), stored))
        else:
            with pytest.raises(ConsistencyCorruptionError, match=match):
                checkpoint_module._validate_checkpoint_artifacts(session, (stored,))


def test_checkpoint_head_cas_classifier_is_exhaustive(monkeypatch: pytest.MonkeyPatch) -> None:
    repository = SqlAlchemyCheckpointRepository(cast(Session, MagicMock()))
    key = (RUN_ID, NODE_ID, PARTITION)
    scenarios = (
        (None, ConsistencyRecordNotFoundError),
        (checkpoint_state(head_row=2), ConsistencyStaleRowVersionError),
        (checkpoint_state(current=1), CheckpointConflictError),
        (checkpoint_state(), CheckpointConflictError),
    )
    for state, error in scenarios:
        monkeypatch.setattr(
            checkpoint_module, "_load_checkpoint_state", lambda *_args, state=state: state
        )
        with pytest.raises(error):
            repository._raise_head_cas_failure(key, CheckpointVersion(0), 1)


def test_checkpoint_work_cas_classifier_is_exhaustive(monkeypatch: pytest.MonkeyPatch) -> None:
    session = cast(Session, MagicMock())
    repository = SqlAlchemyCheckpointRepository(session)
    key = (RUN_ID, NODE_ID, PARTITION)
    session.execute.return_value = result(one_or_none=None)
    with pytest.raises(ConsistencyRecordNotFoundError):
        repository._raise_work_cas_failure(key, CheckpointVersion(0), 1)

    scenarios = (
        (work_checkpoint(row_version=2), ConsistencyStaleRowVersionError),
        (work_checkpoint(current=1), CheckpointConflictError),
        (work_checkpoint(), CheckpointConflictError),
    )
    session.execute.return_value = result(one_or_none={})
    for work, error in scenarios:
        monkeypatch.setattr(
            checkpoint_module, "updated_work_checkpoint_from_row", lambda _row, work=work: work
        )
        with pytest.raises(error):
            repository._raise_work_cas_failure(key, CheckpointVersion(0), 1)


def test_checkpoint_append_enters_both_zero_row_cas_classifiers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = cast(Session, MagicMock())
    session.in_transaction.return_value = True
    repository = SqlAlchemyCheckpointRepository(session)
    monkeypatch.setattr(
        checkpoint_module, "_load_checkpoint_state", lambda *_args: checkpoint_state()
    )

    def rejected_head(*_args: object) -> NoReturn:
        raise CheckpointConflictError("head")

    monkeypatch.setattr(repository, "_raise_head_cas_failure", rejected_head)
    session.execute.return_value = result(one_or_none=None)
    with pytest.raises(CheckpointConflictError, match="head"):
        repository.append(
            RUN_ID,
            NODE_ID,
            PARTITION,
            expected_current_version=CheckpointVersion(0),
            expected_head_row_version=1,
            expected_work_row_version=1,
            payload_schema_version=1,
            source_cursor=None,
            output_position=None,
            artifact_id=None,
            committed_at=NOW,
        )

    head_row = {
        "run_id": str(RUN_ID),
        "node_id": str(NODE_ID),
        "partition_key": str(PARTITION),
        "current_version": 1,
        "updated_at": str(NOW),
        "row_version": 2,
    }
    session.execute.side_effect = [result(one_or_none=head_row), result(one_or_none=None)]

    def rejected_work(*_args: object) -> NoReturn:
        raise CheckpointConflictError("work")

    monkeypatch.setattr(repository, "_raise_work_cas_failure", rejected_work)
    with pytest.raises(CheckpointConflictError, match="work"):
        repository.append(
            RUN_ID,
            NODE_ID,
            PARTITION,
            expected_current_version=CheckpointVersion(0),
            expected_head_row_version=1,
            expected_work_row_version=1,
            payload_schema_version=1,
            source_cursor=None,
            output_position=None,
            artifact_id=None,
            committed_at=NOW,
        )


def test_checkpoint_replay_requires_its_immediate_history(monkeypatch: pytest.MonkeyPatch) -> None:
    session = cast(Session, MagicMock())
    session.execute.return_value = result(one_or_none=None)
    repository = SqlAlchemyCheckpointRepository(session)
    requested = checkpoint_module._RequestedCheckpoint(
        (RUN_ID, NODE_ID, PARTITION),
        CheckpointVersion(1),
        1,
        None,
        None,
        None,
        LATER,
    )
    with pytest.raises(ConsistencyCorruptionError, match="missing"):
        repository._replay_append(
            checkpoint_state(current=1, head_row=2, work_row=2),
            requested,
            expected_head_row=1,
            expected_work_row=1,
        )

    session.execute.return_value = result(one_or_none={})
    monkeypatch.setattr(checkpoint_module, "checkpoint_from_row", lambda _row: checkpoint_record())
    conflicting = checkpoint_module._RequestedCheckpoint(
        (RUN_ID, NODE_ID, PARTITION),
        CheckpointVersion(1),
        2,
        None,
        None,
        None,
        LATER,
    )
    with pytest.raises(CheckpointConflictError):
        repository._replay_append(
            checkpoint_state(current=1, head_row=2, work_row=2),
            conflicting,
            expected_head_row=1,
            expected_work_row=1,
        )


def test_event_state_and_frontier_corruption_classifiers(monkeypatch: pytest.MonkeyPatch) -> None:
    session = cast(Session, MagicMock())
    session.execute.side_effect = [result(one_or_none={}), result(one_or_none=None)]
    with pytest.raises(ConsistencyCorruptionError, match="incomplete"):
        event_module._load_event_state(session, RUN_ID)

    session.execute.side_effect = [
        result(one_or_none={"run_id": str(RUN_ID)}),
        result(one_or_none={}),
    ]
    monkeypatch.setattr(event_module, "stored_run_id", lambda _value: RunId("run_other"))
    monkeypatch.setattr(
        event_module,
        "event_counter_from_row",
        lambda _row: EventCounterState(RUN_ID, EventSequence(1), 1),
    )
    with pytest.raises(ConsistencyCorruptionError, match="identity"):
        event_module._load_event_state(session, RUN_ID)

    session.execute.side_effect = None
    for aggregate, sequence in (((1, 1, 1), 1), ((1, 1, 2), 3)):
        session.execute.return_value = result(one=aggregate)
        with pytest.raises(ConsistencyCorruptionError):
            event_module._validate_event_frontier(session, RUN_ID, EventSequence(sequence))


def test_event_record_validation_is_exhaustive(monkeypatch: pytest.MonkeyPatch) -> None:
    session = cast(Session, MagicMock())
    state = event_state()
    cases = (
        event_record(run_id=RunId("run_other")),
        event_record(occurred_at=UtcTimestamp(datetime(2026, 8, 12, 11, 59, tzinfo=UTC))),
        event_record(subject_id=RunId("run_other")),
    )
    for record in cases:
        with pytest.raises(ConsistencyCorruptionError):
            event_module._validate_event_records(session, state, (record,))

    work_record = event_record(subject_kind=EventSubjectKind.WORK_ITEM, subject_id=WORK_ID)

    def reject_subjects(*_args: object) -> NoReturn:
        raise ConsistencyInvalidRequestError("subject")

    monkeypatch.setattr(event_module, "_validate_subjects", reject_subjects)
    with pytest.raises(ConsistencyCorruptionError, match="work subject"):
        event_module._validate_event_records(session, state, (work_record,))


def test_event_counter_cas_classifier_is_exhaustive(monkeypatch: pytest.MonkeyPatch) -> None:
    repository = SqlAlchemyExecutionEventRepository(cast(Session, MagicMock()))
    scenarios = (
        (None, ConsistencyRecordNotFoundError),
        (event_state(row_version=2), ConsistencyStaleRowVersionError),
        (event_state(next_sequence=2), EventSequenceConflictError),
        (event_state(), EventSequenceConflictError),
    )
    for state, error in scenarios:
        monkeypatch.setattr(event_module, "_load_event_state", lambda *_args, state=state: state)
        with pytest.raises(error):
            repository._raise_counter_cas_failure(RUN_ID, EventSequence(1), 1)


def test_event_append_zero_row_and_result_integrity_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = cast(Session, MagicMock())
    session.in_transaction.return_value = True
    repository = SqlAlchemyExecutionEventRepository(session)
    monkeypatch.setattr(event_module, "_validate_subjects", lambda *_args: None)

    monkeypatch.setattr(
        event_module, "_load_event_state", lambda *_args: event_state(next_sequence=3)
    )
    with pytest.raises(EventSequenceConflictError, match="frontier"):
        repository.append(
            RUN_ID,
            expected_next_sequence=EventSequence(1),
            expected_counter_row_version=1,
            events=(pending_event(),),
        )

    monkeypatch.setattr(event_module, "_load_event_state", lambda *_args: event_state())
    session.execute.return_value = result(one_or_none=None)

    def rejected_counter(*_args: object) -> NoReturn:
        raise EventSequenceConflictError("counter")

    monkeypatch.setattr(repository, "_raise_counter_cas_failure", rejected_counter)
    with pytest.raises(EventSequenceConflictError, match="counter"):
        repository.append(
            RUN_ID,
            expected_next_sequence=EventSequence(1),
            expected_counter_row_version=1,
            events=(pending_event(),),
        )

    counter_row = {
        "run_id": str(RUN_ID),
        "next_sequence_number": 2,
        "row_version": 2,
    }
    session.execute.side_effect = [
        result(one_or_none=counter_row),
        result(all_rows=()),
    ]
    with pytest.raises(ConsistencyCorruptionError, match="incomplete"):
        repository.append(
            RUN_ID,
            expected_next_sequence=EventSequence(1),
            expected_counter_row_version=1,
            events=(pending_event(),),
        )

    session.execute.side_effect = [
        result(one_or_none=counter_row),
        result(all_rows=({},)),
    ]
    monkeypatch.setattr(
        event_module,
        "execution_event_from_row",
        lambda _row: event_record(run_id=RunId("run_other")),
    )
    with pytest.raises(ConsistencyCorruptionError, match="append result"):
        repository.append(
            RUN_ID,
            expected_next_sequence=EventSequence(1),
            expected_counter_row_version=1,
            events=(pending_event(),),
        )

    session.execute.side_effect = [
        result(one_or_none=counter_row),
        result(all_rows=({},)),
    ]
    monkeypatch.setattr(event_module, "execution_event_from_row", lambda _row: event_record())
    monkeypatch.setattr(
        event_module,
        "event_counter_from_row",
        lambda _row: EventCounterState(RUN_ID, EventSequence(3), 2),
    )
    with pytest.raises(ConsistencyCorruptionError, match="counter append"):
        repository.append(
            RUN_ID,
            expected_next_sequence=EventSequence(1),
            expected_counter_row_version=1,
            events=(pending_event(),),
        )


def test_event_replay_requires_complete_range(monkeypatch: pytest.MonkeyPatch) -> None:
    session = cast(Session, MagicMock())
    repository = SqlAlchemyExecutionEventRepository(session)
    session.execute.return_value = result(all_rows=())
    with pytest.raises(ConsistencyCorruptionError, match="incomplete"):
        repository._replay_append(
            event_state(next_sequence=2, row_version=2),
            expected_next=EventSequence(1),
            expected_row=1,
            encoded=(encoded_event(),),
        )

    session.execute.return_value = result(all_rows=({},))
    monkeypatch.setattr(event_module, "execution_event_from_row", lambda _row: event_record())
    monkeypatch.setattr(event_module, "_validate_event_records", lambda *_args: None)
    changed = event_module._EncodedEvent(
        "run_changed", NOW, EventSubjectKind.RUN, RUN_ID, None, 1, '{"safe":true}'
    )
    with pytest.raises(EventSequenceConflictError):
        repository._replay_append(
            event_state(next_sequence=2, row_version=2),
            expected_next=EventSequence(1),
            expected_row=1,
            encoded=(changed,),
        )


def test_idempotency_terminal_cas_classifier_is_exhaustive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = SqlAlchemyIdempotencyRepository(cast(Session, MagicMock()))
    identity = ("scope", "key")
    exact_terminal = stored_idempotency(
        status=IdempotencyStatus.COMPLETED, response=redacted(safe=True), updated_at=LATER
    )
    scenarios = (
        (stored_idempotency(digest="b" * 64), IdempotencyConflictError),
        (exact_terminal, None),
        (
            stored_idempotency(
                status=IdempotencyStatus.FAILED,
                response=redacted(safe=True),
                updated_at=LATER,
            ),
            IdempotencyConflictError,
        ),
        (stored_idempotency(updated_at=LATER), ConsistencyStaleRowVersionError),
        (stored_idempotency(), IdempotencyConflictError),
    )
    for stored, error in scenarios:
        monkeypatch.setattr(repository, "_require_stored", lambda _identity, stored=stored: stored)
        if error is None:
            assert (
                repository._classify_terminal_cas(
                    identity,
                    digest="a" * 64,
                    expected_updated_at=NOW,
                    status=IdempotencyStatus.COMPLETED,
                    schema_version=1,
                    response_json='{"safe":true}',
                    completed_at=LATER,
                )
                == exact_terminal.record
            )
        else:
            with pytest.raises(error):
                repository._classify_terminal_cas(
                    identity,
                    digest="a" * 64,
                    expected_updated_at=NOW,
                    status=IdempotencyStatus.COMPLETED,
                    schema_version=1,
                    response_json='{"safe":true}',
                    completed_at=LATER,
                )


def test_idempotency_public_zero_row_and_corrupt_result_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = cast(Session, MagicMock())
    session.in_transaction.return_value = True
    repository = SqlAlchemyIdempotencyRepository(session)
    request = document(action="safe")
    digest = idempotency_module.request_digest(request)

    monkeypatch.setattr(
        repository, "_require_stored", lambda _identity: stored_idempotency(digest="b" * 64)
    )
    with pytest.raises(IdempotencyConflictError, match="identity has a different request"):
        repository.complete(
            reservation(digest),
            request=request,
            response_schema_version=1,
            response=redacted(safe=True),
            completed_at=LATER,
        )

    with pytest.raises(ConsistencyCorruptionError, match="not in progress"):
        idempotency_module._reservation(
            stored_idempotency(
                status=IdempotencyStatus.COMPLETED,
                response=redacted(safe=True),
                updated_at=LATER,
            )
        )

    session.execute.return_value = result(one_or_none={})
    monkeypatch.setattr(
        idempotency_module,
        "stored_idempotency_from_row",
        lambda _row: stored_idempotency(digest="b" * 64),
    )
    with pytest.raises(ConsistencyCorruptionError, match="insert result"):
        repository.begin(scope="scope", key="key", request=request, started_at=NOW)

    current = stored_idempotency(digest=digest)
    monkeypatch.setattr(repository, "_require_stored", lambda _identity: current)
    session.execute.return_value = result(one_or_none=None)

    def classified(*_args: object, **_kwargs: object) -> NoReturn:
        raise IdempotencyConflictError("classified")

    monkeypatch.setattr(repository, "_classify_terminal_cas", classified)
    with pytest.raises(IdempotencyConflictError, match="classified"):
        repository.complete(
            reservation(digest),
            request=request,
            response_schema_version=1,
            response=redacted(safe=True),
            completed_at=LATER,
        )

    session.execute.return_value = result(one_or_none={})
    monkeypatch.setattr(
        idempotency_module,
        "stored_idempotency_from_row",
        lambda _row: stored_idempotency(
            status=IdempotencyStatus.FAILED,
            digest=digest,
            response=redacted(safe=True),
            updated_at=LATER,
        ),
    )
    with pytest.raises(ConsistencyCorruptionError, match="terminal result"):
        repository.complete(
            reservation(digest),
            request=request,
            response_schema_version=1,
            response=redacted(safe=True),
            completed_at=LATER,
        )
