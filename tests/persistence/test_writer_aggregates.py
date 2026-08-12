"""Integration tests for transactional-writer aggregate foundations."""

# pyright: reportPrivateUsage=false

import sqlite3
from collections.abc import Callable, Iterator
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import select, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence import SQLiteDatabase, SQLiteDatabaseConfig
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyCheckpointRepository,
    SqlAlchemyPipelineRepository,
    SqlAlchemyRunRepository,
    SqlAlchemyWorkItemRepository,
)
from paritygrid.adapters.persistence.repositories import run_revisions as run_revision_runtime
from paritygrid.adapters.persistence.repositories.consistency_common import (
    translate_consistency_storage_errors,
)
from paritygrid.adapters.persistence.repositories.execution_common import (
    translate_execution_storage_errors,
)
from paritygrid.adapters.persistence.repositories.repair_audit_common import (
    translate_audit_storage_errors,
    translate_repair_storage_errors,
)
from paritygrid.adapters.persistence.repositories.run_node_aggregates import (
    SqlAlchemyRunNodeAggregateRepository,
    _advance_metrics,
    _bucket_name,
    _NodeSnapshot,
    _status_for,
    _validate_claim,
    _validate_completed_work,
)
from paritygrid.adapters.persistence.repositories.run_revisions import (
    SqlAlchemyRunRevisionRepository,
)
from paritygrid.adapters.persistence.schema import run_nodes, runs
from paritygrid.adapters.persistence.writer.contention import is_sqlite_contention
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import CheckpointVersion
from paritygrid.application.ports.execution import (
    ExecutionCorruptionError,
    ExecutionInvalidRequestError,
    ExecutionRecordNotFoundError,
    ExecutionStaleRowVersionError,
    ExecutionStateConflictError,
    ExecutionStorageUnavailableError,
    RunNodeRecord,
    RunNodeStatus,
    RunRecord,
    WorkCompletion,
)
from paritygrid.application.ports.run_aggregates import MAX_WORK_METRIC, WorkMetricDelta
from paritygrid.application.ports.writer import PersistenceContentionError
from paritygrid.domain.execution import FailureClassification, RunState, WorkItemState
from paritygrid.domain.models import (
    AttemptNumber,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

PIPELINE_ID = PipelineId("pip_writer")
RUN_ID = RunId("run_writer")
NODE_ID = NodeId("nod_writer")
WORK_ID = WorkItemId("wrk_writer")


@pytest.fixture
def database(tmp_path: Path) -> Iterator[SQLiteDatabase]:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "writer aggregate %.db"))
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    try:
        yield database
    finally:
        database.close()


def timestamp(second: int) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 12, 12, 0, second, tzinfo=UTC))


def document(**values: object) -> ConfigurationDocument:
    return ConfigurationDocument.from_mapping(values)


def seed_running_run(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PIPELINE_ID,
            display_name="Writer pipeline",
            description=None,
            created_at=timestamp(0),
        )
        pipelines.publish_version(
            pipeline_id=PIPELINE_ID,
            expected_latest_version=None,
            specification=document(nodes=[]),
            planner_format_version=1,
            published_at=timestamp(0),
        )
        runs = SqlAlchemyRunRepository(session)
        runs.create(
            run_id=RUN_ID,
            pipeline_id=PIPELINE_ID,
            pipeline_version=PipelineVersion(1),
            runner_kind="threaded",
            runner_configuration=document(max_workers=2),
            scenario_seed=17,
            node_ids=(NODE_ID,),
            created_at=timestamp(1),
        )
        runs.transition(
            RUN_ID,
            expected_row_version=1,
            target_state=RunState.RUNNING,
            transitioned_at=timestamp(2),
        )


def create_and_register(session: Session) -> RunNodeRecord:
    repository = SqlAlchemyWorkItemRepository(session)
    created = repository.create(
        work_item_id=WORK_ID,
        run_id=RUN_ID,
        node_id=NODE_ID,
        partition_key=PartitionKey("part-1"),
        input_reference=document(page=1),
        created_at=timestamp(2),
    )
    return SqlAlchemyRunNodeAggregateRepository(session).register_work(
        created, expected_node_row_version=1
    )


def test_register_claim_completion_and_run_revision_are_exact(
    database: SQLiteDatabase,
) -> None:
    seed_running_run(database)
    with database.transaction() as session:
        registered = create_and_register(session)
        claim = SqlAlchemyWorkItemRepository(session).claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="writer",
            started_at=timestamp(3),
            lease_expires_at=timestamp(8),
            runner_kind="threaded",
            worker_identity="worker-1",
        )
        running = SqlAlchemyRunNodeAggregateRepository(session).apply_claim(
            claim, expected_node_row_version=registered.row_version
        )
        completed = SqlAlchemyWorkItemRepository(session).complete_claim(
            claim,
            WorkCompletion(
                target_state=WorkItemState.SUCCEEDED,
                finished_at=timestamp(4),
                retry_available_at=None,
                failure_classification=None,
                redacted_detail=None,
                result_reference=document(artifact="one"),
                records_processed=7,
                bytes_processed=70,
            ),
        )
        terminal = SqlAlchemyRunNodeAggregateRepository(session).apply_completion(
            completed,
            checkpoint=None,
            expected_node_row_version=running.row_version,
            metrics=WorkMetricDelta(
                records_read=8, records_written=7, bytes_read=80, bytes_written=70
            ),
        )
        revised = SqlAlchemyRunRevisionRepository(session).advance(RUN_ID, expected_row_version=2)

        assert registered.status is RunNodeStatus.PENDING
        assert (registered.work_total, registered.work_pending) == (1, 1)
        assert running.status is RunNodeStatus.RUNNING
        assert running.started_at == timestamp(3)
        assert terminal.status is RunNodeStatus.SUCCEEDED
        assert terminal.finished_at == timestamp(4)
        assert terminal.work_succeeded == 1
        assert terminal.records_read == 8
        assert terminal.records_written == 7
        assert terminal.bytes_read == 80
        assert terminal.bytes_written == 70
        assert terminal.duration == completed.attempt.duration
        assert revised.row_version == 3
        assert revised.state is RunState.RUNNING
        assert revised.started_at == timestamp(2)


def test_checkpointed_completion_validates_the_twice_advanced_work_row(
    database: SQLiteDatabase,
) -> None:
    seed_running_run(database)
    with database.transaction() as session:
        registered = create_and_register(session)
        work = SqlAlchemyWorkItemRepository(session)
        claim = work.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="writer",
            started_at=timestamp(3),
            lease_expires_at=timestamp(9),
            runner_kind="threaded",
            worker_identity="worker-1",
        )
        running = SqlAlchemyRunNodeAggregateRepository(session).apply_claim(
            claim, expected_node_row_version=registered.row_version
        )
        completed = work.complete_claim(
            claim,
            WorkCompletion(
                WorkItemState.SUCCEEDED,
                timestamp(4),
                None,
                None,
                None,
                None,
                1,
                10,
            ),
        )
        checkpoint = SqlAlchemyCheckpointRepository(session).append(
            RUN_ID,
            NODE_ID,
            PartitionKey("part-1"),
            expected_current_version=CheckpointVersion(
                completed.work_item.expected_checkpoint_version
            ),
            expected_head_row_version=1,
            expected_work_row_version=completed.work_item.row_version,
            payload_schema_version=1,
            source_cursor=document(offset=1),
            output_position=None,
            artifact_id=None,
            committed_at=timestamp(5),
        )
        node = SqlAlchemyRunNodeAggregateRepository(session).apply_completion(
            completed,
            checkpoint=checkpoint,
            expected_node_row_version=running.row_version,
            metrics=WorkMetricDelta(records_written=1, bytes_written=10),
        )
        assert checkpoint.work.row_version == completed.work_item.row_version + 1
        assert node.status is RunNodeStatus.SUCCEEDED
        durable_after_checkpoint = work.get(WORK_ID)
        assert durable_after_checkpoint is not None
        _validate_completed_work(durable_after_checkpoint, completed, checkpoint)
        with pytest.raises(ExecutionStateConflictError, match="checkpointed"):
            _validate_completed_work(
                durable_after_checkpoint,
                completed,
                replace(
                    checkpoint,
                    head=replace(checkpoint.head, updated_at=timestamp(6)),
                ),
            )
        with pytest.raises(ExecutionStateConflictError, match="checkpointed"):
            _validate_completed_work(
                durable_after_checkpoint,
                replace(
                    completed,
                    work_item=replace(completed.work_item, row_version=checkpoint.work.row_version),
                ),
                checkpoint,
            )
        with pytest.raises(ExecutionStateConflictError, match="checkpointed"):
            _validate_completed_work(
                durable_after_checkpoint,
                replace(
                    completed,
                    work_item=replace(completed.work_item, expected_checkpoint_version=1),
                ),
                checkpoint,
            )


def test_retry_and_expiry_attempts_drive_pending_retry_and_duration_aggregates(
    database: SQLiteDatabase,
) -> None:
    seed_running_run(database)
    with database.transaction() as session:
        registered = create_and_register(session)
        work = SqlAlchemyWorkItemRepository(session)
        aggregates = SqlAlchemyRunNodeAggregateRepository(session)
        claim = work.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="writer",
            started_at=timestamp(3),
            lease_expires_at=timestamp(5),
            runner_kind="threaded",
            worker_identity="worker-1",
        )
        running = aggregates.apply_claim(claim, expected_node_row_version=registered.row_version)
        recovered = work.recover_expired_claim(
            WORK_ID,
            expected_row_version=claim.row_version,
            expected_attempt_number=claim.attempt_number,
            observed_at=timestamp(5),
            retry_available_at=timestamp(6),
        )
        after_recovery = aggregates.apply_recovery(
            recovered, expected_node_row_version=running.row_version
        )
        assert after_recovery.status is RunNodeStatus.RUNNING
        assert after_recovery.work_pending == 1
        assert after_recovery.work_running == 0
        assert after_recovery.retry_count == 1
        assert after_recovery.duration == recovered.attempt.duration


def test_terminal_precedence_and_mixed_cancellation_are_derived(
    database: SQLiteDatabase,
) -> None:
    seed_running_run(database)
    second = WorkItemId("wrk_writer-2")
    with database.transaction() as session:
        work = SqlAlchemyWorkItemRepository(session)
        aggregates = SqlAlchemyRunNodeAggregateRepository(session)
        first = work.create(
            work_item_id=WORK_ID,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PartitionKey("part-1"),
            input_reference=None,
            created_at=timestamp(2),
        )
        node = aggregates.register_work(first, expected_node_row_version=1)
        other = work.create(
            work_item_id=second,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PartitionKey("part-2"),
            input_reference=None,
            created_at=timestamp(2),
        )
        node = aggregates.register_work(other, expected_node_row_version=node.row_version)
        first_claim = work.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="writer",
            started_at=timestamp(3),
            lease_expires_at=timestamp(9),
            runner_kind="threaded",
            worker_identity="one",
        )
        node = aggregates.apply_claim(first_claim, expected_node_row_version=node.row_version)
        first_done = work.complete_claim(
            first_claim,
            WorkCompletion(
                WorkItemState.CANCELLED,
                timestamp(4),
                None,
                FailureClassification.USER_CANCELLATION,
                None,
                None,
                0,
                0,
            ),
        )
        node = aggregates.apply_completion(
            first_done,
            checkpoint=None,
            expected_node_row_version=node.row_version,
            metrics=WorkMetricDelta(),
        )
        assert node.status is RunNodeStatus.RUNNING
        second_claim = work.claim(
            second,
            expected_row_version=1,
            lease_owner="writer",
            started_at=timestamp(4),
            lease_expires_at=timestamp(9),
            runner_kind="threaded",
            worker_identity="two",
        )
        node = aggregates.apply_claim(second_claim, expected_node_row_version=node.row_version)
        second_done = work.complete_claim(
            second_claim,
            WorkCompletion(
                WorkItemState.SUCCEEDED,
                timestamp(5),
                None,
                None,
                None,
                None,
                0,
                0,
            ),
        )
        node = aggregates.apply_completion(
            second_done,
            checkpoint=None,
            expected_node_row_version=node.row_version,
            metrics=WorkMetricDelta(),
        )
        assert node.status is RunNodeStatus.PARTIALLY_SUCCEEDED


def test_empty_finalization_and_raw_drift_or_stale_revision_fail_closed(
    database: SQLiteDatabase,
) -> None:
    seed_running_run(database)
    with database.transaction() as session:
        node = SqlAlchemyRunNodeAggregateRepository(session).finalize_empty(
            RUN_ID,
            NODE_ID,
            expected_node_row_version=1,
            finalized_at=timestamp(3),
        )
        assert node.status is RunNodeStatus.SUCCEEDED
        assert node.started_at == node.finished_at == timestamp(3)
        with pytest.raises(ExecutionStaleRowVersionError):
            SqlAlchemyRunRevisionRepository(session).advance(RUN_ID, expected_row_version=1)

    other_run = RunId("run_writer-drift")
    with database.transaction() as session:
        SqlAlchemyRunRepository(session).create(
            run_id=other_run,
            pipeline_id=PIPELINE_ID,
            pipeline_version=PipelineVersion(1),
            runner_kind="threaded",
            runner_configuration=document(),
            scenario_seed=None,
            node_ids=(NODE_ID,),
            created_at=timestamp(3),
        )
        created = SqlAlchemyWorkItemRepository(session).create(
            work_item_id=WorkItemId("wrk_writer-drift"),
            run_id=other_run,
            node_id=NODE_ID,
            partition_key=PartitionKey("drift"),
            input_reference=None,
            created_at=timestamp(3),
        )
        session.execute(
            update(run_nodes).where(run_nodes.c.run_id == str(other_run)).values(work_total=9)
        )
        with pytest.raises(ExecutionCorruptionError, match="drift"):
            SqlAlchemyRunNodeAggregateRepository(session).register_work(
                created, expected_node_row_version=1
            )


class _SQLiteFailureError(Exception):
    sqlite_errorcode: int

    def __init__(self, code: int) -> None:
        super().__init__("redacted")
        self.sqlite_errorcode = code


def operational(code: int) -> OperationalError:
    return OperationalError("statement", {}, _SQLiteFailureError(code))


def test_contention_classifier_uses_only_sqlite_busy_and_locked_base_codes() -> None:
    assert is_sqlite_contention(operational(sqlite3.SQLITE_BUSY))
    assert is_sqlite_contention(operational(sqlite3.SQLITE_LOCKED | (3 << 8)))
    assert not is_sqlite_contention(operational(sqlite3.SQLITE_IOERR))
    assert not is_sqlite_contention(OperationalError("statement", {}, RuntimeError()))


def test_repository_translation_exposes_only_redacted_contention() -> None:
    @translate_execution_storage_errors
    def busy() -> None:
        raise operational(sqlite3.SQLITE_BUSY)

    @translate_execution_storage_errors
    def unavailable() -> None:
        raise operational(sqlite3.SQLITE_IOERR)

    with pytest.raises(PersistenceContentionError) as caught:
        busy()
    assert caught.value.args == ("Persistence is temporarily contended.",)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    with pytest.raises(ExecutionStorageUnavailableError):
        unavailable()


@pytest.mark.parametrize(
    "translator",
    [
        translate_consistency_storage_errors,
        translate_repair_storage_errors,
        translate_audit_storage_errors,
    ],
)
def test_every_writer_repository_translates_only_confirmed_contention(
    translator: object,
) -> None:
    typed_translator = cast(Callable[[Callable[[], None]], Callable[[], None]], translator)

    @typed_translator
    def busy() -> None:
        raise operational(sqlite3.SQLITE_LOCKED)

    with pytest.raises(PersistenceContentionError) as caught:
        busy()
    assert caught.value.args == ("Persistence is temporarily contended.",)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def snapshot(**changes: int) -> _NodeSnapshot:
    values = {
        "total": 1,
        "pending": 0,
        "running": 0,
        "succeeded": 1,
        "quarantined": 0,
        "failed": 0,
        "cancelled": 0,
        "retries": 0,
        "duration_microseconds": 0,
    }
    values.update(changes)
    return _NodeSnapshot(**values)


def test_aggregate_bucket_and_terminal_precedence_matrix_is_closed() -> None:
    assert _bucket_name(WorkItemState.PENDING) == "pending"
    assert _bucket_name(WorkItemState.RETRY_WAIT) == "pending"
    assert _bucket_name(WorkItemState.RUNNING) == "running"
    assert _bucket_name(WorkItemState.SUCCEEDED) == "succeeded"
    assert _bucket_name(WorkItemState.QUARANTINED) == "quarantined"
    assert _bucket_name(WorkItemState.FAILED) == "failed"
    assert _bucket_name(WorkItemState.CANCELLED) == "cancelled"
    with pytest.raises(ExecutionCorruptionError, match="transient"):
        _bucket_name(WorkItemState.LEASED)

    assert _status_for(snapshot(total=0, succeeded=0), started=False) is RunNodeStatus.PENDING
    assert _status_for(snapshot(pending=1, succeeded=0), started=False) is RunNodeStatus.PENDING
    assert _status_for(snapshot(pending=1, succeeded=0), started=True) is RunNodeStatus.RUNNING
    assert _status_for(snapshot(failed=1, succeeded=0), started=True) is RunNodeStatus.FAILED
    assert _status_for(snapshot(cancelled=1, succeeded=0), started=True) is RunNodeStatus.CANCELLED
    assert (
        _status_for(snapshot(quarantined=1, succeeded=0), started=True)
        is RunNodeStatus.PARTIALLY_SUCCEEDED
    )
    assert _status_for(snapshot(), started=True) is RunNodeStatus.SUCCEEDED
    with pytest.raises(ExecutionCorruptionError, match="underflow"):
        snapshot(total=0, succeeded=0).reverse_registration()


def test_aggregate_contract_mismatch_and_capacity_helpers_fail_closed(
    database: SQLiteDatabase,
) -> None:
    seed_running_run(database)
    with database.transaction() as session:
        registered = create_and_register(session)
        work_repo = SqlAlchemyWorkItemRepository(session)
        claim = work_repo.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="writer",
            started_at=timestamp(3),
            lease_expires_at=timestamp(8),
            runner_kind="threaded",
            worker_identity="worker-1",
        )
        durable = work_repo.get(WORK_ID)
        assert durable is not None
        _validate_claim(durable, claim)
        with pytest.raises(ExecutionStateConflictError, match="claim"):
            _validate_claim(durable, replace(claim, lease_owner="different"))
        running = SqlAlchemyRunNodeAggregateRepository(session).apply_claim(
            claim, expected_node_row_version=registered.row_version
        )
        with pytest.raises(ExecutionStateConflictError, match="capacity"):
            _advance_metrics(
                replace(running, records_read=9_223_372_036_854_775_807),
                WorkMetricDelta(records_read=1),
            )
        completed = work_repo.complete_claim(
            claim,
            WorkCompletion(
                WorkItemState.FAILED,
                timestamp(4),
                None,
                FailureClassification.UNKNOWN,
                None,
                None,
                0,
                0,
            ),
        )
        _validate_completed_work(completed.work_item, completed, None)
        with pytest.raises(ExecutionStateConflictError, match="completed work"):
            _validate_completed_work(
                replace(completed.work_item, state=WorkItemState.CANCELLED), completed, None
            )
        with pytest.raises(ExecutionStateConflictError, match="completed work"):
            _validate_completed_work(
                completed.work_item,
                replace(completed, work_item=replace(completed.work_item, row_version=99)),
                None,
            )
        with pytest.raises(ExecutionInvalidRequestError, match="expired-lease"):
            SqlAlchemyRunNodeAggregateRepository(session).apply_recovery(
                completed, expected_node_row_version=running.row_version
            )


def test_aggregate_missing_stale_chronology_and_transaction_paths(
    database: SQLiteDatabase,
) -> None:
    seed_running_run(database)
    detached = Session(database.engine)
    try:
        with pytest.raises(ExecutionInvalidRequestError, match="caller-owned"):
            SqlAlchemyRunNodeAggregateRepository(detached).finalize_empty(
                RUN_ID,
                NODE_ID,
                expected_node_row_version=1,
                finalized_at=timestamp(3),
            )
    finally:
        detached.close()
    with database.transaction() as session:
        repository = SqlAlchemyRunNodeAggregateRepository(session)
        with pytest.raises(ExecutionRecordNotFoundError, match="work item"):
            repository._require_work(WorkItemId("wrk_missing"))
        with pytest.raises(ExecutionRecordNotFoundError, match="run node"):
            repository._require_node(RunId("run_missing"), NODE_ID, 1)
        with pytest.raises(ExecutionStaleRowVersionError, match="stale"):
            repository._require_node(RUN_ID, NODE_ID, 2)
        with pytest.raises(ExecutionInvalidRequestError, match="cannot precede"):
            repository.finalize_empty(
                RUN_ID,
                NODE_ID,
                expected_node_row_version=1,
                finalized_at=timestamp(0),
            )
        created = SqlAlchemyWorkItemRepository(session).create(
            work_item_id=WORK_ID,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PartitionKey("nonempty"),
            input_reference=None,
            created_at=timestamp(2),
        )
        repository.register_work(created, expected_node_row_version=1)
        with pytest.raises(ExecutionStateConflictError, match="empty pending"):
            repository.finalize_empty(
                RUN_ID,
                NODE_ID,
                expected_node_row_version=2,
                finalized_at=timestamp(3),
            )


def test_revision_missing_transaction_and_cas_classification(
    database: SQLiteDatabase,
) -> None:
    seed_running_run(database)
    detached = Session(database.engine)
    try:
        with pytest.raises(ExecutionInvalidRequestError, match="caller-owned"):
            SqlAlchemyRunRevisionRepository(detached).advance(RUN_ID, expected_row_version=2)
    finally:
        detached.close()
    with database.transaction() as session:
        repository = SqlAlchemyRunRevisionRepository(session)
        with pytest.raises(ExecutionRecordNotFoundError, match="run"):
            repository.advance(RunId("run_missing"), expected_row_version=1)
        current = SqlAlchemyRunRepository(session).get(RUN_ID)
        assert current is not None
        prior_node = SqlAlchemyRunRepository(session).get_node(RUN_ID, NODE_ID)
        assert prior_node is not None
        session.execute(
            update(run_nodes).where(run_nodes.c.run_id == str(RUN_ID)).values(row_version=2)
        )
        with pytest.raises(ExecutionStaleRowVersionError, match="stale"):
            SqlAlchemyRunNodeAggregateRepository(session)._raise_node_cas(prior_node)


class _Rows:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def all(self) -> list[tuple[object, ...]]:
        return self._rows


class _SnapshotSession:
    def __init__(
        self,
        states: list[tuple[object, ...]],
        attempts: list[tuple[object, ...]] | None = None,
    ) -> None:
        self._results = [_Rows(states), _Rows([] if attempts is None else attempts)]

    def execute(self, _statement: object) -> _Rows:
        return self._results.pop(0)


def test_snapshot_rejects_every_grouped_corruption_shape() -> None:
    cases: tuple[tuple[list[tuple[object, ...]], list[tuple[object, ...]] | None, str], ...] = (
        ([("unknown", 1)], None, "work state"),
        ([(WorkItemState.LEASED.value, 1)], None, "work state"),
        (
            [
                (WorkItemState.PENDING.value, MAX_WORK_METRIC),
                (WorkItemState.RUNNING.value, MAX_WORK_METRIC),
            ],
            None,
            "work total",
        ),
        ([], [("unknown", 1, 1)], "attempt outcome"),
        (
            [],
            [("lease_expired", MAX_WORK_METRIC, 0), ("retry_scheduled", MAX_WORK_METRIC, 0)],
            "attempt aggregate",
        ),
        ([], [("succeeded", 1, 9_223_372_036_854_775_807)], "attempt aggregate"),
    )
    for states, attempts, message in cases:
        repository = SqlAlchemyRunNodeAggregateRepository(
            cast(Session, _SnapshotSession(states, attempts))
        )
        with pytest.raises(ExecutionCorruptionError, match=message):
            repository._snapshot(RUN_ID, NODE_ID)


def test_registration_mismatch_terminal_and_node_cas_missing_paths(
    database: SQLiteDatabase,
) -> None:
    seed_running_run(database)
    with database.transaction() as session:
        work_repo = SqlAlchemyWorkItemRepository(session)
        aggregate = SqlAlchemyRunNodeAggregateRepository(session)
        created = work_repo.create(
            work_item_id=WORK_ID,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PartitionKey("first"),
            input_reference=None,
            created_at=timestamp(2),
        )
        with pytest.raises(ExecutionInvalidRequestError, match="pending"):
            aggregate.register_work(
                replace(created, state=WorkItemState.RUNNING), expected_node_row_version=1
            )
        with pytest.raises(ExecutionStateConflictError, match="storage"):
            aggregate.register_work(replace(created, row_version=2), expected_node_row_version=1)
        node = aggregate.register_work(created, expected_node_row_version=1)
        claim = work_repo.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="writer",
            started_at=timestamp(3),
            lease_expires_at=timestamp(8),
            runner_kind="threaded",
            worker_identity="worker",
        )
        node = aggregate.apply_claim(claim, expected_node_row_version=node.row_version)
        completed = work_repo.complete_claim(
            claim,
            WorkCompletion(
                WorkItemState.SUCCEEDED,
                timestamp(4),
                None,
                None,
                None,
                None,
                0,
                0,
            ),
        )
        node = aggregate.apply_completion(
            completed,
            checkpoint=None,
            expected_node_row_version=node.row_version,
            metrics=WorkMetricDelta(),
        )
        second = work_repo.create(
            work_item_id=WorkItemId("wrk_second"),
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PartitionKey("second"),
            input_reference=None,
            created_at=timestamp(4),
        )
        with pytest.raises(ExecutionStateConflictError, match="terminal"):
            aggregate.register_work(second, expected_node_row_version=node.row_version)


def test_node_cas_same_revision_drift_is_corruption(database: SQLiteDatabase) -> None:
    seed_running_run(database)
    with database.transaction() as session:
        prior = SqlAlchemyRunRepository(session).get_node(RUN_ID, NODE_ID)
        assert prior is not None
        session.execute(
            update(run_nodes)
            .where(run_nodes.c.run_id == str(RUN_ID), run_nodes.c.node_id == str(NODE_ID))
            .values(records_read=1)
        )
        with pytest.raises(ExecutionCorruptionError, match="without a revision"):
            SqlAlchemyRunNodeAggregateRepository(session)._raise_node_cas(prior)


def test_node_update_missing_after_cas_is_typed(database: SQLiteDatabase) -> None:
    seed_running_run(database)
    with database.transaction() as session:
        prior = SqlAlchemyRunRepository(session).get_node(RUN_ID, NODE_ID)
        assert prior is not None
        aggregate = SqlAlchemyRunNodeAggregateRepository(
            cast(Session, _ScriptedSession([None, None]))
        )
        with pytest.raises(ExecutionRecordNotFoundError, match="run node"):
            aggregate._update_node(
                prior,
                snapshot(total=0, succeeded=0),
                status=prior.status,
                started_at=prior.started_at,
            )


def test_node_update_returning_wrong_revision_is_corruption(database: SQLiteDatabase) -> None:
    seed_running_run(database)
    with database.transaction() as session:
        prior = SqlAlchemyRunRepository(session).get_node(RUN_ID, NODE_ID)
        assert prior is not None
        raw = dict(
            session.execute(
                select(run_nodes).where(
                    run_nodes.c.run_id == str(RUN_ID), run_nodes.c.node_id == str(NODE_ID)
                )
            )
            .mappings()
            .one()
        )
    aggregate = SqlAlchemyRunNodeAggregateRepository(cast(Session, _ScriptedSession([raw])))
    with pytest.raises(ExecutionCorruptionError, match="update result"):
        aggregate._update_node(
            prior,
            snapshot(total=0, succeeded=0),
            status=prior.status,
            started_at=None,
        )


def test_run_revision_cas_and_returning_classification_is_exhaustive(
    database: SQLiteDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_running_run(database)
    with database.transaction() as session:
        current = SqlAlchemyRunRepository(session).get(RUN_ID)
        assert current is not None
        raw = dict(
            session.execute(select(runs).where(runs.c.run_id == str(RUN_ID))).mappings().one()
        )

    repository = SqlAlchemyRunRevisionRepository(cast(Session, _ScriptedSession([None])))
    with pytest.raises(ExecutionRecordNotFoundError, match="run"):
        repository._raise_cas(RUN_ID, current.row_version, current)

    stale_raw = {**raw, "row_version": current.row_version + 1}
    repository = SqlAlchemyRunRevisionRepository(cast(Session, _ScriptedSession([stale_raw])))
    with pytest.raises(ExecutionStaleRowVersionError, match="stale"):
        repository._raise_cas(RUN_ID, current.row_version, current)

    changed_raw = {**raw, "state": RunState.PAUSING.value}
    repository = SqlAlchemyRunRevisionRepository(cast(Session, _ScriptedSession([changed_raw])))
    with pytest.raises(ExecutionStateConflictError, match="lifecycle"):
        repository._raise_cas(RUN_ID, current.row_version, current)

    repository = SqlAlchemyRunRevisionRepository(cast(Session, _ScriptedSession([raw])))
    with pytest.raises(ExecutionStateConflictError, match="rejected"):
        repository._raise_cas(RUN_ID, current.row_version, current)

    def fixed_get(_repository: SqlAlchemyRunRepository, _identity: RunId) -> RunRecord:
        return current

    monkeypatch.setattr(run_revision_runtime.SqlAlchemyRunRepository, "get", fixed_get)
    update_lost = SqlAlchemyRunRevisionRepository(
        cast(Session, _ScriptedSession([None, stale_raw]))
    )
    with pytest.raises(ExecutionStaleRowVersionError, match="stale"):
        update_lost.advance(RUN_ID, expected_row_version=current.row_version)

    inconsistent = SqlAlchemyRunRevisionRepository(cast(Session, _ScriptedSession([raw])))
    with pytest.raises(ExecutionStateConflictError, match="inconsistent"):
        inconsistent.advance(RUN_ID, expected_row_version=current.row_version)


def test_claim_cannot_precede_existing_node_start(database: SQLiteDatabase) -> None:
    seed_running_run(database)
    second = WorkItemId("wrk_earlier")
    with database.transaction() as session:
        work = SqlAlchemyWorkItemRepository(session)
        aggregate = SqlAlchemyRunNodeAggregateRepository(session)
        first = work.create(
            work_item_id=WORK_ID,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PartitionKey("first"),
            input_reference=None,
            created_at=timestamp(2),
        )
        node = aggregate.register_work(first, expected_node_row_version=1)
        first_claim = work.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="writer",
            started_at=timestamp(3),
            lease_expires_at=timestamp(8),
            runner_kind="threaded",
            worker_identity="first",
        )
        node = aggregate.apply_claim(first_claim, expected_node_row_version=node.row_version)
        other = work.create(
            work_item_id=second,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PartitionKey("second"),
            input_reference=None,
            created_at=timestamp(2),
        )
        node = aggregate.register_work(other, expected_node_row_version=node.row_version)
        earlier = work.claim(
            second,
            expected_row_version=1,
            lease_owner="writer",
            started_at=timestamp(2),
            lease_expires_at=timestamp(8),
            runner_kind="threaded",
            worker_identity="second",
        )
        with pytest.raises(ExecutionInvalidRequestError, match="precedes"):
            aggregate.apply_claim(earlier, expected_node_row_version=node.row_version)


@pytest.mark.parametrize("recovery", [False, True])
def test_completion_and_recovery_reject_node_without_durable_start(
    database: SQLiteDatabase,
    recovery: bool,
) -> None:
    seed_running_run(database)
    with database.transaction() as session:
        work = SqlAlchemyWorkItemRepository(session)
        aggregate = SqlAlchemyRunNodeAggregateRepository(session)
        created = work.create(
            work_item_id=WORK_ID,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PartitionKey("unstaged-claim"),
            input_reference=None,
            created_at=timestamp(2),
        )
        aggregate.register_work(created, expected_node_row_version=1)
        claim = work.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="writer",
            started_at=timestamp(3),
            lease_expires_at=timestamp(5),
            runner_kind="threaded",
            worker_identity="worker",
        )
        session.execute(
            update(run_nodes)
            .where(run_nodes.c.run_id == str(RUN_ID), run_nodes.c.node_id == str(NODE_ID))
            .values(work_pending=0, work_running=1)
        )
        if recovery:
            completed = work.recover_expired_claim(
                WORK_ID,
                expected_row_version=2,
                expected_attempt_number=AttemptNumber(1),
                observed_at=timestamp(5),
                retry_available_at=timestamp(6),
            )
            with pytest.raises(ExecutionStateConflictError, match="not running"):
                aggregate.apply_recovery(completed, expected_node_row_version=2)
        else:
            completed = work.complete_claim(
                claim,
                WorkCompletion(
                    WorkItemState.FAILED,
                    timestamp(4),
                    None,
                    FailureClassification.UNKNOWN,
                    None,
                    None,
                    0,
                    0,
                ),
            )
            with pytest.raises(ExecutionStateConflictError, match="not running"):
                aggregate.apply_completion(
                    completed,
                    checkpoint=None,
                    expected_node_row_version=2,
                    metrics=WorkMetricDelta(),
                )


class _MappingResult:
    def __init__(self, row: object) -> None:
        self._row = row

    def mappings(self) -> _MappingResult:
        return self

    def one_or_none(self) -> object:
        return self._row


class _ScriptedSession:
    def __init__(self, rows: list[object]) -> None:
        self._rows = rows

    def execute(self, _statement: object) -> _MappingResult:
        return _MappingResult(self._rows.pop(0))

    def in_transaction(self) -> bool:
        return True
