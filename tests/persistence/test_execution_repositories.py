"""Behavioral integration tests for durable run and work repositories."""

from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import event, func, select, update

from paritygrid.adapters.persistence import (
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    create_session_factory,
)
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyPipelineRepository,
    SqlAlchemyRunRepository,
    SqlAlchemyWorkAttemptRepository,
    SqlAlchemyWorkItemRepository,
)
from paritygrid.adapters.persistence.repositories import work_items as work_runtime
from paritygrid.adapters.persistence.schema import (
    checkpoint_heads,
    run_event_counters,
    run_nodes,
    runs,
    work_attempts,
    work_items,
)
from paritygrid.application.ports import ConfigurationDocument
from paritygrid.application.ports.execution import (
    AttemptOutcome,
    ExecutionCorruptionError,
    ExecutionDuplicateError,
    ExecutionInvalidRequestError,
    ExecutionLeaseExpiredError,
    ExecutionLeaseMismatchError,
    ExecutionRecordNotFoundError,
    ExecutionStaleRowVersionError,
    ExecutionStateConflictError,
    ExecutionStorageUnavailableError,
    RunNodeStatus,
    WorkClaim,
    WorkCompletion,
)
from paritygrid.domain.errors import InvalidTransitionError
from paritygrid.domain.execution import FailureClassification, RunState, WorkItemState
from paritygrid.domain.models import (
    AttemptNumber,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    StateFingerprint,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

PIPELINE_ID = PipelineId("pip_execution")
RUN_ID = RunId("run_execution")
NODE_ID = NodeId("nod_source")
WORK_ID = WorkItemId("wrk_partition")


class FatalTransactionProbe(BaseException):
    """Simulate non-Exception unwinding after a repository write set."""


@pytest.fixture
def database(tmp_path: Path) -> Iterator[SQLiteDatabase]:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "execution state %25.db"))
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


def sql_failure_listener(fragment: str, failure_type: type[BaseException]) -> Callable[..., None]:
    def fail_on_statement(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        if statement.startswith("INSERT INTO") and fragment in statement:
            raise failure_type

    return fail_on_statement


def seed_pipeline(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        repository = SqlAlchemyPipelineRepository(session)
        repository.create(
            pipeline_id=PIPELINE_ID,
            display_name="Execution pipeline",
            description=None,
            created_at=timestamp(0),
        )
        repository.publish_version(
            pipeline_id=PIPELINE_ID,
            expected_latest_version=None,
            specification=document(nodes=[]),
            planner_format_version=1,
            published_at=timestamp(0),
        )


def create_run(database: SQLiteDatabase, run_id: RunId = RUN_ID) -> None:
    with database.transaction() as session:
        SqlAlchemyRunRepository(session).create(
            run_id=run_id,
            pipeline_id=PIPELINE_ID,
            pipeline_version=PipelineVersion(1),
            runner_kind="threaded",
            runner_configuration=document(max_workers=2),
            scenario_seed=-9_223_372_036_854_775_808,
            node_ids=(NODE_ID,),
            created_at=timestamp(1),
        )


def start_run(database: SQLiteDatabase, run_id: RunId = RUN_ID) -> None:
    with database.transaction() as session:
        SqlAlchemyRunRepository(session).transition(
            run_id,
            expected_row_version=1,
            target_state=RunState.RUNNING,
            transitioned_at=timestamp(2),
        )


def create_work(database: SQLiteDatabase, work_id: WorkItemId = WORK_ID) -> None:
    with database.transaction() as session:
        SqlAlchemyWorkItemRepository(session).create(
            work_item_id=work_id,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PartitionKey("page-0001"),
            input_reference=document(page=1),
            created_at=timestamp(2),
        )


def claim_work(database: SQLiteDatabase) -> WorkClaim:
    with database.transaction() as session:
        return SqlAlchemyWorkItemRepository(session).claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="scheduler-main",
            started_at=timestamp(3),
            lease_expires_at=timestamp(6),
            runner_kind="threaded",
            worker_identity="worker-01",
        )


def create_run_then_fail(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        SqlAlchemyRunRepository(session).create(
            run_id=RUN_ID,
            pipeline_id=PIPELINE_ID,
            pipeline_version=PipelineVersion(1),
            runner_kind="threaded",
            runner_configuration=document(),
            scenario_seed=None,
            node_ids=(NODE_ID,),
            created_at=timestamp(1),
        )
        raise RuntimeError("rollback")


def create_work_then_fail(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        SqlAlchemyWorkItemRepository(session).create(
            work_item_id=WORK_ID,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PartitionKey("page-0001"),
            input_reference=None,
            created_at=timestamp(2),
        )
        raise RuntimeError("rollback")


def complete_work_then_fail(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        repository = SqlAlchemyWorkItemRepository(session)
        claim = repository.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="scheduler-main",
            started_at=timestamp(3),
            lease_expires_at=timestamp(8),
            runner_kind="threaded",
            worker_identity="worker-01",
        )
        repository.complete_claim(
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
        raise RuntimeError("rollback")


def complete_work_then_fatal(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        repository = SqlAlchemyWorkItemRepository(session)
        claim = repository.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="scheduler-main",
            started_at=timestamp(3),
            lease_expires_at=timestamp(8),
            runner_kind="threaded",
            worker_identity="worker-01",
        )
        repository.complete_claim(
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
        raise FatalTransactionProbe


def test_run_create_is_atomic_strict_and_reopens(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    with database.transaction() as session:
        repository = SqlAlchemyRunRepository(session)
        created = repository.create(
            run_id=RUN_ID,
            pipeline_id=PIPELINE_ID,
            pipeline_version=PipelineVersion(1),
            runner_kind="threaded",
            runner_configuration=document(max_workers=2),
            scenario_seed=9_223_372_036_854_775_807,
            node_ids=(NodeId("nod_target"), NODE_ID),
            created_at=timestamp(1),
        )
        assert created.state is RunState.QUEUED
        assert created.row_version == 1
        assert created.started_at is None
        assert "max_workers" not in repr(created)
        counter = repository.get_event_counter(RUN_ID)
        assert counter is not None
        assert counter.next_sequence_number == 1
        nodes = repository.list_nodes(RUN_ID, limit=1)
        assert nodes.items[0].node_id == NODE_ID
        assert nodes.items[0].status is RunNodeStatus.PENDING
        assert nodes.next_cursor == NODE_ID
        assert repository.list_nodes(RUN_ID, limit=2, after=NODE_ID).items[0].node_id == (
            NodeId("nod_target")
        )
        with pytest.raises(ExecutionDuplicateError):
            repository.create(
                run_id=RUN_ID,
                pipeline_id=PIPELINE_ID,
                pipeline_version=PipelineVersion(1),
                runner_kind="threaded",
                runner_configuration=document(),
                scenario_seed=None,
                node_ids=(NODE_ID,),
                created_at=timestamp(1),
            )
    with database.transaction() as session:
        repository = SqlAlchemyRunRepository(session)
        assert repository.get(RUN_ID) == created
        assert repository.list(limit=1).items == (created,)


def test_run_create_rolls_back_counter_and_nodes_on_failure(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    with pytest.raises(RuntimeError, match="rollback"):
        create_run_then_fail(database)
    with database.transaction() as session:
        assert session.scalar(select(func.count()).select_from(runs)) == 0
        assert session.scalar(select(func.count()).select_from(run_event_counters)) == 0
        assert session.scalar(select(func.count()).select_from(run_nodes)) == 0


def test_run_lifecycle_recovery_and_success_fingerprint(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    fingerprint = StateFingerprint("4" * 64)
    with database.transaction() as session:
        repository = SqlAlchemyRunRepository(session)
        running = repository.transition(
            RUN_ID,
            expected_row_version=1,
            target_state=RunState.RUNNING,
            transitioned_at=timestamp(2),
        )
        recovering = repository.mark_recovery_started(
            RUN_ID, expected_row_version=2, started_at=timestamp(3)
        )
        recovered = repository.mark_recovered(
            RUN_ID, expected_row_version=3, recovered_at=timestamp(4)
        )
        succeeded = repository.transition(
            RUN_ID,
            expected_row_version=4,
            target_state=RunState.SUCCEEDED,
            transitioned_at=timestamp(5),
            final_reconciliation_fingerprint=fingerprint,
        )
        assert running.started_at == timestamp(2)
        assert recovering.recovery_started_at == timestamp(3)
        assert recovered.recovered_at == timestamp(4)
        assert succeeded.finished_at == timestamp(5)
        assert succeeded.final_reconciliation_fingerprint == fingerprint
        with pytest.raises(ExecutionStateConflictError):
            repository.mark_recovery_started(
                RUN_ID, expected_row_version=5, started_at=timestamp(6)
            )


def test_run_cancel_timestamp_rules_and_cas(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    with database.transaction() as session:
        repository = SqlAlchemyRunRepository(session)
        cancelled = repository.transition(
            RUN_ID,
            expected_row_version=1,
            target_state=RunState.CANCELLED,
            transitioned_at=timestamp(2),
        )
        assert cancelled.started_at is None
        assert cancelled.finished_at == cancelled.cancellation_requested_at == timestamp(2)
        with pytest.raises(ExecutionStaleRowVersionError):
            repository.transition(
                RUN_ID,
                expected_row_version=1,
                target_state=RunState.CANCELLED,
                transitioned_at=timestamp(2),
            )


def test_work_create_claim_renew_complete_and_attempt_reads(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    with database.transaction() as session:
        repository = SqlAlchemyWorkItemRepository(session)
        created = repository.get(WORK_ID)
        assert created is not None
        assert created.state is WorkItemState.PENDING
        claim = repository.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="scheduler-main",
            started_at=timestamp(3),
            lease_expires_at=timestamp(8),
            runner_kind="threaded",
            worker_identity="worker-01",
        )
        assert claim.attempt_number == AttemptNumber(1)
        assert "scheduler-main" not in repr(claim)
        renewed = repository.renew_claim(
            claim, renewed_at=timestamp(4), lease_expires_at=timestamp(9)
        )
        completed = repository.complete_claim(
            renewed,
            WorkCompletion(
                target_state=WorkItemState.SUCCEEDED,
                finished_at=timestamp(5),
                retry_available_at=None,
                failure_classification=None,
                redacted_detail=None,
                result_reference=document(artifact_id="art_result"),
                records_processed=12,
                bytes_processed=2048,
            ),
        )
        assert completed.work_item.state is WorkItemState.SUCCEEDED
        assert completed.work_item.completed_attempt_count == 1
        assert completed.attempt.outcome is AttemptOutcome.SUCCEEDED
        assert completed.attempt.duration.microseconds == 2_000_000
        attempts = SqlAlchemyWorkAttemptRepository(session)
        assert attempts.get(WORK_ID, AttemptNumber(1)) == completed.attempt
        assert attempts.list_for_work_item(WORK_ID, limit=1).items == (completed.attempt,)


def test_work_retry_then_second_attempt_is_contiguous(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    with database.transaction() as session:
        repository = SqlAlchemyWorkItemRepository(session)
        first = repository.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="scheduler-main",
            started_at=timestamp(3),
            lease_expires_at=timestamp(5),
            runner_kind="threaded",
            worker_identity="worker-01",
        )
        retry = repository.complete_claim(
            first,
            WorkCompletion(
                target_state=WorkItemState.RETRY_WAIT,
                finished_at=timestamp(4),
                retry_available_at=timestamp(5),
                failure_classification=FailureClassification.HTTP_429,
                redacted_detail="Synthetic rate limit.",
                result_reference=None,
                records_processed=0,
                bytes_processed=0,
            ),
        )
        second = repository.claim(
            WORK_ID,
            expected_row_version=retry.work_item.row_version,
            lease_owner="scheduler-main",
            started_at=timestamp(5),
            lease_expires_at=timestamp(8),
            runner_kind="threaded",
            worker_identity="worker-02",
        )
        assert second.attempt_number == AttemptNumber(2)
        completed = repository.complete_claim(
            second,
            WorkCompletion(
                WorkItemState.SUCCEEDED,
                timestamp(6),
                None,
                None,
                None,
                None,
                1,
                10,
            ),
        )
        attempts = SqlAlchemyWorkAttemptRepository(session)
        first_page = attempts.list_for_work_item(WORK_ID, limit=1)
        assert first_page.next_cursor == AttemptNumber(1)
        second_page = attempts.list_for_work_item(WORK_ID, limit=1, after=first_page.next_cursor)
        assert second_page.items == (completed.attempt,)
        assert second_page.next_cursor is None


def test_expired_claim_recovery_is_not_owner_authorized(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    with database.transaction() as session:
        repository = SqlAlchemyWorkItemRepository(session)
        claim = repository.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="scheduler-main",
            started_at=timestamp(3),
            lease_expires_at=timestamp(5),
            runner_kind="threaded",
            worker_identity="worker-01",
        )
        with pytest.raises(ExecutionLeaseExpiredError):
            repository.complete_claim(
                claim,
                WorkCompletion(
                    WorkItemState.FAILED,
                    timestamp(5),
                    None,
                    FailureClassification.TIMEOUT,
                    None,
                    None,
                    0,
                    0,
                ),
            )
        recovered = repository.recover_expired_claim(
            WORK_ID,
            expected_row_version=claim.row_version,
            expected_attempt_number=claim.attempt_number,
            observed_at=timestamp(5),
            retry_available_at=timestamp(6),
            redacted_detail="Lease expired.",
        )
        assert recovered.work_item.state is WorkItemState.RETRY_WAIT
        assert recovered.attempt.outcome is AttemptOutcome.LEASE_EXPIRED
        assert recovered.attempt.failure_classification is FailureClassification.TIMEOUT


def test_claim_capability_and_parent_run_state_are_enforced(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    with database.transaction() as session:
        runs_repository = SqlAlchemyRunRepository(session)
        runs_repository.transition(
            RUN_ID,
            expected_row_version=2,
            target_state=RunState.PAUSING,
            transitioned_at=timestamp(3),
        )
        with pytest.raises(ExecutionStateConflictError, match="run state"):
            SqlAlchemyWorkItemRepository(session).claim(
                WORK_ID,
                expected_row_version=1,
                lease_owner="scheduler-main",
                started_at=timestamp(3),
                lease_expires_at=timestamp(5),
                runner_kind="threaded",
                worker_identity="worker-01",
            )


def test_work_create_is_atomic_with_checkpoint_head(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    with pytest.raises(RuntimeError, match="rollback"):
        create_work_then_fail(database)
    with database.transaction() as session:
        assert session.scalar(select(func.count()).select_from(work_items)) == 0
        assert session.scalar(select(func.count()).select_from(checkpoint_heads)) == 0
        assert session.scalar(select(func.count()).select_from(work_attempts)) == 0


def test_failed_completion_rolls_back_cas_winner_and_attempt(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    with pytest.raises(RuntimeError, match="rollback"):
        complete_work_then_fail(database)
    with database.transaction() as session:
        work = SqlAlchemyWorkItemRepository(session).get(WORK_ID)
        assert work is not None
        assert work.state is WorkItemState.PENDING
        assert (
            SqlAlchemyWorkAttemptRepository(session).list_for_work_item(WORK_ID, limit=10).items
            == ()
        )


def test_base_exception_after_completion_rolls_back_work_and_attempt(
    database: SQLiteDatabase,
) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    with pytest.raises(FatalTransactionProbe):
        complete_work_then_fatal(database)
    with database.transaction() as session:
        work = SqlAlchemyWorkItemRepository(session).get(WORK_ID)
        assert work is not None
        assert work.state is WorkItemState.PENDING
        assert work.row_version == 1
        assert (
            SqlAlchemyWorkAttemptRepository(session).list_for_work_item(WORK_ID, limit=10).items
            == ()
        )


@pytest.mark.parametrize(
    ("fragment", "failure_type"),
    [
        ("run_event_counters", RuntimeError),
        ("run_nodes", FatalTransactionProbe),
    ],
)
def test_run_creation_internal_failpoints_rollback_every_row(
    database: SQLiteDatabase,
    fragment: str,
    failure_type: type[BaseException],
) -> None:
    seed_pipeline(database)
    listener = sql_failure_listener(fragment, failure_type)
    event.listen(database.engine, "before_cursor_execute", listener)
    try:
        with pytest.raises(failure_type):
            create_run(database)
    finally:
        event.remove(database.engine, "before_cursor_execute", listener)
    with database.transaction() as session:
        assert session.scalar(select(func.count()).select_from(runs)) == 0
        assert session.scalar(select(func.count()).select_from(run_event_counters)) == 0
        assert session.scalar(select(func.count()).select_from(run_nodes)) == 0


def test_work_creation_internal_failpoint_rolls_back_item_and_head(
    database: SQLiteDatabase,
) -> None:
    seed_pipeline(database)
    create_run(database)
    listener = sql_failure_listener("checkpoint_heads", RuntimeError)
    event.listen(database.engine, "before_cursor_execute", listener)
    try:
        with pytest.raises(RuntimeError):
            create_work(database)
    finally:
        event.remove(database.engine, "before_cursor_execute", listener)
    with database.transaction() as session:
        assert session.scalar(select(func.count()).select_from(work_items)) == 0
        assert session.scalar(select(func.count()).select_from(checkpoint_heads)) == 0


@pytest.mark.parametrize(
    ("operation", "failure_type"),
    [
        ("completion", RuntimeError),
        ("recovery", FatalTransactionProbe),
    ],
)
def test_attempt_insert_internal_failpoints_rollback_the_winning_cas(
    database: SQLiteDatabase,
    operation: str,
    failure_type: type[BaseException],
) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    claim = claim_work(database)
    listener = sql_failure_listener("work_attempts", failure_type)
    event.listen(database.engine, "before_cursor_execute", listener)

    def invoke() -> None:
        with database.transaction() as session:
            repository = SqlAlchemyWorkItemRepository(session)
            if operation == "completion":
                repository.complete_claim(
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
            else:
                repository.recover_expired_claim(
                    WORK_ID,
                    expected_row_version=claim.row_version,
                    expected_attempt_number=claim.attempt_number,
                    observed_at=timestamp(6),
                    retry_available_at=timestamp(7),
                )

    try:
        with pytest.raises(failure_type):
            invoke()
    finally:
        event.remove(database.engine, "before_cursor_execute", listener)
    with database.transaction() as session:
        durable = SqlAlchemyWorkItemRepository(session).get(WORK_ID)
        assert durable is not None
        assert durable.state is WorkItemState.RUNNING
        assert durable.row_version == claim.row_version
        assert durable.completed_attempt_count == 0
        assert session.scalar(select(func.count()).select_from(work_attempts)) == 0


def test_completion_rejects_mismatched_claim_without_attempt_insert(
    database: SQLiteDatabase,
) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    with database.transaction() as session:
        repository = SqlAlchemyWorkItemRepository(session)
        claim = repository.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="scheduler-main",
            started_at=timestamp(3),
            lease_expires_at=timestamp(8),
            runner_kind="threaded",
            worker_identity="worker-01",
        )
        wrong = type(claim)(
            claim.work_item_id,
            claim.attempt_number,
            "different-owner",
            claim.row_version,
            claim.started_at,
            claim.lease_expires_at,
            claim.runner_kind,
            claim.worker_identity,
        )
        with pytest.raises(ExecutionLeaseMismatchError):
            repository.complete_claim(
                wrong,
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
        assert session.scalar(select(func.count()).select_from(work_attempts)) == 0


def test_repositories_require_caller_transaction(database: SQLiteDatabase) -> None:
    session = create_session_factory(database.engine)()
    try:
        with pytest.raises(ExecutionInvalidRequestError, match="caller-owned"):
            SqlAlchemyRunRepository(session).get(RUN_ID)
        with pytest.raises(ExecutionInvalidRequestError, match="caller-owned"):
            SqlAlchemyWorkItemRepository(session).get(WORK_ID)
        with pytest.raises(ExecutionInvalidRequestError, match="caller-owned"):
            SqlAlchemyWorkAttemptRepository(session).get(WORK_ID, AttemptNumber(1))
    finally:
        session.close()


def test_run_create_parent_archival_publication_time_and_input_guards(
    database: SQLiteDatabase,
) -> None:
    seed_pipeline(database)
    with database.transaction() as session:
        repository = SqlAlchemyRunRepository(session)
        with pytest.raises(ExecutionRecordNotFoundError):
            repository.create(
                run_id=RUN_ID,
                pipeline_id=PipelineId("pip_missing"),
                pipeline_version=PipelineVersion(1),
                runner_kind="threaded",
                runner_configuration=document(),
                scenario_seed=None,
                node_ids=(NODE_ID,),
                created_at=timestamp(1),
            )
        with pytest.raises(ExecutionInvalidRequestError, match="publication"):
            repository.create(
                run_id=RUN_ID,
                pipeline_id=PIPELINE_ID,
                pipeline_version=PipelineVersion(1),
                runner_kind="threaded",
                runner_configuration=document(),
                scenario_seed=None,
                node_ids=(NODE_ID,),
                created_at=UtcTimestamp.parse("2026-08-11T12:00:00Z"),
            )
        with pytest.raises(ExecutionInvalidRequestError, match="unique"):
            repository.create(
                run_id=RUN_ID,
                pipeline_id=PIPELINE_ID,
                pipeline_version=PipelineVersion(1),
                runner_kind="threaded",
                runner_configuration=document(),
                scenario_seed=None,
                node_ids=(NODE_ID, NODE_ID),
                created_at=timestamp(1),
            )
        SqlAlchemyPipelineRepository(session).archive(
            PIPELINE_ID, expected_row_version=1, archived_at=timestamp(2)
        )
        with pytest.raises(ExecutionStateConflictError, match="archived"):
            repository.create(
                run_id=RUN_ID,
                pipeline_id=PIPELINE_ID,
                pipeline_version=PipelineVersion(1),
                runner_kind="threaded",
                runner_configuration=document(),
                scenario_seed=None,
                node_ids=(NODE_ID,),
                created_at=timestamp(3),
            )


def test_run_lists_filters_missing_reads_and_empty_children(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database, RunId("run_alpha"))
    create_run(database, RunId("run_beta"))
    with database.transaction() as session:
        repository = SqlAlchemyRunRepository(session)
        first = repository.list(limit=1)
        assert first.items[0].run_id == RunId("run_alpha")
        assert first.next_cursor == RunId("run_alpha")
        assert repository.list(limit=1, after=first.next_cursor).items[0].run_id == (
            RunId("run_beta")
        )
        assert len(repository.list(limit=10, state=RunState.QUEUED).items) == 2
        with pytest.raises(ExecutionInvalidRequestError, match="state filter"):
            repository.list(limit=1, state="queued")  # type: ignore[arg-type]
        missing = RunId("run_missing")
        assert repository.get(missing) is None
        assert repository.get_event_counter(missing) is None
        assert repository.get_node(missing, NODE_ID) is None
        assert repository.list_nodes(missing, limit=1).items == ()


def test_run_pause_resume_cancel_flow_and_transition_validation(
    database: SQLiteDatabase,
) -> None:
    seed_pipeline(database)
    create_run(database)
    with database.transaction() as session:
        repository = SqlAlchemyRunRepository(session)
        with pytest.raises(ExecutionInvalidRequestError, match="target state"):
            repository.transition(
                RUN_ID,
                expected_row_version=1,
                target_state="running",  # type: ignore[arg-type]
                transitioned_at=timestamp(2),
            )
        running = repository.transition(
            RUN_ID,
            expected_row_version=1,
            target_state=RunState.RUNNING,
            transitioned_at=timestamp(2),
        )
        with pytest.raises(ExecutionInvalidRequestError, match="fingerprint"):
            repository.transition(
                RUN_ID,
                expected_row_version=2,
                target_state=RunState.SUCCEEDED,
                transitioned_at=timestamp(3),
            )
        pausing = repository.transition(
            RUN_ID,
            expected_row_version=2,
            target_state=RunState.PAUSING,
            transitioned_at=timestamp(3),
        )
        paused = repository.transition(
            RUN_ID,
            expected_row_version=3,
            target_state=RunState.PAUSED,
            transitioned_at=timestamp(4),
        )
        resuming = repository.transition(
            RUN_ID,
            expected_row_version=4,
            target_state=RunState.RESUMING,
            transitioned_at=timestamp(5),
        )
        resumed = repository.transition(
            RUN_ID,
            expected_row_version=5,
            target_state=RunState.RUNNING,
            transitioned_at=timestamp(6),
        )
        cancelling = repository.transition(
            RUN_ID,
            expected_row_version=6,
            target_state=RunState.CANCELLING,
            transitioned_at=timestamp(7),
        )
        cancelled = repository.transition(
            RUN_ID,
            expected_row_version=7,
            target_state=RunState.CANCELLED,
            transitioned_at=timestamp(8),
        )
        assert running.started_at == resumed.started_at == timestamp(2)
        assert pausing.state is RunState.PAUSING
        assert paused.state is RunState.PAUSED
        assert resuming.state is RunState.RESUMING
        assert cancelling.cancellation_requested_at == timestamp(7)
        assert cancelled.finished_at == timestamp(8)
        with pytest.raises(InvalidTransitionError):
            repository.transition(
                RUN_ID,
                expected_row_version=8,
                target_state=RunState.RUNNING,
                transitioned_at=timestamp(9),
            )


def test_run_recovery_rejects_unstarted_nonmonotonic_and_repeat(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    with database.transaction() as session:
        repository = SqlAlchemyRunRepository(session)
        with pytest.raises(ExecutionStateConflictError, match="not started"):
            repository.mark_recovered(RUN_ID, expected_row_version=2, recovered_at=timestamp(3))
        with pytest.raises(ExecutionInvalidRequestError, match="cannot precede"):
            repository.mark_recovery_started(
                RUN_ID, expected_row_version=2, started_at=timestamp(1)
            )
        started = repository.mark_recovery_started(
            RUN_ID, expected_row_version=2, started_at=timestamp(3)
        )
        with pytest.raises(ExecutionStateConflictError, match="already recorded"):
            repository.mark_recovery_started(
                RUN_ID, expected_row_version=started.row_version, started_at=timestamp(4)
            )
        with pytest.raises(ExecutionInvalidRequestError, match="cannot precede"):
            repository.mark_recovered(
                RUN_ID, expected_row_version=started.row_version, recovered_at=timestamp(2)
            )
        recovered = repository.mark_recovered(
            RUN_ID, expected_row_version=started.row_version, recovered_at=timestamp(4)
        )
        with pytest.raises(ExecutionStateConflictError, match="already completed"):
            repository.mark_recovered(
                RUN_ID, expected_row_version=recovered.row_version, recovered_at=timestamp(5)
            )


def test_work_create_conflicts_pagination_and_state_filters(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    with database.transaction() as session:
        repository = SqlAlchemyWorkItemRepository(session)
        with pytest.raises(ExecutionRecordNotFoundError):
            repository.create(
                work_item_id=WORK_ID,
                run_id=RUN_ID,
                node_id=NodeId("nod_missing"),
                partition_key=PartitionKey("page-0001"),
                input_reference=None,
                created_at=timestamp(2),
            )
        repository.create(
            work_item_id=WorkItemId("wrk_alpha"),
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PartitionKey("page-alpha"),
            input_reference=None,
            created_at=timestamp(2),
        )
        repository.create(
            work_item_id=WorkItemId("wrk_beta"),
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PartitionKey("page-beta"),
            input_reference=None,
            created_at=timestamp(2),
        )
        with pytest.raises(ExecutionDuplicateError):
            repository.create(
                work_item_id=WorkItemId("wrk_other"),
                run_id=RUN_ID,
                node_id=NODE_ID,
                partition_key=PartitionKey("page-alpha"),
                input_reference=None,
                created_at=timestamp(2),
            )
        first = repository.list_for_run(RUN_ID, limit=1)
        assert first.items[0].work_item_id == WorkItemId("wrk_alpha")
        assert first.next_cursor == WorkItemId("wrk_alpha")
        assert repository.list_for_run(
            RUN_ID, limit=1, after=first.next_cursor, state=WorkItemState.PENDING
        ).items[0].work_item_id == WorkItemId("wrk_beta")
        with pytest.raises(ExecutionInvalidRequestError, match="not durable"):
            repository.list_for_run(RUN_ID, limit=1, state=WorkItemState.LEASED)
        assert repository.get(WorkItemId("wrk_missing")) is None


def test_claim_and_renewal_input_and_state_conflicts(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    with database.transaction() as session:
        repository = SqlAlchemyWorkItemRepository(session)
        with pytest.raises(ExecutionInvalidRequestError, match="expiry"):
            repository.claim(
                WORK_ID,
                expected_row_version=1,
                lease_owner="owner",
                started_at=timestamp(3),
                lease_expires_at=timestamp(3),
                runner_kind="threaded",
                worker_identity="worker",
            )
        claim = repository.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="owner",
            started_at=timestamp(3),
            lease_expires_at=timestamp(8),
            runner_kind="threaded",
            worker_identity="worker",
        )
        with pytest.raises(ExecutionStaleRowVersionError):
            repository.claim(
                WORK_ID,
                expected_row_version=1,
                lease_owner="owner",
                started_at=timestamp(3),
                lease_expires_at=timestamp(8),
                runner_kind="threaded",
                worker_identity="worker",
            )
        with pytest.raises(ExecutionInvalidRequestError, match="advance"):
            repository.renew_claim(claim, renewed_at=timestamp(3), lease_expires_at=timestamp(9))
        with pytest.raises(ExecutionInvalidRequestError, match="extend"):
            repository.renew_claim(claim, renewed_at=timestamp(4), lease_expires_at=timestamp(8))


@pytest.mark.parametrize(
    ("target", "classification", "outcome"),
    [
        (WorkItemState.QUARANTINED, FailureClassification.VALIDATION, AttemptOutcome.QUARANTINED),
        (WorkItemState.FAILED, FailureClassification.HTTP_4XX, AttemptOutcome.FAILED),
        (
            WorkItemState.CANCELLED,
            FailureClassification.USER_CANCELLATION,
            AttemptOutcome.CANCELLED,
        ),
    ],
)
def test_non_success_completion_outcomes(
    database: SQLiteDatabase,
    target: WorkItemState,
    classification: FailureClassification,
    outcome: AttemptOutcome,
) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    with database.transaction() as session:
        repository = SqlAlchemyWorkItemRepository(session)
        claim = repository.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="owner",
            started_at=timestamp(3),
            lease_expires_at=timestamp(8),
            runner_kind="threaded",
            worker_identity="worker",
        )
        completed = repository.complete_claim(
            claim,
            WorkCompletion(target, timestamp(4), None, classification, "Redacted.", None, 0, 0),
        )
        assert completed.attempt.outcome is outcome


def test_completion_contract_guards(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    with database.transaction() as session:
        repository = SqlAlchemyWorkItemRepository(session)
        claim = repository.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="owner",
            started_at=timestamp(3),
            lease_expires_at=timestamp(8),
            runner_kind="threaded",
            worker_identity="worker",
        )
        invalid = (
            WorkCompletion(
                WorkItemState.SUCCEEDED,
                timestamp(4),
                None,
                FailureClassification.UNKNOWN,
                None,
                None,
                0,
                0,
            ),
            WorkCompletion(WorkItemState.FAILED, timestamp(4), None, None, None, None, 0, 0),
            WorkCompletion(
                WorkItemState.RETRY_WAIT,
                timestamp(4),
                None,
                FailureClassification.TIMEOUT,
                None,
                None,
                0,
                0,
            ),
            WorkCompletion(
                WorkItemState.FAILED,
                timestamp(4),
                timestamp(5),
                FailureClassification.UNKNOWN,
                None,
                None,
                0,
                0,
            ),
            WorkCompletion(
                WorkItemState.RETRY_WAIT,
                timestamp(4),
                timestamp(3),
                FailureClassification.TIMEOUT,
                None,
                None,
                0,
                0,
            ),
            WorkCompletion(
                WorkItemState.PENDING,
                timestamp(4),
                None,
                FailureClassification.UNKNOWN,
                None,
                None,
                0,
                0,
            ),
        )
        for completion in invalid:
            with pytest.raises(ExecutionInvalidRequestError):
                repository.complete_claim(claim, completion)


def test_expiry_recovery_contract_guards(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    with database.transaction() as session:
        repository = SqlAlchemyWorkItemRepository(session)
        with pytest.raises(ExecutionStateConflictError, match="no active"):
            repository.recover_expired_claim(
                WORK_ID,
                expected_row_version=1,
                expected_attempt_number=AttemptNumber(1),
                observed_at=timestamp(4),
                retry_available_at=timestamp(5),
            )
        claim = repository.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="owner",
            started_at=timestamp(3),
            lease_expires_at=timestamp(6),
            runner_kind="threaded",
            worker_identity="worker",
        )
        with pytest.raises(ExecutionLeaseMismatchError):
            repository.recover_expired_claim(
                WORK_ID,
                expected_row_version=claim.row_version,
                expected_attempt_number=AttemptNumber(2),
                observed_at=timestamp(6),
                retry_available_at=timestamp(7),
            )
        with pytest.raises(ExecutionStateConflictError, match="not expired"):
            repository.recover_expired_claim(
                WORK_ID,
                expected_row_version=claim.row_version,
                expected_attempt_number=claim.attempt_number,
                observed_at=timestamp(5),
                retry_available_at=timestamp(7),
            )
        with pytest.raises(ExecutionInvalidRequestError, match="retry availability"):
            repository.recover_expired_claim(
                WORK_ID,
                expected_row_version=claim.row_version,
                expected_attempt_number=claim.attempt_number,
                observed_at=timestamp(6),
                retry_available_at=timestamp(5),
            )


def test_attempt_repository_missing_parent_missing_number_and_pagination(
    database: SQLiteDatabase,
) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    with database.transaction() as session:
        attempts = SqlAlchemyWorkAttemptRepository(session)
        with pytest.raises(ExecutionRecordNotFoundError):
            attempts.get(WorkItemId("wrk_missing"), AttemptNumber(1))
        with pytest.raises(ExecutionRecordNotFoundError):
            attempts.list_for_work_item(WorkItemId("wrk_missing"), limit=1)
        assert attempts.get(WORK_ID, AttemptNumber(1)) is None


def test_run_non_success_rejects_fingerprint_and_transition_time_regression(
    database: SQLiteDatabase,
) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    with database.transaction() as session:
        repository = SqlAlchemyRunRepository(session)
        with pytest.raises(ExecutionInvalidRequestError, match="successful runs only"):
            repository.transition(
                RUN_ID,
                expected_row_version=2,
                target_state=RunState.FAILED,
                transitioned_at=timestamp(3),
                final_reconciliation_fingerprint=StateFingerprint("4" * 64),
            )
        with pytest.raises(ExecutionInvalidRequestError, match="monotonic"):
            repository.transition(
                RUN_ID,
                expected_row_version=2,
                target_state=RunState.FAILED,
                transitioned_at=timestamp(1),
            )


def test_run_counter_missing_and_orphan_are_corruption(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    with database.engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys = OFF")
        connection.exec_driver_sql('DROP TRIGGER "trg_run_event_counters_prohibit_delete"')
        connection.exec_driver_sql(
            "DELETE FROM run_event_counters WHERE run_id = ?", (str(RUN_ID),)
        )
        connection.commit()
    with database.transaction() as session:
        repository = SqlAlchemyRunRepository(session)
        with pytest.raises(ExecutionCorruptionError, match="counter"):
            repository.get(RUN_ID)
        with pytest.raises(ExecutionCorruptionError, match="counter"):
            repository.list(limit=10)
        with pytest.raises(ExecutionCorruptionError, match="counter"):
            repository.get_event_counter(RUN_ID)


def test_run_counter_orphan_is_corruption(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    with database.engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys = OFF")
        connection.exec_driver_sql('DROP TRIGGER "trg_runs_prohibit_delete"')
        connection.exec_driver_sql("DELETE FROM runs WHERE run_id = ?", (str(RUN_ID),))
        connection.commit()
    with database.transaction() as session, pytest.raises(ExecutionCorruptionError, match="parent"):
        SqlAlchemyRunRepository(session).get_event_counter(RUN_ID)


def test_work_list_uses_constant_integrity_queries(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    with database.transaction() as session:
        repository = SqlAlchemyWorkItemRepository(session)
        for suffix in ("alpha", "beta", "gamma"):
            repository.create(
                work_item_id=WorkItemId(f"wrk_{suffix}"),
                run_id=RUN_ID,
                node_id=NODE_ID,
                partition_key=PartitionKey(f"page-{suffix}"),
                input_reference=None,
                created_at=timestamp(2),
            )
    query_count = 0

    def count_query(*_arguments: object) -> None:
        nonlocal query_count
        query_count += 1

    event.listen(database.engine, "before_cursor_execute", count_query)
    try:
        with database.transaction() as session:
            assert (
                len(SqlAlchemyWorkItemRepository(session).list_for_run(RUN_ID, limit=10).items) == 3
            )
    finally:
        event.remove(database.engine, "before_cursor_execute", count_query)
    assert query_count == 3


def test_work_checkpoint_and_attempt_aggregate_corruption_is_rejected(
    database: SQLiteDatabase,
) -> None:
    seed_pipeline(database)
    create_run(database)
    create_work(database)
    with database.transaction() as session:
        session.execute(
            update(checkpoint_heads)
            .where(checkpoint_heads.c.run_id == str(RUN_ID))
            .values(current_version=1)
        )
    with (
        database.transaction() as session,
        pytest.raises(ExecutionCorruptionError, match="checkpoint version"),
    ):
        SqlAlchemyWorkItemRepository(session).get(WORK_ID)


def test_work_attempt_count_without_history_is_corruption(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    create_work(database)
    with database.transaction() as session:
        session.execute(
            update(work_items)
            .where(work_items.c.work_item_id == str(WORK_ID))
            .values(completed_attempt_count=1)
        )
    with (
        database.transaction() as session,
        pytest.raises(ExecutionCorruptionError, match="attempt history"),
    ):
        SqlAlchemyWorkAttemptRepository(session).list_for_work_item(WORK_ID, limit=10)


def test_work_helpers_reject_missing_claim_and_unbounded_duration(
    database: SQLiteDatabase,
) -> None:
    seed_pipeline(database)
    create_run(database)
    with database.transaction() as session:
        repository = SqlAlchemyWorkItemRepository(session)
        with pytest.raises(ExecutionRecordNotFoundError):
            repository.claim(
                WorkItemId("wrk_missing"),
                expected_row_version=1,
                lease_owner="owner",
                started_at=timestamp(3),
                lease_expires_at=timestamp(4),
                runner_kind="threaded",
                worker_identity="worker",
            )
        work_runtime._verify_work_records(session, ())  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(ExecutionCorruptionError, match="start time"):
        work_runtime._duration_between(None, timestamp(2))  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(ExecutionInvalidRequestError, match="duration"):
        work_runtime._duration_between(  # pyright: ignore[reportPrivateUsage]
            UtcTimestamp.parse("2025-01-01T00:00:00Z"),
            UtcTimestamp.parse("2026-08-12T00:00:00Z"),
        )


def test_terminal_run_cannot_record_recovery_completion(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    with database.transaction() as session:
        repository = SqlAlchemyRunRepository(session)
        failed = repository.transition(
            RUN_ID,
            expected_row_version=2,
            target_state=RunState.FAILED,
            transitioned_at=timestamp(3),
        )
        with pytest.raises(ExecutionStateConflictError, match="terminal"):
            repository.mark_recovered(
                RUN_ID,
                expected_row_version=failed.row_version,
                recovered_at=timestamp(4),
            )


def test_work_creation_enforces_parent_state_and_chronology(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    with database.transaction() as session:
        repository = SqlAlchemyWorkItemRepository(session)
        with pytest.raises(ExecutionInvalidRequestError, match="precede"):
            repository.create(
                work_item_id=WORK_ID,
                run_id=RUN_ID,
                node_id=NODE_ID,
                partition_key=PartitionKey("page-0001"),
                input_reference=None,
                created_at=timestamp(0),
            )
        runs_repository = SqlAlchemyRunRepository(session)
        runs_repository.transition(
            RUN_ID,
            expected_row_version=1,
            target_state=RunState.RUNNING,
            transitioned_at=timestamp(2),
        )
        runs_repository.transition(
            RUN_ID,
            expected_row_version=2,
            target_state=RunState.PAUSING,
            transitioned_at=timestamp(3),
        )
        with pytest.raises(ExecutionStateConflictError, match="creation"):
            repository.create(
                work_item_id=WORK_ID,
                run_id=RUN_ID,
                node_id=NODE_ID,
                partition_key=PartitionKey("page-0001"),
                input_reference=None,
                created_at=timestamp(4),
            )


def test_claim_enforces_claimable_state_retry_time_and_monotonic_start(
    database: SQLiteDatabase,
) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    with database.transaction() as session:
        repository = SqlAlchemyWorkItemRepository(session)
        claim = repository.claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="owner",
            started_at=timestamp(3),
            lease_expires_at=timestamp(6),
            runner_kind="threaded",
            worker_identity="worker",
        )
        with pytest.raises(ExecutionStateConflictError, match="not claimable"):
            repository.claim(
                WORK_ID,
                expected_row_version=claim.row_version,
                lease_owner="owner",
                started_at=timestamp(4),
                lease_expires_at=timestamp(7),
                runner_kind="threaded",
                worker_identity="worker",
            )
        retry = repository.complete_claim(
            claim,
            WorkCompletion(
                WorkItemState.RETRY_WAIT,
                timestamp(4),
                timestamp(6),
                FailureClassification.TIMEOUT,
                None,
                None,
                0,
                0,
            ),
        ).work_item
        with pytest.raises(ExecutionStateConflictError, match="not available"):
            repository.claim(
                WORK_ID,
                expected_row_version=retry.row_version,
                lease_owner="owner",
                started_at=timestamp(5),
                lease_expires_at=timestamp(8),
                runner_kind="threaded",
                worker_identity="worker",
            )

    second = WorkItemId("wrk_monotonic")
    with database.transaction() as session:
        repository = SqlAlchemyWorkItemRepository(session)
        repository.create(
            work_item_id=second,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PartitionKey("page-monotonic"),
            input_reference=None,
            created_at=timestamp(4),
        )
        with pytest.raises(ExecutionInvalidRequestError, match="not monotonic"):
            repository.claim(
                second,
                expected_row_version=1,
                lease_owner="owner",
                started_at=timestamp(3),
                lease_expires_at=timestamp(8),
                runner_kind="threaded",
                worker_identity="worker",
            )


def test_wal_snapshot_claim_race_has_one_winner_and_reopens(
    database: SQLiteDatabase,
) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    sessions = create_session_factory(database.engine)
    losing_session = sessions()
    try:
        losing_session.begin()
        losing_session.connection().exec_driver_sql("BEGIN")
        losing_repository = SqlAlchemyWorkItemRepository(losing_session)
        before = losing_repository.get(WORK_ID)
        assert before is not None
        assert before.state is WorkItemState.PENDING

        with database.transaction() as winning_session:
            winning_claim = SqlAlchemyWorkItemRepository(winning_session).claim(
                WORK_ID,
                expected_row_version=1,
                lease_owner="winner",
                started_at=timestamp(3),
                lease_expires_at=timestamp(8),
                runner_kind="threaded",
                worker_identity="worker-winner",
            )
        with pytest.raises(ExecutionStorageUnavailableError):
            losing_repository.claim(
                WORK_ID,
                expected_row_version=1,
                lease_owner="loser",
                started_at=timestamp(3),
                lease_expires_at=timestamp(8),
                runner_kind="threaded",
                worker_identity="worker-loser",
            )
        losing_session.rollback()
    finally:
        losing_session.close()

    with database.transaction() as session:
        durable = SqlAlchemyWorkItemRepository(session).get(WORK_ID)
        assert durable is not None
        assert durable.state is WorkItemState.RUNNING
        assert durable.row_version == winning_claim.row_version == 2
        assert durable.active_attempt_number == AttemptNumber(1)
        assert session.scalar(select(func.count()).select_from(work_attempts)) == 0

    path = Path(str(database.engine.url.database))
    database.close()
    reopened = SQLiteDatabase.open(SQLiteDatabaseConfig(path))
    try:
        with reopened.transaction() as session:
            durable = SqlAlchemyWorkItemRepository(session).get(WORK_ID)
            assert durable is not None
            assert durable.state is WorkItemState.RUNNING
            assert SqlAlchemyRunRepository(session).get(RUN_ID) is not None
        with reopened.engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA journal_mode").scalar_one() == "wal"
            assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
            assert connection.exec_driver_sql("PRAGMA quick_check").scalar_one() == "ok"
            assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
    finally:
        reopened.close()


def test_two_sessions_run_transition_and_claim_each_have_one_cas_winner(
    database: SQLiteDatabase,
) -> None:
    seed_pipeline(database)
    create_run(database)
    with database.transaction() as winning_session:
        running = SqlAlchemyRunRepository(winning_session).transition(
            RUN_ID,
            expected_row_version=1,
            target_state=RunState.RUNNING,
            transitioned_at=timestamp(2),
        )
    with (
        database.transaction() as losing_session,
        pytest.raises(ExecutionStaleRowVersionError),
    ):
        SqlAlchemyRunRepository(losing_session).transition(
            RUN_ID,
            expected_row_version=1,
            target_state=RunState.RUNNING,
            transitioned_at=timestamp(2),
        )
    assert running.row_version == 2

    create_work(database)
    with database.transaction() as winning_session:
        claim = SqlAlchemyWorkItemRepository(winning_session).claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="winner",
            started_at=timestamp(3),
            lease_expires_at=timestamp(6),
            runner_kind="threaded",
            worker_identity="worker-winner",
        )
    with (
        database.transaction() as losing_session,
        pytest.raises(ExecutionStaleRowVersionError),
    ):
        SqlAlchemyWorkItemRepository(losing_session).claim(
            WORK_ID,
            expected_row_version=1,
            lease_owner="loser",
            started_at=timestamp(3),
            lease_expires_at=timestamp(6),
            runner_kind="threaded",
            worker_identity="worker-loser",
        )
    with database.transaction() as session:
        durable = SqlAlchemyWorkItemRepository(session).get(WORK_ID)
        assert durable is not None
        assert durable.row_version == claim.row_version == 2
        assert durable.state is WorkItemState.RUNNING
        assert session.scalar(select(func.count()).select_from(work_attempts)) == 0


@pytest.mark.parametrize("winner", ["completion", "expiry"])
def test_completion_and_expiry_race_installs_exactly_one_attempt(
    database: SQLiteDatabase,
    winner: str,
) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    claim = claim_work(database)
    if winner == "completion":
        with database.transaction() as session:
            SqlAlchemyWorkItemRepository(session).complete_claim(
                claim,
                WorkCompletion(
                    WorkItemState.SUCCEEDED,
                    timestamp(4),
                    None,
                    None,
                    None,
                    None,
                    1,
                    1,
                ),
            )
        with (
            database.transaction() as session,
            pytest.raises(ExecutionStaleRowVersionError),
        ):
            SqlAlchemyWorkItemRepository(session).recover_expired_claim(
                WORK_ID,
                expected_row_version=claim.row_version,
                expected_attempt_number=claim.attempt_number,
                observed_at=timestamp(6),
                retry_available_at=timestamp(7),
            )
        expected_state = WorkItemState.SUCCEEDED
        expected_outcome = AttemptOutcome.SUCCEEDED
    else:
        with database.transaction() as session:
            SqlAlchemyWorkItemRepository(session).recover_expired_claim(
                WORK_ID,
                expected_row_version=claim.row_version,
                expected_attempt_number=claim.attempt_number,
                observed_at=timestamp(6),
                retry_available_at=timestamp(7),
            )
        with (
            database.transaction() as session,
            pytest.raises(ExecutionStaleRowVersionError),
        ):
            SqlAlchemyWorkItemRepository(session).complete_claim(
                claim,
                WorkCompletion(
                    WorkItemState.SUCCEEDED,
                    timestamp(4),
                    None,
                    None,
                    None,
                    None,
                    1,
                    1,
                ),
            )
        expected_state = WorkItemState.RETRY_WAIT
        expected_outcome = AttemptOutcome.LEASE_EXPIRED
    with database.transaction() as session:
        work = SqlAlchemyWorkItemRepository(session).get(WORK_ID)
        attempts = SqlAlchemyWorkAttemptRepository(session).list_for_work_item(WORK_ID, limit=10)
        assert work is not None
        assert work.state is expected_state
        assert work.completed_attempt_count == 1
        assert len(attempts.items) == 1
        assert attempts.items[0].outcome is expected_outcome


def test_two_expiry_recovery_sessions_install_one_attempt(database: SQLiteDatabase) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    claim = claim_work(database)
    with database.transaction() as winning_session:
        SqlAlchemyWorkItemRepository(winning_session).recover_expired_claim(
            WORK_ID,
            expected_row_version=claim.row_version,
            expected_attempt_number=claim.attempt_number,
            observed_at=timestamp(6),
            retry_available_at=timestamp(7),
        )
    with (
        database.transaction() as losing_session,
        pytest.raises(ExecutionStaleRowVersionError),
    ):
        SqlAlchemyWorkItemRepository(losing_session).recover_expired_claim(
            WORK_ID,
            expected_row_version=claim.row_version,
            expected_attempt_number=claim.attempt_number,
            observed_at=timestamp(6),
            retry_available_at=timestamp(7),
        )
    with database.transaction() as session:
        attempts = SqlAlchemyWorkAttemptRepository(session).list_for_work_item(WORK_ID, limit=10)
        assert len(attempts.items) == 1
        assert attempts.items[0].outcome is AttemptOutcome.LEASE_EXPIRED


def test_uncommitted_completion_is_invisible_then_atomic_after_commit(
    database: SQLiteDatabase,
) -> None:
    seed_pipeline(database)
    create_run(database)
    start_run(database)
    create_work(database)
    claim = claim_work(database)
    sessions = create_session_factory(database.engine)
    writer = sessions()
    try:
        writer.begin()
        SqlAlchemyWorkItemRepository(writer).complete_claim(
            claim,
            WorkCompletion(
                WorkItemState.SUCCEEDED,
                timestamp(4),
                None,
                None,
                None,
                None,
                1,
                1,
            ),
        )
        with database.transaction() as reader:
            before = SqlAlchemyWorkItemRepository(reader).get(WORK_ID)
            attempts_before = SqlAlchemyWorkAttemptRepository(reader).list_for_work_item(
                WORK_ID, limit=10
            )
            assert before is not None
            assert before.state is WorkItemState.RUNNING
            assert attempts_before.items == ()
        writer.commit()
    finally:
        writer.close()
    with database.transaction() as reader:
        after = SqlAlchemyWorkItemRepository(reader).get(WORK_ID)
        attempts_after = SqlAlchemyWorkAttemptRepository(reader).list_for_work_item(
            WORK_ID, limit=10
        )
        assert after is not None
        assert after.state is WorkItemState.SUCCEEDED
        assert len(attempts_after.items) == 1
