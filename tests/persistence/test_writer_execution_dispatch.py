"""Atomic integration tests for execution writer dispatch."""

# pyright: reportPrivateUsage=false

from collections.abc import Callable, Iterator
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence import (
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    create_session_factory,
)
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.repositories import SqlAlchemyPipelineRepository
from paritygrid.adapters.persistence.schema import (
    checkpoint_heads,
    checkpoints,
    execution_events,
    run_event_counters,
    run_nodes,
    runs,
    work_attempts,
    work_items,
)
from paritygrid.adapters.persistence.writer import dispatch as dispatch_runtime
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter
from paritygrid.adapters.persistence.writer.dispatch import dispatch_command, validate_command
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.execution import (
    ExecutionStateConflictError,
    RunNodeStatus,
    WorkClaim,
    WorkCompletion,
)
from paritygrid.application.ports.run_aggregates import WorkMetricDelta
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    WriterCommand,
    WriterInvalidRequestError,
    WriterSettings,
)
from paritygrid.application.writes.execution import (
    WORK_LEASE_EVENT_PAYLOAD_SCHEMA_VERSION,
    WORK_RESULT_EVENT_PAYLOAD_SCHEMA_VERSION,
    BootstrapWork,
    CheckpointWrite,
    ClaimWork,
    CommitWorkAttempt,
    CommitWorkWithCheckpoint,
    CreateCapturedRun,
    FinalizeEmptyRunNode,
    RecoverExpiredWork,
    RenewWorkClaim,
    TransitionRun,
)
from paritygrid.domain.execution import FailureClassification, RunState, WorkItemState
from paritygrid.domain.models import (
    ArtifactId,
    AttemptNumber,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

PIPELINE_ID = PipelineId("pip_writerdispatch")
RUN_ID = RunId("run_writerdispatch")
SUCCESS_NODE = NodeId("nod_success")
FAILURE_NODE = NodeId("nod_failure")
RECOVERY_NODE = NodeId("nod_recovery")
EMPTY_NODE = NodeId("nod_empty")
SUCCESS_WORK = WorkItemId("wrk_success")
FAILURE_WORK = WorkItemId("wrk_failure")
RECOVERY_WORK = WorkItemId("wrk_recovery")


class _DispatchFailure(BaseException):
    pass


@pytest.fixture
def database(tmp_path: Path) -> Iterator[SQLiteDatabase]:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "writer dispatch %.db"))
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


def seed_pipeline(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        repository = SqlAlchemyPipelineRepository(session)
        repository.create(
            pipeline_id=PIPELINE_ID,
            display_name="Writer dispatch pipeline",
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


def append_request(
    sequence: int,
    kind: str,
    subject_id: RunId | WorkItemId,
    occurred_at: UtcTimestamp,
) -> EventAppendRequest:
    subject_kind = EventSubjectKind.RUN if type(subject_id) is RunId else EventSubjectKind.WORK_ITEM
    return EventAppendRequest(
        EventSequence(sequence),
        sequence,
        PendingExecutionEvent(
            event_kind=kind,
            occurred_at=occurred_at,
            subject_kind=subject_kind,
            subject_id=subject_id,
            correlation_id="corr-writer-dispatch",
            payload_schema_version=1,
            payload=RedactedDocument.from_mapping({"kind": kind}),
        ),
    )


def claim_append_request(
    sequence: int,
    node_id: NodeId,
    work_item_id: WorkItemId,
    occurred_at: UtcTimestamp,
    attempt_number: AttemptNumber | None = None,
) -> EventAppendRequest:
    expected_attempt = AttemptNumber(1) if attempt_number is None else attempt_number
    return EventAppendRequest(
        EventSequence(sequence),
        sequence,
        PendingExecutionEvent(
            event_kind="work_claimed",
            occurred_at=occurred_at,
            subject_kind=EventSubjectKind.WORK_ITEM,
            subject_id=work_item_id,
            correlation_id="corr-writer-dispatch",
            payload_schema_version=WORK_LEASE_EVENT_PAYLOAD_SCHEMA_VERSION,
            payload=RedactedDocument.from_mapping(
                {
                    "attempt_number": int(expected_attempt),
                    "lease_expires_at": str(timestamp(int(occurred_at.to_datetime().second) + 3)),
                    "node_id": str(node_id),
                    "runner_kind": "threaded",
                }
            ),
        ),
    )


def renewal_append_request(
    sequence: int,
    node_id: NodeId,
    claim: WorkClaim,
    renewed_at: UtcTimestamp,
    lease_expires_at: UtcTimestamp,
) -> EventAppendRequest:
    return EventAppendRequest(
        EventSequence(sequence),
        sequence,
        PendingExecutionEvent(
            event_kind="work_claim_renewed",
            occurred_at=renewed_at,
            subject_kind=EventSubjectKind.WORK_ITEM,
            subject_id=claim.work_item_id,
            correlation_id="corr-writer-dispatch",
            payload_schema_version=WORK_LEASE_EVENT_PAYLOAD_SCHEMA_VERSION,
            payload=RedactedDocument.from_mapping(
                {
                    "attempt_number": int(claim.attempt_number),
                    "lease_expires_at": str(lease_expires_at),
                    "node_id": str(node_id),
                    "runner_kind": claim.runner_kind,
                }
            ),
        ),
    )


def completion_append_request(
    sequence: int,
    node_id: NodeId,
    claim: WorkClaim,
    target: WorkItemState,
    occurred_at: UtcTimestamp,
    *,
    failure_classification: FailureClassification | None = None,
    retry_available_at: UtcTimestamp | None = None,
    partition_key: PartitionKey | None = None,
    checkpoint_payload_schema_version: int | None = None,
    artifact_id: ArtifactId | None = None,
) -> EventAppendRequest:
    payload: dict[str, object] = {
        "attempt_number": int(claim.attempt_number),
        "failure_classification": (
            None if failure_classification is None else failure_classification.value
        ),
        "node_id": str(node_id),
        "retry_available_at": None if retry_available_at is None else str(retry_available_at),
        "runner_kind": claim.runner_kind,
        "target_state": target.value,
    }
    if target is WorkItemState.SUCCEEDED:
        assert partition_key is not None
        assert checkpoint_payload_schema_version is not None
        payload.update(
            {
                "artifact_id": None if artifact_id is None else str(artifact_id),
                "checkpoint_payload_schema_version": checkpoint_payload_schema_version,
                "partition_key": str(partition_key),
            }
        )
    return EventAppendRequest(
        EventSequence(sequence),
        sequence,
        PendingExecutionEvent(
            event_kind=(
                "checkpoint_committed"
                if target is WorkItemState.SUCCEEDED
                else f"work_{target.value}"
            ),
            occurred_at=occurred_at,
            subject_kind=EventSubjectKind.WORK_ITEM,
            subject_id=claim.work_item_id,
            correlation_id="corr-writer-dispatch",
            payload_schema_version=WORK_RESULT_EVENT_PAYLOAD_SCHEMA_VERSION,
            payload=RedactedDocument.from_mapping(payload),
        ),
    )


def create_run_command() -> CreateCapturedRun:
    return CreateCapturedRun(
        run_id=RUN_ID,
        pipeline_id=PIPELINE_ID,
        pipeline_version=PipelineVersion(1),
        runner_kind="threaded",
        runner_configuration=document(max_workers=4),
        scenario_seed=7,
        node_ids=(SUCCESS_NODE, FAILURE_NODE, RECOVERY_NODE, EMPTY_NODE),
        created_at=timestamp(1),
        event=append_request(1, "run_created", RUN_ID, timestamp(1)),
    )


def bootstrap_command(
    sequence: int,
    run_row_version: int,
    node_id: NodeId,
    work_item_id: WorkItemId,
    second: int,
) -> BootstrapWork:
    return BootstrapWork(
        run_id=RUN_ID,
        node_id=node_id,
        work_item_id=work_item_id,
        partition_key=PartitionKey(f"partition-{sequence}"),
        input_reference=document(sequence=sequence),
        created_at=timestamp(second),
        expected_node_row_version=1,
        expected_run_row_version=run_row_version,
        event=append_request(sequence, "work_created", work_item_id, timestamp(second)),
    )


def claim_command(
    sequence: int,
    run_row_version: int,
    node_id: NodeId,
    work_item_id: WorkItemId,
    second: int,
) -> ClaimWork:
    return ClaimWork(
        run_id=RUN_ID,
        node_id=node_id,
        work_item_id=work_item_id,
        expected_attempt_number=AttemptNumber(1),
        expected_work_row_version=1,
        expected_node_row_version=2,
        expected_run_row_version=run_row_version,
        lease_owner="scheduler-main",
        started_at=timestamp(second),
        lease_expires_at=timestamp(second + 3),
        runner_kind="threaded",
        worker_identity="worker-01",
        event=claim_append_request(sequence, node_id, work_item_id, timestamp(second)),
    )


def test_all_execution_composites_preserve_order_and_advance_revision_once(
    database: SQLiteDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_pipeline(database)
    order: list[str] = []
    original_event = cast(
        Callable[[Session, RunId, EventAppendRequest], object],
        vars(dispatch_runtime)["_append_event"],
    )
    original_advance = cast(
        Callable[[Session, RunId, int], object],
        vars(dispatch_runtime)["_advance_run"],
    )
    original_register = dispatch_runtime.SqlAlchemyRunNodeAggregateRepository.register_work
    original_claim = dispatch_runtime.SqlAlchemyRunNodeAggregateRepository.apply_claim
    original_completion = dispatch_runtime.SqlAlchemyRunNodeAggregateRepository.apply_completion
    original_recovery = dispatch_runtime.SqlAlchemyRunNodeAggregateRepository.apply_recovery
    original_empty = dispatch_runtime.SqlAlchemyRunNodeAggregateRepository.finalize_empty
    original_run_create = dispatch_runtime.SqlAlchemyRunRepository.create
    original_transition = dispatch_runtime.SqlAlchemyRunRepository.transition
    original_work_create = dispatch_runtime.SqlAlchemyWorkItemRepository.create
    original_work_claim = dispatch_runtime.SqlAlchemyWorkItemRepository.claim
    original_renew = dispatch_runtime.SqlAlchemyWorkItemRepository.renew_claim
    original_complete = dispatch_runtime.SqlAlchemyWorkItemRepository.complete_claim
    original_recover = dispatch_runtime.SqlAlchemyWorkItemRepository.recover_expired_claim
    original_checkpoint = dispatch_runtime.SqlAlchemyCheckpointRepository.append

    def event_spy(session: Session, run_id: RunId, request: EventAppendRequest) -> object:
        order.append("event")
        return original_event(session, run_id, request)

    def advance_spy(session: Session, run_id: RunId, expected: int) -> object:
        order.append("revision")
        return original_advance(session, run_id, expected)

    def register_spy(repository: object, *args: object, **kwargs: object) -> object:
        order.append("aggregate")
        return original_register(repository, *args, **kwargs)  # type: ignore[arg-type]

    def claim_spy(repository: object, *args: object, **kwargs: object) -> object:
        order.append("aggregate")
        return original_claim(repository, *args, **kwargs)  # type: ignore[arg-type]

    def completion_spy(repository: object, *args: object, **kwargs: object) -> object:
        order.append("aggregate")
        return original_completion(repository, *args, **kwargs)  # type: ignore[arg-type]

    def recovery_spy(repository: object, *args: object, **kwargs: object) -> object:
        order.append("aggregate")
        return original_recovery(repository, *args, **kwargs)  # type: ignore[arg-type]

    def empty_spy(repository: object, *args: object, **kwargs: object) -> object:
        order.append("aggregate")
        return original_empty(repository, *args, **kwargs)  # type: ignore[arg-type]

    def run_create_spy(repository: object, *args: object, **kwargs: object) -> object:
        order.append("run")
        return original_run_create(repository, *args, **kwargs)  # type: ignore[arg-type]

    def transition_spy(repository: object, *args: object, **kwargs: object) -> object:
        order.append("run")
        return original_transition(repository, *args, **kwargs)  # type: ignore[arg-type]

    def work_create_spy(repository: object, *args: object, **kwargs: object) -> object:
        order.append("work")
        return original_work_create(repository, *args, **kwargs)  # type: ignore[arg-type]

    def work_claim_spy(repository: object, *args: object, **kwargs: object) -> object:
        order.append("work")
        return original_work_claim(repository, *args, **kwargs)  # type: ignore[arg-type]

    def renew_spy(repository: object, *args: object, **kwargs: object) -> object:
        order.append("work")
        return original_renew(repository, *args, **kwargs)  # type: ignore[arg-type]

    def complete_spy(repository: object, *args: object, **kwargs: object) -> object:
        order.append("work")
        return original_complete(repository, *args, **kwargs)  # type: ignore[arg-type]

    def recover_spy(repository: object, *args: object, **kwargs: object) -> object:
        order.append("work")
        return original_recover(repository, *args, **kwargs)  # type: ignore[arg-type]

    def checkpoint_spy(repository: object, *args: object, **kwargs: object) -> object:
        order.append("checkpoint")
        return original_checkpoint(repository, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(dispatch_runtime, "_append_event", event_spy)
    monkeypatch.setattr(dispatch_runtime, "_advance_run", advance_spy)
    monkeypatch.setattr(
        dispatch_runtime.SqlAlchemyRunNodeAggregateRepository, "register_work", register_spy
    )
    monkeypatch.setattr(
        dispatch_runtime.SqlAlchemyRunNodeAggregateRepository, "apply_claim", claim_spy
    )
    monkeypatch.setattr(
        dispatch_runtime.SqlAlchemyRunNodeAggregateRepository,
        "apply_completion",
        completion_spy,
    )
    monkeypatch.setattr(
        dispatch_runtime.SqlAlchemyRunNodeAggregateRepository, "apply_recovery", recovery_spy
    )
    monkeypatch.setattr(
        dispatch_runtime.SqlAlchemyRunNodeAggregateRepository, "finalize_empty", empty_spy
    )
    monkeypatch.setattr(dispatch_runtime.SqlAlchemyRunRepository, "create", run_create_spy)
    monkeypatch.setattr(dispatch_runtime.SqlAlchemyRunRepository, "transition", transition_spy)
    monkeypatch.setattr(dispatch_runtime.SqlAlchemyWorkItemRepository, "create", work_create_spy)
    monkeypatch.setattr(dispatch_runtime.SqlAlchemyWorkItemRepository, "claim", work_claim_spy)
    monkeypatch.setattr(dispatch_runtime.SqlAlchemyWorkItemRepository, "renew_claim", renew_spy)
    monkeypatch.setattr(
        dispatch_runtime.SqlAlchemyWorkItemRepository, "complete_claim", complete_spy
    )
    monkeypatch.setattr(
        dispatch_runtime.SqlAlchemyWorkItemRepository, "recover_expired_claim", recover_spy
    )
    monkeypatch.setattr(dispatch_runtime.SqlAlchemyCheckpointRepository, "append", checkpoint_spy)

    with database.transaction() as session:
        created = dispatch_command(session, create_run_command())
    assert created.result.run.row_version == 1  # type: ignore[attr-defined]
    assert order == ["run", "event"]
    order.clear()

    with database.transaction() as session:
        transitioned = dispatch_command(
            session,
            TransitionRun(
                RUN_ID,
                1,
                RunState.RUNNING,
                timestamp(2),
                None,
                append_request(2, "run_started", RUN_ID, timestamp(2)),
            ),
        )
    assert transitioned.result.run.row_version == 2  # type: ignore[attr-defined]
    assert order == ["run", "event"]
    order.clear()

    with database.transaction() as session:
        dispatch_command(session, bootstrap_command(3, 2, SUCCESS_NODE, SUCCESS_WORK, 2))
    assert order == ["work", "event", "aggregate", "revision"]
    order.clear()

    with database.transaction() as session:
        claimed = dispatch_command(session, claim_command(4, 3, SUCCESS_NODE, SUCCESS_WORK, 3))
    success_claim = cast(WorkClaim, claimed.result.claim)  # type: ignore[attr-defined]
    assert order == ["work", "event", "aggregate", "revision"]
    order.clear()

    with database.transaction() as session:
        renewed = dispatch_command(
            session,
            RenewWorkClaim(
                RUN_ID,
                SUCCESS_NODE,
                success_claim,
                4,
                timestamp(4),
                timestamp(8),
                renewal_append_request(5, SUCCESS_NODE, success_claim, timestamp(4), timestamp(8)),
            ),
        )
    success_claim = cast(WorkClaim, renewed.result.claim)  # type: ignore[attr-defined]
    assert order == ["work", "event", "revision"]
    order.clear()

    with database.transaction() as session:
        succeeded = dispatch_command(
            session,
            CommitWorkWithCheckpoint(
                RUN_ID,
                SUCCESS_NODE,
                success_claim,
                WorkCompletion(
                    WorkItemState.SUCCEEDED,
                    timestamp(5),
                    None,
                    None,
                    None,
                    document(output="ok"),
                    10,
                    100,
                ),
                CheckpointWrite(
                    PartitionKey("partition-3"),
                    1,
                    document(offset=1),
                    document(rows=10),
                    None,
                    timestamp(5),
                ),
                WorkMetricDelta(10, 10, 0, 100, 80),
                3,
                5,
                completion_append_request(
                    6,
                    SUCCESS_NODE,
                    success_claim,
                    WorkItemState.SUCCEEDED,
                    timestamp(5),
                    partition_key=PartitionKey("partition-3"),
                    checkpoint_payload_schema_version=1,
                ),
            ),
        )
    assert succeeded.result.node.status is RunNodeStatus.SUCCEEDED  # type: ignore[attr-defined]
    assert order == ["work", "checkpoint", "event", "aggregate", "revision"]
    order.clear()

    with database.transaction() as session:
        dispatch_command(session, bootstrap_command(7, 6, FAILURE_NODE, FAILURE_WORK, 6))
        failed_claimed = dispatch_command(
            session, claim_command(8, 7, FAILURE_NODE, FAILURE_WORK, 7)
        )
        failed = dispatch_command(
            session,
            CommitWorkAttempt(
                RUN_ID,
                FAILURE_NODE,
                cast(WorkClaim, failed_claimed.result.claim),  # type: ignore[attr-defined]
                WorkCompletion(
                    WorkItemState.FAILED,
                    timestamp(8),
                    None,
                    FailureClassification.UNKNOWN,
                    "permanent failure",
                    None,
                    2,
                    20,
                ),
                WorkMetricDelta(2, 0, 0, 20, 0),
                3,
                8,
                completion_append_request(
                    9,
                    FAILURE_NODE,
                    cast(WorkClaim, failed_claimed.result.claim),  # type: ignore[attr-defined]
                    WorkItemState.FAILED,
                    timestamp(8),
                    failure_classification=FailureClassification.UNKNOWN,
                ),
            ),
        )
    assert failed.result.node.status is RunNodeStatus.FAILED  # type: ignore[attr-defined]
    assert order == [
        "work",
        "event",
        "aggregate",
        "revision",
        "work",
        "event",
        "aggregate",
        "revision",
        "work",
        "event",
        "aggregate",
        "revision",
    ]
    order.clear()

    with database.transaction() as session:
        dispatch_command(session, bootstrap_command(10, 9, RECOVERY_NODE, RECOVERY_WORK, 9))
        recovery_claimed = dispatch_command(
            session, claim_command(11, 10, RECOVERY_NODE, RECOVERY_WORK, 10)
        )
    assert cast(WorkClaim, recovery_claimed.result.claim).attempt_number == AttemptNumber(1)  # type: ignore[attr-defined]
    with database.transaction() as session:
        recovered = dispatch_command(
            session,
            RecoverExpiredWork(
                RUN_ID,
                RECOVERY_NODE,
                RECOVERY_WORK,
                2,
                AttemptNumber(1),
                timestamp(13),
                timestamp(14),
                "lease expired",
                3,
                11,
                append_request(12, "work_lease_expired", RECOVERY_WORK, timestamp(13)),
            ),
        )
    assert recovered.result.node.status is RunNodeStatus.RUNNING  # type: ignore[attr-defined]
    assert order[-4:] == ["work", "event", "aggregate", "revision"]
    order.clear()

    with database.transaction() as session:
        finalized = dispatch_command(
            session,
            FinalizeEmptyRunNode(
                RUN_ID,
                EMPTY_NODE,
                1,
                12,
                timestamp(14),
                append_request(13, "run_node_succeeded", RUN_ID, timestamp(14)),
            ),
        )
    assert finalized.result.node.status is RunNodeStatus.SUCCEEDED  # type: ignore[attr-defined]
    assert order == ["event", "aggregate", "revision"]
    with database.transaction() as session:
        assert session.scalar(select(runs.c.row_version).where(runs.c.run_id == str(RUN_ID))) == 13
        counter = (
            session.execute(
                select(run_event_counters).where(run_event_counters.c.run_id == str(RUN_ID))
            )
            .mappings()
            .one()
        )
        assert (counter["next_sequence_number"], counter["row_version"]) == (14, 14)
        assert session.scalar(select(func.count()).select_from(execution_events)) == 13


def test_work_commands_reject_cross_parent_hybrids_before_companions(
    database: SQLiteDatabase,
) -> None:
    seed_pipeline(database)
    with database.transaction() as session:
        dispatch_command(session, create_run_command())
        dispatch_command(
            session,
            TransitionRun(
                RUN_ID,
                1,
                RunState.RUNNING,
                timestamp(2),
                None,
                append_request(2, "run_started", RUN_ID, timestamp(2)),
            ),
        )
        dispatch_command(session, bootstrap_command(3, 2, SUCCESS_NODE, SUCCESS_WORK, 2))

    valid_claim = claim_command(4, 3, SUCCESS_NODE, SUCCESS_WORK, 3)
    wrong_claims = (
        replace(
            valid_claim,
            node_id=FAILURE_NODE,
            event=claim_append_request(4, FAILURE_NODE, SUCCESS_WORK, timestamp(3)),
        ),
        replace(valid_claim, run_id=RunId("run_writerdispatch-other")),
    )
    for wrong in wrong_claims:
        with (
            pytest.raises(WriterInvalidRequestError, match="another run or node"),
            database.transaction() as session,
        ):
            dispatch_command(session, wrong)
    with database.transaction() as session:
        row = (
            session.execute(
                select(work_items).where(work_items.c.work_item_id == str(SUCCESS_WORK))
            )
            .mappings()
            .one()
        )
        assert (row["state"], row["row_version"]) == (WorkItemState.PENDING.value, 1)
        assert session.scalar(select(func.count()).select_from(execution_events)) == 3
        claimed = dispatch_command(session, claim_command(4, 3, SUCCESS_NODE, SUCCESS_WORK, 3))
    claim = cast(WorkClaim, claimed.result.claim)  # type: ignore[attr-defined]

    renewal = RenewWorkClaim(
        RUN_ID,
        FAILURE_NODE,
        claim,
        4,
        timestamp(4),
        timestamp(8),
        renewal_append_request(5, FAILURE_NODE, claim, timestamp(4), timestamp(8)),
    )
    with (
        pytest.raises(WriterInvalidRequestError, match="another run or node"),
        database.transaction() as session,
    ):
        dispatch_command(session, renewal)
    with database.transaction() as session:
        row = (
            session.execute(
                select(work_items).where(work_items.c.work_item_id == str(SUCCESS_WORK))
            )
            .mappings()
            .one()
        )
        assert (row["row_version"], row["lease_expires_at"]) == (2, str(timestamp(6)))
        renewed = dispatch_command(
            session,
            replace(
                renewal,
                node_id=SUCCESS_NODE,
                event=renewal_append_request(5, SUCCESS_NODE, claim, timestamp(4), timestamp(8)),
            ),
        )
    claim = cast(WorkClaim, renewed.result.claim)  # type: ignore[attr-defined]

    completion = CommitWorkWithCheckpoint(
        RUN_ID,
        FAILURE_NODE,
        claim,
        WorkCompletion(
            WorkItemState.SUCCEEDED,
            timestamp(5),
            None,
            None,
            None,
            document(output="ok"),
            1,
            10,
        ),
        CheckpointWrite(PartitionKey("partition-3"), 1, None, None, None, timestamp(5)),
        WorkMetricDelta(),
        3,
        5,
        completion_append_request(
            6,
            FAILURE_NODE,
            claim,
            WorkItemState.SUCCEEDED,
            timestamp(5),
            partition_key=PartitionKey("partition-3"),
            checkpoint_payload_schema_version=1,
        ),
    )
    with (
        pytest.raises(WriterInvalidRequestError, match="another run or node"),
        database.transaction() as session,
    ):
        dispatch_command(session, completion)
    with database.transaction() as session:
        row = (
            session.execute(
                select(work_items).where(work_items.c.work_item_id == str(SUCCESS_WORK))
            )
            .mappings()
            .one()
        )
        assert (row["state"], row["row_version"], row["completed_attempt_count"]) == (
            WorkItemState.RUNNING.value,
            3,
            0,
        )
        assert session.scalar(select(func.count()).select_from(work_attempts)) == 0
        assert session.scalar(select(func.count()).select_from(checkpoints)) == 0
        dispatch_command(
            session,
            replace(
                completion,
                node_id=SUCCESS_NODE,
                event=completion_append_request(
                    6,
                    SUCCESS_NODE,
                    claim,
                    WorkItemState.SUCCEEDED,
                    timestamp(5),
                    partition_key=PartitionKey("partition-3"),
                    checkpoint_payload_schema_version=1,
                ),
            ),
        )

    with database.transaction() as session:
        dispatch_command(session, bootstrap_command(7, 6, RECOVERY_NODE, RECOVERY_WORK, 6))
        recovered_claim = dispatch_command(
            session, claim_command(8, 7, RECOVERY_NODE, RECOVERY_WORK, 7)
        )
    recovery = RecoverExpiredWork(
        RUN_ID,
        FAILURE_NODE,
        RECOVERY_WORK,
        2,
        AttemptNumber(1),
        timestamp(11),
        timestamp(12),
        None,
        3,
        8,
        append_request(9, "work_lease_expired", RECOVERY_WORK, timestamp(11)),
    )
    assert cast(WorkClaim, recovered_claim.result.claim).lease_expires_at == timestamp(10)  # type: ignore[attr-defined]
    with (
        pytest.raises(WriterInvalidRequestError, match="another run or node"),
        database.transaction() as session,
    ):
        dispatch_command(session, recovery)
    with database.transaction() as session:
        row = (
            session.execute(
                select(work_items).where(work_items.c.work_item_id == str(RECOVERY_WORK))
            )
            .mappings()
            .one()
        )
        assert (row["state"], row["row_version"], row["completed_attempt_count"]) == (
            WorkItemState.RUNNING.value,
            2,
            0,
        )
        dispatch_command(session, replace(recovery, node_id=RECOVERY_NODE))

    with database.transaction() as session:
        dispatch_command(session, bootstrap_command(10, 9, FAILURE_NODE, FAILURE_WORK, 12))
        failed_claim = dispatch_command(
            session, claim_command(11, 10, FAILURE_NODE, FAILURE_WORK, 13)
        )
    attempt = CommitWorkAttempt(
        RUN_ID,
        SUCCESS_NODE,
        cast(WorkClaim, failed_claim.result.claim),  # type: ignore[attr-defined]
        WorkCompletion(
            WorkItemState.FAILED,
            timestamp(14),
            None,
            FailureClassification.UNKNOWN,
            "failed safely",
            None,
            0,
            0,
        ),
        WorkMetricDelta(),
        3,
        11,
        completion_append_request(
            12,
            SUCCESS_NODE,
            cast(WorkClaim, failed_claim.result.claim),  # type: ignore[attr-defined]
            WorkItemState.FAILED,
            timestamp(14),
            failure_classification=FailureClassification.UNKNOWN,
        ),
    )
    with (
        pytest.raises(WriterInvalidRequestError, match="another run or node"),
        database.transaction() as session,
    ):
        dispatch_command(session, attempt)
    with database.transaction() as session:
        row = (
            session.execute(
                select(work_items).where(work_items.c.work_item_id == str(FAILURE_WORK))
            )
            .mappings()
            .one()
        )
        assert (row["state"], row["row_version"], row["completed_attempt_count"]) == (
            WorkItemState.RUNNING.value,
            2,
            0,
        )
        dispatch_command(
            session,
            replace(
                attempt,
                node_id=FAILURE_NODE,
                event=completion_append_request(
                    12,
                    FAILURE_NODE,
                    cast(WorkClaim, failed_claim.result.claim),  # type: ignore[attr-defined]
                    WorkItemState.FAILED,
                    timestamp(14),
                    failure_classification=FailureClassification.UNKNOWN,
                ),
            ),
        )


def test_writer_rejects_bootstrap_after_empty_terminal_and_continues(
    database: SQLiteDatabase,
) -> None:
    seed_pipeline(database)
    writer = SQLiteTransactionalWriter(
        create_session_factory(database.engine),
        WriterSettings(contention_delay_seconds=0.0),
    )
    writer.start()
    try:
        writer.submit(create_run_command(), timeout_seconds=1.0).result(timeout_seconds=2.0)
        writer.submit(
            TransitionRun(
                RUN_ID,
                1,
                RunState.RUNNING,
                timestamp(2),
                None,
                append_request(2, "run_started", RUN_ID, timestamp(2)),
            ),
            timeout_seconds=1.0,
        ).result(timeout_seconds=2.0)
        writer.submit(
            FinalizeEmptyRunNode(
                RUN_ID,
                EMPTY_NODE,
                1,
                2,
                timestamp(3),
                append_request(3, "run_node_succeeded", RUN_ID, timestamp(3)),
            ),
            timeout_seconds=1.0,
        ).result(timeout_seconds=2.0)
        rejected = writer.submit(
            replace(
                bootstrap_command(4, 3, EMPTY_NODE, WorkItemId("wrk_terminal-node"), 3),
                expected_node_row_version=2,
            ),
            timeout_seconds=1.0,
        )
        accepted = writer.submit(
            bootstrap_command(4, 3, SUCCESS_NODE, SUCCESS_WORK, 3),
            timeout_seconds=1.0,
        )
        with pytest.raises(ExecutionStateConflictError, match="terminal"):
            rejected.result(timeout_seconds=2.0)
        assert accepted.result(timeout_seconds=2.0).mutated
    finally:
        assert writer.close(timeout_seconds=2.0).drained
    with database.transaction() as session:
        assert (
            session.scalar(
                select(func.count())
                .select_from(work_items)
                .where(work_items.c.node_id == str(EMPTY_NODE))
            )
            == 0
        )
        assert session.scalar(select(func.count()).select_from(execution_events)) == 4


def test_parent_helpers_reject_missing_or_invalid_work_records(
    database: SQLiteDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    claim = WorkClaim(
        SUCCESS_WORK,
        AttemptNumber(1),
        "owner",
        1,
        timestamp(1),
        timestamp(2),
        "threaded",
        "worker",
    )

    def missing(_repository: object, _work_id: WorkItemId) -> None:
        return None

    monkeypatch.setattr(dispatch_runtime.SqlAlchemyWorkItemRepository, "get", missing)
    with (
        pytest.raises(WriterInvalidRequestError, match="does not exist"),
        database.transaction() as session,
    ):
        dispatch_runtime._require_claim_parent(session, claim, RUN_ID, SUCCESS_NODE)
    with pytest.raises(WriterInvalidRequestError, match="invalid record"):
        dispatch_runtime._require_work_parent(object(), RUN_ID, SUCCESS_NODE)


def test_composite_failure_rolls_back_every_prior_mutation(
    database: SQLiteDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_pipeline(database)
    with database.transaction() as session:
        dispatch_command(session, create_run_command())
        dispatch_command(
            session,
            TransitionRun(
                RUN_ID,
                1,
                RunState.RUNNING,
                timestamp(2),
                None,
                append_request(2, "run_started", RUN_ID, timestamp(2)),
            ),
        )

    def fail_revision(_session: Session, _run_id: RunId, _expected: int) -> object:
        raise _DispatchFailure

    monkeypatch.setattr(dispatch_runtime, "_advance_run", fail_revision)
    with pytest.raises(_DispatchFailure), database.transaction() as session:
        dispatch_command(session, bootstrap_command(3, 2, SUCCESS_NODE, SUCCESS_WORK, 2))
    with database.transaction() as session:
        assert session.scalar(select(func.count()).select_from(work_items)) == 0
        assert session.scalar(select(func.count()).select_from(checkpoint_heads)) == 0
        assert session.scalar(select(func.count()).select_from(execution_events)) == 2
        node = (
            session.execute(
                select(run_nodes).where(
                    run_nodes.c.run_id == str(RUN_ID),
                    run_nodes.c.node_id == str(SUCCESS_NODE),
                )
            )
            .mappings()
            .one()
        )
        assert (node["row_version"], node["work_total"]) == (1, 0)
        assert session.scalar(select(runs.c.row_version).where(runs.c.run_id == str(RUN_ID))) == 2


def test_checkpoint_completion_failure_after_each_seam_is_atomic(
    database: SQLiteDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_pipeline(database)
    with database.transaction() as session:
        dispatch_command(session, create_run_command())
        dispatch_command(
            session,
            TransitionRun(
                RUN_ID,
                1,
                RunState.RUNNING,
                timestamp(2),
                None,
                append_request(2, "run_started", RUN_ID, timestamp(2)),
            ),
        )
        dispatch_command(session, bootstrap_command(3, 2, SUCCESS_NODE, SUCCESS_WORK, 2))
        claimed = dispatch_command(session, claim_command(4, 3, SUCCESS_NODE, SUCCESS_WORK, 3))
    claim = cast(WorkClaim, claimed.result.claim)  # type: ignore[attr-defined]
    command_value = CommitWorkWithCheckpoint(
        RUN_ID,
        SUCCESS_NODE,
        claim,
        WorkCompletion(
            WorkItemState.SUCCEEDED,
            timestamp(4),
            None,
            None,
            None,
            document(output="ok"),
            1,
            2,
        ),
        CheckpointWrite(
            PartitionKey("partition-3"),
            1,
            document(offset=1),
            None,
            None,
            timestamp(4),
        ),
        WorkMetricDelta(1, 1, 0, 2, 2),
        3,
        4,
        completion_append_request(
            5,
            SUCCESS_NODE,
            claim,
            WorkItemState.SUCCEEDED,
            timestamp(4),
            partition_key=PartitionKey("partition-3"),
            checkpoint_payload_schema_version=1,
        ),
    )
    original_event = cast(
        Callable[[Session, RunId, EventAppendRequest], object],
        vars(dispatch_runtime)["_append_event"],
    )

    def fail_event(_session: Session, _run_id: RunId, _request: EventAppendRequest) -> object:
        raise RuntimeError("event failpoint")

    monkeypatch.setattr(dispatch_runtime, "_append_event", fail_event)
    with pytest.raises(RuntimeError, match="event failpoint"), database.transaction() as session:
        dispatch_command(session, command_value)
    monkeypatch.setattr(dispatch_runtime, "_append_event", original_event)
    with database.transaction() as session:
        row = (
            session.execute(
                select(work_items).where(work_items.c.work_item_id == str(SUCCESS_WORK))
            )
            .mappings()
            .one()
        )
        assert (row["state"], row["row_version"], row["expected_checkpoint_version"]) == (
            "running",
            2,
            0,
        )
        assert session.scalar(select(func.count()).select_from(checkpoints)) == 0
        head = session.execute(select(checkpoint_heads)).mappings().one()
        assert (head["current_version"], head["row_version"]) == (0, 1)
        assert session.scalar(select(func.count()).select_from(work_attempts)) == 0


def test_checkpoint_partition_mismatch_rolls_back_every_completion_fact(
    database: SQLiteDatabase,
) -> None:
    seed_pipeline(database)
    with database.transaction() as session:
        dispatch_command(session, create_run_command())
        dispatch_command(
            session,
            TransitionRun(
                RUN_ID,
                1,
                RunState.RUNNING,
                timestamp(2),
                None,
                append_request(2, "run_started", RUN_ID, timestamp(2)),
            ),
        )
        dispatch_command(session, bootstrap_command(3, 2, SUCCESS_NODE, SUCCESS_WORK, 2))
        claimed = dispatch_command(session, claim_command(4, 3, SUCCESS_NODE, SUCCESS_WORK, 3))
    claim = cast(WorkClaim, claimed.result.claim)  # type: ignore[attr-defined]
    foreign = PartitionKey("partition-foreign")
    command = CommitWorkWithCheckpoint(
        RUN_ID,
        SUCCESS_NODE,
        claim,
        WorkCompletion(
            WorkItemState.SUCCEEDED,
            timestamp(4),
            None,
            None,
            None,
            None,
            1,
            2,
        ),
        CheckpointWrite(foreign, 1, None, None, None, timestamp(4)),
        WorkMetricDelta(),
        3,
        4,
        completion_append_request(
            5,
            SUCCESS_NODE,
            claim,
            WorkItemState.SUCCEEDED,
            timestamp(4),
            partition_key=foreign,
            checkpoint_payload_schema_version=1,
        ),
    )
    with (
        pytest.raises(ExecutionStateConflictError, match="partition"),
        database.transaction() as session,
    ):
        dispatch_command(session, command)
    with database.transaction() as session:
        work = (
            session.execute(
                select(work_items).where(work_items.c.work_item_id == str(SUCCESS_WORK))
            )
            .mappings()
            .one()
        )
        assert (work["state"], work["row_version"], work["completed_attempt_count"]) == (
            WorkItemState.RUNNING.value,
            2,
            0,
        )
        assert session.scalar(select(func.count()).select_from(work_attempts)) == 0
        assert session.scalar(select(func.count()).select_from(checkpoints)) == 0
        assert session.scalar(select(func.count()).select_from(execution_events)) == 4
        assert session.scalar(select(runs.c.row_version).where(runs.c.run_id == str(RUN_ID))) == 4


def test_result_event_validation_is_closed_and_command_derived() -> None:
    claim = WorkClaim(
        SUCCESS_WORK,
        AttemptNumber(1),
        "scheduler",
        2,
        timestamp(3),
        timestamp(8),
        "threaded",
        "worker",
    )
    retry_completion = WorkCompletion(
        WorkItemState.RETRY_WAIT,
        timestamp(4),
        timestamp(5),
        FailureClassification.CONNECTION,
        None,
        None,
        0,
        0,
    )
    retry = CommitWorkAttempt(
        RUN_ID,
        SUCCESS_NODE,
        claim,
        retry_completion,
        WorkMetricDelta(),
        1,
        1,
        completion_append_request(
            2,
            SUCCESS_NODE,
            claim,
            WorkItemState.RETRY_WAIT,
            timestamp(4),
            failure_classification=FailureClassification.CONNECTION,
            retry_available_at=timestamp(5),
        ),
    )
    assert validate_command(retry) is retry
    with pytest.raises(WriterInvalidRequestError, match="retry availability"):
        validate_command(
            replace(retry, completion=replace(retry_completion, retry_available_at=None))
        )
    failure = replace(
        retry_completion,
        target_state=WorkItemState.FAILED,
        retry_available_at=timestamp(5),
    )
    with pytest.raises(WriterInvalidRequestError, match="only retry"):
        validate_command(replace(retry, completion=failure))
    success_completion = WorkCompletion(
        WorkItemState.SUCCEEDED,
        timestamp(4),
        None,
        None,
        None,
        None,
        0,
        0,
    )
    with pytest.raises(WriterInvalidRequestError, match="requires a checkpoint"):
        validate_command(replace(retry, completion=success_completion))

    artifact_id = ArtifactId("art_dispatch-result")
    checkpointed = CommitWorkWithCheckpoint(
        RUN_ID,
        SUCCESS_NODE,
        claim,
        success_completion,
        CheckpointWrite(
            PartitionKey("partition-3"),
            1,
            None,
            None,
            artifact_id,
            timestamp(4),
        ),
        WorkMetricDelta(),
        1,
        1,
        completion_append_request(
            2,
            SUCCESS_NODE,
            claim,
            WorkItemState.SUCCEEDED,
            timestamp(4),
            partition_key=PartitionKey("partition-3"),
            checkpoint_payload_schema_version=1,
            artifact_id=artifact_id,
        ),
    )
    assert validate_command(checkpointed) is checkpointed
    wrong_time = replace(
        checkpointed.event,
        event=replace(checkpointed.event.event, occurred_at=timestamp(5)),
    )
    with pytest.raises(WriterInvalidRequestError, match="event time"):
        validate_command(replace(checkpointed, event=wrong_time))
    wrong_schema = replace(
        checkpointed.event,
        event=replace(checkpointed.event.event, payload_schema_version=1),
    )
    with pytest.raises(WriterInvalidRequestError, match="event schema"):
        validate_command(replace(checkpointed, event=wrong_schema))
    wrong_payload = replace(
        checkpointed.event,
        event=replace(
            checkpointed.event.event,
            payload=RedactedDocument.from_mapping({"target_state": "succeeded"}),
        ),
    )
    with pytest.raises(WriterInvalidRequestError, match="event payload"):
        validate_command(replace(checkpointed, event=wrong_payload))


def test_command_validation_rejects_mismatched_events_before_sql(database: SQLiteDatabase) -> None:
    valid = create_run_command()
    assert validate_command(valid) is valid
    invalid_values: tuple[WriterCommand, ...] = (
        cast(WriterCommand, object()),
        CreateCapturedRun(
            valid.run_id,
            valid.pipeline_id,
            valid.pipeline_version,
            valid.runner_kind,
            valid.runner_configuration,
            valid.scenario_seed,
            valid.node_ids,
            valid.created_at,
            append_request(2, "run_created", RUN_ID, timestamp(1)),
        ),
        FinalizeEmptyRunNode(
            RUN_ID,
            EMPTY_NODE,
            1,
            1,
            timestamp(2),
            append_request(1, "run_node_succeeded", SUCCESS_WORK, timestamp(2)),
        ),
    )
    with database.transaction() as session:
        for invalid in invalid_values:
            with pytest.raises(WriterInvalidRequestError):
                dispatch_command(session, invalid)
        assert session.scalar(select(func.count()).select_from(runs)) == 0


def test_execution_validation_error_matrix_is_fail_fast() -> None:
    created = create_run_command()
    invalid_create = (
        replace(created, run_id=cast(RunId, object())),
        replace(created, runner_kind=""),
        replace(created, scenario_seed=cast(int, True)),
        replace(created, node_ids=()),
        replace(
            created,
            event=replace(created.event, expected_counter_row_version=2),
        ),
    )
    for invalid in invalid_create:
        with pytest.raises(WriterInvalidRequestError):
            validate_command(invalid)

    transition = TransitionRun(
        RUN_ID,
        1,
        cast(RunState, RunState.QUEUED),
        timestamp(2),
        None,
        append_request(2, "run_started", RUN_ID, timestamp(2)),
    )
    with pytest.raises(WriterInvalidRequestError, match="target"):
        validate_command(transition)

    claim = WorkClaim(
        SUCCESS_WORK,
        AttemptNumber(1),
        "scheduler",
        2,
        timestamp(3),
        timestamp(8),
        "threaded",
        "worker",
    )
    failure = WorkCompletion(
        WorkItemState.FAILED,
        timestamp(4),
        None,
        FailureClassification.UNKNOWN,
        None,
        None,
        0,
        0,
    )
    base_attempt = CommitWorkAttempt(
        RUN_ID,
        SUCCESS_NODE,
        claim,
        failure,
        WorkMetricDelta(),
        1,
        1,
        append_request(2, "work_failed", SUCCESS_WORK, timestamp(4)),
    )
    invalid_attempts = (
        replace(base_attempt, completion=cast(WorkCompletion, object())),
        replace(base_attempt, metrics=cast(WorkMetricDelta, object())),
        replace(
            base_attempt,
            completion=replace(failure, target_state=WorkItemState.SUCCEEDED),
        ),
    )
    for invalid in invalid_attempts:
        with pytest.raises(WriterInvalidRequestError):
            validate_command(invalid)

    checkpointed = CommitWorkWithCheckpoint(
        RUN_ID,
        SUCCESS_NODE,
        claim,
        replace(failure, target_state=WorkItemState.SUCCEEDED, failure_classification=None),
        CheckpointWrite(PartitionKey("partition-3"), 1, None, None, None, timestamp(4)),
        WorkMetricDelta(),
        1,
        1,
        append_request(2, "checkpoint_committed", SUCCESS_WORK, timestamp(4)),
    )
    with pytest.raises(WriterInvalidRequestError, match="must succeed"):
        validate_command(replace(checkpointed, completion=failure))
    with pytest.raises(WriterInvalidRequestError, match="checkpoint write"):
        validate_command(replace(checkpointed, checkpoint=cast(CheckpointWrite, object())))

    bootstrap = bootstrap_command(2, 1, SUCCESS_NODE, SUCCESS_WORK, 2)
    run_subject = append_request(2, "work_created", RUN_ID, timestamp(2))
    with pytest.raises(WriterInvalidRequestError, match="work subject"):
        validate_command(replace(bootstrap, event=run_subject))
    with pytest.raises(WriterInvalidRequestError, match="event append request"):
        validate_command(replace(bootstrap, event=cast(EventAppendRequest, object())))
    with pytest.raises(WriterInvalidRequestError, match="event sequence"):
        validate_command(
            replace(
                bootstrap,
                event=replace(bootstrap.event, expected_next_sequence=cast(EventSequence, 2)),
            )
        )
    with pytest.raises(WriterInvalidRequestError, match="pending event"):
        validate_command(
            replace(
                bootstrap,
                event=replace(bootstrap.event, event=cast(PendingExecutionEvent, object())),
            )
        )
    with pytest.raises(WriterInvalidRequestError, match="event kind"):
        validate_command(
            replace(
                bootstrap,
                event=append_request(2, "work_claimed", SUCCESS_WORK, timestamp(2)),
            )
        )
    with pytest.raises(WriterInvalidRequestError, match="supported range"):
        validate_command(replace(bootstrap, expected_node_row_version=0))

    recovery = RecoverExpiredWork(
        RUN_ID,
        RECOVERY_NODE,
        RECOVERY_WORK,
        2,
        AttemptNumber(1),
        timestamp(5),
        timestamp(6),
        "bounded detail",
        2,
        2,
        append_request(2, "work_lease_expired", RECOVERY_WORK, timestamp(5)),
    )
    assert validate_command(recovery) is recovery
    validated_recovery = cast(
        RecoverExpiredWork, validate_command(replace(recovery, redacted_detail=None))
    )
    assert validated_recovery.redacted_detail is None


def test_claim_and_renewal_events_require_exact_command_derived_evidence() -> None:
    claim = claim_command(4, 3, SUCCESS_NODE, SUCCESS_WORK, 3)
    assert validate_command(claim) is claim
    claim_payload = claim.event.event.payload.to_mapping()
    invalid_claims = (
        (
            replace(claim, expected_attempt_number=cast(AttemptNumber, 1)),
            "attempt number",
        ),
        (
            replace(
                claim,
                event=replace(
                    claim.event,
                    event=replace(claim.event.event, occurred_at=timestamp(4)),
                ),
            ),
            "event time",
        ),
        (
            replace(
                claim,
                event=replace(
                    claim.event,
                    event=replace(claim.event.event, payload_schema_version=1),
                ),
            ),
            "event schema",
        ),
        (
            replace(
                claim,
                event=replace(
                    claim.event,
                    event=replace(claim.event.event, payload=cast(RedactedDocument, object())),
                ),
            ),
            "event payload",
        ),
        (
            replace(
                claim,
                event=replace(
                    claim.event,
                    event=replace(
                        claim.event.event,
                        payload=RedactedDocument.from_mapping(
                            {**claim_payload, "attempt_number": 2}
                        ),
                    ),
                ),
            ),
            "payload is inconsistent",
        ),
    )
    for invalid, message in invalid_claims:
        with pytest.raises(WriterInvalidRequestError, match=message):
            validate_command(invalid)

    active_claim = WorkClaim(
        SUCCESS_WORK,
        AttemptNumber(1),
        "scheduler",
        2,
        timestamp(3),
        timestamp(6),
        "threaded",
        "worker",
    )
    renewal = RenewWorkClaim(
        RUN_ID,
        SUCCESS_NODE,
        active_claim,
        4,
        timestamp(4),
        timestamp(8),
        renewal_append_request(5, SUCCESS_NODE, active_claim, timestamp(4), timestamp(8)),
    )
    assert validate_command(renewal) is renewal
    renewal_payload = renewal.event.event.payload.to_mapping()
    invalid_renewals = (
        replace(
            renewal,
            event=replace(
                renewal.event,
                event=replace(renewal.event.event, occurred_at=timestamp(5)),
            ),
        ),
        replace(
            renewal,
            event=replace(
                renewal.event,
                event=replace(renewal.event.event, payload_schema_version=1),
            ),
        ),
        replace(
            renewal,
            event=replace(
                renewal.event,
                event=replace(
                    renewal.event.event,
                    payload=RedactedDocument.from_mapping(
                        {**renewal_payload, "lease_expires_at": str(timestamp(9))}
                    ),
                ),
            ),
        ),
    )
    for invalid in invalid_renewals:
        with pytest.raises(WriterInvalidRequestError, match="lease event"):
            validate_command(invalid)


def test_claim_attempt_mismatch_rolls_back_before_event_or_aggregate_commit(
    database: SQLiteDatabase,
) -> None:
    seed_pipeline(database)
    with database.transaction() as session:
        dispatch_command(session, create_run_command())
        dispatch_command(
            session,
            TransitionRun(
                RUN_ID,
                1,
                RunState.RUNNING,
                timestamp(2),
                None,
                append_request(2, "run_started", RUN_ID, timestamp(2)),
            ),
        )
        dispatch_command(session, bootstrap_command(3, 2, SUCCESS_NODE, SUCCESS_WORK, 2))

    mismatched = replace(
        claim_command(4, 3, SUCCESS_NODE, SUCCESS_WORK, 3),
        expected_attempt_number=AttemptNumber(2),
        event=claim_append_request(
            4,
            SUCCESS_NODE,
            SUCCESS_WORK,
            timestamp(3),
            AttemptNumber(2),
        ),
    )
    with (
        pytest.raises(WriterInvalidRequestError, match="attempt number"),
        database.transaction() as session,
    ):
        dispatch_command(session, mismatched)

    with database.transaction() as session:
        row = (
            session.execute(
                select(work_items).where(work_items.c.work_item_id == str(SUCCESS_WORK))
            )
            .mappings()
            .one()
        )
        assert (row["state"], row["row_version"]) == (WorkItemState.PENDING.value, 1)
        assert session.scalar(select(func.count()).select_from(execution_events)) == 3


def test_transactional_writer_commits_to_wal_and_reopens_with_integrity(
    tmp_path: Path,
) -> None:
    path = tmp_path / "writer WAL reopen %.db"
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(path))
    try:
        with database.engine.connect() as connection:
            upgrade_to_head(connection)
        seed_pipeline(database)
        writer = SQLiteTransactionalWriter(
            create_session_factory(database.engine),
            WriterSettings(
                queue_capacity=2,
                notification_capacity=2,
                max_contention_attempts=2,
                contention_delay_seconds=0.0,
                thread_name="paritygrid-wal-writer",
            ),
        )
        writer.start()
        created = writer.submit(create_run_command(), timeout_seconds=1.0).result(
            timeout_seconds=2.0
        )
        transitioned = writer.submit(
            TransitionRun(
                RUN_ID,
                1,
                RunState.RUNNING,
                timestamp(2),
                None,
                append_request(2, "run_started", RUN_ID, timestamp(2)),
            ),
            timeout_seconds=1.0,
        ).result(timeout_seconds=2.0)
        assert created.submission_id.number == 1
        assert transitioned.submission_id.number == 2
        assert writer.close(timeout_seconds=2.0).drained
        with database.engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA journal_mode").scalar_one() == "wal"
            assert connection.exec_driver_sql("PRAGMA quick_check").scalar_one() == "ok"
            assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
    finally:
        database.close()

    reopened = SQLiteDatabase.open(SQLiteDatabaseConfig(path))
    try:
        with reopened.transaction() as session:
            run = session.execute(select(runs).where(runs.c.run_id == str(RUN_ID))).mappings().one()
            assert (run["state"], run["row_version"]) == ("running", 2)
            assert session.scalar(select(func.count()).select_from(execution_events)) == 2
        with reopened.engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA quick_check").scalar_one() == "ok"
            assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
    finally:
        reopened.close()
