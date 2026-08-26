"""Production result-commit factory compilation and real-writer proof."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from paritygrid.adapters.persistence import (
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    SQLiteResultCoordinatorReader,
    create_session_factory,
)
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyPipelineRepository,
    SqlAlchemyWorkItemRepository,
)
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter
from paritygrid.application.execution import DurableResultCommitFactory
from paritygrid.application.execution.capacity import ScheduledWorkLimiters
from paritygrid.application.execution.channels import CHANNEL_KIND_RESULT, BoundedChannel
from paritygrid.application.execution.clock_policy import ManualClock
from paritygrid.application.execution.concurrent_scheduler import (
    ConcurrentScheduler,
    WorkIdentity,
)
from paritygrid.application.execution.result_coordinator import (
    CommitIntent,
    ConcurrentResultCoordinator,
    RegisteredAssignment,
    ResultValidationRejection,
)
from paritygrid.application.execution.result_coordinator_writer import (
    TransactionalResultCoordinatorWriter,
)
from paritygrid.application.execution.runner_contract import (
    RUNNER_CONTRACT_VERSION,
    WORK_RESULT_PROTOCOL,
    ContractCleanupEvidence,
    ContractCleanupStatus,
    ContractMetric,
    ContractOutcome,
    ControlGeneration,
    WorkResultV1,
)
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.execution import WorkItemState
from paritygrid.application.ports.writer import EventAppendRequest, WriterSettings
from paritygrid.application.writes.execution import (
    WORK_LEASE_EVENT_PAYLOAD_SCHEMA_VERSION,
    BootstrapWork,
    ClaimWork,
    CreateCapturedRun,
    TransitionRun,
)
from paritygrid.domain.execution import RunState
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

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
PIPELINE_ID = PipelineId("pip_factory")
RUN_ID = RunId("run_factory")
NODE = NodeId("nod_extract")
WORK = WorkItemId("wrk_extract-0")
PARTITION = "partition-0"


def timestamp(second: int) -> UtcTimestamp:
    return UtcTimestamp(_EPOCH + timedelta(seconds=second))


def micros(value: UtcTimestamp) -> int:
    delta = value.to_datetime() - _EPOCH
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


@pytest.fixture
def database(tmp_path: Path) -> Iterator[SQLiteDatabase]:
    config = SQLiteDatabaseConfig(tmp_path / "factory.db", create_parent=True)
    database = SQLiteDatabase.open(config)
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    yield database
    database.close()


def _carrier(
    sequence: int, counter: int, subject: RunId | WorkItemId, kind: str, at: UtcTimestamp
) -> EventAppendRequest:
    subject_kind = EventSubjectKind.RUN if type(subject) is RunId else EventSubjectKind.WORK_ITEM
    return EventAppendRequest(
        EventSequence(sequence),
        counter,
        PendingExecutionEvent(
            kind,
            at,
            subject_kind,
            subject,
            None,
            1,
            RedactedDocument.from_mapping({"kind": kind}),
        ),
    )


def _seed(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        repository = SqlAlchemyPipelineRepository(session)
        repository.create(
            pipeline_id=PIPELINE_ID,
            display_name="Factory pipeline",
            description=None,
            created_at=timestamp(0),
        )
        repository.publish_version(
            pipeline_id=PIPELINE_ID,
            expected_latest_version=None,
            specification=ConfigurationDocument.from_mapping({"nodes": []}),
            planner_format_version=1,
            published_at=timestamp(0),
        )


def _claim_through_writer(database: SQLiteDatabase, writer: SQLiteTransactionalWriter) -> int:
    """Drive one work item to a claimed RUNNING state; return its lease fence."""
    _seed(database)
    writer.submit(
        CreateCapturedRun(
            run_id=RUN_ID,
            pipeline_id=PIPELINE_ID,
            pipeline_version=PipelineVersion(1),
            runner_kind="threaded",
            runner_configuration=ConfigurationDocument.from_mapping({"workers": 2}),
            scenario_seed=3,
            node_ids=(NODE,),
            created_at=timestamp(1),
            event=_carrier(1, 1, RUN_ID, "run_created", timestamp(1)),
        ),
        timeout_seconds=5.0,
    ).result(timeout_seconds=5.0)
    writer.submit(
        TransitionRun(
            run_id=RUN_ID,
            expected_run_row_version=1,
            target_state=RunState.RUNNING,
            transitioned_at=timestamp(2),
            execution_evidence_fingerprint=None,
            execution_evidence_fingerprint_version=None,
            event=_carrier(2, 2, RUN_ID, "run_started", timestamp(2)),
        ),
        timeout_seconds=5.0,
    ).result(timeout_seconds=5.0)
    writer.submit(
        BootstrapWork(
            run_id=RUN_ID,
            node_id=NODE,
            work_item_id=WORK,
            partition_key=PartitionKey(PARTITION),
            input_reference=None,
            created_at=timestamp(3),
            expected_node_row_version=1,
            expected_run_row_version=2,
            event=_carrier(3, 3, WORK, "work_created", timestamp(3)),
        ),
        timeout_seconds=5.0,
    ).result(timeout_seconds=5.0)
    claimed = writer.submit(
        ClaimWork(
            run_id=RUN_ID,
            node_id=NODE,
            work_item_id=WORK,
            expected_attempt_number=AttemptNumber(1),
            expected_work_row_version=1,
            expected_node_row_version=2,
            expected_run_row_version=3,
            lease_owner="engine-main",
            started_at=timestamp(4),
            lease_expires_at=timestamp(64),
            runner_kind="threaded",
            worker_identity="worker-01",
            event=_lease_event(4, 4),
        ),
        timeout_seconds=5.0,
    ).result(timeout_seconds=5.0)
    claim = claimed.result.claim  # type: ignore[attr-defined]
    return int(claim.row_version)  # type: ignore[attr-defined]


def _lease_event(sequence: int, counter: int) -> EventAppendRequest:
    return EventAppendRequest(
        EventSequence(sequence),
        counter,
        PendingExecutionEvent(
            "work_claimed",
            timestamp(4),
            EventSubjectKind.WORK_ITEM,
            WORK,
            None,
            WORK_LEASE_EVENT_PAYLOAD_SCHEMA_VERSION,
            RedactedDocument.from_mapping(
                {
                    "attempt_number": 1,
                    "lease_expires_at": str(timestamp(64)),
                    "node_id": str(NODE),
                    "runner_kind": "threaded",
                }
            ),
        ),
    )


def _result_envelope(
    outcome: ContractOutcome,
    *,
    attempt: int = 1,
    lease_fence: int = 2,
) -> WorkResultV1:
    return WorkResultV1(
        protocol=WORK_RESULT_PROTOCOL,
        contract_version=RUNNER_CONTRACT_VERSION,
        plan_fingerprint="a" * 64,
        run_id=str(RUN_ID),
        node_id=str(NODE),
        partition_key=PARTITION,
        work_item_id=str(WORK),
        attempt_number=attempt,
        lease_fence=lease_fence,
        lease_owner="engine-main",
        control_generation=ControlGeneration(1),
        outcome=outcome,
        metrics=(
            ContractMetric("records_read", 3),
            ContractMetric("records_written", 3),
            ContractMetric("bytes_read", 30),
            ContractMetric("bytes_written", 30),
        ),
        artifact_references=(),
        checkpoint_proposal=outcome is ContractOutcome.SUCCEEDED,
        failure_detail=None if outcome is ContractOutcome.SUCCEEDED else "scripted",
        cleanup=ContractCleanupEvidence(
            ContractCleanupStatus.COMPLETED,
            (),
            "cleanup-factory-test",
        ),
    )


def _intent_for(
    reader: SQLiteResultCoordinatorReader,
    result: WorkResultV1,
    *,
    lease_fence: int,
    classification: str | None,
    retry_eligible_at_micros: int = 0,
) -> CommitIntent:
    frontier = reader.rebase(str(RUN_ID), str(NODE), PARTITION, str(WORK))
    return CommitIntent(
        run_id=str(RUN_ID),
        plan_fingerprint="a" * 64,
        node_id=str(NODE),
        partition_key=PARTITION,
        work_item_id=str(WORK),
        attempt_number=frontier.attempt_number,
        lease_fence=frontier.lease_fence,
        lease_owner=frontier.lease_owner,
        observed_at_micros=frontier.observed_at_micros,
        lease_expires_at_micros=frontier.expires_at_micros,
        outcome=result.outcome.value,
        expected_run_row_version=frontier.run_row_version,
        expected_node_row_version=frontier.node_row_version,
        next_event_sequence=frontier.next_event_sequence,
        event_counter_row_version=frontier.event_counter_row_version,
        checkpoint_proposed=result.checkpoint_proposal,
        artifact_ids=result.artifact_references,
        result=result,
        runner_kind=frontier.runner_kind,
        worker_identity=frontier.worker_identity,
        started_at_micros=frontier.started_at_micros,
        retry_eligible_at_micros=retry_eligible_at_micros,
        failure_classification=classification,
    )


def _coordinator(
    writer: SQLiteTransactionalWriter,
    database: SQLiteDatabase,
    reader: SQLiteResultCoordinatorReader,
    scheduler: ConcurrentScheduler,
    capacity: ScheduledWorkLimiters,
    channel: BoundedChannel,
) -> ConcurrentResultCoordinator:
    adapter = TransactionalResultCoordinatorWriter(
        writer,
        DurableResultCommitFactory(correlation_id="corr-factory"),
    )
    return ConcurrentResultCoordinator(
        run_id=str(RUN_ID),
        plan_fingerprint="a" * 64,
        control_generation=1,
        reader=reader,
        writer=adapter,
        result_channel=channel,
        scheduler=scheduler,
        capacity=capacity,
    )


def test_successful_intent_commits_through_real_writer(database: SQLiteDatabase) -> None:
    """The production factory bridges one intent to a durable commit."""
    writer = SQLiteTransactionalWriter(create_session_factory(database.engine), WriterSettings())
    writer.start()
    try:
        lease_fence = _claim_through_writer(database, writer)
        reader = SQLiteResultCoordinatorReader(database, ManualClock(timestamp(5)))
        result = _result_envelope(ContractOutcome.SUCCEEDED, lease_fence=lease_fence)
        _intent_for(reader, result, lease_fence=lease_fence, classification=None)
        scheduler = ConcurrentScheduler(
            run_id=str(RUN_ID),
            plan_fingerprint="a" * 64,
            node_order=(str(NODE),),
            edges=(),
            partitions_by_node={str(NODE): (PARTITION,)},
            control_generation=ControlGeneration(1),
        )
        identity = WorkIdentity(str(RUN_ID), str(NODE), PARTITION)
        scheduler.register_admission(identity, lease_fence)
        from paritygrid.application.execution.concurrency_settings import (
            CapturedConcurrencySettings,
        )

        capacity = ScheduledWorkLimiters(
            CapturedConcurrencySettings(),
            strategy_id="threaded",
            node_ids=(str(NODE),),
            clock=ManualClock(timestamp(5)),
        )
        capacity.acquire("engine-main", str(NODE))
        channel = BoundedChannel(kind=CHANNEL_KIND_RESULT, capacity=8)
        coordinator = _coordinator(writer, database, reader, scheduler, capacity, channel)
        coordinator.register_assignment(
            RegisteredAssignment(
                identity=identity,
                work_item_id=str(WORK),
                attempt_number=1,
                lease_fence=lease_fence,
                lease_owner="engine-main",
                control_generation=1,
                deadline_micros=micros(timestamp(64)),
                allowed_artifact_ids=(),
            )
        )
        coordinator.submit_result(result)
        assert coordinator.committed_count == 1
        assert coordinator.registered_identities == ()
        with database.transaction() as session:
            record = SqlAlchemyWorkItemRepository(session).get(WORK)
            assert record is not None
            assert record.state is WorkItemState.SUCCEEDED
        assert scheduler.is_finished
    finally:
        writer.close(timeout_seconds=5.0)


def test_factory_rejects_incompilable_intents_before_admission(
    database: SQLiteDatabase,
) -> None:
    """Classification coupling and metric discipline fail closed in the factory."""
    factory = DurableResultCommitFactory()
    succeeded = _result_envelope(ContractOutcome.SUCCEEDED)
    with pytest.raises(TypeError):
        factory.build(object())  # type: ignore[arg-type]
    writer = SQLiteTransactionalWriter(create_session_factory(database.engine), WriterSettings())
    writer.start()
    try:
        lease_fence = _claim_through_writer(database, writer)
        reader = SQLiteResultCoordinatorReader(database, ManualClock(timestamp(5)))
        quarantined = _result_envelope(ContractOutcome.QUARANTINED, lease_fence=lease_fence)
        missing = _intent_for(reader, quarantined, lease_fence=lease_fence, classification=None)
        with pytest.raises(ResultValidationRejection, match="failure classification"):
            factory.build(missing)
        succeeded_intent = _intent_for(
            reader, succeeded, lease_fence=lease_fence, classification="connection"
        )
        with pytest.raises(ResultValidationRejection, match="cannot carry"):
            factory.build(succeeded_intent)
        foreign_metric = _result_envelope(ContractOutcome.SUCCEEDED, lease_fence=lease_fence)
        from dataclasses import replace

        tampered = replace(
            foreign_metric,
            metrics=(*foreign_metric.metrics, ContractMetric("side_effect", 1)),
        )
        tampered_intent = _intent_for(
            reader, tampered, lease_fence=lease_fence, classification=None
        )
        with pytest.raises(ResultValidationRejection, match="durable metric set"):
            factory.build(tampered_intent)
    finally:
        writer.close(timeout_seconds=5.0)


def test_factory_correlation_id_is_validated() -> None:
    """The factory rejects non-printable correlation evidence."""
    with pytest.raises(ResultValidationRejection, match="printable ASCII"):
        DurableResultCommitFactory(correlation_id="corr✓")
    with pytest.raises(ResultValidationRejection, match="length"):
        DurableResultCommitFactory(correlation_id="c" * 97)
