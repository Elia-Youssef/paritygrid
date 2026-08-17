"""Real SQLite boundary tests for coherent pause-frontier reads."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.adapters.persistence import (
    SqlAlchemyExecutionEventRepository,
    SqlAlchemyPipelineRepository,
    SqlAlchemyRunRepository,
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    SQLitePauseStateReader,
    SQLiteTransactionalWriter,
    create_session_factory,
    upgrade_to_head,
)
from paritygrid.application.execution import (
    AcquireWorkLeaseRequest,
    AttemptEventContext,
    AttemptSucceeded,
    PauseCoordinator,
    PauseCoordinatorSettings,
    ResultCheckpoint,
    ResultMetrics,
    ResultSubmission,
    RunnerNodeOutcome,
    RunnerNodeRequest,
    RunnerNodeResult,
    RunnerStatus,
    SequentialRunner,
    SuccessfulWorkResult,
    TransactionalCheckpointResultSink,
    WorkLeaseService,
    WorkLeaseSettings,
    submit_work_result,
)
from paritygrid.application.planner import (
    ExecutionPlan,
    ExecutionPlanNode,
    NodeRole,
    PlannerRunnerKind,
    ResourcePolicy,
    RetryBehavior,
)
from paritygrid.application.planner.registry import ConnectorRequirement
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.execution import ExecutionRecordNotFoundError
from paritygrid.application.ports.run_aggregates import WorkMetricDelta
from paritygrid.application.ports.writer import EventAppendRequest, WriterCommand
from paritygrid.application.writes import (
    WORK_LEASE_EVENT_PAYLOAD_SCHEMA_VERSION,
    BootstrapWork,
    CreateCapturedRun,
    TransitionRun,
)
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    AttemptNumber,
    Duration,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import NodeKind, PartitionKey

RUN_ID = RunId("run_pause-reader")
PIPELINE_ID = PipelineId("pip_pause-reader")
NODE_ID = NodeId("nod_pause-reader")
WORK_ID = WorkItemId("wrk_pause-reader")
PARTITION = PartitionKey("partition-pause-reader")
NOW = UtcTimestamp(datetime(2026, 8, 14, 10, tzinfo=UTC))


class _Clock:
    def __init__(self, *values: UtcTimestamp) -> None:
        self.values = list(values)

    def now(self) -> UtcTimestamp:
        return self.values.pop(0)


def _time(hour: int) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 14, hour, tzinfo=UTC))


def _plan() -> ExecutionPlan:
    return ExecutionPlan(
        nodes=(
            ExecutionPlanNode(
                node_id=NODE_ID,
                kind=NodeKind("transform.normalize"),
                role=NodeRole.TRANSFORM,
                configuration_version=1,
                configuration=ConfigurationDocument(()),
                connector_requirement=ConnectorRequirement.NONE,
                connector_id=None,
                supported_runners=(PlannerRunnerKind.SEQUENTIAL,),
                retry_behavior=RetryBehavior.NEVER,
                requires_idempotency=False,
            ),
        ),
        edges=(),
        resource_policy=ResourcePolicy(),
        connector_bindings=(),
    )


def _event(
    sequence: int,
    kind: str,
    occurred_at: UtcTimestamp,
    subject: RunId | WorkItemId = RUN_ID,
    *,
    payload_schema_version: int = 1,
    payload: RedactedDocument | None = None,
) -> EventAppendRequest:
    return EventAppendRequest(
        EventSequence(sequence),
        sequence,
        PendingExecutionEvent(
            kind,
            occurred_at,
            EventSubjectKind.RUN if type(subject) is RunId else EventSubjectKind.WORK_ITEM,
            subject,
            "pause-reader-test",
            payload_schema_version,
            RedactedDocument.from_mapping({"kind": kind}) if payload is None else payload,
        ),
    )


def _submit(writer: SQLiteTransactionalWriter, command: WriterCommand) -> None:
    writer.submit(command, timeout_seconds=5.0).result(timeout_seconds=5.0)


def test_pause_reader_returns_run_and_counter_from_one_short_transaction(
    tmp_path: Path,
) -> None:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "pause reader.db"))
    try:
        with database.engine.connect() as connection:
            upgrade_to_head(connection)
        with database.transaction() as session:
            pipelines = SqlAlchemyPipelineRepository(session)
            pipelines.create(
                pipeline_id=PIPELINE_ID,
                display_name="Pause reader",
                description=None,
                created_at=NOW,
            )
            pipelines.publish_version(
                pipeline_id=PIPELINE_ID,
                expected_latest_version=None,
                specification=ConfigurationDocument(()),
                planner_format_version=1,
                published_at=NOW,
            )
            node_ids = tuple(NodeId(f"nod_pause-page-{index:03}") for index in range(101))
            SqlAlchemyRunRepository(session).create(
                run_id=RUN_ID,
                pipeline_id=PIPELINE_ID,
                pipeline_version=PipelineVersion(1),
                runner_kind="sequential",
                runner_configuration=ConfigurationDocument(()),
                scenario_seed=None,
                node_ids=node_ids,
                created_at=NOW,
            )

        reader = SQLitePauseStateReader(database)
        state = reader.read(RUN_ID)
        assert state.run.run_id == RUN_ID
        assert state.run.row_version == 1
        assert state.next_event_sequence.number == 1
        assert state.event_counter_row_version == 1
        assert state.active_work_count == 0
        with pytest.raises(ExecutionRecordNotFoundError, match="does not exist"):
            reader.read(RunId("run_pause-missing"))
    finally:
        database.close()


def test_pause_reader_requires_exact_database_and_run_identity(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="SQLiteDatabase"):
        SQLitePauseStateReader(cast(Any, object()))
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "pause reader exact.db"))
    try:
        reader = SQLitePauseStateReader(database)
        with pytest.raises(TypeError, match="RunId"):
            reader.read(cast(Any, object()))
    finally:
        database.close()


def test_real_wal_pause_and_resume_persist_exact_ordered_frontier(tmp_path: Path) -> None:
    config = SQLiteDatabaseConfig(tmp_path / "pause coordinator %.db")
    database = SQLiteDatabase.open(config)
    writer: SQLiteTransactionalWriter | None = None
    try:
        with database.engine.connect() as connection:
            upgrade_to_head(connection)
        with database.transaction() as session:
            pipelines = SqlAlchemyPipelineRepository(session)
            pipelines.create(
                pipeline_id=PIPELINE_ID,
                display_name="Pause coordinator",
                description=None,
                created_at=_time(0),
            )
            pipelines.publish_version(
                pipeline_id=PIPELINE_ID,
                expected_latest_version=None,
                specification=ConfigurationDocument(()),
                planner_format_version=1,
                published_at=_time(0),
            )
        writer = SQLiteTransactionalWriter(create_session_factory(database.engine))
        writer.start()
        _submit(
            writer,
            CreateCapturedRun(
                RUN_ID,
                PIPELINE_ID,
                PipelineVersion(1),
                "sequential",
                ConfigurationDocument(()),
                None,
                (NODE_ID,),
                _time(1),
                _event(1, "run_created", _time(1)),
            ),
        )
        _submit(
            writer,
            TransitionRun(
                RUN_ID,
                1,
                RunState.RUNNING,
                _time(2),
                None,
                _event(2, "run_started", _time(2)),
            ),
        )
        _submit(
            writer,
            BootstrapWork(
                RUN_ID,
                NODE_ID,
                WORK_ID,
                PARTITION,
                None,
                _time(2),
                1,
                2,
                _event(3, "work_created", _time(2), WORK_ID),
            ),
        )

        reader = SQLitePauseStateReader(database)
        leases = WorkLeaseService(
            writer,
            _Clock(_time(3)),
            settings=WorkLeaseSettings(Duration(10_800_000_000), 5.0, 5.0),
        )
        lease = leases.acquire(
            AcquireWorkLeaseRequest(
                RUN_ID,
                NODE_ID,
                WORK_ID,
                AttemptNumber(1),
                1,
                2,
                3,
                "pause-owner",
                PlannerRunnerKind.SEQUENTIAL.value,
                "pause-worker",
                _event(
                    4,
                    "work_claimed",
                    _time(3),
                    WORK_ID,
                    payload_schema_version=WORK_LEASE_EVENT_PAYLOAD_SCHEMA_VERSION,
                    payload=RedactedDocument.from_mapping(
                        {
                            "attempt_number": 1,
                            "lease_expires_at": str(_time(6)),
                            "node_id": str(NODE_ID),
                            "runner_kind": PlannerRunnerKind.SEQUENTIAL.value,
                        }
                    ),
                ),
            )
        )
        coordinator = PauseCoordinator(
            writer,
            reader,
            leases,
            _Clock(_time(6), _time(7)),
            settings=PauseCoordinatorSettings(5.0, 5.0),
        )
        sink = TransactionalCheckpointResultSink(writer)

        class CheckpointingExecutor:
            def execute(self, request: RunnerNodeRequest, /) -> RunnerNodeResult:
                coordinator.request_pause(RUN_ID)
                assert reader.read(RUN_ID).active_work_count == 1
                submit_work_result(
                    sink,
                    ResultSubmission(
                        lease,
                        SuccessfulWorkResult(
                            AttemptSucceeded(
                                AttemptEventContext(
                                    RUN_ID,
                                    NODE_ID,
                                    WORK_ID,
                                    AttemptNumber(1),
                                    lease.claim.started_at,
                                    PlannerRunnerKind.SEQUENTIAL,
                                    lease.claim.worker_identity,
                                    "pause-real",
                                ),
                                _time(5),
                            ),
                            ResultCheckpoint(PARTITION, 1, None, None, None),
                            ResultMetrics(1, 1, WorkMetricDelta(1, 1, 0, 1, 1)),
                        ),
                    ),
                    lease_service=leases,
                )
                assert reader.read(RUN_ID).active_work_count == 0
                return RunnerNodeResult(request.node.node_id, RunnerNodeOutcome.PAUSED)

            def close(self) -> None:
                return

        runner_report = SequentialRunner(
            CheckpointingExecutor(),
            pause=coordinator.token,
        ).run(_plan())
        assert runner_report.status is RunnerStatus.PAUSED
        assert runner_report.scheduler_state.ready_node_ids == (NODE_ID,)
        assert runner_report.pause_acknowledgement is not None
        paused, _pause_report = coordinator.pause(
            runner_report.pause_acknowledgement,
            correlation_id="pause-real",
        )
        resumed = coordinator.resume(paused, correlation_id="resume-real")
        assert resumed.run.state is RunState.RUNNING
        assert resumed.run.row_version == 9

        reopened = reader.read(RUN_ID)
        assert reopened.run == resumed.run
        assert reopened.next_event_sequence == EventSequence(10)
        assert reopened.event_counter_row_version == 10
        assert reopened.active_work_count == 0
        with database.transaction() as session:
            page = SqlAlchemyExecutionEventRepository(session).list_after(
                RUN_ID,
                after=None,
                limit=10,
            )
        assert [item.event_kind for item in page.items] == [
            "run_created",
            "run_started",
            "work_created",
            "work_claimed",
            "checkpoint_committed",
            "run_pausing",
            "run_paused",
            "run_resuming",
            "run_started",
        ]
        writer.close(timeout_seconds=5.0)
        writer = None
        database.close()
        database = SQLiteDatabase.open(config)
        reopened = SQLitePauseStateReader(database).read(RUN_ID)
        assert reopened.run == resumed.run
        assert reopened.next_event_sequence == EventSequence(10)
        assert reopened.active_work_count == 0
        with database.engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA quick_check").scalar_one() == "ok"
            assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
    finally:
        if writer is not None:
            writer.close(timeout_seconds=5.0)
        database.close()
