"""Deterministic sequential end-to-end scenario over public Phase 2-6 contracts."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any

from paritygrid.adapters.analytics.duckdb import DuckDBLifecycleCoordinator
from paritygrid.adapters.analytics.run_statistics import DuckDBRunStatisticsQueryEngine
from paritygrid.adapters.artifacts.manifests import (
    ArtifactManifestRecord,
    FileSystemArtifactManifestRepository,
)
from paritygrid.adapters.artifacts.writer import (
    ArtifactWriteReceipt,
    FileSystemArtifactWriter,
)
from paritygrid.adapters.persistence import (
    SqlAlchemyConnectorRepository,
    SqlAlchemyPipelineRepository,
    SqlAlchemyRunRepository,
    SqlAlchemyWorkItemRepository,
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    SQLiteTransactionalWriter,
    create_session_factory,
    upgrade_to_head,
)
from paritygrid.adapters.persistence.writer.notifications import (
    BoundedCommittedNotificationBuffer,
)
from paritygrid.application.execution import (
    AcquireWorkLeaseRequest,
    AttemptEventContext,
    AttemptFailed,
    AttemptSucceeded,
    RenewWorkLeaseRequest,
    ResultCheckpoint,
    ResultMetrics,
    ResultSinkCommitted,
    ResultSubmission,
    RetryPolicyName,
    RetryScheduledDecision,
    RetryStoppedDecision,
    RunnerNodeOutcome,
    RunnerNodeRequest,
    RunnerNodeResult,
    SchedulerState,
    SuccessfulWorkResult,
    TransactionalCheckpointResultSink,
    UnsuccessfulWorkResult,
    WorkLease,
    WorkLeaseService,
    WorkLeaseSettings,
    submit_work_result,
)
from paritygrid.application.planner import (
    ExecutionPlan,
    PipelineDocument,
    PipelinePublicationService,
    PlanFingerprint,
    PlannerRunnerKind,
    PublishedPipelineSpecification,
    compile_execution_plan,
    fingerprint_execution_plan,
)
from paritygrid.application.ports.analytics import AnalyticalDatabaseConfig
from paritygrid.application.ports.artifacts import ArtifactRelativePath
from paritygrid.application.ports.configuration import (
    ConfigurationDocument,
    ConnectorSecretReference,
)
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.run_aggregates import WorkMetricDelta
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    WriterCommand,
    WriterReceipt,
    WriterSettings,
)
from paritygrid.application.writes import (
    BootstrapWork,
    CreateCapturedRun,
    TransitionRun,
)
from paritygrid.domain.execution import FailureClassification, FailureDisposition, RunState
from paritygrid.domain.models import (
    ArtifactId,
    AttemptNumber,
    ConnectorId,
    Duration,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

SCENARIO_VERSION = 1


def _partition_key(index: int) -> PartitionKey:
    # Mirror the planner's PARTITION_KEY_DIGITS so scenario keys equal the
    # compiled plan's stable keys.
    return PartitionKey(f"partition-{index:08d}")


PIPELINE_ID = PipelineId("pip_e2e-sequential")
CONNECTOR_ID = ConnectorId("con_e2e-source")
SOURCE_NODE = NodeId("nod_e2e-source01")
NORMALIZE_NODE = NodeId("nod_e2e-normal01")
VALIDATE_NODE = NodeId("nod_e2e-valid01")
PARTITION_NODE = NodeId("nod_e2e-parti01")
EXPORT_NODE = NodeId("nod_e2e-export01")
PLAN_NODE_ORDER = (SOURCE_NODE, NORMALIZE_NODE, VALIDATE_NODE, PARTITION_NODE, EXPORT_NODE)
PARTITION_COUNT = 2
LEASE_MICROSECONDS = 600_000_000
ARTIFACT_BODY = b"paritygrid sequential scenario artifact attempt 1"
WORKER_IDENTITY = "e2e-sequential-worker"
LEASE_OWNER = "e2e-sequential-owner"
CORRELATION_ID = "corr-e2e-seq"

MAIN_RUN_ID = RunId("run_e2e-main0001")
CANCEL_RUN_ID = RunId("run_e2e-cancel01")
INTERRUPTED_RUN_ID = RunId("run_e2e-interr01")

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_BASE = datetime(2026, 8, 18, 8, 0, 0, tzinfo=UTC)


class StepClock:
    """Deterministic monotonic clock advanced only by explicit steps."""

    __slots__ = ("_microseconds",)

    def __init__(self, start: UtcTimestamp) -> None:
        self._microseconds = _to_microseconds(start)

    def now(self) -> UtcTimestamp:
        return _from_microseconds(self._microseconds)

    def advance(self, seconds: int) -> UtcTimestamp:
        self._microseconds += seconds * 1_000_000
        return self.now()


class ScriptedOutcome(StrEnum):
    """Closed per-partition scripted terminal behaviors."""

    SUCCESS = "success"
    RETRY_THEN_SUCCESS = "retry_then_success"
    QUARANTINE = "quarantine"
    ABANDON = "abandon"


@dataclass(frozen=True, slots=True)
class ScriptStep:
    """One immutable scripted partition execution."""

    node_id: NodeId
    partition_key: PartitionKey
    outcome: ScriptedOutcome
    with_artifact: bool


SCRIPT: tuple[ScriptStep, ...] = (
    ScriptStep(SOURCE_NODE, _partition_key(0), ScriptedOutcome.SUCCESS, False),
    ScriptStep(NORMALIZE_NODE, _partition_key(0), ScriptedOutcome.SUCCESS, False),
    ScriptStep(VALIDATE_NODE, _partition_key(0), ScriptedOutcome.SUCCESS, False),
    ScriptStep(PARTITION_NODE, _partition_key(0), ScriptedOutcome.RETRY_THEN_SUCCESS, False),
    ScriptStep(PARTITION_NODE, _partition_key(1), ScriptedOutcome.QUARANTINE, False),
    ScriptStep(EXPORT_NODE, _partition_key(0), ScriptedOutcome.SUCCESS, True),
)


def _to_microseconds(value: UtcTimestamp) -> int:
    delta = value.to_datetime() - _EPOCH
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def _from_microseconds(value: int) -> UtcTimestamp:
    return UtcTimestamp(_EPOCH + timedelta(microseconds=value))


def work_item_id(run_id: RunId, node_id: NodeId, partition_key: PartitionKey) -> WorkItemId:
    """Return the deterministic per-run work identity for one node partition."""
    suffix = partition_key.value.replace("partition-", "")
    run_code = run_id.value.replace("run_e2e-", "")
    return WorkItemId(f"wrk_{run_code}-{node_id.value.replace('nod_', '')}-{suffix}")


def artifact_bytes() -> bytes:
    """Return the exact committed artifact payload of the scenario."""
    return ARTIFACT_BODY


def draft_document() -> PipelineDocument:
    """Return the reviewed scenario pipeline draft."""
    value: dict[str, object] = {
        "canonical_format_version": 1,
        "edges": [
            _edge(SOURCE_NODE, NORMALIZE_NODE),
            _edge(NORMALIZE_NODE, VALIDATE_NODE),
            _edge(VALIDATE_NODE, PARTITION_NODE),
            _edge(PARTITION_NODE, EXPORT_NODE),
        ],
        "layout": [
            {"node_id": str(node_id), "x": index * 10, "y": 0}
            for index, node_id in enumerate(PLAN_NODE_ORDER)
        ],
        "nodes": [
            _node(
                SOURCE_NODE, "source.csv", {"encoding": "utf-8", "header": True}, str(CONNECTOR_ID)
            ),
            _node(NORMALIZE_NODE, "transform.normalize", {}, None),
            _node(VALIDATE_NODE, "transform.validate", {}, None),
            _node(
                PARTITION_NODE, "transform.partition", {"partition_count": PARTITION_COUNT}, None
            ),
            _node(EXPORT_NODE, "export.parquet", {"compression": "zstd"}, None),
        ],
        "resource_policy": {
            "max_concurrency": 1,
            "max_in_flight": 4,
            "memory_limit_bytes": 268_435_456,
            "operation_timeout_seconds": 30,
            "queue_capacity": 8,
        },
        "schema_version": 1,
    }
    return PipelineDocument.from_mapping(value)


def _edge(source: NodeId, target: NodeId) -> dict[str, object]:
    return {
        "source_node_id": str(source),
        "source_port": "records",
        "target_node_id": str(target),
        "target_port": "records",
    }


def _node(
    node_id: NodeId,
    kind: str,
    configuration: dict[str, object],
    connector_id: str | None,
) -> dict[str, object]:
    return {
        "configuration": configuration,
        "configuration_version": 1,
        "connector_id": connector_id,
        "id": str(node_id),
        "kind": kind,
    }


@dataclass(slots=True)
class ScenarioHarness:
    """Owned resource bundle for one deterministic scenario execution."""

    database: SQLiteDatabase
    writer: SQLiteTransactionalWriter
    notifications: BoundedCommittedNotificationBuffer
    artifact_root: Path
    analytics_path: Path
    clock: StepClock
    analytics: DuckDBRunStatisticsQueryEngine
    analytics_coordinator: DuckDBLifecycleCoordinator
    pipeline_version: PipelineVersion
    plan_fingerprint: PlanFingerprint
    node_partition_keys: dict[NodeId, tuple[PartitionKey, ...]] = field(
        default_factory=dict[NodeId, tuple[PartitionKey, ...]]
    )
    attempt_events: list[str] = field(default_factory=list[str])
    artifact_receipts: dict[WorkItemId, ArtifactWriteReceipt] = field(
        default_factory=dict[WorkItemId, ArtifactWriteReceipt]
    )

    def close(self) -> None:
        """Release every owned resource in reverse acquisition order."""
        cleanup_failed = False
        fatal_error: BaseException | None = None
        try:
            self.analytics_coordinator.close()
        except BaseException as error:
            cleanup_failed = True
            if not isinstance(error, Exception):
                fatal_error = error
        try:
            closed = self.writer.close(timeout_seconds=5.0)
            cleanup_failed = cleanup_failed or not closed.drained
        except BaseException as error:
            cleanup_failed = True
            if fatal_error is None and not isinstance(error, Exception):
                fatal_error = error
        try:
            self.database.close()
        except BaseException as error:
            cleanup_failed = True
            if fatal_error is None and not isinstance(error, Exception):
                fatal_error = error
        if fatal_error is not None:
            raise fatal_error
        if cleanup_failed:
            raise ScenarioHarnessCleanupError(
                "sequential scenario did not release every owned resource"
            ) from None

    def partitions_of(self, node_id: NodeId) -> tuple[PartitionKey, ...]:
        """Return the plan's stable partition keys for one node."""
        return self.node_partition_keys[node_id]


class ScenarioHarnessCleanupError(RuntimeError):
    """One or more owned scenario resources did not close cleanly."""


def prepare_harness(
    database_path: Path,
    artifact_root: Path,
    analytics_path: Path,
) -> ScenarioHarness:
    """Publish the scenario pipeline and open every owned scenario resource."""
    artifact_root.mkdir(parents=True, exist_ok=True)
    analytics_path.parent.mkdir(parents=True, exist_ok=True)
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(database_path))
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    published_at = UtcTimestamp(_BASE)
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        connectors = SqlAlchemyConnectorRepository(session)
        pipelines.create(
            pipeline_id=PIPELINE_ID,
            display_name="Sequential scenario",
            description=None,
            created_at=published_at,
        )
        connectors.create(
            connector_id=CONNECTOR_ID,
            kind="csv-local",
            display_name="Scenario source",
            configuration=ConfigurationDocument.from_mapping(
                {"api_token_reference": "source.token", "path": "inventory.csv"}
            ),
            capabilities=ConfigurationDocument.from_mapping({"read": True}),
            schema_discovery=None,
            secret_references=(
                ConnectorSecretReference("source.token", "PARITYGRID_SOURCE_TOKEN"),
            ),
            created_at=published_at,
        )
        service = PipelinePublicationService(pipelines, connectors)
        published = service.publish(
            pipeline_id=PIPELINE_ID,
            expected_latest_version=None,
            draft=draft_document(),
            published_at=published_at,
        )
    envelope = PublishedPipelineSpecification.from_configuration_document(published.specification)
    plan = compile_execution_plan(envelope)
    notifications = BoundedCommittedNotificationBuffer(256)
    writer = SQLiteTransactionalWriter(
        create_session_factory(database.engine),
        settings=WriterSettings(contention_delay_seconds=0.0),
        notifications=notifications,
    )
    writer.start()
    analytics_coordinator = DuckDBLifecycleCoordinator(
        AnalyticalDatabaseConfig(analytics_path.resolve())
    )
    analytics_coordinator.open()
    keys: dict[NodeId, tuple[PartitionKey, ...]] = {}
    for node in plan.nodes:
        keys[node.node_id] = node.partition_strategy.keys()
    return ScenarioHarness(
        database=database,
        writer=writer,
        notifications=notifications,
        artifact_root=artifact_root,
        analytics_path=analytics_path,
        clock=StepClock(UtcTimestamp(_BASE)),
        analytics=DuckDBRunStatisticsQueryEngine(analytics_coordinator),
        analytics_coordinator=analytics_coordinator,
        pipeline_version=published.version,
        plan_fingerprint=fingerprint_execution_plan(plan),
        node_partition_keys=keys,
    )


def compiled_plan(harness: ScenarioHarness) -> ExecutionPlan:
    """Recompile the published scenario plan from durable publication."""
    with harness.database.transaction() as session:
        version = SqlAlchemyPipelineRepository(session).get_version(
            PIPELINE_ID, harness.pipeline_version
        )
    assert version is not None
    envelope = PublishedPipelineSpecification.from_configuration_document(version.specification)
    return compile_execution_plan(envelope)


def _run_exists(database: SQLiteDatabase, run_id: RunId) -> bool:
    with database.transaction() as session:
        return SqlAlchemyRunRepository(session).get(run_id) is not None


def read_frontier(
    database: SQLiteDatabase,
    run_id: RunId,
    node_id: NodeId,
    work_id: WorkItemId,
) -> dict[str, int] | None:
    """Read one coherent optimistic frontier from durable evidence."""
    with database.transaction() as session:
        runs = SqlAlchemyRunRepository(session)
        run = runs.get(run_id)
        counter = runs.get_event_counter(run_id)
        node = runs.get_node(run_id, node_id)
        work = SqlAlchemyWorkItemRepository(session).get(work_id)
        if run is None or counter is None or node is None:
            return None
        return {
            "run": run.row_version,
            "node": node.row_version,
            "work": -1 if work is None else work.row_version,
            "attempts": 0 if work is None else work.completed_attempt_count,
            "sequence": counter.next_sequence_number,
            "counter": counter.row_version,
        }


class ScenarioExecutor:
    """Deterministic node executor driving scripted partitions through boundaries."""

    def __init__(
        self,
        harness: ScenarioHarness,
        run_id: RunId,
        *,
        abandon_at: NodeId | None = None,
        cancel_boundary: Callable[[ScenarioExecutor, WorkLease], None] | None = None,
        renew_at: NodeId | None = None,
        on_node_complete: Callable[[ScenarioExecutor, NodeId], None] | None = None,
    ) -> None:
        self._harness = harness
        self._run_id = run_id
        self._abandon_at = abandon_at
        self._cancel_boundary = cancel_boundary
        self._renew_at = renew_at
        self._on_node_complete = on_node_complete
        self._lease_service = WorkLeaseService(
            harness.writer,
            harness.clock,
            settings=WorkLeaseSettings(
                lease_duration=Duration(LEASE_MICROSECONDS),
                admission_timeout_seconds=5.0,
                result_timeout_seconds=5.0,
            ),
        )
        self._sink = TransactionalCheckpointResultSink(harness.writer)
        self._artifact_writer = FileSystemArtifactWriter(
            harness.artifact_root, maximum_bytes=1_048_576
        )
        self._created = False

    @property
    def lease_service(self) -> WorkLeaseService:
        """Return the lease service shared with pause and cancellation coordination."""
        return self._lease_service

    def close(self) -> None:
        return

    def execute(self, request: RunnerNodeRequest, /) -> RunnerNodeResult:
        """Execute every scripted partition of one node through public boundaries."""
        node_id = request.node.node_id
        steps = [step for step in SCRIPT if step.node_id == node_id]
        # Decomposition precedes execution: every partition is registered
        # before any completes so node aggregates always see the true total.
        for step in steps:
            self._bootstrap(step)
        for step in steps:
            lease = self._claim(step, attempt=1)
            if self._renew_at == node_id:
                lease = self._renew(step, lease)
            if self._abandon_at == node_id:
                # Controlled interruption: the claim is abandoned mid-flight
                # without submitting any result or checkpoint.
                return RunnerNodeResult(node_id, RunnerNodeOutcome.FAILED)
            if self._cancel_boundary is not None:
                self._cancel_boundary(self, lease)
                return RunnerNodeResult(node_id, RunnerNodeOutcome.CANCELLED)
            self._drive(step, lease, attempt=1)
        if self._on_node_complete is not None:
            self._on_node_complete(self, node_id)
        return RunnerNodeResult(node_id, RunnerNodeOutcome.SUCCEEDED)

    def _drive(self, step: ScriptStep, lease: WorkLease, *, attempt: int) -> None:
        harness = self._harness
        work_id = lease.claim.work_item_id
        context = AttemptEventContext(
            self._run_id,
            step.node_id,
            work_id,
            lease.claim.attempt_number,
            lease.claim.started_at,
            PlannerRunnerKind.SEQUENTIAL,
            WORKER_IDENTITY,
            CORRELATION_ID,
        )
        harness.attempt_events.append("attempt_started")
        finished_at = harness.clock.advance(1)
        outcome = _outcome_for(step, attempt)
        if outcome is ScriptedOutcome.SUCCESS:
            artifact = self._commit_artifact(step) if step.with_artifact else None
            submission = ResultSubmission(
                lease,
                SuccessfulWorkResult(
                    AttemptSucceeded(context, finished_at),
                    ResultCheckpoint(
                        step.partition_key,
                        1,
                        ConfigurationDocument.from_mapping({"cursor": attempt - 1}),
                        ConfigurationDocument.from_mapping({"position": attempt}),
                        artifact,
                    ),
                    ResultMetrics(2, 4, WorkMetricDelta(2, 2, 0, 4, 4)),
                ),
            )
        else:
            classification = (
                FailureClassification.CONNECTION
                if outcome is ScriptedOutcome.RETRY_THEN_SUCCESS
                else FailureClassification.VALIDATION
            )
            terminal = AttemptFailed(context, finished_at, classification)
            if outcome is ScriptedOutcome.RETRY_THEN_SUCCESS:
                decision: Any = RetryScheduledDecision(
                    RetryPolicyName("bounded_exponential_v1"),
                    work_id,
                    lease.claim.attempt_number,
                    classification,
                    finished_at,
                    finished_at,
                    None,
                    Duration(0),
                    Duration(1_000_000),
                    _from_microseconds(_to_microseconds(finished_at) + 1_000_000),
                )
            else:
                decision = RetryStoppedDecision(
                    RetryPolicyName("bounded_exponential_v1"),
                    work_id,
                    lease.claim.attempt_number,
                    classification,
                    finished_at,
                    FailureDisposition.QUARANTINE,
                    False,
                )
            harness.attempt_events.append("attempt_failed")
            submission = ResultSubmission(
                lease,
                UnsuccessfulWorkResult(
                    terminal,
                    decision,
                    ResultMetrics(0, 0, WorkMetricDelta(0, 0, 0, 0, 0)),
                ),
            )
        committed = submit_work_result(self._sink, submission, lease_service=self._lease_service)
        assert type(committed) is ResultSinkCommitted
        harness.attempt_events.append(
            "attempt_succeeded" if outcome is ScriptedOutcome.SUCCESS else "attempt_failed"
        )
        if outcome is ScriptedOutcome.RETRY_THEN_SUCCESS:
            harness.clock.advance(2)
            retry_lease = self._claim(step, attempt=attempt + 1)
            self._drive_success_only(step, retry_lease, attempt + 1)

    def _drive_success_only(self, step: ScriptStep, lease: WorkLease, attempt: int) -> None:
        harness = self._harness
        work_id = lease.claim.work_item_id
        context = AttemptEventContext(
            self._run_id,
            step.node_id,
            work_id,
            lease.claim.attempt_number,
            lease.claim.started_at,
            PlannerRunnerKind.SEQUENTIAL,
            WORKER_IDENTITY,
            CORRELATION_ID,
        )
        harness.attempt_events.append("attempt_started")
        finished_at = harness.clock.advance(1)
        submission = ResultSubmission(
            lease,
            SuccessfulWorkResult(
                AttemptSucceeded(context, finished_at),
                ResultCheckpoint(
                    step.partition_key,
                    1,
                    ConfigurationDocument.from_mapping({"cursor": attempt - 1}),
                    ConfigurationDocument.from_mapping({"position": attempt}),
                    None,
                ),
                ResultMetrics(2, 4, WorkMetricDelta(2, 2, 0, 4, 4)),
            ),
        )
        committed = submit_work_result(self._sink, submission, lease_service=self._lease_service)
        assert type(committed) is ResultSinkCommitted
        harness.attempt_events.append("attempt_succeeded")

    def _bootstrap(self, step: ScriptStep) -> None:
        self._ensure_created()
        work_id = work_item_id(self._run_id, step.node_id, step.partition_key)
        frontier = read_frontier(self._harness.database, self._run_id, step.node_id, work_id)
        assert frontier is not None
        if frontier["work"] >= 0:
            return
        created_at = self._harness.clock.advance(1)
        _submit_direct(
            self._harness.writer,
            BootstrapWork(
                run_id=self._run_id,
                node_id=step.node_id,
                work_item_id=work_id,
                partition_key=step.partition_key,
                input_reference=None,
                created_at=created_at,
                expected_node_row_version=frontier["node"],
                expected_run_row_version=frontier["run"],
                event=_carrier_event(
                    frontier["sequence"], frontier["counter"], work_id, "work_created", created_at
                ),
            ),
        )

    def _claim(self, step: ScriptStep, *, attempt: int) -> WorkLease:
        self._ensure_created()
        work_id = work_item_id(self._run_id, step.node_id, step.partition_key)
        frontier = read_frontier(self._harness.database, self._run_id, step.node_id, work_id)
        assert frontier is not None
        assert frontier["work"] >= 0
        started_at = self._harness.clock.advance(1)
        return self._lease_service.acquire(
            AcquireWorkLeaseRequest(
                run_id=self._run_id,
                node_id=step.node_id,
                work_item_id=work_id,
                expected_attempt_number=AttemptNumber(frontier["attempts"] + 1),
                expected_work_row_version=frontier["work"],
                expected_node_row_version=frontier["node"],
                expected_run_row_version=frontier["run"],
                lease_owner=LEASE_OWNER,
                runner_kind="sequential",
                worker_identity=WORKER_IDENTITY,
                event=_carrier_event(
                    frontier["sequence"], frontier["counter"], work_id, "work_claimed", started_at
                ),
            )
        )

    def _renew(self, step: ScriptStep, lease: WorkLease) -> WorkLease:
        frontier = read_frontier(
            self._harness.database,
            self._run_id,
            step.node_id,
            lease.claim.work_item_id,
        )
        assert frontier is not None
        renewed_at = self._harness.clock.advance(1)
        return self._lease_service.renew(
            lease,
            RenewWorkLeaseRequest(
                expected_run_row_version=frontier["run"],
                event=_carrier_event(
                    frontier["sequence"],
                    frontier["counter"],
                    lease.claim.work_item_id,
                    "work_claim_renewed",
                    renewed_at,
                ),
            ),
        )

    def _commit_artifact(self, step: ScriptStep) -> ArtifactManifestRecord:
        relative = ArtifactRelativePath(
            f"runs/{self._run_id.value}/{step.node_id.value}/{step.partition_key.value}.parquet"
        )
        receipt = self._artifact_writer.write(relative, [ARTIFACT_BODY])
        run_code = self._run_id.value.replace("run_e2e-", "")
        artifact_id = ArtifactId(
            f"art_{run_code}-{step.node_id.value.replace('nod_', '')}-"
            f"{step.partition_key.value.replace('partition-', '')}"
        )
        with self._harness.database.transaction() as session:
            manifest = FileSystemArtifactManifestRepository(
                session, self._harness.artifact_root
            ).register(
                artifact_id=artifact_id,
                run_id=self._run_id,
                node_id=step.node_id,
                partition_key=step.partition_key,
                write_receipt=receipt,
                media_type="application/vnd.apache.parquet",
                schema_version=1,
                row_count=1,
                created_at=self._harness.clock.now(),
            )
        self._harness.artifact_receipts[
            work_item_id(self._run_id, step.node_id, step.partition_key)
        ] = receipt
        return manifest

    def _ensure_created(self) -> None:
        if self._created:
            return
        if _run_exists(self._harness.database, self._run_id):
            # A restarted executor resumes an already-captured run.
            self._created = True
            return
        created_at = self._harness.clock.now()
        _submit_direct(
            self._harness.writer,
            CreateCapturedRun(
                run_id=self._run_id,
                pipeline_id=PIPELINE_ID,
                pipeline_version=self._harness.pipeline_version,
                runner_kind="sequential",
                runner_configuration=ConfigurationDocument.from_mapping(
                    {"strategy": "sequential", "version": SCENARIO_VERSION}
                ),
                scenario_seed=SCENARIO_VERSION,
                node_ids=PLAN_NODE_ORDER,
                created_at=created_at,
                event=_carrier_event(1, 1, self._run_id, "run_created", created_at),
            ),
        )
        started_at = self._harness.clock.advance(1)
        _submit_direct(
            self._harness.writer,
            TransitionRun(
                run_id=self._run_id,
                expected_run_row_version=1,
                target_state=RunState.RUNNING,
                transitioned_at=started_at,
                execution_evidence_fingerprint=None,
                execution_evidence_fingerprint_version=None,
                event=_carrier_event(2, 2, self._run_id, "run_started", started_at),
            ),
        )
        self._created = True


def _outcome_for(step: ScriptStep, attempt: int) -> ScriptedOutcome:
    if step.outcome is ScriptedOutcome.RETRY_THEN_SUCCESS and attempt == 1:
        return ScriptedOutcome.RETRY_THEN_SUCCESS
    if step.outcome is ScriptedOutcome.RETRY_THEN_SUCCESS:
        return ScriptedOutcome.SUCCESS
    return step.outcome


def _carrier_event(
    sequence: int,
    counter: int,
    subject: RunId | WorkItemId,
    kind: str,
    occurred_at: UtcTimestamp,
) -> EventAppendRequest:
    """Carrier frontier the lease service rewrites with command-derived facts."""
    subject_kind = EventSubjectKind.RUN if type(subject) is RunId else EventSubjectKind.WORK_ITEM
    return EventAppendRequest(
        EventSequence(sequence),
        counter,
        PendingExecutionEvent(
            kind,
            occurred_at,
            subject_kind,
            subject,
            CORRELATION_ID,
            1,
            RedactedDocument.from_mapping({"kind": kind}),
        ),
    )


def _submit_direct(writer: SQLiteTransactionalWriter, command: WriterCommand) -> WriterReceipt:
    return writer.submit(command, timeout_seconds=5.0).result(timeout_seconds=5.0)


def scheduler_state_after(plan: ExecutionPlan, completed: tuple[NodeId, ...]) -> SchedulerState:
    """Rebuild the sequential frontier from durable node completions."""
    from paritygrid.application.execution import DependencyTracker

    tracker = DependencyTracker(plan)
    for node_id in completed:
        tracker.start(node_id)
        tracker.succeed(node_id)
    return tracker.state


__all__ = [
    "ARTIFACT_BODY",
    "CANCEL_RUN_ID",
    "CONNECTOR_ID",
    "EXPORT_NODE",
    "INTERRUPTED_RUN_ID",
    "LEASE_MICROSECONDS",
    "MAIN_RUN_ID",
    "NORMALIZE_NODE",
    "PARTITION_COUNT",
    "PARTITION_NODE",
    "PIPELINE_ID",
    "PLAN_NODE_ORDER",
    "SCENARIO_VERSION",
    "SCRIPT",
    "SOURCE_NODE",
    "VALIDATE_NODE",
    "ScenarioExecutor",
    "ScenarioHarness",
    "ScenarioHarnessCleanupError",
    "ScriptStep",
    "ScriptedOutcome",
    "StepClock",
    "artifact_bytes",
    "compiled_plan",
    "draft_document",
    "prepare_harness",
    "read_frontier",
    "scheduler_state_after",
    "work_item_id",
]
