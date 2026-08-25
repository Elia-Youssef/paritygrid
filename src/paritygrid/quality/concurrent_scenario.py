"""Shared concurrent execution scenario harness for Phase 7 proof.

This module composes the accepted Phase 6 and Phase 7 components into
one deterministic, parent-owned scenario every concurrent strategy and
suite reuses: a real SQLite database with the transactional writer, a
real work-lease service, the concurrent engine, the production result
commit factory, the bounded channel set, the capacity ledger, the
lifecycle and cleanup coordinators, and a scripted operation executor
whose behaviors, metrics, and retry eligibility are fully determined
by the injected clock and seed.

The scenario topology is a five-node barriered plan with two source
partitions and two partition-node partitions, so independent-node
overlap, partition overlap, dependency barriers, retries, quarantine,
artifacts, pause, cancellation, and recovery all have deterministic
coverage.  The plan fingerprint is derived canonically from the
topology so every strategy sees the same logical plan.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path

from paritygrid.adapters.artifacts import (
    FileSystemArtifactManifestRepository,
    FileSystemArtifactWriter,
)
from paritygrid.adapters.persistence import (
    SqlAlchemyExecutionEventRepository,
    SqlAlchemyPipelineRepository,
    SqlAlchemyRunRepository,
    SqlAlchemyWorkAttemptRepository,
    SqlAlchemyWorkItemRepository,
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    SQLitePauseStateReader,
    SQLiteResultCoordinatorReader,
    SQLiteTransactionalWriter,
    create_session_factory,
)
from paritygrid.adapters.persistence.concurrent_execution import (
    SQLiteAdmissionStateReader,
    SQLiteConcurrentRecoveryReader,
)
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.writer.core import WriterSettings
from paritygrid.application.execution import DurableResultCommitFactory
from paritygrid.application.execution.capacity import ScheduledWorkLimiters
from paritygrid.application.execution.channels import ChannelSet
from paritygrid.application.execution.concurrency_settings import (
    CapturedConcurrencySettings,
)
from paritygrid.application.execution.concurrent_cleanup import (
    ConcurrentCleanupCoordinator,
)
from paritygrid.application.execution.concurrent_engine import (
    AdmissionStateReader,
    ConcurrentRunEngine,
)
from paritygrid.application.execution.concurrent_lifecycle import (
    ConcurrentLifecycleCoordinator,
    ConcurrentPauseSignal,
)
from paritygrid.application.execution.concurrent_recovery import (
    ConcurrentRecoveryService,
)
from paritygrid.application.execution.concurrent_scheduler import (
    ConcurrentScheduler,
    WorkIdentity,
)
from paritygrid.application.execution.full_plan_strategy import (
    ExecutedWork,
    FullPlanStrategy,
    WorkOperationExecutor,
)
from paritygrid.application.execution.leasing import WorkLeaseService
from paritygrid.application.execution.result_coordinator import (
    ConcurrentResultCoordinator,
)
from paritygrid.application.execution.result_coordinator_writer import (
    TransactionalResultCoordinatorWriter,
)
from paritygrid.application.execution.runner import CancellationToken
from paritygrid.application.execution.runner_contract import (
    RUNNER_CONTRACT_VERSION,
    WORK_RESULT_PROTOCOL,
    ContractCleanupEvidence,
    ContractCleanupStatus,
    ContractMetric,
    ContractOutcome,
    ControlGeneration,
    WorkAssignmentV1,
    WorkResultV1,
)
from paritygrid.application.ports.artifacts import ArtifactRelativePath
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.writer import EventAppendRequest
from paritygrid.application.writes.execution import (
    BootstrapWork,
    CreateCapturedRun,
    TransitionRun,
)
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    ArtifactId,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

CONCURRENT_SCENARIO_VERSION = 1
PIPELINE_ID = PipelineId("pip_concurrent")
SOURCE_NODE = NodeId("nod_c-src")
NORMALIZE_NODE = NodeId("nod_c-norm")
VALIDATE_NODE = NodeId("nod_c-val")
PARTITION_NODE = NodeId("nod_c-part")
EXPORT_NODE = NodeId("nod_c-export")
NODE_ORDER: tuple[NodeId, ...] = (
    SOURCE_NODE,
    NORMALIZE_NODE,
    VALIDATE_NODE,
    PARTITION_NODE,
    EXPORT_NODE,
)
EDGES: tuple[tuple[NodeId, NodeId], ...] = (
    (SOURCE_NODE, NORMALIZE_NODE),
    (NORMALIZE_NODE, VALIDATE_NODE),
    (VALIDATE_NODE, PARTITION_NODE),
    (PARTITION_NODE, EXPORT_NODE),
)
PARTITIONS_BY_NODE: dict[str, tuple[str, ...]] = {
    str(SOURCE_NODE): ("partition-0", "partition-1"),
    str(NORMALIZE_NODE): ("partition-0",),
    str(VALIDATE_NODE): ("partition-0",),
    str(PARTITION_NODE): ("partition-0", "partition-1"),
    str(EXPORT_NODE): ("partition-0",),
}
NODE_KINDS: dict[str, str] = {
    str(SOURCE_NODE): "csv_source",
    str(NORMALIZE_NODE): "normalize_records",
    str(VALIDATE_NODE): "validate_records",
    str(PARTITION_NODE): "partition_records",
    str(EXPORT_NODE): "export_parquet",
}
ARTIFACT_NODES = frozenset({str(EXPORT_NODE)})

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_BASE = datetime(2026, 8, 24, 8, 0, 0, tzinfo=UTC)
_DEFAULT_RETRY_DELAY_MICROS = 1_000_000


def _to_micros(value: UtcTimestamp) -> int:
    delta = value.to_datetime() - _EPOCH
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def _from_micros(value: int) -> UtcTimestamp:
    return UtcTimestamp(_EPOCH + timedelta(microseconds=value))


def scenario_plan_fingerprint() -> str:
    """Return the canonical topology-derived plan fingerprint."""
    payload = "|".join(
        [
            ",".join(str(node) for node in NODE_ORDER),
            ";".join(f"{source}->{target}" for source, target in EDGES),
            ";".join(f"{node}:{','.join(parts)}" for node, parts in PARTITIONS_BY_NODE.items()),
        ]
    )
    return hashlib.sha256(payload.encode("ascii")).hexdigest()


def scenario_run_id(seed: int) -> RunId:
    """Return the deterministic run identity for one scenario seed."""
    return RunId(f"run_c-{seed:04d}")


def scenario_work_item_id(run_id: RunId, node_id: NodeId, partition_key: str) -> WorkItemId:
    """Return the deterministic work identity for one node partition."""
    node_short = str(node_id).removeprefix("nod_c-")
    partition_short = partition_key.removeprefix("partition-")
    run_short = str(run_id).removeprefix("run_c-")
    return WorkItemId(f"wrk_c-{run_short}-{node_short}-{partition_short}")


def scenario_artifact_id(run_id: RunId, node_id: NodeId, partition_key: str) -> str:
    """Return the deterministic artifact identity for one node partition."""
    work = scenario_work_item_id(run_id, node_id, partition_key)
    return f"art_{work.value.removeprefix('wrk_')}"


class StepClock:
    """Deterministic monotonic clock advanced only by explicit steps."""

    __slots__ = ("_microseconds",)

    def __init__(self, start: UtcTimestamp) -> None:
        self._microseconds = _to_micros(start)

    def now(self) -> UtcTimestamp:
        return _from_micros(self._microseconds)

    def advance(self, seconds: int) -> UtcTimestamp:
        self._microseconds += seconds * 1_000_000
        return self.now()

    def advance_to_micros(self, target: int) -> UtcTimestamp:
        """Advance exactly to a later injected instant."""
        if target < self._microseconds:
            raise ValueError("step clock never moves backwards")
        self._microseconds = target
        return self.now()

    @property
    def microseconds(self) -> int:
        return self._microseconds


class ConcurrentBehavior(StrEnum):
    """Closed scripted per-partition behaviors."""

    SUCCESS = "success"
    RETRY_THEN_SUCCESS = "retry_then_success"
    QUARANTINE = "quarantine"
    FAIL = "fail"
    ARTIFACT_SUCCESS = "artifact_success"


@dataclass(frozen=True, slots=True)
class ScenarioStep:
    """One immutable scripted partition behavior."""

    node_id: NodeId
    partition_key: str
    behavior: ConcurrentBehavior


DEFAULT_SCRIPT: tuple[ScenarioStep, ...] = tuple(
    ScenarioStep(node, partition, ConcurrentBehavior.SUCCESS)
    for node in NODE_ORDER
    for partition in PARTITIONS_BY_NODE[str(node)]
)


class ConcurrentScenarioHarness:
    """Owns every real resource of one concurrent scenario."""

    database: SQLiteDatabase
    writer: SQLiteTransactionalWriter
    artifact_root: Path
    artifact_writer: FileSystemArtifactWriter
    clock: StepClock
    pipeline_version: PipelineVersion
    plan_fingerprint: str

    __slots__ = (
        "artifact_root",
        "artifact_writer",
        "clock",
        "database",
        "pipeline_version",
        "plan_fingerprint",
        "writer",
    )

    def __init__(
        self,
        database: SQLiteDatabase,
        writer: SQLiteTransactionalWriter,
        artifact_root: Path,
        clock: StepClock,
        pipeline_version: PipelineVersion,
    ) -> None:
        self.database = database
        self.writer = writer
        self.artifact_root = artifact_root
        self.artifact_writer = FileSystemArtifactWriter(
            artifact_root, maximum_bytes=64 * 1024 * 1024
        )
        self.clock = clock
        self.pipeline_version = pipeline_version
        self.plan_fingerprint = scenario_plan_fingerprint()

    def close(self) -> None:
        """Release owned resources in reverse startup order."""
        self.writer.close(timeout_seconds=10.0)
        self.database.close()


def prepare_concurrent_harness(
    database_path: Path,
    artifact_root: Path,
) -> ConcurrentScenarioHarness:
    """Publish the scenario pipeline and open every owned scenario resource."""
    artifact_root.mkdir(parents=True, exist_ok=True)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(database_path, create_parent=True))
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    published_at = UtcTimestamp(_BASE)
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PIPELINE_ID,
            display_name="Concurrent scenario",
            description=None,
            created_at=published_at,
        )
        pipelines.publish_version(
            pipeline_id=PIPELINE_ID,
            expected_latest_version=None,
            specification=ConfigurationDocument.from_mapping({"nodes": []}),
            planner_format_version=1,
            published_at=published_at,
        )
    writer = SQLiteTransactionalWriter(
        create_session_factory(database.engine),
        WriterSettings(),
    )
    writer.start()
    return ConcurrentScenarioHarness(
        database=database,
        writer=writer,
        artifact_root=artifact_root,
        clock=StepClock(UtcTimestamp(_BASE)),
        pipeline_version=PipelineVersion(1),
    )


def bootstrap_scenario_run(
    harness: ConcurrentScenarioHarness,
    run_id: RunId,
) -> None:
    """Create the captured run and bootstrap every scheduled work item."""
    created_at = harness.clock.now()
    _submit(
        harness,
        CreateCapturedRun(
            run_id=run_id,
            pipeline_id=PIPELINE_ID,
            pipeline_version=harness.pipeline_version,
            runner_kind="concurrent",
            runner_configuration=ConfigurationDocument.from_mapping(
                {"scenario": CONCURRENT_SCENARIO_VERSION}
            ),
            scenario_seed=CONCURRENT_SCENARIO_VERSION,
            node_ids=NODE_ORDER,
            created_at=created_at,
            event=_run_event(1, 1, run_id, "run_created", created_at),
        ),
    )
    started_at = harness.clock.advance(1)
    _submit(
        harness,
        TransitionRun(
            run_id=run_id,
            expected_run_row_version=1,
            target_state=RunState.RUNNING,
            transitioned_at=started_at,
            execution_evidence_fingerprint=None,
            execution_evidence_fingerprint_version=None,
            event=_run_event(2, 2, run_id, "run_started", started_at),
        ),
    )
    sequence = 3
    with harness.database.transaction() as session:
        runs = SqlAlchemyRunRepository(session)
        run = runs.get(run_id)
        assert run is not None
    for plan_node in NODE_ORDER:
        for partition in PARTITIONS_BY_NODE[str(plan_node)]:
            with harness.database.transaction() as session:
                runs = SqlAlchemyRunRepository(session)
                run_record = runs.get(run_id)
                node_record = runs.get_node(run_id, plan_node)
                assert run_record is not None
                assert node_record is not None
            work_id = scenario_work_item_id(run_id, plan_node, partition)
            bootstrapped_at = harness.clock.advance(1)
            _submit(
                harness,
                BootstrapWork(
                    run_id=run_id,
                    node_id=plan_node,
                    work_item_id=work_id,
                    partition_key=PartitionKey(partition),
                    input_reference=None,
                    created_at=bootstrapped_at,
                    expected_node_row_version=node_record.row_version,
                    expected_run_row_version=run_record.row_version,
                    event=_work_event(sequence, sequence, work_id, "work_created", bootstrapped_at),
                ),
            )
            sequence += 1


class ScriptedConcurrentExecutor:
    """Deterministic strategy-neutral operation executor.

    Behaviors come from a frozen script keyed by node and partition;
    metrics, classifications, and retry eligibility derive only from
    the injected clock and the script, so the same seed produces the
    same durable evidence under every strategy.
    """

    __slots__ = (
        "_artifact_bodies",
        "_artifact_root",
        "_artifact_writer",
        "_clock",
        "_database",
        "_hooks",
        "_retry_delay_micros",
        "_script",
        "executed",
    )

    def __init__(
        self,
        harness: ConcurrentScenarioHarness,
        *,
        script: tuple[ScenarioStep, ...] = DEFAULT_SCRIPT,
        retry_delay_micros: int = _DEFAULT_RETRY_DELAY_MICROS,
        on_execute: Callable[[WorkAssignmentV1], None] | None = None,
    ) -> None:
        self._database = harness.database
        self._artifact_root = harness.artifact_root
        self._artifact_writer = harness.artifact_writer
        self._clock = harness.clock
        self._script = {(step.node_id, step.partition_key): step.behavior for step in script}
        self._retry_delay_micros = retry_delay_micros
        self._hooks = on_execute
        self._artifact_bodies: dict[str, bytes] = {}
        self.executed: list[tuple[str, str, int]] = []

    def execute(self, assignment: WorkAssignmentV1) -> ExecutedWork:
        """Execute one assignment exactly as its script prescribes."""
        if self._hooks is not None:
            self._hooks(assignment)
        node = NodeId(assignment.node_id)
        partition = assignment.partition_key
        behavior = self._script.get((node, partition), ConcurrentBehavior.SUCCESS)
        self.executed.append((assignment.node_id, partition, assignment.attempt_number))
        now = self._clock.now()
        now_micros = _to_micros(now)
        if behavior is ConcurrentBehavior.RETRY_THEN_SUCCESS and (assignment.attempt_number == 1):
            return self._envelope(
                assignment,
                ContractOutcome.RETRY_WAIT,
                classification="connection",
                retry_eligible_at_micros=now_micros + self._retry_delay_micros,
                failure_detail="scripted connection failure",
            )
        if behavior is ConcurrentBehavior.QUARANTINE:
            return self._envelope(
                assignment,
                ContractOutcome.QUARANTINED,
                classification="validation",
                failure_detail="scripted validation quarantine",
            )
        if behavior is ConcurrentBehavior.FAIL:
            return self._envelope(
                assignment,
                ContractOutcome.FAILED,
                classification="unknown",
                failure_detail="scripted permanent failure",
            )
        artifact_ids: tuple[str, ...] = ()
        if behavior is ConcurrentBehavior.ARTIFACT_SUCCESS or (
            assignment.node_id in ARTIFACT_NODES
        ):
            artifact_id = self._register_artifact(assignment, now)
            artifact_ids = (artifact_id,)
        return self._envelope(
            assignment,
            ContractOutcome.SUCCEEDED,
            classification=None,
            artifact_ids=artifact_ids,
        )

    def close(self) -> None:
        """Release nothing: the harness owns every resource."""

    def _register_artifact(self, assignment: WorkAssignmentV1, at: UtcTimestamp) -> str:
        run_id = RunId(assignment.run_id)
        node = NodeId(assignment.node_id)
        artifact_id = scenario_artifact_id(run_id, node, assignment.partition_key)
        if artifact_id in self._artifact_bodies:
            return artifact_id
        body = (
            hashlib.sha256(
                f"{assignment.run_id}:{assignment.node_id}:{assignment.partition_key}".encode(
                    "ascii"
                )
            )
            .hexdigest()
            .encode("ascii")
        )
        receipt = self._artifact_writer.write(
            ArtifactRelativePath(f"{assignment.run_id}/{artifact_id}.bin"),
            [body],
        )
        with self._database.transaction() as session:
            FileSystemArtifactManifestRepository(session, self._artifact_root).register(
                artifact_id=ArtifactId(artifact_id),
                run_id=run_id,
                node_id=node,
                partition_key=PartitionKey(assignment.partition_key),
                write_receipt=receipt,
                media_type="application/octet-stream",
                schema_version=1,
                row_count=1,
                created_at=at,
            )
        self._artifact_bodies[artifact_id] = body
        return artifact_id

    def _envelope(
        self,
        assignment: WorkAssignmentV1,
        outcome: ContractOutcome,
        *,
        classification: str | None,
        retry_eligible_at_micros: int = 0,
        failure_detail: str | None = None,
        artifact_ids: tuple[str, ...] = (),
    ) -> ExecutedWork:
        metrics = (
            (
                ContractMetric("records_read", 2),
                ContractMetric("records_written", 2),
                ContractMetric("bytes_read", 20),
                ContractMetric("bytes_written", 20),
            )
            if outcome is ContractOutcome.SUCCEEDED
            else (ContractMetric("records_read", 2), ContractMetric("bytes_read", 20))
        )
        result = WorkResultV1(
            protocol=WORK_RESULT_PROTOCOL,
            contract_version=RUNNER_CONTRACT_VERSION,
            plan_fingerprint=assignment.plan_fingerprint,
            run_id=assignment.run_id,
            node_id=assignment.node_id,
            partition_key=assignment.partition_key,
            work_item_id=assignment.work_item_id,
            attempt_number=assignment.attempt_number,
            lease_fence=assignment.lease_fence,
            lease_owner=assignment.lease_owner,
            control_generation=assignment.control_generation,
            outcome=outcome,
            metrics=metrics,
            artifact_references=artifact_ids,
            checkpoint_proposal=outcome is ContractOutcome.SUCCEEDED,
            failure_detail=failure_detail,
            cleanup=ContractCleanupEvidence(
                ContractCleanupStatus.COMPLETED,
                (),
                f"cleanup-{assignment.work_item_id}",
            ),
        )
        return ExecutedWork(
            result=result,
            failure_classification=classification,
            retry_eligible_at_micros=retry_eligible_at_micros,
        )


def build_scenario_engine(
    harness: ConcurrentScenarioHarness,
    run_id: RunId,
    *,
    strategy: FullPlanStrategy,
    executor: WorkOperationExecutor,
    settings: CapturedConcurrencySettings | None = None,
    lease_owner: str = "engine-main",
    correlation_id: str | None = "corr-concurrent-scenario",
) -> ConcurrentRunEngine:
    """Compose one engine over the real scenario resources."""
    captured = settings or CapturedConcurrencySettings()
    channels = ChannelSet(
        assignment_capacity=captured.assignment_channel_capacity,
        result_capacity=captured.result_channel_capacity,
        telemetry_capacity=captured.telemetry_capacity,
        writer_capacity=captured.writer_channel_capacity,
    )
    scheduler = ConcurrentScheduler(
        run_id=str(run_id),
        plan_fingerprint=harness.plan_fingerprint,
        node_order=tuple(str(node) for node in NODE_ORDER),
        edges=tuple((str(source), str(target)) for source, target in EDGES),
        partitions_by_node=dict(PARTITIONS_BY_NODE),
        control_generation=ControlGeneration(1),
    )
    capacity = ScheduledWorkLimiters(
        captured,
        strategy_id=strategy.strategy_id,
        node_ids=tuple(str(node) for node in NODE_ORDER),
        clock=harness.clock,
    )
    result_channel = channels.result
    coordinator = ConcurrentResultCoordinator(
        run_id=str(run_id),
        plan_fingerprint=harness.plan_fingerprint,
        control_generation=1,
        reader=SQLiteResultCoordinatorReader(harness.database, harness.clock),
        writer=TransactionalResultCoordinatorWriter(
            harness.writer,
            DurableResultCommitFactory(correlation_id=correlation_id),
        ),
        result_channel=result_channel,
        scheduler=scheduler,
        capacity=capacity,
    )
    lease_service = WorkLeaseService(harness.writer, harness.clock)
    lifecycle = ConcurrentLifecycleCoordinator(
        harness.writer,
        SQLitePauseStateReader(harness.database),
        harness.clock,
        correlation_id=correlation_id,
    )
    reader: AdmissionStateReader = SQLiteAdmissionStateReader(harness.database)

    def artifact_allowance(identity: WorkIdentity) -> tuple[str, ...]:
        if identity.node_id in ARTIFACT_NODES:
            artifact_id = scenario_artifact_id(
                RunId(identity.run_id), NodeId(identity.node_id), identity.partition_key
            )
            return (artifact_id,)
        return ()

    def clock_wait(target_micros: int) -> None:
        harness.clock.advance_to_micros(target_micros)

    return ConcurrentRunEngine(
        run_id=str(run_id),
        plan_fingerprint=harness.plan_fingerprint,
        node_order=tuple(str(node) for node in NODE_ORDER),
        edges=tuple((str(source), str(target)) for source, target in EDGES),
        partitions_by_node=dict(PARTITIONS_BY_NODE),
        node_kinds=dict(NODE_KINDS),
        settings=captured,
        clock=harness.clock,
        strategy=strategy,
        executor=executor,
        admission_reader=reader,
        lease_service=lease_service,
        lifecycle=lifecycle,
        coordinator=coordinator,
        channels=channels,
        capacity=capacity,
        pause_signal=ConcurrentPauseSignal(),
        cancellation=CancellationToken(),
        cleanup=ConcurrentCleanupCoordinator(),
        scheduler=scheduler,
        lease_owner=lease_owner,
        correlation_id=correlation_id,
        artifact_allowance=artifact_allowance,
        clock_wait=clock_wait,
    )


def scenario_recovery_service(
    harness: ConcurrentScenarioHarness,
) -> ConcurrentRecoveryService:
    """Build the exclusive recovery service over the scenario resources."""
    return ConcurrentRecoveryService(
        harness.writer,
        SQLiteConcurrentRecoveryReader(harness.database),
        harness.clock,
    )


@dataclass(frozen=True)
class ScenarioDurableEvidence:
    """Read-only durable evidence snapshot for assertions and comparison."""

    run_state: str
    run_row_version: int
    work_states: tuple[tuple[str, str, str, str], ...]
    attempt_outcomes: tuple[tuple[str, int, str], ...]
    event_kinds: tuple[str, ...]


def read_scenario_evidence(
    harness: ConcurrentScenarioHarness,
    run_id: RunId,
) -> ScenarioDurableEvidence:
    """Read one coherent durable evidence snapshot from SQLite."""
    with harness.database.transaction() as session:
        runs = SqlAlchemyRunRepository(session)
        work_repository = SqlAlchemyWorkItemRepository(session)
        attempts = SqlAlchemyWorkAttemptRepository(session)
        events = SqlAlchemyExecutionEventRepository(session)
        run = runs.get(run_id)
        assert run is not None
        page = work_repository.list_for_run(run_id, limit=100)
        items = list(page.items)
        while page.next_cursor is not None:
            page = work_repository.list_for_run(run_id, limit=100, after=page.next_cursor)
            items.extend(page.items)
        work_states = tuple(
            sorted(
                (
                    str(item.node_id),
                    str(item.partition_key),
                    str(item.work_item_id),
                    item.state.value,
                )
                for item in items
            )
        )
        attempt_rows: list[tuple[str, int, str]] = []
        for item in items:
            attempt_page = attempts.list_for_work_item(item.work_item_id, limit=100)
            attempt_rows.extend(
                (
                    str(item.work_item_id),
                    int(record.attempt_number),
                    record.outcome.value,
                )
                for record in attempt_page.items
            )
        event_rows = events.list_after(run_id, after=None, limit=100)
        event_kinds = tuple(record.event_kind for record in event_rows.items)
    return ScenarioDurableEvidence(
        run_state=run.state.value,
        run_row_version=run.row_version,
        work_states=work_states,
        attempt_outcomes=tuple(sorted(attempt_rows)),
        event_kinds=event_kinds,
    )


def _submit(
    harness: ConcurrentScenarioHarness,
    command: CreateCapturedRun | TransitionRun | BootstrapWork,
) -> None:
    receipt = harness.writer.submit(command, timeout_seconds=5.0)
    receipt.result(timeout_seconds=5.0)


def _run_event(
    sequence: int,
    counter: int,
    run_id: RunId,
    kind: str,
    at: UtcTimestamp,
) -> EventAppendRequest:
    return EventAppendRequest(
        EventSequence(sequence),
        counter,
        PendingExecutionEvent(
            kind,
            at,
            EventSubjectKind.RUN,
            run_id,
            "corr-concurrent-scenario",
            1,
            RedactedDocument.from_mapping({"kind": kind}),
        ),
    )


def _work_event(
    sequence: int,
    counter: int,
    work_item_id: WorkItemId,
    kind: str,
    at: UtcTimestamp,
) -> EventAppendRequest:
    return EventAppendRequest(
        EventSequence(sequence),
        counter,
        PendingExecutionEvent(
            kind,
            at,
            EventSubjectKind.WORK_ITEM,
            work_item_id,
            "corr-concurrent-scenario",
            1,
            RedactedDocument.from_mapping({"kind": kind}),
        ),
    )


__all__ = [
    "ARTIFACT_NODES",
    "DEFAULT_SCRIPT",
    "EDGES",
    "NODE_KINDS",
    "NODE_ORDER",
    "PARTITIONS_BY_NODE",
    "PIPELINE_ID",
    "ConcurrentBehavior",
    "ConcurrentScenarioHarness",
    "ScenarioDurableEvidence",
    "ScenarioStep",
    "ScriptedConcurrentExecutor",
    "StepClock",
    "bootstrap_scenario_run",
    "build_scenario_engine",
    "prepare_concurrent_harness",
    "read_scenario_evidence",
    "scenario_artifact_id",
    "scenario_plan_fingerprint",
    "scenario_recovery_service",
    "scenario_run_id",
    "scenario_work_item_id",
]
