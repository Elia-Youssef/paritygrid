"""Cross-runner verification manifest built from Phase 7 execution evidence.

The manifest runs one canonical plan through the three required full-plan
strategies — sequential, threaded, and asyncio — and compares the sorted,
durable execution evidence the accepted P7.17 harness freezes:
``ExecutionEvidenceSnapshot`` (evidence kind and version, plan identity,
sorted durable work states, attempt outcomes and counts, node aggregates,
artifact-manifest identities, normalized causal events, and the
execution-evidence fingerprint).

Correctness is established before any timing is recorded: wall-clock durations
are measured while each strategy runs but are attached to the manifest only
after every pairwise execution-evidence comparison is equal.  Timing, worker
identities, run-local identities, and concurrent global event order never
enter the comparison.

The manifest never claims that equal execution evidence proves equal
reconciliation classifications, repair plans, repair effects, or target
state; those claims are structurally absent from its closed field set and
explicitly disclaimed in every document.
"""

import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from math import isfinite
from pathlib import Path
from typing import cast

from paritygrid.adapters.artifacts import FileSystemArtifactManifestRepository
from paritygrid.adapters.persistence import SQLitePauseStateReader, SQLiteResultCoordinatorReader
from paritygrid.adapters.persistence.concurrent_execution import (
    SQLiteAdmissionStateReader,
)
from paritygrid.adapters.persistence.repositories import SqlAlchemyRunRepository
from paritygrid.application.execution import DurableResultCommitFactory
from paritygrid.application.execution.asyncio_strategy import AsyncioFullPlanStrategy
from paritygrid.application.execution.capacity import ScheduledWorkLimiters
from paritygrid.application.execution.channels import ChannelSet
from paritygrid.application.execution.concurrency_settings import CapturedConcurrencySettings
from paritygrid.application.execution.concurrent_cleanup import ConcurrentCleanupCoordinator
from paritygrid.application.execution.concurrent_engine import (
    AdmissionStateReader,
    ConcurrentRunEngine,
    EngineStatus,
)
from paritygrid.application.execution.concurrent_lifecycle import (
    ConcurrentLifecycleCoordinator,
    ConcurrentPauseSignal,
)
from paritygrid.application.execution.concurrent_scheduler import (
    ConcurrentScheduler,
    ControlGeneration,
    WorkIdentity,
)
from paritygrid.application.execution.evidence_comparison import (
    EXECUTION_EVIDENCE_COMPARISON_VERSION,
    EXECUTION_EVIDENCE_KIND,
    EvidenceComparison,
    ExecutionEvidenceSnapshot,
    build_evidence_snapshot,
    compare_execution_evidence,
)
from paritygrid.application.execution.full_plan_strategy import (
    ExecutedWork,
    FullPlanStrategy,
    SequentialFullPlanStrategy,
    WorkOperationExecutor,
)
from paritygrid.application.execution.leasing import WorkLeaseService
from paritygrid.application.execution.result_coordinator import (
    ConcurrentResultCoordinator,
    ResultCoordinatorWriter,
)
from paritygrid.application.execution.result_coordinator_writer import (
    TransactionalResultCoordinatorWriter,
)
from paritygrid.application.execution.runner import CancellationToken
from paritygrid.application.execution.runner_contract import ContractOutcome, WorkAssignmentV1
from paritygrid.application.execution.threaded_strategy import ThreadedFullPlanStrategy
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
from paritygrid.demo.datasets import WireValue, canonical_json_bytes
from paritygrid.demo.scenarios import (
    CANONICAL_CORRELATION_ID,
    CANONICAL_EDGES,
    CANONICAL_NODE_KINDS,
    CANONICAL_NODES,
    CANONICAL_PARTITIONS_BY_NODE,
    CANONICAL_SCENARIO_SEED,
    CANONICAL_SCENARIO_VERSION,
    canonical_plan_fingerprint,
)
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    ArtifactId,
    NodeId,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey
from paritygrid.quality.concurrent_scenario import (
    PIPELINE_ID,
    ConcurrentBehavior,
    ConcurrentScenarioHarness,
    ScenarioStep,
    ScriptedConcurrentExecutor,
    prepare_concurrent_harness,
    read_scenario_evidence,
)

RUNNER_MANIFEST_FORMAT = "paritygrid-cross-runner-verification"
RUNNER_MANIFEST_VERSION = 1
SEQUENTIAL_STRATEGY = "sequential"
THREADED_STRATEGY = "threaded"
ASYNCIO_STRATEGY = "asyncio"
REQUIRED_STRATEGIES: tuple[str, ...] = (
    SEQUENTIAL_STRATEGY,
    THREADED_STRATEGY,
    ASYNCIO_STRATEGY,
)

NON_EQUIVALENCE_DISCLAIMERS: tuple[str, ...] = (
    "execution-evidence equality does not prove reconciliation equality",
    "execution-evidence equality does not prove repair-plan equality",
    "execution-evidence equality does not prove repair-effect equality",
    "execution-evidence equality does not prove target-state equality",
)

_STRATEGY_TYPES: dict[str, type[FullPlanStrategy]] = {
    SEQUENTIAL_STRATEGY: SequentialFullPlanStrategy,
    THREADED_STRATEGY: ThreadedFullPlanStrategy,
    ASYNCIO_STRATEGY: AsyncioFullPlanStrategy,
}
_CANONICAL_RETRY_NODE = "nod_can-async-src"
_CANONICAL_ARTIFACT_NODE = "nod_can-export"


def _engine_run_suffix(run_id: str) -> str:
    """Return the run-local identity suffix used in derived identifiers.

    Canonical engine runs keep the accepted ``run_can-engine-`` shape.  A
    run created through the public API contributes its own payload after
    the ``run_`` prefix — already lowercase ASCII and dashes — so derived
    work, artifact, and checkpoint identities stay valid and unique.
    """
    if run_id.startswith("run_can-engine-"):
        return run_id.removeprefix("run_can-engine-")
    return run_id.removeprefix("run_")


class RunnerManifestError(ValueError):
    """Raised when a cross-runner manifest is built from invalid evidence."""


def canonical_engine_script() -> tuple[ScenarioStep, ...]:
    """Derive the engine-plane script from the canonical scenario story.

    The asynchronous source's work item carries the single scripted retry
    (the rate-limit fault) and every other work item succeeds, so the export
    partition publishes its artifact and the durable tail completes.  Row-level
    quarantine belongs to the scenario plane's analytical evidence: a
    quarantined work item is a blocked terminal that would fence the whole
    downstream tail, while the product story quarantines malformed rows during
    normalization without failing their source read.
    """
    steps: list[ScenarioStep] = []
    for node in CANONICAL_NODES:
        for partition in CANONICAL_PARTITIONS_BY_NODE[node]:
            if node == _CANONICAL_RETRY_NODE:
                behavior = ConcurrentBehavior.RETRY_THEN_SUCCESS
            elif node == _CANONICAL_ARTIFACT_NODE:
                behavior = ConcurrentBehavior.ARTIFACT_SUCCESS
            else:
                behavior = ConcurrentBehavior.SUCCESS
            steps.append(ScenarioStep(NodeId(node), partition, behavior))
    return tuple(steps)


class CanonicalEngineExecutor(ScriptedConcurrentExecutor):
    """The scripted executor over the canonical topology and story.

    The single retry is classified ``http_429`` exactly like the connector
    rate-limit fault of the scenario plane, and durable artifacts register
    only for the canonical export node.
    """

    __slots__ = ("_behaviors", "_canonical_bodies", "_harness")

    def __init__(self, harness: ConcurrentScenarioHarness) -> None:
        super().__init__(harness, script=canonical_engine_script())
        self._harness = harness
        self._canonical_bodies: dict[str, bytes] = {}
        self._behaviors: dict[tuple[str, str], ConcurrentBehavior] = {
            (str(step.node_id), step.partition_key): step.behavior
            for step in canonical_engine_script()
        }

    def execute(self, assignment: WorkAssignmentV1) -> ExecutedWork:
        node = str(assignment.node_id)
        partition = str(assignment.partition_key)
        behavior = self._behavior_for(node, partition)
        self.executed.append((node, partition, assignment.attempt_number))
        now = self._clock.now()
        now_micros = _to_micros(now)
        if behavior is ConcurrentBehavior.RETRY_THEN_SUCCESS and assignment.attempt_number == 1:
            return self._envelope(
                assignment,
                ContractOutcome.RETRY_WAIT,
                classification="http_429",
                retry_eligible_at_micros=now_micros + self._retry_delay_micros,
                failure_detail="scripted rate-limit fault",
            )
        if behavior is ConcurrentBehavior.QUARANTINE:
            return self._envelope(
                assignment,
                ContractOutcome.QUARANTINED,
                classification="validation",
                failure_detail="scripted validation quarantine",
            )
        artifact_ids: tuple[str, ...] = ()
        if behavior is ConcurrentBehavior.ARTIFACT_SUCCESS or node == _CANONICAL_ARTIFACT_NODE:
            artifact_ids = (self._register_canonical_artifact(assignment, now),)
        return self._envelope(
            assignment,
            ContractOutcome.SUCCEEDED,
            classification=None,
            artifact_ids=artifact_ids,
        )

    def _behavior_for(self, node: str, partition: str) -> ConcurrentBehavior:
        return self._behaviors.get((node, partition), ConcurrentBehavior.SUCCESS)

    def _register_canonical_artifact(self, assignment: WorkAssignmentV1, at: UtcTimestamp) -> str:
        run_id = str(assignment.run_id)
        node = str(assignment.node_id)
        partition = str(assignment.partition_key)
        run_suffix = _engine_run_suffix(run_id)
        artifact_id = f"art_can-e-{run_suffix}-{node.removeprefix('nod_can-')}-{partition}"
        if artifact_id in self._canonical_bodies:
            return artifact_id
        body = sha256(f"{run_id}:{node}:{partition}".encode("ascii")).hexdigest().encode("ascii")
        receipt = self._artifact_writer.write(
            ArtifactRelativePath(f"{run_id}/{artifact_id}.bin"),
            [body],
        )
        with self._harness.database.transaction() as session:
            FileSystemArtifactManifestRepository(session, self._harness.artifact_root).register(
                artifact_id=ArtifactId(artifact_id),
                run_id=RunId(run_id),
                node_id=NodeId(node),
                partition_key=PartitionKey(partition),
                write_receipt=receipt,
                media_type="application/octet-stream",
                schema_version=1,
                row_count=1,
                created_at=at,
            )
        self._canonical_bodies[artifact_id] = body
        return artifact_id


_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _to_micros(value: UtcTimestamp) -> int:
    delta = value.to_datetime() - _EPOCH
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


@dataclass(frozen=True, slots=True)
class RunnerExecutionRecord:
    """One strategy's durable execution evidence and optional timing."""

    strategy_id: str
    run_id: str
    evidence: ExecutionEvidenceSnapshot
    checkpoint_count: int
    checkpoints: tuple[tuple[str, str, int, int, str | None, str | None, str | None], ...]
    node_metrics: tuple[
        tuple[str, str, int, int, int, int, int, int, int, int, int, int, int, int, int], ...
    ]
    attempt_outcome_counts: dict[str, int]
    duration_seconds: float | None = None

    def __post_init__(self) -> None:
        if self.checkpoint_count != len(self.checkpoints):
            raise RunnerManifestError("checkpoint count must match the durable projection")
        if tuple(sorted(self.checkpoints)) != self.checkpoints:
            raise RunnerManifestError("durable checkpoints must be sorted")
        if tuple(sorted(self.node_metrics)) != self.node_metrics:
            raise RunnerManifestError("durable node metrics must be sorted")
        if self.duration_seconds is not None and (
            type(self.duration_seconds) is not float
            or not isfinite(self.duration_seconds)
            or self.duration_seconds < 0.0
        ):
            raise RunnerManifestError("runner duration must be a finite nonnegative float")

    def canonical_mapping(self) -> dict[str, object]:
        """Return the bounded document for this record."""
        return {
            "attempt_outcome_counts": dict(sorted(self.attempt_outcome_counts.items())),
            "checkpoint_count": self.checkpoint_count,
            "checkpoints": [list(checkpoint) for checkpoint in self.checkpoints],
            "execution_evidence_fingerprint": self.evidence.execution_evidence_fingerprint,
            "node_metrics": [list(metrics) for metrics in self.node_metrics],
            "run_id": self.run_id,
            "strategy_id": self.strategy_id,
        }


@dataclass(frozen=True, slots=True)
class CrossRunnerVerificationManifest:
    """The correctness-first comparison of every required strategy.

    The closed field set carries execution evidence only: no field could
    express reconciliation, repair, or target-state equivalence.
    """

    scenario_version: int
    plan_fingerprint: str
    evidence_kind: str
    evidence_version: int
    comparison_version: int
    records: tuple[RunnerExecutionRecord, ...]
    comparisons: tuple[EvidenceComparison, ...]
    timings_recorded: bool
    manifest_version: int = RUNNER_MANIFEST_VERSION

    @property
    def equal(self) -> bool:
        """Return whether every pairwise comparison is equal."""
        return all(comparison.equal for comparison in self.comparisons)

    @property
    def differences(self) -> tuple[str, ...]:
        """Return every distinct difference across the pairwise comparisons."""
        differences: list[str] = []
        for comparison in self.comparisons:
            differences.extend(comparison.differences)
        return tuple(dict.fromkeys(differences))

    def canonical_bytes(self) -> bytes:
        """Return the byte-stable manifest document."""
        document = {
            "comparisons": [
                {"differences": list(comparison.differences), "equal": comparison.equal}
                for comparison in self.comparisons
            ],
            "evidence_kind": self.evidence_kind,
            "evidence_version": self.evidence_version,
            "comparison_version": self.comparison_version,
            "format": RUNNER_MANIFEST_FORMAT,
            "manifest_version": self.manifest_version,
            "non_equivalence_disclaimers": list(NON_EQUIVALENCE_DISCLAIMERS),
            "plan_fingerprint": self.plan_fingerprint,
            "records": [record.canonical_mapping() for record in self.records],
            "scenario_version": self.scenario_version,
            "timings_recorded": self.timings_recorded,
        }
        return canonical_json_bytes(cast("Mapping[str, WireValue]", document))


def prepare_harness(root: Path) -> ConcurrentScenarioHarness:
    """Prepare the engine-plane harness beneath an isolated root."""
    return prepare_concurrent_harness(root / "engine.db", root / "artifacts")


def canonical_run_id(seed: int) -> RunId:
    """Return the deterministic engine-plane run identity for one seed."""
    return RunId(f"run_can-engine-{seed:04d}")


def freeze_runner_record(
    harness: ConcurrentScenarioHarness,
    strategy_id: str,
    run_id: RunId,
    *,
    execution_evidence_fingerprint: str | None = None,
) -> RunnerExecutionRecord:
    """Read one run's durable evidence into a comparable record."""
    evidence = read_scenario_evidence(harness, run_id)
    work_positions = {
        work_id: (node, partition) for node, partition, work_id, _state in evidence.work_states
    }
    attempt_outcomes = tuple(
        (f"{node}/{partition}", attempt, outcome)
        for work_id, attempt, outcome in evidence.attempt_outcomes
        for node, partition in [work_positions[work_id]]
    )
    counts: dict[str, int] = {}
    for _identity, _attempt, outcome in attempt_outcomes:
        counts[outcome] = counts.get(outcome, 0) + 1
    # Artifact identifiers embed their run-local directory identity, so the
    # snapshot normalizes them exactly like every other run-local identity
    # before strategies are compared.
    run_suffix = _engine_run_suffix(str(run_id))
    normalized_artifacts = tuple(
        sorted(
            identity.replace(f"-{run_suffix}-", "-", 1)
            for identity in _artifact_identities(harness, run_id)
        )
    )
    node_aggregates, node_metrics = _node_projections(harness, run_id)
    snapshot = build_evidence_snapshot(
        run_id=run_id.value,
        plan_fingerprint=canonical_plan_fingerprint(),
        work_states=tuple(
            (node, partition, state) for node, partition, _work, state in evidence.work_states
        ),
        attempt_outcomes=attempt_outcomes,
        node_aggregates=node_aggregates,
        artifact_identities=normalized_artifacts,
        event_kinds=evidence.event_kinds,
        execution_evidence_fingerprint=execution_evidence_fingerprint,
    )
    checkpoints = _checkpoint_projection(harness, run_id)
    return RunnerExecutionRecord(
        strategy_id=strategy_id,
        run_id=run_id.value,
        evidence=snapshot,
        checkpoint_count=len(checkpoints),
        checkpoints=checkpoints,
        node_metrics=node_metrics,
        attempt_outcome_counts=counts,
    )


def _artifact_identities(harness: ConcurrentScenarioHarness, run_id: RunId) -> tuple[str, ...]:
    """Read the durable artifact-manifest identities of one run."""
    from sqlalchemy import select

    from paritygrid.adapters.persistence.schema import artifact_manifests

    with harness.database.transaction() as session:
        identities = (
            session.execute(
                select(artifact_manifests.c.artifact_id).where(
                    artifact_manifests.c.run_id == run_id.value
                )
            )
            .scalars()
            .all()
        )
    return tuple(identities)


def build_cross_runner_manifest(
    records: tuple[RunnerExecutionRecord, ...],
    durations: dict[str, float],
) -> CrossRunnerVerificationManifest:
    """Compare the strategies first, then attach timing only when equal.

    Every required strategy must be present exactly once, in the required
    order.  Durations attach to the records only when every pairwise
    execution-evidence comparison is equal; an unequal manifest never records
    timing.
    """
    if tuple(record.strategy_id for record in records) != REQUIRED_STRATEGIES:
        raise RunnerManifestError(
            "the cross-runner manifest requires exactly the ordered required strategies"
        )
    if set(durations) - set(REQUIRED_STRATEGIES):
        raise RunnerManifestError("durations carry an unknown runner strategy")
    if any(
        type(duration) is not float or not isfinite(duration) or duration < 0.0
        for duration in durations.values()
    ):
        raise RunnerManifestError("durations must be finite nonnegative floats")
    comparisons: list[EvidenceComparison] = [
        _compare_runner_records(records[first], records[second])
        for first in range(len(records))
        for second in range(first + 1, len(records))
    ]
    equal = all(comparison.equal for comparison in comparisons)
    # Timing is recorded only when correctness held AND every strategy was
    # measured; a partial durations dict records nothing.
    timings_recorded = (
        equal and bool(records) and all(record.strategy_id in durations for record in records)
    )
    evidence_versions = {record.evidence.evidence_version for record in records}
    if len(evidence_versions) != 1:
        raise RunnerManifestError("the compared snapshots must share one evidence version")
    timed_records = tuple(
        record
        if not timings_recorded
        else RunnerExecutionRecord(
            strategy_id=record.strategy_id,
            run_id=record.run_id,
            evidence=record.evidence,
            checkpoint_count=record.checkpoint_count,
            checkpoints=record.checkpoints,
            node_metrics=record.node_metrics,
            attempt_outcome_counts=record.attempt_outcome_counts,
            duration_seconds=durations[record.strategy_id],
        )
        for record in records
    )
    return CrossRunnerVerificationManifest(
        scenario_version=CANONICAL_SCENARIO_VERSION,
        plan_fingerprint=canonical_plan_fingerprint(),
        evidence_kind=EXECUTION_EVIDENCE_KIND,
        evidence_version=next(iter(evidence_versions)),
        comparison_version=EXECUTION_EVIDENCE_COMPARISON_VERSION,
        records=timed_records,
        comparisons=tuple(comparisons),
        timings_recorded=timings_recorded,
    )


def run_canonical_strategy(
    harness: ConcurrentScenarioHarness,
    strategy_id: str,
    run_id: RunId,
) -> RunnerExecutionRecord:
    """Execute one canonical engine run and freeze its durable evidence."""
    strategy = _STRATEGY_TYPES[strategy_id]()
    bootstrap_canonical_run(harness, run_id)
    executor = CanonicalEngineExecutor(harness)
    engine = build_canonical_engine(harness, run_id, strategy=strategy, executor=executor)
    report = engine.run()
    if report.status is not EngineStatus.COMPLETED:
        raise RunnerManifestError(f"the canonical engine run did not complete: {strategy_id}")
    fingerprint = _finalize_engine_run(harness, run_id)
    return freeze_runner_record(
        harness,
        strategy_id,
        run_id,
        execution_evidence_fingerprint=fingerprint,
    )


def _finalize_engine_run(harness: ConcurrentScenarioHarness, run_id: RunId) -> str:
    """Finalize one engine run and return its execution-evidence fingerprint."""
    from paritygrid.adapters.analytics.duckdb import DuckDBLifecycleCoordinator
    from paritygrid.adapters.analytics.run_statistics import DuckDBRunStatisticsQueryEngine
    from paritygrid.adapters.persistence import SQLiteFinalizationStateReader
    from paritygrid.application.execution.finalization import FinalizationSettings, RunFinalizer
    from paritygrid.application.planner import PlanFingerprint
    from paritygrid.application.ports.analytics import AnalyticalDatabaseConfig

    analytics_path = harness.artifact_root / "engine-analytics.duckdb"
    coordinator = DuckDBLifecycleCoordinator(AnalyticalDatabaseConfig(analytics_path.resolve()))
    coordinator.open()
    try:
        finalizer = RunFinalizer(
            harness.writer,
            SQLiteFinalizationStateReader(harness.database),
            DuckDBRunStatisticsQueryEngine(coordinator),
            harness.clock,
            settings=FinalizationSettings(5.0, 5.0),
        )
        finalization = finalizer.finalize(
            run_id,
            plan_nodes=tuple(NodeId(node) for node in CANONICAL_NODES),
            plan_fingerprint=PlanFingerprint(canonical_plan_fingerprint()),
        )
    finally:
        coordinator.close()
    if finalization.fingerprint is None:
        raise RunnerManifestError("the engine run finalized without a fingerprint")
    return finalization.fingerprint.value


def build_cross_runner_verification(
    harness: ConcurrentScenarioHarness,
) -> CrossRunnerVerificationManifest:
    """Run every required strategy over the canonical plan and compare.

    Correctness equality is established before timing is recorded: durations
    are measured per strategy but attach only to an equal manifest.
    """
    records: list[RunnerExecutionRecord] = []
    durations: dict[str, float] = {}
    for offset, strategy_id in enumerate(REQUIRED_STRATEGIES, start=1):
        run_id = canonical_run_id(offset)
        started = time.perf_counter()
        record = run_canonical_strategy(harness, strategy_id, run_id)
        durations[strategy_id] = time.perf_counter() - started
        records.append(record)
    return build_cross_runner_manifest(tuple(records), durations)


def bootstrap_canonical_run(
    harness: ConcurrentScenarioHarness,
    run_id: RunId,
) -> None:
    """Create the captured engine-plane run and bootstrap every work item."""
    created_at = harness.clock.now()
    _submit(
        harness,
        CreateCapturedRun(
            run_id=run_id,
            pipeline_id=PIPELINE_ID,
            pipeline_version=harness.pipeline_version,
            runner_kind="concurrent",
            runner_configuration=ConfigurationDocument.from_mapping(
                {"scenario": CANONICAL_SCENARIO_VERSION}
            ),
            scenario_seed=CANONICAL_SCENARIO_SEED,
            node_ids=tuple(NodeId(node) for node in CANONICAL_NODES),
            created_at=created_at,
            event=_run_event(1, run_id, "run_created", created_at),
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
            event=_run_event(2, run_id, "run_started", started_at),
        ),
    )
    sequence = 3
    for node in CANONICAL_NODES:
        for partition in CANONICAL_PARTITIONS_BY_NODE[node]:
            node_id = NodeId(node)
            run_suffix = _engine_run_suffix(str(run_id))
            work_item_id = WorkItemId(
                f"wrk_can-e-{run_suffix}-{node.removeprefix('nod_can-')}-{partition}"
            )
            with harness.database.transaction() as session:
                runs = SqlAlchemyRunRepository(session)
                run_record = runs.get(run_id)
                node_record = runs.get_node(run_id, node_id)
            if run_record is None or node_record is None:
                raise RunnerManifestError("the engine-plane run disappeared during bootstrap")
            bootstrapped_at = harness.clock.advance(1)
            _submit(
                harness,
                BootstrapWork(
                    run_id=run_id,
                    node_id=node_id,
                    work_item_id=work_item_id,
                    partition_key=PartitionKey(partition),
                    input_reference=None,
                    created_at=bootstrapped_at,
                    expected_node_row_version=node_record.row_version,
                    expected_run_row_version=run_record.row_version,
                    event=_work_event(sequence, work_item_id, "work_created", bootstrapped_at),
                ),
            )
            sequence += 1


def build_canonical_engine(
    harness: ConcurrentScenarioHarness,
    run_id: RunId,
    *,
    strategy: FullPlanStrategy,
    executor: WorkOperationExecutor,
) -> ConcurrentRunEngine:
    """Compose one canonical engine over the accepted Phase 7 components."""
    engine, _channels = build_canonical_engine_with_observation(
        harness, run_id, strategy=strategy, executor=executor
    )
    return engine


def build_canonical_engine_with_observation(
    harness: ConcurrentScenarioHarness,
    run_id: RunId,
    *,
    strategy: FullPlanStrategy,
    executor: WorkOperationExecutor,
    admission_reader: AdmissionStateReader | None = None,
    result_writer: ResultCoordinatorWriter | None = None,
) -> tuple[ConcurrentRunEngine, ChannelSet]:
    """Compose one canonical engine and expose its owned channels.

    Observability-minded callers (the performance harness and resource
    bound exercises) need the ``ChannelSet`` after the run to read real
    per-channel high-water marks, and may supply an alternative admission
    reader with the accepted ``read`` surface to timestamp admissions or
    an alternative result writer with the accepted ``submit`` surface to
    time durable commit transactions.  The composition is otherwise
    identical to :func:`build_canonical_engine`; the engine stays the
    sole owner of the returned channels and closes them during its own
    cleanup.
    """
    captured = CapturedConcurrencySettings()
    channels = ChannelSet(
        assignment_capacity=captured.assignment_channel_capacity,
        result_capacity=captured.result_channel_capacity,
        telemetry_capacity=captured.telemetry_capacity,
        writer_capacity=captured.writer_channel_capacity,
    )
    scheduler = ConcurrentScheduler(
        run_id=str(run_id),
        plan_fingerprint=canonical_plan_fingerprint(),
        node_order=CANONICAL_NODES,
        edges=tuple((source, target) for source, target in CANONICAL_EDGES),
        partitions_by_node=dict(CANONICAL_PARTITIONS_BY_NODE),
        control_generation=ControlGeneration(1),
    )
    capacity = ScheduledWorkLimiters(
        captured,
        strategy_id=strategy.strategy_id,
        node_ids=CANONICAL_NODES,
        clock=harness.clock,
    )
    resolved_result_writer = (
        result_writer
        if result_writer is not None
        else TransactionalResultCoordinatorWriter(
            harness.writer,
            DurableResultCommitFactory(correlation_id=CANONICAL_CORRELATION_ID),
        )
    )
    resolved_admission_reader = (
        admission_reader
        if admission_reader is not None
        else SQLiteAdmissionStateReader(harness.database)
    )
    coordinator = ConcurrentResultCoordinator(
        run_id=str(run_id),
        plan_fingerprint=canonical_plan_fingerprint(),
        control_generation=1,
        reader=SQLiteResultCoordinatorReader(harness.database, harness.clock),
        writer=resolved_result_writer,
        result_channel=channels.result,
        scheduler=scheduler,
        capacity=capacity,
    )
    lease_service = WorkLeaseService(harness.writer, harness.clock)
    lifecycle = ConcurrentLifecycleCoordinator(
        harness.writer,
        SQLitePauseStateReader(harness.database),
        harness.clock,
        correlation_id=CANONICAL_CORRELATION_ID,
    )

    def artifact_allowance(identity: WorkIdentity) -> tuple[str, ...]:
        node = identity.node_id
        if node == _CANONICAL_ARTIFACT_NODE:
            run_suffix = _engine_run_suffix(str(identity.run_id))
            return (
                f"art_can-e-{run_suffix}-{node.removeprefix('nod_can-')}-{identity.partition_key}",
            )
        return ()

    def clock_wait(target_micros: int) -> None:
        harness.clock.advance_to_micros(target_micros)

    engine = ConcurrentRunEngine(
        run_id=str(run_id),
        plan_fingerprint=canonical_plan_fingerprint(),
        node_order=CANONICAL_NODES,
        edges=tuple((source, target) for source, target in CANONICAL_EDGES),
        partitions_by_node=dict(CANONICAL_PARTITIONS_BY_NODE),
        node_kinds=dict(CANONICAL_NODE_KINDS),
        settings=captured,
        clock=harness.clock,
        strategy=strategy,
        executor=executor,
        admission_reader=resolved_admission_reader,
        lease_service=lease_service,
        lifecycle=lifecycle,
        coordinator=coordinator,
        channels=channels,
        capacity=capacity,
        pause_signal=ConcurrentPauseSignal(),
        cancellation=CancellationToken(),
        cleanup=ConcurrentCleanupCoordinator(),
        scheduler=scheduler,
        lease_owner="canonical-engine",
        correlation_id=CANONICAL_CORRELATION_ID,
        artifact_allowance=artifact_allowance,
        clock_wait=clock_wait,
    )
    return engine, channels


def _submit(
    harness: ConcurrentScenarioHarness,
    command: CreateCapturedRun | TransitionRun | BootstrapWork,
) -> None:
    receipt = harness.writer.submit(command, timeout_seconds=5.0)
    receipt.result(timeout_seconds=60.0)


def _run_event(sequence: int, run_id: RunId, kind: str, at: object) -> EventAppendRequest:
    return EventAppendRequest(
        EventSequence(sequence),
        sequence,
        PendingExecutionEvent(
            kind,
            at,  # type: ignore[arg-type]
            EventSubjectKind.RUN,
            run_id,
            CANONICAL_CORRELATION_ID,
            1,
            RedactedDocument.from_mapping({"kind": kind}),
        ),
    )


def _work_event(
    sequence: int, work_item_id: WorkItemId, kind: str, at: object
) -> EventAppendRequest:
    return EventAppendRequest(
        EventSequence(sequence),
        sequence,
        PendingExecutionEvent(
            kind,
            at,  # type: ignore[arg-type]
            EventSubjectKind.WORK_ITEM,
            work_item_id,
            CANONICAL_CORRELATION_ID,
            1,
            RedactedDocument.from_mapping({"kind": kind}),
        ),
    )


def _compare_runner_records(
    left: RunnerExecutionRecord,
    right: RunnerExecutionRecord,
) -> EvidenceComparison:
    """Compare accepted execution evidence plus the Phase 19 checkpoint projection."""
    evidence = compare_execution_evidence(left.evidence, right.evidence)
    differences = list(evidence.differences)
    if left.checkpoints != right.checkpoints:
        differences.append("durable checkpoints differ")
    if left.node_metrics != right.node_metrics:
        differences.append("durable node metrics differ")
    if differences:
        return EvidenceComparison(equal=False, differences=tuple(differences))
    return EvidenceComparison(equal=True, differences=())


def _checkpoint_projection(
    harness: ConcurrentScenarioHarness,
    run_id: RunId,
) -> tuple[tuple[str, str, int, int, str | None, str | None, str | None], ...]:
    """Read the sorted, run-neutral durable checkpoint projection."""
    from sqlalchemy import select

    from paritygrid.adapters.persistence.schema import checkpoints

    with harness.database.transaction() as session:
        rows = session.execute(
            select(
                checkpoints.c.node_id,
                checkpoints.c.partition_key,
                checkpoints.c.version,
                checkpoints.c.payload_schema_version,
                checkpoints.c.source_cursor_json,
                checkpoints.c.output_position_json,
                checkpoints.c.artifact_id,
            ).where(checkpoints.c.run_id == run_id.value)
        ).all()
    run_suffix = _engine_run_suffix(str(run_id))
    return tuple(
        sorted(
            (
                str(row.node_id),
                str(row.partition_key),
                int(row.version),
                int(row.payload_schema_version),
                None if row.source_cursor_json is None else str(row.source_cursor_json),
                None if row.output_position_json is None else str(row.output_position_json),
                None
                if row.artifact_id is None
                else str(row.artifact_id).replace(f"-{run_suffix}-", "-", 1),
            )
            for row in rows
        )
    )


def _node_projections(
    harness: ConcurrentScenarioHarness,
    run_id: RunId,
) -> tuple[
    tuple[tuple[str, int, int, int, int, int], ...],
    tuple[
        tuple[str, str, int, int, int, int, int, int, int, int, int, int, int, int, int],
        ...,
    ],
]:
    """Read stable aggregate and full metric projections from durable node rows."""
    from sqlalchemy import select

    from paritygrid.adapters.persistence.schema import run_nodes

    with harness.database.transaction() as session:
        rows = session.execute(
            select(
                run_nodes.c.node_id,
                run_nodes.c.state,
                run_nodes.c.work_total,
                run_nodes.c.work_pending,
                run_nodes.c.work_running,
                run_nodes.c.work_succeeded,
                run_nodes.c.work_quarantined,
                run_nodes.c.work_failed,
                run_nodes.c.work_cancelled,
                run_nodes.c.records_read,
                run_nodes.c.records_written,
                run_nodes.c.records_quarantined,
                run_nodes.c.bytes_read,
                run_nodes.c.bytes_written,
                run_nodes.c.retry_count,
            ).where(run_nodes.c.run_id == run_id.value)
        ).all()
    aggregates = tuple(
        sorted(
            (
                str(row.node_id),
                int(row.work_total),
                int(row.work_succeeded),
                int(row.work_quarantined),
                int(row.work_failed),
                int(row.work_cancelled),
            )
            for row in rows
        )
    )
    metrics = tuple(
        sorted(
            (
                str(row.node_id),
                str(row.state),
                int(row.work_total),
                int(row.work_pending),
                int(row.work_running),
                int(row.work_succeeded),
                int(row.work_quarantined),
                int(row.work_failed),
                int(row.work_cancelled),
                int(row.records_read),
                int(row.records_written),
                int(row.records_quarantined),
                int(row.bytes_read),
                int(row.bytes_written),
                int(row.retry_count),
            )
            for row in rows
        )
    )
    return aggregates, metrics
