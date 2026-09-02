"""Canonical engine-plane runs over the composed runtime resources.

The demo executes the accepted Phase 19 engine story — the canonical topology
driven by the scripted canonical executor through a real
``ConcurrentRunEngine`` — directly against the runtime's database, writer,
and artifact root, so every engine run is visible through the ordinary
application boundary.  The ``--runner`` selection chooses the full-plan
strategy; only sequential, threaded, and asyncio exist here, and a requested
strategy is never silently substituted.

Two clock modes exist.  The injected deterministic clock makes headless smoke
runs reproducible and instant.  The real-time pacing clock serves the live
product so a browser can observe progress, retries, pause, and cancellation;
timing never enters canonical correctness bytes.
"""

import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from sqlalchemy import select

from paritygrid.adapters.analytics.duckdb import DuckDBLifecycleCoordinator
from paritygrid.adapters.analytics.run_statistics import DuckDBRunStatisticsQueryEngine
from paritygrid.adapters.persistence import (
    SQLiteDatabase,
    SQLiteFinalizationStateReader,
    SQLiteTransactionalWriter,
)
from paritygrid.adapters.persistence.repositories import SqlAlchemyRunRepository
from paritygrid.adapters.persistence.schema import runs as runs_table
from paritygrid.application.execution.asyncio_strategy import AsyncioFullPlanStrategy
from paritygrid.application.execution.concurrent_engine import EngineStatus
from paritygrid.application.execution.finalization import FinalizationSettings, RunFinalizer
from paritygrid.application.execution.full_plan_strategy import (
    FullPlanStrategy,
    SequentialFullPlanStrategy,
)
from paritygrid.application.execution.threaded_strategy import ThreadedFullPlanStrategy
from paritygrid.application.planner import PlanFingerprint
from paritygrid.application.ports.analytics import AnalyticalDatabaseConfig
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
from paritygrid.demo.datasets import WireValue
from paritygrid.demo.scenarios import (
    CANONICAL_CORRELATION_ID,
    CANONICAL_PIPELINE_ID,
    CANONICAL_SCENARIO_SEED,
    canonical_plan_fingerprint,
)
from paritygrid.demo.verification import (
    CanonicalEngineExecutor,
    CrossRunnerVerificationManifest,
    RunnerExecutionRecord,
    RunnerManifestError,
    build_canonical_engine,
    canonical_engine_script,
    freeze_runner_record,
)
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey
from paritygrid.quality.concurrent_scenario import ConcurrentScenarioHarness, StepClock

EngineClockMode = Literal["injected", "realtime"]
ENGINE_STRATEGIES: tuple[str, ...] = ("sequential", "threaded", "asyncio")
# Stable per-runner engine run offsets — exactly the identities the accepted
# Phase 19 cross-runner manifest freezes (run_can-engine-0001..0003).
ENGINE_RUN_OFFSETS: dict[str, int] = {
    "sequential": 1,
    "threaded": 2,
    "asyncio": 3,
}
ENGINE_RETRY_NODE = "nod_can-async-src"
ENGINE_RUN_PREFIX = "run_can-engine"
_STRATEGY_TYPES: dict[str, type[FullPlanStrategy]] = {
    "sequential": SequentialFullPlanStrategy,
    "threaded": ThreadedFullPlanStrategy,
    "asyncio": AsyncioFullPlanStrategy,
}
_MAX_REALTIME_WAIT_SECONDS = 30.0


class DemoEngineError(RuntimeError):
    """Raised when a canonical engine-plane run cannot be executed or verified."""


class RealtimePacingClock:
    """Wall-clock clock with the StepClock surface for live engine runs.

    Retry eligibility and rate tokens share one wall-microsecond timeline, so
    ``advance_to_micros`` sleeps until the real instant instead of jumping,
    which keeps scripted retry behavior observable without changing which
    decisions the engine records.  ``advance`` only moves the timestamp
    baseline used for bootstrap bookkeeping and never sleeps: a launcher run
    must become controllable through its registered owner immediately after
    its durable start transition.
    """

    __slots__ = ("_lock",)

    def __init__(self) -> None:
        self._lock = threading.Lock()

    @property
    def microseconds(self) -> int:
        return time.time_ns() // 1_000

    def now(self) -> UtcTimestamp:
        return UtcTimestamp(datetime.now(UTC))

    def advance(self, seconds: int) -> UtcTimestamp:
        del seconds
        return self.now()

    def advance_to_micros(self, target: int) -> UtcTimestamp:
        self._wait(target - self.microseconds)
        return self.now()

    def _wait(self, microseconds: float) -> None:
        seconds = max(0.0, microseconds / 1_000_000)
        if seconds > _MAX_REALTIME_WAIT_SECONDS:
            raise DemoEngineError("a realtime engine wait exceeded its bounded budget")
        with self._lock:
            time.sleep(seconds)


def injected_engine_clock() -> StepClock:
    """Return the demo's injected engine clock, anchored at the current instant.

    The clock still advances only by explicit deterministic steps, so every
    retry eligibility and rate decision is reproducible relative to its
    anchor.  The anchor follows real time because the demo publishes through
    the runtime's real-time services and run creation must follow
    publication; timestamps never enter canonical correctness bytes.
    """
    return StepClock(UtcTimestamp(datetime.now(UTC)))


def demo_engine_harness(
    database: SQLiteDatabase,
    writer: SQLiteTransactionalWriter,
    artifact_root: Path,
    clock: StepClock | RealtimePacingClock,
    pipeline_version: PipelineVersion,
) -> ConcurrentScenarioHarness:
    """Wrap the runtime's owned resources in the accepted engine harness view.

    The harness is a read/write view, not an owner: the composed runtime keeps
    ownership of the database and writer, so
    ``ConcurrentScenarioHarness.close`` must never be called on this instance.
    """
    return ConcurrentScenarioHarness(
        database=database,
        writer=writer,
        artifact_root=artifact_root,
        clock=clock,  # type: ignore[arg-type]
        pipeline_version=pipeline_version,
    )


def canonical_engine_nodes() -> tuple[str, ...]:
    """Return the canonical engine node order, unique per node."""
    from paritygrid.demo.scenarios import CANONICAL_NODES

    return CANONICAL_NODES


def canonical_engine_partitions() -> dict[str, tuple[str, ...]]:
    """Return the partition layout the canonical engine script executes."""
    partitions: dict[str, tuple[str, ...]] = {}
    for step in canonical_engine_script():
        partitions.setdefault(str(step.node_id), ())
        partitions[str(step.node_id)] = (
            *partitions[str(step.node_id)],
            step.partition_key,
        )
    return partitions


def engine_work_item_id(run_id: RunId, node: str, partition: str) -> WorkItemId:
    """Return the deterministic engine work-item identity.

    Canonical engine runs keep the accepted Phase 19 identity shape.  A run
    created through the public API reuses its own identity payload after the
    ``run_`` prefix — already restricted to lowercase ASCII and dashes — so
    the derived work identity stays valid and unique per run.
    """
    value = str(run_id)
    if value.startswith(f"{ENGINE_RUN_PREFIX}-"):
        suffix = value.removeprefix(f"{ENGINE_RUN_PREFIX}-")
    else:
        suffix = value.removeprefix("run_")
    return WorkItemId(f"wrk_can-e-{suffix}-{node.removeprefix('nod_can-')}-{partition}")


def bootstrap_engine_run(
    harness: ConcurrentScenarioHarness,
    run_id: RunId,
    *,
    runner_configuration: dict[str, WireValue],
) -> None:
    """Create the captured canonical engine run and bootstrap every work item."""
    created_at = harness.clock.now()
    _submit(
        harness,
        CreateCapturedRun(
            run_id=run_id,
            pipeline_id=PipelineId(CANONICAL_PIPELINE_ID),
            pipeline_version=harness.pipeline_version,
            runner_kind="concurrent",
            runner_configuration=ConfigurationDocument.from_mapping(dict(runner_configuration)),
            scenario_seed=CANONICAL_SCENARIO_SEED,
            node_ids=tuple(NodeId(node) for node in canonical_engine_nodes()),
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
    bootstrap_engine_work(harness, run_id)


def bootstrap_engine_work(harness: ConcurrentScenarioHarness, run_id: RunId) -> None:
    """Bootstrap every canonical engine work item for an existing running run.

    The serve-mode launcher uses this for runs created through the public
    API: the run already exists and is durably running, so only the work
    items are created.
    """
    sequence = _event_frontier_next(harness, run_id)
    for step in canonical_engine_script():
        node_id = NodeId(str(step.node_id))
        work_item_id = engine_work_item_id(run_id, str(step.node_id), step.partition_key)
        with harness.database.transaction() as session:
            runs = SqlAlchemyRunRepository(session)
            run_record = runs.get(run_id)
            node_record = runs.get_node(run_id, node_id)
        if run_record is None or node_record is None:
            raise DemoEngineError("the engine run disappeared during bootstrap")
        bootstrapped_at = harness.clock.advance(1)
        _submit(
            harness,
            BootstrapWork(
                run_id=run_id,
                node_id=node_id,
                work_item_id=work_item_id,
                partition_key=PartitionKey(step.partition_key),
                input_reference=None,
                created_at=bootstrapped_at,
                expected_node_row_version=node_record.row_version,
                expected_run_row_version=run_record.row_version,
                event=_work_event(sequence, work_item_id, "work_created", bootstrapped_at),
            ),
        )
        sequence += 1


def _event_frontier_next(harness: ConcurrentScenarioHarness, run_id: RunId) -> int:
    """Return the next durable event sequence for one run."""
    from sqlalchemy import select

    from paritygrid.adapters.persistence.schema import run_event_counters

    with harness.database.transaction() as session:
        row = session.execute(
            select(run_event_counters.c.next_sequence_number).where(
                run_event_counters.c.run_id == run_id.value
            )
        ).first()
    if row is None:
        raise DemoEngineError("a running engine run lacks its durable event counter")
    return int(row.next_sequence_number)


def run_demo_engine_strategy(
    harness: ConcurrentScenarioHarness,
    strategy_id: str,
    run_id: RunId,
    *,
    analytics_path: Path,
    runner_configuration: dict[str, WireValue],
) -> RunnerExecutionRecord:
    """Execute one canonical engine run with the exact requested strategy.

    A resumed demo root keeps the committed engine evidence: an already
    terminal run is re-frozen from durable state instead of executing a second
    run, so repeated invocations never duplicate work or effects.
    """
    strategy_type = _STRATEGY_TYPES.get(strategy_id)
    if strategy_type is None:
        raise DemoEngineError(
            f"{strategy_id!r} is not a full-plan runner; the closed set is {ENGINE_STRATEGIES}"
        )
    durable_fingerprint = _durable_engine_fingerprint(harness, run_id)
    if durable_fingerprint is not None:
        return freeze_runner_record(
            harness, strategy_id, run_id, execution_evidence_fingerprint=durable_fingerprint
        )
    strategy = strategy_type()
    bootstrap_engine_run(harness, run_id, runner_configuration=runner_configuration)
    executor = CanonicalEngineExecutor(harness)
    engine = build_canonical_engine(harness, run_id, strategy=strategy, executor=executor)
    report = engine.run()
    if report.status is not EngineStatus.COMPLETED:
        raise DemoEngineError(f"the canonical engine run did not complete: {strategy_id}")
    fingerprint = _finalize_engine_run(harness, run_id, analytics_path)
    return freeze_runner_record(
        harness, strategy_id, run_id, execution_evidence_fingerprint=fingerprint
    )


def collect_cross_runner_manifest(
    database: SQLiteDatabase,
    writer: SQLiteTransactionalWriter,
    artifact_root: Path,
    pipeline_version: PipelineVersion,
) -> CrossRunnerVerificationManifest:
    """Freeze the three durable engine runs and compare their evidence.

    Every required strategy must already be durably terminal in the demo
    root.  The comparison is the accepted correctness-first proof: sorted
    durable work states, attempt outcomes, node aggregates, checkpoint and
    artifact identities, causal events, and the execution-evidence
    fingerprint — never timing, and never reconciliation, repair, or
    target-state equivalence.
    """
    from paritygrid.demo.verification import (
        REQUIRED_STRATEGIES,
        build_cross_runner_manifest,
    )

    harness = demo_engine_harness(
        database, writer, artifact_root, injected_engine_clock(), pipeline_version
    )
    records: list[RunnerExecutionRecord] = []
    for offset, strategy_id in enumerate(REQUIRED_STRATEGIES, start=1):
        run_id = RunId(f"{ENGINE_RUN_PREFIX}-{offset:04d}")
        fingerprint = _durable_engine_fingerprint(harness, run_id)
        if fingerprint is None:
            raise DemoEngineError(
                f"the {strategy_id} engine run ({run_id}) is absent from the demo root; "
                "run its smoke profile before comparing runners"
            )
        records.append(
            freeze_runner_record(
                harness, strategy_id, run_id, execution_evidence_fingerprint=fingerprint
            )
        )
    return build_cross_runner_manifest(tuple(records), {})


def _durable_engine_fingerprint(harness: ConcurrentScenarioHarness, run_id: RunId) -> str | None:
    """Return the run's durable fingerprint, or None when the run is absent."""
    with harness.database.transaction() as session:
        row = session.execute(
            select(
                runs_table.c.state,
                runs_table.c.execution_evidence_fingerprint,
            ).where(runs_table.c.run_id == run_id.value)
        ).first()
    if row is None:
        return None
    state = str(row.state)
    if state != "succeeded":
        raise DemoEngineError(
            "an existing engine run is not durably terminal; the demo root "
            "requires recovery before it can be reused"
        )
    if row.execution_evidence_fingerprint is None:
        raise DemoEngineError("a finalized engine run lacks its execution-evidence fingerprint")
    return str(row.execution_evidence_fingerprint)


def _finalize_engine_run(
    harness: ConcurrentScenarioHarness,
    run_id: RunId,
    analytics_path: Path,
) -> str:
    """Finalize one engine run and return its execution-evidence fingerprint."""
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
            plan_nodes=tuple(NodeId(node) for node in canonical_engine_nodes()),
            plan_fingerprint=PlanFingerprint(canonical_plan_fingerprint()),
        )
    finally:
        coordinator.close()
    if finalization.fingerprint is None:
        raise RunnerManifestError("the engine run finalized without a fingerprint")
    return finalization.fingerprint.value


def _submit(
    harness: ConcurrentScenarioHarness,
    command: CreateCapturedRun | TransitionRun | BootstrapWork,
) -> None:
    receipt = harness.writer.submit(command, timeout_seconds=5.0)
    receipt.result(timeout_seconds=60.0)


def _run_event(sequence: int, run_id: RunId, kind: str, at: UtcTimestamp) -> EventAppendRequest:
    return EventAppendRequest(
        EventSequence(sequence),
        sequence,
        _pending_event(kind, at, EventSubjectKind.RUN, run_id),
    )


def _work_event(
    sequence: int, work_item_id: WorkItemId, kind: str, at: UtcTimestamp
) -> EventAppendRequest:
    return EventAppendRequest(
        EventSequence(sequence),
        sequence,
        _pending_event(kind, at, EventSubjectKind.WORK_ITEM, work_item_id),
    )


def _pending_event(
    kind: str,
    at: UtcTimestamp,
    subject_kind: EventSubjectKind,
    subject_id: RunId | WorkItemId,
) -> PendingExecutionEvent:
    return PendingExecutionEvent(
        kind,
        at,
        subject_kind,
        subject_id,
        CANONICAL_CORRELATION_ID,
        1,
        RedactedDocument.from_mapping({"kind": kind}),
    )
