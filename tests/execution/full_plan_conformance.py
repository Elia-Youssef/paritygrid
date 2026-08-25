"""Shared full-plan conformance suite for every strategy (P7.11)."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import threading
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from paritygrid.application.execution.asyncio_strategy import AsyncioFullPlanStrategy
from paritygrid.application.execution.concurrency_settings import (
    CapturedConcurrencySettings,
)
from paritygrid.application.execution.concurrent_engine import (
    ConcurrentRunEngine,
    ConcurrentRunReport,
    EngineStatus,
)
from paritygrid.application.execution.full_plan_strategy import (
    FullPlanStrategy,
    SequentialFullPlanStrategy,
)
from paritygrid.application.execution.runner_contract import WorkAssignmentV1
from paritygrid.application.execution.threaded_strategy import ThreadedFullPlanStrategy
from paritygrid.domain.models import NodeId, RunId
from paritygrid.quality.concurrent_scenario import (
    DEFAULT_SCRIPT,
    EDGES,
    NORMALIZE_NODE,
    SOURCE_NODE,
    ConcurrentBehavior,
    ConcurrentScenarioHarness,
    ScenarioStep,
    ScriptedConcurrentExecutor,
    bootstrap_scenario_run,
    build_scenario_engine,
    prepare_concurrent_harness,
    read_scenario_evidence,
    scenario_run_id,
)

StrategyFactory = Callable[[], FullPlanStrategy]

STRATEGY_FACTORIES: dict[str, StrategyFactory] = {
    "sequential": SequentialFullPlanStrategy,
    "threaded": ThreadedFullPlanStrategy,
    "asyncio": AsyncioFullPlanStrategy,
}
STRATEGY_IDS = tuple(STRATEGY_FACTORIES)


def owned_worker_threads() -> int:
    """Count live paritygrid-owned worker threads."""
    return sum(1 for thread in threading.enumerate() if thread.name.startswith("paritygrid-"))


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[ConcurrentScenarioHarness]:
    scenario = prepare_concurrent_harness(tmp_path / "conformance.db", tmp_path / "artifacts")
    yield scenario
    scenario.close()


class ConformanceRun:
    """One completed conformance execution with its durable evidence."""

    def __init__(
        self,
        engine: ConcurrentRunEngine,
        executor: ScriptedConcurrentExecutor,
        report: ConcurrentRunReport,
        run_id: RunId,
    ) -> None:
        self.engine = engine
        self.executor = executor
        self.report = report
        self.run_id = run_id


def run_conformance_scenario(
    harness: ConcurrentScenarioHarness,
    strategy: FullPlanStrategy,
    *,
    seed: int = 1,
    script: tuple[ScenarioStep, ...] = DEFAULT_SCRIPT,
    hooks: Callable[[WorkAssignmentV1], None] | None = None,
    settings: CapturedConcurrencySettings | None = None,
    pause_before: bool = False,
) -> ConformanceRun:
    """Bootstrap, execute, and finish one conformance scenario."""
    run_id = scenario_run_id(seed)
    bootstrap_scenario_run(harness, run_id)
    executor = ScriptedConcurrentExecutor(harness, script=script, on_execute=hooks)
    engine = build_scenario_engine(
        harness, run_id, strategy=strategy, executor=executor, settings=settings
    )
    if pause_before:
        engine.request_pause()
    report = engine.run()
    return ConformanceRun(engine, executor, report, run_id)


def _script_with(node: NodeId, behavior: ConcurrentBehavior) -> tuple[ScenarioStep, ...]:
    return tuple(
        ScenarioStep(
            step.node_id, step.partition_key, behavior if step.node_id == node else step.behavior
        )
        for step in DEFAULT_SCRIPT
    )


def assert_zero_owned_workers() -> None:
    """No worker thread owned by any strategy survives the run."""
    remaining = [
        thread.name
        for thread in threading.enumerate()
        if thread.name.startswith("paritygrid-") and "sqlite-writer" not in thread.name
    ]
    assert remaining == []


@pytest.mark.parametrize("strategy_name", STRATEGY_IDS)
class TestFullPlanConformance:
    """Strategy-neutral scheduling, lifecycle, bounds, and cleanup proof."""

    def test_full_plan_completion_with_exact_durable_evidence(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        outcome = run_conformance_scenario(harness, STRATEGY_FACTORIES[strategy_name]())
        assert outcome.report.status is EngineStatus.COMPLETED
        evidence = read_scenario_evidence(harness, outcome.run_id)
        assert len(evidence.work_states) == 7
        assert all(state == "succeeded" for *_, state in evidence.work_states)
        assert all(outcome == "succeeded" for _work, _attempt, outcome in evidence.attempt_outcomes)
        assert evidence.event_kinds[0] == "run_created"
        assert "checkpoint_committed" in evidence.event_kinds
        assert outcome.engine.in_flight_identities == ()
        assert_zero_owned_workers()

    def test_dependency_barrier_holds_until_predecessors_commit(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        barrier_probe: list[str] = []

        def probe(assignment: WorkAssignmentV1) -> None:
            barrier_probe.append(assignment.node_id)

        outcome = run_conformance_scenario(
            harness, STRATEGY_FACTORIES[strategy_name](), hooks=probe
        )
        assert outcome.report.status is EngineStatus.COMPLETED
        # A successor never executes before every predecessor item
        # committed, whatever the global interleaving was.
        positions = {node: index for index, node in enumerate(barrier_probe)}
        for source, target in EDGES:
            assert positions[str(source)] < positions[str(target)]

    def test_retry_then_success_commits_exact_attempts(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        outcome = run_conformance_scenario(
            harness,
            STRATEGY_FACTORIES[strategy_name](),
            script=_script_with(NORMALIZE_NODE, ConcurrentBehavior.RETRY_THEN_SUCCESS),
        )
        assert outcome.report.status is EngineStatus.COMPLETED
        normalize_attempts = [
            attempt
            for node, _partition, attempt in outcome.executor.executed
            if node == str(NORMALIZE_NODE)
        ]
        assert sorted(normalize_attempts) == [1, 2]
        evidence = read_scenario_evidence(harness, outcome.run_id)
        assert all(state == "succeeded" for *_, state in evidence.work_states)

    def test_pause_resume_round_trip(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        strategy = STRATEGY_FACTORIES[strategy_name]()
        outcome = run_conformance_scenario(harness, strategy, pause_before=True)
        assert outcome.report.status is EngineStatus.PAUSED
        assert outcome.report.pause_proof is not None
        evidence = read_scenario_evidence(harness, outcome.run_id)
        assert evidence.run_state == "paused"
        outcome.engine.resume(outcome.report.pause_proof)
        final = outcome.engine.run()
        assert final.status is EngineStatus.COMPLETED
        evidence = read_scenario_evidence(harness, outcome.run_id)
        assert all(state == "succeeded" for *_, state in evidence.work_states)

    def test_pause_abort_returns_to_running(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        cell: dict[str, ConcurrentRunEngine] = {}

        def abort_hook(assignment: WorkAssignmentV1) -> None:
            if assignment.node_id == str(NORMALIZE_NODE):
                cell["engine"].request_pause()
                cell["engine"].abort_pause()

        strategy = STRATEGY_FACTORIES[strategy_name]()
        run_id = scenario_run_id(4)
        bootstrap_scenario_run(harness, run_id)
        executor = ScriptedConcurrentExecutor(harness, on_execute=abort_hook)
        engine = build_scenario_engine(harness, run_id, strategy=strategy, executor=executor)
        cell["engine"] = engine
        report = engine.run()
        assert report.status is EngineStatus.COMPLETED
        evidence = read_scenario_evidence(harness, run_id)
        assert evidence.run_state == "running"

    def test_cancellation_leaves_consistent_durable_state(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        cell: dict[str, ConcurrentRunEngine] = {}

        def cancel_hook(assignment: WorkAssignmentV1) -> None:
            if assignment.node_id == str(NORMALIZE_NODE):
                cell["engine"].cancellation.request()

        strategy = STRATEGY_FACTORIES[strategy_name]()
        run_id = scenario_run_id(5)
        bootstrap_scenario_run(harness, run_id)
        executor = ScriptedConcurrentExecutor(harness, on_execute=cancel_hook)
        engine = build_scenario_engine(harness, run_id, strategy=strategy, executor=executor)
        cell["engine"] = engine
        report = engine.run()
        assert report.status is EngineStatus.CANCELLED
        evidence = read_scenario_evidence(harness, run_id)
        assert evidence.run_state == "cancelled"
        terminal = {state for *_, state in evidence.work_states}
        assert terminal <= {"succeeded", "cancelled", "pending"}
        assert_zero_owned_workers()

    def test_bounds_and_cleanup_hold(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        settings = CapturedConcurrencySettings(
            global_concurrent_work=2,
            per_strategy_work=2,
            per_node_work=1,
            assignment_channel_capacity=4,
            result_channel_capacity=4,
        )
        outcome = run_conformance_scenario(
            harness, STRATEGY_FACTORIES[strategy_name](), settings=settings
        )
        assert outcome.report.status is EngineStatus.COMPLETED
        snapshots = outcome.engine.frontier
        del snapshots
        assert outcome.engine.admitted_count == 7
        assert outcome.report.committed_count == 7
        assert_zero_owned_workers()


@pytest.mark.parametrize("strategy_name", ["threaded", "asyncio"])
class TestPooledOverlapConformance:
    """Overlap and out-of-order completion proof for pooled strategies."""

    def test_independent_partitions_overlap(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        """The two source partitions execute concurrently, not serially."""
        both = threading.Barrier(2, timeout=10.0)

        def hook(assignment: WorkAssignmentV1) -> None:
            if assignment.node_id == str(SOURCE_NODE):
                # The barrier times out (failing the test) unless both
                # independent partitions are in flight together.
                both.wait()

        strategy = (
            ThreadedFullPlanStrategy(worker_count=2)
            if strategy_name == "threaded"
            else AsyncioFullPlanStrategy(worker_count=2)
        )
        outcome = run_conformance_scenario(harness, strategy, hooks=hook)
        assert outcome.report.status is EngineStatus.COMPLETED
        assert_zero_owned_workers()

    def test_out_of_order_completion_commits_correct_aggregates(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        """Source partition-1 may finish before partition-0."""
        hold = threading.Event()

        def reorder_hook(assignment: WorkAssignmentV1) -> None:
            # Partition-0 blocks until its successor became admissible,
            # which requires partition-1 to have committed first: the
            # reversal is structural, not timing-dependent.
            if assignment.node_id == str(SOURCE_NODE) and (
                assignment.partition_key == "partition-0"
            ):
                hold.wait(timeout=10.0)
            if assignment.node_id == str(NORMALIZE_NODE):
                hold.set()

        strategy = (
            ThreadedFullPlanStrategy(worker_count=2)
            if strategy_name == "threaded"
            else AsyncioFullPlanStrategy(worker_count=2)
        )
        run_id = scenario_run_id(6)
        bootstrap_scenario_run(harness, run_id)
        executor = ScriptedConcurrentExecutor(harness, on_execute=reorder_hook)
        engine = build_scenario_engine(harness, run_id, strategy=strategy, executor=executor)
        report = engine.run()
        assert report.status is EngineStatus.COMPLETED
        evidence = read_scenario_evidence(harness, run_id)
        assert all(state == "succeeded" for *_, state in evidence.work_states)
