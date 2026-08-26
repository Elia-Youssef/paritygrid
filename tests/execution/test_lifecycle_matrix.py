"""Full lifecycle, recovery, backpressure, and cleanup matrix (P7.18)."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from paritygrid.adapters.persistence import SqlAlchemyExecutionEventRepository
from paritygrid.application.execution.asyncio_strategy import AsyncioFullPlanStrategy
from paritygrid.application.execution.concurrent_engine import (
    ConcurrentRunEngine,
    EngineStatus,
)
from paritygrid.application.execution.full_plan_strategy import (
    SequentialFullPlanStrategy,
    StrategyCapabilitiesV1,
    StrategyContext,
    StrategyMode,
)
from paritygrid.application.execution.runner_contract import WorkAssignmentV1
from paritygrid.application.execution.threaded_strategy import ThreadedFullPlanStrategy
from paritygrid.domain.models import RunId
from paritygrid.quality.concurrent_scenario import (
    DEFAULT_SCRIPT,
    NORMALIZE_NODE,
    ConcurrentScenarioHarness,
    ScenarioStep,
    ScriptedConcurrentExecutor,
    bootstrap_scenario_run,
    build_scenario_engine,
    prepare_concurrent_harness,
    read_scenario_evidence,
    scenario_run_id,
)
from tests.execution.full_plan_conformance import (
    STRATEGY_FACTORIES,
    assert_zero_owned_workers,
)

STRATEGY_IDS = ("sequential", "threaded", "asyncio")


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[ConcurrentScenarioHarness]:
    scenario = prepare_concurrent_harness(tmp_path / "matrix.db", tmp_path / "artifacts")
    yield scenario
    scenario.close()


def _run(
    harness: ConcurrentScenarioHarness,
    strategy_name: str,
    *,
    seed: int,
    hooks: Callable[[WorkAssignmentV1], None] | None = None,
) -> tuple[ConcurrentRunEngine, RunId]:
    run_id = scenario_run_id(seed)
    bootstrap_scenario_run(harness, run_id)
    executor = ScriptedConcurrentExecutor(harness, on_execute=hooks)
    engine = build_scenario_engine(
        harness,
        run_id,
        strategy=STRATEGY_FACTORIES[strategy_name](),
        executor=executor,
    )
    return engine, run_id


def _assert_contiguous_events(harness: ConcurrentScenarioHarness, run_id: RunId) -> None:
    with harness.database.transaction() as session:
        page = SqlAlchemyExecutionEventRepository(session).list_after(run_id, after=None, limit=100)
        sequences = [int(item.sequence) for item in page.items]
    assert sequences == list(range(1, len(sequences) + 1))


@pytest.mark.parametrize("strategy_name", STRATEGY_IDS)
class TestLifecycleMatrix:
    def test_normal_completion_cell(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        engine, run_id = _run(harness, strategy_name, seed=201)
        report = engine.run()
        assert report.status is EngineStatus.COMPLETED
        evidence = read_scenario_evidence(harness, run_id)
        assert all(state == "succeeded" for *_, state in evidence.work_states)
        assert len(evidence.attempt_outcomes) == 7
        _assert_contiguous_events(harness, run_id)
        assert_zero_owned_workers()

    def test_retry_cell(self, harness: ConcurrentScenarioHarness, strategy_name: str) -> None:
        from paritygrid.quality.concurrent_scenario import ConcurrentBehavior

        script = tuple(
            ScenarioStep(
                step.node_id,
                step.partition_key,
                ConcurrentBehavior.RETRY_THEN_SUCCESS
                if step.node_id == NORMALIZE_NODE
                else step.behavior,
            )
            for step in DEFAULT_SCRIPT
        )
        run_id = scenario_run_id(202)
        bootstrap_scenario_run(harness, run_id)
        executor = ScriptedConcurrentExecutor(harness, script=script)
        engine = build_scenario_engine(
            harness,
            run_id,
            strategy=STRATEGY_FACTORIES[strategy_name](),
            executor=executor,
        )
        report = engine.run()
        assert report.status is EngineStatus.COMPLETED
        assert report.committed_count == 8
        _assert_contiguous_events(harness, run_id)

    def test_pause_resume_cell(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        engine, run_id = _run(harness, strategy_name, seed=203)
        engine.request_pause()
        paused = engine.run()
        assert paused.status is EngineStatus.PAUSED
        assert paused.pause_proof is not None
        engine.resume(paused.pause_proof)
        assert engine.run().status is EngineStatus.COMPLETED
        _assert_contiguous_events(harness, run_id)

    def test_cancellation_cell(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        cell: dict[str, object] = {}

        def cancel_hook(assignment: WorkAssignmentV1) -> None:
            if assignment.node_id == str(NORMALIZE_NODE):
                engine_ref = cell["engine"]
                engine_ref.cancellation.request()  # type: ignore[attr-defined]

        engine, run_id = _run(harness, strategy_name, seed=204, hooks=cancel_hook)
        cell["engine"] = engine
        report = engine.run()
        assert report.status is EngineStatus.CANCELLED
        evidence = read_scenario_evidence(harness, run_id)
        assert evidence.run_state == "cancelled"
        _assert_contiguous_events(harness, run_id)
        assert_zero_owned_workers()

    def test_worker_failure_cell(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        def crash(assignment: WorkAssignmentV1) -> None:
            if assignment.node_id == str(NORMALIZE_NODE):
                raise RuntimeError("matrix worker failure")

        engine, run_id = _run(harness, strategy_name, seed=205, hooks=crash)
        report = engine.run()
        assert report.status is EngineStatus.COMPLETED
        evidence = read_scenario_evidence(harness, run_id)
        normalize_state = next(
            state
            for node, _partition, _work, state in evidence.work_states
            if node == str(NORMALIZE_NODE)
        )
        assert normalize_state == "failed"
        _assert_contiguous_events(harness, run_id)

    def test_unknown_writer_outcome_cell(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        from tests.execution.conformance_defects import _DefectiveForgedFenceExecutor

        run_id = scenario_run_id(206)
        bootstrap_scenario_run(harness, run_id)
        executor = _DefectiveForgedFenceExecutor(harness)
        engine = build_scenario_engine(
            harness,
            run_id,
            strategy=STRATEGY_FACTORIES[strategy_name](),
            executor=executor,
        )
        report = engine.run()
        assert report.status is EngineStatus.RECOVERY_REQUIRED
        assert report.recovery_reason is not None

    def test_blocked_writer_backpressure_cell(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        """A blocked downstream stops admission at the captured bound.

        With no worker draining results, admission must stop exactly at
        the assignment-channel capacity and memory stays bounded by
        that capacity: backpressure is structural, not timing-based.
        """

        from paritygrid.application.execution.concurrency_settings import (
            CapturedConcurrencySettings,
        )

        settings = CapturedConcurrencySettings(
            global_concurrent_work=8,
            per_strategy_work=8,
            per_node_work=8,
            assignment_channel_capacity=1,
            result_channel_capacity=2,
        )
        run_id = scenario_run_id(207)
        bootstrap_scenario_run(harness, run_id)

        class _NoDrainStrategy:
            """Pooled strategy whose workers never pull (blocked writer)."""

            def __init__(self) -> None:
                self.strategy_id = strategy_name

            @property
            def capabilities(self) -> StrategyCapabilitiesV1:
                from paritygrid.application.execution.full_plan_strategy import (
                    SEQUENTIAL_STRATEGY_CAPABILITIES,
                )

                return SEQUENTIAL_STRATEGY_CAPABILITIES

            @property
            def mode(self) -> StrategyMode:
                from paritygrid.application.execution.full_plan_strategy import (
                    StrategyMode,
                )

                return StrategyMode.POOLED

            def start(self, context: StrategyContext) -> None:
                del context

            def execute_pending(self) -> int:
                return 0

            def shutdown(self, *, timeout_seconds: float) -> None:
                del timeout_seconds

        executor = ScriptedConcurrentExecutor(harness)
        engine = build_scenario_engine(
            harness, run_id, strategy=_NoDrainStrategy(), executor=executor, settings=settings
        )
        engine._ensure_started()
        admitted = engine._admit_until_limited()
        # Two source partitions are ready but the bound admits one.
        assert admitted == 1
        assert engine._channels.assignment.queued == 1
        assert engine._channels.assignment.queued <= settings.assignment_channel_capacity
        assert engine._admit_until_limited() == 0
        engine.cancellation.request()
        report = engine.run()
        assert report.status is EngineStatus.CANCELLED
        evidence = read_scenario_evidence(harness, run_id)
        assert evidence.run_state == "cancelled"
        assert_zero_owned_workers()

    def test_repeated_cleanup_cell(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        engine, _run_id = _run(harness, strategy_name, seed=208)
        engine._ensure_started()
        engine.cleanup()
        engine.cleanup()
        assert_zero_owned_workers()


@pytest.mark.parametrize("strategy_name", ["threaded", "asyncio"])
class TestRestartMatrix:
    def test_restart_with_expired_lease_recovers(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        from paritygrid.quality.concurrent_scenario import (
            EDGES,
            NODE_ORDER,
            PARTITIONS_BY_NODE,
            SOURCE_NODE,
            scenario_plan_fingerprint,
            scenario_recovery_service,
        )

        run_id = scenario_run_id(209)
        bootstrap_scenario_run(harness, run_id)
        executor = ScriptedConcurrentExecutor(harness)
        engine = build_scenario_engine(
            harness, run_id, strategy=STRATEGY_FACTORIES[strategy_name](), executor=executor
        )
        engine._ensure_started()
        identity = engine._scheduler.next_ready(1)[0]
        assert engine._admit_one(identity)
        # Simulate parent termination: abandon the in-flight lease.
        engine.cleanup()
        harness.clock.advance(120)
        service = scenario_recovery_service(harness)
        report = service.recover(
            run_id=str(run_id),
            plan_fingerprint=scenario_plan_fingerprint(),
            node_order=tuple(str(node) for node in NODE_ORDER),
            edges=tuple((str(source), str(target)) for source, target in EDGES),
            partitions_by_node=dict(PARTITIONS_BY_NODE),
            control_generation=2,
        )
        assert report.recovered_work == (identity,)
        assert report.frontier.control_state.value == "running"
        assert_zero_owned_workers()
        del SOURCE_NODE

    def test_restart_with_non_expired_lease_stays_recovery_required(
        self, harness: ConcurrentScenarioHarness, strategy_name: str
    ) -> None:
        from paritygrid.application.execution.concurrent_recovery import (
            ConcurrentRecoveryNonExpiredLeaseError,
        )
        from paritygrid.quality.concurrent_scenario import (
            EDGES,
            NODE_ORDER,
            PARTITIONS_BY_NODE,
            scenario_plan_fingerprint,
            scenario_recovery_service,
        )

        run_id = scenario_run_id(210)
        bootstrap_scenario_run(harness, run_id)
        executor = ScriptedConcurrentExecutor(harness)
        engine = build_scenario_engine(
            harness, run_id, strategy=STRATEGY_FACTORIES[strategy_name](), executor=executor
        )
        engine._ensure_started()
        identity = engine._scheduler.next_ready(1)[0]
        assert engine._admit_one(identity)
        engine.cleanup()
        service = scenario_recovery_service(harness)
        with pytest.raises(ConcurrentRecoveryNonExpiredLeaseError):
            service.recover(
                run_id=str(run_id),
                plan_fingerprint=scenario_plan_fingerprint(),
                node_order=tuple(str(node) for node in NODE_ORDER),
                edges=tuple((str(source), str(target)) for source, target in EDGES),
                partitions_by_node=dict(PARTITIONS_BY_NODE),
                control_generation=2,
            )


def test_sequential_strategy_is_reference_for_matrix() -> None:
    """The sequential reference participates in the same matrix."""
    assert SequentialFullPlanStrategy().strategy_id == "sequential"
    assert ThreadedFullPlanStrategy().strategy_id == "threaded"
    assert AsyncioFullPlanStrategy().strategy_id == "asyncio"
