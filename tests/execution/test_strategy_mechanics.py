"""Threaded and asyncio strategy mechanics (P7.12 / P7.13)."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from paritygrid.application.execution.asyncio_strategy import AsyncioFullPlanStrategy
from paritygrid.application.execution.channels import ChannelSet
from paritygrid.application.execution.concurrency_settings import (
    CapturedConcurrencySettings,
)
from paritygrid.application.execution.concurrent_engine import (
    ConcurrentRunEngine,
    EngineStatus,
)
from paritygrid.application.execution.full_plan_strategy import (
    FullPlanStrategy,
    ResultFactsRegistry,
    StrategyContext,
)
from paritygrid.application.execution.runner_contract import (
    RUNNER_CONTRACT_VERSION,
    WORK_ASSIGNMENT_PROTOCOL,
    ContractDocument,
    ControlGeneration,
    RunnerContractLoopError,
    WorkAssignmentV1,
)
from paritygrid.application.execution.threaded_strategy import (
    MAX_THREADED_WORKERS,
    ThreadedFullPlanStrategy,
    derive_worker_count,
)
from paritygrid.quality.concurrent_scenario import (
    NORMALIZE_NODE,
    ConcurrentScenarioHarness,
    ScriptedConcurrentExecutor,
    bootstrap_scenario_run,
    build_scenario_engine,
    prepare_concurrent_harness,
    scenario_run_id,
)


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[ConcurrentScenarioHarness]:
    scenario = prepare_concurrent_harness(tmp_path / "mechanics.db", tmp_path / "artifacts")
    yield scenario
    scenario.close()


def _engine_for(
    harness: ConcurrentScenarioHarness,
    strategy: FullPlanStrategy | None,
    *,
    seed: int = 41,
    settings: CapturedConcurrencySettings | None = None,
    hooks: Callable[[WorkAssignmentV1], None] | None = None,
) -> tuple[ConcurrentRunEngine, ScriptedConcurrentExecutor]:
    from paritygrid.application.execution.full_plan_strategy import (
        SequentialFullPlanStrategy,
    )

    run_id = scenario_run_id(seed)
    bootstrap_scenario_run(harness, run_id)
    executor = ScriptedConcurrentExecutor(harness, on_execute=hooks)
    engine = build_scenario_engine(
        harness,
        run_id,
        strategy=strategy or SequentialFullPlanStrategy(),
        executor=executor,
        settings=settings,
    )
    return engine, executor


def _gate_source(assignment: WorkAssignmentV1, both: threading.Barrier) -> None:
    if assignment.node_id == "nod_c-src":
        both.wait()


class TestThreadedStrategy:
    def test_worker_count_derivation_respects_bounds(self) -> None:
        settings = CapturedConcurrencySettings(per_strategy_work=3)
        assert derive_worker_count(settings) == 3
        from paritygrid.application.execution.full_plan_strategy import (
            FullPlanStrategyInvalidRequestError,
        )

        with pytest.raises(FullPlanStrategyInvalidRequestError):
            ThreadedFullPlanStrategy(worker_count=MAX_THREADED_WORKERS + 1)

    def test_workers_are_named_bounded_and_joined(self, harness: ConcurrentScenarioHarness) -> None:
        strategy = ThreadedFullPlanStrategy(worker_count=3)
        engine, _executor = _engine_for(harness, strategy)
        engine._ensure_started()
        assert strategy.worker_identities == tuple(
            f"paritygrid-threaded-worker-{index:03d}" for index in (1, 2, 3)
        )
        assert strategy.alive_worker_count == 3
        engine.cleanup()
        assert strategy.alive_worker_count == 0

    def test_two_workers_genuinely_overlap(self, harness: ConcurrentScenarioHarness) -> None:
        """Two source partitions execute simultaneously under saturation."""
        both = threading.Barrier(2, timeout=10.0)
        settings = CapturedConcurrencySettings(
            global_concurrent_work=2,
            per_strategy_work=2,
            per_node_work=2,
            assignment_channel_capacity=8,
            result_channel_capacity=8,
        )
        engine, _executor = _engine_for(
            harness,
            ThreadedFullPlanStrategy(worker_count=2),
            settings=settings,
            seed=42,
            hooks=lambda assignment: _gate_source(assignment, both),
        )
        report = engine.run()
        assert report.status is EngineStatus.COMPLETED

    def test_slow_results_do_not_deadlock_small_channels(
        self, harness: ConcurrentScenarioHarness
    ) -> None:
        """A tiny result channel and slow executor still complete boundedly."""
        settings = CapturedConcurrencySettings(
            global_concurrent_work=2,
            per_strategy_work=2,
            per_node_work=1,
            assignment_channel_capacity=2,
            result_channel_capacity=1,
        )
        pace = threading.Semaphore(1)

        def slow_hook(_assignment: WorkAssignmentV1) -> None:
            with pace:
                pass

        engine, _executor = _engine_for(
            harness,
            ThreadedFullPlanStrategy(worker_count=2),
            settings=settings,
            seed=43,
            hooks=slow_hook,
        )
        report = engine.run()
        assert report.status is EngineStatus.COMPLETED

    def test_worker_failure_commits_durable_failure(
        self, harness: ConcurrentScenarioHarness
    ) -> None:
        def crash(assignment: WorkAssignmentV1) -> None:
            if assignment.node_id == str(NORMALIZE_NODE):
                raise RuntimeError("worker crash")

        engine, _executor = _engine_for(
            harness,
            ThreadedFullPlanStrategy(worker_count=2),
            hooks=crash,
            seed=44,
        )
        report = engine.run()
        # The crashed normalize commits a durable FAILED, which blocks its
        # successors exactly like the sequential contract; the two source
        # partitions and the failure itself are the three durable commits.
        assert report.status is EngineStatus.COMPLETED
        assert report.committed_count == 3


class TestAsyncioStrategyLoopSafety:
    def _standalone_context(self, harness: ConcurrentScenarioHarness) -> StrategyContext:
        channels = ChannelSet(
            assignment_capacity=4,
            result_capacity=4,
            telemetry_capacity=4,
            writer_capacity=4,
        )
        return StrategyContext(
            run_id="run_c-0045",
            plan_fingerprint="a" * 64,
            settings=CapturedConcurrencySettings(),
            assignment_channel=channels.assignment,
            result_channel=channels.result,
            executor=ScriptedConcurrentExecutor(harness),
            facts=ResultFactsRegistry(),
        )

    @staticmethod
    def _assignment() -> WorkAssignmentV1:
        return WorkAssignmentV1(
            protocol=WORK_ASSIGNMENT_PROTOCOL,
            contract_version=RUNNER_CONTRACT_VERSION,
            plan_fingerprint="a" * 64,
            run_id="run_c-0045",
            node_id="nod_c-src",
            partition_key="partition-0",
            work_item_id="wrk_c-0045-src-0",
            attempt_number=1,
            lease_fence=2,
            lease_owner="engine-main:wrk_c-0045-src-0",
            control_generation=ControlGeneration(1),
            deadline_utc="2026-08-24T08:01:00.000000Z",
            operation_descriptor=_descriptor(),
            input_references=(),
            captured_settings_ref="captured-concurrency-settings.v1",
        )

    def test_sync_facade_rejects_active_loop(self) -> None:
        strategy = AsyncioFullPlanStrategy()

        async def attempt() -> None:
            with pytest.raises(RunnerContractLoopError):
                strategy.start(object())  # type: ignore[arg-type]

        asyncio.run(attempt())

    def test_async_entry_point_adapts_blocking_work(
        self, harness: ConcurrentScenarioHarness
    ) -> None:
        """start_async runs workers inside an already active event loop."""
        strategy = AsyncioFullPlanStrategy(worker_count=2)
        context = self._standalone_context(harness)

        async def scenario() -> None:
            await strategy.start_async(context)
            await context.assignment_channel.send_async(self._assignment())
            envelope = None
            for _ in range(200):
                try:
                    envelope = await context.result_channel.recv_async(iterations=8)
                except Exception:
                    envelope = None
                if envelope is not None:
                    break
                await asyncio.sleep(0)
            assert envelope is not None
            assert getattr(envelope, "work_item_id", None) == "wrk_c-0045-src-0"
            await strategy.shutdown_async()

        asyncio.run(scenario())
        assert not strategy._tasks

    def test_shutdown_closes_adaptation_pool_and_tasks(
        self, harness: ConcurrentScenarioHarness
    ) -> None:
        strategy = AsyncioFullPlanStrategy(worker_count=2)
        engine, _executor = _engine_for(harness, strategy, seed=46)
        engine._ensure_started()
        engine.cleanup()
        assert strategy._adaptation_executor is None
        assert strategy._tasks == []
        assert strategy._loop_thread is None or not strategy._loop_thread.is_alive()

    def test_cancellation_during_saturation_completes(
        self, harness: ConcurrentScenarioHarness
    ) -> None:
        cell: dict[str, object] = {}

        def cancel_hook(assignment: WorkAssignmentV1) -> None:
            if assignment.node_id == str(NORMALIZE_NODE):
                engine_ref = cell["engine"]
                engine_ref.cancellation.request()  # type: ignore[attr-defined]

        run_id = scenario_run_id(47)
        bootstrap_scenario_run(harness, run_id)
        executor = ScriptedConcurrentExecutor(harness, on_execute=cancel_hook)
        engine = build_scenario_engine(
            harness,
            run_id,
            strategy=AsyncioFullPlanStrategy(worker_count=2),
            executor=executor,
        )
        cell["engine"] = engine
        report = engine.run()
        assert report.status is EngineStatus.CANCELLED


def _descriptor() -> ContractDocument:
    return ContractDocument((("node_kind", "csv_source"),))
