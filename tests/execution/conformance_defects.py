"""Deliberately defective doubles proving conformance assertions fail (P7.11)."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import threading
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest

from paritygrid.application.execution.concurrent_engine import (
    ConcurrentRunEngine,
    EngineStatus,
)
from paritygrid.application.execution.full_plan_strategy import (
    SEQUENTIAL_STRATEGY_CAPABILITIES,
    ExecutedWork,
    FullPlanStrategy,
    StrategyCapabilitiesV1,
    StrategyContext,
    StrategyMode,
)
from paritygrid.application.execution.runner_contract import WorkAssignmentV1
from paritygrid.domain.models import RunId
from paritygrid.quality.concurrent_scenario import (
    ConcurrentScenarioHarness,
    ScriptedConcurrentExecutor,
    bootstrap_scenario_run,
    build_scenario_engine,
    prepare_concurrent_harness,
    scenario_run_id,
)
from tests.execution.full_plan_conformance import assert_zero_owned_workers


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[ConcurrentScenarioHarness]:
    scenario = prepare_concurrent_harness(tmp_path / "defects.db", tmp_path / "artifacts")
    yield scenario
    scenario.close()


class _DefectiveDropResultStrategy:
    """Pulls assignments and discards them without any result envelope."""

    def __init__(self) -> None:
        self.context: StrategyContext | None = None
        self.pulled = 0

    @property
    def strategy_id(self) -> str:
        return "sequential"

    @property
    def capabilities(self) -> StrategyCapabilitiesV1:
        return SEQUENTIAL_STRATEGY_CAPABILITIES

    @property
    def mode(self) -> StrategyMode:
        return StrategyMode.INLINE

    def start(self, context: StrategyContext) -> None:
        self.context = context

    def execute_pending(self) -> int:
        context = self.context
        if context is None:
            return 0
        try:
            context.assignment_channel.recv(timeout=0.1)
        except Exception:
            return 0
        self.pulled += 1
        return 1

    def shutdown(self, *, timeout_seconds: float) -> None:
        del timeout_seconds


class _DefectiveLeakStrategy:
    """Spawns a worker thread that ignores shutdown and stays alive."""

    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def strategy_id(self) -> str:
        return "sequential"

    @property
    def capabilities(self) -> StrategyCapabilitiesV1:
        return SEQUENTIAL_STRATEGY_CAPABILITIES

    @property
    def mode(self) -> StrategyMode:
        return StrategyMode.POOLED

    def start(self, context: StrategyContext) -> None:
        del context
        self._thread = threading.Thread(
            target=self._leak, name="paritygrid-threaded-worker-leak", daemon=True
        )
        self._thread.start()

    def _leak(self) -> None:
        self._stop.wait(timeout=30.0)

    def execute_pending(self) -> int:
        return 0

    def shutdown(self, *, timeout_seconds: float) -> None:
        # The defect: shutdown claims success while the thread lives on.
        del timeout_seconds

    def force_stop(self) -> None:
        self._stop.set()


class _DefectiveForgedFenceExecutor(ScriptedConcurrentExecutor):
    """Returns results whose lease fence never matches the assignment."""

    def execute(self, assignment: WorkAssignmentV1) -> ExecutedWork:
        executed = super().execute(assignment)
        from dataclasses import replace

        forged = replace(executed.result, lease_fence=assignment.lease_fence + 5)
        return ExecutedWork(
            result=forged,
            failure_classification=executed.failure_classification,
            retry_eligible_at_micros=executed.retry_eligible_at_micros,
        )


def _build(
    harness: ConcurrentScenarioHarness,
    strategy: FullPlanStrategy | None,
    executor: ScriptedConcurrentExecutor,
    seed: int = 31,
) -> tuple[ConcurrentRunEngine, RunId]:
    from paritygrid.application.execution.full_plan_strategy import (
        SequentialFullPlanStrategy,
    )

    run_id = scenario_run_id(seed)
    bootstrap_scenario_run(harness, run_id)
    engine = build_scenario_engine(
        harness,
        run_id,
        strategy=strategy or SequentialFullPlanStrategy(),
        executor=executor,
    )
    return engine, run_id


def test_defective_drop_result_fails_completion(harness: ConcurrentScenarioHarness) -> None:
    """A strategy that loses accepted work cannot pass conformance."""
    strategy = _DefectiveDropResultStrategy()
    engine, _run_id = _build(harness, strategy, ScriptedConcurrentExecutor(harness))
    engine._ensure_started()
    assert engine._admit_next()
    assert strategy.execute_pending() == 1
    assert strategy.pulled == 1
    # The conformance invariant: every pulled assignment must produce a
    # result envelope this defective strategy never sent.
    assert engine._channels.result.queued == 0
    assert engine._coordinator.committed_count == 0
    assert engine.in_flight_identities != ()


def test_defective_leak_fails_zero_worker_assertion(harness: ConcurrentScenarioHarness) -> None:
    """A strategy that leaks a worker fails the cleanup assertion."""
    strategy = _DefectiveLeakStrategy()
    engine, _run_id = _build(harness, strategy, ScriptedConcurrentExecutor(harness))
    engine._ensure_started()
    engine.cancellation.request()
    report = engine.run()
    assert report.status is EngineStatus.CANCELLED
    leaked = [
        thread.name
        for thread in threading.enumerate()
        if thread.name.startswith("paritygrid-threaded-worker")
    ]
    assert leaked, "the leak double must keep its worker alive"
    with pytest.raises(AssertionError):
        assert_zero_owned_workers()
    strategy.force_stop()


def test_defective_forged_fence_is_fenced_before_writer(harness: ConcurrentScenarioHarness) -> None:
    """A forged lease fence drives the run recovery-required."""
    from paritygrid.application.execution.full_plan_strategy import (
        SequentialFullPlanStrategy,
    )

    executor = _DefectiveForgedFenceExecutor(harness)
    engine, _run_id = _build(harness, SequentialFullPlanStrategy(), executor)
    report = engine.run()
    assert report.status is EngineStatus.RECOVERY_REQUIRED
    assert report.recovery_reason is not None


def test_defective_factless_result_is_rejected_fail_closed(
    harness: ConcurrentScenarioHarness,
) -> None:
    """A result without registered commit facts never reaches the writer."""

    class _FactlessInline:
        """Inline worker that skips the facts registry entirely."""

        def __init__(self) -> None:
            self.context: StrategyContext | None = None

        @property
        def strategy_id(self) -> str:
            return "sequential"

        @property
        def capabilities(self) -> StrategyCapabilitiesV1:
            return SEQUENTIAL_STRATEGY_CAPABILITIES

        @property
        def mode(self) -> StrategyMode:
            return StrategyMode.INLINE

        def start(self, context: StrategyContext) -> None:
            self.context = context

        def execute_pending(self) -> int:
            context = self.context
            if context is None:
                return 0
            try:
                assignment = context.assignment_channel.recv(timeout=0.1)
            except Exception:
                return 0
            executed = context.executor.execute(cast(WorkAssignmentV1, assignment))
            # The defect: push the envelope without recording facts.
            context.result_channel.send(executed.result)
            return 1

        def shutdown(self, *, timeout_seconds: float) -> None:
            del timeout_seconds

    engine, _run_id = _build(harness, _FactlessInline(), ScriptedConcurrentExecutor(harness))
    report = engine.run()
    assert report.status is EngineStatus.RECOVERY_REQUIRED
    assert "factless" in (report.recovery_reason or "")
