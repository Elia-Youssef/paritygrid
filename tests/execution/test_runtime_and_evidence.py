"""Runtime capability registration, interpreter pool, evidence comparison."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from paritygrid.adapters.runners.interpreter_pool import (
    InterpreterPoolUnavailableError,
    SubordinateInterpreterPool,
    interpreter_pool_availability,
)
from paritygrid.application.execution.asyncio_strategy import AsyncioFullPlanStrategy
from paritygrid.application.execution.concurrency_settings import (
    CapturedConcurrencySettings,
)
from paritygrid.application.execution.concurrent_engine import EngineStatus
from paritygrid.application.execution.evidence_comparison import (
    EvidenceComparisonError,
    ExecutionEvidenceSnapshot,
    build_evidence_snapshot,
    compare_execution_evidence,
)
from paritygrid.application.execution.full_plan_strategy import (
    FullPlanStrategy,
    SequentialFullPlanStrategy,
)
from paritygrid.application.execution.runtime_capabilities import (
    SUBORDINATE_PROCESS_POOL_ID,
    RuntimeStrategyCatalog,
)
from paritygrid.application.execution.threaded_strategy import ThreadedFullPlanStrategy
from paritygrid.quality.concurrent_scenario import (
    NODE_ORDER,
    PARTITIONS_BY_NODE,
    ConcurrentScenarioHarness,
    ScriptedConcurrentExecutor,
    bootstrap_scenario_run,
    build_scenario_engine,
    prepare_concurrent_harness,
    read_scenario_evidence,
    scenario_plan_fingerprint,
    scenario_run_id,
)


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[ConcurrentScenarioHarness]:
    scenario = prepare_concurrent_harness(tmp_path / "runtime.db", tmp_path / "artifacts")
    yield scenario
    scenario.close()


class TestRuntimeCatalog:
    def test_exact_registration_under_complete_environment(self) -> None:
        catalog = RuntimeStrategyCatalog(CapturedConcurrencySettings())
        catalog.register_detected()
        identifiers = tuple(entry.strategy_id for entry in catalog.full_plan_strategies)
        assert identifiers == ("sequential", "threaded", "asyncio")
        assert all(entry.available for entry in catalog.full_plan_strategies)
        pool_ids = tuple(pool.pool_id for pool in catalog.subordinate_pools)
        assert SUBORDINATE_PROCESS_POOL_ID in pool_ids
        # The process pool is registered as a subordinate capability and
        # is never exposed as a full-plan runner.
        assert catalog.is_subordinate_pool(SUBORDINATE_PROCESS_POOL_ID)
        with pytest.raises(Exception, match="not registered"):
            catalog.resolve_full_plan(SUBORDINATE_PROCESS_POOL_ID)

    def test_partial_environment_drops_only_the_missing_capability(self) -> None:
        catalog = RuntimeStrategyCatalog(CapturedConcurrencySettings())
        catalog.register_detected(asyncio_available=False)
        asyncio_entry = catalog.resolve_full_plan("asyncio")
        assert not asyncio_entry.available
        assert asyncio_entry.unavailability_reason == "asyncio capability is unavailable"
        assert asyncio_entry.capabilities is None
        assert catalog.resolve_full_plan("sequential").available
        assert catalog.resolve_full_plan("threaded").available

    def test_startup_shutdown_are_exactly_once_and_idempotent(self) -> None:
        started: list[str] = []
        shutdown: list[str] = []

        class _Listener:
            def on_strategy_started(self, strategy_id: str) -> None:
                started.append(strategy_id)

            def on_strategy_shutdown(self, strategy_id: str) -> None:
                shutdown.append(strategy_id)

        catalog = RuntimeStrategyCatalog(CapturedConcurrencySettings(), listener=_Listener())
        catalog.register_detected()
        catalog.startup()
        assert started == ["sequential", "threaded", "asyncio"]
        catalog.shutdown()
        catalog.shutdown()
        assert shutdown == ["asyncio", "threaded", "sequential"]

    def test_failed_startup_rolls_back_in_reverse_order(self) -> None:
        started: list[str] = []
        shutdown: list[str] = []

        class _FailingListener:
            def on_strategy_started(self, strategy_id: str) -> None:
                started.append(strategy_id)
                if strategy_id == "asyncio":
                    raise RuntimeError("startup failed")

            def on_strategy_shutdown(self, strategy_id: str) -> None:
                shutdown.append(strategy_id)

        catalog = RuntimeStrategyCatalog(CapturedConcurrencySettings(), listener=_FailingListener())
        catalog.register_detected()
        with pytest.raises(Exception, match="startup failed"):
            catalog.startup()
        assert started == ["sequential", "threaded", "asyncio"]
        assert shutdown == ["threaded", "sequential"]


class TestInterpreterPool:
    def test_pool_uses_the_captured_cpu_bound_and_fences_wrong_response(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from datetime import UTC, datetime

        import paritygrid.adapters.runners.interpreter_pool as pool_module
        from paritygrid.adapters.runners.subordinate_codec import encode_response
        from paritygrid.application.execution.capacity import (
            ScheduledWorkLimiters,
            SubordinateCallLimiter,
        )
        from paritygrid.application.execution.clock_policy import ManualClock
        from paritygrid.domain.models import UtcTimestamp

        class _Future:
            def __init__(self, response: bytes) -> None:
                self.response = response

            def result(self, *, timeout: float | None = None) -> object:
                assert timeout == 2.0
                return self.response

            def done(self) -> bool:
                return True

            def add_done_callback(self, callback: Callable[[_Future], object], /) -> None:
                callback(self)

        class _Executor:
            def __init__(self, *, max_workers: int) -> None:
                self.max_workers = max_workers
                self.response = encode_response("sort_integers", {"sorted": [1]})
                self.closed = False

            def submit(self, _function: object, /, *_arguments: bytes) -> _Future:
                return _Future(self.response)

            def shutdown(self, *, wait: bool, cancel_futures: bool) -> None:
                assert wait
                assert cancel_futures
                self.closed = True

        instances: list[_Executor] = []

        def factory(*, max_workers: int) -> _Executor:
            instance = _Executor(max_workers=max_workers)
            instances.append(instance)
            return instance

        base = UtcTimestamp(datetime(2026, 8, 25, 8, 0, 0, tzinfo=UTC))
        settings = CapturedConcurrencySettings(cpu_pool_operations=3)
        parent = ScheduledWorkLimiters(
            settings, strategy_id="threaded", node_ids=("nod_c-src",), clock=ManualClock(base)
        )
        capacity = SubordinateCallLimiter(
            category="cpu_pool", limit=3, clock=ManualClock(base), parent_limiter=parent
        )
        monkeypatch.setattr(pool_module, "_resolve_interpreter_executor", lambda: factory)
        pool = SubordinateInterpreterPool(capacity=capacity, timeout_seconds=2.0)
        assert instances[0].max_workers == 3
        triple = parent.acquire("interpreter-fake", "nod_c-src")
        result = pool.submit("interpreter-fake", "sort_integers", {"values": [1]}, parent=triple)
        assert result.result == {"sorted": [1]}
        parent.release("interpreter-fake", "nod_c-src")
        pool.close()
        pool.close()
        assert instances[0].closed
        with pytest.raises(Exception, match="no longer admits"):
            pool.submit("closed", "sort_integers", {"values": []}, parent=())  # type: ignore[arg-type]

        mismatch_pool = SubordinateInterpreterPool(capacity=capacity, timeout_seconds=2.0)
        instances[1].response = encode_response("sha256_digest", {"digest": "ab" * 32})
        mismatch_triple = parent.acquire("interpreter-mismatch", "nod_c-src")
        with pytest.raises(Exception, match="does not match"):
            mismatch_pool.submit(
                "interpreter-mismatch",
                "sort_integers",
                {"values": [1]},
                parent=mismatch_triple,
            )
        parent.release("interpreter-mismatch", "nod_c-src")
        mismatch_pool.close()

    def test_timeout_retains_cpu_capacity_until_the_future_finishes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from datetime import UTC, datetime

        import paritygrid.adapters.runners.interpreter_pool as pool_module
        from paritygrid.application.execution.capacity import (
            ScheduledWorkLimiters,
            SubordinateCallLimiter,
        )
        from paritygrid.application.execution.clock_policy import ManualClock
        from paritygrid.domain.models import UtcTimestamp

        class _PendingFuture:
            def __init__(self) -> None:
                self.callbacks: list[Callable[[_PendingFuture], object]] = []
                self.finished = False

            def result(self, *, timeout: float | None = None) -> object:
                assert timeout == 0.01
                raise TimeoutError("still executing")

            def done(self) -> bool:
                return self.finished

            def add_done_callback(self, callback: Callable[[_PendingFuture], object], /) -> None:
                self.callbacks.append(callback)

            def finish(self) -> None:
                self.finished = True
                for callback in self.callbacks:
                    callback(self)

        class _Executor:
            def __init__(self, *, max_workers: int) -> None:
                assert max_workers == 1
                self.future = _PendingFuture()
                self.shutdown_calls: list[tuple[bool, bool]] = []

            def submit(self, _function: object, /, *_arguments: bytes) -> _PendingFuture:
                return self.future

            def shutdown(self, *, wait: bool, cancel_futures: bool) -> None:
                self.shutdown_calls.append((wait, cancel_futures))

        executor: _Executor | None = None

        def factory(*, max_workers: int) -> _Executor:
            nonlocal executor
            executor = _Executor(max_workers=max_workers)
            return executor

        base = UtcTimestamp(datetime(2026, 8, 25, 8, 0, 0, tzinfo=UTC))
        settings = CapturedConcurrencySettings(cpu_pool_operations=1)
        parent = ScheduledWorkLimiters(
            settings, strategy_id="threaded", node_ids=("nod_c-src",), clock=ManualClock(base)
        )
        capacity = SubordinateCallLimiter(
            category="cpu_pool", limit=1, clock=ManualClock(base), parent_limiter=parent
        )
        monkeypatch.setattr(pool_module, "_resolve_interpreter_executor", lambda: factory)
        pool = SubordinateInterpreterPool(capacity=capacity, timeout_seconds=0.01)
        triple = parent.acquire("interpreter-timeout", "nod_c-src")
        with pytest.raises(Exception, match="remains tracked"):
            pool.submit(
                "interpreter-timeout",
                "sort_integers",
                {"values": [1]},
                parent=triple,
            )
        assert capacity.snapshot().in_use == 1
        with pytest.raises(Exception, match="unresolved"):
            pool.close()
        assert executor is not None
        executor.future.finish()
        assert capacity.snapshot().in_use == 0
        pool.close()
        assert executor.shutdown_calls == [(False, True), (True, True)]
        parent.release("interpreter-timeout", "nod_c-src")

    def test_availability_reflects_the_actual_runtime(self) -> None:
        available, reason = interpreter_pool_availability()
        assert available is True
        assert reason is None

    def test_availability_reports_absent_and_broken_runtime(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import paritygrid.adapters.runners.interpreter_pool as pool_module

        with pytest.raises(TypeError, match="capacity"):
            SubordinateInterpreterPool(capacity=object(), timeout_seconds=1.0)  # type: ignore[arg-type]
        monkeypatch.setattr(pool_module.concurrent.futures, "InterpreterPoolExecutor", None)
        assert pool_module._resolve_interpreter_executor() is None  # pyright: ignore[reportPrivateUsage]
        monkeypatch.setattr(pool_module, "_resolve_interpreter_executor", lambda: None)
        assert interpreter_pool_availability() == (
            False,
            "concurrent.futures.InterpreterPoolExecutor is absent",
        )

        def broken_factory(*, max_workers: int) -> object:
            del max_workers
            raise RuntimeError("probe failure")

        monkeypatch.setattr(pool_module, "_resolve_interpreter_executor", lambda: broken_factory)
        available, reason = interpreter_pool_availability()
        assert not available
        assert reason == "interpreter probe failed: RuntimeError"

    def test_interpreter_pool_runs_registered_operations(self, tmp_path: Path) -> None:
        from datetime import UTC, datetime

        from paritygrid.application.execution.capacity import (
            ScheduledWorkLimiters,
            SubordinateCallLimiter,
        )
        from paritygrid.application.execution.clock_policy import ManualClock
        from paritygrid.domain.models import UtcTimestamp

        base = UtcTimestamp(datetime(2026, 8, 24, 8, 0, 0, tzinfo=UTC))
        settings = CapturedConcurrencySettings(cpu_pool_operations=2)
        parent = ScheduledWorkLimiters(
            settings, strategy_id="threaded", node_ids=("nod_c-src",), clock=ManualClock(base)
        )
        capacity = SubordinateCallLimiter(
            category="cpu_pool",
            limit=2,
            clock=ManualClock(base),
            parent_limiter=parent,
        )
        pool = SubordinateInterpreterPool(capacity=capacity, timeout_seconds=5.0)
        triple = parent.acquire("interp-owner", "nod_c-src")
        result = pool.submit(
            "interp-owner",
            "sort_integers",
            {"values": [5, 3, 9, 1]},
            parent=triple,
        )
        assert result.result["sorted"] == [1, 3, 5, 9]
        parent.release("interp-owner", "nod_c-src")
        pool.close()
        pool.close()
        assert pool.is_closed

    def test_unavailable_runtime_fails_closed_with_typed_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from datetime import UTC, datetime

        import paritygrid.adapters.runners.interpreter_pool as pool_module
        from paritygrid.application.execution.capacity import (
            ScheduledWorkLimiters,
            SubordinateCallLimiter,
        )
        from paritygrid.application.execution.clock_policy import ManualClock
        from paritygrid.domain.models import UtcTimestamp

        base = UtcTimestamp(datetime(2026, 8, 24, 8, 0, 0, tzinfo=UTC))
        settings = CapturedConcurrencySettings()
        parent = ScheduledWorkLimiters(
            settings, strategy_id="threaded", node_ids=("nod_c-src",), clock=ManualClock(base)
        )
        capacity = SubordinateCallLimiter(
            category="cpu_pool",
            limit=1,
            clock=ManualClock(base),
            parent_limiter=parent,
        )
        monkeypatch.setattr(pool_module, "_resolve_interpreter_executor", lambda: None)
        with pytest.raises(InterpreterPoolUnavailableError):
            SubordinateInterpreterPool(capacity=capacity, timeout_seconds=5.0)
        for timeout in (0, float("nan"), "bad"):
            with pytest.raises((TypeError, Exception)):
                SubordinateInterpreterPool(capacity=capacity, timeout_seconds=timeout)  # type: ignore[arg-type]


class TestExecutionEvidenceComparison:
    def _snapshots_for_strategy(
        self,
        harness: ConcurrentScenarioHarness,
        strategy: FullPlanStrategy,
        seed: int,
    ) -> ExecutionEvidenceSnapshot:
        run_id = scenario_run_id(seed)
        bootstrap_scenario_run(harness, run_id)
        executor = ScriptedConcurrentExecutor(harness)
        engine = build_scenario_engine(harness, run_id, strategy=strategy, executor=executor)
        report = engine.run()
        assert report.status is EngineStatus.COMPLETED
        evidence = read_scenario_evidence(harness, run_id)
        # Attempt evidence is normalized to the logical work identity so
        # the same seeded run compares equal regardless of its run id.
        work_positions = {
            work_id: (node, partition) for node, partition, work_id, _state in evidence.work_states
        }
        return build_evidence_snapshot(
            run_id=run_id.value,
            plan_fingerprint=scenario_plan_fingerprint(),
            work_states=tuple(
                (node, partition, state) for node, partition, _work, state in evidence.work_states
            ),
            attempt_outcomes=tuple(
                (f"{node}/{partition}", attempt, outcome)
                for work_id, attempt, outcome in evidence.attempt_outcomes
                for node, partition in [work_positions[work_id]]
            ),
            node_aggregates=tuple(
                (str(node), total, total, 0, 0, 0)
                for node in NODE_ORDER
                for total in [len(PARTITIONS_BY_NODE[str(node)])]
            ),
            artifact_identities=(),
            event_kinds=evidence.event_kinds,
        )

    def test_same_logical_run_compares_equal_across_strategies(
        self, harness: ConcurrentScenarioHarness
    ) -> None:
        sequential = self._snapshots_for_strategy(harness, SequentialFullPlanStrategy(), 101)
        threaded = self._snapshots_for_strategy(harness, ThreadedFullPlanStrategy(), 102)
        asyncio_run = self._snapshots_for_strategy(harness, AsyncioFullPlanStrategy(), 103)
        assert compare_execution_evidence(sequential, threaded).equal
        assert compare_execution_evidence(sequential, asyncio_run).equal
        assert compare_execution_evidence(threaded, asyncio_run).equal

    def test_different_durable_evidence_is_not_equal(self) -> None:
        left = build_evidence_snapshot(
            run_id="run_c-0001",
            plan_fingerprint="a" * 64,
            work_states=(("nod_a", "p0", "succeeded"),),
            attempt_outcomes=(("wrk_a", 1, "succeeded"),),
            node_aggregates=(("nod_a", 1, 1, 0, 0, 0),),
            artifact_identities=(),
            event_kinds=("run_created",),
        )
        right = build_evidence_snapshot(
            run_id="run_c-0002",
            plan_fingerprint="a" * 64,
            work_states=(("nod_a", "p0", "failed"),),
            attempt_outcomes=(("wrk_a", 1, "failed"),),
            node_aggregates=(("nod_a", 1, 0, 0, 1, 0),),
            artifact_identities=(),
            event_kinds=("run_created",),
        )
        comparison = compare_execution_evidence(left, right)
        assert not comparison.equal
        assert "durable work states differ" in comparison.differences
        assert "attempt outcomes differ" in comparison.differences

    def test_equal_evidence_never_claims_reconciliation_equivalence(self) -> None:
        from dataclasses import fields

        from paritygrid.application.execution.evidence_comparison import (
            EvidenceComparison,
        )

        verdict_fields = {field.name for field in fields(EvidenceComparison)}
        assert verdict_fields == {"equal", "differences", "comparison_version"}
        left = build_evidence_snapshot(
            run_id="run_c-0003",
            plan_fingerprint="b" * 64,
            work_states=(),
            attempt_outcomes=(),
            node_aggregates=(),
            artifact_identities=(),
            event_kinds=(),
        )
        right = build_evidence_snapshot(
            run_id="run_c-0003",
            plan_fingerprint="b" * 64,
            work_states=(),
            attempt_outcomes=(),
            node_aggregates=(),
            artifact_identities=(),
            event_kinds=(),
        )
        comparison = compare_execution_evidence(left, right)
        assert comparison.equal
        with pytest.raises(EvidenceComparisonError):
            EvidenceComparison(equal=True, differences=("reconciliation differs",))

    def test_negative_fixtures_lock_the_distinction(self) -> None:
        left = build_evidence_snapshot(
            run_id="run_c-0004",
            plan_fingerprint="c" * 64,
            work_states=(("nod_a", "p0", "succeeded"),),
            attempt_outcomes=(("wrk_a", 1, "succeeded"),),
            node_aggregates=(("nod_a", 1, 1, 0, 0, 0),),
            artifact_identities=(),
            event_kinds=("run_created",),
        )
        same_execution = build_evidence_snapshot(
            run_id="run_c-0004",
            plan_fingerprint="c" * 64,
            work_states=(("nod_a", "p0", "succeeded"),),
            attempt_outcomes=(("wrk_a", 1, "succeeded"),),
            node_aggregates=(("nod_a", 1, 1, 0, 0, 0),),
            artifact_identities=(),
            event_kinds=("run_created",),
        )
        # Equal execution evidence with different reconciliation outcomes
        # still compares equal: reconciliation is a Phase 9 fingerprint.
        assert compare_execution_evidence(left, same_execution).equal
        differing_fingerprint = build_evidence_snapshot(
            run_id="run_c-0004",
            plan_fingerprint="c" * 64,
            work_states=(("nod_a", "p0", "succeeded"),),
            attempt_outcomes=(("wrk_a", 1, "succeeded"),),
            node_aggregates=(("nod_a", 1, 1, 0, 0, 0),),
            artifact_identities=(),
            event_kinds=("run_created",),
            execution_evidence_fingerprint="d" * 64,
        )
        verdict = compare_execution_evidence(left, differing_fingerprint)
        assert not verdict.equal
        assert "execution-evidence fingerprint differs" in verdict.differences
