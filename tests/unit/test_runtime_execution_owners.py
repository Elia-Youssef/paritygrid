"""Runtime registration over the accepted concurrent execution owner."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from threading import Event, Thread
from time import monotonic, sleep

import pytest

from paritygrid.adapters.persistence.repositories.runs import SqlAlchemyRunRepository
from paritygrid.application.execution.concurrent_engine import ConcurrentRunEngine
from paritygrid.application.execution.full_plan_strategy import SequentialFullPlanStrategy
from paritygrid.application.ports.execution import RunRecord
from paritygrid.application.ports.run_control import RunControlEvidence
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import RunId
from paritygrid.quality.concurrent_scenario import (
    ConcurrentScenarioHarness,
    ScriptedConcurrentExecutor,
    bootstrap_scenario_run,
    build_scenario_engine,
    prepare_concurrent_harness,
    scenario_run_id,
)
from paritygrid.runtime.execution_owners import RuntimeExecutionOwnership
from paritygrid.runtime.run_controls import RuntimeActiveRunControlRegistry


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[ConcurrentScenarioHarness]:
    scenario = prepare_concurrent_harness(tmp_path / "owners.db", tmp_path / "artifacts")
    yield scenario
    scenario.close()


def test_runtime_ownership_registers_real_engine_and_retires_after_resume(
    harness: ConcurrentScenarioHarness,
) -> None:
    """Pause/resume crosses the accepted engine, not a test writer shim."""
    entered = Event()
    release = Event()
    engine_cell: dict[str, ConcurrentRunEngine] = {}

    def block_first_assignment(_assignment: object) -> None:
        entered.set()
        assert release.wait(timeout=2.0)

    run_id = scenario_run_id(701)
    bootstrap_scenario_run(harness, run_id)
    executor = ScriptedConcurrentExecutor(harness, on_execute=block_first_assignment)
    engine = build_scenario_engine(
        harness,
        run_id,
        strategy=SequentialFullPlanStrategy(),
        executor=executor,
    )
    engine_cell["engine"] = engine
    registry = RuntimeActiveRunControlRegistry()
    ownership = RuntimeExecutionOwnership(
        active_run_controls=registry,
        read_run=lambda identity: _read_run(harness, identity),
    )

    owner = ownership.start_concurrent(engine)
    assert entered.wait(timeout=2.0)
    assert registry.active_count == 1

    paused = _in_thread(
        lambda: owner.pause(
            correlation_id="corr-owner-pause",
            timeout_seconds=2.0,
            converge_on_duplicate=False,
        )
    )
    _wait_until(lambda: engine_cell["engine"]._pause_signal.is_requested)  # pyright: ignore[reportPrivateUsage]
    release.set()
    pause_evidence = paused()
    assert pause_evidence.run.state is RunState.PAUSED
    assert pause_evidence.submission_ids
    assert _read_run(harness, run_id).state is RunState.PAUSED

    resume_evidence = owner.resume(
        correlation_id="corr-owner-resume",
        timeout_seconds=2.0,
        converge_on_duplicate=False,
    )
    assert resume_evidence.run.state is RunState.RUNNING
    assert resume_evidence.submission_ids
    _wait_until(lambda: registry.active_count == 0)


def test_runtime_ownership_cancels_through_real_engine_and_retires(
    harness: ConcurrentScenarioHarness,
) -> None:
    """Cancellation drains through the accepted engine lifecycle coordinator."""
    entered = Event()
    release = Event()

    def block_first_assignment(_assignment: object) -> None:
        entered.set()
        assert release.wait(timeout=2.0)

    run_id = scenario_run_id(702)
    bootstrap_scenario_run(harness, run_id)
    engine = build_scenario_engine(
        harness,
        run_id,
        strategy=SequentialFullPlanStrategy(),
        executor=ScriptedConcurrentExecutor(harness, on_execute=block_first_assignment),
    )
    registry = RuntimeActiveRunControlRegistry()
    ownership = RuntimeExecutionOwnership(
        active_run_controls=registry,
        read_run=lambda identity: _read_run(harness, identity),
    )

    owner = ownership.start_concurrent(engine)
    assert entered.wait(timeout=2.0)
    cancelled = _in_thread(
        lambda: owner.cancel(
            correlation_id="corr-owner-cancel",
            timeout_seconds=2.0,
            converge_on_duplicate=False,
        )
    )
    _wait_until(lambda: engine.cancellation.is_requested)
    release.set()
    cancel_evidence = cancelled()
    assert cancel_evidence.run.state is RunState.CANCELLED
    assert cancel_evidence.submission_ids
    assert _read_run(harness, run_id).state is RunState.CANCELLED
    _wait_until(lambda: registry.active_count == 0)


def test_runtime_shutdown_joins_an_already_requested_cancellation(
    harness: ConcurrentScenarioHarness,
) -> None:
    """Shutdown must join, not re-request, an accepted cancellation in flight."""
    entered = Event()
    release = Event()

    def block_first_assignment(_assignment: object) -> None:
        entered.set()
        assert release.wait(timeout=2.0)

    run_id = scenario_run_id(703)
    bootstrap_scenario_run(harness, run_id)
    engine = build_scenario_engine(
        harness,
        run_id,
        strategy=SequentialFullPlanStrategy(),
        executor=ScriptedConcurrentExecutor(harness, on_execute=block_first_assignment),
    )
    registry = RuntimeActiveRunControlRegistry()
    ownership = RuntimeExecutionOwnership(
        active_run_controls=registry,
        read_run=lambda identity: _read_run(harness, identity),
    )

    owner = ownership.start_concurrent(engine)
    assert entered.wait(timeout=2.0)
    cancelled = _in_thread(
        lambda: owner.cancel(
            correlation_id="corr-owner-cancel-close",
            timeout_seconds=2.0,
            converge_on_duplicate=False,
        )
    )
    _wait_until(lambda: engine.cancellation.is_requested)

    close_errors: list[BaseException] = []

    def close_owner() -> None:
        try:
            owner.close(timeout_seconds=2.0)
        except BaseException as error:  # pragma: no cover - assertion owns evidence
            close_errors.append(error)

    close_thread = Thread(target=close_owner, daemon=False)
    close_thread.start()
    release.set()

    cancel_evidence = cancelled()
    close_thread.join(timeout=2.0)
    assert not close_thread.is_alive()
    assert not close_errors
    assert cancel_evidence.run.state is RunState.CANCELLED
    assert _read_run(harness, run_id).state is RunState.CANCELLED
    _wait_until(lambda: registry.active_count == 0)


def _read_run(harness: ConcurrentScenarioHarness, run_id: RunId) -> RunRecord:
    with harness.database.transaction() as session:
        run = SqlAlchemyRunRepository(session).get(run_id)
    assert run is not None
    return run


def _in_thread(operation: Callable[[], RunControlEvidence]) -> Callable[[], RunControlEvidence]:
    result: dict[str, RunControlEvidence] = {}
    errors: list[BaseException] = []

    def invoke() -> None:
        try:
            result["value"] = operation()
        except BaseException as error:  # pragma: no cover - assertion below owns evidence
            errors.append(error)

    thread = Thread(target=invoke, daemon=False)
    thread.start()

    def collect() -> RunControlEvidence:
        thread.join(timeout=2.0)
        assert not thread.is_alive(), "runtime control operation did not finish"
        assert not errors
        return result["value"]

    return collect


def _wait_until(predicate: Callable[[], bool]) -> None:
    deadline = monotonic() + 2.0
    while not predicate():
        if monotonic() >= deadline:
            raise AssertionError("condition did not become true within the bounded test wait")
        sleep(0.001)
