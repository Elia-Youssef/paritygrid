"""Pause compare-and-set and durable lifecycle arrows over real SQLite."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from paritygrid.adapters.persistence import (
    SQLitePauseStateReader,
)
from paritygrid.application.execution.concurrent_lifecycle import (
    ConcurrentLifecycleBusyError,
    ConcurrentLifecycleCoordinator,
    ConcurrentLifecycleRejectedError,
    ConcurrentPauseSignal,
)
from paritygrid.quality.concurrent_scenario import (
    ConcurrentScenarioHarness,
    bootstrap_scenario_run,
    prepare_concurrent_harness,
    scenario_run_id,
)


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[ConcurrentScenarioHarness]:
    scenario = prepare_concurrent_harness(tmp_path / "lifecycle.db", tmp_path / "artifacts")
    yield scenario
    scenario.close()


def _lifecycle(harness: ConcurrentScenarioHarness) -> ConcurrentLifecycleCoordinator:
    return ConcurrentLifecycleCoordinator(
        harness.writer,
        SQLitePauseStateReader(harness.database),
        harness.clock,
    )


def test_pause_signal_exactly_one_winner() -> None:
    signal = ConcurrentPauseSignal()
    signal.request(5)
    assert signal.try_acknowledge(5) is True
    assert signal.try_abort(5) is False
    assert signal.clear(5) is True

    signal.request(6)
    assert signal.try_abort(6) is True
    assert signal.try_acknowledge(6) is False
    assert signal.is_requested is False


def test_pause_signal_rejects_overlap_and_wrong_generation() -> None:
    signal = ConcurrentPauseSignal()
    signal.request(3)
    with pytest.raises(ConcurrentLifecycleBusyError):
        signal.request(4)
    assert signal.try_acknowledge(4) is False
    assert signal.try_abort(9) is False
    assert signal.requested_generation == 3


def test_complete_pause_writes_durable_arrows(harness: ConcurrentScenarioHarness) -> None:
    from paritygrid.adapters.persistence import SqlAlchemyRunRepository
    from paritygrid.application.execution.leasing import WorkLeaseService
    from paritygrid.domain.execution import RunState
    from paritygrid.domain.models import RunId

    run_id = scenario_run_id(21)
    bootstrap_scenario_run(harness, run_id)
    lifecycle = _lifecycle(harness)
    lease_service = WorkLeaseService(harness.writer, harness.clock)
    signal = ConcurrentPauseSignal()
    generation = 7
    signal.request(generation)
    reservation = lease_service.reserve_pause(RunId(run_id.value))
    proof = lifecycle.complete_pause(
        RunId(run_id.value),
        lease_service=lease_service,
        reservation=reservation,
        signal=signal,
        generation=generation,
    )
    assert proof.generation == generation
    with harness.database.transaction() as session:
        run = SqlAlchemyRunRepository(session).get(run_id)
        assert run is not None
        assert run.state is RunState.PAUSED
    report = lifecycle.resume(
        proof,
        lease_service=lease_service,
        reservation=reservation,
        signal=signal,
    )
    assert report.to_state is RunState.RUNNING
    with harness.database.transaction() as session:
        run = SqlAlchemyRunRepository(session).get(run_id)
        assert run is not None
        assert run.state is RunState.RUNNING


def test_complete_pause_rejects_when_abort_already_won(harness: ConcurrentScenarioHarness) -> None:
    from paritygrid.application.execution.leasing import WorkLeaseService
    from paritygrid.domain.models import RunId

    run_id = scenario_run_id(22)
    bootstrap_scenario_run(harness, run_id)
    lifecycle = _lifecycle(harness)
    lease_service = WorkLeaseService(harness.writer, harness.clock)
    signal = ConcurrentPauseSignal()
    signal.request(4)
    assert signal.try_abort(4) is True
    reservation = lease_service.reserve_pause(RunId(run_id.value))
    with pytest.raises(ConcurrentLifecycleRejectedError, match="compare-and-set"):
        lifecycle.complete_pause(
            RunId(run_id.value),
            lease_service=lease_service,
            reservation=reservation,
            signal=signal,
            generation=4,
        )


def test_cancellation_arrows_are_durable(harness: ConcurrentScenarioHarness) -> None:
    from paritygrid.adapters.persistence import SqlAlchemyRunRepository
    from paritygrid.domain.execution import RunState
    from paritygrid.domain.models import RunId

    run_id = scenario_run_id(23)
    bootstrap_scenario_run(harness, run_id)
    lifecycle = _lifecycle(harness)
    begun = lifecycle.begin_cancellation(RunId(run_id.value))
    assert begun.to_state is RunState.CANCELLING
    finished = lifecycle.finish_cancellation(RunId(run_id.value))
    assert finished.to_state is RunState.CANCELLED
    again = lifecycle.finish_cancellation(RunId(run_id.value))
    assert again.action == "already_cancelled"
    with harness.database.transaction() as session:
        run = SqlAlchemyRunRepository(session).get(run_id)
        assert run is not None
        assert run.state is RunState.CANCELLED
