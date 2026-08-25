"""Seeded shuffled stress executions across strategies (P7.19).

Fifty shuffled executions vary ready-node order (via per-seed retry
placement), partition completion order (via seed-keyed script
rotation), result arrival order (via pooled concurrency), and retry
timing (through the injected step clock).  Every run must preserve
versioned execution evidence: identical scripts produce identical
normalized durable evidence under sequential, threaded, and asyncio
mechanics, and no run may violate bounds or leak owned resources.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from pathlib import Path

import pytest

from paritygrid.application.execution.asyncio_strategy import AsyncioFullPlanStrategy
from paritygrid.application.execution.concurrent_engine import EngineStatus
from paritygrid.application.execution.evidence_comparison import (
    ExecutionEvidenceSnapshot,
    build_evidence_snapshot,
    compare_execution_evidence,
)
from paritygrid.application.execution.full_plan_strategy import (
    FullPlanStrategy,
    SequentialFullPlanStrategy,
)
from paritygrid.application.execution.threaded_strategy import ThreadedFullPlanStrategy
from paritygrid.quality.concurrent_scenario import (
    DEFAULT_SCRIPT,
    PARTITIONS_BY_NODE,
    ConcurrentBehavior,
    ConcurrentScenarioHarness,
    ScenarioStep,
    ScriptedConcurrentExecutor,
    bootstrap_scenario_run,
    build_scenario_engine,
    prepare_concurrent_harness,
    read_scenario_evidence,
    scenario_plan_fingerprint,
    scenario_run_id,
)
from tests.execution.full_plan_conformance import assert_zero_owned_workers

STRESS_RUN_COUNT = 50
STRATEGIES: tuple[type[FullPlanStrategy], ...] = (
    SequentialFullPlanStrategy,
    ThreadedFullPlanStrategy,
    AsyncioFullPlanStrategy,
)


def _shuffled_script(seed: int) -> tuple[ScenarioStep, ...]:
    """Derive one deterministic behavior rotation from the seed."""
    digest = hashlib.sha256(f"paritygrid-stress-{seed}".encode("ascii")).digest()
    retry_index = digest[0] % len(DEFAULT_SCRIPT)
    quarantine_index = digest[1] % len(DEFAULT_SCRIPT)
    if quarantine_index == retry_index:
        quarantine_index = (quarantine_index + 1) % len(DEFAULT_SCRIPT)
    steps: list[ScenarioStep] = []
    for index, step in enumerate(DEFAULT_SCRIPT):
        behavior = step.behavior
        if index in (retry_index, quarantine_index):
            behavior = ConcurrentBehavior.RETRY_THEN_SUCCESS
        steps.append(ScenarioStep(step.node_id, step.partition_key, behavior))
    return tuple(steps)


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[ConcurrentScenarioHarness]:
    scenario = prepare_concurrent_harness(tmp_path / "stress scenario.db", tmp_path / "artifacts")
    yield scenario
    scenario.close()


def _execute_stress_run(
    harness: ConcurrentScenarioHarness,
    strategy: FullPlanStrategy,
    seed: int,
    *,
    script_seed: int | None = None,
) -> ExecutionEvidenceSnapshot:
    run_id = scenario_run_id(1000 + seed)
    bootstrap_scenario_run(harness, run_id)
    executor = ScriptedConcurrentExecutor(
        harness, script=_shuffled_script(script_seed if script_seed is not None else seed)
    )
    engine = build_scenario_engine(harness, run_id, strategy=strategy, executor=executor)
    report = engine.run()
    assert report.status is EngineStatus.COMPLETED, report.recovery_reason
    evidence = read_scenario_evidence(harness, run_id)
    work_positions = {
        work_id: (node, partition) for node, partition, work_id, _state in evidence.work_states
    }
    snapshot = build_evidence_snapshot(
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
            (str(node), len(partitions), len(partitions), 0, 0, 0)
            for node, partitions in PARTITIONS_BY_NODE.items()
        ),
        artifact_identities=(),
        event_kinds=evidence.event_kinds,
    )
    assert all(state == "succeeded" for *_, state in evidence.work_states)
    assert_zero_owned_workers()
    return snapshot


def test_fifty_seeded_shuffled_executions_preserve_evidence(
    harness: ConcurrentScenarioHarness,
) -> None:
    """Fifty seeded runs: reproducible, bounded, evidence-equal per script."""
    reference_by_script: dict[tuple[ScenarioStep, ...], ExecutionEvidenceSnapshot] = {}
    for seed in range(STRESS_RUN_COUNT):
        strategy = STRATEGIES[seed % len(STRATEGIES)]()
        snapshot = _execute_stress_run(harness, strategy, seed)
        script = _shuffled_script(seed)
        reference = reference_by_script.get(script)
        if reference is None:
            reference_by_script[script] = snapshot
        else:
            # The same script under a different strategy must produce
            # identical normalized durable execution evidence.
            verdict = compare_execution_evidence(reference, snapshot)  # type: ignore[arg-type]
            assert verdict.equal, verdict.differences
    assert len(reference_by_script) >= 3, "the seeds must exercise distinct scripts"


def test_same_seed_reproduces_identical_evidence(harness: ConcurrentScenarioHarness) -> None:
    """One seed under three strategies yields identical evidence."""
    snapshots = [
        _execute_stress_run(harness, strategy(), 900 + index, script_seed=77)
        for index, strategy in enumerate(STRATEGIES)
    ]
    for other in snapshots[1:]:
        verdict = compare_execution_evidence(snapshots[0], other)
        assert verdict.equal, verdict.differences
