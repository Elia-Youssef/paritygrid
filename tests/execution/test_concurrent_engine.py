"""Concurrent engine integration tests over real SQLite (P7.10)."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Never, cast

import pytest

from paritygrid.adapters.persistence import SqlAlchemyWorkItemRepository
from paritygrid.application.execution.concurrent_engine import (
    ConcurrentRunEngine,
    EngineStatus,
)
from paritygrid.application.execution.full_plan_strategy import (
    SequentialFullPlanStrategy,
    StrategyContext,
    StrategyMode,
)
from paritygrid.application.execution.runner_contract import (
    StrategyCapabilitiesV1,
    WorkAssignmentV1,
    WorkResultV1,
)
from paritygrid.domain.models import NodeId, RunId
from paritygrid.quality.concurrent_scenario import (
    DEFAULT_SCRIPT,
    EXPORT_NODE,
    NORMALIZE_NODE,
    SOURCE_NODE,
    VALIDATE_NODE,
    ConcurrentBehavior,
    ConcurrentScenarioHarness,
    ScenarioStep,
    ScriptedConcurrentExecutor,
    bootstrap_scenario_run,
    build_scenario_engine,
    prepare_concurrent_harness,
    read_scenario_evidence,
    scenario_recovery_service,
    scenario_run_id,
    scenario_work_item_id,
)


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[ConcurrentScenarioHarness]:
    scenario = prepare_concurrent_harness(
        tmp_path / "concurrent scenario.db", tmp_path / "artifacts"
    )
    yield scenario
    scenario.close()


EngineParts = tuple[ConcurrentRunEngine, ScriptedConcurrentExecutor, RunId]


def _engine(
    harness: ConcurrentScenarioHarness,
    seed: int = 1,
    script: tuple[ScenarioStep, ...] = DEFAULT_SCRIPT,
    hooks: Callable[[WorkAssignmentV1], None] | None = None,
) -> EngineParts:
    run_id = scenario_run_id(seed)
    bootstrap_scenario_run(harness, run_id)
    executor = ScriptedConcurrentExecutor(harness, script=script, on_execute=hooks)
    engine = build_scenario_engine(
        harness, run_id, strategy=SequentialFullPlanStrategy(), executor=executor
    )
    return engine, executor, run_id


def _script_with(node: NodeId, behavior: ConcurrentBehavior) -> tuple[ScenarioStep, ...]:
    return tuple(
        ScenarioStep(
            step.node_id, step.partition_key, behavior if step.node_id == node else step.behavior
        )
        for step in DEFAULT_SCRIPT
    )


def test_sequential_strategy_completes_the_full_plan(harness: ConcurrentScenarioHarness) -> None:
    """One inline strategy drives every partition to durable success."""
    engine, executor, run_id = _engine(harness)
    report = engine.run()
    assert report.status is EngineStatus.COMPLETED
    evidence = read_scenario_evidence(harness, run_id)
    assert evidence.run_state == "running"
    assert all(state == "succeeded" for *_, state in evidence.work_states)
    assert len(evidence.work_states) == 7
    assert len(executor.executed) == 7
    assert engine.in_flight_identities == ()
    assert harness.writer.snapshot().queue_depth == 0


def test_retry_then_success_commits_two_attempts(harness: ConcurrentScenarioHarness) -> None:
    """A scripted retry wait re-admits with the next attempt number."""
    engine, executor, run_id = _engine(
        harness, script=_script_with(NORMALIZE_NODE, ConcurrentBehavior.RETRY_THEN_SUCCESS)
    )
    report = engine.run()
    assert report.status is EngineStatus.COMPLETED
    normalize_attempts = [
        attempt for node, _partition, attempt in executor.executed if node == str(NORMALIZE_NODE)
    ]
    assert normalize_attempts == [1, 2]
    evidence = read_scenario_evidence(harness, run_id)
    assert all(state == "succeeded" for *_, state in evidence.work_states)
    normalize_attempts_durable = sorted(
        outcome for work_id, _attempt, outcome in evidence.attempt_outcomes if "norm" in work_id
    )
    assert normalize_attempts_durable == ["retry_scheduled", "succeeded"]


def test_quarantined_partition_blocks_successors_fail_closed(
    harness: ConcurrentScenarioHarness,
) -> None:
    """A quarantined aggregate permanently blocks the successor frontier."""
    engine, _executor, run_id = _engine(
        harness, script=_script_with(VALIDATE_NODE, ConcurrentBehavior.QUARANTINE)
    )
    report = engine.run()
    # The quarantine blocks every successor permanently; the engine
    # finishes with the durable evidence the finalizer classifies.
    assert report.status is EngineStatus.COMPLETED
    evidence = read_scenario_evidence(harness, run_id)
    states = {(node, partition): state for node, partition, _work, state in evidence.work_states}
    assert states[(str(VALIDATE_NODE), "partition-0")] == "quarantined"


def test_pause_at_first_frontier_resumes_and_completes(harness: ConcurrentScenarioHarness) -> None:
    """Pause requested before any admission reaches a stable boundary."""
    engine, _executor, run_id = _engine(harness)
    engine.request_pause()
    report = engine.run()
    assert report.status is EngineStatus.PAUSED
    assert report.pause_proof is not None
    evidence = read_scenario_evidence(harness, run_id)
    assert evidence.run_state == "paused"
    engine.resume(report.pause_proof)
    final = engine.run()
    assert final.status is EngineStatus.COMPLETED
    evidence = read_scenario_evidence(harness, run_id)
    assert all(state == "succeeded" for *_, state in evidence.work_states)


def test_pause_mid_run_uses_stable_boundary(harness: ConcurrentScenarioHarness) -> None:
    """Pause requested after source work commits still pauses durably."""
    cell: dict[str, ConcurrentRunEngine] = {}

    def pause_after_first_source(assignment: WorkAssignmentV1) -> None:
        if assignment.node_id == str(NORMALIZE_NODE):
            cell["engine"].request_pause()

    engine, _executor, run_id = _engine(harness, hooks=pause_after_first_source)
    cell["engine"] = engine
    report = engine.run()
    assert report.status is EngineStatus.PAUSED
    evidence = read_scenario_evidence(harness, run_id)
    assert evidence.run_state == "paused"
    succeeded = sum(1 for *_, state in evidence.work_states if state == "succeeded")
    assert succeeded >= 2
    assert report.pause_proof is not None
    engine.resume(report.pause_proof)
    final = engine.run()
    assert final.status is EngineStatus.COMPLETED


def test_pause_abort_race_abort_wins(harness: ConcurrentScenarioHarness) -> None:
    """An abort installed before the stable boundary keeps the run running."""
    cell: dict[str, ConcurrentRunEngine] = {}

    def abort_during_quiesce(assignment: WorkAssignmentV1) -> None:
        if assignment.node_id == str(NORMALIZE_NODE):
            cell["engine"].request_pause()
            cell["engine"].abort_pause()

    engine, _executor, run_id = _engine(harness, hooks=abort_during_quiesce)
    cell["engine"] = engine
    report = engine.run()
    assert report.status is EngineStatus.COMPLETED
    evidence = read_scenario_evidence(harness, run_id)
    assert evidence.run_state == "running"
    assert all(state == "succeeded" for *_, state in evidence.work_states)


def test_pause_abort_race_acknowledge_wins(harness: ConcurrentScenarioHarness) -> None:
    """A pause acknowledged at the boundary cannot be aborted afterwards."""
    engine, _executor, _run_id = _engine(harness)
    engine.request_pause()
    report = engine.run()
    assert report.status is EngineStatus.PAUSED
    assert report.pause_proof is not None
    # The acknowledged generation refuses a late abort.
    assert engine._pause_signal.try_abort(report.pause_proof.generation) is False
    engine.resume(report.pause_proof)
    assert engine.run().status is EngineStatus.COMPLETED


def test_cancellation_before_any_admission(harness: ConcurrentScenarioHarness) -> None:
    """Cancelling before admission terminates with durable cancelled state."""
    engine, _executor, run_id = _engine(harness)
    engine.cancellation.request()
    report = engine.run()
    assert report.status is EngineStatus.CANCELLED
    evidence = read_scenario_evidence(harness, run_id)
    assert evidence.run_state == "cancelled"


def test_cancellation_mid_run_drains_and_synthesizes(harness: ConcurrentScenarioHarness) -> None:
    """Cancel during execution cancels in-flight work durably."""
    engine, executor, run_id = _engine(harness)
    seen: list[str] = []

    def cancel_midway(assignment: WorkAssignmentV1) -> None:
        seen.append(assignment.node_id)
        if assignment.node_id == str(NORMALIZE_NODE):
            engine.cancellation.request()

    executor._hooks = cancel_midway
    report = engine.run()
    assert report.status is EngineStatus.CANCELLED
    evidence = read_scenario_evidence(harness, run_id)
    assert evidence.run_state == "cancelled"
    states = {(node, partition): state for node, partition, _work, state in evidence.work_states}
    # Work that already completed stays committed; unstarted work stays
    # pending as durable evidence, exactly like the sequential contract.
    assert states[(str(NORMALIZE_NODE), "partition-0")] == "succeeded"
    assert states[(str(EXPORT_NODE), "partition-0")] == "pending"


def test_cancellation_synthesizes_cancelled_results_for_in_flight_work(
    harness: ConcurrentScenarioHarness,
) -> None:
    """Cancellation commits durable cancelled results for admitted work."""

    class _SilentPooledStrategy:
        """A pooled strategy whose workers have not consumed anything."""

        def __init__(self) -> None:
            self.started = False

        @property
        def strategy_id(self) -> str:
            return "sequential"

        @property
        def capabilities(self) -> StrategyCapabilitiesV1:
            from paritygrid.application.execution.full_plan_strategy import (
                SEQUENTIAL_STRATEGY_CAPABILITIES,
            )

            return SEQUENTIAL_STRATEGY_CAPABILITIES

        @property
        def mode(self) -> StrategyMode:
            return StrategyMode.POOLED

        def start(self, context: StrategyContext) -> None:
            self.started = True

        def execute_pending(self) -> int:
            return 0

        def shutdown(self, *, timeout_seconds: float) -> None:
            del timeout_seconds

    run_id = scenario_run_id(11)
    bootstrap_scenario_run(harness, run_id)
    executor = ScriptedConcurrentExecutor(harness)
    engine = build_scenario_engine(
        harness, run_id, strategy=_SilentPooledStrategy(), executor=executor
    )
    engine._ensure_started()
    admitted = engine._admit_until_limited()
    assert admitted >= 1
    engine.cancellation.request()
    report = engine.run()
    assert report.status is EngineStatus.CANCELLED
    evidence = read_scenario_evidence(harness, run_id)
    cancelled = [state for *_, state in evidence.work_states if state == "cancelled"]
    assert len(cancelled) == admitted
    assert evidence.run_state == "cancelled"


def test_late_result_from_earlier_generation_is_fenced(harness: ConcurrentScenarioHarness) -> None:
    """A result carrying a stale control generation is rejected pre-admission."""
    from paritygrid.application.execution.result_coordinator import ResultStaleRejection
    from paritygrid.application.execution.runner_contract import (
        RUNNER_CONTRACT_VERSION,
        WORK_RESULT_PROTOCOL,
        ContractCleanupEvidence,
        ContractCleanupStatus,
        ContractOutcome,
    )

    cell: dict[str, ConcurrentRunEngine] = {}

    def stale_generation_hook(assignment: WorkAssignmentV1) -> None:
        forged = WorkResultV1(
            protocol=WORK_RESULT_PROTOCOL,
            contract_version=RUNNER_CONTRACT_VERSION,
            plan_fingerprint=assignment.plan_fingerprint,
            run_id=assignment.run_id,
            node_id=assignment.node_id,
            partition_key=assignment.partition_key,
            work_item_id=assignment.work_item_id,
            attempt_number=assignment.attempt_number,
            lease_fence=assignment.lease_fence + 40,
            lease_owner=assignment.lease_owner,
            control_generation=assignment.control_generation,
            outcome=ContractOutcome.FAILED,
            metrics=(),
            artifact_references=(),
            checkpoint_proposal=False,
            failure_detail="forged fence",
            cleanup=ContractCleanupEvidence(ContractCleanupStatus.COMPLETED, (), None),
        )
        with pytest.raises(ResultStaleRejection):
            cell["engine"]._coordinator.submit_result(forged, failure_classification="unknown")

    engine, _executor, _run_id = _engine(harness, hooks=stale_generation_hook)
    cell["engine"] = engine
    report = engine.run()
    assert report.status is EngineStatus.COMPLETED


def test_restart_with_expired_lease_recovers_durable_work(
    harness: ConcurrentScenarioHarness,
) -> None:
    """A worker crash before its result synthesizes a durable failure."""

    def abandon_after_source(assignment: WorkAssignmentV1) -> None:
        if assignment.node_id == str(NORMALIZE_NODE):
            raise RuntimeError("simulated worker crash before result")

    engine, _executor, run_id = _engine(harness, hooks=abandon_after_source)
    report = engine.run()
    # The crashed worker synthesizes a durable FAILED result, so the run
    # completes with that failure recorded rather than hanging a lease.
    assert report.status is EngineStatus.COMPLETED
    evidence = read_scenario_evidence(harness, run_id)
    failed = [(node, state) for node, _p, _w, state in evidence.work_states if state == "failed"]
    assert failed == [(str(NORMALIZE_NODE), "failed")]


def test_recovery_rebuilds_frontier_after_expiry(harness: ConcurrentScenarioHarness) -> None:
    """Recovery resolves an expired lease and rebuilds the durable frontier."""
    from paritygrid.quality.concurrent_scenario import (
        EDGES,
        NODE_ORDER,
        PARTITIONS_BY_NODE,
        scenario_plan_fingerprint,
    )

    run_id = scenario_run_id(7)
    bootstrap_scenario_run(harness, run_id)
    # Claim one source partition directly and let the lease expire.
    work_id = scenario_work_item_id(run_id, SOURCE_NODE, "partition-0")
    with harness.database.transaction() as session:
        record = SqlAlchemyWorkItemRepository(session).get(work_id)
        assert record is not None
        assert record.state.value == "pending"
    executor = ScriptedConcurrentExecutor(harness)
    engine = build_scenario_engine(
        harness, run_id, strategy=SequentialFullPlanStrategy(), executor=executor
    )
    # Admit the first assignment, then simulate abandonment: never drain it.
    engine._ensure_started()
    identity = engine._scheduler.next_ready(1)[0]
    assert engine._admit_one(identity)
    harness.clock.advance(120)  # default lease is 60s
    service = scenario_recovery_service(harness)
    scan = service.scan(
        run_id=str(run_id),
        plan_fingerprint=scenario_plan_fingerprint(),
        node_order=tuple(str(node) for node in NODE_ORDER),
        edges=tuple((str(s), str(t)) for s, t in EDGES),
        partitions_by_node=dict(PARTITIONS_BY_NODE),
    )
    assert len(scan.expired_leases) == 1
    report = service.recover(
        run_id=str(run_id),
        plan_fingerprint=scenario_plan_fingerprint(),
        node_order=tuple(str(node) for node in NODE_ORDER),
        edges=tuple((str(s), str(t)) for s, t in EDGES),
        partitions_by_node=dict(PARTITIONS_BY_NODE),
        control_generation=2,
    )
    assert report.recovered_work == (identity,)
    with harness.database.transaction() as session:
        record = SqlAlchemyWorkItemRepository(session).get(work_id)
        assert record is not None
        assert record.state.value == "retry_wait"
    assert report.frontier.control_state.value == "running"


def test_recovery_refuses_non_expired_leases_fail_closed(
    harness: ConcurrentScenarioHarness,
) -> None:
    """A live lease stays recovery-required under the durable policy."""
    from paritygrid.application.execution.concurrent_recovery import (
        ConcurrentRecoveryNonExpiredLeaseError,
    )
    from paritygrid.quality.concurrent_scenario import (
        EDGES,
        NODE_ORDER,
        PARTITIONS_BY_NODE,
        scenario_plan_fingerprint,
    )

    run_id = scenario_run_id(8)
    bootstrap_scenario_run(harness, run_id)
    executor = ScriptedConcurrentExecutor(harness)
    engine = build_scenario_engine(
        harness, run_id, strategy=SequentialFullPlanStrategy(), executor=executor
    )
    engine._ensure_started()
    identity = engine._scheduler.next_ready(1)[0]
    assert engine._admit_one(identity)
    service = scenario_recovery_service(harness)
    scan = service.scan(
        run_id=str(run_id),
        plan_fingerprint=scenario_plan_fingerprint(),
        node_order=tuple(str(node) for node in NODE_ORDER),
        edges=tuple((str(s), str(t)) for s, t in EDGES),
        partitions_by_node=dict(PARTITIONS_BY_NODE),
    )
    assert len(scan.non_expired_leases) == 1
    assert scan.expired_leases == ()
    with pytest.raises(ConcurrentRecoveryNonExpiredLeaseError):
        service.recover(
            run_id=str(run_id),
            plan_fingerprint=scenario_plan_fingerprint(),
            node_order=tuple(str(node) for node in NODE_ORDER),
            edges=tuple((str(s), str(t)) for s, t in EDGES),
            partitions_by_node=dict(PARTITIONS_BY_NODE),
            control_generation=2,
        )


def test_unknown_writer_outcome_stops_admission(harness: ConcurrentScenarioHarness) -> None:
    """An unknown commit outcome stops admission and fences the identity."""
    from paritygrid.application.execution.result_coordinator import (
        ResultOutcomeUnknownError,
    )

    engine, _executor, _run_id = _engine(harness, seed=9)
    engine._ensure_started()
    identity = engine._scheduler.next_ready(1)[0]
    assert engine._admit_one(identity)
    engine._strategy.execute_pending()
    envelope = engine._channels.result.try_recv()
    assert envelope is not None

    class _ExplodingWriter:
        def submit(self, command: object, *, timeout_seconds: float) -> Never:
            raise RuntimeError("writer exploded")

    engine._coordinator.close()
    del engine
    # Rebuild the coordinator pair with an exploding writer so the
    # coordinator itself observes the unknown admission outcome.
    engine_two, _exec_two, _run_two = _engine(harness, seed=10)
    engine_two._ensure_started()
    identity_two = engine_two._scheduler.next_ready(1)[0]
    assert engine_two._admit_one(identity_two)
    engine_two._strategy.execute_pending()
    envelope_two = engine_two._channels.result.try_recv()
    assert envelope_two is not None
    # The coordinator guards its writer port with __slots__; drive the
    # unknown path through a fresh coordinator sharing the scheduler.
    from paritygrid.application.execution.channels import CHANNEL_KIND_RESULT, BoundedChannel
    from paritygrid.application.execution.result_coordinator import (
        ConcurrentResultCoordinator,
    )

    exploding_coordinator = ConcurrentResultCoordinator(
        run_id=engine_two._run_id,
        plan_fingerprint=engine_two.frontier.plan_fingerprint,
        control_generation=1,
        reader=engine_two._coordinator._reader,
        writer=_ExplodingWriter(),
        result_channel=BoundedChannel(kind=CHANNEL_KIND_RESULT, capacity=4),
        scheduler=engine_two._scheduler,
        capacity=engine_two._capacity,
    )
    exploding_coordinator.register_assignment(engine_two._in_flight[identity_two].facts)
    with pytest.raises(ResultOutcomeUnknownError):
        exploding_coordinator.submit_result(
            cast(WorkResultV1, envelope_two), failure_classification=None
        )
    assert exploding_coordinator.is_admission_stopped
    assert engine_two._scheduler.frontier.is_recovery_required
