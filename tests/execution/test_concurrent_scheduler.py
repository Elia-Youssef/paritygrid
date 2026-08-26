"""Frontier, admission, ordering, and serialization tests for the concurrent scheduler."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import cast

import pytest

from paritygrid.application.execution import (
    MAX_FRONTIER_REASON_LENGTH,
    MAX_FRONTIER_WORK_ITEMS,
    MAX_METRIC_VALUE,
    SCHEDULER_FRONTIER_VERSION,
    ConcurrentScheduler,
    ConcurrentSchedulerCapacityError,
    ConcurrentSchedulerError,
    ConcurrentSchedulerInvalidStateError,
    ConcurrentSchedulerTransitionError,
    ContractLifecycleState,
    ControlGeneration,
    FrontierNodeAggregate,
    FrontierRetryWait,
    FrontierWorkState,
    SchedulerFrontierCorruptError,
    SchedulerFrontierV2,
    SchedulerFrontierVersionError,
    WorkIdentity,
)

FINGERPRINT = "0123456789abcdef" * 4
OTHER_FINGERPRINT = "fedcba9876543210" * 4
RUN_ID = "run-frontier"


def _work(node_id: str, partition_key: str, run_id: str = RUN_ID) -> WorkIdentity:
    return WorkIdentity(run_id=run_id, node_id=node_id, partition_key=partition_key)


def _scheduler(
    nodes: tuple[str, ...] = ("extract", "normalize", "export"),
    edges: tuple[tuple[str, str], ...] = (
        ("extract", "normalize"),
        ("normalize", "export"),
    ),
    partitions: dict[str, tuple[str, ...]] | None = None,
    run_id: str = RUN_ID,
    plan_fingerprint: str = FINGERPRINT,
    control_generation: ControlGeneration | None = None,
) -> ConcurrentScheduler:
    partition_map = (
        partitions
        if partitions is not None
        else {"extract": ("p2", "p1"), "normalize": ("p1",), "export": ("p1",)}
    )
    return ConcurrentScheduler(
        run_id=run_id,
        plan_fingerprint=plan_fingerprint,
        node_order=nodes,
        edges=edges,
        partitions_by_node=partition_map,
        control_generation=control_generation
        if control_generation is not None
        else ControlGeneration(1),
    )


def _commit_success(
    scheduler: ConcurrentScheduler,
    identity: WorkIdentity,
    fence: int = 1,
) -> None:
    scheduler.register_admission(identity, fence)
    scheduler.commit_result(identity, "succeeded")


def _with_state(
    frontier: SchedulerFrontierV2,
    identity: WorkIdentity,
    state: FrontierWorkState,
) -> SchedulerFrontierV2:
    return replace(
        frontier,
        work_states=tuple(
            (work, state if work == identity else current) for work, current in frontier.work_states
        ),
    )


def _drive_to(scheduler: ConcurrentScheduler, state: ContractLifecycleState) -> None:
    if state is ContractLifecycleState.QUIESCING:
        scheduler.request_pause()
    elif state is ContractLifecycleState.PAUSED:
        scheduler.request_pause()
        scheduler.mark_paused()
    elif state is ContractLifecycleState.CANCELLING:
        scheduler.request_cancel()
    elif state is ContractLifecycleState.RECOVERY_REQUIRED:
        scheduler.mark_recovery_required("writer outcome unknown")


def test_module_constants() -> None:
    assert SCHEDULER_FRONTIER_VERSION == 2
    assert MAX_FRONTIER_WORK_ITEMS == 65_536
    assert MAX_FRONTIER_REASON_LENGTH == 256
    assert MAX_METRIC_VALUE == 2**31 - 1


def test_error_hierarchy_is_typed_under_one_module_base() -> None:
    for error in (
        ConcurrentSchedulerInvalidStateError,
        ConcurrentSchedulerTransitionError,
        SchedulerFrontierVersionError,
        SchedulerFrontierCorruptError,
        ConcurrentSchedulerCapacityError,
    ):
        assert issubclass(error, ConcurrentSchedulerError)
        assert issubclass(error, RuntimeError)


@pytest.mark.parametrize(
    ("state", "terminal"),
    [
        (FrontierWorkState.BLOCKED, False),
        (FrontierWorkState.READY, False),
        (FrontierWorkState.ADMITTED, False),
        (FrontierWorkState.AWAITING_COMMIT, False),
        (FrontierWorkState.RETRY_WAIT, False),
        (FrontierWorkState.SUCCEEDED, True),
        (FrontierWorkState.QUARANTINED, True),
        (FrontierWorkState.FAILED, True),
        (FrontierWorkState.CANCELLED, True),
    ],
)
def test_work_state_terminal_matrix(state: FrontierWorkState, terminal: bool) -> None:
    assert state.is_terminal is terminal
    assert FrontierWorkState(state.value) is state


def test_work_identity_as_tuple_equality_and_hash() -> None:
    identity = _work("extract", "p1")
    assert identity.as_tuple() == (RUN_ID, "extract", "p1")
    assert identity == _work("extract", "p1")
    assert hash(identity) == hash(_work("extract", "p1"))
    assert identity != _work("extract", "p2")


def test_work_identity_sort_key_orders_node_then_partition_then_run() -> None:
    first = _work("alpha", "pb")
    second = _work("beta", "pa")
    third = _work("beta", "pb")
    fourth = _work("beta", "pb", run_id="run-later")
    assert WorkIdentity.sort_key(first) == ("alpha", "pb", RUN_ID)
    assert sorted([third, fourth, second, first], key=WorkIdentity.sort_key) == [
        first,
        second,
        third,
        fourth,
    ]


def test_work_identity_repr_is_plain_and_non_secret() -> None:
    representation = repr(_work("extract", "p1"))
    assert "extract" in representation
    assert "p1" in representation
    assert "redacted" not in representation


@pytest.mark.parametrize("field", ["run_id", "node_id", "partition_key"])
def test_work_identity_rejects_empty_field(field: str) -> None:
    values: dict[str, str] = {"run_id": RUN_ID, "node_id": "extract", "partition_key": "p1"}
    values[field] = ""
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="length"):
        WorkIdentity(**values)


@pytest.mark.parametrize("field", ["run_id", "node_id", "partition_key"])
def test_work_identity_rejects_oversized_field(field: str) -> None:
    values: dict[str, str] = {"run_id": RUN_ID, "node_id": "extract", "partition_key": "p1"}
    values[field] = "a" * 129
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="length"):
        WorkIdentity(**values)


@pytest.mark.parametrize("character", ["\t", "é", "\x7f", "\n"])
def test_work_identity_rejects_non_printable_characters(character: str) -> None:
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="printable ASCII"):
        WorkIdentity(run_id=RUN_ID, node_id=f"extract{character}", partition_key="p1")


@pytest.mark.parametrize("field", ["run_id", "node_id", "partition_key"])
def test_work_identity_rejects_non_text_fields(field: str) -> None:
    values: dict[str, object] = {
        "run_id": RUN_ID,
        "node_id": "extract",
        "partition_key": "p1",
    }
    values[field] = 7
    with pytest.raises(TypeError, match="must be text"):
        WorkIdentity(**cast("dict[str, str]", values))


def test_aggregate_all_succeeded_permits_continuation() -> None:
    aggregate = FrontierNodeAggregate(
        node_id="extract",
        total=3,
        succeeded=3,
        quarantined=0,
        failed=0,
        cancelled=0,
        in_flight=0,
        retry_wait=0,
    )
    assert aggregate.is_complete
    assert aggregate.permits_continuation
    assert not aggregate.is_blocked_terminal


def test_aggregate_partial_success_does_not_permit_continuation() -> None:
    aggregate = FrontierNodeAggregate(
        node_id="extract",
        total=3,
        succeeded=2,
        quarantined=0,
        failed=0,
        cancelled=0,
        in_flight=1,
        retry_wait=0,
    )
    assert not aggregate.is_complete
    assert not aggregate.permits_continuation
    assert not aggregate.is_blocked_terminal


@pytest.mark.parametrize("field", ["quarantined", "failed", "cancelled"])
def test_aggregate_blocking_outcomes_are_blocked_terminal(field: str) -> None:
    counts: dict[str, int] = {
        "succeeded": 0,
        "quarantined": 0,
        "failed": 0,
        "cancelled": 0,
    }
    counts[field] = 1
    aggregate = FrontierNodeAggregate(
        node_id="extract",
        total=2,
        succeeded=counts["succeeded"],
        quarantined=counts["quarantined"],
        failed=counts["failed"],
        cancelled=counts["cancelled"],
        in_flight=1,
        retry_wait=0,
    )
    assert aggregate.is_blocked_terminal
    assert not aggregate.permits_continuation
    assert not aggregate.is_complete


def test_aggregate_in_flight_and_retry_wait_prevent_continuation() -> None:
    in_flight = FrontierNodeAggregate(
        node_id="a",
        total=2,
        succeeded=1,
        quarantined=0,
        failed=0,
        cancelled=0,
        in_flight=1,
        retry_wait=0,
    )
    waiting = FrontierNodeAggregate(
        node_id="a",
        total=2,
        succeeded=1,
        quarantined=0,
        failed=0,
        cancelled=0,
        in_flight=0,
        retry_wait=1,
    )
    assert not in_flight.permits_continuation
    assert not waiting.permits_continuation


def test_aggregate_zero_total_is_not_complete() -> None:
    aggregate = FrontierNodeAggregate(
        node_id="a",
        total=0,
        succeeded=0,
        quarantined=0,
        failed=0,
        cancelled=0,
        in_flight=0,
        retry_wait=0,
    )
    assert not aggregate.is_complete
    assert not aggregate.permits_continuation
    assert not aggregate.is_blocked_terminal


@pytest.mark.parametrize("field", ["total", "succeeded", "in_flight", "retry_wait"])
def test_aggregate_rejects_negative_counts(field: str) -> None:
    values: dict[str, int] = {
        "total": 2,
        "succeeded": 0,
        "quarantined": 0,
        "failed": 0,
        "cancelled": 0,
        "in_flight": 0,
        "retry_wait": 0,
    }
    values[field] = -1
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="supported range"):
        FrontierNodeAggregate(node_id="a", **values)


def test_aggregate_rejects_non_integer_and_boolean_counts() -> None:
    with pytest.raises(TypeError, match="integer"):
        FrontierNodeAggregate(
            node_id="a",
            total=cast(int, cast(object, "2")),
            succeeded=0,
            quarantined=0,
            failed=0,
            cancelled=0,
            in_flight=0,
            retry_wait=0,
        )
    with pytest.raises(TypeError, match="integer"):
        FrontierNodeAggregate(
            node_id="a",
            total=2,
            succeeded=cast(int, cast(object, True)),
            quarantined=0,
            failed=0,
            cancelled=0,
            in_flight=0,
            retry_wait=0,
        )


def test_aggregate_rejects_counts_exceeding_total() -> None:
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="terminal count exceeds"):
        FrontierNodeAggregate(
            node_id="a",
            total=1,
            succeeded=1,
            quarantined=1,
            failed=0,
            cancelled=0,
            in_flight=0,
            retry_wait=0,
        )
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="counts exceed the node total"):
        FrontierNodeAggregate(
            node_id="a",
            total=1,
            succeeded=1,
            quarantined=0,
            failed=0,
            cancelled=0,
            in_flight=1,
            retry_wait=0,
        )


def test_aggregate_rejects_invalid_node_identity() -> None:
    with pytest.raises(TypeError, match="text"):
        FrontierNodeAggregate(
            node_id=cast(str, cast(object, 5)),
            total=1,
            succeeded=1,
            quarantined=0,
            failed=0,
            cancelled=0,
            in_flight=0,
            retry_wait=0,
        )
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="printable ASCII"):
        FrontierNodeAggregate(
            node_id="a\t",
            total=1,
            succeeded=1,
            quarantined=0,
            failed=0,
            cancelled=0,
            in_flight=0,
            retry_wait=0,
        )


def test_retry_wait_holds_identity_eligibility_and_reason() -> None:
    wait = FrontierRetryWait(
        identity=_work("extract", "p1"),
        eligible_at_micros=1_000,
        reason="http-429 retry after",
    )
    assert wait.identity == _work("extract", "p1")
    assert wait.eligible_at_micros == 1_000
    assert wait.reason == "http-429 retry after"


def test_retry_wait_validation_matrix() -> None:
    identity = _work("extract", "p1")
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="supported range"):
        FrontierRetryWait(identity=identity, eligible_at_micros=-1, reason="retry")
    with pytest.raises(TypeError, match="integer"):
        FrontierRetryWait(
            identity=identity,
            eligible_at_micros=cast(int, cast(object, True)),
            reason="retry",
        )
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="length"):
        FrontierRetryWait(identity=identity, eligible_at_micros=1, reason="")
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="length"):
        FrontierRetryWait(
            identity=identity,
            eligible_at_micros=1,
            reason="a" * (MAX_FRONTIER_REASON_LENGTH + 1),
        )
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="printable ASCII"):
        FrontierRetryWait(identity=identity, eligible_at_micros=1, reason="wait\t")
    with pytest.raises(TypeError, match="text"):
        FrontierRetryWait(
            identity=identity,
            eligible_at_micros=1,
            reason=cast(str, cast(object, 3)),
        )
    with pytest.raises(TypeError, match="WorkIdentity"):
        FrontierRetryWait(
            identity=cast(WorkIdentity, cast(object, "run")),
            eligible_at_micros=1,
            reason="retry",
        )


def test_initial_frontier_blocks_successors_and_readies_sources() -> None:
    scheduler = _scheduler()
    frontier = scheduler.frontier
    assert scheduler.next_ready(10) == (_work("extract", "p1"), _work("extract", "p2"))
    assert dict(frontier.work_states)[_work("normalize", "p1")] is FrontierWorkState.BLOCKED
    assert dict(frontier.work_states)[_work("export", "p1")] is FrontierWorkState.BLOCKED
    assert dict(frontier.work_states)[_work("extract", "p1")] is FrontierWorkState.READY
    assert frontier.in_flight_identities == ()
    assert not scheduler.is_finished
    assert scheduler.failed_node_ids == ()


def test_partitions_are_ordered_deterministically_regardless_of_input() -> None:
    scheduler = _scheduler(
        partitions={"extract": ("z", "a", "m"), "normalize": ("p1",), "export": ("p1",)},
    )
    assert scheduler.next_ready(10) == (
        _work("extract", "a"),
        _work("extract", "m"),
        _work("extract", "z"),
    )


def test_constructor_validation_matrix() -> None:
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="not be empty"):
        _scheduler(nodes=(), edges=(), partitions={})
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="unique"):
        _scheduler(
            nodes=("extract", "extract"),
            edges=(),
            partitions={"extract": ("p1",)},
        )
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="known node"):
        _scheduler(edges=(("extract", "ghost"),))
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="self dependency"):
        _scheduler(edges=(("extract", "extract"),))
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="directed cycle"):
        _scheduler(
            edges=(
                ("extract", "normalize"),
                ("normalize", "export"),
                ("export", "extract"),
            )
        )
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="unique"):
        _scheduler(edges=(("extract", "normalize"), ("extract", "normalize")))
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="cover every planned node"):
        _scheduler(
            partitions={"extract": ("p1", "p2")},
        )
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="cover every planned node"):
        _scheduler(
            partitions={
                "extract": ("p1", "p2"),
                "normalize": ("p1",),
                "export": ("p1",),
                "ghost": ("p1",),
            },
        )
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="not be empty"):
        _scheduler(
            partitions={"extract": ("p2", "p1"), "normalize": (), "export": ("p1",)},
        )
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="unique"):
        _scheduler(
            partitions={"extract": ("p1", "p1"), "normalize": ("p1",), "export": ("p1",)},
        )


def test_constructor_rejects_invalid_plan_fingerprints() -> None:
    for fingerprint in ("", "ABCDEF0123456789" * 4, "0123456789abcde", "no-hex-" + "0" * 56):
        with pytest.raises(ConcurrentSchedulerInvalidStateError, match="hexadecimal"):
            _scheduler(plan_fingerprint=fingerprint)


def test_constructor_rejects_invalid_identity_text() -> None:
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="printable ASCII"):
        _scheduler(run_id="run\tfrontier")
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="printable ASCII"):
        _scheduler(
            partitions={"extract": ("p2", "p1\t"), "normalize": ("p1",), "export": ("p1",)},
        )
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="printable ASCII"):
        _scheduler(nodes=("extráct", "normalize", "export"))


def test_constructor_rejects_work_item_bound() -> None:
    partitions = {
        "solo": tuple(f"p{index:06d}" for index in range(MAX_FRONTIER_WORK_ITEMS + 1)),
    }
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="work item bound"):
        _scheduler(nodes=("solo",), edges=(), partitions=partitions)


def test_constructor_type_mismatch_matrix() -> None:
    with pytest.raises(TypeError, match="text"):
        _scheduler(run_id=cast(str, cast(object, 5)))
    with pytest.raises(TypeError, match="text"):
        _scheduler(plan_fingerprint=cast(str, cast(object, 5)))
    with pytest.raises(TypeError, match="tuple"):
        _scheduler(
            nodes=cast("tuple[str, ...]", ["extract", "normalize", "export"]),
        )
    with pytest.raises(TypeError, match="invalid value"):
        _scheduler(nodes=cast("tuple[str, ...]", ("extract", "normalize", 5)))
    with pytest.raises(TypeError, match="tuple"):
        _scheduler(edges=cast("tuple[tuple[str, str], ...]", [["extract", "normalize"]]))
    with pytest.raises(TypeError, match="source-target pairs"):
        _scheduler(edges=cast("tuple[tuple[str, str], ...]", ("extract->normalize",)))
    with pytest.raises(TypeError, match="source-target pairs"):
        _scheduler(edges=cast("tuple[tuple[str, str], ...]", (("extract",),)))
    with pytest.raises(TypeError, match="source-target pairs"):
        _scheduler(
            edges=cast("tuple[tuple[str, str], ...]", (("extract", "normalize", "export"),)),
        )
    with pytest.raises(TypeError, match="text"):
        _scheduler(edges=(("extract", cast(str, cast(object, 4))),))
    with pytest.raises(TypeError, match="dict"):
        _scheduler(partitions=cast("dict[str, tuple[str, ...]]", cast(object, [("extract", ())])))
    with pytest.raises(TypeError, match="text"):
        _scheduler(
            partitions=cast(
                "dict[str, tuple[str, ...]]",
                {5: ("p1",), "normalize": ("p1",), "export": ("p1",)},
            ),
        )
    with pytest.raises(TypeError, match="tuple"):
        _scheduler(
            partitions=cast(
                "dict[str, tuple[str, ...]]",
                {"extract": ["p1"], "normalize": ("p1",), "export": ("p1",)},
            ),
        )
    with pytest.raises(TypeError, match="text"):
        _scheduler(
            partitions=cast(
                "dict[str, tuple[str, ...]]",
                {"extract": cast("tuple[str, ...]", (5,)), "normalize": ("p1",), "export": ("p1",)},
            ),
        )
    with pytest.raises(TypeError, match="ControlGeneration"):
        _scheduler(control_generation=cast(ControlGeneration, cast(object, 1)))


def test_next_ready_orders_by_plan_node_then_partition() -> None:
    scheduler = _scheduler(
        nodes=("zeta", "alpha"),
        edges=(),
        partitions={"zeta": ("b", "a"), "alpha": ("c",)},
    )
    assert scheduler.next_ready(10) == (
        _work("zeta", "a"),
        _work("zeta", "b"),
        _work("alpha", "c"),
    )


def test_next_ready_respects_limit_and_skips_non_ready() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 1)
    assert scheduler.next_ready(1) == (_work("extract", "p2"),)
    assert scheduler.next_ready(99) == (_work("extract", "p2"),)


def test_next_ready_limit_validation() -> None:
    scheduler = _scheduler()
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="positive"):
        scheduler.next_ready(0)
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="positive"):
        scheduler.next_ready(-3)
    with pytest.raises(TypeError, match="integer"):
        scheduler.next_ready(cast(int, cast(object, True)))
    with pytest.raises(TypeError, match="integer"):
        scheduler.next_ready(cast(int, cast(object, 1.0)))


def test_next_ready_empty_when_nothing_ready() -> None:
    scheduler = _scheduler()
    _commit_success(scheduler, _work("extract", "p1"))
    _commit_success(scheduler, _work("extract", "p2"))
    _commit_success(scheduler, _work("normalize", "p1"))
    _commit_success(scheduler, _work("export", "p1"))
    assert scheduler.next_ready(4) == ()
    assert scheduler.is_finished


@pytest.mark.parametrize(
    "state",
    [
        ContractLifecycleState.QUIESCING,
        ContractLifecycleState.PAUSED,
        ContractLifecycleState.CANCELLING,
        ContractLifecycleState.RECOVERY_REQUIRED,
    ],
)
def test_next_ready_empty_outside_running_control_state(state: ContractLifecycleState) -> None:
    scheduler = _scheduler()
    _drive_to(scheduler, state)
    assert scheduler.next_ready(4) == ()
    assert scheduler.frontier.control_state is state


def test_register_admission_records_state_and_fence() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 41)
    frontier = scheduler.frontier
    assert dict(frontier.work_states)[_work("extract", "p1")] is FrontierWorkState.ADMITTED
    assert dict(frontier.lease_fences)[_work("extract", "p1")] == 41
    assert frontier.in_flight_identities == (_work("extract", "p1"),)


def test_register_admission_duplicate_raises_capacity_error() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 1)
    with pytest.raises(ConcurrentSchedulerCapacityError, match="already admitted"):
        scheduler.register_admission(_work("extract", "p1"), 2)
    scheduler.mark_result_received(_work("extract", "p1"))
    with pytest.raises(ConcurrentSchedulerCapacityError, match="already admitted"):
        scheduler.register_admission(_work("extract", "p1"), 3)
    assert scheduler.frontier.in_flight_identities == (_work("extract", "p1"),)


def test_register_admission_requires_ready_state() -> None:
    scheduler = _scheduler()
    with pytest.raises(ConcurrentSchedulerTransitionError, match="only ready"):
        scheduler.register_admission(_work("normalize", "p1"), 1)
    _commit_success(scheduler, _work("extract", "p1"))
    _commit_success(scheduler, _work("extract", "p2"))
    scheduler.register_admission(_work("normalize", "p1"), 1)
    scheduler.schedule_retry(_work("normalize", "p1"), 10, "timeout")
    with pytest.raises(ConcurrentSchedulerTransitionError, match="only ready"):
        scheduler.register_admission(_work("normalize", "p1"), 2)


def test_terminal_identity_is_never_readmitted() -> None:
    scheduler = _scheduler()
    _commit_success(scheduler, _work("extract", "p1"))
    with pytest.raises(ConcurrentSchedulerTransitionError, match="only ready"):
        scheduler.register_admission(_work("extract", "p1"), 9)
    scheduler.register_admission(_work("extract", "p2"), 2)
    scheduler.commit_result(_work("extract", "p2"), "failed")
    with pytest.raises(ConcurrentSchedulerTransitionError, match="only ready"):
        scheduler.register_admission(_work("extract", "p2"), 9)


def test_register_admission_rejects_unknown_identities() -> None:
    scheduler = _scheduler()
    for identity in (
        _work("extract", "ghost"),
        _work("ghost", "p1"),
        _work("extract", "p1", run_id="run-other"),
    ):
        with pytest.raises(ConcurrentSchedulerInvalidStateError, match="unknown"):
            scheduler.register_admission(identity, 1)


def test_register_admission_fence_bounds() -> None:
    scheduler = _scheduler()
    for fence in (0, -1, 2**31, cast(int, cast(object, True))):
        with pytest.raises((ConcurrentSchedulerInvalidStateError, TypeError)):
            scheduler.register_admission(_work("extract", "p1"), fence)
    scheduler.register_admission(_work("extract", "p1"), 2**31 - 1)
    assert dict(scheduler.frontier.lease_fences)[_work("extract", "p1")] == 2**31 - 1


def test_register_admission_stops_outside_running_control_state() -> None:
    scheduler = _scheduler()
    scheduler.request_pause()
    with pytest.raises(ConcurrentSchedulerTransitionError, match="does not admit"):
        scheduler.register_admission(_work("extract", "p1"), 1)
    scheduler.mark_paused()
    with pytest.raises(ConcurrentSchedulerTransitionError, match="does not admit"):
        scheduler.register_admission(_work("extract", "p1"), 1)


def test_register_admission_rejects_non_identity_type() -> None:
    scheduler = _scheduler()
    with pytest.raises(TypeError, match="WorkIdentity"):
        scheduler.register_admission(
            cast(WorkIdentity, cast(object, (RUN_ID, "extract", "p1"))),
            1,
        )


def test_mark_result_received_moves_admitted_to_awaiting_commit() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 7)
    scheduler.mark_result_received(_work("extract", "p1"))
    states = dict(scheduler.frontier.work_states)
    assert states[_work("extract", "p1")] is FrontierWorkState.AWAITING_COMMIT
    assert dict(scheduler.frontier.lease_fences)[_work("extract", "p1")] == 7


def test_mark_result_received_keeps_capacity_in_flight() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 7)
    before = scheduler.frontier.in_flight_identities
    scheduler.mark_result_received(_work("extract", "p1"))
    assert scheduler.frontier.in_flight_identities == before
    assert dict(scheduler.frontier.work_states)[_work("normalize", "p1")] is (
        FrontierWorkState.BLOCKED
    )


def test_mark_result_received_requires_admitted_state() -> None:
    scheduler = _scheduler()
    with pytest.raises(ConcurrentSchedulerTransitionError, match="admitted"):
        scheduler.mark_result_received(_work("extract", "p1"))
    scheduler.register_admission(_work("extract", "p1"), 1)
    scheduler.mark_result_received(_work("extract", "p1"))
    with pytest.raises(ConcurrentSchedulerTransitionError, match="admitted"):
        scheduler.mark_result_received(_work("extract", "p1"))
    scheduler.commit_result(_work("extract", "p1"), "succeeded")
    with pytest.raises(ConcurrentSchedulerTransitionError, match="admitted"):
        scheduler.mark_result_received(_work("extract", "p1"))


def test_mark_result_received_rejects_unknown_and_foreign_identities() -> None:
    scheduler = _scheduler()
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="unknown"):
        scheduler.mark_result_received(_work("ghost", "p1"))
    with pytest.raises(TypeError, match="WorkIdentity"):
        scheduler.mark_result_received(cast(WorkIdentity, cast(object, None)))


@pytest.mark.parametrize(
    "outcome",
    ["succeeded", "quarantined", "failed", "cancelled"],
)
def test_commit_result_accepts_every_terminal_outcome(outcome: str) -> None:
    scheduler = _scheduler(nodes=("solo",), edges=(), partitions={"solo": ("p1",)})
    scheduler.register_admission(_work("solo", "p1"), 3)
    scheduler.commit_result(_work("solo", "p1"), outcome)
    assert dict(scheduler.frontier.work_states)[_work("solo", "p1")] is (FrontierWorkState(outcome))
    assert scheduler.frontier.lease_fences == ()
    assert scheduler.is_finished


def test_commit_result_removes_fence_and_releases_capacity() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 5)
    scheduler.register_admission(_work("extract", "p2"), 6)
    scheduler.mark_result_received(_work("extract", "p1"))
    scheduler.commit_result(_work("extract", "p1"), "succeeded")
    frontier = scheduler.frontier
    assert frontier.lease_fences == ((_work("extract", "p2"), 6),)
    assert frontier.in_flight_identities == (_work("extract", "p2"),)


def test_commit_result_requires_in_flight_state() -> None:
    scheduler = _scheduler()
    with pytest.raises(ConcurrentSchedulerTransitionError, match="in-flight"):
        scheduler.commit_result(_work("extract", "p1"), "succeeded")
    with pytest.raises(ConcurrentSchedulerTransitionError, match="in-flight"):
        scheduler.commit_result(_work("normalize", "p1"), "succeeded")
    _commit_success(scheduler, _work("extract", "p1"))
    with pytest.raises(ConcurrentSchedulerTransitionError, match="in-flight"):
        scheduler.commit_result(_work("extract", "p1"), "failed")


def test_commit_result_rejects_unknown_or_non_terminal_outcomes() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 1)
    for outcome in ("exploded", "", "SUCCEEDED", "succeeded ", "blocked", "ready", "admitted"):
        with pytest.raises(ConcurrentSchedulerInvalidStateError):
            scheduler.commit_result(_work("extract", "p1"), outcome)
    for outcome in ("awaiting_commit", "retry_wait"):
        with pytest.raises(ConcurrentSchedulerInvalidStateError, match="terminal"):
            scheduler.commit_result(_work("extract", "p1"), outcome)
    with pytest.raises(TypeError, match="text"):
        scheduler.commit_result(_work("extract", "p1"), cast(str, cast(object, 1)))


def test_commit_result_rejects_unknown_identity_and_type() -> None:
    scheduler = _scheduler()
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="unknown"):
        scheduler.commit_result(_work("ghost", "p1"), "succeeded")
    with pytest.raises(TypeError, match="WorkIdentity"):
        scheduler.commit_result(cast(WorkIdentity, cast(object, "x")), "succeeded")


def test_successor_blocked_while_any_predecessor_item_awaits_commit() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 1)
    scheduler.register_admission(_work("extract", "p2"), 2)
    scheduler.commit_result(_work("extract", "p1"), "succeeded")
    scheduler.mark_result_received(_work("extract", "p2"))
    assert dict(scheduler.frontier.work_states)[_work("normalize", "p1")] is (
        FrontierWorkState.BLOCKED
    )
    assert scheduler.frontier.ready_identities == ()


def test_result_received_never_releases_dependency() -> None:
    scheduler = _scheduler(
        nodes=("source", "sink"),
        edges=(("source", "sink"),),
        partitions={"source": ("p1",), "sink": ("p1",)},
    )
    scheduler.register_admission(_work("source", "p1"), 1)
    scheduler.mark_result_received(_work("source", "p1"))
    assert dict(scheduler.frontier.work_states)[_work("sink", "p1")] is (FrontierWorkState.BLOCKED)
    scheduler.commit_result(_work("source", "p1"), "succeeded")
    assert dict(scheduler.frontier.work_states)[_work("sink", "p1")] is FrontierWorkState.READY


def test_successor_released_only_after_every_predecessor_item_commits() -> None:
    scheduler = _scheduler()
    _commit_success(scheduler, _work("extract", "p1"))
    assert dict(scheduler.frontier.work_states)[_work("normalize", "p1")] is (
        FrontierWorkState.BLOCKED
    )
    _commit_success(scheduler, _work("extract", "p2"))
    assert dict(scheduler.frontier.work_states)[_work("normalize", "p1")] is (
        FrontierWorkState.READY
    )


def test_multi_predecessor_barrier_requires_every_predecessor() -> None:
    scheduler = _scheduler(
        nodes=("alpha", "beta", "gamma"),
        edges=(("alpha", "gamma"), ("beta", "gamma")),
        partitions={"alpha": ("pa",), "beta": ("pb",), "gamma": ("pc",)},
    )
    _commit_success(scheduler, _work("alpha", "pa"))
    assert dict(scheduler.frontier.work_states)[_work("gamma", "pc")] is (FrontierWorkState.BLOCKED)
    _commit_success(scheduler, _work("beta", "pb"))
    assert dict(scheduler.frontier.work_states)[_work("gamma", "pc")] is (FrontierWorkState.READY)


def test_successor_items_become_ready_together_in_partition_order() -> None:
    scheduler = _scheduler(
        nodes=("source", "sink"),
        edges=(("source", "sink"),),
        partitions={"source": ("p1",), "sink": ("z", "a")},
    )
    _commit_success(scheduler, _work("source", "p1"))
    assert scheduler.next_ready(10) == (_work("sink", "a"), _work("sink", "z"))


@pytest.mark.parametrize("outcome", ["quarantined", "failed", "cancelled"])
def test_blocking_predecessor_blocks_successor_permanently(outcome: str) -> None:
    scheduler = _scheduler(
        nodes=("source", "sink"),
        edges=(("source", "sink"),),
        partitions={"source": ("p1",), "sink": ("p1",)},
    )
    scheduler.register_admission(_work("source", "p1"), 1)
    scheduler.commit_result(_work("source", "p1"), outcome)
    assert dict(scheduler.frontier.work_states)[_work("sink", "p1")] is (FrontierWorkState.BLOCKED)
    assert scheduler.next_ready(10) == ()
    assert scheduler.failed_node_ids == ("source",)
    assert not scheduler.is_finished


def test_quarantined_predecessor_blocks_even_after_sibling_success() -> None:
    scheduler = _scheduler(
        nodes=("source", "sink"),
        edges=(("source", "sink"),),
        partitions={"source": ("p1", "p2"), "sink": ("q",)},
    )
    scheduler.register_admission(_work("source", "p1"), 1)
    scheduler.commit_result(_work("source", "p1"), "quarantined")
    _commit_success(scheduler, _work("source", "p2"))
    assert dict(scheduler.frontier.work_states)[_work("sink", "q")] is (FrontierWorkState.BLOCKED)
    assert scheduler.failed_node_ids == ("source",)


def test_retry_wait_predecessor_does_not_release_dependency() -> None:
    scheduler = _scheduler(
        nodes=("source", "sink"),
        edges=(("source", "sink"),),
        partitions={"source": ("p1",), "sink": ("p1",)},
    )
    scheduler.register_admission(_work("source", "p1"), 1)
    scheduler.schedule_retry(_work("source", "p1"), 100, "timeout")
    assert dict(scheduler.frontier.work_states)[_work("sink", "p1")] is (FrontierWorkState.BLOCKED)
    assert scheduler.retry_eligible(100) == (_work("source", "p1"),)
    assert dict(scheduler.frontier.work_states)[_work("sink", "p1")] is (FrontierWorkState.BLOCKED)


def test_failed_nodes_surface_in_plan_order() -> None:
    scheduler = _scheduler(
        nodes=("one", "two", "three"),
        edges=(),
        partitions={"one": ("p",), "two": ("p",), "three": ("p",)},
    )
    scheduler.register_admission(_work("three", "p"), 1)
    scheduler.commit_result(_work("three", "p"), "cancelled")
    scheduler.register_admission(_work("one", "p"), 2)
    scheduler.commit_result(_work("one", "p"), "failed")
    assert scheduler.failed_node_ids == ("one", "three")


def test_independent_branches_continue_after_failure_elsewhere() -> None:
    scheduler = _scheduler(
        nodes=("a", "b", "c"),
        edges=(("a", "b"),),
        partitions={"a": ("p1",), "b": ("p1",), "c": ("p1",)},
    )
    scheduler.register_admission(_work("a", "p1"), 1)
    scheduler.commit_result(_work("a", "p1"), "failed")
    assert scheduler.next_ready(10) == (_work("c", "p1"),)
    assert dict(scheduler.frontier.work_states)[_work("b", "p1")] is FrontierWorkState.BLOCKED


def test_schedule_retry_from_admitted_releases_capacity() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 4)
    scheduler.schedule_retry(_work("extract", "p1"), 500, "http 503")
    frontier = scheduler.frontier
    assert dict(frontier.work_states)[_work("extract", "p1")] is FrontierWorkState.RETRY_WAIT
    assert frontier.lease_fences == ()
    assert frontier.in_flight_identities == ()
    assert frontier.retry_waits == (FrontierRetryWait(_work("extract", "p1"), 500, "http 503"),)


def test_schedule_retry_from_awaiting_commit() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 4)
    scheduler.mark_result_received(_work("extract", "p1"))
    scheduler.schedule_retry(_work("extract", "p1"), 5, "sqlite contention")
    frontier = scheduler.frontier
    assert dict(frontier.work_states)[_work("extract", "p1")] is FrontierWorkState.RETRY_WAIT
    assert frontier.in_flight_identities == ()


def test_schedule_retry_requires_in_flight_state() -> None:
    scheduler = _scheduler()
    with pytest.raises(ConcurrentSchedulerTransitionError, match="in-flight"):
        scheduler.schedule_retry(_work("extract", "p1"), 5, "timeout")
    with pytest.raises(ConcurrentSchedulerTransitionError, match="in-flight"):
        scheduler.schedule_retry(_work("normalize", "p1"), 5, "timeout")
    _commit_success(scheduler, _work("extract", "p1"))
    with pytest.raises(ConcurrentSchedulerTransitionError, match="in-flight"):
        scheduler.schedule_retry(_work("extract", "p1"), 5, "timeout")


def test_retry_wait_counts_surface_in_failed_node_scan() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 1)
    scheduler.schedule_retry(_work("extract", "p1"), 10, "timeout")
    assert scheduler.failed_node_ids == ()
    assert not scheduler.is_finished


def test_schedule_retry_validation_matrix() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 4)
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="negative"):
        scheduler.schedule_retry(_work("extract", "p1"), -1, "timeout")
    with pytest.raises(TypeError, match="integer"):
        scheduler.schedule_retry(
            _work("extract", "p1"),
            cast(int, cast(object, True)),
            "timeout",
        )
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="length"):
        scheduler.schedule_retry(_work("extract", "p1"), 1, "")
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="length"):
        scheduler.schedule_retry(
            _work("extract", "p1"),
            1,
            "r" * (MAX_FRONTIER_REASON_LENGTH + 1),
        )
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="printable ASCII"):
        scheduler.schedule_retry(_work("extract", "p1"), 1, "wait\n")
    with pytest.raises(TypeError, match="text"):
        scheduler.schedule_retry(
            _work("extract", "p1"),
            1,
            cast(str, cast(object, 9)),
        )
    with pytest.raises(TypeError, match="WorkIdentity"):
        scheduler.schedule_retry(cast(WorkIdentity, cast(object, 0)), 1, "timeout")
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="unknown"):
        scheduler.schedule_retry(_work("ghost", "p1"), 1, "timeout")


def test_retry_eligible_boundary_is_inclusive() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 1)
    scheduler.schedule_retry(_work("extract", "p1"), 1_000, "http 429")
    assert scheduler.retry_eligible(999) == ()
    assert dict(scheduler.frontier.work_states)[_work("extract", "p1")] is (
        FrontierWorkState.RETRY_WAIT
    )
    assert scheduler.retry_eligible(1_000) == (_work("extract", "p1"),)
    assert dict(scheduler.frontier.work_states)[_work("extract", "p1")] is (FrontierWorkState.READY)
    assert scheduler.frontier.retry_waits == ()


def test_retry_eligible_orders_multiple_due_items_deterministically() -> None:
    scheduler = _scheduler(
        nodes=("zeta", "alpha"),
        edges=(),
        partitions={"zeta": ("b", "a"), "alpha": ("c",)},
    )
    for node_id, partition in (("zeta", "b"), ("zeta", "a"), ("alpha", "c")):
        scheduler.register_admission(_work(node_id, partition), 1)
        scheduler.schedule_retry(_work(node_id, partition), 10, "timeout")
    assert scheduler.retry_eligible(10) == (
        _work("zeta", "a"),
        _work("zeta", "b"),
        _work("alpha", "c"),
    )


def test_retry_eligible_keeps_future_waits_and_mixes_due_items() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 1)
    scheduler.register_admission(_work("extract", "p2"), 2)
    scheduler.schedule_retry(_work("extract", "p1"), 10, "http 429")
    scheduler.schedule_retry(_work("extract", "p2"), 20, "timeout")
    assert scheduler.retry_eligible(15) == (_work("extract", "p1"),)
    states = dict(scheduler.frontier.work_states)
    assert states[_work("extract", "p1")] is FrontierWorkState.READY
    assert states[_work("extract", "p2")] is FrontierWorkState.RETRY_WAIT


def test_retry_eligible_identity_can_be_admitted_again() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 1)
    scheduler.schedule_retry(_work("extract", "p1"), 10, "timeout")
    assert scheduler.retry_eligible(10) == (_work("extract", "p1"),)
    scheduler.register_admission(_work("extract", "p1"), 2)
    assert scheduler.frontier.in_flight_identities == (_work("extract", "p1"),)
    scheduler.commit_result(_work("extract", "p1"), "succeeded")


def test_retry_eligible_validation() -> None:
    scheduler = _scheduler()
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="negative"):
        scheduler.retry_eligible(-1)
    with pytest.raises(TypeError, match="integer"):
        scheduler.retry_eligible(cast(int, cast(object, False)))


def test_pause_resume_cycle_increments_control_generations() -> None:
    scheduler = _scheduler()
    assert scheduler.request_pause().value == 2
    assert scheduler.frontier.control_state is ContractLifecycleState.QUIESCING
    assert scheduler.frontier.control_generation.value == 2
    scheduler.mark_paused()
    assert scheduler.frontier.control_state is ContractLifecycleState.PAUSED
    assert scheduler.resume().value == 3
    assert scheduler.frontier.control_state is ContractLifecycleState.RUNNING
    assert scheduler.frontier.control_generation.value == 3


@pytest.mark.parametrize(
    "state",
    [
        ContractLifecycleState.QUIESCING,
        ContractLifecycleState.PAUSED,
        ContractLifecycleState.CANCELLING,
        ContractLifecycleState.RECOVERY_REQUIRED,
    ],
)
def test_request_pause_requires_running(state: ContractLifecycleState) -> None:
    scheduler = _scheduler()
    _drive_to(scheduler, state)
    with pytest.raises(ConcurrentSchedulerTransitionError, match="cannot request a pause"):
        scheduler.request_pause()


def test_mark_paused_requires_quiescing() -> None:
    scheduler = _scheduler()
    with pytest.raises(ConcurrentSchedulerTransitionError, match="cannot mark paused"):
        scheduler.mark_paused()
    scheduler.request_pause()
    scheduler.mark_paused()
    with pytest.raises(ConcurrentSchedulerTransitionError, match="cannot mark paused"):
        scheduler.mark_paused()


def test_resume_requires_paused() -> None:
    scheduler = _scheduler()
    with pytest.raises(ConcurrentSchedulerTransitionError, match="cannot resume"):
        scheduler.resume()
    scheduler.request_pause()
    with pytest.raises(ConcurrentSchedulerTransitionError, match="cannot resume"):
        scheduler.resume()
    scheduler.mark_paused()
    assert scheduler.resume().value == 3


def test_cancellation_is_one_way() -> None:
    scheduler = _scheduler()
    scheduler.request_cancel()
    assert scheduler.frontier.control_state is ContractLifecycleState.CANCELLING
    with pytest.raises(ConcurrentSchedulerTransitionError, match="cannot request"):
        scheduler.request_cancel()
    with pytest.raises(ConcurrentSchedulerTransitionError, match="cannot request a pause"):
        scheduler.request_pause()
    with pytest.raises(ConcurrentSchedulerTransitionError, match="cannot resume"):
        scheduler.resume()
    with pytest.raises(ConcurrentSchedulerTransitionError, match="cannot mark paused"):
        scheduler.mark_paused()


@pytest.mark.parametrize(
    "state",
    [
        ContractLifecycleState.RUNNING,
        ContractLifecycleState.QUIESCING,
        ContractLifecycleState.PAUSED,
        ContractLifecycleState.CANCELLING,
    ],
)
def test_mark_recovery_required_from_every_live_state(state: ContractLifecycleState) -> None:
    scheduler = _scheduler()
    _drive_to(scheduler, state)
    scheduler.mark_recovery_required("unknown commit outcome")
    frontier = scheduler.frontier
    assert frontier.control_state is ContractLifecycleState.RECOVERY_REQUIRED
    assert frontier.is_recovery_required
    assert frontier.recovery_required_reason == "unknown commit outcome"
    with pytest.raises(ConcurrentSchedulerTransitionError, match="recovery again"):
        scheduler.mark_recovery_required("another reason")


def test_mark_recovery_required_validates_reason() -> None:
    scheduler = _scheduler()
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="length"):
        scheduler.mark_recovery_required("")
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="length"):
        scheduler.mark_recovery_required("r" * (MAX_FRONTIER_REASON_LENGTH + 1))
    with pytest.raises(ConcurrentSchedulerInvalidStateError, match="printable ASCII"):
        scheduler.mark_recovery_required("outcome\tunknown")
    with pytest.raises(TypeError, match="text"):
        scheduler.mark_recovery_required(cast(str, cast(object, None)))


def test_quiesce_stops_admission_but_permits_durable_commits() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 1)
    scheduler.request_pause()
    assert scheduler.next_ready(4) == ()
    scheduler.mark_result_received(_work("extract", "p1"))
    scheduler.commit_result(_work("extract", "p1"), "succeeded")
    states = dict(scheduler.frontier.work_states)
    assert states[_work("extract", "p1")] is FrontierWorkState.SUCCEEDED
    assert states[_work("normalize", "p1")] is FrontierWorkState.BLOCKED
    scheduler.mark_paused()
    scheduler.resume()
    assert scheduler.next_ready(4) == (_work("extract", "p2"),)


def test_control_generation_exhaustion_fails_closed() -> None:
    scheduler = _scheduler(control_generation=ControlGeneration(MAX_METRIC_VALUE))
    with pytest.raises(ConcurrentSchedulerTransitionError, match="exhausted"):
        scheduler.request_pause()
    assert scheduler.frontier.control_state is ContractLifecycleState.RUNNING


def test_is_finished_requires_every_work_item_terminal() -> None:
    scheduler = _scheduler(nodes=("solo",), edges=(), partitions={"solo": ("p1", "p2")})
    assert not scheduler.is_finished
    scheduler.register_admission(_work("solo", "p1"), 1)
    scheduler.commit_result(_work("solo", "p1"), "quarantined")
    assert not scheduler.is_finished
    scheduler.register_admission(_work("solo", "p2"), 2)
    scheduler.commit_result(_work("solo", "p2"), "cancelled")
    assert scheduler.is_finished


def test_scheduler_repr_and_frontier_repr_are_bounded_summaries() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 1)
    assert repr(scheduler) == (
        "ConcurrentScheduler("
        f"run_id={RUN_ID!r}, nodes=3, work_items=4, in_flight=1, control_state='running')"
    )
    assert repr(scheduler.frontier) == (
        "SchedulerFrontierV2("
        f"version=2, run_id={RUN_ID!r}, nodes=3, work_items=4, in_flight=1, "
        "control_state='running')"
    )


def test_frontier_property_records_authoritative_fields() -> None:
    scheduler = _scheduler()
    frontier = scheduler.frontier
    assert frontier.version == SCHEDULER_FRONTIER_VERSION
    assert frontier.plan_fingerprint == FINGERPRINT
    assert frontier.control_generation == ControlGeneration(1)
    assert frontier.run_id == RUN_ID
    assert frontier.node_order == ("extract", "normalize", "export")
    assert frontier.edges == (("extract", "normalize"), ("normalize", "export"))
    assert frontier.control_state is ContractLifecycleState.RUNNING
    assert frontier.recovery_required_reason is None
    assert not frontier.is_recovery_required


def test_frontier_work_states_are_sorted_by_identity_not_plan_order() -> None:
    scheduler = _scheduler(
        nodes=("zeta", "alpha"),
        edges=(),
        partitions={"zeta": ("b", "a"), "alpha": ("c",)},
    )
    assert tuple(identity for identity, _ in scheduler.frontier.work_states) == (
        _work("alpha", "c"),
        _work("zeta", "a"),
        _work("zeta", "b"),
    )


def test_frontier_in_flight_identities_are_sorted_across_nodes() -> None:
    scheduler = _scheduler(
        nodes=("m-node", "k-node"),
        edges=(),
        partitions={"m-node": ("p1",), "k-node": ("p1",)},
    )
    scheduler.register_admission(_work("m-node", "p1"), 5)
    scheduler.register_admission(_work("k-node", "p1"), 6)
    scheduler.mark_result_received(_work("k-node", "p1"))
    assert scheduler.frontier.in_flight_identities == (
        _work("k-node", "p1"),
        _work("m-node", "p1"),
    )


def test_frontier_ready_identities_follow_plan_order() -> None:
    scheduler = _scheduler(
        nodes=("zeta", "alpha"),
        edges=(),
        partitions={"zeta": ("b", "a"), "alpha": ("c",)},
    )
    scheduler.register_admission(_work("zeta", "b"), 1)
    assert scheduler.frontier.ready_identities == (
        _work("zeta", "a"),
        _work("alpha", "c"),
    )


def test_frontier_snapshot_returns_the_same_immutable_value() -> None:
    scheduler = _scheduler()
    frontier = scheduler.frontier
    assert frontier.snapshot() is frontier


def test_frontier_rebuild_is_stable_and_equal() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 3)
    scheduler.mark_result_received(_work("extract", "p1"))
    first = scheduler.frontier
    assert scheduler.frontier == first
    assert scheduler.frontier.snapshot() == first


def test_to_mapping_is_plain_and_json_serializable() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 3)
    scheduler.mark_result_received(_work("extract", "p1"))
    scheduler.register_admission(_work("extract", "p2"), 4)
    scheduler.schedule_retry(_work("extract", "p2"), 99, "http 429")
    mapping = scheduler.frontier.to_mapping()
    assert set(mapping) == {
        "version",
        "plan_fingerprint",
        "control_generation",
        "run_id",
        "node_order",
        "edges",
        "work_states",
        "lease_fences",
        "retry_waits",
        "control_state",
        "recovery_required_reason",
    }
    assert mapping["node_order"] == ["extract", "normalize", "export"]
    assert mapping["edges"] == [["extract", "normalize"], ["normalize", "export"]]
    assert mapping["lease_fences"] == [[RUN_ID, "extract", "p1", 3]]
    assert mapping["retry_waits"] == [[RUN_ID, "extract", "p2", 99, "http 429"]]
    assert mapping["control_state"] == "running"
    assert mapping["recovery_required_reason"] is None
    assert isinstance(json.dumps(mapping), str)


def test_round_trip_of_fresh_frontier_is_equal() -> None:
    scheduler = _scheduler()
    frontier = scheduler.frontier
    assert SchedulerFrontierV2.from_mapping(frontier.to_mapping()) == frontier


def test_round_trip_after_progress_preserves_every_field() -> None:
    scheduler = _scheduler()
    _commit_success(scheduler, _work("extract", "p1"))
    scheduler.register_admission(_work("extract", "p2"), 6)
    scheduler.mark_result_received(_work("extract", "p2"))
    scheduler.schedule_retry(_work("extract", "p2"), 250, "http 429")
    frontier = scheduler.frontier
    restored = SchedulerFrontierV2.from_mapping(frontier.to_mapping())
    assert restored == frontier
    assert restored.version == SCHEDULER_FRONTIER_VERSION
    assert restored.plan_fingerprint == FINGERPRINT
    assert restored.control_generation == ControlGeneration(1)
    assert restored.run_id == RUN_ID
    assert restored.node_order == frontier.node_order
    assert restored.edges == frontier.edges
    assert restored.work_states == frontier.work_states
    assert restored.lease_fences == frontier.lease_fences
    assert restored.retry_waits == frontier.retry_waits
    assert restored.control_state is ContractLifecycleState.RUNNING
    assert restored.recovery_required_reason is None
    assert restored.in_flight_identities == ()
    states = dict(restored.work_states)
    assert states[_work("extract", "p1")] is FrontierWorkState.SUCCEEDED
    assert states[_work("extract", "p2")] is FrontierWorkState.RETRY_WAIT
    assert states[_work("normalize", "p1")] is FrontierWorkState.BLOCKED


def test_round_trip_of_recovery_required_frontier() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 1)
    scheduler.request_pause()
    scheduler.mark_recovery_required("unknown writer outcome")
    frontier = scheduler.frontier
    restored = SchedulerFrontierV2.from_mapping(frontier.to_mapping())
    assert restored == frontier
    assert restored.is_recovery_required
    assert restored.recovery_required_reason == "unknown writer outcome"
    assert restored.control_generation == ControlGeneration(2)


def test_from_mapping_rejects_non_mapping_payloads() -> None:
    payloads: tuple[object, ...] = ([], "version=2", None, 2)
    for payload in payloads:
        with pytest.raises(SchedulerFrontierCorruptError, match="mapping"):
            SchedulerFrontierV2.from_mapping(payload)


@pytest.mark.parametrize(
    "key",
    ["version", "plan_fingerprint", "control_generation", "run_id", "node_order", "edges"],
)
def test_from_mapping_rejects_missing_keys(key: str) -> None:
    mapping = _scheduler().frontier.to_mapping()
    del mapping[key]
    with pytest.raises(SchedulerFrontierCorruptError, match="missing or unknown"):
        SchedulerFrontierV2.from_mapping(mapping)


def test_from_mapping_rejects_unknown_keys() -> None:
    mapping = _scheduler().frontier.to_mapping()
    mapping["surprise"] = 1
    with pytest.raises(SchedulerFrontierCorruptError, match="missing or unknown"):
        SchedulerFrontierV2.from_mapping(mapping)


@pytest.mark.parametrize("version", [1, 3, 0])
def test_from_mapping_rejects_unknown_versions(version: int) -> None:
    mapping = _scheduler().frontier.to_mapping()
    mapping["version"] = version
    with pytest.raises(SchedulerFrontierVersionError, match="unsupported"):
        SchedulerFrontierV2.from_mapping(mapping)


@pytest.mark.parametrize("version", ["2", True, 2.0, None])
def test_from_mapping_rejects_non_integer_versions(version: object) -> None:
    mapping = _scheduler().frontier.to_mapping()
    mapping["version"] = version
    with pytest.raises(SchedulerFrontierCorruptError, match="version"):
        SchedulerFrontierV2.from_mapping(mapping)


def test_from_mapping_rejects_boolean_and_out_of_range_integers() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 3)
    fence_mapping = scheduler.frontier.to_mapping()
    fence_mapping["lease_fences"] = [[RUN_ID, "extract", "p1", True]]
    with pytest.raises(SchedulerFrontierCorruptError, match="integer"):
        SchedulerFrontierV2.from_mapping(fence_mapping)
    generation_mapping = _scheduler().frontier.to_mapping()
    generation_mapping["control_generation"] = True
    with pytest.raises(SchedulerFrontierCorruptError, match="integer"):
        SchedulerFrontierV2.from_mapping(generation_mapping)
    for generation in (0, -1, 2**31):
        out_of_range = _scheduler().frontier.to_mapping()
        out_of_range["control_generation"] = generation
        with pytest.raises(SchedulerFrontierCorruptError, match="generation"):
            SchedulerFrontierV2.from_mapping(out_of_range)
    waiting = _scheduler()
    waiting.register_admission(_work("extract", "p1"), 3)
    waiting.schedule_retry(_work("extract", "p1"), 10, "http 429")
    retry_mapping = waiting.frontier.to_mapping()
    retry_mapping["retry_waits"] = [[RUN_ID, "extract", "p1", True, "http 429"]]
    with pytest.raises(SchedulerFrontierCorruptError, match="integer"):
        SchedulerFrontierV2.from_mapping(retry_mapping)


def test_from_mapping_rejects_invalid_plan_fingerprints() -> None:
    for fingerprint in ("", "A" * 64, "a" * 63, 5):
        mapping = _scheduler().frontier.to_mapping()
        mapping["plan_fingerprint"] = fingerprint
        with pytest.raises(SchedulerFrontierCorruptError):
            SchedulerFrontierV2.from_mapping(mapping)


def test_from_mapping_rejects_invalid_node_order_rows() -> None:
    for node_order in ([], ["extract", "extract"], ["extract", 5], "extract"):
        mapping = _scheduler().frontier.to_mapping()
        mapping["node_order"] = node_order
        with pytest.raises(SchedulerFrontierCorruptError):
            SchedulerFrontierV2.from_mapping(mapping)


def test_from_mapping_rejects_invalid_edge_rows() -> None:
    for edges in ([["extract"]], [["extract", "normalize", "export"]], [[5, "normalize"]]):
        mapping = _scheduler().frontier.to_mapping()
        mapping["edges"] = edges
        with pytest.raises(SchedulerFrontierCorruptError):
            SchedulerFrontierV2.from_mapping(mapping)


def test_from_mapping_rejects_malformed_work_state_rows() -> None:
    for rows in (
        [[RUN_ID, "extract", "p1"]],
        [[RUN_ID, "extract", "p1", "ready", "extra"]],
        [[RUN_ID, "extract", "p1", 5]],
        [[RUN_ID, "extract", "p1", "exploded"]],
        ("rows",),
        [[RUN_ID, "extract", "p1", "ready"]],
    ):
        mapping = _scheduler().frontier.to_mapping()
        mapping["work_states"] = rows
        with pytest.raises(SchedulerFrontierCorruptError):
            SchedulerFrontierV2.from_mapping(mapping)


def test_from_mapping_rejects_malformed_lease_fence_rows() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 3)
    for rows in (
        [[RUN_ID, "extract", "p1"]],
        [[RUN_ID, "extract", "p1", 3, 3]],
        [[RUN_ID, "extract", "p1", 0]],
        [[RUN_ID, "extract", "p1", 2**31]],
        [[RUN_ID, "extract", "p1", "3"]],
    ):
        mapping = scheduler.frontier.to_mapping()
        mapping["lease_fences"] = rows
        with pytest.raises(SchedulerFrontierCorruptError):
            SchedulerFrontierV2.from_mapping(mapping)


def test_from_mapping_rejects_malformed_retry_wait_rows() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 3)
    scheduler.schedule_retry(_work("extract", "p1"), 10, "http 429")
    for rows in (
        [[RUN_ID, "extract", "p1", 10]],
        [[RUN_ID, "extract", "p1", 10, "http 429", "extra"]],
        [[RUN_ID, "extract", "p1", -1, "http 429"]],
        [[RUN_ID, "extract", "p1", True, "http 429"]],
        [[RUN_ID, "extract", "p1", 10, ""]],
        [[RUN_ID, "extract", "p1", 10, "r" * (MAX_FRONTIER_REASON_LENGTH + 1)]],
        [[RUN_ID, "extract", "p1", 10, "wait\n"]],
        [[RUN_ID, "extract", "p1", 10, 5]],
        "rows",
    ):
        mapping = scheduler.frontier.to_mapping()
        mapping["retry_waits"] = rows
        with pytest.raises(SchedulerFrontierCorruptError):
            SchedulerFrontierV2.from_mapping(mapping)


def test_from_mapping_rejects_invalid_control_states() -> None:
    for state in ("new", "open", "closed", "restarting", 5, None):
        mapping = _scheduler().frontier.to_mapping()
        mapping["control_state"] = state
        with pytest.raises(SchedulerFrontierCorruptError):
            SchedulerFrontierV2.from_mapping(mapping)


def test_from_mapping_rejects_recovery_reason_mismatch() -> None:
    healthy = _scheduler().frontier.to_mapping()
    healthy["recovery_required_reason"] = "stale reason"
    with pytest.raises(SchedulerFrontierCorruptError, match="recovery-required"):
        SchedulerFrontierV2.from_mapping(healthy)
    scheduler = _scheduler()
    scheduler.mark_recovery_required("unknown commit outcome")
    required = scheduler.frontier.to_mapping()
    required["recovery_required_reason"] = None
    with pytest.raises(SchedulerFrontierCorruptError, match="recovery reason"):
        SchedulerFrontierV2.from_mapping(required)
    blank = scheduler.frontier.to_mapping()
    blank["recovery_required_reason"] = ""
    with pytest.raises(SchedulerFrontierCorruptError, match="length"):
        SchedulerFrontierV2.from_mapping(blank)
    tabbed = scheduler.frontier.to_mapping()
    tabbed["recovery_required_reason"] = "unknown\toutcome"
    with pytest.raises(SchedulerFrontierCorruptError, match="printable ASCII"):
        SchedulerFrontierV2.from_mapping(tabbed)


def test_from_mapping_rejects_identity_text_and_run_violations() -> None:
    mapping = _scheduler().frontier.to_mapping()
    mapping["work_states"] = [
        [RUN_ID, "extract", "p1", "ready"],
        ["a" * 129, "extract", "p2", "ready"],
        [RUN_ID, "normalize", "p1", "blocked"],
        [RUN_ID, "export", "p1", "blocked"],
    ]
    with pytest.raises(SchedulerFrontierCorruptError, match="length"):
        SchedulerFrontierV2.from_mapping(mapping)
    foreign = _scheduler().frontier.to_mapping()
    foreign["work_states"] = [
        ["run-other", "extract", "p1", "ready"],
        ["run-other", "extract", "p2", "ready"],
        ["run-other", "normalize", "p1", "blocked"],
        ["run-other", "export", "p1", "blocked"],
    ]
    with pytest.raises(SchedulerFrontierCorruptError, match="frontier run"):
        SchedulerFrontierV2.from_mapping(foreign)


def test_frontier_rejects_unknown_versions_directly() -> None:
    frontier = _scheduler().frontier
    for version in (1, 3):
        with pytest.raises(SchedulerFrontierVersionError, match="unsupported"):
            replace(frontier, version=version)
    with pytest.raises(TypeError, match="integer"):
        replace(frontier, version=cast(int, cast(object, True)))


def test_frontier_rejects_duplicate_work_identities() -> None:
    frontier = _scheduler().frontier
    duplicated = frontier.work_states + frontier.work_states[:1]
    with pytest.raises(SchedulerFrontierCorruptError, match="unique"):
        replace(frontier, work_states=duplicated)


def test_frontier_rejects_unsorted_work_states() -> None:
    frontier = _scheduler().frontier
    with pytest.raises(SchedulerFrontierCorruptError, match="sorted"):
        replace(frontier, work_states=tuple(reversed(frontier.work_states)))


def test_frontier_rejects_fence_on_terminal_work() -> None:
    scheduler = _scheduler()
    _commit_success(scheduler, _work("extract", "p1"))
    scheduler.register_admission(_work("extract", "p2"), 6)
    frontier = scheduler.frontier
    fences = ((_work("extract", "p1"), 5), (_work("extract", "p2"), 6))
    with pytest.raises(SchedulerFrontierCorruptError, match="non-in-flight"):
        replace(frontier, lease_fences=fences)


def test_frontier_rejects_fence_on_retry_wait_work() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 6)
    scheduler.schedule_retry(_work("extract", "p1"), 5, "timeout")
    with pytest.raises(SchedulerFrontierCorruptError, match="non-in-flight"):
        replace(scheduler.frontier, lease_fences=((_work("extract", "p1"), 6),))


def test_frontier_requires_fences_for_in_flight_work() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 3)
    admitted = scheduler.frontier
    with pytest.raises(SchedulerFrontierCorruptError, match="lease fence"):
        replace(admitted, lease_fences=())
    scheduler.mark_result_received(_work("extract", "p1"))
    awaiting = scheduler.frontier
    with pytest.raises(SchedulerFrontierCorruptError, match="lease fence"):
        replace(awaiting, lease_fences=())


def test_frontier_retry_entries_must_match_retry_wait_work() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 3)
    scheduler.schedule_retry(_work("extract", "p1"), 5, "timeout")
    frontier = scheduler.frontier
    with pytest.raises(SchedulerFrontierCorruptError, match="retry-wait work"):
        replace(frontier, retry_waits=())
    with pytest.raises(SchedulerFrontierCorruptError, match="retry-wait work"):
        _with_state(frontier, _work("extract", "p1"), FrontierWorkState.READY)


def test_frontier_rejects_duplicate_and_unsorted_fences() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 3)
    scheduler.register_admission(_work("extract", "p2"), 4)
    frontier = scheduler.frontier
    with pytest.raises(SchedulerFrontierCorruptError, match="lease fences must be unique"):
        replace(frontier, lease_fences=((_work("extract", "p1"), 3), (_work("extract", "p1"), 4)))
    with pytest.raises(SchedulerFrontierCorruptError, match="lease fences must be sorted"):
        replace(
            frontier,
            lease_fences=((_work("extract", "p2"), 4), (_work("extract", "p1"), 3)),
        )


def test_frontier_rejects_out_of_range_fences() -> None:
    scheduler = _scheduler()
    scheduler.register_admission(_work("extract", "p1"), 3)
    for fence in (0, 2**31):
        with pytest.raises(SchedulerFrontierCorruptError, match="supported range"):
            replace(scheduler.frontier, lease_fences=((_work("extract", "p1"), fence),))


def test_frontier_rejects_duplicate_and_unsorted_retry_waits() -> None:
    scheduler = _scheduler(
        nodes=("m-node", "k-node"),
        edges=(),
        partitions={"m-node": ("p1",), "k-node": ("p1",)},
    )
    scheduler.register_admission(_work("m-node", "p1"), 3)
    scheduler.schedule_retry(_work("m-node", "p1"), 5, "one")
    scheduler.register_admission(_work("k-node", "p1"), 4)
    scheduler.schedule_retry(_work("k-node", "p1"), 6, "two")
    frontier = scheduler.frontier
    with pytest.raises(SchedulerFrontierCorruptError, match="retry waits must be unique"):
        replace(frontier, retry_waits=frontier.retry_waits + frontier.retry_waits[:1])
    with pytest.raises(SchedulerFrontierCorruptError, match="retry waits must be sorted"):
        replace(frontier, retry_waits=tuple(reversed(frontier.retry_waits)))


def test_frontier_rejects_unknown_nodes_edges_and_node_coverage() -> None:
    frontier = _scheduler().frontier
    with pytest.raises(SchedulerFrontierCorruptError, match="known node"):
        replace(frontier, edges=(("extract", "ghost"),))
    with pytest.raises(SchedulerFrontierCorruptError, match="unique"):
        replace(frontier, edges=(("extract", "normalize"), ("extract", "normalize")))
    with pytest.raises(SchedulerFrontierCorruptError, match="sorted"):
        replace(
            frontier,
            edges=(("normalize", "export"), ("extract", "normalize")),
        )
    with pytest.raises(SchedulerFrontierCorruptError, match="not be empty"):
        replace(frontier, node_order=())
    with pytest.raises(SchedulerFrontierCorruptError, match="unique"):
        replace(
            frontier,
            node_order=("extract", "extract", "normalize", "export"),
        )
    with pytest.raises(SchedulerFrontierCorruptError, match="requires work items"):
        replace(frontier, work_states=frontier.work_states[:3])


def test_frontier_rejects_undurable_control_states_and_bad_text() -> None:
    frontier = _scheduler().frontier
    for state in (ContractLifecycleState.NEW, ContractLifecycleState.CLOSED):
        with pytest.raises(SchedulerFrontierCorruptError, match="not durable"):
            replace(frontier, control_state=state)
    with pytest.raises(SchedulerFrontierCorruptError, match="recovery-required"):
        replace(frontier, recovery_required_reason="reason without state")
    with pytest.raises(SchedulerFrontierCorruptError, match="length"):
        replace(frontier, run_id="")
    with pytest.raises(ConcurrentSchedulerError, match="hexadecimal"):
        replace(frontier, plan_fingerprint="F" * 64)


def test_frontier_recovery_state_requires_validated_reason() -> None:
    running = _scheduler().frontier
    with pytest.raises(SchedulerFrontierCorruptError, match="recovery reason"):
        replace(running, control_state=ContractLifecycleState.RECOVERY_REQUIRED)
    scheduler = _scheduler()
    scheduler.mark_recovery_required("writer outcome unknown")
    with pytest.raises(SchedulerFrontierCorruptError, match="printable ASCII"):
        replace(scheduler.frontier, recovery_required_reason="outcome\tunknown")


def test_from_mapping_rejects_unknown_work_references() -> None:
    ghost_node = _scheduler().frontier.to_mapping()
    ghost_node["work_states"] = [
        [RUN_ID, "ghost", "p1", "ready"],
        [RUN_ID, "extract", "p2", "ready"],
        [RUN_ID, "normalize", "p1", "blocked"],
        [RUN_ID, "export", "p1", "blocked"],
    ]
    with pytest.raises(SchedulerFrontierCorruptError, match="known node"):
        SchedulerFrontierV2.from_mapping(ghost_node)
    ghost_fence = _scheduler().frontier.to_mapping()
    ghost_fence["lease_fences"] = [[RUN_ID, "extract", "ghost", 3]]
    with pytest.raises(SchedulerFrontierCorruptError, match="known work"):
        SchedulerFrontierV2.from_mapping(ghost_fence)
    overflowing = _scheduler()
    overflowing.register_admission(_work("extract", "p1"), 3)
    overflowing.schedule_retry(_work("extract", "p1"), 10, "http 429")
    overflow = overflowing.frontier.to_mapping()
    overflow["retry_waits"] = [[RUN_ID, "extract", "p1", 2**31, "http 429"]]
    with pytest.raises(SchedulerFrontierCorruptError, match="supported range"):
        SchedulerFrontierV2.from_mapping(overflow)


def test_row_parsers_reject_wrong_container_kinds() -> None:
    frontier = _scheduler().frontier
    identity = _work("extract", "p1")
    with pytest.raises(TypeError, match="must be a tuple"):
        replace(
            frontier,
            work_states=cast(
                "tuple[tuple[WorkIdentity, FrontierWorkState], ...]",
                [(identity, FrontierWorkState.READY)],
            ),
        )
    with pytest.raises(TypeError, match="identity-state pairs"):
        replace(frontier, work_states=([identity, FrontierWorkState.READY],))
    with pytest.raises(TypeError, match="must be a tuple"):
        replace(
            frontier,
            lease_fences=cast("tuple[tuple[WorkIdentity, int], ...]", [(identity, 3)]),
        )
    with pytest.raises(TypeError, match="identity-fence pairs"):
        replace(frontier, lease_fences=([identity, 3],))
    for field, rows in (
        ("edges", (("extract", "normalize"),)),
        ("work_states", ([RUN_ID, "extract", "p1", "ready"],)),
        ("lease_fences", ([RUN_ID, "extract", "p1", 3],)),
        ("retry_waits", ([RUN_ID, "extract", "p1", 10, "timeout"],)),
    ):
        mapping = frontier.to_mapping()
        mapping[field] = rows
        with pytest.raises(SchedulerFrontierCorruptError, match="must be a list"):
            SchedulerFrontierV2.from_mapping(mapping)
    for field, rows, fragment in (
        ("edges", [("extract", "normalize")], "pairs"),
        ("work_states", [(RUN_ID, "extract", "p1", "ready")], "rows"),
        ("lease_fences", [(RUN_ID, "extract", "p1", 3)], "rows"),
        ("retry_waits", [(RUN_ID, "extract", "p1", 10, "timeout")], "rows"),
    ):
        mapping = frontier.to_mapping()
        mapping[field] = rows
        with pytest.raises(SchedulerFrontierCorruptError, match=fragment):
            SchedulerFrontierV2.from_mapping(mapping)


@pytest.mark.parametrize(
    "state",
    [
        FrontierWorkState.READY,
        FrontierWorkState.ADMITTED,
        FrontierWorkState.AWAITING_COMMIT,
        FrontierWorkState.RETRY_WAIT,
        FrontierWorkState.SUCCEEDED,
        FrontierWorkState.QUARANTINED,
        FrontierWorkState.FAILED,
        FrontierWorkState.CANCELLED,
    ],
)
def test_frontier_mapping_rejects_successor_that_bypasses_unsatisfied_dependency(
    state: FrontierWorkState,
) -> None:
    scheduler = _scheduler(
        nodes=("source", "sink"),
        edges=(("source", "sink"),),
        partitions={"source": ("p1",), "sink": ("q",)},
    )
    mapping = scheduler.frontier.to_mapping()
    mapping["work_states"] = [
        [run_id, node_id, partition, state.value]
        if (node_id, partition) == ("sink", "q")
        else [run_id, node_id, partition, work_state]
        for run_id, node_id, partition, work_state in cast(
            "list[list[object]]", mapping["work_states"]
        )
    ]
    if state in {FrontierWorkState.ADMITTED, FrontierWorkState.AWAITING_COMMIT}:
        mapping["lease_fences"] = [[RUN_ID, "sink", "q", 9]]
    if state is FrontierWorkState.RETRY_WAIT:
        mapping["retry_waits"] = [[RUN_ID, "sink", "q", 100, "retry"]]
    with pytest.raises(SchedulerFrontierCorruptError, match="unsatisfied dependency"):
        SchedulerFrontierV2.from_mapping(mapping)


def test_restore_revalidates_successor_dependency_barriers() -> None:
    scheduler = _scheduler(
        nodes=("source", "sink"),
        edges=(("source", "sink"),),
        partitions={"source": ("p1",), "sink": ("q",)},
    )
    frontier = scheduler.frontier
    object.__setattr__(
        frontier,
        "work_states",
        tuple(
            (work, FrontierWorkState.READY if work == _work("sink", "q") else state)
            for work, state in frontier.work_states
        ),
    )
    with pytest.raises(SchedulerFrontierCorruptError, match="unsatisfied dependency"):
        scheduler.restore(frontier)


def test_frontier_mapping_rejects_blocked_successor_after_predecessors_succeed() -> None:
    scheduler = _scheduler(
        nodes=("source", "sink"),
        edges=(("source", "sink"),),
        partitions={"source": ("p1",), "sink": ("q",)},
    )
    _commit_success(scheduler, _work("source", "p1"))
    with pytest.raises(SchedulerFrontierCorruptError, match="all predecessors succeeded"):
        SchedulerFrontierV2.from_mapping(
            scheduler.frontier.to_mapping()
            | {
                "work_states": [
                    [RUN_ID, "sink", "q", "blocked"],
                    [RUN_ID, "source", "p1", "succeeded"],
                ]
            }
        )


@pytest.mark.parametrize(
    "edges",
    [
        [["source", "source"]],
        [["sink", "source"], ["source", "sink"]],
    ],
)
def test_frontier_mapping_rejects_self_dependencies_and_cycles(
    edges: list[list[str]],
) -> None:
    scheduler = _scheduler(
        nodes=("source", "sink"),
        edges=(("source", "sink"),),
        partitions={"source": ("p1",), "sink": ("q",)},
    )
    with pytest.raises(SchedulerFrontierCorruptError, match=r"self dependency|directed cycle"):
        SchedulerFrontierV2.from_mapping(scheduler.frontier.to_mapping() | {"edges": edges})


@pytest.mark.parametrize("outcome", ["quarantined", "failed", "cancelled"])
def test_restore_preserves_blocked_successors_after_blocking_predecessor(
    outcome: str,
) -> None:
    scheduler = _scheduler(
        nodes=("source", "sink"),
        edges=(("source", "sink"),),
        partitions={"source": ("p1",), "sink": ("q",)},
    )
    scheduler.register_admission(_work("source", "p1"), 1)
    scheduler.commit_result(_work("source", "p1"), outcome)
    restored = _scheduler(
        nodes=("source", "sink"),
        edges=(("source", "sink"),),
        partitions={"source": ("p1",), "sink": ("q",)},
    ).restore(scheduler.frontier)
    assert dict(restored.frontier.work_states)[_work("sink", "q")] is FrontierWorkState.BLOCKED


def test_restore_preserves_admitted_successor_after_predecessors_succeed() -> None:
    scheduler = _scheduler(
        nodes=("source", "sink"),
        edges=(("source", "sink"),),
        partitions={"source": ("p1", "p2"), "sink": ("q",)},
    )
    _commit_success(scheduler, _work("source", "p1"))
    _commit_success(scheduler, _work("source", "p2"))
    scheduler.register_admission(_work("sink", "q"), 9)
    restored = _scheduler(
        nodes=("source", "sink"),
        edges=(("source", "sink"),),
        partitions={"source": ("p1", "p2"), "sink": ("q",)},
    ).restore(scheduler.frontier)
    assert dict(restored.frontier.work_states)[_work("sink", "q")] is (FrontierWorkState.ADMITTED)


def test_frontier_type_mismatch_matrix() -> None:
    frontier = _scheduler().frontier
    identity = _work("extract", "p1")
    with pytest.raises(TypeError, match="integer"):
        replace(frontier, version=cast(int, cast(object, 2.0)))
    with pytest.raises(TypeError, match="text"):
        replace(frontier, plan_fingerprint=cast(str, cast(object, 64)))
    with pytest.raises(TypeError, match="ControlGeneration"):
        replace(frontier, control_generation=cast(ControlGeneration, cast(object, 1)))
    with pytest.raises(TypeError, match="tuple"):
        replace(frontier, node_order=cast("tuple[str, ...]", ["extract"]))
    with pytest.raises(TypeError, match="invalid value"):
        replace(frontier, node_order=cast("tuple[str, ...]", ("extract", 4)))
    with pytest.raises(TypeError, match="tuple"):
        replace(frontier, edges=cast("tuple[tuple[str, str], ...]", [["extract", "normalize"]]))
    with pytest.raises(TypeError, match="source-target pairs"):
        replace(frontier, edges=(("extract", "normalize"), ("extract",)))
    with pytest.raises(TypeError, match="source-target pairs"):
        replace(frontier, edges=(("extract", "normalize"), cast("tuple[str, str]", "edge")))
    with pytest.raises(SchedulerFrontierCorruptError, match="must be text"):
        replace(
            frontier,
            edges=(("extract", cast(str, cast(object, None))),),
        )
    with pytest.raises(TypeError, match="identity-state pairs"):
        replace(frontier, work_states=((identity,),))
    with pytest.raises(TypeError, match="WorkIdentity"):
        replace(
            frontier,
            work_states=((cast(WorkIdentity, cast(object, "x")), FrontierWorkState.READY),),
        )
    with pytest.raises(TypeError, match="FrontierWorkState"):
        replace(frontier, work_states=((identity, cast(FrontierWorkState, cast(object, "ready"))),))
    with pytest.raises(TypeError, match="identity-fence pairs"):
        replace(frontier, lease_fences=((identity, 3, 3),))
    with pytest.raises(TypeError, match="integer"):
        replace(frontier, lease_fences=((identity, cast(int, cast(object, True))),))
    with pytest.raises(TypeError, match="invalid value"):
        replace(
            frontier,
            retry_waits=cast(
                "tuple[FrontierRetryWait, ...]",
                (FrontierRetryWait(identity, 1, "r"), "wait"),
            ),
        )
    with pytest.raises(TypeError, match="ContractLifecycleState"):
        replace(frontier, control_state=cast(ContractLifecycleState, cast(object, "running")))


def test_frontier_rejects_work_item_bound() -> None:
    frontier = _scheduler(nodes=("solo",), edges=(), partitions={"solo": ("p1",)}).frontier
    identities = tuple(
        (_work("solo", f"p{index:06d}"), FrontierWorkState.SUCCEEDED)
        for index in range(MAX_FRONTIER_WORK_ITEMS + 1)
    )
    with pytest.raises(SchedulerFrontierCorruptError, match="work item bound"):
        replace(frontier, work_states=identities)


def test_restore_rebuilds_state_and_commits_release_dependencies() -> None:
    first = _scheduler()
    _commit_success(first, _work("extract", "p1"))
    first.register_admission(_work("extract", "p2"), 6)
    snapshot = first.frontier

    second = _scheduler()
    restored = second.restore(snapshot)
    assert restored is second
    assert second.frontier == snapshot
    assert second.next_ready(10) == ()
    with pytest.raises(ConcurrentSchedulerCapacityError, match="already admitted"):
        second.register_admission(_work("extract", "p2"), 7)
    second.commit_result(_work("extract", "p2"), "succeeded")
    states = dict(second.frontier.work_states)
    assert states[_work("normalize", "p1")] is FrontierWorkState.READY
    assert second.next_ready(10) == (_work("normalize", "p1"),)


def test_restore_preserves_control_state_generation_and_recovery() -> None:
    first = _scheduler()
    first.register_admission(_work("extract", "p1"), 1)
    first.request_pause()
    first.mark_recovery_required("unknown writer outcome")
    restored = _scheduler().restore(first.frontier)
    frontier = restored.frontier
    assert frontier.control_state is ContractLifecycleState.RECOVERY_REQUIRED
    assert frontier.control_generation == ControlGeneration(2)
    assert frontier.recovery_required_reason == "unknown writer outcome"
    assert restored.next_ready(4) == ()
    assert frontier.in_flight_identities == (_work("extract", "p1"),)
    restored.commit_result(_work("extract", "p1"), "succeeded")
    assert restored.frontier.in_flight_identities == ()


def test_restore_round_trip_through_plain_mapping() -> None:
    first = _scheduler()
    _commit_success(first, _work("extract", "p1"))
    first.register_admission(_work("extract", "p2"), 2)
    snapshot = SchedulerFrontierV2.from_mapping(first.frontier.to_mapping())
    restored = _scheduler().restore(snapshot)
    assert restored.frontier == snapshot
    restored.commit_result(_work("extract", "p2"), "succeeded")
    assert dict(restored.frontier.work_states)[_work("normalize", "p1")] is (
        FrontierWorkState.READY
    )


def test_restore_rejects_plan_and_partition_mismatches() -> None:
    scheduler = _scheduler()
    snapshot = scheduler.frontier
    with pytest.raises(SchedulerFrontierCorruptError, match="fingerprint"):
        _scheduler(plan_fingerprint=OTHER_FINGERPRINT).restore(snapshot)
    with pytest.raises(SchedulerFrontierCorruptError, match="run"):
        _scheduler(run_id="run-second").restore(snapshot)
    with pytest.raises(SchedulerFrontierCorruptError, match="nodes"):
        _scheduler(nodes=("normalize", "export", "extract")).restore(snapshot)
    with pytest.raises(SchedulerFrontierCorruptError, match="edges"):
        _scheduler(edges=(("normalize", "export"),)).restore(snapshot)
    with pytest.raises(SchedulerFrontierCorruptError, match="partitions"):
        _scheduler(
            partitions={
                "extract": ("p2",),
                "normalize": ("p1",),
                "export": ("p1",),
            },
        ).restore(snapshot)


def test_restore_rejects_non_frontier_values() -> None:
    scheduler = _scheduler()
    with pytest.raises(TypeError, match="SchedulerFrontierV2"):
        scheduler.restore(cast(SchedulerFrontierV2, cast(object, scheduler.frontier.to_mapping())))


def test_deterministic_admission_order_across_overlapping_nodes() -> None:
    scheduler = _scheduler(
        nodes=("one", "two", "three"),
        edges=(("one", "three"),),
        partitions={"one": ("b", "a"), "two": ("z", "y"), "three": ("p",)},
    )
    assert scheduler.next_ready(6) == (
        _work("one", "a"),
        _work("one", "b"),
        _work("two", "y"),
        _work("two", "z"),
    )
    assert scheduler.next_ready(2) == (_work("one", "a"), _work("one", "b"))
    _commit_success(scheduler, _work("one", "a"))
    _commit_success(scheduler, _work("one", "b"))
    _commit_success(scheduler, _work("two", "y"))
    assert scheduler.next_ready(6) == (_work("two", "z"), _work("three", "p"))
