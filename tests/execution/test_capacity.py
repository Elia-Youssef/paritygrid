"""Bounded capacity ledger tests for P7.6."""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

import paritygrid.application.execution as execution_package
from paritygrid.application.execution import (
    CAPACITY_CATEGORY_CONNECTOR,
    CAPACITY_CATEGORY_CPU_POOL,
    CAPACITY_CATEGORY_GLOBAL,
    CAPACITY_CATEGORY_NODE,
    CAPACITY_CATEGORY_STRATEGY,
    MAX_WAIT_MICROSECONDS,
    CapacityClosedError,
    CapacityError,
    CapacityOrderError,
    CapacityOwnershipError,
    CapacityPermit,
    CapacityRateDeferredError,
    CapacitySnapshot,
    CapacityTimeoutError,
    CapacityValidationError,
    CapturedConcurrencySettings,
    ManualClock,
    PolicyClock,
    RateLimitPolicy,
    ScheduledWorkLimiters,
    SubordinateCallLimiter,
    capacity,
)
from paritygrid.domain.models import UtcTimestamp

_BASE = datetime(2026, 8, 24, 12, tzinfo=UTC)
_NODE = "n1"
_FAR_DEADLINE_MICROSECONDS = 30_000_000
_RATE_INTERVAL_MICROSECONDS = 500_000


def _ts(microseconds: int = 0) -> UtcTimestamp:
    return UtcTimestamp(_BASE + timedelta(microseconds=microseconds))


def _settings(
    global_concurrent_work: int = 4,
    per_strategy_work: int = 4,
    per_node_work: int = 4,
) -> CapturedConcurrencySettings:
    return CapturedConcurrencySettings(
        global_concurrent_work=global_concurrent_work,
        per_strategy_work=per_strategy_work,
        per_node_work=per_node_work,
    )


def _coordinator(
    settings: CapturedConcurrencySettings | None = None,
    *,
    node_ids: tuple[str, ...] = (_NODE,),
    clock: ManualClock | None = None,
) -> tuple[ScheduledWorkLimiters, ManualClock]:
    selected_settings = _settings() if settings is None else settings
    selected_clock = ManualClock(_ts()) if clock is None else clock
    limiter = ScheduledWorkLimiters(
        selected_settings,
        strategy_id="sequential",
        node_ids=node_ids,
        clock=selected_clock,
    )
    return limiter, selected_clock


def _hold(
    limiter: ScheduledWorkLimiters,
    owner: str,
    node: str = _NODE,
) -> tuple[CapacityPermit, CapacityPermit, CapacityPermit]:
    return limiter.acquire(owner, node)


def _connector(
    parent_limiter: ScheduledWorkLimiters,
    clock: ManualClock,
    *,
    category: str = CAPACITY_CATEGORY_CONNECTOR,
    limit: int = 1,
    rate: RateLimitPolicy | None = None,
) -> SubordinateCallLimiter:
    return SubordinateCallLimiter(
        category=category,
        limit=limit,
        rate=rate,
        clock=clock,
        parent_limiter=parent_limiter,
    )


def _rate() -> RateLimitPolicy:
    return RateLimitPolicy.per_second(2.0, burst=1)


def _node_waiting(limiter: ScheduledWorkLimiters) -> int:
    return limiter.snapshot()[2].waiting


def _waiting_total(limiter: ScheduledWorkLimiters) -> int:
    return sum(snapshot.waiting for snapshot in limiter.snapshot())


def _spin_until(predicate: Callable[[], bool]) -> None:
    for _ in range(2_000_000):
        if predicate():
            return
    raise AssertionError("expected limiter state was never observed")


def _join_all(threads: list[threading.Thread]) -> None:
    for thread in threads:
        thread.join()
    for thread in threads:
        assert not thread.is_alive()


class _ErrorSink:
    __slots__ = ("_lock", "errors")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.errors: list[BaseException] = []

    def record(self, error: BaseException) -> None:
        with self._lock:
            self.errors.append(error)


class _Counter:
    __slots__ = ("_condition", "_count")

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._count = 0

    def increment(self) -> None:
        with self._condition:
            self._count += 1
            self._condition.notify_all()

    @property
    def count(self) -> int:
        with self._condition:
            return self._count

    def wait_until_at_least(self, target: int) -> None:
        with self._condition:
            while self._count < target:
                self._condition.wait()


class _Peak:
    __slots__ = ("_active", "_lock", "peak")

    def __init__(self) -> None:
        self._active = 0
        self._lock = threading.Lock()
        self.peak = 0

    def enter(self) -> None:
        with self._lock:
            self._active += 1
            if self._active > self.peak:
                self.peak = self._active

    def leave(self) -> None:
        with self._lock:
            self._active -= 1


# --------------------------------------------------------------------------------------
# Module constants, exports, and error hierarchy
# --------------------------------------------------------------------------------------


def test_module_constants_are_pinned() -> None:
    assert CAPACITY_CATEGORY_GLOBAL == "global"
    assert CAPACITY_CATEGORY_STRATEGY == "strategy"
    assert CAPACITY_CATEGORY_NODE == "node"
    assert CAPACITY_CATEGORY_CONNECTOR == "connector"
    assert CAPACITY_CATEGORY_CPU_POOL == "cpu_pool"
    assert MAX_WAIT_MICROSECONDS == 60_000_000


def test_module_all_exports_deliberate_public_names() -> None:
    assert capacity.__all__ == [
        "CAPACITY_CATEGORY_CONNECTOR",
        "CAPACITY_CATEGORY_CPU_POOL",
        "CAPACITY_CATEGORY_GLOBAL",
        "CAPACITY_CATEGORY_NODE",
        "CAPACITY_CATEGORY_STRATEGY",
        "MAX_WAIT_MICROSECONDS",
        "CapacityClosedError",
        "CapacityError",
        "CapacityOrderError",
        "CapacityOwnershipError",
        "CapacityPermit",
        "CapacityRateDeferredError",
        "CapacitySnapshot",
        "CapacityTimeoutError",
        "CapacityValidationError",
        "ScheduledWorkLimiters",
        "SubordinateCallLimiter",
    ]


def test_module_source_contains_no_ambient_clock_or_sleep_usage() -> None:
    module_file = capacity.__file__
    assert module_file is not None
    source = Path(module_file).read_text(encoding="utf-8")
    for forbidden in (
        "time.time",
        "datetime.now",
        "utcnow",
        "perf_counter",
        "monotonic(",
        "sleep(",
    ):
        assert forbidden not in source


def test_error_hierarchy_under_capacity_error() -> None:
    for error_type in (
        CapacityValidationError,
        CapacityOrderError,
        CapacityOwnershipError,
        CapacityTimeoutError,
        CapacityClosedError,
    ):
        assert issubclass(error_type, CapacityError)
        assert issubclass(error_type, RuntimeError)
    assert issubclass(CapacityRateDeferredError, CapacityTimeoutError)


def test_package_reexports_capacity_public_api() -> None:
    assert execution_package.ScheduledWorkLimiters is capacity.ScheduledWorkLimiters
    assert execution_package.SubordinateCallLimiter is capacity.SubordinateCallLimiter
    for name in capacity.__all__:
        assert getattr(execution_package, name) is getattr(capacity, name)


def test_rate_deferred_error_carries_exact_retry_instant() -> None:
    error = CapacityRateDeferredError(_ts(5_000_000))
    assert error.retry_at == _ts(5_000_000)
    custom = CapacityRateDeferredError(_ts(1), "custom message")
    assert str(custom) == "custom message"


def test_rate_deferred_error_rejects_non_exact_retry_instant() -> None:
    with pytest.raises(CapacityValidationError):
        CapacityRateDeferredError(cast(UtcTimestamp, _BASE))


# --------------------------------------------------------------------------------------
# CapacityPermit
# --------------------------------------------------------------------------------------


def test_permit_accepts_valid_fields() -> None:
    permit = CapacityPermit(category=CAPACITY_CATEGORY_GLOBAL, limit=4, owner="a", slot=1)
    assert permit.category == CAPACITY_CATEGORY_GLOBAL
    assert permit.limit == 4
    assert permit.owner == "a"
    assert permit.slot == 1


@pytest.mark.parametrize("category", [123, None, "bogus", "Global", b"global"])
def test_permit_rejects_unknown_categories(category: object) -> None:
    with pytest.raises(CapacityValidationError):
        CapacityPermit(category=cast(str, category), limit=4, owner="a", slot=1)


@pytest.mark.parametrize("limit", [True, 1.0, "4", None, 0, -1, 65_537])
def test_permit_rejects_invalid_limits(limit: object) -> None:
    with pytest.raises(CapacityValidationError):
        CapacityPermit(category=CAPACITY_CATEGORY_GLOBAL, limit=cast(int, limit), owner="a", slot=1)


@pytest.mark.parametrize("owner", [123, None, "", "x" * 129, "own\u00e9er", "bad owner\t"])
def test_permit_rejects_invalid_owners(owner: object) -> None:
    with pytest.raises(CapacityValidationError):
        CapacityPermit(category=CAPACITY_CATEGORY_GLOBAL, limit=4, owner=cast(str, owner), slot=1)


@pytest.mark.parametrize("slot", [True, 1.5, "1", None, 0, -1, 5])
def test_permit_rejects_invalid_slots(slot: object) -> None:
    with pytest.raises(CapacityValidationError):
        CapacityPermit(category=CAPACITY_CATEGORY_GLOBAL, limit=4, owner="a", slot=cast(int, slot))


def test_permit_equality_uses_category_owner_and_slot() -> None:
    left = CapacityPermit(category=CAPACITY_CATEGORY_GLOBAL, limit=4, owner="a", slot=1)
    same_different_limit = CapacityPermit(
        category=CAPACITY_CATEGORY_GLOBAL, limit=8, owner="a", slot=1
    )
    assert left == same_different_limit
    assert hash(left) == hash(same_different_limit)
    assert left != CapacityPermit(category=CAPACITY_CATEGORY_GLOBAL, limit=4, owner="a", slot=2)
    assert left != CapacityPermit(category=CAPACITY_CATEGORY_STRATEGY, limit=4, owner="a", slot=1)
    assert left != CapacityPermit(category=CAPACITY_CATEGORY_GLOBAL, limit=4, owner="b", slot=1)


def test_permit_repr_redacts_owner() -> None:
    permit = CapacityPermit(category=CAPACITY_CATEGORY_NODE, limit=2, owner="secret-owner", slot=1)
    text = repr(permit)
    assert "secret-owner" not in text
    assert "owner=<redacted>" in text
    assert "category='node'" in text


def test_permit_is_frozen_and_slotted() -> None:
    permit = CapacityPermit(category=CAPACITY_CATEGORY_CONNECTOR, limit=2, owner="a", slot=1)
    with pytest.raises(FrozenInstanceError):
        permit.slot = 2  # type: ignore[misc]
    assert not hasattr(permit, "__dict__")


# --------------------------------------------------------------------------------------
# CapacitySnapshot
# --------------------------------------------------------------------------------------


def test_snapshot_accepts_valid_bounds() -> None:
    snapshot = CapacitySnapshot(
        category=CAPACITY_CATEGORY_GLOBAL,
        limit=4,
        in_use=2,
        waiting=3,
        max_observed_in_use=4,
    )
    assert snapshot.in_use == 2
    assert snapshot.waiting == 3
    assert snapshot.max_observed_in_use == 4
    assert snapshot == CapacitySnapshot(
        category=CAPACITY_CATEGORY_GLOBAL,
        limit=4,
        in_use=2,
        waiting=3,
        max_observed_in_use=4,
    )


@pytest.mark.parametrize("category", [123, None, "bogus"])
def test_snapshot_rejects_unknown_categories(category: object) -> None:
    with pytest.raises(CapacityValidationError):
        CapacitySnapshot(
            category=cast(str, category),
            limit=4,
            in_use=0,
            waiting=0,
            max_observed_in_use=0,
        )


@pytest.mark.parametrize(
    ("limit", "in_use", "waiting", "max_observed"),
    [
        (0, 0, 0, 0),
        (65_537, 0, 0, 0),
        (4, -1, 0, 0),
        (4, 5, 0, 5),
        (4, 0, -1, 0),
        (4, 2, 0, 1),
        (4, 0, 0, 5),
    ],
)
def test_snapshot_rejects_out_of_bound_counts(
    limit: int,
    in_use: int,
    waiting: int,
    max_observed: int,
) -> None:
    with pytest.raises(CapacityValidationError):
        CapacitySnapshot(
            category=CAPACITY_CATEGORY_NODE,
            limit=limit,
            in_use=in_use,
            waiting=waiting,
            max_observed_in_use=max_observed,
        )


# --------------------------------------------------------------------------------------
# ScheduledWorkLimiters construction
# --------------------------------------------------------------------------------------


def test_constructor_builds_level_ledgers_from_captured_settings() -> None:
    limiter, _ = _coordinator(_settings(4, 3, 2), node_ids=("n1", "n2"), clock=ManualClock(_ts()))
    snapshots = limiter.snapshot()
    assert [snapshot.category for snapshot in snapshots] == [
        CAPACITY_CATEGORY_GLOBAL,
        CAPACITY_CATEGORY_STRATEGY,
        CAPACITY_CATEGORY_NODE,
        CAPACITY_CATEGORY_NODE,
    ]
    assert [snapshot.limit for snapshot in snapshots] == [4, 3, 2, 2]


def test_constructor_rejects_non_exact_settings() -> None:
    with pytest.raises(CapacityValidationError):
        ScheduledWorkLimiters(
            cast(CapturedConcurrencySettings, object()),
            strategy_id="sequential",
            node_ids=("n1",),
            clock=ManualClock(_ts()),
        )


@pytest.mark.parametrize("strategy_id", [123, None, "", "Sequential", "strategy id", "x" * 65])
def test_constructor_rejects_invalid_strategy_ids(strategy_id: object) -> None:
    with pytest.raises(CapacityValidationError):
        ScheduledWorkLimiters(
            _settings(),
            strategy_id=cast(str, strategy_id),
            node_ids=("n1",),
            clock=ManualClock(_ts()),
        )


@pytest.mark.parametrize(
    "node_ids",
    [["n1"], (), ("n1", "n1"), ("n1", 123), ("global",), ("strategy",), ("n\u00e9",), ("",)],
)
def test_constructor_rejects_invalid_node_ids(node_ids: object) -> None:
    with pytest.raises(CapacityValidationError):
        ScheduledWorkLimiters(
            _settings(),
            strategy_id="sequential",
            node_ids=cast("tuple[str, ...]", node_ids),
            clock=ManualClock(_ts()),
        )


def test_constructor_rejects_too_many_nodes() -> None:
    node_ids = tuple(f"n{index}" for index in range(65_537))
    with pytest.raises(CapacityValidationError):
        ScheduledWorkLimiters(
            _settings(),
            strategy_id="sequential",
            node_ids=node_ids,
            clock=ManualClock(_ts()),
        )


def test_constructor_rejects_non_clock() -> None:
    with pytest.raises(CapacityValidationError):
        ScheduledWorkLimiters(
            _settings(),
            strategy_id="sequential",
            node_ids=("n1",),
            clock=cast(PolicyClock, object()),
        )


# --------------------------------------------------------------------------------------
# ScheduledWorkLimiters.acquire
# --------------------------------------------------------------------------------------


def test_acquire_returns_ordered_triple_with_captured_limits() -> None:
    limiter, _ = _coordinator(_settings(4, 3, 2), node_ids=("n1", "n2"))
    global_permit, strategy_permit, node_permit = limiter.acquire("a", "n1")
    assert global_permit.category == CAPACITY_CATEGORY_GLOBAL
    assert global_permit.limit == 4
    assert global_permit.slot == 1
    assert strategy_permit.category == CAPACITY_CATEGORY_STRATEGY
    assert strategy_permit.limit == 3
    assert node_permit.category == CAPACITY_CATEGORY_NODE
    assert node_permit.limit == 2
    assert limiter.in_use(CAPACITY_CATEGORY_GLOBAL) == 1
    assert limiter.in_use(CAPACITY_CATEGORY_NODE, "n1") == 1
    second = limiter.acquire("b", "n2")
    assert [permit.slot for permit in second] == [2, 2, 1]
    assert limiter.in_use(CAPACITY_CATEGORY_NODE, "n2") == 1


def test_acquire_rejects_unknown_node() -> None:
    limiter, _ = _coordinator()
    with pytest.raises(CapacityValidationError, match="not registered"):
        limiter.acquire("a", "nX")


@pytest.mark.parametrize("owner", [123, None, "", "x" * 129, "own\u00e9er"])
def test_acquire_rejects_invalid_owners(owner: object) -> None:
    limiter, _ = _coordinator()
    with pytest.raises(CapacityValidationError):
        limiter.acquire(cast(str, owner), _NODE)


def test_acquire_rejects_invalid_node_argument() -> None:
    limiter, _ = _coordinator()
    with pytest.raises(CapacityValidationError):
        limiter.acquire("a", cast(str, 123))


def test_acquire_rejects_invalid_deadline_values() -> None:
    limiter, _ = _coordinator()
    with pytest.raises(CapacityValidationError):
        limiter.acquire("a", _NODE, wait_deadline=cast(UtcTimestamp, _BASE))
    with pytest.raises(CapacityValidationError, match="maximum bounded wait"):
        limiter.acquire("a", _NODE, wait_deadline=_ts(MAX_WAIT_MICROSECONDS + 2))


def test_acquire_rejects_owner_already_holding() -> None:
    limiter, _ = _coordinator()
    _hold(limiter, "a")
    with pytest.raises(CapacityOwnershipError):
        limiter.acquire("a", _NODE)


def test_acquire_rejects_owner_with_partial_levels() -> None:
    limiter, _ = _coordinator()
    limiter.acquire_level(CAPACITY_CATEGORY_GLOBAL, "a")
    limiter.acquire_level(CAPACITY_CATEGORY_STRATEGY, "a")
    with pytest.raises(CapacityOwnershipError):
        limiter.acquire("a", _NODE)


def test_acquire_rejects_owner_already_pending() -> None:
    limiter, _ = _coordinator(_settings(1, 1, 1))
    _hold(limiter, "holder")
    errors = _ErrorSink()

    def waiter() -> None:
        try:
            limiter.acquire("w", _NODE, wait_deadline=_ts(_FAR_DEADLINE_MICROSECONDS))
        except Exception as error:
            errors.record(error)

    thread = threading.Thread(target=waiter)
    thread.start()
    _spin_until(lambda: _node_waiting(limiter) >= 1)
    with pytest.raises(CapacityOwnershipError):
        limiter.acquire("w", _NODE)
    limiter.close()
    thread.join()
    assert not thread.is_alive()
    assert isinstance(errors.errors[0], CapacityClosedError)


# --------------------------------------------------------------------------------------
# Saturation and all-or-nothing rollback
# --------------------------------------------------------------------------------------


def test_try_once_saturation_at_strategy_level_rolls_back() -> None:
    limiter, _ = _coordinator(_settings(4, 1, 4))
    _hold(limiter, "a")
    with pytest.raises(CapacityTimeoutError):
        limiter.acquire("b", _NODE)
    assert limiter.in_use(CAPACITY_CATEGORY_GLOBAL) == 1
    assert limiter.in_use(CAPACITY_CATEGORY_STRATEGY) == 1
    assert limiter.in_use(CAPACITY_CATEGORY_NODE, _NODE) == 1
    assert _waiting_total(limiter) == 0
    assert limiter.max_observed() == {
        CAPACITY_CATEGORY_GLOBAL: 1,
        CAPACITY_CATEGORY_STRATEGY: 1,
        _NODE: 1,
    }


def test_try_once_saturation_at_node_level_rolls_back() -> None:
    limiter, _ = _coordinator(_settings(4, 4, 1))
    _hold(limiter, "a")
    with pytest.raises(CapacityTimeoutError):
        limiter.acquire("b", _NODE)
    assert limiter.in_use(CAPACITY_CATEGORY_GLOBAL) == 1
    assert limiter.in_use(CAPACITY_CATEGORY_STRATEGY) == 1
    assert limiter.in_use(CAPACITY_CATEGORY_NODE, _NODE) == 1
    assert _waiting_total(limiter) == 0


def test_try_once_saturation_at_global_level_rolls_back() -> None:
    limiter, _ = _coordinator(_settings(1, 1, 1))
    _hold(limiter, "a")
    with pytest.raises(CapacityTimeoutError):
        limiter.acquire("b", _NODE)
    assert limiter.in_use(CAPACITY_CATEGORY_GLOBAL) == 1
    assert limiter.in_use(CAPACITY_CATEGORY_STRATEGY) == 1
    assert _waiting_total(limiter) == 0


def test_max_observed_never_exceeds_captured_limits_across_cycles() -> None:
    limiter, _ = _coordinator(_settings(2, 2, 2))
    for round_index in range(25):
        first_owner = f"a{round_index}"
        second_owner = f"b{round_index}"
        _hold(limiter, first_owner)
        _hold(limiter, second_owner)
        with pytest.raises(CapacityTimeoutError):
            limiter.acquire("c", _NODE)
        limiter.release(first_owner, _NODE)
        limiter.release(second_owner, _NODE)
    observed = limiter.max_observed()
    assert observed == {
        CAPACITY_CATEGORY_GLOBAL: 2,
        CAPACITY_CATEGORY_STRATEGY: 2,
        _NODE: 2,
    }
    assert all(snapshot.in_use == 0 for snapshot in limiter.snapshot())


def test_contended_acquisition_never_exceeds_captured_limits() -> None:
    limit = 2
    attempts = limit + 4
    limiter, _ = _coordinator(_settings(2, 2, 2))
    barrier = threading.Barrier(attempts + 1)
    gate = threading.Event()
    successes = _Counter()
    peak = _Peak()
    errors = _ErrorSink()

    def worker(index: int) -> None:
        owner = f"owner-{index}"
        try:
            barrier.wait()
            permits = limiter.acquire(owner, _NODE, wait_deadline=_ts(_FAR_DEADLINE_MICROSECONDS))
            peak.enter()
            successes.increment()
            gate.wait()
            limiter.release(owner, _NODE)
            peak.leave()
            assert len(permits) == 3
        except Exception as error:
            errors.record(error)

    def helper() -> None:
        barrier.wait()
        successes.wait_until_at_least(limit)
        gate.set()

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(attempts)]
    helper_thread = threading.Thread(target=helper)
    for thread in threads:
        thread.start()
    helper_thread.start()
    _join_all([*threads, helper_thread])

    assert errors.errors == []
    assert successes.count == attempts
    assert peak.peak == limit
    assert limiter.in_use(CAPACITY_CATEGORY_GLOBAL) == 0
    assert limiter.in_use(CAPACITY_CATEGORY_STRATEGY) == 0
    assert limiter.in_use(CAPACITY_CATEGORY_NODE, _NODE) == 0
    assert _waiting_total(limiter) == 0
    assert limiter.max_observed() == {
        CAPACITY_CATEGORY_GLOBAL: 2,
        CAPACITY_CATEGORY_STRATEGY: 2,
        _NODE: 2,
    }


# --------------------------------------------------------------------------------------
# Bounded waiting, deadlines, and closing
# --------------------------------------------------------------------------------------


def test_expired_deadline_times_out_immediately() -> None:
    limiter, _ = _coordinator()
    with pytest.raises(CapacityTimeoutError) as error:
        limiter.acquire("a", _NODE, wait_deadline=_ts(-1))
    assert type(error.value) is CapacityTimeoutError
    assert limiter.in_use(CAPACITY_CATEGORY_GLOBAL) == 0
    assert _waiting_total(limiter) == 0


def test_waiter_woken_before_deadline_succeeds() -> None:
    limiter, _ = _coordinator(_settings(1, 1, 1))
    _hold(limiter, "holder")
    errors = _ErrorSink()
    acquired = threading.Event()
    gate = threading.Event()

    def waiter() -> None:
        try:
            limiter.acquire("w", _NODE, wait_deadline=_ts(_FAR_DEADLINE_MICROSECONDS))
            acquired.set()
            gate.wait()
            limiter.release("w", _NODE)
        except Exception as error:
            errors.record(error)

    thread = threading.Thread(target=waiter)
    thread.start()
    _spin_until(lambda: _node_waiting(limiter) >= 1)
    limiter.release("holder", _NODE)
    assert acquired.wait()
    assert limiter.in_use(CAPACITY_CATEGORY_GLOBAL) == 1
    gate.set()
    thread.join()
    assert not thread.is_alive()
    assert errors.errors == []
    assert all(snapshot.in_use == 0 for snapshot in limiter.snapshot())


def test_waiter_times_out_when_injected_deadline_passes() -> None:
    clock = ManualClock(_ts())
    limiter, _ = _coordinator(_settings(1, 1, 1), clock=clock)
    _hold(limiter, "holder")
    errors = _ErrorSink()
    timed_out = threading.Event()

    def waiter() -> None:
        try:
            limiter.acquire("w", _NODE, wait_deadline=_ts(10_000_000))
        except CapacityTimeoutError:
            timed_out.set()
        except Exception as error:
            errors.record(error)

    thread = threading.Thread(target=waiter)
    thread.start()
    _spin_until(lambda: _node_waiting(limiter) >= 1)
    clock.advance(10_000_001)
    limiter.release("holder", _NODE)
    assert timed_out.wait()
    thread.join()
    assert not thread.is_alive()
    assert errors.errors == []
    assert _waiting_total(limiter) == 0


def test_close_wakes_scheduled_waiters_with_typed_error() -> None:
    limiter, _ = _coordinator(_settings(1, 1, 1))
    _hold(limiter, "holder")
    errors = _ErrorSink()
    closed = threading.Event()

    def waiter() -> None:
        try:
            limiter.acquire("w", _NODE, wait_deadline=_ts(_FAR_DEADLINE_MICROSECONDS))
            errors.record(AssertionError("waiter should not have acquired"))
        except CapacityClosedError:
            closed.set()
        except Exception as error:
            errors.record(error)

    thread = threading.Thread(target=waiter)
    thread.start()
    _spin_until(lambda: _node_waiting(limiter) >= 1)
    limiter.close()
    assert closed.wait()
    thread.join()
    assert not thread.is_alive()
    assert errors.errors == []


def test_snapshot_reports_waiting_counts_while_waiters_queue() -> None:
    limiter, _ = _coordinator(_settings(1, 1, 1))
    _hold(limiter, "holder")
    errors = _ErrorSink()

    def waiter() -> None:
        try:
            limiter.acquire("w", _NODE, wait_deadline=_ts(_FAR_DEADLINE_MICROSECONDS))
        except Exception as error:
            errors.record(error)

    thread = threading.Thread(target=waiter)
    thread.start()
    _spin_until(lambda: _node_waiting(limiter) >= 1)
    snapshots = limiter.snapshot()
    assert [snapshot.waiting for snapshot in snapshots] == [1, 1, 1]
    assert [snapshot.in_use for snapshot in snapshots] == [1, 1, 1]
    limiter.close()
    thread.join()
    assert not thread.is_alive()
    assert isinstance(errors.errors[0], CapacityClosedError)


# --------------------------------------------------------------------------------------
# FIFO fairness
# --------------------------------------------------------------------------------------


def test_fifo_waiters_are_served_in_arrival_order() -> None:
    limiter, _ = _coordinator(_settings(1, 1, 1))
    _hold(limiter, "holder")
    order: list[str] = []
    order_lock = threading.Lock()
    errors = _ErrorSink()
    done = {
        "a": threading.Event(),
        "b": threading.Event(),
        "c": threading.Event(),
    }
    gate = threading.Event()

    def worker(owner: str) -> None:
        try:
            limiter.acquire(owner, _NODE, wait_deadline=_ts(_FAR_DEADLINE_MICROSECONDS))
            with order_lock:
                order.append(owner)
            done[owner].set()
            gate.wait()
            limiter.release(owner, _NODE)
        except Exception as error:
            errors.record(error)

    threads = [threading.Thread(target=worker, args=(owner,)) for owner in ("a", "b", "c")]
    for expected, thread in enumerate(threads, start=1):
        thread.start()
        _spin_until(lambda bound=expected: _node_waiting(limiter) >= bound)

    limiter.release("holder", _NODE)
    assert done["a"].wait()
    assert order == ["a"]
    assert not done["b"].is_set()
    assert not done["c"].is_set()

    gate.set()
    assert done["b"].wait()
    assert done["c"].wait()
    _join_all(threads)
    assert order == ["a", "b", "c"]
    assert errors.errors == []
    assert all(snapshot.in_use == 0 for snapshot in limiter.snapshot())


# --------------------------------------------------------------------------------------
# ScheduledWorkLimiters.release
# --------------------------------------------------------------------------------------


def test_release_returns_capacity_and_wakes_waiters() -> None:
    limiter, _ = _coordinator(_settings(1, 1, 1))
    _hold(limiter, "a")
    errors = _ErrorSink()
    acquired = threading.Event()
    gate = threading.Event()

    def waiter() -> None:
        try:
            limiter.acquire("b", _NODE, wait_deadline=_ts(_FAR_DEADLINE_MICROSECONDS))
            acquired.set()
            gate.wait()
            limiter.release("b", _NODE)
        except Exception as error:
            errors.record(error)

    thread = threading.Thread(target=waiter)
    thread.start()
    _spin_until(lambda: _node_waiting(limiter) >= 1)
    limiter.release("a", _NODE)
    assert acquired.wait()
    assert limiter.in_use(CAPACITY_CATEGORY_GLOBAL) == 1
    gate.set()
    thread.join()
    assert not thread.is_alive()
    assert errors.errors == []
    assert all(snapshot.in_use == 0 for snapshot in limiter.snapshot())


def test_release_rejects_unknown_owner() -> None:
    limiter, _ = _coordinator()
    with pytest.raises(CapacityOwnershipError, match="no scheduled-work reservation"):
        limiter.release("a", _NODE)


def test_release_rejects_wrong_node_without_releasing() -> None:
    limiter, _ = _coordinator(node_ids=("n1", "n2"))
    _hold(limiter, "a", "n1")
    with pytest.raises(CapacityOwnershipError, match="complete"):
        limiter.release("a", "n2")
    assert limiter.in_use(CAPACITY_CATEGORY_GLOBAL) == 1
    limiter.release("a", "n1")
    assert limiter.in_use(CAPACITY_CATEGORY_GLOBAL) == 0


def test_double_release_raises_typed_error() -> None:
    limiter, _ = _coordinator()
    _hold(limiter, "a")
    limiter.release("a", _NODE)
    with pytest.raises(CapacityOwnershipError):
        limiter.release("a", _NODE)


def test_release_rejects_invalid_arguments() -> None:
    limiter, _ = _coordinator()
    with pytest.raises(CapacityValidationError):
        limiter.release(cast(str, 123), _NODE)
    with pytest.raises(CapacityValidationError):
        limiter.release("a", cast(str, 123))


def test_release_reservation_alias_behaves_identically() -> None:
    limiter, _ = _coordinator()
    _hold(limiter, "a")
    limiter.release_reservation("a", _NODE)
    with pytest.raises(CapacityOwnershipError):
        limiter.release_reservation("a", _NODE)
    with pytest.raises(CapacityOwnershipError):
        limiter.release("a", _NODE)
    assert limiter.in_use(CAPACITY_CATEGORY_GLOBAL) == 0


def test_release_reservation_rejects_failed_partial_admission() -> None:
    limiter, _ = _coordinator()
    limiter.acquire_level(CAPACITY_CATEGORY_GLOBAL, "a")
    with pytest.raises(CapacityOwnershipError):
        limiter.release_reservation("a", _NODE)


def test_release_after_close_still_drains() -> None:
    limiter, _ = _coordinator()
    _hold(limiter, "a")
    limiter.close()
    limiter.release("a", _NODE)
    assert limiter.in_use(CAPACITY_CATEGORY_GLOBAL) == 0


# --------------------------------------------------------------------------------------
# Level-wise acquisition and release order
# --------------------------------------------------------------------------------------


def test_acquire_level_and_release_level_follow_stable_order() -> None:
    limiter, _ = _coordinator()
    global_permit = limiter.acquire_level(CAPACITY_CATEGORY_GLOBAL, "a")
    assert limiter.parent_holds("a") is False
    strategy_permit = limiter.acquire_level(CAPACITY_CATEGORY_STRATEGY, "a")
    node_permit = limiter.acquire_level(CAPACITY_CATEGORY_NODE, "a", _NODE)
    assert (
        global_permit.category,
        strategy_permit.category,
        node_permit.category,
    ) == (CAPACITY_CATEGORY_GLOBAL, CAPACITY_CATEGORY_STRATEGY, CAPACITY_CATEGORY_NODE)
    assert limiter.parent_holds("a") is True
    limiter.release_level(CAPACITY_CATEGORY_NODE, "a", _NODE)
    limiter.release_level(CAPACITY_CATEGORY_STRATEGY, "a")
    limiter.release_level(CAPACITY_CATEGORY_GLOBAL, "a")
    assert limiter.parent_holds("a") is False
    assert all(snapshot.in_use == 0 for snapshot in limiter.snapshot())


def test_acquire_level_rejects_inversion_and_skips() -> None:
    limiter, _ = _coordinator()
    with pytest.raises(CapacityOrderError):
        limiter.acquire_level(CAPACITY_CATEGORY_STRATEGY, "a")
    with pytest.raises(CapacityOrderError):
        limiter.acquire_level(CAPACITY_CATEGORY_NODE, "a", _NODE)
    limiter.acquire_level(CAPACITY_CATEGORY_GLOBAL, "a")
    with pytest.raises(CapacityOrderError):
        limiter.acquire_level(CAPACITY_CATEGORY_NODE, "a", _NODE)
    with pytest.raises(CapacityOrderError):
        limiter.acquire_level(CAPACITY_CATEGORY_GLOBAL, "a")


def test_acquire_level_rejects_second_node_per_admission() -> None:
    limiter, _ = _coordinator(node_ids=("n1", "n2"))
    limiter.acquire_level(CAPACITY_CATEGORY_GLOBAL, "a")
    limiter.acquire_level(CAPACITY_CATEGORY_STRATEGY, "a")
    limiter.acquire_level(CAPACITY_CATEGORY_NODE, "a", "n1")
    with pytest.raises(CapacityOrderError):
        limiter.acquire_level(CAPACITY_CATEGORY_NODE, "a", "n2")


def test_acquire_level_saturation_fails_without_waiting() -> None:
    limiter, _ = _coordinator(_settings(1, 1, 1))
    _hold(limiter, "a")
    with pytest.raises(CapacityTimeoutError):
        limiter.acquire_level(CAPACITY_CATEGORY_GLOBAL, "b")


def test_acquire_level_cannot_jump_queued_waiters() -> None:
    limiter, _ = _coordinator(_settings(2, 2, 1))
    _hold(limiter, "holder")
    errors = _ErrorSink()

    def waiter() -> None:
        try:
            limiter.acquire("w", _NODE, wait_deadline=_ts(_FAR_DEADLINE_MICROSECONDS))
        except Exception as error:
            errors.record(error)

    thread = threading.Thread(target=waiter)
    thread.start()
    _spin_until(lambda: _node_waiting(limiter) >= 1)
    with pytest.raises(CapacityTimeoutError):
        limiter.acquire_level(CAPACITY_CATEGORY_GLOBAL, "jumper")
    limiter.close()
    thread.join()
    assert not thread.is_alive()
    assert isinstance(errors.errors[0], CapacityClosedError)


@pytest.mark.parametrize(
    "category",
    [CAPACITY_CATEGORY_CONNECTOR, CAPACITY_CATEGORY_CPU_POOL],
)
def test_acquire_level_rejects_subordinate_categories(category: str) -> None:
    limiter, _ = _coordinator()
    with pytest.raises(CapacityOwnershipError):
        limiter.acquire_level(category, "a")


def test_acquire_level_rejects_unknown_category() -> None:
    limiter, _ = _coordinator()
    with pytest.raises(CapacityValidationError):
        limiter.acquire_level("bogus", "a")


def test_acquire_level_argument_validation() -> None:
    limiter, _ = _coordinator()
    with pytest.raises(CapacityValidationError):
        limiter.acquire_level(CAPACITY_CATEGORY_NODE, "a")
    with pytest.raises(CapacityValidationError):
        limiter.acquire_level(CAPACITY_CATEGORY_GLOBAL, "a", _NODE)
    with pytest.raises(CapacityValidationError):
        limiter.acquire_level(CAPACITY_CATEGORY_NODE, "a", "nX")
    with pytest.raises(CapacityValidationError):
        limiter.acquire_level(CAPACITY_CATEGORY_GLOBAL, cast(str, ""))


def test_acquire_level_after_close_raises_typed_error() -> None:
    limiter, _ = _coordinator()
    limiter.close()
    with pytest.raises(CapacityClosedError):
        limiter.acquire_level(CAPACITY_CATEGORY_GLOBAL, "a")


def test_release_level_enforces_reverse_order() -> None:
    limiter, _ = _coordinator()
    limiter.acquire_level(CAPACITY_CATEGORY_GLOBAL, "a")
    limiter.acquire_level(CAPACITY_CATEGORY_STRATEGY, "a")
    limiter.acquire_level(CAPACITY_CATEGORY_NODE, "a", _NODE)
    with pytest.raises(CapacityOrderError):
        limiter.release_level(CAPACITY_CATEGORY_GLOBAL, "a")
    with pytest.raises(CapacityOrderError):
        limiter.release_level(CAPACITY_CATEGORY_STRATEGY, "a")
    limiter.release_level(CAPACITY_CATEGORY_NODE, "a", _NODE)
    with pytest.raises(CapacityOrderError):
        limiter.release_level(CAPACITY_CATEGORY_GLOBAL, "a")
    limiter.release_level(CAPACITY_CATEGORY_STRATEGY, "a")
    limiter.release_level(CAPACITY_CATEGORY_GLOBAL, "a")
    assert all(snapshot.in_use == 0 for snapshot in limiter.snapshot())


def test_release_level_rejects_unknown_owner_and_categories() -> None:
    limiter, _ = _coordinator()
    with pytest.raises(CapacityOwnershipError):
        limiter.release_level(CAPACITY_CATEGORY_GLOBAL, "a")
    with pytest.raises(CapacityOwnershipError):
        limiter.release_level(CAPACITY_CATEGORY_CONNECTOR, "a")
    with pytest.raises(CapacityValidationError):
        limiter.release_level("bogus", "a")


def test_release_level_argument_validation() -> None:
    limiter, _ = _coordinator()
    with pytest.raises(CapacityValidationError):
        limiter.release_level(CAPACITY_CATEGORY_NODE, "a")
    with pytest.raises(CapacityValidationError):
        limiter.release_level(CAPACITY_CATEGORY_GLOBAL, "a", _NODE)
    with pytest.raises(CapacityValidationError):
        limiter.release_level(CAPACITY_CATEGORY_NODE, "a", "nX")


# --------------------------------------------------------------------------------------
# parent_holds and subordinate registration
# --------------------------------------------------------------------------------------


def test_parent_holds_validates_owner_and_reports_full_hold() -> None:
    limiter, _ = _coordinator()
    with pytest.raises(CapacityValidationError):
        limiter.parent_holds(cast(str, 123))
    assert limiter.parent_holds("a") is False
    _hold(limiter, "a")
    assert limiter.parent_holds("a") is True
    limiter.release("a", _NODE)
    assert limiter.parent_holds("a") is False


def test_register_subordinate_enforces_subordinate_rules() -> None:
    limiter, _ = _coordinator()
    global_permit, _, _ = _hold(limiter, "a")
    connector_permit = CapacityPermit(CAPACITY_CATEGORY_CONNECTOR, 1, "a", 1)
    with pytest.raises(CapacityValidationError):
        limiter.register_subordinate("a", cast(CapacityPermit, object()))
    with pytest.raises(CapacityOwnershipError, match="connector or cpu-pool"):
        limiter.register_subordinate("a", global_permit)
    with pytest.raises(CapacityOwnershipError, match="same owner key"):
        limiter.register_subordinate("a", CapacityPermit(CAPACITY_CATEGORY_CONNECTOR, 1, "b", 1))
    limiter.register_subordinate("a", connector_permit)
    with pytest.raises(CapacityOwnershipError, match="before scheduled-work"):
        limiter.release("a", _NODE)
    limiter.unregister_subordinate("a", connector_permit)
    with pytest.raises(CapacityOwnershipError):
        limiter.unregister_subordinate("a", connector_permit)
    limiter.release("a", _NODE)


def test_register_subordinate_requires_parent_hold() -> None:
    limiter, _ = _coordinator()
    permit = CapacityPermit(CAPACITY_CATEGORY_CPU_POOL, 1, "z", 1)
    with pytest.raises(CapacityOwnershipError, match="held scheduled-work reservation"):
        limiter.register_subordinate("z", permit)


def test_unregister_subordinate_rejects_wrong_permit_and_types() -> None:
    limiter, _ = _coordinator()
    _hold(limiter, "a")
    registered = CapacityPermit(CAPACITY_CATEGORY_CONNECTOR, 1, "a", 1)
    limiter.register_subordinate("a", registered)
    with pytest.raises(CapacityValidationError):
        limiter.unregister_subordinate("a", cast(CapacityPermit, "nope"))
    with pytest.raises(CapacityValidationError):
        limiter.unregister_subordinate(cast(str, 123), registered)
    with pytest.raises(CapacityOwnershipError):
        limiter.unregister_subordinate("a", CapacityPermit(CAPACITY_CATEGORY_CPU_POOL, 2, "a", 1))
    limiter.unregister_subordinate("a", registered)
    limiter.release("a", _NODE)


def test_owner_may_hold_subordinate_permits_from_distinct_limiters() -> None:
    limiter, clock = _coordinator()
    permits = _hold(limiter, "a")
    connector = _connector(limiter, clock)
    cpu_pool = _connector(limiter, clock, category=CAPACITY_CATEGORY_CPU_POOL)
    connector_permit = connector.acquire("a", parent=permits)
    cpu_permit = cpu_pool.acquire("a", parent=permits)
    with pytest.raises(CapacityOwnershipError):
        limiter.release("a", _NODE)
    connector.release(connector_permit)
    with pytest.raises(CapacityOwnershipError):
        limiter.release("a", _NODE)
    cpu_pool.release(cpu_permit)
    limiter.release("a", _NODE)
    assert limiter.parent_holds("a") is False


# --------------------------------------------------------------------------------------
# Observability: in_use, snapshot, max_observed
# --------------------------------------------------------------------------------------


def test_in_use_reports_each_level_independently() -> None:
    limiter, _ = _coordinator(node_ids=("n1", "n2"))
    _hold(limiter, "a", "n1")
    _hold(limiter, "b", "n2")
    assert limiter.in_use(CAPACITY_CATEGORY_GLOBAL) == 2
    assert limiter.in_use(CAPACITY_CATEGORY_STRATEGY) == 2
    assert limiter.in_use(CAPACITY_CATEGORY_NODE, "n1") == 1
    assert limiter.in_use(CAPACITY_CATEGORY_NODE, "n2") == 1
    limiter.release("a", "n1")
    assert limiter.in_use(CAPACITY_CATEGORY_GLOBAL) == 1
    assert limiter.in_use(CAPACITY_CATEGORY_NODE, "n1") == 0


def test_in_use_argument_validation() -> None:
    limiter, _ = _coordinator()
    with pytest.raises(CapacityValidationError):
        limiter.in_use(CAPACITY_CATEGORY_CONNECTOR)
    with pytest.raises(CapacityValidationError):
        limiter.in_use("bogus")
    with pytest.raises(CapacityValidationError):
        limiter.in_use(CAPACITY_CATEGORY_NODE)
    with pytest.raises(CapacityValidationError):
        limiter.in_use(CAPACITY_CATEGORY_GLOBAL, _NODE)
    with pytest.raises(CapacityValidationError):
        limiter.in_use(CAPACITY_CATEGORY_NODE, "nX")


def test_snapshot_orders_global_strategy_then_configured_nodes() -> None:
    limiter, _ = _coordinator(_settings(4, 3, 2), node_ids=("n1", "n2"))
    snapshots = limiter.snapshot()
    assert [snapshot.category for snapshot in snapshots] == [
        CAPACITY_CATEGORY_GLOBAL,
        CAPACITY_CATEGORY_STRATEGY,
        CAPACITY_CATEGORY_NODE,
        CAPACITY_CATEGORY_NODE,
    ]
    assert [snapshot.limit for snapshot in snapshots] == [4, 3, 2, 2]
    assert all(snapshot.in_use == 0 for snapshot in snapshots)
    assert all(snapshot.max_observed_in_use == 0 for snapshot in snapshots)


def test_max_observed_keys_levels_and_nodes() -> None:
    limiter, _ = _coordinator(node_ids=("n1", "n2"))
    _hold(limiter, "a", "n1")
    assert limiter.max_observed() == {
        CAPACITY_CATEGORY_GLOBAL: 1,
        CAPACITY_CATEGORY_STRATEGY: 1,
        "n1": 1,
        "n2": 0,
    }


def test_close_is_idempotent_and_rejects_acquisitions() -> None:
    limiter, _ = _coordinator()
    limiter.close()
    limiter.close()
    limiter.close()
    with pytest.raises(CapacityClosedError):
        limiter.acquire("a", _NODE)
    with pytest.raises(CapacityClosedError):
        limiter.acquire_level(CAPACITY_CATEGORY_GLOBAL, "a")
    assert limiter.snapshot()


# --------------------------------------------------------------------------------------
# SubordinateCallLimiter construction
# --------------------------------------------------------------------------------------


def test_subordinate_constructor_accepts_connector_and_cpu_pool() -> None:
    limiter, clock = _coordinator()
    for category in (CAPACITY_CATEGORY_CONNECTOR, CAPACITY_CATEGORY_CPU_POOL):
        subordinate = _connector(limiter, clock, category=category, limit=8)
        snapshot = subordinate.snapshot()
        assert snapshot.category == category
        assert snapshot.limit == 8
        assert snapshot.in_use == 0


@pytest.mark.parametrize(
    "category",
    [
        CAPACITY_CATEGORY_GLOBAL,
        CAPACITY_CATEGORY_STRATEGY,
        CAPACITY_CATEGORY_NODE,
        "bogus",
        123,
        None,
    ],
)
def test_subordinate_constructor_rejects_other_categories(category: object) -> None:
    limiter, clock = _coordinator()
    with pytest.raises(CapacityValidationError):
        SubordinateCallLimiter(
            category=cast(str, category),
            limit=1,
            clock=clock,
            parent_limiter=limiter,
        )


@pytest.mark.parametrize("limit", [0, -1, 65_537, True, 1.0, "4", None])
def test_subordinate_constructor_rejects_invalid_limits(limit: object) -> None:
    limiter, clock = _coordinator()
    with pytest.raises(CapacityValidationError):
        SubordinateCallLimiter(
            category=CAPACITY_CATEGORY_CONNECTOR,
            limit=cast(int, limit),
            clock=clock,
            parent_limiter=limiter,
        )


def test_subordinate_constructor_rejects_invalid_rate_clock_and_parent() -> None:
    limiter, clock = _coordinator()
    with pytest.raises(CapacityValidationError):
        _connector(limiter, clock, rate=cast(RateLimitPolicy, object()))
    with pytest.raises(CapacityValidationError):
        SubordinateCallLimiter(
            category=CAPACITY_CATEGORY_CONNECTOR,
            limit=1,
            clock=cast(PolicyClock, object()),
            parent_limiter=limiter,
        )
    with pytest.raises(CapacityValidationError):
        SubordinateCallLimiter(
            category=CAPACITY_CATEGORY_CONNECTOR,
            limit=1,
            clock=clock,
            parent_limiter=cast(ScheduledWorkLimiters, object()),
        )


# --------------------------------------------------------------------------------------
# SubordinateCallLimiter.acquire
# --------------------------------------------------------------------------------------


def test_subordinate_acquire_grants_permit_and_registers_with_parent() -> None:
    limiter, clock = _coordinator()
    permits = _hold(limiter, "a")
    connector = _connector(limiter, clock, limit=2)
    permit = connector.acquire("a", parent=permits)
    assert (
        permit.category,
        permit.limit,
        permit.owner,
        permit.slot,
    ) == (CAPACITY_CATEGORY_CONNECTOR, 2, "a", 1)
    assert connector.snapshot().in_use == 1
    with pytest.raises(CapacityOwnershipError):
        limiter.release("a", _NODE)
    connector.release(permit)
    limiter.release("a", _NODE)
    assert connector.snapshot().in_use == 0


def test_subordinate_acquire_requires_parent_hold() -> None:
    limiter, clock = _coordinator()
    connector = _connector(limiter, clock)
    permits = _hold(limiter, "a")
    with pytest.raises(CapacityOwnershipError):
        connector.acquire("b", parent=permits)
    limiter.release("a", _NODE)
    with pytest.raises(CapacityOwnershipError):
        connector.acquire("a", parent=permits)


@pytest.mark.parametrize(
    "parent",
    [
        None,
        "global",
        (CapacityPermit(CAPACITY_CATEGORY_GLOBAL, 4, "a", 1),),
        (1, 2, 3),
        [CapacityPermit(CAPACITY_CATEGORY_GLOBAL, 4, "a", 1)],
    ],
)
def test_subordinate_acquire_rejects_malformed_parent_evidence(parent: object) -> None:
    limiter, clock = _coordinator()
    _hold(limiter, "a")
    connector = _connector(limiter, clock)
    with pytest.raises(CapacityValidationError):
        connector.acquire(
            "a", parent=cast("tuple[CapacityPermit, CapacityPermit, CapacityPermit]", parent)
        )


def test_subordinate_acquire_rejects_reordered_or_substituted_parent_evidence() -> None:
    limiter, clock = _coordinator()
    global_permit, strategy_permit, node_permit = _hold(limiter, "a")
    connector_permit = CapacityPermit(CAPACITY_CATEGORY_CONNECTOR, 1, "a", 1)
    connector = _connector(limiter, clock)
    with pytest.raises(CapacityOrderError):
        connector.acquire(
            "a",
            parent=(strategy_permit, global_permit, node_permit),
        )
    with pytest.raises(CapacityOrderError):
        connector.acquire(
            "a",
            parent=(connector_permit, strategy_permit, node_permit),
        )


def test_subordinate_acquire_rejects_foreign_or_forged_parent_evidence() -> None:
    limiter, clock = _coordinator()
    permits = _hold(limiter, "a")
    connector = _connector(limiter, clock)
    with pytest.raises(CapacityOwnershipError):
        connector.acquire("b", parent=permits)
    forged = (
        CapacityPermit(CAPACITY_CATEGORY_GLOBAL, 4, "b", 1),
        CapacityPermit(CAPACITY_CATEGORY_STRATEGY, 4, "b", 1),
        CapacityPermit(CAPACITY_CATEGORY_NODE, 4, "b", 1),
    )
    with pytest.raises(CapacityOwnershipError):
        connector.acquire("b", parent=forged)


def test_subordinate_acquire_rejects_duplicate_owner_hold() -> None:
    limiter, clock = _coordinator()
    permits = _hold(limiter, "a")
    connector = _connector(limiter, clock)
    first = connector.acquire("a", parent=permits)
    with pytest.raises(CapacityOwnershipError):
        connector.acquire("a", parent=permits)
    connector.release(first)


def test_subordinate_acquire_rejects_invalid_owner() -> None:
    limiter, clock = _coordinator()
    permits = _hold(limiter, "a")
    connector = _connector(limiter, clock)
    with pytest.raises(CapacityValidationError):
        connector.acquire(cast(str, 123), parent=permits)


def test_subordinate_deadline_validation() -> None:
    limiter, clock = _coordinator()
    permits = _hold(limiter, "a")
    connector = _connector(limiter, clock)
    with pytest.raises(CapacityValidationError):
        connector.acquire("a", parent=permits, wait_deadline=cast(UtcTimestamp, "soon"))
    with pytest.raises(CapacityValidationError, match="maximum bounded wait"):
        connector.acquire("a", parent=permits, wait_deadline=_ts(MAX_WAIT_MICROSECONDS + 2))


def test_subordinate_expired_deadline_times_out_immediately() -> None:
    limiter, clock = _coordinator()
    permits = _hold(limiter, "a")
    connector = _connector(limiter, clock, limit=2)
    with pytest.raises(CapacityTimeoutError):
        connector.acquire("a", parent=permits, wait_deadline=_ts(-1))
    assert connector.snapshot().in_use == 0


def test_subordinate_saturation_without_deadline_times_out() -> None:
    limiter, clock = _coordinator()
    permits_a = _hold(limiter, "a")
    permits_b = _hold(limiter, "b")
    connector = _connector(limiter, clock, limit=1)
    first = connector.acquire("a", parent=permits_a)
    with pytest.raises(CapacityTimeoutError):
        connector.acquire("b", parent=permits_b)
    assert connector.snapshot().in_use == 1
    assert connector.snapshot().waiting == 0
    connector.release(first)


def test_subordinate_waiter_wakes_on_release() -> None:
    limiter, clock = _coordinator()
    permits_a = _hold(limiter, "a")
    permits_b = _hold(limiter, "b")
    connector = _connector(limiter, clock, limit=1)
    first = connector.acquire("a", parent=permits_a)
    errors = _ErrorSink()
    acquired = threading.Event()
    gate = threading.Event()

    def waiter() -> None:
        try:
            permit = connector.acquire(
                "b",
                parent=permits_b,
                wait_deadline=_ts(_FAR_DEADLINE_MICROSECONDS),
            )
            acquired.set()
            gate.wait()
            connector.release(permit)
        except Exception as error:
            errors.record(error)

    thread = threading.Thread(target=waiter)
    thread.start()
    _spin_until(lambda: connector.snapshot().waiting >= 1)
    connector.release(first)
    assert acquired.wait()
    assert connector.snapshot().in_use == 1
    gate.set()
    thread.join()
    assert not thread.is_alive()
    assert errors.errors == []
    assert connector.snapshot().in_use == 0


def test_subordinate_waiter_fails_when_parent_released_during_wait() -> None:
    limiter, clock = _coordinator()
    permits_a = _hold(limiter, "a")
    permits_b = _hold(limiter, "b")
    connector = _connector(limiter, clock, limit=1)
    first = connector.acquire("a", parent=permits_a)
    errors = _ErrorSink()
    finished = threading.Event()

    def waiter() -> None:
        try:
            connector.acquire(
                "b",
                parent=permits_b,
                wait_deadline=_ts(_FAR_DEADLINE_MICROSECONDS),
            )
            errors.record(AssertionError("waiter should not have acquired"))
        except CapacityOwnershipError:
            finished.set()
        except Exception as error:
            errors.record(error)

    thread = threading.Thread(target=waiter)
    thread.start()
    _spin_until(lambda: connector.snapshot().waiting >= 1)
    limiter.release("b", _NODE)
    connector.release(first)
    assert finished.wait()
    thread.join()
    assert not thread.is_alive()
    assert errors.errors == []
    limiter.release("a", _NODE)


def test_subordinate_waiter_times_out_when_injected_deadline_passes() -> None:
    clock = ManualClock(_ts())
    limiter, _ = _coordinator(clock=clock)
    permits_a = _hold(limiter, "a")
    permits_b = _hold(limiter, "b")
    connector = _connector(limiter, clock, limit=1)
    first = connector.acquire("a", parent=permits_a)
    errors = _ErrorSink()
    timed_out = threading.Event()

    def waiter() -> None:
        try:
            connector.acquire("b", parent=permits_b, wait_deadline=_ts(10_000_000))
        except CapacityTimeoutError:
            timed_out.set()
        except Exception as error:
            errors.record(error)

    thread = threading.Thread(target=waiter)
    thread.start()
    _spin_until(lambda: connector.snapshot().waiting >= 1)
    clock.advance(10_000_001)
    connector.release(first)
    assert timed_out.wait()
    thread.join()
    assert not thread.is_alive()
    assert errors.errors == []
    limiter.release("a", _NODE)
    limiter.release("b", _NODE)


def test_subordinate_close_wakes_waiters_and_rejects_acquisition() -> None:
    limiter, clock = _coordinator()
    permits_a = _hold(limiter, "a")
    permits_b = _hold(limiter, "b")
    connector = _connector(limiter, clock, limit=1)
    first = connector.acquire("a", parent=permits_a)
    errors = _ErrorSink()
    closed = threading.Event()

    def waiter() -> None:
        try:
            connector.acquire(
                "b",
                parent=permits_b,
                wait_deadline=_ts(_FAR_DEADLINE_MICROSECONDS),
            )
            errors.record(AssertionError("waiter should not have acquired"))
        except CapacityClosedError:
            closed.set()
        except Exception as error:
            errors.record(error)

    thread = threading.Thread(target=waiter)
    thread.start()
    _spin_until(lambda: connector.snapshot().waiting >= 1)
    connector.close()
    assert closed.wait()
    thread.join()
    assert not thread.is_alive()
    assert errors.errors == []
    with pytest.raises(CapacityClosedError):
        connector.acquire("a", parent=permits_a)
    connector.close()
    connector.release(first)
    assert connector.snapshot().in_use == 0


# --------------------------------------------------------------------------------------
# Rate-limited subordinate acquisition
# --------------------------------------------------------------------------------------


def test_rate_limited_subordinate_returns_exact_retry_instant() -> None:
    clock = ManualClock(_ts())
    limiter, _ = _coordinator(clock=clock)
    permits_a = _hold(limiter, "a")
    permits_b = _hold(limiter, "b")
    connector = _connector(limiter, clock, limit=2, rate=_rate())
    assert connector.acquire("a", parent=permits_a).slot == 1
    with pytest.raises(CapacityTimeoutError) as plain:
        connector.acquire("b", parent=permits_b)
    assert type(plain.value) is CapacityTimeoutError
    assert not isinstance(plain.value, CapacityRateDeferredError)
    with pytest.raises(CapacityRateDeferredError) as deferred:
        connector.acquire("b", parent=permits_b, wait_deadline=_ts(_FAR_DEADLINE_MICROSECONDS))
    assert deferred.value.retry_at == _ts(_RATE_INTERVAL_MICROSECONDS)
    assert connector.snapshot().in_use == 1
    clock.advance(_RATE_INTERVAL_MICROSECONDS)
    permit = connector.acquire(
        "b",
        parent=permits_b,
        wait_deadline=_ts(_FAR_DEADLINE_MICROSECONDS + _RATE_INTERVAL_MICROSECONDS),
    )
    assert permit.slot == 2
    assert connector.snapshot().in_use == 2


def test_rate_limited_subordinate_deadline_before_retry_times_out() -> None:
    limiter, clock = _coordinator()
    permits_a = _hold(limiter, "a")
    permits_b = _hold(limiter, "b")
    connector = _connector(limiter, clock, limit=2, rate=_rate())
    connector.acquire("a", parent=permits_a)
    with pytest.raises(CapacityTimeoutError) as error:
        connector.acquire("b", parent=permits_b, wait_deadline=_ts(400_000))
    assert type(error.value) is CapacityTimeoutError
    assert not isinstance(error.value, CapacityRateDeferredError)


# --------------------------------------------------------------------------------------
# SubordinateCallLimiter.try_acquire and release
# --------------------------------------------------------------------------------------


def test_try_acquire_requires_parent_and_respects_capacity() -> None:
    limiter, clock = _coordinator()
    _hold(limiter, "a")
    _hold(limiter, "b")
    connector = _connector(limiter, clock, limit=1)
    with pytest.raises(CapacityOwnershipError):
        connector.try_acquire("z")
    permit = connector.try_acquire("a")
    assert isinstance(permit, CapacityPermit)
    assert connector.snapshot().in_use == 1
    assert connector.try_acquire("b") is None
    with pytest.raises(CapacityOwnershipError):
        connector.try_acquire("a")
    connector.release(permit)
    assert connector.snapshot().in_use == 0


def test_try_acquire_respects_rate_availability() -> None:
    clock = ManualClock(_ts())
    limiter, _ = _coordinator(clock=clock)
    _hold(limiter, "a")
    _hold(limiter, "b")
    connector = _connector(limiter, clock, limit=2, rate=_rate())
    permit_a = connector.try_acquire("a")
    assert isinstance(permit_a, CapacityPermit)
    assert connector.try_acquire("b") is None
    clock.advance(_RATE_INTERVAL_MICROSECONDS)
    permit_b = connector.try_acquire("b")
    assert isinstance(permit_b, CapacityPermit)
    connector.release(permit_a)
    connector.release(permit_b)


def test_try_acquire_rejects_invalid_owner_and_close() -> None:
    limiter, clock = _coordinator()
    _hold(limiter, "a")
    connector = _connector(limiter, clock)
    with pytest.raises(CapacityValidationError):
        connector.try_acquire(cast(str, ""))
    connector.close()
    with pytest.raises(CapacityClosedError):
        connector.try_acquire("a")


def test_subordinate_release_rejects_mismatches_and_double_release() -> None:
    limiter, clock = _coordinator()
    permits_a = _hold(limiter, "a")
    permits_b = _hold(limiter, "b")
    connector = _connector(limiter, clock, limit=2)
    permit = connector.acquire("a", parent=permits_a)
    other = connector.acquire("b", parent=permits_b)
    with pytest.raises(CapacityValidationError):
        connector.release(cast(CapacityPermit, "nope"))
    with pytest.raises(CapacityOwnershipError, match="different capacity category"):
        connector.release(CapacityPermit(CAPACITY_CATEGORY_CPU_POOL, 2, "a", 1))
    with pytest.raises(CapacityOwnershipError):
        connector.release(CapacityPermit(CAPACITY_CATEGORY_CONNECTOR, 2, "a", 2))
    connector.release(permit)
    with pytest.raises(CapacityOwnershipError):
        connector.release(permit)
    connector.release(other)
    assert connector.snapshot().in_use == 0
    limiter.release("a", _NODE)
    limiter.release("b", _NODE)


def test_subordinate_release_unblocks_parent_release() -> None:
    limiter, clock = _coordinator()
    permits = _hold(limiter, "a")
    connector = _connector(limiter, clock)
    permit = connector.acquire("a", parent=permits)
    with pytest.raises(CapacityOwnershipError):
        limiter.release_level(CAPACITY_CATEGORY_NODE, "a", _NODE)
    connector.release(permit)
    limiter.release("a", _NODE)
    assert limiter.parent_holds("a") is False


def test_subordinate_snapshot_tracks_use_waiting_and_peak() -> None:
    limiter, clock = _coordinator()
    permits = _hold(limiter, "a")
    connector = _connector(limiter, clock, limit=1)
    connector.acquire("a", parent=permits)
    snapshot = connector.snapshot()
    assert (
        snapshot.category,
        snapshot.limit,
        snapshot.in_use,
        snapshot.waiting,
        snapshot.max_observed_in_use,
    ) == (CAPACITY_CATEGORY_CONNECTOR, 1, 1, 0, 1)
