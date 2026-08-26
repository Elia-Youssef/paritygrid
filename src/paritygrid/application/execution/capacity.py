"""Bounded scheduled-work and subordinate capacity ledgers for P7.6.

This module provides admission-time capacity accounting only. It never
creates a process pool, worker, channel, or connection, and it never
consults an ambient clock: every wait bound and rate decision comes from
the injected policy clock.
"""

from __future__ import annotations

import re
from collections import deque
from dataclasses import dataclass, field
from datetime import timedelta
from threading import Condition
from typing import cast

from paritygrid.application.execution.clock_policy import (
    PolicyClock,
    RateLimitPolicy,
    TokenBucket,
)
from paritygrid.application.execution.concurrency_settings import (
    MAX_CAPTURED_LIMIT,
    MAX_STRATEGY_ID_LENGTH,
    CapturedConcurrencySettings,
)
from paritygrid.domain.models import UtcTimestamp

CAPACITY_CATEGORY_CONNECTOR = "connector"
CAPACITY_CATEGORY_CPU_POOL = "cpu_pool"
CAPACITY_CATEGORY_GLOBAL = "global"
CAPACITY_CATEGORY_NODE = "node"
CAPACITY_CATEGORY_STRATEGY = "strategy"
MAX_WAIT_MICROSECONDS = 60_000_000

MIN_PERMIT_LIMIT = 1
MAX_OWNER_LENGTH = 128

_SCHEDULED_CATEGORIES: tuple[str, ...] = (
    CAPACITY_CATEGORY_GLOBAL,
    CAPACITY_CATEGORY_STRATEGY,
    CAPACITY_CATEGORY_NODE,
)
_SUBORDINATE_CATEGORIES: tuple[str, ...] = (
    CAPACITY_CATEGORY_CONNECTOR,
    CAPACITY_CATEGORY_CPU_POOL,
)
_SCHEDULED_CATEGORY_SET = frozenset(_SCHEDULED_CATEGORIES)
_ALL_CATEGORY_SET = frozenset({*_SCHEDULED_CATEGORIES, *_SUBORDINATE_CATEGORIES})
_RESERVED_NODE_IDS = frozenset(_SCHEDULED_CATEGORIES)

_STRATEGY_ID_PATTERN: re.Pattern[str] = re.compile(r"[a-z][a-z0-9-]*")


class CapacityError(RuntimeError):
    """Base class for capacity ledger failures."""


class CapacityValidationError(CapacityError):
    """A capacity input has an unsupported type, limit, or key."""


class CapacityOrderError(CapacityError):
    """Scheduled-work levels were acquired or released out of the stable order."""


class CapacityOwnershipError(CapacityError):
    """A release, hold, or subordinate rule was violated for one owner key."""


class CapacityTimeoutError(CapacityError):
    """A bounded wait elapsed, or no wait deadline was given while saturated."""


class CapacityClosedError(CapacityError):
    """The capacity limiter is closed and grants no further permits."""


class CapacityRateDeferredError(CapacityTimeoutError):
    """A rate-limited acquisition that can retry at one exact injected instant."""

    def __init__(
        self, retry_at: UtcTimestamp, message: str = "rate policy defers this acquisition"
    ) -> None:
        if type(retry_at) is not UtcTimestamp:
            raise CapacityValidationError("rate retry instant must use UtcTimestamp")
        super().__init__(message)
        self.retry_at = retry_at


@dataclass(frozen=True, slots=True, repr=False)
class CapacityPermit:
    """One immutable capacity slot reservation for a single owner key."""

    category: str
    limit: int = field(compare=False)
    owner: str
    slot: int

    def __post_init__(self) -> None:
        if type(self.category) is not str or self.category not in _ALL_CATEGORY_SET:
            raise CapacityValidationError("permit category must be a known capacity category")
        if type(self.limit) is not int or not MIN_PERMIT_LIMIT <= self.limit <= MAX_CAPTURED_LIMIT:
            raise CapacityValidationError("permit limit is outside the supported range")
        _validate_owner_key(self.owner, "permit owner")
        if type(self.slot) is not int or not 1 <= self.slot <= self.limit:
            raise CapacityValidationError("permit slot is outside the permit limit")

    def __repr__(self) -> str:
        return (
            f"CapacityPermit(category={self.category!r}, limit={self.limit!r}, "
            f"slot={self.slot!r}, owner=<redacted>)"
        )


@dataclass(frozen=True, slots=True)
class CapacitySnapshot:
    """Immutable bounded observability facts for one capacity ledger."""

    category: str
    limit: int
    in_use: int
    waiting: int
    max_observed_in_use: int

    def __post_init__(self) -> None:
        if type(self.category) is not str or self.category not in _ALL_CATEGORY_SET:
            raise CapacityValidationError("snapshot category must be a known capacity category")
        if type(self.limit) is not int or not MIN_PERMIT_LIMIT <= self.limit <= MAX_CAPTURED_LIMIT:
            raise CapacityValidationError("snapshot limit is outside the supported range")
        if type(self.in_use) is not int or not 0 <= self.in_use <= self.limit:
            raise CapacityValidationError("snapshot in-use count is outside the limit")
        if type(self.waiting) is not int or self.waiting < 0:
            raise CapacityValidationError("snapshot waiting count must be non-negative")
        if (
            type(self.max_observed_in_use) is not int
            or not self.in_use <= self.max_observed_in_use <= self.limit
        ):
            raise CapacityValidationError("snapshot maximum in-use count is outside the limit")


def _validate_owner_key(value: object, subject: str) -> str:
    if type(value) is not str:
        raise CapacityValidationError(f"{subject} must be text")
    owner = value
    if not 1 <= len(owner) <= MAX_OWNER_LENGTH:
        raise CapacityValidationError(f"{subject} length is outside the supported range")
    for character in owner:
        if not "\x20" <= character <= "\x7e":
            raise CapacityValidationError(f"{subject} must use printable ASCII characters")
    return owner


def _validate_strategy_id(value: object) -> str:
    if type(value) is not str:
        raise CapacityValidationError("capacity strategy identifier must be text")
    strategy_id = value
    if not 1 <= len(strategy_id) <= MAX_STRATEGY_ID_LENGTH:
        raise CapacityValidationError(
            "capacity strategy identifier length is outside the supported range"
        )
    if _STRATEGY_ID_PATTERN.fullmatch(strategy_id) is None:
        raise CapacityValidationError(
            "capacity strategy identifier must use a lowercase strategy name"
        )
    return strategy_id


def _validate_node_id(value: object) -> str:
    node_id = _validate_owner_key(value, "node identifier")
    if node_id in _RESERVED_NODE_IDS:
        raise CapacityValidationError(
            "node identifier must not collide with a scheduled-work level"
        )
    return node_id


def _require_deadline(value: object) -> UtcTimestamp | None:
    if value is None:
        return None
    if type(value) is not UtcTimestamp:
        raise CapacityValidationError("wait deadline must use UtcTimestamp")
    return value


def _require_bounded_deadline(deadline: UtcTimestamp, now: UtcTimestamp) -> None:
    if _elapsed_micros(deadline, now) > MAX_WAIT_MICROSECONDS:
        raise CapacityValidationError("wait deadline exceeds the maximum bounded wait")


def _elapsed_micros(later: UtcTimestamp, earlier: UtcTimestamp) -> int:
    return (later.to_datetime() - earlier.to_datetime()) // timedelta(microseconds=1)


def _require_scheduled_category(value: object) -> str:
    if type(value) is not str or value not in _SCHEDULED_CATEGORY_SET:
        raise CapacityValidationError("category must name a scheduled-work level")
    return value


def _require_known_category(value: object) -> str:
    if type(value) is not str or value not in _ALL_CATEGORY_SET:
        raise CapacityValidationError("category must name a known capacity category")
    return value


def _resolve_level_node(category: str, node_id: object) -> str | None:
    if category == CAPACITY_CATEGORY_NODE:
        if node_id is None:
            raise CapacityValidationError("node-level capacity requires the node identifier")
        return _validate_node_id(node_id)
    if node_id is not None:
        raise CapacityValidationError("global and strategy capacity take no node identifier")
    return None


def _validate_parent_triple(owner: str, parent: object) -> None:
    if type(parent) is not tuple:
        raise CapacityValidationError("scheduled-work parent evidence must be a three-permit tuple")
    entries = cast(tuple[object, ...], parent)
    if len(entries) != 3:
        raise CapacityValidationError(
            "scheduled-work parent evidence must carry exactly three permits"
        )
    for entry, expected in zip(entries, _SCHEDULED_CATEGORIES, strict=True):
        if type(entry) is not CapacityPermit:
            raise CapacityValidationError(
                "scheduled-work parent evidence entries must be capacity permits"
            )
        if entry.category != expected:
            raise CapacityOrderError("parent evidence must be ordered global, strategy, then node")
        if entry.owner != owner:
            raise CapacityOwnershipError(
                "parent evidence permits must belong to the acquiring owner"
            )


class _Ledger:
    """One bounded permit ledger with FIFO waiter fairness."""

    __slots__ = (
        "category",
        "free_slots",
        "held",
        "limit",
        "max_observed_in_use",
        "waiters",
    )

    def __init__(self, category: str, limit: int) -> None:
        self.category = category
        self.limit = limit
        self.held: dict[str, int] = {}
        self.free_slots: set[int] = set(range(1, limit + 1))
        self.waiters: deque[str] = deque()
        self.max_observed_in_use = 0

    def in_use(self) -> int:
        return len(self.held)

    def waiting(self) -> int:
        return len(self.waiters)

    def holds(self, owner: str) -> bool:
        return owner in self.held

    def has_waiter(self, owner: str) -> bool:
        return owner in self.waiters

    def enqueue(self, owner: str) -> None:
        self.waiters.append(owner)

    def dequeue(self, owner: str) -> None:
        if owner in self.waiters:
            self.waiters.remove(owner)

    def may_grant(self, owner: str) -> bool:
        if not self.free_slots:
            return False
        return not (self.waiters and self.waiters[0] != owner)

    def grant(self, owner: str) -> int:
        slot = min(self.free_slots)
        self.free_slots.remove(slot)
        self.held[owner] = slot
        if len(self.held) > self.max_observed_in_use:
            self.max_observed_in_use = len(self.held)
        self.dequeue(owner)
        return slot

    def ungrant(self, owner: str) -> None:
        slot = self.held.pop(owner)
        self.free_slots.add(slot)

    def release(self, owner: str) -> None:
        slot = self.held.pop(owner)
        self.free_slots.add(slot)
        self.dequeue(owner)

    def snapshot(self) -> CapacitySnapshot:
        return CapacitySnapshot(
            category=self.category,
            limit=self.limit,
            in_use=self.in_use(),
            waiting=self.waiting(),
            max_observed_in_use=self.max_observed_in_use,
        )


class ScheduledWorkLimiters:
    """Three-level scheduled-work capacity coordinator for one admission path.

    Permits are acquired global, then strategy, then node, and released in
    reverse. Capacity accounting never exceeds the captured limits; waiting
    is bounded by an injected absolute deadline and observes FIFO fairness
    per level.
    """

    __slots__ = (
        "_clock",
        "_closed",
        "_condition",
        "_global",
        "_node_ids",
        "_nodes",
        "_strategy",
        "_strategy_id",
        "_subordinates",
    )

    def __init__(
        self,
        settings: CapturedConcurrencySettings,
        *,
        strategy_id: str,
        node_ids: tuple[str, ...],
        clock: PolicyClock,
    ) -> None:
        if type(settings) is not CapturedConcurrencySettings:
            raise CapacityValidationError(
                "scheduled-work settings must use CapturedConcurrencySettings"
            )
        selected_strategy = _validate_strategy_id(strategy_id)
        if type(node_ids) is not tuple:
            raise CapacityValidationError("scheduled-work node identifiers must be a tuple")
        entries = cast(tuple[object, ...], node_ids)
        if not 1 <= len(entries) <= MAX_CAPTURED_LIMIT:
            raise CapacityValidationError(
                "scheduled-work node identifier count is outside the supported range"
            )
        nodes: list[str] = []
        seen: set[str] = set()
        for entry in entries:
            node_id = _validate_node_id(entry)
            if node_id in seen:
                raise CapacityValidationError("scheduled-work node identifiers must be unique")
            seen.add(node_id)
            nodes.append(node_id)
        clock_candidate = cast(object, clock)
        if not isinstance(clock_candidate, PolicyClock):
            raise CapacityValidationError(
                "scheduled-work coordinator requires an injected policy clock"
            )
        self._clock = clock
        self._strategy_id = selected_strategy
        self._node_ids = tuple(nodes)
        self._global = _Ledger(CAPACITY_CATEGORY_GLOBAL, settings.global_concurrent_work)
        self._strategy = _Ledger(CAPACITY_CATEGORY_STRATEGY, settings.per_strategy_work)
        self._nodes = {
            node_id: _Ledger(CAPACITY_CATEGORY_NODE, settings.per_node_work) for node_id in nodes
        }
        self._subordinates: dict[str, set[CapacityPermit]] = {}
        self._condition = Condition()
        self._closed = False

    def acquire(
        self,
        owner: str,
        node_id: str,
        *,
        wait_deadline: UtcTimestamp | None = None,
    ) -> tuple[CapacityPermit, CapacityPermit, CapacityPermit]:
        """Acquire the global, strategy, and node permits for one owner.

        The three levels are granted atomically: a saturated level releases
        every level taken in this call before waiting. A ``None`` deadline is
        try-once; a deadline is an absolute injected instant.
        """
        owner_key = _validate_owner_key(owner, "acquisition owner")
        node = _validate_node_id(node_id)
        deadline = _require_deadline(wait_deadline)
        with self._condition:
            node_ledger = self._node_ledger(node)
            if self._closed:
                raise CapacityClosedError("scheduled-work coordinator is closed")
            if self._is_pending_or_holding(owner_key):
                raise CapacityOwnershipError(
                    "owner key already holds or awaits scheduled-work capacity"
                )
            if deadline is not None:
                _require_bounded_deadline(deadline, self._clock.now())
            for ledger in (self._global, self._strategy, node_ledger):
                ledger.enqueue(owner_key)
            try:
                while True:
                    if self._closed:
                        raise CapacityClosedError("scheduled-work coordinator closed while waiting")
                    if deadline is not None and self._clock.now() >= deadline:
                        raise CapacityTimeoutError(
                            "scheduled-work wait deadline elapsed before capacity became available"
                        )
                    if (
                        self._global.may_grant(owner_key)
                        and self._strategy.may_grant(owner_key)
                        and node_ledger.may_grant(owner_key)
                    ):
                        return (
                            self._grant(self._global, owner_key),
                            self._grant(self._strategy, owner_key),
                            self._grant(node_ledger, owner_key),
                        )
                    if deadline is None:
                        raise CapacityTimeoutError(
                            "scheduled-work capacity is saturated and no wait deadline was given"
                        )
                    remaining = _elapsed_micros(deadline, self._clock.now())
                    self._condition.wait(remaining / 1_000_000.0)
            finally:
                self._global.dequeue(owner_key)
                self._strategy.dequeue(owner_key)
                node_ledger.dequeue(owner_key)

    def release(self, owner: str, node_id: str) -> None:
        """Release the full three-level reservation for one owner exactly once."""
        self._release_reservation(owner, node_id)

    def release_reservation(self, owner: str, node_id: str) -> None:
        """Release a failed admission's reservation; identical to ``release``."""
        self._release_reservation(owner, node_id)

    def acquire_level(
        self,
        category: str,
        owner: str,
        node_id: str | None = None,
    ) -> CapacityPermit:
        """Acquire one scheduled-work level without waiting.

        The stable order is enforced: global first, then strategy, then node.
        Any inversion, skip, or repeat raises ``CapacityOrderError``.
        """
        selected_category = _require_known_category(category)
        if selected_category in _SUBORDINATE_CATEGORIES:
            raise CapacityOwnershipError(
                "subordinate capacity cannot be acquired through the scheduled-work coordinator"
            )
        owner_key = _validate_owner_key(owner, "acquisition owner")
        node = _resolve_level_node(selected_category, node_id)
        with self._condition:
            if self._closed:
                raise CapacityClosedError("scheduled-work coordinator is closed")
            ledger = self._level_ledger(selected_category, node)
            held = self._held_levels(owner_key)
            if selected_category in held:
                raise CapacityOrderError(
                    "each scheduled-work level is acquired at most once per admission"
                )
            expected = set(_SCHEDULED_CATEGORIES[: _SCHEDULED_CATEGORIES.index(selected_category)])
            if held != expected or self._is_pending(owner_key):
                raise CapacityOrderError(
                    "scheduled-work levels must be acquired global, strategy, then node"
                )
            if not ledger.may_grant(owner_key):
                raise CapacityTimeoutError(
                    "scheduled-work level is saturated and incremental acquisition does not wait"
                )
            return self._grant(ledger, owner_key)

    def release_level(
        self,
        category: str,
        owner: str,
        node_id: str | None = None,
    ) -> None:
        """Release one scheduled-work level, enforcing the reverse release order."""
        selected_category = _require_known_category(category)
        if selected_category in _SUBORDINATE_CATEGORIES:
            raise CapacityOwnershipError(
                "subordinate capacity cannot be released through the scheduled-work coordinator"
            )
        owner_key = _validate_owner_key(owner, "release owner")
        node = _resolve_level_node(selected_category, node_id)
        with self._condition:
            ledger = self._level_ledger(selected_category, node)
            held = self._held_levels(owner_key)
            for later in _SCHEDULED_CATEGORIES[
                _SCHEDULED_CATEGORIES.index(selected_category) + 1 :
            ]:
                if later in held:
                    raise CapacityOrderError(
                        "scheduled-work levels must be released node, strategy, then global"
                    )
            if not ledger.holds(owner_key):
                raise CapacityOwnershipError("owner holds no capacity at this scheduled-work level")
            self._require_no_subordinates(owner_key)
            ledger.release(owner_key)
            self._condition.notify_all()

    def parent_holds(self, owner: str) -> bool:
        """Return whether the owner holds the full global, strategy, and node triple."""
        owner_key = _validate_owner_key(owner, "ownership owner")
        with self._condition:
            return self._holds_all(owner_key)

    def register_subordinate(self, owner: str, permit: CapacityPermit) -> None:
        """Record one subordinate permit held under this owner's reservation."""
        owner_key = _validate_owner_key(owner, "subordinate owner")
        if type(permit) is not CapacityPermit:
            raise CapacityValidationError("subordinate registration requires a capacity permit")
        selected = permit
        with self._condition:
            if selected.category not in _SUBORDINATE_CATEGORIES:
                raise CapacityOwnershipError(
                    "subordinate registration accepts only connector or cpu-pool permits"
                )
            if selected.owner != owner_key:
                raise CapacityOwnershipError("subordinate permit must belong to the same owner key")
            if not self._holds_all(owner_key):
                raise CapacityOwnershipError(
                    "subordinate capacity requires a held scheduled-work reservation"
                )
            self._subordinates.setdefault(owner_key, set()).add(selected)

    def unregister_subordinate(self, owner: str, permit: CapacityPermit) -> None:
        """Remove one recorded subordinate permit for this owner exactly once."""
        owner_key = _validate_owner_key(owner, "subordinate owner")
        if type(permit) is not CapacityPermit:
            raise CapacityValidationError("subordinate removal requires a capacity permit")
        selected = permit
        with self._condition:
            registered = self._subordinates.get(owner_key)
            if registered is None or selected not in registered:
                raise CapacityOwnershipError("subordinate permit is not registered for this owner")
            registered.remove(selected)
            if not registered:
                del self._subordinates[owner_key]

    def in_use(self, category_level: str, node_id: str | None = None) -> int:
        """Return the current in-use permit count for one scheduled-work level."""
        selected_category = _require_scheduled_category(category_level)
        node = _resolve_level_node(selected_category, node_id)
        with self._condition:
            return self._level_ledger(selected_category, node).in_use()

    def snapshot(self) -> tuple[CapacitySnapshot, ...]:
        """Return global, strategy, then per-node snapshots in configured order."""
        with self._condition:
            snapshots = [self._global.snapshot(), self._strategy.snapshot()]
            snapshots.extend(self._nodes[node_id].snapshot() for node_id in self._node_ids)
            return tuple(snapshots)

    def max_observed(self) -> dict[str, int]:
        """Return the peak in-use count keyed by level and node identifier."""
        with self._condition:
            observed = {
                CAPACITY_CATEGORY_GLOBAL: self._global.max_observed_in_use,
                CAPACITY_CATEGORY_STRATEGY: self._strategy.max_observed_in_use,
            }
            for node_id in self._node_ids:
                observed[node_id] = self._nodes[node_id].max_observed_in_use
            return observed

    def close(self) -> None:
        """Close the coordinator idempotently, waking every waiter with a typed error."""
        with self._condition:
            if self._closed:
                return
            self._closed = True
            self._condition.notify_all()

    def _release_reservation(self, owner: object, node_id: object) -> None:
        owner_key = _validate_owner_key(owner, "release owner")
        node = _validate_node_id(node_id)
        with self._condition:
            node_ledger = self._node_ledger(node)
            held_any = (
                self._global.holds(owner_key)
                or self._strategy.holds(owner_key)
                or node_ledger.holds(owner_key)
            )
            held_all = (
                self._global.holds(owner_key)
                and self._strategy.holds(owner_key)
                and node_ledger.holds(owner_key)
            )
            if not held_any:
                raise CapacityOwnershipError("owner holds no scheduled-work reservation")
            if not held_all:
                raise CapacityOwnershipError(
                    "release requires the complete global, strategy, and node reservation"
                )
            self._require_no_subordinates(owner_key)
            for ledger in (node_ledger, self._strategy, self._global):
                ledger.release(owner_key)
            self._condition.notify_all()

    def _grant(self, ledger: _Ledger, owner: str) -> CapacityPermit:
        slot = ledger.grant(owner)
        return CapacityPermit(
            category=ledger.category,
            limit=ledger.limit,
            owner=owner,
            slot=slot,
        )

    def _node_ledger(self, node_id: str) -> _Ledger:
        ledger = self._nodes.get(node_id)
        if ledger is None:
            raise CapacityValidationError("node identifier is not registered with this coordinator")
        return ledger

    def _level_ledger(self, category: str, node_id: str | None) -> _Ledger:
        if category == CAPACITY_CATEGORY_NODE:
            return self._node_ledger(_validate_node_id(node_id))
        if category == CAPACITY_CATEGORY_GLOBAL:
            return self._global
        return self._strategy

    def _held_levels(self, owner: str) -> set[str]:
        held: set[str] = set()
        if self._global.holds(owner):
            held.add(CAPACITY_CATEGORY_GLOBAL)
        if self._strategy.holds(owner):
            held.add(CAPACITY_CATEGORY_STRATEGY)
        if self._any_node_holds(owner):
            held.add(CAPACITY_CATEGORY_NODE)
        return held

    def _any_node_holds(self, owner: str) -> bool:
        return any(ledger.holds(owner) for ledger in self._nodes.values())

    def _holds_all(self, owner: str) -> bool:
        return (
            self._global.holds(owner)
            and self._strategy.holds(owner)
            and self._any_node_holds(owner)
        )

    def _is_pending(self, owner: str) -> bool:
        return self._global.has_waiter(owner) or self._strategy.has_waiter(owner)

    def _is_pending_or_holding(self, owner: str) -> bool:
        return (
            self._global.has_waiter(owner)
            or self._global.holds(owner)
            or self._strategy.has_waiter(owner)
            or self._strategy.holds(owner)
        )

    def _require_no_subordinates(self, owner: str) -> None:
        if self._subordinates.get(owner):
            raise CapacityOwnershipError(
                "subordinate permits must be released before scheduled-work capacity"
            )


class SubordinateCallLimiter:
    """Connector or CPU-pool capacity ledger subordinate to scheduled work.

    Subordinate permits can never replace or release a scheduled-work
    permit: every acquisition requires a currently held scheduled-work
    reservation for the same owner key, tracked through the owning
    coordinator.
    """

    __slots__ = (
        "_bucket",
        "_category",
        "_clock",
        "_closed",
        "_condition",
        "_ledger",
        "_parent_limiter",
    )

    def __init__(
        self,
        *,
        category: str,
        limit: int,
        rate: RateLimitPolicy | None = None,
        clock: PolicyClock,
        parent_limiter: ScheduledWorkLimiters,
    ) -> None:
        if type(category) is not str or category not in _SUBORDINATE_CATEGORIES:
            raise CapacityValidationError("subordinate category must be connector or cpu_pool")
        if type(limit) is not int or not MIN_PERMIT_LIMIT <= limit <= MAX_CAPTURED_LIMIT:
            raise CapacityValidationError("subordinate limit is outside the supported range")
        if rate is not None and type(rate) is not RateLimitPolicy:
            raise CapacityValidationError("subordinate rate policy must use RateLimitPolicy")
        clock_candidate = cast(object, clock)
        if not isinstance(clock_candidate, PolicyClock):
            raise CapacityValidationError("subordinate limiter requires an injected policy clock")
        if type(parent_limiter) is not ScheduledWorkLimiters:
            raise CapacityValidationError(
                "subordinate limiter requires the owning scheduled-work coordinator"
            )
        self._category = category
        self._clock = clock
        self._parent_limiter = parent_limiter
        self._ledger = _Ledger(category, limit)
        self._bucket = None if rate is None else TokenBucket(rate)
        self._condition = Condition()
        self._closed = False

    def acquire(
        self,
        owner: str,
        *,
        parent: tuple[CapacityPermit, CapacityPermit, CapacityPermit],
        wait_deadline: UtcTimestamp | None = None,
    ) -> CapacityPermit:
        """Acquire one subordinate permit under a verified parent reservation.

        When the injected token bucket denies at ``now``, the call never
        blocks: it raises ``CapacityRateDeferredError`` carrying the exact
        retry instant, or a plain timeout when no deadline covers it.
        """
        owner_key = _validate_owner_key(owner, "acquisition owner")
        _validate_parent_triple(owner_key, parent)
        deadline = _require_deadline(wait_deadline)
        with self._condition:
            if self._closed:
                raise CapacityClosedError("subordinate capacity limiter is closed")
            if not self._parent_limiter.parent_holds(owner_key):
                raise CapacityOwnershipError(
                    "subordinate capacity requires a held scheduled-work reservation"
                )
            if self._ledger.holds(owner_key) or self._ledger.has_waiter(owner_key):
                raise CapacityOwnershipError(
                    "owner key already holds or awaits subordinate capacity"
                )
            if deadline is not None:
                _require_bounded_deadline(deadline, self._clock.now())
            self._ledger.enqueue(owner_key)
            try:
                while True:
                    if self._closed:
                        raise CapacityClosedError(
                            "subordinate capacity limiter closed while waiting"
                        )
                    if deadline is not None and self._clock.now() >= deadline:
                        raise CapacityTimeoutError(
                            "subordinate wait deadline elapsed before capacity became available"
                        )
                    if self._ledger.may_grant(owner_key):
                        return self._grant_checked(owner_key, deadline)
                    if deadline is None:
                        raise CapacityTimeoutError(
                            "subordinate capacity is saturated and no wait deadline was given"
                        )
                    remaining = _elapsed_micros(deadline, self._clock.now())
                    self._condition.wait(remaining / 1_000_000.0)
            finally:
                self._ledger.dequeue(owner_key)

    def try_acquire(self, owner: str) -> CapacityPermit | None:
        """Try once, without waiting, to acquire one subordinate permit.

        The permit is returned to the caller so every successful acquisition
        has the evidence required for an exact release. Saturation returns
        ``None`` and never leaves a ledger or parent registration behind.
        """
        owner_key = _validate_owner_key(owner, "acquisition owner")
        with self._condition:
            if self._closed:
                raise CapacityClosedError("subordinate capacity limiter is closed")
            if not self._parent_limiter.parent_holds(owner_key):
                raise CapacityOwnershipError(
                    "subordinate capacity requires a held scheduled-work reservation"
                )
            if self._ledger.holds(owner_key) or self._ledger.has_waiter(owner_key):
                raise CapacityOwnershipError(
                    "owner key already holds or awaits subordinate capacity"
                )
            if not self._ledger.may_grant(owner_key):
                return None
            bucket = self._bucket
            if bucket is not None and not bucket.try_acquire(self._clock.now()):
                return None
            permit = self._grant(owner_key)
            self._register_with_parent(owner_key, permit)
            return permit

    def release(self, permit: CapacityPermit) -> None:
        """Release one subordinate permit exactly once."""
        if type(permit) is not CapacityPermit:
            raise CapacityValidationError("subordinate release requires a capacity permit")
        selected = permit
        with self._condition:
            if selected.category != self._category:
                raise CapacityOwnershipError("permit belongs to a different capacity category")
            if self._ledger.held.get(selected.owner) != selected.slot:
                raise CapacityOwnershipError(
                    "permit is not currently held at this subordinate limiter"
                )
            self._parent_limiter.unregister_subordinate(selected.owner, selected)
            self._ledger.release(selected.owner)
            self._condition.notify_all()

    def snapshot(self) -> CapacitySnapshot:
        """Return the bounded observability facts for this subordinate ledger."""
        with self._condition:
            return self._ledger.snapshot()

    def close(self) -> None:
        """Close the limiter idempotently, waking every waiter with a typed error."""
        with self._condition:
            if self._closed:
                return
            self._closed = True
            self._condition.notify_all()

    def _grant(self, owner: str) -> CapacityPermit:
        slot = self._ledger.grant(owner)
        return CapacityPermit(
            category=self._category,
            limit=self._ledger.limit,
            owner=owner,
            slot=slot,
        )

    def _register_with_parent(self, owner: str, permit: CapacityPermit) -> None:
        try:
            self._parent_limiter.register_subordinate(owner, permit)
        except CapacityError:
            self._ledger.ungrant(owner)
            self._condition.notify_all()
            raise

    def _grant_checked(self, owner: str, deadline: UtcTimestamp | None) -> CapacityPermit:
        permit = self._grant(owner)
        bucket = self._bucket
        if bucket is not None:
            now = self._clock.now()
            if not bucket.try_acquire(now):
                self._ledger.ungrant(owner)
                self._condition.notify_all()
                retry_at = bucket.acquire_at(now)
                if deadline is None:
                    raise CapacityTimeoutError(
                        "subordinate rate policy is saturated and no wait deadline was given"
                    )
                if retry_at > deadline:
                    raise CapacityTimeoutError("subordinate rate retry exceeds the wait deadline")
                raise CapacityRateDeferredError(retry_at)
        self._register_with_parent(owner, permit)
        return permit


__all__ = [
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
