"""Durable scheduler frontier and admission coordination for concurrent execution."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from paritygrid.application.execution.runner_contract import (
    ContractLifecycleState,
    ControlGeneration,
)

SCHEDULER_FRONTIER_VERSION = 2
MAX_FRONTIER_WORK_ITEMS = 65_536
# Absolute injected-clock microsecond instants stay finite and validated.
_MAX_FRONTIER_RETRY_ELIGIBILITY_MICROS = 2**53 - 1
MAX_FRONTIER_REASON_LENGTH = 256

_MAX_FRONTIER_IDENTITY_LENGTH = 128
_MAX_FRONTIER_LEASE_FENCE = 2**31 - 1
_FRONTIER_FINGERPRINT_PATTERN: re.Pattern[str] = re.compile(r"[0-9a-f]{64}")
_FRONTIER_CONTROL_STATES: frozenset[ContractLifecycleState] = frozenset(
    {
        ContractLifecycleState.RUNNING,
        ContractLifecycleState.QUIESCING,
        ContractLifecycleState.PAUSED,
        ContractLifecycleState.CANCELLING,
        ContractLifecycleState.RECOVERY_REQUIRED,
    }
)
_FRONTIER_MAPPING_KEYS: frozenset[str] = frozenset(
    {
        "control_generation",
        "control_state",
        "edges",
        "lease_fences",
        "node_order",
        "plan_fingerprint",
        "recovery_required_reason",
        "retry_waits",
        "run_id",
        "version",
        "work_states",
    }
)


class ConcurrentSchedulerError(RuntimeError):
    """Base failure for concurrent scheduler state, admission, and frontiers."""


class ConcurrentSchedulerInvalidStateError(ConcurrentSchedulerError):
    """A concurrent scheduler request or snapshot is malformed or out of bounds."""


class ConcurrentSchedulerTransitionError(ConcurrentSchedulerError):
    """The requested scheduler transition is not valid at the current frontier."""


class SchedulerFrontierVersionError(ConcurrentSchedulerError):
    """A frontier uses an unknown or mismatched schema version."""


class SchedulerFrontierCorruptError(ConcurrentSchedulerError):
    """A frontier carries malformed identity relationships or serialization."""


class ConcurrentSchedulerCapacityError(ConcurrentSchedulerError):
    """A work identity was admitted twice while still counting against capacity."""


class FrontierWorkState(StrEnum):
    """Closed durable state of one scheduled work identity."""

    BLOCKED = "blocked"
    READY = "ready"
    ADMITTED = "admitted"
    AWAITING_COMMIT = "awaiting_commit"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    QUARANTINED = "quarantined"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        """Return whether the state admits no later work transition."""
        return self in {
            FrontierWorkState.SUCCEEDED,
            FrontierWorkState.QUARANTINED,
            FrontierWorkState.FAILED,
            FrontierWorkState.CANCELLED,
        }


_IN_FLIGHT_WORK_STATES: frozenset[FrontierWorkState] = frozenset(
    {FrontierWorkState.ADMITTED, FrontierWorkState.AWAITING_COMMIT}
)


@dataclass(frozen=True, slots=True)
class WorkIdentity:
    """Immutable identity triple of the only scheduled and capacity-counted work unit."""

    run_id: str
    node_id: str
    partition_key: str

    def __post_init__(self) -> None:
        _require_identity_text(self.run_id, "work identity run")
        _require_identity_text(self.node_id, "work identity node")
        _require_identity_text(self.partition_key, "work identity partition key")

    def as_tuple(self) -> tuple[str, str, str]:
        """Return the durable work identity triple as plain text values."""
        return (self.run_id, self.node_id, self.partition_key)

    @staticmethod
    def sort_key(identity: WorkIdentity) -> tuple[str, str, str]:
        """Return the deterministic frontier ordering key for one work identity."""
        return (identity.node_id, identity.partition_key, identity.run_id)


@dataclass(frozen=True, slots=True)
class FrontierNodeAggregate:
    """Immutable per-node work aggregate behind one dependency barrier."""

    node_id: str
    total: int
    succeeded: int
    quarantined: int
    failed: int
    cancelled: int
    in_flight: int
    retry_wait: int

    def __post_init__(self) -> None:
        _require_identity_text(self.node_id, "frontier node aggregate identity")
        for value, subject in (
            (self.total, "frontier node aggregate total"),
            (self.succeeded, "frontier node aggregate succeeded"),
            (self.quarantined, "frontier node aggregate quarantined"),
            (self.failed, "frontier node aggregate failed"),
            (self.cancelled, "frontier node aggregate cancelled"),
            (self.in_flight, "frontier node aggregate in flight"),
            (self.retry_wait, "frontier node aggregate retry wait"),
        ):
            _require_bounded_int(value, 0, MAX_FRONTIER_WORK_ITEMS, subject)
        terminal = self.succeeded + self.quarantined + self.failed + self.cancelled
        if terminal > self.total:
            raise ConcurrentSchedulerInvalidStateError(
                "frontier node aggregate terminal count exceeds the node total"
            )
        if self.in_flight + self.retry_wait + terminal > self.total:
            raise ConcurrentSchedulerInvalidStateError(
                "frontier node aggregate counts exceed the node total"
            )

    @property
    def is_complete(self) -> bool:
        """Return whether every work item of the node reached a terminal state."""
        return self.total > 0 and (
            self.succeeded + self.quarantined + self.failed + self.cancelled == self.total
        )

    @property
    def permits_continuation(self) -> bool:
        """Return whether the node aggregate permits successor work to become ready."""
        return (
            self.total > 0
            and self.succeeded == self.total
            and self.in_flight == 0
            and self.retry_wait == 0
        )

    @property
    def is_blocked_terminal(self) -> bool:
        """Return whether any work item committed a blocking terminal outcome."""
        return self.quarantined + self.failed + self.cancelled > 0


@dataclass(frozen=True, slots=True)
class FrontierRetryWait:
    """Immutable durable retry wait with an injected-clock eligibility time."""

    identity: WorkIdentity
    eligible_at_micros: int
    reason: str

    def __post_init__(self) -> None:
        _require_exact(self.identity, WorkIdentity, "frontier retry wait identity")
        # Retry eligibility shares the absolute injected-clock microsecond
        # frame of ``retry_eligible``, so the bound covers epoch instants.
        _require_bounded_int(
            self.eligible_at_micros,
            0,
            _MAX_FRONTIER_RETRY_ELIGIBILITY_MICROS,
            "frontier retry wait eligibility",
        )
        _require_reason_text(self.reason, "frontier retry wait reason")


@dataclass(frozen=True, slots=True, repr=False)
class SchedulerFrontierV2:
    """Immutable durable concurrency frontier for one execution plan run."""

    version: int
    plan_fingerprint: str
    control_generation: ControlGeneration
    run_id: str
    node_order: tuple[str, ...]
    edges: tuple[tuple[str, str], ...]
    work_states: tuple[tuple[WorkIdentity, FrontierWorkState], ...]
    lease_fences: tuple[tuple[WorkIdentity, int], ...]
    retry_waits: tuple[FrontierRetryWait, ...]
    control_state: ContractLifecycleState
    recovery_required_reason: str | None

    def __post_init__(self) -> None:
        version = cast(object, self.version)
        if type(version) is not int:
            raise TypeError("frontier version must be an integer")
        if version != SCHEDULER_FRONTIER_VERSION:
            raise SchedulerFrontierVersionError("frontier schema version is unsupported")
        _require_frontier_fingerprint(self.plan_fingerprint)
        _require_exact(
            self.control_generation,
            ControlGeneration,
            "frontier control generation",
        )
        _require_frontier_text(self.run_id, "frontier run identity")
        node_order = _require_exact_tuple(self.node_order, str, "frontier node order")
        for node_id in node_order:
            _require_frontier_text(node_id, "frontier node identity")
        if not node_order:
            raise SchedulerFrontierCorruptError("frontier node order must not be empty")
        if len(set(node_order)) != len(node_order):
            raise SchedulerFrontierCorruptError("frontier node order must be unique")
        known_nodes = frozenset(node_order)
        edges = _frontier_edge_pairs(self.edges)
        for source, target in edges:
            if source not in known_nodes or target not in known_nodes:
                raise SchedulerFrontierCorruptError("frontier edge must identify a known node")
            if source == target:
                raise SchedulerFrontierCorruptError("frontier edge cannot be a self dependency")
        if _edges_contain_cycle(node_order, edges):
            raise SchedulerFrontierCorruptError("frontier edges must not contain a directed cycle")
        work_states = _frontier_state_entries(self.work_states)
        if len(work_states) > MAX_FRONTIER_WORK_ITEMS:
            raise SchedulerFrontierCorruptError("frontier exceeds the work item bound")
        fence_entries = _frontier_fence_entries(self.lease_fences)
        retry_waits = _require_exact_tuple(
            self.retry_waits,
            FrontierRetryWait,
            "frontier retry waits",
        )

        states: dict[WorkIdentity, FrontierWorkState] = {}
        for identity, state in work_states:
            if identity.node_id not in known_nodes:
                raise SchedulerFrontierCorruptError(
                    "frontier work item must reference a known node"
                )
            if identity.run_id != self.run_id:
                raise SchedulerFrontierCorruptError(
                    "frontier work item must reference the frontier run"
                )
            states[identity] = state
        if len(states) != len(work_states):
            raise SchedulerFrontierCorruptError("frontier work identities must be unique")
        if [identity for identity, _ in work_states] != sorted(
            states,
            key=WorkIdentity.sort_key,
        ):
            raise SchedulerFrontierCorruptError("frontier work states must be sorted")
        if {identity.node_id for identity in states} != set(node_order):
            raise SchedulerFrontierCorruptError("every frontier node requires work items")

        fences: dict[WorkIdentity, int] = {}
        for identity, fence in fence_entries:
            if identity not in states:
                raise SchedulerFrontierCorruptError(
                    "frontier lease fence must reference known work"
                )
            fences[identity] = fence
        if len(fences) != len(fence_entries):
            raise SchedulerFrontierCorruptError("frontier lease fences must be unique")
        if list(fences) != sorted(fences, key=WorkIdentity.sort_key):
            raise SchedulerFrontierCorruptError("frontier lease fences must be sorted")

        waits: dict[WorkIdentity, FrontierRetryWait] = {wait.identity: wait for wait in retry_waits}
        if len(waits) != len(retry_waits):
            raise SchedulerFrontierCorruptError("frontier retry waits must be unique")
        if list(waits) != sorted(waits, key=WorkIdentity.sort_key):
            raise SchedulerFrontierCorruptError("frontier retry waits must be sorted")

        for identity, state in states.items():
            if state in _IN_FLIGHT_WORK_STATES:
                if identity not in fences:
                    raise SchedulerFrontierCorruptError(
                        "in-flight frontier work must hold a lease fence"
                    )
            elif identity in fences:
                raise SchedulerFrontierCorruptError(
                    "frontier lease fence is held by non-in-flight work"
                )
        retry_identities = {
            identity for identity, state in states.items() if state is FrontierWorkState.RETRY_WAIT
        }
        if retry_identities != set(waits):
            raise SchedulerFrontierCorruptError("frontier retry waits must match retry-wait work")

        _validate_frontier_dependency_barriers(node_order, edges, states)

        _require_exact(
            self.control_state,
            ContractLifecycleState,
            "frontier control state",
        )
        if self.control_state not in _FRONTIER_CONTROL_STATES:
            raise SchedulerFrontierCorruptError("frontier control state is not durable")
        reason = cast(object, self.recovery_required_reason)
        if self.control_state is ContractLifecycleState.RECOVERY_REQUIRED:
            _require_frontier_text(
                reason,
                "frontier recovery reason",
                MAX_FRONTIER_REASON_LENGTH,
            )
        elif reason is not None:
            raise SchedulerFrontierCorruptError(
                "frontier recovery reason requires recovery-required control state"
            )

    @property
    def in_flight_identities(self) -> tuple[WorkIdentity, ...]:
        """Return admitted and awaiting-commit identities in deterministic order."""
        states = dict(self.work_states)
        return tuple(
            identity
            for identity in sorted(states, key=WorkIdentity.sort_key)
            if states[identity] in _IN_FLIGHT_WORK_STATES
        )

    @property
    def ready_identities(self) -> tuple[WorkIdentity, ...]:
        """Return ready identities ordered by plan node position then partition key."""
        position = {node_id: index for index, node_id in enumerate(self.node_order)}
        states = dict(self.work_states)
        return tuple(
            sorted(
                (
                    identity
                    for identity, state in states.items()
                    if state is FrontierWorkState.READY
                ),
                key=lambda identity: (position[identity.node_id], identity.partition_key),
            )
        )

    @property
    def is_recovery_required(self) -> bool:
        """Return whether the frontier holds a recovery-required control state."""
        return self.control_state is ContractLifecycleState.RECOVERY_REQUIRED

    def snapshot(self) -> SchedulerFrontierV2:
        """Return this frontier; the value is already immutable."""
        return self

    def to_mapping(self) -> dict[str, object]:
        """Return a plain serializable mapping of every authoritative field."""
        return {
            "version": self.version,
            "plan_fingerprint": self.plan_fingerprint,
            "control_generation": self.control_generation.value,
            "run_id": self.run_id,
            "node_order": list(self.node_order),
            "edges": [[source, target] for source, target in self.edges],
            "work_states": [
                [identity.run_id, identity.node_id, identity.partition_key, state.value]
                for identity, state in self.work_states
            ],
            "lease_fences": [
                [identity.run_id, identity.node_id, identity.partition_key, fence]
                for identity, fence in self.lease_fences
            ],
            "retry_waits": [
                [
                    wait.identity.run_id,
                    wait.identity.node_id,
                    wait.identity.partition_key,
                    wait.eligible_at_micros,
                    wait.reason,
                ]
                for wait in self.retry_waits
            ],
            "control_state": self.control_state.value,
            "recovery_required_reason": self.recovery_required_reason,
        }

    @classmethod
    def from_mapping(cls, data: object) -> SchedulerFrontierV2:
        """Rebuild one frontier from a plain mapping, failing closed on any defect."""
        if type(data) is not dict:
            raise SchedulerFrontierCorruptError("frontier payload must be a mapping")
        mapping = cast(dict[object, object], data)
        mapping_keys = set(mapping)
        if len(mapping_keys) != len(_FRONTIER_MAPPING_KEYS) or not mapping_keys.issuperset(
            _FRONTIER_MAPPING_KEYS
        ):
            raise SchedulerFrontierCorruptError("frontier payload keys are missing or unknown")
        version = mapping["version"]
        if type(version) is not int:
            raise SchedulerFrontierCorruptError("frontier payload version must be an integer")
        if version != SCHEDULER_FRONTIER_VERSION:
            raise SchedulerFrontierVersionError("frontier schema version is unsupported")
        _require_frontier_text(mapping["plan_fingerprint"], "frontier payload fingerprint")
        fingerprint = cast(str, mapping["plan_fingerprint"])
        if _FRONTIER_FINGERPRINT_PATTERN.fullmatch(fingerprint) is None:
            raise SchedulerFrontierCorruptError(
                "frontier payload fingerprint must use 64 lowercase hexadecimal characters"
            )
        raw_generation = mapping["control_generation"]
        if type(raw_generation) is not int:
            raise SchedulerFrontierCorruptError(
                "frontier payload control generation must be an integer"
            )
        try:
            generation = ControlGeneration(raw_generation)
        except (RuntimeError, TypeError, ValueError) as error:
            raise SchedulerFrontierCorruptError(
                "frontier payload control generation is invalid"
            ) from error
        _require_frontier_text(mapping["run_id"], "frontier payload run identity")
        node_order = _frontier_text_rows(mapping["node_order"], "frontier payload node order")
        if not node_order:
            raise SchedulerFrontierCorruptError("frontier payload node order must not be empty")
        if len(set(node_order)) != len(node_order):
            raise SchedulerFrontierCorruptError("frontier payload node order must be unique")
        edges = _frontier_edge_rows(mapping["edges"], "frontier payload edges")
        work_states = _frontier_work_state_rows(
            mapping["work_states"],
            "frontier payload work states",
        )
        lease_fences = _frontier_lease_fence_rows(
            mapping["lease_fences"],
            "frontier payload lease fences",
        )
        retry_waits = _frontier_retry_wait_rows(
            mapping["retry_waits"],
            "frontier payload retry waits",
        )
        control_state = _frontier_control_state_value(
            mapping["control_state"],
            "frontier payload control state",
        )
        reason = mapping["recovery_required_reason"]
        if reason is not None:
            _require_frontier_text(
                reason,
                "frontier payload recovery reason",
                MAX_FRONTIER_REASON_LENGTH,
            )
        try:
            return cls(
                version=version,
                plan_fingerprint=fingerprint,
                control_generation=generation,
                run_id=cast(str, mapping["run_id"]),
                node_order=node_order,
                edges=edges,
                work_states=work_states,
                lease_fences=lease_fences,
                retry_waits=retry_waits,
                control_state=control_state,
                recovery_required_reason=cast("str | None", reason),
            )
        except SchedulerFrontierCorruptError:
            raise

    def __repr__(self) -> str:
        return (
            "SchedulerFrontierV2("
            f"version={self.version!r}, run_id={self.run_id!r}, "
            f"nodes={len(self.node_order)}, work_items={len(self.work_states)}, "
            f"in_flight={len(self.in_flight_identities)}, "
            f"control_state={self.control_state.value!r})"
        )


class ConcurrentScheduler:
    """Mutable admission coordinator over the durable scheduler frontier."""

    __slots__ = (
        "_control_generation",
        "_control_state",
        "_edges",
        "_fences",
        "_identities_by_node",
        "_node_index",
        "_node_order",
        "_partitions",
        "_plan_fingerprint",
        "_predecessors",
        "_recovery_reason",
        "_retry_waits",
        "_run_id",
        "_states",
        "_successors",
    )

    def __init__(
        self,
        run_id: str,
        plan_fingerprint: str,
        node_order: tuple[str, ...],
        edges: tuple[tuple[str, str], ...],
        partitions_by_node: dict[str, tuple[str, ...]],
        control_generation: ControlGeneration,
    ) -> None:
        _require_identity_text(run_id, "scheduler run identity")
        _require_plan_fingerprint(plan_fingerprint)
        nodes = _require_exact_tuple(node_order, str, "scheduler node order")
        for node_id in nodes:
            _require_identity_text(node_id, "scheduler node identity")
        if not nodes:
            raise ConcurrentSchedulerInvalidStateError("scheduler node order must not be empty")
        if len(set(nodes)) != len(nodes):
            raise ConcurrentSchedulerInvalidStateError("scheduler node identities must be unique")
        known_nodes = frozenset(nodes)
        edge_pairs = _scheduler_edge_pairs(edges)
        for source, target in edge_pairs:
            if source not in known_nodes or target not in known_nodes:
                raise ConcurrentSchedulerInvalidStateError(
                    "scheduler edge must identify a known node"
                )
            if source == target:
                raise ConcurrentSchedulerInvalidStateError(
                    "scheduler edge cannot be a self dependency"
                )
        if len(set(edge_pairs)) != len(edge_pairs):
            raise ConcurrentSchedulerInvalidStateError("scheduler edges must be unique")
        if _edges_contain_cycle(nodes, edge_pairs):
            raise ConcurrentSchedulerInvalidStateError(
                "scheduler edges must not contain a directed cycle"
            )
        _require_exact(partitions_by_node, dict, "scheduler partitions")
        partition_map = cast(dict[object, object], partitions_by_node)
        partition_keys = set(partition_map)
        for key in partition_keys:
            if type(key) is not str:
                raise TypeError("scheduler partition keys must be text")
        if len(partition_keys) != len(known_nodes) or not partition_keys.issuperset(known_nodes):
            raise ConcurrentSchedulerInvalidStateError(
                "scheduler partitions must cover every planned node"
            )
        _require_exact(control_generation, ControlGeneration, "scheduler control generation")

        self._partitions: dict[str, tuple[str, ...]] = {}
        total_work_items = 0
        for node_id in nodes:
            raw_partitions = partition_map[node_id]
            if type(raw_partitions) is not tuple:
                raise TypeError("scheduler node partitions must be a tuple")
            partitions = cast(tuple[object, ...], raw_partitions)
            if not partitions:
                raise ConcurrentSchedulerInvalidStateError(
                    "scheduler node partitions must not be empty"
                )
            for partition in partitions:
                _require_identity_text(partition, "scheduler partition key")
            ordered = tuple(sorted(cast("tuple[str, ...]", partitions)))
            if len(set(ordered)) != len(ordered):
                raise ConcurrentSchedulerInvalidStateError(
                    "scheduler node partitions must be unique"
                )
            self._partitions[node_id] = ordered
            total_work_items += len(ordered)
        if total_work_items > MAX_FRONTIER_WORK_ITEMS:
            raise ConcurrentSchedulerInvalidStateError("scheduler exceeds the work item bound")

        self._run_id = run_id
        self._plan_fingerprint = plan_fingerprint
        self._node_order = nodes
        self._node_index = {node_id: index for index, node_id in enumerate(nodes)}
        self._edges = tuple(sorted(edge_pairs))
        predecessor_sets: dict[str, set[str]] = {node_id: set() for node_id in nodes}
        for source, target in edge_pairs:
            predecessor_sets[target].add(source)
        self._predecessors = {
            node_id: tuple(sorted(predecessor_sets[node_id])) for node_id in nodes
        }
        self._successors = {
            node_id: tuple(
                candidate for candidate in nodes if node_id in predecessor_sets[candidate]
            )
            for node_id in nodes
        }
        self._control_generation = control_generation
        self._control_state = ContractLifecycleState.RUNNING
        self._recovery_reason: str | None = None
        self._states: dict[WorkIdentity, FrontierWorkState] = {}
        self._identities_by_node: dict[str, tuple[WorkIdentity, ...]] = {}
        self._fences: dict[WorkIdentity, int] = {}
        self._retry_waits: dict[WorkIdentity, FrontierRetryWait] = {}
        for node_id in nodes:
            identities = tuple(
                WorkIdentity(run_id=run_id, node_id=node_id, partition_key=partition)
                for partition in self._partitions[node_id]
            )
            self._identities_by_node[node_id] = identities
            initial_state = (
                FrontierWorkState.BLOCKED
                if self._predecessors[node_id]
                else FrontierWorkState.READY
            )
            for identity in identities:
                self._states[identity] = initial_state

    @property
    def frontier(self) -> SchedulerFrontierV2:
        """Return the current immutable scheduler frontier snapshot."""
        return SchedulerFrontierV2(
            version=SCHEDULER_FRONTIER_VERSION,
            plan_fingerprint=self._plan_fingerprint,
            control_generation=self._control_generation,
            run_id=self._run_id,
            node_order=self._node_order,
            edges=self._edges,
            work_states=tuple(
                (identity, self._states[identity])
                for identity in sorted(self._states, key=WorkIdentity.sort_key)
            ),
            lease_fences=tuple(
                (identity, self._fences[identity])
                for identity in sorted(self._fences, key=WorkIdentity.sort_key)
            ),
            retry_waits=tuple(
                self._retry_waits[identity]
                for identity in sorted(self._retry_waits, key=WorkIdentity.sort_key)
            ),
            control_state=self._control_state,
            recovery_required_reason=self._recovery_reason,
        )

    def next_ready(self, limit: int) -> tuple[WorkIdentity, ...]:
        """Return up to the limit of eligible ready identities, or none while not running."""
        if type(limit) is not int:
            raise TypeError("admission limit must be an integer")
        if limit < 1:
            raise ConcurrentSchedulerInvalidStateError("admission limit must be positive")
        if self._control_state is not ContractLifecycleState.RUNNING:
            return ()
        ready = sorted(
            (
                identity
                for identity, state in self._states.items()
                if state is FrontierWorkState.READY
            ),
            key=self._plan_position,
        )
        return tuple(ready[:limit])

    def register_admission(self, identity: WorkIdentity, lease_fence: int) -> None:
        """Admit one ready work identity and record its durable lease fence."""
        if type(identity) is not WorkIdentity:
            raise TypeError("admitted work identity must use WorkIdentity")
        if type(lease_fence) is not int:
            raise TypeError("admitted lease fence must be an integer")
        if not 1 <= lease_fence <= _MAX_FRONTIER_LEASE_FENCE:
            raise ConcurrentSchedulerInvalidStateError(
                "admitted lease fence is outside the supported range"
            )
        if identity not in self._states:
            raise ConcurrentSchedulerInvalidStateError("admitted work identity is unknown")
        state = self._states[identity]
        if state in _IN_FLIGHT_WORK_STATES:
            raise ConcurrentSchedulerCapacityError(
                "work identity is already admitted and still in flight"
            )
        if state is not FrontierWorkState.READY:
            raise ConcurrentSchedulerTransitionError("only ready work can be admitted")
        if self._control_state is not ContractLifecycleState.RUNNING:
            raise ConcurrentSchedulerTransitionError(
                "scheduler control state does not admit new work"
            )
        self._states[identity] = FrontierWorkState.ADMITTED
        self._fences[identity] = lease_fence

    def mark_result_received(self, identity: WorkIdentity) -> None:
        """Move one admitted identity to awaiting-commit without releasing capacity."""
        if type(identity) is not WorkIdentity:
            raise TypeError("received work identity must use WorkIdentity")
        if identity not in self._states:
            raise ConcurrentSchedulerInvalidStateError("received work identity is unknown")
        if self._states[identity] is not FrontierWorkState.ADMITTED:
            raise ConcurrentSchedulerTransitionError("only admitted work can receive a result")
        self._states[identity] = FrontierWorkState.AWAITING_COMMIT

    def commit_result(self, identity: WorkIdentity, outcome: str) -> None:
        """Commit one terminal outcome and release dependencies it completes."""
        if type(identity) is not WorkIdentity:
            raise TypeError("committed work identity must use WorkIdentity")
        if type(outcome) is not str:
            raise TypeError("committed outcome must be text")
        try:
            terminal = FrontierWorkState(outcome)
        except ValueError as error:
            raise ConcurrentSchedulerInvalidStateError("committed outcome is unknown") from error
        if not terminal.is_terminal:
            raise ConcurrentSchedulerInvalidStateError("committed outcome must be terminal")
        if identity not in self._states:
            raise ConcurrentSchedulerInvalidStateError("committed work identity is unknown")
        if self._states[identity] not in _IN_FLIGHT_WORK_STATES:
            raise ConcurrentSchedulerTransitionError("only in-flight work can commit a result")
        self._states[identity] = terminal
        self._fences.pop(identity, None)
        self._release_successors(identity.node_id)

    def schedule_retry(
        self,
        identity: WorkIdentity,
        eligible_at_micros: int,
        reason: str,
    ) -> None:
        """Move one in-flight identity into a durable retry wait, releasing capacity."""
        if type(identity) is not WorkIdentity:
            raise TypeError("retried work identity must use WorkIdentity")
        if type(eligible_at_micros) is not int:
            raise TypeError("retry eligibility must be an integer")
        if eligible_at_micros < 0:
            raise ConcurrentSchedulerInvalidStateError("retry eligibility must not be negative")
        _require_reason_text(reason, "scheduler retry reason")
        if identity not in self._states:
            raise ConcurrentSchedulerInvalidStateError("retried work identity is unknown")
        if self._states[identity] not in _IN_FLIGHT_WORK_STATES:
            raise ConcurrentSchedulerTransitionError("only in-flight work can wait for retry")
        self._states[identity] = FrontierWorkState.RETRY_WAIT
        self._fences.pop(identity, None)
        self._retry_waits[identity] = FrontierRetryWait(
            identity=identity,
            eligible_at_micros=eligible_at_micros,
            reason=reason,
        )

    def retry_eligible(self, now_micros: int) -> tuple[WorkIdentity, ...]:
        """Return retry-wait identities eligible at the injected now, moving them to ready."""
        if type(now_micros) is not int:
            raise TypeError("retry eligibility clock must be an integer")
        if now_micros < 0:
            raise ConcurrentSchedulerInvalidStateError("retry clock must not be negative")
        due = sorted(
            (
                identity
                for identity, wait in self._retry_waits.items()
                if wait.eligible_at_micros <= now_micros
            ),
            key=self._plan_position,
        )
        for identity in due:
            self._states[identity] = FrontierWorkState.READY
            del self._retry_waits[identity]
        return tuple(due)

    def request_pause(self) -> ControlGeneration:
        """Stop admission and return the new pause control generation."""
        self._require_control(ContractLifecycleState.RUNNING, "request a pause")
        generation = self._next_generation()
        self._control_state = ContractLifecycleState.QUIESCING
        self._control_generation = generation
        return generation

    def mark_paused(self) -> None:
        """Complete an acknowledged pause from the quiescing control state."""
        self._require_control(ContractLifecycleState.QUIESCING, "mark paused")
        self._control_state = ContractLifecycleState.PAUSED

    def abort_pause(self) -> ControlGeneration:
        """Abort an unacknowledged quiesce and return to running admission.

        Only the quiescing control state may abort: an acknowledged pause
        (``PAUSED``) requires an explicit resume instead. The control
        generation bumps so results and signals from the aborted quiesce
        stay fenced.
        """

        self._require_control(ContractLifecycleState.QUIESCING, "abort a pause")
        generation = self._next_generation()
        self._control_state = ContractLifecycleState.RUNNING
        self._control_generation = generation
        return generation

    def resume(self) -> ControlGeneration:
        """Resume a paused scheduler and return the new control generation."""
        self._require_control(ContractLifecycleState.PAUSED, "resume")
        generation = self._next_generation()
        self._control_state = ContractLifecycleState.RUNNING
        self._control_generation = generation
        return generation

    def request_cancel(self) -> None:
        """Stop admission permanently from the running control state."""
        self._require_control(ContractLifecycleState.RUNNING, "request cancellation")
        self._control_state = ContractLifecycleState.CANCELLING

    def mark_recovery_required(self, reason: str) -> None:
        """Mark the scheduler recovery-required with a durable reason."""
        if type(reason) is not str:
            raise TypeError("recovery reason must be text")
        _require_reason_text(reason, "scheduler recovery reason")
        if self._control_state not in (
            ContractLifecycleState.RUNNING,
            ContractLifecycleState.QUIESCING,
            ContractLifecycleState.PAUSED,
            ContractLifecycleState.CANCELLING,
        ):
            raise ConcurrentSchedulerTransitionError(
                "scheduler control state cannot require recovery again"
            )
        self._control_state = ContractLifecycleState.RECOVERY_REQUIRED
        self._recovery_reason = reason

    @property
    def is_finished(self) -> bool:
        """Return whether every scheduled work item reached a terminal state."""
        return all(state.is_terminal for state in self._states.values())

    @property
    def failed_node_ids(self) -> tuple[str, ...]:
        """Return nodes with blocked-terminal aggregates in plan order."""
        return tuple(
            node_id
            for node_id in self._node_order
            if self._node_aggregate(node_id).is_blocked_terminal
        )

    def restore(self, frontier: SchedulerFrontierV2) -> ConcurrentScheduler:
        """Rebuild this scheduler's internal state exactly from a durable frontier."""
        if type(frontier) is not SchedulerFrontierV2:
            raise TypeError("restored frontier must use SchedulerFrontierV2")
        if frontier.run_id != self._run_id:
            raise SchedulerFrontierCorruptError(
                "restored frontier run does not match the scheduler"
            )
        if frontier.plan_fingerprint != self._plan_fingerprint:
            raise SchedulerFrontierCorruptError(
                "restored frontier plan fingerprint does not match the scheduler"
            )
        if frontier.node_order != self._node_order:
            raise SchedulerFrontierCorruptError(
                "restored frontier nodes do not match the scheduler"
            )
        if frontier.edges != self._edges:
            raise SchedulerFrontierCorruptError(
                "restored frontier edges do not match the scheduler"
            )
        states = dict(frontier.work_states)
        if set(states) != set(self._states):
            raise SchedulerFrontierCorruptError(
                "restored frontier work does not match the scheduler partitions"
            )
        _validate_frontier_dependency_barriers(frontier.node_order, frontier.edges, states)
        self._states = states
        self._fences = dict(frontier.lease_fences)
        self._retry_waits = {wait.identity: wait for wait in frontier.retry_waits}
        self._control_state = frontier.control_state
        self._control_generation = frontier.control_generation
        self._recovery_reason = frontier.recovery_required_reason
        return self

    def _plan_position(self, identity: WorkIdentity) -> tuple[int, str]:
        return (self._node_index[identity.node_id], identity.partition_key)

    def _next_generation(self) -> ControlGeneration:
        value = self._control_generation.value + 1
        if value > _MAX_FRONTIER_LEASE_FENCE:
            raise ConcurrentSchedulerTransitionError(
                "scheduler control generation space is exhausted"
            )
        return ControlGeneration(value)

    def _require_control(self, expected: ContractLifecycleState, action: str) -> None:
        if self._control_state is not expected:
            raise ConcurrentSchedulerTransitionError(f"scheduler control state cannot {action}")

    def _node_aggregate(self, node_id: str) -> FrontierNodeAggregate:
        succeeded = 0
        quarantined = 0
        failed = 0
        cancelled = 0
        in_flight = 0
        retry_wait = 0
        for identity in self._identities_by_node[node_id]:
            state = self._states[identity]
            if state is FrontierWorkState.SUCCEEDED:
                succeeded += 1
            elif state is FrontierWorkState.QUARANTINED:
                quarantined += 1
            elif state is FrontierWorkState.FAILED:
                failed += 1
            elif state is FrontierWorkState.CANCELLED:
                cancelled += 1
            elif state in _IN_FLIGHT_WORK_STATES:
                in_flight += 1
            elif state is FrontierWorkState.RETRY_WAIT:
                retry_wait += 1
        return FrontierNodeAggregate(
            node_id=node_id,
            total=len(self._identities_by_node[node_id]),
            succeeded=succeeded,
            quarantined=quarantined,
            failed=failed,
            cancelled=cancelled,
            in_flight=in_flight,
            retry_wait=retry_wait,
        )

    def _release_successors(self, node_id: str) -> None:
        for successor in self._successors[node_id]:
            if not all(
                self._node_aggregate(predecessor).permits_continuation
                for predecessor in self._predecessors[successor]
            ):
                continue
            for identity in self._identities_by_node[successor]:
                if self._states[identity] is FrontierWorkState.BLOCKED:
                    self._states[identity] = FrontierWorkState.READY

    def __repr__(self) -> str:
        return (
            "ConcurrentScheduler("
            f"run_id={self._run_id!r}, nodes={len(self._node_order)}, "
            f"work_items={len(self._states)}, in_flight={len(self._fences)}, "
            f"control_state={self._control_state.value!r})"
        )


def _require_exact(value: object, expected: type[object], subject: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{subject} must use {expected.__name__}")


def _require_exact_tuple[T](value: object, item_type: type[T], subject: str) -> tuple[T, ...]:
    if type(value) is not tuple:
        raise TypeError(f"{subject} must be a tuple")
    values = cast(tuple[object, ...], value)
    if any(type(item) is not item_type for item in values):
        raise TypeError(f"{subject} contains an invalid value")
    return cast(tuple[T, ...], values)


def _require_identity_text(value: object, subject: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{subject} must be text")
    text = value
    if not 1 <= len(text) <= _MAX_FRONTIER_IDENTITY_LENGTH:
        raise ConcurrentSchedulerInvalidStateError(
            f"{subject} length is outside the supported range"
        )
    for character in text:
        if not "\x20" <= character <= "\x7e":
            raise ConcurrentSchedulerInvalidStateError(
                f"{subject} must use printable ASCII characters"
            )


def _require_bounded_int(value: object, minimum: int, maximum: int, subject: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    if not minimum <= value <= maximum:
        raise ConcurrentSchedulerInvalidStateError(f"{subject} is outside the supported range")


def _require_reason_text(value: object, subject: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{subject} must be text")
    if not 1 <= len(value) <= MAX_FRONTIER_REASON_LENGTH:
        raise ConcurrentSchedulerInvalidStateError(
            f"{subject} length is outside the supported range"
        )
    for character in value:
        if not "\x20" <= character <= "\x7e":
            raise ConcurrentSchedulerInvalidStateError(
                f"{subject} must use printable ASCII characters"
            )


def _require_plan_fingerprint(value: object) -> None:
    if type(value) is not str:
        raise TypeError("scheduler plan fingerprint must be text")
    if _FRONTIER_FINGERPRINT_PATTERN.fullmatch(value) is None:
        raise ConcurrentSchedulerInvalidStateError(
            "scheduler plan fingerprint must use 64 lowercase hexadecimal characters"
        )


def _require_frontier_text(
    value: object,
    subject: str,
    maximum: int = _MAX_FRONTIER_IDENTITY_LENGTH,
) -> None:
    if type(value) is not str:
        raise SchedulerFrontierCorruptError(f"{subject} must be text")
    text = value
    if not 1 <= len(text) <= maximum:
        raise SchedulerFrontierCorruptError(f"{subject} length is outside the supported range")
    for character in text:
        if not "\x20" <= character <= "\x7e":
            raise SchedulerFrontierCorruptError(f"{subject} must use printable ASCII characters")


def _require_frontier_fingerprint(value: object) -> None:
    if type(value) is not str:
        raise TypeError("frontier plan fingerprint must be text")
    if _FRONTIER_FINGERPRINT_PATTERN.fullmatch(value) is None:
        raise SchedulerFrontierCorruptError(
            "frontier plan fingerprint must use 64 lowercase hexadecimal characters"
        )


def _scheduler_edge_pairs(value: object) -> tuple[tuple[str, str], ...]:
    if type(value) is not tuple:
        raise TypeError("scheduler edges must be a tuple")
    pairs: list[tuple[str, str]] = []
    for entry in cast(tuple[object, ...], value):
        if type(entry) is not tuple:
            raise TypeError("scheduler edges must be source-target pairs")
        pair = cast(tuple[object, ...], entry)
        if len(pair) != 2:
            raise TypeError("scheduler edges must be source-target pairs")
        _require_identity_text(pair[0], "scheduler edge source")
        _require_identity_text(pair[1], "scheduler edge target")
        pairs.append((cast(str, pair[0]), cast(str, pair[1])))
    return tuple(pairs)


def _edges_contain_cycle(
    node_order: tuple[str, ...],
    edges: tuple[tuple[str, str], ...],
) -> bool:
    """Return whether a known-node directed graph has a cycle."""
    successors: dict[str, list[str]] = {node_id: [] for node_id in node_order}
    in_degree: dict[str, int] = dict.fromkeys(node_order, 0)
    for source, target in edges:
        successors[source].append(target)
        in_degree[target] += 1

    ready = [node_id for node_id in node_order if in_degree[node_id] == 0]
    visited = 0
    while ready:
        node_id = ready.pop()
        visited += 1
        for successor in successors[node_id]:
            in_degree[successor] -= 1
            if in_degree[successor] == 0:
                ready.append(successor)
    return visited != len(node_order)


def _validate_frontier_dependency_barriers(
    node_order: tuple[str, ...],
    edges: tuple[tuple[str, str], ...],
    states: dict[WorkIdentity, FrontierWorkState],
) -> None:
    """Reject work states that could not result from the dependency barrier protocol."""
    predecessors: dict[str, list[str]] = {node_id: [] for node_id in node_order}
    node_states: dict[str, list[FrontierWorkState]] = {node_id: [] for node_id in node_order}
    for source, target in edges:
        predecessors[target].append(source)
    for identity, state in states.items():
        node_states[identity.node_id].append(state)

    aggregates = {
        node_id: FrontierNodeAggregate(
            node_id=node_id,
            total=len(node_states[node_id]),
            succeeded=node_states[node_id].count(FrontierWorkState.SUCCEEDED),
            quarantined=node_states[node_id].count(FrontierWorkState.QUARANTINED),
            failed=node_states[node_id].count(FrontierWorkState.FAILED),
            cancelled=node_states[node_id].count(FrontierWorkState.CANCELLED),
            in_flight=sum(state in _IN_FLIGHT_WORK_STATES for state in node_states[node_id]),
            retry_wait=node_states[node_id].count(FrontierWorkState.RETRY_WAIT),
        )
        for node_id in node_order
    }
    for node_id, required_predecessors in predecessors.items():
        if not required_predecessors:
            continue
        continuation_permitted = all(
            aggregates[predecessor].permits_continuation for predecessor in required_predecessors
        )
        for state in node_states[node_id]:
            if continuation_permitted and state is FrontierWorkState.BLOCKED:
                raise SchedulerFrontierCorruptError(
                    "frontier dependency barrier leaves work blocked after all "
                    "predecessors succeeded"
                )
            if not continuation_permitted and state is not FrontierWorkState.BLOCKED:
                raise SchedulerFrontierCorruptError(
                    "frontier work bypasses an unsatisfied dependency barrier"
                )


def _frontier_edge_pairs(value: object) -> tuple[tuple[str, str], ...]:
    if type(value) is not tuple:
        raise TypeError("frontier edges must be a tuple")
    pairs: list[tuple[str, str]] = []
    for entry in cast(tuple[object, ...], value):
        if type(entry) is not tuple:
            raise TypeError("frontier edges must be source-target pairs")
        pair = cast(tuple[object, ...], entry)
        if len(pair) != 2:
            raise TypeError("frontier edges must be source-target pairs")
        _require_frontier_text(pair[0], "frontier edge source")
        _require_frontier_text(pair[1], "frontier edge target")
        pairs.append((cast(str, pair[0]), cast(str, pair[1])))
    if len(set(pairs)) != len(pairs):
        raise SchedulerFrontierCorruptError("frontier edges must be unique")
    if pairs != sorted(pairs):
        raise SchedulerFrontierCorruptError("frontier edges must be sorted")
    return tuple(pairs)


def _frontier_state_entries(
    value: object,
) -> tuple[tuple[WorkIdentity, FrontierWorkState], ...]:
    if type(value) is not tuple:
        raise TypeError("frontier work states must be a tuple")
    entries: list[tuple[WorkIdentity, FrontierWorkState]] = []
    for entry in cast(tuple[object, ...], value):
        if type(entry) is not tuple:
            raise TypeError("frontier work states must be identity-state pairs")
        pair = cast(tuple[object, ...], entry)
        if len(pair) != 2:
            raise TypeError("frontier work states must be identity-state pairs")
        _require_exact(pair[0], WorkIdentity, "frontier work state identity")
        _require_exact(pair[1], FrontierWorkState, "frontier work state")
        entries.append((cast(WorkIdentity, pair[0]), cast(FrontierWorkState, pair[1])))
    return tuple(entries)


def _frontier_fence_entries(value: object) -> tuple[tuple[WorkIdentity, int], ...]:
    if type(value) is not tuple:
        raise TypeError("frontier lease fences must be a tuple")
    entries: list[tuple[WorkIdentity, int]] = []
    for entry in cast(tuple[object, ...], value):
        if type(entry) is not tuple:
            raise TypeError("frontier lease fences must be identity-fence pairs")
        pair = cast(tuple[object, ...], entry)
        if len(pair) != 2:
            raise TypeError("frontier lease fences must be identity-fence pairs")
        _require_exact(pair[0], WorkIdentity, "frontier lease fence identity")
        if type(pair[1]) is not int:
            raise TypeError("frontier lease fence must be an integer")
        if not 1 <= pair[1] <= _MAX_FRONTIER_LEASE_FENCE:
            raise SchedulerFrontierCorruptError(
                "frontier lease fence is outside the supported range"
            )
        entries.append((cast(WorkIdentity, pair[0]), pair[1]))
    return tuple(entries)


def _frontier_text_rows(value: object, subject: str) -> tuple[str, ...]:
    if type(value) is not list:
        raise SchedulerFrontierCorruptError(f"{subject} must be a list")
    for item in cast(list[object], value):
        _require_frontier_text(item, f"{subject} entry")
    return tuple(cast("list[str]", value))


def _frontier_edge_rows(value: object, subject: str) -> tuple[tuple[str, str], ...]:
    if type(value) is not list:
        raise SchedulerFrontierCorruptError(f"{subject} must be a list")
    pairs: list[tuple[str, str]] = []
    for entry in cast(list[object], value):
        if type(entry) is not list:
            raise SchedulerFrontierCorruptError(f"{subject} entries must be source-target pairs")
        parts = cast(list[object], entry)
        if len(parts) != 2:
            raise SchedulerFrontierCorruptError(f"{subject} entries must be source-target pairs")
        _require_frontier_text(parts[0], f"{subject} source")
        _require_frontier_text(parts[1], f"{subject} target")
        pairs.append((cast(str, parts[0]), cast(str, parts[1])))
    return tuple(pairs)


def _frontier_identity_from_row(
    parts: list[object],
    subject: str,
) -> WorkIdentity:
    _require_frontier_text(parts[0], f"{subject} run identity")
    _require_frontier_text(parts[1], f"{subject} node identity")
    _require_frontier_text(parts[2], f"{subject} partition key")
    return WorkIdentity(
        run_id=cast(str, parts[0]),
        node_id=cast(str, parts[1]),
        partition_key=cast(str, parts[2]),
    )


def _frontier_work_state_rows(
    value: object,
    subject: str,
) -> tuple[tuple[WorkIdentity, FrontierWorkState], ...]:
    if type(value) is not list:
        raise SchedulerFrontierCorruptError(f"{subject} must be a list")
    entries: list[tuple[WorkIdentity, FrontierWorkState]] = []
    for entry in cast(list[object], value):
        if type(entry) is not list:
            raise SchedulerFrontierCorruptError(f"{subject} entries must be identity-state rows")
        parts = cast(list[object], entry)
        if len(parts) != 4:
            raise SchedulerFrontierCorruptError(f"{subject} entries must be identity-state rows")
        identity = _frontier_identity_from_row(parts, subject)
        raw_state = parts[3]
        if type(raw_state) is not str:
            raise SchedulerFrontierCorruptError(f"{subject} work state must be text")
        try:
            state = FrontierWorkState(raw_state)
        except ValueError as error:
            raise SchedulerFrontierCorruptError(f"{subject} work state is unknown") from error
        entries.append((identity, state))
    return tuple(entries)


def _frontier_lease_fence_rows(
    value: object,
    subject: str,
) -> tuple[tuple[WorkIdentity, int], ...]:
    if type(value) is not list:
        raise SchedulerFrontierCorruptError(f"{subject} must be a list")
    entries: list[tuple[WorkIdentity, int]] = []
    for entry in cast(list[object], value):
        if type(entry) is not list:
            raise SchedulerFrontierCorruptError(f"{subject} entries must be identity-fence rows")
        parts = cast(list[object], entry)
        if len(parts) != 4:
            raise SchedulerFrontierCorruptError(f"{subject} entries must be identity-fence rows")
        identity = _frontier_identity_from_row(parts, subject)
        fence = parts[3]
        if type(fence) is not int:
            raise SchedulerFrontierCorruptError(f"{subject} lease fence must be an integer")
        if not 1 <= fence <= _MAX_FRONTIER_LEASE_FENCE:
            raise SchedulerFrontierCorruptError(
                f"{subject} lease fence is outside the supported range"
            )
        entries.append((identity, fence))
    return tuple(entries)


def _frontier_retry_wait_rows(
    value: object,
    subject: str,
) -> tuple[FrontierRetryWait, ...]:
    if type(value) is not list:
        raise SchedulerFrontierCorruptError(f"{subject} must be a list")
    entries: list[FrontierRetryWait] = []
    for entry in cast(list[object], value):
        if type(entry) is not list:
            raise SchedulerFrontierCorruptError(f"{subject} entries must be retry-wait rows")
        parts = cast(list[object], entry)
        if len(parts) != 5:
            raise SchedulerFrontierCorruptError(f"{subject} entries must be retry-wait rows")
        identity = _frontier_identity_from_row(parts, subject)
        eligible_at = parts[3]
        if type(eligible_at) is not int:
            raise SchedulerFrontierCorruptError(f"{subject} eligibility must be an integer")
        if not 0 <= eligible_at <= _MAX_FRONTIER_LEASE_FENCE:
            raise SchedulerFrontierCorruptError(
                f"{subject} eligibility is outside the supported range"
            )
        reason = parts[4]
        if type(reason) is not str:
            raise SchedulerFrontierCorruptError(f"{subject} reason must be text")
        if not 1 <= len(reason) <= MAX_FRONTIER_REASON_LENGTH:
            raise SchedulerFrontierCorruptError(f"{subject} reason length is unsupported")
        for character in reason:
            if not "\x20" <= character <= "\x7e":
                raise SchedulerFrontierCorruptError(
                    f"{subject} reason must use printable ASCII characters"
                )
        entries.append(
            FrontierRetryWait(
                identity=identity,
                eligible_at_micros=eligible_at,
                reason=reason,
            )
        )
    return tuple(entries)


def _frontier_control_state_value(value: object, subject: str) -> ContractLifecycleState:
    if type(value) is not str:
        raise SchedulerFrontierCorruptError(f"{subject} must be text")
    try:
        state = ContractLifecycleState(value)
    except ValueError as error:
        raise SchedulerFrontierCorruptError(f"{subject} is unknown") from error
    if state not in _FRONTIER_CONTROL_STATES:
        raise SchedulerFrontierCorruptError(f"{subject} is not a durable frontier state")
    return state


__all__ = [
    "MAX_FRONTIER_REASON_LENGTH",
    "MAX_FRONTIER_WORK_ITEMS",
    "SCHEDULER_FRONTIER_VERSION",
    "ConcurrentScheduler",
    "ConcurrentSchedulerCapacityError",
    "ConcurrentSchedulerError",
    "ConcurrentSchedulerInvalidStateError",
    "ConcurrentSchedulerTransitionError",
    "FrontierNodeAggregate",
    "FrontierRetryWait",
    "FrontierWorkState",
    "SchedulerFrontierCorruptError",
    "SchedulerFrontierV2",
    "SchedulerFrontierVersionError",
    "WorkIdentity",
]
