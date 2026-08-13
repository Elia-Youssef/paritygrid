"""Dependency-neutral contracts for rebuildable run statistics."""

import hashlib
import json
from dataclasses import dataclass, field
from typing import Protocol, cast

from paritygrid.application.ports.analytics import AnalyticalViewCatalogSnapshot
from paritygrid.application.ports.execution import (
    AttemptOutcome,
    RunNodeRecord,
    RunNodeStatus,
    RunRecord,
    WorkAttemptRecord,
    WorkItemRecord,
)
from paritygrid.domain.execution import RunState, WorkItemState
from paritygrid.domain.models import (
    AttemptNumber,
    Duration,
    NodeId,
    RunId,
    UtcTimestamp,
    WorkItemId,
)

MAX_RUN_STATISTICS_NODES = 10_000
MAX_RUN_STATISTICS_WORK_ITEMS = 1_000_000
MAX_RUN_STATISTICS_ATTEMPTS = 5_000_000
MAX_RUN_STATISTICS_PAGE_SIZE = 100
MAX_RUN_STATISTICS_METRIC = 9_223_372_036_854_775_807


class RunStatisticsError(RuntimeError):
    """Base failure for rebuildable run statistics."""


class RunStatisticsInvalidError(RunStatisticsError):
    """A source snapshot or query request violates the contract."""


class RunStatisticsStateError(RunStatisticsError):
    """The requested source snapshot is not currently installed."""


class RunStatisticsCorruptionError(RunStatisticsError):
    """Disposable analytical state disagrees with the source snapshot."""


class RunStatisticsStorageError(RunStatisticsError):
    """The analytical database rejected a bounded statistics operation."""


@dataclass(frozen=True, slots=True, repr=False)
class RunStatisticsSourceSnapshot:
    """One coherent, immutable projection copied from authoritative SQLite reads."""

    run: RunRecord
    nodes: tuple[RunNodeRecord, ...]
    work_items: tuple[WorkItemRecord, ...]
    attempts: tuple[WorkAttemptRecord, ...]
    source_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        run = _require_exact(self.run, RunRecord, "statistics run")
        nodes = _exact_tuple(self.nodes, RunNodeRecord, "statistics nodes")
        work_items = _exact_tuple(self.work_items, WorkItemRecord, "statistics work items")
        attempts = _exact_tuple(self.attempts, WorkAttemptRecord, "statistics attempts")
        if not nodes or len(nodes) > MAX_RUN_STATISTICS_NODES:
            raise RunStatisticsInvalidError("statistics node count is outside the range")
        if len(work_items) > MAX_RUN_STATISTICS_WORK_ITEMS:
            raise RunStatisticsInvalidError("statistics work-item count exceeds the limit")
        if len(attempts) > MAX_RUN_STATISTICS_ATTEMPTS:
            raise RunStatisticsInvalidError("statistics attempt count exceeds the limit")
        ordered_nodes = tuple(sorted(nodes, key=lambda item: str(item.node_id)))
        ordered_work = tuple(sorted(work_items, key=lambda item: str(item.work_item_id)))
        ordered_attempts = tuple(
            sorted(
                attempts,
                key=lambda item: (str(item.work_item_id), int(item.attempt_number)),
            )
        )
        _require_unique(tuple(str(node.node_id) for node in ordered_nodes), "node")
        _require_unique(tuple(str(work.work_item_id) for work in ordered_work), "work item")
        _require_unique(
            tuple(
                (str(attempt.work_item_id), int(attempt.attempt_number))
                for attempt in ordered_attempts
            ),
            "attempt",
        )
        _validate_relationships(run, ordered_nodes, ordered_work, ordered_attempts)
        object.__setattr__(self, "nodes", ordered_nodes)
        object.__setattr__(self, "work_items", ordered_work)
        object.__setattr__(self, "attempts", ordered_attempts)
        object.__setattr__(
            self,
            "source_sha256",
            _source_sha256(run, ordered_nodes, ordered_work, ordered_attempts),
        )

    def __repr__(self) -> str:
        return (
            "RunStatisticsSourceSnapshot("
            f"run_id={self.run.run_id!r}, run_version={self.run.row_version!r}, "
            f"node_count={len(self.nodes)!r}, work_item_count={len(self.work_items)!r}, "
            f"attempt_count={len(self.attempts)!r}, source_sha256={self.source_sha256!r})"
        )


@dataclass(frozen=True, slots=True)
class RunStatisticsQuerySnapshot:
    """Exact source/version and reviewed views installed for one statistics rebuild."""

    run_id: RunId
    run_version: int
    source_sha256: str
    query_sha256: str
    node_count: int
    work_item_count: int
    attempt_count: int
    view_catalog: AnalyticalViewCatalogSnapshot

    def __post_init__(self) -> None:
        _require_exact(self.run_id, RunId, "statistics snapshot run")
        _positive_int(self.run_version, "statistics run version")
        _sha256(self.source_sha256, "statistics source digest")
        _sha256(self.query_sha256, "statistics query digest")
        _bounded_count(self.node_count, MAX_RUN_STATISTICS_NODES, "statistics node count")
        _bounded_count(
            self.work_item_count,
            MAX_RUN_STATISTICS_WORK_ITEMS,
            "statistics work-item count",
        )
        _bounded_count(
            self.attempt_count,
            MAX_RUN_STATISTICS_ATTEMPTS,
            "statistics attempt count",
        )
        if type(self.view_catalog) is not AnalyticalViewCatalogSnapshot:
            raise TypeError("statistics view catalog is invalid")


@dataclass(frozen=True, slots=True)
class RunStatisticsSummary:
    """One exact run-level analytical metric projection."""

    run_id: RunId
    run_version: int
    state: RunState
    node_count: int
    work_total: int
    work_pending: int
    work_running: int
    work_succeeded: int
    work_quarantined: int
    work_failed: int
    work_cancelled: int
    attempt_count: int
    retry_count: int
    records_read: int
    records_written: int
    records_quarantined: int
    bytes_read: int
    bytes_written: int
    duration_microseconds: int
    attempt_latency_p50_microseconds: int | None
    attempt_latency_p95_microseconds: int | None
    attempt_latency_p99_microseconds: int | None
    started_at: UtcTimestamp | None
    finished_at: UtcTimestamp | None

    def __post_init__(self) -> None:
        _require_exact(self.run_id, RunId, "statistics summary run")
        _positive_int(self.run_version, "statistics summary run version")
        _require_exact(self.state, RunState, "statistics run state")
        for value in (
            self.node_count,
            self.work_total,
            self.work_pending,
            self.work_running,
            self.work_succeeded,
            self.work_quarantined,
            self.work_failed,
            self.work_cancelled,
            self.attempt_count,
            self.retry_count,
            self.records_read,
            self.records_written,
            self.records_quarantined,
            self.bytes_read,
            self.bytes_written,
            self.duration_microseconds,
        ):
            _metric(value)
        _validate_metric_coherence(
            self.work_total,
            self.work_pending,
            self.work_running,
            self.work_succeeded,
            self.work_quarantined,
            self.work_failed,
            self.work_cancelled,
            self.attempt_count,
            self.retry_count,
        )
        for value in (
            self.attempt_latency_p50_microseconds,
            self.attempt_latency_p95_microseconds,
            self.attempt_latency_p99_microseconds,
        ):
            if value is not None:
                _metric(value)
        _optional_timestamp(self.started_at)
        _optional_timestamp(self.finished_at)


@dataclass(frozen=True, slots=True)
class RunNodeStatisticsRecord:
    """One node-level analytical metric projection."""

    node_id: NodeId
    status: RunNodeStatus
    row_version: int
    work_total: int
    work_pending: int
    work_running: int
    work_succeeded: int
    work_quarantined: int
    work_failed: int
    work_cancelled: int
    attempt_count: int
    retry_count: int
    records_read: int
    records_written: int
    records_quarantined: int
    bytes_read: int
    bytes_written: int
    duration_microseconds: int
    attempt_latency_p50_microseconds: int | None
    attempt_latency_p95_microseconds: int | None
    attempt_latency_p99_microseconds: int | None

    def __post_init__(self) -> None:
        _require_exact(self.node_id, NodeId, "node statistics identity")
        _require_exact(self.status, RunNodeStatus, "node statistics status")
        _positive_int(self.row_version, "node statistics row version")
        for value in (
            self.work_total,
            self.work_pending,
            self.work_running,
            self.work_succeeded,
            self.work_quarantined,
            self.work_failed,
            self.work_cancelled,
            self.attempt_count,
            self.retry_count,
            self.records_read,
            self.records_written,
            self.records_quarantined,
            self.bytes_read,
            self.bytes_written,
            self.duration_microseconds,
        ):
            _metric(value)
        _validate_metric_coherence(
            self.work_total,
            self.work_pending,
            self.work_running,
            self.work_succeeded,
            self.work_quarantined,
            self.work_failed,
            self.work_cancelled,
            self.attempt_count,
            self.retry_count,
        )
        for value in (
            self.attempt_latency_p50_microseconds,
            self.attempt_latency_p95_microseconds,
            self.attempt_latency_p99_microseconds,
        ):
            if value is not None:
                _metric(value)


@dataclass(frozen=True, slots=True)
class RunNodeStatisticsPage:
    """One bounded node page tied to the exact run statistics snapshot."""

    snapshot: RunStatisticsQuerySnapshot
    items: tuple[RunNodeStatisticsRecord, ...]
    next_cursor: NodeId | None

    def __post_init__(self) -> None:
        _require_exact(self.snapshot, RunStatisticsQuerySnapshot, "node statistics snapshot")
        items = _exact_tuple(self.items, RunNodeStatisticsRecord, "node statistics page items")
        if len(items) > MAX_RUN_STATISTICS_PAGE_SIZE:
            raise RunStatisticsInvalidError("node statistics page exceeds the limit")
        identities = tuple(str(item.node_id) for item in items)
        if identities != tuple(sorted(identities)) or len(set(identities)) != len(identities):
            raise RunStatisticsInvalidError("node statistics page must use unique identity order")
        if self.next_cursor is not None:
            _require_exact(self.next_cursor, NodeId, "node statistics cursor")
            if not items or self.next_cursor != items[-1].node_id:
                raise RunStatisticsInvalidError(
                    "node statistics cursor must identify the final item"
                )


class RunStatisticsQueryEngine(Protocol):
    """Rebuild and query disposable statistics from one coherent SQLite snapshot."""

    def rebuild(self, source: RunStatisticsSourceSnapshot) -> RunStatisticsQuerySnapshot:
        """Replace disposable state using the supplied authoritative projection."""
        ...

    def get_summary(self, snapshot: RunStatisticsQuerySnapshot) -> RunStatisticsSummary:
        """Return the exact run-level metric projection."""
        ...

    def list_nodes(
        self,
        snapshot: RunStatisticsQuerySnapshot,
        *,
        limit: int,
        after: NodeId | None = None,
    ) -> RunNodeStatisticsPage:
        """Return one deterministic node-statistics keyset page."""
        ...


def validate_run_statistics_page_limit(value: object) -> int:
    """Validate one exact bounded node-statistics page size."""
    if type(value) is not int or not 1 <= value <= MAX_RUN_STATISTICS_PAGE_SIZE:
        raise RunStatisticsInvalidError("node statistics page limit is outside the range")
    return value


def _validate_relationships(
    run: RunRecord,
    nodes: tuple[RunNodeRecord, ...],
    work_items: tuple[WorkItemRecord, ...],
    attempts: tuple[WorkAttemptRecord, ...],
) -> None:
    _require_exact(run.run_id, RunId, "statistics run identity")
    _require_exact(run.state, RunState, "statistics run state")
    _positive_int(run.row_version, "statistics run row version")
    _require_exact(run.created_at, UtcTimestamp, "statistics run creation time")
    _optional_timestamp(run.started_at)
    _optional_timestamp(run.finished_at)
    for node in nodes:
        _require_exact(node.run_id, RunId, "statistics node run identity")
        _require_exact(node.node_id, NodeId, "statistics node identity")
        _require_exact(node.status, RunNodeStatus, "statistics node status")
        _positive_int(node.row_version, "statistics node row version")
        for value in (
            node.work_total,
            node.work_pending,
            node.work_running,
            node.work_succeeded,
            node.work_quarantined,
            node.work_failed,
            node.work_cancelled,
            node.records_read,
            node.records_written,
            node.records_quarantined,
            node.bytes_read,
            node.bytes_written,
            node.retry_count,
        ):
            _metric(value)
        _require_exact(node.duration, Duration, "statistics node duration")
        _optional_timestamp(node.started_at)
        _optional_timestamp(node.finished_at)
    for work in work_items:
        _require_exact(work.work_item_id, WorkItemId, "statistics work identity")
        _require_exact(work.run_id, RunId, "statistics work run identity")
        _require_exact(work.node_id, NodeId, "statistics work node identity")
        _require_exact(work.state, WorkItemState, "statistics work state")
        _bounded_count(
            work.completed_attempt_count,
            2_147_483_647,
            "statistics completed-attempt count",
        )
    for attempt in attempts:
        _require_exact(attempt.work_item_id, WorkItemId, "statistics attempt work identity")
        _require_exact(attempt.attempt_number, AttemptNumber, "statistics attempt number")
        _require_exact(attempt.outcome, AttemptOutcome, "statistics attempt outcome")
        _require_exact(attempt.duration, Duration, "statistics attempt duration")
    node_by_id = {node.node_id: node for node in nodes}
    if any(node.run_id != run.run_id for node in nodes):
        raise RunStatisticsInvalidError("statistics node belongs to another run")
    work_by_id = {work.work_item_id: work for work in work_items}
    if any(work.run_id != run.run_id or work.node_id not in node_by_id for work in work_items):
        raise RunStatisticsInvalidError("statistics work item has an invalid parent")
    attempts_by_work: dict[object, list[WorkAttemptRecord]] = {
        identity: [] for identity in work_by_id
    }
    for attempt in attempts:
        if attempt.work_item_id not in work_by_id:
            raise RunStatisticsInvalidError("statistics attempt has an invalid parent")
        attempts_by_work[attempt.work_item_id].append(attempt)
    for work in work_items:
        children = attempts_by_work[work.work_item_id]
        numbers = tuple(int(item.attempt_number) for item in children)
        if numbers != tuple(range(1, work.completed_attempt_count + 1)):
            raise RunStatisticsInvalidError("statistics attempt history is not contiguous")
    for node in nodes:
        children = tuple(work for work in work_items if work.node_id == node.node_id)
        state_counts = {state: 0 for state in WorkItemState if state is not WorkItemState.LEASED}
        for work in children:
            if work.state is WorkItemState.LEASED:
                raise RunStatisticsInvalidError("statistics work state is transient")
            state_counts[work.state] += 1
        node_attempts = tuple(
            attempt
            for attempt in attempts
            if work_by_id[attempt.work_item_id].node_id == node.node_id
        )
        retry_count = sum(
            attempt.outcome in {AttemptOutcome.RETRY_SCHEDULED, AttemptOutcome.LEASE_EXPIRED}
            for attempt in node_attempts
        )
        aggregate = (
            len(children),
            state_counts[WorkItemState.PENDING] + state_counts[WorkItemState.RETRY_WAIT],
            state_counts[WorkItemState.RUNNING],
            state_counts[WorkItemState.SUCCEEDED],
            state_counts[WorkItemState.QUARANTINED],
            state_counts[WorkItemState.FAILED],
            state_counts[WorkItemState.CANCELLED],
            retry_count,
            sum(attempt.duration.microseconds for attempt in node_attempts),
        )
        stored = (
            node.work_total,
            node.work_pending,
            node.work_running,
            node.work_succeeded,
            node.work_quarantined,
            node.work_failed,
            node.work_cancelled,
            node.retry_count,
            node.duration.microseconds,
        )
        if aggregate != stored:
            raise RunStatisticsInvalidError("statistics run-node aggregate is inconsistent")


def _source_sha256(
    run: RunRecord,
    nodes: tuple[RunNodeRecord, ...],
    work_items: tuple[WorkItemRecord, ...],
    attempts: tuple[WorkAttemptRecord, ...],
) -> str:
    value = {
        "run": {
            "created_at": str(run.created_at),
            "finished_at": None if run.finished_at is None else str(run.finished_at),
            "row_version": run.row_version,
            "run_id": str(run.run_id),
            "started_at": None if run.started_at is None else str(run.started_at),
            "state": run.state.value,
        },
        "nodes": [
            {
                "bytes_read": node.bytes_read,
                "bytes_written": node.bytes_written,
                "duration_microseconds": node.duration.microseconds,
                "node_id": str(node.node_id),
                "records_quarantined": node.records_quarantined,
                "records_read": node.records_read,
                "records_written": node.records_written,
                "retry_count": node.retry_count,
                "row_version": node.row_version,
                "state": node.status.value,
                "work": [
                    node.work_total,
                    node.work_pending,
                    node.work_running,
                    node.work_succeeded,
                    node.work_quarantined,
                    node.work_failed,
                    node.work_cancelled,
                ],
            }
            for node in nodes
        ],
        "work_items": [
            {
                "completed_attempt_count": work.completed_attempt_count,
                "node_id": str(work.node_id),
                "state": work.state.value,
                "work_item_id": str(work.work_item_id),
            }
            for work in work_items
        ],
        "attempts": [
            {
                "attempt_number": int(attempt.attempt_number),
                "duration_microseconds": attempt.duration.microseconds,
                "outcome": attempt.outcome.value,
                "work_item_id": str(attempt.work_item_id),
            }
            for attempt in attempts
        ],
    }
    encoded = json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def _require_exact[T](value: object, expected: type[T], subject: str) -> T:
    if type(value) is not expected:
        raise TypeError(f"{subject} must use {expected.__name__}")
    return cast(T, value)


def _exact_tuple[T](value: object, expected: type[T], subject: str) -> tuple[T, ...]:
    if type(value) is not tuple:
        raise TypeError(f"{subject} must be a tuple")
    items = cast(tuple[object, ...], value)
    if any(type(item) is not expected for item in items):
        raise TypeError(f"{subject} contains an invalid value")
    return cast(tuple[T, ...], items)


def _require_unique(values: tuple[object, ...], subject: str) -> None:
    if len(set(values)) != len(values):
        raise RunStatisticsInvalidError(f"statistics {subject} identity is duplicated")


def _positive_int(value: object, subject: str) -> int:
    if type(value) is not int or not 1 <= value <= 2_147_483_647:
        raise ValueError(f"{subject} is outside the range")
    return value


def _bounded_count(value: object, maximum: int, subject: str) -> int:
    if type(value) is not int or not 0 <= value <= maximum:
        raise ValueError(f"{subject} is outside the range")
    return value


def _metric(value: object) -> int:
    if type(value) is not int or not 0 <= value <= MAX_RUN_STATISTICS_METRIC:
        raise ValueError("run statistics metric is outside the range")
    return value


def _sha256(value: object, subject: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise TypeError(f"{subject} is invalid")
    return value


def _optional_timestamp(value: object) -> None:
    if value is not None:
        _require_exact(value, UtcTimestamp, "statistics timestamp")


def _validate_metric_coherence(
    work_total: int,
    work_pending: int,
    work_running: int,
    work_succeeded: int,
    work_quarantined: int,
    work_failed: int,
    work_cancelled: int,
    attempt_count: int,
    retry_count: int,
) -> None:
    if work_total != sum(
        (
            work_pending,
            work_running,
            work_succeeded,
            work_quarantined,
            work_failed,
            work_cancelled,
        )
    ):
        raise RunStatisticsInvalidError("run statistics work buckets are inconsistent")
    if retry_count > attempt_count:
        raise RunStatisticsInvalidError("run statistics retry count is inconsistent")
