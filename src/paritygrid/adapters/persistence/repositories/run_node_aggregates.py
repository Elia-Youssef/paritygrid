"""SQLAlchemy adapter for run-node aggregate CAS updates."""

from dataclasses import dataclass
from typing import NoReturn

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.schema import Column

from paritygrid.adapters.persistence.repositories.execution_common import (
    positive_int,
    require_exact,
    require_incrementable,
    require_node_id,
    require_run_id,
    require_timestamp,
    translate_execution_storage_errors,
)
from paritygrid.adapters.persistence.repositories.execution_mapping import (
    run_node_from_row,
    stored_nonnegative_int,
)
from paritygrid.adapters.persistence.repositories.runs import SqlAlchemyRunRepository
from paritygrid.adapters.persistence.repositories.work_items import SqlAlchemyWorkItemRepository
from paritygrid.adapters.persistence.schema import run_nodes, work_attempts, work_items
from paritygrid.application.ports.consistency import CheckpointCommit
from paritygrid.application.ports.execution import (
    AttemptOutcome,
    CompletedWork,
    ExecutionCorruptionError,
    ExecutionInvalidRequestError,
    ExecutionRecordNotFoundError,
    ExecutionStaleRowVersionError,
    ExecutionStateConflictError,
    RunNodeRecord,
    RunNodeStatus,
    WorkClaim,
    WorkItemRecord,
)
from paritygrid.application.ports.run_aggregates import (
    MAX_WORK_METRIC,
    RunNodeAggregateRepository,
    WorkMetricDelta,
)
from paritygrid.domain.execution import WorkItemState
from paritygrid.domain.models import Duration, NodeId, RunId, UtcTimestamp, WorkItemId


@dataclass(frozen=True, slots=True)
class _NodeSnapshot:
    total: int
    pending: int
    running: int
    succeeded: int
    quarantined: int
    failed: int
    cancelled: int
    retries: int
    duration_microseconds: int

    def reverse_registration(self) -> _NodeSnapshot:
        return self._replace(total=self.total - 1, pending=self.pending - 1)

    def reverse_claim(self) -> _NodeSnapshot:
        return self._replace(pending=self.pending + 1, running=self.running - 1)

    def reverse_attempt(
        self,
        *,
        outcome: AttemptOutcome,
        duration_microseconds: int,
        state: WorkItemState,
    ) -> _NodeSnapshot:
        values = _bucket_values(self)
        bucket = _bucket_name(state)
        values[bucket] -= 1
        values["running"] += 1
        retry = outcome in {AttemptOutcome.RETRY_SCHEDULED, AttemptOutcome.LEASE_EXPIRED}
        return self._replace(
            **values,
            retries=self.retries - int(retry),
            duration_microseconds=self.duration_microseconds - duration_microseconds,
        )

    def _replace(self, **changes: int) -> _NodeSnapshot:
        values = {
            "total": self.total,
            "pending": self.pending,
            "running": self.running,
            "succeeded": self.succeeded,
            "quarantined": self.quarantined,
            "failed": self.failed,
            "cancelled": self.cancelled,
            "retries": self.retries,
            "duration_microseconds": self.duration_microseconds,
        }
        values.update(changes)
        if any(value < 0 for value in values.values()):
            raise ExecutionCorruptionError("run-node aggregate history underflowed")
        return _NodeSnapshot(**values)


class SqlAlchemyRunNodeAggregateRepository(RunNodeAggregateRepository):
    """Derive and CAS-update one run-node aggregate after work mutations."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @translate_execution_storage_errors
    def register_work(
        self,
        work: WorkItemRecord,
        *,
        expected_node_row_version: int,
    ) -> RunNodeRecord:
        self._require_transaction()
        installed = require_exact(work, WorkItemRecord, "registered work item")
        if installed.state is not WorkItemState.PENDING:
            raise ExecutionInvalidRequestError("registered work item must be pending")
        durable = self._require_work(installed.work_item_id)
        if durable != installed:
            raise ExecutionStateConflictError("registered work item does not match storage")
        node = self._require_node(installed.run_id, installed.node_id, expected_node_row_version)
        current = self._snapshot(installed.run_id, installed.node_id)
        prior = current.reverse_registration()
        self._validate_prior(node, prior)
        if node.status not in {RunNodeStatus.PENDING, RunNodeStatus.RUNNING}:
            raise ExecutionStateConflictError("terminal run node cannot register work")
        return self._update_node(node, current, status=node.status, started_at=node.started_at)

    @translate_execution_storage_errors
    def apply_claim(
        self,
        claim: WorkClaim,
        *,
        expected_node_row_version: int,
    ) -> RunNodeRecord:
        self._require_transaction()
        capability = require_exact(claim, WorkClaim, "work claim")
        durable = self._require_work(capability.work_item_id)
        _validate_claim(durable, capability)
        node = self._require_node(durable.run_id, durable.node_id, expected_node_row_version)
        current = self._snapshot(durable.run_id, durable.node_id)
        prior = current.reverse_claim()
        self._validate_prior(node, prior)
        if node.started_at is not None and capability.started_at < node.started_at:
            raise ExecutionInvalidRequestError("work claim precedes run-node start")
        started = capability.started_at if node.started_at is None else node.started_at
        return self._update_node(
            node,
            current,
            status=RunNodeStatus.RUNNING,
            started_at=started,
        )

    @translate_execution_storage_errors
    def apply_completion(
        self,
        completed: CompletedWork,
        *,
        checkpoint: CheckpointCommit | None,
        expected_node_row_version: int,
        metrics: WorkMetricDelta,
    ) -> RunNodeRecord:
        self._require_transaction()
        result = require_exact(completed, CompletedWork, "completed work")
        delta = require_exact(metrics, WorkMetricDelta, "work metric delta")
        durable = self._require_work(result.work_item.work_item_id)
        _validate_completed_work(durable, result, checkpoint)
        node = self._require_node(durable.run_id, durable.node_id, expected_node_row_version)
        current = self._snapshot(durable.run_id, durable.node_id)
        prior = current.reverse_attempt(
            outcome=result.attempt.outcome,
            duration_microseconds=result.attempt.duration.microseconds,
            state=durable.state,
        )
        self._validate_prior(node, prior)
        if node.status is not RunNodeStatus.RUNNING or node.started_at is None:
            raise ExecutionStateConflictError("run node is not running")
        metrics_values = _advance_metrics(node, delta)
        status = _status_for(current, started=True)
        finished = result.attempt.finished_at if status is not RunNodeStatus.RUNNING else None
        return self._update_node(
            node,
            current,
            status=status,
            started_at=node.started_at,
            finished_at=finished,
            **metrics_values,
        )

    @translate_execution_storage_errors
    def apply_recovery(
        self,
        completed: CompletedWork,
        *,
        expected_node_row_version: int,
    ) -> RunNodeRecord:
        self._require_transaction()
        result = require_exact(completed, CompletedWork, "recovered work")
        if result.attempt.outcome is not AttemptOutcome.LEASE_EXPIRED:
            raise ExecutionInvalidRequestError("recovery requires an expired-lease attempt")
        durable = self._require_work(result.work_item.work_item_id)
        _validate_completed_work(durable, result, None)
        node = self._require_node(durable.run_id, durable.node_id, expected_node_row_version)
        current = self._snapshot(durable.run_id, durable.node_id)
        prior = current.reverse_attempt(
            outcome=result.attempt.outcome,
            duration_microseconds=result.attempt.duration.microseconds,
            state=durable.state,
        )
        self._validate_prior(node, prior)
        if node.status is not RunNodeStatus.RUNNING or node.started_at is None:
            raise ExecutionStateConflictError("run node is not running")
        return self._update_node(
            node,
            current,
            status=RunNodeStatus.RUNNING,
            started_at=node.started_at,
        )

    @translate_execution_storage_errors
    def finalize_empty(
        self,
        run_id: RunId,
        node_id: NodeId,
        *,
        expected_node_row_version: int,
        finalized_at: UtcTimestamp,
    ) -> RunNodeRecord:
        self._require_transaction()
        run_identity = require_run_id(run_id)
        node_identity = require_node_id(node_id)
        timestamp = require_timestamp(finalized_at, "empty run-node finalization time")
        node = self._require_node(run_identity, node_identity, expected_node_row_version)
        snapshot = self._snapshot(run_identity, node_identity)
        self._validate_prior(node, snapshot)
        if snapshot.total != 0 or node.status is not RunNodeStatus.PENDING:
            raise ExecutionStateConflictError("only an empty pending run node can be finalized")
        parent = SqlAlchemyRunRepository(self._session).get(run_identity)
        assert parent is not None
        if timestamp < parent.created_at:
            raise ExecutionInvalidRequestError("run-node finalization cannot precede its run")
        return self._update_node(
            node,
            snapshot,
            status=RunNodeStatus.SUCCEEDED,
            started_at=timestamp,
            finished_at=timestamp,
        )

    def _require_work(self, identity: WorkItemId) -> WorkItemRecord:
        work = SqlAlchemyWorkItemRepository(self._session).get(identity)
        if work is None:
            raise ExecutionRecordNotFoundError("work item does not exist")
        return work

    def _require_node(
        self, run_id: RunId, node_id: NodeId, expected_row_version: int
    ) -> RunNodeRecord:
        expected = positive_int(expected_row_version, "expected run-node row version")
        require_incrementable(expected, "run-node row version")
        node = SqlAlchemyRunRepository(self._session).get_node(run_id, node_id)
        if node is None:
            raise ExecutionRecordNotFoundError("run node does not exist")
        if node.row_version != expected:
            raise ExecutionStaleRowVersionError("run-node row version is stale")
        return node

    def _snapshot(self, run_id: RunId, node_id: NodeId) -> _NodeSnapshot:
        state_rows = self._session.execute(
            select(work_items.c.state, func.count())
            .where(
                work_items.c.run_id == str(run_id),
                work_items.c.node_id == str(node_id),
            )
            .group_by(work_items.c.state)
        ).all()
        counts = {state: 0 for state in WorkItemState if state is not WorkItemState.LEASED}
        for raw_state, raw_count in state_rows:
            try:
                state = WorkItemState(raw_state)
            except (TypeError, ValueError) as error:
                raise ExecutionCorruptionError("run-node work state is corrupt") from error
            if state is WorkItemState.LEASED or state not in counts:
                raise ExecutionCorruptionError("run-node work state is corrupt")
            counts[state] = stored_nonnegative_int(raw_count, "run-node work count")
        total = sum(counts.values())
        if total > MAX_WORK_METRIC:
            raise ExecutionCorruptionError("run-node work total is corrupt")
        attempt_rows = self._session.execute(
            select(
                work_attempts.c.outcome,
                func.count(),
                func.sum(work_attempts.c.duration_microseconds),
            )
            .join(work_items, work_items.c.work_item_id == work_attempts.c.work_item_id)
            .where(
                work_items.c.run_id == str(run_id),
                work_items.c.node_id == str(node_id),
            )
            .group_by(work_attempts.c.outcome)
        ).all()
        retries = 0
        duration = 0
        for raw_outcome, raw_count, raw_duration in attempt_rows:
            try:
                outcome = AttemptOutcome(raw_outcome)
            except (TypeError, ValueError) as error:
                raise ExecutionCorruptionError("run-node attempt outcome is corrupt") from error
            count = stored_nonnegative_int(raw_count, "run-node attempt count")
            outcome_duration = stored_nonnegative_int(raw_duration, "run-node attempt duration")
            if outcome in {AttemptOutcome.RETRY_SCHEDULED, AttemptOutcome.LEASE_EXPIRED}:
                retries += count
            duration += outcome_duration
        if retries > MAX_WORK_METRIC or duration > Duration.MAX_MICROSECONDS:
            raise ExecutionCorruptionError("run-node attempt aggregate is corrupt")
        return _NodeSnapshot(
            total=total,
            pending=counts[WorkItemState.PENDING] + counts[WorkItemState.RETRY_WAIT],
            running=counts[WorkItemState.RUNNING],
            succeeded=counts[WorkItemState.SUCCEEDED],
            quarantined=counts[WorkItemState.QUARANTINED],
            failed=counts[WorkItemState.FAILED],
            cancelled=counts[WorkItemState.CANCELLED],
            retries=retries,
            duration_microseconds=duration,
        )

    @staticmethod
    def _validate_prior(node: RunNodeRecord, snapshot: _NodeSnapshot) -> None:
        expected_status = _status_for(snapshot, started=node.started_at is not None)
        stored = (
            node.status,
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
        derived = (
            expected_status,
            snapshot.total,
            snapshot.pending,
            snapshot.running,
            snapshot.succeeded,
            snapshot.quarantined,
            snapshot.failed,
            snapshot.cancelled,
            snapshot.retries,
            snapshot.duration_microseconds,
        )
        if stored != derived:
            raise ExecutionCorruptionError("run-node aggregate drift was detected")

    def _update_node(
        self,
        node: RunNodeRecord,
        snapshot: _NodeSnapshot,
        *,
        status: RunNodeStatus,
        started_at: UtcTimestamp | None,
        finished_at: UtcTimestamp | None = None,
        records_read: int | None = None,
        records_written: int | None = None,
        records_quarantined: int | None = None,
        bytes_read: int | None = None,
        bytes_written: int | None = None,
    ) -> RunNodeRecord:
        values = {
            "state": status.value,
            "row_version": node.row_version + 1,
            "work_total": snapshot.total,
            "work_pending": snapshot.pending,
            "work_running": snapshot.running,
            "work_succeeded": snapshot.succeeded,
            "work_quarantined": snapshot.quarantined,
            "work_failed": snapshot.failed,
            "work_cancelled": snapshot.cancelled,
            "records_read": node.records_read if records_read is None else records_read,
            "records_written": node.records_written if records_written is None else records_written,
            "records_quarantined": (
                node.records_quarantined if records_quarantined is None else records_quarantined
            ),
            "bytes_read": node.bytes_read if bytes_read is None else bytes_read,
            "bytes_written": node.bytes_written if bytes_written is None else bytes_written,
            "retry_count": snapshot.retries,
            "duration_microseconds": snapshot.duration_microseconds,
            "started_at": None if started_at is None else str(started_at),
            "finished_at": None if finished_at is None else str(finished_at),
        }
        row = (
            self._session.execute(
                update(run_nodes)
                .where(*_node_matches(node))
                .values(**values)
                .returning(*run_nodes.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            self._raise_node_cas(node)
        updated = run_node_from_row(row)
        if updated.row_version != node.row_version + 1:
            raise ExecutionCorruptionError("run-node update result is corrupt")
        return updated

    def _raise_node_cas(self, prior: RunNodeRecord) -> NoReturn:
        row = (
            self._session.execute(
                select(run_nodes).where(
                    run_nodes.c.run_id == str(prior.run_id),
                    run_nodes.c.node_id == str(prior.node_id),
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise ExecutionRecordNotFoundError("run node does not exist")
        current = run_node_from_row(row)
        if current.row_version != prior.row_version:
            raise ExecutionStaleRowVersionError("run-node row version is stale")
        raise ExecutionCorruptionError("run-node aggregate changed without a revision")

    def _require_transaction(self) -> None:
        if not self._session.in_transaction():
            raise ExecutionInvalidRequestError("repository requires a caller-owned transaction")


def _bucket_name(state: WorkItemState) -> str:
    if state in {WorkItemState.PENDING, WorkItemState.RETRY_WAIT}:
        return "pending"
    if state is WorkItemState.RUNNING:
        return "running"
    if state is WorkItemState.SUCCEEDED:
        return "succeeded"
    if state is WorkItemState.QUARANTINED:
        return "quarantined"
    if state is WorkItemState.FAILED:
        return "failed"
    if state is WorkItemState.CANCELLED:
        return "cancelled"
    raise ExecutionCorruptionError("transient work state reached durable aggregation")


def _bucket_values(snapshot: _NodeSnapshot) -> dict[str, int]:
    return {
        "pending": snapshot.pending,
        "running": snapshot.running,
        "succeeded": snapshot.succeeded,
        "quarantined": snapshot.quarantined,
        "failed": snapshot.failed,
        "cancelled": snapshot.cancelled,
    }


def _status_for(snapshot: _NodeSnapshot, *, started: bool) -> RunNodeStatus:
    if snapshot.total == 0:
        return RunNodeStatus.PENDING
    if snapshot.pending or snapshot.running:
        return RunNodeStatus.RUNNING if started else RunNodeStatus.PENDING
    if snapshot.failed:
        return RunNodeStatus.FAILED
    if snapshot.cancelled == snapshot.total:
        return RunNodeStatus.CANCELLED
    if snapshot.quarantined or snapshot.cancelled:
        return RunNodeStatus.PARTIALLY_SUCCEEDED
    return RunNodeStatus.SUCCEEDED


def _validate_claim(work: WorkItemRecord, claim: WorkClaim) -> None:
    stored = (
        work.work_item_id,
        work.state,
        work.row_version,
        work.lease_owner,
        work.lease_expires_at,
        work.active_attempt_number,
        work.active_attempt_started_at,
        work.active_runner_kind,
        work.active_worker_identity,
    )
    capability = (
        claim.work_item_id,
        WorkItemState.RUNNING,
        claim.row_version,
        claim.lease_owner,
        claim.lease_expires_at,
        claim.attempt_number,
        claim.started_at,
        claim.runner_kind,
        claim.worker_identity,
    )
    if stored != capability:
        raise ExecutionStateConflictError("work claim does not match durable state")


def _validate_completed_work(
    durable: WorkItemRecord,
    completed: CompletedWork,
    checkpoint: CheckpointCommit | None,
) -> None:
    work = completed.work_item
    attempt = completed.attempt
    stored = (
        durable.work_item_id,
        durable.run_id,
        durable.node_id,
        durable.partition_key,
        durable.state,
        durable.completed_attempt_count,
        attempt.work_item_id,
        int(attempt.attempt_number),
    )
    supplied = (
        work.work_item_id,
        work.run_id,
        work.node_id,
        work.partition_key,
        work.state,
        work.completed_attempt_count,
        work.work_item_id,
        work.completed_attempt_count,
    )
    if stored != supplied:
        raise ExecutionStateConflictError("completed work does not match durable state")
    if checkpoint is None:
        if durable != work:
            raise ExecutionStateConflictError("completed work does not match durable state")
        return
    checkpoint_state = (
        checkpoint.work.work_item_id,
        checkpoint.work.run_id,
        checkpoint.work.node_id,
        checkpoint.work.partition_key,
        checkpoint.work.row_version,
        int(checkpoint.work.expected_checkpoint_version),
        checkpoint.head.updated_at,
    )
    expected_state = (
        work.work_item_id,
        work.run_id,
        work.node_id,
        work.partition_key,
        durable.row_version,
        durable.expected_checkpoint_version,
        durable.updated_at,
    )
    advanced = (
        durable.row_version == work.row_version + 1
        and durable.expected_checkpoint_version == work.expected_checkpoint_version + 1
    )
    if checkpoint_state != expected_state or not advanced:
        raise ExecutionStateConflictError("checkpointed work does not match durable state")


def _advance_metrics(node: RunNodeRecord, delta: WorkMetricDelta) -> dict[str, int]:
    values = {
        "records_read": node.records_read + delta.records_read,
        "records_written": node.records_written + delta.records_written,
        "records_quarantined": node.records_quarantined + delta.records_quarantined,
        "bytes_read": node.bytes_read + delta.bytes_read,
        "bytes_written": node.bytes_written + delta.bytes_written,
    }
    if any(value > MAX_WORK_METRIC for value in values.values()):
        raise ExecutionStateConflictError("run-node metric cannot advance beyond storage capacity")
    return values


def _node_matches(node: RunNodeRecord) -> tuple[ColumnElement[bool], ...]:
    return (
        run_nodes.c.run_id == str(node.run_id),
        run_nodes.c.node_id == str(node.node_id),
        run_nodes.c.state == node.status.value,
        run_nodes.c.row_version == node.row_version,
        run_nodes.c.work_total == node.work_total,
        run_nodes.c.work_pending == node.work_pending,
        run_nodes.c.work_running == node.work_running,
        run_nodes.c.work_succeeded == node.work_succeeded,
        run_nodes.c.work_quarantined == node.work_quarantined,
        run_nodes.c.work_failed == node.work_failed,
        run_nodes.c.work_cancelled == node.work_cancelled,
        run_nodes.c.records_read == node.records_read,
        run_nodes.c.records_written == node.records_written,
        run_nodes.c.records_quarantined == node.records_quarantined,
        run_nodes.c.bytes_read == node.bytes_read,
        run_nodes.c.bytes_written == node.bytes_written,
        run_nodes.c.retry_count == node.retry_count,
        run_nodes.c.duration_microseconds == node.duration.microseconds,
        _nullable_equals(run_nodes.c.started_at, node.started_at),
        _nullable_equals(run_nodes.c.finished_at, node.finished_at),
    )


def _nullable_equals(column: Column[str], value: UtcTimestamp | None) -> ColumnElement[bool]:
    if value is None:
        return column.is_(None)
    return column == str(value)


__all__ = ["SqlAlchemyRunNodeAggregateRepository"]
