"""SQLite adapters for concurrent admission and recovery evidence readers."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyRunRepository,
    SqlAlchemyWorkItemRepository,
)
from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
from paritygrid.application.execution.concurrent_engine import (
    AdmissionFacts,
    ConcurrentEngineError,
)
from paritygrid.application.execution.concurrent_recovery import (
    ConcurrentRecoveryEvidence,
    RecoveryWorkEvidence,
)
from paritygrid.application.ports.execution import (
    RunNodePage,
    RunNodeRecord,
    WorkItemPage,
    WorkItemRecord,
)
from paritygrid.domain.models import NodeId, RunId, UtcTimestamp

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class SQLiteAdmissionStateReader:
    """Read current work-item admission evidence in one short transaction."""

    __slots__ = ("_database",)

    def __init__(self, database: SQLiteDatabase) -> None:
        if type(database) is not SQLiteDatabase:
            raise TypeError("admission reader database must use SQLiteDatabase")
        self._database = database

    def read(self, run_id: str, node_id: str, partition_key: str) -> AdmissionFacts:
        """Return the durable admission facts for one work identity."""
        run_identity = RunId(run_id)
        node_identity = NodeId(node_id)
        with self._database.transaction() as session:
            runs = SqlAlchemyRunRepository(session)
            work_repository = SqlAlchemyWorkItemRepository(session)
            run = runs.get(run_identity)
            node = runs.get_node(run_identity, node_identity)
            counter = runs.get_event_counter(run_identity)
            work = _find_work(work_repository, run_identity, node_identity, partition_key)
        if run is None or node is None or counter is None:
            raise ConcurrentEngineError("durable admission frontier no longer exists")
        if work is None:
            raise ConcurrentEngineError("durable work item does not exist for the identity")
        return AdmissionFacts(
            work_item_id=str(work.work_item_id),
            partition_key=str(work.partition_key),
            work_row_version=work.row_version,
            node_row_version=node.row_version,
            run_row_version=run.row_version,
            completed_attempt_count=work.completed_attempt_count,
            next_event_sequence=counter.next_sequence_number,
            event_counter_row_version=counter.row_version,
            state=work.state.value,
        )


class SQLiteConcurrentRecoveryReader:
    """Assemble one coherent durable recovery snapshot for a concurrent run."""

    __slots__ = ("_database",)

    def __init__(self, database: SQLiteDatabase) -> None:
        if type(database) is not SQLiteDatabase:
            raise TypeError("recovery reader database must use SQLiteDatabase")
        self._database = database

    def read(self, run_id: str) -> ConcurrentRecoveryEvidence:
        """Return run, node, work, and event-frontier evidence in one transaction."""
        run_identity = RunId(run_id)
        with self._database.transaction() as session:
            runs = SqlAlchemyRunRepository(session)
            work_repository = SqlAlchemyWorkItemRepository(session)
            run = runs.get(run_identity)
            if run is None:
                raise ConcurrentEngineError("recovery run does not exist")
            nodes = _list_nodes(runs, run_identity)
            counter = runs.get_event_counter(run_identity)
            if counter is None:
                raise ConcurrentEngineError("recovery event frontier does not exist")
            work_items = list(_iter_work(work_repository, run_identity))
        ordered_nodes = tuple(sorted(nodes, key=lambda record: str(record.node_id)))
        ordered_work = tuple(
            sorted(
                (
                    RecoveryWorkEvidence(
                        node_id=str(item.node_id),
                        partition_key=str(item.partition_key),
                        work_item_id=str(item.work_item_id),
                        state=item.state.value,
                        row_version=item.row_version,
                        completed_attempt_count=item.completed_attempt_count,
                        lease_owner=item.lease_owner,
                        lease_expires_at_micros=(
                            _to_micros(item.lease_expires_at)
                            if item.lease_expires_at is not None
                            else None
                        ),
                        retry_available_at_micros=(
                            _to_micros(item.retry_available_at)
                            if item.retry_available_at is not None
                            else None
                        ),
                        active_attempt_number=(
                            int(item.active_attempt_number)
                            if item.active_attempt_number is not None
                            else None
                        ),
                    )
                    for item in work_items
                ),
                key=lambda evidence: (evidence.node_id, evidence.partition_key),
            )
        )
        return ConcurrentRecoveryEvidence(
            run=run,
            nodes=ordered_nodes,
            work=ordered_work,
            next_event_sequence=counter.next_sequence_number,
            event_counter_row_version=counter.row_version,
            in_progress_idempotency=0,
            integrity_issue_count=0,
        )


def _find_work(
    repository: SqlAlchemyWorkItemRepository,
    run_id: RunId,
    node_id: NodeId,
    partition_key: str,
) -> WorkItemRecord | None:
    for item in _iter_work(repository, run_id):
        if item.node_id == node_id and str(item.partition_key) == partition_key:
            return item
    return None


def _iter_work(
    repository: SqlAlchemyWorkItemRepository,
    run_id: RunId,
) -> Iterator[WorkItemRecord]:
    page: WorkItemPage | None = repository.list_for_run(run_id, limit=100)
    while page is not None:
        yield from page.items
        page = (
            repository.list_for_run(run_id, limit=100, after=page.next_cursor)
            if page.next_cursor is not None
            else None
        )


def _list_nodes(runs: SqlAlchemyRunRepository, run_identity: RunId) -> list[RunNodeRecord]:
    page: RunNodePage | None = runs.list_nodes(run_identity, limit=100)
    nodes: list[RunNodeRecord] = []
    while page is not None:
        nodes.extend(page.items)
        page = (
            runs.list_nodes(run_identity, limit=100, after=page.next_cursor)
            if page.next_cursor is not None
            else None
        )
    return nodes


def _to_micros(value: UtcTimestamp) -> int:
    delta = value.to_datetime() - _EPOCH
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


__all__ = [
    "SQLiteAdmissionStateReader",
    "SQLiteConcurrentRecoveryReader",
]
