"""Short-transaction SQLite frontier reader for concurrent result admission."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyRunRepository,
    SqlAlchemyWorkItemRepository,
)
from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
from paritygrid.application.execution.clock_policy import PolicyClock
from paritygrid.application.execution.result_coordinator import (
    RebasedFrontier,
    ResultStaleRejection,
)
from paritygrid.domain.execution import WorkItemState
from paritygrid.domain.models import NodeId, RunId, UtcTimestamp, WorkItemId

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class SQLiteResultCoordinatorReader:
    """Read coherent run, node, work-claim, and event evidence from SQLite."""

    __slots__ = ("_clock", "_database")

    def __init__(self, database: SQLiteDatabase, clock: PolicyClock) -> None:
        if type(database) is not SQLiteDatabase:
            raise TypeError("result coordinator database must use SQLiteDatabase")
        clock_value = cast(object, clock)
        if not isinstance(clock_value, PolicyClock):
            raise TypeError("result coordinator clock must implement PolicyClock")
        self._database = database
        self._clock = clock_value

    def rebase(
        self,
        run_id: str,
        node_id: str,
        partition_key: str,
        work_item_id: str,
    ) -> RebasedFrontier:
        """Return one durable frontier captured in a single short transaction."""
        run_identity = RunId(run_id)
        node_identity = NodeId(node_id)
        work_identity = WorkItemId(work_item_id)
        observed_at = self._clock.now()
        if type(observed_at) is not UtcTimestamp:
            raise TypeError("result coordinator clock returned an invalid timestamp")
        with self._database.transaction() as session:
            run_repository = SqlAlchemyRunRepository(session)
            work_repository = SqlAlchemyWorkItemRepository(session)
            run = run_repository.get(run_identity)
            node = run_repository.get_node(run_identity, node_identity)
            counter = run_repository.get_event_counter(run_identity)
            work = work_repository.get(work_identity)
        if run is None or node is None or counter is None or work is None:
            raise ResultStaleRejection("durable result frontier no longer exists")
        if (
            work.run_id != run_identity
            or work.node_id != node_identity
            or str(work.partition_key) != partition_key
        ):
            raise ResultStaleRejection("durable result frontier parents are inconsistent")
        attempt = work.active_attempt_number
        owner = work.lease_owner
        expiry = work.lease_expires_at
        if attempt is None or owner is None or expiry is None:
            raise ResultStaleRejection("durable work item has no active lease evidence")
        owning = work.state is WorkItemState.RUNNING and observed_at <= expiry
        return RebasedFrontier(
            run_id=str(run.run_id),
            run_row_version=run.row_version,
            node_id=str(node.node_id),
            node_row_version=node.row_version,
            work_item_id=str(work.work_item_id),
            attempt_number=int(attempt),
            lease_fence=work.row_version,
            lease_owner=owner,
            next_event_sequence=counter.next_sequence_number,
            event_counter_row_version=counter.row_version,
            attempt_state="running" if owning else "expired",
            observed_at_micros=_timestamp_micros(observed_at),
            expires_at_micros=_timestamp_micros(expiry),
            runner_kind=work.active_runner_kind or "sequential",
            worker_identity=work.active_worker_identity or "engine-default",
            started_at_micros=(
                _timestamp_micros(work.active_attempt_started_at)
                if work.active_attempt_started_at is not None
                else 0
            ),
        )


def _timestamp_micros(value: UtcTimestamp) -> int:
    delta = value.value - _EPOCH
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


__all__ = ["SQLiteResultCoordinatorReader"]
