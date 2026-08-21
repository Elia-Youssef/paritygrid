"""Short-transaction SQLite reader for terminal run finalization."""

from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyCheckpointRepository,
    SqlAlchemyExecutionEventRepository,
    SqlAlchemyRunRepository,
    SqlAlchemyWorkAttemptRepository,
    SqlAlchemyWorkItemRepository,
)
from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
from paritygrid.application.execution.finalization import FinalizationEvidence
from paritygrid.application.ports.consistency import (
    EventSequence,
)
from paritygrid.application.ports.execution import (
    MAX_EXECUTION_PAGE_SIZE,
    ExecutionRecordNotFoundError,
    RunNodeRecord,
    WorkAttemptRecord,
    WorkItemRecord,
)
from paritygrid.domain.models import NodeId, RunId, WorkItemId


class SQLiteFinalizationStateReader:
    """Read one terminal finalization frontier without owning database lifecycle."""

    __slots__ = ("_database",)

    def __init__(self, database: SQLiteDatabase) -> None:
        if type(database) is not SQLiteDatabase:
            raise TypeError("finalization reader database must use SQLiteDatabase")
        self._database = database

    def read(self, run_id: RunId, /) -> FinalizationEvidence:
        """Return one coherent frontier from a caller-independent short transaction."""
        if type(run_id) is not RunId:
            raise TypeError("finalization reader run identity must use RunId")
        with self._database.transaction() as session:
            runs = SqlAlchemyRunRepository(session)
            run = runs.get(run_id)
            counter = runs.get_event_counter(run_id)
            if run is None or counter is None:
                raise ExecutionRecordNotFoundError("finalization run frontier does not exist")
            # One aggregate-validated event read proves the counter and the
            # durable history share one gap-free frontier.
            SqlAlchemyExecutionEventRepository(session).list_after(run_id, after=None, limit=1)
            nodes: list[RunNodeRecord] = []
            node_cursor: NodeId | None = None
            while True:
                page = runs.list_nodes(run_id, limit=MAX_EXECUTION_PAGE_SIZE, after=node_cursor)
                nodes.extend(page.items)
                if page.next_cursor is None:
                    break
                node_cursor = page.next_cursor
            work_items: list[WorkItemRecord] = []
            work_cursor: WorkItemId | None = None
            work_repository = SqlAlchemyWorkItemRepository(session)
            while True:
                page = work_repository.list_for_run(
                    run_id, limit=MAX_EXECUTION_PAGE_SIZE, after=work_cursor
                )
                work_items.extend(page.items)
                if page.next_cursor is None:
                    break
                work_cursor = page.next_cursor
            attempts: list[WorkAttemptRecord] = []
            attempts_repository = SqlAlchemyWorkAttemptRepository(session)
            for work in work_items:
                cursor = None
                while True:
                    page = attempts_repository.list_for_work_item(
                        work.work_item_id, limit=MAX_EXECUTION_PAGE_SIZE, after=cursor
                    )
                    attempts.extend(page.items)
                    if page.next_cursor is None:
                        break
                    cursor = page.next_cursor
            checkpoints_repository = SqlAlchemyCheckpointRepository(session)
            checkpoint_versions: list[tuple[WorkItemId, int]] = []
            for work in work_items:
                head = checkpoints_repository.get_head(run_id, work.node_id, work.partition_key)
                # The work listing above already verified every head exists.
                if head is None:
                    from paritygrid.application.execution.finalization import (
                        FinalizationConflictError,
                    )

                    raise FinalizationConflictError("checkpoint head disappeared mid-read")
                checkpoint_versions.append((work.work_item_id, head.current_version.number))
        return FinalizationEvidence(
            run,
            EventSequence(counter.next_sequence_number),
            counter.row_version,
            tuple(nodes),
            tuple(work_items),
            tuple(attempts),
            tuple(checkpoint_versions),
        )


__all__ = ["SQLiteFinalizationStateReader"]
