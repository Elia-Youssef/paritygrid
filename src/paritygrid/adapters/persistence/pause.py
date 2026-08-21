"""Short-transaction SQLite reader for stable pause coordination."""

from paritygrid.adapters.persistence.repositories import SqlAlchemyRunRepository
from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
from paritygrid.application.execution.pause import PauseDurableState
from paritygrid.application.ports.consistency import EventSequence
from paritygrid.application.ports.execution import (
    MAX_EXECUTION_PAGE_SIZE,
    ExecutionRecordNotFoundError,
)
from paritygrid.domain.models import RunId


class SQLitePauseStateReader:
    """Read one run and event-counter frontier without owning database lifecycle."""

    __slots__ = ("_database",)

    def __init__(self, database: SQLiteDatabase) -> None:
        if type(database) is not SQLiteDatabase:
            raise TypeError("pause state reader database must use SQLiteDatabase")
        self._database = database

    def read(self, run_id: RunId, /) -> PauseDurableState:
        """Return one coherent frontier from a caller-independent short transaction."""
        if type(run_id) is not RunId:
            raise TypeError("pause state reader run identity must use RunId")
        with self._database.transaction() as session:
            repository = SqlAlchemyRunRepository(session)
            run = repository.get(run_id)
            counter = repository.get_event_counter(run_id)
            active_work_count = 0
            after = None
            while True:
                page = repository.list_nodes(
                    run_id,
                    limit=MAX_EXECUTION_PAGE_SIZE,
                    after=after,
                )
                active_work_count += sum(node.work_running for node in page.items)
                if page.next_cursor is None:
                    break
                after = page.next_cursor
        if run is None or counter is None:
            raise ExecutionRecordNotFoundError("pause durable run frontier does not exist")
        return PauseDurableState(
            run,
            EventSequence(counter.next_sequence_number),
            counter.row_version,
            active_work_count,
        )


__all__ = ["SQLitePauseStateReader"]
