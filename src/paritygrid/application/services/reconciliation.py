"""Read-side translation of persisted reconciliation facts for transport."""

import re
from dataclasses import dataclass

from paritygrid.application.ports.execution import RunRecord
from paritygrid.application.ports.operations import OperationalUnitOfWork
from paritygrid.application.ports.reconciliation_persistence import (
    ReconciliationConflictPage,
    ReconciliationSummaryRecord,
    validate_reconciliation_conflict_page_limit,
)
from paritygrid.application.services.errors import (
    OperationalRecordNotFoundError,
    OperationalRequestError,
)
from paritygrid.domain.models import RunId

_MAX_CONFLICT_CURSOR_LENGTH = 64
_CANONICAL_CURSOR_PATTERN = re.compile(r"[A-Z0-9]+(?:-[A-Z0-9]+)*\Z", flags=re.ASCII)


@dataclass(frozen=True, slots=True)
class ReconciliationSnapshot:
    """One coherent view of a run's durable reconciliation summary."""

    run: RunRecord
    summary: ReconciliationSummaryRecord


@dataclass(frozen=True, slots=True)
class ReconciliationConflictView:
    """One coherent page of conflict evidence for a reconciled run."""

    run: RunRecord
    summary: ReconciliationSummaryRecord
    page: ReconciliationConflictPage


class ReconciliationService:
    """Expose persisted reconciliation state without owning reconciliation."""

    def __init__(self, *, unit_of_work: OperationalUnitOfWork) -> None:
        self._unit_of_work = unit_of_work

    def snapshot(self, run_id: str) -> ReconciliationSnapshot:
        """Return the durable reconciliation summary for one run."""
        identity = _run_id(run_id)
        with self._unit_of_work.transaction() as repositories:
            run = repositories.runs.get(identity)
            summary = repositories.reconciliation.get_summary(identity)
        if run is None:
            raise OperationalRecordNotFoundError("run", run_id)
        if summary is None:
            raise OperationalRecordNotFoundError("reconciliation", run_id)
        return ReconciliationSnapshot(run=run, summary=summary)

    def conflicts(
        self, *, run_id: str, limit: int, after: str | None
    ) -> ReconciliationConflictView:
        """Return one bounded keyset page of persisted conflicts."""
        identity = _run_id(run_id)
        page_limit = validate_reconciliation_conflict_page_limit(limit)
        if after is not None and (
            type(after) is not str
            or _CANONICAL_CURSOR_PATTERN.fullmatch(after) is None
            or len(after) > _MAX_CONFLICT_CURSOR_LENGTH
        ):
            raise OperationalRequestError(
                "conflict cursor must be one bounded canonical key", field="cursor"
            )
        with self._unit_of_work.transaction() as repositories:
            run = repositories.runs.get(identity)
            summary = repositories.reconciliation.get_summary(identity)
            page = repositories.reconciliation.list_conflicts(
                identity, after=after, limit=page_limit
            )
        if run is None:
            raise OperationalRecordNotFoundError("run", run_id)
        if summary is None:
            raise OperationalRecordNotFoundError("reconciliation", run_id)
        return ReconciliationConflictView(run=run, summary=summary, page=page)


def _run_id(run_id: str) -> RunId:
    try:
        return RunId.parse(run_id)
    except ValueError as error:
        raise OperationalRequestError(
            "run identity must use the canonical run format",
            field="run_id",
        ) from error


__all__ = [
    "ReconciliationConflictView",
    "ReconciliationService",
    "ReconciliationSnapshot",
]
