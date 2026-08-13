"""SQLAlchemy adapter for subordinate run-revision advancement."""

from typing import NoReturn

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence.repositories.execution_common import (
    positive_int,
    require_incrementable,
    require_run_id,
    translate_execution_storage_errors,
)
from paritygrid.adapters.persistence.repositories.execution_mapping import run_from_row
from paritygrid.adapters.persistence.repositories.runs import SqlAlchemyRunRepository
from paritygrid.adapters.persistence.schema import runs
from paritygrid.application.ports.execution import (
    ExecutionInvalidRequestError,
    ExecutionRecordNotFoundError,
    ExecutionStaleRowVersionError,
    ExecutionStateConflictError,
    RunRecord,
)
from paritygrid.application.ports.run_aggregates import RunRevisionRepository
from paritygrid.domain.models import RunId


class SqlAlchemyRunRevisionRepository(RunRevisionRepository):
    """Advance a run revision while preserving lifecycle and timestamps."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @translate_execution_storage_errors
    def advance(self, run_id: RunId, *, expected_row_version: int) -> RunRecord:
        self._require_transaction()
        identity = require_run_id(run_id)
        expected = positive_int(expected_row_version, "expected run row version")
        require_incrementable(expected, "run row version")
        current = SqlAlchemyRunRepository(self._session).get(identity)
        if current is None:
            raise ExecutionRecordNotFoundError("run does not exist")
        if current.row_version != expected:
            raise ExecutionStaleRowVersionError("run row version is stale")
        row = (
            self._session.execute(
                update(runs)
                .where(
                    runs.c.run_id == str(identity),
                    runs.c.row_version == expected,
                    runs.c.state == current.state.value,
                )
                .values(row_version=expected + 1)
                .returning(*runs.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            self._raise_cas(identity, expected, current)
        updated = run_from_row(row)
        if updated != _advanced_record(current):
            raise ExecutionStateConflictError("run revision update returned inconsistent state")
        return updated

    def _raise_cas(self, run_id: RunId, expected: int, prior: RunRecord) -> NoReturn:
        row = (
            self._session.execute(select(runs).where(runs.c.run_id == str(run_id)))
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise ExecutionRecordNotFoundError("run does not exist")
        current = run_from_row(row)
        if current.row_version != expected:
            raise ExecutionStaleRowVersionError("run row version is stale")
        if current.state is not prior.state:
            raise ExecutionStateConflictError("run lifecycle state changed")
        raise ExecutionStateConflictError("run revision update was rejected")

    def _require_transaction(self) -> None:
        if not self._session.in_transaction():
            raise ExecutionInvalidRequestError("repository requires a caller-owned transaction")


def _advanced_record(record: RunRecord) -> RunRecord:
    return RunRecord(
        run_id=record.run_id,
        pipeline_id=record.pipeline_id,
        pipeline_version=record.pipeline_version,
        runner_kind=record.runner_kind,
        runner_configuration=record.runner_configuration,
        state=record.state,
        row_version=record.row_version + 1,
        scenario_seed=record.scenario_seed,
        created_at=record.created_at,
        started_at=record.started_at,
        finished_at=record.finished_at,
        cancellation_requested_at=record.cancellation_requested_at,
        recovery_started_at=record.recovery_started_at,
        recovered_at=record.recovered_at,
        final_reconciliation_fingerprint=record.final_reconciliation_fingerprint,
    )


__all__ = ["SqlAlchemyRunRevisionRepository"]
