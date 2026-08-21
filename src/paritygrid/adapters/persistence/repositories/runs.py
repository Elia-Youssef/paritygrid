"""SQLAlchemy repository for captured runs and initial execution aggregates."""

from collections.abc import Mapping, Sequence
from typing import NoReturn

from sqlalchemy import insert, select, tuple_, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence.repositories.execution_common import (
    bounded_text,
    encode_execution_document,
    optional_sqlite_int,
    positive_int,
    require_document,
    require_fingerprint,
    require_incrementable,
    require_node_id,
    require_pipeline_id,
    require_pipeline_version,
    require_run_id,
    require_timestamp,
    translate_execution_storage_errors,
    validate_node_ids,
)
from paritygrid.adapters.persistence.repositories.execution_mapping import (
    run_event_counter_from_row,
    run_from_row,
    run_node_from_row,
)
from paritygrid.adapters.persistence.repositories.mapping import (
    pipeline_from_row,
    pipeline_version_from_row,
)
from paritygrid.adapters.persistence.schema import (
    pipeline_versions,
    pipelines,
    run_event_counters,
    run_nodes,
    runs,
)
from paritygrid.application.ports.configuration import (
    ConfigurationDocument,
    ConfigurationRepositoryError,
    PipelineRecord,
    PipelineVersionRecord,
)
from paritygrid.application.ports.execution import (
    ExecutionCorruptionError,
    ExecutionDuplicateError,
    ExecutionInvalidRequestError,
    ExecutionRecordNotFoundError,
    ExecutionStaleRowVersionError,
    ExecutionStateConflictError,
    RunEventCounterRecord,
    RunNodePage,
    RunNodeRecord,
    RunNodeStatus,
    RunPage,
    RunRecord,
    RunRepository,
    validate_execution_page_limit,
)
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    StateFingerprint,
    UtcTimestamp,
)


class SqlAlchemyRunRepository(RunRepository):
    """Persist runs without owning the caller's Session or transaction."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @translate_execution_storage_errors
    def create(
        self,
        *,
        run_id: RunId,
        pipeline_id: PipelineId,
        pipeline_version: PipelineVersion,
        runner_kind: str,
        runner_configuration: ConfigurationDocument,
        scenario_seed: int | None,
        node_ids: Sequence[NodeId],
        created_at: UtcTimestamp,
    ) -> RunRecord:
        self._require_transaction()
        identity = require_run_id(run_id)
        pipeline_identity = require_pipeline_id(pipeline_id)
        version = require_pipeline_version(pipeline_version)
        runner = bounded_text(runner_kind, "runner kind", 32)
        configuration = encode_execution_document(
            require_document(runner_configuration, "runner configuration"),
            "runner configuration",
        )
        seed = optional_sqlite_int(scenario_seed, "scenario seed")
        nodes = validate_node_ids(node_ids)
        timestamp = require_timestamp(created_at, "run creation time")
        parent, published = self._read_pipeline_parent(pipeline_identity, version)
        if parent.archived_at is not None:
            raise ExecutionStateConflictError("archived pipeline cannot create runs")
        if published.published_at > timestamp:
            raise ExecutionInvalidRequestError("run creation cannot precede pipeline publication")
        row = (
            self._session.execute(
                sqlite_insert(runs)
                .values(
                    run_id=str(identity),
                    pipeline_id=str(pipeline_identity),
                    pipeline_version_number=int(version),
                    runner_kind=runner,
                    runner_configuration_json=configuration.text,
                    state=RunState.QUEUED.value,
                    row_version=1,
                    scenario_seed=seed,
                    created_at=str(timestamp),
                    started_at=None,
                    finished_at=None,
                    cancellation_requested_at=None,
                    recovery_started_at=None,
                    recovered_at=None,
                    execution_evidence_fingerprint=None,
                )
                .on_conflict_do_nothing(index_elements=[runs.c.run_id])
                .returning(*runs.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise ExecutionDuplicateError("run already exists")
        self._session.execute(
            insert(run_event_counters).values(
                run_id=str(identity), next_sequence_number=1, row_version=1
            )
        )
        self._session.execute(
            insert(run_nodes),
            [
                {
                    "run_id": str(identity),
                    "node_id": str(node_id),
                    "state": RunNodeStatus.PENDING.value,
                    "row_version": 1,
                }
                for node_id in nodes
            ],
        )
        record = run_from_row(row)
        self._require_counter(record.run_id)
        return record

    @translate_execution_storage_errors
    def get(self, run_id: RunId) -> RunRecord | None:
        self._require_transaction()
        identity = require_run_id(run_id)
        row = (
            self._session.execute(select(runs).where(runs.c.run_id == str(identity)))
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        record = run_from_row(row)
        self._require_counter(record.run_id)
        self._require_pipeline_parents((record,))
        return record

    @translate_execution_storage_errors
    def list(
        self,
        *,
        limit: int,
        after: RunId | None = None,
        state: RunState | None = None,
    ) -> RunPage:
        self._require_transaction()
        page_size = validate_execution_page_limit(limit)
        cursor = None if after is None else require_run_id(after)
        if state is not None and type(state) is not RunState:
            raise ExecutionInvalidRequestError("run state filter must use RunState")
        query = select(runs)
        if cursor is not None:
            query = query.where(runs.c.run_id > str(cursor))
        if state is not None:
            query = query.where(runs.c.state == state.value)
        rows = (
            self._session.execute(query.order_by(runs.c.run_id).limit(page_size + 1))
            .mappings()
            .all()
        )
        records = tuple(run_from_row(row) for row in rows[:page_size])
        self._require_counters(records)
        self._require_pipeline_parents(records)
        next_cursor = records[-1].run_id if len(rows) > page_size else None
        return RunPage(records, next_cursor)

    @translate_execution_storage_errors
    def transition(
        self,
        run_id: RunId,
        *,
        expected_row_version: int,
        target_state: RunState,
        transitioned_at: UtcTimestamp,
        execution_evidence_fingerprint: StateFingerprint | None = None,
        execution_evidence_fingerprint_version: int | None = None,
    ) -> RunRecord:
        self._require_transaction()
        identity = require_run_id(run_id)
        expected = positive_int(expected_row_version, "expected run row version")
        if type(target_state) is not RunState:
            raise ExecutionInvalidRequestError("target state must use RunState")
        timestamp = require_timestamp(transitioned_at, "run transition time")
        current = self._require_run(identity, expected)
        current.state.transition_to(target_state)
        successful = target_state in {RunState.SUCCEEDED, RunState.PARTIALLY_SUCCEEDED}
        if (execution_evidence_fingerprint is None) != (
            execution_evidence_fingerprint_version is None
        ):
            raise ExecutionInvalidRequestError(
                "execution-evidence fingerprint and version must be present together"
            )
        if successful:
            if execution_evidence_fingerprint is None:
                raise ExecutionInvalidRequestError(
                    "successful run transition requires an execution-evidence fingerprint"
                )
            fingerprint = require_fingerprint(execution_evidence_fingerprint)
            if type(execution_evidence_fingerprint_version) is not int or (
                not 1 <= execution_evidence_fingerprint_version <= 2_147_483_647
            ):
                raise ExecutionInvalidRequestError(
                    "execution-evidence fingerprint version is outside the supported range"
                )
        else:
            if execution_evidence_fingerprint is not None:
                raise ExecutionInvalidRequestError(
                    "execution-evidence fingerprint is allowed for successful runs only"
                )
            fingerprint = None
        require_incrementable(expected, "run row version")
        values = self._transition_values(
            current,
            target_state,
            timestamp,
            fingerprint,
            execution_evidence_fingerprint_version,
        )
        row = self._update_run(
            identity,
            expected_row_version=expected,
            expected_state=current.state,
            values=values,
        )
        return run_from_row(row)

    @translate_execution_storage_errors
    def mark_recovery_started(
        self,
        run_id: RunId,
        *,
        expected_row_version: int,
        started_at: UtcTimestamp,
    ) -> RunRecord:
        self._require_transaction()
        identity = require_run_id(run_id)
        expected = positive_int(expected_row_version, "expected run row version")
        timestamp = require_timestamp(started_at, "recovery start time")
        current = self._require_run(identity, expected)
        if current.state.is_terminal:
            raise ExecutionStateConflictError("terminal run cannot begin recovery")
        if current.recovery_started_at is not None or current.recovered_at is not None:
            raise ExecutionStateConflictError("run recovery was already recorded")
        evidence = tuple(
            value
            for value in (current.created_at, current.started_at, current.cancellation_requested_at)
            if value is not None
        )
        if timestamp < max(evidence):
            raise ExecutionInvalidRequestError("recovery time cannot precede run creation")
        require_incrementable(expected, "run row version")
        row = self._update_run(
            identity,
            expected_row_version=expected,
            expected_state=current.state,
            values={"recovery_started_at": str(timestamp), "row_version": expected + 1},
        )
        return run_from_row(row)

    @translate_execution_storage_errors
    def mark_recovered(
        self,
        run_id: RunId,
        *,
        expected_row_version: int,
        recovered_at: UtcTimestamp,
    ) -> RunRecord:
        self._require_transaction()
        identity = require_run_id(run_id)
        expected = positive_int(expected_row_version, "expected run row version")
        timestamp = require_timestamp(recovered_at, "recovery completion time")
        current = self._require_run(identity, expected)
        if current.state.is_terminal:
            raise ExecutionStateConflictError("terminal run cannot complete recovery")
        if current.recovery_started_at is None:
            raise ExecutionStateConflictError("run recovery has not started")
        if current.recovered_at is not None:
            raise ExecutionStateConflictError("run recovery was already completed")
        evidence = tuple(
            value
            for value in (
                current.created_at,
                current.started_at,
                current.cancellation_requested_at,
                current.recovery_started_at,
            )
            if value is not None
        )
        if timestamp < max(evidence):
            raise ExecutionInvalidRequestError("recovery completion cannot precede its start")
        require_incrementable(expected, "run row version")
        row = self._update_run(
            identity,
            expected_row_version=expected,
            expected_state=current.state,
            values={"recovered_at": str(timestamp), "row_version": expected + 1},
        )
        return run_from_row(row)

    @translate_execution_storage_errors
    def get_event_counter(self, run_id: RunId) -> RunEventCounterRecord | None:
        self._require_transaction()
        identity = require_run_id(run_id)
        row = (
            self._session.execute(
                select(run_event_counters).where(run_event_counters.c.run_id == str(identity))
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            parent = self._session.execute(
                select(runs.c.run_id).where(runs.c.run_id == str(identity))
            ).scalar_one_or_none()
            if parent is not None:
                raise ExecutionCorruptionError("run event counter is missing")
            return None
        counter = run_event_counter_from_row(row)
        parent_row = (
            self._session.execute(select(runs).where(runs.c.run_id == str(identity)))
            .mappings()
            .one_or_none()
        )
        if parent_row is None or counter.run_id != identity:
            raise ExecutionCorruptionError("run event counter parent is missing")
        parent = run_from_row(parent_row)
        self._require_pipeline_parents((parent,))
        return counter

    @translate_execution_storage_errors
    def get_node(self, run_id: RunId, node_id: NodeId) -> RunNodeRecord | None:
        self._require_transaction()
        run_identity = require_run_id(run_id)
        node_identity = require_node_id(node_id)
        row = (
            self._session.execute(
                select(run_nodes).where(
                    run_nodes.c.run_id == str(run_identity),
                    run_nodes.c.node_id == str(node_identity),
                )
            )
            .mappings()
            .one_or_none()
        )
        parent = self.get(run_identity)
        if parent is None:
            if row is not None:
                raise ExecutionCorruptionError("run-node parent is missing")
            return None
        return None if row is None else run_node_from_row(row)

    @translate_execution_storage_errors
    def list_nodes(
        self,
        run_id: RunId,
        *,
        limit: int,
        after: NodeId | None = None,
    ) -> RunNodePage:
        self._require_transaction()
        identity = require_run_id(run_id)
        page_size = validate_execution_page_limit(limit)
        cursor = None if after is None else require_node_id(after)
        query = select(run_nodes).where(run_nodes.c.run_id == str(identity))
        if cursor is not None:
            query = query.where(run_nodes.c.node_id > str(cursor))
        rows = (
            self._session.execute(query.order_by(run_nodes.c.node_id).limit(page_size + 1))
            .mappings()
            .all()
        )
        records = tuple(run_node_from_row(row) for row in rows[:page_size])
        parent = self.get(identity)
        if parent is None:
            if records:
                raise ExecutionCorruptionError("run-node parent is missing")
            return RunNodePage((), None)
        next_cursor = records[-1].node_id if len(rows) > page_size else None
        return RunNodePage(records, next_cursor)

    def _transition_values(
        self,
        current: RunRecord,
        target: RunState,
        timestamp: UtcTimestamp,
        fingerprint: StateFingerprint | None,
        fingerprint_version: int | None,
    ) -> dict[str, object]:
        if timestamp < current.created_at:
            raise ExecutionInvalidRequestError("run transition cannot precede creation")
        relevant = tuple(
            value
            for value in (
                current.started_at,
                current.cancellation_requested_at,
                current.recovery_started_at,
                current.recovered_at,
            )
            if value is not None
        )
        if relevant and timestamp < max(relevant):
            raise ExecutionInvalidRequestError("run transition time is not monotonic")
        values: dict[str, object] = {
            "state": target.value,
            "row_version": current.row_version + 1,
        }
        if current.state is RunState.QUEUED and target is RunState.RUNNING:
            values["started_at"] = str(timestamp)
        if target is RunState.CANCELLING:
            values["cancellation_requested_at"] = str(timestamp)
        if current.state is RunState.QUEUED and target is RunState.CANCELLED:
            values["cancellation_requested_at"] = str(timestamp)
        if target.is_terminal:
            values["finished_at"] = str(timestamp)
        if fingerprint is not None:
            values["execution_evidence_fingerprint"] = str(fingerprint)
            values["execution_evidence_fingerprint_version"] = fingerprint_version
        return values

    def _require_run(self, run_id: RunId, expected: int) -> RunRecord:
        current = self.get(run_id)
        if current is None:
            raise ExecutionRecordNotFoundError("run does not exist")
        if current.row_version != expected:
            raise ExecutionStaleRowVersionError("run row version is stale")
        return current

    def _read_pipeline_parent(
        self, pipeline_id: PipelineId, version: PipelineVersion
    ) -> tuple[PipelineRecord, PipelineVersionRecord]:
        pipeline_row = (
            self._session.execute(
                select(pipelines).where(pipelines.c.pipeline_id == str(pipeline_id))
            )
            .mappings()
            .one_or_none()
        )
        version_row = (
            self._session.execute(
                select(pipeline_versions).where(
                    pipeline_versions.c.pipeline_id == str(pipeline_id),
                    pipeline_versions.c.version_number == int(version),
                )
            )
            .mappings()
            .one_or_none()
        )
        if pipeline_row is None or version_row is None:
            raise ExecutionRecordNotFoundError("pipeline version does not exist")
        try:
            parent = pipeline_from_row(pipeline_row)
            published = pipeline_version_from_row(
                version_row, pipeline_created_at=parent.created_at
            )
        except ConfigurationRepositoryError as error:
            raise ExecutionCorruptionError("pipeline version parent is corrupt") from error
        if parent.pipeline_id != pipeline_id or published.pipeline_id != pipeline_id:
            raise ExecutionCorruptionError("pipeline version parent identity is corrupt")
        if published.version != version:
            raise ExecutionCorruptionError("pipeline version parent identity is corrupt")
        return parent, published

    def _require_counter(self, run_id: RunId) -> RunEventCounterRecord:
        row = (
            self._session.execute(
                select(run_event_counters).where(run_event_counters.c.run_id == str(run_id))
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise ExecutionCorruptionError("run event counter is missing")
        counter = run_event_counter_from_row(row)
        if counter.run_id != run_id:
            raise ExecutionCorruptionError("run event counter identity is corrupt")
        return counter

    def _require_counters(self, records: tuple[RunRecord, ...]) -> None:
        if not records:
            return
        rows = (
            self._session.execute(
                select(run_event_counters).where(
                    run_event_counters.c.run_id.in_([str(record.run_id) for record in records])
                )
            )
            .mappings()
            .all()
        )
        counters = {counter.run_id: counter for counter in map(run_event_counter_from_row, rows)}
        if set(counters) != {record.run_id for record in records}:
            raise ExecutionCorruptionError("run event counters are incomplete")

    def _require_pipeline_parents(self, records: tuple[RunRecord, ...]) -> None:
        """Validate captured pipeline versions in a fixed number of queries."""
        if not records:
            return
        pipeline_ids = {record.pipeline_id for record in records}
        pipeline_rows = (
            self._session.execute(
                select(pipelines).where(
                    pipelines.c.pipeline_id.in_([str(identity) for identity in pipeline_ids])
                )
            )
            .mappings()
            .all()
        )
        try:
            parents = {
                parent.pipeline_id: parent for parent in map(pipeline_from_row, pipeline_rows)
            }
        except ConfigurationRepositoryError as error:
            raise ExecutionCorruptionError("run pipeline parent is corrupt") from error
        if set(parents) != pipeline_ids:
            raise ExecutionCorruptionError("run pipeline parent is missing")

        version_keys = {(record.pipeline_id, record.pipeline_version) for record in records}
        version_rows = (
            self._session.execute(
                select(pipeline_versions).where(
                    tuple_(
                        pipeline_versions.c.pipeline_id,
                        pipeline_versions.c.version_number,
                    ).in_(
                        [(str(pipeline_id), int(version)) for pipeline_id, version in version_keys]
                    )
                )
            )
            .mappings()
            .all()
        )
        versions: set[tuple[PipelineId, PipelineVersion]] = set()
        try:
            for row in version_rows:
                pipeline_id = PipelineId(str(row["pipeline_id"]))
                parent = parents[pipeline_id]
                published = pipeline_version_from_row(row, pipeline_created_at=parent.created_at)
                versions.add((published.pipeline_id, published.version))
        except ConfigurationRepositoryError as error:
            raise ExecutionCorruptionError("run pipeline version parent is corrupt") from error
        if versions != version_keys:
            raise ExecutionCorruptionError("run pipeline version parent is missing")

    def _update_run(
        self,
        run_id: RunId,
        *,
        expected_row_version: int,
        expected_state: RunState,
        values: Mapping[str, object],
    ) -> RowMapping:
        row = (
            self._session.execute(
                update(runs)
                .where(
                    runs.c.run_id == str(run_id),
                    runs.c.row_version == expected_row_version,
                    runs.c.state == expected_state.value,
                )
                .values(**values)
                .returning(*runs.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            self._raise_cas_failure(run_id, expected_row_version, expected_state)
        return row

    def _raise_cas_failure(
        self, run_id: RunId, expected: int, expected_state: RunState
    ) -> NoReturn:
        current = self.get(run_id)
        if current is None:
            raise ExecutionRecordNotFoundError("run does not exist")
        if current.row_version != expected:
            raise ExecutionStaleRowVersionError("run row version is stale")
        if current.state is not expected_state:
            raise ExecutionStateConflictError("run lifecycle state changed")
        raise ExecutionStateConflictError("run update was rejected")

    def _require_transaction(self) -> None:
        if not self._session.in_transaction():
            raise ExecutionInvalidRequestError("repository requires a caller-owned transaction")


__all__ = ["SqlAlchemyRunRepository"]
