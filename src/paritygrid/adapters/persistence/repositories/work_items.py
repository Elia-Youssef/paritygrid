"""SQLAlchemy repositories for durable work claims and immutable attempts."""

from collections.abc import Mapping
from typing import NoReturn

from sqlalchemy import func, insert, select, tuple_, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement

from paritygrid.adapters.persistence.repositories.execution_common import (
    MAX_PERSISTED_INTEGER,
    bounded_text,
    encode_execution_document,
    nonnegative_int,
    optional_text,
    positive_int,
    require_attempt_number,
    require_document,
    require_exact,
    require_incrementable,
    require_node_id,
    require_partition_key,
    require_run_id,
    require_timestamp,
    require_work_item_id,
    translate_execution_storage_errors,
)
from paritygrid.adapters.persistence.repositories.execution_mapping import (
    run_from_row,
    run_node_from_row,
    stored_node_id,
    stored_nonnegative_int,
    stored_partition_key,
    stored_positive_int,
    stored_run_id,
    stored_timestamp,
    stored_work_item_id,
    work_attempt_from_row,
    work_item_from_row,
)
from paritygrid.adapters.persistence.schema import (
    checkpoint_heads,
    run_nodes,
    runs,
    work_attempts,
    work_items,
)
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.execution import (
    AttemptOutcome,
    CompletedWork,
    ExecutionCorruptionError,
    ExecutionDuplicateError,
    ExecutionInvalidRequestError,
    ExecutionLeaseExpiredError,
    ExecutionLeaseMismatchError,
    ExecutionRecordNotFoundError,
    ExecutionStaleRowVersionError,
    ExecutionStateConflictError,
    WorkAttemptPage,
    WorkAttemptRecord,
    WorkAttemptRepository,
    WorkClaim,
    WorkCompletion,
    WorkItemPage,
    WorkItemRecord,
    WorkItemRepository,
    validate_execution_page_limit,
)
from paritygrid.domain.execution import FailureClassification, RunState, WorkItemState
from paritygrid.domain.models import (
    AttemptNumber,
    Duration,
    NodeId,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

_TARGET_OUTCOMES: Mapping[WorkItemState, AttemptOutcome] = {
    WorkItemState.SUCCEEDED: AttemptOutcome.SUCCEEDED,
    WorkItemState.RETRY_WAIT: AttemptOutcome.RETRY_SCHEDULED,
    WorkItemState.QUARANTINED: AttemptOutcome.QUARANTINED,
    WorkItemState.FAILED: AttemptOutcome.FAILED,
    WorkItemState.CANCELLED: AttemptOutcome.CANCELLED,
}


class SqlAlchemyWorkItemRepository(WorkItemRepository):
    """Persist work claims and completions in a caller-owned transaction."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @translate_execution_storage_errors
    def create(
        self,
        *,
        work_item_id: WorkItemId,
        run_id: RunId,
        node_id: NodeId,
        partition_key: PartitionKey,
        input_reference: ConfigurationDocument | None,
        created_at: UtcTimestamp,
    ) -> WorkItemRecord:
        self._require_transaction()
        identity = require_work_item_id(work_item_id)
        run_identity = require_run_id(run_id)
        node_identity = require_node_id(node_id)
        partition = require_partition_key(partition_key)
        timestamp = require_timestamp(created_at, "work-item creation time")
        reference = (
            None
            if input_reference is None
            else encode_execution_document(
                require_document(input_reference, "work input reference"),
                "work input reference",
            )
        )
        run_row = (
            self._session.execute(select(runs).where(runs.c.run_id == str(run_identity)))
            .mappings()
            .one_or_none()
        )
        node_row = (
            self._session.execute(
                select(run_nodes).where(
                    run_nodes.c.run_id == str(run_identity),
                    run_nodes.c.node_id == str(node_identity),
                )
            )
            .mappings()
            .one_or_none()
        )
        if run_row is None or node_row is None:
            raise ExecutionRecordNotFoundError("run node does not exist")
        parent_run = run_from_row(run_row)
        parent_node = run_node_from_row(node_row)
        if parent_run.run_id != run_identity or parent_node.run_id != run_identity:
            raise ExecutionCorruptionError("run-node parent identity is corrupt")
        if parent_node.node_id != node_identity:
            raise ExecutionCorruptionError("run-node parent identity is corrupt")
        if parent_run.state not in {RunState.QUEUED, RunState.RUNNING}:
            raise ExecutionStateConflictError("run state does not permit work-item creation")
        if timestamp < parent_run.created_at:
            raise ExecutionInvalidRequestError("work-item creation cannot precede its run")
        row = (
            self._session.execute(
                sqlite_insert(work_items)
                .values(
                    work_item_id=str(identity),
                    run_id=str(run_identity),
                    node_id=str(node_identity),
                    partition_key=str(partition),
                    state=WorkItemState.PENDING.value,
                    row_version=1,
                    completed_attempt_count=0,
                    expected_checkpoint_version=0,
                    input_reference_json=None if reference is None else reference.text,
                    retry_available_at=None,
                    lease_owner=None,
                    lease_expires_at=None,
                    active_attempt_number=None,
                    active_attempt_started_at=None,
                    active_runner_kind=None,
                    active_worker_identity=None,
                    created_at=str(timestamp),
                    updated_at=str(timestamp),
                )
                .on_conflict_do_nothing()
                .returning(*work_items.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise ExecutionDuplicateError("work item identity or partition already exists")
        self._session.execute(
            insert(checkpoint_heads).values(
                run_id=str(run_identity),
                node_id=str(node_identity),
                partition_key=str(partition),
                current_version=0,
                updated_at=str(timestamp),
                row_version=1,
            )
        )
        record = work_item_from_row(row)
        _verify_work_records(self._session, (record,))
        return record

    @translate_execution_storage_errors
    def get(self, work_item_id: WorkItemId) -> WorkItemRecord | None:
        self._require_transaction()
        identity = require_work_item_id(work_item_id)
        return _load_verified_work_parent(self._session, identity)

    @translate_execution_storage_errors
    def list_for_run(
        self,
        run_id: RunId,
        *,
        limit: int,
        after: WorkItemId | None = None,
        state: WorkItemState | None = None,
    ) -> WorkItemPage:
        self._require_transaction()
        identity = require_run_id(run_id)
        page_size = validate_execution_page_limit(limit)
        cursor = None if after is None else require_work_item_id(after)
        if state is not None and (
            type(state) is not WorkItemState or state is WorkItemState.LEASED
        ):
            raise ExecutionInvalidRequestError("work state filter is not durable")
        query = select(work_items).where(work_items.c.run_id == str(identity))
        if cursor is not None:
            query = query.where(work_items.c.work_item_id > str(cursor))
        if state is not None:
            query = query.where(work_items.c.state == state.value)
        rows = (
            self._session.execute(query.order_by(work_items.c.work_item_id).limit(page_size + 1))
            .mappings()
            .all()
        )
        records = tuple(work_item_from_row(row) for row in rows[:page_size])
        _verify_work_records(self._session, records)
        next_cursor = records[-1].work_item_id if len(rows) > page_size else None
        return WorkItemPage(records, next_cursor)

    @translate_execution_storage_errors
    def claim(
        self,
        work_item_id: WorkItemId,
        *,
        expected_row_version: int,
        lease_owner: str,
        started_at: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
        runner_kind: str,
        worker_identity: str,
    ) -> WorkClaim:
        self._require_transaction()
        identity = require_work_item_id(work_item_id)
        expected = positive_int(expected_row_version, "expected work-item row version")
        owner = bounded_text(lease_owner, "lease owner", 128)
        started = require_timestamp(started_at, "attempt start time")
        expires = require_timestamp(lease_expires_at, "lease expiry")
        runner = bounded_text(runner_kind, "runner kind", 32)
        worker = bounded_text(worker_identity, "worker identity", 128)
        current = self._require_work(identity, expected)
        if current.state not in {WorkItemState.PENDING, WorkItemState.RETRY_WAIT}:
            raise ExecutionStateConflictError("work item is not claimable")
        if not self._run_allows_claim(current.run_id):
            raise ExecutionStateConflictError("run state does not permit work claims")
        current.state.transition_to(WorkItemState.LEASED)
        WorkItemState.LEASED.transition_to(WorkItemState.RUNNING)
        if current.retry_available_at is not None and started < current.retry_available_at:
            raise ExecutionStateConflictError("work item retry is not available")
        if started < current.updated_at:
            raise ExecutionInvalidRequestError("attempt start time is not monotonic")
        if expires <= started:
            raise ExecutionInvalidRequestError("lease expiry must follow attempt start")
        require_incrementable(expected, "work-item row version")
        require_incrementable(current.completed_attempt_count, "attempt number")
        attempt = AttemptNumber(current.completed_attempt_count + 1)
        row = (
            self._session.execute(
                update(work_items)
                .where(
                    work_items.c.work_item_id == str(identity),
                    work_items.c.row_version == expected,
                    work_items.c.state == current.state.value,
                    select(runs.c.run_id)
                    .where(
                        runs.c.run_id == work_items.c.run_id,
                        runs.c.state == RunState.RUNNING.value,
                    )
                    .exists(),
                )
                .values(
                    state=WorkItemState.RUNNING.value,
                    row_version=expected + 1,
                    retry_available_at=None,
                    lease_owner=owner,
                    lease_expires_at=str(expires),
                    active_attempt_number=int(attempt),
                    active_attempt_started_at=str(started),
                    active_runner_kind=runner,
                    active_worker_identity=worker,
                    updated_at=str(started),
                )
                .returning(*work_items.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            self._raise_claim_cas_failure(identity, expected, current.state)
        record = self._verified_from_row(row)
        return _claim_from_record(record)

    @translate_execution_storage_errors
    def renew_claim(
        self,
        claim: WorkClaim,
        *,
        renewed_at: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
    ) -> WorkClaim:
        self._require_transaction()
        capability = require_exact(claim, WorkClaim, "work claim")
        renewed = require_timestamp(renewed_at, "lease renewal time")
        expires = require_timestamp(lease_expires_at, "lease expiry")
        current = self._require_claim(capability, renewed)
        if renewed <= current.updated_at:
            raise ExecutionInvalidRequestError("lease renewal time must advance work-item time")
        assert current.lease_expires_at is not None
        if expires <= current.lease_expires_at:
            raise ExecutionInvalidRequestError("renewed lease must extend the current expiry")
        require_incrementable(current.row_version, "work-item row version")
        row = self._claim_update(
            current,
            capability,
            observed_at=renewed,
            values={
                "lease_expires_at": str(expires),
                "updated_at": str(renewed),
                "row_version": current.row_version + 1,
            },
        )
        return _claim_from_record(self._verified_from_row(row))

    @translate_execution_storage_errors
    def complete_claim(self, claim: WorkClaim, completion: WorkCompletion) -> CompletedWork:
        self._require_transaction()
        capability = require_exact(claim, WorkClaim, "work claim")
        result = require_exact(completion, WorkCompletion, "work completion")
        finished = require_timestamp(result.finished_at, "attempt finish time")
        current = self._require_claim(capability, finished)
        if type(result.target_state) is not WorkItemState:
            raise ExecutionInvalidRequestError("completion target must use WorkItemState")
        outcome = _TARGET_OUTCOMES.get(result.target_state)
        if outcome is None:
            raise ExecutionInvalidRequestError("completion target is not durable")
        current.state.transition_to(result.target_state)
        classification = _validate_completion_classification(outcome, result)
        retry_at = _validate_retry_time(result, finished)
        detail = optional_text(result.redacted_detail, "redacted attempt detail", 4096)
        reference = (
            None
            if result.result_reference is None
            else encode_execution_document(
                require_document(result.result_reference, "attempt result reference"),
                "attempt result reference",
            )
        )
        records = nonnegative_int(result.records_processed, "records processed")
        byte_count = nonnegative_int(result.bytes_processed, "bytes processed")
        duration = _duration_between(current.active_attempt_started_at, finished)
        require_incrementable(current.row_version, "work-item row version")
        updated_row = self._claim_update(
            current,
            capability,
            observed_at=finished,
            values={
                "state": result.target_state.value,
                "row_version": current.row_version + 1,
                "completed_attempt_count": current.completed_attempt_count + 1,
                "retry_available_at": None if retry_at is None else str(retry_at),
                "lease_owner": None,
                "lease_expires_at": None,
                "active_attempt_number": None,
                "active_attempt_started_at": None,
                "active_runner_kind": None,
                "active_worker_identity": None,
                "updated_at": str(finished),
            },
        )
        attempt_row = self._insert_attempt(
            current,
            outcome=outcome,
            failure_classification=classification,
            redacted_detail=detail,
            result_reference_json=None if reference is None else reference.text,
            records_processed=records,
            bytes_processed=byte_count,
            finished_at=finished,
            duration=duration,
        )
        work = work_item_from_row(updated_row)
        _verify_work_records(self._session, (work,))
        return CompletedWork(work, work_attempt_from_row(attempt_row))

    @translate_execution_storage_errors
    def recover_expired_claim(
        self,
        work_item_id: WorkItemId,
        *,
        expected_row_version: int,
        expected_attempt_number: AttemptNumber,
        observed_at: UtcTimestamp,
        retry_available_at: UtcTimestamp,
        redacted_detail: str | None = None,
    ) -> CompletedWork:
        self._require_transaction()
        identity = require_work_item_id(work_item_id)
        expected = positive_int(expected_row_version, "expected work-item row version")
        attempt_number = require_attempt_number(expected_attempt_number)
        observed = require_timestamp(observed_at, "lease observation time")
        retry_at = require_timestamp(retry_available_at, "retry availability time")
        detail = optional_text(redacted_detail, "redacted attempt detail", 4096)
        current = self._require_work(identity, expected)
        if current.state is not WorkItemState.RUNNING:
            raise ExecutionStateConflictError("work item has no active lease to recover")
        if current.active_attempt_number != attempt_number:
            raise ExecutionLeaseMismatchError("active attempt does not match recovery request")
        assert current.lease_expires_at is not None
        if current.lease_expires_at > observed:
            raise ExecutionStateConflictError("work-item lease has not expired")
        if retry_at < observed:
            raise ExecutionInvalidRequestError("retry availability cannot precede recovery")
        current.state.transition_to(WorkItemState.RETRY_WAIT)
        require_incrementable(current.row_version, "work-item row version")
        duration = _duration_between(current.active_attempt_started_at, observed)
        row = (
            self._session.execute(
                update(work_items)
                .where(
                    work_items.c.work_item_id == str(identity),
                    work_items.c.row_version == expected,
                    work_items.c.state == WorkItemState.RUNNING.value,
                    work_items.c.active_attempt_number == int(attempt_number),
                    work_items.c.lease_expires_at <= str(observed),
                )
                .values(
                    state=WorkItemState.RETRY_WAIT.value,
                    row_version=expected + 1,
                    completed_attempt_count=current.completed_attempt_count + 1,
                    retry_available_at=str(retry_at),
                    lease_owner=None,
                    lease_expires_at=None,
                    active_attempt_number=None,
                    active_attempt_started_at=None,
                    active_runner_kind=None,
                    active_worker_identity=None,
                    updated_at=str(observed),
                )
                .returning(*work_items.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            self._raise_recovery_cas_failure(identity, expected, attempt_number, observed)
        attempt_row = self._insert_attempt(
            current,
            outcome=AttemptOutcome.LEASE_EXPIRED,
            failure_classification=FailureClassification.TIMEOUT,
            redacted_detail=detail,
            result_reference_json=None,
            records_processed=0,
            bytes_processed=0,
            finished_at=observed,
            duration=duration,
        )
        work = work_item_from_row(row)
        _verify_work_records(self._session, (work,))
        return CompletedWork(work, work_attempt_from_row(attempt_row))

    def _verified_from_row(self, row: RowMapping) -> WorkItemRecord:
        record = work_item_from_row(row)
        _verify_work_records(self._session, (record,))
        return record

    def _require_work(self, work_item_id: WorkItemId, expected: int) -> WorkItemRecord:
        record = self.get(work_item_id)
        if record is None:
            raise ExecutionRecordNotFoundError("work item does not exist")
        if record.row_version != expected:
            raise ExecutionStaleRowVersionError("work-item row version is stale")
        return record

    def _require_claim(self, claim: WorkClaim, observed_at: UtcTimestamp) -> WorkItemRecord:
        current = self._require_work(claim.work_item_id, claim.row_version)
        if current.state is not WorkItemState.RUNNING:
            raise ExecutionLeaseMismatchError("work claim is no longer active")
        if not _claim_matches(current, claim):
            raise ExecutionLeaseMismatchError("work claim does not match the active lease")
        assert current.lease_expires_at is not None
        if observed_at < current.updated_at:
            raise ExecutionInvalidRequestError("claim observation time is not monotonic")
        if current.lease_expires_at <= observed_at:
            raise ExecutionLeaseExpiredError("work claim has expired")
        return current

    def _claim_update(
        self,
        current: WorkItemRecord,
        claim: WorkClaim,
        *,
        observed_at: UtcTimestamp,
        values: Mapping[str, object],
    ) -> RowMapping:
        row = (
            self._session.execute(
                update(work_items)
                .where(
                    *_claim_conditions(current, claim),
                    work_items.c.lease_expires_at > str(observed_at),
                )
                .values(**values)
                .returning(*work_items.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            latest = self.get(claim.work_item_id)
            if latest is None:
                raise ExecutionRecordNotFoundError("work item does not exist")
            if latest.row_version != claim.row_version:
                raise ExecutionStaleRowVersionError("work-item row version is stale")
            if latest.lease_expires_at is not None and latest.lease_expires_at <= observed_at:
                raise ExecutionLeaseExpiredError("work claim has expired")
            raise ExecutionLeaseMismatchError("work claim does not match the active lease")
        return row

    def _insert_attempt(
        self,
        current: WorkItemRecord,
        *,
        outcome: AttemptOutcome,
        failure_classification: FailureClassification | None,
        redacted_detail: str | None,
        result_reference_json: str | None,
        records_processed: int,
        bytes_processed: int,
        finished_at: UtcTimestamp,
        duration: Duration,
    ) -> RowMapping:
        assert current.active_attempt_number is not None
        assert current.active_attempt_started_at is not None
        assert current.active_runner_kind is not None
        assert current.active_worker_identity is not None
        return (
            self._session.execute(
                insert(work_attempts)
                .values(
                    work_item_id=str(current.work_item_id),
                    attempt_number=int(current.active_attempt_number),
                    started_at=str(current.active_attempt_started_at),
                    finished_at=str(finished_at),
                    runner_kind=current.active_runner_kind,
                    worker_identity=current.active_worker_identity,
                    outcome=outcome.value,
                    failure_classification=(
                        None if failure_classification is None else failure_classification.value
                    ),
                    redacted_detail=redacted_detail,
                    result_reference_json=result_reference_json,
                    records_processed=records_processed,
                    bytes_processed=bytes_processed,
                    duration_microseconds=duration.microseconds,
                )
                .returning(*work_attempts.c)
            )
            .mappings()
            .one()
        )

    def _raise_claim_cas_failure(
        self, work_item_id: WorkItemId, expected: int, expected_state: WorkItemState
    ) -> NoReturn:
        current = self.get(work_item_id)
        if current is None:
            raise ExecutionRecordNotFoundError("work item does not exist")
        if current.row_version != expected:
            raise ExecutionStaleRowVersionError("work-item row version is stale")
        if current.state is not expected_state:
            raise ExecutionStateConflictError("work-item lifecycle state changed")
        if not self._run_allows_claim(current.run_id):
            raise ExecutionStateConflictError("run state does not permit work claims")
        raise ExecutionStateConflictError("work-item claim was rejected")

    def _run_allows_claim(self, run_id: RunId) -> bool:
        state = self._session.execute(
            select(runs.c.state).where(runs.c.run_id == str(run_id))
        ).scalar_one_or_none()
        if state is None:
            raise ExecutionCorruptionError("work-item run parent is missing")
        try:
            return RunState(state) is RunState.RUNNING
        except (TypeError, ValueError) as error:
            raise ExecutionCorruptionError("work-item run parent state is corrupt") from error

    def _raise_recovery_cas_failure(
        self,
        work_item_id: WorkItemId,
        expected: int,
        attempt_number: AttemptNumber,
        observed_at: UtcTimestamp,
    ) -> NoReturn:
        current = self.get(work_item_id)
        if current is None:
            raise ExecutionRecordNotFoundError("work item does not exist")
        if current.row_version != expected:
            raise ExecutionStaleRowVersionError("work-item row version is stale")
        if current.state is not WorkItemState.RUNNING:
            raise ExecutionStateConflictError("work item has no active lease to recover")
        if current.active_attempt_number != attempt_number:
            raise ExecutionLeaseMismatchError("active attempt does not match recovery request")
        if current.lease_expires_at is not None and current.lease_expires_at > observed_at:
            raise ExecutionStateConflictError("work-item lease has not expired")
        raise ExecutionStateConflictError("expired work-item recovery was rejected")

    def _require_transaction(self) -> None:
        if not self._session.in_transaction():
            raise ExecutionInvalidRequestError("repository requires a caller-owned transaction")


def _load_verified_work_parent(session: Session, work_item_id: WorkItemId) -> WorkItemRecord | None:
    row = (
        session.execute(select(work_items).where(work_items.c.work_item_id == str(work_item_id)))
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    record = work_item_from_row(row)
    _verify_work_records(session, (record,))
    return record


def _verify_work_records(session: Session, records: tuple[WorkItemRecord, ...]) -> None:
    """Validate checkpoint and complete-attempt aggregates in bounded queries."""
    if not records:
        return
    keys = tuple(
        (str(record.run_id), str(record.node_id), str(record.partition_key)) for record in records
    )
    head_rows = (
        session.execute(
            select(checkpoint_heads).where(
                tuple_(
                    checkpoint_heads.c.run_id,
                    checkpoint_heads.c.node_id,
                    checkpoint_heads.c.partition_key,
                ).in_(keys)
            )
        )
        .mappings()
        .all()
    )
    heads: dict[tuple[RunId, NodeId, PartitionKey], RowMapping] = {}
    for row in head_rows:
        key = (
            stored_run_id(row["run_id"]),
            stored_node_id(row["node_id"]),
            stored_partition_key(row["partition_key"]),
        )
        heads[key] = row
    if len(heads) != len(records):
        raise ExecutionCorruptionError("work-item checkpoint heads are incomplete")
    aggregate_rows = session.execute(
        select(
            work_attempts.c.work_item_id,
            func.count(work_attempts.c.attempt_number),
            func.min(work_attempts.c.attempt_number),
            func.max(work_attempts.c.attempt_number),
        )
        .where(work_attempts.c.work_item_id.in_([str(record.work_item_id) for record in records]))
        .group_by(work_attempts.c.work_item_id)
    ).all()
    aggregates = {stored_work_item_id(row[0]): (row[1], row[2], row[3]) for row in aggregate_rows}
    for work in records:
        head = heads.get((work.run_id, work.node_id, work.partition_key))
        if head is None:
            raise ExecutionCorruptionError("work-item checkpoint head is missing")
        current = stored_nonnegative_int(
            head["current_version"],
            "checkpoint current version",
            maximum=MAX_PERSISTED_INTEGER,
        )
        stored_positive_int(head["row_version"], "checkpoint row version")
        updated = stored_timestamp(head["updated_at"], "checkpoint update time")
        if updated < work.created_at:
            raise ExecutionCorruptionError("checkpoint update time is corrupt")
        if current != work.expected_checkpoint_version:
            raise ExecutionCorruptionError("work-item checkpoint version is inconsistent")
        aggregate = aggregates.get(work.work_item_id)
        if aggregate is None:
            if work.completed_attempt_count != 0:
                raise ExecutionCorruptionError("work-item attempt history is corrupt")
            continue
        count = stored_nonnegative_int(
            aggregate[0], "attempt history count", maximum=MAX_PERSISTED_INTEGER
        )
        minimum = stored_positive_int(aggregate[1], "first attempt number")
        maximum = stored_positive_int(aggregate[2], "latest attempt number")
        if minimum != 1 or maximum != count or count != work.completed_attempt_count:
            raise ExecutionCorruptionError("work-item attempt history is not contiguous")


class SqlAlchemyWorkAttemptRepository(WorkAttemptRepository):
    """Read immutable attempt history without exposing stored rows."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @translate_execution_storage_errors
    def get(
        self, work_item_id: WorkItemId, attempt_number: AttemptNumber
    ) -> WorkAttemptRecord | None:
        self._require_transaction()
        identity = require_work_item_id(work_item_id)
        attempt = require_attempt_number(attempt_number)
        parent = _load_verified_work_parent(self._session, identity)
        if parent is None:
            raise ExecutionRecordNotFoundError("work item does not exist")
        row = (
            self._session.execute(
                select(work_attempts).where(
                    work_attempts.c.work_item_id == str(identity),
                    work_attempts.c.attempt_number == int(attempt),
                )
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else work_attempt_from_row(row)

    @translate_execution_storage_errors
    def list_for_work_item(
        self,
        work_item_id: WorkItemId,
        *,
        limit: int,
        after: AttemptNumber | None = None,
    ) -> WorkAttemptPage:
        self._require_transaction()
        identity = require_work_item_id(work_item_id)
        page_size = validate_execution_page_limit(limit)
        cursor = None if after is None else require_attempt_number(after)
        parent = _load_verified_work_parent(self._session, identity)
        if parent is None:
            raise ExecutionRecordNotFoundError("work item does not exist")
        query = select(work_attempts).where(work_attempts.c.work_item_id == str(identity))
        if cursor is not None:
            query = query.where(work_attempts.c.attempt_number > int(cursor))
        rows = (
            self._session.execute(
                query.order_by(work_attempts.c.attempt_number).limit(page_size + 1)
            )
            .mappings()
            .all()
        )
        records = tuple(work_attempt_from_row(row) for row in rows[:page_size])
        next_cursor = records[-1].attempt_number if len(rows) > page_size else None
        return WorkAttemptPage(records, next_cursor)

    def _require_transaction(self) -> None:
        if not self._session.in_transaction():
            raise ExecutionInvalidRequestError("repository requires a caller-owned transaction")


def _claim_from_record(record: WorkItemRecord) -> WorkClaim:
    if record.state is not WorkItemState.RUNNING:
        raise ExecutionCorruptionError("work-item claim is not durable")
    assert record.active_attempt_number is not None
    assert record.lease_owner is not None
    assert record.active_attempt_started_at is not None
    assert record.lease_expires_at is not None
    assert record.active_runner_kind is not None
    assert record.active_worker_identity is not None
    return WorkClaim(
        work_item_id=record.work_item_id,
        attempt_number=record.active_attempt_number,
        lease_owner=record.lease_owner,
        row_version=record.row_version,
        started_at=record.active_attempt_started_at,
        lease_expires_at=record.lease_expires_at,
        runner_kind=record.active_runner_kind,
        worker_identity=record.active_worker_identity,
    )


def _claim_matches(record: WorkItemRecord, claim: WorkClaim) -> bool:
    return (
        record.work_item_id == claim.work_item_id
        and record.active_attempt_number == claim.attempt_number
        and record.lease_owner == claim.lease_owner
        and record.row_version == claim.row_version
        and record.active_attempt_started_at == claim.started_at
        and record.lease_expires_at == claim.lease_expires_at
        and record.active_runner_kind == claim.runner_kind
        and record.active_worker_identity == claim.worker_identity
    )


def _claim_conditions(record: WorkItemRecord, claim: WorkClaim) -> tuple[ColumnElement[bool], ...]:
    return (
        work_items.c.work_item_id == str(record.work_item_id),
        work_items.c.state == WorkItemState.RUNNING.value,
        work_items.c.row_version == claim.row_version,
        work_items.c.active_attempt_number == int(claim.attempt_number),
        work_items.c.lease_owner == claim.lease_owner,
        work_items.c.active_attempt_started_at == str(claim.started_at),
        work_items.c.lease_expires_at == str(claim.lease_expires_at),
        work_items.c.active_runner_kind == claim.runner_kind,
        work_items.c.active_worker_identity == claim.worker_identity,
    )


def _validate_completion_classification(
    outcome: AttemptOutcome, completion: WorkCompletion
) -> FailureClassification | None:
    classification = completion.failure_classification
    if outcome is AttemptOutcome.SUCCEEDED:
        if classification is not None:
            raise ExecutionInvalidRequestError("successful attempt cannot have a failure class")
        return None
    if type(classification) is not FailureClassification:
        raise ExecutionInvalidRequestError("non-successful attempt requires a failure class")
    return classification


def _validate_retry_time(
    completion: WorkCompletion, finished_at: UtcTimestamp
) -> UtcTimestamp | None:
    if completion.target_state is WorkItemState.RETRY_WAIT:
        if completion.retry_available_at is None:
            raise ExecutionInvalidRequestError("retry completion requires an availability time")
        retry_at = require_timestamp(completion.retry_available_at, "retry availability time")
        if retry_at < finished_at:
            raise ExecutionInvalidRequestError("retry availability cannot precede completion")
        return retry_at
    if completion.retry_available_at is not None:
        raise ExecutionInvalidRequestError("retry availability is allowed for retry state only")
    return None


def _duration_between(started_at: UtcTimestamp | None, finished_at: UtcTimestamp) -> Duration:
    if started_at is None:
        raise ExecutionCorruptionError("active attempt start time is missing")
    delta = finished_at.to_datetime() - started_at.to_datetime()
    microseconds = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    try:
        return Duration(microseconds)
    except (TypeError, ValueError) as error:
        raise ExecutionInvalidRequestError(
            "attempt duration is outside the supported range"
        ) from error


__all__ = ["SqlAlchemyWorkAttemptRepository", "SqlAlchemyWorkItemRepository"]
