"""Dependency-neutral contracts for durable execution state."""

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.domain.execution import FailureClassification, RunState, WorkItemState
from paritygrid.domain.models import (
    AttemptNumber,
    Duration,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    StateFingerprint,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey

MAX_EXECUTION_PAGE_SIZE = 100


class RunNodeStatus(StrEnum):
    """Aggregate execution status exposed by the application boundary."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIALLY_SUCCEEDED = "partially_succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AttemptOutcome(StrEnum):
    """Durable outcome of one completed or recovered work attempt."""

    SUCCEEDED = "succeeded"
    RETRY_SCHEDULED = "retry_scheduled"
    QUARANTINED = "quarantined"
    FAILED = "failed"
    CANCELLED = "cancelled"
    LEASE_EXPIRED = "lease_expired"


class ExecutionRepositoryError(Exception):
    """Base class for stable execution repository failures."""


class ExecutionInvalidRequestError(ExecutionRepositoryError):
    """The requested operation violates the execution repository contract."""


class ExecutionDuplicateError(ExecutionRepositoryError):
    """A durable execution identity or logical key already exists."""


class ExecutionRecordNotFoundError(ExecutionRepositoryError):
    """A requested execution record does not exist."""


class ExecutionStaleRowVersionError(ExecutionRepositoryError):
    """An optimistic row version no longer matches durable state."""


class ExecutionStateConflictError(ExecutionRepositoryError):
    """Current lifecycle or claim state rejects the requested operation."""


class ExecutionLeaseLostError(ExecutionRepositoryError):
    """A work claim no longer authorizes an owner mutation."""


class ExecutionLeaseExpiredError(ExecutionLeaseLostError):
    """A work claim expired before the requested owner mutation."""


class ExecutionLeaseMismatchError(ExecutionLeaseLostError):
    """A work claim does not match the active durable owner and attempt."""


class ExecutionCorruptionError(ExecutionRepositoryError):
    """Persisted execution data failed strict boundary validation."""


class ExecutionStorageError(ExecutionRepositoryError):
    """An unexpected persistence implementation failure prevented the operation."""


class ExecutionStorageUnavailableError(ExecutionStorageError):
    """Execution storage was unavailable for the requested operation."""


@dataclass(frozen=True, slots=True, repr=False)
class RunRecord:
    run_id: RunId
    pipeline_id: PipelineId
    pipeline_version: PipelineVersion
    runner_kind: str
    runner_configuration: ConfigurationDocument
    state: RunState
    row_version: int
    scenario_seed: int | None
    created_at: UtcTimestamp
    started_at: UtcTimestamp | None
    finished_at: UtcTimestamp | None
    cancellation_requested_at: UtcTimestamp | None
    recovery_started_at: UtcTimestamp | None
    recovered_at: UtcTimestamp | None
    final_reconciliation_fingerprint: StateFingerprint | None

    def __repr__(self) -> str:
        return (
            "RunRecord("
            f"run_id={self.run_id!r}, pipeline_id={self.pipeline_id!r}, "
            f"pipeline_version={self.pipeline_version!r}, runner_kind={self.runner_kind!r}, "
            f"state={self.state!r}, row_version={self.row_version!r}, "
            f"created_at={self.created_at!r}, started_at={self.started_at!r}, "
            f"finished_at={self.finished_at!r}, "
            f"final_reconciliation_fingerprint={self.final_reconciliation_fingerprint!r}, "
            "runner_configuration=<redacted>)"
        )


@dataclass(frozen=True, slots=True)
class RunEventCounterRecord:
    run_id: RunId
    next_sequence_number: int
    row_version: int


@dataclass(frozen=True, slots=True)
class RunNodeRecord:
    run_id: RunId
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
    records_read: int
    records_written: int
    records_quarantined: int
    bytes_read: int
    bytes_written: int
    retry_count: int
    duration: Duration
    started_at: UtcTimestamp | None
    finished_at: UtcTimestamp | None


@dataclass(frozen=True, slots=True)
class RunPage:
    items: tuple[RunRecord, ...]
    next_cursor: RunId | None


@dataclass(frozen=True, slots=True)
class RunNodePage:
    items: tuple[RunNodeRecord, ...]
    next_cursor: NodeId | None


@dataclass(frozen=True, slots=True, repr=False)
class WorkItemRecord:
    work_item_id: WorkItemId
    run_id: RunId
    node_id: NodeId
    partition_key: PartitionKey
    state: WorkItemState
    row_version: int
    completed_attempt_count: int
    expected_checkpoint_version: int
    input_reference: ConfigurationDocument | None
    retry_available_at: UtcTimestamp | None
    lease_owner: str | None
    lease_expires_at: UtcTimestamp | None
    active_attempt_number: AttemptNumber | None
    active_attempt_started_at: UtcTimestamp | None
    active_runner_kind: str | None
    active_worker_identity: str | None
    created_at: UtcTimestamp
    updated_at: UtcTimestamp

    def __repr__(self) -> str:
        return (
            "WorkItemRecord("
            f"work_item_id={self.work_item_id!r}, run_id={self.run_id!r}, "
            f"node_id={self.node_id!r}, partition_key={self.partition_key!r}, "
            f"state={self.state!r}, row_version={self.row_version!r}, "
            f"completed_attempt_count={self.completed_attempt_count!r}, "
            f"expected_checkpoint_version={self.expected_checkpoint_version!r}, "
            f"created_at={self.created_at!r}, updated_at={self.updated_at!r}, "
            "input_reference=<redacted>, claim=<redacted>)"
        )


@dataclass(frozen=True, slots=True, repr=False)
class WorkClaim:
    work_item_id: WorkItemId
    attempt_number: AttemptNumber
    lease_owner: str
    row_version: int
    started_at: UtcTimestamp
    lease_expires_at: UtcTimestamp
    runner_kind: str
    worker_identity: str

    def __repr__(self) -> str:
        return (
            "WorkClaim("
            f"work_item_id={self.work_item_id!r}, attempt_number={self.attempt_number!r}, "
            f"row_version={self.row_version!r}, started_at={self.started_at!r}, "
            f"lease_expires_at={self.lease_expires_at!r}, runner_kind={self.runner_kind!r}, "
            "lease_owner=<redacted>, worker_identity=<redacted>)"
        )


@dataclass(frozen=True, slots=True, repr=False)
class WorkAttemptRecord:
    work_item_id: WorkItemId
    attempt_number: AttemptNumber
    started_at: UtcTimestamp
    finished_at: UtcTimestamp
    runner_kind: str
    worker_identity: str
    outcome: AttemptOutcome
    failure_classification: FailureClassification | None
    redacted_detail: str | None
    result_reference: ConfigurationDocument | None
    records_processed: int
    bytes_processed: int
    duration: Duration

    def __repr__(self) -> str:
        return (
            "WorkAttemptRecord("
            f"work_item_id={self.work_item_id!r}, attempt_number={self.attempt_number!r}, "
            f"started_at={self.started_at!r}, finished_at={self.finished_at!r}, "
            f"runner_kind={self.runner_kind!r}, worker_identity=<redacted>, "
            f"outcome={self.outcome!r}, failure_classification={self.failure_classification!r}, "
            f"records_processed={self.records_processed!r}, "
            f"bytes_processed={self.bytes_processed!r}, duration={self.duration!r}, "
            "redacted_detail=<redacted>, result_reference=<redacted>)"
        )


@dataclass(frozen=True, slots=True, repr=False)
class WorkCompletion:
    target_state: WorkItemState
    finished_at: UtcTimestamp
    retry_available_at: UtcTimestamp | None
    failure_classification: FailureClassification | None
    redacted_detail: str | None
    result_reference: ConfigurationDocument | None
    records_processed: int
    bytes_processed: int


@dataclass(frozen=True, slots=True)
class CompletedWork:
    work_item: WorkItemRecord
    attempt: WorkAttemptRecord


@dataclass(frozen=True, slots=True)
class WorkItemPage:
    items: tuple[WorkItemRecord, ...]
    next_cursor: WorkItemId | None


@dataclass(frozen=True, slots=True)
class WorkAttemptPage:
    items: tuple[WorkAttemptRecord, ...]
    next_cursor: AttemptNumber | None


class RunRepository(Protocol):
    """Persistence operations for captured runs and their initial aggregates."""

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
    ) -> RunRecord: ...

    def get(self, run_id: RunId) -> RunRecord | None: ...

    def list(
        self,
        *,
        limit: int,
        after: RunId | None = None,
        state: RunState | None = None,
    ) -> RunPage: ...

    def transition(
        self,
        run_id: RunId,
        *,
        expected_row_version: int,
        target_state: RunState,
        transitioned_at: UtcTimestamp,
        final_reconciliation_fingerprint: StateFingerprint | None = None,
    ) -> RunRecord: ...

    def mark_recovery_started(
        self,
        run_id: RunId,
        *,
        expected_row_version: int,
        started_at: UtcTimestamp,
    ) -> RunRecord: ...

    def mark_recovered(
        self,
        run_id: RunId,
        *,
        expected_row_version: int,
        recovered_at: UtcTimestamp,
    ) -> RunRecord: ...

    def get_event_counter(self, run_id: RunId) -> RunEventCounterRecord | None: ...

    def get_node(self, run_id: RunId, node_id: NodeId) -> RunNodeRecord | None: ...

    def list_nodes(
        self,
        run_id: RunId,
        *,
        limit: int,
        after: NodeId | None = None,
    ) -> RunNodePage: ...


class WorkItemRepository(Protocol):
    """Persistence operations for work items, claims, and immutable completions."""

    def create(
        self,
        *,
        work_item_id: WorkItemId,
        run_id: RunId,
        node_id: NodeId,
        partition_key: PartitionKey,
        input_reference: ConfigurationDocument | None,
        created_at: UtcTimestamp,
    ) -> WorkItemRecord: ...

    def get(self, work_item_id: WorkItemId) -> WorkItemRecord | None: ...

    def list_for_run(
        self,
        run_id: RunId,
        *,
        limit: int,
        after: WorkItemId | None = None,
        state: WorkItemState | None = None,
    ) -> WorkItemPage: ...

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
    ) -> WorkClaim: ...

    def renew_claim(
        self,
        claim: WorkClaim,
        *,
        renewed_at: UtcTimestamp,
        lease_expires_at: UtcTimestamp,
    ) -> WorkClaim: ...

    def complete_claim(self, claim: WorkClaim, completion: WorkCompletion) -> CompletedWork: ...

    def recover_expired_claim(
        self,
        work_item_id: WorkItemId,
        *,
        expected_row_version: int,
        expected_attempt_number: AttemptNumber,
        observed_at: UtcTimestamp,
        retry_available_at: UtcTimestamp,
        redacted_detail: str | None = None,
    ) -> CompletedWork: ...


class WorkAttemptRepository(Protocol):
    """Read-only access to immutable completed attempt history."""

    def get(
        self, work_item_id: WorkItemId, attempt_number: AttemptNumber
    ) -> WorkAttemptRecord | None: ...

    def list_for_work_item(
        self,
        work_item_id: WorkItemId,
        *,
        limit: int,
        after: AttemptNumber | None = None,
    ) -> WorkAttemptPage: ...


def validate_execution_page_limit(limit: object) -> int:
    """Validate an execution collection page size without coercion."""
    if type(limit) is not int or not 1 <= limit <= MAX_EXECUTION_PAGE_SIZE:
        raise ExecutionInvalidRequestError(
            f"page limit must be an integer between 1 and {MAX_EXECUTION_PAGE_SIZE}"
        )
    return limit
