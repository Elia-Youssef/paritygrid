"""Closed execution commands accepted by the transactional writer."""

from dataclasses import dataclass

from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    CheckpointCommit,
    ExecutionEventBatch,
)
from paritygrid.application.ports.execution import (
    CompletedWork,
    RunNodeRecord,
    RunRecord,
    WorkClaim,
    WorkCompletion,
    WorkItemRecord,
)
from paritygrid.application.ports.run_aggregates import WorkMetricDelta
from paritygrid.application.ports.writer import EventAppendRequest, WriterCommandKind
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    ArtifactId,
    AttemptNumber,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    StateFingerprint,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey


@dataclass(frozen=True, slots=True, repr=False)
class CheckpointWrite:
    """Checkpoint fields installed after a winning work completion."""

    expected_head_row_version: int
    payload_schema_version: int
    source_cursor: ConfigurationDocument | None
    output_position: ConfigurationDocument | None
    artifact_id: ArtifactId | None
    committed_at: UtcTimestamp


@dataclass(frozen=True, slots=True, repr=False)
class CreateCapturedRun:
    run_id: RunId
    pipeline_id: PipelineId
    pipeline_version: PipelineVersion
    runner_kind: str
    runner_configuration: ConfigurationDocument
    scenario_seed: int | None
    node_ids: tuple[NodeId, ...]
    created_at: UtcTimestamp
    event: EventAppendRequest

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.CREATE_CAPTURED_RUN


@dataclass(frozen=True, slots=True, repr=False)
class TransitionRun:
    run_id: RunId
    expected_run_row_version: int
    target_state: RunState
    transitioned_at: UtcTimestamp
    final_reconciliation_fingerprint: StateFingerprint | None
    event: EventAppendRequest

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.TRANSITION_RUN


@dataclass(frozen=True, slots=True, repr=False)
class BootstrapWork:
    run_id: RunId
    node_id: NodeId
    work_item_id: WorkItemId
    partition_key: PartitionKey
    input_reference: ConfigurationDocument | None
    created_at: UtcTimestamp
    expected_node_row_version: int
    expected_run_row_version: int
    event: EventAppendRequest

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.BOOTSTRAP_WORK


@dataclass(frozen=True, slots=True, repr=False)
class ClaimWork:
    run_id: RunId
    node_id: NodeId
    work_item_id: WorkItemId
    expected_work_row_version: int
    expected_node_row_version: int
    expected_run_row_version: int
    lease_owner: str
    started_at: UtcTimestamp
    lease_expires_at: UtcTimestamp
    runner_kind: str
    worker_identity: str
    event: EventAppendRequest

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.CLAIM_WORK


@dataclass(frozen=True, slots=True, repr=False)
class RenewWorkClaim:
    run_id: RunId
    node_id: NodeId
    claim: WorkClaim
    expected_run_row_version: int
    renewed_at: UtcTimestamp
    lease_expires_at: UtcTimestamp
    event: EventAppendRequest

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.RENEW_WORK_CLAIM


@dataclass(frozen=True, slots=True, repr=False)
class CommitWorkAttempt:
    run_id: RunId
    node_id: NodeId
    claim: WorkClaim
    completion: WorkCompletion
    metrics: WorkMetricDelta
    expected_node_row_version: int
    expected_run_row_version: int
    event: EventAppendRequest

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.COMMIT_WORK_ATTEMPT


@dataclass(frozen=True, slots=True, repr=False)
class CommitWorkWithCheckpoint:
    run_id: RunId
    node_id: NodeId
    claim: WorkClaim
    completion: WorkCompletion
    checkpoint: CheckpointWrite
    metrics: WorkMetricDelta
    expected_node_row_version: int
    expected_run_row_version: int
    event: EventAppendRequest

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.COMMIT_WORK_WITH_CHECKPOINT


@dataclass(frozen=True, slots=True, repr=False)
class RecoverExpiredWork:
    run_id: RunId
    node_id: NodeId
    work_item_id: WorkItemId
    expected_work_row_version: int
    expected_attempt_number: AttemptNumber
    observed_at: UtcTimestamp
    retry_available_at: UtcTimestamp
    redacted_detail: str | None
    expected_node_row_version: int
    expected_run_row_version: int
    event: EventAppendRequest

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.RECOVER_EXPIRED_WORK


@dataclass(frozen=True, slots=True, repr=False)
class FinalizeEmptyRunNode:
    run_id: RunId
    node_id: NodeId
    expected_node_row_version: int
    expected_run_row_version: int
    finalized_at: UtcTimestamp
    event: EventAppendRequest

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.FINALIZE_EMPTY_RUN_NODE


@dataclass(frozen=True, slots=True)
class CreateCapturedRunResult:
    run: RunRecord
    events: ExecutionEventBatch

    @property
    def result_kind(self) -> WriterCommandKind:
        return WriterCommandKind.CREATE_CAPTURED_RUN


@dataclass(frozen=True, slots=True)
class TransitionRunResult:
    run: RunRecord
    events: ExecutionEventBatch

    @property
    def result_kind(self) -> WriterCommandKind:
        return WriterCommandKind.TRANSITION_RUN


@dataclass(frozen=True, slots=True)
class BootstrapWorkResult:
    work: WorkItemRecord
    node: RunNodeRecord
    events: ExecutionEventBatch
    run: RunRecord

    @property
    def result_kind(self) -> WriterCommandKind:
        return WriterCommandKind.BOOTSTRAP_WORK


@dataclass(frozen=True, slots=True)
class ClaimWorkResult:
    claim: WorkClaim
    node: RunNodeRecord
    events: ExecutionEventBatch
    run: RunRecord

    @property
    def result_kind(self) -> WriterCommandKind:
        return WriterCommandKind.CLAIM_WORK


@dataclass(frozen=True, slots=True)
class RenewWorkClaimResult:
    claim: WorkClaim
    events: ExecutionEventBatch
    run: RunRecord

    @property
    def result_kind(self) -> WriterCommandKind:
        return WriterCommandKind.RENEW_WORK_CLAIM


@dataclass(frozen=True, slots=True)
class CommitWorkResult:
    completed: CompletedWork
    node: RunNodeRecord
    checkpoint: CheckpointCommit | None
    events: ExecutionEventBatch
    run: RunRecord

    @property
    def result_kind(self) -> WriterCommandKind:
        return (
            WriterCommandKind.COMMIT_WORK_ATTEMPT
            if self.checkpoint is None
            else WriterCommandKind.COMMIT_WORK_WITH_CHECKPOINT
        )


@dataclass(frozen=True, slots=True)
class RecoverExpiredWorkResult:
    completed: CompletedWork
    node: RunNodeRecord
    events: ExecutionEventBatch
    run: RunRecord

    @property
    def result_kind(self) -> WriterCommandKind:
        return WriterCommandKind.RECOVER_EXPIRED_WORK


@dataclass(frozen=True, slots=True)
class FinalizeEmptyRunNodeResult:
    node: RunNodeRecord
    events: ExecutionEventBatch
    run: RunRecord

    @property
    def result_kind(self) -> WriterCommandKind:
        return WriterCommandKind.FINALIZE_EMPTY_RUN_NODE


__all__ = [
    "BootstrapWork",
    "BootstrapWorkResult",
    "CheckpointWrite",
    "ClaimWork",
    "ClaimWorkResult",
    "CommitWorkAttempt",
    "CommitWorkResult",
    "CommitWorkWithCheckpoint",
    "CreateCapturedRun",
    "CreateCapturedRunResult",
    "FinalizeEmptyRunNode",
    "FinalizeEmptyRunNodeResult",
    "RecoverExpiredWork",
    "RecoverExpiredWorkResult",
    "RenewWorkClaim",
    "RenewWorkClaimResult",
    "TransitionRun",
    "TransitionRunResult",
]
