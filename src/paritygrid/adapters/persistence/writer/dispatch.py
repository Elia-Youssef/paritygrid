"""Validation and ordered dispatch for the closed transactional write set."""

from dataclasses import dataclass
from typing import cast

from sqlalchemy.orm import Session

from paritygrid.adapters.persistence.repositories.audits import SqlAlchemyAuditRepository
from paritygrid.adapters.persistence.repositories.checkpoints import (
    SqlAlchemyCheckpointRepository,
)
from paritygrid.adapters.persistence.repositories.execution_events import (
    SqlAlchemyExecutionEventRepository,
)
from paritygrid.adapters.persistence.repositories.repairs import SqlAlchemyRepairRepository
from paritygrid.adapters.persistence.repositories.run_node_aggregates import (
    SqlAlchemyRunNodeAggregateRepository,
)
from paritygrid.adapters.persistence.repositories.run_revisions import (
    SqlAlchemyRunRevisionRepository,
)
from paritygrid.adapters.persistence.repositories.runs import SqlAlchemyRunRepository
from paritygrid.adapters.persistence.repositories.work_items import (
    SqlAlchemyWorkItemRepository,
)
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    CheckpointVersion,
    EventSequence,
    EventSubjectKind,
    ExecutionEventBatch,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.execution import (
    RunRecord,
    WorkClaim,
    WorkCompletion,
    WorkItemRecord,
)
from paritygrid.application.ports.repair_audit import (
    AppliedRepairAction,
    AuditEntryRecord,
    PendingAuditEntry,
    RepairActionKeyMap,
    RepairActionStatus,
    RepairApplicationBeginDisposition,
    RepairApplicationReservation,
    RepairApplicationResult,
    RepairPlanAggregate,
    RepairPlanStatus,
)
from paritygrid.application.ports.run_aggregates import WorkMetricDelta
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    WriterCommand,
    WriterCommandResult,
    WriterInvalidRequestError,
)
from paritygrid.application.writes.execution import (
    BootstrapWork,
    BootstrapWorkResult,
    CheckpointWrite,
    ClaimWork,
    ClaimWorkResult,
    CommitWorkAttempt,
    CommitWorkResult,
    CommitWorkWithCheckpoint,
    CreateCapturedRun,
    CreateCapturedRunResult,
    FinalizeEmptyRunNode,
    FinalizeEmptyRunNodeResult,
    RecoverExpiredWork,
    RecoverExpiredWorkResult,
    RenewWorkClaim,
    RenewWorkClaimResult,
    TransitionRun,
    TransitionRunResult,
)
from paritygrid.application.writes.repairs import (
    ApproveRepairPlan,
    BeginRepairApplication,
    BeginRepairApplicationResult,
    CompleteRepairApplication,
    CreateRepairPlan,
    RecordRepairActionApplied,
    RecordRepairActionFailed,
    RejectRepairPlan,
    RepairActionAppliedResult,
    RepairCompanions,
    RepairMutationResult,
)
from paritygrid.domain.execution import RunState, WorkItemState
from paritygrid.domain.models import (
    AttemptNumber,
    NodeId,
    PipelineId,
    PipelineVersion,
    RepairActionId,
    RepairPlanId,
    RunId,
    StateFingerprint,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey
from paritygrid.domain.repair import RepairPlan

type ExecutionCommand = (
    CreateCapturedRun
    | TransitionRun
    | BootstrapWork
    | ClaimWork
    | RenewWorkClaim
    | CommitWorkAttempt
    | CommitWorkWithCheckpoint
    | RecoverExpiredWork
    | FinalizeEmptyRunNode
)
type RepairCommand = (
    CreateRepairPlan
    | ApproveRepairPlan
    | RejectRepairPlan
    | BeginRepairApplication
    | RecordRepairActionApplied
    | RecordRepairActionFailed
    | CompleteRepairApplication
)
type ClosedCommand = ExecutionCommand | RepairCommand

_COMMAND_TYPES = (
    CreateCapturedRun,
    TransitionRun,
    BootstrapWork,
    ClaimWork,
    RenewWorkClaim,
    CommitWorkAttempt,
    CommitWorkWithCheckpoint,
    RecoverExpiredWork,
    FinalizeEmptyRunNode,
    CreateRepairPlan,
    ApproveRepairPlan,
    RejectRepairPlan,
    BeginRepairApplication,
    RecordRepairActionApplied,
    RecordRepairActionFailed,
    CompleteRepairApplication,
)


@dataclass(frozen=True, slots=True)
class DispatchOutcome:
    result: WriterCommandResult
    mutated: bool = True


def validate_command(command: WriterCommand) -> ClosedCommand:
    """Validate command relationships that require no database access."""
    if type(command) not in _COMMAND_TYPES:
        raise WriterInvalidRequestError("writer command type is not supported")
    closed = cast(ClosedCommand, command)
    if type(closed.run_id) is not RunId:
        raise WriterInvalidRequestError("writer command requires RunId")
    if isinstance(closed, (CreateCapturedRun, TransitionRun)):
        _validate_run_command(closed)
    elif isinstance(closed, BootstrapWork):
        _exact(closed.node_id, NodeId, "node identity")
        _exact(closed.work_item_id, WorkItemId, "work identity")
        _exact(closed.partition_key, PartitionKey, "partition key")
        _optional_document(closed.input_reference, "work input reference")
        _exact(closed.created_at, UtcTimestamp, "work creation time")
        _validate_work_event(closed.event, closed.run_id, closed.work_item_id, "work_created")
        _positive(closed.expected_node_row_version, "node row version")
        _positive(closed.expected_run_row_version, "run row version")
    elif isinstance(closed, ClaimWork):
        _exact(closed.node_id, NodeId, "node identity")
        _exact(closed.work_item_id, WorkItemId, "work identity")
        _bounded_text(closed.lease_owner, "lease owner")
        _exact(closed.started_at, UtcTimestamp, "claim start time")
        _exact(closed.lease_expires_at, UtcTimestamp, "lease expiry time")
        _bounded_text(closed.runner_kind, "runner kind")
        _bounded_text(closed.worker_identity, "worker identity")
        _validate_work_event(closed.event, closed.run_id, closed.work_item_id, "work_claimed")
        _positive(closed.expected_work_row_version, "work row version")
        _positive(closed.expected_node_row_version, "node row version")
        _positive(closed.expected_run_row_version, "run row version")
    elif isinstance(closed, RenewWorkClaim):
        _exact(closed.node_id, NodeId, "node identity")
        claim = _exact(closed.claim, WorkClaim, "work claim")
        _exact(closed.renewed_at, UtcTimestamp, "claim renewal time")
        _exact(closed.lease_expires_at, UtcTimestamp, "lease expiry time")
        _validate_work_event(closed.event, closed.run_id, claim.work_item_id, "work_claim_renewed")
        _positive(closed.expected_run_row_version, "run row version")
    elif isinstance(closed, (CommitWorkAttempt, CommitWorkWithCheckpoint)):
        _validate_completion_command(closed)
    elif isinstance(closed, RecoverExpiredWork):
        _exact(closed.node_id, NodeId, "node identity")
        _exact(closed.work_item_id, WorkItemId, "work identity")
        _exact(closed.expected_attempt_number, AttemptNumber, "attempt number")
        _exact(closed.observed_at, UtcTimestamp, "lease observation time")
        _exact(closed.retry_available_at, UtcTimestamp, "retry availability time")
        if closed.redacted_detail is not None:
            _bounded_text(closed.redacted_detail, "recovery detail")
        _validate_work_event(closed.event, closed.run_id, closed.work_item_id, "work_lease_expired")
        _positive(closed.expected_work_row_version, "work row version")
        _positive(closed.expected_node_row_version, "node row version")
        _positive(closed.expected_run_row_version, "run row version")
    elif isinstance(closed, FinalizeEmptyRunNode):
        _exact(closed.node_id, NodeId, "node identity")
        _exact(closed.finalized_at, UtcTimestamp, "node finalization time")
        _validate_run_event(closed.event, closed.run_id, "run_node_succeeded")
        _positive(closed.expected_node_row_version, "node row version")
        _positive(closed.expected_run_row_version, "run row version")
    else:
        _validate_repair_command(closed)
    return closed


def dispatch_command(session: Session, command: WriterCommand) -> DispatchOutcome:
    """Execute one validated command in the caller-owned transaction."""
    closed = validate_command(command)
    if isinstance(closed, CreateCapturedRun):
        return _create_run(session, closed)
    if isinstance(closed, TransitionRun):
        return _transition_run(session, closed)
    if isinstance(closed, BootstrapWork):
        return _bootstrap_work(session, closed)
    if isinstance(closed, ClaimWork):
        return _claim_work(session, closed)
    if isinstance(closed, RenewWorkClaim):
        return _renew_claim(session, closed)
    if isinstance(closed, CommitWorkAttempt):
        return _commit_without_checkpoint(session, closed)
    if isinstance(closed, CommitWorkWithCheckpoint):
        return _commit_with_checkpoint(session, closed)
    if isinstance(closed, RecoverExpiredWork):
        return _recover_work(session, closed)
    if isinstance(closed, FinalizeEmptyRunNode):
        return _finalize_empty(session, closed)
    return _dispatch_repair(session, closed)


def _create_run(session: Session, command: CreateCapturedRun) -> DispatchOutcome:
    run = SqlAlchemyRunRepository(session).create(
        run_id=command.run_id,
        pipeline_id=command.pipeline_id,
        pipeline_version=command.pipeline_version,
        runner_kind=command.runner_kind,
        runner_configuration=command.runner_configuration,
        scenario_seed=command.scenario_seed,
        node_ids=command.node_ids,
        created_at=command.created_at,
    )
    events = _append_event(session, command.run_id, command.event)
    return DispatchOutcome(CreateCapturedRunResult(run, events))


def _transition_run(session: Session, command: TransitionRun) -> DispatchOutcome:
    run = SqlAlchemyRunRepository(session).transition(
        command.run_id,
        expected_row_version=command.expected_run_row_version,
        target_state=command.target_state,
        transitioned_at=command.transitioned_at,
        final_reconciliation_fingerprint=command.final_reconciliation_fingerprint,
    )
    events = _append_event(session, command.run_id, command.event)
    return DispatchOutcome(TransitionRunResult(run, events))


def _bootstrap_work(session: Session, command: BootstrapWork) -> DispatchOutcome:
    work = SqlAlchemyWorkItemRepository(session).create(
        work_item_id=command.work_item_id,
        run_id=command.run_id,
        node_id=command.node_id,
        partition_key=command.partition_key,
        input_reference=command.input_reference,
        created_at=command.created_at,
    )
    _require_work_parent(work, command.run_id, command.node_id)
    events = _append_event(session, command.run_id, command.event)
    node = SqlAlchemyRunNodeAggregateRepository(session).register_work(
        work, expected_node_row_version=command.expected_node_row_version
    )
    run = _advance_run(session, command.run_id, command.expected_run_row_version)
    return DispatchOutcome(BootstrapWorkResult(work, node, events, run))


def _claim_work(session: Session, command: ClaimWork) -> DispatchOutcome:
    claim = SqlAlchemyWorkItemRepository(session).claim(
        command.work_item_id,
        expected_row_version=command.expected_work_row_version,
        lease_owner=command.lease_owner,
        started_at=command.started_at,
        lease_expires_at=command.lease_expires_at,
        runner_kind=command.runner_kind,
        worker_identity=command.worker_identity,
    )
    _require_claim_parent(session, claim, command.run_id, command.node_id)
    events = _append_event(session, command.run_id, command.event)
    node = SqlAlchemyRunNodeAggregateRepository(session).apply_claim(
        claim, expected_node_row_version=command.expected_node_row_version
    )
    run = _advance_run(session, command.run_id, command.expected_run_row_version)
    return DispatchOutcome(ClaimWorkResult(claim, node, events, run))


def _renew_claim(session: Session, command: RenewWorkClaim) -> DispatchOutcome:
    claim = SqlAlchemyWorkItemRepository(session).renew_claim(
        command.claim,
        renewed_at=command.renewed_at,
        lease_expires_at=command.lease_expires_at,
    )
    _require_claim_parent(session, claim, command.run_id, command.node_id)
    events = _append_event(session, command.run_id, command.event)
    run = _advance_run(session, command.run_id, command.expected_run_row_version)
    return DispatchOutcome(RenewWorkClaimResult(claim, events, run))


def _commit_without_checkpoint(session: Session, command: CommitWorkAttempt) -> DispatchOutcome:
    completed = SqlAlchemyWorkItemRepository(session).complete_claim(
        command.claim, command.completion
    )
    _require_work_parent(completed.work_item, command.run_id, command.node_id)
    events = _append_event(session, command.run_id, command.event)
    node = SqlAlchemyRunNodeAggregateRepository(session).apply_completion(
        completed,
        checkpoint=None,
        expected_node_row_version=command.expected_node_row_version,
        metrics=command.metrics,
    )
    run = _advance_run(session, command.run_id, command.expected_run_row_version)
    return DispatchOutcome(CommitWorkResult(completed, node, None, events, run))


def _commit_with_checkpoint(session: Session, command: CommitWorkWithCheckpoint) -> DispatchOutcome:
    completed = SqlAlchemyWorkItemRepository(session).complete_claim(
        command.claim, command.completion
    )
    _require_work_parent(completed.work_item, command.run_id, command.node_id)
    checkpoint = SqlAlchemyCheckpointRepository(session).append(
        command.run_id,
        command.node_id,
        completed.work_item.partition_key,
        expected_current_version=CheckpointVersion(completed.work_item.expected_checkpoint_version),
        expected_head_row_version=command.checkpoint.expected_head_row_version,
        expected_work_row_version=completed.work_item.row_version,
        payload_schema_version=command.checkpoint.payload_schema_version,
        source_cursor=command.checkpoint.source_cursor,
        output_position=command.checkpoint.output_position,
        artifact_id=command.checkpoint.artifact_id,
        committed_at=command.checkpoint.committed_at,
    )
    events = _append_event(session, command.run_id, command.event)
    node = SqlAlchemyRunNodeAggregateRepository(session).apply_completion(
        completed,
        checkpoint=checkpoint,
        expected_node_row_version=command.expected_node_row_version,
        metrics=command.metrics,
    )
    run = _advance_run(session, command.run_id, command.expected_run_row_version)
    return DispatchOutcome(CommitWorkResult(completed, node, checkpoint, events, run))


def _recover_work(session: Session, command: RecoverExpiredWork) -> DispatchOutcome:
    completed = SqlAlchemyWorkItemRepository(session).recover_expired_claim(
        command.work_item_id,
        expected_row_version=command.expected_work_row_version,
        expected_attempt_number=command.expected_attempt_number,
        observed_at=command.observed_at,
        retry_available_at=command.retry_available_at,
        redacted_detail=command.redacted_detail,
    )
    _require_work_parent(completed.work_item, command.run_id, command.node_id)
    events = _append_event(session, command.run_id, command.event)
    node = SqlAlchemyRunNodeAggregateRepository(session).apply_recovery(
        completed, expected_node_row_version=command.expected_node_row_version
    )
    run = _advance_run(session, command.run_id, command.expected_run_row_version)
    return DispatchOutcome(RecoverExpiredWorkResult(completed, node, events, run))


def _finalize_empty(session: Session, command: FinalizeEmptyRunNode) -> DispatchOutcome:
    events = _append_event(session, command.run_id, command.event)
    node = SqlAlchemyRunNodeAggregateRepository(session).finalize_empty(
        command.run_id,
        command.node_id,
        expected_node_row_version=command.expected_node_row_version,
        finalized_at=command.finalized_at,
    )
    run = _advance_run(session, command.run_id, command.expected_run_row_version)
    return DispatchOutcome(FinalizeEmptyRunNodeResult(node, events, run))


def _dispatch_repair(session: Session, command: RepairCommand) -> DispatchOutcome:
    repairs = SqlAlchemyRepairRepository(session)
    if isinstance(command, CreateRepairPlan):
        replay = repairs.get(command.plan.plan_id) is not None
        aggregate = repairs.create_plan(
            run_id=command.run_id,
            plan=command.plan,
            action_keys=command.action_keys,
            created_at=command.created_at,
        )
        _require_repair_parent(aggregate, command.run_id)
        return _repair_mutation(session, command, aggregate, replay=replay)
    if isinstance(command, ApproveRepairPlan):
        prior = repairs.get(command.repair_plan_id)
        replay = prior is not None and prior.approval is not None
        aggregate = repairs.approve(
            command.repair_plan_id,
            expected_row_version=command.expected_plan_row_version,
            current_reconciliation_fingerprint=command.current_reconciliation_fingerprint,
            approved_by=command.approved_by,
            approved_at=command.approved_at,
            correlation_id=command.correlation_id,
            schema_version=command.schema_version,
            detail=command.detail,
        )
        _require_repair_parent(aggregate, command.run_id)
        return _repair_mutation(session, command, aggregate, replay=replay)
    if isinstance(command, RejectRepairPlan):
        prior = repairs.get(command.repair_plan_id)
        replay = prior is not None and prior.plan.status is RepairPlanStatus.REJECTED
        aggregate = repairs.reject(
            command.repair_plan_id,
            expected_row_version=command.expected_plan_row_version,
            rejected_at=command.rejected_at,
        )
        _require_repair_parent(aggregate, command.run_id)
        return _repair_mutation(session, command, aggregate, replay=replay)
    if isinstance(command, BeginRepairApplication):
        operation = repairs.begin_application(
            command.repair_plan_id,
            expected_row_version=command.expected_plan_row_version,
            current_reconciliation_fingerprint=command.current_reconciliation_fingerprint,
            applying_at=command.applying_at,
        )
        _require_repair_parent(operation.aggregate, command.run_id)
        if operation.disposition is not RepairApplicationBeginDisposition.STARTED:
            _verify_repair_replay(session, command.run_id, command.companions)
            return DispatchOutcome(
                BeginRepairApplicationResult(operation, None, None, None), mutated=False
            )
        audit, events, run = _repair_companions(session, command.run_id, command.companions)
        return DispatchOutcome(BeginRepairApplicationResult(operation, audit, events, run))
    if isinstance(command, RecordRepairActionApplied):
        prior = repairs.get_action(command.repair_action_id)
        replay = prior is not None and prior.status is RepairActionStatus.APPLIED
        operation = repairs.record_action_applied(
            command.reservation,
            command.repair_action_id,
            result=command.result,
            target_version=command.target_version,
            applied_at=command.applied_at,
        )
        _require_repair_action_parent(operation, command.run_id)
        if replay:
            _verify_repair_replay(session, command.run_id, command.companions)
            return DispatchOutcome(RepairActionAppliedResult(operation, None, None, None), False)
        audit, events, run = _repair_companions(session, command.run_id, command.companions)
        return DispatchOutcome(RepairActionAppliedResult(operation, audit, events, run))
    if isinstance(command, RecordRepairActionFailed):
        prior = repairs.get_action(command.repair_action_id)
        replay = prior is not None and prior.status is RepairActionStatus.FAILED
        aggregate = repairs.record_action_failed(
            command.reservation,
            command.repair_action_id,
            result=command.result,
            failed_at=command.failed_at,
            plan_failure=command.plan_failure,
        )
        _require_repair_parent(aggregate, command.run_id)
        return _repair_mutation(session, command, aggregate, replay=replay)
    assert isinstance(command, CompleteRepairApplication)
    prior = repairs.get(command.reservation.repair_plan_id)
    replay = prior is not None and prior.plan.status is RepairPlanStatus.APPLIED
    aggregate = repairs.complete_application(command.reservation, applied_at=command.applied_at)
    _require_repair_parent(aggregate, command.run_id)
    return _repair_mutation(session, command, aggregate, replay=replay)


def _repair_mutation(
    session: Session,
    command: RepairCommand,
    aggregate: RepairPlanAggregate,
    *,
    replay: bool,
) -> DispatchOutcome:
    if replay:
        _verify_repair_replay(session, command.run_id, command.companions)
        return DispatchOutcome(
            RepairMutationResult(command.kind, aggregate, None, None, None), False
        )
    audit, events, run = _repair_companions(session, command.run_id, command.companions)
    return DispatchOutcome(RepairMutationResult(command.kind, aggregate, audit, events, run))


def _repair_companions(
    session: Session,
    run_id: RunId,
    companions: RepairCompanions,
) -> tuple[AuditEntryRecord, ExecutionEventBatch, RunRecord]:
    audit = SqlAlchemyAuditRepository(session).append(companions.audit)
    events = _append_event(session, run_id, companions.event)
    run = _advance_run(session, run_id, companions.expected_run_row_version)
    return audit, events, run


def _verify_repair_replay(session: Session, run_id: RunId, companions: RepairCompanions) -> None:
    SqlAlchemyAuditRepository(session).match_exact(companions.audit)
    _append_event(session, run_id, companions.event)
    run = SqlAlchemyRunRepository(session).get(run_id)
    if run is None or run.row_version != companions.expected_run_row_version + 1:
        raise WriterInvalidRequestError("repair replay is not at its immediate run revision")


def _append_event(
    session: Session, run_id: RunId, request: EventAppendRequest
) -> ExecutionEventBatch:
    return SqlAlchemyExecutionEventRepository(session).append(
        run_id,
        expected_next_sequence=request.expected_next_sequence,
        expected_counter_row_version=request.expected_counter_row_version,
        events=(request.event,),
    )


def _advance_run(session: Session, run_id: RunId, expected: int) -> RunRecord:
    return SqlAlchemyRunRevisionRepository(session).advance(run_id, expected_row_version=expected)


def _require_claim_parent(
    session: Session, claim: WorkClaim, run_id: RunId, node_id: NodeId
) -> None:
    work = SqlAlchemyWorkItemRepository(session).get(claim.work_item_id)
    if work is None:
        raise WriterInvalidRequestError("work claim parent does not exist")
    _require_work_parent(work, run_id, node_id)


def _require_work_parent(work: object, run_id: RunId, node_id: NodeId) -> None:
    if type(work) is not WorkItemRecord:
        raise WriterInvalidRequestError("work mutation returned an invalid record")
    record = work
    if record.run_id != run_id or record.node_id != node_id:
        raise WriterInvalidRequestError("work item belongs to another run or node")


def _require_repair_parent(aggregate: RepairPlanAggregate, run_id: RunId) -> None:
    if aggregate.plan.run_id != run_id or any(
        action.run_id != run_id for action in aggregate.actions
    ):
        raise WriterInvalidRequestError("repair plan belongs to another run")


def _require_repair_action_parent(operation: AppliedRepairAction, run_id: RunId) -> None:
    if operation.action.run_id != run_id or operation.reservation.run_id != run_id:
        raise WriterInvalidRequestError("repair action belongs to another run")


def _validate_run_command(command: CreateCapturedRun | TransitionRun) -> None:
    if isinstance(command, CreateCapturedRun):
        _exact(command.pipeline_id, PipelineId, "pipeline identity")
        _exact(command.pipeline_version, PipelineVersion, "pipeline version")
        _bounded_text(command.runner_kind, "runner kind")
        _exact(command.runner_configuration, ConfigurationDocument, "runner configuration")
        if command.scenario_seed is not None and (
            type(command.scenario_seed) is not int
            or not -9_223_372_036_854_775_808 <= command.scenario_seed <= 9_223_372_036_854_775_807
        ):
            raise WriterInvalidRequestError("scenario seed is outside the supported range")
        if (
            type(command.node_ids) is not tuple
            or not command.node_ids
            or any(type(node_id) is not NodeId for node_id in command.node_ids)
            or len(set(command.node_ids)) != len(command.node_ids)
        ):
            raise WriterInvalidRequestError("run node identities are invalid")
        _exact(command.created_at, UtcTimestamp, "run creation time")
        _validate_run_event(command.event, command.run_id, "run_created")
        if command.event.expected_next_sequence != EventSequence(1):
            raise WriterInvalidRequestError("created run event must start at sequence one")
        if command.event.expected_counter_row_version != 1:
            raise WriterInvalidRequestError(
                "created run event counter must start at row version one"
            )
        return
    _positive(command.expected_run_row_version, "run row version")
    _exact(command.target_state, RunState, "run transition target")
    _exact(command.transitioned_at, UtcTimestamp, "run transition time")
    expected_kind = {
        RunState.RUNNING: "run_started",
        RunState.PAUSING: "run_pausing",
        RunState.PAUSED: "run_paused",
        RunState.RESUMING: "run_resuming",
        RunState.CANCELLING: "run_cancelling",
        RunState.CANCELLED: "run_cancelled",
        RunState.SUCCEEDED: "run_succeeded",
        RunState.PARTIALLY_SUCCEEDED: "run_partially_succeeded",
        RunState.FAILED: "run_failed",
    }.get(command.target_state)
    if expected_kind is None:
        raise WriterInvalidRequestError("run transition target is invalid")
    _validate_run_event(command.event, command.run_id, expected_kind)


def _validate_completion_command(
    command: CommitWorkAttempt | CommitWorkWithCheckpoint,
) -> None:
    if type(command.completion) is not WorkCompletion:
        raise WriterInvalidRequestError("work completion type is invalid")
    if type(command.metrics) is not WorkMetricDelta:
        raise WriterInvalidRequestError("work metric delta type is invalid")
    _exact(command.node_id, NodeId, "node identity")
    claim = _exact(command.claim, WorkClaim, "work claim")
    _positive(command.expected_node_row_version, "node row version")
    _positive(command.expected_run_row_version, "run row version")
    target = command.completion.target_state
    if isinstance(command, CommitWorkAttempt):
        if target is WorkItemState.SUCCEEDED:
            raise WriterInvalidRequestError("successful work requires a checkpoint")
        event_kind = f"work_{target.value}"
    else:
        if target is not WorkItemState.SUCCEEDED:
            raise WriterInvalidRequestError("checkpointed work must succeed")
        if type(command.checkpoint) is not CheckpointWrite:
            raise WriterInvalidRequestError("checkpoint write type is invalid")
        _positive(command.checkpoint.expected_head_row_version, "checkpoint row version")
        _positive(command.checkpoint.payload_schema_version, "checkpoint schema version")
        _optional_document(command.checkpoint.source_cursor, "checkpoint source cursor")
        _optional_document(command.checkpoint.output_position, "checkpoint output position")
        _exact(command.checkpoint.committed_at, UtcTimestamp, "checkpoint commit time")
        event_kind = "checkpoint_committed"
    _validate_work_event(command.event, command.run_id, claim.work_item_id, event_kind)


def _validate_repair_command(command: RepairCommand) -> None:
    companions = _exact(command.companions, RepairCompanions, "repair companions")
    _positive(companions.expected_run_row_version, "run row version")
    if isinstance(command, CreateRepairPlan):
        plan = _exact(command.plan, RepairPlan, "repair plan")
        _exact(command.action_keys, RepairActionKeyMap, "repair action keys")
        _exact(command.created_at, UtcTimestamp, "repair creation time")
        plan_id = plan.plan_id
        object_kind = "repair_plan"
        event_kind = operation = "repair_plan_created"
        occurred_at = command.created_at
    elif isinstance(command, ApproveRepairPlan):
        plan_id = _exact(command.repair_plan_id, RepairPlanId, "repair plan identity")
        _positive(command.expected_plan_row_version, "repair plan row version")
        _exact(
            command.current_reconciliation_fingerprint,
            StateFingerprint,
            "reconciliation fingerprint",
        )
        _bounded_text(command.approved_by, "repair approver")
        _exact(command.approved_at, UtcTimestamp, "repair approval time")
        _bounded_text(command.correlation_id, "repair correlation")
        _positive(command.schema_version, "repair approval schema version")
        _exact(command.detail, RedactedDocument, "repair approval detail")
        object_kind = "repair_plan"
        event_kind = operation = "repair_plan_approved"
        occurred_at = command.approved_at
    elif isinstance(command, RejectRepairPlan):
        plan_id = _exact(command.repair_plan_id, RepairPlanId, "repair plan identity")
        _positive(command.expected_plan_row_version, "repair plan row version")
        _exact(command.rejected_at, UtcTimestamp, "repair rejection time")
        object_kind = "repair_plan"
        event_kind = operation = "repair_plan_rejected"
        occurred_at = command.rejected_at
    elif isinstance(command, BeginRepairApplication):
        plan_id = _exact(command.repair_plan_id, RepairPlanId, "repair plan identity")
        _positive(command.expected_plan_row_version, "repair plan row version")
        _exact(
            command.current_reconciliation_fingerprint,
            StateFingerprint,
            "reconciliation fingerprint",
        )
        _exact(command.applying_at, UtcTimestamp, "repair application start time")
        object_kind = "repair_plan"
        event_kind = operation = "repair_application_started"
        occurred_at = command.applying_at
    elif isinstance(command, RecordRepairActionApplied):
        reservation = _exact(
            command.reservation,
            RepairApplicationReservation,
            "repair application reservation",
        )
        _validate_reservation(command.run_id, reservation.run_id)
        _exact(command.repair_action_id, RepairActionId, "repair action identity")
        _exact(command.result, RepairApplicationResult, "repair application result")
        _positive(command.target_version, "repair target version")
        _exact(command.applied_at, UtcTimestamp, "repair action application time")
        plan_id = reservation.repair_plan_id
        object_kind = "repair_action"
        event_kind = operation = "repair_action_applied"
        occurred_at = command.applied_at
    elif isinstance(command, RecordRepairActionFailed):
        reservation = _exact(
            command.reservation,
            RepairApplicationReservation,
            "repair application reservation",
        )
        _validate_reservation(command.run_id, reservation.run_id)
        _exact(command.repair_action_id, RepairActionId, "repair action identity")
        _exact(command.result, RepairApplicationResult, "repair application result")
        _exact(command.failed_at, UtcTimestamp, "repair action failure time")
        _exact(command.plan_failure, RedactedDocument, "repair plan failure detail")
        plan_id = reservation.repair_plan_id
        object_kind = "repair_action"
        event_kind = operation = "repair_action_failed"
        occurred_at = command.failed_at
    else:
        reservation = _exact(
            command.reservation,
            RepairApplicationReservation,
            "repair application reservation",
        )
        _validate_reservation(command.run_id, reservation.run_id)
        _exact(command.applied_at, UtcTimestamp, "repair completion time")
        plan_id = reservation.repair_plan_id
        object_kind = "repair_plan"
        event_kind = operation = "repair_application_completed"
        occurred_at = command.applied_at
    object_id = (
        command.repair_action_id
        if isinstance(command, (RecordRepairActionApplied, RecordRepairActionFailed))
        else plan_id
    )
    _validate_repair_companions(
        command.companions,
        command.run_id,
        event_kind,
        operation,
        object_kind,
        object_id,
        occurred_at,
    )


def _validate_reservation(command_run: RunId, reservation_run: RunId) -> None:
    if command_run != reservation_run:
        raise WriterInvalidRequestError("repair reservation belongs to another run")


def _validate_repair_companions(
    companions: RepairCompanions,
    run_id: RunId,
    event_kind: str,
    operation: str,
    object_kind: str,
    object_id: RepairPlanId | RepairActionId,
    occurred_at: UtcTimestamp,
) -> None:
    if type(companions) is not RepairCompanions:
        raise WriterInvalidRequestError("repair companion type is invalid")
    audit = companions.audit
    if type(audit) is not PendingAuditEntry:
        raise WriterInvalidRequestError("repair audit type is invalid")
    if (
        audit.operation != operation
        or audit.object_kind != object_kind
        or audit.object_id != str(object_id)
        or audit.occurred_at != occurred_at
    ):
        raise WriterInvalidRequestError("repair audit does not match its command")
    event = companions.event.event
    if audit.correlation_id != event.correlation_id:
        raise WriterInvalidRequestError("repair companion correlation is inconsistent")
    _validate_run_event(companions.event, run_id, event_kind)
    if event.occurred_at != occurred_at:
        raise WriterInvalidRequestError("repair event time does not match its command")


def _validate_run_event(request: EventAppendRequest, run_id: RunId, event_kind: str) -> None:
    event = _validate_event_request(request, event_kind)
    if (
        event.subject_kind is not EventSubjectKind.RUN
        or type(event.subject_id) is not RunId
        or event.subject_id != run_id
    ):
        raise WriterInvalidRequestError("durable event run subject is inconsistent")


def _validate_work_event(
    request: EventAppendRequest,
    run_id: RunId,
    work_item_id: WorkItemId,
    event_kind: str,
) -> None:
    _exact(run_id, RunId, "event run identity")
    event = _validate_event_request(request, event_kind)
    if (
        event.subject_kind is not EventSubjectKind.WORK_ITEM
        or type(event.subject_id) is not WorkItemId
        or event.subject_id != work_item_id
    ):
        raise WriterInvalidRequestError("durable event work subject is inconsistent")


def _validate_event_request(
    request: EventAppendRequest, expected_kind: str
) -> PendingExecutionEvent:
    if type(request) is not EventAppendRequest:
        raise WriterInvalidRequestError("event append request type is invalid")
    if type(request.expected_next_sequence) is not EventSequence:
        raise WriterInvalidRequestError("event sequence type is invalid")
    _positive(request.expected_counter_row_version, "event counter row version")
    if type(request.event) is not PendingExecutionEvent:
        raise WriterInvalidRequestError("pending event type is invalid")
    if request.event.event_kind != expected_kind:
        raise WriterInvalidRequestError("durable event kind does not match its command")
    return request.event


def _positive(value: object, subject: str) -> int:
    if type(value) is not int or not 1 <= value <= 2_147_483_647:
        raise WriterInvalidRequestError(f"{subject} is outside the supported range")
    return value


def _exact[T](value: object, expected: type[T], subject: str) -> T:
    if type(value) is not expected:
        raise WriterInvalidRequestError(f"{subject} has an invalid type")
    return cast(T, value)


def _optional_document(value: object, subject: str) -> None:
    if value is not None:
        _exact(value, ConfigurationDocument, subject)


def _bounded_text(value: object, subject: str) -> str:
    if type(value) is not str or not 1 <= len(value) <= 512:
        raise WriterInvalidRequestError(f"{subject} is outside the supported range")
    assert isinstance(value, str)
    return value


__all__ = ["DispatchOutcome", "dispatch_command", "validate_command"]
