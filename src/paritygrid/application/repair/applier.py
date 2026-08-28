"""Idempotent application of one approved repair plan to the target.

Application owns three fences. First, a plan may only be applied from the
approved state; a completed, failed, or rejected plan is never re-applied
and an interrupted ``applying`` plan resumes instead of restarting. Second,
every target effect uses the plan's durable external idempotency key, so a
replayed or interrupted attempt converges on exactly one logical effect.
Third, each applied effect is recorded through the transactional writer
under the reservation captured when application began, so late or stale
work loses its compare-and-set race instead of double-recording.

Ambiguous target outcomes (post-commit timeout or connection loss) are
resolved by replaying the same idempotency key within a bound; an
unresolved effect suspends application without recording any terminal
state, and recovery is a fresh call to this service.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from paritygrid.application.ports.connector_redaction import redact_exception
from paritygrid.application.ports.connectors import (
    NEVER_CANCELLED,
    ConnectorAmbiguousError,
    ConnectorCallContext,
    ConnectorCancellationToken,
    ConnectorCancelledError,
    ConnectorConflictError,
    ConnectorError,
    ConnectorPermanentError,
    ConnectorRateLimitedError,
    ConnectorRetryableError,
    ConnectorServerFailureError,
    ConnectorTimeoutError,
    ConnectorUnknownError,
    ConnectorValidationError,
    TargetConnector,
    TargetWriteOutcome,
    TargetWritePrecondition,
    TargetWriteRequest,
)
from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.application.ports.repair_audit import (
    AuditSequenceConflictError,
    RepairActionRecord,
    RepairActionStatus,
    RepairApplicationBeginDisposition,
    RepairApplicationConflictError,
    RepairApplicationReservation,
    RepairApplicationResult,
    RepairPlanAggregate,
    RepairPlanStatus,
    RepairStaleRowVersionError,
    RepairStateConflictError,
)
from paritygrid.application.ports.writer import TransactionalWriter, WriterCommand
from paritygrid.application.repair.companions import (
    build_companions,
    frontier_from_evidence,
    submit_command,
)
from paritygrid.application.repair.errors import (
    RepairPlanMismatchError,
    RepairPlanStateError,
    RepairReconciliationMissingError,
    RepairReconciliationStaleError,
    RepairWriterOutcomeUnknownError,
    TargetApplicationError,
)
from paritygrid.application.repair.evidence import RepairWorkflowReader
from paritygrid.application.repair.payloads import render_effect_payload
from paritygrid.application.writes.repairs import (
    BeginRepairApplication,
    BeginRepairApplicationResult,
    CompleteRepairApplication,
    RecordRepairActionApplied,
    RecordRepairActionFailed,
    RepairActionAppliedResult,
    RepairMutationResult,
)
from paritygrid.domain.models import (
    RepairActionId,
    RepairPlanId,
    RunId,
    StateFingerprint,
    UtcTimestamp,
)

APPLICATION_RESULT_SCHEMA_VERSION = 1
_MAX_RATE_LIMIT_WAIT_SECONDS = 60.0


class RepairApplicationDisposition(StrEnum):
    """The closed outcomes of one application attempt."""

    COMPLETED = "completed"
    ALREADY_APPLIED = "already_applied"
    FAILED = "failed"
    UNRESOLVED = "unresolved"
    INTERRUPTED = "interrupted"


@dataclass(frozen=True, slots=True)
class RepairApplicationPolicy:
    """Bounded retry and delay behavior for target effects."""

    max_attempts_per_action: int = 4
    max_ambiguous_replays: int = 3
    max_writer_replays: int = 3
    delay_seconds: float = 0.05
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        for name in (
            "max_attempts_per_action",
            "max_ambiguous_replays",
            "max_writer_replays",
        ):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= 100:
                raise ValueError(f"{name} must be between 1 and 100")
        for name in ("delay_seconds", "timeout_seconds"):
            value = getattr(self, name)
            if type(value) is not float or not 0.0 <= value <= 300.0:
                raise ValueError(f"{name} must be a bounded nonnegative float")


@dataclass(frozen=True, slots=True)
class AppliedEffectEvidence:
    """Bounded evidence for one attempted effect."""

    action_id: RepairActionId
    canonical_key: str
    outcome: str
    attempts: int
    target_version: int | None

    def __post_init__(self) -> None:
        if type(self.outcome) is not str or not 1 <= len(self.outcome) <= 32:
            raise ValueError("applied effect outcome is invalid")
        if self.target_version is not None and (
            type(self.target_version) is not int or self.target_version < 1
        ):
            raise ValueError("applied effect target version is invalid")


@dataclass(frozen=True, slots=True)
class RepairApplicationReport:
    """What one application attempt durably achieved."""

    disposition: RepairApplicationDisposition
    aggregate: RepairPlanAggregate
    effects: tuple[AppliedEffectEvidence, ...]
    resumed: bool
    unresolved_action: RepairActionId | None


@dataclass(frozen=True, slots=True)
class AppliedEffect:
    """One durably recorded applied effect and its advanced reservation."""

    evidence: AppliedEffectEvidence
    reservation: RepairApplicationReservation


@dataclass(frozen=True, slots=True)
class _Suspended:
    """An application stopped without recording a terminal plan state."""

    disposition: RepairApplicationDisposition
    detail: str


class RepairApplicationService:
    """Apply one approved repair plan through the Phase 9 target connector."""

    def __init__(
        self,
        writer: TransactionalWriter,
        reader: RepairWorkflowReader,
        *,
        now: Callable[[], UtcTimestamp],
        policy: RepairApplicationPolicy | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._writer = writer
        self._reader = reader
        self._now = now
        self._policy = policy if policy is not None else RepairApplicationPolicy()
        self._sleep = sleep if sleep is not None else asyncio.sleep

    async def apply(
        self,
        *,
        run_id: RunId,
        repair_plan_id: RepairPlanId,
        target: TargetConnector,
        context_id: str,
        cancellation: ConnectorCancellationToken = NEVER_CANCELLED,
    ) -> RepairApplicationReport:
        """Apply, resume, or observe one plan; never re-apply a terminal plan."""
        if type(run_id) is not RunId or type(repair_plan_id) is not RepairPlanId:
            raise TypeError("repair application requires typed plan and run identities")
        context = ConnectorCallContext(correlation_id=context_id, cancellation_token=cancellation)
        evidence = self._reader.load(run_id)
        if evidence.summary is None:
            raise RepairReconciliationMissingError("the run has no reconciliation snapshot")
        aggregate = self._require_plan(run_id, repair_plan_id)
        plan = aggregate.plan
        status = plan.status
        if status is RepairPlanStatus.APPLIED:
            return RepairApplicationReport(
                disposition=RepairApplicationDisposition.ALREADY_APPLIED,
                aggregate=aggregate,
                effects=(),
                resumed=False,
                unresolved_action=None,
            )
        if status in {RepairPlanStatus.FAILED, RepairPlanStatus.REJECTED}:
            raise RepairPlanStateError(f"repair plan is {status.value} and cannot be applied")
        if status is RepairPlanStatus.PROPOSED:
            raise RepairPlanStateError("repair plan requires approval before application")
        resumed = status is RepairPlanStatus.APPLYING
        if resumed:
            reservation = _reconstruct_reservation(aggregate)
        else:
            current = evidence.summary.reconciliation_fingerprint
            if current != plan.reconciliation_fingerprint:
                raise RepairReconciliationStaleError(
                    expected=current.value, actual=plan.reconciliation_fingerprint.value
                )
            reservation = await self._begin(run_id, repair_plan_id, aggregate, current, context_id)
        effects: list[AppliedEffectEvidence] = []
        for action in aggregate.actions:
            if action.status is RepairActionStatus.APPLIED:
                continue
            if action.status is RepairActionStatus.FAILED:
                raise RepairPlanStateError("repair plan carries a failed action")
            outcome = await self._apply_one(
                action, target, context, reservation, run_id, context_id
            )
            if isinstance(outcome, _Suspended):
                return RepairApplicationReport(
                    disposition=outcome.disposition,
                    aggregate=self._require_plan(run_id, repair_plan_id),
                    effects=tuple(effects),
                    resumed=resumed,
                    unresolved_action=action.effect.action_id,
                )
            effects.append(outcome.evidence)
            reservation = outcome.reservation
        completed_at = self._now()
        companions = build_companions(
            frontier=frontier_from_evidence(self._reader.load(run_id)),
            run_id=run_id,
            operation="repair_application_completed",
            object_kind="repair_plan",
            object_id=repair_plan_id.value,
            actor="repair-operator",
            correlation_id=context_id,
            occurred_at=completed_at,
            payload={
                "action_count": len(aggregate.actions),
                "content_fingerprint": plan.content_fingerprint.value,
                "reconciliation_fingerprint": plan.reconciliation_fingerprint.value,
            },
        )
        command = CompleteRepairApplication(
            run_id=run_id,
            reservation=reservation,
            applied_at=completed_at,
            companions=companions,
        )
        try:
            _, result, _mutated = self._submit_with_replay(command)
        except (
            RepairApplicationConflictError,
            AuditSequenceConflictError,
        ) as error:
            raise RepairPlanStateError(
                "a concurrent application owns the durable plan frontier"
            ) from error
        final = cast(RepairMutationResult, result).aggregate
        return RepairApplicationReport(
            disposition=RepairApplicationDisposition.COMPLETED,
            aggregate=final,
            effects=tuple(effects),
            resumed=resumed,
            unresolved_action=None,
        )

    async def _begin(
        self,
        run_id: RunId,
        repair_plan_id: RepairPlanId,
        aggregate: RepairPlanAggregate,
        current: StateFingerprint,
        context_id: str,
    ) -> RepairApplicationReservation:
        began_at = self._now()
        companions = build_companions(
            frontier=frontier_from_evidence(self._reader.load(run_id)),
            run_id=run_id,
            operation="repair_application_started",
            object_kind="repair_plan",
            object_id=repair_plan_id.value,
            actor="repair-operator",
            correlation_id=context_id,
            occurred_at=began_at,
            payload={
                "action_count": len(aggregate.actions),
                "content_fingerprint": aggregate.plan.content_fingerprint.value,
                "reconciliation_fingerprint": aggregate.plan.reconciliation_fingerprint.value,
            },
        )
        command = BeginRepairApplication(
            run_id=run_id,
            repair_plan_id=repair_plan_id,
            expected_plan_row_version=aggregate.plan.row_version,
            current_reconciliation_fingerprint=current,
            applying_at=began_at,
            companions=companions,
        )
        try:
            _, result, _mutated = self._submit_with_replay(command)
        except (
            RepairStaleRowVersionError,
            RepairStateConflictError,
            AuditSequenceConflictError,
        ) as error:
            raise RepairPlanStateError(
                "a concurrent application owns the durable plan frontier"
            ) from error
        operation = cast(BeginRepairApplicationResult, result).operation
        if operation.disposition is RepairApplicationBeginDisposition.STARTED:
            return cast(RepairApplicationReservation, operation.reservation)
        # A retry that lost its receipt (or an identical racing begin) replays
        # as in-progress: the durable plan is applying, so resume from the
        # reservation the writer returned instead of misreporting completion.
        if operation.disposition is RepairApplicationBeginDisposition.IN_PROGRESS_REPLAY:
            return _reconstruct_reservation(operation.aggregate)
        if operation.disposition is RepairApplicationBeginDisposition.APPLIED_REPLAY:
            raise RepairPlanStateError(
                "repair plan completed application concurrently; reapply is a no-op"
            )
        raise RepairPlanStateError(
            "repair plan failed application concurrently and cannot be applied"
        )

    async def _apply_one(
        self,
        action: RepairActionRecord,
        target: TargetConnector,
        context: ConnectorCallContext,
        reservation: RepairApplicationReservation,
        run_id: RunId,
        context_id: str,
    ) -> AppliedEffect | _Suspended:
        policy = self._policy
        request = TargetWriteRequest(
            sku=action.effect.proposed.sku,
            payload=render_effect_payload(action.effect.proposed),
            idempotency_key=action.external_idempotency_key,
            precondition=(
                TargetWritePrecondition.must_be_absent()
                if action.effect.expected_target is None
                else TargetWritePrecondition.expected_payload(
                    render_effect_payload(action.effect.expected_target)
                )
            ),
        )
        attempts = 0
        ambiguous_replays = 0
        while True:
            attempts += 1
            try:
                context.raise_if_cancelled()
            except ConnectorCancelledError:
                return _Suspended(RepairApplicationDisposition.INTERRUPTED, "cancelled")
            try:
                outcome = await target.write_record_async(request, context)
            except ConnectorCancelledError as error:
                return _Suspended(RepairApplicationDisposition.INTERRUPTED, redact_exception(error))
            except ConnectorValidationError as error:
                return await self._record_failure(
                    action,
                    reservation,
                    run_id,
                    context_id,
                    "invalid_request",
                    attempts,
                    redact_exception(error),
                )
            except (ConnectorPermanentError, ConnectorConflictError) as error:
                return await self._record_failure(
                    action,
                    reservation,
                    run_id,
                    context_id,
                    "target_rejected",
                    attempts,
                    redact_exception(error),
                )
            except ConnectorRateLimitedError as error:
                wait = (
                    float(error.retry_after_seconds)
                    if error.retry_after_seconds is not None
                    else policy.delay_seconds
                )
                if attempts >= policy.max_attempts_per_action:
                    return await self._record_failure(
                        action,
                        reservation,
                        run_id,
                        context_id,
                        "rate_limited_exhausted",
                        attempts,
                        redact_exception(error),
                    )
                await self._sleep(min(wait, _MAX_RATE_LIMIT_WAIT_SECONDS))
                continue
            except (
                ConnectorRetryableError,
                ConnectorTimeoutError,
                ConnectorServerFailureError,
            ) as error:
                if attempts >= policy.max_attempts_per_action:
                    return await self._record_failure(
                        action,
                        reservation,
                        run_id,
                        context_id,
                        "retry_exhausted",
                        attempts,
                        redact_exception(error),
                    )
                await self._sleep(policy.delay_seconds)
                continue
            except (ConnectorAmbiguousError, ConnectorUnknownError) as error:
                ambiguous_replays += 1
                if ambiguous_replays > policy.max_ambiguous_replays:
                    return _Suspended(
                        RepairApplicationDisposition.UNRESOLVED, redact_exception(error)
                    )
                await self._sleep(policy.delay_seconds)
                continue
            except ConnectorError as error:
                return _Suspended(RepairApplicationDisposition.UNRESOLVED, redact_exception(error))
            return await self._record_applied(
                action, reservation, run_id, context_id, outcome, attempts
            )

    async def _record_applied(
        self,
        action: RepairActionRecord,
        reservation: RepairApplicationReservation,
        run_id: RunId,
        context_id: str,
        outcome: TargetWriteOutcome,
        attempts: int,
    ) -> AppliedEffect | _Suspended:
        applied_at = self._now()
        result = RepairApplicationResult(
            schema_version=APPLICATION_RESULT_SCHEMA_VERSION,
            detail=RedactedDocument.from_mapping(
                {
                    "attempts": attempts,
                    "outcome": outcome.outcome.value,
                    "record_version": outcome.record_version,
                    "target_version": outcome.target_version,
                }
            ),
        )
        companions = build_companions(
            frontier=frontier_from_evidence(self._reader.load(run_id)),
            run_id=run_id,
            operation="repair_action_applied",
            object_kind="repair_action",
            object_id=action.effect.action_id.value,
            actor="repair-operator",
            correlation_id=context_id,
            occurred_at=applied_at,
            payload={
                "canonical_key": action.effect.proposed.sku,
                "outcome": outcome.outcome.value,
                "target_version": outcome.target_version,
            },
        )
        command = RecordRepairActionApplied(
            run_id=run_id,
            reservation=reservation,
            repair_action_id=action.effect.action_id,
            result=result,
            target_version=outcome.target_version,
            applied_at=applied_at,
            companions=companions,
        )
        try:
            _, command_result, _mutated = self._submit_with_replay(command)
        except RepairWriterOutcomeUnknownError:
            return _Suspended(
                RepairApplicationDisposition.UNRESOLVED,
                "the durable outcome of the applied-effect record is unknown",
            )
        except (
            RepairApplicationConflictError,
            RepairStaleRowVersionError,
            AuditSequenceConflictError,
        ) as error:
            raise RepairPlanStateError(
                "a concurrent application owns the durable plan frontier"
            ) from error
        operation = cast(RepairActionAppliedResult, command_result).operation
        return AppliedEffect(
            evidence=AppliedEffectEvidence(
                action_id=action.effect.action_id,
                canonical_key=action.effect.proposed.sku,
                outcome=outcome.outcome.value,
                attempts=attempts,
                target_version=outcome.target_version,
            ),
            reservation=operation.reservation,
        )

    async def _record_failure(
        self,
        action: RepairActionRecord,
        reservation: RepairApplicationReservation,
        run_id: RunId,
        context_id: str,
        reason: str,
        attempts: int,
        detail: str,
    ) -> AppliedEffect | _Suspended:
        failed_at = self._now()
        result = RepairApplicationResult(
            schema_version=APPLICATION_RESULT_SCHEMA_VERSION,
            detail=RedactedDocument.from_mapping(
                {"attempts": attempts, "reason": reason, "detail": detail}
            ),
        )
        companions = build_companions(
            frontier=frontier_from_evidence(self._reader.load(run_id)),
            run_id=run_id,
            operation="repair_action_failed",
            object_kind="repair_action",
            object_id=action.effect.action_id.value,
            actor="repair-operator",
            correlation_id=context_id,
            occurred_at=failed_at,
            payload={"canonical_key": action.effect.proposed.sku, "reason": reason},
        )
        command = RecordRepairActionFailed(
            run_id=run_id,
            reservation=reservation,
            repair_action_id=action.effect.action_id,
            result=result,
            failed_at=failed_at,
            plan_failure=RedactedDocument.from_mapping({"reason": reason, "detail": detail}),
            companions=companions,
        )
        try:
            _, command_result, _mutated = self._submit_with_replay(command)
        except RepairWriterOutcomeUnknownError:
            return _Suspended(
                RepairApplicationDisposition.UNRESOLVED,
                "the durable outcome of the failed-effect record is unknown",
            )
        except (
            RepairApplicationConflictError,
            RepairStaleRowVersionError,
            AuditSequenceConflictError,
        ) as error:
            raise RepairPlanStateError(
                "a concurrent application owns the durable plan frontier"
            ) from error
        aggregate = cast(RepairMutationResult, command_result).aggregate
        if aggregate.plan.status is not RepairPlanStatus.FAILED:
            return _Suspended(
                RepairApplicationDisposition.UNRESOLVED,
                "the durable plan did not terminalize the failed effect",
            )
        raise TargetApplicationError(
            f"repair effect for {action.effect.proposed.sku} failed: {reason}"
        )

    def _submit_with_replay(self, command: WriterCommand) -> tuple[object, object, bool]:
        """Submit a command, replaying the identical command on unknown outcome."""
        attempts = 0
        while True:
            attempts += 1
            try:
                return submit_command(
                    self._writer, command, timeout_seconds=self._policy.timeout_seconds
                )
            except RepairWriterOutcomeUnknownError:
                if attempts >= self._policy.max_writer_replays:
                    raise
                continue

    def _require_plan(self, run_id: RunId, repair_plan_id: RepairPlanId) -> RepairPlanAggregate:
        aggregate = self._reader.load_plan(repair_plan_id)
        if aggregate is None:
            raise RepairPlanMismatchError("repair plan does not exist")
        if aggregate.plan.run_id != run_id:
            raise RepairPlanMismatchError("repair plan belongs to another run")
        return aggregate


def _reconstruct_reservation(aggregate: RepairPlanAggregate) -> RepairApplicationReservation:
    plan = aggregate.plan
    if plan.status is not RepairPlanStatus.APPLYING or plan.applying_at is None:
        raise RepairPlanStateError("repair application state is corrupt")
    return RepairApplicationReservation(
        repair_plan_id=plan.repair_plan_id,
        run_id=plan.run_id,
        reconciliation_fingerprint=plan.reconciliation_fingerprint,
        content_fingerprint=plan.content_fingerprint,
        applying_at=plan.applying_at,
        row_version=plan.row_version,
    )


__all__ = [
    "APPLICATION_RESULT_SCHEMA_VERSION",
    "AppliedEffect",
    "AppliedEffectEvidence",
    "RepairApplicationDisposition",
    "RepairApplicationPolicy",
    "RepairApplicationReport",
    "RepairApplicationService",
]
