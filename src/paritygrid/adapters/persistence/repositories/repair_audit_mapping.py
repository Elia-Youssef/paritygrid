"""Strict mapping from untrusted repair and audit rows to application values."""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping

from paritygrid.adapters.persistence.repositories.repair_audit_common import (
    audit_bounded_text,
    audit_portable_identity,
    audit_positive_int,
    audit_snake_case,
    decode_application_result,
    decode_audit_detail,
    decode_effect,
    decode_redacted_document,
    effect_content_fingerprint,
    effect_digest,
    effect_mismatches,
    stored_fingerprint,
    stored_identifier,
    stored_optional_timestamp,
    stored_positive_int,
    stored_timestamp,
    validate_mismatch_evidence,
)
from paritygrid.adapters.persistence.values import (
    RepairActionApplicationStatus as StoredRepairActionStatus,
)
from paritygrid.adapters.persistence.values import RepairPlanStatus as StoredRepairPlanStatus
from paritygrid.application.ports.repair_audit import (
    AuditCorruptionError,
    AuditEntryRecord,
    AuditInvalidRequestError,
    AuditSequence,
    RepairActionEffect,
    RepairActionRecord,
    RepairActionStatus,
    RepairApprovalRecord,
    RepairCorruptionError,
    RepairPlanAggregate,
    RepairPlanRecord,
    RepairPlanStatus,
)
from paritygrid.domain.models import (
    ConflictId,
    RepairActionId,
    RepairPlanId,
    RunId,
    UtcTimestamp,
)
from paritygrid.domain.repair import RepairActionKind, RepairPlanBinding


def plan_from_row(row: Mapping[str, object]) -> RepairPlanRecord:
    """Map one plan row and reject every unsupported stored state."""
    status = _stored_plan_status(row["status"])
    failure_value = row["failure_detail"]
    run_id = stored_identifier(row["run_id"], RunId, "repair run identifier")
    reconciliation = stored_fingerprint(
        row["reconciliation_fingerprint"], "repair reconciliation fingerprint"
    )
    return RepairPlanRecord(
        repair_plan_id=stored_identifier(
            row["repair_plan_id"], RepairPlanId, "repair-plan identifier"
        ),
        run_id=run_id,
        reconciliation_fingerprint=reconciliation,
        content_fingerprint=stored_fingerprint(
            row["content_fingerprint"], "repair content fingerprint"
        ),
        binding=RepairPlanBinding(
            run_id=run_id,
            reconciliation_fingerprint=reconciliation,
            source_input_identity=stored_fingerprint(
                row["source_input_identity"], "repair source identity"
            ).value,
            target_input_identity=stored_fingerprint(
                row["target_input_identity"], "repair target identity"
            ).value,
            policy_version=stored_positive_int(row["policy_version"], "repair policy version"),
            generation_version=stored_positive_int(row["generation_version"], "repair generation version"),
            rules_version=stored_positive_int(row["rules_version"], "repair rules version"),
            analysis_version=stored_positive_int(row["analysis_version"], "repair analysis version"),
            analytical_query_version=stored_positive_int(
                row["analytical_query_version"], "repair analytical query version"
            ),
            action_count=_stored_nonnegative_int(row["action_count"], "repair action count"),
        ),
        status=status,
        row_version=stored_positive_int(row["row_version"], "repair-plan row version"),
        created_at=stored_timestamp(row["created_at"], "repair-plan creation time"),
        applying_at=stored_optional_timestamp(row["applying_at"], "repair applying time"),
        applied_at=stored_optional_timestamp(row["applied_at"], "repair applied time"),
        rejected_at=stored_optional_timestamp(row["rejected_at"], "repair rejected time"),
        failed_at=stored_optional_timestamp(row["failed_at"], "repair failed time"),
        failure=(
            None
            if failure_value is None
            else decode_redacted_document(failure_value, "repair failure detail")
        ),
    )


def approval_from_row(row: Mapping[str, object]) -> RepairApprovalRecord:
    """Map one immutable approval fact."""
    return RepairApprovalRecord(
        repair_plan_id=stored_identifier(
            row["repair_plan_id"], RepairPlanId, "approval repair-plan identifier"
        ),
        reconciliation_fingerprint=stored_fingerprint(
            row["reconciliation_fingerprint"], "approval reconciliation fingerprint"
        ),
        approved_by=_stored_portable(row["approved_by"], "approval actor", 128),
        approved_at=stored_timestamp(row["approved_at"], "approval time"),
        correlation_id=_stored_portable(
            row["correlation_id"], "approval correlation identifier", 96
        ),
        schema_version=stored_positive_int(
            row["approval_schema_version"], "approval schema version"
        ),
        detail=decode_redacted_document(row["detail_json"], "approval detail"),
    )


def action_from_row(row: Mapping[str, object]) -> RepairActionRecord:
    """Map one repair action and verify every derived durable value."""
    kind = _stored_action_kind(row["action_kind"])
    proposed = decode_effect(row["proposed_record_json"], "proposed repair effect")
    expected = (
        None
        if row["expected_target_record_json"] is None
        else decode_effect(row["expected_target_record_json"], "expected repair effect")
    )
    if kind is RepairActionKind.CREATE_TARGET and expected is not None:
        raise RepairCorruptionError("create repair action shape is corrupt")
    if kind is RepairActionKind.UPDATE_TARGET and expected is None:
        raise RepairCorruptionError("update repair action shape is corrupt")
    expected_classification = (
        "missing_from_target" if kind is RepairActionKind.CREATE_TARGET else "field_mismatch"
    )
    suggested = row["conflict_suggested_resolution"]
    if row["conflict_classification"] != expected_classification or suggested not in {
        None,
        kind.value,
    }:
        raise RepairCorruptionError("repair action conflict relationship is corrupt")
    effect = RepairActionEffect(
        action_id=stored_identifier(
            row["repair_action_id"], RepairActionId, "repair-action identifier"
        ),
        conflict_id=stored_identifier(row["conflict_id"], ConflictId, "conflict identifier"),
        reconciliation_fingerprint=stored_fingerprint(
            row["reconciliation_fingerprint"], "action reconciliation fingerprint"
        ),
        kind=kind,
        proposed=proposed,
        expected_target=expected,
        mismatches=effect_mismatches(proposed, expected),
    )
    validate_mismatch_evidence(row["mismatch_evidence_json"], effect)
    canonical_key = _stored_text(row["canonical_key"], "repair canonical key", 64)
    if canonical_key != effect.proposed.sku:
        raise RepairCorruptionError("repair canonical key is corrupt")
    before = (
        None
        if row["before_sha256"] is None
        else stored_fingerprint(row["before_sha256"], "repair before digest")
    )
    expected_before = None if expected is None else effect_digest(expected)
    proposed_digest = stored_fingerprint(row["proposed_after_sha256"], "repair proposed digest")
    if before != expected_before or proposed_digest != effect_digest(proposed):
        raise RepairCorruptionError("repair effect digest is corrupt")
    status = _stored_action_status(row["application_status"])
    result = (
        None
        if row["application_result_json"] is None
        else decode_application_result(row["application_result_json"])
    )
    target_version = (
        None
        if row["target_version"] is None
        else stored_positive_int(row["target_version"], "repair target version")
    )
    applied_at = stored_optional_timestamp(row["applied_at"], "repair action applied time")
    failed_at = stored_optional_timestamp(row["failed_at"], "repair action failed time")
    _validate_action_result(status, result, target_version, applied_at, failed_at)
    return RepairActionRecord(
        repair_plan_id=stored_identifier(
            row["repair_plan_id"], RepairPlanId, "action repair-plan identifier"
        ),
        run_id=stored_identifier(row["run_id"], RunId, "action run identifier"),
        effect=effect,
        external_idempotency_key=_stored_portable(
            row["external_idempotency_key"], "external repair key", 128
        ),
        before_sha256=before,
        proposed_after_sha256=proposed_digest,
        status=status,
        result=result,
        target_version=target_version,
        applied_at=applied_at,
        failed_at=failed_at,
    )


def validate_aggregate(
    plan: RepairPlanRecord,
    approval: RepairApprovalRecord | None,
    actions: tuple[RepairActionRecord, ...],
) -> RepairPlanAggregate:
    """Validate plan, approval, and action rows as one coherent aggregate."""
    if not 1 <= len(actions) <= 10_000:
        raise RepairCorruptionError("repair plan action set is corrupt")
    if plan.binding is None or plan.binding.action_count != len(actions):
        raise RepairCorruptionError("repair plan binding action count is corrupt")
    if tuple(sorted(actions, key=_action_order)) != actions:
        raise RepairCorruptionError("repair plan action ordering is corrupt")
    if len({action.effect.action_id for action in actions}) != len(actions):
        raise RepairCorruptionError("repair action identities are corrupt")
    if len({action.effect.conflict_id for action in actions}) != len(actions):
        raise RepairCorruptionError("repair conflict identities are corrupt")
    if len({action.effect.proposed.sku for action in actions}) != len(actions):
        raise RepairCorruptionError("repair canonical keys are corrupt")
    for action in actions:
        if (
            action.repair_plan_id != plan.repair_plan_id
            or action.run_id != plan.run_id
            or action.effect.reconciliation_fingerprint != plan.reconciliation_fingerprint
        ):
            raise RepairCorruptionError("repair aggregate identity is corrupt")
    computed = effect_content_fingerprint(
        plan.repair_plan_id,
        plan.reconciliation_fingerprint,
        tuple(action.effect for action in actions),
        plan.binding,
    )
    if computed != plan.content_fingerprint:
        raise RepairCorruptionError("repair plan content fingerprint is corrupt")
    if approval is not None and (
        approval.repair_plan_id != plan.repair_plan_id
        or approval.reconciliation_fingerprint != plan.reconciliation_fingerprint
    ):
        raise RepairCorruptionError("repair approval identity is corrupt")
    _validate_plan_lifecycle(plan, approval, actions)
    return RepairPlanAggregate(plan=plan, approval=approval, actions=actions)


def audit_from_row(row: Mapping[str, object]) -> AuditEntryRecord:
    """Map one append-only audit row."""
    try:
        sequence_value = row["sequence_number"]
        if type(sequence_value) is not int:
            raise ValueError
        sequence = AuditSequence(sequence_value)
        actor = audit_bounded_text(row["actor"], "audit actor", 128)
        operation = audit_snake_case(row["operation"], "audit operation", 96)
        object_kind = audit_snake_case(row["object_kind"], "audit object kind", 48)
        object_raw = row["object_id"]
        object_id = (
            None
            if object_raw is None
            else audit_portable_identity(object_raw, "audit object identifier", 128)
        )
        correlation_id = audit_portable_identity(
            row["correlation_id"], "audit correlation identifier", 96
        )
        timestamp_raw = row["occurred_at"]
        if type(timestamp_raw) is not str:
            raise ValueError
        occurred_at = UtcTimestamp.parse(timestamp_raw)
        detail_schema_version = audit_positive_int(
            row["detail_schema_version"], "audit detail schema version"
        )
        detail = decode_audit_detail(row["detail_json"])
    except AuditCorruptionError:
        raise
    except (AuditInvalidRequestError, TypeError, ValueError) as error:
        raise AuditCorruptionError("audit entry is corrupt") from error
    return AuditEntryRecord(
        sequence=sequence,
        actor=actor,
        operation=operation,
        object_kind=object_kind,
        object_id=object_id,
        correlation_id=correlation_id,
        occurred_at=occurred_at,
        detail_schema_version=detail_schema_version,
        detail=detail,
    )


def _validate_action_result(
    status: RepairActionStatus,
    result: object,
    target_version: int | None,
    applied_at: UtcTimestamp | None,
    failed_at: UtcTimestamp | None,
) -> None:
    if status is RepairActionStatus.PENDING:
        valid = (
            result is None and target_version is None and applied_at is None and failed_at is None
        )
    elif status is RepairActionStatus.APPLIED:
        valid = (
            result is not None
            and target_version is not None
            and applied_at is not None
            and failed_at is None
        )
    else:
        valid = (
            result is not None
            and target_version is None
            and applied_at is None
            and failed_at is not None
        )
    if not valid:
        raise RepairCorruptionError("repair action application result is corrupt")


def _validate_plan_lifecycle(
    plan: RepairPlanRecord,
    approval: RepairApprovalRecord | None,
    actions: tuple[RepairActionRecord, ...],
) -> None:
    status = plan.status
    pending = all(action.status is RepairActionStatus.PENDING for action in actions)
    applied = all(action.status is RepairActionStatus.APPLIED for action in actions)
    any_failed = any(action.status is RepairActionStatus.FAILED for action in actions)
    if status is RepairPlanStatus.PROPOSED:
        valid = plan.row_version == 1 and approval is None and pending
        expected_times = (None, None, None, None, None)
    elif status is RepairPlanStatus.APPROVED:
        valid = plan.row_version == 2 and approval is not None and pending
        expected_times = (None, None, None, None, None)
    elif status is RepairPlanStatus.REJECTED:
        valid = plan.row_version == 2 and approval is None and pending
        expected_times = (None, None, plan.rejected_at, None, None)
        valid = valid and plan.rejected_at is not None
    elif status is RepairPlanStatus.APPLYING:
        completed = sum(action.status is RepairActionStatus.APPLIED for action in actions)
        valid = (
            approval is not None
            and not any_failed
            and plan.applying_at is not None
            and plan.row_version == 3 + completed
        )
        expected_times = (plan.applying_at, None, None, None, None)
    elif status is RepairPlanStatus.APPLIED:
        valid = (
            approval is not None
            and applied
            and plan.applying_at is not None
            and plan.applied_at is not None
            and plan.row_version == 4 + len(actions)
        )
        expected_times = (plan.applying_at, plan.applied_at, None, None, None)
    else:
        terminal_count = sum(action.status is not RepairActionStatus.PENDING for action in actions)
        valid = (
            approval is not None
            and any_failed
            and plan.applying_at is not None
            and plan.failed_at is not None
            and plan.failure is not None
            and plan.row_version == 3 + terminal_count
        )
        expected_times = (plan.applying_at, None, None, plan.failed_at, plan.failure)
    actual_times = (
        plan.applying_at,
        plan.applied_at,
        plan.rejected_at,
        plan.failed_at,
        plan.failure,
    )
    if not valid or actual_times != expected_times:
        raise RepairCorruptionError("repair plan lifecycle is corrupt")
    evidence: list[UtcTimestamp] = [plan.created_at]
    if approval is not None:
        evidence.append(approval.approved_at)
    if plan.applying_at is not None:
        evidence.append(plan.applying_at)
    for action in actions:
        outcome_at = action.applied_at if action.applied_at is not None else action.failed_at
        if outcome_at is not None:
            if plan.applying_at is None or outcome_at < plan.applying_at:
                raise RepairCorruptionError("repair action chronology is corrupt")
            evidence.append(outcome_at)
    terminal = plan.applied_at or plan.rejected_at or plan.failed_at
    if terminal is not None and terminal < max(evidence):
        raise RepairCorruptionError("repair plan chronology is corrupt")
    if approval is not None and approval.approved_at < plan.created_at:
        raise RepairCorruptionError("repair approval chronology is corrupt")
    if (
        plan.applying_at is not None
        and approval is not None
        and plan.applying_at < approval.approved_at
    ):
        raise RepairCorruptionError("repair application chronology is corrupt")


def _stored_plan_status(value: object) -> RepairPlanStatus:
    if type(value) is not str:
        raise RepairCorruptionError("repair-plan status is corrupt")
    try:
        stored = StoredRepairPlanStatus(value)
        return RepairPlanStatus(stored.value)
    except ValueError as error:
        raise RepairCorruptionError("repair-plan status is corrupt") from error


def _stored_action_status(value: object) -> RepairActionStatus:
    if type(value) is not str:
        raise RepairCorruptionError("repair-action status is corrupt")
    try:
        stored = StoredRepairActionStatus(value)
        return RepairActionStatus(stored.value)
    except ValueError as error:
        raise RepairCorruptionError("repair-action status is corrupt") from error


def _stored_action_kind(value: object) -> RepairActionKind:
    if type(value) is not str:
        raise RepairCorruptionError("repair action kind is corrupt")
    try:
        return RepairActionKind(value)
    except ValueError as error:
        raise RepairCorruptionError("repair action kind is corrupt") from error


def _stored_text(value: object, subject: str, maximum: int) -> str:
    if type(value) is not str or not 1 <= len(value) <= maximum:
        raise RepairCorruptionError(f"{subject} is corrupt")
    if unicodedata.normalize("NFC", value) != value:
        raise RepairCorruptionError(f"{subject} is corrupt")
    return value


def _stored_nonnegative_int(value: object, subject: str) -> int:
    if type(value) is not int or value < 0:
        raise RepairCorruptionError(f"{subject} is corrupt")
    return value


def _stored_portable(value: object, subject: str, maximum: int) -> str:
    try:
        return audit_portable_identity(value, subject, maximum)
    except AuditInvalidRequestError as error:
        raise RepairCorruptionError(f"{subject} is corrupt") from error


def _action_order(action: RepairActionRecord) -> tuple[str, str, str]:
    return (
        action.effect.proposed.sku,
        action.effect.kind.value,
        action.effect.action_id.value,
    )
