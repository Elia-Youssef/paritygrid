"""SQLAlchemy adapter for immutable repair plans and guarded application effects."""

from __future__ import annotations

from collections.abc import Mapping
from typing import NoReturn, cast

from sqlalchemy import and_, insert, or_, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence.repositories.repair_audit_common import (
    effect_digest,
    encode_application_result,
    encode_effect,
    encode_mismatch_evidence,
    encode_redacted_document,
    incrementable_int,
    plan_content_fingerprint,
    portable_identity,
    positive_int,
    require_action_cursor,
    require_exact,
    require_plan_cursor,
    require_reservation,
    stored_fingerprint,
    stored_timestamp,
    translate_repair_storage_errors,
)
from paritygrid.adapters.persistence.repositories.repair_audit_mapping import (
    action_from_row,
    approval_from_row,
    plan_from_row,
    validate_aggregate,
)
from paritygrid.adapters.persistence.schema import (
    reconciliation_conflicts,
    reconciliation_summaries,
    repair_actions,
    repair_approvals,
    repair_plans,
    runs,
)
from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.application.ports.repair_audit import (
    AppliedRepairAction,
    RepairActionCursor,
    RepairActionEffect,
    RepairActionKeyMap,
    RepairActionPage,
    RepairActionRecord,
    RepairActionStatus,
    RepairApplicationBeginDisposition,
    RepairApplicationBeginResult,
    RepairApplicationConflictError,
    RepairApplicationReservation,
    RepairApplicationResult,
    RepairApprovalConflictError,
    RepairApprovalRecord,
    RepairCorruptionError,
    RepairDuplicateError,
    RepairInvalidRequestError,
    RepairPlanAggregate,
    RepairPlanContentConflictError,
    RepairPlanCursor,
    RepairPlanPage,
    RepairPlanRecord,
    RepairPlanStatus,
    RepairRecordNotFoundError,
    RepairRepository,
    RepairStaleRowVersionError,
    RepairStateConflictError,
    validate_repair_page_limit,
)
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    RepairActionId,
    RepairPlanId,
    RunId,
    StateFingerprint,
    UtcTimestamp,
)
from paritygrid.domain.reconciliation import ReconciliationClassification
from paritygrid.domain.repair import RepairActionKind, RepairPlan


class SqlAlchemyRepairRepository(RepairRepository):
    """Persist repair aggregates without owning the caller's transaction."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @translate_repair_storage_errors
    def create_plan(
        self,
        *,
        run_id: RunId,
        plan: RepairPlan,
        action_keys: RepairActionKeyMap,
        created_at: UtcTimestamp,
    ) -> RepairPlanAggregate:
        self._require_transaction()
        run = require_exact(run_id, RunId, "repair run identifier")
        domain_plan = require_exact(plan, RepairPlan, "repair plan")
        keys = require_exact(action_keys, RepairActionKeyMap, "repair action keys")
        timestamp = require_exact(created_at, UtcTimestamp, "repair-plan creation time")
        key_map = keys.to_mapping()
        action_ids = {action.action_id for action in domain_plan.actions}
        if set(key_map) != action_ids:
            raise RepairInvalidRequestError("repair action keys must exactly match plan actions")
        normalized_keys = {
            action_id: portable_identity(key, "external repair key", 128)
            for action_id, key in key_map.items()
        }
        self._validate_summary(run, domain_plan.state_fingerprint, timestamp)
        effects = tuple(RepairActionEffect.from_action(action) for action in domain_plan.actions)
        self._validate_conflicts(run, effects)
        content = plan_content_fingerprint(domain_plan)
        inserted = self._session.execute(
            sqlite_insert(repair_plans)
            .values(
                repair_plan_id=domain_plan.plan_id.value,
                run_id=run.value,
                reconciliation_fingerprint=domain_plan.state_fingerprint.value,
                content_fingerprint=content.value,
                status=RepairPlanStatus.PROPOSED.value,
                row_version=1,
                created_at=str(timestamp),
            )
            .on_conflict_do_nothing()
            .returning(repair_plans.c.repair_plan_id)
        ).scalar_one_or_none()
        if inserted is None:
            return self._classify_create_replay(
                run, domain_plan, normalized_keys, timestamp, effects, content
            )
        self._insert_actions(domain_plan, run, effects, normalized_keys)
        return self._require_aggregate(domain_plan.plan_id)

    def _insert_actions(
        self,
        plan: RepairPlan,
        run: RunId,
        effects: tuple[RepairActionEffect, ...],
        keys: Mapping[RepairActionId, str],
    ) -> None:
        for effect in effects:
            expected = effect.expected_target
            action_inserted = self._session.execute(
                sqlite_insert(repair_actions)
                .values(
                    repair_action_id=effect.action_id.value,
                    repair_plan_id=plan.plan_id.value,
                    run_id=run.value,
                    conflict_id=effect.conflict_id.value,
                    canonical_key=effect.proposed.sku,
                    action_kind=effect.kind.value,
                    external_idempotency_key=keys[effect.action_id],
                    before_sha256=(None if expected is None else effect_digest(expected).value),
                    proposed_after_sha256=effect_digest(effect.proposed).value,
                    proposed_record_json=encode_effect(effect.proposed).text,
                    expected_target_record_json=(
                        None if expected is None else encode_effect(expected).text
                    ),
                    mismatch_evidence_json=encode_mismatch_evidence(effect).text,
                    application_status=RepairActionStatus.PENDING.value,
                )
                .on_conflict_do_nothing()
                .returning(repair_actions.c.repair_action_id)
            ).scalar_one_or_none()
            if action_inserted is None:
                raise RepairDuplicateError("repair action identity or effect key already exists")

    @translate_repair_storage_errors
    def get(self, repair_plan_id: RepairPlanId) -> RepairPlanAggregate | None:
        self._require_transaction()
        identity = require_exact(repair_plan_id, RepairPlanId, "repair-plan identifier")
        return self._get_aggregate(identity)

    @translate_repair_storage_errors
    def list_for_run(
        self,
        run_id: RunId,
        *,
        limit: int,
        after: RepairPlanCursor | None = None,
    ) -> RepairPlanPage:
        self._require_transaction()
        run = require_exact(run_id, RunId, "repair run identifier")
        page_limit = validate_repair_page_limit(limit)
        cursor = None if after is None else require_plan_cursor(after)
        statement = select(repair_plans).where(repair_plans.c.run_id == run.value)
        if cursor is not None:
            statement = statement.where(
                or_(
                    repair_plans.c.created_at > str(cursor.created_at),
                    and_(
                        repair_plans.c.created_at == str(cursor.created_at),
                        repair_plans.c.repair_plan_id > cursor.repair_plan_id.value,
                    ),
                )
            )
        rows = tuple(
            self._session.execute(
                statement.order_by(repair_plans.c.created_at, repair_plans.c.repair_plan_id).limit(
                    page_limit + 1
                )
            ).mappings()
        )
        visible = rows[:page_limit]
        aggregates = self._aggregates_for_plan_rows(visible)
        records = tuple(aggregate.plan for aggregate in aggregates)
        next_cursor = None
        if len(rows) > page_limit and records:
            last = records[-1]
            next_cursor = RepairPlanCursor(last.created_at, last.repair_plan_id)
        return RepairPlanPage(records, next_cursor)

    @translate_repair_storage_errors
    def get_action(self, repair_action_id: RepairActionId) -> RepairActionRecord | None:
        self._require_transaction()
        identity = require_exact(repair_action_id, RepairActionId, "repair-action identifier")
        row = self._get_action_row(identity)
        if row is None:
            return None
        record = action_from_row(cast(Mapping[str, object], row))
        aggregate = self._require_aggregate(record.repair_plan_id)
        return next(action for action in aggregate.actions if action.effect.action_id == identity)

    @translate_repair_storage_errors
    def list_actions(
        self,
        repair_plan_id: RepairPlanId,
        *,
        limit: int,
        after: RepairActionCursor | None = None,
    ) -> RepairActionPage:
        self._require_transaction()
        identity = require_exact(repair_plan_id, RepairPlanId, "repair-plan identifier")
        page_limit = validate_repair_page_limit(limit)
        cursor = None if after is None else require_action_cursor(after)
        aggregate = self._require_aggregate(identity)
        actions = aggregate.actions
        if cursor is not None:
            actions = tuple(
                action
                for action in actions
                if (action.effect.proposed.sku, action.effect.action_id.value)
                > (cursor.canonical_key, cursor.repair_action_id.value)
            )
        visible = actions[:page_limit]
        next_cursor = None
        if len(actions) > page_limit and visible:
            last = visible[-1]
            next_cursor = RepairActionCursor(last.effect.proposed.sku, last.effect.action_id)
        return RepairActionPage(visible, next_cursor)

    @translate_repair_storage_errors
    def approve(
        self,
        repair_plan_id: RepairPlanId,
        *,
        expected_row_version: int,
        current_reconciliation_fingerprint: StateFingerprint,
        approved_by: str,
        approved_at: UtcTimestamp,
        correlation_id: str,
        schema_version: int,
        detail: RedactedDocument,
    ) -> RepairPlanAggregate:
        self._require_transaction()
        identity = require_exact(repair_plan_id, RepairPlanId, "repair-plan identifier")
        expected = positive_int(expected_row_version, "expected repair-plan row version")
        current = require_exact(
            current_reconciliation_fingerprint,
            StateFingerprint,
            "current reconciliation fingerprint",
        )
        actor = portable_identity(approved_by, "approval actor", 128)
        timestamp = require_exact(approved_at, UtcTimestamp, "approval time")
        correlation = portable_identity(correlation_id, "approval correlation identifier", 96)
        version = positive_int(schema_version, "approval schema version")
        encoded_detail = encode_redacted_document(detail, "approval detail")
        aggregate = self._require_aggregate(identity)
        if aggregate.approval is not None:
            return self._classify_approval_replay(
                aggregate, actor, timestamp, correlation, version, encoded_detail.text
            )
        plan = aggregate.plan
        self._require_fresh(plan, current)
        self._require_transition(plan, expected, RepairPlanStatus.PROPOSED)
        self._require_monotonic(timestamp, plan.created_at, "approval time")
        self._advance_plan(identity, expected, RepairPlanStatus.PROPOSED, status="approved")
        self._insert_approval(
            identity,
            plan.reconciliation_fingerprint,
            actor,
            timestamp,
            correlation,
            version,
            encoded_detail.text,
        )
        return self._require_aggregate(identity)

    def _insert_approval(
        self,
        identity: RepairPlanId,
        fingerprint: StateFingerprint,
        actor: str,
        approved_at: UtcTimestamp,
        correlation: str,
        schema_version: int,
        detail_json: str,
    ) -> None:
        self._session.execute(
            insert(repair_approvals).values(
                repair_plan_id=identity.value,
                reconciliation_fingerprint=fingerprint.value,
                approved_by=actor,
                approved_at=str(approved_at),
                correlation_id=correlation,
                approval_schema_version=schema_version,
                detail_json=detail_json,
            )
        )

    @translate_repair_storage_errors
    def reject(
        self,
        repair_plan_id: RepairPlanId,
        *,
        expected_row_version: int,
        rejected_at: UtcTimestamp,
    ) -> RepairPlanAggregate:
        self._require_transaction()
        identity = require_exact(repair_plan_id, RepairPlanId, "repair-plan identifier")
        expected = positive_int(expected_row_version, "expected repair-plan row version")
        timestamp = require_exact(rejected_at, UtcTimestamp, "repair rejection time")
        aggregate = self._require_aggregate(identity)
        plan = aggregate.plan
        if plan.status is RepairPlanStatus.REJECTED:
            if plan.row_version == expected + 1 and plan.rejected_at == timestamp:
                return aggregate
            raise RepairStateConflictError("repair rejection differs from durable state")
        self._require_transition(plan, expected, RepairPlanStatus.PROPOSED)
        self._require_monotonic(timestamp, plan.created_at, "repair rejection time")
        self._advance_plan(
            identity,
            expected,
            RepairPlanStatus.PROPOSED,
            status="rejected",
            rejected_at=str(timestamp),
        )
        return self._require_aggregate(identity)

    @translate_repair_storage_errors
    def begin_application(
        self,
        repair_plan_id: RepairPlanId,
        *,
        expected_row_version: int,
        current_reconciliation_fingerprint: StateFingerprint,
        applying_at: UtcTimestamp,
    ) -> RepairApplicationBeginResult:
        self._require_transaction()
        identity = require_exact(repair_plan_id, RepairPlanId, "repair-plan identifier")
        expected = positive_int(expected_row_version, "expected repair-plan row version")
        current = require_exact(
            current_reconciliation_fingerprint,
            StateFingerprint,
            "current reconciliation fingerprint",
        )
        timestamp = require_exact(applying_at, UtcTimestamp, "repair application time")
        aggregate = self._require_aggregate(identity)
        plan = aggregate.plan
        if plan.status is not RepairPlanStatus.APPROVED:
            disposition = _application_disposition(plan.status)
            return RepairApplicationBeginResult(disposition, aggregate, None)
        self._require_fresh(plan, current)
        self._require_transition(plan, expected, RepairPlanStatus.APPROVED)
        approval = cast("RepairApprovalRecord", aggregate.approval)
        self._require_monotonic(timestamp, approval.approved_at, "repair application time")
        self._advance_plan(
            identity,
            expected,
            RepairPlanStatus.APPROVED,
            status="applying",
            applying_at=str(timestamp),
        )
        installed = self._require_aggregate(identity)
        reservation = _reservation(installed.plan)
        return RepairApplicationBeginResult(
            RepairApplicationBeginDisposition.STARTED, installed, reservation
        )

    @translate_repair_storage_errors
    def record_action_applied(
        self,
        reservation: RepairApplicationReservation,
        repair_action_id: RepairActionId,
        *,
        result: RepairApplicationResult,
        target_version: int,
        applied_at: UtcTimestamp,
    ) -> AppliedRepairAction:
        self._require_transaction()
        claim = require_reservation(reservation)
        action_id = require_exact(repair_action_id, RepairActionId, "repair-action identifier")
        encoded_result = encode_application_result(result)
        version = positive_int(target_version, "repair target version")
        timestamp = require_exact(applied_at, UtcTimestamp, "repair action applied time")
        aggregate = self._require_claim(claim)
        current = _find_action(aggregate, action_id)
        if aggregate.plan.row_version == claim.row_version + 1:
            if _matches_applied(current, encoded_result.text, version, timestamp):
                return AppliedRepairAction(current, _reservation(aggregate.plan))
            raise RepairApplicationConflictError("repair action result differs from durable state")
        self._require_claim_frontier(aggregate.plan, claim)
        self._require_monotonic(timestamp, claim.applying_at, "repair action applied time")
        self._advance_plan(
            claim.repair_plan_id,
            claim.row_version,
            RepairPlanStatus.APPLYING,
        )
        changed = self._session.execute(
            update(repair_actions)
            .where(
                repair_actions.c.repair_action_id == action_id.value,
                repair_actions.c.repair_plan_id == claim.repair_plan_id.value,
                repair_actions.c.application_status == RepairActionStatus.PENDING.value,
            )
            .values(
                application_status=RepairActionStatus.APPLIED.value,
                application_result_json=encoded_result.text,
                target_version=version,
                applied_at=str(timestamp),
            )
            .returning(repair_actions.c.repair_action_id)
        ).scalar_one_or_none()
        if changed is None:
            raise RepairApplicationConflictError("repair action application lost its race")
        installed = self._require_aggregate(claim.repair_plan_id)
        return AppliedRepairAction(_find_action(installed, action_id), _reservation(installed.plan))

    @translate_repair_storage_errors
    def record_action_failed(
        self,
        reservation: RepairApplicationReservation,
        repair_action_id: RepairActionId,
        *,
        result: RepairApplicationResult,
        failed_at: UtcTimestamp,
        plan_failure: RedactedDocument,
    ) -> RepairPlanAggregate:
        self._require_transaction()
        claim = require_reservation(reservation)
        action_id = require_exact(repair_action_id, RepairActionId, "repair-action identifier")
        encoded_result = encode_application_result(result)
        timestamp = require_exact(failed_at, UtcTimestamp, "repair action failure time")
        encoded_failure = encode_redacted_document(
            plan_failure, "repair plan failure detail", maximum_bytes=4096
        )
        aggregate = self._require_claim(claim)
        current = _find_action(aggregate, action_id)
        if aggregate.plan.status is RepairPlanStatus.FAILED:
            if (
                aggregate.plan.row_version == claim.row_version + 1
                and aggregate.plan.failed_at == timestamp
                and _matches_failed(current, encoded_result.text, timestamp)
                and aggregate.plan.failure is not None
                and encode_redacted_document(
                    aggregate.plan.failure,
                    "repair plan failure detail",
                    maximum_bytes=4096,
                ).text
                == encoded_failure.text
            ):
                return aggregate
            raise RepairApplicationConflictError("repair failure differs from durable state")
        self._require_claim_frontier(aggregate.plan, claim)
        self._require_monotonic(timestamp, claim.applying_at, "repair action failure time")
        self._advance_plan(
            claim.repair_plan_id,
            claim.row_version,
            RepairPlanStatus.APPLYING,
            status="failed",
            failed_at=str(timestamp),
            failure_detail=encoded_failure.text,
        )
        changed = self._session.execute(
            update(repair_actions)
            .where(
                repair_actions.c.repair_action_id == action_id.value,
                repair_actions.c.repair_plan_id == claim.repair_plan_id.value,
                repair_actions.c.application_status == RepairActionStatus.PENDING.value,
            )
            .values(
                application_status=RepairActionStatus.FAILED.value,
                application_result_json=encoded_result.text,
                target_version=None,
                failed_at=str(timestamp),
            )
            .returning(repair_actions.c.repair_action_id)
        ).scalar_one_or_none()
        if changed is None:
            raise RepairApplicationConflictError("repair action failure lost its race")
        return self._require_aggregate(claim.repair_plan_id)

    @translate_repair_storage_errors
    def complete_application(
        self,
        reservation: RepairApplicationReservation,
        *,
        applied_at: UtcTimestamp,
    ) -> RepairPlanAggregate:
        self._require_transaction()
        claim = require_reservation(reservation)
        timestamp = require_exact(applied_at, UtcTimestamp, "repair-plan applied time")
        aggregate = self._require_claim(claim)
        if aggregate.plan.status is RepairPlanStatus.APPLIED:
            if (
                aggregate.plan.row_version == claim.row_version + 1
                and aggregate.plan.applied_at == timestamp
            ):
                return aggregate
            raise RepairApplicationConflictError("repair completion differs from durable state")
        self._require_claim_frontier(aggregate.plan, claim)
        if any(action.status is not RepairActionStatus.APPLIED for action in aggregate.actions):
            raise RepairStateConflictError("repair plan still has incomplete actions")
        latest_action = max(cast(UtcTimestamp, action.applied_at) for action in aggregate.actions)
        self._require_monotonic(timestamp, latest_action, "repair-plan applied time")
        self._advance_plan(
            claim.repair_plan_id,
            claim.row_version,
            RepairPlanStatus.APPLYING,
            status="applied",
            applied_at=str(timestamp),
        )
        return self._require_aggregate(claim.repair_plan_id)

    def _get_aggregate(self, identity: RepairPlanId) -> RepairPlanAggregate | None:
        row = (
            self._session.execute(
                select(repair_plans).where(repair_plans.c.repair_plan_id == identity.value)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        plan = plan_from_row(cast(Mapping[str, object], row))
        approval_row = (
            self._session.execute(
                select(repair_approvals).where(repair_approvals.c.repair_plan_id == identity.value)
            )
            .mappings()
            .one_or_none()
        )
        action_rows = tuple(
            self._session.execute(
                select(
                    *repair_actions.c,
                    repair_plans.c.reconciliation_fingerprint.label("reconciliation_fingerprint"),
                    reconciliation_conflicts.c.classification.label("conflict_classification"),
                    reconciliation_conflicts.c.suggested_resolution.label(
                        "conflict_suggested_resolution"
                    ),
                )
                .join(
                    repair_plans,
                    repair_plans.c.repair_plan_id == repair_actions.c.repair_plan_id,
                )
                .outerjoin(
                    reconciliation_conflicts,
                    and_(
                        reconciliation_conflicts.c.conflict_id == repair_actions.c.conflict_id,
                        reconciliation_conflicts.c.run_id == repair_actions.c.run_id,
                        reconciliation_conflicts.c.canonical_key == repair_actions.c.canonical_key,
                    ),
                )
                .where(repair_actions.c.repair_plan_id == identity.value)
                .order_by(
                    repair_actions.c.canonical_key,
                    repair_actions.c.action_kind,
                    repair_actions.c.repair_action_id,
                )
            ).mappings()
        )
        approval = (
            None
            if approval_row is None
            else approval_from_row(cast(Mapping[str, object], approval_row))
        )
        actions = tuple(action_from_row(cast(Mapping[str, object], item)) for item in action_rows)
        return validate_aggregate(plan, approval, actions)

    def _require_aggregate(self, identity: RepairPlanId) -> RepairPlanAggregate:
        aggregate = self._get_aggregate(identity)
        if aggregate is None:
            raise RepairRecordNotFoundError("repair plan does not exist")
        return aggregate

    def _aggregates_for_plan_rows(
        self, rows: tuple[RowMapping, ...]
    ) -> tuple[RepairPlanAggregate, ...]:
        plans = tuple(plan_from_row(cast(Mapping[str, object], row)) for row in rows)
        if not plans:
            return ()
        identities = tuple(plan.repair_plan_id.value for plan in plans)
        approval_rows = tuple(
            self._session.execute(
                select(repair_approvals).where(repair_approvals.c.repair_plan_id.in_(identities))
            ).mappings()
        )
        action_rows = tuple(
            self._session.execute(
                select(
                    *repair_actions.c,
                    repair_plans.c.reconciliation_fingerprint.label("reconciliation_fingerprint"),
                    reconciliation_conflicts.c.classification.label("conflict_classification"),
                    reconciliation_conflicts.c.suggested_resolution.label(
                        "conflict_suggested_resolution"
                    ),
                )
                .join(
                    repair_plans,
                    repair_plans.c.repair_plan_id == repair_actions.c.repair_plan_id,
                )
                .outerjoin(
                    reconciliation_conflicts,
                    and_(
                        reconciliation_conflicts.c.conflict_id == repair_actions.c.conflict_id,
                        reconciliation_conflicts.c.run_id == repair_actions.c.run_id,
                        reconciliation_conflicts.c.canonical_key == repair_actions.c.canonical_key,
                    ),
                )
                .where(repair_actions.c.repair_plan_id.in_(identities))
                .order_by(
                    repair_actions.c.repair_plan_id,
                    repair_actions.c.canonical_key,
                    repair_actions.c.action_kind,
                    repair_actions.c.repair_action_id,
                )
            ).mappings()
        )
        approvals = {
            approval.repair_plan_id: approval
            for approval in (
                approval_from_row(cast(Mapping[str, object], row)) for row in approval_rows
            )
        }
        actions_by_plan: dict[RepairPlanId, list[RepairActionRecord]] = {
            plan.repair_plan_id: [] for plan in plans
        }
        for row in action_rows:
            action = action_from_row(cast(Mapping[str, object], row))
            actions_by_plan[action.repair_plan_id].append(action)
        return tuple(
            validate_aggregate(
                plan,
                approvals.get(plan.repair_plan_id),
                tuple(actions_by_plan[plan.repair_plan_id]),
            )
            for plan in plans
        )

    def _get_action_row(self, identity: RepairActionId) -> RowMapping | None:
        return (
            self._session.execute(
                select(
                    *repair_actions.c,
                    repair_plans.c.reconciliation_fingerprint.label("reconciliation_fingerprint"),
                    reconciliation_conflicts.c.classification.label("conflict_classification"),
                    reconciliation_conflicts.c.suggested_resolution.label(
                        "conflict_suggested_resolution"
                    ),
                )
                .join(
                    repair_plans,
                    repair_plans.c.repair_plan_id == repair_actions.c.repair_plan_id,
                )
                .outerjoin(
                    reconciliation_conflicts,
                    and_(
                        reconciliation_conflicts.c.conflict_id == repair_actions.c.conflict_id,
                        reconciliation_conflicts.c.run_id == repair_actions.c.run_id,
                        reconciliation_conflicts.c.canonical_key == repair_actions.c.canonical_key,
                    ),
                )
                .where(repair_actions.c.repair_action_id == identity.value)
            )
            .mappings()
            .one_or_none()
        )

    def _validate_summary(
        self, run: RunId, fingerprint: StateFingerprint, created_at: UtcTimestamp
    ) -> None:
        row = (
            self._session.execute(
                select(
                    reconciliation_summaries.c.reconciliation_fingerprint,
                    reconciliation_summaries.c.created_at,
                    runs.c.state.label("run_state"),
                    runs.c.execution_evidence_fingerprint,
                )
                .join(runs, runs.c.run_id == reconciliation_summaries.c.run_id)
                .where(reconciliation_summaries.c.run_id == run.value)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise RepairRecordNotFoundError("reconciliation summary does not exist")
        stored = stored_fingerprint(
            row["reconciliation_fingerprint"], "reconciliation summary fingerprint"
        )
        self._validate_run_fingerprint(cast(Mapping[str, object], row), stored)
        summary_at = stored_timestamp(row["created_at"], "reconciliation summary time")
        if stored != fingerprint:
            raise RepairStateConflictError("repair plan reconciliation is stale")
        self._require_monotonic(created_at, summary_at, "repair-plan creation time")

    def _validate_conflicts(self, run: RunId, effects: tuple[RepairActionEffect, ...]) -> None:
        ids = tuple(effect.conflict_id.value for effect in effects)
        rows = tuple(
            self._session.execute(
                select(
                    reconciliation_conflicts.c.conflict_id,
                    reconciliation_conflicts.c.run_id,
                    reconciliation_conflicts.c.canonical_key,
                    reconciliation_conflicts.c.classification,
                    reconciliation_conflicts.c.suggested_resolution,
                ).where(reconciliation_conflicts.c.conflict_id.in_(ids))
            ).mappings()
        )
        by_id = {cast(str, row["conflict_id"]): row for row in rows}
        if len(by_id) != len(effects):
            raise RepairRecordNotFoundError("repair conflict does not exist")
        for effect in effects:
            row = by_id[effect.conflict_id.value]
            expected_classification = (
                ReconciliationClassification.MISSING_FROM_TARGET.value
                if effect.kind is RepairActionKind.CREATE_TARGET
                else ReconciliationClassification.FIELD_MISMATCH.value
            )
            suggested = row["suggested_resolution"]
            if (
                row["run_id"] != run.value
                or row["canonical_key"] != effect.proposed.sku
                or row["classification"] != expected_classification
                or suggested not in {None, effect.kind.value}
            ):
                raise RepairStateConflictError("repair action conflicts with reconciliation")

    def _classify_create_replay(
        self,
        run: RunId,
        plan: RepairPlan,
        keys: Mapping[RepairActionId, str],
        created_at: UtcTimestamp,
        effects: tuple[RepairActionEffect, ...],
        content: StateFingerprint,
    ) -> RepairPlanAggregate:
        aggregate = self._get_aggregate(plan.plan_id)
        if aggregate is None:
            same_content = self._session.execute(
                select(repair_plans.c.repair_plan_id).where(
                    repair_plans.c.run_id == run.value,
                    repair_plans.c.content_fingerprint == content.value,
                )
            ).scalar_one_or_none()
            if same_content is not None:
                raise RepairPlanContentConflictError(
                    "repair plan content already uses another identity"
                )
            raise RepairDuplicateError("repair plan identity or effect key already exists")
        expected_effects = tuple(effects)
        if (
            aggregate.plan.run_id != run
            or aggregate.plan.reconciliation_fingerprint != plan.state_fingerprint
            or aggregate.plan.content_fingerprint != content
            or aggregate.plan.created_at != created_at
            or aggregate.plan.status is not RepairPlanStatus.PROPOSED
            or tuple(action.effect for action in aggregate.actions) != expected_effects
            or {
                action.effect.action_id: action.external_idempotency_key
                for action in aggregate.actions
            }
            != dict(keys)
        ):
            raise RepairPlanContentConflictError("repair plan replay differs from durable state")
        return aggregate

    def _classify_approval_replay(
        self,
        aggregate: RepairPlanAggregate,
        actor: str,
        approved_at: UtcTimestamp,
        correlation: str,
        schema_version: int,
        detail_json: str,
    ) -> RepairPlanAggregate:
        exact = cast("RepairApprovalRecord", aggregate.approval)
        if (
            exact.approved_by == actor
            and exact.approved_at == approved_at
            and exact.correlation_id == correlation
            and exact.schema_version == schema_version
            and encode_redacted_document(exact.detail, "approval detail").text == detail_json
        ):
            return aggregate
        raise RepairApprovalConflictError("repair approval differs from durable state")

    def _require_fresh(self, plan: RepairPlanRecord, current: StateFingerprint) -> None:
        row = (
            self._session.execute(
                select(
                    reconciliation_summaries.c.reconciliation_fingerprint,
                    runs.c.state.label("run_state"),
                    runs.c.execution_evidence_fingerprint,
                )
                .join(runs, runs.c.run_id == reconciliation_summaries.c.run_id)
                .where(reconciliation_summaries.c.run_id == plan.run_id.value)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise RepairCorruptionError("repair reconciliation summary is missing")
        summary = stored_fingerprint(
            row["reconciliation_fingerprint"], "reconciliation summary fingerprint"
        )
        self._validate_run_fingerprint(cast(Mapping[str, object], row), summary)
        if summary != plan.reconciliation_fingerprint:
            raise RepairCorruptionError("repair reconciliation relationship is corrupt")
        if current != summary:
            raise RepairStateConflictError("repair plan reconciliation is stale")

    @staticmethod
    def _validate_run_fingerprint(row: Mapping[str, object], summary: StateFingerprint) -> None:
        state_value = row["run_state"]
        if type(state_value) is not str:
            raise RepairCorruptionError("repair run state is corrupt")
        try:
            state = RunState(state_value)
        except ValueError as error:
            raise RepairCorruptionError("repair run state is corrupt") from error
        if state not in {RunState.SUCCEEDED, RunState.PARTIALLY_SUCCEEDED}:
            raise RepairStateConflictError("repair run has not completed reconciliation")
        fingerprint_value = row["execution_evidence_fingerprint"]
        if fingerprint_value is None:
            raise RepairCorruptionError("repair run final fingerprint is missing")
        final = stored_fingerprint(fingerprint_value, "repair run final fingerprint")
        if final != summary:
            raise RepairCorruptionError("repair run and summary fingerprints diverge")

    def _require_transition(
        self, plan: RepairPlanRecord, expected: int, status: RepairPlanStatus
    ) -> None:
        if plan.row_version != expected:
            raise RepairStaleRowVersionError("repair-plan row version is stale")
        if plan.status is not status:
            raise RepairStateConflictError("repair-plan lifecycle state changed")

    def _advance_plan(
        self,
        identity: RepairPlanId,
        expected: int,
        old_status: RepairPlanStatus,
        **values: object,
    ) -> None:
        incrementable_int(expected, "repair-plan row version")
        changed = self._session.execute(
            update(repair_plans)
            .where(
                repair_plans.c.repair_plan_id == identity.value,
                repair_plans.c.status == old_status.value,
                repair_plans.c.row_version == expected,
            )
            .values(row_version=expected + 1, **values)
            .returning(repair_plans.c.repair_plan_id)
        ).scalar_one_or_none()
        if changed is None:
            self._raise_plan_cas(identity, expected, old_status)

    def _raise_plan_cas(
        self, identity: RepairPlanId, expected: int, status: RepairPlanStatus
    ) -> NoReturn:
        aggregate = self._get_aggregate(identity)
        if aggregate is None:
            raise RepairRecordNotFoundError("repair plan does not exist")
        if aggregate.plan.row_version != expected:
            raise RepairStaleRowVersionError("repair-plan row version is stale")
        if aggregate.plan.status is not status:
            raise RepairStateConflictError("repair-plan lifecycle state changed")
        raise RepairStateConflictError("repair-plan update was rejected")

    def _require_claim(self, claim: RepairApplicationReservation) -> RepairPlanAggregate:
        aggregate = self._require_aggregate(claim.repair_plan_id)
        plan = aggregate.plan
        if (
            plan.run_id != claim.run_id
            or plan.reconciliation_fingerprint != claim.reconciliation_fingerprint
            or plan.content_fingerprint != claim.content_fingerprint
            or plan.applying_at != claim.applying_at
        ):
            raise RepairApplicationConflictError("repair application reservation does not match")
        return aggregate

    def _require_claim_frontier(
        self, plan: RepairPlanRecord, claim: RepairApplicationReservation
    ) -> None:
        if plan.row_version != claim.row_version:
            raise RepairApplicationConflictError("repair application reservation is stale")
        if plan.status is not RepairPlanStatus.APPLYING:
            raise RepairApplicationConflictError("repair plan is not applying")

    @staticmethod
    def _require_monotonic(candidate: UtcTimestamp, evidence: UtcTimestamp, subject: str) -> None:
        if candidate < evidence:
            raise RepairInvalidRequestError(f"{subject} is not monotonic")

    def _require_transaction(self) -> None:
        if not self._session.in_transaction():
            raise RepairInvalidRequestError("repository requires a caller-owned transaction")


def _reservation(plan: RepairPlanRecord) -> RepairApplicationReservation:
    if plan.status is not RepairPlanStatus.APPLYING or plan.applying_at is None:
        raise RepairCorruptionError("repair application reservation state is corrupt")
    return RepairApplicationReservation(
        repair_plan_id=plan.repair_plan_id,
        run_id=plan.run_id,
        reconciliation_fingerprint=plan.reconciliation_fingerprint,
        content_fingerprint=plan.content_fingerprint,
        applying_at=plan.applying_at,
        row_version=plan.row_version,
    )


def _application_disposition(
    status: RepairPlanStatus,
) -> RepairApplicationBeginDisposition:
    if status is RepairPlanStatus.APPLYING:
        return RepairApplicationBeginDisposition.IN_PROGRESS_REPLAY
    if status is RepairPlanStatus.APPLIED:
        return RepairApplicationBeginDisposition.APPLIED_REPLAY
    if status is RepairPlanStatus.FAILED:
        return RepairApplicationBeginDisposition.FAILED_REPLAY
    raise RepairStateConflictError("repair plan is not approved for application")


def _find_action(aggregate: RepairPlanAggregate, identity: RepairActionId) -> RepairActionRecord:
    for action in aggregate.actions:
        if action.effect.action_id == identity:
            return action
    raise RepairRecordNotFoundError("repair action does not exist")


def _matches_applied(
    action: RepairActionRecord,
    result_json: str,
    target_version: int,
    applied_at: UtcTimestamp,
) -> bool:
    if action.status is not RepairActionStatus.APPLIED or action.result is None:
        return False
    return (
        encode_application_result(action.result).text == result_json
        and action.target_version == target_version
        and action.applied_at == applied_at
    )


def _matches_failed(action: RepairActionRecord, result_json: str, failed_at: UtcTimestamp) -> bool:
    if action.status is not RepairActionStatus.FAILED or action.result is None:
        return False
    return (
        encode_application_result(action.result).text == result_json
        and action.failed_at == failed_at
    )


__all__ = ["SqlAlchemyRepairRepository"]
