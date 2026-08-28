"""Repair workflow routes over the accepted Phase 11 services."""

import asyncio

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from paritygrid.api.dependencies import (
    command_response,
    correlation_of,
    execute_command,
    get_services,
)
from paritygrid.api.schemas.repair import (
    ObservationSideInput,
    RepairActionResponse,
    RepairApplyResponse,
    RepairApprovalRequestBody,
    RepairApprovalSummary,
    RepairPlanCreateRequest,
    RepairPlanResponse,
)
from paritygrid.application.ports.repair_audit import RepairPlanAggregate
from paritygrid.application.repair.applier import RepairApplicationDisposition
from paritygrid.application.repair.errors import (
    TargetApplicationInterruptedError,
    TargetApplicationUnresolvedError,
)
from paritygrid.application.services.repair import ObservationSide

router = APIRouter(prefix="/api/v1", tags=["repairs"])


@router.post(
    "/runs/{run_id}/repair-plans",
    status_code=status.HTTP_201_CREATED,
    response_model=RepairPlanResponse,
)
def create_repair_plan(
    run_id: str, payload: RepairPlanCreateRequest, request: Request
) -> JSONResponse:
    return command_response(
        execute_command(
            request,
            scope="repair-plans:create",
            canonical_request={"run_id": run_id, **payload.model_dump(mode="json")},
            handler=lambda: _create_outcome(request, run_id, payload),
        )
    )


@router.get("/repair-plans/{plan_id}", response_model=RepairPlanResponse)
def get_repair_plan(plan_id: str, request: Request) -> RepairPlanResponse:
    services = get_services(request)
    view = services.repair.plan(plan_id)
    return _plan_response(
        view.aggregate,
        run_version=view.run.row_version,
        state=view.run.state.value,
        observed_at=services.clock(),
    )


@router.post("/repair-plans/{plan_id}/approve", response_model=RepairPlanResponse)
def approve_repair_plan(
    plan_id: str, payload: RepairApprovalRequestBody, request: Request
) -> JSONResponse:
    return command_response(
        execute_command(
            request,
            scope="repair-plans:approve",
            canonical_request={"plan_id": plan_id, **payload.model_dump(mode="json")},
            handler=lambda: _approve_outcome(request, plan_id, payload),
        )
    )


@router.post("/repair-plans/{plan_id}/apply", response_model=RepairApplyResponse)
def apply_repair_plan(plan_id: str, request: Request) -> JSONResponse:
    return command_response(
        execute_command(
            request,
            scope="repair-plans:apply",
            canonical_request={"plan_id": plan_id},
            handler=lambda: _apply_outcome(request, plan_id),
        )
    )


def _create_outcome(
    request: Request, run_id: str, payload: RepairPlanCreateRequest
) -> tuple[int, dict[str, object]]:
    services = get_services(request)
    view = services.repair.create_plan(
        run_id=run_id,
        source=_side(payload.source),
        target=_side(payload.target),
        actor="http-operator",
        correlation_id=correlation_of(request),
    )
    return (
        status.HTTP_201_CREATED,
        _plan_response(
            view.aggregate,
            run_version=view.run.row_version,
            state=view.run.state.value,
            observed_at=services.clock(),
        ).model_dump(mode="json"),
    )


def _approve_outcome(
    request: Request, plan_id: str, payload: RepairApprovalRequestBody
) -> tuple[int, dict[str, object]]:
    services = get_services(request)
    view = services.repair.approve(
        plan_id=plan_id,
        approved_by=payload.approved_by,
        approved_content_fingerprint=payload.approved_content_fingerprint,
        approved_reconciliation_fingerprint=payload.approved_reconciliation_fingerprint,
        correlation_id=correlation_of(request),
    )
    return (
        status.HTTP_200_OK,
        _plan_response(
            view.outcome.aggregate,
            run_version=view.run.row_version,
            state=view.run.state.value,
            observed_at=services.clock(),
        ).model_dump(mode="json"),
    )


def _apply_outcome(request: Request, plan_id: str) -> tuple[int, dict[str, object]]:
    services = get_services(request)
    # Sync handlers run on the offload threadpool, where a private event loop
    # can drive the async target connector without touching the main loop.
    view = asyncio.run(
        services.repair_application.apply(plan_id=plan_id, context_id=correlation_of(request))
    )
    report = view.report
    if report.disposition is RepairApplicationDisposition.UNRESOLVED:
        # The Phase 11 applier deliberately left the plan resumable.  Do not
        # cache a 200 response under idempotency: the caller must see a retry-
        # safe 503 and re-enter the same fenced application workflow.
        raise TargetApplicationUnresolvedError("repair application is unresolved")
    if report.disposition is RepairApplicationDisposition.INTERRUPTED:
        raise TargetApplicationInterruptedError("repair application was interrupted")
    plan = report.aggregate
    return (
        status.HTTP_200_OK,
        RepairApplyResponse(
            plan_id=plan.plan.repair_plan_id.value,
            run_id=plan.plan.run_id.value,
            run_version=view.run.row_version,
            state=view.run.state.value,
            observed_at=str(services.clock()),
            status=plan.plan.status.value,
            reconciliation_fingerprint=plan.plan.reconciliation_fingerprint.value,
            content_fingerprint=plan.plan.content_fingerprint.value,
            disposition=report.disposition.value,
            resumed=report.resumed,
            effects=[
                {
                    "action_id": effect.action_id.value,
                    "canonical_key": effect.canonical_key,
                    "outcome": effect.outcome,
                    "attempts": effect.attempts,
                    "target_version": effect.target_version,
                }
                for effect in report.effects
            ],
        ).model_dump(mode="json"),
    )


def _side(side: ObservationSideInput) -> ObservationSide:
    return ObservationSide(
        connector_id=side.connector_id,
        input_identity=side.input_identity,
        observations=tuple(
            {
                "position": observation.position,
                "payload": observation.payload,
                "malformed_reason": observation.malformed_reason,
            }
            for observation in side.observations
        ),
    )


def _plan_response(
    aggregate: RepairPlanAggregate,
    *,
    run_version: int,
    state: str,
    observed_at: object,
) -> RepairPlanResponse:
    plan = aggregate.plan
    approval = aggregate.approval
    return RepairPlanResponse(
        plan_id=plan.repair_plan_id.value,
        run_id=plan.run_id.value,
        run_version=run_version,
        state=state,
        observed_at=str(observed_at),
        status=plan.status.value,
        reconciliation_fingerprint=plan.reconciliation_fingerprint.value,
        content_fingerprint=plan.content_fingerprint.value,
        created_at=str(plan.created_at),
        applying_at=None if plan.applying_at is None else str(plan.applying_at),
        applied_at=None if plan.applied_at is None else str(plan.applied_at),
        rejected_at=None if plan.rejected_at is None else str(plan.rejected_at),
        failed_at=None if plan.failed_at is None else str(plan.failed_at),
        approval=(
            None
            if approval is None
            else RepairApprovalSummary(
                approved_by=approval.approved_by,
                approved_at=str(approval.approved_at),
                correlation_id=approval.correlation_id,
                approval_schema_version=approval.schema_version,
            )
        ),
        actions=[
            RepairActionResponse(
                action_id=action.effect.action_id.value,
                canonical_key=action.effect.proposed.sku,
                kind=action.effect.kind.value,
                status=action.status.value,
                before_sha256=("" if action.before_sha256 is None else action.before_sha256.value),
                proposed_after_sha256=action.proposed_after_sha256.value,
                applied_at=None if action.applied_at is None else str(action.applied_at),
                failed_at=None if action.failed_at is None else str(action.failed_at),
                target_version=action.target_version,
            )
            for action in aggregate.actions
        ],
    )
