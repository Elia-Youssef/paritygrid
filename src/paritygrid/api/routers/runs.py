"""Run lifecycle routes."""

from typing import Annotated

from fastapi import APIRouter, Query, Request, status
from fastapi.responses import JSONResponse

from paritygrid.api.dependencies import (
    command_response,
    correlation_of,
    execute_command,
    get_services,
)
from paritygrid.api.schemas.runs import RunCreateRequest, RunPageResponse, RunResponse
from paritygrid.application.ports.execution import RunRecord
from paritygrid.domain.models import UtcTimestamp

router = APIRouter(prefix="/api/v1/runs", tags=["runs"])


@router.post("", status_code=status.HTTP_201_CREATED)
def create_run(payload: RunCreateRequest, request: Request) -> JSONResponse:
    execution = execute_command(
        request,
        scope="runs:create",
        canonical_request=payload.model_dump(mode="json"),
        handler=lambda: _create_outcome(request, payload, converge=False),
        reclaimed_handler=lambda: _create_outcome(request, payload, converge=True),
    )
    return command_response(execution)


@router.get("", response_model=RunPageResponse)
def list_runs(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    cursor: str | None = None,
    state: str | None = None,
) -> RunPageResponse:
    services = get_services(request)
    observed_at = services.clock()
    page = services.runs.list(limit=limit, after=cursor, state=state)
    return RunPageResponse(
        items=[_run_response(record, observed_at=observed_at) for record in page.items],
        limit=limit,
        next_cursor=None if page.next_cursor is None else page.next_cursor.value,
    )


@router.get("/{run_id}", response_model=RunResponse)
def get_run(run_id: str, request: Request) -> RunResponse:
    services = get_services(request)
    record = services.runs.get(run_id)
    return _run_response(record, observed_at=services.clock())


@router.post("/{run_id}/pause")
def pause_run(run_id: str, request: Request) -> JSONResponse:
    return _lifecycle(run_id, request, "pause")


@router.post("/{run_id}/resume")
def resume_run(run_id: str, request: Request) -> JSONResponse:
    return _lifecycle(run_id, request, "resume")


@router.post("/{run_id}/cancel")
def cancel_run(run_id: str, request: Request) -> JSONResponse:
    return _lifecycle(run_id, request, "cancel")


def _lifecycle(run_id: str, request: Request, direction: str) -> JSONResponse:
    execution = execute_command(
        request,
        scope=f"runs:{direction}",
        canonical_request={"run_id": run_id},
        handler=lambda: _lifecycle_outcome(request, run_id, direction, converge=False),
        reclaimed_handler=lambda: _lifecycle_outcome(request, run_id, direction, converge=True),
    )
    return command_response(execution)


def _create_outcome(
    request: Request, payload: RunCreateRequest, *, converge: bool
) -> tuple[int, dict[str, object]]:
    services = get_services(request)
    record = services.runs.create(
        run_id=payload.run_id,
        pipeline_id=payload.pipeline_id,
        pipeline_version=payload.pipeline_version,
        runner_kind=payload.runner_kind,
        scenario_seed=payload.scenario_seed,
        runner_configuration=payload.runner_configuration,
        correlation_id=correlation_of(request),
        converge_on_duplicate=converge,
    )
    return (
        status.HTTP_201_CREATED,
        _run_response(record, observed_at=services.clock()).model_dump(mode="json"),
    )


def _lifecycle_outcome(
    request: Request, run_id: str, direction: str, *, converge: bool
) -> tuple[int, dict[str, object]]:
    services = get_services(request)
    transition = getattr(services.run_lifecycle, direction)
    record = transition(
        run_id,
        correlation_id=correlation_of(request),
        converge_on_duplicate=converge,
    )
    return (
        status.HTTP_200_OK,
        _run_response(record, observed_at=services.clock()).model_dump(mode="json"),
    )


def _run_response(record: RunRecord, *, observed_at: UtcTimestamp) -> RunResponse:
    return RunResponse(
        run_id=record.run_id.value,
        run_version=record.row_version,
        state=record.state.value,
        pipeline_id=record.pipeline_id.value,
        pipeline_version=record.pipeline_version.number,
        runner_kind=record.runner_kind,
        scenario_seed=record.scenario_seed,
        created_at=str(record.created_at),
        started_at=None if record.started_at is None else str(record.started_at),
        finished_at=None if record.finished_at is None else str(record.finished_at),
        cancellation_requested_at=(
            None
            if record.cancellation_requested_at is None
            else str(record.cancellation_requested_at)
        ),
        observed_at=str(observed_at),
        execution_evidence_fingerprint=(
            None
            if record.execution_evidence_fingerprint is None
            else str(record.execution_evidence_fingerprint)
        ),
        execution_evidence_fingerprint_version=record.execution_evidence_fingerprint_version,
    )
