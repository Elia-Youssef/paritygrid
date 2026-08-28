"""Pipeline and pipeline-version routes."""

from typing import Annotated

from fastapi import APIRouter, Query, Request, status
from fastapi.responses import JSONResponse

from paritygrid.api.dependencies import (
    command_response,
    execute_command,
    get_services,
)
from paritygrid.api.schemas.pipelines import (
    PipelineCreateRequest,
    PipelinePageResponse,
    PipelineResponse,
    PipelineVersionAckResponse,
    PipelineVersionPublishRequest,
    PipelineVersionResponse,
)
from paritygrid.application.ports.configuration import (
    PipelineRecord,
    PipelineVersionRecord,
)

router = APIRouter(prefix="/api/v1/pipelines", tags=["pipelines"])


@router.get("", response_model=PipelinePageResponse)
def list_pipelines(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    cursor: str | None = None,
    include_archived: bool = False,
) -> PipelinePageResponse:
    services = get_services(request)
    page = services.pipelines.list(limit=limit, after=cursor, include_archived=include_archived)
    return PipelinePageResponse(
        items=[_pipeline_response(record) for record in page.items],
        limit=limit,
        next_cursor=None if page.next_cursor is None else page.next_cursor.value,
    )


@router.post("", status_code=status.HTTP_201_CREATED)
def create_pipeline(payload: PipelineCreateRequest, request: Request) -> JSONResponse:
    execution = execute_command(
        request,
        scope="pipelines:create",
        canonical_request=payload.model_dump(mode="json"),
        handler=lambda: _create_outcome(request, payload, converge=False),
        reclaimed_handler=lambda: _create_outcome(request, payload, converge=True),
    )
    return command_response(execution)


@router.get("/{pipeline_id}", response_model=PipelineResponse)
def get_pipeline(pipeline_id: str, request: Request) -> PipelineResponse:
    record = get_services(request).pipelines.get(pipeline_id)
    return _pipeline_response(record)


@router.post("/{pipeline_id}/versions", status_code=status.HTTP_201_CREATED)
def publish_pipeline_version(
    payload: PipelineVersionPublishRequest, pipeline_id: str, request: Request
) -> JSONResponse:
    execution = execute_command(
        request,
        scope="pipelines:publish-version",
        canonical_request={
            "pipeline_id": pipeline_id,
            **payload.model_dump(mode="json"),
        },
        handler=lambda: _publish_outcome(request, pipeline_id, payload, converge=False),
        reclaimed_handler=lambda: _publish_outcome(request, pipeline_id, payload, converge=True),
    )
    return command_response(execution)


@router.get("/{pipeline_id}/versions/{version}", response_model=PipelineVersionResponse)
def get_pipeline_version(
    pipeline_id: str, version: int, request: Request
) -> PipelineVersionResponse:
    record = get_services(request).pipelines.get_version(pipeline_id, version)
    return _version_response(record)


def _create_outcome(
    request: Request, payload: PipelineCreateRequest, *, converge: bool
) -> tuple[int, dict[str, object]]:
    record = get_services(request).pipelines.create(
        pipeline_id=payload.pipeline_id,
        display_name=payload.display_name,
        description=payload.description,
        converge_on_duplicate=converge,
    )
    return status.HTTP_201_CREATED, _pipeline_response(record).model_dump(mode="json")


def _publish_outcome(
    request: Request,
    pipeline_id: str,
    payload: PipelineVersionPublishRequest,
    *,
    converge: bool,
) -> tuple[int, dict[str, object]]:
    record = get_services(request).pipelines.publish(
        pipeline_id=pipeline_id,
        document=payload.document,
        expected_latest_version=payload.expected_latest_version,
        converge_on_duplicate=converge,
    )
    ack = PipelineVersionAckResponse(
        pipeline_id=record.pipeline_id.value,
        version=record.version.number,
        specification_sha256=record.specification_sha256,
        planner_format_version=record.planner_format_version,
        published_at=str(record.published_at),
    )
    return status.HTTP_201_CREATED, ack.model_dump(mode="json")


def _pipeline_response(record: PipelineRecord) -> PipelineResponse:
    return PipelineResponse(
        pipeline_id=record.pipeline_id.value,
        display_name=record.display_name,
        description=record.description,
        created_at=str(record.created_at),
        archived_at=None if record.archived_at is None else str(record.archived_at),
        row_version=record.row_version,
    )


def _version_response(record: PipelineVersionRecord) -> PipelineVersionResponse:
    return PipelineVersionResponse(
        pipeline_id=record.pipeline_id.value,
        version=record.version.number,
        specification=record.specification.to_mapping(),
        specification_sha256=record.specification_sha256,
        planner_format_version=record.planner_format_version,
        published_at=str(record.published_at),
    )
