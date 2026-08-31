"""Connector registration and connection-test routes."""

from typing import Annotated

from fastapi import APIRouter, Query, Request, status
from fastapi.responses import JSONResponse

from paritygrid.api.dependencies import (
    command_response,
    execute_command,
    get_services,
)
from paritygrid.api.schemas.connectors import (
    ConnectorCreateAckResponse,
    ConnectorCreateRequest,
    ConnectorPageResponse,
    ConnectorResponse,
    ConnectorSecretReferenceBody,
    ConnectorTestCheckBody,
    ConnectorTestResponse,
)
from paritygrid.application.ports.configuration import ConnectorRecord

router = APIRouter(prefix="/api/v1/connectors", tags=["connectors"])


@router.get("", response_model=ConnectorPageResponse)
def list_connectors(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    cursor: str | None = None,
    include_archived: bool = False,
) -> ConnectorPageResponse:
    page = get_services(request).connectors.list(
        limit=limit, after=cursor, include_archived=include_archived
    )
    return ConnectorPageResponse(
        items=[_connector_response(record) for record in page.items],
        limit=limit,
        next_cursor=None if page.next_cursor is None else page.next_cursor.value,
    )


@router.post("", status_code=status.HTTP_201_CREATED)
def create_connector(payload: ConnectorCreateRequest, request: Request) -> JSONResponse:
    execution = execute_command(
        request,
        scope="connectors:create",
        canonical_request=payload.model_dump(mode="json"),
        handler=lambda: _create_outcome(request, payload, converge=False),
        reclaimed_handler=lambda: _create_outcome(request, payload, converge=True),
    )
    return command_response(execution)


@router.post("/{connector_id}/test")
def test_connector(connector_id: str, request: Request) -> JSONResponse:
    execution = execute_command(
        request,
        scope="connectors:test",
        canonical_request={"connector_id": connector_id},
        handler=lambda: _test_outcome(request, connector_id),
    )
    return command_response(execution)


def _create_outcome(
    request: Request, payload: ConnectorCreateRequest, *, converge: bool
) -> tuple[int, dict[str, object]]:
    record = get_services(request).connectors.register(
        connector_id=payload.connector_id,
        kind=payload.kind,
        display_name=payload.display_name,
        configuration=payload.configuration,
        capabilities=payload.capabilities,
        schema_discovery=payload.schema_discovery,
        secret_references=[
            (item.reference_name, item.environment_variable_name)
            for item in payload.secret_references
        ],
        converge_on_duplicate=converge,
    )
    ack = ConnectorCreateAckResponse(
        connector_id=record.connector_id.value,
        kind=record.kind,
        display_name=record.display_name,
        revision=record.revision,
        created_at=str(record.created_at),
        row_version=record.row_version,
    )
    return status.HTTP_201_CREATED, ack.model_dump(mode="json")


def _test_outcome(request: Request, connector_id: str) -> tuple[int, dict[str, object]]:
    services = get_services(request)
    report = services.connector_tests.test(connector_id)
    response = ConnectorTestResponse(
        connector_id=report.connector_id,
        kind=report.kind,
        passed=report.passed,
        checks=[
            ConnectorTestCheckBody(name=check.name, passed=check.passed, detail=check.detail)
            for check in report.checks
        ],
        tested_at=str(report.tested_at),
    )
    return status.HTTP_200_OK, response.model_dump(mode="json")


def _connector_response(record: ConnectorRecord) -> ConnectorResponse:
    return ConnectorResponse(
        connector_id=record.connector_id.value,
        kind=record.kind,
        display_name=record.display_name,
        configuration=record.configuration.to_mapping(),
        capabilities=record.capabilities.to_mapping(),
        schema_discovery=(
            None if record.schema_discovery is None else record.schema_discovery.to_mapping()
        ),
        secret_references=[
            ConnectorSecretReferenceBody(
                reference_name=reference.reference_name,
                environment_variable_name=reference.environment_variable_name,
            )
            for reference in record.secret_references
        ],
        revision=record.revision,
        created_at=str(record.created_at),
        updated_at=str(record.updated_at),
        archived_at=None if record.archived_at is None else str(record.archived_at),
        row_version=record.row_version,
    )
