"""Liveness and readiness endpoints."""

from typing import Literal

from fastapi import APIRouter, Response, status

from paritygrid.api.operational import ReadinessProbe
from paritygrid.api.schemas.operational import HealthResponse, ReadinessResponse


def build_operational_router(
    *,
    readiness: ReadinessProbe,
    service: str,
    version: str,
) -> APIRouter:
    """Build operational routes around the supplied readiness boundary."""
    router = APIRouter(tags=["system"])

    async def health_response() -> HealthResponse:
        return HealthResponse(service=service, version=version)

    router.add_api_route(
        "/healthz",
        health_response,
        methods=["GET"],
        response_model=HealthResponse,
    )

    async def readiness_response(response: Response) -> ReadinessResponse:
        result = await readiness.check()
        if not result.ready:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        readiness_status: Literal["ready", "not_ready"] = "ready" if result.ready else "not_ready"
        return ReadinessResponse(
            status=readiness_status,
            service=service,
            version=version,
            detail=result.detail,
        )

    router.add_api_route(
        "/readyz",
        readiness_response,
        methods=["GET"],
        response_model=ReadinessResponse,
        responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ReadinessResponse}},
    )

    return router
