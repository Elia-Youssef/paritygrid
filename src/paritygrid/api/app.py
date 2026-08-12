"""FastAPI application factory."""

from fastapi import FastAPI

from paritygrid import __version__
from paritygrid.api.operational import ReadinessProbe, ReadinessResult, StaticReadinessProbe
from paritygrid.api.routers.operational import build_operational_router

DEFAULT_SERVICE_NAME = "ParityGrid"


def create_app(
    *,
    readiness: ReadinessProbe | None = None,
    service_name: str = DEFAULT_SERVICE_NAME,
    version: str = __version__,
) -> FastAPI:
    """Create an application without opening external resources."""
    readiness_probe = readiness or StaticReadinessProbe(
        ReadinessResult(ready=True, detail="Runtime initialization is complete.")
    )
    application = FastAPI(
        title=service_name,
        version=version,
        docs_url="/api/docs",
        redoc_url=None,
        openapi_url="/api/openapi.json",
    )
    application.include_router(
        build_operational_router(
            readiness=readiness_probe,
            service=service_name,
            version=version,
        )
    )
    return application
