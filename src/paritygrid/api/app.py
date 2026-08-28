"""FastAPI application factory."""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager

from fastapi import FastAPI

from paritygrid import __version__
from paritygrid.api.dependencies import ApiServices
from paritygrid.api.errors.handlers import register_exception_handlers
from paritygrid.api.frontend import FrontendAssets
from paritygrid.api.middleware.correlation import CorrelationIdMiddleware
from paritygrid.api.middleware.request_limits import (
    RequestLimitSettings,
    RequestLimitsMiddleware,
)
from paritygrid.api.middleware.security_headers import SecurityHeadersMiddleware
from paritygrid.api.operational import ReadinessProbe, ReadinessResult, StaticReadinessProbe
from paritygrid.api.routers.artifacts import router as artifacts_router
from paritygrid.api.routers.connectors import router as connectors_router
from paritygrid.api.routers.live import router as live_router
from paritygrid.api.routers.operational import build_operational_router
from paritygrid.api.routers.pipelines import router as pipelines_router
from paritygrid.api.routers.reconciliation import router as reconciliation_router
from paritygrid.api.routers.repairs import router as repairs_router
from paritygrid.api.routers.runs import router as runs_router
from paritygrid.api.routers.stream import router as stream_router
from paritygrid.api.routers.system import router as system_router

DEFAULT_SERVICE_NAME = "ParityGrid"


def create_app(
    *,
    readiness: ReadinessProbe | None = None,
    service_name: str = DEFAULT_SERVICE_NAME,
    version: str = __version__,
    services: ApiServices | None = None,
    limits: RequestLimitSettings | None = None,
    lifespan: Callable[[FastAPI], AbstractAsyncContextManager[None]] | None = None,
    frontend: FrontendAssets | None = None,
) -> FastAPI:
    """Create an application without opening external resources.

    Resources remain owned by the caller: a composed runtime installs its
    container through ``lifespan`` and ``services``, while the bare factory
    serves liveness and reports itself not ready.
    """
    readiness_probe = readiness or StaticReadinessProbe(
        ReadinessResult(ready=False, detail="Runtime composition is not configured.")
    )
    application = FastAPI(
        title=service_name,
        version=version,
        docs_url="/api/docs",
        redoc_url=None,
        openapi_url="/api/openapi.json",
        lifespan=lifespan,
    )
    application.state.services = services
    # Later additions wrap earlier ones, so the security headers and
    # correlation identity apply to every response, including limits and
    # errors raised by the inner middlewares.
    application.add_middleware(RequestLimitsMiddleware, settings=limits or RequestLimitSettings())
    application.add_middleware(CorrelationIdMiddleware)
    application.add_middleware(SecurityHeadersMiddleware)
    register_exception_handlers(application)
    application.include_router(
        build_operational_router(
            readiness=readiness_probe,
            service=service_name,
            version=version,
        )
    )
    application.include_router(system_router)
    application.include_router(pipelines_router)
    application.include_router(connectors_router)
    application.include_router(runs_router)
    application.include_router(artifacts_router)
    application.include_router(reconciliation_router)
    application.include_router(repairs_router)
    application.include_router(stream_router)
    application.include_router(live_router)
    if frontend is not None:
        application.include_router(frontend.router)
    return application
