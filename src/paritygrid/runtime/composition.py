"""Runtime dependency composition."""

from fastapi import FastAPI

from paritygrid.api.app import create_app
from paritygrid.api.operational import ReadinessResult, StaticReadinessProbe
from paritygrid.runtime.config import Settings


def create_runtime_app(settings: Settings | None = None) -> FastAPI:
    """Assemble the HTTP application from validated runtime settings."""
    runtime_settings = settings or Settings()
    readiness = StaticReadinessProbe(
        ReadinessResult(ready=True, detail="Runtime initialization is complete.")
    )
    application = create_app(readiness=readiness)
    application.state.settings = runtime_settings
    return application
