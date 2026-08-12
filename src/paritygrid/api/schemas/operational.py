"""Response schemas for operational endpoints."""

from typing import Literal

from pydantic import BaseModel, ConfigDict


class HealthResponse(BaseModel):
    """Process liveness response."""

    model_config = ConfigDict(frozen=True)

    status: Literal["ok"] = "ok"
    service: str
    version: str


class ReadinessResponse(BaseModel):
    """Runtime readiness response."""

    model_config = ConfigDict(frozen=True)

    status: Literal["ready", "not_ready"]
    service: str
    version: str
    detail: str
