"""Versioned transport schemas for run lifecycle routes."""

from typing import Literal

from pydantic import Field

from paritygrid.api.schemas.common import TRANSPORT_SCHEMA_VERSION, TransportModel


class RunCreateRequest(TransportModel):
    """Body of ``POST /api/v1/runs``."""

    run_id: str = Field(min_length=5, max_length=68, pattern=r"^run_[a-z0-9]+(?:-[a-z0-9]+)*$")
    pipeline_id: str = Field(min_length=7, max_length=68, pattern=r"^pip_[a-z0-9]+(?:-[a-z0-9]+)*$")
    pipeline_version: int = Field(ge=1, le=2_147_483_647)
    runner_kind: str = Field(min_length=1, max_length=32)
    scenario_seed: int | None = Field(default=None, ge=0, le=2_147_483_647)
    runner_configuration: dict[str, object] | None = None


class RunResponse(TransportModel):
    """One run snapshot with version coherence fields."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    run_id: str
    run_version: int
    state: str
    pipeline_id: str
    pipeline_version: int
    runner_kind: str
    scenario_seed: int | None
    created_at: str
    started_at: str | None
    finished_at: str | None
    cancellation_requested_at: str | None
    observed_at: str
    execution_evidence_fingerprint: str | None = None
    execution_evidence_fingerprint_version: int | None = None


class RunPageResponse(TransportModel):
    """Paginated run collection with version coherence."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    items: list[RunResponse]
    limit: int
    next_cursor: str | None
