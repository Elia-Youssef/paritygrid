"""Versioned transport schemas for pipeline routes."""

from typing import Literal

from pydantic import Field

from paritygrid.api.schemas.common import TRANSPORT_SCHEMA_VERSION, TransportModel


class PipelineCreateRequest(TransportModel):
    """Body of ``POST /api/v1/pipelines``."""

    pipeline_id: str = Field(min_length=7, max_length=68, pattern=r"^pip_[a-z0-9]+(?:-[a-z0-9]+)*$")
    display_name: str = Field(min_length=1, max_length=128)
    description: str | None = Field(default=None, min_length=1, max_length=1024)


class PipelineResponse(TransportModel):
    """One pipeline identity with its mutable display metadata."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    pipeline_id: str
    display_name: str
    description: str | None
    created_at: str
    archived_at: str | None
    row_version: int


class PipelinePageResponse(TransportModel):
    """Paginated pipeline collection."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    items: list[PipelineResponse]
    limit: int
    next_cursor: str | None


class PipelineVersionPublishRequest(TransportModel):
    """Body of ``POST /api/v1/pipelines/{id}/versions``."""

    expected_latest_version: int | None = Field(default=None, ge=0, le=2_147_483_647)
    document: dict[str, object]


class PipelineVersionResponse(TransportModel):
    """One immutable published pipeline version."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    pipeline_id: str
    version: int
    specification: dict[str, object]
    specification_sha256: str
    planner_format_version: int
    published_at: str


class PipelineVersionAckResponse(TransportModel):
    """Publication acknowledgement without the specification body.

    The published specification embeds connector binding snapshots whose
    field names cannot pass the durable redaction wall applied to stored
    idempotent responses; the full document is addressable through the
    version route.
    """

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    pipeline_id: str
    version: int
    specification_sha256: str
    planner_format_version: int
    published_at: str
