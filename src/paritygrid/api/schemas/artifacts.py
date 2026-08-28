"""Versioned transport schemas for artifact routes."""

from typing import Literal

from paritygrid.api.schemas.common import TRANSPORT_SCHEMA_VERSION, TransportModel


class ArtifactResponse(TransportModel):
    """One committed artifact manifest summary.

    The confined relative path stays internal: callers address artifacts by
    their committed manifest identity only.
    """

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    artifact_id: str
    run_id: str
    node_id: str
    partition_key: str
    media_type: str
    artifact_schema_version: int
    byte_size: int
    row_count: int
    sha256: str
    created_at: str


class ArtifactPageResponse(TransportModel):
    """Paginated artifact listing for one run with run version coherence."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    run_id: str
    run_version: int
    observed_at: str
    items: list[ArtifactResponse]
    limit: int
    next_cursor: str | None
