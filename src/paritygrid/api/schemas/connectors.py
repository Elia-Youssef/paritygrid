"""Versioned transport schemas for connector routes."""

from typing import Literal

from pydantic import Field

from paritygrid.api.schemas.common import TRANSPORT_SCHEMA_VERSION, TransportModel


class ConnectorSecretReferenceBody(TransportModel):
    """One secret reference: names only, never resolved values."""

    reference_name: str = Field(min_length=1, max_length=128)
    environment_variable_name: str = Field(
        min_length=1, max_length=128, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$"
    )


class ConnectorCreateRequest(TransportModel):
    """Body of ``POST /api/v1/connectors``."""

    connector_id: str = Field(
        min_length=5, max_length=68, pattern=r"^con_[a-z0-9]+(?:-[a-z0-9]+)*$"
    )
    kind: str = Field(min_length=1, max_length=96)
    display_name: str = Field(min_length=1, max_length=128)
    configuration: dict[str, object]
    capabilities: dict[str, object]
    schema_discovery: dict[str, object] | None = None
    secret_references: list[ConnectorSecretReferenceBody] = Field(
        default_factory=list[ConnectorSecretReferenceBody], max_length=64
    )


class ConnectorResponse(TransportModel):
    """One connector record with secret reference names only."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    connector_id: str
    kind: str
    display_name: str
    configuration: dict[str, object]
    capabilities: dict[str, object]
    schema_discovery: dict[str, object] | None
    secret_references: list[ConnectorSecretReferenceBody]
    revision: int
    created_at: str
    updated_at: str
    archived_at: str | None
    row_version: int


class ConnectorCreateAckResponse(TransportModel):
    """Registration acknowledgement without secret reference names.

    Secret reference names stay addressable through connector records; the
    acknowledgement shape passes the durable redaction wall applied to
    stored idempotent responses.
    """

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    connector_id: str
    kind: str
    display_name: str
    revision: int
    created_at: str
    row_version: int


class ConnectorPageResponse(TransportModel):
    """Paginated connector collection."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    items: list[ConnectorResponse]
    limit: int
    next_cursor: str | None


class ConnectorTestCheckBody(TransportModel):
    """One bounded connection-test check result."""

    name: str
    passed: bool
    detail: str


class ConnectorTestResponse(TransportModel):
    """Bounded connection-test evidence for one connector."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    connector_id: str
    kind: str
    passed: bool
    checks: list[ConnectorTestCheckBody]
    tested_at: str
