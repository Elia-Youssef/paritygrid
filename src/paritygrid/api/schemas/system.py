"""Versioned transport schemas for system capability reporting."""

from typing import Literal

from pydantic import BaseModel, ConfigDict

from paritygrid.api.schemas.common import TRANSPORT_SCHEMA_VERSION


class SqliteCapabilitiesBody(BaseModel):
    """Public SQLite runtime facts."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    library_version: str
    minimum_supported_version: str
    threadsafety: int
    journal_mode: str
    synchronous_level: int
    busy_timeout_ms: int
    supports_json_sql: bool
    supports_returning: bool


class RunnerStrategyBody(BaseModel):
    """One full-plan strategy availability fact."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    strategy_id: str
    available: bool
    unavailability_reason: str | None = None


class SubordinatePoolBody(BaseModel):
    """One subordinate pool availability fact."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    pool_id: str
    available: bool
    unavailability_reason: str | None = None


class OperationalLimitsBody(BaseModel):
    """The configured public request limits."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    max_request_body_bytes: int
    max_json_depth: int
    max_concurrent_requests: int
    request_timeout_seconds: float
    max_page_size: int
    idempotency_lease_seconds: float
    artifact_chunk_bytes: int


class FeatureBody(BaseModel):
    """One optional feature availability flag."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    available: bool


class CapabilitiesResponse(BaseModel):
    """Everything ``GET /api/v1/system/capabilities`` reports."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    service: str
    version: str
    sqlite: SqliteCapabilitiesBody
    runners: list[RunnerStrategyBody]
    subordinate_pools: list[SubordinatePoolBody]
    limits: OperationalLimitsBody
    features: list[FeatureBody]
