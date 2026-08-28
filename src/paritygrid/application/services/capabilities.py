"""Versioned capability and limit reporting for the operational boundary."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SqliteCapabilityView:
    """Public SQLite runtime facts without host paths or secrets."""

    library_version: str
    minimum_supported_version: str
    threadsafety: int
    journal_mode: str
    synchronous_level: int
    busy_timeout_ms: int
    supports_json_sql: bool
    supports_returning: bool


@dataclass(frozen=True, slots=True)
class RunnerStrategyView:
    """One full-plan strategy availability fact."""

    strategy_id: str
    available: bool
    unavailability_reason: str | None


@dataclass(frozen=True, slots=True)
class SubordinatePoolView:
    """One subordinate pool availability fact."""

    pool_id: str
    available: bool
    unavailability_reason: str | None


@dataclass(frozen=True, slots=True)
class OperationalLimitsView:
    """The configured public request limits."""

    max_request_body_bytes: int
    max_json_depth: int
    max_concurrent_requests: int
    request_timeout_seconds: float
    max_page_size: int
    idempotency_lease_seconds: float
    artifact_chunk_bytes: int


@dataclass(frozen=True, slots=True)
class CapabilitiesView:
    """Everything the capabilities route may report."""

    service: str
    version: str
    sqlite: SqliteCapabilityView
    runners: tuple[RunnerStrategyView, ...]
    pools: tuple[SubordinatePoolView, ...]
    limits: OperationalLimitsView
    features: tuple[tuple[str, bool], ...]
