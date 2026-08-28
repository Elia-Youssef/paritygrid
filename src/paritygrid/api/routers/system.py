"""System capability reporting route."""

from fastapi import APIRouter, Request

from paritygrid.api.dependencies import get_services
from paritygrid.api.schemas.system import (
    CapabilitiesResponse,
    FeatureBody,
    OperationalLimitsBody,
    RunnerStrategyBody,
    SqliteCapabilitiesBody,
    SubordinatePoolBody,
)

router = APIRouter(prefix="/api/v1/system", tags=["system"])


@router.get("/capabilities", response_model=CapabilitiesResponse)
def capabilities(request: Request) -> CapabilitiesResponse:
    view = get_services(request).capabilities
    return CapabilitiesResponse(
        service=view.service,
        version=view.version,
        sqlite=SqliteCapabilitiesBody(
            library_version=view.sqlite.library_version,
            minimum_supported_version=view.sqlite.minimum_supported_version,
            threadsafety=view.sqlite.threadsafety,
            journal_mode=view.sqlite.journal_mode,
            synchronous_level=view.sqlite.synchronous_level,
            busy_timeout_ms=view.sqlite.busy_timeout_ms,
            supports_json_sql=view.sqlite.supports_json_sql,
            supports_returning=view.sqlite.supports_returning,
        ),
        runners=[
            RunnerStrategyBody(
                strategy_id=runner.strategy_id,
                available=runner.available,
                unavailability_reason=runner.unavailability_reason,
            )
            for runner in view.runners
        ],
        subordinate_pools=[
            SubordinatePoolBody(
                pool_id=pool.pool_id,
                available=pool.available,
                unavailability_reason=pool.unavailability_reason,
            )
            for pool in view.pools
        ],
        limits=OperationalLimitsBody(
            max_request_body_bytes=view.limits.max_request_body_bytes,
            max_json_depth=view.limits.max_json_depth,
            max_concurrent_requests=view.limits.max_concurrent_requests,
            request_timeout_seconds=view.limits.request_timeout_seconds,
            max_page_size=view.limits.max_page_size,
            idempotency_lease_seconds=view.limits.idempotency_lease_seconds,
            artifact_chunk_bytes=view.limits.artifact_chunk_bytes,
        ),
        features=[FeatureBody(name=name, available=available) for name, available in view.features],
    )
