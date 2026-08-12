"""Operational endpoint contract tests."""

from dataclasses import dataclass

import pytest
from httpx import ASGITransport, AsyncClient

from paritygrid.api.app import create_app
from paritygrid.api.operational import ReadinessResult


@dataclass(frozen=True, slots=True)
class StubReadinessProbe:
    result: ReadinessResult

    async def check(self) -> ReadinessResult:
        return self.result


def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_health_reports_process_liveness_without_readiness_dependency() -> None:
    probe = StubReadinessProbe(ReadinessResult(ready=False, detail="Storage is unavailable."))

    async with AsyncClient(
        transport=ASGITransport(app=create_app(readiness=probe)),
        base_url="http://testserver",
    ) as client:
        response = await client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "ParityGrid",
        "version": "0.1.0",
    }


@pytest.mark.anyio
async def test_ready_reports_initialized_runtime() -> None:
    probe = StubReadinessProbe(ReadinessResult(ready=True, detail="Storage is ready."))

    async with AsyncClient(
        transport=ASGITransport(app=create_app(readiness=probe)),
        base_url="http://testserver",
    ) as client:
        response = await client.get("/readyz")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "service": "ParityGrid",
        "version": "0.1.0",
        "detail": "Storage is ready.",
    }


@pytest.mark.anyio
async def test_ready_reports_service_unavailable_when_runtime_is_not_initialized() -> None:
    probe = StubReadinessProbe(ReadinessResult(ready=False, detail="Migration is pending."))

    async with AsyncClient(
        transport=ASGITransport(app=create_app(readiness=probe)),
        base_url="http://testserver",
    ) as client:
        response = await client.get("/readyz")

    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "service": "ParityGrid",
        "version": "0.1.0",
        "detail": "Migration is pending.",
    }
