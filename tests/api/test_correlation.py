"""Correlation identity validation and propagation tests."""

from typing import cast

import httpx
import pytest
from sqlalchemy import select

from paritygrid.adapters.persistence.schema import execution_events
from paritygrid.runtime.composition import RuntimeContainer
from tests.api.conftest import seed_scenario


@pytest.mark.anyio
async def test_correlation_id_is_generated_when_absent(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/healthz")
    correlation = response.headers.get("x-correlation-id")
    assert correlation is not None
    assert correlation.startswith("pg-")
    assert 1 <= len(correlation) <= 96


@pytest.mark.anyio
async def test_supplied_correlation_id_is_validated_and_echoed(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/healthz", headers={"X-Correlation-ID": "op-42.abc"})
    assert response.headers["x-correlation-id"] == "op-42.abc"


@pytest.mark.anyio
async def test_invalid_correlation_id_is_rejected(client: httpx.AsyncClient) -> None:
    invalid: list[object] = [
        "has space",
        "x" * 97,
        "semi;colon",
        # Raw non-ASCII bytes survive header transport and must fail the
        # portable-ASCII validation instead of reaching the application.
        "ümlaut".encode(),
    ]
    for bad in invalid:
        response = await client.get("/healthz", headers={"X-Correlation-ID": cast(str, bad)})
        assert response.status_code == 400, bad
        body = response.json()
        assert body["code"] == "invalid_correlation_id"


@pytest.mark.anyio
async def test_problem_responses_carry_the_correlation_identity(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get(
        "/api/v1/pipelines/pip_missing-one", headers={"X-Correlation-ID": "corr-probe"}
    )
    assert response.status_code == 404
    assert response.json()["correlation_id"] == "corr-probe"
    assert response.headers["x-correlation-id"] == "corr-probe"


@pytest.mark.anyio
async def test_run_creation_propagates_correlation_into_durable_events(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    await seed_scenario(client)
    # Recreate a run with a distinct correlation identity.
    response = await client.post(
        "/api/v1/runs",
        json={
            "run_id": "run_scenario-02",
            "pipeline_id": "pip_demo-alpha",
            "pipeline_version": 1,
            "runner_kind": "sequential",
            "scenario_seed": 7,
        },
        headers={"X-Correlation-ID": "corr-durable-1"},
    )
    assert response.status_code == 201
    with container.database.transaction() as session:
        rows = session.execute(
            select(
                execution_events.c.event_kind,
                execution_events.c.correlation_id,
            ).where(execution_events.c.run_id == "run_scenario-02")
        ).all()
    assert rows
    assert all(correlation == "corr-durable-1" for _, correlation in rows)
    assert any(kind == "run_created" for kind, _ in rows)
