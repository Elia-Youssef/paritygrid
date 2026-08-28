"""Regression tests for the independent Phase 12 review findings."""

import socket
import threading

import anyio
import httpx
import pytest
from sqlalchemy import func, select

from paritygrid.adapters.persistence.repositories.idempotency import (
    SqlAlchemyIdempotencyRepository,
)
from paritygrid.adapters.persistence.schema import pipelines as pipelines_table
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.runtime.composition import RuntimeContainer, RuntimeServices
from tests.api.conftest import (
    PIPELINE_ID,
    DeterministicClock,
    build_app,
    clock_driven_services,
    seed_scenario,
)


@pytest.mark.anyio
async def test_publish_key_reuse_across_pipelines_conflicts(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    """Same key plus a different addressed pipeline must conflict, not replay."""
    await seed_scenario(client)
    await client.post(
        "/api/v1/pipelines", json={"pipeline_id": "pip_other-001", "display_name": "B"}
    )
    from tests.api.conftest import DOCUMENT

    document = DOCUMENT
    headers = {"Idempotency-Key": "publish-cross"}
    first = await client.post(
        f"/api/v1/pipelines/{PIPELINE_ID}/versions",
        json={"document": document, "expected_latest_version": 1},
        headers=headers,
    )
    assert first.status_code == 201
    assert first.json()["pipeline_id"] == PIPELINE_ID

    cross = await client.post(
        "/api/v1/pipelines/pip_other-001/versions",
        json={"document": document, "expected_latest_version": 1},
        headers=headers,
    )
    assert cross.status_code == 409
    assert cross.json()["code"] == "idempotency_key_reused"
    # The second pipeline never received a version.
    missing = await client.get("/api/v1/pipelines/pip_other-001/versions/1")
    assert missing.status_code == 404


@pytest.mark.anyio
async def test_nesting_bomb_inside_the_byte_cap_is_a_bounded_rejection(
    client: httpx.AsyncClient,
) -> None:
    bomb = b"[" * 400_000
    response = await client.post(
        "/api/v1/pipelines", content=bomb, headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 400
    assert response.json()["code"] == "malformed_json_body"
    assert "nesting" in response.json()["detail"]


@pytest.mark.anyio
async def test_invalid_correlation_rejection_still_echoes_an_identity(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/healthz", headers={"X-Correlation-ID": "bad id"})
    assert response.status_code == 400
    echoed = response.headers.get("x-correlation-id")
    assert echoed is not None
    assert echoed.startswith("pg-")
    assert response.json()["correlation_id"] == echoed


@pytest.mark.anyio
async def test_empty_body_with_non_json_media_type_is_rejected(
    client: httpx.AsyncClient,
) -> None:
    response = await client.post(
        "/api/v1/pipelines", content=b"", headers={"Content-Type": "text/plain"}
    )
    assert response.status_code == 415
    assert response.json()["code"] == "unsupported_media_type"


def test_pipeline_create_converges_after_a_crashed_owner(
    container: RuntimeContainer,
) -> None:
    """A reclaimed create whose effect already committed replays the record."""
    clock = DeterministicClock()
    canonical = {"pipeline_id": "pip_conv-001", "display_name": "Converged"}
    with container.database.transaction() as session:
        SqlAlchemyIdempotencyRepository(session).begin(
            scope="pipelines:create",
            key="conv-1",
            request=ConfigurationDocument.from_mapping(canonical),
            started_at=clock.now(),
        )
    # The first owner committed the pipeline but never terminalized.
    container.services.pipelines.create(
        pipeline_id="pip_conv-001",
        display_name="Converged",
        description=None,
        converge_on_duplicate=True,
    )
    clock.advance_seconds(61.0)
    services = clock_driven_services(container, clock, lease_seconds=60.0)
    from paritygrid.application.services.idempotency import CommandOutcome

    def handler() -> CommandOutcome:
        record = services.pipelines.create(
            pipeline_id="pip_conv-001",
            display_name="Converged",
            description=None,
            converge_on_duplicate=True,
        )
        return CommandOutcome(
            status_code=201,
            media_type="application/json",
            body={"pipeline_id": record.pipeline_id.value},
            terminal=True,
        )

    execution = services.idempotency.execute(
        scope="pipelines:create", key="conv-1", request=canonical, handler=handler
    )
    assert execution.replayed is False
    assert execution.outcome.status_code == 201
    with container.database.transaction() as session:
        count = session.execute(
            select(func.count())
            .select_from(pipelines_table)
            .where(pipelines_table.c.pipeline_id == "pip_conv-001")
        ).scalar_one()
    assert count == 1


@pytest.mark.anyio
async def test_state_filter_excludes_other_states(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    from paritygrid.domain.execution import RunState
    from tests.api.conftest import transition_run

    await seed_scenario(client, run_id="run_stay-001")
    await seed_scenario(client, run_id="run_gone-001")
    transition_run(container, "run_gone-001", RunState.RUNNING)
    transition_run(container, "run_gone-001", RunState.FAILED)
    listing = await client.get("/api/v1/runs", params={"state": "queued"})
    identifiers = {item["run_id"] for item in listing.json()["items"]}
    assert identifiers == {"run_stay-001"}


@pytest.mark.anyio
async def test_http_retry_of_a_stranded_reservation_conflicts_within_the_lease(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    canonical = {
        "pipeline_id": "pip_strand-001",
        "display_name": "Strand",
        "description": None,
    }
    started_at = container.services.clock()
    with container.database.transaction() as session:
        SqlAlchemyIdempotencyRepository(session).begin(
            scope="pipelines:create",
            key="strand-1",
            request=ConfigurationDocument.from_mapping(canonical),
            started_at=started_at,
        )
    response = await client.post(
        "/api/v1/pipelines", json=canonical, headers={"Idempotency-Key": "strand-1"}
    )
    assert response.status_code == 409
    assert response.json()["code"] == "idempotency_in_progress"


@pytest.mark.anyio
async def test_fresh_idempotency_key_does_not_adopt_an_existing_effect(
    client: httpx.AsyncClient,
) -> None:
    payload = {"pipeline_id": "pip_fresh-key", "display_name": "Owned once"}
    first = await client.post(
        "/api/v1/pipelines",
        json=payload,
        headers={"Idempotency-Key": "first-owner"},
    )
    second = await client.post(
        "/api/v1/pipelines",
        json=payload,
        headers={"Idempotency-Key": "different-owner"},
    )

    assert first.status_code == 201
    assert second.status_code == 409
    assert second.json()["code"] == "duplicate_record"


@pytest.mark.anyio
async def test_catch_all_internal_error_renders_a_problem_over_real_http(
    container: RuntimeContainer,
) -> None:
    import uvicorn

    class ExplodingPipelines:
        def list(self, *, limit: int, after: str | None, include_archived: bool) -> None:
            raise KeyError("boom")

    services = RuntimeServices(
        pipelines=ExplodingPipelines(),  # type: ignore[arg-type]
        connectors=container.services.connectors,
        connector_tests=container.services.connector_tests,
        runs=container.services.runs,
        run_lifecycle=container.services.run_lifecycle,
        artifacts=container.services.artifacts,
        idempotency=container.services.idempotency,
        capabilities=container.services.capabilities,
        reconciliation=container.services.reconciliation,
        repair=container.services.repair,
        repair_application=container.services.repair_application,
        event_stream=container.services.event_stream,
        telemetry=container.services.telemetry,
        clock=container.services.clock,
    )
    application = build_app(container, services=services)
    config = uvicorn.Config(application, host="127.0.0.1", port=0, log_level="critical")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        for _ in range(100):
            if server.started:
                break
            await anyio.sleep(0.05)
        assert server.started
        port = server.servers[0].sockets[0].getsockname()[1]
        with socket.create_connection(("127.0.0.1", port), timeout=5.0) as sock:
            sock.sendall(
                f"GET /api/v1/pipelines HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
                "Connection: close\r\n\r\n".encode("ascii")
            )
            sock.settimeout(5.0)
            payload = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                payload += chunk
        assert b"500" in payload.split(b"\r\n", 1)[0]
        assert b"application/problem+json" in payload
        assert b"boom" not in payload
    finally:
        server.should_exit = True
        thread.join(timeout=10.0)
