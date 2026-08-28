"""Live telemetry WebSocket contracts: snapshots, validation, isolation."""

import json
from typing import Any, cast

import anyio
import httpx
import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect, WebSocketState

from paritygrid.api.app import create_app
from paritygrid.api.routers import live as live_router
from paritygrid.application.ports.writer import WriterDiagnostics, WriterState
from paritygrid.application.services.telemetry import (
    LiveTelemetryChannel,
    LiveTelemetryHub,
    snapshot_record,
)
from paritygrid.domain.models import UtcTimestamp
from paritygrid.runtime.composition import (
    RuntimeContainer,
    RuntimeReadinessProbe,
)
from tests.api.conftest import seed_scenario

RUN_ID = "run_live-01"


def _wait_for_cleanup(hub: LiveTelemetryHub, run_id: str) -> None:
    import time

    for _ in range(200):
        if hub.subscriber_count(run_id) == 0:
            return
        time.sleep(0.02)


def _app(container: RuntimeContainer) -> FastAPI:
    return create_app(
        readiness=RuntimeReadinessProbe(container_provider=lambda: container),
        limits=container.limits,
        services=container.services,
    )


def _seed(container: RuntimeContainer) -> None:
    async def seed() -> None:
        transport = httpx.ASGITransport(app=_app(container))
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            await seed_scenario(client, run_id=RUN_ID)

    anyio.run(seed)


def test_connect_snapshot_ping_pong_and_disconnect_cleanup(
    container: RuntimeContainer,
) -> None:
    _seed(container)
    hub = container.services.telemetry.hub
    with TestClient(_app(container)) as test_client:  # noqa: SIM117
        with test_client.websocket_connect(f"/api/v1/live/runs/{RUN_ID}") as socket:
            snapshot = json.loads(socket.receive_text())
            assert snapshot["channel"] == "telemetry"
            assert snapshot["advisory"] is True
            assert snapshot["records"][0]["run_id"] == RUN_ID
            metric_names = {metric["name"] for metric in snapshot["records"][0]["metrics"]}
            assert "writer_queue_depth" in metric_names
            socket.send_text(json.dumps({"type": "ping"}))
            pong = json.loads(socket.receive_text())
            assert pong["type"] == "pong"
            assert pong["channel"] == "telemetry"
    _wait_for_cleanup(hub, RUN_ID)


def test_published_telemetry_reaches_the_live_client(container: RuntimeContainer) -> None:
    _seed(container)
    hub = container.services.telemetry.hub
    with TestClient(_app(container)) as test_client:  # noqa: SIM117
        with test_client.websocket_connect(f"/api/v1/live/runs/{RUN_ID}") as socket:
            json.loads(socket.receive_text())
            hub.publish(
                snapshot_record(
                    run_id=RUN_ID,
                    observed_at_micros=1_000,
                    queue_depth=3,
                    queue_capacity=64,
                )
            )
            frame = json.loads(socket.receive_text())
            assert frame["channel"] == "telemetry"
            assert frame["advisory"] is True
            assert frame["sampled"] is True
            assert frame["records"][0]["observed_at_micros"] == 1_000
            assert "sequence" not in frame


def test_live_transport_publishes_current_writer_diagnostics(container: RuntimeContainer) -> None:
    """The live transport has a production publisher, independent of test injection."""
    _seed(container)
    with TestClient(_app(container)) as test_client:  # noqa: SIM117
        with test_client.websocket_connect(f"/api/v1/live/runs/{RUN_ID}") as socket:
            json.loads(socket.receive_text())
            frame = json.loads(socket.receive_text())
            assert frame["channel"] == "telemetry"
            assert frame["advisory"] is True
            assert frame["sampled"] is True
            assert frame["records"][0]["run_id"] == RUN_ID
            assert "sequence" not in frame


def test_unknown_run_is_rejected_before_upgrade(container: RuntimeContainer) -> None:
    with (
        TestClient(_app(container)) as test_client,
        pytest.raises(WebSocketDisconnect) as closed,
        test_client.websocket_connect("/api/v1/live/runs/run_ghost-996"),
    ):
        pass
    assert closed.value.code == live_router.RUN_NOT_FOUND_CLOSE_CODE


def test_malformed_client_message_closes_with_policy_code(
    container: RuntimeContainer,
) -> None:
    _seed(container)
    with TestClient(_app(container)) as test_client:  # noqa: SIM117
        with test_client.websocket_connect(f"/api/v1/live/runs/{RUN_ID}") as socket:
            json.loads(socket.receive_text())
            socket.send_text("this is not json")
            with pytest.raises(WebSocketDisconnect) as closed:
                socket.receive_text()
    assert closed.value.code == live_router.POLICY_CLOSE_CODE


def test_unsupported_client_message_closes_with_policy_code(
    container: RuntimeContainer,
) -> None:
    _seed(container)
    with TestClient(_app(container)) as test_client:  # noqa: SIM117
        with test_client.websocket_connect(f"/api/v1/live/runs/{RUN_ID}") as socket:
            json.loads(socket.receive_text())
            socket.send_text(json.dumps({"type": "commands"}))
            with pytest.raises(WebSocketDisconnect) as closed:
                socket.receive_text()
    assert closed.value.code == live_router.POLICY_CLOSE_CODE


def test_binary_client_message_closes_with_policy_code(
    container: RuntimeContainer,
) -> None:
    """The live protocol accepts UTF-8 text JSON only, never binary frames."""
    _seed(container)
    with TestClient(_app(container)) as test_client:  # noqa: SIM117
        with test_client.websocket_connect(f"/api/v1/live/runs/{RUN_ID}") as socket:
            json.loads(socket.receive_text())
            socket.send_bytes(b'{"type":"ping"}')
            with pytest.raises(WebSocketDisconnect) as closed:
                socket.receive_text()
    assert closed.value.code == live_router.POLICY_CLOSE_CODE


def test_extra_client_message_fields_close_with_policy_code(
    container: RuntimeContainer,
) -> None:
    _seed(container)
    with TestClient(_app(container)) as test_client:  # noqa: SIM117
        with test_client.websocket_connect(f"/api/v1/live/runs/{RUN_ID}") as socket:
            json.loads(socket.receive_text())
            socket.send_text(json.dumps({"type": "ping", "unexpected": True}))
            with pytest.raises(WebSocketDisconnect) as closed:
                socket.receive_text()
    assert closed.value.code == live_router.POLICY_CLOSE_CODE


def test_oversized_client_message_closes_with_size_code(
    container: RuntimeContainer,
) -> None:
    _seed(container)
    with TestClient(_app(container)) as test_client:  # noqa: SIM117
        with test_client.websocket_connect(f"/api/v1/live/runs/{RUN_ID}") as socket:
            json.loads(socket.receive_text())
            socket.send_text("x" * 5_000)
            with pytest.raises(WebSocketDisconnect) as closed:
                socket.receive_text()
    assert closed.value.code == live_router.OVERSIZED_CLOSE_CODE


def test_disconnected_live_client_cannot_block_or_modify_execution(
    container: RuntimeContainer,
) -> None:
    from paritygrid.adapters.persistence.repositories.runs import SqlAlchemyRunRepository
    from paritygrid.domain.models import RunId

    _seed(container)
    with container.database.transaction() as session:
        before = SqlAlchemyRunRepository(session).get(RunId(RUN_ID))
    assert before is not None
    version_before = before.row_version
    hub = container.services.telemetry.hub
    with TestClient(_app(container)) as test_client:
        with test_client.websocket_connect(f"/api/v1/live/runs/{RUN_ID}") as socket:
            json.loads(socket.receive_text())
        # The client is gone; publishing a large burst must neither block
        # nor change any durable state.
        for index in range(5_000):
            hub.publish(
                snapshot_record(
                    run_id=RUN_ID,
                    observed_at_micros=index,
                    queue_depth=index % 64,
                    queue_capacity=64,
                )
            )
    with container.database.transaction() as session:
        after = SqlAlchemyRunRepository(session).get(RunId(RUN_ID))
    assert after is not None
    version_after = after.row_version
    assert version_after == version_before
    _wait_for_cleanup(hub, RUN_ID)
    assert hub.subscriber_count(RUN_ID) == 0


def test_reconnect_receives_fresh_advisory_snapshot_and_never_a_durable_sequence(
    container: RuntimeContainer,
) -> None:
    _seed(container)
    hub = container.services.telemetry.hub
    with TestClient(_app(container)) as test_client:
        with test_client.websocket_connect(f"/api/v1/live/runs/{RUN_ID}") as first:
            initial = json.loads(first.receive_text())
            sampled = json.loads(first.receive_text())
            assert initial["advisory"] is True
            assert sampled["sampled"] is True
            assert "sequence" not in sampled
        _wait_for_cleanup(hub, RUN_ID)
        assert hub.subscriber_count(RUN_ID) == 0
        with test_client.websocket_connect(f"/api/v1/live/runs/{RUN_ID}") as second:
            fresh = json.loads(second.receive_text())
            assert fresh["advisory"] is True
            assert fresh["records"][0]["run_id"] == RUN_ID
    _wait_for_cleanup(hub, RUN_ID)
    assert hub.subscriber_count(RUN_ID) == 0


def test_hub_bounds_queues_and_subscriber_capacity() -> None:
    hub = LiveTelemetryHub(queue_capacity=4, max_subscribers_per_run=1)
    first = hub.subscribe("run_hub-01")
    with pytest.raises(Exception, match="maximum number of live subscribers"):
        hub.subscribe("run_hub-01")
    stale = LiveTelemetryHub(queue_capacity=2)
    abandoned = stale.subscribe("run_hub-02")
    abandoned.close()
    fresh = stale.subscribe("run_hub-02")
    assert fresh is not abandoned
    for index in range(10):
        hub.publish(
            snapshot_record(
                run_id="run_hub-01",
                observed_at_micros=index,
                queue_depth=0,
                queue_capacity=1,
            )
        )
    drained = first.drain()
    assert len(drained) == 4
    assert first.dropped == 6
    hub.close()
    assert first.closed
    assert hub.subscriber_count("run_hub-01") == 0


def _flat_diagnostics() -> WriterDiagnostics:
    return WriterDiagnostics(
        state=WriterState.RUNNING,
        queue_capacity=64,
        admission_capacity=64,
        accepted=0,
        completed=0,
        queue_depth=0,
        admission_waiters=0,
        in_flight=0,
        max_queue_depth=0,
        max_admission_waiters=0,
        max_resident=0,
        contention_retries=0,
    )


def test_channel_validates_bounds() -> None:
    with pytest.raises(ValueError, match="outside the supported range"):
        LiveTelemetryChannel(
            hub=LiveTelemetryHub(),
            writer_snapshot=_flat_diagnostics,
            clock=_frozen_clock,
            send_timeout_seconds=0.01,
        )
    with pytest.raises(ValueError, match="outside the supported range"):
        LiveTelemetryChannel(
            hub=LiveTelemetryHub(),
            writer_snapshot=_flat_diagnostics,
            clock=_frozen_clock,
            poll_seconds=0.01,
        )
    with pytest.raises(ValueError, match="supported range"):
        LiveTelemetryHub(queue_capacity=0)
    with pytest.raises(ValueError, match="supported range"):
        LiveTelemetryHub(max_subscribers_per_run=0)


def test_slow_consumer_is_closed_without_blocking_the_publisher() -> None:
    class BlockedWebSocket:
        application_state = WebSocketState.CONNECTED

        def __init__(self) -> None:
            self.close_codes: list[int] = []

        async def send_text(self, _text: str) -> None:
            await anyio.sleep(5.0)

        async def close(self, *, code: int, reason: str) -> None:
            del reason
            self.close_codes.append(code)

    hub = LiveTelemetryHub(queue_capacity=1)
    subscription = hub.subscribe(RUN_ID)
    hub.publish(
        snapshot_record(run_id=RUN_ID, observed_at_micros=1, queue_depth=0, queue_capacity=1)
    )
    channel = LiveTelemetryChannel(
        hub=hub,
        writer_snapshot=_flat_diagnostics,
        clock=_frozen_clock,
        send_timeout_seconds=0.1,
    )
    websocket = BlockedWebSocket()
    anyio.run(
        live_router._pump_telemetry,  # pyright: ignore[reportPrivateUsage]
        cast(Any, websocket),
        channel,
        subscription,
    )
    assert websocket.close_codes == [live_router.POLICY_CLOSE_CODE]
    assert subscription.dropped == 0
    subscription.close()


def _frozen_clock() -> UtcTimestamp:
    from datetime import UTC, datetime

    return UtcTimestamp(datetime(2026, 8, 28, 9, 0, tzinfo=UTC))


def test_saturated_run_closes_with_capacity_code(container: RuntimeContainer) -> None:
    from paritygrid.adapters.persistence.operational import SQLOperationalUnitOfWork
    from paritygrid.application.services.telemetry import LiveTelemetryChannel
    from paritygrid.runtime.composition import RuntimeServices

    _seed(container)
    saturated_hub = LiveTelemetryHub(queue_capacity=8, max_subscribers_per_run=1)
    existing = saturated_hub.subscribe(RUN_ID)
    try:
        services = container.services
        replacement = RuntimeServices(
            pipelines=services.pipelines,
            connectors=services.connectors,
            connector_tests=services.connector_tests,
            runs=services.runs,
            run_lifecycle=services.run_lifecycle,
            artifacts=services.artifacts,
            idempotency=services.idempotency,
            capabilities=services.capabilities,
            reconciliation=services.reconciliation,
            repair=services.repair,
            repair_application=services.repair_application,
            event_stream=services.event_stream,
            telemetry=LiveTelemetryChannel(
                hub=saturated_hub,
                writer_snapshot=container.writer.snapshot,
                clock=services.clock,
            ),
            clock=services.clock,
        )
        del SQLOperationalUnitOfWork, LiveTelemetryChannel
        application = create_app(
            readiness=RuntimeReadinessProbe(container_provider=lambda: container),
            limits=container.limits,
            services=replacement,
        )
        with (
            TestClient(application) as test_client,
            pytest.raises(WebSocketDisconnect),
            test_client.websocket_connect(f"/api/v1/live/runs/{RUN_ID}"),
        ):
            pass
    finally:
        existing.close()


def test_real_server_serves_the_live_channel(container: RuntimeContainer) -> None:
    import threading

    import uvicorn
    import websockets

    _seed(container)
    application = _app(container)
    config = uvicorn.Config(application, host="127.0.0.1", port=0, log_level="critical")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    async def converse() -> None:
        for _ in range(100):
            if server.started:
                break
            await anyio.sleep(0.05)
        assert server.started
        port = server.servers[0].sockets[0].getsockname()[1]
        async with websockets.connect(f"ws://127.0.0.1:{port}/api/v1/live/runs/{RUN_ID}") as socket:
            snapshot = json.loads(await socket.recv())
            assert snapshot["channel"] == "telemetry"
            await socket.send(json.dumps({"type": "ping"}))
            pong = json.loads(await socket.recv())
            assert pong["type"] == "pong"

    try:
        anyio.run(converse)
    finally:
        server.should_exit = True
        thread.join(timeout=10.0)
