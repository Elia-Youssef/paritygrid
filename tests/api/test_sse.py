"""Durable SSE contracts: replay, resume, gaps, heartbeats, slow clients."""

import json
import threading
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import cast

import anyio
import httpx
import pytest
from fastapi import FastAPI
from starlette.types import Message, Receive, Scope, Send

from paritygrid.adapters.persistence.operational import SQLOperationalUnitOfWork
from paritygrid.api.app import create_app
from paritygrid.api.middleware.request_limits import RequestLimitSettings, RequestLimitsMiddleware
from paritygrid.api.routers.stream import BoundedSSEStreamingResponse
from paritygrid.application.ports.consistency import (
    EventSequence,
    ExecutionEventPage,
    ExecutionEventRecord,
)
from paritygrid.application.ports.execution import RunEventCounterRecord, RunRecord
from paritygrid.application.ports.operations import OperationalUnitOfWork
from paritygrid.application.services.events import (
    DurableEventHistoryGapError,
    DurableEventStreamService,
)
from paritygrid.domain.models import RunId
from paritygrid.runtime.composition import (
    RuntimeContainer,
    RuntimeReadinessProbe,
    RuntimeServices,
)
from tests.api.conftest import seed_scenario, transition_run

pytestmark = pytest.mark.anyio


def _stream_app(
    container: RuntimeContainer,
    *,
    heartbeat: float = 0.2,
    event_stream: DurableEventStreamService | None = None,
) -> FastAPI:
    """Build one app whose stream polls quickly for deterministic tests."""
    services = container.services
    fast_stream = event_stream or DurableEventStreamService(
        unit_of_work=SQLOperationalUnitOfWork(
            container.database,
            artifact_root=container.settings.artifact_root_path,
            artifact_chunk_bytes=container.settings.artifact_chunk_bytes,
        ),
        heartbeat_seconds=heartbeat,
        poll_seconds=0.05,
    )
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
        event_stream=fast_stream,
        telemetry=services.telemetry,
        clock=services.clock,
    )
    return create_app(
        readiness=RuntimeReadinessProbe(container_provider=lambda: container),
        limits=container.limits,
        services=replacement,
    )


@asynccontextmanager
async def _live_server(
    container: RuntimeContainer, *, heartbeat: float = 0.2
) -> AsyncGenerator[tuple[httpx.AsyncClient, DurableEventStreamService]]:
    import uvicorn

    application = _stream_app(container, heartbeat=heartbeat)
    stream = cast(DurableEventStreamService, application.state.services.event_stream)
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
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client:
            yield client, stream
    finally:
        server.should_exit = True
        thread.join(timeout=10.0)


async def _seed_events(container: RuntimeContainer, *, run_id: str) -> None:
    from paritygrid.domain.execution import RunState

    transition_run(container, run_id, RunState.RUNNING)
    transition_run(container, run_id, RunState.PAUSING)
    transition_run(container, run_id, RunState.PAUSED)


def _parse_frame(block: str) -> dict[str, object]:
    fields: dict[str, str] = {}
    for line in block.splitlines():
        if line.startswith(("id:", "event:", "data:")):
            key, _, value = line.partition(":")
            fields[key] = value.strip()
    return {
        "id": int(fields["id"]),
        "event": fields["event"],
        "payload": json.loads(fields["data"]),
    }


async def _collect(
    client: httpx.AsyncClient,
    url: str,
    *,
    limit: int,
    headers: dict[str, str] | None = None,
) -> list[dict[str, object]]:
    frames: list[dict[str, object]] = []
    buffer = ""
    async with client.stream("GET", url, headers=headers) as response:
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        assert response.headers["cache-control"] == "no-store"
        async for chunk in response.aiter_text():
            buffer += chunk
            while "\n\n" in buffer:
                block, buffer = buffer.split("\n\n", 1)
                if block.startswith("id:"):
                    frames.append(_parse_frame(block))
                    if len(frames) >= limit:
                        return frames
    return frames


async def test_stream_replays_every_committed_event_in_order(
    client: httpx.AsyncClient, container: RuntimeContainer
) -> None:
    await seed_scenario(client, run_id="run_sse-01")
    await _seed_events(container, run_id="run_sse-01")
    async with _live_server(container) as (fast, _stream):
        frames = await _collect(fast, "/api/v1/stream/runs/run_sse-01", limit=4)
    assert [frame["id"] for frame in frames] == [1, 2, 3, 4]
    kinds = [frame["event"] for frame in frames]
    assert kinds == ["run_created", "run_started", "run_pausing", "run_paused"]
    payload = cast("dict[str, object]", frames[0]["payload"])
    assert payload["channel"] == "durable-events"
    assert payload["run_id"] == "run_sse-01"
    assert payload["sequence"] == 1
    assert payload["payload_schema_version"] == 1
    assert "advisory" not in payload


async def test_stream_resumes_after_last_acknowledged_event(
    client: httpx.AsyncClient, container: RuntimeContainer
) -> None:
    await seed_scenario(client, run_id="run_sse-02")
    await _seed_events(container, run_id="run_sse-02")
    async with _live_server(container) as (fast, _stream):
        frames = await _collect(fast, "/api/v1/stream/runs/run_sse-02?after=2", limit=2)
        header_frames = await _collect(
            fast,
            "/api/v1/stream/runs/run_sse-02",
            limit=2,
            headers={"Last-Event-ID": "2"},
        )
    assert [frame["id"] for frame in frames] == [3, 4]
    assert [frame["id"] for frame in header_frames] == [3, 4]


async def test_stream_rejects_invalid_resume_positions(
    client: httpx.AsyncClient, container: RuntimeContainer
) -> None:
    await seed_scenario(client, run_id="run_sse-03")
    await _seed_events(container, run_id="run_sse-03")
    both = await client.get(
        "/api/v1/stream/runs/run_sse-03?after=1", headers={"Last-Event-ID": "1"}
    )
    assert both.status_code == 400
    assert both.json()["code"] == "invalid_resume_position"
    repeated = await client.get("/api/v1/stream/runs/run_sse-03?after=1&after=2")
    assert repeated.status_code == 400
    assert repeated.json()["code"] == "invalid_resume_position"
    malformed = await client.get(
        "/api/v1/stream/runs/run_sse-03", headers={"Last-Event-ID": "not-a-number"}
    )
    assert malformed.status_code == 400
    assert malformed.json()["code"] == "invalid_last_event_id"
    padded = await client.get("/api/v1/stream/runs/run_sse-03", headers={"Last-Event-ID": "02"})
    assert padded.status_code == 400
    whitespace = await client.get(
        "/api/v1/stream/runs/run_sse-03", headers={"Last-Event-ID": " 2 "}
    )
    assert whitespace.status_code == 400
    for invalid_after in ("-1", "02", "not-a-number", "2147483648"):
        invalid = await client.get(f"/api/v1/stream/runs/run_sse-03?after={invalid_after}")
        assert invalid.status_code == 400
        assert invalid.json()["code"] == "invalid_last_event_id"
    ahead = await client.get("/api/v1/stream/runs/run_sse-03?after=99")
    assert ahead.status_code == 409
    assert ahead.json()["code"] == "stream_sequence_ahead"


async def test_stream_rejects_unknown_run(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/stream/runs/run_ghost-997")
    assert response.status_code == 404
    assert response.json()["code"] == "run_not_found"


async def test_stream_fails_closed_when_durable_history_contains_a_gap(
    container: RuntimeContainer,
) -> None:
    run_id = "run_sse-gap-01"
    stream = DurableEventStreamService(
        unit_of_work=cast("OperationalUnitOfWork", _GappedEventUnitOfWork(run_id)),
        heartbeat_seconds=0.2,
        poll_seconds=0.05,
    )

    with pytest.raises(DurableEventHistoryGapError):
        stream.read_page(run_id, after=0)
    with pytest.raises(DurableEventHistoryGapError):
        stream.read_page(run_id, after=2)

    transport = httpx.ASGITransport(app=_stream_app(container, event_stream=stream))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as local:
        response = await local.get(f"/api/v1/stream/runs/{run_id}?after=2")
        assert response.status_code == 503
        assert response.json()["code"] == "unavailable"


async def test_stream_emits_heartbeats_while_idle(
    client: httpx.AsyncClient, container: RuntimeContainer
) -> None:
    await seed_scenario(client, run_id="run_sse-04")
    await _seed_events(container, run_id="run_sse-04")
    saw_heartbeat = False
    async with (
        _live_server(container, heartbeat=0.1) as (fast, _stream),
        fast.stream("GET", "/api/v1/stream/runs/run_sse-04?after=4") as response,
    ):
        async for chunk in response.aiter_text():
            if "paritygrid-heartbeat" in chunk:
                saw_heartbeat = True
                break
    assert saw_heartbeat


async def test_stream_disconnect_leaves_execution_and_writer_untouched(
    client: httpx.AsyncClient, container: RuntimeContainer
) -> None:
    await seed_scenario(client, run_id="run_sse-05")
    await _seed_events(container, run_id="run_sse-05")
    from paritygrid.adapters.persistence.repositories.runs import SqlAlchemyRunRepository
    from paritygrid.domain.models import RunId

    with container.database.transaction() as session:
        before = SqlAlchemyRunRepository(session).get(RunId("run_sse-05"))
    assert before is not None
    version_before = before.row_version
    async with _live_server(container) as (fast, stream):
        queue_before = container.writer.snapshot().queue_depth
        frames = await _collect(fast, "/api/v1/stream/runs/run_sse-05", limit=4)
        assert len(frames) == 4
        await anyio.sleep(0.2)
        assert not stream.is_stopping()
        assert container.writer.snapshot().queue_depth == queue_before
    with container.database.transaction() as session:
        after = SqlAlchemyRunRepository(session).get(RunId("run_sse-05"))
    assert after is not None
    version_after = after.row_version
    assert version_after == version_before


async def test_stream_terminates_on_shutdown_signal(
    client: httpx.AsyncClient, container: RuntimeContainer
) -> None:
    await seed_scenario(client, run_id="run_sse-06")
    await _seed_events(container, run_id="run_sse-06")
    async with _live_server(container) as (fast, stream):
        buffer = ""
        async with fast.stream("GET", "/api/v1/stream/runs/run_sse-06") as response:
            iterator = response.aiter_text().__aiter__()
            while "run_paused" not in buffer:
                buffer += await iterator.__anext__()
            stream.stop()
            tail = ""
            async for chunk in iterator:
                tail += chunk
    assert "run_paused" in buffer
    assert tail == ""


async def test_slow_sse_send_times_out_and_releases_the_request_slot() -> None:
    """A non-reading client must not retain a stream iterator or HTTP permit."""
    body_closed = anyio.Event()
    body_started = anyio.Event()
    application_calls = 0

    async def body() -> AsyncGenerator[bytes]:
        try:
            body_started.set()
            yield b"data: delayed\n\n"
            await anyio.sleep_forever()
        finally:
            body_closed.set()

    async def application(scope: Scope, receive: Receive, send: Send) -> None:
        nonlocal application_calls
        application_calls += 1
        if application_calls > 1:
            await send({"type": "http.response.start", "status": 204, "headers": []})
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            return
        response = BoundedSSEStreamingResponse(
            body(),
            send_timeout_seconds=0.1,
            media_type="text/event-stream",
        )
        await response(scope, receive, send)

    async def receive() -> Message:
        await anyio.sleep_forever()
        raise AssertionError("unreachable")

    async def non_reading_send(message: Message) -> None:
        if message["type"] == "http.response.body":
            await anyio.sleep_forever()

    middleware = RequestLimitsMiddleware(
        application,
        settings=RequestLimitSettings(
            max_body_bytes=1_024,
            max_json_depth=8,
            request_timeout_seconds=1.0,
            max_concurrent_requests=1,
        ),
    )
    scope = cast(
        "Scope",
        {
            "type": "http",
            "method": "GET",
            "path": "/api/v1/stream/runs/run_sse-slow-01",
            "headers": [],
        },
    )
    with pytest.raises(TimeoutError):
        await middleware(scope, receive, non_reading_send)
    assert body_started.is_set()
    assert body_closed.is_set()
    sent: list[Message] = []

    async def collecting_send(message: Message) -> None:
        sent.append(message)

    await middleware(scope, receive, collecting_send)
    assert sent[0]["status"] == 204


class _GappedEventUnitOfWork:
    """Test-only durable read port whose counter exposes a missing sequence."""

    def __init__(self, run_id: str) -> None:
        self._run_id = RunId(run_id)

    @contextmanager
    def transaction(self) -> Generator[_GappedEventRepositories]:
        yield _GappedEventRepositories(self._run_id)


class _GappedEventRepositories:
    def __init__(self, run_id: RunId) -> None:
        self.runs = _GappedRunRepository(run_id)
        self.events = _GappedEventRepository(run_id)


class _GappedRunRepository:
    def __init__(self, run_id: RunId) -> None:
        self._run_id = run_id

    def get(self, run_id: RunId) -> RunRecord | None:
        if run_id != self._run_id:
            return None
        return cast("RunRecord", object())

    def get_event_counter(self, run_id: RunId) -> RunEventCounterRecord | None:
        if run_id != self._run_id:
            return None
        return RunEventCounterRecord(run_id=run_id, next_sequence_number=5, row_version=1)


class _GappedEventRepository:
    def __init__(self, run_id: RunId) -> None:
        self._run_id = run_id

    def get(self, run_id: RunId, sequence: EventSequence) -> object | None:
        if run_id != self._run_id or sequence.number == 3:
            return None
        return _GappedEvent(sequence)

    def list_after(
        self, run_id: RunId, *, after: EventSequence | None, limit: int
    ) -> ExecutionEventPage:
        del limit
        if run_id != self._run_id:
            return ExecutionEventPage(items=(), next_cursor=None)
        if after is not None and after.number == 2:
            return ExecutionEventPage(
                items=(cast("ExecutionEventRecord", _GappedEvent(EventSequence(4))),),
                next_cursor=None,
            )
        return ExecutionEventPage(items=(), next_cursor=None)


class _GappedEvent:
    def __init__(self, sequence: EventSequence) -> None:
        self.sequence = sequence
