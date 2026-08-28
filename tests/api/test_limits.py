"""Request limit tests: concurrency, timeout, body bounds, and JSON bounds."""

import threading
import time

import anyio
import httpx
import pytest
from fastapi import FastAPI
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from paritygrid.api.json_bounds import BoundedJsonError, JsonBounds, decode_bounded_json
from paritygrid.api.middleware.request_limits import (
    ConcurrencyGate,
    RequestLimitSettings,
    RequestLimitsMiddleware,
)


def _ok_app() -> ASGIApp:
    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": b'{"status":"ok"}'})

    return app


def _scope(method: str = "GET", path: str = "/x") -> Scope:
    return {
        "type": "http",
        "method": method,
        "path": path,
        "raw_path": path.encode("ascii"),
        "headers": [],
        "query_string": b"",
    }


class _Recorder:
    def __init__(self) -> None:
        self.messages: list[Message] = []

    async def receive(self) -> Message:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(self, message: Message) -> None:
        self.messages.append(message)

    @property
    def status(self) -> int:
        first = self.messages[0]
        return int(first["status"])  # type: ignore[arg-type]


def test_concurrency_gate_admits_up_to_the_limit_only() -> None:
    gate = ConcurrencyGate(2)
    assert gate.try_acquire()
    assert gate.try_acquire()
    assert not gate.try_acquire()
    assert gate.active == 2
    gate.release()
    assert gate.try_acquire()


def test_concurrency_gate_release_never_drops_below_zero() -> None:
    gate = ConcurrencyGate(1)
    gate.release()
    gate.release()
    assert gate.active == 0


@pytest.mark.parametrize("field", ["max_body_bytes", "max_json_depth", "max_concurrent_requests"])
@pytest.mark.parametrize("value", [0, 65_536 * 1_025])
def test_limit_settings_reject_out_of_range_integers(field: str, value: int) -> None:
    constructor = {
        "max_body_bytes": lambda: RequestLimitSettings(max_body_bytes=value),
        "max_json_depth": lambda: RequestLimitSettings(max_json_depth=value),
        "max_concurrent_requests": lambda: RequestLimitSettings(max_concurrent_requests=value),
    }[field]
    with pytest.raises(ValueError, match="supported range"):
        constructor()


@pytest.mark.parametrize("value", [0.0, 301.0])
def test_limit_settings_reject_out_of_range_timeouts(value: float) -> None:
    with pytest.raises(ValueError, match="supported range"):
        RequestLimitSettings(request_timeout_seconds=value)


def test_bounded_json_rejects_adversarial_documents() -> None:
    bounds = JsonBounds(max_body_bytes=1024, max_depth=4)
    cases = [
        b"",
        b"\xef\xbb\xbf{}",
        b"{invalid",
        b'{"a": NaN}',
        b'{"a": 1, "a": 2}',
        b'{"a": "\xff\xfe"}',
        b'[1, {"a": {"b": {"c": {"d": {"e": 1}}}}}]',
    ]
    for raw in cases:
        with pytest.raises(BoundedJsonError):
            decode_bounded_json(raw, bounds=bounds)


def test_bounded_json_accepts_a_valid_document() -> None:
    bounds = JsonBounds(max_body_bytes=1024, max_depth=8)
    parsed = decode_bounded_json(b'{"a": [1, 2, {"b": true}]}', bounds=bounds)
    assert parsed == {"a": [1, 2, {"b": True}]}


@pytest.mark.anyio
async def test_concurrency_saturation_returns_a_problem() -> None:
    settings = RequestLimitSettings(
        max_body_bytes=1024,
        max_json_depth=8,
        request_timeout_seconds=5.0,
        max_concurrent_requests=1,
    )
    release = anyio.Event()

    async def holding_app(scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("path") == "/healthz":
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b""})
            return
        await release.wait()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    middleware = RequestLimitsMiddleware(holding_app, settings=settings)
    held = _Recorder()

    async with anyio.create_task_group() as group:

        async def hold() -> None:
            await middleware(_scope(), held.receive, held.send)

        group.start_soon(hold)
        await anyio.sleep(0.05)
        rejected = _Recorder()
        await middleware(_scope(), rejected.receive, rejected.send)
        assert rejected.status == 429
        body = rejected.messages[-1]
        assert b"too_many_requests" in body["body"]  # type: ignore[index]
        release.set()


@pytest.mark.anyio
async def test_request_timeout_returns_a_problem_when_response_has_not_started() -> None:
    settings = RequestLimitSettings(
        max_body_bytes=1024,
        max_json_depth=8,
        request_timeout_seconds=0.1,
        max_concurrent_requests=10,
    )

    async def slow_app(scope: Scope, receive: Receive, send: Send) -> None:
        await anyio.sleep(2.0)

    middleware = RequestLimitsMiddleware(slow_app, settings=settings)
    recorder = _Recorder()
    await middleware(_scope(), recorder.receive, recorder.send)
    assert recorder.status == 503
    assert b"request_timeout" in recorder.messages[-1]["body"]  # type: ignore[index]


@pytest.mark.anyio
async def test_slow_synchronous_route_returns_at_deadline_and_retains_its_permit() -> None:
    """A sync worker cannot delay the HTTP waiter or escape concurrency limits."""
    completed = threading.Event()
    application = FastAPI()

    def slow() -> dict[str, bool]:
        time.sleep(0.8)
        completed.set()
        return {"finished": True}

    def fast() -> dict[str, bool]:
        return {"finished": True}

    application.add_api_route("/slow", slow, methods=["GET"])
    application.add_api_route("/fast", fast, methods=["GET"])

    application.add_middleware(
        RequestLimitsMiddleware,
        settings=RequestLimitSettings(
            max_body_bytes=1024,
            max_json_depth=8,
            request_timeout_seconds=0.2,
            max_concurrent_requests=1,
        ),
    )
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        started = anyio.current_time()
        timed_out = await client.get("/slow")
        elapsed = anyio.current_time() - started

        assert timed_out.status_code == 503
        assert timed_out.json()["code"] == "request_timeout"
        assert 0.15 <= elapsed < 0.5

        # The background worker still owns the original request slot, so a
        # timeout cannot inflate the amount of concurrently executing work.
        saturated = await client.get("/fast")
        assert saturated.status_code == 429

        await anyio.sleep(0.7)
        assert completed.is_set()
        await anyio.sleep(0.05)
        recovered = await client.get("/fast")

    assert recovered.status_code == 200


@pytest.mark.anyio
async def test_request_timeout_covers_slow_body_acquisition() -> None:
    settings = RequestLimitSettings(
        max_body_bytes=1024,
        max_json_depth=8,
        request_timeout_seconds=0.1,
        max_concurrent_requests=1,
    )
    middleware = RequestLimitsMiddleware(_ok_app(), settings=settings)
    recorder = _Recorder()

    async def slow_receive() -> Message:
        await anyio.sleep(2.0)
        return {"type": "http.request", "body": b"{}", "more_body": False}

    scope = _scope(method="POST", path="/api/v1/pipelines")
    scope["headers"].append(  # type: ignore[union-attr]
        (b"content-type", b"application/json")
    )
    await middleware(scope, slow_receive, recorder.send)

    assert recorder.status == 503
    assert b"request_timeout" in recorder.messages[-1]["body"]  # type: ignore[index]
    assert middleware._gate.active == 0  # pyright: ignore[reportPrivateUsage]


@pytest.mark.anyio
async def test_incomplete_body_returns_a_problem() -> None:
    settings = RequestLimitSettings(
        max_body_bytes=1024,
        max_json_depth=8,
        request_timeout_seconds=5.0,
        max_concurrent_requests=10,
    )
    middleware = RequestLimitsMiddleware(_ok_app(), settings=settings)
    recorder = _Recorder()

    async def disconnecting_receive() -> Message:
        return {"type": "http.disconnect"}

    scope = _scope(method="POST", path="/api/v1/pipelines")
    scope["headers"].append(  # type: ignore[union-attr]
        (b"content-type", b"application/json")
    )
    await middleware(scope, disconnecting_receive, recorder.send)
    assert recorder.status == 400
    assert b"incomplete_request_body" in recorder.messages[-1]["body"]  # type: ignore[index]


@pytest.mark.anyio
async def test_body_is_replayed_to_the_inner_application() -> None:
    settings = RequestLimitSettings(
        max_body_bytes=1024,
        max_json_depth=8,
        request_timeout_seconds=5.0,
        max_concurrent_requests=10,
    )
    seen: list[bytes] = []

    async def reading_app(scope: Scope, receive: Receive, send: Send) -> None:
        body = b""
        while True:
            message = await receive()
            if message["type"] != "http.request":
                break
            body += message.get("body", b"")
            if not message.get("more_body", False):
                break
        seen.append(body)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    middleware = RequestLimitsMiddleware(reading_app, settings=settings)
    recorder = _Recorder()
    payload = b'{"pipeline_id": "pip_replay-001"}'

    async def body_receive() -> Message:
        return {"type": "http.request", "body": payload, "more_body": False}

    scope = _scope(method="POST", path="/api/v1/pipelines")
    headers = scope["headers"]
    headers.append((b"content-type", b"application/json"))  # type: ignore[union-attr]
    await middleware(scope, body_receive, recorder.send)
    assert recorder.status == 200
    assert seen == [payload]


@pytest.mark.anyio
@pytest.mark.parametrize("content_type", [None, "application/jsonx", "text/plain"])
async def test_non_json_payload_media_types_are_rejected(content_type: str | None) -> None:
    middleware = RequestLimitsMiddleware(
        _ok_app(),
        settings=RequestLimitSettings(
            max_body_bytes=1024,
            max_json_depth=8,
            request_timeout_seconds=5.0,
            max_concurrent_requests=1,
        ),
    )
    recorder = _Recorder()
    payload = b'{"pipeline_id":"pip_media-001"}'

    async def body_receive() -> Message:
        return {"type": "http.request", "body": payload, "more_body": False}

    scope = _scope(method="POST", path="/api/v1/pipelines")
    if content_type is not None:
        scope["headers"].append(  # type: ignore[union-attr]
            (b"content-type", content_type.encode("ascii"))
        )
    await middleware(scope, body_receive, recorder.send)

    assert recorder.status == 415
    assert b"unsupported_media_type" in recorder.messages[-1]["body"]  # type: ignore[index]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/pipelines",
        "/api/v1/pipelines/pip_media-001/versions",
    ],
)
async def test_empty_json_command_without_media_type_is_rejected(path: str) -> None:
    middleware = RequestLimitsMiddleware(
        _ok_app(),
        settings=RequestLimitSettings(
            max_body_bytes=1024,
            max_json_depth=8,
            request_timeout_seconds=5.0,
            max_concurrent_requests=1,
        ),
    )
    recorder = _Recorder()

    await middleware(
        _scope(method="POST", path=path),
        recorder.receive,
        recorder.send,
    )

    assert recorder.status == 415
    assert b"unsupported_media_type" in recorder.messages[-1]["body"]  # type: ignore[index]


@pytest.mark.anyio
async def test_empty_non_json_command_does_not_require_a_media_type() -> None:
    middleware = RequestLimitsMiddleware(
        _ok_app(),
        settings=RequestLimitSettings(
            max_body_bytes=1024,
            max_json_depth=8,
            request_timeout_seconds=5.0,
            max_concurrent_requests=1,
        ),
    )
    recorder = _Recorder()

    await middleware(
        _scope(method="POST", path="/api/v1/runs/run_example-001/pause"),
        recorder.receive,
        recorder.send,
    )

    assert recorder.status == 200


@pytest.mark.anyio
async def test_liveness_is_answerable_while_the_gate_is_saturated() -> None:
    settings = RequestLimitSettings(
        max_body_bytes=1024,
        max_json_depth=8,
        request_timeout_seconds=5.0,
        max_concurrent_requests=1,
    )
    release = anyio.Event()

    async def holding_app(scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("path") == "/healthz":
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b""})
            return
        await release.wait()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    middleware = RequestLimitsMiddleware(holding_app, settings=settings)
    held = _Recorder()

    async with anyio.create_task_group() as group:

        async def hold() -> None:
            await middleware(_scope(), held.receive, held.send)

        group.start_soon(hold)
        await anyio.sleep(0.05)
        liveness = _Recorder()
        await middleware(_scope(path="/healthz"), liveness.receive, liveness.send)
        assert liveness.status == 200
        rejected = _Recorder()
        await middleware(_scope(path="/readyz"), rejected.receive, rejected.send)
        assert rejected.status == 429
        release.set()
