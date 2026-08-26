"""Wire-level guard tests shared by the blocking and async HTTP engines."""

import asyncio
import socket
import time
from collections.abc import AsyncIterator

import pytest

from paritygrid.demo.datasets import DatasetProfile, ScenarioSeed, ScenarioVersion, generate_dataset
from paritygrid.demo.failures import FailureScript
from paritygrid.demo.simulators.blocking_server import BlockingHttpService, BlockingHttpServiceError
from paritygrid.demo.simulators.blocking_source import BlockingInventorySource
from paritygrid.demo.simulators.http_wire import (
    HttpWireError,
    PlannedResponse,
    RequestBodyTooLargeError,
    build_request,
    encode_response,
    error_response,
    json_response,
    parse_request_head,
    query_parameters,
    request_path_only,
)

pytestmark = pytest.mark.anyio
_DATASET = generate_dataset(
    ScenarioSeed(919),
    ScenarioVersion(1),
    DatasetProfile(record_count=6, malformed_count=1, boundary_count=1, duplicate_count=1),
)


def _exchange(port: int, payload: bytes, read_timeout: float = 5.0) -> bytes:
    with socket.create_connection(("127.0.0.1", port), timeout=read_timeout) as connection:
        connection.sendall(payload)
        chunks: list[bytes] = []
        connection.settimeout(read_timeout)
        try:
            while True:
                chunk = connection.recv(4_096)
                if not chunk:
                    break
                chunks.append(chunk)
        except TimeoutError:
            pass
        return b"".join(chunks)


def _valid_head_line(path: str = "/healthz") -> bytes:
    return f"GET {path} HTTP/1.1\r\nHost: demo\r\n\r\n".encode("ascii")


@pytest.fixture
async def blocking_source() -> AsyncIterator[BlockingInventorySource]:
    simulator = BlockingInventorySource(_DATASET, FailureScript.empty())
    simulator.start()
    try:
        yield simulator
    finally:
        await simulator.aclose()


async def test_blocking_engine_refuses_oversized_heads(
    blocking_source: BlockingInventorySource,
) -> None:
    raw = _exchange(
        blocking_source.port,
        b"GET /healthz HTTP/1.1\r\nHost: x\r\nX-Big: " + b"b" * 20_000 + b"\r\n\r\n",
    )
    assert raw.startswith(b"HTTP/1.1 431")


async def test_blocking_engine_refuses_transfer_encoding_and_malformed_lines(
    blocking_source: BlockingInventorySource,
) -> None:
    raw = _exchange(
        blocking_source.port,
        b"PUT /x HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n",
    )
    assert raw.startswith(b"HTTP/1.1 400")
    raw = _exchange(blocking_source.port, b"GARBAGE-REQUEST\r\n\r\n")
    assert raw.startswith(b"HTTP/1.1 400")


async def test_blocking_engine_refuses_oversized_declared_bodies(
    blocking_source: BlockingInventorySource,
) -> None:
    raw = _exchange(
        blocking_source.port,
        b"PUT /x HTTP/1.1\r\nHost: x\r\nContent-Length: 99999999\r\n\r\n",
    )
    assert raw.startswith(b"HTTP/1.1 413")


async def test_blocking_engine_drops_truncated_bodies(
    blocking_source: BlockingInventorySource,
) -> None:
    raw = _exchange(
        blocking_source.port,
        b"PUT /x HTTP/1.1\r\nHost: x\r\nContent-Length: 40\r\n\r\nshort",
    )
    assert raw == b""


async def test_blocking_engine_serves_expect_continue_requests(
    blocking_source: BlockingInventorySource,
) -> None:
    with socket.create_connection(("127.0.0.1", blocking_source.port), timeout=5.0) as connection:
        connection.sendall(
            b"PUT /healthz HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\nContent-Length: 0\r\n\r\n"
        )
        connection.settimeout(5.0)
        interim = connection.recv(4_096)
    assert interim.startswith((b"HTTP/1.1 405", b"HTTP/1.1 100"))


async def test_blocking_engine_honors_connection_close(
    blocking_source: BlockingInventorySource,
) -> None:
    raw = _exchange(
        blocking_source.port,
        b"GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
    )
    assert raw.startswith(b"HTTP/1.1 200")
    assert raw.endswith(b"}")


async def test_blocking_engine_honors_http_1_0_close_semantics(
    blocking_source: BlockingInventorySource,
) -> None:
    raw = _exchange(blocking_source.port, b"GET /healthz HTTP/1.0\r\nHost: x\r\n\r\n")
    assert raw.startswith(b"HTTP/1.1 200")


async def test_blocking_service_ownership_guards() -> None:
    service = BlockingHttpService(
        service_name="guard",
        handler=lambda request: json_response(200, {"ok": True}),
    )
    with pytest.raises(BlockingHttpServiceError, match="has not started"):
        _ = service.port
    with pytest.raises(BlockingHttpServiceError, match="has not started"):
        _ = service.thread
    service.close()
    service.start()
    assert service.is_serving() is True
    with pytest.raises(BlockingHttpServiceError, match="already started"):
        service.start()
    service.close()
    assert service.is_serving() is False


async def test_blocking_close_force_closes_idle_keep_alive_connections() -> None:
    simulator = BlockingInventorySource(_DATASET, FailureScript.empty())
    simulator.start()
    connection = socket.create_connection(("127.0.0.1", simulator.port), timeout=5.0)
    connection.sendall(_valid_head_line())
    assert connection.recv(4_096).startswith(b"HTTP/1.1 200")
    await asyncio.to_thread(time.sleep, 0.1)
    await simulator.aclose()
    connection.settimeout(5.0)
    try:
        residual = connection.recv(4_096)
    except OSError:
        residual = b""
    assert residual == b""
    connection.close()


def test_parse_request_head_rejects_malformed_input() -> None:
    with pytest.raises(HttpWireError, match="ASCII"):
        parse_request_head("GET / HTTP/1.1\r\n".encode("utf-16"))
    with pytest.raises(HttpWireError, match="empty"):
        parse_request_head(b"")
    with pytest.raises(HttpWireError, match=r"HTTP/1\.0 or HTTP/1\.1"):
        parse_request_head(b"GET / HTTP/2\r\nHost: x\r\n\r\n"[: -len(b"\r\n\r\n")])
    with pytest.raises(HttpWireError, match="uppercase"):
        parse_request_head(b"get / HTTP/1.1\r\nHost: x")
    with pytest.raises(HttpWireError, match="malformed header"):
        parse_request_head(b"GET / HTTP/1.1\r\nNoColonHere\r\n")
    with pytest.raises(HttpWireError, match="nonnegative"):
        parse_request_head(b"PUT / HTTP/1.1\r\nHost: x\r\nContent-Length: -5")
    with pytest.raises(RequestBodyTooLargeError):
        parse_request_head(b"PUT / HTTP/1.1\r\nHost: x\r\nContent-Length: 99999999")
    too_many = b"GET / HTTP/1.1\r\n" + b"".join(
        f"X-H{index}: v\r\n".encode() for index in range(80)
    )
    with pytest.raises(HttpWireError, match="too many headers"):
        parse_request_head(too_many.rstrip(b"\r\n"))


def test_parse_request_head_accepts_valid_heads() -> None:
    head = parse_request_head(b"PUT /v1/x?a=b HTTP/1.1\r\nHost: h\r\nContent-Length: 3")
    assert head.method == "PUT"
    assert head.path == "/v1/x?a=b"
    assert head.content_length == 3
    assert head.expects_continue is False
    assert head.connection_close is False
    continuing = parse_request_head(
        b"PUT / HTTP/1.1\r\nHost: h\r\nExpect: 100-Continue\r\nConnection: close"
    )
    assert continuing.expects_continue is True
    assert continuing.connection_close is True


def test_query_parameters_reject_repeats_and_fragments() -> None:
    assert query_parameters("/v1/x?a=1&b=2") == {"a": "1", "b": "2"}
    assert query_parameters("/v1/x") == {}
    with pytest.raises(HttpWireError, match="repeated"):
        query_parameters("/v1/x?a=1&a=2")
    with pytest.raises(HttpWireError, match="fragment"):
        query_parameters("/v1/x?a=1#frag")
    assert request_path_only("/v1/x?a=1") == "/v1/x"


def test_build_request_validates_body_length() -> None:
    head = parse_request_head(b"PUT /x HTTP/1.1\r\nHost: h\r\nContent-Length: 4")
    with pytest.raises(HttpWireError, match="does not match"):
        build_request(head, b"abc")
    request = build_request(head, b"abcd")
    assert request.method == "PUT"
    assert request.headers["host"] == "h"
    assert request.body == b"abcd"


def test_encoded_responses_are_deterministic_and_bounded() -> None:
    planned = json_response(200, {"b": 1, "a": 2})
    assert planned.encoded() == encode_response(planned)
    assert planned.encoded() == planned.encoded()
    assert planned.encoded().startswith(b"HTTP/1.1 200 OK\r\n")
    assert b"Content-Length: 13\r\n" in planned.encoded()
    error = error_response(404, "not_found", "Missing.")
    assert error.encoded().startswith(b"HTTP/1.1 404 Not Found\r\n")
    closing = PlannedResponse(status=200, body=b"x", close_after_response=True)
    assert b"Connection: close\r\n" in closing.encoded()
    delayed = PlannedResponse(status=200, body=b"x", delay_microseconds=1_500_000)
    assert delayed.delay_seconds == 1.5
    held = PlannedResponse(status=200, body=b"", hold_cap_microseconds=2_000_000)
    assert held.hold_cap_seconds == 2.0
