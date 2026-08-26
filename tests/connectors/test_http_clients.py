"""HTTP client engine unit tests: wire parsing, bounds, and transport errors."""

import asyncio
import contextlib
import http.client
import socket
import threading

import pytest

from paritygrid.adapters.connectors.http_clients import (
    AsyncHttpClient,
    BlockingHttpClient,
    HttpTransportError,
    HttpTransportErrorKind,
    _encode_request_head,
    _parse_response_head,
    _split_base_url,
)

# pyright: reportPrivateUsage=false

pytestmark = pytest.mark.anyio


class _SyncWireServer:
    """A one-shot raw TCP server for adversarial wire responses.

    The serving thread is a daemon and the listener closes on teardown so
    a client that never connects cannot wedge the session at exit.
    """

    def __init__(self, payload: bytes) -> None:
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(1)
        self.port = self._listener.getsockname()[1]
        self._payload = payload
        self._thread = threading.Thread(
            target=self._serve, name="paritygrid-test-wire", daemon=True
        )
        self._thread.start()

    def _serve(self) -> None:
        try:
            with contextlib.suppress(OSError):
                connection, _ = self._listener.accept()
                connection.recv(65_536)
                connection.sendall(self._payload)
                connection.close()
        finally:
            with contextlib.suppress(OSError):
                self._listener.close()

    def close(self) -> None:
        # Closing a listener from another thread does not reliably wake a
        # blocking accept() on every platform. A loopback connection gives
        # the one-shot server a deterministic exit path before teardown.
        with contextlib.suppress(OSError):
            wake = socket.create_connection(("127.0.0.1", self.port), timeout=0.25)
            wake.close()
        with contextlib.suppress(OSError):
            self._listener.close()
        self._thread.join(timeout=5)
        assert not self._thread.is_alive()


async def test_async_client_round_trip_and_close() -> None:
    server = _SyncWireServer(
        b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 7\r\n\r\n{"a":1}'
    )
    try:
        client = AsyncHttpClient(f"http://127.0.0.1:{server.port}")
        response = await client.request("GET", "/", timeout_seconds=2.0)
        assert response.status == 200
        assert response.headers["content-type"] == "application/json"
        assert response.body == b'{"a":1}'
        await client.aclose()
        with pytest.raises(HttpTransportError) as closed:
            await client.request("GET", "/", timeout_seconds=1.0)
        assert closed.value.kind is HttpTransportErrorKind.CLIENT_CLOSED
    finally:
        server.close()


async def test_async_client_rejects_non_positive_timeout() -> None:
    server = _SyncWireServer(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
    try:
        client = AsyncHttpClient(f"http://127.0.0.1:{server.port}")
        with pytest.raises(HttpTransportError) as error:
            await client.request("GET", "/", timeout_seconds=0.0)
        assert error.value.kind is HttpTransportErrorKind.PROTOCOL
    finally:
        server.close()


@pytest.mark.parametrize(
    "payload",
    [
        b"NOT-HTTP 200 OK\r\nContent-Length: 0\r\n\r\n",  # malformed status line
        b"HTTP/1.1 abc OK\r\nContent-Length: 0\r\n\r\n",  # non-integer status
        b"HTTP/1.1 700 OK\r\nContent-Length: 0\r\n\r\n",  # status outside range
        b"HTTP/1.1 200 OK\r\nContent-Length: -1\r\n\r\n",  # negative length
        b"HTTP/1.1 200 OK\r\nContent-Length: x\r\n\r\n",  # non-integer length
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",  # repeated header
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",  # chunked unsupported
        b"HTTP/1.1 200 OK\r\nBadHeader\r\n\r\n",  # malformed header line
    ],
)
async def test_async_client_rejects_protocol_violations(payload: bytes) -> None:
    server = _SyncWireServer(payload)
    try:
        client = AsyncHttpClient(f"http://127.0.0.1:{server.port}")
        with pytest.raises(HttpTransportError) as error:
            await client.request("GET", "/", timeout_seconds=2.0)
        assert error.value.kind is HttpTransportErrorKind.PROTOCOL
    finally:
        server.close()


async def test_async_client_enforces_response_size_bound() -> None:
    body = b"x" * 100
    server = _SyncWireServer(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n" + body)
    try:
        client = AsyncHttpClient(f"http://127.0.0.1:{server.port}", max_response_bytes=10)
        with pytest.raises(HttpTransportError) as error:
            await client.request("GET", "/", timeout_seconds=2.0)
        assert error.value.kind is HttpTransportErrorKind.RESPONSE_TOO_LARGE
    finally:
        server.close()


async def test_async_client_reports_connection_loss_mid_body() -> None:
    server = _SyncWireServer(b"HTTP/1.1 200 OK\r\nContent-Length: 50\r\n\r\nshort")
    try:
        client = AsyncHttpClient(f"http://127.0.0.1:{server.port}")
        with pytest.raises(HttpTransportError) as error:
            await client.request("GET", "/", timeout_seconds=2.0)
        assert error.value.kind is HttpTransportErrorKind.CONNECTION_LOST
    finally:
        server.close()


def test_blocking_client_round_trip_and_close() -> None:
    server = _SyncWireServer(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
    try:
        client = BlockingHttpClient(f"http://127.0.0.1:{server.port}")
        response = client.request("GET", "/", timeout_seconds=2.0)
        assert response.status == 200
        assert response.body == b"ok"
        assert client.requests_issued() == 1
        client.close()
        with pytest.raises(HttpTransportError) as closed:
            client.request("GET", "/", timeout_seconds=1.0)
        assert closed.value.kind is HttpTransportErrorKind.CLIENT_CLOSED
    finally:
        server.close()


def test_blocking_client_rejects_protocol_violations() -> None:
    server = _SyncWireServer(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
    try:
        client = BlockingHttpClient(f"http://127.0.0.1:{server.port}")
        with pytest.raises(HttpTransportError) as error:
            client.request("GET", "/", timeout_seconds=2.0)
        assert error.value.kind is HttpTransportErrorKind.PROTOCOL
    finally:
        server.close()


def test_blocking_client_enforces_response_size_bound() -> None:
    server = _SyncWireServer(b"HTTP/1.1 200 OK\r\nContent-Length: 64\r\n\r\n" + b"y" * 64)
    try:
        client = BlockingHttpClient(f"http://127.0.0.1:{server.port}", max_response_bytes=8)
        with pytest.raises(HttpTransportError) as error:
            client.request("GET", "/", timeout_seconds=2.0)
        assert error.value.kind is HttpTransportErrorKind.RESPONSE_TOO_LARGE
    finally:
        server.close()


def test_blocking_client_reports_connection_loss_mid_body() -> None:
    server = _SyncWireServer(b"HTTP/1.1 200 OK\r\nContent-Length: 40\r\n\r\npartial")
    try:
        client = BlockingHttpClient(f"http://127.0.0.1:{server.port}")
        with pytest.raises(HttpTransportError) as error:
            client.request("GET", "/", timeout_seconds=2.0)
        assert error.value.kind is HttpTransportErrorKind.CONNECTION_LOST
    finally:
        server.close()


def test_blocking_client_connect_failure_is_connect_kind() -> None:
    held = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    held.bind(("127.0.0.1", 0))
    port = held.getsockname()[1]
    try:
        client = BlockingHttpClient(f"http://127.0.0.1:{port}")
        with pytest.raises(HttpTransportError) as error:
            client.request("GET", "/", timeout_seconds=2.0)
        assert error.value.kind in (
            HttpTransportErrorKind.CONNECT,
            HttpTransportErrorKind.CONNECT_TIMEOUT,
        )
    finally:
        held.close()


def test_request_head_encoding_and_refuses_header_injection() -> None:
    head = _encode_request_head("GET", "/x?a=b", {"X-Test": "1"}, b"")
    assert head.startswith(b"GET /x?a=b HTTP/1.1\r\n")
    assert b"X-Test: 1" in head
    assert b"Connection: close" in head
    with pytest.raises(HttpTransportError):
        _encode_request_head("GET", "/x", {"Bad": "value\r\nInjected: 1"}, b"")


def test_response_head_parser_surfaces_protocol_shapes() -> None:
    status, headers, length = _parse_response_head(
        b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 3\r\nContent-Length: 0\r\n\r\n"
    )
    assert status == 429
    assert headers["retry-after"] == "3"
    assert length == 0
    with pytest.raises(HttpTransportError):
        _parse_response_head(b"HTTP/1.1 200 OK\r\nNon-Ascii: \xff\r\n\r\n"[:-4])


def test_split_base_url_rejects_credentials_and_https() -> None:
    assert _split_base_url("http://127.0.0.1:8000") == ("127.0.0.1", 8000)
    assert _split_base_url("http://localhost") == ("localhost", 80)
    with pytest.raises(ValueError, match="plain http"):
        _split_base_url("https://example.com")
    with pytest.raises(ValueError, match="plain http"):
        _split_base_url("http://user:pass@example.com")


def test_async_client_request_cancellation_closes_connection() -> None:
    stall = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    stall.bind(("127.0.0.1", 0))
    stall.listen(1)
    port = stall.getsockname()[1]

    async def scenario() -> None:
        client = AsyncHttpClient(f"http://127.0.0.1:{port}")
        task = asyncio.create_task(client.request("GET", "/", timeout_seconds=5.0))
        await asyncio.sleep(0.05)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    try:
        asyncio.run(scenario())
    finally:
        stall.close()


def test_translate_blocking_error_matrix() -> None:
    from paritygrid.adapters.connectors.http_clients import _translate_blocking_error

    lost = _translate_blocking_error(http.client.RemoteDisconnected("closed"))
    assert lost.kind is HttpTransportErrorKind.CONNECTION_LOST
    reset = _translate_blocking_error(ConnectionResetError("reset"))
    assert reset.kind is HttpTransportErrorKind.CONNECTION_LOST
    connect = _translate_blocking_error(ConnectionRefusedError("refused"))
    assert connect.kind is HttpTransportErrorKind.CONNECT
    protocol = _translate_blocking_error(http.client.BadStatusLine("garbage"))
    assert protocol.kind is HttpTransportErrorKind.PROTOCOL
    unknown = _translate_blocking_error(RuntimeError("unexpected"))
    assert unknown.kind is HttpTransportErrorKind.PROTOCOL


def test_blocking_client_rejects_non_integer_content_length() -> None:
    server = _SyncWireServer(b"HTTP/1.1 200 OK\r\nContent-Length: abc\r\n\r\n")
    try:
        client = BlockingHttpClient(f"http://127.0.0.1:{server.port}")
        with pytest.raises(HttpTransportError) as error:
            client.request("GET", "/", timeout_seconds=2.0)
        assert error.value.kind is HttpTransportErrorKind.PROTOCOL
    finally:
        server.close()
