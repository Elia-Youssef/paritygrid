"""Bounded HTTP/1.1 client engines for the Phase 9 connectors.

Two transports implement one identical request surface over the closed
simulator wire contract: an asyncio engine whose every step is a
couroutine-safe operation, and a genuinely blocking engine over
``http.client`` for the legacy source. Both share the response shape, the
transport error taxonomy, byte bounds, and absolute-deadline timeout
semantics, so connectors built on them differ only in concurrency model.

The engines are deliberately minimal: plain ``http`` only, no redirects,
no chunked transfer decoding, one request per connection. Anything outside
that envelope is a typed protocol failure — never a guess — which is the
posture the synthetic systems expose and the connector contract requires.
"""

import asyncio
import contextlib
import http.client
import socket
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import urlsplit

from paritygrid.application.ports.connectors import ConnectorCancellationToken

MAX_RESPONSE_HEAD_BYTES = 16_384
DEFAULT_MAX_RESPONSE_BYTES = 1_048_576
_IO_STREAM_LIMIT_BYTES = MAX_RESPONSE_HEAD_BYTES + 1

_HEAD_TERMINATOR = b"\r\n\r\n"


class HttpTransportError(Exception):
    """One classified transport failure below the connector boundary."""

    def __init__(self, kind: HttpTransportErrorKind, summary: str) -> None:
        super().__init__(summary)
        self.kind = kind


class HttpTransportErrorKind(StrEnum):
    """Closed transport failure kinds both engines report."""

    CONNECT = "connect"
    CONNECT_TIMEOUT = "connect_timeout"
    READ_TIMEOUT = "read_timeout"
    CONNECTION_LOST = "connection_lost"
    PROTOCOL = "protocol"
    RESPONSE_TOO_LARGE = "response_too_large"
    CLIENT_CLOSED = "client_closed"


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """One complete HTTP response with lower-cased header names."""

    status: int
    headers: Mapping[str, str]
    body: bytes


class AsyncHttpClient:
    """Asyncio HTTP/1.1 engine that never blocks the running loop.

    Each request uses exactly one connection, closed in every outcome
    path: success, transport failure, timeout, and task cancellation.
    The timeout is an absolute deadline over the whole exchange.
    """

    def __init__(
        self, base_url: str, *, max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES
    ) -> None:
        self._host, self._port = _split_base_url(base_url)
        self._max_response_bytes = max_response_bytes
        self._closed = False
        self._requests = 0

    def requests_issued(self) -> int:
        """Return how many requests this client has issued."""
        return self._requests

    async def aclose(self) -> None:
        """Mark the client closed; per-request connections self-release."""
        self._closed = True

    async def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: bytes = b"",
        timeout_seconds: float,
    ) -> HttpResponse:
        """Perform one bounded request-response exchange.

        The timeout is an absolute deadline over connect, write, and read;
        cancellation propagates unchanged after the connection closes.
        """
        if self._closed:
            raise HttpTransportError(HttpTransportErrorKind.CLIENT_CLOSED, "client is closed")
        if timeout_seconds <= 0:
            raise HttpTransportError(HttpTransportErrorKind.PROTOCOL, "timeout must be positive")
        self._requests += 1
        head = _encode_request_head(method, path, headers or {}, body)
        writer: asyncio.StreamWriter | None = None
        try:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + timeout_seconds
            # The connect phase runs under wait_for: on Windows proactor
            # loops a refusal surfacing inside a cancellation-based timeout
            # is masked into a bare timeout, losing the classification.
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self._host, self._port, limit=_IO_STREAM_LIMIT_BYTES),
                    timeout=timeout_seconds,
                )
            except TimeoutError:
                raise HttpTransportError(
                    HttpTransportErrorKind.CONNECT_TIMEOUT,
                    "the connect phase exceeded its deadline",
                ) from None
            except ConnectionError, OSError:
                raise HttpTransportError(
                    HttpTransportErrorKind.CONNECT, "the connection could not be completed"
                ) from None
            # asyncio.timeout expires immediately on a nonpositive
            # remaining budget, so no separate pre-check is needed here.
            async with asyncio.timeout(deadline - loop.time()):
                writer.write(head + body)
                await writer.drain()
                raw_head = await reader.readuntil(_HEAD_TERMINATOR)
                status, response_headers, content_length = _parse_response_head(raw_head)
                if content_length > self._max_response_bytes:
                    raise HttpTransportError(
                        HttpTransportErrorKind.RESPONSE_TOO_LARGE,
                        "response body exceeds the configured bound",
                    )
                response_body = await reader.readexactly(content_length) if content_length else b""
            return HttpResponse(status=status, headers=response_headers, body=response_body)
        except HttpTransportError:
            raise
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            raise HttpTransportError(
                HttpTransportErrorKind.READ_TIMEOUT, "the request exceeded its deadline"
            ) from None
        except asyncio.LimitOverrunError:
            raise HttpTransportError(
                HttpTransportErrorKind.RESPONSE_TOO_LARGE, "response head exceeds its bound"
            ) from None
        except asyncio.IncompleteReadError as error:
            raise HttpTransportError(
                HttpTransportErrorKind.CONNECTION_LOST, "the connection ended mid-body"
            ) from error
        except ConnectionError, OSError:
            raise HttpTransportError(
                HttpTransportErrorKind.CONNECT, "the connection could not be completed"
            ) from None
        finally:
            if writer is not None:
                writer.close()
                with contextlib.suppress(ConnectionError, OSError):
                    await writer.wait_closed()


class BlockingHttpClient:
    """Genuinely blocking HTTP/1.1 engine over ``http.client``.

    The request budget is an absolute deadline: it is applied to the
    connect phase and re-applied with the remaining time before the
    response read. Callers must keep this engine off event-loop threads.
    """

    def __init__(
        self, base_url: str, *, max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES
    ) -> None:
        self._host, self._port = _split_base_url(base_url)
        self._max_response_bytes = max_response_bytes
        self._closed = False
        self._requests = 0
        self._connection_lock = threading.Lock()
        self._connections: set[http.client.HTTPConnection] = set()

    def requests_issued(self) -> int:
        """Return how many requests this client has issued."""
        return self._requests

    def close(self) -> None:
        """Close the engine; repeated calls are safe."""
        with self._connection_lock:
            self._closed = True
            connections = tuple(self._connections)
        for connection in connections:
            _interrupt_connection(connection)

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: bytes = b"",
        timeout_seconds: float,
        cancellation_token: ConnectorCancellationToken | None = None,
    ) -> HttpResponse:
        """Perform one bounded blocking request-response exchange."""
        if self._closed:
            raise HttpTransportError(HttpTransportErrorKind.CLIENT_CLOSED, "client is closed")
        if timeout_seconds <= 0:
            raise HttpTransportError(HttpTransportErrorKind.PROTOCOL, "timeout must be positive")
        if cancellation_token is not None:
            cancellation_token.raise_if_cancelled()
        self._requests += 1
        deadline = time.monotonic() + timeout_seconds
        connection = http.client.HTTPConnection(self._host, self._port, timeout=timeout_seconds)
        if not self._register_connection(connection):
            raise HttpTransportError(HttpTransportErrorKind.CLIENT_CLOSED, "client is closed")
        watcher_stop = threading.Event()
        watcher = (
            threading.Thread(
                target=_watch_for_cancellation,
                args=(connection, cancellation_token, watcher_stop),
                name="paritygrid-http-cancellation",
                daemon=True,
            )
            if cancellation_token is not None
            else None
        )
        if watcher is not None:
            watcher.start()
        try:
            self._connect_with_deadline(connection, deadline)
            if cancellation_token is not None:
                cancellation_token.raise_if_cancelled()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise HttpTransportError(
                    HttpTransportErrorKind.READ_TIMEOUT, "the request exceeded its deadline"
                )
            _apply_socket_timeout(connection, remaining)
            try:
                connection.request(
                    method,
                    path,
                    body=body if body else None,
                    headers=dict(headers or {}),
                )
                response = connection.getresponse()
            except TimeoutError:
                raise HttpTransportError(
                    HttpTransportErrorKind.READ_TIMEOUT, "the request exceeded its deadline"
                ) from None
            except (ConnectionError, OSError, http.client.HTTPException) as error:
                if cancellation_token is not None:
                    cancellation_token.raise_if_cancelled()
                raise _translate_blocking_error(error) from error
            if cancellation_token is not None:
                cancellation_token.raise_if_cancelled()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise HttpTransportError(
                    HttpTransportErrorKind.READ_TIMEOUT, "the request exceeded its deadline"
                )
            _apply_socket_timeout(connection, remaining)
            status = response.status
            response_headers = {name.lower(): value for name, value in response.getheaders()}
            content_length_text = response_headers.get("content-length")
            if response_headers.get("transfer-encoding", "").lower() == "chunked":
                raise HttpTransportError(
                    HttpTransportErrorKind.PROTOCOL, "chunked transfer encoding is unsupported"
                )
            if content_length_text is None:
                # The simulator wire contract always frames responses with
                # Content-Length; an unframed response means the exchange was
                # cut mid-response (http.client otherwise parses a truncated
                # head as a valid headless response), so this is a lost
                # connection, not a protocol guess.
                raise HttpTransportError(
                    HttpTransportErrorKind.CONNECTION_LOST,
                    "the connection ended without a framed response",
                )
            else:
                try:
                    content_length = int(content_length_text)
                except ValueError as error:
                    raise HttpTransportError(
                        HttpTransportErrorKind.PROTOCOL, "content-length is not an integer"
                    ) from error
                if content_length < 0:
                    raise HttpTransportError(
                        HttpTransportErrorKind.PROTOCOL, "content-length is negative"
                    )
                if content_length > self._max_response_bytes:
                    raise HttpTransportError(
                        HttpTransportErrorKind.RESPONSE_TOO_LARGE,
                        "response body exceeds the configured bound",
                    )
                response_body = self._read_exactly(
                    response,
                    content_length,
                    deadline,
                    connection,
                    cancellation_token,
                )
            if cancellation_token is not None:
                cancellation_token.raise_if_cancelled()
            return HttpResponse(status=status, headers=response_headers, body=response_body)
        except HttpTransportError:
            if cancellation_token is not None:
                cancellation_token.raise_if_cancelled()
            raise
        finally:
            watcher_stop.set()
            _interrupt_connection(connection)
            self._unregister_connection(connection)
            if watcher is not None:
                watcher.join(timeout=0.25)

    def _register_connection(self, connection: http.client.HTTPConnection) -> bool:
        with self._connection_lock:
            if self._closed:
                return False
            self._connections.add(connection)
            return True

    def _unregister_connection(self, connection: http.client.HTTPConnection) -> None:
        with self._connection_lock:
            self._connections.discard(connection)

    def _connect_with_deadline(
        self, connection: http.client.HTTPConnection, deadline: float
    ) -> None:
        try:
            connection.connect()
        except TimeoutError:
            raise HttpTransportError(
                HttpTransportErrorKind.CONNECT_TIMEOUT, "the connect phase exceeded its deadline"
            ) from None
        except (ConnectionError, OSError) as error:
            raise HttpTransportError(
                HttpTransportErrorKind.CONNECT, "the connection could not be completed"
            ) from error

    def _read_exactly(
        self,
        response: http.client.HTTPResponse,
        content_length: int,
        deadline: float,
        connection: http.client.HTTPConnection,
        cancellation_token: ConnectorCancellationToken | None,
    ) -> bytes:
        if content_length == 0:
            return b""
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise HttpTransportError(
                HttpTransportErrorKind.READ_TIMEOUT, "the request exceeded its deadline"
            )
        _apply_socket_timeout(connection, remaining)
        try:
            payload = response.read(content_length)
        except TimeoutError:
            raise HttpTransportError(
                HttpTransportErrorKind.READ_TIMEOUT, "the request exceeded its deadline"
            ) from None
        except (ConnectionError, OSError, http.client.HTTPException) as error:
            if cancellation_token is not None:
                cancellation_token.raise_if_cancelled()
            raise _translate_blocking_error(error) from error
        if cancellation_token is not None:
            cancellation_token.raise_if_cancelled()
        if len(payload) != content_length:
            raise HttpTransportError(
                HttpTransportErrorKind.CONNECTION_LOST, "the connection ended mid-body"
            )
        return payload


def _watch_for_cancellation(
    connection: http.client.HTTPConnection,
    cancellation_token: ConnectorCancellationToken,
    stop: threading.Event,
) -> None:
    """Interrupt one blocking socket promptly after cooperative cancellation."""
    while not stop.wait(0.025):
        if cancellation_token.is_cancelled():
            _interrupt_connection(connection)
            return


def _interrupt_connection(connection: http.client.HTTPConnection) -> None:
    """Best-effort socket shutdown that unblocks another request thread."""
    sock = connection.sock
    if sock is not None:
        with contextlib.suppress(OSError):
            sock.shutdown(socket.SHUT_RDWR)
        with contextlib.suppress(OSError):
            sock.close()
    with contextlib.suppress(OSError):
        connection.close()


def _apply_socket_timeout(connection: http.client.HTTPConnection, seconds: float) -> None:
    sock = connection.sock
    if sock is not None:
        sock.settimeout(seconds)


def _translate_blocking_error(error: BaseException) -> HttpTransportError:
    if isinstance(error, (http.client.RemoteDisconnected, ConnectionResetError)):
        return HttpTransportError(
            HttpTransportErrorKind.CONNECTION_LOST, "the connection ended mid-response"
        )
    if isinstance(error, (ConnectionError, OSError)):
        return HttpTransportError(
            HttpTransportErrorKind.CONNECT, "the connection could not be completed"
        )
    if isinstance(error, http.client.HTTPException):
        return HttpTransportError(
            HttpTransportErrorKind.PROTOCOL, "the response violated the expected protocol"
        )
    return HttpTransportError(
        HttpTransportErrorKind.PROTOCOL, "the exchange failed for an unknown reason"
    )


def _split_base_url(base_url: str) -> tuple[str, int]:
    split = urlsplit(base_url)
    if split.scheme != "http" or not split.hostname or split.username or split.password:
        raise ValueError("base url must be credential-free plain http")
    return split.hostname, split.port if split.port is not None else 80


def _encode_request_head(
    method: str,
    path: str,
    headers: Mapping[str, str],
    body: bytes,
) -> bytes:
    lines = [f"{method} {path} HTTP/1.1"]
    rendered = dict(headers)
    rendered.setdefault("Host", "paritygrid-connector")
    rendered.setdefault("Content-Length", str(len(body)))
    rendered.setdefault("Connection", "close")
    rendered.setdefault("Accept", "application/json")
    for name, value in rendered.items():
        if "\r" in name or "\n" in name or "\r" in value or "\n" in value:
            raise HttpTransportError(
                HttpTransportErrorKind.PROTOCOL, "header values must be single-line"
            )
        lines.append(f"{name}: {value}")
    return ("\r\n".join(lines) + "\r\n\r\n").encode("ascii")


def _parse_response_head(raw_head: bytes) -> tuple[int, dict[str, str], int]:
    try:
        head_text = raw_head[: -len(_HEAD_TERMINATOR)].decode("ascii")
    except UnicodeDecodeError as error:
        raise HttpTransportError(
            HttpTransportErrorKind.PROTOCOL, "response head is not ascii"
        ) from error
    lines = head_text.split("\r\n")
    status_line = lines[0].split(" ", 2)
    if len(status_line) < 2 or not status_line[0].startswith("HTTP/1."):
        raise HttpTransportError(
            HttpTransportErrorKind.PROTOCOL, "response status line is malformed"
        )
    try:
        status = int(status_line[1])
    except ValueError as error:
        raise HttpTransportError(
            HttpTransportErrorKind.PROTOCOL, "response status is not an integer"
        ) from error
    if not 100 <= status <= 599:
        raise HttpTransportError(
            HttpTransportErrorKind.PROTOCOL, "response status is outside the http range"
        )
    headers: dict[str, str] = {}
    content_length = 0
    for line in lines[1:]:
        name, separator, value = line.partition(":")
        if not separator or not name or not value.startswith(" "):
            raise HttpTransportError(
                HttpTransportErrorKind.PROTOCOL, "response header line is malformed"
            )
        lowered = name.strip().lower()
        if lowered in headers:
            raise HttpTransportError(HttpTransportErrorKind.PROTOCOL, "response header is repeated")
        headers[lowered] = value.strip()
        if lowered == "content-length":
            try:
                content_length = int(value.strip())
            except ValueError as error:
                raise HttpTransportError(
                    HttpTransportErrorKind.PROTOCOL, "content-length is not an integer"
                ) from error
            if content_length < 0:
                raise HttpTransportError(
                    HttpTransportErrorKind.PROTOCOL, "content-length is negative"
                )
        if lowered == "transfer-encoding":
            raise HttpTransportError(
                HttpTransportErrorKind.PROTOCOL, "chunked transfer encoding is unsupported"
            )
    return status, headers, content_length


__all__ = [
    "DEFAULT_MAX_RESPONSE_BYTES",
    "MAX_RESPONSE_HEAD_BYTES",
    "AsyncHttpClient",
    "BlockingHttpClient",
    "HttpResponse",
    "HttpTransportError",
    "HttpTransportErrorKind",
]
