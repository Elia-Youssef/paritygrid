"""Genuinely blocking thread-per-connection HTTP/1.1 server engine.

Every accepted connection is served by one handler thread performing blocking
socket reads, blocking writes, and event-interruptible delay waits. The engine
exists so a later blocking connector faces a real blocking legacy boundary
rather than an async server in disguise.
"""

import contextlib
import socket
import socketserver
import threading
import time
from collections.abc import Callable
from typing import Final

from paritygrid.demo.simulators.http_wire import (
    MAX_HEADER_BYTES,
    MAX_REQUEST_LINE_BYTES,
    HttpRequest,
    PlannedResponse,
    RequestBodyTooLargeError,
    RequestHead,
    ResponseAction,
    build_request,
    error_response,
    parse_request_head,
)

_LOOPBACK_HOST: Final = "127.0.0.1"
_HEAD_TERMINATOR = b"\r\n\r\n"
_IO_TIMEOUT_SECONDS: Final = 30.0
_JOIN_TIMEOUT_SECONDS: Final = 10.0
_READ_CHUNK_BYTES: Final = 4_096
_CLOSE_POLL_SECONDS: Final = 0.05


class BlockingHttpServiceError(RuntimeError):
    """Raised when the blocking service is misused."""


class _BlockingHttpServer(socketserver.ThreadingTCPServer):
    """Threaded TCP server that tracks every open connection it owns."""

    allow_reuse_address = True
    daemon_threads = False

    def __init__(
        self, handler: Callable[[HttpRequest], PlannedResponse], service_name: str
    ) -> None:
        self.request_handler_binding = handler
        self.service_name = service_name
        self._connections: set[socket.socket] = set()
        self._connections_lock = threading.Lock()
        self._closing = threading.Event()
        super().__init__((_LOOPBACK_HOST, 0), _ConnectionHandler)

    def register_connection(self, connection: socket.socket) -> None:
        with self._connections_lock:
            self._connections.add(connection)

    def unregister_connection(self, connection: socket.socket) -> None:
        with self._connections_lock:
            self._connections.discard(connection)

    def is_closing(self) -> bool:
        """Report whether service teardown has started."""
        return self._closing.is_set()

    def wait_for_close(self, seconds: float) -> bool:
        """Wait for teardown while allowing delayed handlers to be interrupted."""
        return self._closing.wait(seconds)

    def close_open_connections(self) -> None:
        """Force-close tracked connections so their handler threads unblock."""
        self._closing.set()
        with self._connections_lock:
            connections = list(self._connections)
            self._connections.clear()
        for connection in connections:
            with contextlib.suppress(OSError):
                connection.shutdown(socket.SHUT_RDWR)
            with contextlib.suppress(OSError):
                connection.close()


class _ConnectionHandler(socketserver.StreamRequestHandler):
    """Serves keep-alive requests on one blocking connection."""

    def setup(self) -> None:
        super().setup()
        self._pending = b""
        self.connection.settimeout(_IO_TIMEOUT_SECONDS)
        server = self.server
        assert isinstance(server, _BlockingHttpServer)
        server.register_connection(self.connection)

    def finish(self) -> None:
        server = self.server
        assert isinstance(server, _BlockingHttpServer)
        server.unregister_connection(self.connection)
        super().finish()

    def handle(self) -> None:
        server = self.server
        assert isinstance(server, _BlockingHttpServer)
        handler = server.request_handler_binding
        while True:
            head = self._read_head()
            if head is None:
                return
            request_head, keep_alive = head
            if request_head.expects_continue:
                self.wfile.write(b"HTTP/1.1 100 Continue\r\n\r\n")
                self.wfile.flush()
            body = self._read_body(request_head.content_length)
            if body is None:
                return
            try:
                request = build_request(request_head, body)
            except ValueError:
                self._respond(
                    error_response(400, "bad_request", "The request query was not accepted.")
                )
                return
            try:
                planned = handler(request)
            except Exception:
                planned = PlannedResponse(
                    status=500,
                    body=(
                        b'{"error":{"code":"internal_error",'
                        b'"message":"The simulator failed to handle the request."}}'
                    ),
                    headers=(("Content-Type", "application/json"),),
                    close_after_response=True,
                )
            if planned.delay_microseconds and server.wait_for_close(planned.delay_seconds):
                return
            if planned.action is ResponseAction.HOLD_UNTIL_DISCONNECT:
                self._hold_until_disconnect(planned.hold_cap_seconds)
                return
            if planned.action is ResponseAction.CLOSE_PARTIAL:
                encoded = planned.body
                with contextlib.suppress(OSError):
                    self.connection.sendall(encoded[: max(planned.partial_bytes, 1)])
                return
            self.wfile.write(planned.encoded())
            self.wfile.flush()
            if not keep_alive or planned.close_after_response:
                return

    def _read_head(self) -> tuple[RequestHead, bool] | None:
        server = self.server
        assert isinstance(server, _BlockingHttpServer)
        accumulated = bytearray(self._pending)
        self._pending = b""
        deadline = time.monotonic() + _IO_TIMEOUT_SECONDS
        while _HEAD_TERMINATOR not in accumulated:
            if len(accumulated) > MAX_HEADER_BYTES + MAX_REQUEST_LINE_BYTES:
                self._respond(
                    error_response(431, "headers_too_large", "The request head exceeds its bound.")
                )
                return None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            try:
                self.connection.settimeout(min(_CLOSE_POLL_SECONDS, remaining))
                chunk = self.connection.recv(_READ_CHUNK_BYTES)
            except TimeoutError:
                if server.is_closing() or time.monotonic() >= deadline:
                    return None
                continue
            except OSError:
                return None
            if not chunk:
                return None
            accumulated += chunk
        head_part, _, remainder = bytes(accumulated).partition(_HEAD_TERMINATOR)
        self._pending = remainder
        if len(head_part) > MAX_HEADER_BYTES:
            self._respond(
                error_response(431, "headers_too_large", "The request head exceeds its bound.")
            )
            return None
        try:
            parsed = parse_request_head(head_part)
        except RequestBodyTooLargeError:
            self._respond(
                error_response(413, "payload_too_large", "The request body exceeds its bound.")
            )
            return None
        except ValueError:
            self._respond(error_response(400, "bad_request", "The request could not be parsed."))
            return None
        return parsed, not parsed.connection_close

    def _read_body(self, content_length: int) -> bytes | None:
        body = bytearray(self._pending)
        self._pending = b""
        server = self.server
        assert isinstance(server, _BlockingHttpServer)
        deadline = time.monotonic() + _IO_TIMEOUT_SECONDS
        while len(body) < content_length:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            try:
                self.connection.settimeout(min(_CLOSE_POLL_SECONDS, remaining))
                chunk = self.connection.recv(min(65_536, content_length - len(body)))
            except TimeoutError:
                if server.is_closing() or time.monotonic() >= deadline:
                    return None
                continue
            except OSError:
                return None
            if not chunk:
                return None
            body += chunk
        return bytes(body)

    def _respond(self, planned: PlannedResponse) -> None:
        try:
            self.wfile.write(planned.encoded())
            self.wfile.flush()
        except OSError:
            pass

    def _hold_until_disconnect(self, cap_seconds: float) -> None:
        server = self.server
        assert isinstance(server, _BlockingHttpServer)
        deadline = time.monotonic() + (cap_seconds if cap_seconds > 0 else _IO_TIMEOUT_SECONDS)
        try:
            while not server.is_closing():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                self.connection.settimeout(min(_CLOSE_POLL_SECONDS, remaining))
                try:
                    self.connection.recv(1)
                except TimeoutError:
                    continue
                except OSError:
                    return
                return
        finally:
            with contextlib.suppress(OSError):
                self.connection.settimeout(_IO_TIMEOUT_SECONDS)


class BlockingHttpService:
    """One loopback blocking HTTP service around a request handler."""

    def __init__(
        self,
        *,
        service_name: str,
        handler: Callable[[HttpRequest], PlannedResponse],
    ) -> None:
        self._service_name = service_name
        self._handler = handler
        self._server: _BlockingHttpServer | None = None
        self._thread: threading.Thread | None = None
        self._port: int | None = None

    @property
    def service_name(self) -> str:
        """Return the service name used for diagnostics."""
        return self._service_name

    @property
    def port(self) -> int:
        """Return the dynamically assigned loopback port."""
        if self._port is None:
            raise BlockingHttpServiceError("the blocking service has not started")
        return self._port

    @property
    def base_url(self) -> str:
        """Return the loopback base URL of this service."""
        return f"http://{_LOOPBACK_HOST}:{self.port}"

    @property
    def thread(self) -> threading.Thread:
        """Return the owned serving thread."""
        if self._thread is None:
            raise BlockingHttpServiceError("the blocking service has not started")
        return self._thread

    def is_serving(self) -> bool:
        """Report whether the owned listener thread is still alive."""
        return self._server is not None and self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        """Bind the dynamic loopback port and spawn the serving thread."""
        if self._server is not None:
            raise BlockingHttpServiceError("the blocking service already started")
        server = _BlockingHttpServer(self._handler, self._service_name)
        self._port = server.server_address[1]
        thread = threading.Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": _CLOSE_POLL_SECONDS},
            name=f"paritygrid-{self._service_name}",
            daemon=True,
        )
        thread.start()
        self._server = server
        self._thread = thread

    def close(self) -> None:
        """Stop serving, close owned connections and listener, and join."""
        server = self._server
        thread = self._thread
        self._server = None
        self._thread = None
        self._port = None
        if server is None or thread is None:
            return
        server.close_open_connections()
        server.shutdown()
        server.server_close()
        thread.join(_JOIN_TIMEOUT_SECONDS)
        if thread.is_alive():
            raise BlockingHttpServiceError(
                "the blocking service did not stop within its join bound"
            )
