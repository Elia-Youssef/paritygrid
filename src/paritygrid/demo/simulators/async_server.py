"""Asyncio HTTP/1.1 server engine for the synthetic simulators.

The engine owns one bound loopback listener with a dynamic port and one task
per accepted connection. It applies the transport actions a
:class:`~paritygrid.demo.simulators.http_wire.PlannedResponse` requests:
delayed responses, holding a response until the client disconnects, and
aborting a connection after a partial write. When the plan's action is
``CLOSE_PARTIAL`` the plan body carries the fully encoded response and only
``partial_bytes`` of it are written before the socket closes.
"""

import asyncio
import contextlib
from collections.abc import Callable
from typing import Final

from paritygrid.demo.simulators.http_wire import (
    MAX_HEADER_BYTES,
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
_STREAM_LIMIT_BYTES: Final = 1 << 20


class AsyncHttpServiceError(RuntimeError):
    """Raised when the async service is misused."""


class AsyncHttpService:
    """One loopback asyncio HTTP service around a request handler."""

    def __init__(
        self,
        *,
        service_name: str,
        handler: Callable[[HttpRequest], PlannedResponse],
    ) -> None:
        self._service_name = service_name
        self._handler = handler
        self._server: asyncio.Server | None = None
        self._port: int | None = None
        self._closing = False
        self._connection_tasks: set[asyncio.Task[object]] = set()
        self._connection_writers: set[asyncio.StreamWriter] = set()

    @property
    def service_name(self) -> str:
        """Return the service name used for diagnostics."""
        return self._service_name

    @property
    def port(self) -> int:
        """Return the dynamically assigned loopback port."""
        if self._port is None:
            raise AsyncHttpServiceError("the async service has not started")
        return self._port

    @property
    def base_url(self) -> str:
        """Return the loopback base URL of this service."""
        return f"http://{_LOOPBACK_HOST}:{self.port}"

    def is_serving(self) -> bool:
        """Report whether the bound listener still accepts connections."""
        return self._server is not None and self._server.is_serving()

    async def start(self) -> None:
        """Bind the dynamic loopback port and begin serving."""
        if self._server is not None:
            raise AsyncHttpServiceError("the async service already started")
        self._closing = False
        self._server = await asyncio.start_server(
            self._handle_connection,
            host=_LOOPBACK_HOST,
            port=0,
            limit=_STREAM_LIMIT_BYTES,
        )
        sockets = self._server.sockets
        if not sockets:
            raise AsyncHttpServiceError("the async service bound no socket")
        self._port = sockets[0].getsockname()[1]

    async def aclose(self) -> None:
        """Stop accepting connections and promptly interrupt active handlers."""
        server = self._server
        self._server = None
        self._port = None
        self._closing = True
        if server is not None:
            server.close()
        for writer in tuple(self._connection_writers):
            writer.close()
        current_task = asyncio.current_task()
        active_tasks = tuple(task for task in self._connection_tasks if task is not current_task)
        for task in active_tasks:
            task.cancel()
        if active_tasks:
            await asyncio.gather(*active_tasks, return_exceptions=True)
        if server is not None:
            await server.wait_closed()

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        task = asyncio.current_task()
        if self._closing:
            writer.close()
            with contextlib.suppress(ConnectionError, OSError):
                await writer.wait_closed()
            return
        self._connection_writers.add(writer)
        if task is not None:
            self._connection_tasks.add(task)
        try:
            await self._serve_requests(reader, writer)
        except ConnectionError, asyncio.IncompleteReadError, asyncio.CancelledError, OSError:
            pass
        finally:
            self._connection_writers.discard(writer)
            if task is not None:
                self._connection_tasks.discard(task)
            writer.close()
            with contextlib.suppress(ConnectionError, OSError):
                await writer.wait_closed()

    async def _serve_requests(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        while True:
            head = await self._read_head(reader, writer)
            if head is None:
                return
            request_head, keep_alive = head
            if request_head.expects_continue:
                writer.write(b"HTTP/1.1 100 Continue\r\n\r\n")
                await writer.drain()
            body = (
                await asyncio.wait_for(
                    reader.readexactly(request_head.content_length), timeout=_IO_TIMEOUT_SECONDS
                )
                if request_head.content_length
                else b""
            )
            try:
                request = build_request(request_head, body)
            except ValueError:
                writer.write(
                    error_response(
                        400, "bad_request", "The request query was not accepted."
                    ).encoded()
                )
                await writer.drain()
                return
            try:
                planned = self._handler(request)
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
            if planned.delay_microseconds:
                await asyncio.sleep(planned.delay_microseconds / 1_000_000)
            if planned.action is ResponseAction.HOLD_UNTIL_DISCONNECT:
                await self._hold_until_disconnect(reader, planned.hold_cap_microseconds)
                return
            if planned.action is ResponseAction.CLOSE_PARTIAL:
                encoded = planned.body
                writer.write(encoded[: max(planned.partial_bytes, 1)])
                await writer.drain()
                return
            writer.write(planned.encoded())
            await writer.drain()
            if not keep_alive or planned.close_after_response:
                return

    async def _read_head(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> tuple[RequestHead, bool] | None:
        """Read one request head; answer protocol errors and close."""
        try:
            head_bytes = await asyncio.wait_for(
                reader.readuntil(_HEAD_TERMINATOR), timeout=_IO_TIMEOUT_SECONDS
            )
        except TimeoutError:
            return None
        except asyncio.IncompleteReadError:
            return None
        except asyncio.LimitOverrunError:
            await self._write_and_drain(
                writer,
                error_response(431, "headers_too_large", "The request head exceeds its bound."),
            )
            return None
        except ConnectionError, OSError:
            return None
        raw_head = head_bytes[: -len(_HEAD_TERMINATOR)]
        if len(raw_head) > MAX_HEADER_BYTES:
            await self._write_and_drain(
                writer,
                error_response(431, "headers_too_large", "The request head exceeds its bound."),
            )
            return None
        try:
            parsed = parse_request_head(raw_head)
        except RequestBodyTooLargeError:
            await self._write_and_drain(
                writer,
                error_response(413, "payload_too_large", "The request body exceeds its bound."),
            )
            return None
        except ValueError:
            await self._write_and_drain(
                writer,
                error_response(400, "bad_request", "The request could not be parsed."),
            )
            return None
        return parsed, not parsed.connection_close

    async def _write_and_drain(
        self, writer: asyncio.StreamWriter, planned: PlannedResponse
    ) -> None:
        writer.write(planned.encoded())
        with contextlib.suppress(ConnectionError, OSError):
            await writer.drain()

    async def _hold_until_disconnect(self, reader: asyncio.StreamReader, cap: int) -> None:
        """Wait until the client disconnects or the hold cap elapses."""
        seconds = cap / 1_000_000 if cap else _IO_TIMEOUT_SECONDS
        with contextlib.suppress(TimeoutError, ConnectionError, OSError):
            await asyncio.wait_for(reader.read(1), timeout=seconds)
