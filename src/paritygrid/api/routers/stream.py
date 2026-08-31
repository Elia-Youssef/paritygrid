"""Durable SSE stream over committed per-run execution events.

Every frame replays committed durable history only: identifiers are the
per-run sequence numbers, resume positions come from ``after`` or a valid
``Last-Event-ID``, and the connection terminates rather than silently
skipping when durable contiguity cannot be proven.  Each bounded page is
read in a short independent transaction.  Timed sends disconnect a client
that stops consuming without blocking persistence or execution.
"""

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from typing import Annotated, cast

import anyio
import anyio.to_thread
from fastapi import APIRouter, Header, Query, Request
from fastapi.responses import StreamingResponse
from starlette.types import Message, Send

from paritygrid.api.dependencies import get_services
from paritygrid.api.errors.problems import ProblemError
from paritygrid.api.schemas.streaming import DurableEventFrame
from paritygrid.application.ports.consistency import ExecutionEventRecord
from paritygrid.application.services.events import DurableEventPageView, DurableEventStreamService

router = APIRouter(prefix="/api/v1/stream/runs", tags=["streams"])

LAST_EVENT_ID_HEADER = "Last-Event-ID"
DEFAULT_HEARTBEAT_SECONDS = 15.0
DEFAULT_POLL_SECONDS = 0.25
DEFAULT_SEND_TIMEOUT_SECONDS = 5.0
MAX_SEQUENCE = 2_147_483_647


@router.get(
    "/{run_id}",
    response_class=StreamingResponse,
    summary="Stream committed durable run events",
    description=(
        "Replays committed events after ``after`` or a valid ``Last-Event-ID``. "
        "Frame identifiers are the per-run durable sequence numbers; heartbeats "
        "are comments while idle; resume never duplicates or skips a retained "
        "event. A sequence ahead of the durable frontier is rejected with 409 "
        "before streaming begins."
    ),
)
def stream_run_events(
    run_id: str,
    request: Request,
    after: Annotated[str | None, Query()] = None,
    last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
) -> StreamingResponse:
    services = get_services(request)
    stream = services.event_stream
    if len(request.query_params.getlist("after")) > 1:
        raise ProblemError(
            type_slug="validation",
            title="Request validation failed",
            status=400,
            detail="supply one resume position",
            code="invalid_resume_position",
        )
    supplied = _resume_position(after, last_event_id)
    _run, frontier = stream.frontier(run_id)
    if supplied > frontier:
        raise ProblemError(
            type_slug="stream-sequence-ahead",
            title="Stream resume position is ahead of durable history",
            status=409,
            detail=(
                "the requested sequence is ahead of the durable frontier; "
                "restart the stream from an earlier sequence"
            ),
            code="stream_sequence_ahead",
        )
    # Prime the first bounded page before the HTTP response starts.  A gap is
    # therefore a normal bounded Problem Details response, never a partially
    # emitted stream that silently skips durable history.
    initial = stream.read_page(run_id, after=supplied)
    return BoundedSSEStreamingResponse(
        _durable_frames(
            stream,
            run_id=run_id,
            cursor=supplied,
            heartbeat_seconds=stream.heartbeat_seconds,
            poll_seconds=stream.poll_seconds,
            initial=initial,
        ),
        send_timeout_seconds=DEFAULT_SEND_TIMEOUT_SECONDS,
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-store",
            "X-Accel-Buffering": "no",
        },
    )


def _resume_position(after: str | None, last_event_id: str | None) -> int:
    if after is not None and last_event_id is not None:
        raise ProblemError(
            type_slug="validation",
            title="Request validation failed",
            status=400,
            detail="supply either the after parameter or Last-Event-ID, not both",
            code="invalid_resume_position",
        )
    if after is not None:
        return _parse_resume_sequence(after)
    if last_event_id is None:
        return 0
    return _parse_resume_sequence(last_event_id)


def _parse_resume_sequence(text: str) -> int:
    canonical = (
        text.isascii() and text.isdecimal() and (text == "0" or (bool(text) and text[0] != "0"))
    )
    if not canonical or len(text) > 10 or int(text) > MAX_SEQUENCE:
        raise ProblemError(
            type_slug="validation",
            title="Request validation failed",
            status=400,
            detail="the resume position must be one canonical durable sequence number",
            code="invalid_last_event_id",
        )
    return int(text)


async def _durable_frames(
    stream: DurableEventStreamService,
    *,
    run_id: str,
    cursor: int,
    heartbeat_seconds: float,
    poll_seconds: float,
    initial: DurableEventPageView,
) -> AsyncIterator[bytes]:
    position = cursor
    idle = 0.0
    first: DurableEventPageView | None = initial
    yield b"retry: 3000\n\n"
    while not stream.is_stopping():
        if first is not None:
            view = first
            first = None
        else:
            view = await anyio.to_thread.run_sync(
                lambda position=position: stream.read_page(run_id, after=position)
            )
        page = view.page
        if page.items:
            for record in page.items:
                yield _frame(record)
                position = record.sequence.number
            continue
        await anyio.sleep(poll_seconds)
        idle += poll_seconds
        if idle >= heartbeat_seconds:
            yield b": paritygrid-heartbeat\n\n"
            idle = 0.0


def _frame(record: ExecutionEventRecord) -> bytes:
    frame = DurableEventFrame(
        sequence=record.sequence.number,
        run_id=record.run_id.value,
        event_kind=record.event_kind,
        subject_kind=record.subject_kind.value,
        subject_id=_subject_id(record),
        occurred_at=str(record.occurred_at),
        correlation_id=record.correlation_id,
        payload_schema_version=record.payload_schema_version,
        payload=dict(record.payload.to_mapping()),
    )
    data = frame.model_dump_json()
    return f"id: {record.sequence.number}\nevent: {record.event_kind}\ndata: {data}\n\n".encode()


def _subject_id(record: ExecutionEventRecord) -> str:
    subject = record.subject_id
    return getattr(subject, "value", str(subject))


class BoundedSSEStreamingResponse(StreamingResponse):
    """A streaming response that aborts a stalled HTTP send within a bound."""

    def __init__(
        self,
        content: AsyncIterator[bytes],
        *,
        send_timeout_seconds: float,
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
        media_type: str | None = None,
    ) -> None:
        if type(send_timeout_seconds) is not float or not 0.1 <= send_timeout_seconds <= 60.0:
            raise ValueError("SSE send timeout is outside the supported range")
        super().__init__(content, status_code=status_code, headers=headers, media_type=media_type)
        self._send_timeout_seconds = send_timeout_seconds

    async def stream_response(self, send: Send) -> None:
        try:
            await self._send_with_timeout(
                send,
                {
                    "type": "http.response.start",
                    "status": self.status_code,
                    "headers": self.raw_headers,
                },
            )
            async for chunk in self.body_iterator:
                body = chunk.encode(self.charset) if isinstance(chunk, str) else bytes(chunk)
                await self._send_with_timeout(
                    send,
                    {"type": "http.response.body", "body": body, "more_body": True},
                )
            await self._send_with_timeout(
                send,
                {"type": "http.response.body", "body": b"", "more_body": False},
            )
        finally:
            closer = cast(
                "Callable[[], Awaitable[None]] | None", getattr(self.body_iterator, "aclose", None)
            )
            if closer is not None:
                await closer()

    async def _send_with_timeout(self, send: Send, message: Message) -> None:
        with anyio.fail_after(self._send_timeout_seconds):
            await send(message)


__all__ = [
    "DEFAULT_HEARTBEAT_SECONDS",
    "DEFAULT_POLL_SECONDS",
    "DEFAULT_SEND_TIMEOUT_SECONDS",
    "LAST_EVENT_ID_HEADER",
    "MAX_SEQUENCE",
    "BoundedSSEStreamingResponse",
    "router",
]
