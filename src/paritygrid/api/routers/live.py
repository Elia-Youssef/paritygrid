"""Ephemeral live telemetry WebSocket for one run.

The channel carries only bounded advisory telemetry: a connection
snapshot, sampled metric batches, and keepalive replies.  It is never
authoritative state, never persisted, and unmistakably distinct from the
durable SSE channel through its ``channel: "telemetry"`` envelope and its
explicit ``advisory`` marker.  Disconnects, slow consumers, malformed
input, and sampling loss can never alter run state or durable history.
"""

import contextlib
import json
from typing import cast

import anyio
import anyio.to_thread
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from paritygrid.api.dependencies import ApiServices
from paritygrid.api.schemas.streaming import (
    LivePingFrame,
    LivePongFrame,
    LiveSnapshotFrame,
    TelemetryFrame,
    TelemetryMetricBody,
    TelemetryRecordBody,
)
from paritygrid.application.execution.telemetry import TelemetryRecord
from paritygrid.application.services.errors import OperationalRecordNotFoundError
from paritygrid.application.services.telemetry import (
    LiveTelemetryChannel,
    TelemetrySubscriberLimitError,
    TelemetrySubscription,
)

router = APIRouter(prefix="/api/v1/live", tags=["live"])

MAX_CLIENT_MESSAGE_BYTES = 4_096
RUN_NOT_FOUND_CLOSE_CODE = 4404
POLICY_CLOSE_CODE = 1008
OVERSIZED_CLOSE_CODE = 1009
CAPACITY_CLOSE_CODE = 1013
INTERNAL_CLOSE_CODE = 1011


@router.websocket("/runs/{run_id}")
async def live_run_telemetry(websocket: WebSocket, run_id: str) -> None:
    """Serve bounded advisory telemetry for one run after access validation."""
    services = _services(websocket)
    channel: LiveTelemetryChannel = services.telemetry
    try:
        run = await anyio.to_thread.run_sync(lambda: services.runs.get(run_id))
    except OperationalRecordNotFoundError:
        await websocket.close(code=RUN_NOT_FOUND_CLOSE_CODE, reason="run not found")
        return
    try:
        subscription = await anyio.to_thread.run_sync(
            lambda: channel.hub.subscribe(run.run_id.value)
        )
    except TelemetrySubscriberLimitError:
        await websocket.close(code=CAPACITY_CLOSE_CODE, reason="telemetry capacity")
        return
    await websocket.accept()
    try:
        snapshot = await anyio.to_thread.run_sync(
            lambda: channel.snapshot_for(run_id=run.run_id.value)
        )
        await _send(websocket, channel, _snapshot_text(snapshot))
        async with anyio.create_task_group() as group:
            group.start_soon(_pump_and_cancel, group.cancel_scope, websocket, channel, subscription)
            await _serve_client_messages(websocket, channel)
            # A client disconnect or policy close owns the group's lifetime:
            # never leave the pump polling after its subscriber has gone.
            group.cancel_scope.cancel()
    except TimeoutError:
        await _safe_close(websocket, POLICY_CLOSE_CODE, reason="slow consumer")
    except Exception:
        await _safe_close(websocket, INTERNAL_CLOSE_CODE, reason="internal error")
        raise
    finally:
        # Client and application cancellation must never strand a bounded
        # subscriber in the hub. Shield just this local cleanup; it cannot
        # affect the durable writer or execution owner.
        with anyio.CancelScope(shield=True):
            await anyio.to_thread.run_sync(subscription.close)


async def _pump_telemetry(
    websocket: WebSocket, channel: LiveTelemetryChannel, subscription: TelemetrySubscription
) -> None:
    while not subscription.closed and not _client_gone(websocket):
        records = await anyio.to_thread.run_sync(subscription.drain)
        if records:
            frame = TelemetryFrame(
                sampled=True,
                dropped=subscription.dropped,
                records=[_record_body(record) for record in records],
            )
            try:
                await _send(websocket, channel, frame.model_dump_json())
            except TimeoutError:
                # A consumer that cannot keep up is disconnected here rather
                # than ever delaying execution or persistence.
                await _safe_close(websocket, POLICY_CLOSE_CODE, reason="slow consumer")
                return
            except WebSocketDisconnect:
                return
            continue
        await anyio.sleep(channel.poll_seconds)
        # This is a real, bounded production publication of current writer
        # diagnostics. It is advisory only and may be sampled or dropped.
        await anyio.to_thread.run_sync(lambda: channel.publish_snapshot(run_id=subscription.run_id))


async def _pump_and_cancel(
    cancel_scope: anyio.CancelScope,
    websocket: WebSocket,
    channel: LiveTelemetryChannel,
    subscription: TelemetrySubscription,
) -> None:
    """Stop the reader when the telemetry pump reaches a terminal condition."""
    await _pump_telemetry(websocket, channel, subscription)
    cancel_scope.cancel()


def _client_gone(websocket: WebSocket) -> bool:
    return websocket.application_state is WebSocketState.DISCONNECTED


async def _serve_client_messages(websocket: WebSocket, channel: LiveTelemetryChannel) -> None:
    while True:
        message = await websocket.receive()
        if message["type"] == "websocket.disconnect":
            return
        close_code: int | None = None
        close_reason = ""
        text = message.get("text")
        if type(text) is not str:
            close_code, close_reason = POLICY_CLOSE_CODE, "unsupported message"
        elif len(text.encode("utf-8")) > MAX_CLIENT_MESSAGE_BYTES:
            close_code, close_reason = OVERSIZED_CLOSE_CODE, "message too large"
        else:
            try:
                document: object = json.loads(text)
            except ValueError:
                close_code, close_reason = POLICY_CLOSE_CODE, "malformed message"
            else:
                try:
                    LivePingFrame.model_validate(document)
                except ValueError:
                    close_code, close_reason = POLICY_CLOSE_CODE, "unsupported message"
        if close_code is not None:
            await _safe_close(websocket, close_code, reason=close_reason)
            return
        try:
            await _send(websocket, channel, LivePongFrame().model_dump_json())
        except TimeoutError:
            await _safe_close(websocket, POLICY_CLOSE_CODE, reason="slow consumer")
            return
        except WebSocketDisconnect:
            return


def _snapshot_text(record: TelemetryRecord) -> str:
    frame = LiveSnapshotFrame(records=[_record_body(record)])
    return frame.model_dump_json()


async def _send(websocket: WebSocket, channel: LiveTelemetryChannel, text: str) -> None:
    with anyio.fail_after(channel.send_timeout_seconds):
        await websocket.send_text(text)


async def _safe_close(websocket: WebSocket, code: int, *, reason: str) -> None:
    with contextlib.suppress(Exception):
        await websocket.close(code=code, reason=reason)


def _record_body(record: TelemetryRecord) -> TelemetryRecordBody:
    return TelemetryRecordBody(
        schema_version=record.schema_version,
        observed_at_micros=record.observed_at_micros,
        run_id=record.run_id,
        metrics=[
            TelemetryMetricBody(
                name=metric.name,
                kind=metric.kind.value,
                value=metric.value,
                labels=dict(metric.labels),
            )
            for metric in record.metrics
        ],
    )


def _services(websocket: WebSocket) -> ApiServices:
    services = getattr(websocket.app.state, "services", None)
    if services is None:
        raise RuntimeError("runtime services are not configured")
    return cast(ApiServices, services)
