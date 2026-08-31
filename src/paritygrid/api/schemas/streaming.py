"""Wire shapes for durable and ephemeral live channels.

The two channel envelopes are deliberately distinct: durable frames carry
the per-run sequence identity and the committed payload, while telemetry
frames carry only advisory sampled metrics and never a sequence identity.
"""

from typing import Literal

from pydantic import Field

from paritygrid.api.schemas.common import TRANSPORT_SCHEMA_VERSION, TransportModel

DURABLE_CHANNEL = "durable-events"
TELEMETRY_CHANNEL = "telemetry"


class DurableEventFrame(TransportModel):
    """One committed durable event rendered on the SSE channel."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    channel: Literal["durable-events"] = DURABLE_CHANNEL
    sequence: int = Field(ge=1, le=2_147_483_647)
    run_id: str
    event_kind: str
    subject_kind: str
    subject_id: str
    occurred_at: str
    correlation_id: str | None = None
    payload_schema_version: int
    payload: dict[str, object]


class TelemetryMetricBody(TransportModel):
    """One bounded advisory metric observation."""

    name: str
    kind: str
    value: int
    labels: dict[str, str]


class TelemetryRecordBody(TransportModel):
    """One sampled ephemeral observation; never authoritative state."""

    schema_version: int
    observed_at_micros: int
    run_id: str
    metrics: list[TelemetryMetricBody]


class TelemetryFrame(TransportModel):
    """One advisory telemetry batch rendered on the live channel."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    channel: Literal["telemetry"] = TELEMETRY_CHANNEL
    advisory: Literal[True] = True
    sampled: bool
    dropped: int
    records: list[TelemetryRecordBody]


class LiveSnapshotFrame(TransportModel):
    """The bounded snapshot sent immediately after a live channel opens."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    channel: Literal["telemetry"] = TELEMETRY_CHANNEL
    advisory: Literal[True] = True
    records: list[TelemetryRecordBody]


class LivePongFrame(TransportModel):
    """The bounded reply to one client keepalive."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    channel: Literal["telemetry"] = TELEMETRY_CHANNEL
    type: Literal["pong"] = "pong"


class LivePingFrame(TransportModel):
    """The only client frame accepted by the advisory live channel."""

    type: Literal["ping"]
