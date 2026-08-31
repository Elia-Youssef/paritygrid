"""Deterministic OpenAPI document construction for contract export."""

import json

from paritygrid.api.app import create_app

LIVE_TRANSPORT_EXTENSION = {
    "x-paritygrid-live-transports": [
        {
            "channel": "telemetry",
            "path": "/api/v1/live/runs/{run_id}",
            "protocol": "websocket",
            "envelope_schema_version": 1,
            "advisory": True,
            "description": (
                "Ephemeral operational telemetry only; never authoritative "
                "state and never durable history."
            ),
        },
        {
            "channel": "durable-events",
            "path": "/api/v1/stream/runs/{run_id}",
            "protocol": "sse",
            "envelope_schema_version": 1,
            "advisory": False,
            "description": "Replay of committed durable run events by sequence.",
        },
    ]
}


def build_document() -> str:
    """Render the deterministic OpenAPI document from the application."""
    application = create_app()
    document = application.openapi()
    document.update(LIVE_TRANSPORT_EXTENSION)
    return json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


__all__ = ["LIVE_TRANSPORT_EXTENSION", "build_document"]
