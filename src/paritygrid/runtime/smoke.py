"""Local HTTP startup and shutdown verification."""

import json
import socket
from collections.abc import Callable
from dataclasses import dataclass
from threading import Thread
from time import monotonic, sleep
from typing import Final
from urllib.request import urlopen

import uvicorn

from paritygrid.runtime.composition import create_runtime_app
from paritygrid.runtime.config import Settings

_LOOPBACK_HOST: Final = "127.0.0.1"


@dataclass(frozen=True, slots=True)
class SmokeResult:
    """Observed operational endpoint states from a local server."""

    health_status: str
    readiness_status: str


def _wait_until(predicate: Callable[[], bool], timeout_seconds: float) -> None:
    deadline = monotonic() + timeout_seconds
    while monotonic() < deadline:
        if predicate():
            return
        sleep(0.01)
    msg = "Timed out while waiting for the local server."
    raise TimeoutError(msg)


def run_smoke(settings: Settings | None = None) -> SmokeResult:
    """Start a loopback server, probe it over HTTP, and stop it cleanly."""
    runtime_settings = settings or Settings()
    application = create_runtime_app(runtime_settings)
    config = uvicorn.Config(
        application,
        host=_LOOPBACK_HOST,
        port=0,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(config)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((_LOOPBACK_HOST, 0))
        listener.listen(128)
        port = listener.getsockname()[1]
        server_thread = Thread(
            target=server.run,
            kwargs={"sockets": [listener]},
            name="paritygrid-smoke-server",
            daemon=True,
        )
        server_thread.start()
        try:
            _wait_until(lambda: server.started, runtime_settings.smoke_timeout_seconds)
            with urlopen(
                f"http://{_LOOPBACK_HOST}:{port}/healthz",
                timeout=runtime_settings.smoke_timeout_seconds,
            ) as health_response:
                health_payload = json.load(health_response)
            with urlopen(
                f"http://{_LOOPBACK_HOST}:{port}/readyz",
                timeout=runtime_settings.smoke_timeout_seconds,
            ) as readiness_response:
                readiness_payload = json.load(readiness_response)
        finally:
            server.should_exit = True
            server_thread.join(runtime_settings.smoke_timeout_seconds)

    if server_thread.is_alive():
        msg = "The local server did not stop within the configured timeout."
        raise TimeoutError(msg)
    return SmokeResult(
        health_status=str(health_payload["status"]),
        readiness_status=str(readiness_payload["status"]),
    )
