"""Slow and disconnected HTTP client behavior tests."""

import contextlib
import socket
import threading
from collections.abc import Generator
from typing import cast

import anyio
import httpx
import pytest
from fastapi import FastAPI

from paritygrid.application.ports.artifact_streaming import (
    ArtifactByteRange,
    ArtifactByteStream,
    ArtifactStreamMetadata,
)
from paritygrid.runtime.composition import RuntimeContainer
from tests.api.conftest import seed_scenario
from tests.api.test_artifacts import commit_artifact

RUN_ID = "run_scenario-01"
CONTENT = b"slow client artifact bytes " * 64
ARTIFACT_PATH = "/api/v1/artifacts/art_slow-001"


@pytest.fixture
def artifact_path(container: RuntimeContainer) -> str:
    """Path only; tests commit the artifact after seeding the run."""
    del container
    return ARTIFACT_PATH


@pytest.mark.anyio
async def test_cancelling_the_stream_releases_the_underlying_descriptor(
    container: RuntimeContainer,
    app: FastAPI,
    client: httpx.AsyncClient,
    artifact_path: str,
) -> None:
    await seed_scenario(client)
    commit_artifact(container, artifact_id="art_slow-001", content=CONTENT)
    close_events: list[bool] = []
    original_open = container.services.artifacts.open

    def tracking_open(
        artifact_id: str, *, byte_range: ArtifactByteRange | None
    ) -> tuple[ArtifactStreamMetadata, _CloseTrackingStream]:
        metadata, stream = original_open(artifact_id, byte_range=byte_range)
        close_events.append(False)
        return metadata, _CloseTrackingStream(stream, close_events)

    container.services.artifacts.open = tracking_open  # type: ignore[method-assign]

    transport = httpx.ASGITransport(app=app)
    collected = bytearray()

    async def consume_partially() -> None:
        async with (
            httpx.AsyncClient(transport=transport, base_url="http://t") as probe,
            probe.stream("GET", artifact_path) as response,
        ):
            assert response.status_code == 200
            async for chunk in response.aiter_bytes():
                collected.extend(chunk)
                # A slow reader stops mid-stream and disconnects.
                raise httpx.ReadError("client disconnected")

    with pytest.raises(httpx.ReadError):
        await consume_partially()
    assert 0 < len(collected) <= len(CONTENT)
    assert close_events, "stream must close on disconnect"
    assert close_events[-1] is True

    async with httpx.AsyncClient(transport=transport, base_url="http://t") as again:
        response = await again.get(artifact_path)
    assert response.status_code == 200
    assert response.content == CONTENT


class _CloseTrackingStream:
    """Delegate that records descriptor release for the disconnect proof."""

    def __init__(self, inner: ArtifactByteStream, close_events: list[bool]) -> None:
        self._inner = inner
        self._close_events = close_events

    def __iter__(self) -> _CloseTrackingStream:
        return self

    def __next__(self) -> bytes:
        return next(self._inner)

    @property
    def metadata(self) -> ArtifactStreamMetadata:
        return self._inner.metadata

    def close(self) -> None:
        self._close_events[-1] = True
        self._inner.close()


@pytest.mark.anyio
async def test_generator_closes_its_stream_on_early_close() -> None:
    from paritygrid.api.routers.artifacts import bounded_stream_chunks

    closed: list[bool] = []

    class FakeStream:
        metadata = None

        def __enter__(self) -> FakeStream:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            self.close()

        def __iter__(self) -> FakeStream:
            return self

        def __next__(self) -> bytes:
            return b"chunk"

        def close(self) -> None:
            closed.append(True)

    stream = cast(ArtifactByteStream, FakeStream())
    generator = bounded_stream_chunks(stream)
    assert next(generator) == b"chunk"
    typed_generator = cast(Generator[bytes], generator)
    typed_generator.close()
    # Close is reached at least once; delegation may close again, and the
    # real stream contract makes close idempotent.
    assert closed
    assert closed[-1] is True


@pytest.mark.anyio
async def test_real_server_survives_a_slow_disconnecting_client(
    container: RuntimeContainer,
    app: FastAPI,
    client: httpx.AsyncClient,
    artifact_path: str,
) -> None:
    await seed_scenario(client)
    commit_artifact(container, artifact_id="art_slow-001", content=CONTENT)
    import uvicorn

    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="critical")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        for _ in range(100):
            if server.started:
                break
            await anyio.sleep(0.05)
        assert server.started
        port = server.servers[0].sockets[0].getsockname()[1]

        with socket.create_connection(("127.0.0.1", port), timeout=5.0) as sock:
            request = (
                f"GET {artifact_path} HTTP/1.1\r\n"
                f"Host: 127.0.0.1:{port}\r\n"
                "Connection: close\r\n\r\n"
            ).encode("ascii")
            sock.sendall(request)
            sock.settimeout(5.0)
            with contextlib.suppress(OSError):
                sock.recv(64)
            # Abrupt disconnect after the first response bytes.

        with socket.create_connection(("127.0.0.1", port), timeout=5.0) as sock:
            sock.sendall(
                f"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
                "Connection: close\r\n\r\n".encode("ascii")
            )
            sock.settimeout(5.0)
            payload = b""
            while b"\r\n\r\n" not in payload:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                payload += chunk
        assert b"200 OK" in payload
    finally:
        server.should_exit = True
        thread.join(timeout=10.0)
