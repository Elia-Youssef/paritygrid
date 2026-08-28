"""Packaged frontend serving: confinement, cache policy, SPA fallback."""

import os
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI

from paritygrid.api.app import create_app
from paritygrid.api.errors.problems import ProblemError
from paritygrid.api.frontend import FrontendAssets
from paritygrid.runtime.composition import RuntimeContainer
from tests.api.conftest import seed_scenario

INDEX_HTML = "<!doctype html><html><body>shell</body></html>"
ASSET_JS = "console.log('packaged');"
ASSET_CSS = "body{margin:0}"
UNKNOWN_EXT = b"binary-ish"


@pytest.fixture
def dist(tmp_path: Path) -> Path:
    root = tmp_path / "dist"
    (root / "assets").mkdir(parents=True)
    (root / "index.html").write_text(INDEX_HTML, encoding="utf-8")
    (root / "assets" / "index-abc123.js").write_text(ASSET_JS, encoding="utf-8")
    (root / "assets" / "index-abc123.css").write_text(ASSET_CSS, encoding="utf-8")
    (root / "assets" / "blob.bin").write_bytes(UNKNOWN_EXT)
    (root / "legal.txt").write_text("notice", encoding="utf-8")
    return root


def _app(dist: Path) -> FastAPI:
    return create_app(frontend=FrontendAssets(dist))


@pytest.fixture
def frontend_client(dist: Path) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=_app(dist))
    client = httpx.AsyncClient(transport=transport, base_url="http://t")
    return client


pytestmark = pytest.mark.anyio


async def test_root_serves_the_shell_with_frontend_policy(
    frontend_client: httpx.AsyncClient,
) -> None:
    response = await frontend_client.get("/")
    assert response.status_code == 200
    assert response.text == INDEX_HTML
    assert response.headers["content-type"].startswith("text/html")
    assert response.headers["cache-control"] == "no-cache"
    assert "script-src 'self'" in response.headers["content-security-policy"]
    assert "connect-src 'self'" in response.headers["content-security-policy"]
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"


async def test_hashed_assets_are_immutable_with_safe_media_types(
    frontend_client: httpx.AsyncClient,
) -> None:
    script = await frontend_client.get("/assets/index-abc123.js")
    assert script.status_code == 200
    assert script.text == ASSET_JS
    assert script.headers["content-type"].startswith("text/javascript")
    assert script.headers["cache-control"] == "public, max-age=31536000, immutable"
    style = await frontend_client.get("/assets/index-abc123.css")
    assert style.headers["content-type"].startswith("text/css")
    blob = await frontend_client.get("/assets/blob.bin")
    assert blob.headers["content-type"] == "application/octet-stream"
    assert blob.headers["x-content-type-options"] == "nosniff"


async def test_spa_fallback_serves_the_shell_for_navigation(
    frontend_client: httpx.AsyncClient,
) -> None:
    for path in ("/runs", "/runs/run_deep-01", "/app/pipelines/new"):
        response = await frontend_client.get(path)
        assert response.status_code == 200
        assert response.text == INDEX_HTML
        assert response.headers["cache-control"] == "no-cache"


async def test_api_paths_are_never_swallowed_by_the_fallback(
    client: httpx.AsyncClient, container: RuntimeContainer, dist: Path
) -> None:
    await seed_scenario(client)
    transport = httpx.ASGITransport(app=_app(dist))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as front:
        unknown_api = await front.get("/api/v1/definitely-not-a-route")
        assert unknown_api.status_code == 404
        assert unknown_api.headers["content-type"].startswith("application/problem+json")
        body = unknown_api.json()
        assert body["type"] == "https://paritygrid.dev/problems/not-found"
        probe = await front.get("/healthz")
        assert probe.status_code == 200
        stream_path = await front.get("/api/v1/stream/runs/run_x-001")
        assert stream_path.status_code in {404, 503}
        live_path = await front.get("/api/v1/live/runs/run_x-001")
        assert live_path.status_code in {404, 405, 503}


async def test_traversal_and_encoded_paths_are_confined(frontend_client: httpx.AsyncClient) -> None:
    candidates = (
        "/..%2findex.html",
        "/%2e%2e/index.html",
        "/%252e%252e/index.html",
        "/assets/..%5cindex.html",
        "/a%5cb",
        "/C:/windows/system32/config",
        "/a\\b",
        "/%00",
        "/.hidden",
        "/assets/.hidden.js",
        "/assets/missing.js",
        "/legal.txt.bak.",
    )
    for path in candidates:
        response = await frontend_client.get(path)
        assert response.status_code == 404, path
        assert response.headers["content-type"].startswith("application/problem+json")


async def test_directories_never_produce_listings(frontend_client: httpx.AsyncClient) -> None:
    response = await frontend_client.get("/assets")
    assert response.status_code == 404
    listing = await frontend_client.get("/assets/")
    assert listing.status_code == 404


async def test_known_file_with_unknown_extension_serves_plain(
    frontend_client: httpx.AsyncClient,
) -> None:
    response = await frontend_client.get("/legal.txt")
    assert response.status_code == 200
    assert response.text == "notice"
    assert response.headers["content-type"].startswith("text/plain")


async def test_raw_socket_dot_traversal_is_rejected(dist: Path) -> None:
    import contextlib
    import socket
    import threading

    import anyio
    import uvicorn

    application = _app(dist)
    config = uvicorn.Config(application, host="127.0.0.1", port=0, log_level="critical")
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
        for raw_target in ("/../index.html", "/assets/../../secret.txt"):
            with socket.create_connection(("127.0.0.1", port), timeout=5.0) as sock:
                request = (
                    f"GET {raw_target} HTTP/1.1\r\n"
                    f"Host: 127.0.0.1:{port}\r\n"
                    "Connection: close\r\n\r\n"
                ).encode("ascii")
                sock.sendall(request)
                sock.settimeout(5.0)
                head = b""
                with contextlib.suppress(OSError):
                    while b"\r\n\r\n" not in head and len(head) < 8192:
                        chunk = sock.recv(4096)
                        if not chunk:
                            break
                        head += chunk
                assert b" 404 " in head.split(b"\r\n")[0], raw_target
    finally:
        server.should_exit = True
        thread.join(timeout=10.0)


def _has_symlink_support() -> bool:
    try:
        import tempfile

        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "target"
            target.mkdir()
            link = Path(temporary) / "link"
            os.symlink(target, link, target_is_directory=True)
            return True
    except OSError:
        return False


@pytest.mark.skipif(
    not _has_symlink_support(), reason="filesystem links are unavailable in this environment"
)
async def test_symlink_escape_is_rejected(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("secret", encoding="utf-8")
    dist = tmp_path / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text(INDEX_HTML, encoding="utf-8")
    os.symlink(outside, dist / "assets" / "escape", target_is_directory=True)
    application = _app(dist)
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        response = await client.get("/assets/escape/secret.txt")
    assert response.status_code == 404


@pytest.mark.skipif(
    not _has_symlink_support(), reason="filesystem links are unavailable in this environment"
)
async def test_spa_fallback_rejects_an_index_symlink_escape(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "shell.html").write_text("outside shell", encoding="utf-8")
    dist = tmp_path / "dist"
    dist.mkdir()
    os.symlink(outside / "shell.html", dist / "index.html")
    application = _app(dist)
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        response = await client.get("/runs/run_link-01")
    assert response.status_code == 404
    assert "outside shell" not in response.text


def test_spa_fallback_confines_the_resolved_shell_without_symlink_support(
    dist: Path, tmp_path: Path
) -> None:
    outside_shell = tmp_path / "outside-shell.html"
    outside_shell.write_text("outside shell", encoding="utf-8")
    assets = FrontendAssets(dist)
    object.__setattr__(assets, "_index", outside_shell)
    with pytest.raises(ProblemError):
        assets.resolve("/runs/run_link-01", raw_path=b"/runs/run_link-01")


def test_missing_root_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="existing directory"):
        FrontendAssets(tmp_path / "not-there")


def test_frontend_assets_resolve_plain_paths(dist: Path) -> None:
    assets = FrontendAssets(dist)
    resolution = assets.resolve("/assets/index-abc123.js", raw_path=b"/assets/index-abc123.js")
    assert resolution.file.name == "index-abc123.js"
    assert resolution.cache_control == "public, max-age=31536000, immutable"
