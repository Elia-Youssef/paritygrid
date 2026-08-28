"""Security header and CORS-default tests."""

import httpx
import pytest


@pytest.mark.anyio
async def test_every_response_carries_the_security_headers(
    client: httpx.AsyncClient,
) -> None:
    for path in ("/healthz", "/api/v1/pipelines", "/api/v1/system/capabilities"):
        response = await client.get(path)
        headers = response.headers
        assert headers["x-content-type-options"] == "nosniff", path
        assert headers["x-frame-options"] == "DENY", path
        assert headers["referrer-policy"] == "no-referrer", path
        assert "default-src 'none'" in headers["content-security-policy"], path
        assert headers["cross-origin-opener-policy"] == "same-origin", path


@pytest.mark.anyio
async def test_error_responses_carry_the_security_headers(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/pipelines/pip_missing-one")
    assert response.status_code == 404
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"


@pytest.mark.anyio
async def test_api_data_responses_are_not_storable(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/pipelines")
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.anyio
async def test_cors_is_disabled_by_default(client: httpx.AsyncClient) -> None:
    preflight = await client.options(
        "/api/v1/pipelines",
        headers={
            "Origin": "http://example.invalid",
            "Access-Control-Request-Method": "POST",
        },
    )
    assert "access-control-allow-origin" not in preflight.headers
    response = await client.get("/api/v1/pipelines", headers={"Origin": "http://example.invalid"})
    assert "access-control-allow-origin" not in response.headers


@pytest.mark.anyio
async def test_responses_do_not_expose_server_internals(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/system/capabilities")
    for header in response.headers:
        lowered = header.lower()
        assert not lowered.startswith("x-powered"), lowered
        assert lowered not in {"server", "x-aspnet-version"}
    text = response.text
    for forbidden in ("\\\\", "C:\\", "/home/", "Traceback", "sqlite:///"):
        assert forbidden not in text
