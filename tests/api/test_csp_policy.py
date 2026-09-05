"""Production Content Security Policy contract tests (P22.3).

The policies pinned here are the shipped browser policy: the packaged
shell runs under the exact restrictive frontend policy, every packaged
asset and every API response is denied outright, and the narrow
documentation policy is confined to the two documentation paths.  A
wildcard origin, ``unsafe-eval``, or a remote origin outside the
documentation policy fails these tests before any browser runs.
"""

import re
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from paritygrid.api.app import create_app
from paritygrid.api.frontend import FrontendAssets
from paritygrid.api.middleware.security_headers import (
    DENY_ALL_CSP as DENY_ALL_CSP_BYTES,
)
from paritygrid.api.middleware.security_headers import (
    DOCUMENTATION_CSP,
)
from paritygrid.api.middleware.security_headers import (
    FRONTEND_CSP as FRONTEND_CSP_BYTES,
)

SHELL_CSP = (
    "default-src 'none'; script-src 'self'; style-src 'self'; "
    "img-src 'self'; font-src 'self'; connect-src 'self'; "
    "object-src 'none'; frame-ancestors 'none'; base-uri 'none'; "
    "form-action 'self'"
)
DENY_ALL_CSP = "default-src 'none'; frame-ancestors 'none'"
DOCUMENTATION_PATHS = ("/api/docs", "/api/docs/oauth2-redirect")

_FRONTEND_E2E = Path(__file__).resolve().parents[2] / "web" / "e2e"

pytestmark = pytest.mark.anyio


@pytest.fixture
def dist(tmp_path: Path) -> Path:
    root = tmp_path / "dist"
    (root / "assets").mkdir(parents=True)
    (root / "index.html").write_text(
        "<!doctype html><html><body>shell</body></html>", encoding="utf-8"
    )
    (root / "assets" / "index-abc123.js").write_text("export {};", encoding="utf-8")
    (root / "assets" / "index-abc123.css").write_text("body{margin:0}", encoding="utf-8")
    (root / "assets" / "inter-latin-wght-normal-Ab12Cd34.woff2").write_bytes(b"font")
    return root


@pytest.fixture
async def frontend_client(dist: Path) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=create_app(frontend=FrontendAssets(dist)))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        yield client


async def test_root_and_deep_links_carry_the_exact_shell_csp(
    frontend_client: httpx.AsyncClient,
) -> None:
    for path in ("/", "/runs", "/runs/run_deep-01", "/reconciliation"):
        response = await frontend_client.get(path)
        assert response.status_code == 200, path
        assert response.headers["content-type"].startswith("text/html"), path
        assert response.headers["content-security-policy"] == SHELL_CSP, path


async def test_packaged_assets_carry_the_deny_all_csp(
    frontend_client: httpx.AsyncClient,
) -> None:
    for path in (
        "/assets/index-abc123.js",
        "/assets/index-abc123.css",
        "/assets/inter-latin-wght-normal-Ab12Cd34.woff2",
    ):
        response = await frontend_client.get(path)
        assert response.status_code == 200, path
        assert response.headers["content-security-policy"] == DENY_ALL_CSP, path
        assert response.headers["x-content-type-options"] == "nosniff", path


async def test_no_policy_contains_wildcards_or_unsafe_eval(
    frontend_client: httpx.AsyncClient,
) -> None:
    paths = ("/", "/assets/index-abc123.js", "/api/v1/pipelines", "/api/docs")
    for path in paths:
        response = await frontend_client.get(path)
        policy = response.headers.get("content-security-policy", "")
        assert "*" not in policy, path
        assert "unsafe-eval" not in policy, path


async def test_remote_origins_are_confined_to_the_documentation_policy(
    frontend_client: httpx.AsyncClient,
) -> None:
    for path in (
        "/",
        "/assets/index-abc123.js",
        "/api/v1/pipelines",
        "/api/openapi.json",
        "/api/docs-near",
    ):
        response = await frontend_client.get(path)
        policy = response.headers["content-security-policy"]
        assert policy in (SHELL_CSP, DENY_ALL_CSP), path
        assert "https://" not in policy, path
    for path in DOCUMENTATION_PATHS:
        response = await frontend_client.get(path)
        policy = response.headers["content-security-policy"]
        assert policy not in (SHELL_CSP, DENY_ALL_CSP), path


async def test_documentation_policy_is_the_exact_accepted_exception(
    frontend_client: httpx.AsyncClient,
) -> None:
    """The docs page needs CDN swagger assets; nothing else may widen.

    The accepted exception is pinned to its exact literal: a second
    remote origin or any other relaxation of the documentation policy
    fails here exactly like it would for the shell policy.
    """
    response = await frontend_client.get("/api/docs")
    policy = response.headers["content-security-policy"]
    assert policy == DOCUMENTATION_CSP.decode("ascii")
    assert "default-src 'none'" in policy
    assert "object-src 'none'" in policy
    assert "frame-ancestors 'none'" in policy
    assert "connect-src 'self'" in policy
    assert "form-action 'none'" in policy
    assert "unsafe-eval" not in policy
    assert "*" not in policy


def _concatenated_literal(source: str, constant_name: str) -> str:
    """Evaluate the string-concatenation value of a frontend CSP constant.

    The browser lane keeps the shipped policy in JavaScript string
    fragments joined with ``+``; this reads the fragments the way the
    runtime would, so any edit to any fragment fails here instead of
    silently detaching the mirror policy from the production one.
    """
    match = re.search(rf"const {constant_name}\s*=(.*?);\s*\n", source, re.DOTALL)
    assert match is not None, constant_name
    fragments = re.findall(r'"([^"]*)"', match.group(1))
    assert fragments, constant_name
    return "".join(fragments)


def test_cross_language_policy_literals_cannot_silently_drift() -> None:
    """Every browser-lane copy of the policy must equal the shipped Python policy.

    ``security_headers.py`` is the production source of the policy.  The
    static server (``static-server.mjs``) mirrors it so the whole negative
    browser lane runs under the shipped policy, and the two Playwright
    specs pin the wire headers they assert against.  Without this check
    the mirror cluster could drift from the production policy while every
    test stays green.
    """
    assert FRONTEND_CSP_BYTES.decode("ascii") == SHELL_CSP
    assert DENY_ALL_CSP_BYTES.decode("ascii") == DENY_ALL_CSP

    static_server = (_FRONTEND_E2E / "static-server.mjs").read_text(encoding="utf-8")
    csp_spec = (_FRONTEND_E2E / "csp-policy.spec.ts").read_text(encoding="utf-8")
    demo_spec = (_FRONTEND_E2E / "demo.spec.ts").read_text(encoding="utf-8")

    assert _concatenated_literal(static_server, "shellCsp") == SHELL_CSP
    assert _concatenated_literal(static_server, "denyAllCsp") == DENY_ALL_CSP
    assert _concatenated_literal(csp_spec, "SHELL_CSP") == SHELL_CSP
    assert _concatenated_literal(csp_spec, "DENY_ALL_CSP") == DENY_ALL_CSP
    assert _concatenated_literal(demo_spec, "PRODUCTION_SHELL_CSP") == SHELL_CSP
