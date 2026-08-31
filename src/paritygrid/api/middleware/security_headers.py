"""Security response headers and MIME-sniffing prevention."""

from starlette.types import ASGIApp, Message, Receive, Scope, Send

_API_SECURITY_HEADERS: tuple[tuple[bytes, bytes], ...] = (
    (b"x-content-type-options", b"nosniff"),
    (b"x-frame-options", b"DENY"),
    (b"referrer-policy", b"no-referrer"),
    (b"cross-origin-opener-policy", b"same-origin"),
)
_DENY_ALL_CSP = b"default-src 'none'; frame-ancestors 'none'"
# The packaged application shell loads only same-origin script, style,
# image, font, and connect targets; everything else stays denied.
_FRONTEND_CSP = (
    b"default-src 'none'; script-src 'self'; style-src 'self'; "
    b"img-src 'self' data:; font-src 'self'; connect-src 'self'; "
    b"frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
)
# Swagger documentation is HTML+JS served from the CDN assets FastAPI
# embeds; it gets the narrowest policy that keeps the local docs usable.
_DOCUMENTATION_CSP = (
    b"default-src 'none'; base-uri 'none'; form-action 'none'; "
    b"frame-ancestors 'none'; object-src 'none'; connect-src 'self'; "
    b"script-src https://cdn.jsdelivr.net 'unsafe-inline'; "
    b"style-src https://cdn.jsdelivr.net; "
    b"img-src 'self' data: https://fastapi.tiangolo.com"
)
_DOCUMENTATION_PATHS = frozenset({"/api/docs", "/api/docs/oauth2-redirect"})
_NO_STORE = b"no-store"


class SecurityHeadersMiddleware:
    """Apply the required security headers to every HTTP response.

    API data responses are additionally marked ``Cache-Control: no-store``.
    Frontend HTML documents carry the application shell content security
    policy instead of the deny-all API policy.  CORS stays disabled: no
    origin reflection or allow-origin header is ever emitted, matching the
    loopback-only packaged application posture.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = scope.get("path", "")
        api_response = isinstance(path, str) and (
            path.startswith("/api/") or path in {"/readyz", "/healthz"}
        )

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                present = {name.lower() for name, _ in headers}
                for name, value in _API_SECURITY_HEADERS:
                    if name != b"content-security-policy" and name not in present:
                        headers.append((name, value))
                if b"content-security-policy" not in present:
                    if api_response:
                        content_security_policy = (
                            _DOCUMENTATION_CSP if path in _DOCUMENTATION_PATHS else _DENY_ALL_CSP
                        )
                        headers.append((b"content-security-policy", content_security_policy))
                    else:
                        _apply_frontend_policy(message, headers)
                if api_response and b"cache-control" not in present:
                    headers.append((b"cache-control", _NO_STORE))
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_with_headers)


def _apply_frontend_policy(message: Message, headers: list[tuple[bytes, bytes]]) -> None:
    """Apply the shell policy to HTML, deny-all to every other document."""
    content_type = b""
    for name, value in headers:
        if name.lower() == b"content-type":
            content_type = value
            break
    if content_type.startswith(b"text/html"):
        headers.append((b"content-security-policy", _FRONTEND_CSP))
    else:
        headers.append((b"content-security-policy", _DENY_ALL_CSP))
