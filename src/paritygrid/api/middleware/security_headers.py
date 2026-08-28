"""Security response headers and MIME-sniffing prevention."""

from starlette.types import ASGIApp, Message, Receive, Scope, Send

_SECURITY_HEADERS: tuple[tuple[bytes, bytes], ...] = (
    (b"x-content-type-options", b"nosniff"),
    (b"x-frame-options", b"DENY"),
    (b"referrer-policy", b"no-referrer"),
    (b"content-security-policy", b"default-src 'none'; frame-ancestors 'none'"),
    (b"cross-origin-opener-policy", b"same-origin"),
)
_NO_STORE = b"no-store"


class SecurityHeadersMiddleware:
    """Apply the required security headers to every HTTP response.

    API data responses are additionally marked ``Cache-Control: no-store``.
    CORS stays disabled: no origin reflection or allow-origin header is ever
    emitted, matching the loopback-only packaged application posture.
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
                for name, value in _SECURITY_HEADERS:
                    if name not in present:
                        headers.append((name, value))
                if api_response and b"cache-control" not in present:
                    headers.append((b"cache-control", _NO_STORE))
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_with_headers)
