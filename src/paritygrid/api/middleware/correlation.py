"""Correlation identity middleware for the HTTP boundary."""

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from paritygrid.api.correlation import (
    CORRELATION_HEADER,
    generate_correlation_id,
    validate_correlation_id,
)
from paritygrid.api.errors.problems import ProblemError


class CorrelationIdMiddleware:
    """Validate or generate one correlation identity per request.

    The identity is installed on the request state before any inner
    middleware or route runs, echoed on every response, and required by the
    Problem Details renderer, so diagnostics and durable events share one
    request identity.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        supplied = _supplied_correlation(scope)
        if supplied is None:
            correlation_id = generate_correlation_id()
        else:
            try:
                correlation_id = validate_correlation_id(supplied)
            except ValueError as error:
                await _send_invalid_correlation(scope, receive, send, str(error))
                return
        state = scope.setdefault("state", {})
        state["correlation_id"] = correlation_id

        async def send_with_correlation(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append(
                    (
                        CORRELATION_HEADER.lower().encode("ascii"),
                        correlation_id.encode("ascii"),
                    )
                )
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_with_correlation)


def _supplied_correlation(scope: Scope) -> str | None:
    """Return the supplied header decoded permissively for validation.

    Latin-1 decoding never fails, and any non-portable byte fails the
    portable-ASCII validation instead of crashing the middleware.
    """
    for name, raw in scope.get("headers", []):
        if name == b"x-correlation-id":
            return raw.decode("latin-1")
    return None


async def _send_invalid_correlation(
    scope: Scope, receive: Receive, send: Send, detail: str
) -> None:
    from paritygrid.api.errors.handlers import problem_response

    # The supplied identity is unusable, so the rejection carries a freshly
    # generated one: every response still echoes exactly one identity.
    replacement = generate_correlation_id()
    problem = ProblemError(
        type_slug="invalid-correlation-id",
        title="Correlation identity is invalid",
        status=400,
        detail=detail,
        code="invalid_correlation_id",
    )
    instance = scope.get("path", "")
    response = problem_response(
        problem,
        instance=instance if isinstance(instance, str) else "",
        correlation_id=replacement,
    )
    response.headers.append(CORRELATION_HEADER, replacement)
    await response(scope, receive, send)
