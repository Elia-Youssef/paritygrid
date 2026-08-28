"""Bounded request handling: body size, JSON depth, timeout, concurrency.

All limits are enforced before the request reaches route parsing, connector
work, database work, or artifact access.  Rejected requests receive the same
versioned Problem Details document the route layer produces.
"""

import asyncio
from dataclasses import dataclass

import anyio
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from paritygrid.api.errors.handlers import problem_response
from paritygrid.api.errors.problems import ProblemError
from paritygrid.api.json_bounds import BoundedJsonError, JsonBounds, decode_bounded_json

_BODY_METHODS = frozenset({"POST", "PUT", "PATCH"})
_JSON_BODY_COMMANDS = frozenset(
    {
        ("POST", "/api/v1/connectors"),
        ("POST", "/api/v1/pipelines"),
        ("POST", "/api/v1/runs"),
    }
)
_MIN_BODY_BYTES = 1
_MAX_BODY_BYTES_CEILING = 64 * 1024 * 1024
_MIN_CONCURRENCY = 1
_MAX_CONCURRENCY_CEILING = 10_000
_MIN_TIMEOUT_SECONDS = 0.1
_MAX_TIMEOUT_SECONDS = 300.0


@dataclass(frozen=True, slots=True)
class RequestLimitSettings:
    """Explicit request handling limits captured at composition time."""

    max_body_bytes: int = 1_048_576
    max_json_depth: int = 64
    request_timeout_seconds: float = 30.0
    max_concurrent_requests: int = 64

    def __post_init__(self) -> None:
        if (
            type(self.max_body_bytes) is not int
            or not _MIN_BODY_BYTES <= (self.max_body_bytes) <= _MAX_BODY_BYTES_CEILING
        ):
            raise ValueError("request body limit is outside the supported range")
        if type(self.max_json_depth) is not int or not 1 <= self.max_json_depth <= 512:
            raise ValueError("json depth limit is outside the supported range")
        if (
            type(self.request_timeout_seconds) is not float
            or not (_MIN_TIMEOUT_SECONDS) <= self.request_timeout_seconds <= _MAX_TIMEOUT_SECONDS
        ):
            raise ValueError("request timeout is outside the supported range")
        if (
            type(self.max_concurrent_requests) is not int
            or not _MIN_CONCURRENCY <= (self.max_concurrent_requests) <= _MAX_CONCURRENCY_CEILING
        ):
            raise ValueError("request concurrency limit is outside the supported range")

    @property
    def json_bounds(self) -> JsonBounds:
        return JsonBounds(max_body_bytes=self.max_body_bytes, max_depth=self.max_json_depth)


class ConcurrencyGate:
    """Bounded in-flight request counter for one event loop.

    The middleware runs on the serving event loop, so a plain counter admits
    or rejects immediately without queueing and without cross-loop state.
    """

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._active = 0

    def try_acquire(self) -> bool:
        if self._active >= self._limit:
            return False
        self._active += 1
        return True

    def release(self) -> None:
        if self._active > 0:
            self._active -= 1

    @property
    def active(self) -> int:
        return self._active


class RequestLimitsMiddleware:
    """Enforce concurrency, size, media-type, and timeout bounds per request."""

    def __init__(self, app: ASGIApp, *, settings: RequestLimitSettings) -> None:
        self.app = app
        self._settings = settings
        self._gate = ConcurrencyGate(settings.max_concurrent_requests)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = scope.get("path", "")
        # Liveness must stay answerable while the concurrency gate is
        # saturated; it owns no expensive resources and remains time-bounded.
        liveness = isinstance(path, str) and path == "/healthz"
        if liveness:
            await self._dispatch(scope, receive, send, owns_concurrency_permit=False)
            return
        if not self._gate.try_acquire():
            await _send_problem(
                scope,
                receive,
                send,
                ProblemError(
                    type_slug="too-many-requests",
                    title="Too many concurrent requests",
                    status=429,
                    detail="the configured concurrent request limit is saturated",
                    code="too_many_requests",
                ),
            )
            return
        release_here = True
        try:
            release_here = await self._dispatch(scope, receive, send, owns_concurrency_permit=True)
        finally:
            if release_here:
                self._gate.release()

    async def _dispatch(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
        *,
        owns_concurrency_permit: bool,
    ) -> bool:
        """Return whether the caller still owns the concurrency permit.

        A synchronous FastAPI endpoint is run in a non-abandoning worker
        thread.  Cancelling the ASGI task therefore waits for that worker and
        cannot enforce an HTTP deadline.  We first bound body acquisition,
        then use the remaining time as an HTTP *waiter*.  When it expires, the
        command continues under its own durable idempotency ownership while
        this middleware suppresses its late response and releases the permit
        only after that work has actually finished.
        """
        deadline = anyio.current_time() + self._settings.request_timeout_seconds
        try:
            # Body acquisition must be bounded before a handler is launched:
            # a slow client must neither create work nor retain a permit.
            with anyio.fail_after(self._settings.request_timeout_seconds):
                wrapped_receive = await self._prepare_dispatch(scope, receive, send)
        except TimeoutError:
            await _send_timeout(scope, receive, send)
            return True

        if wrapped_receive is None:
            return True
        remaining = deadline - anyio.current_time()
        if remaining <= 0:
            await _send_timeout(scope, receive, send)
            return True

        response_started = False
        discard_late_response = False

        async def tracking_send(message: Message) -> None:
            nonlocal response_started
            if discard_late_response:
                return
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        async def run_handler() -> None:
            await self.app(scope, wrapped_receive, tracking_send)

        task: asyncio.Task[None] = asyncio.create_task(
            run_handler(), name="paritygrid-request-handler"
        )
        done: set[asyncio.Task[None]]
        _pending: set[asyncio.Task[None]]
        done, _pending = await asyncio.wait({task}, timeout=remaining)
        if task in done:
            await task
            return True

        if response_started:
            # HTTP headers are irrevocable.  Keep the application task alive
            # and let its response complete rather than attempting a second
            # response.  This branch is intentionally limited to streaming
            # responses; command endpoints do not begin a response before
            # their durable work has an outcome.
            await task
            return True

        discard_late_response = True
        task.add_done_callback(
            lambda completed: self._finish_background_request(
                completed, release_concurrency_permit=owns_concurrency_permit
            )
        )
        await _send_timeout(scope, receive, send)
        return False

    def _finish_background_request(
        self, task: asyncio.Task[None], *, release_concurrency_permit: bool
    ) -> None:
        """Consume detached task failures and, when held, return its permit."""
        try:
            task.result()
        except asyncio.CancelledError, Exception:
            # The normal exception middleware has already rendered failures
            # that occur before timeout.  A late response cannot be sent after
            # the timeout document, but its durable command outcome remains
            # recoverable through the idempotency record.
            pass
        finally:
            if release_concurrency_permit:
                self._gate.release()

    async def _prepare_dispatch(self, scope: Scope, receive: Receive, send: Send) -> Receive | None:
        method = scope.get("method", "")
        headers = _header_map(scope)
        if isinstance(method, str) and method in _BODY_METHODS:
            declared = headers.get("content-length")
            if declared is not None and _declared_length(declared) > (
                self._settings.max_body_bytes
            ):
                await _send_problem(scope, receive, send, _body_too_large())
                return None
            content_type = headers.get("content-type", "")
            media_type = content_type.split(";", maxsplit=1)[0].strip().casefold()
            requires_json = _requires_json_content_type(method, scope.get("path", ""))
            if (requires_json and not content_type) or (
                content_type and media_type != "application/json"
            ):
                await _send_problem(
                    scope,
                    receive,
                    send,
                    ProblemError(
                        type_slug="unsupported-media-type",
                        title="Media type is not supported",
                        status=415,
                        detail="JSON commands require the application/json media type",
                        code="unsupported_media_type",
                    ),
                )
                return None
            bounded = await self._read_body(receive, self._settings.max_body_bytes)
            if isinstance(bounded, ProblemError):
                await _send_problem(scope, receive, send, bounded)
                return None
            raw, wrapped_receive = bounded
            if raw and _requires_empty_body(method, scope.get("path", "")):
                await _send_problem(
                    scope,
                    receive,
                    send,
                    ProblemError(
                        type_slug="invalid-input",
                        title="Request input is invalid",
                        status=400,
                        detail="this command does not accept a request body",
                        code="request_body_not_allowed",
                    ),
                )
                return None
            if raw and not content_type:
                await _send_problem(
                    scope,
                    receive,
                    send,
                    ProblemError(
                        type_slug="unsupported-media-type",
                        title="Media type is not supported",
                        status=415,
                        detail="JSON commands require the application/json media type",
                        code="unsupported_media_type",
                    ),
                )
                return
            if raw:
                try:
                    decode_bounded_json(raw, bounds=self._settings.json_bounds)
                except BoundedJsonError as error:
                    await _send_problem(
                        scope,
                        receive,
                        send,
                        ProblemError(
                            type_slug="invalid-input",
                            title="Request input is invalid",
                            status=400,
                            detail=str(error),
                            code="malformed_json_body",
                        ),
                    )
                    return None
        else:
            wrapped_receive = receive

        return wrapped_receive

    async def _read_body(
        self, receive: Receive, maximum: int
    ) -> tuple[bytes, Receive] | ProblemError:
        chunks: list[bytes] = []
        total = 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return ProblemError(
                    type_slug="invalid-input",
                    title="Request input is invalid",
                    status=400,
                    detail="the request body ended before it was complete",
                    code="incomplete_request_body",
                )
            if message["type"] != "http.request":
                continue
            body = message.get("body", b"")
            total += len(body)
            if total > maximum:
                return _body_too_large()
            chunks.append(body)
            if not message.get("more_body", False):
                break
        raw = b"".join(chunks)
        replay_state = {"served": False}

        async def bounded_replay() -> Message:
            if replay_state["served"]:
                return {"type": "http.disconnect"}
            replay_state["served"] = True
            return {"type": "http.request", "body": raw, "more_body": False}

        return raw, bounded_replay


def _header_map(scope: Scope) -> dict[str, str]:
    headers: dict[str, str] = {}
    for name, raw in scope.get("headers", []):
        headers[name.decode("latin-1").lower()] = raw.decode("latin-1")
    return headers


def _requires_json_content_type(method: object, path: object) -> bool:
    if type(method) is not str or type(path) is not str:
        return False
    if (method, path) in _JSON_BODY_COMMANDS:
        return True
    for prefix, suffix in (
        ("/api/v1/pipelines/", "/versions"),
        ("/api/v1/runs/", "/repair-plans"),
        ("/api/v1/repair-plans/", "/approve"),
    ):
        if method != "POST" or not path.startswith(prefix) or not path.endswith(suffix):
            continue
        identifier = path[len(prefix) : -len(suffix)]
        if identifier and "/" not in identifier:
            return True
    return False


def _requires_empty_body(method: object, path: object) -> bool:
    """Return whether a command has an explicitly bodyless wire contract."""
    if type(method) is not str or type(path) is not str or method != "POST":
        return False
    prefix = "/api/v1/repair-plans/"
    suffix = "/apply"
    if not path.startswith(prefix) or not path.endswith(suffix):
        return False
    plan_id = path[len(prefix) : -len(suffix)]
    return bool(plan_id) and "/" not in plan_id


def _declared_length(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        return 0


def _body_too_large() -> ProblemError:
    return ProblemError(
        type_slug="request-too-large",
        title="Request body exceeds the configured limit",
        status=413,
        detail="the request body exceeds the configured maximum size",
        code="request_body_too_large",
    )


async def _send_timeout(scope: Scope, receive: Receive, send: Send) -> None:
    await _send_problem(
        scope,
        receive,
        send,
        ProblemError(
            type_slug="request-timeout",
            title="Request exceeded its time budget",
            status=503,
            detail="the request exceeded the configured time budget",
            code="request_timeout",
        ),
    )


async def _send_problem(scope: Scope, receive: Receive, send: Send, problem: ProblemError) -> None:
    from paritygrid.api.correlation import correlation_from_scope

    instance = scope.get("path", "")
    response = problem_response(
        problem,
        instance=instance if isinstance(instance, str) else "",
        correlation_id=correlation_from_scope(scope),
    )
    await response(scope, receive, send)
