"""Logical source semantics shared by the async and blocking simulators.

Both transports route requests through :class:`SourceBehavior` so cursor paging
and legacy page paging expose the same deterministic dataset, bounds, and
scripted failure semantics; only the transport differs.
"""

import base64
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from paritygrid.demo.datasets import SyntheticDataset, WireObject, WireValue
from paritygrid.demo.failures import (
    AppliedFailure,
    FailureScript,
    ScriptedFailure,
    ScriptedFailureKind,
)
from paritygrid.demo.simulators.http_wire import (
    HttpRequest,
    PlannedResponse,
    ResponseAction,
    error_response,
    json_response,
    request_path_only,
)

DEFAULT_MAX_PAGE_SIZE = 200
_MAX_SEQUENCE = 2_147_483_647
_CURSOR_PREFIX = "pg1"

PageRenderer = Callable[[list[WireObject], int], PlannedResponse]


class SourceError(ValueError):
    """Raised when source simulator settings are invalid."""


class SourcePagingStyle(StrEnum):
    """Cursor paging for the async API, page numbers for the legacy API."""

    CURSOR = "cursor"
    PAGES = "pages"


@dataclass(frozen=True, slots=True)
class SourceSettings:
    """Bounded settings for one source simulator instance."""

    service_name: str = "source"
    paging_style: SourcePagingStyle = SourcePagingStyle.CURSOR
    max_page_size: int = DEFAULT_MAX_PAGE_SIZE
    request_latency_microseconds: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "service_name", _validated_service_name(self.service_name))
        object.__setattr__(self, "paging_style", _validated_paging_style(self.paging_style))
        object.__setattr__(self, "max_page_size", _validated_max_page_size(self.max_page_size))
        object.__setattr__(
            self,
            "request_latency_microseconds",
            _validated_latency(self.request_latency_microseconds),
        )


class SourceBehavior:
    """Deterministic dataset paging and scripted failure application."""

    def __init__(
        self,
        dataset: SyntheticDataset,
        script: FailureScript,
        settings: SourceSettings,
    ) -> None:
        self._rows = dataset.rows
        self._script = script
        self._settings = settings
        self._request_count = 0
        self._applied: list[AppliedFailure] = []

    @property
    def settings(self) -> SourceSettings:
        """Return the frozen settings for this behavior."""
        return self._settings

    @property
    def service_name(self) -> str:
        """Return the service name used in health documents."""
        return self._settings.service_name

    def request_count(self) -> int:
        """Return how many data requests have been counted."""
        return self._request_count

    def applied_failures(self) -> tuple[AppliedFailure, ...]:
        """Return every applied scripted failure in application order."""
        return tuple(self._applied)

    def handle(self, request: HttpRequest) -> PlannedResponse:
        """Route one request and apply any scripted failure."""
        path = request_path_only(request.path)
        if path == "/healthz":
            if request.method != "GET":
                return _method_not_allowed("GET")
            return json_response(200, {"service": self._settings.service_name, "status": "ok"})
        if path == "/v1/inventory":
            if self._settings.paging_style is not SourcePagingStyle.CURSOR:
                return error_response(404, "not_found", "This source does not use cursor paging.")
            if request.method != "GET":
                return _method_not_allowed("GET")
            return self._handle_cursor_page(request)
        if path.startswith("/v1/inventory/pages/"):
            if self._settings.paging_style is not SourcePagingStyle.PAGES:
                return error_response(404, "not_found", "This source does not use page paging.")
            if request.method != "GET":
                return _method_not_allowed("GET")
            return self._handle_numbered_page(request, path)
        return error_response(404, "not_found", "The requested resource does not exist.")

    def _handle_cursor_page(self, request: HttpRequest) -> PlannedResponse:
        cursor_text = request.query.get("cursor", "")
        position = 0 if cursor_text == "" else self._decode_cursor(cursor_text)
        if position is None or position > len(self._rows):
            return error_response(400, "invalid_cursor", "The cursor is not valid for this source.")
        limit = self._parse_limit(request.query.get("limit", str(self._settings.max_page_size)))
        if limit is None:
            return error_response(400, "invalid_limit", "The limit must be a positive integer.")
        clamped = min(limit, self._settings.max_page_size)
        return self._scripted_page(
            position=position,
            limit=clamped,
            render=lambda records, next_position: json_response(
                200,
                {
                    "next_cursor": (
                        ""
                        if next_position >= len(self._rows)
                        else self._encode_cursor(next_position)
                    ),
                    "page_size": clamped,
                    "position": position,
                    "records": records,
                },
            ),
        )

    def _handle_numbered_page(self, request: HttpRequest, path: str) -> PlannedResponse:
        page_text = path.rsplit("/", 1)[1]
        if not page_text.isdigit():
            return error_response(
                400, "invalid_page", "The page number must be a positive integer."
            )
        page = int(page_text)
        if page == 0:
            return error_response(400, "invalid_page", "Pages are numbered from one.")
        page_size = self._parse_limit(
            request.query.get("page_size", str(self._settings.max_page_size))
        )
        if page_size is None:
            return error_response(
                400, "invalid_page_size", "The page size must be a positive integer."
            )
        page_size = min(page_size, self._settings.max_page_size)
        total_pages = (len(self._rows) + page_size - 1) // page_size if self._rows else 0
        if page > total_pages:
            return error_response(
                404,
                "page_out_of_range",
                "The requested page is beyond the last page.",
            )
        position = (page - 1) * page_size
        return self._scripted_page(
            position=position,
            limit=page_size,
            render=lambda records, _next_position: json_response(
                200,
                {
                    "page": page,
                    "page_size": page_size,
                    "records": records,
                    "total_pages": total_pages,
                },
            ),
        )

    def _scripted_page(
        self,
        *,
        position: int,
        limit: int,
        render: PageRenderer,
    ) -> PlannedResponse:
        self._request_count += 1
        failure = self._script.failure_for(self._request_count)
        latency = self._settings.request_latency_microseconds
        if failure is not None:
            self._applied.append(AppliedFailure(sequence=failure.sequence, kind=failure.kind))
            planned = self._failure_response(failure, position, limit, render)
            if planned is not None:
                return PlannedResponse(
                    status=planned.status,
                    body=planned.body,
                    headers=planned.headers,
                    delay_microseconds=max(latency, planned.delay_microseconds),
                    action=planned.action,
                    hold_cap_microseconds=planned.hold_cap_microseconds,
                    partial_bytes=planned.partial_bytes,
                )
        records, next_position = self._slice(position, limit)
        page = render(records, next_position)
        return PlannedResponse(
            status=page.status,
            body=page.body,
            headers=page.headers,
            delay_microseconds=latency,
        )

    def _failure_response(
        self,
        failure: ScriptedFailure,
        position: int,
        limit: int,
        render: PageRenderer,
    ) -> PlannedResponse | None:
        """Return the response plan for a scripted failure, if it has one."""
        if failure.kind is ScriptedFailureKind.RATE_LIMIT:
            return PlannedResponse(
                status=429,
                body=_error_body("rate_limited", "The request exceeded the source rate limit."),
                headers=(
                    ("Content-Type", "application/json"),
                    ("Retry-After", str(failure.retry_after_seconds)),
                ),
            )
        if failure.kind is ScriptedFailureKind.TRANSIENT_ERROR:
            return PlannedResponse(
                status=503,
                body=_error_body("transient", "The source is temporarily unavailable."),
                headers=(("Content-Type", "application/json"),),
            )
        if failure.kind is ScriptedFailureKind.MALFORMED_RESPONSE:
            return PlannedResponse(
                status=200,
                body=b'{"records": [',
                headers=(("Content-Type", "application/json"),),
            )
        if failure.kind is ScriptedFailureKind.DUPLICATE_RECORDS:
            records, next_position = self._slice(position, limit, duplicate=True)
            return render(records, next_position)
        if failure.kind is ScriptedFailureKind.CONNECTION_LOSS:
            encoded = PlannedResponse(
                status=200,
                body=_error_body("connection_lost", "The connection was lost mid-response."),
                headers=(("Content-Type", "application/json"),),
            ).encoded()
            return PlannedResponse(
                status=200,
                body=encoded,
                action=ResponseAction.CLOSE_PARTIAL,
                partial_bytes=failure.partial_bytes if failure.partial_bytes is not None else 1,
            )
        if failure.kind is ScriptedFailureKind.TIMEOUT:
            records, next_position = self._slice(position, limit)
            page = render(records, next_position)
            return PlannedResponse(
                status=page.status,
                body=page.body,
                headers=page.headers,
                delay_microseconds=failure.delay_microseconds or 0,
            )
        if failure.kind is ScriptedFailureKind.HANG:
            return PlannedResponse(
                status=200,
                body=b"",
                action=ResponseAction.HOLD_UNTIL_DISCONNECT,
                hold_cap_microseconds=failure.delay_microseconds or 0,
            )
        return None

    def _slice(
        self,
        position: int,
        limit: int,
        *,
        duplicate: bool = False,
    ) -> tuple[list[WireObject], int]:
        window = self._rows[position : position + limit]
        records: list[WireObject] = [dict(row.payload) for row in window]
        if duplicate:
            duplicated: list[WireObject] = []
            for record in records:
                duplicated.append(record)
                duplicated.append(dict(record))
            # The duplicate-record anomaly must retain its repeated-record
            # shape without exceeding the caller's requested page bound.
            records = duplicated[:limit]
        return records, position + len(window)

    def _encode_cursor(self, position: int) -> str:
        payload = f"{_CURSOR_PREFIX}:{position:010d}".encode("ascii")
        return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")

    def _decode_cursor(self, cursor_text: str) -> int | None:
        if not cursor_text or len(cursor_text) > 64 or not cursor_text.isascii():
            return None
        padded = cursor_text + "=" * (-len(cursor_text) % 4)
        try:
            decoded = base64.urlsafe_b64decode(padded.encode("ascii")).decode("ascii")
        except ValueError, UnicodeDecodeError:
            return None
        prefix, separator, digits = decoded.partition(":")
        if prefix != _CURSOR_PREFIX or not separator or not digits.isdigit():
            return None
        position = int(digits)
        if not 0 <= position <= _MAX_SEQUENCE:
            return None
        return position

    def _parse_limit(self, limit_text: str) -> int | None:
        if not limit_text.isdigit():
            return None
        value = int(limit_text)
        if not 1 <= value <= _MAX_SEQUENCE:
            return None
        return value


def _method_not_allowed(allow: str) -> PlannedResponse:
    return PlannedResponse(
        status=405,
        body=_error_body("method_not_allowed", "The method is not supported for this resource."),
        headers=(("Allow", allow), ("Content-Type", "application/json")),
    )


def _error_body(code: str, message: str) -> bytes:
    document = {"error": {"code": code, "message": message}}
    return json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode(
        "ascii"
    )


def decode_records_payload(body: bytes) -> list[Mapping[str, WireValue]]:
    """Parse the records list from a successful page response body."""
    document = cast("dict[str, object]", json.loads(body.decode("ascii")))
    return cast("list[Mapping[str, WireValue]]", document["records"])


def _validated_service_name(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise SourceError("service name must be non-empty text")
    return value


def _validated_paging_style(value: object) -> SourcePagingStyle:
    if not isinstance(value, SourcePagingStyle):
        raise SourceError("paging style must be a SourcePagingStyle")
    return value


def _validated_max_page_size(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SourceError("max page size must be an integer")
    if not 1 <= value <= DEFAULT_MAX_PAGE_SIZE:
        raise SourceError(f"max page size must be between 1 and {DEFAULT_MAX_PAGE_SIZE}")
    return value


def _validated_latency(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SourceError("request latency must be integer microseconds")
    if not 0 <= value <= 60_000_000:
        raise SourceError("request latency must be between 0 and 60 seconds")
    return value
