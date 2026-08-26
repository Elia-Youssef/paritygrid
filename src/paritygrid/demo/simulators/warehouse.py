"""Simulated warehouse target with observable versions and idempotent effects.

The warehouse is a loopback HTTP service over in-memory state. Every mutating
request carries an ``Idempotency-Key`` header; replaying a key with the same
request body returns the originally recorded outcome without a second effect,
while replaying a key with a different body is a conflict. Applied effects
bump one observable global ``target_version`` and a per-SKU ``record_version``,
and a deterministic content fingerprint summarizes the logical state so later
repair verification can observe exact convergence.
"""

import base64
import json
import re
from dataclasses import dataclass
from hashlib import sha256
from typing import cast

from paritygrid.demo.failures import (
    AppliedFailure,
    FailureScript,
    ScriptedFailure,
    ScriptedFailureKind,
    require_transport_script,
)
from paritygrid.demo.simulators.async_server import AsyncHttpService
from paritygrid.demo.simulators.http_wire import (
    HttpRequest,
    PlannedResponse,
    ResponseAction,
    error_response,
    json_response,
    request_path_only,
)

WAREHOUSE_SERVICE_NAME = "warehouse"
WAREHOUSE_CAPACITY = 10_000
WAREHOUSE_MAX_LIST_PAGE = 500
WAREHOUSE_FINGERPRINT_VERSION = 1

_EMPTY_FINGERPRINT = sha256(b"paritygrid-warehouse-empty-v1").hexdigest()
_SKU_PATTERN = re.compile(r"[A-Z0-9]+(?:-[A-Z0-9]+)*", flags=re.ASCII)
_MAX_IDEMPOTENCY_ENTRIES = 10_000
_IDEMPOTENCY_KEY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", flags=re.ASCII)
_CURSOR_PREFIX = "wh1"


class WarehouseError(ValueError):
    """Raised when warehouse simulator settings are invalid."""


@dataclass(frozen=True, slots=True)
class WarehouseSettings:
    """Bounded settings for one warehouse simulator instance."""

    capacity: int = WAREHOUSE_CAPACITY
    max_list_page_size: int = WAREHOUSE_MAX_LIST_PAGE

    def __post_init__(self) -> None:
        object.__setattr__(self, "capacity", _validated_capacity(self.capacity))
        object.__setattr__(
            self, "max_list_page_size", _validated_list_page_size(self.max_list_page_size)
        )


@dataclass(frozen=True, slots=True)
class _IdempotencyEntry:
    request_fingerprint: str
    outcome: str
    target_version: int
    record_version: int
    status: int


@dataclass(frozen=True, slots=True)
class WarehouseEffect:
    """One observed effect decision for a mutating request."""

    outcome: str
    target_version: int
    record_version: int
    replayed: bool


class WarehouseBehavior:
    """Deterministic warehouse routing, versions, and idempotent effects."""

    def __init__(
        self,
        script: FailureScript,
        settings: WarehouseSettings | None = None,
    ) -> None:
        self._script = require_transport_script(script, subject="warehouse")
        self._settings = settings if settings is not None else WarehouseSettings()
        self._records: dict[str, tuple[dict[str, object], int]] = {}
        self._target_version = 0
        self._idempotency: dict[str, _IdempotencyEntry] = {}
        self._request_count = 0
        self._applied: list[AppliedFailure] = []

    @property
    def settings(self) -> WarehouseSettings:
        """Return the frozen settings for this behavior."""
        return self._settings

    @property
    def target_version(self) -> int:
        """Return the observable global version of the warehouse."""
        return self._target_version

    @property
    def record_count(self) -> int:
        """Return the number of stored records."""
        return len(self._records)

    def request_count(self) -> int:
        """Return how many mutating requests have been counted."""
        return self._request_count

    def applied_failures(self) -> tuple[AppliedFailure, ...]:
        """Return every applied scripted failure in application order."""
        return tuple(self._applied)

    def content_fingerprint(self) -> str:
        """Return the deterministic fingerprint of the logical content."""
        if not self._records:
            return _EMPTY_FINGERPRINT
        stream = bytearray()
        for sku in sorted(self._records):
            payload, record_version = self._records[sku]
            canonical = json.dumps(
                payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True
            ).encode("ascii")
            stream += _frame(f"wh-v{WAREHOUSE_FINGERPRINT_VERSION}".encode("ascii"))
            stream += _frame(sku.encode("ascii"))
            stream += _frame(canonical)
            stream += record_version.to_bytes(8, byteorder="big")
        return sha256(stream).hexdigest()

    def state_snapshot(self) -> dict[str, object]:
        """Return an inspectable copy of the current logical state."""
        return {
            "record_count": len(self._records),
            "records": {
                sku: {"payload": dict(payload), "record_version": version}
                for sku, (payload, version) in self._records.items()
            },
            "target_version": self._target_version,
        }

    def handle(self, request: HttpRequest) -> PlannedResponse:
        """Route one warehouse request."""
        path = request_path_only(request.path)
        if path == "/healthz":
            if request.method != "GET":
                return _method_not_allowed("GET")
            return json_response(200, {"service": WAREHOUSE_SERVICE_NAME, "status": "ok"})
        if path == "/v1/state":
            if request.method != "GET":
                return _method_not_allowed("GET")
            return json_response(
                200,
                {
                    "capacity": self._settings.capacity,
                    "content_fingerprint": self.content_fingerprint(),
                    "record_count": len(self._records),
                    "target_version": self._target_version,
                },
            )
        if path == "/v1/records":
            if request.method != "GET":
                return _method_not_allowed("GET")
            return self._handle_listing(request)
        if path.startswith("/v1/records/"):
            sku = path.rsplit("/", 1)[1]
            if _SKU_PATTERN.fullmatch(sku) is None or not sku:
                return error_response(400, "invalid_sku", "The record key is not canonical.")
            if request.method == "GET":
                return self._handle_fetch(sku)
            if request.method == "PUT":
                return self._handle_upsert(sku, request)
            if request.method == "DELETE":
                return _method_not_allowed("GET, PUT")
            return _method_not_allowed("GET, PUT")
        return error_response(404, "not_found", "The requested resource does not exist.")

    def _handle_listing(self, request: HttpRequest) -> PlannedResponse:
        cursor_text = request.query.get("cursor", "")
        skus = sorted(self._records)
        position = 0 if cursor_text == "" else self._decode_cursor(cursor_text, len(skus))
        if position is None:
            return error_response(400, "invalid_cursor", "The cursor is not valid.")
        limit_text = request.query.get("limit", str(self._settings.max_list_page_size))
        if not limit_text.isdigit() or int(limit_text) < 1:
            return error_response(400, "invalid_limit", "The limit must be a positive integer.")
        limit = min(int(limit_text), self._settings.max_list_page_size)
        window = skus[position : position + limit]
        next_position = position + len(window)
        return json_response(
            200,
            {
                "next_cursor": ""
                if next_position >= len(skus)
                else self._encode_cursor(next_position),
                "records": [self._record_document(sku) for sku in window],
            },
        )

    def _handle_fetch(self, sku: str) -> PlannedResponse:
        if sku not in self._records:
            return error_response(404, "record_not_found", "The record does not exist.")
        return json_response(200, self._record_document(sku))

    def _handle_upsert(self, sku: str, request: HttpRequest) -> PlannedResponse:
        self._request_count += 1
        failure = self._script.failure_for(self._request_count)
        if failure is not None:
            self._applied.append(AppliedFailure(sequence=failure.sequence, kind=failure.kind))
            planned = self._pre_commit_transport_failure(failure)
            if planned is not None:
                return planned
        key = request.headers.get("idempotency-key")
        if key is None or _IDEMPOTENCY_KEY_PATTERN.fullmatch(key) is None:
            return error_response(
                400, "missing_idempotency_key", "A canonical Idempotency-Key header is required."
            )
        try:
            document_value = json.loads(request.body.decode("utf-8"))
        except UnicodeDecodeError, json.JSONDecodeError:
            return error_response(400, "invalid_body", "The request body must be a JSON object.")
        if not isinstance(document_value, dict):
            return error_response(400, "invalid_body", "The request body must be a JSON object.")
        payload = cast("dict[str, object]", document_value)
        if payload.get("sku") != sku:
            return error_response(
                400, "body_mismatch", "The body SKU must match the addressed record."
            )
        fingerprint = _request_fingerprint(
            request.method, request_path_only(request.path), request.body
        )
        stored = self._idempotency.get(key)
        if stored is not None:
            if stored.request_fingerprint != fingerprint:
                return error_response(
                    409,
                    "idempotency_conflict",
                    "The idempotency key was reused with a different request.",
                )
            response = _with_header(
                json_response(
                    stored.status,
                    {
                        "outcome": stored.outcome,
                        "record_version": stored.record_version,
                        "replayed": True,
                        "target_version": stored.target_version,
                    },
                ),
                "Idempotency-Replayed",
                "true",
            )
            return self._post_commit_transport_failure(failure, response)
        if sku not in self._records and len(self._records) >= self._settings.capacity:
            return error_response(
                507, "capacity_exceeded", "The warehouse record capacity is exhausted."
            )
        existing = self._records.get(sku)
        canonical = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        if (
            existing is not None
            and json.dumps(existing[0], ensure_ascii=True, separators=(",", ":"), sort_keys=True)
            == canonical
        ):
            entry = _IdempotencyEntry(
                request_fingerprint=fingerprint,
                outcome="unchanged",
                target_version=self._target_version,
                record_version=existing[1],
                status=200,
            )
            self._store_idempotency(key, entry)
            response = json_response(
                200,
                {
                    "outcome": entry.outcome,
                    "record_version": entry.record_version,
                    "replayed": False,
                    "target_version": entry.target_version,
                },
            )
            return self._post_commit_transport_failure(failure, response)
        self._target_version += 1
        record_version = 1 if existing is None else existing[1] + 1
        self._records[sku] = (payload, record_version)
        entry = _IdempotencyEntry(
            request_fingerprint=fingerprint,
            outcome="applied",
            target_version=self._target_version,
            record_version=record_version,
            status=200,
        )
        self._store_idempotency(key, entry)
        response = json_response(
            200,
            {
                "outcome": "applied",
                "record_version": record_version,
                "replayed": False,
                "target_version": self._target_version,
            },
        )
        return self._post_commit_transport_failure(failure, response)

    def _store_idempotency(self, key: str, entry: _IdempotencyEntry) -> None:
        if key not in self._idempotency and len(self._idempotency) >= _MAX_IDEMPOTENCY_ENTRIES:
            # The registry is bounded so simulator memory stays bounded; an
            # exhausted registry is surfaced explicitly rather than silently
            # weakening idempotency by eviction.
            raise WarehouseError("the idempotency registry is exhausted")
        self._idempotency[key] = entry

    def _record_document(self, sku: str) -> dict[str, object]:
        payload, record_version = self._records[sku]
        return {
            "payload": payload,
            "record_version": record_version,
            "sku": sku,
            "target_version": self._target_version,
        }

    def _pre_commit_transport_failure(self, failure: ScriptedFailure) -> PlannedResponse | None:
        """Return failures that prevent a mutation from reaching the target."""
        if failure.kind is ScriptedFailureKind.RATE_LIMIT:
            return PlannedResponse(
                status=429,
                body=_error_body("rate_limited", "The warehouse request exceeded its rate limit."),
                headers=(
                    ("Content-Type", "application/json"),
                    ("Retry-After", str(failure.retry_after_seconds)),
                ),
            )
        if failure.kind is ScriptedFailureKind.TRANSIENT_ERROR:
            return PlannedResponse(
                status=503,
                body=_error_body("transient", "The warehouse is temporarily unavailable."),
                headers=(("Content-Type", "application/json"),),
            )
        if failure.kind is ScriptedFailureKind.HANG:
            return PlannedResponse(
                status=200,
                body=b"",
                action=ResponseAction.HOLD_UNTIL_DISCONNECT,
                hold_cap_microseconds=failure.delay_microseconds or 0,
            )
        return None

    def _post_commit_transport_failure(
        self,
        failure: ScriptedFailure | None,
        response: PlannedResponse,
    ) -> PlannedResponse:
        """Drop or delay a committed response without repeating its effect.

        A write that times out or loses its connection after the request was
        accepted is ambiguous to the caller, but the idempotency registry must
        already contain the single logical outcome.  Replaying the same key
        therefore returns that recorded outcome rather than applying again.
        """
        if failure is None:
            return response
        if failure.kind is ScriptedFailureKind.TIMEOUT:
            return PlannedResponse(
                status=response.status,
                body=response.body,
                headers=response.headers,
                delay_microseconds=failure.delay_microseconds or 0,
                action=response.action,
                hold_cap_microseconds=response.hold_cap_microseconds,
                partial_bytes=response.partial_bytes,
                close_after_response=response.close_after_response,
            )
        if failure.kind is ScriptedFailureKind.CONNECTION_LOSS:
            return PlannedResponse(
                status=response.status,
                body=response.encoded(),
                action=ResponseAction.CLOSE_PARTIAL,
                partial_bytes=failure.partial_bytes if failure.partial_bytes is not None else 1,
            )
        return response

    def _encode_cursor(self, position: int) -> str:
        payload = f"{_CURSOR_PREFIX}:{position:010d}".encode("ascii")
        return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")

    def _decode_cursor(self, cursor_text: str, total: int) -> int | None:
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
        if not 0 <= position <= total:
            return None
        return position


class SimulatedWarehouse:
    """The loopback warehouse target served over real HTTP."""

    def __init__(
        self,
        script: FailureScript | None = None,
        *,
        settings: WarehouseSettings | None = None,
    ) -> None:
        self._behavior = WarehouseBehavior(
            script if script is not None else FailureScript.empty(),
            settings,
        )
        self._service = AsyncHttpService(
            service_name=WAREHOUSE_SERVICE_NAME,
            handler=self._behavior.handle,
        )

    @property
    def port(self) -> int:
        """Return the dynamically assigned loopback port."""
        return self._service.port

    @property
    def base_url(self) -> str:
        """Return the loopback base URL of this warehouse."""
        return self._service.base_url

    def is_serving(self) -> bool:
        """Report whether the listener still accepts connections."""
        return self._service.is_serving()

    @property
    def behavior(self) -> WarehouseBehavior:
        """Return the behavior for direct inspection in tests."""
        return self._behavior

    def request_count(self) -> int:
        """Return how many mutating requests have been counted."""
        return self._behavior.request_count()

    def applied_failures(self) -> tuple[AppliedFailure, ...]:
        """Return every applied scripted failure in application order."""
        return self._behavior.applied_failures()

    async def start(self) -> None:
        """Bind the dynamic loopback port and begin serving."""
        await self._service.start()

    async def aclose(self) -> None:
        """Stop serving and release the owned listener."""
        await self._service.aclose()


def _request_fingerprint(method: str, path: str, body: bytes) -> str:
    payload = b"\0".join((method.encode("ascii"), path.encode("ascii"), body))
    return sha256(b"paritygrid-warehouse-request-v1\0" + payload).hexdigest()


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


def _frame(value: bytes) -> bytes:
    return len(value).to_bytes(8, byteorder="big") + value


def _with_header(planned: PlannedResponse, name: str, value: str) -> PlannedResponse:
    return PlannedResponse(
        status=planned.status,
        body=planned.body,
        headers=(*planned.headers, (name, value)),
        delay_microseconds=planned.delay_microseconds,
        action=planned.action,
        hold_cap_microseconds=planned.hold_cap_microseconds,
        partial_bytes=planned.partial_bytes,
        close_after_response=planned.close_after_response,
    )


def _validated_capacity(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise WarehouseError("capacity must be an integer")
    if not 1 <= value <= 1_000_000:
        raise WarehouseError("capacity must be between 1 and 1,000,000")
    return value


def _validated_list_page_size(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise WarehouseError("max list page size must be an integer")
    if not 1 <= value <= WAREHOUSE_MAX_LIST_PAGE:
        raise WarehouseError(f"max list page size must not exceed {WAREHOUSE_MAX_LIST_PAGE}")
    return value
