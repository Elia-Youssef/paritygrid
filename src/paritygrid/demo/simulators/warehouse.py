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
import contextlib
import json
import os
import re
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import cast

from paritygrid.application.ports.connectors import canonical_target_payload_sha256
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
WAREHOUSE_STATE_FORMAT = "paritygrid.demo.warehouse-state"
WAREHOUSE_STATE_VERSION = 1
WAREHOUSE_STATE_FILENAME = "warehouse-state.json"

_EMPTY_FINGERPRINT = sha256(b"paritygrid-warehouse-empty-v1").hexdigest()
_SKU_PATTERN = re.compile(r"[A-Z0-9]+(?:-[A-Z0-9]+)*", flags=re.ASCII)
_MAX_IDEMPOTENCY_ENTRIES = 10_000
_IDEMPOTENCY_KEY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", flags=re.ASCII)
_CURSOR_PREFIX = "wh1"
_TARGET_PRECONDITION_HEADER = "x-paritygrid-target-precondition"
_MAX_WAREHOUSE_STATE_BYTES = 16 * 1024 * 1024
_MAX_WAREHOUSE_STATE_DEPTH = 12
_MAX_WAREHOUSE_STATE_ITEMS = 10_000
_MAX_WAREHOUSE_STATE_STRING_LENGTH = 16_384
_MAX_COUNTER = 2_147_483_647
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)


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
        *,
        state_root: Path | None = None,
    ) -> None:
        self._script = require_transport_script(script, subject="warehouse")
        self._settings = settings if settings is not None else WarehouseSettings()
        self._records: dict[str, tuple[dict[str, object], int]] = {}
        self._target_version = 0
        self._idempotency: dict[str, _IdempotencyEntry] = {}
        self._request_count = 0
        self._applied: list[AppliedFailure] = []
        self._effect_commits: dict[str, int] = {}
        self._state_path = _persistent_state_path(state_root)
        if self._state_path is not None:
            self._load_persisted_state()

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

    def external_effect_counts(self) -> dict[str, int]:
        """Return durable logical-effect commits keyed by idempotency key.

        This is intentionally separate from request counts: a retry can be a
        real HTTP request without being a second target effect.  The
        interruption proof compares these durable counts to the repair-action
        keys, so process restart cannot be mistaken for exactly-once external
        behavior merely because a new in-memory simulator was started.
        """
        return dict(self._effect_commits)

    def has_idempotency_keys(self, keys: tuple[str, ...]) -> bool:
        """Report whether every exact durable request receipt is present."""
        return all(key in self._idempotency for key in keys)

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
        self._persist_state()
        failure = self._script.failure_for(self._request_count)
        if failure is not None:
            self._applied.append(AppliedFailure(sequence=failure.sequence, kind=failure.kind))
            self._persist_state()
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
        precondition = request.headers.get(_TARGET_PRECONDITION_HEADER)
        fingerprint = _request_fingerprint(
            request.method, request_path_only(request.path), request.body, precondition
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
        if not _precondition_holds(precondition, existing):
            return error_response(
                409,
                "target_precondition_failed",
                "The target no longer satisfies the repair precondition.",
            )
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
            self._persist_state()
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
        self._effect_commits[key] = self._effect_commits.get(key, 0) + 1
        self._persist_state()
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

    def _state_document(self) -> dict[str, object]:
        """Return the exact bounded durable target state document."""
        return {
            "applied_failures": [
                {"kind": failure.kind.value, "sequence": failure.sequence}
                for failure in self._applied
            ],
            "effect_commits": dict(sorted(self._effect_commits.items())),
            "format": WAREHOUSE_STATE_FORMAT,
            "idempotency": {
                key: {
                    "outcome": entry.outcome,
                    "record_version": entry.record_version,
                    "request_fingerprint": entry.request_fingerprint,
                    "status": entry.status,
                    "target_version": entry.target_version,
                }
                for key, entry in sorted(self._idempotency.items())
            },
            "records": {
                sku: {"payload": payload, "record_version": record_version}
                for sku, (payload, record_version) in sorted(self._records.items())
            },
            "request_count": self._request_count,
            "target_version": self._target_version,
            "version": WAREHOUSE_STATE_VERSION,
        }

    def _load_persisted_state(self) -> None:
        state_path = self._state_path
        if state_path is None or not state_path.exists():
            return
        _reject_link_or_non_file(state_path, label="warehouse state")
        try:
            raw = state_path.read_bytes()
        except OSError as error:
            raise WarehouseError("the persistent warehouse state is unreadable") from error
        if len(raw) > _MAX_WAREHOUSE_STATE_BYTES:
            raise WarehouseError("the persistent warehouse state is oversized")
        try:
            decoded: object = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise WarehouseError("the persistent warehouse state is malformed") from error
        if not isinstance(decoded, dict):
            raise WarehouseError("the persistent warehouse state must be a JSON object")
        document = cast("dict[str, object]", decoded)
        expected_keys = {
            "applied_failures",
            "effect_commits",
            "format",
            "idempotency",
            "records",
            "request_count",
            "target_version",
            "version",
        }
        if set(document) != expected_keys:
            raise WarehouseError("the persistent warehouse state has an unexpected schema")
        if (
            document["format"] != WAREHOUSE_STATE_FORMAT
            or document["version"] != WAREHOUSE_STATE_VERSION
        ):
            raise WarehouseError("the persistent warehouse state has an unsupported version")
        self._target_version = _state_counter(document["target_version"], "target_version")
        self._request_count = _state_counter(document["request_count"], "request_count", minimum=0)
        self._records = _state_records(document["records"], self._target_version, self._settings)
        self._idempotency = _state_idempotency(document["idempotency"], self._target_version)
        self._effect_commits = _state_effect_commits(
            document["effect_commits"], self._idempotency, self._target_version
        )
        self._applied = _state_applied_failures(document["applied_failures"])

    def _persist_state(self) -> None:
        state_path = self._state_path
        if state_path is None:
            return
        payload = json.dumps(
            self._state_document(), ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ).encode("ascii")
        if len(payload) > _MAX_WAREHOUSE_STATE_BYTES:
            raise WarehouseError("the persistent warehouse state exceeds its bounded size")
        import uuid

        partial = state_path.with_name(f"{state_path.name}.{uuid.uuid4().hex}.partial")
        _reject_link_or_non_file(partial, label="warehouse state partial", allow_missing=True)
        handle: int | None = None
        try:
            handle = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            written = 0
            while written < len(payload):
                written += os.write(handle, payload[written:])
            os.fsync(handle)
            os.close(handle)
            handle = None
            os.replace(partial, state_path)
        except OSError as error:
            raise WarehouseError("the persistent warehouse state could not be committed") from error
        finally:
            if handle is not None:
                os.close(handle)
            if partial.exists():
                with contextlib.suppress(OSError):
                    partial.unlink()

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
        state_root: Path | None = None,
    ) -> None:
        self._behavior = WarehouseBehavior(
            script if script is not None else FailureScript.empty(),
            settings,
            state_root=state_root,
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


def _persistent_state_path(state_root: Path | None) -> Path | None:
    """Resolve one exact, link-free state file beneath an existing root.

    Persistence is deliberately opt-in.  Ordinary simulator tests and
    callers remain fully in-memory; the demo lifecycle supplies its already
    validated ``scenario`` directory so only that owned location gains a
    durable external-target model across an interruption restart.
    """
    if state_root is None:
        return None
    if not state_root.is_absolute():
        raise WarehouseError("the persistent warehouse state root must be an absolute Path")
    if not state_root.is_dir() or state_root.is_symlink() or state_root.is_junction():
        raise WarehouseError("the persistent warehouse state root must be a plain directory")
    resolved = state_root.resolve(strict=True)
    if str(resolved).casefold() != str(state_root).casefold():
        raise WarehouseError("the persistent warehouse state root traverses a link")
    for component in (state_root, *state_root.parents):
        if component.is_symlink() or component.is_junction():
            raise WarehouseError("the persistent warehouse state root traverses a link")
    return state_root / WAREHOUSE_STATE_FILENAME


def _reject_link_or_non_file(path: Path, *, label: str, allow_missing: bool = False) -> None:
    """Reject link/reparse and non-regular-file state targets before I/O."""
    if path.is_symlink() or path.is_junction():
        raise WarehouseError(f"the {label} must not be a symbolic link or junction")
    if not path.exists():
        if allow_missing:
            return
        raise WarehouseError(f"the {label} is missing")
    if not path.is_file():
        raise WarehouseError(f"the {label} must be a regular file")


def _state_counter(value: object, field: str, *, minimum: int = 1) -> int:
    if type(value) is not int or not minimum <= value <= _MAX_COUNTER:
        raise WarehouseError(f"persistent warehouse state field {field} is invalid")
    return value


def _state_records(
    value: object,
    target_version: int,
    settings: WarehouseSettings,
) -> dict[str, tuple[dict[str, object], int]]:
    if not isinstance(value, dict):
        raise WarehouseError("persistent warehouse records are invalid")
    entries = cast("dict[object, object]", value)
    if len(entries) > settings.capacity:
        raise WarehouseError("persistent warehouse records are invalid")
    records: dict[str, tuple[dict[str, object], int]] = {}
    for sku_value, entry_value in entries.items():
        if (
            type(sku_value) is not str
            or _SKU_PATTERN.fullmatch(sku_value) is None
            or not isinstance(entry_value, dict)
            or set(cast("dict[object, object]", entry_value)) != {"payload", "record_version"}
        ):
            raise WarehouseError("persistent warehouse record entry is invalid")
        entry = cast("dict[str, object]", entry_value)
        payload_value = _state_json_value(entry["payload"], depth=0)
        if not isinstance(payload_value, dict):
            raise WarehouseError("persistent warehouse record payload must be an object")
        record_version = _state_counter(entry["record_version"], "record_version")
        if record_version > target_version:
            raise WarehouseError("persistent warehouse record version exceeds target version")
        records[sku_value] = (cast("dict[str, object]", payload_value), record_version)
    if target_version == 0 and records:
        raise WarehouseError("a zero-version persistent warehouse cannot carry records")
    return records


def _state_idempotency(
    value: object,
    target_version: int,
) -> dict[str, _IdempotencyEntry]:
    if not isinstance(value, dict):
        raise WarehouseError("persistent warehouse idempotency state is invalid")
    entries_value = cast("dict[object, object]", value)
    if len(entries_value) > _MAX_IDEMPOTENCY_ENTRIES:
        raise WarehouseError("persistent warehouse idempotency state is invalid")
    entries: dict[str, _IdempotencyEntry] = {}
    expected = {
        "outcome",
        "record_version",
        "request_fingerprint",
        "status",
        "target_version",
    }
    for key, entry_value in entries_value.items():
        if (
            type(key) is not str
            or _IDEMPOTENCY_KEY_PATTERN.fullmatch(key) is None
            or not isinstance(entry_value, dict)
            or set(cast("dict[object, object]", entry_value)) != expected
        ):
            raise WarehouseError("persistent warehouse idempotency entry is invalid")
        entry = cast("dict[str, object]", entry_value)
        fingerprint = entry["request_fingerprint"]
        outcome = entry["outcome"]
        if (
            type(fingerprint) is not str
            or _SHA256_PATTERN.fullmatch(fingerprint) is None
            or outcome not in ("applied", "unchanged")
            or entry["status"] != 200
        ):
            raise WarehouseError("persistent warehouse idempotency receipt is invalid")
        entry_target_version = _state_counter(
            entry["target_version"], "idempotency target_version", minimum=0
        )
        entry_record_version = _state_counter(entry["record_version"], "idempotency record_version")
        if entry_target_version > target_version or entry_record_version > target_version:
            raise WarehouseError("persistent warehouse idempotency receipt exceeds target state")
        entries[key] = _IdempotencyEntry(
            request_fingerprint=fingerprint,
            outcome=cast("str", outcome),
            target_version=entry_target_version,
            record_version=entry_record_version,
            status=200,
        )
    return entries


def _state_effect_commits(
    value: object,
    idempotency: dict[str, _IdempotencyEntry],
    target_version: int,
) -> dict[str, int]:
    if not isinstance(value, dict):
        raise WarehouseError("persistent warehouse effect counts are invalid")
    entries = cast("dict[object, object]", value)
    if len(entries) > _MAX_IDEMPOTENCY_ENTRIES:
        raise WarehouseError("persistent warehouse effect counts are invalid")
    effects: dict[str, int] = {}
    for key, count_value in entries.items():
        if type(key) is not str:
            raise WarehouseError("persistent warehouse effect key is invalid")
        entry = idempotency.get(key)
        if entry is None or entry.outcome != "applied":
            raise WarehouseError("persistent warehouse effect lacks an applied idempotency receipt")
        effects[key] = _state_counter(count_value, "effect_commit")
    if sum(effects.values()) != target_version:
        raise WarehouseError("persistent warehouse effect count diverges from target version")
    return effects


def _state_applied_failures(value: object) -> list[AppliedFailure]:
    if not isinstance(value, list):
        raise WarehouseError("persistent warehouse applied failures are invalid")
    entries = cast("list[object]", value)
    if len(entries) > _MAX_WAREHOUSE_STATE_ITEMS:
        raise WarehouseError("persistent warehouse applied failures are invalid")
    applied: list[AppliedFailure] = []
    for item in entries:
        if not isinstance(item, dict) or set(cast("dict[object, object]", item)) != {
            "kind",
            "sequence",
        }:
            raise WarehouseError("persistent warehouse applied failure is invalid")
        entry = cast("dict[str, object]", item)
        sequence = _state_counter(entry["sequence"], "failure sequence")
        kind_value = entry["kind"]
        if type(kind_value) is not str:
            raise WarehouseError("persistent warehouse applied failure kind is invalid")
        try:
            kind = ScriptedFailureKind(kind_value)
        except ValueError as error:
            raise WarehouseError(
                "persistent warehouse applied failure kind is unsupported"
            ) from error
        applied.append(AppliedFailure(sequence=sequence, kind=kind))
    return applied


def _state_json_value(value: object, *, depth: int) -> object:
    """Validate persisted payload values without admitting executable objects."""
    if depth > _MAX_WAREHOUSE_STATE_DEPTH:
        raise WarehouseError("persistent warehouse payload nesting is too deep")
    if value is None or type(value) in (bool, int):
        return value
    if type(value) is float:
        if value != value or value in (float("inf"), float("-inf")):
            raise WarehouseError("persistent warehouse payload carries a non-finite number")
        return value
    if type(value) is str:
        if len(value) > _MAX_WAREHOUSE_STATE_STRING_LENGTH:
            raise WarehouseError("persistent warehouse payload text is too long")
        return value
    if isinstance(value, list):
        items = cast("list[object]", value)
        if len(items) > _MAX_WAREHOUSE_STATE_ITEMS:
            raise WarehouseError("persistent warehouse payload list is too large")
        return [_state_json_value(item, depth=depth + 1) for item in items]
    if isinstance(value, dict):
        entries = cast("dict[object, object]", value)
        if len(entries) > _MAX_WAREHOUSE_STATE_ITEMS:
            raise WarehouseError("persistent warehouse payload object is too large")
        converted: dict[str, object] = {}
        for key, nested in entries.items():
            if type(key) is not str or len(key) > _MAX_WAREHOUSE_STATE_STRING_LENGTH:
                raise WarehouseError("persistent warehouse payload object key is invalid")
            converted[key] = _state_json_value(nested, depth=depth + 1)
        return converted
    raise WarehouseError("persistent warehouse payload carries an unsupported value")


def _request_fingerprint(
    method: str, path: str, body: bytes, precondition: str | None = None
) -> str:
    condition = b"" if precondition is None else precondition.encode("ascii")
    payload = b"\0".join((method.encode("ascii"), path.encode("ascii"), body, condition))
    return sha256(b"paritygrid-warehouse-request-v1\0" + payload).hexdigest()


def _precondition_holds(
    precondition: str | None,
    existing: tuple[dict[str, object], int] | None,
) -> bool:
    """Evaluate one target predicate in the same turn as the mutation.

    Idempotency lookup runs before this predicate, so a retry of a committed
    write returns its durable receipt even if another actor has since changed
    the record.  New writes must prove the precondition atomically.
    """
    if precondition is None:
        return True
    if precondition == "absent":
        return existing is None
    digest_prefix = "sha256:"
    if not precondition.startswith(digest_prefix):
        return False
    expected = precondition.removeprefix(digest_prefix)
    if len(expected) != 64 or re.fullmatch(r"[0-9a-f]{64}", expected, flags=re.ASCII) is None:
        return False
    return existing is not None and canonical_target_payload_sha256(existing[0]) == expected


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
