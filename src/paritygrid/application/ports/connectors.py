"""Application-owned runner-neutral connector contract version 1 (P9.1).

This module freezes the one boundary every ParityGrid connector implements.
It is deliberately independent of execution strategies: adapters never learn
which runner scheduled the work item that issued a call, and the contract
carries no scheduler, lease, or persistence concepts. Domain ownership stays
inward — adapters import classification and capability vocabulary from the
application and domain layers and add no behavior branches of their own.

The contract covers, explicitly:

- **Configuration** — frozen validated per-adapter configuration values.
- **Capability** — closed capability metadata in the planner vocabulary.
- **Lifecycle** — ``created → open → closed`` with idempotent close and
  full resource release after success, failure, cancellation, and partial
  initialization.
- **Cancellation** — a cooperative token for blocking calls plus native
  ``asyncio`` task cancellation for async calls; cancellation is never
  translated into an ordinary failure and vice versa.
- **Timeout** — a per-call absolute request budget in microseconds.
- **Pagination** — opaque bounded cursors; a page never exceeds the
  configured record bound and exhausted sources return ``None``.
- **Bounds** — validated response, record, page, file, and row limits; no
  connector may materialize an unbounded source.
- **Error** — a closed error taxonomy mapped onto the domain
  :class:`~paritygrid.domain.execution.failures.FailureClassification`
  values so scheduler retry policy needs no connector-specific branches.
- **Observability** — closed connector event kinds with redacted, bounded
  fields delivered through an observer that can never break a call.

Public text crossing this boundary is validated against registered secret
material; an error or event that would carry a secret fails closed at
construction instead of leaking.
"""

import asyncio
import re
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol, cast

from paritygrid.application.planner.connectors import (
    ConnectorCapability,
    ConnectorCapabilitySet,
)
from paritygrid.application.ports.connector_redaction import (
    MAX_PUBLIC_DETAIL_LENGTH,
    REDACTION_PLACEHOLDER,
    SecretMaterial,
    assert_public_text_is_safe,
)
from paritygrid.domain.execution.failures import (
    FailureClassification,
    FailureDisposition,
    disposition_for,
)

CONNECTOR_CONTRACT_VERSION = 1
CONNECTOR_CAPABILITIES_PROTOCOL = "paritygrid.connector.capabilities.v1"

MAX_CURSOR_LENGTH = 128
MAX_CORRELATION_ID_LENGTH = 96
MAX_PAGE_RECORDS = 200
MAX_RESPONSE_BYTES = 1_048_576
MAX_RECORD_BYTES = 65_536
MAX_RECORD_FIELDS = 32
MAX_RECORD_DEPTH = 4
MAX_RECORD_LIST_ITEMS = 64
MAX_RECORD_FIELD_TEXT = 1_024
MAX_FILE_BYTES = 8 * 1_024 * 1_024
MAX_FILE_ROWS = 5_000
MAX_REQUEST_TIMEOUT_MICROSECONDS = 60_000_000
MAX_EVENT_DETAILS = 16
MAX_EVENT_DETAIL_TEXT = 128
MAX_SKU_LENGTH = 64
MAX_IDEMPOTENCY_KEY_LENGTH = 128

_CURSOR_PATTERN = re.compile(r"[A-Za-z0-9._:-]{1,128}", flags=re.ASCII)
_CORRELATION_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,95}", flags=re.ASCII)
_SKU_PATTERN = re.compile(r"[A-Z0-9]+(?:-[A-Z0-9]+)*", flags=re.ASCII)
_IDEMPOTENCY_KEY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", flags=re.ASCII)


class ConnectorContractError(RuntimeError):
    """Base failure for connector contract misuse."""


class ConnectorLoopError(ConnectorContractError):
    """A blocking connector entry point ran inside an active event loop."""


class ConnectorLifecycleError(ConnectorContractError):
    """A connector operation was issued in a disallowed lifecycle state."""


class ConnectorConfigurationError(ConnectorContractError, ValueError):
    """Connector configuration values are invalid."""


class ConnectorError(RuntimeError):
    """Base typed connector failure carrying a closed classification.

    ``detail`` is the only text intended for durable persistence or public
    transport; the exception message mirrors it and both are checked
    against the registered secrets at construction time.
    """

    classification: FailureClassification = FailureClassification.UNKNOWN

    def __init__(
        self,
        message: str,
        *,
        detail: str | None = None,
        retry_after_seconds: int | None = None,
        secrets: SecretMaterial | None = None,
    ) -> None:
        safe_message = assert_public_text_is_safe(message, secrets)
        safe_detail = (
            assert_public_text_is_safe(detail, secrets) if detail is not None else safe_message
        )
        if len(safe_detail) > MAX_PUBLIC_DETAIL_LENGTH:
            safe_detail = safe_detail[: MAX_PUBLIC_DETAIL_LENGTH - 3] + "..."
        if retry_after_seconds is not None and not 1 <= retry_after_seconds <= 60:
            raise ConnectorContractError("retry_after_seconds must be between 1 and 60")
        super().__init__(safe_message)
        self._detail = safe_detail
        self._retry_after_seconds = retry_after_seconds

    @property
    def detail(self) -> str:
        """Return the bounded, redacted public failure detail."""
        return self._detail

    @property
    def retry_after_seconds(self) -> int | None:
        """Return the server-advised retry delay, when classified."""
        return self._retry_after_seconds

    @property
    def disposition(self) -> FailureDisposition:
        """Return the scheduler disposition for this classification."""
        return disposition_for(self.classification)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(classification={self.classification.value!r}, "
            f"detail={self._detail!r}, retry_after_seconds={self._retry_after_seconds!r})"
        )


class ConnectorRetryableError(ConnectorError):
    """A transient failure the scheduler may retry within its policy."""

    classification = FailureClassification.CONNECTION


class ConnectorTimeoutError(ConnectorRetryableError):
    """The call exceeded its configured request budget."""

    classification = FailureClassification.TIMEOUT


class ConnectorRateLimitedError(ConnectorRetryableError):
    """The remote system answered with an explicit retryable throttle."""

    classification = FailureClassification.HTTP_429


class ConnectorServerFailureError(ConnectorRetryableError):
    """The remote system answered with a bounded-retry server failure."""

    classification = FailureClassification.HTTP_5XX


class ConnectorPermanentError(ConnectorError):
    """A failure retrying cannot repair."""

    classification = FailureClassification.HTTP_4XX


class ConnectorValidationError(ConnectorError, ValueError):
    """Input, configuration, or payload validation failed (quarantine)."""

    classification = FailureClassification.VALIDATION


class ConnectorConflictError(ConnectorError):
    """An idempotency key was reused with a different logical request."""

    classification = FailureClassification.IDEMPOTENCY_CONFLICT


class ConnectorCancelledError(ConnectorError):
    """The caller cancelled the call through its cooperative token."""

    classification = FailureClassification.USER_CANCELLATION


class ConnectorAmbiguousError(ConnectorError):
    """The outcome of a mutating call is unknown and must not be guessed.

    The effect may or may not have been applied. Classification is
    ``unknown`` (fail closed); the documented resolution is a replay with
    the same idempotency identity, which the target answers without a
    second logical effect.
    """

    classification = FailureClassification.UNKNOWN


class ConnectorUnknownError(ConnectorError):
    """A response violated the connector's protocol or bounds contract.

    Classification is ``unknown`` and fail closed: the connector refuses
    to guess whether retrying would help, so the disposition stays
    permanent until a caller explicitly intervenes.
    """

    classification = FailureClassification.UNKNOWN


class ConnectorKind(StrEnum):
    """Closed registry of the Phase 9 connector kinds."""

    ASYNC_HTTP_SOURCE = "async_http_source"
    BLOCKING_HTTP_SOURCE = "blocking_http_source"
    CSV_SOURCE = "csv_source"
    JSONL_SOURCE = "jsonl_source"
    WAREHOUSE_TARGET = "warehouse_target"


class ConnectorState(StrEnum):
    """Closed connector lifecycle states."""

    CREATED = "created"
    OPEN = "open"
    CLOSED = "closed"


class SourceOutcome(StrEnum):
    """What one emitted source record represents."""

    VALID = "valid"
    MALFORMED = "malformed"


class TargetEffectOutcome(StrEnum):
    """What one observed target write achieved."""

    APPLIED = "applied"
    UNCHANGED = "unchanged"
    REPLAYED = "replayed"


@dataclass(frozen=True, slots=True)
class ConnectorCapabilitiesV1:
    """Immutable capability metadata for one connector kind.

    ``max_page_records`` is zero exactly when the kind does not page
    (the warehouse target exchanges single records, not pages).
    """

    protocol: str
    contract_version: int
    kind: ConnectorKind
    capabilities: ConnectorCapabilitySet
    max_page_records: int
    supports_cursors: bool

    def __post_init__(self) -> None:
        if self.protocol != CONNECTOR_CAPABILITIES_PROTOCOL:
            raise ConnectorConfigurationError("connector capabilities protocol is unknown")
        if self.contract_version != CONNECTOR_CONTRACT_VERSION:
            raise ConnectorConfigurationError("connector capabilities version is unsupported")
        if type(self.kind) is not ConnectorKind:
            raise ConnectorConfigurationError("connector kind must use ConnectorKind")
        if type(self.capabilities) is not ConnectorCapabilitySet:
            raise ConnectorConfigurationError("capabilities must use ConnectorCapabilitySet")
        if type(self.max_page_records) is not int or (
            not 0 <= self.max_page_records <= MAX_PAGE_RECORDS
        ):
            raise ConnectorConfigurationError("max page records is outside the bound")
        if type(self.supports_cursors) is not bool:
            raise ConnectorConfigurationError("supports_cursors must be a boolean")

    def supports(self, capability: ConnectorCapability) -> bool:
        """Return whether this connector exposes one closed capability."""
        return self.capabilities.supports(capability)


@dataclass(frozen=True, slots=True)
class ConnectorCallBounds:
    """Validated per-call limits shared by every connector kind."""

    request_timeout_microseconds: int = 5_000_000
    max_response_bytes: int = MAX_RESPONSE_BYTES
    max_record_bytes: int = MAX_RECORD_BYTES
    max_page_records: int = MAX_PAGE_RECORDS

    def __post_init__(self) -> None:
        for name, maximum in (
            ("request_timeout_microseconds", MAX_REQUEST_TIMEOUT_MICROSECONDS),
            ("max_response_bytes", MAX_RESPONSE_BYTES),
            ("max_record_bytes", MAX_RECORD_BYTES),
            ("max_page_records", MAX_PAGE_RECORDS),
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ConnectorConfigurationError(f"{name} must be an integer")
            if not 1 <= value <= maximum:
                raise ConnectorConfigurationError(f"{name} must be between 1 and {maximum}")


@dataclass(frozen=True, slots=True)
class FileReadBounds:
    """Validated streaming bounds for file connectors.

    A file connector never materializes its source: it reads one bounded
    line at a time and enforces both the whole-file byte cap and the row
    cap incrementally, so a source that grows during a read is still
    bounded.
    """

    max_file_bytes: int = MAX_FILE_BYTES
    max_rows: int = MAX_FILE_ROWS

    def __post_init__(self) -> None:
        for name, maximum in (("max_file_bytes", MAX_FILE_BYTES), ("max_rows", MAX_FILE_ROWS)):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ConnectorConfigurationError(f"{name} must be an integer")
            if not 1 <= value <= maximum:
                raise ConnectorConfigurationError(f"{name} must be between 1 and {maximum}")


@dataclass(frozen=True, slots=True)
class ConnectorAuthentication:
    """A resolved bearer-style secret with an always-redacted surface.

    Configuration persists environment-variable *names*; this value object
    exists only in memory for the lifetime of one connector and is
    registered as secret material for every redaction the connector
    performs.
    """

    header_name: str
    scheme: str
    token: str

    def __init__(
        self, *, header_name: str = "Authorization", scheme: str = "Bearer", token: str
    ) -> None:
        if type(header_name) is not str or not re.fullmatch(r"[A-Za-z][A-Za-z0-9-]*", header_name):
            raise ConnectorConfigurationError("authentication header name is invalid")
        if type(scheme) is not str or not re.fullmatch(r"[A-Za-z]{1,16}", scheme):
            raise ConnectorConfigurationError("authentication scheme is invalid")
        if type(token) is not str or not 1 <= len(token) <= 256:
            raise ConnectorConfigurationError("authentication token must be 1-256 characters")
        object.__setattr__(self, "header_name", header_name)
        object.__setattr__(self, "scheme", scheme)
        object.__setattr__(self, "token", token)

    def header_value(self) -> str:
        """Return the wire header value carrying the secret."""
        return f"{self.scheme} {self.token}"

    def secret_material(self) -> SecretMaterial:
        """Return the secret registry covering this token."""
        return SecretMaterial((self.token,))

    def __repr__(self) -> str:
        return "ConnectorAuthentication(redacted=True)"


class ConnectorCancellationToken(Protocol):
    """Cooperative cancellation shared by blocking and async calls."""

    def is_cancelled(self) -> bool: ...

    def raise_if_cancelled(self) -> None: ...


@dataclass(frozen=True, slots=True)
class _NeverCancelledToken:
    """The immutable token used when a call has no cancellation source."""

    def is_cancelled(self) -> bool:
        return False

    def raise_if_cancelled(self) -> None:
        return None


NEVER_CANCELLED: ConnectorCancellationToken = _NeverCancelledToken()


@dataclass(slots=True)
class EventCancellationToken:
    """Thread-safe cancellable token implementation over one event."""

    _event: threading.Event = field(default_factory=threading.Event)

    def is_cancelled(self) -> bool:
        """Return whether cancellation was requested."""
        return self._event.is_set()

    def cancel(self) -> None:
        """Request cancellation; safe to call repeatedly."""
        self._event.set()

    def raise_if_cancelled(self) -> None:
        """Raise the typed cancellation failure when cancelled."""
        if self._event.is_set():
            raise ConnectorCancelledError("the connector call was cancelled before completion")


@dataclass(frozen=True, slots=True)
class ConnectorCallContext:
    """One call's correlation, cancellation, and secret context."""

    correlation_id: str | None = None
    cancellation_token: ConnectorCancellationToken = NEVER_CANCELLED
    secrets: SecretMaterial = field(default_factory=SecretMaterial)

    def __post_init__(self) -> None:
        correlation = self.correlation_id
        if correlation is not None and _CORRELATION_ID_PATTERN.fullmatch(correlation) is None:
            raise ConnectorConfigurationError("correlation id is outside the accepted shape")

    def raise_if_cancelled(self) -> None:
        """Propagate cooperative cancellation at a safe boundary."""
        self.cancellation_token.raise_if_cancelled()


def validate_cursor(cursor: object) -> str:
    """Validate one opaque cursor text and return it."""
    if not isinstance(cursor, str):
        raise ConnectorValidationError("cursor must be text")
    if _CURSOR_PATTERN.fullmatch(cursor) is None:
        raise ConnectorValidationError("cursor is outside the accepted shape")
    return cursor


def validate_base_url(url: object) -> str:
    """Validate one plain-HTTP base URL without credentials."""
    if not isinstance(url, str) or not url:
        raise ConnectorConfigurationError("base url must be non-empty text")
    pattern = re.compile(
        r"http://[A-Za-z0-9._-]+(?::[0-9]{1,5})?(?:/[A-Za-z0-9._/-]*)?",
        flags=re.ASCII,
    )
    if pattern.fullmatch(url) is None or "@" in url:
        raise ConnectorConfigurationError("base url must be credential-free plain http")
    return url


def validate_source_record_payload(payload: object) -> Mapping[str, object]:
    """Validate one closed record payload of bounded JSON primitives."""
    if not isinstance(payload, Mapping):
        raise ConnectorValidationError("record payload must be a mapping")
    document = cast("Mapping[str, object]", payload)
    if len(document) > MAX_RECORD_FIELDS:
        raise ConnectorValidationError("record payload exceeds the field bound")

    def validate_value(value: object, depth: int) -> None:
        if value is None or isinstance(value, bool):
            return
        if isinstance(value, int):
            if not -(2**53) <= value <= 2**53:
                raise ConnectorValidationError("record integer is outside the safe range")
            return
        if isinstance(value, str):
            if len(value) > MAX_RECORD_FIELD_TEXT:
                raise ConnectorValidationError("record text field exceeds the length bound")
            return
        if depth >= MAX_RECORD_DEPTH:
            raise ConnectorValidationError("record payload exceeds the nesting bound")
        if isinstance(value, Mapping):
            nested = cast("Mapping[str, object]", value)
            if len(nested) > MAX_RECORD_FIELDS:
                raise ConnectorValidationError("record mapping exceeds the field bound")
            for item in nested.values():
                validate_value(item, depth + 1)
            return
        if isinstance(value, (list, tuple)):
            items = cast("tuple[object, ...] | list[object]", value)
            if len(items) > MAX_RECORD_LIST_ITEMS:
                raise ConnectorValidationError("record list exceeds the item bound")
            for item in items:
                validate_value(item, depth + 1)
            return
        raise ConnectorValidationError("record payload carries an unsupported value type")

    for value in document.values():
        validate_value(value, 1)
    return document


@dataclass(frozen=True, slots=True)
class SourceRecord:
    """One emitted source record, valid or explicitly malformed.

    Malformed records keep their source position and a bounded reason so
    later normalization (Phase 10) can quarantine them without losing
    provenance; the payload is ``None`` for malformed rows so no partially
    trusted document travels onward.
    """

    position: int
    outcome: SourceOutcome
    payload: Mapping[str, object] | None
    malformed_reason: str | None = None

    def __post_init__(self) -> None:
        if type(self.position) is not int:
            raise ConnectorValidationError("record position must be an integer")
        if self.position < 0:
            raise ConnectorValidationError("record position must not be negative")
        if type(self.outcome) is not SourceOutcome:
            raise ConnectorValidationError("record outcome must use SourceOutcome")
        if self.outcome is SourceOutcome.VALID:
            if self.payload is None:
                raise ConnectorValidationError("valid records must carry a payload")
            validate_source_record_payload(self.payload)
            if self.malformed_reason is not None:
                raise ConnectorValidationError("valid records carry no malformed reason")
        else:
            if self.payload is not None:
                raise ConnectorValidationError("malformed records carry no payload")
            if not isinstance(self.malformed_reason, str) or not self.malformed_reason:
                raise ConnectorValidationError("malformed records require a reason")

    @property
    def is_malformed(self) -> bool:
        """Return whether this record was rejected as malformed."""
        return self.outcome is SourceOutcome.MALFORMED


@dataclass(frozen=True, slots=True)
class SourcePage:
    """One bounded page of source records and its continuation cursor."""

    records: tuple[SourceRecord, ...]
    next_cursor: str | None
    request_count: int
    byte_count: int

    def __post_init__(self) -> None:
        if type(self.records) is not tuple:
            raise ConnectorValidationError("page records must be a tuple")
        if len(self.records) > MAX_PAGE_RECORDS:
            raise ConnectorValidationError("page exceeds the record bound")
        for name in ("request_count", "byte_count"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ConnectorValidationError(f"page {name} must be a nonnegative integer")
        if self.next_cursor is not None:
            validate_cursor(self.next_cursor)

    @property
    def exhausted(self) -> bool:
        """Return whether the source has no further pages."""
        return self.next_cursor is None


def validate_sku(sku: object) -> str:
    """Validate one canonical warehouse record key."""
    if not isinstance(sku, str) or _SKU_PATTERN.fullmatch(sku) is None:
        raise ConnectorValidationError("record key is outside the canonical shape")
    return sku


def validate_idempotency_key(key: object) -> str:
    """Validate one external idempotency key."""
    if not isinstance(key, str) or _IDEMPOTENCY_KEY_PATTERN.fullmatch(key) is None:
        raise ConnectorValidationError("idempotency key is outside the accepted shape")
    return key


@dataclass(frozen=True, slots=True)
class TargetWriteRequest:
    """One target upsert with its stable idempotency identity."""

    sku: str
    payload: Mapping[str, object]
    idempotency_key: str

    def __post_init__(self) -> None:
        validate_sku(self.sku)
        validate_source_record_payload(self.payload)
        if self.payload.get("sku") != self.sku:
            raise ConnectorValidationError("payload must address the same record key")
        validate_idempotency_key(self.idempotency_key)


@dataclass(frozen=True, slots=True)
class TargetWriteOutcome:
    """The observed logical effect of one target write."""

    outcome: TargetEffectOutcome
    record_version: int
    target_version: int
    request_count: int

    def __post_init__(self) -> None:
        if type(self.outcome) is not TargetEffectOutcome:
            raise ConnectorValidationError("target outcome must use TargetEffectOutcome")
        for name in ("record_version", "target_version", "request_count"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ConnectorValidationError(f"target {name} must be a nonnegative integer")

    @property
    def changed_state(self) -> bool:
        """Return whether this call produced a new logical effect."""
        return self.outcome is TargetEffectOutcome.APPLIED


@dataclass(frozen=True, slots=True)
class TargetRecord:
    """One stored target record as observed by a read."""

    sku: str
    payload: Mapping[str, object]
    record_version: int
    target_version: int

    def __post_init__(self) -> None:
        validate_sku(self.sku)
        validate_source_record_payload(self.payload)
        for name in ("record_version", "target_version"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ConnectorValidationError(f"target {name} must be a nonnegative integer")


@dataclass(frozen=True, slots=True)
class TargetStateSnapshot:
    """The observable target state summary without record payloads."""

    record_count: int
    target_version: int
    content_fingerprint: str
    capacity: int

    def __post_init__(self) -> None:
        for name in ("record_count", "target_version", "capacity"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ConnectorValidationError(f"target state {name} must be a nonnegative integer")
        if type(self.content_fingerprint) is not str or not re.fullmatch(
            r"[0-9a-f]{64}", self.content_fingerprint
        ):
            raise ConnectorValidationError("target fingerprint must be a lowercase sha-256 text")


class ConnectorEventKind(StrEnum):
    """Closed connector observability event kinds."""

    OPENED = "opened"
    OPEN_FAILED = "open_failed"
    CLOSED = "closed"
    CALL_STARTED = "call_started"
    CALL_COMPLETED = "call_completed"
    CALL_FAILED = "call_failed"
    PAGE_COMPLETED = "page_completed"
    RECORD_REJECTED = "record_rejected"
    TARGET_EFFECT_OBSERVED = "target_effect_observed"
    OBSERVER_FAILED = "observer_failed"


@dataclass(frozen=True, slots=True)
class ConnectorEvent:
    """One bounded, redacted connector observability event.

    Every text field is filtered through the call's registered secrets at
    construction; an event quoting a secret fails closed instead of
    escaping the connector boundary.
    """

    kind: ConnectorEventKind
    connector_kind: ConnectorKind
    correlation_id: str | None
    details: Mapping[str, object]

    def __post_init__(self) -> None:
        if type(self.kind) is not ConnectorEventKind:
            raise ConnectorContractError("event kind must use ConnectorEventKind")
        if type(self.connector_kind) is not ConnectorKind:
            raise ConnectorContractError("event connector kind must use ConnectorKind")
        correlation = self.correlation_id
        if correlation is not None and _CORRELATION_ID_PATTERN.fullmatch(correlation) is None:
            raise ConnectorContractError("event correlation id is outside the accepted shape")
        event_details = self.details
        if len(event_details) > MAX_EVENT_DETAILS:
            raise ConnectorContractError("event details exceed the field bound")
        for key, value in event_details.items():
            if type(key) is not str or not key:
                raise ConnectorContractError("event detail keys must be non-empty text")
            if isinstance(value, (bool, int)):
                continue
            if isinstance(value, str):
                if len(value) > MAX_EVENT_DETAIL_TEXT:
                    raise ConnectorContractError("event detail text exceeds the length bound")
                continue
            raise ConnectorContractError("event detail values must be text or integers")


ConnectorObserver = Callable[[ConnectorEvent], None]


@dataclass(slots=True)
class ConnectorEventPublisher:
    """Bounded event publisher that never lets an observer break a call.

    Observer failures are counted and never propagated into connector
    behavior, mirroring the telemetry non-authority discipline of Phase 7.
    """

    _observers: list[ConnectorObserver]
    _failed_observers: int = 0

    def __init__(self, observers: list[ConnectorObserver] | None = None) -> None:
        self._observers = list(observers) if observers is not None else []
        self._failed_observers = 0

    def add_observer(self, observer: ConnectorObserver) -> None:
        if not callable(observer):
            raise ConnectorContractError("connector observers must be callable")
        self._observers.append(observer)

    def failed_observer_count(self) -> int:
        """Return how many observer deliveries have failed."""
        return self._failed_observers

    def publish(self, event: ConnectorEvent) -> None:
        """Deliver one event, isolating and counting observer failures."""
        for observer in tuple(self._observers):
            try:
                observer(event)
            except Exception:
                self._failed_observers += 1


class AsyncSourceConnector(Protocol):
    """The async source adapter surface (P9.2 shape).

    Every I/O path is a coroutine; the adapter never blocks the running
    event loop and re-raises ``asyncio.CancelledError`` after closing its
    owned resources.
    """

    def capabilities(self) -> ConnectorCapabilitiesV1: ...

    def state(self) -> ConnectorState: ...

    async def open_async(self) -> None: ...

    async def read_page_async(
        self,
        cursor: str | None,
        context: ConnectorCallContext,
    ) -> SourcePage: ...

    async def aclose(self) -> None: ...


class BlockingSourceConnector(Protocol):
    """The blocking source adapter surface (P9.3, P9.4, P9.5 shape).

    Calls perform genuinely blocking I/O and refuse to run on an active
    event-loop thread; callers isolate them through the established
    blocking boundary (worker thread or ``asyncio.to_thread``).
    """

    def capabilities(self) -> ConnectorCapabilitiesV1: ...

    def state(self) -> ConnectorState: ...

    def open(self) -> None: ...

    def read_page(
        self,
        cursor: str | None,
        context: ConnectorCallContext,
    ) -> SourcePage: ...

    def close(self) -> None: ...


class TargetConnector(Protocol):
    """The target adapter surface (P9.6 shape).

    Writes are idempotent under the request's idempotency identity:
    replays, retries, and ambiguous-outcome resolution repeat the call
    without repeating the logical effect.
    """

    def capabilities(self) -> ConnectorCapabilitiesV1: ...

    def state(self) -> ConnectorState: ...

    async def open_async(self) -> None: ...

    async def write_record_async(
        self,
        request: TargetWriteRequest,
        context: ConnectorCallContext,
    ) -> TargetWriteOutcome: ...

    async def read_record_async(
        self,
        sku: str,
        context: ConnectorCallContext,
    ) -> TargetRecord | None: ...

    async def state_snapshot_async(
        self,
        context: ConnectorCallContext,
    ) -> TargetStateSnapshot: ...

    async def aclose(self) -> None: ...


def require_no_running_loop(subject: str) -> None:
    """Refuse blocking connector entry points on an active event loop.

    Blocking adapters must be isolated through the established blocking
    boundary (a worker thread or ``asyncio.to_thread``); calling them on
    the loop thread would block every other task and is a contract error,
    not a degraded mode.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    raise ConnectorLoopError(f"{subject} must not run while an event loop is active")


def describe_connector_error(error: ConnectorError) -> str:
    """Render one connector failure as bounded public text."""
    suffix = (
        f"; retry_after={error.retry_after_seconds}s"
        if error.retry_after_seconds is not None
        else ""
    )
    return f"{type(error).__name__}({error.classification.value}): {error.detail}{suffix}"


__all__ = [
    "CONNECTOR_CAPABILITIES_PROTOCOL",
    "CONNECTOR_CONTRACT_VERSION",
    "NEVER_CANCELLED",
    "REDACTION_PLACEHOLDER",
    "AsyncSourceConnector",
    "BlockingSourceConnector",
    "ConnectorAmbiguousError",
    "ConnectorAuthentication",
    "ConnectorCallBounds",
    "ConnectorCallContext",
    "ConnectorCancellationToken",
    "ConnectorCancelledError",
    "ConnectorCapabilitiesV1",
    "ConnectorConflictError",
    "ConnectorContractError",
    "ConnectorError",
    "ConnectorEvent",
    "ConnectorEventKind",
    "ConnectorEventPublisher",
    "ConnectorKind",
    "ConnectorLifecycleError",
    "ConnectorLoopError",
    "ConnectorObserver",
    "ConnectorPermanentError",
    "ConnectorRateLimitedError",
    "ConnectorRetryableError",
    "ConnectorServerFailureError",
    "ConnectorState",
    "ConnectorTimeoutError",
    "ConnectorUnknownError",
    "ConnectorValidationError",
    "EventCancellationToken",
    "FileReadBounds",
    "SourceOutcome",
    "SourcePage",
    "SourceRecord",
    "TargetConnector",
    "TargetEffectOutcome",
    "TargetRecord",
    "TargetStateSnapshot",
    "TargetWriteOutcome",
    "TargetWriteRequest",
    "describe_connector_error",
    "require_no_running_loop",
    "validate_base_url",
    "validate_cursor",
    "validate_idempotency_key",
    "validate_sku",
    "validate_source_record_payload",
]
