"""Simulated warehouse target connector (P9.6).

The connector exercises the Phase 8 warehouse simulator over the async
HTTP engine: idempotent upserts keyed by an ``Idempotency-Key`` header,
plus the read and state observations later phases need. Every write
carries a stable idempotency identity, so retries, replays, and the
resolution of ambiguous outcomes repeat the *call* without repeating the
logical effect.

Outcome discipline is explicit:

- Transport failure **before** the request reached the wire is retryable.
- Transport failure **after** the request was sent — connection loss or
  timeout mid-response — is ambiguous: classification is ``unknown``,
  never a guess, and the documented resolution is replaying the same
  idempotency identity, which the target answers from its recorded
  outcome without a second effect.
- A 409 idempotency conflict is a terminal conflict: the key was reused
  for a different logical request.
"""

import json
import re
from collections.abc import Mapping
from hashlib import sha256
from typing import cast

from paritygrid.adapters.connectors.http_clients import (
    AsyncHttpClient,
    HttpResponse,
    HttpTransportError,
    HttpTransportErrorKind,
)
from paritygrid.application.planner.connectors import ConnectorCapability, ConnectorCapabilitySet
from paritygrid.application.ports.connector_redaction import (
    SecretMaterial,
    build_public_detail,
)
from paritygrid.application.ports.connectors import (
    CONNECTOR_CAPABILITIES_PROTOCOL,
    CONNECTOR_CONTRACT_VERSION,
    ConnectorAmbiguousError,
    ConnectorAuthentication,
    ConnectorCallBounds,
    ConnectorCallContext,
    ConnectorCapabilitiesV1,
    ConnectorConfigurationError,
    ConnectorConflictError,
    ConnectorError,
    ConnectorEvent,
    ConnectorEventKind,
    ConnectorEventPublisher,
    ConnectorKind,
    ConnectorLifecycleError,
    ConnectorObserver,
    ConnectorPermanentError,
    ConnectorRateLimitedError,
    ConnectorRetryableError,
    ConnectorServerFailureError,
    ConnectorState,
    ConnectorTimeoutError,
    ConnectorUnknownError,
    ConnectorValidationError,
    TargetEffectOutcome,
    TargetRecord,
    TargetRecordPage,
    TargetStateSnapshot,
    TargetWriteOutcome,
    TargetWriteRequest,
    validate_base_url,
    validate_sku,
)

_IDEMPOTENCY_PREFIX_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,31}", flags=re.ASCII)

WAREHOUSE_RECORDS_PREFIX = "/v1/records"
WAREHOUSE_STATE_PATH = "/v1/state"


class WarehouseTargetConfig:
    """Validated configuration for the warehouse target connector."""

    __slots__ = ("base_url", "bounds", "idempotency_prefix")

    def __init__(
        self,
        base_url: str,
        *,
        bounds: ConnectorCallBounds | None = None,
        idempotency_prefix: str = "pg-write",
    ) -> None:
        self.base_url = validate_base_url(base_url)
        self.bounds = bounds if bounds is not None else ConnectorCallBounds()
        if (
            type(idempotency_prefix) is not str
            or _IDEMPOTENCY_PREFIX_PATTERN.fullmatch(idempotency_prefix) is None
        ):
            raise ConnectorConfigurationError("idempotency prefix is outside the accepted shape")
        self.idempotency_prefix = idempotency_prefix

    def __repr__(self) -> str:
        return f"WarehouseTargetConfig(base_url={self.base_url!r})"


def derive_idempotency_key(prefix: str, request: TargetWriteRequest) -> str:
    """Derive the stable idempotency identity of one logical write.

    The key binds the addressed record, exact payload bytes, and conditional
    target-state predicate.  A write with another precondition is therefore
    another logical request and cannot silently reuse an old receipt.
    """
    canonical = json.dumps(
        dict(request.payload), ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    precondition = "" if request.precondition is None else request.precondition.header_value()
    digest = sha256(canonical + b"\0" + precondition.encode("ascii")).hexdigest()[:32]
    return f"{prefix}:{request.sku}:{digest}"


class WarehouseTargetConnector:
    """Idempotent target adapter over the simulated warehouse."""

    def __init__(
        self,
        config: WarehouseTargetConfig,
        *,
        authentication: ConnectorAuthentication | None = None,
        observers: list[ConnectorObserver] | None = None,
    ) -> None:
        if type(config) is not WarehouseTargetConfig:
            raise ConnectorConfigurationError("configuration must use WarehouseTargetConfig")
        self._config = config
        self._authentication = authentication
        self._secrets = (
            authentication.secret_material() if authentication is not None else SecretMaterial()
        )
        self._events = ConnectorEventPublisher(observers)
        self._state = ConnectorState.CREATED
        self._client: AsyncHttpClient | None = None

    def capabilities(self) -> ConnectorCapabilitiesV1:
        """Return the immutable capability metadata for this kind."""
        return ConnectorCapabilitiesV1(
            protocol=CONNECTOR_CAPABILITIES_PROTOCOL,
            contract_version=CONNECTOR_CONTRACT_VERSION,
            kind=ConnectorKind.WAREHOUSE_TARGET,
            capabilities=ConnectorCapabilitySet(
                (
                    ConnectorCapability.READ,
                    ConnectorCapability.WRITE,
                    ConnectorCapability.ASYNC_IO,
                    ConnectorCapability.IDEMPOTENCY,
                )
            ),
            max_page_records=self._config.bounds.max_page_records,
            supports_cursors=True,
        )

    def state(self) -> ConnectorState:
        """Return the current lifecycle state."""
        return self._state

    async def open_async(self) -> None:
        """Prepare the owned client; failures leave the connector closed."""
        if self._state is ConnectorState.OPEN:
            raise ConnectorLifecycleError("the connector is already open")
        if self._state is ConnectorState.CLOSED:
            raise ConnectorLifecycleError("the connector is closed")
        try:
            self._client = AsyncHttpClient(
                self._config.base_url,
                max_response_bytes=self._config.bounds.max_response_bytes,
            )
            self._state = ConnectorState.OPEN
        except Exception:
            self._client = None
            self._state = ConnectorState.CLOSED
            self._events.publish(
                ConnectorEvent(
                    kind=ConnectorEventKind.OPEN_FAILED,
                    connector_kind=ConnectorKind.WAREHOUSE_TARGET,
                    correlation_id=None,
                    details={"reason": "client construction failed"},
                )
            )
            raise
        self._events.publish(
            ConnectorEvent(
                kind=ConnectorEventKind.OPENED,
                connector_kind=ConnectorKind.WAREHOUSE_TARGET,
                correlation_id=None,
                details={},
            )
        )

    async def write_record_async(
        self,
        request: TargetWriteRequest,
        context: ConnectorCallContext,
    ) -> TargetWriteOutcome:
        """Apply one idempotent upsert and observe its logical effect."""
        client = self._require_open()
        secrets = SecretMaterial.combine(self._secrets, context.secrets)
        context.raise_if_cancelled()
        path = f"{WAREHOUSE_RECORDS_PREFIX}/{request.sku}"
        body = json.dumps(
            dict(request.payload), ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": request.idempotency_key,
        }
        if request.precondition is not None:
            headers["X-ParityGrid-Target-Precondition"] = request.precondition.header_value()
        if self._authentication is not None:
            headers[self._authentication.header_name] = self._authentication.header_value()
        self._events.publish(
            ConnectorEvent(
                kind=ConnectorEventKind.CALL_STARTED,
                connector_kind=ConnectorKind.WAREHOUSE_TARGET,
                correlation_id=context.correlation_id,
                details={"operation": "write_record"},
            )
        )
        try:
            response = await client.request(
                "PUT",
                path,
                headers=headers,
                body=body,
                timeout_seconds=self._config.bounds.request_timeout_microseconds / 1_000_000,
            )
        except HttpTransportError as error:
            connector_error = self._classify_write_transport_error(error, secrets)
            self._events.publish(
                ConnectorEvent(
                    kind=ConnectorEventKind.CALL_FAILED,
                    connector_kind=ConnectorKind.WAREHOUSE_TARGET,
                    correlation_id=context.correlation_id,
                    details={"classification": connector_error.classification.value},
                )
            )
            raise connector_error from error
        context.raise_if_cancelled()
        if response.status != 200:
            connector_error = self._classify_write_status(response, secrets)
            self._events.publish(
                ConnectorEvent(
                    kind=ConnectorEventKind.CALL_FAILED,
                    connector_kind=ConnectorKind.WAREHOUSE_TARGET,
                    correlation_id=context.correlation_id,
                    details={"classification": connector_error.classification.value},
                )
            )
            raise connector_error
        document = self._parse_document(response.body, secrets)
        outcome, record_version, target_version, replayed = _decode_write_document(document)
        effect = (
            TargetEffectOutcome.REPLAYED
            if replayed
            else (
                TargetEffectOutcome.APPLIED
                if outcome == "applied"
                else TargetEffectOutcome.UNCHANGED
            )
        )
        result = TargetWriteOutcome(
            outcome=effect,
            record_version=record_version,
            target_version=target_version,
            request_count=1,
        )
        self._events.publish(
            ConnectorEvent(
                kind=ConnectorEventKind.TARGET_EFFECT_OBSERVED,
                connector_kind=ConnectorKind.WAREHOUSE_TARGET,
                correlation_id=context.correlation_id,
                details={
                    "effect": effect.value,
                    "record_version": record_version,
                    "target_version": target_version,
                },
            )
        )
        return result

    async def read_record_async(
        self,
        sku: str,
        context: ConnectorCallContext,
    ) -> TargetRecord | None:
        """Read one stored record; ``None`` when the target has none."""
        client = self._require_open()
        secrets = SecretMaterial.combine(self._secrets, context.secrets)
        validate_sku(sku)
        context.raise_if_cancelled()
        headers: dict[str, str] = {}
        if self._authentication is not None:
            headers[self._authentication.header_name] = self._authentication.header_value()
        try:
            response = await client.request(
                "GET",
                f"{WAREHOUSE_RECORDS_PREFIX}/{sku}",
                headers=headers,
                timeout_seconds=self._config.bounds.request_timeout_microseconds / 1_000_000,
            )
        except HttpTransportError as error:
            raise self._classify_read_transport_error(error, secrets) from error
        if response.status == 404:
            return None
        if response.status != 200:
            raise self._classify_read_status(response, secrets)
        document = self._parse_document(response.body, secrets)
        try:
            return _decode_target_record(sku, document)
        except ConnectorValidationError as error:
            raise ConnectorUnknownError(
                "the target record document violated the contract",
                detail=build_public_detail(
                    "record document failed the connector contract",
                    details={"reason": error.detail},
                    secrets=secrets,
                ),
                secrets=secrets,
            ) from error

    async def list_records_async(
        self,
        cursor: str | None,
        context: ConnectorCallContext,
    ) -> TargetRecordPage:
        """Read one bounded target-inventory page for independent verification."""
        client = self._require_open()
        secrets = SecretMaterial.combine(self._secrets, context.secrets)
        if cursor is not None:
            from paritygrid.application.ports.connectors import validate_cursor

            validate_cursor(cursor)
        context.raise_if_cancelled()
        query = f"?limit={self._config.bounds.max_page_records}"
        if cursor is not None:
            query += f"&cursor={cursor}"
        headers: dict[str, str] = {}
        if self._authentication is not None:
            headers[self._authentication.header_name] = self._authentication.header_value()
        try:
            response = await client.request(
                "GET",
                f"{WAREHOUSE_RECORDS_PREFIX}{query}",
                headers=headers,
                timeout_seconds=self._config.bounds.request_timeout_microseconds / 1_000_000,
            )
        except HttpTransportError as error:
            raise self._classify_read_transport_error(error, secrets) from error
        context.raise_if_cancelled()
        if response.status != 200:
            raise self._classify_read_status(response, secrets)
        document = self._parse_document(response.body, secrets)
        try:
            page = _decode_target_page(document, byte_count=len(response.body))
        except ConnectorValidationError as error:
            raise ConnectorUnknownError(
                "the target record page violated the contract",
                detail=build_public_detail(
                    "record page failed the connector contract",
                    details={"reason": error.detail},
                    secrets=secrets,
                ),
                secrets=secrets,
            ) from error
        self._events.publish(
            ConnectorEvent(
                kind=ConnectorEventKind.PAGE_COMPLETED,
                connector_kind=ConnectorKind.WAREHOUSE_TARGET,
                correlation_id=context.correlation_id,
                details={"record_count": len(page.records)},
            )
        )
        return page

    async def state_snapshot_async(
        self,
        context: ConnectorCallContext,
    ) -> TargetStateSnapshot:
        """Read the observable target state summary."""
        client = self._require_open()
        secrets = SecretMaterial.combine(self._secrets, context.secrets)
        context.raise_if_cancelled()
        headers: dict[str, str] = {}
        if self._authentication is not None:
            headers[self._authentication.header_name] = self._authentication.header_value()
        try:
            response = await client.request(
                "GET",
                WAREHOUSE_STATE_PATH,
                headers=headers,
                timeout_seconds=self._config.bounds.request_timeout_microseconds / 1_000_000,
            )
        except HttpTransportError as error:
            raise self._classify_read_transport_error(error, secrets) from error
        if response.status != 200:
            raise self._classify_read_status(response, secrets)
        document = self._parse_document(response.body, secrets)
        if not isinstance(document, dict):
            raise ConnectorUnknownError(
                "the target state document is malformed",
                detail=build_public_detail("state document shape is invalid", secrets=secrets),
                secrets=secrets,
            )
        state_document = cast("dict[str, object]", document)
        try:
            return TargetStateSnapshot(
                record_count=_document_int(state_document, "record_count"),
                target_version=_document_int(state_document, "target_version"),
                content_fingerprint=_document_text(state_document, "content_fingerprint"),
                capacity=_document_int(state_document, "capacity"),
            )
        except ConnectorValidationError as error:
            raise ConnectorUnknownError(
                "the target state document violated the contract",
                detail=build_public_detail(
                    "state document failed the connector contract",
                    details={"reason": error.detail},
                    secrets=secrets,
                ),
                secrets=secrets,
            ) from error

    async def aclose(self) -> None:
        """Close the owned client; safe after every prior outcome."""
        client = self._client
        self._client = None
        if self._state is ConnectorState.CLOSED:
            return
        self._state = ConnectorState.CLOSED
        if client is not None:
            await client.aclose()
        self._events.publish(
            ConnectorEvent(
                kind=ConnectorEventKind.CLOSED,
                connector_kind=ConnectorKind.WAREHOUSE_TARGET,
                correlation_id=None,
                details={},
            )
        )

    def _require_open(self) -> AsyncHttpClient:
        if self._state is not ConnectorState.OPEN or self._client is None:
            raise ConnectorLifecycleError("the connector is not open")
        return self._client

    def _parse_document(self, body: bytes, secrets: SecretMaterial) -> object:
        try:
            return json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ConnectorUnknownError(
                "the target returned a malformed response body",
                detail=build_public_detail(
                    "response body is not valid json", fragment=body, secrets=secrets
                ),
                secrets=secrets,
            ) from error

    def _classify_write_transport_error(
        self, error: HttpTransportError, secrets: SecretMaterial
    ) -> ConnectorError:
        # After the request bytes were handed to the transport, no local
        # signal can prove whether the effect committed, so everything
        # short of a pre-send failure is ambiguous rather than retryable.
        if error.kind is HttpTransportErrorKind.CONNECT:
            return ConnectorRetryableError(
                "the target connection could not be completed",
                detail=build_public_detail("transport connect failure", secrets=secrets),
                secrets=secrets,
            )
        if error.kind is HttpTransportErrorKind.CONNECT_TIMEOUT:
            return ConnectorTimeoutError(
                "the target connect phase exceeded its deadline",
                detail=build_public_detail("transport deadline exceeded", secrets=secrets),
                secrets=secrets,
            )
        if error.kind is HttpTransportErrorKind.READ_TIMEOUT:
            return ConnectorAmbiguousError(
                "the target write outcome is unknown after a timeout",
                detail=build_public_detail(
                    "write outcome unknown: request sent, response timed out",
                    secrets=secrets,
                ),
                secrets=secrets,
            )
        if error.kind is HttpTransportErrorKind.CONNECTION_LOST:
            return ConnectorAmbiguousError(
                "the target write outcome is unknown after connection loss",
                detail=build_public_detail(
                    "write outcome unknown: connection lost mid-response",
                    secrets=secrets,
                ),
                secrets=secrets,
            )
        return ConnectorUnknownError(
            "the target exchange violated the wire contract",
            detail=build_public_detail("transport protocol failure", secrets=secrets),
            secrets=secrets,
        )

    def _classify_read_transport_error(
        self, error: HttpTransportError, secrets: SecretMaterial
    ) -> ConnectorError:
        if error.kind is HttpTransportErrorKind.CONNECT:
            return ConnectorRetryableError(
                "the target connection could not be completed",
                detail=build_public_detail("transport connect failure", secrets=secrets),
                secrets=secrets,
            )
        if error.kind in (
            HttpTransportErrorKind.READ_TIMEOUT,
            HttpTransportErrorKind.CONNECT_TIMEOUT,
        ):
            return ConnectorTimeoutError(
                "the target read exceeded its deadline",
                detail=build_public_detail("transport deadline exceeded", secrets=secrets),
                secrets=secrets,
            )
        if error.kind is HttpTransportErrorKind.CONNECTION_LOST:
            return ConnectorRetryableError(
                "the target connection ended mid-response",
                detail=build_public_detail("transport connection lost", secrets=secrets),
                secrets=secrets,
            )
        return ConnectorUnknownError(
            "the target response violated the wire contract",
            detail=build_public_detail("transport protocol failure", secrets=secrets),
            secrets=secrets,
        )

    def _classify_write_status(
        self, response: HttpResponse, secrets: SecretMaterial
    ) -> ConnectorError:
        status = response.status
        retry_after = response.headers.get("retry-after")
        detail = build_public_detail(
            "the target rejected the write",
            details={"status": status},
            fragment=response.body,
            secrets=secrets,
        )
        if status == 429:
            retry_seconds = _parse_retry_after(retry_after)
            return ConnectorRateLimitedError(
                "the target rate limited the write",
                detail=detail,
                retry_after_seconds=retry_seconds,
                secrets=secrets,
            )
        if status == 409:
            if _target_error_code(response.body) == "target_precondition_failed":
                return ConnectorConflictError(
                    "the target no longer satisfies the write precondition",
                    detail=detail,
                    secrets=secrets,
                )
            return ConnectorConflictError(
                "the idempotency key was reused with a different request",
                detail=detail,
                secrets=secrets,
            )
        if 500 <= status <= 599:
            return ConnectorServerFailureError(
                "the target reported a server failure",
                detail=detail,
                secrets=secrets,
            )
        if 400 <= status <= 499:
            return ConnectorPermanentError(
                "the target rejected the write permanently",
                detail=detail,
                secrets=secrets,
            )
        return ConnectorUnknownError(
            "the target returned an unexpected status class",
            detail=build_public_detail(
                "unexpected http status class", details={"status": status}, secrets=secrets
            ),
            secrets=secrets,
        )

    def _classify_read_status(
        self, response: HttpResponse, secrets: SecretMaterial
    ) -> ConnectorError:
        status = response.status
        if 500 <= status <= 599:
            return ConnectorServerFailureError(
                "the target reported a server failure",
                detail=build_public_detail(
                    "the target rejected the read",
                    details={"status": status},
                    secrets=secrets,
                ),
                secrets=secrets,
            )
        return ConnectorPermanentError(
            "the target rejected the read permanently",
            detail=build_public_detail(
                "the target rejected the read",
                details={"status": status},
                secrets=secrets,
            ),
            secrets=secrets,
        )


def _parse_retry_after(retry_after_text: str | None) -> int | None:
    if retry_after_text is None:
        return None
    try:
        value = int(retry_after_text.strip())
    except ValueError:
        return None
    if not 1 <= value <= 60:
        return None
    return value


def _target_error_code(body: bytes) -> str | None:
    """Return one known bounded target error code without trusting its message."""
    try:
        document = json.loads(body.decode("utf-8"))
    except UnicodeDecodeError, json.JSONDecodeError:
        return None
    if not isinstance(document, dict):
        return None
    error = cast("dict[str, object]", document).get("error")
    if not isinstance(error, dict):
        return None
    code = cast("dict[str, object]", error).get("code")
    return (
        code
        if isinstance(code, str) and code in {"idempotency_conflict", "target_precondition_failed"}
        else None
    )


def _decode_write_document(document: object) -> tuple[str, int, int, bool]:
    if not isinstance(document, dict):
        raise ConnectorUnknownError("the target write document is malformed")
    write_document = cast("dict[str, object]", document)
    outcome = write_document.get("outcome")
    replayed = write_document.get("replayed")
    record_version = write_document.get("record_version")
    target_version = write_document.get("target_version")
    if (
        not isinstance(outcome, str)
        or outcome not in ("applied", "unchanged")
        or not isinstance(replayed, bool)
        or not isinstance(record_version, int)
        or not isinstance(target_version, int)
    ):
        raise ConnectorUnknownError("the target write document is malformed")
    return outcome, record_version, target_version, replayed


def _decode_target_record(sku: str, document: object) -> TargetRecord:
    if not isinstance(document, dict):
        raise ConnectorUnknownError("the target record document is malformed")
    record_document = cast("dict[str, object]", document)
    payload = record_document.get("payload")
    record_version = record_document.get("record_version")
    target_version = record_document.get("target_version")
    if (
        not isinstance(payload, dict)
        or not isinstance(record_version, int)
        or not isinstance(target_version, int)
    ):
        raise ConnectorUnknownError("the target record document is malformed")
    return TargetRecord(
        sku=sku,
        payload=cast("Mapping[str, object]", payload),
        record_version=record_version,
        target_version=target_version,
    )


def _decode_target_page(document: object, *, byte_count: int) -> TargetRecordPage:
    if not isinstance(document, dict):
        raise ConnectorValidationError("target record page is malformed")
    page_document = cast("dict[str, object]", document)
    records_value = page_document.get("records")
    cursor_value = page_document.get("next_cursor")
    if not isinstance(records_value, list) or not isinstance(cursor_value, str):
        raise ConnectorValidationError("target record page is malformed")
    records: list[TargetRecord] = []
    for record_document in cast("list[object]", records_value):
        if not isinstance(record_document, dict):
            raise ConnectorValidationError("target record page is malformed")
        record_mapping = cast("dict[str, object]", record_document)
        sku = record_mapping.get("sku")
        if not isinstance(sku, str):
            raise ConnectorValidationError("target record page is malformed")
        records.append(_decode_target_record(sku, record_mapping))
    return TargetRecordPage(
        records=tuple(records),
        next_cursor=None if cursor_value == "" else cursor_value,
        request_count=1,
        byte_count=byte_count,
    )


def _document_int(document: dict[str, object], key: str) -> int:
    value = document.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConnectorValidationError(f"state field {key} must be an integer")
    return value


def _document_text(document: dict[str, object], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str):
        raise ConnectorValidationError(f"state field {key} must be text")
    return value


__all__ = [
    "WAREHOUSE_RECORDS_PREFIX",
    "WAREHOUSE_STATE_PATH",
    "WarehouseTargetConfig",
    "WarehouseTargetConnector",
    "derive_idempotency_key",
]
