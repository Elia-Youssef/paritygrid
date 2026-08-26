"""Shared wire-response handling for the HTTP source connectors.

Both the async (cursor-paginated) and the blocking (page-numbered)
connector read the same uniform JSON documents from the Phase 8
simulators and must classify transport and status failures identically,
so the parsing, status mapping, and record extraction live here once.
The module owns no I/O and no lifecycle; connectors stay responsible for
cancellation, resources, and observability.
"""

import json
from collections.abc import Mapping
from typing import cast

from paritygrid.adapters.connectors.http_clients import (
    HttpTransportError,
    HttpTransportErrorKind,
)
from paritygrid.application.ports.connector_redaction import (
    SecretMaterial,
    build_public_detail,
)
from paritygrid.application.ports.connectors import (
    ConnectorError,
    ConnectorPermanentError,
    ConnectorRateLimitedError,
    ConnectorRetryableError,
    ConnectorServerFailureError,
    ConnectorTimeoutError,
    ConnectorUnknownError,
    ConnectorValidationError,
    SourceOutcome,
    SourceRecord,
)

_RETRY_AFTER_BOUND_SECONDS = 60


def parse_json_document(body: bytes, *, secrets: SecretMaterial) -> object:
    """Parse one response body as JSON, failing closed on shape errors."""
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ConnectorUnknownError(
            "the source returned a malformed response body",
            detail=build_public_detail(
                "response body is not valid json",
                fragment=body,
                secrets=secrets,
            ),
        ) from error


def classify_status_response(
    status: int,
    *,
    retry_after_text: str | None,
    body: bytes,
    secrets: SecretMaterial,
) -> ConnectorError:
    """Map one non-success HTTP status onto the closed error taxonomy."""
    detail = build_public_detail(
        "the source rejected the request",
        details={"status": status},
        fragment=body,
        secrets=secrets,
    )
    if status == 429:
        retry_after = _parse_retry_after(retry_after_text)
        return ConnectorRateLimitedError(
            "the source rate limited the request",
            detail=detail,
            retry_after_seconds=retry_after,
            secrets=secrets,
        )
    if 500 <= status <= 599:
        return ConnectorServerFailureError(
            "the source reported a server failure",
            detail=detail,
            secrets=secrets,
        )
    if 400 <= status <= 499:
        return ConnectorPermanentError(
            "the source rejected the request permanently",
            detail=detail,
            secrets=secrets,
        )
    return ConnectorUnknownError(
        "the source returned an unexpected status class",
        detail=build_public_detail(
            "unexpected http status class",
            details={"status": status},
            secrets=secrets,
        ),
        secrets=secrets,
    )


def map_transport_error(error: HttpTransportError, *, secrets: SecretMaterial) -> ConnectorError:
    """Translate one transport failure into the closed connector taxonomy."""
    kind = error.kind
    if kind is HttpTransportErrorKind.CONNECT:
        return ConnectorRetryableError(
            "the source connection could not be completed",
            detail=build_public_detail("transport connect failure", secrets=secrets),
            secrets=secrets,
        )
    if kind in (HttpTransportErrorKind.READ_TIMEOUT, HttpTransportErrorKind.CONNECT_TIMEOUT):
        return ConnectorTimeoutError(
            "the source request exceeded its deadline",
            detail=build_public_detail("transport deadline exceeded", secrets=secrets),
            secrets=secrets,
        )
    if kind is HttpTransportErrorKind.CONNECTION_LOST:
        return ConnectorRetryableError(
            "the source connection ended mid-response",
            detail=build_public_detail("transport connection lost", secrets=secrets),
            secrets=secrets,
        )
    if kind is HttpTransportErrorKind.RESPONSE_TOO_LARGE:
        return ConnectorUnknownError(
            "the source response exceeded the configured bound",
            detail=build_public_detail("response exceeded configured bound", secrets=secrets),
            secrets=secrets,
        )
    if kind is HttpTransportErrorKind.CLIENT_CLOSED:
        return ConnectorPermanentError(
            "the connector client is closed",
            detail=build_public_detail("connector client already closed", secrets=secrets),
            secrets=secrets,
        )
    return ConnectorUnknownError(
        "the source response violated the wire contract",
        detail=build_public_detail("transport protocol failure", secrets=secrets),
        secrets=secrets,
    )


def extract_cursor_page(
    document: object,
    *,
    fallback_position: int,
    max_records: int,
    secrets: SecretMaterial,
) -> tuple[tuple[SourceRecord, ...], str | None, int]:
    """Validate and extract one cursor-paginated page document.

    ``fallback_position`` is the connector's running record count; the
    server-declared ``position`` wins when present so a retried page
    reports stable source positions.
    """
    if not isinstance(document, Mapping):
        raise _malformed_page(secrets)
    page_document = cast("Mapping[str, object]", document)
    next_cursor_value = page_document.get("next_cursor")
    records_value = page_document.get("records")
    if not isinstance(next_cursor_value, str) or not isinstance(records_value, list):
        raise _malformed_page(secrets)
    page_records = cast("list[object]", records_value)
    if len(page_records) > max_records:
        raise ConnectorUnknownError(
            "the source page exceeded the configured record bound",
            detail=build_public_detail(
                "page exceeded configured record bound",
                details={"records": len(page_records), "bound": max_records},
                secrets=secrets,
            ),
            secrets=secrets,
        )
    declared_position = page_document.get("position")
    start_position = (
        declared_position
        if isinstance(declared_position, int) and declared_position >= 0
        else fallback_position
    )
    records = tuple(
        _record_from_payload(index, payload, start_position + index, secrets)
        for index, payload in enumerate(page_records)
    )
    next_cursor = next_cursor_value if next_cursor_value else None
    return records, next_cursor, len(page_records)


def extract_numbered_page(
    document: object,
    *,
    page_number: int,
    max_records: int,
    secrets: SecretMaterial,
) -> tuple[tuple[SourceRecord, ...], int | None]:
    """Validate and extract one page-numbered page document.

    Source positions derive from the server-declared page size so a
    source that clamps the requested size still reports stable positions.
    """
    if not isinstance(document, Mapping):
        raise _malformed_page(secrets)
    page_document = cast("Mapping[str, object]", document)
    records_value = page_document.get("records")
    total_pages_value = page_document.get("total_pages")
    page_value = page_document.get("page")
    page_size_value = page_document.get("page_size")
    if not isinstance(records_value, list):
        raise _malformed_page(secrets)
    numbered_records = cast("list[object]", records_value)
    if not isinstance(total_pages_value, int) or not isinstance(page_value, int):
        raise _malformed_page(secrets)
    if not isinstance(page_size_value, int) or page_size_value < 1:
        raise _malformed_page(secrets)
    if page_value != page_number:
        raise ConnectorUnknownError(
            "the source answered with a different page than requested",
            detail=build_public_detail(
                "source page mismatch",
                details={"requested": page_number, "answered": page_value},
                secrets=secrets,
            ),
            secrets=secrets,
        )
    if len(numbered_records) > max_records:
        raise ConnectorUnknownError(
            "the source page exceeded the configured record bound",
            detail=build_public_detail(
                "page exceeded configured record bound",
                details={"records": len(numbered_records), "bound": max_records},
                secrets=secrets,
            ),
            secrets=secrets,
        )
    start_position = (page_number - 1) * page_size_value
    records = tuple(
        _record_from_payload(index, payload, start_position + index, secrets)
        for index, payload in enumerate(numbered_records)
    )
    next_page = page_number + 1 if page_number < total_pages_value else None
    return records, next_page


def _record_from_payload(
    index: int,
    payload: object,
    position: int,
    secrets: SecretMaterial,
) -> SourceRecord:
    if not isinstance(payload, Mapping):
        raise ConnectorUnknownError(
            "the source page carried a non-object record",
            detail=build_public_detail(
                "page record is not an object",
                details={"record_index": index},
                secrets=secrets,
            ),
            secrets=secrets,
        )
    try:
        return SourceRecord(
            position=position,
            outcome=SourceOutcome.VALID,
            payload=cast("Mapping[str, object]", payload),
        )
    except ConnectorValidationError as error:
        raise ConnectorUnknownError(
            "the source record violated the payload contract",
            detail=build_public_detail(
                "record payload failed the connector contract",
                details={"record_index": index, "reason": error.detail},
                secrets=secrets,
            ),
            secrets=secrets,
        ) from error


def _malformed_page(secrets: SecretMaterial) -> ConnectorUnknownError:
    return ConnectorUnknownError(
        "the source page document is malformed",
        detail=build_public_detail("page document shape is invalid", secrets=secrets),
        secrets=secrets,
    )


def _parse_retry_after(retry_after_text: str | None) -> int | None:
    if retry_after_text is None:
        return None
    try:
        value = int(retry_after_text.strip())
    except ValueError:
        return None
    if not 1 <= value <= _RETRY_AFTER_BOUND_SECONDS:
        return None
    return value


__all__ = [
    "classify_status_response",
    "extract_cursor_page",
    "extract_numbered_page",
    "map_transport_error",
    "parse_json_document",
]
