"""Blocking page-numbered HTTP source connector (P9.3).

The connector targets the Phase 8 blocking legacy source simulator:
page-numbered reads over ``GET /v1/inventory/pages/{n}``. Its calls are
genuinely blocking and refuse to run on an active event-loop thread —
callers isolate the adapter through the established blocking boundary
(a worker thread or ``asyncio.to_thread``), exactly like the legacy
systems it models. Cursor text is the one-based page number, so page
identities are stable across retries and replays.
"""

from urllib.parse import urlencode

from paritygrid.adapters.connectors.http_clients import (
    BlockingHttpClient,
    HttpTransportError,
)
from paritygrid.adapters.connectors.source_wire import (
    classify_status_response,
    extract_numbered_page,
    map_transport_error,
    parse_json_document,
)
from paritygrid.application.planner.connectors import ConnectorCapability, ConnectorCapabilitySet
from paritygrid.application.ports.connector_redaction import SecretMaterial
from paritygrid.application.ports.connectors import (
    CONNECTOR_CAPABILITIES_PROTOCOL,
    CONNECTOR_CONTRACT_VERSION,
    ConnectorAuthentication,
    ConnectorCallBounds,
    ConnectorCallContext,
    ConnectorCapabilitiesV1,
    ConnectorConfigurationError,
    ConnectorEvent,
    ConnectorEventKind,
    ConnectorEventPublisher,
    ConnectorKind,
    ConnectorLifecycleError,
    ConnectorObserver,
    ConnectorState,
    ConnectorValidationError,
    SourcePage,
    require_no_running_loop,
    validate_base_url,
    validate_cursor,
)

BLOCKING_SOURCE_PAGES_PREFIX = "/v1/inventory/pages"
_MAX_PAGE_NUMBER = 2_147_483_647


class BlockingHttpSourceConfig:
    """Validated configuration for the blocking HTTP source connector."""

    __slots__ = ("base_url", "bounds")

    def __init__(self, base_url: str, bounds: ConnectorCallBounds | None = None) -> None:
        self.base_url = validate_base_url(base_url)
        self.bounds = bounds if bounds is not None else ConnectorCallBounds()
        if type(self.bounds) is not ConnectorCallBounds:
            raise ConnectorConfigurationError("bounds must use ConnectorCallBounds")

    def __repr__(self) -> str:
        return f"BlockingHttpSourceConfig(base_url={self.base_url!r})"


def encode_page_cursor(page_number: int) -> str:
    """Encode one one-based page number as its opaque cursor text."""
    if type(page_number) is not int:
        raise ConnectorValidationError("page number must be an integer")
    if not 1 <= page_number <= _MAX_PAGE_NUMBER:
        raise ConnectorValidationError("page number is outside the supported range")
    return f"page:{page_number:010d}"


def decode_page_cursor(cursor: str) -> int:
    """Decode one page cursor produced by :func:`encode_page_cursor`."""
    if not cursor.startswith("page:") or not cursor[5:].isdigit():
        raise ConnectorValidationError("cursor is not a page cursor")
    page = int(cursor[5:])
    if page < 1:
        raise ConnectorValidationError("pages are numbered from one")
    return page


class BlockingHttpSourceConnector:
    """Page-numbered blocking source adapter over the legacy client engine."""

    def __init__(
        self,
        config: BlockingHttpSourceConfig,
        *,
        authentication: ConnectorAuthentication | None = None,
        observers: list[ConnectorObserver] | None = None,
    ) -> None:
        if type(config) is not BlockingHttpSourceConfig:
            raise ConnectorConfigurationError("configuration must use BlockingHttpSourceConfig")
        self._config = config
        self._authentication = authentication
        self._secrets = (
            authentication.secret_material() if authentication is not None else SecretMaterial()
        )
        self._events = ConnectorEventPublisher(observers)
        self._state = ConnectorState.CREATED
        self._client: BlockingHttpClient | None = None

    def capabilities(self) -> ConnectorCapabilitiesV1:
        """Return the immutable capability metadata for this kind."""
        return ConnectorCapabilitiesV1(
            protocol=CONNECTOR_CAPABILITIES_PROTOCOL,
            contract_version=CONNECTOR_CONTRACT_VERSION,
            kind=ConnectorKind.BLOCKING_HTTP_SOURCE,
            capabilities=ConnectorCapabilitySet(
                (ConnectorCapability.READ, ConnectorCapability.BLOCKING_IO)
            ),
            max_page_records=self._config.bounds.max_page_records,
            supports_cursors=True,
        )

    def state(self) -> ConnectorState:
        """Return the current lifecycle state."""
        return self._state

    def open(self) -> None:
        """Prepare the owned blocking client; failures close the connector."""
        require_no_running_loop("blocking source open")
        if self._state is ConnectorState.OPEN:
            raise ConnectorLifecycleError("the connector is already open")
        if self._state is ConnectorState.CLOSED:
            raise ConnectorLifecycleError("the connector is closed")
        try:
            self._client = BlockingHttpClient(
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
                    connector_kind=ConnectorKind.BLOCKING_HTTP_SOURCE,
                    correlation_id=None,
                    details={"reason": "client construction failed"},
                )
            )
            raise
        self._events.publish(
            ConnectorEvent(
                kind=ConnectorEventKind.OPENED,
                connector_kind=ConnectorKind.BLOCKING_HTTP_SOURCE,
                correlation_id=None,
                details={},
            )
        )

    def read_page(
        self,
        cursor: str | None,
        context: ConnectorCallContext,
    ) -> SourcePage:
        """Read one bounded numbered page with genuinely blocking I/O."""
        require_no_running_loop("blocking source read_page")
        client = self._require_open()
        page_number = 1 if cursor is None else decode_page_cursor(validate_cursor(cursor))
        context.raise_if_cancelled()
        secrets = SecretMaterial.combine(self._secrets, context.secrets)
        query = urlencode({"page_size": self._config.bounds.max_page_records})
        path = f"{BLOCKING_SOURCE_PAGES_PREFIX}/{page_number}?{query}"
        headers: dict[str, str] = {}
        if self._authentication is not None:
            headers[self._authentication.header_name] = self._authentication.header_value()
        self._events.publish(
            ConnectorEvent(
                kind=ConnectorEventKind.CALL_STARTED,
                connector_kind=ConnectorKind.BLOCKING_HTTP_SOURCE,
                correlation_id=context.correlation_id,
                details={"operation": "read_page", "page": page_number},
            )
        )
        try:
            response = client.request(
                "GET",
                path,
                headers=headers,
                timeout_seconds=self._config.bounds.request_timeout_microseconds / 1_000_000,
                cancellation_token=context.cancellation_token,
            )
        except HttpTransportError as error:
            connector_error = map_transport_error(error, secrets=secrets)
            self._events.publish(
                ConnectorEvent(
                    kind=ConnectorEventKind.CALL_FAILED,
                    connector_kind=ConnectorKind.BLOCKING_HTTP_SOURCE,
                    correlation_id=context.correlation_id,
                    details={"classification": connector_error.classification.value},
                )
            )
            raise connector_error from error
        context.raise_if_cancelled()
        if response.status != 200:
            connector_error = classify_status_response(
                response.status,
                retry_after_text=response.headers.get("retry-after"),
                body=response.body,
                secrets=secrets,
            )
            self._events.publish(
                ConnectorEvent(
                    kind=ConnectorEventKind.CALL_FAILED,
                    connector_kind=ConnectorKind.BLOCKING_HTTP_SOURCE,
                    correlation_id=context.correlation_id,
                    details={"classification": connector_error.classification.value},
                )
            )
            raise connector_error
        document = parse_json_document(response.body, secrets=secrets)
        records, next_page = extract_numbered_page(
            document,
            page_number=page_number,
            max_records=self._config.bounds.max_page_records,
            secrets=secrets,
        )
        page = SourcePage(
            records=records,
            next_cursor=encode_page_cursor(next_page) if next_page is not None else None,
            request_count=1,
            byte_count=len(response.body),
        )
        self._events.publish(
            ConnectorEvent(
                kind=ConnectorEventKind.PAGE_COMPLETED,
                connector_kind=ConnectorKind.BLOCKING_HTTP_SOURCE,
                correlation_id=context.correlation_id,
                details={
                    "records": len(records),
                    "bytes": page.byte_count,
                    "page": page_number,
                    "exhausted": 1 if page.exhausted else 0,
                },
            )
        )
        return page

    def close(self) -> None:
        """Close the owned blocking client; safe after every prior outcome."""
        client = self._client
        self._client = None
        if self._state is ConnectorState.CLOSED:
            return
        self._state = ConnectorState.CLOSED
        if client is not None:
            client.close()
        self._events.publish(
            ConnectorEvent(
                kind=ConnectorEventKind.CLOSED,
                connector_kind=ConnectorKind.BLOCKING_HTTP_SOURCE,
                correlation_id=None,
                details={},
            )
        )

    def _require_open(self) -> BlockingHttpClient:
        if self._state is not ConnectorState.OPEN or self._client is None:
            raise ConnectorLifecycleError("the connector is not open")
        return self._client


__all__ = [
    "BLOCKING_SOURCE_PAGES_PREFIX",
    "BlockingHttpSourceConfig",
    "BlockingHttpSourceConnector",
    "decode_page_cursor",
    "encode_page_cursor",
]
