"""Async cursor-paginated HTTP source connector (P9.2).

The connector targets the Phase 8 async source simulator: bounded cursor
pagination over ``GET /v1/inventory``. Every I/O path is asynchronous —
the adapter never blocks the running event loop — and ``asyncio`` task
cancellation propagates unchanged after the owned client releases its
connection. The adapter knows nothing about which runner scheduled the
call: it receives a cursor and a call context and returns one bounded
page or one typed, redacted failure.
"""

from dataclasses import dataclass, field
from urllib.parse import urlencode

from paritygrid.adapters.connectors.http_clients import AsyncHttpClient, HttpTransportError
from paritygrid.adapters.connectors.source_wire import (
    classify_status_response,
    extract_cursor_page,
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
    SourcePage,
    validate_base_url,
    validate_cursor,
)

ASYNC_SOURCE_INVENTORY_PATH = "/v1/inventory"


@dataclass(frozen=True, slots=True)
class AsyncHttpSourceConfig:
    """Validated configuration for the async HTTP source connector."""

    base_url: str
    bounds: ConnectorCallBounds = field(default_factory=ConnectorCallBounds)

    def __post_init__(self) -> None:
        validate_base_url(self.base_url)
        if type(self.bounds) is not ConnectorCallBounds:
            raise ConnectorConfigurationError("bounds must use ConnectorCallBounds")


class AsyncHttpSourceConnector:
    """Cursor-paginated source adapter over the asyncio client engine."""

    def __init__(
        self,
        config: AsyncHttpSourceConfig,
        *,
        authentication: ConnectorAuthentication | None = None,
        observers: list[ConnectorObserver] | None = None,
    ) -> None:
        if type(config) is not AsyncHttpSourceConfig:
            raise ConnectorConfigurationError("configuration must use AsyncHttpSourceConfig")
        self._config = config
        self._authentication = authentication
        self._secrets = (
            authentication.secret_material() if authentication is not None else SecretMaterial()
        )
        self._events = ConnectorEventPublisher(observers)
        self._state = ConnectorState.CREATED
        self._client: AsyncHttpClient | None = None
        self._positions_read = 0

    def capabilities(self) -> ConnectorCapabilitiesV1:
        """Return the immutable capability metadata for this kind."""
        return ConnectorCapabilitiesV1(
            protocol=CONNECTOR_CAPABILITIES_PROTOCOL,
            contract_version=CONNECTOR_CONTRACT_VERSION,
            kind=ConnectorKind.ASYNC_HTTP_SOURCE,
            capabilities=ConnectorCapabilitySet(
                (ConnectorCapability.READ, ConnectorCapability.ASYNC_IO)
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
            # Partial initialization owns nothing once the constructor
            # raised, so closing here is a pure state transition.
            self._client = None
            self._state = ConnectorState.CLOSED
            self._events.publish(
                ConnectorEvent(
                    kind=ConnectorEventKind.OPEN_FAILED,
                    connector_kind=ConnectorKind.ASYNC_HTTP_SOURCE,
                    correlation_id=None,
                    details={"reason": "client construction failed"},
                )
            )
            raise
        self._events.publish(
            ConnectorEvent(
                kind=ConnectorEventKind.OPENED,
                connector_kind=ConnectorKind.ASYNC_HTTP_SOURCE,
                correlation_id=None,
                details={},
            )
        )

    async def read_page_async(
        self,
        cursor: str | None,
        context: ConnectorCallContext,
    ) -> SourcePage:
        """Read one bounded cursor page without blocking the event loop."""
        client = self._require_open()
        if cursor is not None:
            validate_cursor(cursor)
        context.raise_if_cancelled()
        secrets = SecretMaterial.combine(self._secrets, context.secrets)
        query = urlencode({"cursor": cursor or "", "limit": self._config.bounds.max_page_records})
        path = f"{ASYNC_SOURCE_INVENTORY_PATH}?{query}"
        headers: dict[str, str] = {}
        if self._authentication is not None:
            headers[self._authentication.header_name] = self._authentication.header_value()
        self._events.publish(
            ConnectorEvent(
                kind=ConnectorEventKind.CALL_STARTED,
                connector_kind=ConnectorKind.ASYNC_HTTP_SOURCE,
                correlation_id=context.correlation_id,
                details={"operation": "read_page"},
            )
        )
        try:
            response = await client.request(
                "GET",
                path,
                headers=headers,
                timeout_seconds=self._config.bounds.request_timeout_microseconds / 1_000_000,
            )
        except HttpTransportError as error:
            connector_error = map_transport_error(error, secrets=secrets)
            self._events.publish(
                ConnectorEvent(
                    kind=ConnectorEventKind.CALL_FAILED,
                    connector_kind=ConnectorKind.ASYNC_HTTP_SOURCE,
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
                    connector_kind=ConnectorKind.ASYNC_HTTP_SOURCE,
                    correlation_id=context.correlation_id,
                    details={"classification": connector_error.classification.value},
                )
            )
            raise connector_error
        document = parse_json_document(response.body, secrets=secrets)
        records, next_cursor, record_count = extract_cursor_page(
            document,
            fallback_position=self._positions_read,
            max_records=self._config.bounds.max_page_records,
            secrets=secrets,
        )
        self._positions_read += record_count
        page = SourcePage(
            records=records,
            next_cursor=next_cursor,
            request_count=1,
            byte_count=len(response.body),
        )
        self._events.publish(
            ConnectorEvent(
                kind=ConnectorEventKind.PAGE_COMPLETED,
                connector_kind=ConnectorKind.ASYNC_HTTP_SOURCE,
                correlation_id=context.correlation_id,
                details={
                    "records": len(records),
                    "bytes": page.byte_count,
                    "exhausted": 1 if page.exhausted else 0,
                },
            )
        )
        return page

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
                connector_kind=ConnectorKind.ASYNC_HTTP_SOURCE,
                correlation_id=None,
                details={},
            )
        )

    def _require_open(self) -> AsyncHttpClient:
        if self._state is not ConnectorState.OPEN or self._client is None:
            raise ConnectorLifecycleError("the connector is not open")
        return self._client


__all__ = [
    "ASYNC_SOURCE_INVENTORY_PATH",
    "AsyncHttpSourceConfig",
    "AsyncHttpSourceConnector",
]
