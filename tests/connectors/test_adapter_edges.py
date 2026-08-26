"""Adapter edge-case tests: config validation, open failures, read classes."""

import contextlib
import socket
import threading
from pathlib import Path

import pytest

from paritygrid.adapters.connectors import (
    AsyncHttpSourceConfig,
    AsyncHttpSourceConnector,
    BlockingHttpSourceConfig,
    BlockingHttpSourceConnector,
    ConnectorCallContext,
    ConnectorConfigurationError,
    ConnectorEvent,
    ConnectorEventKind,
    ConnectorFileError,
    ConnectorKind,
    ConnectorServerFailureError,
    ConnectorUnknownError,
    ConnectorValidationError,
    CsvFileSourceConfig,
    CsvFileSourceConnector,
    JsonlFileSourceConfig,
    JsonlFileSourceConnector,
    SourceFileLocation,
    WarehouseTargetConfig,
    WarehouseTargetConnector,
    encode_page_cursor,
)
from paritygrid.adapters.connectors.redaction import SecretMaterial

pytestmark = pytest.mark.anyio

_SECRET = SecretMaterial.empty()


class _OneShotHttpServer:
    """A one-shot raw HTTP server for adversarial target responses."""

    def __init__(self, payload: bytes) -> None:
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind(("127.0.0.1", 0))
        self._socket.listen(1)
        self.port = self._socket.getsockname()[1]
        self._payload = payload
        self._thread = threading.Thread(
            target=self._serve, name="paritygrid-test-edge", daemon=True
        )
        self._thread.start()

    def _serve(self) -> None:

        try:
            connection, _ = self._socket.accept()
            connection.recv(65_536)
            connection.sendall(self._payload)
            connection.close()
        except OSError:
            pass
        finally:
            with contextlib.suppress(OSError):
                self._socket.close()

    def close(self) -> None:
        with contextlib.suppress(OSError):
            wake = socket.create_connection(("127.0.0.1", self.port), timeout=0.25)
            wake.close()
        with contextlib.suppress(OSError):
            self._socket.close()
        self._thread.join(timeout=5)
        assert not self._thread.is_alive()


class TestConfigurationValidation:
    def test_async_config_rejects_wrong_bounds_type(self) -> None:
        with pytest.raises(ConnectorConfigurationError):
            AsyncHttpSourceConfig("http://127.0.0.1:1", bounds="fast")  # type: ignore[arg-type]

    def test_async_connector_rejects_wrong_config_type(self) -> None:
        with pytest.raises(ConnectorConfigurationError):
            AsyncHttpSourceConnector("http://127.0.0.1:1")  # type: ignore[arg-type]

    def test_blocking_config_rejects_wrong_bounds_type(self) -> None:
        with pytest.raises(ConnectorConfigurationError):
            BlockingHttpSourceConfig("http://127.0.0.1:1", bounds=7)  # type: ignore[arg-type]

    def test_blocking_connector_rejects_wrong_config_type(self) -> None:
        with pytest.raises(ConnectorConfigurationError):
            BlockingHttpSourceConnector(9)  # type: ignore[arg-type]

    def test_csv_config_rejects_wrong_bounds_types(self, fixture_root: Path) -> None:
        location = SourceFileLocation.create(fixture_root, "inventory.csv")
        with pytest.raises(ConnectorConfigurationError):
            CsvFileSourceConfig(location, bounds="nope")  # type: ignore[arg-type]
        with pytest.raises(ConnectorConfigurationError):
            CsvFileSourceConfig(location, file_bounds=5)  # type: ignore[arg-type]

    def test_csv_config_rejects_bad_columns(self, fixture_root: Path) -> None:
        location = SourceFileLocation.create(fixture_root, "inventory.csv")
        with pytest.raises(ConnectorConfigurationError):
            CsvFileSourceConfig(location, columns=())
        with pytest.raises(ConnectorConfigurationError):
            CsvFileSourceConfig(location, columns=("",))
        with pytest.raises(ConnectorConfigurationError):
            CsvFileSourceConfig(location, columns=("ok", 5))  # type: ignore[arg-type]

    def test_csv_connector_rejects_wrong_config_type(self) -> None:
        with pytest.raises(ConnectorConfigurationError):
            CsvFileSourceConnector("nope")  # type: ignore[arg-type]

    def test_jsonl_config_and_connector_type_checks(self, fixture_root: Path) -> None:
        location = SourceFileLocation.create(fixture_root, "inventory.jsonl")
        with pytest.raises(ConnectorConfigurationError):
            JsonlFileSourceConfig(location, bounds=object())  # type: ignore[arg-type]
        with pytest.raises(ConnectorConfigurationError):
            JsonlFileSourceConfig(location, file_bounds="x")  # type: ignore[arg-type]

        with pytest.raises(ConnectorConfigurationError):
            JsonlFileSourceConnector(location)  # type: ignore[arg-type]

    def test_warehouse_connector_rejects_wrong_config_type(self) -> None:
        with pytest.raises(ConnectorConfigurationError):
            WarehouseTargetConnector("http://127.0.0.1:1")  # type: ignore[arg-type]

    def test_location_rejects_non_path_root(self, fixture_root: Path) -> None:
        with pytest.raises(ConnectorFileError):
            SourceFileLocation.create(str(fixture_root), "inventory.csv")  # type: ignore[arg-type]


class TestPageCursorBoundaries:
    @pytest.mark.parametrize("page", [0, -1, 2_147_483_648])
    def test_page_cursor_rejects_out_of_range_pages(self, page: int) -> None:
        with pytest.raises(ConnectorValidationError):
            encode_page_cursor(page)


class TestAsyncOpenFailure:
    async def test_open_failure_closes_the_connector_and_emits_event(self) -> None:
        # The port range passes configuration validation but fails client
        # construction, exercising the partial-initialization path.
        events: list[ConnectorEvent] = []
        connector = AsyncHttpSourceConnector(
            AsyncHttpSourceConfig("http://127.0.0.1:99999"),
            observers=[events.append],
        )
        with pytest.raises(ValueError, match="Port out of range"):
            await connector.open_async()
        assert connector.state().value == "closed"
        assert any(event.kind is ConnectorEventKind.OPEN_FAILED for event in events)
        await connector.aclose()

    async def test_open_after_close_is_rejected(self) -> None:
        connector = AsyncHttpSourceConnector(AsyncHttpSourceConfig("http://127.0.0.1:1"))
        await connector.aclose()
        with pytest.raises(Exception, match="closed"):
            await connector.open_async()


class TestTargetReadClassification:
    async def _read_with_response(self, payload: bytes) -> None:
        server = _OneShotHttpServer(payload)
        connector = WarehouseTargetConnector(
            WarehouseTargetConfig(f"http://127.0.0.1:{server.port}")
        )
        await connector.open_async()
        try:
            await connector.read_record_async("GRID-1", ConnectorCallContext())
        finally:
            await connector.aclose()
            server.close()

    async def test_read_5xx_is_a_server_failure(self) -> None:
        with pytest.raises(ConnectorServerFailureError):
            await self._read_with_response(
                b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n"
            )

    async def test_read_unexpected_status_is_permanent(self) -> None:
        from paritygrid.adapters.connectors import ConnectorPermanentError

        with pytest.raises(ConnectorPermanentError):
            await self._read_with_response(b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\n\r\n")

    async def test_read_malformed_body_is_unknown(self) -> None:
        with pytest.raises(ConnectorUnknownError):
            await self._read_with_response(b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nnot json!")

    async def _state_with_response(self, payload: bytes) -> None:
        server = _OneShotHttpServer(payload)
        connector = WarehouseTargetConnector(
            WarehouseTargetConfig(f"http://127.0.0.1:{server.port}")
        )
        await connector.open_async()
        try:
            await connector.state_snapshot_async(ConnectorCallContext())
        finally:
            await connector.aclose()
            server.close()

    async def test_state_malformed_document_is_unknown(self) -> None:
        with pytest.raises(ConnectorUnknownError):
            await self._state_with_response(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ntrue")

    async def test_state_missing_fields_is_unknown(self) -> None:
        with pytest.raises(ConnectorUnknownError):
            await self._state_with_response(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")

    async def test_write_unexpected_status_class_is_unknown(self) -> None:
        from paritygrid.adapters.connectors import TargetWriteRequest

        server = _OneShotHttpServer(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
        connector = WarehouseTargetConnector(
            WarehouseTargetConfig(f"http://127.0.0.1:{server.port}")
        )
        await connector.open_async()
        try:
            with pytest.raises(ConnectorUnknownError):
                await connector.write_record_async(
                    TargetWriteRequest(
                        sku="GRID-1",
                        payload={"sku": "GRID-1"},
                        idempotency_key="k-1",
                    ),
                    ConnectorCallContext(),
                )
        finally:
            await connector.aclose()
            server.close()

    async def test_write_malformed_success_document_is_unknown(self) -> None:
        from paritygrid.adapters.connectors import TargetWriteRequest

        body = b'{"nope": true}'
        head = b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(body)).encode("ascii") + b"\r\n\r\n"
        server = _OneShotHttpServer(head + body)
        connector = WarehouseTargetConnector(
            WarehouseTargetConfig(f"http://127.0.0.1:{server.port}")
        )
        await connector.open_async()
        try:
            with pytest.raises(ConnectorUnknownError):
                await connector.write_record_async(
                    TargetWriteRequest(
                        sku="GRID-1", payload={"sku": "GRID-1"}, idempotency_key="k-2"
                    ),
                    ConnectorCallContext(),
                )
        finally:
            await connector.aclose()
            server.close()


class TestCapabilitiesValidation:
    def test_capabilities_reject_wrong_version(self) -> None:
        from paritygrid.adapters.connectors import (
            CONNECTOR_CAPABILITIES_PROTOCOL,
            ConnectorCapabilitiesV1,
        )
        from paritygrid.application.planner.connectors import (
            ConnectorCapability,
            ConnectorCapabilitySet,
        )

        with pytest.raises(ConnectorConfigurationError):
            ConnectorCapabilitiesV1(
                protocol=CONNECTOR_CAPABILITIES_PROTOCOL,
                contract_version=99,
                kind=ConnectorKind.CSV_SOURCE,
                capabilities=ConnectorCapabilitySet((ConnectorCapability.READ,)),
                max_page_records=1,
                supports_cursors=True,
            )
