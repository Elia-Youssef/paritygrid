"""Targeted branch tests lifting the connectors package over 90% coverage."""

from pathlib import Path
from typing import cast

import pytest

from paritygrid.adapters.connectors import (
    AsyncHttpSourceConfig,
    AsyncHttpSourceConnector,
    ConnectorCallBounds,
    ConnectorCallContext,
    ConnectorCancelledError,
    ConnectorConfigurationError,
    ConnectorContractError,
    ConnectorEvent,
    ConnectorLifecycleError,
    ConnectorRetryableError,
    ConnectorTimeoutError,
    ConnectorUnknownError,
    ConnectorValidationError,
    CsvFileSourceConfig,
    CsvFileSourceConnector,
    EventCancellationToken,
    FileReadBounds,
    SourceFileLocation,
    SourceOutcome,
    SourcePage,
    SourceRecord,
    TargetRecord,
    WarehouseTargetConfig,
    WarehouseTargetConnector,
)
from paritygrid.adapters.connectors.http_clients import (
    HttpTransportError,
    HttpTransportErrorKind,
)
from paritygrid.adapters.connectors.redaction import SecretMaterial

# These tests exercise classification helpers directly; the helpers are
# module-private implementation details of the connector boundary.
# pyright: reportPrivateUsage=false

pytestmark = pytest.mark.anyio

_SECRET = SecretMaterial.empty()
_CONTEXT = ConnectorCallContext()


class TestTargetTransportClassification:
    @pytest.mark.parametrize(
        ("kind", "expected"),
        [
            (HttpTransportErrorKind.CONNECT, ConnectorRetryableError),
            (HttpTransportErrorKind.READ_TIMEOUT, ConnectorTimeoutError),
            (HttpTransportErrorKind.CONNECT_TIMEOUT, ConnectorTimeoutError),
            (HttpTransportErrorKind.CONNECTION_LOST, ConnectorRetryableError),
            (HttpTransportErrorKind.PROTOCOL, ConnectorUnknownError),
        ],
    )
    async def test_read_transport_kinds_classify(
        self, kind: HttpTransportErrorKind, expected: type
    ) -> None:
        connector = WarehouseTargetConnector(WarehouseTargetConfig("http://127.0.0.1:1"))
        error = connector._classify_read_transport_error(HttpTransportError(kind, "unit"), _SECRET)
        assert type(error) is expected

    async def test_warehouse_open_failure_closes_and_emits(self) -> None:
        events: list[ConnectorEvent] = []
        connector = WarehouseTargetConnector(
            WarehouseTargetConfig("http://127.0.0.1:99999"), observers=[events.append]
        )
        with pytest.raises(ValueError, match="Port out of range"):
            await connector.open_async()
        assert connector.state().value == "closed"
        assert any(event.kind.value == "open_failed" for event in events)

    async def test_warehouse_open_after_close_is_rejected(self) -> None:
        connector = WarehouseTargetConnector(WarehouseTargetConfig("http://127.0.0.1:1"))
        await connector.aclose()
        with pytest.raises(ConnectorLifecycleError, match="closed"):
            await connector.open_async()

    @pytest.mark.parametrize(
        "kind",
        [
            HttpTransportErrorKind.CONNECT,
            HttpTransportErrorKind.CONNECT_TIMEOUT,
            HttpTransportErrorKind.READ_TIMEOUT,
            HttpTransportErrorKind.CONNECTION_LOST,
            HttpTransportErrorKind.PROTOCOL,
        ],
    )
    async def test_write_transport_kinds_classify(self, kind: HttpTransportErrorKind) -> None:
        connector = WarehouseTargetConnector(WarehouseTargetConfig("http://127.0.0.1:1"))
        error = connector._classify_write_transport_error(HttpTransportError(kind, "unit"), _SECRET)
        assert "target" in str(error)


class TestSourcePageValidation:
    def test_page_rejects_negative_counters(self) -> None:
        with pytest.raises(ConnectorValidationError):
            SourcePage(
                records=(),
                next_cursor=None,
                request_count=-1,
                byte_count=0,
            )

    def test_page_rejects_non_tuple_records(self) -> None:
        with pytest.raises(ConnectorValidationError):
            SourcePage(
                records=[],  # type: ignore[arg-type]
                next_cursor=None,
                request_count=1,
                byte_count=0,
            )

    def test_valid_record_position_rejects_wrong_type(self) -> None:
        with pytest.raises(ConnectorValidationError):
            SourceRecord(position=True, outcome=SourceOutcome.VALID, payload={"a": 1})  # type: ignore[arg-type]

    def test_malformed_record_rejects_payload(self) -> None:
        with pytest.raises(ConnectorValidationError):
            SourceRecord(
                position=0,
                outcome=SourceOutcome.MALFORMED,
                payload={"a": 1},
                malformed_reason="bad",
            )

    def test_valid_record_rejects_reason(self) -> None:
        with pytest.raises(ConnectorValidationError):
            SourceRecord(
                position=0,
                outcome=SourceOutcome.VALID,
                payload={"a": 1},
                malformed_reason="unexpected",
            )

    def test_negative_position_is_rejected(self) -> None:
        with pytest.raises(ConnectorValidationError):
            SourceRecord(position=-1, outcome=SourceOutcome.VALID, payload={"a": 1})

    def test_target_state_rejects_negative_values(self) -> None:
        with pytest.raises(ConnectorValidationError):
            TargetRecord(
                sku="GRID-1", payload={"sku": "GRID-1"}, record_version=-1, target_version=1
            )

    def test_context_rejects_bad_correlation_shape(self) -> None:
        with pytest.raises(ConnectorConfigurationError):
            ConnectorCallContext(correlation_id="bad correlation")

    def test_event_rejects_bad_connector_kind(self) -> None:
        from paritygrid.adapters.connectors import ConnectorEventKind, ConnectorKind

        with pytest.raises(ConnectorContractError):
            ConnectorEvent(
                kind=ConnectorEventKind.CLOSED,
                connector_kind="csv_source",  # type: ignore[arg-type]
                correlation_id=None,
                details={},
            )
        with pytest.raises(ConnectorContractError):
            ConnectorEvent(
                kind=ConnectorEventKind.CLOSED,
                connector_kind=ConnectorKind.CSV_SOURCE,
                correlation_id="bad correlation",
                details={},
            )

    def test_bounds_and_file_bounds_validation(self) -> None:
        with pytest.raises(ConnectorConfigurationError):
            ConnectorCallBounds(max_record_bytes=0)
        with pytest.raises(ConnectorConfigurationError):
            ConnectorCallBounds(max_page_records=10**9)
        with pytest.raises(ConnectorConfigurationError):
            FileReadBounds(max_file_bytes=0)
        with pytest.raises(ConnectorConfigurationError):
            FileReadBounds(max_rows=10**9)

    def test_authentication_rejects_bad_shapes(self) -> None:
        from paritygrid.adapters.connectors import ConnectorAuthentication

        with pytest.raises(ConnectorConfigurationError):
            ConnectorAuthentication(header_name="2 Bad", token="x")
        with pytest.raises(ConnectorConfigurationError):
            ConnectorAuthentication(scheme="2fast", token="x")
        with pytest.raises(ConnectorConfigurationError):
            ConnectorAuthentication(scheme="Bearer", token="")


class TestCsvEdgeBranches:
    def _write(self, root: Path, name: str, content: str) -> None:
        (root / name).write_bytes(content.encode("utf-8"))

    def test_blank_lines_are_skipped_without_records(self, fixture_root: Path) -> None:
        header = ",".join(
            (
                "source_record_key",
                "sku",
                "name",
                "quantity",
                "currency",
                "amount",
                "updated_at",
                "attributes",
            )
        )
        self._write(fixture_root, "blanks.csv", header + "\n\n\nGRID-1,x\n")
        connector = CsvFileSourceConnector(
            CsvFileSourceConfig(SourceFileLocation.create(fixture_root, "blanks.csv"))
        )
        connector.open()
        try:
            page = connector.read_page(None, _CONTEXT)
        finally:
            connector.close()
        assert len(page.records) == 1

    def test_csv_cursor_beyond_end_is_empty(self, fixture_root: Path) -> None:
        connector = CsvFileSourceConnector(
            CsvFileSourceConfig(SourceFileLocation.create(fixture_root, "inventory.csv"))
        )
        connector.open()
        try:
            page = connector.read_page("rows:0000000099", _CONTEXT)
            assert page.records == ()
            assert page.exhausted
        finally:
            connector.close()

    def test_csv_cursor_bounds_are_enforced(self, fixture_root: Path) -> None:
        connector = CsvFileSourceConnector(
            CsvFileSourceConfig(SourceFileLocation.create(fixture_root, "inventory.csv"))
        )
        connector.open()
        try:
            with pytest.raises(ConnectorValidationError):
                connector.read_page("rows:9999999999", _CONTEXT)
        finally:
            connector.close()

    def test_csv_empty_header_line_fails(self, fixture_root: Path) -> None:
        self._write(fixture_root, "empty-header.csv", "\n")
        connector = CsvFileSourceConnector(
            CsvFileSourceConfig(SourceFileLocation.create(fixture_root, "empty-header.csv"))
        )
        connector.open()
        try:
            with pytest.raises(ConnectorValidationError):
                connector.read_page(None, _CONTEXT)
        finally:
            connector.close()


class TestJsonlEdgeBranches:
    def test_jsonl_cursor_rejects_foreign_shapes(self, fixture_root: Path) -> None:
        connector = CsvFileSourceConnector(
            CsvFileSourceConfig(SourceFileLocation.create(fixture_root, "inventory.csv"))
        )
        connector.open()
        try:
            with pytest.raises(ConnectorValidationError):
                connector.read_page("rows:", _CONTEXT)
        finally:
            connector.close()


class TestAsyncSourceEdges:
    async def test_read_after_double_close_raises(self) -> None:
        connector = AsyncHttpSourceConnector(AsyncHttpSourceConfig("http://127.0.0.1:1"))
        await connector.open_async()
        await connector.aclose()
        with pytest.raises(ConnectorLifecycleError, match="not open"):
            await connector.read_page_async(None, _CONTEXT)

    async def test_invalid_cursor_shape_fails_before_io(self) -> None:
        connector = AsyncHttpSourceConnector(AsyncHttpSourceConfig("http://127.0.0.1:1"))
        await connector.open_async()
        try:
            with pytest.raises(ConnectorValidationError):
                await connector.read_page_async("bad cursor!", _CONTEXT)
        finally:
            await connector.aclose()

    async def test_cancelled_token_after_response_fails(self) -> None:
        connector = AsyncHttpSourceConnector(AsyncHttpSourceConfig("http://127.0.0.1:1"))
        await connector.open_async()
        token = EventCancellationToken()
        context = ConnectorCallContext(cancellation_token=token)
        token.cancel()
        with pytest.raises(ConnectorCancelledError):
            await connector.read_page_async(None, context)
        await connector.aclose()


class TestWarehouseHelpers:
    def test_target_error_and_page_helpers_reject_malformed_wire_documents(self) -> None:
        from paritygrid.adapters.connectors.warehouse_target import (
            _decode_target_page,
            _target_error_code,
        )

        assert _target_error_code(b'{"error":{"code":"target_precondition_failed"}}') == (
            "target_precondition_failed"
        )
        assert _target_error_code(b'{"error":{"code":"untrusted"}}') is None
        assert _target_error_code(b"not-json") is None
        assert _target_error_code(b"[]") is None
        assert _target_error_code(b'{"error":"not-an-object"}') is None
        invalid_documents: tuple[object, ...] = (
            None,
            cast(object, {"records": "not-a-list", "next_cursor": ""}),
            cast(object, {"records": ["not-an-object"], "next_cursor": ""}),
            cast(object, {"records": [{}], "next_cursor": ""}),
        )
        for document in invalid_documents:
            with pytest.raises(ConnectorValidationError):
                _decode_target_page(document, byte_count=1)

    @pytest.mark.parametrize("status", [503, 400, 418])
    def test_target_read_status_classes_are_explicit(self, status: int) -> None:
        from paritygrid.adapters.connectors.http_clients import HttpResponse

        connector = WarehouseTargetConnector(WarehouseTargetConfig("http://127.0.0.1:1"))
        error = connector._classify_read_status(HttpResponse(status, {}, b"{}"), _SECRET)
        assert isinstance(error, (ConnectorUnknownError, ConnectorTimeoutError)) is False

    def test_target_write_status_distinguishes_precondition_conflict(self) -> None:
        from paritygrid.adapters.connectors.http_clients import HttpResponse

        connector = WarehouseTargetConnector(WarehouseTargetConfig("http://127.0.0.1:1"))
        error = connector._classify_write_status(
            HttpResponse(409, {}, b'{"error":{"code":"target_precondition_failed"}}'),
            _SECRET,
        )
        assert "precondition" in str(error)

    @pytest.mark.parametrize("status", [500, 400, 200])
    def test_target_write_status_handles_every_remaining_status_class(self, status: int) -> None:
        from paritygrid.adapters.connectors.http_clients import HttpResponse

        connector = WarehouseTargetConnector(WarehouseTargetConfig("http://127.0.0.1:1"))
        error = connector._classify_write_status(HttpResponse(status, {}, b"{}"), _SECRET)
        assert "target" in str(error)

    @pytest.mark.parametrize(
        "document",
        [
            None,
            {"outcome": "invalid", "replayed": False, "record_version": 1, "target_version": 1},
            {"outcome": "applied", "replayed": "false", "record_version": 1, "target_version": 1},
            {"outcome": "applied", "replayed": False, "record_version": "1", "target_version": 1},
            {"outcome": "applied", "replayed": False, "record_version": 1, "target_version": "1"},
        ],
    )
    def test_write_document_rejects_each_untrusted_field(self, document: object) -> None:
        from paritygrid.adapters.connectors.warehouse_target import _decode_write_document

        with pytest.raises(ConnectorUnknownError):
            _decode_write_document(document)

    def test_parse_retry_after_accepts_only_bounded_integers(self) -> None:
        from paritygrid.adapters.connectors.warehouse_target import _parse_retry_after

        assert _parse_retry_after("7") == 7
        assert _parse_retry_after(" 9 ") == 9
        for invalid in (None, "soon", "", "0", "61", "-3"):
            assert _parse_retry_after(invalid) is None

    def test_document_field_validators_reject_wrong_types(self) -> None:
        from paritygrid.adapters.connectors.warehouse_target import (
            _document_int,
            _document_text,
        )

        with pytest.raises(ConnectorValidationError):
            _document_int({"capacity": "many"}, "capacity")
        with pytest.raises(ConnectorValidationError):
            _document_int({"capacity": True}, "capacity")
        with pytest.raises(ConnectorValidationError):
            _document_text({"fingerprint": 5}, "fingerprint")

    async def test_authenticated_reads_send_the_header_without_leaking(self) -> None:
        from paritygrid.adapters.connectors import (
            ConnectorAuthentication,
            TargetWriteRequest,
        )
        from paritygrid.demo.simulators.warehouse import SimulatedWarehouse

        token = "tok_read_secret_9"
        sim = SimulatedWarehouse()
        await sim.start()
        try:
            connector = WarehouseTargetConnector(
                WarehouseTargetConfig(sim.base_url),
                authentication=ConnectorAuthentication(token=token),
            )
            await connector.open_async()
            try:
                await connector.write_record_async(
                    TargetWriteRequest(
                        sku="GRID-1", payload={"sku": "GRID-1"}, idempotency_key="k-1"
                    ),
                    _CONTEXT,
                )
                record = await connector.read_record_async("GRID-1", _CONTEXT)
                state = await connector.state_snapshot_async(_CONTEXT)
                assert record is not None
                assert record.sku == "GRID-1"
                assert state.record_count == 1
            finally:
                await connector.aclose()
        finally:
            await sim.aclose()

    async def test_state_transport_failure_classifies(self) -> None:
        connector = WarehouseTargetConnector(WarehouseTargetConfig("http://127.0.0.1:99999"))
        with pytest.raises(ValueError, match="Port out of range"):
            await connector.open_async()

    async def test_read_transport_error_paths(self) -> None:
        import socket

        held = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        held.bind(("127.0.0.1", 0))
        port = held.getsockname()[1]
        try:
            connector = WarehouseTargetConnector(WarehouseTargetConfig(f"http://127.0.0.1:{port}"))
            await connector.open_async()
            try:
                with pytest.raises((ConnectorRetryableError, ConnectorTimeoutError)):
                    await connector.read_record_async("GRID-1", _CONTEXT)
                with pytest.raises((ConnectorRetryableError, ConnectorTimeoutError)):
                    await connector.state_snapshot_async(_CONTEXT)
            finally:
                await connector.aclose()
        finally:
            held.close()


class TestPayloadListBranch:
    def test_payload_accepts_bounded_lists(self) -> None:
        record = SourceRecord(
            position=0,
            outcome=SourceOutcome.VALID,
            payload={"tags": ["a", "b"], "nested": {"deep": ["x"]}},
        )
        assert record.payload is not None
        with pytest.raises(ConnectorValidationError):
            SourceRecord(
                position=0,
                outcome=SourceOutcome.VALID,
                payload={"tags": ["a"] * 65},
            )
