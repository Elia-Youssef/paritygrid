"""Source wire helper and target decode unit tests for the failure taxonomy."""

from typing import cast

import pytest

from paritygrid.adapters.connectors import (
    ConnectorConfigurationError,
    ConnectorPermanentError,
    ConnectorRateLimitedError,
    ConnectorRetryableError,
    ConnectorServerFailureError,
    ConnectorTimeoutError,
    ConnectorUnknownError,
    SourceOutcome,
    SourceRecord,
    TargetEffectOutcome,
    TargetRecord,
    TargetWriteRequest,
    WarehouseTargetConfig,
    derive_idempotency_key,
)
from paritygrid.adapters.connectors.http_clients import (
    HttpTransportError,
    HttpTransportErrorKind,
)
from paritygrid.adapters.connectors.redaction import SecretMaterial
from paritygrid.adapters.connectors.source_wire import (
    classify_status_response,
    extract_cursor_page,
    extract_numbered_page,
    map_transport_error,
    parse_json_document,
)
from paritygrid.adapters.connectors.warehouse_target import (
    _decode_target_record,
    _decode_write_document,
)

# pyright: reportPrivateUsage=false

pytestmark = pytest.mark.anyio

_SECRETS = SecretMaterial.empty()


def _transport(kind: HttpTransportErrorKind) -> HttpTransportError:
    return HttpTransportError(kind, "unit")


class TestStatusClassification:
    def test_429_parses_server_retry_delay(self) -> None:
        error = classify_status_response(429, retry_after_text="5", body=b"{}", secrets=_SECRETS)
        assert isinstance(error, ConnectorRateLimitedError)
        assert error.retry_after_seconds == 5

    def test_429_without_usable_delay_keeps_none(self) -> None:
        for text in (None, "soon", "0", "999"):
            error = classify_status_response(429, retry_after_text=text, body=b"", secrets=_SECRETS)
            assert error.retry_after_seconds is None

    def test_5xx_is_server_failure(self) -> None:
        error = classify_status_response(503, retry_after_text=None, body=b"", secrets=_SECRETS)
        assert isinstance(error, ConnectorServerFailureError)

    def test_4xx_is_permanent(self) -> None:
        error = classify_status_response(404, retry_after_text=None, body=b"", secrets=_SECRETS)
        assert isinstance(error, ConnectorPermanentError)

    def test_unexpected_class_is_unknown(self) -> None:
        error = classify_status_response(301, retry_after_text=None, body=b"", secrets=_SECRETS)
        assert isinstance(error, ConnectorUnknownError)

    def test_status_detail_redacts_fragment_secrets(self) -> None:
        secrets = SecretMaterial(("tok_status_secret",))
        error = classify_status_response(
            400, retry_after_text=None, body=b"failed for tok_status_secret", secrets=secrets
        )
        assert "tok_status_secret" not in error.detail


class TestTransportClassification:
    @pytest.mark.parametrize(
        ("kind", "error_type"),
        [
            (HttpTransportErrorKind.CONNECT, ConnectorRetryableError),
            (HttpTransportErrorKind.CONNECT_TIMEOUT, ConnectorTimeoutError),
            (HttpTransportErrorKind.READ_TIMEOUT, ConnectorTimeoutError),
            (HttpTransportErrorKind.CONNECTION_LOST, ConnectorRetryableError),
            (HttpTransportErrorKind.RESPONSE_TOO_LARGE, ConnectorUnknownError),
            (HttpTransportErrorKind.CLIENT_CLOSED, ConnectorPermanentError),
            (HttpTransportErrorKind.PROTOCOL, ConnectorUnknownError),
        ],
    )
    def test_every_transport_kind_maps_once(
        self, kind: HttpTransportErrorKind, error_type: type
    ) -> None:
        error = map_transport_error(_transport(kind), secrets=_SECRETS)
        assert isinstance(error, error_type)


class TestJsonParsing:
    def test_valid_document_parses(self) -> None:
        assert parse_json_document(b'{"a": 1}', secrets=_SECRETS) == {"a": 1}

    @pytest.mark.parametrize("body", [b"not json", b"", b"\xff\xfe", b'{"records": ['])
    def test_invalid_documents_fail_closed(self, body: bytes) -> None:
        with pytest.raises(ConnectorUnknownError):
            parse_json_document(body, secrets=_SECRETS)


class TestCursorPageExtraction:
    def _record(self, index: int) -> dict[str, object]:
        return {"sku": f"GRID-{index}", "quantity": index}

    def test_cursor_page_extracts_records_and_cursor(self) -> None:
        document = {
            "next_cursor": "next",
            "position": 4,
            "records": [self._record(0), self._record(1)],
        }
        records, cursor, count = extract_cursor_page(
            document, fallback_position=0, max_records=10, secrets=_SECRETS
        )
        assert cursor == "next"
        assert count == 2
        assert [r.position for r in records] == [4, 5]

    def test_cursor_page_uses_fallback_position_when_absent(self) -> None:
        document = {"next_cursor": "", "records": [self._record(0)]}
        records, cursor, _ = extract_cursor_page(
            document, fallback_position=7, max_records=10, secrets=_SECRETS
        )
        assert cursor is None
        assert records[0].position == 7

    @pytest.mark.parametrize(
        "document",
        [
            "not-a-mapping",
            {"records": []},
            {"next_cursor": "x", "records": "no"},
            {"next_cursor": 5, "records": []},
        ],
    )
    def test_cursor_page_rejects_malformed_documents(self, document: object) -> None:
        with pytest.raises(ConnectorUnknownError):
            extract_cursor_page(document, fallback_position=0, max_records=10, secrets=_SECRETS)

    def test_cursor_page_rejects_oversized_pages(self) -> None:
        document: dict[str, object] = {
            "next_cursor": "",
            "records": [self._record(i) for i in range(3)],
        }
        with pytest.raises(ConnectorUnknownError):
            extract_cursor_page(document, fallback_position=0, max_records=2, secrets=_SECRETS)

    def test_cursor_page_rejects_non_object_records(self) -> None:
        document: dict[str, object] = {"next_cursor": "", "records": ["not-an-object"]}
        with pytest.raises(ConnectorUnknownError):
            extract_cursor_page(document, fallback_position=0, max_records=5, secrets=_SECRETS)


class TestNumberedPageExtraction:
    def test_numbered_page_extracts_records_and_next_page(self) -> None:
        document: dict[str, object] = {"page": 2, "page_size": 5, "total_pages": 4, "records": [{}]}
        records, next_page = extract_numbered_page(
            document, page_number=2, max_records=10, secrets=_SECRETS
        )
        assert next_page == 3
        assert records[0].position == 5

    def test_final_numbered_page_reports_no_continuation(self) -> None:
        document: dict[str, object] = {"page": 4, "page_size": 5, "total_pages": 4, "records": []}
        _, next_page = extract_numbered_page(
            document, page_number=4, max_records=10, secrets=_SECRETS
        )
        assert next_page is None

    @pytest.mark.parametrize(
        "document",
        [
            "nope",
            {"page": 1, "page_size": 5, "total_pages": 1},
            {"page": 1, "records": [], "total_pages": 1},
            {"page": 1, "records": [], "page_size": 0, "total_pages": 1},
            {"page": 9, "page_size": 5, "total_pages": 9, "records": []},
        ],
    )
    def test_numbered_page_rejects_malformed_documents(self, document: object) -> None:
        with pytest.raises(ConnectorUnknownError):
            extract_numbered_page(document, page_number=1, max_records=10, secrets=_SECRETS)

    def test_numbered_page_rejects_oversized_pages(self) -> None:
        document = {
            "page": 1,
            "page_size": 5,
            "total_pages": 9,
            "records": [{"i": i} for i in range(4)],
        }
        with pytest.raises(ConnectorUnknownError):
            extract_numbered_page(document, page_number=1, max_records=2, secrets=_SECRETS)


class TestTargetDecode:
    def test_write_document_decodes_applied(self) -> None:
        outcome, record_version, target_version, replayed = _decode_write_document(
            {"outcome": "applied", "record_version": 2, "target_version": 5, "replayed": False}
        )
        assert (outcome, record_version, target_version, replayed) == ("applied", 2, 5, False)

    @pytest.mark.parametrize(
        "document",
        [
            "nope",
            {},
            {"outcome": "applied"},
            {"outcome": "weird", "record_version": 1, "target_version": 1, "replayed": False},
            {"outcome": "applied", "record_version": "x", "target_version": 1, "replayed": False},
        ],
    )
    def test_write_document_rejects_malformed(self, document: dict[str, object] | str) -> None:
        with pytest.raises(ConnectorUnknownError):
            _decode_write_document(cast("object", document))

    def test_target_record_decodes(self) -> None:
        record = _decode_target_record(
            "GRID-1", {"payload": {"sku": "GRID-1"}, "record_version": 1, "target_version": 1}
        )
        assert isinstance(record, TargetRecord)
        assert record.sku == "GRID-1"

    @pytest.mark.parametrize(
        "document",
        [
            "nope",
            {},
            {"payload": {}, "record_version": 1},
            {"payload": [], "record_version": 1, "target_version": 1},
        ],
    )
    def test_target_record_rejects_malformed(self, document: object) -> None:
        with pytest.raises(ConnectorUnknownError):
            _decode_target_record("GRID-1", document)


class TestTargetConfig:
    def test_config_defaults_and_repr(self) -> None:
        config = WarehouseTargetConfig("http://127.0.0.1:9000")
        assert config.idempotency_prefix == "pg-write"
        assert "9000" in repr(config)

    @pytest.mark.parametrize("prefix", ["", "-bad", "x" * 33, "has space"])
    def test_config_rejects_bad_prefixes(self, prefix: str) -> None:
        with pytest.raises(ConnectorConfigurationError):
            WarehouseTargetConfig("http://127.0.0.1:9000", idempotency_prefix=prefix)

    def test_derived_key_is_stable_and_bounded(self) -> None:
        request = TargetWriteRequest(
            sku="GRID-1", payload={"sku": "GRID-1", "n": 1}, idempotency_key="k"
        )
        key = derive_idempotency_key("pg", request)
        assert key == derive_idempotency_key("pg", request)
        assert len(key) <= 128
        assert key.startswith("pg:GRID-1:")


class TestRecordRepr:
    def test_malformed_record_carries_reason(self) -> None:
        record = SourceRecord(
            position=0, outcome=SourceOutcome.MALFORMED, payload=None, malformed_reason="bad"
        )
        assert record.is_malformed
        assert record.malformed_reason == "bad"

    def test_target_effect_outcomes_are_closed(self) -> None:
        assert {outcome.value for outcome in TargetEffectOutcome} == {
            "applied",
            "unchanged",
            "replayed",
        }
