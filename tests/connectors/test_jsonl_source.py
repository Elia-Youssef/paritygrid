"""JSON Lines file source connector tests over the Phase 8 fixture contract."""

from pathlib import Path

import pytest

from paritygrid.adapters.connectors import (
    ConnectorCallBounds,
    ConnectorCallContext,
    ConnectorCancelledError,
    ConnectorEvent,
    ConnectorEventKind,
    ConnectorFileError,
    ConnectorKind,
    ConnectorLifecycleError,
    ConnectorLoopError,
    ConnectorObserver,
    ConnectorState,
    ConnectorValidationError,
    EventCancellationToken,
    FileReadBounds,
    JsonlFileSourceConfig,
    JsonlFileSourceConnector,
    SourceFileLocation,
    SourceRecord,
)
from paritygrid.demo.datasets import SyntheticDataset

pytestmark = pytest.mark.anyio

_PAGED = ConnectorCallBounds(max_page_records=4, max_record_bytes=2_048)


def _context() -> ConnectorCallContext:
    return ConnectorCallContext(correlation_id="jsonl-call-1")


def _connector(
    root: Path,
    relative: str = "inventory.jsonl",
    bounds: ConnectorCallBounds = _PAGED,
    *,
    file_bounds: FileReadBounds | None = None,
    observers: list[ConnectorObserver] | None = None,
) -> JsonlFileSourceConnector:
    location = SourceFileLocation.create(root, relative)
    return JsonlFileSourceConnector(
        JsonlFileSourceConfig(location, bounds=bounds, file_bounds=file_bounds),
        observers=observers,
    )


def _collect(connector: JsonlFileSourceConnector) -> list[SourceRecord]:
    records: list[SourceRecord] = []
    cursor: str | None = None
    while True:
        page = connector.read_page(cursor, _context())
        assert len(page.records) <= _PAGED.max_page_records
        records.extend(page.records)
        if page.next_cursor is None:
            return records
        cursor = page.next_cursor


class TestPaginationAndContent:
    def test_streams_every_line_in_order(
        self, fixture_root: Path, connector_dataset: SyntheticDataset
    ) -> None:
        connector = _connector(fixture_root)
        connector.open()
        try:
            records = _collect(connector)
        finally:
            connector.close()
        assert len(records) == len(connector_dataset.rows)
        assert [r.position for r in records] == list(range(len(records)))

    def test_valid_payloads_match_the_fixture_documents(
        self, fixture_root: Path, connector_dataset: SyntheticDataset
    ) -> None:
        connector = _connector(fixture_root)
        connector.open()
        try:
            records = _collect(connector)
        finally:
            connector.close()
        from paritygrid.demo.datasets import RowRole

        expected = [
            dict(row.payload) for row in connector_dataset.rows if row.role is not RowRole.MALFORMED
        ]
        actual = [dict(record.payload or {}) for record in records if not record.is_malformed]
        assert actual == expected

    def test_cursor_continuation_resumes_exact_documents(self, fixture_root: Path) -> None:
        connector = _connector(fixture_root)
        connector.open()
        try:
            first = connector.read_page(None, _context())
            assert first.next_cursor is not None
            resumed = connector.read_page(first.next_cursor, _context())
            repeated = connector.read_page(first.next_cursor, _context())
        finally:
            connector.close()
        assert [r.position for r in resumed.records] == [r.position for r in repeated.records]
        assert [dict(r.payload) for r in resumed.records if r.payload] == [
            dict(r.payload) for r in repeated.records if r.payload
        ]
        assert resumed.records[0].position == len(first.records)

    def test_empty_file_yields_one_exhausted_empty_page(self, fixture_root: Path) -> None:
        (fixture_root / "empty.jsonl").write_bytes(b"")
        connector = _connector(fixture_root, "empty.jsonl")
        connector.open()
        try:
            page = connector.read_page(None, _context())
            assert page.records == ()
            assert page.exhausted
        finally:
            connector.close()

    def test_exact_full_final_page_is_exhausted(self, fixture_root: Path) -> None:
        bounds = ConnectorCallBounds(max_page_records=24, max_record_bytes=2_048)
        connector = _connector(fixture_root, bounds=bounds)
        connector.open()
        try:
            page = connector.read_page(None, _context())
        finally:
            connector.close()
        assert len(page.records) == 24
        assert page.next_cursor is None


class TestMalformedLines:
    def test_fixture_malformed_lines_surface_with_reasons(
        self, fixture_root: Path, connector_dataset: SyntheticDataset
    ) -> None:
        connector = _connector(fixture_root)
        connector.open()
        try:
            records = _collect(connector)
        finally:
            connector.close()
        malformed = [record for record in records if record.is_malformed]
        assert len(malformed) == connector_dataset.profile.malformed_count
        reasons = {record.malformed_reason for record in malformed}
        assert reasons == {
            "json_parse_error: the line is not valid json",
            "empty_line: the line carries no document",
        }

    def test_undecodable_bytes_surface_as_malformed(self, fixture_root: Path) -> None:
        header_line = b'{"sku":"GRID-1"}\n'
        (fixture_root / "bad-encoding.jsonl").write_bytes(header_line + b"\xff\xfe\xfd\n")
        connector = _connector(fixture_root, "bad-encoding.jsonl")
        connector.open()
        try:
            page = connector.read_page(None, _context())
        finally:
            connector.close()
        assert len(page.records) == 2
        assert page.records[0].payload is not None
        assert page.records[1].is_malformed
        assert page.records[1].malformed_reason is not None
        assert "encoding_error" in page.records[1].malformed_reason

    def test_non_object_documents_surface_as_malformed(self, fixture_root: Path) -> None:
        (fixture_root / "array.jsonl").write_bytes(b"[1,2,3]\n")
        connector = _connector(fixture_root, "array.jsonl")
        connector.open()
        try:
            page = connector.read_page(None, _context())
        finally:
            connector.close()
        assert page.records[0].is_malformed
        assert "document_shape" in (page.records[0].malformed_reason or "")


class TestBounds:
    def test_file_above_the_byte_bound_is_rejected_at_open(self, fixture_root: Path) -> None:
        (fixture_root / "big.jsonl").write_bytes(b'{"a":1}\n' + b"x" * 100)
        connector = _connector(
            fixture_root, "big.jsonl", file_bounds=FileReadBounds(max_file_bytes=10, max_rows=10)
        )
        with pytest.raises(ConnectorFileError, match="byte bound"):
            connector.open()
        assert connector.state() is ConnectorState.CLOSED

    def test_row_bound_fails_the_page_when_exceeded(self, fixture_root: Path) -> None:
        connector = _connector(
            fixture_root,
            bounds=ConnectorCallBounds(max_page_records=50, max_record_bytes=2_048),
            file_bounds=FileReadBounds(max_file_bytes=8_388_608, max_rows=3),
        )
        connector.open()
        try:
            with pytest.raises(ConnectorValidationError, match="row bound"):
                _collect(connector)
        finally:
            connector.close()

    def test_single_huge_line_is_rejected_within_the_record_bound(self, fixture_root: Path) -> None:
        (fixture_root / "long.jsonl").write_bytes(b'{"a":"' + b"x" * 5_000 + b'"}\n')
        connector = _connector(fixture_root, "long.jsonl")
        connector.open()
        try:
            with pytest.raises(ConnectorValidationError, match="record bound"):
                connector.read_page(None, _context())
        finally:
            connector.close()

    def test_carriage_returns_reject_the_lf_contract(self, fixture_root: Path) -> None:
        (fixture_root / "crlf.jsonl").write_bytes(b'{"a":1}\r\n{"b":2}\n')
        connector = _connector(fixture_root, "crlf.jsonl")
        connector.open()
        try:
            with pytest.raises(ConnectorValidationError, match="lf"):
                connector.read_page(None, _context())
        finally:
            connector.close()

    def test_missing_file_is_rejected_at_open(self, fixture_root: Path) -> None:
        connector = _connector(fixture_root, "absent.jsonl")
        with pytest.raises(ConnectorFileError):
            connector.open()

    def test_cursor_beyond_end_of_file_is_an_empty_page(self, fixture_root: Path) -> None:
        connector = _connector(fixture_root)
        connector.open()
        try:
            page = connector.read_page("jl:0000000024:0000009999", _context())
            assert page.records == ()
            assert page.exhausted
        finally:
            connector.close()


class TestCancellationAndLifecycle:
    def test_pre_cancelled_token_fails_before_io(self, fixture_root: Path) -> None:
        connector = _connector(fixture_root)
        connector.open()
        try:
            token = EventCancellationToken()
            token.cancel()
            context = ConnectorCallContext(cancellation_token=token)
            with pytest.raises(ConnectorCancelledError):
                connector.read_page(None, context)
        finally:
            connector.close()

    def test_lifecycle_transitions_and_errors(self, fixture_root: Path) -> None:
        connector = _connector(fixture_root)
        assert connector.state() is ConnectorState.CREATED
        with pytest.raises(ConnectorLifecycleError):
            connector.read_page(None, _context())
        connector.open()
        with pytest.raises(ConnectorLifecycleError):
            connector.open()
        connector.close()
        assert connector.state() is ConnectorState.CLOSED
        with pytest.raises(ConnectorLifecycleError):
            connector.read_page(None, _context())
        connector.close()

    def test_failed_open_closes_the_connector(self, fixture_root: Path) -> None:
        connector = _connector(fixture_root, "absent.jsonl")
        with pytest.raises(ConnectorFileError):
            connector.open()
        assert connector.state() is ConnectorState.CLOSED

    async def test_blocking_reads_refuse_an_active_event_loop(self, fixture_root: Path) -> None:
        import asyncio

        connector = _connector(fixture_root)
        with pytest.raises(ConnectorLoopError):
            connector.open()
        await asyncio.to_thread(connector.open)
        try:
            with pytest.raises(ConnectorLoopError):
                connector.read_page(None, _context())
        finally:
            await asyncio.to_thread(connector.close)

    def test_events_cover_lifecycle_and_pages(self, fixture_root: Path) -> None:
        events: list[ConnectorEvent] = []
        connector = _connector(fixture_root, observers=[events.append])
        connector.open()
        connector.read_page(None, _context())
        connector.close()
        kinds = [event.kind for event in events]
        assert ConnectorEventKind.OPENED in kinds
        assert ConnectorEventKind.PAGE_COMPLETED in kinds
        assert ConnectorEventKind.CLOSED in kinds
        assert all(event.connector_kind is ConnectorKind.JSONL_SOURCE for event in events)
