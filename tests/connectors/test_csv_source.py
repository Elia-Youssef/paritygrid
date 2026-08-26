"""CSV file source connector tests over the Phase 8 fixture contract."""

from pathlib import Path

import pytest

from paritygrid.adapters.connectors import (
    CSV_SOURCE_COLUMNS,
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
    ConnectorTimeoutError,
    ConnectorValidationError,
    CsvFileSourceConfig,
    CsvFileSourceConnector,
    EventCancellationToken,
    FileReadBounds,
    SourceFileLocation,
    SourceRecord,
)
from paritygrid.demo.datasets import SyntheticDataset

pytestmark = pytest.mark.anyio

_PAGED = ConnectorCallBounds(max_page_records=4, max_record_bytes=2_048)


def _context() -> ConnectorCallContext:
    return ConnectorCallContext(correlation_id="csv-call-1")


def _connector(
    root: Path,
    relative: str = "inventory.csv",
    bounds: ConnectorCallBounds = _PAGED,
    *,
    columns: tuple[str, ...] = CSV_SOURCE_COLUMNS,
    file_bounds: FileReadBounds | None = None,
    observers: list[ConnectorObserver] | None = None,
) -> CsvFileSourceConnector:
    location = SourceFileLocation.create(root, relative)
    return CsvFileSourceConnector(
        CsvFileSourceConfig(location, bounds=bounds, columns=columns, file_bounds=file_bounds),
        observers=observers,
    )


def _collect(connector: CsvFileSourceConnector) -> list[SourceRecord]:
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
    def test_streams_every_row_in_order(
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

    def test_payload_columns_match_the_fixture_contract(self, fixture_root: Path) -> None:
        connector = _connector(fixture_root)
        connector.open()
        try:
            page = connector.read_page(None, _context())
        finally:
            connector.close()
        valid = next(record for record in page.records if not record.is_malformed)
        assert valid.payload is not None
        assert set(valid.payload) == set(CSV_SOURCE_COLUMNS)
        assert valid.payload["sku"] == "MERIDIAN-0000-FE23"

    def test_cursor_continuation_resumes_exact_rows(self, fixture_root: Path) -> None:
        connector = _connector(fixture_root)
        connector.open()
        try:
            first = connector.read_page(None, _context())
            assert first.next_cursor is not None
            resumed = connector.read_page(first.next_cursor, _context())
            again = connector.read_page(first.next_cursor, _context())
        finally:
            connector.close()
        assert [r.position for r in resumed.records] == [r.position for r in again.records]
        assert resumed.records[0].position == len(first.records)

    def test_single_row_pages_never_mix_rows(self, fixture_root: Path) -> None:
        bounds = ConnectorCallBounds(max_page_records=1, max_record_bytes=2_048)
        connector = _connector(fixture_root, bounds=bounds)
        connector.open()
        try:
            records = _collect(connector)
        finally:
            connector.close()
        assert len(records) == 24

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


class TestMalformedRows:
    def test_wrong_field_count_rows_surface_as_malformed_records(
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
        for record in malformed:
            assert record.payload is None
            assert record.malformed_reason is not None
            assert record.malformed_reason.startswith("field_count_mismatch")

    def test_header_mismatch_fails_the_page_as_validation(self, fixture_root: Path) -> None:
        connector = _connector(fixture_root, columns=("a", "b"))
        connector.open()
        try:
            with pytest.raises(ConnectorValidationError, match="header"):
                connector.read_page(None, _context())
        finally:
            connector.close()

    def test_empty_file_fails_the_header_contract(self, fixture_root: Path) -> None:
        (fixture_root / "empty.csv").write_bytes(b"")
        connector = _connector(fixture_root, "empty.csv")
        connector.open()
        try:
            with pytest.raises(ConnectorValidationError, match="header"):
                connector.read_page(None, _context())
        finally:
            connector.close()

    def test_oversized_field_becomes_a_malformed_record(self, fixture_root: Path) -> None:
        # One field beyond the payload text bound must surface per record,
        # never as an unbounded read or a crashed page.
        fields = ["x" for _ in CSV_SOURCE_COLUMNS]
        fields[1] = "x" * 1_500
        huge = ",".join(fields)
        content = ",".join(CSV_SOURCE_COLUMNS) + "\n" + huge + "\n"
        (fixture_root / "huge.csv").write_bytes(content.encode("utf-8"))
        connector = _connector(fixture_root, "huge.csv")
        connector.open()
        try:
            page = connector.read_page(None, _context())
            assert len(page.records) == 1
            assert page.records[0].is_malformed
            assert page.records[0].malformed_reason is not None
            assert "payload_contract" in page.records[0].malformed_reason
        finally:
            connector.close()


class TestBounds:
    def test_file_above_the_byte_bound_is_rejected_at_open(self, fixture_root: Path) -> None:
        from paritygrid.adapters.connectors import FileReadBounds

        (fixture_root / "big.csv").write_bytes(b"header\n" + b"x" * 100)
        connector = _connector(
            fixture_root, "big.csv", file_bounds=FileReadBounds(max_file_bytes=10, max_rows=10)
        )
        with pytest.raises(ConnectorFileError, match="byte bound"):
            connector.open()
        assert connector.state() is ConnectorState.CLOSED

    def test_row_bound_fails_the_page_when_exceeded(self, fixture_root: Path) -> None:
        from paritygrid.adapters.connectors import FileReadBounds

        connector = _connector(
            fixture_root, file_bounds=FileReadBounds(max_file_bytes=8_388_608, max_rows=2)
        )
        connector.open()
        try:
            with pytest.raises(ConnectorValidationError, match="row bound"):
                _collect(connector)
        finally:
            connector.close()

    def test_single_huge_line_is_rejected_within_the_record_bound(self, fixture_root: Path) -> None:
        (fixture_root / "long.csv").write_bytes(
            (",".join(CSV_SOURCE_COLUMNS) + "\n").encode() + b"x" * 5_000
        )
        connector = _connector(fixture_root, "long.csv")
        connector.open()
        try:
            with pytest.raises(ConnectorValidationError, match="record bound"):
                connector.read_page(None, _context())
        finally:
            connector.close()

    def test_carriage_returns_reject_the_lf_contract(self, fixture_root: Path) -> None:
        (fixture_root / "crlf.csv").write_bytes(
            (",".join(CSV_SOURCE_COLUMNS) + "\r\n" + "a" * 30).encode()
        )
        connector = _connector(fixture_root, "crlf.csv")
        connector.open()
        try:
            with pytest.raises(ConnectorValidationError, match="lf"):
                connector.read_page(None, _context())
        finally:
            connector.close()

    def test_missing_file_is_rejected_at_open(self, fixture_root: Path) -> None:
        connector = _connector(fixture_root, "absent.csv")
        with pytest.raises(ConnectorFileError):
            connector.open()


class TestLocationConfinement:
    def test_traversal_members_are_rejected(self, fixture_root: Path) -> None:
        for member in (
            "../escape.csv",
            "a/../b.csv",
            "/absolute.csv",
            "C:/abs.csv",
            "a//b.csv",
            "",
        ):
            with pytest.raises(ConnectorFileError):
                SourceFileLocation.create(fixture_root, member)

    def test_symlink_escape_is_rejected(self, fixture_root: Path, tmp_path: Path) -> None:
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "leak.csv").write_text("data", encoding="utf-8")
        link = fixture_root / "link.csv"
        try:
            link.symlink_to(outside / "leak.csv")
        except OSError:
            pytest.skip("Symbolic-link creation is unavailable in this environment.")
        with pytest.raises(ConnectorFileError, match="allowlisted root"):
            SourceFileLocation.create(fixture_root, "link.csv")

    def test_plain_member_resolves_inside_the_root(self, fixture_root: Path) -> None:
        location = SourceFileLocation.create(fixture_root, "inventory.csv")
        assert location.resolved.parent == location.root


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

    def test_deadline_is_enforced_during_streaming(self, fixture_root: Path) -> None:
        bounds = ConnectorCallBounds(
            request_timeout_microseconds=1,
            max_page_records=24,
            max_record_bytes=2_048,
        )
        connector = _connector(fixture_root, bounds=bounds)
        connector.open()
        try:
            with pytest.raises(ConnectorTimeoutError):
                connector.read_page(None, _context())
        finally:
            connector.close()

    def test_cancellation_is_checked_between_stream_chunks(self, fixture_root: Path) -> None:
        class CancelsDuringRead:
            def __init__(self) -> None:
                self.calls = 0

            def is_cancelled(self) -> bool:
                return self.calls >= 5

            def raise_if_cancelled(self) -> None:
                self.calls += 1
                if self.is_cancelled():
                    raise ConnectorCancelledError("cancelled during file streaming")

        token = CancelsDuringRead()
        connector = _connector(fixture_root)
        connector.open()
        try:
            with pytest.raises(ConnectorCancelledError):
                connector.read_page(None, ConnectorCallContext(cancellation_token=token))
            assert token.calls >= 5
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
        connector = _connector(fixture_root, "absent.csv")
        with pytest.raises(ConnectorFileError):
            connector.open()
        assert connector.state() is ConnectorState.CLOSED
        with pytest.raises(ConnectorLifecycleError):
            connector.read_page(None, _context())

    async def test_blocking_reads_refuse_an_active_event_loop(self, fixture_root: Path) -> None:
        connector = _connector(fixture_root)
        with pytest.raises(ConnectorLoopError):
            connector.open()
        import asyncio

        await asyncio.to_thread(connector.open)
        try:
            with pytest.raises(ConnectorLoopError):
                connector.read_page(None, _context())
        finally:
            await asyncio.to_thread(connector.close)

    def test_configuration_rejects_wrong_types(self, fixture_root: Path) -> None:
        with pytest.raises(ConnectorFileError):
            CsvFileSourceConfig("not-a-location")  # type: ignore[arg-type]

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
        assert all(event.connector_kind is ConnectorKind.CSV_SOURCE for event in events)
