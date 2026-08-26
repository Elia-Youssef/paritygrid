"""CSV file source connector (P9.4).

The connector streams the Phase 8 CSV fixture contract: UTF-8 without a
byte-order mark, LF-terminated lines, one fixed header row, and data rows
whose malformed members carry a wrong field count. Reads page through the
file one bounded line at a time — the file is never materialized — and
each page opens a fresh handle that closes after success, failure, and
cancellation alike.

Encoding and malformed-record behavior is explicit: a header that does
not match the configured columns fails the page as a validation failure;
a data line that does not decode, does not parse, carries a wrong field
count, or violates the record payload contract is emitted as a
``malformed`` :class:`SourceRecord` carrying its position and a bounded
reason instead of failing the page. Blank lines carry no record.
Multi-line quoted records are outside this single-line wire contract.

Cursor text is the number of data lines consumed after the header;
continuation re-scans from the start of the file under the same byte and
row bounds, which keeps cursor semantics correct without assuming quoted
fields can be skipped by byte offset.
"""

import csv
from collections.abc import Callable
from typing import BinaryIO

from paritygrid.adapters.connectors.file_support import (
    BoundedLine,
    BoundedLineReader,
    ConnectorFileError,
    SourceFileLocation,
    StreamingFileSource,
)
from paritygrid.application.planner.connectors import ConnectorCapability, ConnectorCapabilitySet
from paritygrid.application.ports.connectors import (
    CONNECTOR_CAPABILITIES_PROTOCOL,
    CONNECTOR_CONTRACT_VERSION,
    ConnectorCallBounds,
    ConnectorCapabilitiesV1,
    ConnectorConfigurationError,
    ConnectorKind,
    ConnectorObserver,
    ConnectorValidationError,
    FileReadBounds,
    SourceOutcome,
    SourceRecord,
    validate_cursor,
)

#: Column contract of the CSV fixture wire format.
CSV_SOURCE_COLUMNS: tuple[str, ...] = (
    "source_record_key",
    "sku",
    "name",
    "quantity",
    "currency",
    "amount",
    "updated_at",
    "attributes",
)
_MAX_CURSOR_ROWS = 2_147_483_647


class CsvFileSourceConfig:
    """Validated configuration for the CSV source connector."""

    __slots__ = ("bounds", "columns", "file_bounds", "location")

    def __init__(
        self,
        location: SourceFileLocation,
        *,
        bounds: ConnectorCallBounds | None = None,
        file_bounds: FileReadBounds | None = None,
        columns: tuple[str, ...] = CSV_SOURCE_COLUMNS,
    ) -> None:
        if type(location) is not SourceFileLocation:
            raise ConnectorFileError("location must use SourceFileLocation")
        self.location = location
        self.bounds = bounds if bounds is not None else ConnectorCallBounds()
        self.file_bounds = file_bounds if file_bounds is not None else FileReadBounds()
        if type(self.bounds) is not ConnectorCallBounds:
            raise ConnectorConfigurationError("bounds must use ConnectorCallBounds")
        if type(self.file_bounds) is not FileReadBounds:
            raise ConnectorConfigurationError("file bounds must use FileReadBounds")
        if (
            type(columns) is not tuple
            or not columns
            or any(type(column) is not str or not column for column in columns)
        ):
            raise ConnectorConfigurationError("columns must be a non-empty tuple of names")
        self.columns = columns

    def __repr__(self) -> str:
        return f"CsvFileSourceConfig(relative={self.location.relative!r})"


def _decode_csv_cursor(cursor: str | None) -> int:
    """Decode the csv row cursor into its consumed data-row count."""
    if cursor is None:
        return 0
    validate_cursor(cursor)
    if not cursor.startswith("rows:") or not cursor[5:].isdigit():
        raise ConnectorValidationError("cursor is not a csv row cursor")
    rows = int(cursor[5:])
    if rows > _MAX_CURSOR_ROWS:
        raise ConnectorValidationError("cursor row count is outside the supported range")
    return rows


def _encode_csv_cursor(rows: int) -> str:
    """Encode a consumed data-row count as its opaque cursor text."""
    if not 0 <= rows <= _MAX_CURSOR_ROWS:
        raise ConnectorValidationError("cursor row count is outside the supported range")
    return f"rows:{rows:010d}"


def _malformed(position: int, reason: str) -> SourceRecord:
    return SourceRecord(
        position=position,
        outcome=SourceOutcome.MALFORMED,
        payload=None,
        malformed_reason=reason,
    )


class CsvFileSourceConnector(StreamingFileSource):
    """Paged streaming reader for bounded CSV fixture files."""

    def __init__(
        self,
        config: CsvFileSourceConfig,
        *,
        observers: list[ConnectorObserver] | None = None,
    ) -> None:
        if type(config) is not CsvFileSourceConfig:
            raise ConnectorConfigurationError("configuration must use CsvFileSourceConfig")
        super().__init__(
            kind=ConnectorKind.CSV_SOURCE,
            location=config.location,
            call_bounds=config.bounds,
            file_bounds=config.file_bounds,
            observers=observers,
        )
        self._config = config

    def capabilities(self) -> ConnectorCapabilitiesV1:
        """Return the immutable capability metadata for this kind."""
        return ConnectorCapabilitiesV1(
            protocol=CONNECTOR_CAPABILITIES_PROTOCOL,
            contract_version=CONNECTOR_CONTRACT_VERSION,
            kind=ConnectorKind.CSV_SOURCE,
            capabilities=ConnectorCapabilitySet(
                (ConnectorCapability.READ, ConnectorCapability.BLOCKING_IO)
            ),
            max_page_records=self._config.bounds.max_page_records,
            supports_cursors=True,
        )

    def _read_records(
        self,
        stream: BinaryIO,
        cursor: str | None,
        checkpoint: Callable[[], None],
    ) -> tuple[tuple[SourceRecord, ...], str | None, int]:
        cursor_position = _decode_csv_cursor(cursor)
        reader = BoundedLineReader(
            stream,
            bounds=self._config.file_bounds,
            record_bound=self._config.bounds.max_record_bytes,
            checkpoint=checkpoint,
        )
        self._read_header(reader)
        # A page always starts from a fresh stream, so the cursor's data
        # lines are skipped by re-scan; a cursor at or beyond end of file
        # yields an empty exhausted page instead of replaying the start.
        skipped = 0
        while skipped < cursor_position:
            line = reader.read_line()
            if line is None:
                return (), None, reader.bytes_read
            skipped += 1
        records: list[SourceRecord] = []
        lines_consumed = 0
        while len(records) < self._config.bounds.max_page_records:
            line = reader.read_line()
            if line is None:
                return tuple(records), None, reader.bytes_read
            lines_consumed += 1
            absolute_line = cursor_position + lines_consumed
            if absolute_line > self._config.file_bounds.max_rows:
                raise ConnectorValidationError("file read exceeded the configured file row bound")
            record = self._record_for_line(line, absolute_line - 1)
            if record is not None:
                records.append(record)
        while True:
            line = reader.read_line()
            if line is None:
                return tuple(records), None, reader.bytes_read
            lines_consumed += 1
            absolute_line = cursor_position + lines_consumed
            if absolute_line > self._config.file_bounds.max_rows:
                raise ConnectorValidationError("file read exceeded the configured file row bound")
            if self._record_for_line(line, absolute_line - 1) is not None:
                continuation = _encode_csv_cursor(absolute_line - 1)
                return tuple(records), continuation, reader.bytes_read

    def _record_for_line(self, line: BoundedLine, position: int) -> SourceRecord | None:
        if not line.decoded:
            return _malformed(position, "encoding_error: line is not valid utf-8")
        if line.text.strip() == "":
            return None
        try:
            fields = next(csv.reader([line.text]), None)
        except csv.Error:
            return _malformed(position, "csv_parse_error: the line is not parsable")
        if fields is None:
            return _malformed(position, "csv_parse_error: the line is not parsable")
        expected = len(self._config.columns)
        if len(fields) != expected:
            return _malformed(
                position, f"field_count_mismatch: expected {expected} got {len(fields)}"
            )
        payload: dict[str, object] = dict(zip(self._config.columns, fields, strict=True))
        try:
            return SourceRecord(
                position=position,
                outcome=SourceOutcome.VALID,
                payload=payload,
            )
        except ConnectorValidationError as error:
            return _malformed(position, f"payload_contract: {error.detail}")

    def _read_header(self, reader: BoundedLineReader) -> None:
        line = reader.read_line()
        if line is None:
            raise ConnectorValidationError("csv source is empty; a header row is required")
        if not line.decoded:
            raise ConnectorValidationError("csv header is not valid utf-8")
        try:
            header_rows = list(csv.reader([line.text]))
        except csv.Error as error:
            raise ConnectorValidationError("csv header is not parsable") from error
        header = header_rows[0] if header_rows else []
        if tuple(header) != self._config.columns:
            raise ConnectorValidationError("csv header does not match the configured columns")


__all__ = [
    "CSV_SOURCE_COLUMNS",
    "CsvFileSourceConfig",
    "CsvFileSourceConnector",
]
