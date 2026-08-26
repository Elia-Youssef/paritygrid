"""JSON Lines file source connector (P9.5).

The connector streams the Phase 8 JSON Lines fixture contract: UTF-8
without a byte-order mark, LF-terminated lines, one JSON document per
line, and malformed members that are truncated, non-JSON, or empty.
Reads page through the file one bounded line at a time — the file is
never materialized — and each page opens a fresh handle that closes
after success, failure, and cancellation alike.

Encoding and malformed-record behavior is explicit: a line that does not
decode, does not parse as JSON, or is not a JSON object is emitted as a
``malformed`` :class:`SourceRecord` with its position and a bounded
reason; a line whose document violates the record payload contract is
malformed the same way. The page itself never fails for record-level
malformation.

Cursor text is the byte offset just past the last consumed line, so
continuation seeks directly instead of re-scanning.
"""

import json
from collections.abc import Callable, Mapping
from typing import BinaryIO, cast

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

_MAX_CURSOR_OFFSET = 2_147_483_647


class JsonlFileSourceConfig:
    """Validated configuration for the JSON Lines source connector."""

    __slots__ = ("bounds", "file_bounds", "location")

    def __init__(
        self,
        location: SourceFileLocation,
        *,
        bounds: ConnectorCallBounds | None = None,
        file_bounds: FileReadBounds | None = None,
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

    def __repr__(self) -> str:
        return f"JsonlFileSourceConfig(relative={self.location.relative!r})"


def _decode_jsonl_cursor(cursor: str | None) -> tuple[int, int]:
    """Decode the cursor into ``(lines consumed, byte offset)``."""
    if cursor is None:
        return (0, 0)
    validate_cursor(cursor)
    parts = cursor.split(":")
    if len(parts) != 3 or parts[0] != "jl" or not parts[1].isdigit() or not parts[2].isdigit():
        raise ConnectorValidationError("cursor is not a jsonl continuation cursor")
    lines, offset = int(parts[1]), int(parts[2])
    if lines > _MAX_CURSOR_OFFSET or offset > _MAX_CURSOR_OFFSET:
        raise ConnectorValidationError("cursor offset is outside the supported range")
    return (lines, offset)


def _encode_jsonl_cursor(lines: int, offset: int) -> str:
    """Encode a continuation as opaque ``lines:offset`` cursor text."""
    if not 0 <= lines <= _MAX_CURSOR_OFFSET or not 0 <= offset <= _MAX_CURSOR_OFFSET:
        raise ConnectorValidationError("cursor offset is outside the supported range")
    return f"jl:{lines:010d}:{offset:010d}"


def _malformed(position: int, reason: str) -> SourceRecord:
    return SourceRecord(
        position=position,
        outcome=SourceOutcome.MALFORMED,
        payload=None,
        malformed_reason=reason,
    )


class JsonlFileSourceConnector(StreamingFileSource):
    """Paged streaming reader for bounded JSON Lines fixture files."""

    def __init__(
        self,
        config: JsonlFileSourceConfig,
        *,
        observers: list[ConnectorObserver] | None = None,
    ) -> None:
        if type(config) is not JsonlFileSourceConfig:
            raise ConnectorConfigurationError("configuration must use JsonlFileSourceConfig")
        super().__init__(
            kind=ConnectorKind.JSONL_SOURCE,
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
            kind=ConnectorKind.JSONL_SOURCE,
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
        cursor_lines, cursor_offset = _decode_jsonl_cursor(cursor)
        if cursor_offset:
            stream.seek(cursor_offset)
        reader = BoundedLineReader(
            stream,
            bounds=self._config.file_bounds,
            record_bound=self._config.bounds.max_record_bytes,
            checkpoint=checkpoint,
        )
        records: list[SourceRecord] = []
        rows_consumed = 0
        while len(records) < self._config.bounds.max_page_records:
            line = reader.read_line()
            if line is None:
                return tuple(records), None, reader.bytes_read
            rows_consumed += 1
            if cursor_lines + rows_consumed > self._config.file_bounds.max_rows:
                raise ConnectorValidationError("file read exceeded the configured file row bound")
            record = self._record_for_line(line, cursor_lines + rows_consumed - 1)
            records.append(record)
        continuation = _encode_jsonl_cursor(
            cursor_lines + rows_consumed, cursor_offset + reader.offset
        )
        if reader.read_line() is None:
            return tuple(records), None, reader.bytes_read
        return tuple(records), continuation, reader.bytes_read

    def _record_for_line(self, line: BoundedLine, position: int) -> SourceRecord:
        if not line.decoded:
            return _malformed(position, "encoding_error: line is not valid utf-8")
        if line.text.strip() == "":
            return _malformed(position, "empty_line: the line carries no document")
        try:
            document = json.loads(line.text)
        except json.JSONDecodeError:
            return _malformed(position, "json_parse_error: the line is not valid json")
        if not isinstance(document, dict):
            return _malformed(position, "document_shape: the line is not a json object")
        try:
            return SourceRecord(
                position=position,
                outcome=SourceOutcome.VALID,
                payload=cast("Mapping[str, object]", document),
            )
        except ConnectorValidationError as error:
            return _malformed(position, f"payload_contract: {error.detail}")


__all__ = [
    "JsonlFileSourceConfig",
    "JsonlFileSourceConnector",
]
