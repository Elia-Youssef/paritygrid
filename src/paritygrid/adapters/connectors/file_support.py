"""File-source support: allowlisted locations and bounded line streaming.

Connectors that read fixture files never accept a raw filesystem path.
They accept a *location* — an allowlisted root plus a relative member —
validated so traversal, absolute paths, drive references, and symlink
escape are rejected before any file is opened. Line iteration is bounded
per line and cumulatively, so no file connector can materialize an
unbounded source regardless of how the file behaves during a read.

The shared :class:`StreamingFileSource` base owns the file-connector
lifecycle: loop-thread refusal, per-call file ownership (a fresh handle
per page, closed after success, failure, and cancellation alike), row
bounds, and connector events. Concrete connectors supply only cursor
coding and record decoding.
"""

import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from paritygrid.application.ports.connectors import (
    ConnectorCallBounds,
    ConnectorCallContext,
    ConnectorConfigurationError,
    ConnectorEvent,
    ConnectorEventKind,
    ConnectorEventPublisher,
    ConnectorKind,
    ConnectorLifecycleError,
    ConnectorObserver,
    ConnectorState,
    ConnectorTimeoutError,
    ConnectorValidationError,
    FileReadBounds,
    SourcePage,
    SourceRecord,
    require_no_running_loop,
)

FILE_ENCODING = "utf-8"
_READ_CHUNK_BYTES = 8_192
_RELATIVE_MEMBER_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*(?:/[A-Za-z0-9][A-Za-z0-9._-]*)*")


class ConnectorFileError(ConnectorConfigurationError):
    """A file-source location or read violated the file contract."""


@dataclass(frozen=True, slots=True)
class SourceFileLocation:
    """One validated file location confined to an allowlisted root."""

    root: Path
    relative: str
    resolved: Path

    @classmethod
    def create(cls, root: Path, relative: str) -> SourceFileLocation:
        """Validate and confine one location beneath ``root``."""
        # Path subclasses (platform paths) are accepted; only non-path
        # callers are rejected before any filesystem access.
        if not isinstance(root, Path):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise ConnectorFileError("root must be a Path")
        if type(relative) is not str or _RELATIVE_MEMBER_PATTERN.fullmatch(relative) is None:
            raise ConnectorFileError("relative member is outside the accepted shape")
        root_resolved = root.resolve()
        if not root_resolved.is_dir():
            raise ConnectorFileError("root must be an existing directory")
        # resolve() follows symlinks, so a member that escapes the root
        # through one loses containment here and is rejected outright.
        candidate = (root_resolved / relative).resolve()
        try:
            candidate.relative_to(root_resolved)
        except ValueError as error:
            raise ConnectorFileError("relative member escapes the allowlisted root") from error
        return cls(root=root_resolved, relative=relative, resolved=candidate)


class BoundedLine:
    """One decoded line with its byte extent and decode validity."""

    __slots__ = ("byte_length", "decoded", "text")

    def __init__(self, text: str, byte_length: int, decoded: bool) -> None:
        self.text = text
        self.byte_length = byte_length
        self.decoded = decoded


class BoundedLineReader:
    """Incremental line reader enforcing byte and line-length bounds.

    The reader never loads the whole file: it pulls fixed-size chunks,
    splits them on ``\\n`` (the fixture newline contract), and refuses a
    single line longer than the configured record bound. ``\\r`` is
    rejected because the fixture encoding contract is LF-terminated
    UTF-8 without a byte-order mark.
    """

    def __init__(
        self,
        stream: BinaryIO,
        *,
        bounds: FileReadBounds,
        record_bound: int,
        checkpoint: Callable[[], None] | None = None,
    ) -> None:
        self._stream = stream
        self._bounds = bounds
        self._record_bound = record_bound
        self._buffer = bytearray()
        self._offset = 0
        self._bytes_read = 0
        self._exhausted = False
        self._checkpoint = checkpoint if checkpoint is not None else _no_checkpoint

    @property
    def offset(self) -> int:
        """Return the byte offset just past the last consumed line."""
        return self._offset

    @property
    def bytes_read(self) -> int:
        """Return how many file bytes this reader has pulled."""
        return self._bytes_read

    def read_line(self) -> BoundedLine | None:
        """Return the next bounded line, or ``None`` at end of input."""
        while True:
            self._checkpoint()
            newline_position = self._buffer.find(b"\n")
            if newline_position != -1:
                if newline_position > self._record_bound:
                    raise ConnectorValidationError(
                        "a single record line exceeds the configured record bound"
                    )
                raw = bytes(self._buffer[:newline_position])
                del self._buffer[: newline_position + 1]
                return self._decode(raw)
            if self._exhausted:
                if self._buffer:
                    raw = bytes(self._buffer)
                    self._buffer.clear()
                    return self._decode(raw)
                return None
            if len(self._buffer) > self._record_bound:
                raise ConnectorValidationError(
                    "a single record line exceeds the configured record bound"
                )
            chunk = self._stream.read(_READ_CHUNK_BYTES)
            self._checkpoint()
            if not chunk:
                self._exhausted = True
                continue
            if b"\r" in chunk:
                raise ConnectorValidationError("file content must use lf line endings only")
            self._bytes_read += len(chunk)
            if self._bytes_read > self._bounds.max_file_bytes:
                raise ConnectorValidationError("file read exceeded the configured file byte bound")
            self._buffer.extend(chunk)

    def _decode(self, raw: bytes) -> BoundedLine:
        self._offset += len(raw) + 1
        try:
            text = raw.decode(FILE_ENCODING)
        except UnicodeDecodeError:
            return BoundedLine(text="", byte_length=len(raw) + 1, decoded=False)
        return BoundedLine(text=text, byte_length=len(raw) + 1, decoded=True)


def check_file_exists_within_bounds(location: SourceFileLocation, bounds: FileReadBounds) -> int:
    """Validate the file exists and its size fits the byte bound."""
    if not location.resolved.is_file():
        raise ConnectorFileError("source file does not exist")
    size = location.resolved.stat().st_size
    if size > bounds.max_file_bytes:
        raise ConnectorFileError("source file exceeds the configured file byte bound")
    return size


class StreamingFileSource:
    """Shared lifecycle for file connectors that page a bounded stream.

    A page opens the file fresh, positions at the cursor, reads at most
    one page of bounded records, and closes the file in every outcome
    path. No handle is retained between calls, so a caller that forgets
    :meth:`close` still leaks nothing.
    """

    def __init__(
        self,
        *,
        kind: ConnectorKind,
        location: SourceFileLocation,
        call_bounds: ConnectorCallBounds,
        file_bounds: FileReadBounds,
        observers: list[ConnectorObserver] | None,
    ) -> None:
        self._kind = kind
        self._location = location
        self._call_bounds = call_bounds
        self._file_bounds = file_bounds
        self._events = ConnectorEventPublisher(observers)
        self._state = ConnectorState.CREATED

    def state(self) -> ConnectorState:
        """Return the current lifecycle state."""
        return self._state

    def open(self) -> None:
        """Validate the location and size; no handle is retained."""
        require_no_running_loop("file source open")
        if self._state is ConnectorState.OPEN:
            raise ConnectorLifecycleError("the connector is already open")
        if self._state is ConnectorState.CLOSED:
            raise ConnectorLifecycleError("the connector is closed")
        try:
            check_file_exists_within_bounds(self._location, self._file_bounds)
        except Exception:
            self._state = ConnectorState.CLOSED
            self._events.publish(
                ConnectorEvent(
                    kind=ConnectorEventKind.OPEN_FAILED,
                    connector_kind=self._kind,
                    correlation_id=None,
                    details={"reason": "location validation failed"},
                )
            )
            raise
        self._state = ConnectorState.OPEN
        self._events.publish(
            ConnectorEvent(
                kind=ConnectorEventKind.OPENED,
                connector_kind=self._kind,
                correlation_id=None,
                details={},
            )
        )

    def close(self) -> None:
        """Close the connector; repeated calls are safe."""
        if self._state is not ConnectorState.CLOSED:
            self._state = ConnectorState.CLOSED
            self._events.publish(
                ConnectorEvent(
                    kind=ConnectorEventKind.CLOSED,
                    connector_kind=self._kind,
                    correlation_id=None,
                    details={},
                )
            )

    def read_page(
        self,
        cursor: str | None,
        context: ConnectorCallContext,
    ) -> SourcePage:
        """Read one bounded page from a freshly opened stream."""
        require_no_running_loop("file source read_page")
        if self._state is not ConnectorState.OPEN:
            raise ConnectorLifecycleError("the connector is not open")
        context.raise_if_cancelled()
        deadline = time.monotonic() + self._call_bounds.request_timeout_microseconds / 1_000_000

        def checkpoint() -> None:
            context.raise_if_cancelled()
            if time.monotonic() >= deadline:
                raise ConnectorTimeoutError(
                    "file source call exceeded its deadline",
                    secrets=context.secrets,
                )

        checkpoint()
        self._events.publish(
            ConnectorEvent(
                kind=ConnectorEventKind.CALL_STARTED,
                connector_kind=self._kind,
                correlation_id=context.correlation_id,
                details={"operation": "read_page"},
            )
        )
        stream = self._location.resolved.open("rb")
        try:
            records, next_cursor, byte_count = self._read_records(stream, cursor, checkpoint)
            checkpoint()
        finally:
            stream.close()
        page = SourcePage(
            records=records,
            next_cursor=next_cursor,
            request_count=1,
            byte_count=byte_count,
        )
        self._events.publish(
            ConnectorEvent(
                kind=ConnectorEventKind.PAGE_COMPLETED,
                connector_kind=self._kind,
                correlation_id=context.correlation_id,
                details={
                    "records": len(records),
                    "bytes": page.byte_count,
                    "exhausted": 1 if page.exhausted else 0,
                },
            )
        )
        return page

    def _read_records(
        self,
        stream: BinaryIO,
        cursor: str | None,
        checkpoint: Callable[[], None],
    ) -> tuple[tuple[SourceRecord, ...], str | None, int]:
        """Read one page from a positioned stream and produce the next cursor.

        Implementations own their cursor text: ``cursor`` is the opaque
        string (or ``None`` for the first page) and the second result is
        the continuation cursor, or ``None`` when the source is
        exhausted. The stream closes in every outcome path.
        """
        raise NotImplementedError


def _no_checkpoint() -> None:
    return None


__all__ = [
    "FILE_ENCODING",
    "BoundedLine",
    "BoundedLineReader",
    "ConnectorFileError",
    "SourceFileLocation",
    "StreamingFileSource",
    "check_file_exists_within_bounds",
]
