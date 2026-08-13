"""Defensive artifact streaming boundary and descriptor tests."""

# pyright: reportPrivateUsage=false, reportIncompatibleMethodOverride=false, reportUnknownArgumentType=false, reportUnknownLambdaType=false

import hashlib
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Never

import pytest
from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from paritygrid.adapters.artifacts import streaming as runtime
from paritygrid.application.ports import (
    ArtifactManifestCorruptionError,
    ArtifactManifestRecord,
    ArtifactPathError,
    ArtifactRelativePath,
    ArtifactStreamIntegrityError,
    ArtifactStreamMetadata,
    ArtifactStreamStorageError,
    ArtifactStreamStorageUnavailableError,
    PersistenceContentionError,
)
from paritygrid.domain.models import ArtifactId, NodeId, RunId, UtcTimestamp
from paritygrid.domain.pipeline import PartitionKey


class _SQLiteFailureError(Exception):
    sqlite_errorcode: int

    def __init__(self, code: int) -> None:
        super().__init__("redacted")
        self.sqlite_errorcode = code


class _Result:
    def __init__(self, value: object | None) -> None:
        self._value = value

    def mappings(self) -> _Result:
        return self

    def one_or_none(self) -> object | None:
        return self._value


class _Session(Session):
    def __init__(self, values: list[object | None]) -> None:
        super().__init__()
        self._values = iter(values)

    def in_transaction(self) -> bool:
        return True

    def execute(self, _statement: object, _parameters: object = None) -> _Result:
        return _Result(next(self._values))


def _timestamp(second: int = 2) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 13, 12, 0, second, tzinfo=UTC))


def _record(*, artifact_id: str = "art_boundary", byte_size: int = 7) -> ArtifactManifestRecord:
    return ArtifactManifestRecord(
        ArtifactId(artifact_id),
        RunId("run_boundary"),
        NodeId("nod_boundary"),
        PartitionKey("all"),
        ArtifactRelativePath("runs/file.bin"),
        "application/octet-stream",
        1,
        byte_size,
        0,
        hashlib.sha256(b"content").hexdigest(),
        _timestamp(),
    )


def _metadata(path: Path, byte_size: int) -> ArtifactStreamMetadata:
    return ArtifactStreamMetadata(
        ArtifactId("art_boundary"),
        ArtifactRelativePath(path.name),
        "application/octet-stream",
        1,
        byte_size,
        0,
        byte_size,
        hashlib.sha256(b"content").hexdigest(),
    )


def _stream(path: Path, *, selected_size: int | None = None) -> runtime.FileArtifactByteStream:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_BINARY", 0))
    identity = runtime._identity(os.fstat(descriptor))
    size = path.stat().st_size if selected_size is None else selected_size
    return runtime.FileArtifactByteStream(
        descriptor,
        path,
        _metadata(path, size),
        chunk_size=2,
        expected_identity=identity,
    )


def test_storage_error_translation_is_specific_and_redacted() -> None:
    @runtime._translate_stream_storage_errors
    def fail(error: BaseException) -> None:
        raise error

    busy = OperationalError(
        "secret sql", {"private": "value"}, _SQLiteFailureError(sqlite3.SQLITE_BUSY)
    )
    unavailable = OperationalError("secret sql", {}, OSError("private"))
    with pytest.raises(PersistenceContentionError):
        fail(busy)
    with pytest.raises(ArtifactStreamStorageUnavailableError) as operational:
        fail(unavailable)
    with pytest.raises(ArtifactStreamStorageUnavailableError):
        fail(InterfaceError("secret sql", {}, OSError()))
    with pytest.raises(ArtifactStreamStorageError) as generic:
        fail(SQLAlchemyError("secret private"))
    assert "secret" not in str(operational.value)
    assert "secret" not in str(generic.value)
    assert operational.value.__cause__ is None


def test_reader_rejects_corrupt_manifest_identity_and_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    corrupt = runtime.FileSystemArtifactStreamReader(_Session([object()]), tmp_path, chunk_size=1)

    def corrupt_row(_row: object) -> Never:
        raise ArtifactManifestCorruptionError("private row")

    monkeypatch.setattr(runtime, "_manifest_from_row", corrupt_row)
    with pytest.raises(ArtifactStreamIntegrityError, match="manifest"):
        corrupt.open(ArtifactId("art_boundary"))

    monkeypatch.setattr(
        runtime, "_manifest_from_row", lambda _row: _record(artifact_id="art_other")
    )
    mismatched = runtime.FileSystemArtifactStreamReader(
        _Session([object()]), tmp_path, chunk_size=1
    )
    with pytest.raises(ArtifactStreamIntegrityError, match="manifest"):
        mismatched.open(ArtifactId("art_boundary"))

    monkeypatch.setattr(runtime, "_manifest_from_row", lambda _row: _record())
    missing_parent = runtime.FileSystemArtifactStreamReader(
        _Session([object(), None]), tmp_path, chunk_size=1
    )
    with pytest.raises(ArtifactStreamIntegrityError, match="parent"):
        missing_parent.open(ArtifactId("art_boundary"))

    invalid_parent = runtime.FileSystemArtifactStreamReader(
        _Session([object(), ("not-a-timestamp",)]), tmp_path, chunk_size=1
    )
    with pytest.raises(ArtifactStreamIntegrityError, match="parent"):
        invalid_parent.open(ArtifactId("art_boundary"))


def test_stream_reports_read_eof_and_close_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "file.bin"
    path.write_bytes(b"x")
    early = _stream(path, selected_size=2)
    assert next(early) == b"x"
    with pytest.raises(ArtifactStreamIntegrityError, match="ended"):
        next(early)

    failed_read = _stream(path)

    def read_failure(_descriptor: int, _size: int) -> Never:
        raise OSError("private")

    monkeypatch.setattr(runtime.os, "read", read_failure)
    with pytest.raises(ArtifactStreamStorageError, match="read"):
        next(failed_read)

    monkeypatch.undo()
    failed_stat = _stream(path)

    def stat_failure(_descriptor: int) -> Never:
        raise OSError("private")

    monkeypatch.setattr(runtime.os, "fstat", stat_failure)
    with pytest.raises(ArtifactStreamIntegrityError, match="identity"):
        failed_stat.close()


def test_stream_close_failure_is_redacted_and_descriptor_is_recoverable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "file.bin"
    path.write_bytes(b"content")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_BINARY", 0))
    stream = runtime.FileArtifactByteStream(
        descriptor,
        path,
        _metadata(path, 7),
        chunk_size=2,
        expected_identity=runtime._identity(os.fstat(descriptor)),
    )
    real_close = os.close

    def close_failure(_descriptor: int) -> Never:
        raise OSError("private")

    monkeypatch.setattr(runtime.os, "close", close_failure)
    try:
        with pytest.raises(ArtifactStreamStorageError, match="closed") as error:
            stream.close()
        assert "private" not in str(error.value)
    finally:
        real_close(descriptor)


def test_open_verified_stream_rejects_invalid_nonregular_and_unsafe_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(TypeError, match="manifest"):
        runtime._open_verified_stream(tmp_path, object(), byte_range=None, chunk_size=1)  # type: ignore[arg-type]

    path = tmp_path / "runs" / "file.bin"
    path.parent.mkdir()
    path.write_bytes(b"content")
    actual_fstat = os.fstat
    nonregular = SimpleNamespace(
        st_mode=0,
        st_dev=1,
        st_ino=1,
        st_size=7,
        st_mtime_ns=1,
    )
    monkeypatch.setattr(runtime.os, "fstat", lambda _descriptor: nonregular)
    with pytest.raises(ArtifactStreamIntegrityError, match="differs"):
        runtime._open_verified_stream(tmp_path, _record(), byte_range=None, chunk_size=1)

    monkeypatch.setattr(runtime.os, "fstat", actual_fstat)

    def unsafe_path(_root: Path, _relative: ArtifactRelativePath) -> Path:
        raise ArtifactPathError("private")

    monkeypatch.setattr(runtime, "resolve_artifact_path", unsafe_path)
    with pytest.raises(ArtifactStreamIntegrityError, match="confined") as error:
        runtime._open_verified_stream(tmp_path, _record(), byte_range=None, chunk_size=1)
    assert "private" not in str(error.value)


def test_open_verified_stream_maps_operating_system_and_cleanup_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def open_failure(_path: Path, _flags: int) -> Never:
        raise OSError("private")

    monkeypatch.setattr(runtime.os, "open", open_failure)
    with pytest.raises(ArtifactStreamStorageError, match="opened") as error:
        runtime._open_verified_stream(tmp_path, _record(), byte_range=None, chunk_size=1)
    assert "private" not in str(error.value)

    monkeypatch.undo()
    monkeypatch.setattr(runtime.os, "open", lambda _path, _flags: 41)

    def fstat_failure(_descriptor: int) -> Never:
        raise OSError("private")

    monkeypatch.setattr(runtime.os, "fstat", fstat_failure)

    def close_failure(_descriptor: int) -> Never:
        raise OSError("private")

    monkeypatch.setattr(runtime.os, "close", close_failure)
    with pytest.raises(ArtifactStreamStorageError, match="closed"):
        runtime._open_verified_stream(tmp_path, _record(), byte_range=None, chunk_size=1)
