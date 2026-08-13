"""Defensive branch tests for artifact manifest verification."""

# pyright: reportPrivateUsage=false

import hashlib
import os
import sqlite3
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Never, cast

import pytest
from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError

from paritygrid.adapters.artifacts import manifests as runtime
from paritygrid.application.ports import (
    ArtifactIntegrityError,
    ArtifactManifestCorruptionError,
    ArtifactManifestInvalidError,
    ArtifactManifestRecord,
    ArtifactManifestStorageError,
    ArtifactManifestStorageUnavailableError,
    ArtifactPathError,
    ArtifactRelativePath,
    PersistenceContentionError,
)
from paritygrid.domain.models import ArtifactId, NodeId, RunId, UtcTimestamp
from paritygrid.domain.pipeline import PartitionKey


class _SQLiteFailureError(Exception):
    sqlite_errorcode: int

    def __init__(self, code: int) -> None:
        super().__init__("redacted")
        self.sqlite_errorcode = code


def _timestamp(second: int = 2) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 13, 12, 0, second, tzinfo=UTC))


def _record() -> ArtifactManifestRecord:
    content = b"content"
    return ArtifactManifestRecord(
        ArtifactId("art_boundary"),
        RunId("run_boundary"),
        NodeId("nod_boundary"),
        PartitionKey("page-0001"),
        ArtifactRelativePath("runs/file.bin"),
        "application/octet-stream",
        1,
        len(content),
        0,
        hashlib.sha256(content).hexdigest(),
        _timestamp(),
    )


def test_storage_error_translation_is_redacted_and_specific() -> None:
    @runtime._translate_manifest_storage_errors
    def unavailable(error: BaseException) -> None:
        raise error

    with pytest.raises(ArtifactManifestStorageUnavailableError) as operational:
        unavailable(OperationalError("secret sql", {"token": "canary"}, OSError()))
    with pytest.raises(ArtifactManifestStorageUnavailableError):
        unavailable(InterfaceError("secret sql", {}, OSError()))
    with pytest.raises(ArtifactManifestStorageError) as generic:
        unavailable(SQLAlchemyError("secret canary"))
    assert "secret" not in str(operational.value)
    assert "secret" not in str(generic.value)
    assert operational.value.__cause__ is None
    assert generic.value.__cause__ is None

    with pytest.raises(PersistenceContentionError):
        unavailable(OperationalError("secret sql", {}, _SQLiteFailureError(sqlite3.SQLITE_BUSY)))


def test_repository_private_validation_classifies_bad_values() -> None:
    with pytest.raises(ArtifactManifestInvalidError, match="media type"):
        runtime._require_media_type("Application/JSON")
    with pytest.raises(ArtifactManifestInvalidError, match="schema"):
        runtime._require_int(True, "schema", 1, 10)
    with pytest.raises(ArtifactManifestCorruptionError, match="timestamp"):
        runtime._stored_timestamp(7)
    with pytest.raises(ArtifactManifestCorruptionError, match="timestamp"):
        runtime._stored_timestamp("2026-02-31T00:00:00.000000Z")
    assert runtime._stored_timestamp(str(_timestamp())) == _timestamp()


def test_manifest_mapping_rejects_missing_or_invalid_columns() -> None:
    with pytest.raises(ArtifactManifestCorruptionError, match="row is corrupt"):
        runtime._manifest_from_row({})
    values = runtime._stored_values(_record())
    values["relative_path"] = "../escape"
    with pytest.raises(ArtifactManifestCorruptionError, match="row is corrupt"):
        runtime._manifest_from_row(values)


def test_verify_rejects_nonregular_changed_and_replaced_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "file.bin"
    path.write_bytes(b"content")
    original_fstat = os.fstat

    nonregular = SimpleNamespace(
        st_mode=0,
        st_dev=1,
        st_ino=1,
        st_size=7,
        st_mtime_ns=1,
    )

    def nonregular_fstat(_descriptor: int) -> SimpleNamespace:
        return nonregular

    monkeypatch.setattr(runtime.os, "fstat", nonregular_fstat)
    with pytest.raises(ArtifactIntegrityError, match="regular file"):
        runtime._verify_file(path, 7, hashlib.sha256(b"content").hexdigest())

    calls = 0

    def changed_fstat(descriptor: int) -> os.stat_result | SimpleNamespace:
        nonlocal calls
        calls += 1
        current = original_fstat(descriptor)
        if calls == 2:
            return SimpleNamespace(
                st_mode=current.st_mode,
                st_dev=current.st_dev,
                st_ino=current.st_ino,
                st_size=current.st_size,
                st_mtime_ns=current.st_mtime_ns + 1,
            )
        return current

    monkeypatch.setattr(runtime.os, "fstat", changed_fstat)
    with pytest.raises(ArtifactIntegrityError, match="changed during"):
        runtime._verify_file(path, 7, hashlib.sha256(b"content").hexdigest())

    monkeypatch.setattr(runtime.os, "fstat", original_fstat)
    original_stat = os.stat

    def replaced_stat(candidate: Path, *, follow_symlinks: bool = True) -> Any:
        current = original_stat(candidate, follow_symlinks=follow_symlinks)
        return SimpleNamespace(
            st_mode=current.st_mode,
            st_dev=current.st_dev,
            st_ino=current.st_ino + 1,
            st_size=current.st_size,
            st_mtime_ns=current.st_mtime_ns,
        )

    monkeypatch.setattr(runtime.os, "stat", replaced_stat)
    with pytest.raises(ArtifactIntegrityError, match="path changed"):
        runtime._verify_file(path, 7, hashlib.sha256(b"content").hexdigest())


def test_verify_maps_operating_system_and_handle_close_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "file.bin"
    path.write_bytes(b"content")

    def fail_fstat(_descriptor: int) -> Never:
        raise OSError("inspection")

    monkeypatch.setattr(runtime.os, "fstat", fail_fstat)
    with pytest.raises(ArtifactIntegrityError, match="could not be verified"):
        runtime._verify_file(path, 7, hashlib.sha256(b"content").hexdigest())

    def fake_open(*_args: object) -> int:
        return 41

    def fail_close(_descriptor: int) -> Never:
        raise OSError("close")

    monkeypatch.setattr(runtime.os, "open", fake_open)
    monkeypatch.setattr(runtime.os, "close", fail_close)
    with pytest.raises(ArtifactIntegrityError, match="handle could not be closed"):
        runtime._verify_file(path, 7, hashlib.sha256(b"content").hexdigest())


def test_hash_stream_handles_empty_and_multiple_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime, "_VERIFY_CHUNK_BYTES", 2)
    digest = hashlib.sha256()
    assert runtime._hash_stream(cast(Any, BytesIO(b"abcdef")), digest) == 6
    assert digest.hexdigest() == hashlib.sha256(b"abcdef").hexdigest()
    assert runtime._hash_stream(cast(Any, BytesIO(b"")), hashlib.sha256()) == 0


def test_repository_detects_parent_and_path_races(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository = runtime.FileSystemArtifactManifestRepository(cast(Any, object()), tmp_path)

    def missing_parent(
        _repository: runtime.FileSystemArtifactManifestRepository,
        _run_id: RunId,
        _node_id: NodeId,
    ) -> None:
        return None

    monkeypatch.setattr(runtime.FileSystemArtifactManifestRepository, "_parent", missing_parent)
    with pytest.raises(ArtifactManifestCorruptionError, match="parent"):
        repository._validate_parent(_record())

    calls = 0

    def changing_path(_root: Path, _relative: ArtifactRelativePath) -> Path:
        nonlocal calls
        calls += 1
        return tmp_path / ("first" if calls == 1 else "second")

    def verified_file(_path: Path, _size: int, _digest: str) -> None:
        return None

    monkeypatch.setattr(runtime, "resolve_artifact_path", changing_path)
    monkeypatch.setattr(runtime, "_verify_file", verified_file)
    with pytest.raises(ArtifactIntegrityError, match="path changed"):
        repository._verify(_record())

    def unsafe_path(_root: Path, _relative: ArtifactRelativePath) -> Path:
        raise ArtifactPathError("unsafe")

    monkeypatch.setattr(runtime, "resolve_artifact_path", unsafe_path)
    with pytest.raises(ArtifactIntegrityError, match="safely confined"):
        repository._verify(_record())


def test_repository_batch_parent_validation_handles_empty_and_missing(
    tmp_path: Path,
) -> None:
    class EmptyRows:
        def all(self) -> list[object]:
            return []

    class EmptySession:
        def execute(self, _statement: object) -> EmptyRows:
            return EmptyRows()

    repository = runtime.FileSystemArtifactManifestRepository(cast(Any, EmptySession()), tmp_path)
    repository._validate_parents(())
    with pytest.raises(ArtifactManifestCorruptionError, match="parent"):
        repository._validate_parents((_record(),))
