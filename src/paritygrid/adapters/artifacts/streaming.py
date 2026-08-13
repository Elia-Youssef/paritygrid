"""Descriptor-bound artifact streaming with manifest and path verification."""

# pyright: reportPrivateUsage=false

import hashlib
import os
import stat
from collections.abc import Callable, Iterator
from functools import wraps
from pathlib import Path
from threading import RLock
from typing import cast

from sqlalchemy import select
from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from paritygrid.adapters.artifacts.manifests import _manifest_from_row, _stored_timestamp
from paritygrid.adapters.artifacts.paths import resolve_artifact_path, resolve_artifact_root
from paritygrid.adapters.persistence.schema import artifact_manifests, run_nodes, runs
from paritygrid.adapters.persistence.writer.contention import is_sqlite_contention
from paritygrid.application.ports.artifact_streaming import (
    ArtifactByteRange,
    ArtifactByteStream,
    ArtifactStreamIntegrityError,
    ArtifactStreamInvalidError,
    ArtifactStreamMetadata,
    ArtifactStreamNotFoundError,
    ArtifactStreamRangeError,
    ArtifactStreamReader,
    ArtifactStreamStorageError,
    ArtifactStreamStorageUnavailableError,
)
from paritygrid.application.ports.artifacts import (
    MAX_ARTIFACT_CHUNK_BYTES,
    ArtifactManifestCorruptionError,
    ArtifactManifestRecord,
    ArtifactPathError,
)
from paritygrid.application.ports.writer import PersistenceContentionError
from paritygrid.domain.models import ArtifactId

_VERIFY_CHUNK_BYTES = 1_048_576


def _translate_stream_storage_errors[**P, R](operation: Callable[P, R]) -> Callable[P, R]:
    @wraps(operation)
    def translated(*args: P.args, **kwargs: P.kwargs) -> R:
        contention = False
        unavailable = False
        try:
            return operation(*args, **kwargs)
        except OperationalError as error:
            contention = is_sqlite_contention(error)
            unavailable = not contention
        except InterfaceError:
            unavailable = True
        except SQLAlchemyError:
            pass
        if contention:
            raise PersistenceContentionError("Persistence is temporarily contended.") from None
        if unavailable:
            raise ArtifactStreamStorageUnavailableError(
                "artifact stream manifest storage is unavailable"
            ) from None
        raise ArtifactStreamStorageError("artifact stream manifest query failed") from None

    return translated


class FileSystemArtifactStreamReader(ArtifactStreamReader):
    """Open immutable descriptor-bound streams in a caller-owned transaction."""

    __slots__ = ("_chunk_size", "_root", "_session")

    def __init__(self, session: Session, artifact_root: Path, *, chunk_size: int) -> None:
        value = cast(object, session)
        if not isinstance(value, Session):
            raise TypeError("artifact stream reader requires a Session")
        if type(chunk_size) is not int or not 1 <= chunk_size <= MAX_ARTIFACT_CHUNK_BYTES:
            raise ArtifactStreamInvalidError("artifact stream chunk size is outside the range")
        self._session = value
        self._root = resolve_artifact_root(artifact_root)
        self._chunk_size = chunk_size

    @_translate_stream_storage_errors
    def open(
        self,
        artifact_id: ArtifactId,
        *,
        byte_range: ArtifactByteRange | None = None,
    ) -> ArtifactByteStream:
        """Verify one committed immutable file and return its owned descriptor."""
        if not self._session.in_transaction():
            raise ArtifactStreamInvalidError(
                "artifact stream reader requires a caller-owned transaction"
            )
        if type(artifact_id) is not ArtifactId:
            raise ArtifactStreamInvalidError("artifact stream identity is invalid")
        if byte_range is not None and type(byte_range) is not ArtifactByteRange:
            raise ArtifactStreamInvalidError("artifact stream byte range is invalid")
        row = (
            self._session.execute(
                select(artifact_manifests).where(
                    artifact_manifests.c.artifact_id == str(artifact_id)
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise ArtifactStreamNotFoundError("artifact stream does not exist")
        try:
            manifest = _manifest_from_row(row)
        except ArtifactManifestCorruptionError:
            raise ArtifactStreamIntegrityError("artifact stream manifest is corrupt") from None
        if manifest.artifact_id != artifact_id:
            raise ArtifactStreamIntegrityError("artifact stream manifest is corrupt")
        parent = self._session.execute(
            select(runs.c.created_at)
            .select_from(run_nodes.join(runs, runs.c.run_id == run_nodes.c.run_id))
            .where(
                run_nodes.c.run_id == str(manifest.run_id),
                run_nodes.c.node_id == str(manifest.node_id),
            )
        ).one_or_none()
        try:
            if parent is None or manifest.created_at < _stored_timestamp(parent[0]):
                raise ArtifactStreamIntegrityError("artifact stream parent is corrupt")
        except ArtifactManifestCorruptionError:
            raise ArtifactStreamIntegrityError("artifact stream parent is corrupt") from None
        return _open_verified_stream(
            self._root,
            manifest,
            byte_range=byte_range,
            chunk_size=self._chunk_size,
        )


class FileArtifactByteStream(ArtifactByteStream):
    """One descriptor-backed stream that verifies identity when it closes."""

    __slots__ = (
        "_chunk_size",
        "_descriptor",
        "_expected_identity",
        "_lock",
        "_metadata",
        "_path",
        "_remaining",
    )

    def __init__(
        self,
        descriptor: int,
        path: Path,
        metadata: ArtifactStreamMetadata,
        *,
        chunk_size: int,
        expected_identity: tuple[int, int, int, int],
    ) -> None:
        self._descriptor: int | None = descriptor
        self._path = path
        self._metadata = metadata
        self._chunk_size = chunk_size
        self._remaining = metadata.content_length
        self._expected_identity = expected_identity
        self._lock = RLock()

    @property
    def metadata(self) -> ArtifactStreamMetadata:
        """Return immutable verified response metadata."""
        return self._metadata

    def __repr__(self) -> str:
        return (
            "FileArtifactByteStream("
            f"artifact_id={self._metadata.artifact_id!r}, "
            f"content_length={self._metadata.content_length!r}, "
            f"closed={self._descriptor is None!r})"
        )

    def __iter__(self) -> Iterator[bytes]:
        return self

    def __next__(self) -> bytes:
        with self._lock:
            descriptor = self._descriptor
            if descriptor is None:
                raise StopIteration
            if self._remaining == 0:
                self._close_locked()
                raise StopIteration
            try:
                chunk = os.read(descriptor, min(self._chunk_size, self._remaining))
            except OSError:
                self._close_locked()
                raise ArtifactStreamStorageError("artifact stream read failed") from None
            if not chunk:
                self._close_locked()
                raise ArtifactStreamIntegrityError("artifact stream ended before its range")
            self._remaining -= len(chunk)
            if self._remaining == 0:
                self._close_locked()
            return chunk

    def close(self) -> None:
        """Idempotently verify and close the owned descriptor."""
        with self._lock:
            self._close_locked()

    def __enter__(self) -> FileArtifactByteStream:
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: object,
    ) -> None:
        del exception_type, exception, traceback
        self.close()

    def _close_locked(self) -> None:
        descriptor = self._descriptor
        if descriptor is None:
            return
        integrity_failure = False
        try:
            current = os.fstat(descriptor)
            installed = os.stat(self._path, follow_symlinks=False)
            current_identity = _identity(current)
            if (
                current_identity != self._expected_identity
                or _identity(installed) != self._expected_identity
            ):
                integrity_failure = True
        except OSError:
            integrity_failure = True
        close_failure = False
        try:
            os.close(descriptor)
        except OSError:
            close_failure = True
        self._descriptor = None
        if close_failure:
            raise ArtifactStreamStorageError(
                "artifact stream descriptor could not be closed"
            ) from None
        if integrity_failure:
            raise ArtifactStreamIntegrityError("artifact stream file identity changed")


def _open_verified_stream(
    root: Path,
    manifest: ArtifactManifestRecord,
    *,
    byte_range: ArtifactByteRange | None,
    chunk_size: int,
) -> FileArtifactByteStream:
    if type(manifest) is not ArtifactManifestRecord:
        raise TypeError("artifact stream manifest is invalid")
    descriptor: int | None = None
    try:
        path = resolve_artifact_path(root, manifest.relative_path)
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size != manifest.byte_size:
            raise ArtifactStreamIntegrityError("artifact stream file differs from its manifest")
        expected_identity = _identity(before)
        digest = hashlib.sha256()
        os.lseek(descriptor, 0, os.SEEK_SET)
        while True:
            chunk = os.read(descriptor, _VERIFY_CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
        after = os.fstat(descriptor)
        installed = os.stat(path, follow_symlinks=False)
        if (
            _identity(after) != expected_identity
            or _identity(installed) != expected_identity
            or digest.hexdigest() != manifest.sha256
        ):
            raise ArtifactStreamIntegrityError("artifact stream file differs from its manifest")
        start = 0 if byte_range is None else byte_range.start
        end = manifest.byte_size if byte_range is None else byte_range.end_exclusive
        if byte_range is not None and (start >= manifest.byte_size or end > manifest.byte_size):
            raise ArtifactStreamRangeError("artifact byte range is not satisfiable")
        os.lseek(descriptor, start, os.SEEK_SET)
        metadata = ArtifactStreamMetadata(
            artifact_id=manifest.artifact_id,
            relative_path=manifest.relative_path,
            media_type=manifest.media_type,
            schema_version=manifest.schema_version,
            total_byte_size=manifest.byte_size,
            range_start=start,
            range_end_exclusive=end,
            content_sha256=manifest.sha256,
        )
        stream = FileArtifactByteStream(
            descriptor,
            path,
            metadata,
            chunk_size=chunk_size,
            expected_identity=expected_identity,
        )
        descriptor = None
        return stream
    except ArtifactStreamRangeError, ArtifactStreamIntegrityError:
        raise
    except ArtifactPathError, FileNotFoundError:
        raise ArtifactStreamIntegrityError("artifact stream file is not safely confined") from None
    except OSError:
        raise ArtifactStreamStorageError("artifact stream file could not be opened") from None
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                raise ArtifactStreamStorageError(
                    "artifact stream descriptor could not be closed"
                ) from None


def _identity(value: os.stat_result) -> tuple[int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns
