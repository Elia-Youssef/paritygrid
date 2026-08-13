"""Bounded atomic publication of immutable artifact files."""

import hashlib
import os
import tempfile
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import BinaryIO, Protocol, cast

from paritygrid.adapters.artifacts.paths import resolve_artifact_path, resolve_artifact_root
from paritygrid.application.ports.artifacts import (
    MAX_ARTIFACT_CHUNK_BYTES,
    MAX_ARTIFACT_WRITE_BYTES,
    ArtifactAlreadyExistsError,
    ArtifactInvalidWriteError,
    ArtifactPathError,
    ArtifactPublishOutcomeUnknownError,
    ArtifactRelativePath,
    ArtifactSizeLimitError,
    ArtifactStorageError,
    ArtifactWriter,
    ArtifactWriteReceipt,
)

_IS_WINDOWS = os.name == "nt"


class _PublishedButIncompleteError(OSError):
    """Internal signal that the final name now exists."""


class _Digest(Protocol):
    def update(self, value: bytes, /) -> None: ...

    def hexdigest(self) -> str: ...


class FileSystemArtifactWriter(ArtifactWriter):
    """Stream new files beneath one validated root without overwriting."""

    __slots__ = ("_maximum_bytes", "_root")

    def __init__(self, root: Path, *, maximum_bytes: int) -> None:
        maximum = cast(object, maximum_bytes)
        if isinstance(maximum, bool) or not isinstance(maximum, int):
            raise ArtifactInvalidWriteError("artifact byte limit must be an integer")
        if not 1 <= maximum <= MAX_ARTIFACT_WRITE_BYTES:
            raise ArtifactInvalidWriteError("artifact byte limit is outside the supported range")
        self._root = resolve_artifact_root(root)
        self._maximum_bytes = maximum

    def write(
        self,
        relative_path: ArtifactRelativePath,
        chunks: Iterable[bytes],
    ) -> ArtifactWriteReceipt:
        """Stage, flush, and atomically publish one immutable artifact."""
        relative = cast(object, relative_path)
        if not isinstance(relative, ArtifactRelativePath):
            raise ArtifactInvalidWriteError("artifact write path must be validated")
        iterator = _chunk_iterator(chunks)
        target = resolve_artifact_path(self._root, relative)
        parent = _prepare_parent(self._root, relative)
        target = resolve_artifact_path(self._root, relative)
        if target.exists():
            raise ArtifactAlreadyExistsError("artifact destination already exists")

        temporary: Path | None = None
        published = False
        descriptor: int | None = None
        try:
            descriptor, temporary_text = tempfile.mkstemp(prefix=".pg-", suffix=".tmp", dir=parent)
            temporary = Path(temporary_text)
            digest = hashlib.sha256()
            byte_size = 0
            with os.fdopen(descriptor, "wb", closefd=True) as stream:
                descriptor = None
                byte_size = self._write_chunks(stream, iterator, digest)
                stream.flush()
                os.fsync(stream.fileno())
            target = resolve_artifact_path(self._root, relative)
            _publish_no_replace(temporary, target)
            published = True
            _sync_directory(parent)
        except FileExistsError:
            raise ArtifactAlreadyExistsError("artifact destination already exists") from None
        except _PublishedButIncompleteError:
            published = True
            raise ArtifactPublishOutcomeUnknownError(
                "artifact was published but final durability could not be confirmed"
            ) from None
        except ArtifactPathError:
            raise
        except OSError:
            if published:
                raise ArtifactPublishOutcomeUnknownError(
                    "artifact was published but final durability could not be confirmed"
                ) from None
            raise ArtifactStorageError("artifact could not be staged or published") from None
        finally:
            _cleanup_staging(descriptor, temporary, published=published)

        return ArtifactWriteReceipt(relative, byte_size, digest.hexdigest())

    def _write_chunks(
        self,
        stream: BinaryIO,
        chunks: Iterator[object],
        digest: _Digest,
    ) -> int:
        byte_size = 0
        for chunk in chunks:
            if type(chunk) is not bytes:
                raise ArtifactInvalidWriteError("artifact chunks must be immutable bytes")
            if len(chunk) > MAX_ARTIFACT_CHUNK_BYTES:
                raise ArtifactInvalidWriteError("artifact chunk exceeds the byte limit")
            next_size = byte_size + len(chunk)
            if next_size > self._maximum_bytes:
                raise ArtifactSizeLimitError("artifact exceeds its configured byte limit")
            _write_all(stream, chunk)
            digest.update(chunk)
            byte_size = next_size
        return byte_size


def _chunk_iterator(chunks: Iterable[bytes]) -> Iterator[object]:
    value = cast(object, chunks)
    if isinstance(value, str | bytes | bytearray | memoryview):
        raise ArtifactInvalidWriteError("artifact chunks must be an iterable of bytes")
    try:
        return iter(cast(Iterable[object], value))
    except TypeError:
        raise ArtifactInvalidWriteError("artifact chunks must be iterable") from None


def _prepare_parent(root: Path, relative_path: ArtifactRelativePath) -> Path:
    parent = root
    for segment in relative_path.parts[:-1]:
        parent = parent / segment
        try:
            parent.mkdir(mode=0o700, exist_ok=True)
        except FileExistsError:
            pass
        except OSError:
            raise ArtifactStorageError(
                "artifact destination directory could not be created"
            ) from None
        if parent.is_symlink() or parent.is_junction():
            raise ArtifactPathError("artifact path cannot traverse a filesystem link")
        if not parent.is_dir():
            raise ArtifactPathError("artifact destination parent must be a directory")
    return parent


def _write_all(stream: BinaryIO, value: bytes) -> None:
    view = memoryview(value)
    while view:
        written = cast(object, stream.write(view))
        if not isinstance(written, int) or written <= 0:
            raise OSError("short artifact write")
        view = view[written:]


def _remove_temporary(temporary: Path | None, *, published: bool) -> None:
    if temporary is None:
        return
    try:
        if not temporary.exists():
            return
        temporary.unlink()
    except OSError:
        if published:
            return
        raise ArtifactStorageError("artifact staging file could not be removed") from None


def _cleanup_staging(
    descriptor: int | None,
    temporary: Path | None,
    *,
    published: bool,
) -> None:
    close_error: ArtifactStorageError | None = None
    if descriptor is not None:
        try:
            _close_descriptor(descriptor)
        except ArtifactStorageError as error:
            close_error = error
    _remove_temporary(temporary, published=published)
    if close_error is not None:
        raise close_error


def _close_descriptor(descriptor: int) -> None:
    try:
        os.close(descriptor)
    except OSError:
        raise ArtifactStorageError("artifact staging descriptor could not be closed") from None


def _publish_no_replace(temporary: Path, target: Path) -> None:
    if _IS_WINDOWS:
        os.rename(temporary, target)
        return
    os.link(temporary, target, follow_symlinks=False)
    try:
        temporary.unlink()
    except OSError as error:
        raise _PublishedButIncompleteError from error


def _sync_directory(directory: Path) -> None:
    if _IS_WINDOWS:
        return
    descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
