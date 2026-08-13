"""Atomicity, bounds, and failure tests for filesystem artifact writes."""

# pyright: reportPrivateUsage=false

import hashlib
import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path
from threading import Barrier
from typing import Any, BinaryIO, Never, cast

import pytest

from paritygrid.adapters.artifacts import FileSystemArtifactWriter
from paritygrid.adapters.artifacts import writer as runtime
from paritygrid.application.ports import (
    MAX_ARTIFACT_WRITE_BYTES,
    ArtifactAlreadyExistsError,
    ArtifactInvalidWriteError,
    ArtifactPathError,
    ArtifactPublishOutcomeUnknownError,
    ArtifactRelativePath,
    ArtifactSizeLimitError,
    ArtifactStorageError,
)


def _residue(root: Path) -> tuple[Path, ...]:
    return tuple(sorted(path for path in root.rglob("*") if path.name.startswith(".pg-")))


def test_writer_streams_hashes_and_publishes_an_immutable_file(tmp_path: Path) -> None:
    root = tmp_path / "artifacts Café % ميناء"
    root.mkdir()
    writer = FileSystemArtifactWriter(root, maximum_bytes=64)
    relative = ArtifactRelativePath("runs/run-example/raw/part-00001.bin")

    receipt = writer.write(relative, (b"alpha", b"", b"beta"))

    target = root / str(relative)
    assert target.read_bytes() == b"alphabeta"
    assert receipt.relative_path == relative
    assert receipt.byte_size == 9
    assert receipt.sha256 == hashlib.sha256(b"alphabeta").hexdigest()
    assert _residue(root) == ()
    with pytest.raises(ArtifactAlreadyExistsError):
        writer.write(relative, (b"replacement",))
    assert target.read_bytes() == b"alphabeta"


def test_writer_supports_an_empty_artifact(tmp_path: Path) -> None:
    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=1)
    relative = ArtifactRelativePath("empty.bin")

    receipt = writer.write(relative, ())

    assert (tmp_path / "empty.bin").read_bytes() == b""
    assert receipt.byte_size == 0
    assert receipt.sha256 == hashlib.sha256(b"").hexdigest()


@pytest.mark.parametrize("maximum", [True, 0, MAX_ARTIFACT_WRITE_BYTES + 1, "10"])
def test_writer_rejects_invalid_configured_limits(tmp_path: Path, maximum: object) -> None:
    with pytest.raises(ArtifactInvalidWriteError, match="byte limit"):
        FileSystemArtifactWriter(tmp_path, maximum_bytes=cast(Any, maximum))


@pytest.mark.parametrize("chunks", [b"content", "content", bytearray(b"content"), 7])
def test_writer_rejects_non_iterable_or_ambiguous_chunk_sources(
    tmp_path: Path, chunks: object
) -> None:
    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=32)
    with pytest.raises(ArtifactInvalidWriteError, match="chunks"):
        writer.write(ArtifactRelativePath("file.bin"), cast(Any, chunks))
    assert tuple(tmp_path.iterdir()) == ()


@pytest.mark.parametrize("chunk", [bytearray(b"a"), memoryview(b"a"), "a", 1])
def test_writer_rejects_non_bytes_chunks_and_removes_staging(tmp_path: Path, chunk: object) -> None:
    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=32)
    with pytest.raises(ArtifactInvalidWriteError, match="immutable bytes"):
        writer.write(ArtifactRelativePath("file.bin"), cast(Any, (chunk,)))
    assert tuple(tmp_path.iterdir()) == ()


def test_writer_enforces_chunk_and_total_limits_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=4)
    with pytest.raises(ArtifactSizeLimitError):
        writer.write(ArtifactRelativePath("total.bin"), (b"123", b"45"))
    monkeypatch.setattr(runtime, "MAX_ARTIFACT_CHUNK_BYTES", 2)
    with pytest.raises(ArtifactInvalidWriteError, match="chunk exceeds"):
        writer.write(ArtifactRelativePath("chunk.bin"), (b"123",))
    assert tuple(tmp_path.iterdir()) == ()


def test_invalid_path_type_fails_before_filesystem_mutation(tmp_path: Path) -> None:
    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=4)
    with pytest.raises(ArtifactInvalidWriteError, match="must be validated"):
        writer.write(cast(Any, "file.bin"), (b"a",))
    assert tuple(tmp_path.iterdir()) == ()


def test_parent_file_and_creation_failure_are_typed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    blocked = tmp_path / "blocked"
    blocked.write_bytes(b"file")
    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=4)
    with pytest.raises(ArtifactPathError, match="parent must be a directory"):
        writer.write(ArtifactRelativePath("blocked/file.bin"), (b"a",))

    original = Path.mkdir

    def fail_mkdir(
        path: Path,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        if path.name == "new":
            raise OSError("denied")
        original(path, mode=mode, parents=parents, exist_ok=exist_ok)

    monkeypatch.setattr(Path, "mkdir", fail_mkdir)
    with pytest.raises(ArtifactStorageError, match="directory"):
        writer.write(ArtifactRelativePath("new/file.bin"), (b"a",))


class _Interrupt(BaseException):
    pass


def test_chunk_source_base_exception_leaves_no_file_or_staging(tmp_path: Path) -> None:
    def interrupted() -> Iterator[bytes]:
        yield b"partial"
        raise _Interrupt()

    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=64)
    with pytest.raises(_Interrupt):
        writer.write(ArtifactRelativePath("file.bin"), interrupted())
    assert tuple(tmp_path.iterdir()) == ()


@pytest.mark.parametrize("stage", ["create", "write", "flush", "publish"])
def test_prepublication_operating_system_failures_leave_no_final_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=64)
    relative = ArtifactRelativePath("nested/file.bin")

    def fail(*_args: object, **_kwargs: object) -> Never:
        raise OSError

    def fail_sync(_descriptor: int) -> Never:
        raise OSError

    if stage == "create":
        monkeypatch.setattr(runtime.tempfile, "mkstemp", fail)
    elif stage == "write":
        monkeypatch.setattr(runtime, "_write_all", fail)
    elif stage == "flush":
        monkeypatch.setattr(runtime.os, "fsync", fail_sync)
    else:
        monkeypatch.setattr(runtime, "_publish_no_replace", fail)

    with pytest.raises(ArtifactStorageError):
        writer.write(relative, (b"content",))
    assert not (tmp_path / str(relative)).exists()
    assert _residue(tmp_path) == ()


def test_postpublication_sync_failure_reports_unknown_but_keeps_complete_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=64)
    relative = ArtifactRelativePath("file.bin")

    def fail_sync(_path: Path) -> Never:
        raise OSError

    monkeypatch.setattr(runtime, "_sync_directory", fail_sync)

    with pytest.raises(ArtifactPublishOutcomeUnknownError):
        writer.write(relative, (b"complete",))
    assert (tmp_path / "file.bin").read_bytes() == b"complete"
    assert _residue(tmp_path) == ()


def test_post_link_failure_is_mapped_to_unknown_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=64)

    def fail_after_link(_temporary: Path, target: Path) -> Never:
        target.write_bytes(b"complete")
        raise runtime._PublishedButIncompleteError

    monkeypatch.setattr(runtime, "_publish_no_replace", fail_after_link)
    with pytest.raises(ArtifactPublishOutcomeUnknownError):
        writer.write(ArtifactRelativePath("file.bin"), (b"complete",))
    assert (tmp_path / "file.bin").read_bytes() == b"complete"


def test_post_staging_path_revalidation_failure_removes_temporary_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=64)
    original = runtime.resolve_artifact_path
    calls = 0

    def reject_final_check(root: Path, relative: ArtifactRelativePath) -> Path:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise ArtifactPathError("changed path")
        return original(root, relative)

    monkeypatch.setattr(runtime, "resolve_artifact_path", reject_final_check)
    with pytest.raises(ArtifactPathError, match="changed path"):
        writer.write(ArtifactRelativePath("file.bin"), (b"content",))
    assert tuple(tmp_path.iterdir()) == ()


def test_file_object_open_failure_closes_raw_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=64)
    original_close = os.close
    closed: list[int] = []

    def fail_open(*_args: object, **_kwargs: object) -> Never:
        raise OSError("open failure")

    def close_descriptor(descriptor: int) -> None:
        closed.append(descriptor)
        original_close(descriptor)

    monkeypatch.setattr(runtime.os, "fdopen", fail_open)
    monkeypatch.setattr(runtime.os, "close", close_descriptor)
    with pytest.raises(ArtifactStorageError):
        writer.write(ArtifactRelativePath("file.bin"), (b"content",))
    assert len(closed) == 1
    assert tuple(tmp_path.iterdir()) == ()


def test_raw_descriptor_close_failure_remains_typed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=64)
    original_close = os.close

    def fail_open(*_args: object, **_kwargs: object) -> Never:
        raise OSError("open failure")

    def close_then_fail(descriptor: int) -> Never:
        original_close(descriptor)
        raise OSError("close failure")

    monkeypatch.setattr(runtime.os, "fdopen", fail_open)
    monkeypatch.setattr(runtime.os, "close", close_then_fail)
    with pytest.raises(ArtifactStorageError, match="descriptor could not be closed"):
        writer.write(ArtifactRelativePath("file.bin"), (b"content",))
    assert tuple(tmp_path.iterdir()) == ()


def test_unpublished_staging_cleanup_failure_is_explicit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=64)
    original_unlink = Path.unlink

    def fail_publish(*_args: object) -> Never:
        raise OSError("publish failure")

    def fail_staging_unlink(path: Path, *, missing_ok: bool = False) -> None:
        if path.name.startswith(".pg-"):
            raise OSError("cleanup failure")
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(runtime, "_publish_no_replace", fail_publish)
    monkeypatch.setattr(Path, "unlink", fail_staging_unlink)
    with pytest.raises(ArtifactStorageError, match="staging file could not be removed"):
        writer.write(ArtifactRelativePath("file.bin"), (b"content",))
    residue = _residue(tmp_path)
    assert len(residue) == 1
    original_unlink(residue[0])


def test_published_staging_cleanup_failure_preserves_unknown_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=64)
    original_unlink = Path.unlink

    def fail_after_link(_temporary: Path, target: Path) -> Never:
        target.write_bytes(b"complete")
        raise runtime._PublishedButIncompleteError

    def fail_staging_unlink(path: Path, *, missing_ok: bool = False) -> None:
        if path.name.startswith(".pg-"):
            raise OSError("cleanup failure")
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(runtime, "_publish_no_replace", fail_after_link)
    monkeypatch.setattr(Path, "unlink", fail_staging_unlink)
    with pytest.raises(ArtifactPublishOutcomeUnknownError):
        writer.write(ArtifactRelativePath("file.bin"), (b"complete",))
    assert (tmp_path / "file.bin").read_bytes() == b"complete"
    residue = _residue(tmp_path)
    assert len(residue) == 1
    original_unlink(residue[0])


def test_staging_inspection_failure_is_typed_and_cleanup_can_be_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=64)
    original_exists = Path.exists
    original_unlink = Path.unlink

    def fail_publish(*_args: object) -> Never:
        raise OSError("publish failure")

    def fail_staging_exists(path: Path) -> bool:
        if path.name.startswith(".pg-"):
            raise OSError("inspection failure")
        return original_exists(path)

    with monkeypatch.context() as context:
        context.setattr(runtime, "_publish_no_replace", fail_publish)
        context.setattr(Path, "exists", fail_staging_exists)
        with pytest.raises(ArtifactStorageError, match="staging file could not be removed"):
            writer.write(ArtifactRelativePath("file.bin"), (b"content",))
    residue = _residue(tmp_path)
    assert len(residue) == 1
    original_unlink(residue[0])


def test_destination_race_never_overwrites_the_winner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=64)
    target = tmp_path / "file.bin"

    def racing_publish(temporary: Path, destination: Path) -> None:
        destination.write_bytes(b"winner")
        raise FileExistsError

    monkeypatch.setattr(runtime, "_publish_no_replace", racing_publish)
    with pytest.raises(ArtifactAlreadyExistsError):
        writer.write(ArtifactRelativePath("file.bin"), (b"loser",))
    assert target.read_bytes() == b"winner"
    assert _residue(tmp_path) == ()


def test_two_concurrent_writers_have_exactly_one_winner(tmp_path: Path) -> None:
    writer = FileSystemArtifactWriter(tmp_path, maximum_bytes=64)
    relative = ArtifactRelativePath("same.bin")
    barrier = Barrier(2)

    def publish(content: bytes) -> bytes | type[ArtifactAlreadyExistsError]:
        barrier.wait(timeout=5)
        try:
            writer.write(relative, (content,))
        except ArtifactAlreadyExistsError:
            return ArtifactAlreadyExistsError
        return content

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(publish, (b"first", b"second")))

    durable = (tmp_path / "same.bin").read_bytes()
    assert results.count(ArtifactAlreadyExistsError) == 1
    assert durable in {b"first", b"second"}
    assert durable in results
    assert _residue(tmp_path) == ()


@pytest.mark.parametrize("written", [None, 0, -1, "1"])
def test_short_or_invalid_low_level_writes_fail_closed(written: object) -> None:
    class ShortWriter(BytesIO):
        def write(self, value: Any) -> Any:
            del value
            return written

    with pytest.raises(OSError, match="short artifact write"):
        runtime._write_all(cast(BinaryIO, ShortWriter()), b"content")


def test_write_all_retries_a_partial_low_level_write() -> None:
    class PartialWriter(BytesIO):
        def write(self, value: Any) -> int:
            return super().write(bytes(value)[:1])

    stream = PartialWriter()
    runtime._write_all(cast(BinaryIO, stream), b"abc")
    assert stream.getvalue() == b"abc"


def test_posix_publish_and_directory_sync_helpers_are_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    temporary = tmp_path / "temporary"
    target = tmp_path / "target"
    temporary.write_bytes(b"content")
    monkeypatch.setattr(runtime, "_IS_WINDOWS", False)
    runtime._publish_no_replace(temporary, target)
    assert target.read_bytes() == b"content"
    assert not temporary.exists()

    calls: list[tuple[str, int]] = []

    def fake_open(*_args: object) -> int:
        return 17

    def record_sync(descriptor: int) -> None:
        calls.append(("sync", descriptor))

    def record_close(descriptor: int) -> None:
        calls.append(("close", descriptor))

    monkeypatch.setattr(runtime.os, "open", fake_open)
    monkeypatch.setattr(runtime.os, "fsync", record_sync)
    monkeypatch.setattr(runtime.os, "close", record_close)
    runtime._sync_directory(tmp_path)
    assert calls == [("sync", 17), ("close", 17)]


def test_parent_helper_rejects_a_link_created_during_directory_preparation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    linked = tmp_path / "linked"
    linked.mkdir()
    original = Path.is_symlink

    def identifies_link(path: Path) -> bool:
        return path == linked or original(path)

    monkeypatch.setattr(Path, "is_symlink", identifies_link)
    with pytest.raises(ArtifactPathError, match="filesystem link"):
        runtime._prepare_parent(tmp_path, ArtifactRelativePath("linked/file.bin"))


def test_post_link_cleanup_failure_is_classified_as_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    temporary = tmp_path / "temporary"
    target = tmp_path / "target"
    temporary.write_bytes(b"content")
    monkeypatch.setattr(runtime, "_IS_WINDOWS", False)
    original = Path.unlink

    def fail_temporary(path: Path, *, missing_ok: bool = False) -> None:
        if path == temporary:
            raise OSError("denied")
        original(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", fail_temporary)
    with pytest.raises(runtime._PublishedButIncompleteError):
        runtime._publish_no_replace(temporary, target)
    assert target.read_bytes() == b"content"


def test_directory_sync_always_closes_after_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime, "_IS_WINDOWS", False)
    closed: list[int] = []

    def fake_open(*_args: object) -> int:
        return 19

    def fail_sync(_descriptor: int) -> Never:
        raise OSError("sync failure")

    monkeypatch.setattr(runtime.os, "open", fake_open)
    monkeypatch.setattr(runtime.os, "fsync", fail_sync)
    monkeypatch.setattr(runtime.os, "close", closed.append)
    with pytest.raises(OSError, match="sync failure"):
        runtime._sync_directory(Path("ignored"))
    assert closed == [19]


def test_windows_directory_sync_is_an_explicit_no_op(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime, "_IS_WINDOWS", True)

    def fail_open(*_args: object) -> Never:
        pytest.fail("must not open")

    monkeypatch.setattr(runtime.os, "open", fail_open)
    runtime._sync_directory(Path("ignored"))
