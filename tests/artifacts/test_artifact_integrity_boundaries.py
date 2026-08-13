"""Adversarial artifact integrity scanner boundary tests."""

# pyright: reportPrivateUsage=false, reportIncompatibleMethodOverride=false, reportUnknownArgumentType=false, reportUnknownLambdaType=false

import hashlib
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Never

import pytest
from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from paritygrid.adapters.artifacts import integrity as runtime
from paritygrid.application.ports import (
    ArtifactIntegrityIssueKind,
    ArtifactIntegrityScanCorruptionError,
    ArtifactIntegrityScanLimitError,
    ArtifactIntegrityScanStorageError,
    ArtifactIntegrityScanStorageUnavailableError,
    ArtifactManifestCorruptionError,
    ArtifactManifestRecord,
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


class _Rows:
    def __init__(self, rows: list[object]) -> None:
        self._rows = rows

    def mappings(self) -> _Rows:
        return self

    def all(self) -> list[object]:
        return self._rows


class _Session(Session):
    def __init__(self, rows: list[object]) -> None:
        super().__init__()
        self._rows = rows

    def in_transaction(self) -> bool:
        return True

    def execute(self, _statement: object, _parameters: object = None) -> _Rows:
        return _Rows(self._rows)


def _record(relative_path: str = "runs/file.bin") -> ArtifactManifestRecord:
    content = b"content"
    return ArtifactManifestRecord(
        ArtifactId("art_boundary"),
        RunId("run_boundary"),
        NodeId("nod_boundary"),
        PartitionKey("all"),
        ArtifactRelativePath(relative_path),
        "application/octet-stream",
        1,
        len(content),
        0,
        hashlib.sha256(content).hexdigest(),
        UtcTimestamp(datetime(2026, 8, 13, 12, tzinfo=UTC)),
    )


def test_storage_error_translation_is_specific_and_redacted() -> None:
    @runtime._translate_scan_storage_errors
    def fail(error: BaseException) -> None:
        raise error

    busy = OperationalError(
        "secret sql", {"private": "value"}, _SQLiteFailureError(sqlite3.SQLITE_BUSY)
    )
    unavailable = OperationalError("secret sql", {}, OSError("private"))
    with pytest.raises(PersistenceContentionError):
        fail(busy)
    with pytest.raises(ArtifactIntegrityScanStorageUnavailableError) as operational:
        fail(unavailable)
    with pytest.raises(ArtifactIntegrityScanStorageUnavailableError):
        fail(InterfaceError("secret sql", {}, OSError()))
    with pytest.raises(ArtifactIntegrityScanStorageError) as generic:
        fail(SQLAlchemyError("secret private"))
    assert "secret" not in str(operational.value)
    assert "secret" not in str(generic.value)
    assert operational.value.__cause__ is None


def test_manifest_limits_corruption_and_duplicate_paths_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scanner = runtime.FileSystemArtifactIntegrityScanner(_Session([object()]), tmp_path)
    monkeypatch.setattr(runtime, "MAX_ARTIFACT_INTEGRITY_ENTRIES", 0)
    with pytest.raises(ArtifactIntegrityScanLimitError, match="manifest inventory"):
        scanner.scan()

    monkeypatch.setattr(runtime, "MAX_ARTIFACT_INTEGRITY_ENTRIES", 1_000_000)

    def corrupt(_row: object) -> Never:
        raise ArtifactManifestCorruptionError("private row")

    monkeypatch.setattr(runtime, "_manifest_from_row", corrupt)
    with pytest.raises(ArtifactIntegrityScanCorruptionError, match="inventory"):
        scanner.scan()

    monkeypatch.setattr(runtime, "_manifest_from_row", lambda _row: _record())
    duplicate = runtime.FileSystemArtifactIntegrityScanner(_Session([object(), object()]), tmp_path)
    with pytest.raises(ArtifactIntegrityScanCorruptionError, match="not unique"):
        duplicate.scan()


def test_issue_limit_and_path_identity_race_are_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    orphan = tmp_path / "orphan.bin"
    orphan.write_bytes(b"orphan")
    empty = runtime.FileSystemArtifactIntegrityScanner(_Session([]), tmp_path)
    monkeypatch.setattr(runtime, "MAX_ARTIFACT_INTEGRITY_ISSUES", 0)
    with pytest.raises(ArtifactIntegrityScanLimitError, match="issue inventory"):
        empty.scan()

    monkeypatch.setattr(runtime, "MAX_ARTIFACT_INTEGRITY_ISSUES", 100_000)
    orphan.unlink()
    manifested = tmp_path / "runs" / "file.bin"
    manifested.parent.mkdir()
    manifested.write_bytes(b"content")
    scanner = runtime.FileSystemArtifactIntegrityScanner(_Session([object()]), tmp_path)
    monkeypatch.setattr(runtime, "_manifest_from_row", lambda _row: _record())
    monkeypatch.setattr(
        runtime, "resolve_artifact_path", lambda _root, _relative: tmp_path / "changed.bin"
    )
    report = scanner.scan()
    assert tuple(issue.kind for issue in report.issues) == (
        ArtifactIntegrityIssueKind.INVALID_FILE,
    )


def test_filesystem_limit_special_entry_and_read_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "file.bin").write_bytes(b"content")
    monkeypatch.setattr(runtime, "MAX_ARTIFACT_INTEGRITY_ENTRIES", 0)
    with pytest.raises(ArtifactIntegrityScanLimitError, match="filesystem inventory"):
        runtime._observe_files(tmp_path)

    monkeypatch.setattr(runtime, "MAX_ARTIFACT_INTEGRITY_ENTRIES", 1_000_000)

    class _Entry:
        name = "special"
        path = str(tmp_path / "special")

        def is_symlink(self) -> bool:
            return False

        def is_dir(self, *, follow_symlinks: bool) -> bool:
            del follow_symlinks
            return False

        def is_file(self, *, follow_symlinks: bool) -> bool:
            del follow_symlinks
            return False

    class _Scan:
        def __init__(self, entries: list[_Entry]) -> None:
            self._entries = entries

        def __enter__(self) -> list[_Entry]:
            return self._entries

        def __exit__(self, *_args: object) -> None:
            return None

    class _LinkedEntry(_Entry):
        name = "linked"
        path = str(tmp_path / "linked")

        def is_symlink(self) -> bool:
            return True

    monkeypatch.setattr(runtime.os, "scandir", lambda _path: _Scan([_LinkedEntry()]))
    linked_observed, linked_count, linked_issues = runtime._observe_files(tmp_path)
    assert linked_observed == ()
    assert linked_count == 0
    assert len(linked_issues) == 1

    monkeypatch.setattr(runtime.os, "scandir", lambda _path: _Scan([_Entry()]))
    observed, count, issues = runtime._observe_files(tmp_path)
    assert observed == ()
    assert count == 0
    assert len(issues) == 1

    def fail_scan(_path: Path) -> Never:
        raise OSError("private path")

    monkeypatch.setattr(runtime.os, "scandir", fail_scan)
    with pytest.raises(ArtifactIntegrityScanStorageError) as error:
        runtime._observe_files(tmp_path)
    assert "private" not in str(error.value)


def test_link_is_reported_without_being_followed(tmp_path: Path) -> None:
    target = tmp_path / "outside.bin"
    target.write_bytes(b"outside")
    link = tmp_path / "linked.bin"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("filesystem links are unavailable")
    observed, count, issues = runtime._observe_files(tmp_path)
    assert tuple(str(item.relative_path) for item in observed) == ("outside.bin",)
    assert count == 1
    assert len(issues) == 1
    assert issues[0].kind is ArtifactIntegrityIssueKind.UNSAFE_ENTRY
    assert target.read_bytes() == b"outside"
