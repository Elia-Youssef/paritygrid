"""Filesystem confinement tests for artifact paths."""

import os
from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.adapters.artifacts import resolve_artifact_path
from paritygrid.application.ports import ArtifactPathError, ArtifactRelativePath


def test_resolution_returns_only_a_path_beneath_the_root(tmp_path: Path) -> None:
    root = tmp_path / "artifacts Café % ميناء"
    root.mkdir()
    relative = ArtifactRelativePath("runs/run-example/raw/part-00001.parquet")

    resolved = resolve_artifact_path(root, relative)

    assert resolved == root.resolve() / "runs/run-example/raw/part-00001.parquet"
    assert resolved.is_relative_to(root.resolve())
    assert not resolved.exists()


def test_resolution_requires_an_absolute_path_value() -> None:
    with pytest.raises(ArtifactPathError, match="must be absolute"):
        resolve_artifact_path(Path("relative"), ArtifactRelativePath("runs/file.json"))
    with pytest.raises(TypeError, match="must be a Path"):
        resolve_artifact_path(cast(Any, object()), ArtifactRelativePath("runs/file.json"))


def test_resolution_requires_a_validated_relative_path(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="must be validated"):
        resolve_artifact_path(tmp_path, cast(Any, "runs/file.json"))


def test_resolution_requires_an_existing_directory_root(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    with pytest.raises(ArtifactPathError, match="could not be resolved safely"):
        resolve_artifact_path(missing, ArtifactRelativePath("runs/file.json"))

    file_root = tmp_path / "file-root"
    file_root.write_text("not a directory", encoding="utf-8")
    with pytest.raises(ArtifactPathError, match="existing directory"):
        resolve_artifact_path(file_root, ArtifactRelativePath("runs/file.json"))


def test_resolution_rejects_a_linked_descendant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "artifacts"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    linked = root / "linked"
    try:
        linked.symlink_to(outside, target_is_directory=True)
    except OSError:
        original = Path.is_symlink

        def identifies_link(candidate: Path) -> bool:
            return candidate == linked or original(candidate)

        monkeypatch.setattr(
            Path,
            "is_symlink",
            identifies_link,
        )

    with pytest.raises(ArtifactPathError, match="filesystem link"):
        resolve_artifact_path(root, ArtifactRelativePath("linked/escape.json"))


def test_resolution_rejects_a_linked_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    actual = tmp_path / "actual"
    actual.mkdir()
    linked = tmp_path / "linked"
    try:
        linked.symlink_to(actual, target_is_directory=True)
    except OSError:
        original = Path.is_symlink

        def identifies_link(candidate: Path) -> bool:
            return candidate == linked or original(candidate)

        monkeypatch.setattr(
            Path,
            "is_symlink",
            identifies_link,
        )

    with pytest.raises(ArtifactPathError, match="filesystem link"):
        resolve_artifact_path(linked, ArtifactRelativePath("file.json"))


def test_resolution_translates_filesystem_inspection_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = Path.is_symlink

    def fail_on_root(candidate: Path) -> bool:
        if candidate == tmp_path:
            raise OSError("inspection failure")
        return original(candidate)

    monkeypatch.setattr(Path, "is_symlink", fail_on_root)
    with pytest.raises(ArtifactPathError, match="could not be resolved safely") as captured:
        resolve_artifact_path(tmp_path, ArtifactRelativePath("runs/file.json"))
    assert "inspection failure" not in str(captured.value)


def test_resolution_rejects_a_resolver_escape_defensively(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    outside = tmp_path.parent / "outside-artifacts"
    original = Path.resolve

    def escape_candidate(candidate: Path, *, strict: bool = False) -> Path:
        if candidate.name == "file.json":
            return outside / "file.json"
        return original(candidate, strict=strict)

    monkeypatch.setattr(Path, "resolve", escape_candidate)
    with pytest.raises(ArtifactPathError, match="escapes"):
        resolve_artifact_path(tmp_path, ArtifactRelativePath("runs/file.json"))


def test_resolution_translates_a_candidate_resolution_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = Path.resolve

    def fail_candidate(candidate: Path, *, strict: bool = False) -> Path:
        if candidate.name == "file.json":
            raise OSError("candidate failure")
        return original(candidate, strict=strict)

    monkeypatch.setattr(Path, "resolve", fail_candidate)
    with pytest.raises(ArtifactPathError, match="could not be resolved safely"):
        resolve_artifact_path(tmp_path, ArtifactRelativePath("runs/file.json"))


def test_resolution_does_not_create_or_change_filesystem_state(tmp_path: Path) -> None:
    before = tuple(os.scandir(tmp_path))
    resolved = resolve_artifact_path(tmp_path, ArtifactRelativePath("runs/file.json"))
    after = tuple(os.scandir(tmp_path))

    assert resolved == tmp_path / "runs/file.json"
    assert before == after == ()
