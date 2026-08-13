"""Contract tests for portable artifact relative paths."""

import ast
import unicodedata
from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.application.ports import (
    MAX_ARTIFACT_PATH_SEGMENT_BYTES,
    MAX_ARTIFACT_PATH_SEGMENTS,
    MAX_ARTIFACT_RELATIVE_PATH_BYTES,
    ArtifactPathError,
    ArtifactRelativePath,
)


@pytest.mark.parametrize(
    "value",
    [
        "runs/run-example/raw/part-00001.parquet",
        "runs/run-example/reconciliation/Café-ميناء.json",
        "runs/run-example/target/part 1.json",
        "%/literal-percent.bin",
    ],
)
def test_relative_path_round_trips_canonical_utf8(value: str) -> None:
    path = ArtifactRelativePath(value)

    assert str(path) == value
    assert path.parts == tuple(value.split("/"))
    assert bytes(path) == value.encode("utf-8")
    assert path.to_bytes() == value.encode("utf-8")
    assert ArtifactRelativePath.parse(value) == path
    assert ArtifactRelativePath.from_bytes(bytes(path)) == path


@pytest.mark.parametrize(
    "value",
    [
        "",
        "/absolute",
        "trailing/",
        "a//b",
        ".",
        "..",
        "a/./b",
        "a/../b",
        r"C:\escape",
        r"\\server\share",
        "C:/escape",
        "file:stream",
        "name:stream",
        "a/<bad>",
        'a/"bad"',
        "a/b|c",
        "a/b?c",
        "a/b*c",
        "a/.hidden",
        "a/trailing.",
        "a/trailing ",
        "a/ leading",
        "a/NUL",
        "a/con.txt",
        "a/COM1.parquet",
        "a/lpt³.json",
        "a/CONIN$",
        "a/conout$",
        "a/line\nbreak",
        "a/zero\x00byte",
        unicodedata.normalize("NFD", "a/Café.json"),
    ],
)
def test_unsafe_or_noncanonical_paths_are_rejected(value: str) -> None:
    with pytest.raises(ArtifactPathError):
        ArtifactRelativePath(value)


def test_path_contract_enforces_exact_types_and_utf8() -> None:
    with pytest.raises(TypeError, match="must be text"):
        ArtifactRelativePath(cast(Any, Path("runs/file")))
    with pytest.raises(TypeError, match="must be bytes"):
        ArtifactRelativePath.from_bytes(cast(Any, "runs/file"))
    with pytest.raises(ArtifactPathError, match="valid UTF-8"):
        ArtifactRelativePath.from_bytes(b"\xff")
    with pytest.raises(ArtifactPathError, match="valid Unicode"):
        ArtifactRelativePath("runs/\ud800")


def test_path_contract_enforces_published_byte_and_segment_limits() -> None:
    exact_segment = "a" * MAX_ARTIFACT_PATH_SEGMENT_BYTES
    assert ArtifactRelativePath(exact_segment).value == exact_segment
    with pytest.raises(ArtifactPathError, match="segment exceeds"):
        ArtifactRelativePath("a" * (MAX_ARTIFACT_PATH_SEGMENT_BYTES + 1))

    exact_segments = "/".join("a" for _ in range(MAX_ARTIFACT_PATH_SEGMENTS))
    assert len(ArtifactRelativePath(exact_segments).parts) == MAX_ARTIFACT_PATH_SEGMENTS
    with pytest.raises(ArtifactPathError, match="too many"):
        ArtifactRelativePath(f"{exact_segments}/a")

    exact_total = f"{'a' * 255}/{'b' * 255}/{'c' * 255}/{'d' * 254}/e"
    assert len(exact_total.encode("utf-8")) == MAX_ARTIFACT_RELATIVE_PATH_BYTES
    assert ArtifactRelativePath(exact_total).value == exact_total
    with pytest.raises(ArtifactPathError, match="byte limit"):
        ArtifactRelativePath(f"{'a' * 255}/{'b' * 255}/{'c' * 255}/{'d' * 255}/e")


def test_artifact_path_port_has_no_filesystem_or_adapter_dependency() -> None:
    source = Path("src/paritygrid/application/ports/artifacts.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}

    assert "pathlib" not in imported
    assert not any(name.startswith("paritygrid.adapters") for name in imported)
