"""Dependency-neutral artifact streaming contract tests."""

from dataclasses import replace

import pytest

from paritygrid.application.ports import (
    ArtifactByteRange,
    ArtifactRelativePath,
    ArtifactStreamInvalidError,
    ArtifactStreamMetadata,
)
from paritygrid.domain.models import ArtifactId


def _metadata() -> ArtifactStreamMetadata:
    return ArtifactStreamMetadata(
        ArtifactId("art_stream"),
        ArtifactRelativePath("runs/output.bin"),
        "application/octet-stream",
        1,
        10,
        0,
        10,
        "0" * 64,
    )


def test_half_open_range_is_exact_and_nonempty() -> None:
    selected = ArtifactByteRange(2, 7)
    assert selected.length == 5
    for values in ((-1, 1), (1, 1), (2, 1), (0, 1_099_511_627_777)):
        with pytest.raises(ArtifactStreamInvalidError, match="range"):
            ArtifactByteRange(*values)
    with pytest.raises(TypeError, match="integers"):
        ArtifactByteRange(True, 1)


def test_metadata_supports_full_partial_and_empty_artifacts() -> None:
    full = _metadata()
    partial = replace(full, range_start=2, range_end_exclusive=7)
    empty = replace(full, total_byte_size=0, range_end_exclusive=0)

    assert full.content_length == 10
    assert not full.is_partial
    assert partial.content_length == 5
    assert partial.is_partial
    assert empty.content_length == 0
    assert not empty.is_partial


@pytest.mark.parametrize(
    ("change", "error"),
    [
        ({"artifact_id": object()}, TypeError),
        ({"relative_path": object()}, TypeError),
        ({"media_type": "Application/JSON"}, TypeError),
        ({"schema_version": 0}, ValueError),
        ({"total_byte_size": True}, TypeError),
        ({"range_end_exclusive": 11}, ArtifactStreamInvalidError),
        ({"content_sha256": "BAD"}, TypeError),
    ],
)
def test_metadata_rejects_noncanonical_or_inconsistent_values(
    change: dict[str, object], error: type[BaseException]
) -> None:
    with pytest.raises(error):
        replace(_metadata(), **change)
