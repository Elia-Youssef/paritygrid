"""Public contract tests for immutable artifact manifests."""

import hashlib
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from paritygrid.application.ports import (
    MAX_ARTIFACT_PAGE_SIZE,
    MAX_ARTIFACT_ROW_COUNT,
    MAX_ARTIFACT_SCHEMA_VERSION,
    MAX_ARTIFACT_WRITE_BYTES,
    ArtifactManifestInvalidError,
    ArtifactManifestPage,
    ArtifactManifestRecord,
    ArtifactRelativePath,
    validate_artifact_media_type,
    validate_artifact_page_limit,
)
from paritygrid.domain.models import ArtifactId, NodeId, RunId, UtcTimestamp
from paritygrid.domain.pipeline import PartitionKey


def _timestamp() -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 13, 12, 0, tzinfo=UTC))


def _record(**changes: object) -> ArtifactManifestRecord:
    values: dict[str, object] = {
        "artifact_id": ArtifactId("art_contract"),
        "run_id": RunId("run_contract"),
        "node_id": NodeId("nod_contract"),
        "partition_key": PartitionKey("page-0001"),
        "relative_path": ArtifactRelativePath("runs/run-contract/raw/file.json"),
        "media_type": "application/json",
        "schema_version": 1,
        "byte_size": 2,
        "row_count": 1,
        "sha256": hashlib.sha256(b"{}").hexdigest(),
        "created_at": _timestamp(),
    }
    values.update(changes)
    return ArtifactManifestRecord(**cast(Any, values))


@pytest.mark.parametrize(
    "value",
    [
        "application/json",
        "application/vnd.apache.parquet",
        "text/plain",
        "application/x.paritygrid+json",
    ],
)
def test_canonical_media_types_are_accepted(value: str) -> None:
    assert validate_artifact_media_type(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "Application/JSON",
        "application/json; charset=utf-8",
        "application",
        "/json",
        "application/",
        "application/é",
        "a/" + "b" * 126,
        b"application/json",
    ],
)
def test_noncanonical_media_types_are_rejected(value: object) -> None:
    with pytest.raises(TypeError if isinstance(value, bytes) else ValueError):
        validate_artifact_media_type(value)


@pytest.mark.parametrize("value", [1, MAX_ARTIFACT_PAGE_SIZE])
def test_artifact_page_limit_accepts_boundaries(value: int) -> None:
    assert validate_artifact_page_limit(value) == value


@pytest.mark.parametrize("value", [True, 0, MAX_ARTIFACT_PAGE_SIZE + 1, "1"])
def test_artifact_page_limit_rejects_invalid_values(value: object) -> None:
    with pytest.raises(ArtifactManifestInvalidError):
        validate_artifact_page_limit(value)


@pytest.mark.parametrize(
    ("changes", "error"),
    [
        ({"artifact_id": "art_contract"}, TypeError),
        ({"run_id": "run_contract"}, TypeError),
        ({"node_id": "nod_contract"}, TypeError),
        ({"partition_key": "page-0001"}, TypeError),
        ({"relative_path": "runs/file.json"}, TypeError),
        ({"created_at": "2026-08-13T12:00:00.000000Z"}, TypeError),
        ({"schema_version": True}, TypeError),
        ({"schema_version": 0}, ValueError),
        ({"schema_version": MAX_ARTIFACT_SCHEMA_VERSION + 1}, ValueError),
        ({"byte_size": True}, TypeError),
        ({"byte_size": -1}, ValueError),
        ({"byte_size": MAX_ARTIFACT_WRITE_BYTES + 1}, ValueError),
        ({"row_count": True}, TypeError),
        ({"row_count": -1}, ValueError),
        ({"row_count": MAX_ARTIFACT_ROW_COUNT + 1}, ValueError),
        ({"sha256": b"0" * 64}, TypeError),
        ({"sha256": "A" * 64}, ValueError),
    ],
)
def test_manifest_record_rejects_invalid_state(
    changes: dict[str, object], error: type[Exception]
) -> None:
    with pytest.raises(error):
        _record(**changes)


def test_manifest_record_and_page_are_immutable_and_bounded() -> None:
    first = _record()
    second = _record(artifact_id=ArtifactId("art_contract-two"))
    page = ArtifactManifestPage((first, second), second.artifact_id)

    assert page.items == (first, second)
    assert page.next_cursor == second.artifact_id
    with pytest.raises(TypeError, match="immutable record tuple"):
        ArtifactManifestPage(cast(Any, [first]), None)
    with pytest.raises(TypeError, match="immutable record tuple"):
        ArtifactManifestPage(cast(Any, (object(),)), None)
    with pytest.raises(TypeError, match="cursor"):
        ArtifactManifestPage((first,), cast(Any, "art_contract"))
