"""Artifact listing, confinement, range, and streaming tests."""

from pathlib import Path

import httpx
import pytest

from paritygrid.adapters.artifacts.manifests import FileSystemArtifactManifestRepository
from paritygrid.adapters.artifacts.writer import FileSystemArtifactWriter
from paritygrid.application.ports.artifacts import ArtifactRelativePath
from paritygrid.domain.models import ArtifactId, NodeId, RunId
from paritygrid.domain.pipeline import PartitionKey
from paritygrid.runtime.composition import RuntimeContainer
from tests.api.conftest import seed_scenario

RUN_ID = "run_scenario-01"
NODE_ID = "nod_export-001"
CONTENT = b"paritygrid artifact stream fixture: 0123456789ABCDEF"


def commit_artifact(
    container: RuntimeContainer,
    *,
    artifact_id: str = "art_stream-001",
    content: bytes = CONTENT,
) -> Path:
    artifact_root = container.settings.artifact_root_path
    writer = FileSystemArtifactWriter(artifact_root, maximum_bytes=1_048_576)
    receipt = writer.write(ArtifactRelativePath(f"runs/{RUN_ID}/raw/{artifact_id}.bin"), [content])
    with container.database.transaction() as session:
        FileSystemArtifactManifestRepository(session, artifact_root).register(
            artifact_id=ArtifactId(artifact_id),
            run_id=RunId(RUN_ID),
            node_id=NodeId(NODE_ID),
            partition_key=PartitionKey("part-api"),
            write_receipt=receipt,
            media_type="application/octet-stream",
            schema_version=1,
            row_count=1,
            created_at=container.services.clock(),
        )
    return artifact_root / "runs" / RUN_ID / "raw" / f"{artifact_id}.bin"


@pytest.mark.anyio
async def test_listing_reports_manifest_identities_with_run_coherence(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    await seed_scenario(client)
    commit_artifact(container, artifact_id="art_stream-001")
    commit_artifact(container, artifact_id="art_stream-002", content=b"other bytes")
    response = await client.get(f"/api/v1/runs/{RUN_ID}/artifacts")
    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == RUN_ID
    assert body["run_version"] >= 1
    assert body["observed_at"]
    assert [item["artifact_id"] for item in body["items"]] == [
        "art_stream-001",
        "art_stream-002",
    ]
    first = body["items"][0]
    assert first["byte_size"] == len(CONTENT)
    assert len(first["sha256"]) == 64
    assert "relative_path" not in first


@pytest.mark.anyio
async def test_listing_unknown_run_returns_not_found(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/runs/run_missing-1/artifacts")
    assert response.status_code == 404
    assert response.json()["code"] == "run_not_found"


@pytest.mark.anyio
async def test_download_streams_the_full_committed_artifact(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    await seed_scenario(client)
    commit_artifact(container)
    response = await client.get("/api/v1/artifacts/art_stream-001")
    assert response.status_code == 200
    assert response.content == CONTENT
    assert response.headers["content-length"] == str(len(CONTENT))
    assert response.headers["accept-ranges"] == "bytes"
    assert response.headers["etag"].strip('"') == response.headers["x-checksum-sha256"]
    assert response.headers["content-disposition"] == ('attachment; filename="art_stream-001.bin"')
    assert response.headers["content-type"].startswith("application/octet-stream")


@pytest.mark.anyio
async def test_range_requests_stream_partial_content(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    await seed_scenario(client)
    commit_artifact(container)
    response = await client.get("/api/v1/artifacts/art_stream-001", headers={"Range": "bytes=2-6"})
    assert response.status_code == 206
    assert response.content == CONTENT[2:7]
    assert response.headers["content-range"] == (f"bytes 2-6/{len(CONTENT)}")
    assert response.headers["content-length"] == "5"

    open_ended = await client.get(
        "/api/v1/artifacts/art_stream-001", headers={"Range": "bytes=10-"}
    )
    assert open_ended.status_code == 206
    assert open_ended.content == CONTENT[10:]

    suffix = await client.get("/api/v1/artifacts/art_stream-001", headers={"Range": "bytes=-4"})
    assert suffix.status_code == 206
    assert suffix.content == CONTENT[-4:]


@pytest.mark.anyio
async def test_invalid_and_outside_ranges_are_rejected(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    await seed_scenario(client)
    commit_artifact(container)
    invalid = [
        "bytes=6-2",
        f"bytes=0-{len(CONTENT) * 10}",
        f"bytes={len(CONTENT) * 10}-",
        "bytes=0-1,3-4",
        "chars=0-4",
        "bytes=a-b",
        "bytes=-0",
        "bytes=",
        "nonsense",
    ]
    for header in invalid:
        response = await client.get("/api/v1/artifacts/art_stream-001", headers={"Range": header})
        assert response.status_code == 416, header
        assert response.json()["type"].endswith("/range-not-satisfiable")


@pytest.mark.anyio
async def test_unsafe_artifact_identities_are_confined(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    await seed_scenario(client)
    for artifact_id in (
        "../escape",
        "..%2Fescape",
        "%2e%2e%2fescape",
        "C:\\absolute\\path",
        "/etc/passwd",
        "art_%00",
        "art_ok-id/extra",
    ):
        response = await client.get(f"/api/v1/artifacts/{artifact_id}")
        assert response.status_code in {404, 422}, artifact_id


@pytest.mark.anyio
async def test_missing_artifact_returns_not_found(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/artifacts/art_missing-1")
    assert response.status_code == 404
    assert response.json()["code"] == "artifact_not_found"


@pytest.mark.anyio
async def test_deleted_artifact_file_reports_integrity_gone(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    await seed_scenario(client)
    target = commit_artifact(container)
    target.unlink()
    response = await client.get("/api/v1/artifacts/art_stream-001")
    assert response.status_code in {404, 410}
    assert response.json()["code"] in {"artifact_not_found", "artifact_integrity"}


@pytest.mark.anyio
async def test_tampered_artifact_file_reports_integrity_gone(
    container: RuntimeContainer, client: httpx.AsyncClient
) -> None:
    await seed_scenario(client)
    target = commit_artifact(container)
    target.write_bytes(b"tampered contents replace the committed bytes")
    response = await client.get("/api/v1/artifacts/art_stream-001")
    assert response.status_code == 410
    assert response.json()["code"] == "artifact_integrity"
