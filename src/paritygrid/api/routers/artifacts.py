"""Artifact listing and confined streaming download routes."""

from collections.abc import Iterator
from typing import Annotated

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse
from starlette.responses import Response as StarletteResponse

from paritygrid.api.dependencies import ApiServices, get_services
from paritygrid.api.errors.problems import ProblemError
from paritygrid.api.schemas.artifacts import ArtifactPageResponse, ArtifactResponse
from paritygrid.application.ports.artifact_streaming import (
    ArtifactByteRange,
    ArtifactByteStream,
    ArtifactStreamMetadata,
)
from paritygrid.application.ports.artifacts import ArtifactManifestRecord
from paritygrid.application.services.artifacts import RunArtifactPage

router = APIRouter(prefix="/api/v1", tags=["artifacts"])

_MEDIA_TYPE_EXTENSIONS: tuple[tuple[str, str], ...] = (
    ("application/x-parquet", ".parquet"),
    ("application/vnd.apache.parquet", ".parquet"),
    ("application/jsonl", ".jsonl"),
    ("application/x-ndjson", ".jsonl"),
    ("application/json", ".json"),
    ("text/csv", ".csv"),
    ("text/plain", ".txt"),
)


@router.get("/runs/{run_id}/artifacts", response_model=ArtifactPageResponse)
def list_run_artifacts(
    run_id: str,
    request: Request,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    cursor: str | None = None,
) -> ArtifactPageResponse:
    page: RunArtifactPage = get_services(request).artifacts.list_for_run(
        run_id, limit=limit, after=cursor
    )
    return ArtifactPageResponse(
        run_id=page.run.run_id.value,
        run_version=page.run.row_version,
        observed_at=str(page.observed_at),
        items=[_artifact_response(record) for record in page.manifests.items],
        limit=limit,
        next_cursor=(
            None if page.manifests.next_cursor is None else page.manifests.next_cursor.value
        ),
    )


@router.get("/artifacts/{artifact_id}")
def download_artifact(artifact_id: str, request: Request) -> StarletteResponse:
    services = get_services(request)
    supplied_range = request.headers.get("range")
    byte_range = (
        None if supplied_range is None else _resolve_range(services, artifact_id, supplied_range)
    )
    metadata, stream = services.artifacts.open(artifact_id, byte_range=byte_range)
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(metadata.content_length),
        "ETag": f'"{metadata.content_sha256}"',
        "X-Checksum-SHA256": metadata.content_sha256,
        "Content-Disposition": f'attachment; filename="{_safe_filename(metadata)}"',
    }
    if metadata.is_partial:
        headers["Content-Range"] = (
            f"bytes {metadata.range_start}-{metadata.range_end_exclusive - 1}/"
            f"{metadata.total_byte_size}"
        )
    return StreamingResponse(
        bounded_stream_chunks(stream),
        status_code=206 if metadata.is_partial else 200,
        media_type=metadata.media_type,
        headers=headers,
    )


def bounded_stream_chunks(stream: ArtifactByteStream) -> Iterator[bytes]:
    """Iterate one verified stream, closing it on exhaustion or disconnect."""
    try:
        yield from stream
    finally:
        # GeneratorExit (a slow or disconnected client cancels iteration)
        # must release the descriptor so no application resource is held.
        stream.close()


def _resolve_range(services: ApiServices, artifact_id: str, supplied: str) -> ArtifactByteRange:
    specification = supplied.strip()
    if not specification.startswith("bytes=") or "," in specification:
        raise _range_problem()
    bounds = specification[len("bytes=") :]
    if bounds.count("-") != 1:
        raise _range_problem()
    start_text, end_text = (part.strip() for part in bounds.split("-", maxsplit=1))
    if start_text == "":
        return _suffix_range(services, artifact_id, end_text)
    if not _is_decimal(start_text):
        raise _range_problem()
    start = int(start_text)
    if end_text == "":
        manifest = services.artifacts.manifest(artifact_id)
        if start >= manifest.byte_size:
            raise _range_problem()
        return ArtifactByteRange(start, manifest.byte_size)
    if not _is_decimal(end_text):
        raise _range_problem()
    end = int(end_text)
    if start > end:
        raise _range_problem()
    return ArtifactByteRange(start, end + 1)


def _suffix_range(services: ApiServices, artifact_id: str, suffix_text: str) -> ArtifactByteRange:
    if not _is_decimal(suffix_text) or int(suffix_text) <= 0:
        raise _range_problem()
    manifest = services.artifacts.manifest(artifact_id)
    if manifest.byte_size == 0:
        raise _range_problem()
    length = min(int(suffix_text), manifest.byte_size)
    return ArtifactByteRange(manifest.byte_size - length, manifest.byte_size)


def _is_decimal(value: str) -> bool:
    return value.isdigit() and len(value) <= 19 and int(value) <= 2_147_483_647


def _range_problem() -> ProblemError:
    return ProblemError(
        type_slug="range-not-satisfiable",
        title="Requested range is not satisfiable",
        status=416,
        detail="the requested byte range lies outside the committed artifact",
    )


def _safe_filename(metadata: ArtifactStreamMetadata) -> str:
    extension = ".bin"
    for media_type, candidate in _MEDIA_TYPE_EXTENSIONS:
        if metadata.media_type == media_type:
            extension = candidate
            break
    return f"{metadata.artifact_id.value}{extension}"


def _artifact_response(record: ArtifactManifestRecord) -> ArtifactResponse:
    return ArtifactResponse(
        artifact_id=record.artifact_id.value,
        run_id=record.run_id.value,
        node_id=record.node_id.value,
        partition_key=str(record.partition_key),
        media_type=record.media_type,
        artifact_schema_version=record.schema_version,
        byte_size=record.byte_size,
        row_count=record.row_count,
        sha256=record.sha256,
        created_at=str(record.created_at),
    )
