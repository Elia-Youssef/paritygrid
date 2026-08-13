"""Read-only artifact orphan and missing-file integrity tests."""

# pyright: reportPrivateUsage=false

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from paritygrid.adapters.artifacts import (
    FileSystemArtifactIntegrityScanner,
    FileSystemArtifactManifestRepository,
    FileSystemArtifactWriter,
)
from paritygrid.adapters.persistence import SQLiteDatabase, SQLiteDatabaseConfig
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyPipelineRepository,
    SqlAlchemyRunRepository,
)
from paritygrid.application.ports import (
    ArtifactIntegrityIssueKind,
    ArtifactIntegrityScanInvalidError,
    ArtifactIntegrityScanReport,
    ArtifactRelativePath,
    ArtifactWriteReceipt,
    ConfigurationDocument,
)
from paritygrid.domain.models import (
    ArtifactId,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
)
from paritygrid.domain.pipeline import PartitionKey

RUN_ID = RunId("run_integrity")
NODE_ID = NodeId("nod_integrity")


@pytest.fixture
def database(tmp_path: Path) -> Iterator[SQLiteDatabase]:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "integrity state.db"))
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    try:
        yield database
    finally:
        database.close()


@pytest.fixture
def artifact_root(tmp_path: Path) -> Path:
    root = tmp_path / "artifacts Café % ميناء"
    root.mkdir()
    return root


def _timestamp(second: int) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 13, 12, 0, second, tzinfo=UTC))


def _seed_run(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PipelineId("pip_integrity"),
            display_name="Integrity pipeline",
            description=None,
            created_at=_timestamp(0),
        )
        pipelines.publish_version(
            pipeline_id=PipelineId("pip_integrity"),
            expected_latest_version=None,
            specification=ConfigurationDocument.from_mapping({"nodes": []}),
            planner_format_version=1,
            published_at=_timestamp(0),
        )
        SqlAlchemyRunRepository(session).create(
            run_id=RUN_ID,
            pipeline_id=PipelineId("pip_integrity"),
            pipeline_version=PipelineVersion(1),
            runner_kind="threaded",
            runner_configuration=ConfigurationDocument.from_mapping({}),
            scenario_seed=None,
            node_ids=(NODE_ID,),
            created_at=_timestamp(1),
        )


def _write(root: Path, relative_path: str, content: bytes) -> ArtifactWriteReceipt:
    return FileSystemArtifactWriter(root, maximum_bytes=1_024).write(
        ArtifactRelativePath(relative_path), (content,)
    )


def _register(
    database: SQLiteDatabase,
    root: Path,
    *,
    artifact_id: str,
    relative_path: str,
    content: bytes,
) -> Path:
    receipt = _write(root, relative_path, content)
    with database.transaction() as session:
        FileSystemArtifactManifestRepository(session, root).register(
            artifact_id=ArtifactId(artifact_id),
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PartitionKey("all"),
            write_receipt=receipt,
            media_type="application/octet-stream",
            schema_version=1,
            row_count=0,
            created_at=_timestamp(2),
        )
    return root.joinpath(*ArtifactRelativePath(relative_path).parts)


def _scan(database: SQLiteDatabase, root: Path) -> ArtifactIntegrityScanReport:
    with database.transaction() as session:
        return FileSystemArtifactIntegrityScanner(session, root).scan()


def test_clean_scan_is_repeatable_and_read_only(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    _seed_run(database)
    path = _register(
        database,
        artifact_root,
        artifact_id="art_integrity",
        relative_path="runs/verified.bin",
        content=b"verified",
    )
    before = path.read_bytes()

    first = _scan(database, artifact_root)
    second = _scan(database, artifact_root)

    assert first.is_clean
    assert first == second
    assert (first.manifest_count, first.observed_file_count, first.verified_manifest_count) == (
        1,
        1,
        1,
    )
    assert path.read_bytes() == before


def test_missing_file_and_file_without_manifest_are_both_reported(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    _seed_run(database)
    missing = _register(
        database,
        artifact_root,
        artifact_id="art_missing",
        relative_path="runs/missing.bin",
        content=b"missing",
    )
    missing.unlink()
    orphan = artifact_root / "runs" / "orphan.bin"
    orphan.write_bytes(b"orphan")

    report = _scan(database, artifact_root)

    assert tuple(issue.kind for issue in report.issues) == (
        ArtifactIntegrityIssueKind.MISSING_FILE,
        ArtifactIntegrityIssueKind.ORPHAN_FILE,
    )
    assert tuple(
        None if issue.relative_path is None else str(issue.relative_path) for issue in report.issues
    ) == ("runs/missing.bin", "runs/orphan.bin")
    assert orphan.read_bytes() == b"orphan"


def test_changed_file_and_unsafe_entry_are_reported_without_raw_unsafe_name(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    _seed_run(database)
    path = _register(
        database,
        artifact_root,
        artifact_id="art_changed",
        relative_path="runs/changed.bin",
        content=b"original",
    )
    path.write_bytes(b"changed!")
    unsafe = artifact_root / ".private-value"
    unsafe.write_bytes(b"preserve")

    report = _scan(database, artifact_root)

    assert tuple(issue.kind for issue in report.issues) == (
        ArtifactIntegrityIssueKind.INVALID_FILE,
        ArtifactIntegrityIssueKind.UNSAFE_ENTRY,
    )
    unsafe_issue = report.issues[1]
    assert unsafe_issue.relative_path is None
    assert unsafe_issue.observed_path_sha256 == hashlib.sha256(b".private-value").hexdigest()
    assert "private-value" not in repr(unsafe_issue)
    assert unsafe.read_bytes() == b"preserve"


def test_scanner_requires_real_session_and_caller_transaction(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    with pytest.raises(TypeError, match="Session"):
        FileSystemArtifactIntegrityScanner(object(), artifact_root)  # type: ignore[arg-type]
    session = Session(bind=database.engine)
    try:
        scanner = FileSystemArtifactIntegrityScanner(session, artifact_root)
        with pytest.raises(ArtifactIntegrityScanInvalidError, match="transaction"):
            scanner.scan()
    finally:
        session.close()
