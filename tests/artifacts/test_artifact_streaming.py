"""File-backed artifact streaming, range, and confinement tests."""

# pyright: reportPrivateUsage=false

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from paritygrid.adapters.artifacts import (
    FileSystemArtifactManifestRepository,
    FileSystemArtifactStreamReader,
    FileSystemArtifactWriter,
)
from paritygrid.adapters.persistence import (
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    create_session_factory,
)
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyPipelineRepository,
    SqlAlchemyRunRepository,
)
from paritygrid.application.ports import (
    ArtifactByteRange,
    ArtifactRelativePath,
    ArtifactStreamIntegrityError,
    ArtifactStreamInvalidError,
    ArtifactStreamNotFoundError,
    ArtifactStreamRangeError,
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

RUN_ID = RunId("run_streaming")
NODE_ID = NodeId("nod_streaming")
ARTIFACT_ID = ArtifactId("art_streaming")


@pytest.fixture
def database(tmp_path: Path) -> Iterator[SQLiteDatabase]:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "stream state.db"))
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


def _seed(database: SQLiteDatabase, root: Path, content: bytes = b"0123456789") -> Path:
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PipelineId("pip_streaming"),
            display_name="Streaming pipeline",
            description=None,
            created_at=_timestamp(0),
        )
        pipelines.publish_version(
            pipeline_id=PipelineId("pip_streaming"),
            expected_latest_version=None,
            specification=ConfigurationDocument.from_mapping({"nodes": []}),
            planner_format_version=1,
            published_at=_timestamp(0),
        )
        SqlAlchemyRunRepository(session).create(
            run_id=RUN_ID,
            pipeline_id=PipelineId("pip_streaming"),
            pipeline_version=PipelineVersion(1),
            runner_kind="threaded",
            runner_configuration=ConfigurationDocument.from_mapping({}),
            scenario_seed=None,
            node_ids=(NODE_ID,),
            created_at=_timestamp(1),
        )
    relative = ArtifactRelativePath("runs/output.bin")
    receipt = FileSystemArtifactWriter(root, maximum_bytes=1_024).write(relative, (content,))
    with database.transaction() as session:
        FileSystemArtifactManifestRepository(session, root).register(
            artifact_id=ARTIFACT_ID,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PartitionKey("all"),
            write_receipt=receipt,
            media_type="application/octet-stream",
            schema_version=1,
            row_count=0,
            created_at=_timestamp(2),
        )
    return root / "runs" / "output.bin"


def test_full_stream_outlives_short_transaction_and_auto_closes(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    _seed(database, artifact_root)
    with database.transaction() as session:
        stream = FileSystemArtifactStreamReader(session, artifact_root, chunk_size=3).open(
            ARTIFACT_ID
        )
        assert stream.metadata.total_byte_size == 10
        assert not stream.metadata.is_partial

    assert tuple(stream) == (b"012", b"345", b"678", b"9")
    assert tuple(stream) == ()
    assert "stream state.db" not in repr(stream)
    stream.close()


def test_partial_range_returns_only_selected_bytes(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    _seed(database, artifact_root)
    with database.transaction() as session:
        stream = FileSystemArtifactStreamReader(session, artifact_root, chunk_size=2).open(
            ARTIFACT_ID, byte_range=ArtifactByteRange(2, 7)
        )
    with stream:
        assert b"".join(stream) == b"23456"
        assert stream.metadata.content_length == 5
        assert stream.metadata.is_partial


def test_empty_artifact_stream_is_valid(database: SQLiteDatabase, artifact_root: Path) -> None:
    _seed(database, artifact_root, b"")
    with database.transaction() as session:
        stream = FileSystemArtifactStreamReader(session, artifact_root, chunk_size=1).open(
            ARTIFACT_ID
        )
    assert stream.metadata.content_length == 0
    assert tuple(stream) == ()


def test_missing_identity_and_unsatisfiable_ranges_are_typed(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    _seed(database, artifact_root)
    with database.transaction() as session:
        reader = FileSystemArtifactStreamReader(session, artifact_root, chunk_size=4)
        with pytest.raises(ArtifactStreamNotFoundError):
            reader.open(ArtifactId("art_missing"))
        with pytest.raises(ArtifactStreamRangeError):
            reader.open(ARTIFACT_ID, byte_range=ArtifactByteRange(9, 11))
        with pytest.raises(ArtifactStreamInvalidError, match="identity"):
            reader.open("art_streaming")  # type: ignore[arg-type]
        with pytest.raises(ArtifactStreamInvalidError, match="byte range"):
            reader.open(ARTIFACT_ID, byte_range=object())  # type: ignore[arg-type]


def test_missing_or_changed_immutable_file_fails_before_streaming(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    path = _seed(database, artifact_root)
    path.write_bytes(b"abcdefghij")
    with database.transaction() as session:
        reader = FileSystemArtifactStreamReader(session, artifact_root, chunk_size=4)
        with pytest.raises(ArtifactStreamIntegrityError, match="differs"):
            reader.open(ARTIFACT_ID)
    path.unlink()
    with (
        database.transaction() as session,
        pytest.raises(ArtifactStreamIntegrityError, match="confined"),
    ):
        FileSystemArtifactStreamReader(session, artifact_root, chunk_size=4).open(ARTIFACT_ID)


def test_partial_close_detects_post_open_path_change(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    path = _seed(database, artifact_root)
    with database.transaction() as session:
        stream = FileSystemArtifactStreamReader(session, artifact_root, chunk_size=4).open(
            ARTIFACT_ID
        )
    assert next(stream) == b"0123"
    path.write_bytes(b"abcdefghij")
    with pytest.raises(ArtifactStreamIntegrityError, match="identity changed"):
        stream.close()


def test_constructor_and_transaction_contract_are_strict(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    with pytest.raises(TypeError, match="Session"):
        FileSystemArtifactStreamReader(object(), artifact_root, chunk_size=1)  # type: ignore[arg-type]
    with create_session_factory(database.engine)() as session:
        reader = FileSystemArtifactStreamReader(session, artifact_root, chunk_size=1)
        with pytest.raises(ArtifactStreamInvalidError, match="transaction"):
            reader.open(ARTIFACT_ID)
    with (
        database.transaction() as session,
        pytest.raises(ArtifactStreamInvalidError, match="chunk size"),
    ):
        FileSystemArtifactStreamReader(session, artifact_root, chunk_size=0)
