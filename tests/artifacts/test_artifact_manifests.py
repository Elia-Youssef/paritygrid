"""Filesystem and SQLite consistency tests for artifact manifests."""

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy import event, text

from paritygrid.adapters.artifacts import (
    FileSystemArtifactManifestRepository,
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
    ArtifactIntegrityError,
    ArtifactManifestConflictError,
    ArtifactManifestCorruptionError,
    ArtifactManifestInvalidError,
    ArtifactRelativePath,
    ArtifactWriteReceipt,
    ConfigurationDocument,
)
from paritygrid.application.ports.artifacts import ArtifactManifestRecord
from paritygrid.domain.models import (
    ArtifactId,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
)
from paritygrid.domain.pipeline import PartitionKey

PIPELINE_ID = PipelineId("pip_artifacts")
RUN_ID = RunId("run_artifacts")
NODE_ID = NodeId("nod_source")
PARTITION = PartitionKey("page-0001")
ARTIFACT_ID = ArtifactId("art_page-one")


@pytest.fixture
def database(tmp_path: Path) -> Iterator[SQLiteDatabase]:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "artifact state.db"))
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


def _document(**values: object) -> ConfigurationDocument:
    return ConfigurationDocument.from_mapping(values)


def _seed_run(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PIPELINE_ID,
            display_name="Artifact pipeline",
            description=None,
            created_at=_timestamp(0),
        )
        pipelines.publish_version(
            pipeline_id=PIPELINE_ID,
            expected_latest_version=None,
            specification=_document(nodes=[]),
            planner_format_version=1,
            published_at=_timestamp(0),
        )
        SqlAlchemyRunRepository(session).create(
            run_id=RUN_ID,
            pipeline_id=PIPELINE_ID,
            pipeline_version=PipelineVersion(1),
            runner_kind="threaded",
            runner_configuration=_document(max_workers=1),
            scenario_seed=None,
            node_ids=(NODE_ID,),
            created_at=_timestamp(1),
        )


def _write(
    root: Path,
    relative: str = "runs/run-artifacts/raw/page-0001.json",
    content: bytes = b'{"records":[]}',
) -> ArtifactWriteReceipt:
    return FileSystemArtifactWriter(root, maximum_bytes=1_024).write(
        ArtifactRelativePath(relative),
        (content,),
    )


def _register(
    database: SQLiteDatabase,
    root: Path,
    receipt: ArtifactWriteReceipt,
    *,
    artifact_id: ArtifactId = ARTIFACT_ID,
    created_at: UtcTimestamp | None = None,
    row_count: int = 0,
) -> ArtifactManifestRecord:
    with database.transaction() as session:
        return FileSystemArtifactManifestRepository(session, root).register(
            artifact_id=artifact_id,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PARTITION,
            write_receipt=receipt,
            media_type="application/json",
            schema_version=1,
            row_count=row_count,
            created_at=created_at or _timestamp(2),
        )


def test_register_get_exact_replay_and_list_are_file_verified(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    _seed_run(database)
    receipt = _write(artifact_root)
    created = _register(database, artifact_root, receipt, row_count=0)

    assert created.artifact_id == ArtifactId("art_page-one")
    assert created.byte_size == receipt.byte_size
    assert created.sha256 == receipt.sha256
    replay = _register(database, artifact_root, receipt)
    assert replay == created

    with database.transaction() as session:
        repository = FileSystemArtifactManifestRepository(session, artifact_root)
        assert repository.get(ArtifactId("art_page-one")) == created
        assert repository.get(ArtifactId("art_missing")) is None
        page = repository.list_for_run(RUN_ID, limit=10)
        assert page.items == (created,)
        assert page.next_cursor is None


def test_manifest_registration_rollback_leaves_an_explicit_orphan(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    _seed_run(database)
    receipt = _write(artifact_root)

    def register_then_fail() -> None:
        with database.transaction() as session:
            FileSystemArtifactManifestRepository(session, artifact_root).register(
                artifact_id=ARTIFACT_ID,
                run_id=RUN_ID,
                node_id=NODE_ID,
                partition_key=PARTITION,
                write_receipt=receipt,
                media_type="application/json",
                schema_version=1,
                row_count=0,
                created_at=_timestamp(2),
            )
            raise RuntimeError("rollback")

    with pytest.raises(RuntimeError, match="rollback"):
        register_then_fail()

    assert (artifact_root / str(receipt.relative_path)).is_file()
    with database.transaction() as session:
        repository = FileSystemArtifactManifestRepository(session, artifact_root)
        assert repository.get(ArtifactId("art_page-one")) is None


def test_divergent_identity_and_path_replays_are_conflicts(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    _seed_run(database)
    first = _write(artifact_root)
    _register(database, artifact_root, first)
    second = _write(
        artifact_root,
        "runs/run-artifacts/raw/page-0002.json",
        b'{"records":[1]}',
    )

    with pytest.raises(ArtifactManifestConflictError):
        _register(database, artifact_root, second)
    with pytest.raises(ArtifactManifestConflictError):
        _register(database, artifact_root, first, artifact_id=ArtifactId("art_page-two"))


def test_missing_parent_and_precreation_timestamp_fail_before_insert(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    receipt = _write(artifact_root)
    with pytest.raises(ArtifactManifestInvalidError, match="parent"):
        _register(database, artifact_root, receipt)

    _seed_run(database)
    with pytest.raises(ArtifactManifestInvalidError, match="precedes"):
        _register(database, artifact_root, receipt, created_at=_timestamp(0))


def test_missing_or_tampered_file_fails_manifest_reads(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    _seed_run(database)
    receipt = _write(artifact_root)
    _register(database, artifact_root, receipt)
    path = artifact_root / str(receipt.relative_path)
    path.write_bytes(b"tampered")
    with (
        database.transaction() as session,
        pytest.raises(ArtifactIntegrityError, match="does not match"),
    ):
        FileSystemArtifactManifestRepository(session, artifact_root).get(ARTIFACT_ID)

    path.unlink()
    with (
        database.transaction() as session,
        pytest.raises(ArtifactIntegrityError, match="missing"),
    ):
        FileSystemArtifactManifestRepository(session, artifact_root).get(ARTIFACT_ID)


def test_manifest_pagination_is_stable_and_exclusive(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    _seed_run(database)
    identifiers = (
        ArtifactId("art_page-a"),
        ArtifactId("art_page-b"),
        ArtifactId("art_page-c"),
    )
    for index, artifact_id in enumerate(identifiers):
        receipt = _write(
            artifact_root,
            f"runs/run-artifacts/raw/page-000{index}.json",
            f'{{"page":{index}}}'.encode(),
        )
        _register(database, artifact_root, receipt, artifact_id=artifact_id)

    with database.transaction() as session:
        repository = FileSystemArtifactManifestRepository(session, artifact_root)
        first = repository.list_for_run(RUN_ID, limit=2)
        second = repository.list_for_run(RUN_ID, limit=2, after=first.next_cursor)
    assert tuple(item.artifact_id for item in first.items) == identifiers[:2]
    assert first.next_cursor == identifiers[1]
    assert tuple(item.artifact_id for item in second.items) == identifiers[2:]
    assert second.next_cursor is None


def test_manifest_page_parent_validation_uses_one_bounded_query(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    _seed_run(database)
    for index in range(3):
        receipt = _write(
            artifact_root,
            f"runs/run-artifacts/raw/bounded-{index}.json",
            f'{{"page":{index}}}'.encode(),
        )
        _register(
            database,
            artifact_root,
            receipt,
            artifact_id=ArtifactId(f"art_bounded-{index}"),
        )

    statements = 0

    def count_statement(*_args: object) -> None:
        nonlocal statements
        statements += 1

    event.listen(database.engine, "before_cursor_execute", count_statement)
    try:
        with database.transaction() as session:
            page = FileSystemArtifactManifestRepository(session, artifact_root).list_for_run(
                RUN_ID, limit=3
            )
    finally:
        event.remove(database.engine, "before_cursor_execute", count_statement)

    assert len(page.items) == 3
    assert statements == 2


def test_raw_noncanonical_manifest_row_is_reported_as_corruption(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    _seed_run(database)
    receipt = _write(artifact_root)
    _register(database, artifact_root, receipt)
    with database.transaction() as session:
        session.execute(text("DROP TRIGGER trg_artifact_manifests_prohibit_update"))
        session.execute(
            text(
                "UPDATE artifact_manifests SET media_type='Application/JSON' "
                "WHERE artifact_id='art_page-one'"
            )
        )
    with (
        database.transaction() as session,
        pytest.raises(ArtifactManifestCorruptionError, match="row is corrupt"),
    ):
        FileSystemArtifactManifestRepository(session, artifact_root).get(ARTIFACT_ID)


def test_repository_requires_a_caller_transaction_and_exact_inputs(
    database: SQLiteDatabase, artifact_root: Path
) -> None:
    session = create_session_factory(database.engine)()
    try:
        repository = FileSystemArtifactManifestRepository(session, artifact_root)
        with pytest.raises(ArtifactManifestInvalidError, match="caller-owned"):
            repository.get(ArtifactId("art_missing"))
    finally:
        session.close()

    _seed_run(database)
    receipt = _write(artifact_root)
    with database.transaction() as active:
        repository = FileSystemArtifactManifestRepository(active, artifact_root)
        with pytest.raises(ArtifactManifestInvalidError, match="must use"):
            repository.register(
                artifact_id=cast(Any, "art_page-one"),
                run_id=RUN_ID,
                node_id=NODE_ID,
                partition_key=PARTITION,
                write_receipt=receipt,
                media_type="application/json",
                schema_version=1,
                row_count=0,
                created_at=_timestamp(2),
            )
