"""Golden Parquet, atomic-failure, and manifest-protocol conflict artifact tests."""

# pyright: reportPrivateUsage=false

from collections.abc import Generator, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from sqlalchemy import text

from paritygrid.adapters.artifacts import (
    FileSystemArtifactWriter,
)
from paritygrid.adapters.artifacts.manifests import FileSystemArtifactManifestRepository
from paritygrid.adapters.artifacts.parquet import (
    CONFLICT_ARTIFACT_SCHEMA_FINGERPRINT,
    AtomicParquetPartitionWriter,
    conflict_artifact_schema,
    decode_reconciliation_conflict_table,
    encode_reconciliation_conflict_batch,
    parquet_partition_path,
)
from paritygrid.adapters.persistence import SQLiteDatabase, SQLiteDatabaseConfig
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyPipelineRepository,
    SqlAlchemyRunRepository,
)
from paritygrid.application.ports import ConfigurationDocument
from paritygrid.application.ports.artifacts import ArtifactManifestInvalidError
from paritygrid.application.ports.parquet import (
    CONFLICT_PARQUET_SCHEMA_VERSION,
    ParquetDatasetKind,
    ParquetEncodingError,
    ParquetPartitionReceipt,
    ReconciliationConflictBatch,
    ReconciliationConflictRow,
)
from paritygrid.application.reconciliation import (
    ConflictPublicationError,
    publish_conflict_artifact,
)
from paritygrid.application.reconciliation.analysis import (
    ReconciliationAnalysisRequest,
    analyze_reconciliation,
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
from paritygrid.domain.reconciliation import (
    FieldDifference,
    FieldDifferenceKind,
    QuarantineCode,
    ReconciliationClassification,
    SecondaryEvidence,
    SecondaryEvidenceKind,
    SourceObservation,
    SuggestedResolution,
)
from tests.reconciliation.conftest import SOURCE_CONNECTOR, wire_payload

RUN_ID = RunId("run_reconciliation")
NODE_ID = NodeId("nod_reconcile")
PARTITION = PartitionKey("page-0001")
ARTIFACT_ID = ArtifactId("art_conflicts-one")
PIPELINE_ID = PipelineId("pip_reconciliation")


def _timestamp() -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 25, 12, 0, 0, tzinfo=UTC))


def _difference() -> FieldDifference:
    return FieldDifference(
        field="quantity",
        kind=FieldDifferenceKind.VALUE_MISMATCH,
        source_text="2",
        target_text="3",
    )


def _row(index: int = 0, sku: str = "GRID-B") -> ReconciliationConflictRow:
    return ReconciliationConflictRow(
        conflict_index=index,
        sku=sku,
        classification=ReconciliationClassification.FIELD_MISMATCH,
        suggested_resolution=SuggestedResolution.UPDATE_TARGET,
        source_positions=(1,),
        target_positions=(1,),
        source_record_keys=("s-b",),
        target_record_keys=("t-b",),
        differences=(_difference(),),
        secondary=(SecondaryEvidence(SecondaryEvidenceKind.MISMATCH_FIELDS, "quantity"),),
    )


@pytest.fixture
def database(tmp_path: Path) -> Iterator[SQLiteDatabase]:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "conflict state.db"))
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    try:
        yield database
    finally:
        database.close()


@contextmanager
def _manifest_transaction(
    database: SQLiteDatabase, root: Path
) -> Generator[FileSystemArtifactManifestRepository]:
    with database.transaction() as session:
        yield FileSystemArtifactManifestRepository(session, root)


@pytest.fixture
def seeded(database: SQLiteDatabase) -> SQLiteDatabase:
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PIPELINE_ID,
            display_name="Reconciliation pipeline",
            description=None,
            created_at=_timestamp(),
        )
        pipelines.publish_version(
            pipeline_id=PIPELINE_ID,
            expected_latest_version=None,
            specification=ConfigurationDocument.from_mapping({"nodes": []}),
            planner_format_version=1,
            published_at=_timestamp(),
        )
        SqlAlchemyRunRepository(session).create(
            run_id=RUN_ID,
            pipeline_id=PIPELINE_ID,
            pipeline_version=PipelineVersion(1),
            runner_kind="sequential",
            runner_configuration=ConfigurationDocument.from_mapping({}),
            scenario_seed=None,
            node_ids=(NODE_ID,),
            created_at=_timestamp(),
        )
    return database


def test_golden_conflict_batch_round_trips_through_exact_parquet(tmp_path: Path) -> None:
    batch = ReconciliationConflictBatch((_row(),))
    table = encode_reconciliation_conflict_batch(batch)
    assert table.schema.equals(conflict_artifact_schema(), check_metadata=True)
    assert table.num_rows == 1
    decoded = decode_reconciliation_conflict_table(table)
    assert decoded == batch

    path = tmp_path / "conflicts.parquet"
    pq.write_table(table, path)  # pyright: ignore[reportUnknownMemberType]
    reread = pq.read_table(path)  # pyright: ignore[reportUnknownMemberType]
    assert decode_reconciliation_conflict_table(reread) == batch


def test_conflict_schema_fingerprint_and_version_are_frozen() -> None:
    assert CONFLICT_PARQUET_SCHEMA_VERSION == 1
    assert len(CONFLICT_ARTIFACT_SCHEMA_FINGERPRINT) == 64
    assert conflict_artifact_schema().metadata[b"paritygrid.dataset"] == b"reconciliation_conflicts"


def test_conflict_batch_contract_rejects_invalid_rows() -> None:
    with pytest.raises(ValueError, match="cannot carry the match"):
        ReconciliationConflictRow(
            conflict_index=0,
            sku="GRID-A",
            classification=ReconciliationClassification.MATCH,
            suggested_resolution=SuggestedResolution.NONE,
            source_positions=(0,),
            target_positions=(0,),
            source_record_keys=("s",),
            target_record_keys=("t",),
            differences=(),
            secondary=(),
        )
    with pytest.raises(ValueError, match="does not match its classification"):
        ReconciliationConflictRow(
            conflict_index=0,
            sku="GRID-A",
            classification=ReconciliationClassification.MISSING_FROM_TARGET,
            suggested_resolution=SuggestedResolution.UPDATE_TARGET,
            source_positions=(0,),
            target_positions=(),
            source_record_keys=("s",),
            target_record_keys=(),
            differences=(),
            secondary=(),
        )
    with pytest.raises(ValueError, match="parallel"):
        ReconciliationConflictRow(
            conflict_index=0,
            sku="GRID-A",
            classification=ReconciliationClassification.MISSING_FROM_TARGET,
            suggested_resolution=SuggestedResolution.CREATE_TARGET,
            source_positions=(0, 1),
            target_positions=(),
            source_record_keys=("s",),
            target_record_keys=(),
            differences=(),
            secondary=(),
        )
    with pytest.raises(ValueError, match="at least one member"):
        ReconciliationConflictRow(
            conflict_index=0,
            sku="GRID-A",
            classification=ReconciliationClassification.MISSING_FROM_TARGET,
            suggested_resolution=SuggestedResolution.CREATE_TARGET,
            source_positions=(),
            target_positions=(),
            source_record_keys=(),
            target_record_keys=(),
            differences=(),
            secondary=(),
        )
    with pytest.raises(ValueError, match="positions must be sorted and unique"):
        ReconciliationConflictRow(
            conflict_index=0,
            sku="GRID-A",
            classification=ReconciliationClassification.DUPLICATE_SOURCE,
            suggested_resolution=SuggestedResolution.REVIEW_DUPLICATES,
            source_positions=(1, 0),
            target_positions=(),
            source_record_keys=("a", "z"),
            target_record_keys=(),
            differences=(),
            secondary=(),
        )
    with pytest.raises(ValueError, match="contiguous"):
        ReconciliationConflictBatch((_row(index=1),))


def test_conflict_writer_publishes_through_the_atomic_protocol(
    tmp_path: Path, seeded: SQLiteDatabase
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    writer = AtomicParquetPartitionWriter(FileSystemArtifactWriter(root, maximum_bytes=1_048_576))
    batch = ReconciliationConflictBatch((_row(),))
    with _manifest_transaction(seeded, root) as manifests:
        publication = publish_conflict_artifact(
            writer=writer,
            manifests=manifests,
            artifact_id=ARTIFACT_ID,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PARTITION,
            partition_number=0,
            batch=batch,
            created_at=_timestamp(),
        )
    receipt = publication.receipt
    assert receipt.dataset is ParquetDatasetKind.RECONCILIATION
    assert receipt.schema_version == CONFLICT_PARQUET_SCHEMA_VERSION
    assert receipt.schema_fingerprint == CONFLICT_ARTIFACT_SCHEMA_FINGERPRINT
    assert receipt.row_count == 1
    assert receipt.write_receipt.byte_size > 0
    expected_path = root / str(
        parquet_partition_path(
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PARTITION,
            dataset=ParquetDatasetKind.RECONCILIATION,
            partition_number=0,
        )
    )
    assert expected_path.is_file()
    assert publication.manifest.sha256 == receipt.write_receipt.sha256
    assert publication.manifest.relative_path == receipt.write_receipt.relative_path


def test_failed_artifact_write_never_registers_a_manifest(
    tmp_path: Path, seeded: SQLiteDatabase
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    writer = AtomicParquetPartitionWriter(FileSystemArtifactWriter(root, maximum_bytes=1))
    with _manifest_transaction(seeded, root) as manifests, pytest.raises(ConflictPublicationError):
        publish_conflict_artifact(
            writer=writer,
            manifests=manifests,
            artifact_id=ARTIFACT_ID,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PARTITION,
            partition_number=0,
            batch=ReconciliationConflictBatch((_row(),)),
            created_at=_timestamp(),
        )
    with seeded.engine.connect() as connection:
        count = connection.execute(text("SELECT count(*) FROM artifact_manifests")).scalar_one()
    assert count == 0


def test_failed_manifest_registration_reports_no_accepted_publication(
    tmp_path: Path, seeded: SQLiteDatabase
) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    writer = AtomicParquetPartitionWriter(FileSystemArtifactWriter(root, maximum_bytes=1_048_576))

    class FailingManifests:
        def register(self, **_kwargs: object) -> None:
            raise ArtifactManifestInvalidError("simulated registration failure")

    with pytest.raises(ConflictPublicationError, match="not published and accepted"):
        publish_conflict_artifact(
            writer=writer,
            manifests=FailingManifests(),  # type: ignore[arg-type]
            artifact_id=ARTIFACT_ID,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PARTITION,
            partition_number=0,
            batch=ReconciliationConflictBatch((_row(),)),
            created_at=_timestamp(),
        )


def test_publication_requires_exact_contract_types() -> None:
    with pytest.raises(ConflictPublicationError, match="ParquetPartitionWriter"):
        publish_conflict_artifact(
            writer=object(),  # type: ignore[arg-type]
            manifests=object(),  # type: ignore[arg-type]
            artifact_id=ARTIFACT_ID,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PARTITION,
            partition_number=0,
            batch=ReconciliationConflictBatch((_row(),)),
            created_at=_timestamp(),
        )


def test_analysis_conflicts_write_losslessly_through_the_artifact(
    tmp_path: Path, seeded: SQLiteDatabase
) -> None:
    analysis = analyze_reconciliation(
        ReconciliationAnalysisRequest(
            source_observations=(
                SourceObservation(
                    0, SOURCE_CONNECTOR, wire_payload(sku="GRID-A", source_record_key="s-a")
                ),
                SourceObservation(
                    1,
                    SOURCE_CONNECTOR,
                    wire_payload(sku="GRID-B", source_record_key="s-b", quantity=2),
                ),
            ),
            target_observations=(
                SourceObservation(
                    0, SOURCE_CONNECTOR, wire_payload(sku="GRID-A", source_record_key="t-a")
                ),
                SourceObservation(
                    1,
                    SOURCE_CONNECTOR,
                    wire_payload(sku="GRID-B", source_record_key="t-b", quantity=3),
                ),
            ),
            source_input_identity="0" * 64,
            target_input_identity="1" * 64,
        )
    )
    assert len(analysis.conflicts) == 1
    batch = ReconciliationConflictBatch(analysis.conflicts)
    table = encode_reconciliation_conflict_batch(batch)
    assert decode_reconciliation_conflict_table(table) == batch

    root = tmp_path / "artifacts"
    root.mkdir()
    writer = AtomicParquetPartitionWriter(FileSystemArtifactWriter(root, maximum_bytes=1_048_576))
    with _manifest_transaction(seeded, root) as manifests:
        publication = publish_conflict_artifact(
            writer=writer,
            manifests=manifests,
            artifact_id=ARTIFACT_ID,
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PARTITION,
            partition_number=0,
            batch=batch,
            created_at=_timestamp(),
        )
    assert isinstance(publication.receipt, ParquetPartitionReceipt)


def test_oversized_json_column_guard_fails_closed() -> None:
    # The 256 KiB per-value cap is unreachable through a contract-valid row
    # (1,024 bounded keys stay well below it); exercise the guard directly so
    # contract drift cannot silently exceed the artifact bound.
    from paritygrid.adapters.artifacts.parquet import conflicts as adapter

    huge = ["x" * 128 for _index in range(adapter._MAX_JSON_COLUMN_BYTES // 100)]
    with pytest.raises(ParquetEncodingError, match="byte limit"):
        adapter._canonical_json(huge)


def test_normalized_dataset_kind_path_uses_reconciliation_segment() -> None:
    path = parquet_partition_path(
        run_id=RUN_ID,
        node_id=NODE_ID,
        partition_key=PARTITION,
        dataset=ParquetDatasetKind.RECONCILIATION,
        partition_number=3,
    )
    assert str(path) == (
        f"runs/{RUN_ID}/reconciliation/{NODE_ID}/key-{_encoded_key()}/part-0000000003.parquet"
    )


def _encoded_key() -> str:
    import base64

    return base64.b32encode(PARTITION.to_bytes()).decode("ascii").rstrip("=").lower()


def test_quarantine_evidence_stays_out_of_conflict_rows() -> None:
    quarantined_payload = dict(wire_payload(sku="GRID-Q", source_record_key="s-q"))
    quarantined_payload["quantity"] = None  # type: ignore[assignment]
    analysis = analyze_reconciliation(
        ReconciliationAnalysisRequest(
            source_observations=(
                SourceObservation(0, SOURCE_CONNECTOR, quarantined_payload),
                SourceObservation(
                    1, SOURCE_CONNECTOR, wire_payload(sku="GRID-A", source_record_key="s-a")
                ),
            ),
            target_observations=(
                SourceObservation(
                    0, SOURCE_CONNECTOR, wire_payload(sku="GRID-A", source_record_key="t-a")
                ),
            ),
            source_input_identity="0" * 64,
            target_input_identity="1" * 64,
        )
    )
    assert analysis.summary.counts.source_quarantined_count == 1
    assert all(row.sku != "GRID-Q" for row in analysis.conflicts)
    assert analysis.source_quarantined[0].code is QuarantineCode.NULL_FIELD
