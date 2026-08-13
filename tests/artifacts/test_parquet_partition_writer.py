"""Deterministic multi-partition Parquet publication tests."""

# pyright: reportPrivateUsage=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false

import hashlib
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Never, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from paritygrid.adapters.artifacts import (
    AtomicParquetPartitionWriter,
    FileSystemArtifactManifestRepository,
    FileSystemArtifactWriter,
    decode_normalized_inventory_table,
    decode_raw_inventory_table,
    parquet_partition_path,
)
from paritygrid.adapters.artifacts.parquet import partitions as publisher
from paritygrid.adapters.persistence import (
    SQLiteDatabase,
    SQLiteDatabaseConfig,
)
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyPipelineRepository,
    SqlAlchemyRunRepository,
)
from paritygrid.application.ports import (
    MAX_PARQUET_PARTITION_NUMBER,
    ArtifactAlreadyExistsError,
    ArtifactRelativePath,
    ArtifactSizeLimitError,
    ArtifactWriteReceipt,
    ConfigurationDocument,
    NormalizedInventoryBatch,
    NormalizedInventoryRow,
    ParquetDatasetKind,
    ParquetEncodingError,
    RawInventoryBatch,
    RawInventoryRecord,
    RedactedDocument,
)
from paritygrid.domain.models import (
    ArtifactId,
    ConnectorId,
    CurrencyCode,
    InventoryRecord,
    Money,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
)
from paritygrid.domain.pipeline import PartitionKey

RUN_ID = RunId("run_partitions")
NODE_ID = NodeId("nod_export")
PARTITION_KEY = PartitionKey("region:1")


def _timestamp(second: int = 0) -> UtcTimestamp:
    return UtcTimestamp(datetime(2026, 8, 13, 12, 0, second, 123456, tzinfo=UTC))


def _raw(index: int = 0) -> RawInventoryBatch:
    return RawInventoryBatch(
        (
            RawInventoryRecord(
                0,
                ConnectorId("con_partition-source"),
                f"source {index}",
                _timestamp(index),
                RedactedDocument.from_mapping({"quantity": index + 1, "sku": f"SKU-{index + 1}"}),
            ),
        )
    )


def _normalized(index: int = 0) -> NormalizedInventoryBatch:
    record = InventoryRecord.create(
        sku=f"SKU-{index + 1}",
        name="Café — ميناء",
        quantity=index + 1,
        unit_price=Money(Decimal("12.34"), CurrencyCode("USD"), 2),
        updated_at=_timestamp(index),
        connector_id=ConnectorId("con_partition-source"),
        source_record_key=f"source {index}",
        attributes={"color": "Blå"},
    )
    return NormalizedInventoryBatch((NormalizedInventoryRow(0, record),))


def _writer(root: Path, *, maximum_bytes: int = 10_000_000) -> AtomicParquetPartitionWriter:
    return AtomicParquetPartitionWriter(FileSystemArtifactWriter(root, maximum_bytes=maximum_bytes))


def test_partition_paths_are_exact_portable_and_collision_free() -> None:
    assert str(
        parquet_partition_path(
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PARTITION_KEY,
            dataset=ParquetDatasetKind.RAW,
            partition_number=0,
        )
    ) == ("runs/run_partitions/raw/nod_export/key-ojswo2lpny5dc/part-0000000000.parquet")
    first = parquet_partition_path(
        run_id=RUN_ID,
        node_id=NODE_ID,
        partition_key=PartitionKey("a:b"),
        dataset=ParquetDatasetKind.NORMALIZED,
        partition_number=MAX_PARQUET_PARTITION_NUMBER,
    )
    second = parquet_partition_path(
        run_id=RUN_ID,
        node_id=NODE_ID,
        partition_key=PartitionKey("a-b"),
        dataset=ParquetDatasetKind.NORMALIZED,
        partition_number=MAX_PARQUET_PARTITION_NUMBER,
    )
    assert first != second
    assert str(first).endswith("part-2147483647.parquet")
    assert ":" not in str(first)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("run_id", "run_partitions"),
        ("node_id", "nod_export"),
        ("partition_key", "region:1"),
        ("dataset", "raw"),
        ("partition_number", True),
        ("partition_number", -1),
        ("partition_number", MAX_PARQUET_PARTITION_NUMBER + 1),
    ],
)
def test_partition_path_rejects_untyped_or_out_of_range_identity(field: str, value: object) -> None:
    arguments: dict[str, object] = {
        "run_id": RUN_ID,
        "node_id": NODE_ID,
        "partition_key": PARTITION_KEY,
        "dataset": ParquetDatasetKind.RAW,
        "partition_number": 0,
    }
    arguments[field] = value
    with pytest.raises((TypeError, ValueError)):
        parquet_partition_path(**cast(Any, arguments))


def test_raw_and_normalized_partitions_publish_and_round_trip(tmp_path: Path) -> None:
    root = tmp_path / "artifacts Café % ميناء"
    root.mkdir()
    writer = _writer(root)
    raw_receipt = writer.write_raw(
        run_id=RUN_ID,
        node_id=NODE_ID,
        partition_key=PARTITION_KEY,
        partition_number=0,
        batch=_raw(),
    )
    normalized_receipt = writer.write_normalized(
        run_id=RUN_ID,
        node_id=NODE_ID,
        partition_key=PARTITION_KEY,
        partition_number=0,
        batch=_normalized(),
    )

    raw_path = root / str(raw_receipt.write_receipt.relative_path)
    normalized_path = root / str(normalized_receipt.write_receipt.relative_path)
    assert decode_raw_inventory_table(pq.read_table(raw_path)) == _raw()
    assert decode_normalized_inventory_table(pq.read_table(normalized_path)) == _normalized()
    assert raw_receipt.dataset is ParquetDatasetKind.RAW
    assert normalized_receipt.dataset is ParquetDatasetKind.NORMALIZED
    assert raw_receipt.media_type == "application/vnd.apache.parquet"
    assert raw_receipt.row_count == normalized_receipt.row_count == 1
    assert raw_receipt.schema_fingerprint != normalized_receipt.schema_fingerprint
    assert raw_receipt.write_receipt.sha256 == hashlib.sha256(raw_path.read_bytes()).hexdigest()


def test_multi_partition_bytes_are_deterministic_and_have_fixed_writer_metadata(
    tmp_path: Path,
) -> None:
    roots = (tmp_path / "one", tmp_path / "two")
    for root in roots:
        root.mkdir()
    receipts = tuple(
        _writer(root).write_normalized(
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PARTITION_KEY,
            partition_number=7,
            batch=_normalized(),
        )
        for root in roots
    )
    files = tuple(
        root / str(receipt.write_receipt.relative_path)
        for root, receipt in zip(roots, receipts, strict=True)
    )
    assert receipts[0].write_receipt.sha256 == receipts[1].write_receipt.sha256
    assert files[0].read_bytes() == files[1].read_bytes()
    metadata = pq.ParquetFile(files[0]).metadata
    assert metadata.num_rows == 1
    assert metadata.num_row_groups == 1
    assert metadata.format_version == "2.6"


def test_multiple_partition_numbers_publish_distinct_immutable_files(tmp_path: Path) -> None:
    writer = _writer(tmp_path)
    receipts = tuple(
        writer.write_raw(
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PARTITION_KEY,
            partition_number=index,
            batch=_raw(index),
        )
        for index in range(3)
    )
    assert len({receipt.write_receipt.relative_path for receipt in receipts}) == 3
    assert len({receipt.write_receipt.sha256 for receipt in receipts}) == 3
    assert all(
        (tmp_path / str(receipt.write_receipt.relative_path)).is_file() for receipt in receipts
    )
    with pytest.raises(ArtifactAlreadyExistsError):
        writer.write_raw(
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PARTITION_KEY,
            partition_number=0,
            batch=_raw(),
        )


def test_empty_partition_is_valid_and_readable(tmp_path: Path) -> None:
    receipt = _writer(tmp_path).write_raw(
        run_id=RUN_ID,
        node_id=NODE_ID,
        partition_key=PARTITION_KEY,
        partition_number=0,
        batch=RawInventoryBatch(()),
    )
    table = pq.read_table(tmp_path / str(receipt.write_receipt.relative_path))
    assert receipt.row_count == 0
    assert decode_raw_inventory_table(table) == RawInventoryBatch(())


def test_artifact_size_failure_leaves_no_partition_or_staging(tmp_path: Path) -> None:
    writer = _writer(tmp_path, maximum_bytes=1)
    with pytest.raises(ArtifactSizeLimitError):
        writer.write_normalized(
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PARTITION_KEY,
            partition_number=0,
            batch=_normalized(),
        )
    assert tuple(tmp_path.rglob("*.parquet")) == ()
    assert tuple(path for path in tmp_path.rglob("*") if path.name.startswith(".pg-")) == ()


class _CapturingWriter:
    def __init__(self) -> None:
        self.path: ArtifactRelativePath | None = None
        self.chunks: tuple[bytes, ...] = ()

    def write(
        self, relative_path: ArtifactRelativePath, chunks: Iterable[bytes]
    ) -> ArtifactWriteReceipt:
        self.path = relative_path
        self.chunks = tuple(chunks)
        content = b"".join(self.chunks)
        return ArtifactWriteReceipt(
            relative_path, len(content), hashlib.sha256(content).hexdigest()
        )


def test_partition_writer_delegates_bounded_chunks_and_rejects_bad_constructor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(TypeError, match="ArtifactWriter"):
        AtomicParquetPartitionWriter(cast(Any, object()))
    capture = _CapturingWriter()
    monkeypatch.setattr(publisher, "MAX_ARTIFACT_CHUNK_BYTES", 100)
    receipt = AtomicParquetPartitionWriter(capture).write_raw(
        run_id=RUN_ID,
        node_id=NODE_ID,
        partition_key=PARTITION_KEY,
        partition_number=0,
        batch=_raw(),
    )
    assert capture.path == receipt.write_receipt.relative_path
    assert len(capture.chunks) > 1
    assert all(0 < len(chunk) <= 100 for chunk in capture.chunks)
    assert b"".join(capture.chunks).startswith(b"PAR1")


def test_buffer_chunking_handles_empty_and_exact_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(publisher, "MAX_ARTIFACT_CHUNK_BYTES", 3)
    assert tuple(publisher._buffer_chunks(pa.py_buffer(b""))) == ()
    assert tuple(publisher._buffer_chunks(pa.py_buffer(b"abcdef"))) == (b"abc", b"def")
    assert tuple(publisher._buffer_chunks(pa.py_buffer(b"abcdefg"))) == (
        b"abc",
        b"def",
        b"g",
    )


def test_serialization_failure_is_typed_redacted_and_closes_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingSink:
        closed = False

        def close(self) -> None:
            self.closed = True

    sink = FailingSink()

    def fail_write(*_args: object, **_kwargs: object) -> Never:
        raise pa.ArrowInvalid("canary-row-content")

    monkeypatch.setattr(publisher.pa, "BufferOutputStream", lambda: sink)
    monkeypatch.setattr(publisher.pq, "write_table", fail_write)
    with pytest.raises(ParquetEncodingError) as captured:
        publisher._serialize_table(cast(Any, object()))
    assert sink.closed
    assert "canary-row-content" not in str(captured.value)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[SQLiteDatabase]:
    database = SQLiteDatabase.open(SQLiteDatabaseConfig(tmp_path / "partitions.db"))
    with database.engine.connect() as connection:
        upgrade_to_head(connection)
    try:
        yield database
    finally:
        database.close()


def _seed_run(database: SQLiteDatabase) -> None:
    with database.transaction() as session:
        pipelines = SqlAlchemyPipelineRepository(session)
        pipelines.create(
            pipeline_id=PipelineId("pip_partitions"),
            display_name="Partition pipeline",
            description=None,
            created_at=_timestamp(0),
        )
        pipelines.publish_version(
            pipeline_id=PipelineId("pip_partitions"),
            expected_latest_version=None,
            specification=ConfigurationDocument.from_mapping({"nodes": []}),
            planner_format_version=1,
            published_at=_timestamp(0),
        )
        SqlAlchemyRunRepository(session).create(
            run_id=RUN_ID,
            pipeline_id=PipelineId("pip_partitions"),
            pipeline_version=PipelineVersion(1),
            runner_kind="sequential",
            runner_configuration=ConfigurationDocument.from_mapping({"workers": 1}),
            scenario_seed=None,
            node_ids=(NODE_ID,),
            created_at=_timestamp(1),
        )


def test_multiple_partitions_are_manifest_ready_and_verified(
    database: SQLiteDatabase, tmp_path: Path
) -> None:
    _seed_run(database)
    root = tmp_path / "artifacts"
    root.mkdir()
    partition_writer = _writer(root)
    receipts = tuple(
        partition_writer.write_raw(
            run_id=RUN_ID,
            node_id=NODE_ID,
            partition_key=PARTITION_KEY,
            partition_number=index,
            batch=_raw(index),
        )
        for index in range(2)
    )
    with database.transaction() as session:
        manifests = FileSystemArtifactManifestRepository(session, root)
        registered = tuple(
            manifests.register(
                artifact_id=ArtifactId(f"art_partition-{index}"),
                run_id=receipt.run_id,
                node_id=receipt.node_id,
                partition_key=receipt.partition_key,
                write_receipt=receipt.write_receipt,
                media_type=receipt.media_type,
                schema_version=receipt.schema_version,
                row_count=receipt.row_count,
                created_at=_timestamp(2 + index),
            )
            for index, receipt in enumerate(receipts)
        )
    assert tuple(record.sha256 for record in registered) == tuple(
        receipt.write_receipt.sha256 for receipt in receipts
    )
    with database.transaction() as session:
        page = FileSystemArtifactManifestRepository(session, root).list_for_run(RUN_ID, limit=10)
    assert page.items == registered
