"""Deterministic publication of immutable Parquet partitions."""

import base64
from collections.abc import Iterator
from contextlib import suppress
from typing import Protocol, cast

import pyarrow as pa
import pyarrow.parquet as pq

from paritygrid.adapters.artifacts.parquet.normalized import (
    NORMALIZED_INVENTORY_SCHEMA_FINGERPRINT,
    encode_normalized_inventory_batch,
)
from paritygrid.adapters.artifacts.parquet.raw import (
    RAW_INVENTORY_SCHEMA_FINGERPRINT,
    encode_raw_inventory_batch,
)
from paritygrid.application.ports.artifacts import (
    MAX_ARTIFACT_CHUNK_BYTES,
    ArtifactRelativePath,
    ArtifactWriter,
    ArtifactWriteReceipt,
)
from paritygrid.application.ports.parquet import (
    MAX_PARQUET_PARTITION_NUMBER,
    NORMALIZED_PARQUET_SCHEMA_VERSION,
    RAW_PARQUET_SCHEMA_VERSION,
    NormalizedInventoryBatch,
    ParquetDatasetKind,
    ParquetEncodingError,
    ParquetPartitionReceipt,
    ParquetPartitionWriter,
    RawInventoryBatch,
)
from paritygrid.domain.models import NodeId, RunId
from paritygrid.domain.pipeline import PartitionKey

_PARQUET_ROW_GROUP_SIZE = 65_536
_PARQUET_DATA_PAGE_SIZE = 1_048_576
_PARQUET_WRITE_BATCH_SIZE = 4_096


class _WriteMethod(Protocol):
    def __call__(
        self, relative_path: ArtifactRelativePath, chunks: Iterator[bytes], /
    ) -> ArtifactWriteReceipt: ...


class AtomicParquetPartitionWriter(ParquetPartitionWriter):
    """Encode canonical tables and delegate immutable bytes to an artifact writer."""

    __slots__ = ("_write",)

    def __init__(self, writer: ArtifactWriter) -> None:
        candidate = getattr(cast(object, writer), "write", None)
        if not callable(candidate):
            raise TypeError("partition writer requires an ArtifactWriter")
        self._write = cast(_WriteMethod, candidate)

    def write_raw(
        self,
        *,
        run_id: RunId,
        node_id: NodeId,
        partition_key: PartitionKey,
        partition_number: int,
        batch: RawInventoryBatch,
    ) -> ParquetPartitionReceipt:
        """Encode and publish one raw inventory partition."""
        path = parquet_partition_path(
            run_id=run_id,
            node_id=node_id,
            partition_key=partition_key,
            dataset=ParquetDatasetKind.RAW,
            partition_number=partition_number,
        )
        table = encode_raw_inventory_batch(batch)
        receipt = self._write(path, _buffer_chunks(_serialize_table(table)))
        return ParquetPartitionReceipt(
            run_id=run_id,
            node_id=node_id,
            partition_key=partition_key,
            dataset=ParquetDatasetKind.RAW,
            partition_number=partition_number,
            row_count=table.num_rows,
            schema_version=RAW_PARQUET_SCHEMA_VERSION,
            schema_fingerprint=RAW_INVENTORY_SCHEMA_FINGERPRINT,
            write_receipt=receipt,
        )

    def write_normalized(
        self,
        *,
        run_id: RunId,
        node_id: NodeId,
        partition_key: PartitionKey,
        partition_number: int,
        batch: NormalizedInventoryBatch,
    ) -> ParquetPartitionReceipt:
        """Encode and publish one normalized inventory partition."""
        path = parquet_partition_path(
            run_id=run_id,
            node_id=node_id,
            partition_key=partition_key,
            dataset=ParquetDatasetKind.NORMALIZED,
            partition_number=partition_number,
        )
        table = encode_normalized_inventory_batch(batch)
        receipt = self._write(path, _buffer_chunks(_serialize_table(table)))
        return ParquetPartitionReceipt(
            run_id=run_id,
            node_id=node_id,
            partition_key=partition_key,
            dataset=ParquetDatasetKind.NORMALIZED,
            partition_number=partition_number,
            row_count=table.num_rows,
            schema_version=NORMALIZED_PARQUET_SCHEMA_VERSION,
            schema_fingerprint=NORMALIZED_INVENTORY_SCHEMA_FINGERPRINT,
            write_receipt=receipt,
        )


def parquet_partition_path(
    *,
    run_id: RunId,
    node_id: NodeId,
    partition_key: PartitionKey,
    dataset: ParquetDatasetKind,
    partition_number: int,
) -> ArtifactRelativePath:
    """Derive one portable collision-free path from typed partition identity."""
    run = _require_exact(run_id, RunId, "partition run")
    node = _require_exact(node_id, NodeId, "partition node")
    key = _require_exact(partition_key, PartitionKey, "partition key")
    kind = _require_exact(dataset, ParquetDatasetKind, "partition dataset")
    number = _validate_partition_number(partition_number)
    key_segment = base64.b32encode(key.to_bytes()).decode("ascii").rstrip("=").lower()
    return ArtifactRelativePath(
        f"runs/{run}/{kind.value}/{node}/key-{key_segment}/part-{number:010d}.parquet"
    )


def _serialize_table(table: pa.Table) -> pa.Buffer:
    sink = pa.BufferOutputStream()
    try:
        pq.write_table(  # pyright: ignore[reportUnknownMemberType]
            table,
            sink,
            version="2.6",
            compression="zstd",
            compression_level=3,
            use_dictionary=False,
            write_statistics=True,
            row_group_size=_PARQUET_ROW_GROUP_SIZE,
            data_page_size=_PARQUET_DATA_PAGE_SIZE,
            data_page_version="2.0",
            use_compliant_nested_type=True,
            write_batch_size=_PARQUET_WRITE_BATCH_SIZE,
            write_page_checksum=True,
            sorting_columns=(pq.SortingColumn(0),),
            store_schema=True,
        )
        return sink.getvalue()
    except pa.ArrowException, OSError, OverflowError, TypeError, ValueError:
        with suppress(pa.ArrowException, OSError):
            sink.close()
        raise ParquetEncodingError("Parquet partition could not be serialized") from None


def _buffer_chunks(buffer: pa.Buffer) -> Iterator[bytes]:
    for offset in range(0, buffer.size, MAX_ARTIFACT_CHUNK_BYTES):
        size = min(MAX_ARTIFACT_CHUNK_BYTES, buffer.size - offset)
        yield buffer.slice(offset, size).to_pybytes()


def _require_exact[T](value: object, expected: type[T], subject: str) -> T:
    if type(value) is not expected:
        raise TypeError(f"{subject} must use {expected.__name__}")
    return cast(T, value)


def _validate_partition_number(value: object) -> int:
    if type(value) is not int:
        raise TypeError("partition number must be an integer")
    if not 0 <= value <= MAX_PARQUET_PARTITION_NUMBER:
        raise ValueError("partition number is outside the supported range")
    return value
