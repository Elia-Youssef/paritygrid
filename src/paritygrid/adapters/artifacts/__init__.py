"""Filesystem artifact adapter boundaries."""

from paritygrid.adapters.artifacts.manifests import FileSystemArtifactManifestRepository
from paritygrid.adapters.artifacts.parquet import (
    NORMALIZED_INVENTORY_SCHEMA_FINGERPRINT,
    RAW_INVENTORY_SCHEMA_FINGERPRINT,
    AtomicParquetPartitionWriter,
    decode_normalized_inventory_table,
    decode_raw_inventory_table,
    encode_normalized_inventory_batch,
    encode_raw_inventory_batch,
    normalized_inventory_schema,
    parquet_partition_path,
    raw_inventory_schema,
)
from paritygrid.adapters.artifacts.paths import resolve_artifact_path, resolve_artifact_root
from paritygrid.adapters.artifacts.writer import FileSystemArtifactWriter

__all__ = [
    "NORMALIZED_INVENTORY_SCHEMA_FINGERPRINT",
    "RAW_INVENTORY_SCHEMA_FINGERPRINT",
    "AtomicParquetPartitionWriter",
    "FileSystemArtifactManifestRepository",
    "FileSystemArtifactWriter",
    "decode_normalized_inventory_table",
    "decode_raw_inventory_table",
    "encode_normalized_inventory_batch",
    "encode_raw_inventory_batch",
    "normalized_inventory_schema",
    "parquet_partition_path",
    "raw_inventory_schema",
    "resolve_artifact_path",
    "resolve_artifact_root",
]
