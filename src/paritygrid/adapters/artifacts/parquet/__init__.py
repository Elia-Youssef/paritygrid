"""Versioned Arrow schemas and strict record codecs."""

from paritygrid.adapters.artifacts.parquet.conflicts import (
    CONFLICT_ARTIFACT_SCHEMA_FINGERPRINT,
    conflict_artifact_schema,
    decode_reconciliation_conflict_table,
    encode_reconciliation_conflict_batch,
)
from paritygrid.adapters.artifacts.parquet.normalized import (
    NORMALIZED_INVENTORY_SCHEMA_FINGERPRINT,
    decode_normalized_inventory_table,
    encode_normalized_inventory_batch,
    normalized_inventory_schema,
)
from paritygrid.adapters.artifacts.parquet.partitions import (
    AtomicParquetPartitionWriter,
    parquet_partition_path,
)
from paritygrid.adapters.artifacts.parquet.raw import (
    RAW_INVENTORY_SCHEMA_FINGERPRINT,
    decode_raw_inventory_table,
    encode_raw_inventory_batch,
    raw_inventory_schema,
)

__all__ = [
    "CONFLICT_ARTIFACT_SCHEMA_FINGERPRINT",
    "NORMALIZED_INVENTORY_SCHEMA_FINGERPRINT",
    "RAW_INVENTORY_SCHEMA_FINGERPRINT",
    "AtomicParquetPartitionWriter",
    "conflict_artifact_schema",
    "decode_normalized_inventory_table",
    "decode_raw_inventory_table",
    "decode_reconciliation_conflict_table",
    "encode_normalized_inventory_batch",
    "encode_raw_inventory_batch",
    "encode_reconciliation_conflict_batch",
    "normalized_inventory_schema",
    "parquet_partition_path",
    "raw_inventory_schema",
]
