"""Versioned Arrow schemas and strict record codecs."""

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
    "NORMALIZED_INVENTORY_SCHEMA_FINGERPRINT",
    "RAW_INVENTORY_SCHEMA_FINGERPRINT",
    "AtomicParquetPartitionWriter",
    "decode_normalized_inventory_table",
    "decode_raw_inventory_table",
    "encode_normalized_inventory_batch",
    "encode_raw_inventory_batch",
    "normalized_inventory_schema",
    "parquet_partition_path",
    "raw_inventory_schema",
]
