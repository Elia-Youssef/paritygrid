"""Versioned Arrow schemas and strict record codecs."""

from paritygrid.adapters.artifacts.parquet.normalized import (
    NORMALIZED_INVENTORY_SCHEMA_FINGERPRINT,
    decode_normalized_inventory_table,
    encode_normalized_inventory_batch,
    normalized_inventory_schema,
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
    "decode_normalized_inventory_table",
    "decode_raw_inventory_table",
    "encode_normalized_inventory_batch",
    "encode_raw_inventory_batch",
    "normalized_inventory_schema",
    "raw_inventory_schema",
]
