"""Filesystem artifact adapter boundaries."""

from paritygrid.adapters.artifacts.manifests import FileSystemArtifactManifestRepository
from paritygrid.adapters.artifacts.parquet import (
    RAW_INVENTORY_SCHEMA_FINGERPRINT,
    decode_raw_inventory_table,
    encode_raw_inventory_batch,
    raw_inventory_schema,
)
from paritygrid.adapters.artifacts.paths import resolve_artifact_path, resolve_artifact_root
from paritygrid.adapters.artifacts.writer import FileSystemArtifactWriter

__all__ = [
    "RAW_INVENTORY_SCHEMA_FINGERPRINT",
    "FileSystemArtifactManifestRepository",
    "FileSystemArtifactWriter",
    "decode_raw_inventory_table",
    "encode_raw_inventory_batch",
    "raw_inventory_schema",
    "resolve_artifact_path",
    "resolve_artifact_root",
]
