"""Atomic publication of reconciliation conflict artifacts.

A conflict partition is accepted only when both halves of the existing
protocol succeed: the immutable Parquet file is staged, flushed, and atomically
renamed through the artifact writer, and only then is its manifest registered
durably. A failure at either step raises before any accepted publication is
returned, so a partial artifact is never reported as accepted.
"""

from dataclasses import dataclass

from paritygrid.application.ports.artifacts import (
    ArtifactManifestRecord,
    ArtifactManifestRepository,
)
from paritygrid.application.ports.parquet import (
    ParquetPartitionReceipt,
    ParquetPartitionWriter,
    ReconciliationConflictBatch,
)
from paritygrid.domain.models import ArtifactId, NodeId, RunId, UtcTimestamp
from paritygrid.domain.pipeline import PartitionKey


class ConflictPublicationError(RuntimeError):
    """A conflict artifact could not be published and accepted."""


@dataclass(frozen=True, slots=True)
class ConflictArtifactPublication:
    """One accepted conflict artifact with its durable manifest."""

    receipt: ParquetPartitionReceipt
    manifest: ArtifactManifestRecord


def publish_conflict_artifact(
    *,
    writer: ParquetPartitionWriter,
    manifests: ArtifactManifestRepository,
    artifact_id: ArtifactId,
    run_id: RunId,
    node_id: NodeId,
    partition_key: PartitionKey,
    partition_number: int,
    batch: ReconciliationConflictBatch,
    created_at: UtcTimestamp,
) -> ConflictArtifactPublication:
    """Write one conflict partition and register its manifest atomically."""
    if not callable(getattr(writer, "write_conflicts", None)):
        raise ConflictPublicationError("conflict publication requires a ParquetPartitionWriter")
    if not callable(getattr(manifests, "register", None)):
        raise ConflictPublicationError(
            "conflict publication requires an ArtifactManifestRepository"
        )
    try:
        receipt = writer.write_conflicts(
            run_id=run_id,
            node_id=node_id,
            partition_key=partition_key,
            partition_number=partition_number,
            batch=batch,
        )
        manifest = manifests.register(
            artifact_id=artifact_id,
            run_id=run_id,
            node_id=node_id,
            partition_key=partition_key,
            write_receipt=receipt.write_receipt,
            media_type=receipt.media_type,
            schema_version=receipt.schema_version,
            row_count=receipt.row_count,
            created_at=created_at,
        )
    except ConflictPublicationError:
        raise
    except Exception as error:
        raise ConflictPublicationError(
            "conflict artifact was not published and accepted"
        ) from error
    return ConflictArtifactPublication(receipt=receipt, manifest=manifest)
