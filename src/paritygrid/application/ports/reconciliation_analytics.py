"""Dependency-neutral contracts for rebuildable reconciliation analytics."""

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Protocol, cast

from paritygrid.application.ports.analytics import AnalyticalViewCatalogSnapshot
from paritygrid.application.ports.artifacts import ArtifactManifestRecord
from paritygrid.application.ports.parquet import (
    NORMALIZED_PARQUET_SCHEMA_VERSION,
    PARQUET_MEDIA_TYPE,
)
from paritygrid.domain.reconciliation import ReconciliationOutcome

MAX_RECONCILIATION_ARTIFACTS_PER_SIDE = 1_024
MAX_RECONCILIATION_PAGE_SIZE = 100
MAX_RECONCILIATION_INPUT_ROWS_PER_SIDE = 2_147_483_647

_SKU = re.compile(r"[A-Z0-9]+(?:-[A-Z0-9]+)*\Z", flags=re.ASCII)
_LOWER_SHA256 = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)


class ReconciliationQueryError(RuntimeError):
    """Base failure for a disposable reconciliation query."""


class ReconciliationQueryInvalidError(ReconciliationQueryError):
    """Query input violates the bounded analytical contract."""


class ReconciliationQueryStateError(ReconciliationQueryError):
    """The requested snapshot is not the currently prepared snapshot."""


class ReconciliationQueryIntegrityError(ReconciliationQueryError):
    """A committed manifest and normalized Parquet file do not agree."""


class ReconciliationQueryCorruptionError(ReconciliationQueryError):
    """Disposable analytical state or query output is inconsistent."""


class ReconciliationQueryStorageError(ReconciliationQueryError):
    """The analytical database rejected a bounded query operation."""


@dataclass(frozen=True, slots=True)
class NormalizedArtifactSet:
    """One canonical set of committed normalized Parquet manifests."""

    manifests: tuple[ArtifactManifestRecord, ...]
    manifest_sha256: str = field(init=False)
    artifact_sha256s: tuple[str, ...] = field(init=False)
    row_count: int = field(init=False)

    def __post_init__(self) -> None:
        value = cast(object, self.manifests)
        if type(value) is not tuple:
            raise TypeError("normalized artifact manifests must be a tuple")
        records = cast(tuple[object, ...], value)
        if len(records) > MAX_RECONCILIATION_ARTIFACTS_PER_SIDE:
            raise ReconciliationQueryInvalidError(
                "normalized artifact set exceeds the partition limit"
            )
        if any(type(record) is not ArtifactManifestRecord for record in records):
            raise TypeError("normalized artifact set contains an invalid manifest")
        ordered = tuple(
            sorted(
                cast(tuple[ArtifactManifestRecord, ...], records),
                key=lambda item: str(item.artifact_id),
            )
        )
        identities = tuple(str(record.artifact_id) for record in ordered)
        paths = tuple(str(record.relative_path) for record in ordered)
        if len(set(identities)) != len(identities) or len(set(paths)) != len(paths):
            raise ReconciliationQueryInvalidError(
                "normalized artifact manifests must have unique identities and paths"
            )
        for record in ordered:
            if (
                record.media_type != PARQUET_MEDIA_TYPE
                or record.schema_version != NORMALIZED_PARQUET_SCHEMA_VERSION
                or not str(record.relative_path).endswith(".parquet")
            ):
                raise ReconciliationQueryInvalidError(
                    "reconciliation input must use normalized Parquet v1"
                )
        if ordered:
            parent = (ordered[0].run_id, ordered[0].node_id)
            if any((record.run_id, record.node_id) != parent for record in ordered):
                raise ReconciliationQueryInvalidError(
                    "one normalized artifact set must belong to one run node"
                )
        row_count = sum(record.row_count for record in ordered)
        if row_count > MAX_RECONCILIATION_INPUT_ROWS_PER_SIDE:
            raise ReconciliationQueryInvalidError("normalized artifact set exceeds the row limit")
        object.__setattr__(self, "manifests", ordered)
        object.__setattr__(self, "manifest_sha256", _manifest_set_sha256(ordered))
        object.__setattr__(self, "artifact_sha256s", tuple(record.sha256 for record in ordered))
        object.__setattr__(self, "row_count", row_count)


@dataclass(frozen=True, slots=True, order=True)
class ReconciliationQueryCursor:
    """Exclusive canonical SKU cursor for deterministic keyset pages."""

    sku: str

    def __post_init__(self) -> None:
        value = cast(object, self.sku)
        if type(value) is not str:
            raise TypeError("reconciliation cursor SKU must be text")
        if len(value) > 64 or _SKU.fullmatch(value) is None:
            raise ReconciliationQueryInvalidError("reconciliation cursor SKU is invalid")


@dataclass(frozen=True, slots=True)
class ReconciliationQuerySnapshot:
    """Exact committed sources and reviewed views installed for one rebuild."""

    reference_manifest_sha256: str
    target_manifest_sha256: str
    reference_artifact_sha256s: tuple[str, ...]
    target_artifact_sha256s: tuple[str, ...]
    reference_row_count: int
    target_row_count: int
    query_sha256: str
    view_catalog: AnalyticalViewCatalogSnapshot

    def __post_init__(self) -> None:
        for digest in (
            self.reference_manifest_sha256,
            self.target_manifest_sha256,
            self.query_sha256,
        ):
            if type(digest) is not str or _LOWER_SHA256.fullmatch(digest) is None:
                raise TypeError("reconciliation snapshot digest is invalid")
        for values in (self.reference_artifact_sha256s, self.target_artifact_sha256s):
            if type(values) is not tuple or any(
                type(value) is not str or _LOWER_SHA256.fullmatch(value) is None for value in values
            ):
                raise TypeError("reconciliation artifact digests are invalid")
        for value in (self.reference_row_count, self.target_row_count):
            if type(value) is not int or not 0 <= value <= MAX_RECONCILIATION_INPUT_ROWS_PER_SIDE:
                raise ValueError("reconciliation snapshot row count is invalid")
        if type(self.view_catalog) is not AnalyticalViewCatalogSnapshot:
            raise TypeError("reconciliation snapshot view catalog is invalid")


@dataclass(frozen=True, slots=True)
class ReconciliationQueryPage:
    """One bounded deterministic page tied to exact committed source hashes."""

    snapshot: ReconciliationQuerySnapshot
    items: tuple[ReconciliationOutcome, ...]
    next_cursor: ReconciliationQueryCursor | None

    def __post_init__(self) -> None:
        if type(self.snapshot) is not ReconciliationQuerySnapshot:
            raise TypeError("reconciliation page snapshot is invalid")
        if type(self.items) is not tuple or any(
            type(item) is not ReconciliationOutcome for item in self.items
        ):
            raise TypeError("reconciliation page items are invalid")
        if len(self.items) > MAX_RECONCILIATION_PAGE_SIZE:
            raise ValueError("reconciliation page exceeds the result limit")
        skus = tuple(item.sku for item in self.items)
        if skus != tuple(sorted(skus)) or len(set(skus)) != len(skus):
            raise ReconciliationQueryInvalidError(
                "reconciliation page items must use unique SKU order"
            )
        if self.next_cursor is not None and type(self.next_cursor) is not ReconciliationQueryCursor:
            raise TypeError("reconciliation page cursor is invalid")


class ReconciliationQueryEngine(Protocol):
    """Rebuild and query disposable reconciliation state from committed inputs."""

    def rebuild(
        self,
        reference: NormalizedArtifactSet,
        target: NormalizedArtifactSet,
    ) -> ReconciliationQuerySnapshot:
        """Replace disposable state using exact verified source manifests."""
        ...

    def list_outcomes(
        self,
        snapshot: ReconciliationQuerySnapshot,
        *,
        limit: int,
        after: ReconciliationQueryCursor | None = None,
    ) -> ReconciliationQueryPage:
        """Return a stable keyset page from the currently prepared snapshot."""
        ...


def validate_reconciliation_page_limit(value: object) -> int:
    """Validate an exact bounded reconciliation result limit."""
    if type(value) is not int or not 1 <= value <= MAX_RECONCILIATION_PAGE_SIZE:
        raise ReconciliationQueryInvalidError(
            "reconciliation page limit is outside the supported range"
        )
    return value


def _manifest_set_sha256(records: tuple[ArtifactManifestRecord, ...]) -> str:
    encoded = json.dumps(
        [
            {
                "artifact_id": str(record.artifact_id),
                "byte_size": record.byte_size,
                "created_at": str(record.created_at),
                "media_type": record.media_type,
                "node_id": str(record.node_id),
                "partition_key": str(record.partition_key),
                "relative_path": str(record.relative_path),
                "row_count": record.row_count,
                "run_id": str(record.run_id),
                "schema_version": record.schema_version,
                "sha256": record.sha256,
            }
            for record in records
        ],
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
