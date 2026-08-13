"""Verified filesystem and SQLite artifact manifest repository."""

import hashlib
import os
import stat
from collections.abc import Callable
from functools import wraps
from pathlib import Path
from typing import BinaryIO, Protocol, cast

from sqlalchemy import select, tuple_
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from paritygrid.adapters.artifacts.paths import resolve_artifact_path, resolve_artifact_root
from paritygrid.adapters.persistence.repositories.execution_common import (
    MAX_PERSISTED_INTEGER,
)
from paritygrid.adapters.persistence.schema import artifact_manifests, run_nodes, runs
from paritygrid.adapters.persistence.writer.contention import is_sqlite_contention
from paritygrid.application.ports.artifacts import (
    MAX_ARTIFACT_ROW_COUNT,
    ArtifactIntegrityError,
    ArtifactManifestConflictError,
    ArtifactManifestCorruptionError,
    ArtifactManifestInvalidError,
    ArtifactManifestPage,
    ArtifactManifestRecord,
    ArtifactManifestRepository,
    ArtifactManifestStorageError,
    ArtifactManifestStorageUnavailableError,
    ArtifactPathError,
    ArtifactRelativePath,
    ArtifactWriteReceipt,
    validate_artifact_media_type,
    validate_artifact_page_limit,
)
from paritygrid.application.ports.writer import PersistenceContentionError
from paritygrid.domain.models import ArtifactId, NodeId, RunId, UtcTimestamp
from paritygrid.domain.pipeline import PartitionKey

_VERIFY_CHUNK_BYTES = 1_048_576


class _Digest(Protocol):
    def update(self, value: bytes, /) -> None: ...


def _translate_manifest_storage_errors[**P, R](operation: Callable[P, R]) -> Callable[P, R]:
    @wraps(operation)
    def translated(*args: P.args, **kwargs: P.kwargs) -> R:
        contention = False
        unavailable = False
        try:
            return operation(*args, **kwargs)
        except OperationalError as error:
            contention = is_sqlite_contention(error)
            unavailable = not contention
        except InterfaceError:
            unavailable = True
        except SQLAlchemyError:
            pass
        if contention:
            raise PersistenceContentionError("Persistence is temporarily contended.") from None
        if unavailable:
            raise ArtifactManifestStorageUnavailableError(
                "artifact manifest storage is unavailable"
            ) from None
        raise ArtifactManifestStorageError("artifact manifest storage operation failed") from None

    return translated


class FileSystemArtifactManifestRepository(ArtifactManifestRepository):
    """Verify files and persist manifests in one caller-owned Session transaction."""

    __slots__ = ("_root", "_session")

    def __init__(self, session: Session, artifact_root: Path) -> None:
        self._session = session
        self._root = resolve_artifact_root(artifact_root)

    @_translate_manifest_storage_errors
    def register(
        self,
        *,
        artifact_id: ArtifactId,
        run_id: RunId,
        node_id: NodeId,
        partition_key: PartitionKey,
        write_receipt: ArtifactWriteReceipt,
        media_type: str,
        schema_version: int,
        row_count: int,
        created_at: UtcTimestamp,
    ) -> ArtifactManifestRecord:
        self._require_transaction()
        receipt = _require_exact(write_receipt, ArtifactWriteReceipt, "artifact write receipt")
        candidate = ArtifactManifestRecord(
            artifact_id=_require_exact(artifact_id, ArtifactId, "artifact identifier"),
            run_id=_require_exact(run_id, RunId, "run identifier"),
            node_id=_require_exact(node_id, NodeId, "node identifier"),
            partition_key=_require_exact(partition_key, PartitionKey, "partition key"),
            relative_path=receipt.relative_path,
            media_type=_require_media_type(media_type),
            schema_version=_require_int(
                schema_version, "artifact schema version", 1, MAX_PERSISTED_INTEGER
            ),
            byte_size=receipt.byte_size,
            row_count=_require_int(row_count, "artifact row count", 0, MAX_ARTIFACT_ROW_COUNT),
            sha256=receipt.sha256,
            created_at=_require_exact(created_at, UtcTimestamp, "artifact creation time"),
        )
        self._verify(candidate)
        self._require_parent(candidate)
        row = (
            self._session.execute(
                sqlite_insert(artifact_manifests)
                .values(**_stored_values(candidate))
                .on_conflict_do_nothing()
                .returning(*artifact_manifests.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is not None:
            return _manifest_from_row(row)
        installed = self._get_unverified(candidate.artifact_id)
        if installed == candidate:
            return candidate
        raise ArtifactManifestConflictError("artifact manifest identity or path already exists")

    @_translate_manifest_storage_errors
    def get(self, artifact_id: ArtifactId) -> ArtifactManifestRecord | None:
        self._require_transaction()
        identity = _require_exact(artifact_id, ArtifactId, "artifact identifier")
        record = self._get_unverified(identity)
        if record is not None:
            self._validate_parent(record)
            self._verify(record)
        return record

    @_translate_manifest_storage_errors
    def list_for_run(
        self,
        run_id: RunId,
        *,
        limit: int,
        after: ArtifactId | None = None,
    ) -> ArtifactManifestPage:
        self._require_transaction()
        identity = _require_exact(run_id, RunId, "run identifier")
        page_size = validate_artifact_page_limit(limit)
        cursor = None if after is None else _require_exact(after, ArtifactId, "artifact cursor")
        query = select(artifact_manifests).where(artifact_manifests.c.run_id == str(identity))
        if cursor is not None:
            query = query.where(artifact_manifests.c.artifact_id > str(cursor))
        rows = (
            self._session.execute(
                query.order_by(artifact_manifests.c.artifact_id).limit(page_size + 1)
            )
            .mappings()
            .all()
        )
        records = tuple(_manifest_from_row(row) for row in rows[:page_size])
        self._validate_parents(records)
        for record in records:
            self._verify(record)
        next_cursor = records[-1].artifact_id if len(rows) > page_size else None
        return ArtifactManifestPage(records, next_cursor)

    def _get_unverified(self, artifact_id: ArtifactId) -> ArtifactManifestRecord | None:
        row = (
            self._session.execute(
                select(artifact_manifests).where(
                    artifact_manifests.c.artifact_id == str(artifact_id)
                )
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else _manifest_from_row(row)

    def _require_parent(self, record: ArtifactManifestRecord) -> None:
        parent = self._parent(record.run_id, record.node_id)
        if parent is None:
            raise ArtifactManifestInvalidError("artifact run-node parent does not exist")
        if record.created_at < parent:
            raise ArtifactManifestInvalidError("artifact creation time precedes its run")

    def _validate_parent(self, record: ArtifactManifestRecord) -> None:
        parent = self._parent(record.run_id, record.node_id)
        if parent is None or record.created_at < parent:
            raise ArtifactManifestCorruptionError("artifact manifest parent is corrupt")

    def _validate_parents(self, records: tuple[ArtifactManifestRecord, ...]) -> None:
        if not records:
            return
        identities = {(str(record.run_id), str(record.node_id)) for record in records}
        rows = self._session.execute(
            select(run_nodes.c.run_id, run_nodes.c.node_id, runs.c.created_at)
            .select_from(run_nodes.join(runs, runs.c.run_id == run_nodes.c.run_id))
            .where(tuple_(run_nodes.c.run_id, run_nodes.c.node_id).in_(identities))
        ).all()
        parents = {
            (cast(str, row[0]), cast(str, row[1])): _stored_timestamp(row[2]) for row in rows
        }
        for record in records:
            parent = parents.get((str(record.run_id), str(record.node_id)))
            if parent is None or record.created_at < parent:
                raise ArtifactManifestCorruptionError("artifact manifest parent is corrupt")

    def _parent(self, run_id: RunId, node_id: NodeId) -> UtcTimestamp | None:
        row = self._session.execute(
            select(runs.c.created_at)
            .select_from(
                run_nodes.join(
                    runs,
                    (runs.c.run_id == run_nodes.c.run_id),
                )
            )
            .where(
                run_nodes.c.run_id == str(run_id),
                run_nodes.c.node_id == str(node_id),
            )
        ).one_or_none()
        return None if row is None else _stored_timestamp(row[0])

    def _verify(self, record: ArtifactManifestRecord) -> None:
        try:
            path = resolve_artifact_path(self._root, record.relative_path)
            _verify_file(path, record.byte_size, record.sha256)
            if resolve_artifact_path(self._root, record.relative_path) != path:
                raise ArtifactIntegrityError("artifact path changed during verification")
        except ArtifactPathError:
            raise ArtifactIntegrityError("artifact path is no longer safely confined") from None

    def _require_transaction(self) -> None:
        if not self._session.in_transaction():
            raise ArtifactManifestInvalidError(
                "artifact repository requires a caller-owned transaction"
            )


def _verify_file(path: Path, expected_size: int, expected_sha256: str) -> None:
    descriptor: int | None = None
    try:
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ArtifactIntegrityError("artifact manifest does not reference a regular file")
        digest = hashlib.sha256()
        byte_size = 0
        with os.fdopen(descriptor, "rb", closefd=True) as stream:
            descriptor = None
            byte_size = _hash_stream(stream, digest)
            after = os.fstat(stream.fileno())
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise ArtifactIntegrityError("artifact file changed during verification")
        installed = os.stat(path, follow_symlinks=False)
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            installed.st_dev,
            installed.st_ino,
            installed.st_size,
            installed.st_mtime_ns,
        ):
            raise ArtifactIntegrityError("artifact path changed during verification")
        if byte_size != expected_size or digest.hexdigest() != expected_sha256:
            raise ArtifactIntegrityError("artifact file does not match its manifest")
    except FileNotFoundError:
        raise ArtifactIntegrityError("artifact manifest file is missing") from None
    except ArtifactIntegrityError:
        raise
    except OSError:
        raise ArtifactIntegrityError("artifact file could not be verified") from None
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                raise ArtifactIntegrityError(
                    "artifact verification handle could not be closed"
                ) from None


def _hash_stream(stream: BinaryIO, digest: _Digest) -> int:
    total = 0
    while True:
        chunk = stream.read(_VERIFY_CHUNK_BYTES)
        if not chunk:
            return total
        total += len(chunk)
        digest.update(chunk)


def _manifest_from_row(row: object) -> ArtifactManifestRecord:
    try:
        mapping = cast("dict[str, object]", row)
        return ArtifactManifestRecord(
            artifact_id=ArtifactId(cast(str, mapping["artifact_id"])),
            run_id=RunId(cast(str, mapping["run_id"])),
            node_id=NodeId(cast(str, mapping["node_id"])),
            partition_key=PartitionKey(cast(str, mapping["partition_key"])),
            relative_path=ArtifactRelativePath(cast(str, mapping["relative_path"])),
            media_type=cast(str, mapping["media_type"]),
            schema_version=cast(int, mapping["schema_version"]),
            byte_size=cast(int, mapping["byte_size"]),
            row_count=cast(int, mapping["row_count"]),
            sha256=cast(str, mapping["sha256"]),
            created_at=UtcTimestamp.parse(cast(str, mapping["created_at"])),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ArtifactManifestCorruptionError("artifact manifest row is corrupt") from error


def _stored_values(record: ArtifactManifestRecord) -> dict[str, object]:
    return {
        "artifact_id": str(record.artifact_id),
        "run_id": str(record.run_id),
        "node_id": str(record.node_id),
        "partition_key": str(record.partition_key),
        "relative_path": str(record.relative_path),
        "media_type": record.media_type,
        "schema_version": record.schema_version,
        "byte_size": record.byte_size,
        "row_count": record.row_count,
        "sha256": record.sha256,
        "created_at": str(record.created_at),
    }


def _require_exact[T](value: object, expected: type[T], subject: str) -> T:
    if type(value) is not expected:
        raise ArtifactManifestInvalidError(f"{subject} must use {expected.__name__}")
    return cast(T, value)


def _require_media_type(value: object) -> str:
    try:
        return validate_artifact_media_type(value)
    except (TypeError, ValueError) as error:
        raise ArtifactManifestInvalidError("artifact media type is invalid") from error


def _require_int(value: object, subject: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ArtifactManifestInvalidError(f"{subject} is outside the supported range")
    return value


def _stored_timestamp(value: object) -> UtcTimestamp:
    if not isinstance(value, str):
        raise ArtifactManifestCorruptionError("artifact parent timestamp is corrupt")
    try:
        return UtcTimestamp.parse(value)
    except ValueError as error:
        raise ArtifactManifestCorruptionError("artifact parent timestamp is corrupt") from error
