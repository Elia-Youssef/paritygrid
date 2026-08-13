"""Read-only SQLite and filesystem artifact integrity scanner."""

# pyright: reportPrivateUsage=false

import hashlib
import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import cast

from sqlalchemy import select
from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from paritygrid.adapters.artifacts.manifests import (  # pyright: ignore[reportPrivateUsage]
    _manifest_from_row,
    _verify_file,
)
from paritygrid.adapters.artifacts.paths import resolve_artifact_path, resolve_artifact_root
from paritygrid.adapters.persistence.schema import artifact_manifests
from paritygrid.adapters.persistence.writer.contention import is_sqlite_contention
from paritygrid.application.ports.artifact_integrity import (
    MAX_ARTIFACT_INTEGRITY_ENTRIES,
    MAX_ARTIFACT_INTEGRITY_ISSUES,
    ArtifactIntegrityIssue,
    ArtifactIntegrityIssueKind,
    ArtifactIntegrityScanCorruptionError,
    ArtifactIntegrityScanInvalidError,
    ArtifactIntegrityScanLimitError,
    ArtifactIntegrityScanner,
    ArtifactIntegrityScanReport,
    ArtifactIntegrityScanStorageError,
    ArtifactIntegrityScanStorageUnavailableError,
)
from paritygrid.application.ports.artifacts import (
    ArtifactIntegrityError,
    ArtifactManifestCorruptionError,
    ArtifactManifestRecord,
    ArtifactPathError,
    ArtifactRelativePath,
)
from paritygrid.application.ports.writer import PersistenceContentionError


def _translate_scan_storage_errors[**P, R](operation: Callable[P, R]) -> Callable[P, R]:
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
            raise ArtifactIntegrityScanStorageUnavailableError(
                "artifact integrity manifest storage is unavailable"
            ) from None
        raise ArtifactIntegrityScanStorageError(
            "artifact integrity manifest query failed"
        ) from None

    return translated


@dataclass(frozen=True, slots=True)
class _ObservedFile:
    relative_path: ArtifactRelativePath
    absolute_path: Path


class FileSystemArtifactIntegrityScanner(ArtifactIntegrityScanner):
    """Compare all manifests and files without following links or deleting evidence."""

    __slots__ = ("_root", "_session")

    def __init__(self, session: Session, artifact_root: Path) -> None:
        value = cast(object, session)
        if not isinstance(value, Session):
            raise TypeError("artifact integrity scanner requires a Session")
        self._session = value
        self._root = resolve_artifact_root(artifact_root)

    @_translate_scan_storage_errors
    def scan(self) -> ArtifactIntegrityScanReport:
        """Return a deterministic complete comparison within configured bounds."""
        if not self._session.in_transaction():
            raise ArtifactIntegrityScanInvalidError(
                "artifact integrity scan requires a caller-owned transaction"
            )
        rows = (
            self._session.execute(
                select(artifact_manifests)
                .order_by(artifact_manifests.c.relative_path)
                .limit(MAX_ARTIFACT_INTEGRITY_ENTRIES + 1)
            )
            .mappings()
            .all()
        )
        if len(rows) > MAX_ARTIFACT_INTEGRITY_ENTRIES:
            raise ArtifactIntegrityScanLimitError(
                "artifact manifest inventory exceeds the scan limit"
            )
        try:
            manifests = tuple(_manifest_from_row(row) for row in rows)
        except ArtifactManifestCorruptionError:
            raise ArtifactIntegrityScanCorruptionError(
                "artifact manifest inventory is corrupt"
            ) from None
        manifest_paths = tuple(str(record.relative_path) for record in manifests)
        if len(set(manifest_paths)) != len(manifest_paths):
            raise ArtifactIntegrityScanCorruptionError("artifact manifest paths are not unique")

        observed, observed_count, issues = _observe_files(self._root)
        observed_by_path = {str(item.relative_path): item for item in observed}
        manifest_by_path = {str(record.relative_path): record for record in manifests}
        verified = 0
        for record in manifests:
            installed = observed_by_path.get(str(record.relative_path))
            if installed is None:
                issues.append(
                    ArtifactIntegrityIssue(
                        ArtifactIntegrityIssueKind.MISSING_FILE,
                        record.relative_path,
                        record.artifact_id,
                        None,
                    )
                )
                continue
            try:
                expected = resolve_artifact_path(self._root, record.relative_path)
                if expected != installed.absolute_path:
                    raise ArtifactIntegrityError("artifact scan path changed")
                _verify_file(expected, record.byte_size, record.sha256)
            except ArtifactIntegrityError, ArtifactPathError, OSError:
                issues.append(
                    ArtifactIntegrityIssue(
                        ArtifactIntegrityIssueKind.INVALID_FILE,
                        record.relative_path,
                        record.artifact_id,
                        None,
                    )
                )
            else:
                verified += 1
        for item in observed:
            if str(item.relative_path) not in manifest_by_path:
                issues.append(
                    ArtifactIntegrityIssue(
                        ArtifactIntegrityIssueKind.ORPHAN_FILE,
                        item.relative_path,
                        None,
                        None,
                    )
                )
        if len(issues) > MAX_ARTIFACT_INTEGRITY_ISSUES:
            raise ArtifactIntegrityScanLimitError(
                "artifact integrity issue inventory exceeds the scan limit"
            )
        ordered = tuple(sorted(issues, key=lambda issue: issue.order_key))
        digest = _inventory_sha256(manifests, observed, ordered)
        return ArtifactIntegrityScanReport(
            manifest_count=len(manifests),
            observed_file_count=observed_count,
            verified_manifest_count=verified,
            issues=ordered,
            inventory_sha256=digest,
        )


def _observe_files(
    root: Path,
) -> tuple[tuple[_ObservedFile, ...], int, list[ArtifactIntegrityIssue]]:
    pending = [root]
    observed: list[_ObservedFile] = []
    issues: list[ArtifactIntegrityIssue] = []
    entry_count = 0
    observed_file_count = 0
    try:
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as entries:
                for entry in sorted(entries, key=lambda item: item.name):
                    entry_count += 1
                    if entry_count > MAX_ARTIFACT_INTEGRITY_ENTRIES:
                        raise ArtifactIntegrityScanLimitError(
                            "artifact filesystem inventory exceeds the scan limit"
                        )
                    path = Path(entry.path)
                    relative_text = path.relative_to(root).as_posix()
                    if entry.is_symlink() or path.is_junction():
                        issues.append(_unsafe_issue(relative_text))
                    elif entry.is_dir(follow_symlinks=False):
                        pending.append(path)
                    elif entry.is_file(follow_symlinks=False):
                        observed_file_count += 1
                        try:
                            relative_path = ArtifactRelativePath(relative_text)
                        except TypeError, ValueError:
                            issues.append(_unsafe_issue(relative_text))
                        else:
                            observed.append(_ObservedFile(relative_path, path.resolve(strict=True)))
                    else:
                        issues.append(_unsafe_issue(relative_text))
    except ArtifactIntegrityScanLimitError:
        raise
    except OSError:
        raise ArtifactIntegrityScanStorageError(
            "artifact filesystem inventory could not be read"
        ) from None
    ordered = tuple(sorted(observed, key=lambda item: str(item.relative_path)))
    return ordered, observed_file_count, issues


def _unsafe_issue(relative_text: str) -> ArtifactIntegrityIssue:
    encoded = relative_text.encode("utf-8", errors="surrogatepass")
    return ArtifactIntegrityIssue(
        ArtifactIntegrityIssueKind.UNSAFE_ENTRY,
        None,
        None,
        hashlib.sha256(encoded).hexdigest(),
    )


def _inventory_sha256(
    manifests: tuple[ArtifactManifestRecord, ...],
    observed: tuple[_ObservedFile, ...],
    issues: tuple[ArtifactIntegrityIssue, ...],
) -> str:
    value = {
        "manifests": [
            {
                "artifact_id": str(record.artifact_id),
                "byte_size": record.byte_size,
                "relative_path": str(record.relative_path),
                "row_count": record.row_count,
                "schema_version": record.schema_version,
                "sha256": record.sha256,
            }
            for record in manifests
        ],
        "observed_paths": [str(item.relative_path) for item in observed],
        "issues": [issue.order_key for issue in issues],
    }
    encoded = json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()
