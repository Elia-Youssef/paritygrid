"""Short-transaction SQLite reader and integrity-backed startup recovery."""

from pathlib import Path

from sqlalchemy.orm import Session

from paritygrid.adapters.persistence.finalization import SQLiteFinalizationStateReader
from paritygrid.adapters.persistence.repositories import SqlAlchemyIdempotencyRepository
from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
from paritygrid.application.execution.finalization import FinalizationEvidence
from paritygrid.application.execution.recovery import RecoveryCorruptionError, RecoveryEvidence
from paritygrid.application.ports.artifact_integrity import (
    ArtifactIntegrityIssue,
    ArtifactIntegrityScanStorageError,
)
from paritygrid.application.ports.artifacts import ArtifactManifestRecord
from paritygrid.application.ports.consistency import IdempotencyCursor, IdempotencyRecord
from paritygrid.application.ports.execution import MAX_EXECUTION_PAGE_SIZE
from paritygrid.domain.models import RunId


class SQLiteRecoveryStateReader:
    """Read one recovery frontier plus artifact integrity in short transactions.

    The artifact-integrity gate is deliberately store-wide: a startup scanner
    must not recover any single run while any committed artifact in the store
    is orphaned, missing, or changed, because the damage crosses run
    boundaries. Foreign damage therefore fails this run's recovery closed
    (AMBIGUOUS) rather than being attributed or ignored. Stranded in-progress
    idempotency reservations are likewise reported for the whole store for the
    same single-owner startup reason.
    """

    __slots__ = ("_artifact_root", "_database", "_frontier_reader")

    def __init__(self, database: SQLiteDatabase, artifact_root: Path) -> None:
        if type(database) is not SQLiteDatabase:
            raise TypeError("recovery reader database must use SQLiteDatabase")
        if type(artifact_root) not in (Path, type(Path("."))):
            raise TypeError("recovery artifact root must be a Path")
        self._database = database
        self._artifact_root = artifact_root
        self._frontier_reader = SQLiteFinalizationStateReader(database)

    def read(self, run_id: RunId, /) -> RecoveryEvidence:
        """Return one coherent recovery frontier with integrity evidence."""
        if type(run_id) is not RunId:
            raise TypeError("recovery reader run identity must use RunId")
        # Storage integrity is gated before any frontier interpretation so
        # corrupt databases fail closed instead of surfacing driver errors.
        with self._database.transaction() as session:
            self._require_storage_integrity(session)
        frontier: FinalizationEvidence = self._frontier_reader.read(run_id)
        with self._database.transaction() as session:
            integrity = self._integrity_issues(session)
            # Manifest reads re-verify file digests, so integrity findings are
            # collected first and preserved without triggering that re-verify.
            artifacts = () if integrity else self._run_artifacts(session, run_id)
            idempotency = self._idempotency_in_progress(session)
        return RecoveryEvidence(frontier, artifacts, integrity, idempotency)

    def _require_storage_integrity(self, session: Session) -> None:
        connection = session.connection()
        quick_check = connection.exec_driver_sql("PRAGMA quick_check").scalar()
        if quick_check != "ok":
            raise RecoveryCorruptionError("SQLite quick_check reported corruption")
        foreign_keys = connection.exec_driver_sql("PRAGMA foreign_key_check").first()
        if foreign_keys is not None:
            raise RecoveryCorruptionError("SQLite foreign_key_check reported violations")

    def _integrity_issues(self, session: Session) -> tuple[ArtifactIntegrityIssue, ...]:
        # Deferred imports keep the persistence package free of artifact cycles.
        from paritygrid.adapters.artifacts.integrity import FileSystemArtifactIntegrityScanner

        scanner = FileSystemArtifactIntegrityScanner(session, self._artifact_root)
        try:
            report = scanner.scan()
        except ArtifactIntegrityScanStorageError:
            raise RecoveryCorruptionError("artifact integrity storage failed") from None
        return tuple(report.issues)

    def _run_artifacts(self, session: Session, run_id: RunId) -> tuple[ArtifactManifestRecord, ...]:
        from paritygrid.adapters.artifacts.manifests import FileSystemArtifactManifestRepository

        repository = FileSystemArtifactManifestRepository(session, self._artifact_root)
        records: list[ArtifactManifestRecord] = []
        cursor = None
        while True:
            page = repository.list_for_run(run_id, limit=MAX_EXECUTION_PAGE_SIZE, after=cursor)
            records.extend(page.items)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        return tuple(records)

    def _idempotency_in_progress(self, session: Session) -> tuple[IdempotencyRecord, ...]:
        repository = SqlAlchemyIdempotencyRepository(session)
        records: list[IdempotencyRecord] = []
        cursor: IdempotencyCursor | None = None
        while True:
            page = repository.list_in_progress(limit=MAX_EXECUTION_PAGE_SIZE, after=cursor)
            records.extend(page.items)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        return tuple(records)


__all__ = ["SQLiteRecoveryStateReader"]
