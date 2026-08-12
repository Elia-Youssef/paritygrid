"""SQLAlchemy adapter for immutable, SQLite-sequenced audit entries."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from sqlalchemy import insert, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence.repositories.repair_audit_common import (
    audit_bounded_text,
    audit_portable_identity,
    audit_positive_int,
    audit_snake_case,
    encode_audit_detail,
    require_audit_exact,
    translate_audit_storage_errors,
)
from paritygrid.adapters.persistence.repositories.repair_audit_mapping import audit_from_row
from paritygrid.adapters.persistence.schema import audit_entries
from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.application.ports.repair_audit import (
    MAX_PERSISTED_INTEGER,
    AuditCorruptionError,
    AuditEntryRecord,
    AuditInvalidRequestError,
    AuditPage,
    AuditRepository,
    AuditSequence,
    AuditSequenceConflictError,
    PendingAuditEntry,
    validate_audit_page_limit,
)
from paritygrid.domain.models import UtcTimestamp


class SqlAlchemyAuditRepository(AuditRepository):
    """Append audit facts without owning the caller's Session or transaction."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @translate_audit_storage_errors
    def append(self, entry: PendingAuditEntry) -> AuditEntryRecord:
        self._require_transaction()
        pending = require_audit_exact(entry, PendingAuditEntry, "audit entry")
        actor = audit_bounded_text(pending.actor, "audit actor", 128)
        operation = audit_snake_case(pending.operation, "audit operation", 96)
        object_kind = audit_snake_case(pending.object_kind, "audit object kind", 48)
        object_id = (
            None
            if pending.object_id is None
            else audit_portable_identity(pending.object_id, "audit object identifier", 128)
        )
        correlation = audit_portable_identity(
            pending.correlation_id, "audit correlation identifier", 96
        )
        occurred_at = require_audit_exact(
            pending.occurred_at, UtcTimestamp, "audit occurrence time"
        )
        schema_version = audit_positive_int(
            pending.detail_schema_version, "audit detail schema version"
        )
        detail = encode_audit_detail(
            require_audit_exact(pending.detail, RedactedDocument, "audit detail")
        )
        self._preflight_sequence()
        try:
            row = (
                self._session.execute(
                    insert(audit_entries)
                    .values(
                        actor=actor,
                        operation=operation,
                        object_kind=object_kind,
                        object_id=object_id,
                        correlation_id=correlation,
                        occurred_at=str(occurred_at),
                        detail_schema_version=schema_version,
                        detail_json=detail.text,
                    )
                    .returning(*audit_entries.c)
                )
                .mappings()
                .one()
            )
        except IntegrityError:
            self._preflight_sequence()
            raise
        return audit_from_row(cast(Mapping[str, object], row))

    @translate_audit_storage_errors
    def get(self, sequence: AuditSequence) -> AuditEntryRecord | None:
        self._require_transaction()
        identity = require_audit_exact(sequence, AuditSequence, "audit sequence")
        row = (
            self._session.execute(
                select(audit_entries).where(audit_entries.c.sequence_number == identity.number)
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else audit_from_row(cast(Mapping[str, object], row))

    @translate_audit_storage_errors
    def list_after(self, *, after: AuditSequence | None, limit: int) -> AuditPage:
        self._require_transaction()
        page_limit = validate_audit_page_limit(limit)
        cursor = (
            None if after is None else require_audit_exact(after, AuditSequence, "audit cursor")
        )
        statement = select(audit_entries)
        if cursor is not None:
            statement = statement.where(audit_entries.c.sequence_number > cursor.number)
        rows = tuple(
            self._session.execute(
                statement.order_by(audit_entries.c.sequence_number).limit(page_limit + 1)
            ).mappings()
        )
        records = tuple(
            audit_from_row(cast(Mapping[str, object], row)) for row in rows[:page_limit]
        )
        next_cursor = None
        if len(rows) > page_limit and records:
            next_cursor = records[-1].sequence
        return AuditPage(records, next_cursor)

    def _preflight_sequence(self) -> None:
        value = self._session.execute(
            text("SELECT seq FROM sqlite_sequence WHERE name = 'audit_entries'")
        ).scalar_one_or_none()
        if value is None:
            return
        if type(value) is not int or value < 0:
            raise AuditCorruptionError("audit sequence state is corrupt")
        if value >= MAX_PERSISTED_INTEGER:
            raise AuditSequenceConflictError("audit sequence cannot advance")

    def _require_transaction(self) -> None:
        if not self._session.in_transaction():
            raise AuditInvalidRequestError("repository requires a caller-owned transaction")


__all__ = ["SqlAlchemyAuditRepository"]
