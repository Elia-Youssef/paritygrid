"""SQLAlchemy repository for replay-safe idempotent command records."""

from sqlalchemy import and_, or_, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence.repositories.consistency_common import (
    encode_redacted_document,
    idempotency_scope,
    portable_identity,
    positive_int,
    request_digest,
    require_document,
    require_exact,
    require_idempotency_cursor,
    require_redacted_document,
    require_timestamp,
    translate_consistency_storage_errors,
)
from paritygrid.adapters.persistence.repositories.consistency_mapping import (
    StoredIdempotencyRecord,
    stored_idempotency_from_row,
)
from paritygrid.adapters.persistence.schema import idempotency_records
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    ConsistencyCorruptionError,
    ConsistencyInvalidRequestError,
    ConsistencyStaleRowVersionError,
    IdempotencyBeginDisposition,
    IdempotencyBeginResult,
    IdempotencyConflictError,
    IdempotencyCursor,
    IdempotencyPage,
    IdempotencyRecord,
    IdempotencyRepository,
    IdempotencyReservation,
    IdempotencyStatus,
    RedactedDocument,
    validate_consistency_page_limit,
)
from paritygrid.domain.models import UtcTimestamp

_DISPOSITIONS = {
    IdempotencyStatus.IN_PROGRESS: IdempotencyBeginDisposition.IN_PROGRESS_REPLAY,
    IdempotencyStatus.COMPLETED: IdempotencyBeginDisposition.COMPLETED_REPLAY,
    IdempotencyStatus.FAILED: IdempotencyBeginDisposition.FAILED_REPLAY,
}


class SqlAlchemyIdempotencyRepository(IdempotencyRepository):
    """Reserve and complete idempotent commands in a caller-owned transaction."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @translate_consistency_storage_errors
    def begin(
        self,
        *,
        scope: str,
        key: str,
        request: ConfigurationDocument,
        started_at: UtcTimestamp,
    ) -> IdempotencyBeginResult:
        self._require_transaction()
        identity = _require_identity(scope, key)
        request_value = require_document(request, "idempotency request")
        started = require_timestamp(started_at, "idempotency start time")
        digest = request_digest(request_value)
        row = (
            self._session.execute(
                sqlite_insert(idempotency_records)
                .values(
                    scope=identity[0],
                    idempotency_key=identity[1],
                    request_sha256=digest,
                    status=IdempotencyStatus.IN_PROGRESS.value,
                    response_schema_version=None,
                    response_json=None,
                    created_at=str(started),
                    updated_at=str(started),
                    completed_at=None,
                )
                .on_conflict_do_nothing(
                    index_elements=[
                        idempotency_records.c.scope,
                        idempotency_records.c.idempotency_key,
                    ]
                )
                .returning(*idempotency_records.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is not None:
            stored = stored_idempotency_from_row(row)
            if stored.request_sha256 != digest:
                raise ConsistencyCorruptionError("idempotency insert result is corrupt")
            return IdempotencyBeginResult(
                IdempotencyBeginDisposition.STARTED,
                stored.record,
                _reservation(stored),
            )
        existing = self._require_stored(identity)
        if existing.request_sha256 != digest:
            raise IdempotencyConflictError("idempotency identity has a different request")
        return IdempotencyBeginResult(_DISPOSITIONS[existing.record.status], existing.record, None)

    @translate_consistency_storage_errors
    def get(self, *, scope: str, key: str) -> IdempotencyRecord | None:
        self._require_transaction()
        identity = _require_identity(scope, key)
        stored = self._get_stored(identity)
        return None if stored is None else stored.record

    @translate_consistency_storage_errors
    def list_in_progress(
        self, *, limit: int, after: IdempotencyCursor | None = None
    ) -> IdempotencyPage:
        self._require_transaction()
        page_limit = validate_consistency_page_limit(limit)
        cursor = None if after is None else require_idempotency_cursor(after)
        statement = select(idempotency_records).where(
            idempotency_records.c.status == IdempotencyStatus.IN_PROGRESS.value
        )
        if cursor is not None:
            created_at = require_timestamp(cursor.created_at, "idempotency cursor time")
            scope = idempotency_scope(cursor.scope)
            key = portable_identity(cursor.key, "idempotency cursor key", 128)
            statement = statement.where(
                or_(
                    idempotency_records.c.created_at > str(created_at),
                    and_(
                        idempotency_records.c.created_at == str(created_at),
                        idempotency_records.c.scope > scope,
                    ),
                    and_(
                        idempotency_records.c.created_at == str(created_at),
                        idempotency_records.c.scope == scope,
                        idempotency_records.c.idempotency_key > key,
                    ),
                )
            )
        rows = (
            self._session.execute(
                statement.order_by(
                    idempotency_records.c.created_at,
                    idempotency_records.c.scope,
                    idempotency_records.c.idempotency_key,
                ).limit(page_limit + 1)
            )
            .mappings()
            .all()
        )
        records = tuple(stored_idempotency_from_row(row).record for row in rows[:page_limit])
        next_cursor = (
            IdempotencyCursor(
                records[-1].created_at,
                records[-1].scope,
                records[-1].key,
            )
            if len(rows) > page_limit
            else None
        )
        return IdempotencyPage(records, next_cursor)

    @translate_consistency_storage_errors
    def complete(
        self,
        reservation: IdempotencyReservation,
        *,
        request: ConfigurationDocument,
        response_schema_version: int,
        response: RedactedDocument,
        completed_at: UtcTimestamp,
    ) -> IdempotencyRecord:
        return self._terminalize(
            status=IdempotencyStatus.COMPLETED,
            reservation=reservation,
            request=request,
            response_schema_version=response_schema_version,
            response=response,
            completed_at=completed_at,
        )

    @translate_consistency_storage_errors
    def fail(
        self,
        reservation: IdempotencyReservation,
        *,
        request: ConfigurationDocument,
        response_schema_version: int,
        response: RedactedDocument,
        completed_at: UtcTimestamp,
    ) -> IdempotencyRecord:
        return self._terminalize(
            status=IdempotencyStatus.FAILED,
            reservation=reservation,
            request=request,
            response_schema_version=response_schema_version,
            response=response,
            completed_at=completed_at,
        )

    def _terminalize(
        self,
        *,
        status: IdempotencyStatus,
        reservation: IdempotencyReservation,
        request: ConfigurationDocument,
        response_schema_version: int,
        response: RedactedDocument,
        completed_at: UtcTimestamp,
    ) -> IdempotencyRecord:
        self._require_transaction()
        capability = require_exact(reservation, IdempotencyReservation, "idempotency reservation")
        identity = _require_identity(capability.scope, capability.key)
        request_value = require_document(request, "idempotency request")
        created = require_timestamp(capability.created_at, "idempotency reservation creation time")
        expected_update = require_timestamp(
            capability.updated_at, "idempotency reservation update time"
        )
        if created != expected_update:
            raise IdempotencyConflictError("idempotency reservation is not initial")
        schema_version = positive_int(
            response_schema_version, "idempotency response schema version"
        )
        response_value = require_redacted_document(response, "idempotency response")
        response_json = encode_redacted_document(response_value, "idempotency response").text
        completed = require_timestamp(completed_at, "idempotency completion time")
        digest = request_digest(request_value)
        if capability.request_sha256 != digest:
            raise IdempotencyConflictError("idempotency reservation has a different request")
        current = self._require_stored(identity)
        if current.request_sha256 != digest:
            raise IdempotencyConflictError("idempotency identity has a different request")
        if current.record.created_at != created:
            raise IdempotencyConflictError("idempotency reservation does not match durable state")
        if current.record.status is not IdempotencyStatus.IN_PROGRESS:
            if _terminal_replay_matches(
                current.record,
                status=status,
                schema_version=schema_version,
                response_json=response_json,
                completed_at=completed,
            ):
                return current.record
            raise IdempotencyConflictError("idempotency terminal result conflicts with replay")
        if completed < current.record.updated_at:
            raise ConsistencyInvalidRequestError("idempotency completion time is not monotonic")
        row = (
            self._session.execute(
                update(idempotency_records)
                .where(
                    idempotency_records.c.scope == identity[0],
                    idempotency_records.c.idempotency_key == identity[1],
                    idempotency_records.c.request_sha256 == digest,
                    idempotency_records.c.status == IdempotencyStatus.IN_PROGRESS.value,
                    idempotency_records.c.updated_at == str(expected_update),
                )
                .values(
                    status=status.value,
                    response_schema_version=schema_version,
                    response_json=response_json,
                    updated_at=str(completed),
                    completed_at=str(completed),
                )
                .returning(*idempotency_records.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return self._classify_terminal_cas(
                identity,
                digest=digest,
                expected_updated_at=expected_update,
                status=status,
                schema_version=schema_version,
                response_json=response_json,
                completed_at=completed,
            )
        stored = stored_idempotency_from_row(row)
        if stored.request_sha256 != digest or stored.record.status is not status:
            raise ConsistencyCorruptionError("idempotency terminal result is corrupt")
        return stored.record

    def _classify_terminal_cas(
        self,
        identity: tuple[str, str],
        *,
        digest: str,
        expected_updated_at: UtcTimestamp,
        status: IdempotencyStatus,
        schema_version: int,
        response_json: str,
        completed_at: UtcTimestamp,
    ) -> IdempotencyRecord:
        current = self._require_stored(identity)
        if current.request_sha256 != digest:
            raise IdempotencyConflictError("idempotency identity has a different request")
        if current.record.status is not IdempotencyStatus.IN_PROGRESS:
            if _terminal_replay_matches(
                current.record,
                status=status,
                schema_version=schema_version,
                response_json=response_json,
                completed_at=completed_at,
            ):
                return current.record
            raise IdempotencyConflictError("idempotency terminal result conflicts with replay")
        if current.record.updated_at != expected_updated_at:
            raise ConsistencyStaleRowVersionError("idempotency update time is stale")
        raise IdempotencyConflictError("idempotency terminal update was rejected")

    def _get_stored(self, identity: tuple[str, str]) -> StoredIdempotencyRecord | None:
        row = (
            self._session.execute(
                select(idempotency_records).where(
                    idempotency_records.c.scope == identity[0],
                    idempotency_records.c.idempotency_key == identity[1],
                )
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else stored_idempotency_from_row(row)

    def _require_stored(self, identity: tuple[str, str]) -> StoredIdempotencyRecord:
        stored = self._get_stored(identity)
        if stored is None:
            raise IdempotencyConflictError("idempotency reservation does not exist")
        return stored

    def _require_transaction(self) -> None:
        if not self._session.in_transaction():
            raise ConsistencyInvalidRequestError("repository requires a caller-owned transaction")


def _require_identity(scope: object, key: object) -> tuple[str, str]:
    return (
        idempotency_scope(scope),
        portable_identity(key, "idempotency key", 128),
    )


def _reservation(stored: StoredIdempotencyRecord) -> IdempotencyReservation:
    record = stored.record
    if record.status is not IdempotencyStatus.IN_PROGRESS:
        raise ConsistencyCorruptionError("idempotency reservation result is not in progress")
    return IdempotencyReservation(
        scope=record.scope,
        key=record.key,
        request_sha256=stored.request_sha256,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def _terminal_replay_matches(
    record: IdempotencyRecord,
    *,
    status: IdempotencyStatus,
    schema_version: int,
    response_json: str,
    completed_at: UtcTimestamp,
) -> bool:
    return (
        record.status is status
        and record.response_schema_version == schema_version
        and record.response is not None
        and encode_redacted_document(record.response, "stored idempotency response").text
        == response_json
        and record.completed_at == completed_at
        and record.updated_at == completed_at
    )
