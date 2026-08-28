"""SQLAlchemy adapter for immutable reconciliation and verification facts."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from functools import wraps
from typing import cast

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence.repositories.common import MAX_CANONICAL_DOCUMENT_BYTES
from paritygrid.adapters.persistence.repositories.repair_audit_common import (
    stored_fingerprint,
    stored_identifier,
    stored_timestamp,
)
from paritygrid.adapters.persistence.schema import (
    reconciliation_conflicts,
    reconciliation_summaries,
    runs,
    target_state_verifications,
)
from paritygrid.adapters.persistence.values import CanonicalStorageJson, StoragePrimitive
from paritygrid.adapters.persistence.writer.contention import is_sqlite_contention
from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.application.ports.reconciliation_persistence import (
    MAX_RECONCILIATION_CONFLICTS,
    MAX_VERIFICATION_DETAIL_BYTES,
    PersistedConflict,
    ReconciliationCorruptionError,
    ReconciliationInvalidRequestError,
    ReconciliationRecordNotFoundError,
    ReconciliationResultConflictError,
    ReconciliationResultRecord,
    ReconciliationStorageError,
    ReconciliationStorageUnavailableError,
    ReconciliationSummaryRecord,
    TargetVerificationConflictError,
    TargetVerificationCorruptionError,
    TargetVerificationInvalidRequestError,
    TargetVerificationRecord,
    TargetVerificationStorageError,
    TargetVerificationStorageUnavailableError,
    TargetVerificationVerdict,
)
from paritygrid.application.ports.writer import PersistenceContentionError
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import (
    ConflictId,
    RepairPlanId,
    RunId,
    TargetVerificationId,
    UtcTimestamp,
)
from paritygrid.domain.reconciliation import (
    FieldDifference,
    FieldDifferenceKind,
    ReconciliationClassification,
    ReconciliationSummary,
    SuggestedResolution,
)

_COUNT_COLUMN: Mapping[ReconciliationClassification, str] = {
    ReconciliationClassification.MATCH: "match_count",
    ReconciliationClassification.MISSING_FROM_TARGET: "missing_from_target_count",
    ReconciliationClassification.MISSING_FROM_SOURCE: "missing_from_source_count",
    ReconciliationClassification.FIELD_MISMATCH: "field_mismatch_count",
    ReconciliationClassification.DUPLICATE_SOURCE: "duplicate_source_count",
    ReconciliationClassification.DUPLICATE_TARGET: "duplicate_target_count",
    ReconciliationClassification.DUPLICATE_BOTH: "duplicate_both_count",
}


def _translated[**P, R](
    storage_error: type[Exception], unavailable_error: type[Exception]
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Build an error translator bound to one repository failure hierarchy."""

    def decorator(operation: Callable[P, R]) -> Callable[P, R]:
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
                raise unavailable_error("Storage is unavailable.") from None
            raise storage_error("Storage operation failed.") from None

        return translated

    return decorator


class SqlAlchemyReconciliationResultRepository:
    """Persist immutable reconciliation snapshots without owning the transaction."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @_translated(ReconciliationStorageError, ReconciliationStorageUnavailableError)
    def persist(
        self,
        *,
        run_id: RunId,
        summary: ReconciliationSummary,
        conflicts: Sequence[PersistedConflict],
        created_at: UtcTimestamp,
    ) -> ReconciliationResultRecord:
        self._require_transaction()
        identity = _require_exact(run_id, RunId, "reconciliation run identifier")
        exact_summary = _require_exact(summary, ReconciliationSummary, "reconciliation summary")
        timestamp = _require_exact(created_at, UtcTimestamp, "reconciliation snapshot time")
        if not isinstance(conflicts, tuple):
            raise ReconciliationInvalidRequestError("reconciliation conflicts must be a tuple")
        items = tuple(conflicts)
        if len(items) > MAX_RECONCILIATION_CONFLICTS:
            raise ReconciliationInvalidRequestError("reconciliation conflicts exceed the bound")
        if any(type(item) is not PersistedConflict for item in items):
            raise ReconciliationInvalidRequestError(
                "reconciliation conflicts must contain PersistedConflict values"
            )
        keys = [conflict.canonical_key for conflict in items]
        if keys != sorted(keys) or len(set(keys)) != len(keys):
            raise ReconciliationInvalidRequestError(
                "reconciliation conflicts must use sorted unique canonical keys"
            )
        for conflict in items:
            if not conflict.source_references and not conflict.target_references:
                raise ReconciliationInvalidRequestError(
                    "reconciliation conflict requires at least one member record"
                )
            if conflict.created_at != timestamp:
                raise ReconciliationInvalidRequestError(
                    "reconciliation conflicts must share the snapshot time"
                )
        counts = dict(exact_summary.counts.by_classification)
        counted: dict[ReconciliationClassification, int] = {}
        for conflict in items:
            counted[conflict.classification] = counted.get(conflict.classification, 0) + 1
        # Matches have no conflict rows; every other classification must be
        # covered exactly by its persisted conflict count.
        for classification in ReconciliationClassification:
            expected_conflicts = (
                0
                if classification is ReconciliationClassification.MATCH
                else counts[classification]
            )
            if counted.get(classification, 0) != expected_conflicts:
                raise ReconciliationInvalidRequestError(
                    "reconciliation conflicts must cover the summary classification counts"
                )
        if sum(counts.values()) != exact_summary.counts.canonical_key_count:
            raise ReconciliationInvalidRequestError(
                "reconciliation classification counts must cover every canonical key"
            )
        self._require_terminal_run(identity)
        inserted = self._session.execute(
            sqlite_insert(reconciliation_summaries)
            .values(
                run_id=identity.value,
                **{
                    _COUNT_COLUMN[classification]: counts[classification]
                    for classification in ReconciliationClassification
                },
                total_count=exact_summary.counts.canonical_key_count,
                source_fingerprint=exact_summary.source_input_identity,
                target_fingerprint=exact_summary.target_input_identity,
                reconciliation_fingerprint=exact_summary.fingerprint.value,
                analytical_query_version=exact_summary.analytical_query_version,
                created_at=str(timestamp),
            )
            .on_conflict_do_nothing()
            .returning(reconciliation_summaries.c.run_id)
        ).scalar_one_or_none()
        if inserted is None:
            return self._classify_replay(identity, exact_summary, items, timestamp)
        for conflict in items:
            inserted_conflict = self._session.execute(
                sqlite_insert(reconciliation_conflicts)
                .values(
                    conflict_id=conflict.conflict_id.value,
                    run_id=identity.value,
                    canonical_key=conflict.canonical_key,
                    classification=conflict.classification.value,
                    source_references_json=_render_references(conflict.source_references).text,
                    target_reference_json=(
                        None
                        if not conflict.target_references
                        else _render_target_reference(conflict.target_references).text
                    ),
                    field_differences_json=_render_differences(conflict.differences).text,
                    suggested_resolution=(
                        None
                        if conflict.suggested_resolution is None
                        else conflict.suggested_resolution.value
                    ),
                    created_at=str(conflict.created_at),
                )
                .on_conflict_do_nothing()
                .returning(reconciliation_conflicts.c.conflict_id)
            ).scalar_one_or_none()
            if inserted_conflict is None:
                raise ReconciliationResultConflictError(
                    "reconciliation conflict identity already exists with different content"
                )
        return self._load_result(identity, timestamp)

    @_translated(ReconciliationStorageError, ReconciliationStorageUnavailableError)
    def get_summary(self, run_id: RunId) -> ReconciliationSummaryRecord | None:
        self._require_transaction()
        identity = _require_exact(run_id, RunId, "reconciliation run identifier")
        row = self._summary_row(identity)
        if row is None:
            return None
        return self._summary_from_row(cast(Mapping[str, object], row))

    @_translated(ReconciliationStorageError, ReconciliationStorageUnavailableError)
    def get_result(self, run_id: RunId) -> ReconciliationResultRecord | None:
        self._require_transaction()
        identity = _require_exact(run_id, RunId, "reconciliation run identifier")
        row = self._summary_row(identity)
        if row is None:
            return None
        timestamp = stored_timestamp(row["created_at"], "reconciliation summary time")
        return self._load_result(identity, timestamp)

    def _summary_row(self, identity: RunId) -> RowMapping | None:
        return (
            self._session.execute(
                select(reconciliation_summaries).where(
                    reconciliation_summaries.c.run_id == identity.value
                )
            )
            .mappings()
            .one_or_none()
        )

    def _load_result(self, identity: RunId, timestamp: UtcTimestamp) -> ReconciliationResultRecord:
        row = self._summary_row(identity)
        if row is None:
            raise ReconciliationCorruptionError(
                "reconciliation summary disappeared mid-transaction"
            )
        summary = self._summary_from_row(cast(Mapping[str, object], row))
        conflict_rows = tuple(
            self._session.execute(
                select(reconciliation_conflicts)
                .where(reconciliation_conflicts.c.run_id == identity.value)
                .order_by(reconciliation_conflicts.c.canonical_key)
            ).mappings()
        )
        conflicts = tuple(
            _conflict_from_row(cast(Mapping[str, object], item)) for item in conflict_rows
        )
        try:
            return ReconciliationResultRecord(summary=summary, conflicts=conflicts)
        except ValueError as error:
            raise ReconciliationCorruptionError(str(error)) from error

    def _summary_from_row(self, row: Mapping[str, object]) -> ReconciliationSummaryRecord:
        counts = tuple(
            (
                classification,
                _stored_nonnegative_int(row[column], f"reconciliation {column}"),
            )
            for classification, column in sorted(
                _COUNT_COLUMN.items(), key=lambda item: item[0].value
            )
        )
        try:
            return ReconciliationSummaryRecord(
                run_id=stored_identifier(row["run_id"], RunId, "reconciliation run identifier"),
                reconciliation_fingerprint=stored_fingerprint(
                    row["reconciliation_fingerprint"],
                    "reconciliation summary fingerprint",
                ),
                source_fingerprint=stored_fingerprint(
                    row["source_fingerprint"], "reconciliation source fingerprint"
                ),
                target_fingerprint=stored_fingerprint(
                    row["target_fingerprint"], "reconciliation target fingerprint"
                ),
                counts=counts,
                total_count=_stored_nonnegative_int(row["total_count"], "reconciliation total"),
                analytical_query_version=_stored_positive_int(
                    row["analytical_query_version"], "reconciliation analytical query version"
                ),
                created_at=stored_timestamp(row["created_at"], "reconciliation summary time"),
            )
        except (TypeError, ValueError) as error:
            raise ReconciliationCorruptionError("reconciliation summary row is corrupt") from error

    def _classify_replay(
        self,
        identity: RunId,
        summary: ReconciliationSummary,
        conflicts: tuple[PersistedConflict, ...],
        timestamp: UtcTimestamp,
    ) -> ReconciliationResultRecord:
        stored = self._load_result(identity, timestamp)
        if (
            stored.summary.reconciliation_fingerprint.value != summary.fingerprint.value
            or stored.summary.source_fingerprint.value != summary.source_input_identity
            or stored.summary.target_fingerprint.value != summary.target_input_identity
            or stored.summary.created_at != timestamp
            or stored.summary.total_count != summary.counts.canonical_key_count
            or stored.summary.analytical_query_version != summary.analytical_query_version
            or stored.summary.counts
            != tuple(sorted(summary.counts.by_classification, key=lambda item: item[0].value))
            or len(stored.conflicts) != len(conflicts)
            or any(
                _renderable_conflict(stored_conflict) != _renderable_conflict(requested)
                for stored_conflict, requested in zip(stored.conflicts, conflicts, strict=True)
            )
        ):
            raise ReconciliationResultConflictError(
                "reconciliation result replay differs from durable state"
            )
        return stored

    def _require_terminal_run(self, identity: RunId) -> None:
        row = self._session.execute(
            select(runs.c.state).where(runs.c.run_id == identity.value)
        ).scalar_one_or_none()
        if row is None:
            raise ReconciliationRecordNotFoundError("reconciliation run does not exist")
        if type(row) is not str:
            raise ReconciliationCorruptionError("reconciliation run state is corrupt")
        try:
            state = RunState(row)
        except ValueError as error:
            raise ReconciliationCorruptionError("reconciliation run state is corrupt") from error
        if state not in {RunState.SUCCEEDED, RunState.PARTIALLY_SUCCEEDED}:
            raise ReconciliationInvalidRequestError(
                "reconciliation results require a completed run"
            )

    def _require_transaction(self) -> None:
        if not self._session.in_transaction():
            raise ReconciliationInvalidRequestError(
                "reconciliation repository requires a caller-owned transaction"
            )


class SqlAlchemyTargetVerificationRepository:
    """Persist immutable target-state verification facts without owning the transaction."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @_translated(TargetVerificationStorageError, TargetVerificationStorageUnavailableError)
    def record(self, verification: TargetVerificationRecord) -> TargetVerificationRecord:
        self._require_transaction()
        exact = _require_exact(verification, TargetVerificationRecord, "target verification")
        detail = _encode_detail(exact.detail)
        inserted = self._session.execute(
            sqlite_insert(target_state_verifications)
            .values(
                verification_id=exact.verification_id.value,
                run_id=exact.run_id.value,
                repair_plan_id=(
                    None if exact.repair_plan_id is None else exact.repair_plan_id.value
                ),
                reconciliation_fingerprint=exact.reconciliation_fingerprint.value,
                plan_content_fingerprint=(
                    None
                    if exact.plan_content_fingerprint is None
                    else exact.plan_content_fingerprint.value
                ),
                observed_fingerprint=exact.observed_fingerprint.value,
                observed_fingerprint_version=exact.observed_fingerprint_version,
                expected_fingerprint=exact.expected_fingerprint.value,
                verdict=exact.verdict.value,
                observed_record_count=exact.observed_record_count,
                expected_record_count=exact.expected_record_count,
                observed_target_version=exact.observed_target_version,
                observed_at=str(exact.observed_at),
                detail_json=detail.text,
            )
            .on_conflict_do_nothing()
            .returning(target_state_verifications.c.verification_id)
        ).scalar_one_or_none()
        if inserted is None:
            stored = self.get(exact.verification_id)
            if stored is None:
                raise TargetVerificationCorruptionError("target verification replay is missing")
            if (
                stored.run_id != exact.run_id
                or stored.repair_plan_id != exact.repair_plan_id
                or stored.reconciliation_fingerprint != exact.reconciliation_fingerprint
                or stored.plan_content_fingerprint != exact.plan_content_fingerprint
                or stored.observed_fingerprint != exact.observed_fingerprint
                or stored.observed_fingerprint_version != exact.observed_fingerprint_version
                or stored.expected_fingerprint != exact.expected_fingerprint
                or stored.verdict is not exact.verdict
                or stored.observed_record_count != exact.observed_record_count
                or stored.expected_record_count != exact.expected_record_count
                or stored.observed_target_version != exact.observed_target_version
                or stored.observed_at != exact.observed_at
                or _encode_detail(stored.detail).text != detail.text
            ):
                raise TargetVerificationConflictError(
                    "target verification replay differs from durable state"
                )
            return stored
        return self.get(exact.verification_id) or exact

    @_translated(TargetVerificationStorageError, TargetVerificationStorageUnavailableError)
    def get(self, verification_id: TargetVerificationId) -> TargetVerificationRecord | None:
        self._require_transaction()
        identity = _require_exact(
            verification_id, TargetVerificationId, "target verification identity"
        )
        row = self._row_by_identity(identity)
        return None if row is None else _verification_from_row(cast(Mapping[str, object], row))

    @_translated(TargetVerificationStorageError, TargetVerificationStorageUnavailableError)
    def latest_for_run(self, run_id: RunId) -> TargetVerificationRecord | None:
        self._require_transaction()
        identity = _require_exact(run_id, RunId, "target verification run identifier")
        row = self._latest_row_for_run(identity)
        return None if row is None else _verification_from_row(cast(Mapping[str, object], row))

    def _row_by_identity(self, identity: TargetVerificationId) -> RowMapping | None:
        return (
            self._session.execute(
                select(target_state_verifications)
                .where(target_state_verifications.c.verification_id == identity.value)
                .limit(1)
            )
            .mappings()
            .one_or_none()
        )

    def _latest_row_for_run(self, identity: RunId) -> RowMapping | None:
        return (
            self._session.execute(
                select(target_state_verifications)
                .where(target_state_verifications.c.run_id == identity.value)
                .order_by(
                    target_state_verifications.c.observed_at.desc(),
                    target_state_verifications.c.verification_id.desc(),
                )
                .limit(1)
            )
            .mappings()
            .one_or_none()
        )

    def _require_transaction(self) -> None:
        if not self._session.in_transaction():
            raise TargetVerificationInvalidRequestError(
                "target verification repository requires a caller-owned transaction"
            )


def _renderable_conflict(conflict: PersistedConflict) -> tuple[object, ...]:
    return (
        conflict.conflict_id.value,
        conflict.canonical_key,
        conflict.classification.value,
        _render_references(conflict.source_references).text,
        (
            None
            if not conflict.target_references
            else _render_target_reference(conflict.target_references).text
        ),
        _render_differences(conflict.differences).text,
        None if conflict.suggested_resolution is None else conflict.suggested_resolution.value,
        str(conflict.created_at),
    )


def _require_exact[T](value: object, expected: type[T], subject: str) -> T:
    if type(value) is not expected:
        raise TypeError(f"{subject} must use {expected.__name__}")
    return value


def _stored_nonnegative_int(value: object, subject: str) -> int:
    if type(value) is not int or value < 0:
        raise ReconciliationCorruptionError(f"{subject} is corrupt")
    return value


def _stored_positive_int(value: object, subject: str) -> int:
    if type(value) is not int or not 1 <= value <= 2_147_483_647:
        raise ReconciliationCorruptionError(f"{subject} is corrupt")
    return value


def _render_references(references: tuple[tuple[int, str], ...]) -> CanonicalStorageJson:
    return CanonicalStorageJson.encode(
        cast(
            StoragePrimitive,
            [[position, record_key] for position, record_key in references],
        )
    )


def _render_target_reference(references: tuple[tuple[int, str], ...]) -> CanonicalStorageJson:
    return CanonicalStorageJson.encode(
        cast(
            StoragePrimitive,
            {
                "positions": [position for position, _record_key in references],
                "record_keys": [record_key for _position, record_key in references],
            },
        )
    )


def _render_differences(differences: tuple[FieldDifference, ...]) -> CanonicalStorageJson:
    return CanonicalStorageJson.encode(
        cast(
            StoragePrimitive,
            [
                {
                    "field": difference.field,
                    "kind": difference.kind.value,
                    "source": difference.source_text,
                    "target": difference.target_text,
                }
                for difference in differences
            ],
        )
    )


def _decode_references(value: object) -> tuple[tuple[int, str], ...]:
    if type(value) is not str:
        raise ReconciliationCorruptionError("reconciliation conflict references are corrupt")
    try:
        decoded = CanonicalStorageJson(value).decode()
    except (TypeError, ValueError) as error:
        raise ReconciliationCorruptionError(
            "reconciliation conflict references are corrupt"
        ) from error
    if type(decoded) is not list:
        raise ReconciliationCorruptionError("reconciliation conflict references are corrupt")
    references: list[tuple[int, str]] = []
    for item in cast(list[object], decoded):
        if type(item) is not list:
            raise ReconciliationCorruptionError("reconciliation conflict references are corrupt")
        member = cast(list[object], item)
        if len(member) != 2:
            raise ReconciliationCorruptionError("reconciliation conflict references is corrupt")
        position = _stored_position(member[0])
        record_key = _stored_record_key(member[1])
        references.append((position, record_key))
    return tuple(references)


def _decode_target_reference(value: object) -> tuple[tuple[int, str], ...]:
    if value is None:
        return ()
    if type(value) is not str:
        raise ReconciliationCorruptionError("reconciliation conflict target reference is corrupt")
    try:
        decoded = CanonicalStorageJson(value).decode()
    except (TypeError, ValueError) as error:
        raise ReconciliationCorruptionError(
            "reconciliation conflict target reference is corrupt"
        ) from error
    if type(decoded) is not dict or set(decoded) != {"positions", "record_keys"}:
        raise ReconciliationCorruptionError("reconciliation conflict target reference is corrupt")
    mapping = cast(dict[str, object], decoded)
    positions_value = mapping["positions"]
    record_keys_value = mapping["record_keys"]
    positions = cast(list[object], positions_value)
    record_keys = cast(list[object], record_keys_value)
    if type(positions_value) is not list or type(record_keys_value) is not list:
        raise ReconciliationCorruptionError("reconciliation conflict target reference is corrupt")
    if len(positions) != len(record_keys):
        raise ReconciliationCorruptionError("reconciliation conflict target reference is corrupt")
    references: list[tuple[int, str]] = []
    for position, record_key in zip(positions, record_keys, strict=True):
        references.append((_stored_position(position), _stored_record_key(record_key)))
    return tuple(references)


def _decode_differences(value: object) -> tuple[FieldDifference, ...]:
    if type(value) is not str:
        raise ReconciliationCorruptionError("reconciliation conflict differences are corrupt")
    differences: list[FieldDifference] = []
    try:
        for item in _decode_json_array(value):
            if type(item) is not dict:
                raise ValueError
            mapping = cast(dict[str, object], item)
            if set(mapping) != {"field", "kind", "source", "target"}:
                raise ValueError
            field = _stored_text(mapping["field"], "field difference field")
            kind = _stored_text(mapping["kind"], "field difference kind")
            source_text = _stored_text(mapping["source"], "field difference source")
            target_text = _stored_text(mapping["target"], "field difference target")
            differences.append(
                FieldDifference(
                    field=field,
                    kind=FieldDifferenceKind(kind),
                    source_text=source_text,
                    target_text=target_text,
                )
            )
    except (TypeError, ValueError) as error:
        raise ReconciliationCorruptionError(
            "reconciliation conflict differences are corrupt"
        ) from error
    return tuple(differences)


def _decode_json_array(value: str) -> list[object]:
    try:
        decoded = CanonicalStorageJson(value).decode()
    except (TypeError, ValueError) as error:
        raise ReconciliationCorruptionError("stored reconciliation JSON is corrupt") from error
    if type(decoded) is not list:
        raise ReconciliationCorruptionError("stored reconciliation JSON is corrupt")
    return cast(list[object], decoded)


def _conflict_from_row(row: Mapping[str, object]) -> PersistedConflict:
    classification_value = row["classification"]
    if type(classification_value) is not str:
        raise ReconciliationCorruptionError("reconciliation conflict classification is corrupt")
    suggested_value = row["suggested_resolution"]
    try:
        classification = ReconciliationClassification(classification_value)
        suggested = None if suggested_value is None else SuggestedResolution(suggested_value)
        return PersistedConflict(
            conflict_id=stored_identifier(
                row["conflict_id"], ConflictId, "reconciliation conflict identifier"
            ),
            canonical_key=_stored_key(row["canonical_key"]),
            classification=classification,
            source_references=_decode_references(row["source_references_json"]),
            target_references=_decode_target_reference(row["target_reference_json"]),
            differences=_decode_differences(row["field_differences_json"]),
            suggested_resolution=suggested,
            created_at=stored_timestamp(row["created_at"], "reconciliation conflict time"),
        )
    except (TypeError, ValueError) as error:
        if isinstance(error, ReconciliationCorruptionError):
            raise
        raise ReconciliationCorruptionError("reconciliation conflict row is corrupt") from error


def _stored_position(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ReconciliationCorruptionError("reconciliation conflict reference is corrupt")
    return value


def _stored_record_key(value: object) -> str:
    if type(value) is not str or not 1 <= len(value) <= 128:
        raise ReconciliationCorruptionError("reconciliation conflict reference is corrupt")
    return value


def _stored_text(value: object, subject: str) -> str:
    if type(value) is not str:
        raise ReconciliationCorruptionError(f"stored {subject} is corrupt")
    return value


def _stored_key(value: object) -> str:
    if type(value) is not str or not 1 <= len(value) <= 64:
        raise ReconciliationCorruptionError("reconciliation canonical key is corrupt")
    return value


def _verification_from_row(row: Mapping[str, object]) -> TargetVerificationRecord:
    verdict_value = row["verdict"]
    plan_value = row["repair_plan_id"]
    if not isinstance(verdict_value, str):
        raise TargetVerificationCorruptionError("target verification verdict is corrupt")
    try:
        return TargetVerificationRecord(
            verification_id=stored_identifier(
                row["verification_id"], TargetVerificationId, "target verification identity"
            ),
            run_id=stored_identifier(row["run_id"], RunId, "target verification run"),
            repair_plan_id=(None if not isinstance(plan_value, str) else RepairPlanId(plan_value)),
            reconciliation_fingerprint=stored_fingerprint(
                row["reconciliation_fingerprint"],
                "target verification reconciliation fingerprint",
            ),
            plan_content_fingerprint=(
                None
                if row["plan_content_fingerprint"] is None
                else stored_fingerprint(
                    row["plan_content_fingerprint"],
                    "target verification plan content fingerprint",
                )
            ),
            observed_fingerprint=stored_fingerprint(
                row["observed_fingerprint"], "target verification observed fingerprint"
            ),
            observed_fingerprint_version=_stored_positive_int(
                row["observed_fingerprint_version"],
                "target verification fingerprint version",
            ),
            expected_fingerprint=stored_fingerprint(
                row["expected_fingerprint"], "target verification expected fingerprint"
            ),
            verdict=TargetVerificationVerdict(verdict_value),
            observed_record_count=_verification_count(
                row["observed_record_count"], "target verification observed count"
            ),
            expected_record_count=_verification_count(
                row["expected_record_count"], "target verification expected count"
            ),
            observed_target_version=_verification_count(
                row["observed_target_version"], "target verification target version"
            ),
            observed_at=stored_timestamp(row["observed_at"], "target verification time"),
            detail=_decode_detail(row["detail_json"]),
        )
    except (TypeError, ValueError) as error:
        if isinstance(error, TargetVerificationCorruptionError):
            raise
        raise TargetVerificationCorruptionError("target verification row is corrupt") from error


def _verification_count(value: object, subject: str) -> int:
    if type(value) is not int or value < 0:
        raise TargetVerificationCorruptionError(f"{subject} is corrupt")
    return value


def _encode_detail(document: RedactedDocument) -> CanonicalStorageJson:
    try:
        encoded = CanonicalStorageJson.encode(cast(StoragePrimitive, document.to_mapping()))
    except (TypeError, ValueError) as error:
        raise TargetVerificationInvalidRequestError(
            "target verification detail is invalid"
        ) from error
    if len(encoded.text.encode("utf-8")) > min(
        MAX_VERIFICATION_DETAIL_BYTES, MAX_CANONICAL_DOCUMENT_BYTES
    ):
        raise TargetVerificationInvalidRequestError(
            "target verification detail exceeds the supported encoded size"
        )
    return encoded


def _decode_detail(value: object) -> RedactedDocument:
    if type(value) is not str:
        raise TargetVerificationCorruptionError("target verification detail is corrupt")
    try:
        decoded = CanonicalStorageJson(value).decode()
        if type(decoded) is not dict:
            raise ValueError
        return RedactedDocument.from_mapping(cast(dict[str, object], decoded))
    except (TypeError, ValueError, RecursionError) as error:
        raise TargetVerificationCorruptionError("target verification detail is corrupt") from error


__all__ = [
    "SqlAlchemyReconciliationResultRepository",
    "SqlAlchemyTargetVerificationRepository",
]
