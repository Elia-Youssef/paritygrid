"""Closed reconciliation and verification commands accepted by the writer."""

from dataclasses import dataclass

from paritygrid.application.ports.consistency import ExecutionEventBatch
from paritygrid.application.ports.execution import RunRecord
from paritygrid.application.ports.reconciliation_persistence import (
    PersistedConflict,
    ReconciliationResultRecord,
    TargetVerificationRecord,
)
from paritygrid.application.ports.repair_audit import AuditEntryRecord
from paritygrid.application.ports.writer import WriterCommandKind
from paritygrid.application.writes.repairs import RepairCompanions
from paritygrid.domain.models import RunId, UtcTimestamp
from paritygrid.domain.reconciliation import ReconciliationSummary

# Version 2 adds the source-quarantine count already owned by the immutable
# reconciliation summary; version 1 rows remain append-only historical facts.
RECONCILIATION_EVENT_PAYLOAD_SCHEMA_VERSION = 2
TARGET_VERIFICATION_EVENT_PAYLOAD_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True, repr=False)
class PersistReconciliation:
    """Persist one immutable reconciliation snapshot and its conflicts."""

    run_id: RunId
    summary: ReconciliationSummary
    conflicts: tuple[PersistedConflict, ...]
    created_at: UtcTimestamp
    companions: RepairCompanions

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.PERSIST_RECONCILIATION


@dataclass(frozen=True, slots=True)
class PersistReconciliationResult:
    record: ReconciliationResultRecord
    audit: AuditEntryRecord | None
    events: ExecutionEventBatch | None
    run: RunRecord | None

    @property
    def result_kind(self) -> WriterCommandKind:
        return WriterCommandKind.PERSIST_RECONCILIATION


@dataclass(frozen=True, slots=True, repr=False)
class RecordTargetVerification:
    """Persist one immutable independently observed target-state verification."""

    run_id: RunId
    verification: TargetVerificationRecord
    companions: RepairCompanions

    @property
    def kind(self) -> WriterCommandKind:
        return WriterCommandKind.RECORD_TARGET_VERIFICATION


@dataclass(frozen=True, slots=True)
class RecordTargetVerificationResult:
    record: TargetVerificationRecord
    audit: AuditEntryRecord | None
    events: ExecutionEventBatch | None
    run: RunRecord | None

    @property
    def result_kind(self) -> WriterCommandKind:
        return WriterCommandKind.RECORD_TARGET_VERIFICATION


__all__ = [
    "RECONCILIATION_EVENT_PAYLOAD_SCHEMA_VERSION",
    "TARGET_VERIFICATION_EVENT_PAYLOAD_SCHEMA_VERSION",
    "PersistReconciliation",
    "PersistReconciliationResult",
    "RecordTargetVerification",
    "RecordTargetVerificationResult",
]
