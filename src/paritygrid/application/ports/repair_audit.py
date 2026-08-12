"""Dependency-neutral contracts for repair and security audit persistence."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, Self, cast

from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.domain.models import (
    ConflictId,
    InventoryAttributes,
    InventoryRecord,
    Money,
    RepairActionId,
    RepairPlanId,
    RunId,
    StateFingerprint,
    UtcTimestamp,
)
from paritygrid.domain.reconciliation import FieldMismatch, differences_between
from paritygrid.domain.repair import RepairAction, RepairActionKind, RepairPlan

MAX_REPAIR_PAGE_SIZE = 100
MAX_AUDIT_PAGE_SIZE = 100
MAX_PERSISTED_INTEGER = 2_147_483_647


class RepairRepositoryError(Exception):
    """Base class for stable repair repository failures."""


class RepairInvalidRequestError(RepairRepositoryError):
    """A request violates the public repair contract."""


class RepairRecordNotFoundError(RepairRepositoryError):
    """A required repair record does not exist."""


class RepairDuplicateError(RepairRepositoryError):
    """A repair identity is already assigned to another record."""


class RepairStaleRowVersionError(RepairRepositoryError):
    """An optimistic repair-plan row version no longer matches."""


class RepairStateConflictError(RepairRepositoryError):
    """Current durable repair state rejects the requested operation."""


class RepairPlanContentConflictError(RepairStateConflictError):
    """A repair-plan identity or logical content conflicts with durable state."""


class RepairApprovalConflictError(RepairStateConflictError):
    """An approval retry differs from the immutable approved fact."""


class RepairApplicationConflictError(RepairStateConflictError):
    """A repair application reservation or result no longer matches."""


class RepairCorruptionError(RepairRepositoryError):
    """Persisted repair data failed strict boundary validation."""


class RepairStorageError(RepairRepositoryError):
    """An unexpected persistence failure prevented a repair operation."""


class RepairStorageUnavailableError(RepairStorageError):
    """Repair storage was unavailable for the requested operation."""


class AuditRepositoryError(Exception):
    """Base class for stable audit repository failures."""


class AuditInvalidRequestError(AuditRepositoryError):
    """A request violates the public audit contract."""


class AuditSequenceConflictError(AuditRepositoryError):
    """The audit sequence cannot advance within the supported range."""


class AuditCorruptionError(AuditRepositoryError):
    """Persisted audit data failed strict boundary validation."""


class AuditStorageError(AuditRepositoryError):
    """An unexpected persistence failure prevented an audit operation."""


class AuditStorageUnavailableError(AuditStorageError):
    """Audit storage was unavailable for the requested operation."""


class RepairPlanStatus(StrEnum):
    """Application-facing lifecycle of a repair plan."""

    PROPOSED = "proposed"
    APPROVED = "approved"
    APPLYING = "applying"
    APPLIED = "applied"
    REJECTED = "rejected"
    FAILED = "failed"


class RepairActionStatus(StrEnum):
    """Application-facing lifecycle of one repair effect."""

    PENDING = "pending"
    APPLIED = "applied"
    FAILED = "failed"


class RepairApplicationBeginDisposition(StrEnum):
    """Outcome of beginning or observing repair application."""

    STARTED = "started"
    IN_PROGRESS_REPLAY = "in_progress_replay"
    APPLIED_REPLAY = "applied_replay"
    FAILED_REPLAY = "failed_replay"


@dataclass(frozen=True, slots=True)
class InventoryEffect:
    """Inventory business state without observation provenance."""

    sku: str
    name: str
    quantity: int
    unit_price: Money
    updated_at: UtcTimestamp
    attributes: InventoryAttributes

    def __post_init__(self) -> None:
        candidate = _effect_record(self)
        if (
            candidate.sku != self.sku
            or candidate.name != self.name
            or candidate.quantity != self.quantity
            or candidate.unit_price != self.unit_price
            or candidate.updated_at != self.updated_at
            or candidate.attributes != self.attributes
        ):
            raise ValueError("inventory effect must already use canonical values")

    @classmethod
    def from_record(cls, record: InventoryRecord) -> Self:
        """Project a trusted observation to its repairable business effect."""
        if type(record) is not InventoryRecord:
            raise TypeError("inventory effect source must be an InventoryRecord")
        return cls(
            sku=record.sku,
            name=record.name,
            quantity=record.quantity,
            unit_price=record.unit_price,
            updated_at=record.updated_at,
            attributes=record.attributes,
        )


@dataclass(frozen=True, slots=True, repr=False)
class RepairActionEffect:
    """One immutable, provenance-free repair effect and its derived evidence."""

    action_id: RepairActionId
    conflict_id: ConflictId
    reconciliation_fingerprint: StateFingerprint
    kind: RepairActionKind
    proposed: InventoryEffect
    expected_target: InventoryEffect | None
    mismatches: tuple[FieldMismatch, ...]

    def __post_init__(self) -> None:
        if type(self.action_id) is not RepairActionId:
            raise TypeError("repair action effect requires RepairActionId")
        if type(self.conflict_id) is not ConflictId:
            raise TypeError("repair action effect requires ConflictId")
        if type(self.reconciliation_fingerprint) is not StateFingerprint:
            raise TypeError("repair action effect requires StateFingerprint")
        if type(self.kind) is not RepairActionKind:
            raise TypeError("repair action effect requires RepairActionKind")
        if type(self.proposed) is not InventoryEffect:
            raise TypeError("repair action effect requires InventoryEffect")
        mismatch_value = cast(object, self.mismatches)
        if not isinstance(mismatch_value, tuple) or any(
            type(item) is not FieldMismatch for item in cast(tuple[object, ...], mismatch_value)
        ):
            raise TypeError("repair action mismatches must contain FieldMismatch values")
        if self.kind is RepairActionKind.CREATE_TARGET:
            if self.expected_target is not None or self.mismatches:
                raise ValueError("create repair effect requires an absent target")
            return
        if type(self.expected_target) is not InventoryEffect:
            raise TypeError("update repair effect requires an expected InventoryEffect")
        derived = differences_between(
            _effect_record(self.proposed), _effect_record(self.expected_target)
        )
        if not derived or self.mismatches != derived:
            raise ValueError("repair action mismatch evidence is not canonical")

    @classmethod
    def from_action(cls, action: RepairAction) -> Self:
        """Project an exact domain action without retaining observation provenance."""
        if type(action) is not RepairAction:
            raise TypeError("repair effect source must be a RepairAction")
        return cls(
            action_id=action.action_id,
            conflict_id=action.conflict_id,
            reconciliation_fingerprint=action.state_fingerprint,
            kind=action.kind,
            proposed=InventoryEffect.from_record(action.proposed_record),
            expected_target=(
                None
                if action.expected_target_record is None
                else InventoryEffect.from_record(action.expected_target_record)
            ),
            mismatches=action.mismatches,
        )

    def __repr__(self) -> str:
        return (
            "RepairActionEffect("
            f"action_id={self.action_id!r}, conflict_id={self.conflict_id!r}, "
            f"kind={self.kind!r}, reconciliation_fingerprint=<redacted>, "
            "proposed=<redacted>, expected_target=<redacted>, mismatches=<redacted>)"
        )


@dataclass(frozen=True, slots=True, repr=False)
class RepairActionKeyMap:
    """A complete immutable mapping from action identities to external effect keys."""

    items: tuple[tuple[RepairActionId, str], ...]

    def __post_init__(self) -> None:
        value = cast(object, self.items)
        if not isinstance(value, tuple):
            raise TypeError("repair action keys must be a tuple")
        pairs = cast(tuple[object, ...], value)
        parsed: list[tuple[RepairActionId, str]] = []
        for pair in pairs:
            if type(pair) is not tuple:
                raise TypeError("repair action keys must contain identifier-key pairs")
            pair_items = cast(tuple[object, ...], pair)
            if len(pair_items) != 2:
                raise TypeError("repair action keys must contain identifier-key pairs")
            action_id, key = pair_items
            if type(action_id) is not RepairActionId or type(key) is not str:
                raise TypeError("repair action keys contain an invalid pair")
            parsed.append((action_id, key))
        if len({item[0] for item in parsed}) != len(parsed):
            raise ValueError("repair action identities must be unique")
        if len({item[1] for item in parsed}) != len(parsed):
            raise ValueError("external repair keys must be unique")
        object.__setattr__(self, "items", tuple(sorted(parsed, key=lambda item: item[0].value)))

    @classmethod
    def from_mapping(cls, value: Mapping[RepairActionId, str]) -> Self:
        """Defensively copy action effect keys into deterministic order."""
        value_object = cast(object, value)
        if not isinstance(value_object, Mapping):
            raise TypeError("repair action keys must be a mapping")
        items = cast(Mapping[RepairActionId, str], value_object).items()
        return cls(tuple(items))

    def to_mapping(self) -> dict[RepairActionId, str]:
        """Return a detached mapping of action identities to external keys."""
        return dict(self.items)

    def __repr__(self) -> str:
        return f"RepairActionKeyMap(count={len(self.items)}, keys=<redacted>)"


@dataclass(frozen=True, slots=True, repr=False)
class RepairApplicationResult:
    """A versioned, redaction-checked result from one external repair effect."""

    schema_version: int
    detail: RedactedDocument

    def __post_init__(self) -> None:
        value = cast(object, self.schema_version)
        if type(value) is not int:
            raise TypeError("repair result schema version must be an integer")
        if not 1 <= self.schema_version <= MAX_PERSISTED_INTEGER:
            raise ValueError("repair result schema version is outside the supported range")
        if type(self.detail) is not RedactedDocument:
            raise TypeError("repair result detail must be a RedactedDocument")

    def __repr__(self) -> str:
        return f"RepairApplicationResult(schema_version={self.schema_version!r}, detail=<redacted>)"


@dataclass(frozen=True, slots=True, repr=False)
class RepairApprovalRecord:
    repair_plan_id: RepairPlanId
    reconciliation_fingerprint: StateFingerprint
    approved_by: str
    approved_at: UtcTimestamp
    correlation_id: str
    schema_version: int
    detail: RedactedDocument

    def __repr__(self) -> str:
        return (
            "RepairApprovalRecord("
            f"repair_plan_id={self.repair_plan_id!r}, approved_at={self.approved_at!r}, "
            f"schema_version={self.schema_version!r}, approval=<redacted>)"
        )


@dataclass(frozen=True, slots=True, repr=False)
class RepairPlanRecord:
    repair_plan_id: RepairPlanId
    run_id: RunId
    reconciliation_fingerprint: StateFingerprint
    content_fingerprint: StateFingerprint
    status: RepairPlanStatus
    row_version: int
    created_at: UtcTimestamp
    applying_at: UtcTimestamp | None
    applied_at: UtcTimestamp | None
    rejected_at: UtcTimestamp | None
    failed_at: UtcTimestamp | None
    failure: RedactedDocument | None

    def __repr__(self) -> str:
        return (
            "RepairPlanRecord("
            f"repair_plan_id={self.repair_plan_id!r}, run_id={self.run_id!r}, "
            f"status={self.status!r}, row_version={self.row_version!r}, "
            f"created_at={self.created_at!r}, applying_at={self.applying_at!r}, "
            f"applied_at={self.applied_at!r}, rejected_at={self.rejected_at!r}, "
            f"failed_at={self.failed_at!r}, fingerprints=<redacted>, failure=<redacted>)"
        )


@dataclass(frozen=True, slots=True, repr=False)
class RepairActionRecord:
    repair_plan_id: RepairPlanId
    run_id: RunId
    effect: RepairActionEffect
    external_idempotency_key: str
    before_sha256: StateFingerprint | None
    proposed_after_sha256: StateFingerprint
    status: RepairActionStatus
    result: RepairApplicationResult | None
    target_version: int | None
    applied_at: UtcTimestamp | None
    failed_at: UtcTimestamp | None

    def __repr__(self) -> str:
        return (
            "RepairActionRecord("
            f"repair_plan_id={self.repair_plan_id!r}, run_id={self.run_id!r}, "
            f"action_id={self.effect.action_id!r}, kind={self.effect.kind!r}, "
            f"status={self.status!r}, target_version={self.target_version!r}, "
            f"applied_at={self.applied_at!r}, failed_at={self.failed_at!r}, "
            "effect=<redacted>, external_idempotency_key=<redacted>, "
            "hashes=<redacted>, result=<redacted>)"
        )


@dataclass(frozen=True, slots=True)
class RepairPlanAggregate:
    plan: RepairPlanRecord
    approval: RepairApprovalRecord | None
    actions: tuple[RepairActionRecord, ...]


@dataclass(frozen=True, slots=True, repr=False)
class RepairApplicationReservation:
    """Capability held by the current winning repair application step."""

    repair_plan_id: RepairPlanId
    run_id: RunId
    reconciliation_fingerprint: StateFingerprint
    content_fingerprint: StateFingerprint
    applying_at: UtcTimestamp
    row_version: int

    def __repr__(self) -> str:
        return (
            "RepairApplicationReservation("
            f"applying_at={self.applying_at!r}, row_version={self.row_version!r}, "
            "identity=<redacted>, fingerprints=<redacted>)"
        )


@dataclass(frozen=True, slots=True)
class RepairApplicationBeginResult:
    disposition: RepairApplicationBeginDisposition
    aggregate: RepairPlanAggregate
    reservation: RepairApplicationReservation | None

    def __post_init__(self) -> None:
        started = self.disposition is RepairApplicationBeginDisposition.STARTED
        if started != (type(self.reservation) is RepairApplicationReservation):
            raise ValueError("only a started repair application carries a reservation")


@dataclass(frozen=True, slots=True)
class AppliedRepairAction:
    action: RepairActionRecord
    reservation: RepairApplicationReservation


@dataclass(frozen=True, slots=True, repr=False)
class RepairPlanCursor:
    created_at: UtcTimestamp
    repair_plan_id: RepairPlanId

    def __repr__(self) -> str:
        return f"RepairPlanCursor(created_at={self.created_at!r}, identity=<redacted>)"


@dataclass(frozen=True, slots=True)
class RepairPlanPage:
    items: tuple[RepairPlanRecord, ...]
    next_cursor: RepairPlanCursor | None


@dataclass(frozen=True, slots=True, repr=False)
class RepairActionCursor:
    canonical_key: str
    repair_action_id: RepairActionId

    def __repr__(self) -> str:
        return "RepairActionCursor(identity=<redacted>)"


@dataclass(frozen=True, slots=True)
class RepairActionPage:
    items: tuple[RepairActionRecord, ...]
    next_cursor: RepairActionCursor | None


@dataclass(frozen=True, slots=True, order=True)
class AuditSequence:
    """One SQLite-assigned positive audit sequence number."""

    number: int

    def __post_init__(self) -> None:
        value = cast(object, self.number)
        if type(value) is not int:
            raise TypeError("audit sequence must be an integer")
        if not 1 <= self.number <= MAX_PERSISTED_INTEGER:
            raise ValueError("audit sequence is outside the supported range")

    def __int__(self) -> int:
        return self.number


@dataclass(frozen=True, slots=True, repr=False)
class PendingAuditEntry:
    actor: str
    operation: str
    object_kind: str
    object_id: str | None
    correlation_id: str
    occurred_at: UtcTimestamp
    detail_schema_version: int
    detail: RedactedDocument

    def __repr__(self) -> str:
        return (
            "PendingAuditEntry("
            f"operation={self.operation!r}, object_kind={self.object_kind!r}, "
            f"occurred_at={self.occurred_at!r}, "
            f"detail_schema_version={self.detail_schema_version!r}, "
            "actor=<redacted>, identity=<redacted>, detail=<redacted>)"
        )


@dataclass(frozen=True, slots=True, repr=False)
class AuditEntryRecord:
    sequence: AuditSequence
    actor: str
    operation: str
    object_kind: str
    object_id: str | None
    correlation_id: str
    occurred_at: UtcTimestamp
    detail_schema_version: int
    detail: RedactedDocument

    def __repr__(self) -> str:
        return (
            "AuditEntryRecord("
            f"sequence={self.sequence!r}, operation={self.operation!r}, "
            f"object_kind={self.object_kind!r}, occurred_at={self.occurred_at!r}, "
            f"detail_schema_version={self.detail_schema_version!r}, "
            "actor=<redacted>, identity=<redacted>, detail=<redacted>)"
        )


@dataclass(frozen=True, slots=True)
class AuditPage:
    items: tuple[AuditEntryRecord, ...]
    next_cursor: AuditSequence | None


class RepairRepository(Protocol):
    """Persistence contract for immutable repair plans and guarded effects."""

    def create_plan(
        self,
        *,
        run_id: RunId,
        plan: RepairPlan,
        action_keys: RepairActionKeyMap,
        created_at: UtcTimestamp,
    ) -> RepairPlanAggregate: ...

    def get(self, repair_plan_id: RepairPlanId) -> RepairPlanAggregate | None: ...

    def list_for_run(
        self,
        run_id: RunId,
        *,
        limit: int,
        after: RepairPlanCursor | None = None,
    ) -> RepairPlanPage: ...

    def get_action(self, repair_action_id: RepairActionId) -> RepairActionRecord | None: ...

    def list_actions(
        self,
        repair_plan_id: RepairPlanId,
        *,
        limit: int,
        after: RepairActionCursor | None = None,
    ) -> RepairActionPage: ...

    def approve(
        self,
        repair_plan_id: RepairPlanId,
        *,
        expected_row_version: int,
        current_reconciliation_fingerprint: StateFingerprint,
        approved_by: str,
        approved_at: UtcTimestamp,
        correlation_id: str,
        schema_version: int,
        detail: RedactedDocument,
    ) -> RepairPlanAggregate: ...

    def reject(
        self,
        repair_plan_id: RepairPlanId,
        *,
        expected_row_version: int,
        rejected_at: UtcTimestamp,
    ) -> RepairPlanAggregate: ...

    def begin_application(
        self,
        repair_plan_id: RepairPlanId,
        *,
        expected_row_version: int,
        current_reconciliation_fingerprint: StateFingerprint,
        applying_at: UtcTimestamp,
    ) -> RepairApplicationBeginResult: ...

    def record_action_applied(
        self,
        reservation: RepairApplicationReservation,
        repair_action_id: RepairActionId,
        *,
        result: RepairApplicationResult,
        target_version: int,
        applied_at: UtcTimestamp,
    ) -> AppliedRepairAction: ...

    def record_action_failed(
        self,
        reservation: RepairApplicationReservation,
        repair_action_id: RepairActionId,
        *,
        result: RepairApplicationResult,
        failed_at: UtcTimestamp,
        plan_failure: RedactedDocument,
    ) -> RepairPlanAggregate: ...

    def complete_application(
        self,
        reservation: RepairApplicationReservation,
        *,
        applied_at: UtcTimestamp,
    ) -> RepairPlanAggregate: ...


class AuditRepository(Protocol):
    """Append-only SQLite-assigned audit sequence operations."""

    def append(self, entry: PendingAuditEntry) -> AuditEntryRecord: ...

    def match_exact(self, entry: PendingAuditEntry) -> AuditEntryRecord:
        """Return the one byte-identical fact for an immediate command replay."""
        ...

    def get(self, sequence: AuditSequence) -> AuditEntryRecord | None: ...

    def list_after(self, *, after: AuditSequence | None, limit: int) -> AuditPage: ...


def validate_repair_page_limit(limit: object) -> int:
    """Validate a repair collection page size without coercion."""
    if type(limit) is not int or not 1 <= limit <= MAX_REPAIR_PAGE_SIZE:
        raise RepairInvalidRequestError(
            f"page limit must be an integer between 1 and {MAX_REPAIR_PAGE_SIZE}"
        )
    return limit


def validate_audit_page_limit(limit: object) -> int:
    """Validate an audit collection page size without coercion."""
    if type(limit) is not int or not 1 <= limit <= MAX_AUDIT_PAGE_SIZE:
        raise AuditInvalidRequestError(
            f"page limit must be an integer between 1 and {MAX_AUDIT_PAGE_SIZE}"
        )
    return limit


def _effect_record(effect: InventoryEffect) -> InventoryRecord:
    from paritygrid.domain.models import ConnectorId

    return InventoryRecord(
        sku=effect.sku,
        name=effect.name,
        quantity=effect.quantity,
        unit_price=effect.unit_price,
        updated_at=effect.updated_at,
        connector_id=ConnectorId("con_repair-effect"),
        source_record_key="repair-effect",
        attributes=effect.attributes,
    )
