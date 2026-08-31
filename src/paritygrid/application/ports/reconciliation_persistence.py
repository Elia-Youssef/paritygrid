"""Dependency-neutral contracts for durable reconciliation and verification facts."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, cast

from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.domain.models import (
    ConflictId,
    RepairPlanId,
    RunId,
    StateFingerprint,
    TargetVerificationId,
    UtcTimestamp,
)
from paritygrid.domain.reconciliation import (
    FieldDifference,
    ReconciliationClassification,
    ReconciliationSummary,
    SuggestedResolution,
)

MAX_RECONCILIATION_CONFLICTS = 100_000
MAX_CONFLICT_REFERENCES = 1_024
MAX_CONFLICT_DIFFERENCES = 128
MAX_VERIFICATION_DETAIL_BYTES = 4_096


class ReconciliationPersistenceError(Exception):
    """Base class for stable reconciliation persistence failures."""


class ReconciliationInvalidRequestError(ReconciliationPersistenceError):
    """A request violates the public reconciliation persistence contract."""


class ReconciliationRecordNotFoundError(ReconciliationPersistenceError):
    """A required reconciliation record does not exist."""


class ReconciliationResultConflictError(ReconciliationPersistenceError):
    """A replayed reconciliation result differs from the immutable stored fact."""


class ReconciliationCorruptionError(ReconciliationPersistenceError):
    """Persisted reconciliation data failed strict boundary validation."""


class ReconciliationStorageError(ReconciliationPersistenceError):
    """An unexpected persistence failure prevented a reconciliation operation."""


class ReconciliationStorageUnavailableError(ReconciliationStorageError):
    """Reconciliation storage was unavailable for the requested operation."""


class TargetVerificationError(Exception):
    """Base class for stable target-verification persistence failures."""


class TargetVerificationInvalidRequestError(TargetVerificationError):
    """A request violates the public target-verification contract."""


class TargetVerificationConflictError(TargetVerificationError):
    """A replayed verification identity differs from the immutable stored fact."""


class TargetVerificationCorruptionError(TargetVerificationError):
    """Persisted verification data failed strict boundary validation."""


class TargetVerificationStorageError(TargetVerificationError):
    """An unexpected persistence failure prevented a verification operation."""


class TargetVerificationStorageUnavailableError(TargetVerificationStorageError):
    """Verification storage was unavailable for the requested operation."""


class TargetVerificationVerdict(StrEnum):
    """Closed verdicts of one independently observed target state."""

    PARITY_HOLDING = "parity_holding"
    PARITY_DIVERGENT = "parity_divergent"
    OBSERVATION_FAILED = "observation_failed"


@dataclass(frozen=True, slots=True)
class PersistedConflict:
    """One immutable conflict fact bound to its reconciliation snapshot."""

    conflict_id: ConflictId
    canonical_key: str
    classification: ReconciliationClassification
    source_references: tuple[tuple[int, str], ...]
    target_references: tuple[tuple[int, str], ...]
    differences: tuple[FieldDifference, ...]
    suggested_resolution: SuggestedResolution | None
    created_at: UtcTimestamp

    def __post_init__(self) -> None:
        if type(self.conflict_id) is not ConflictId:
            raise TypeError("persisted conflict requires ConflictId")
        if type(self.canonical_key) is not str or not 1 <= len(self.canonical_key) <= 64:
            raise ValueError("persisted conflict canonical key is invalid")
        if type(self.classification) is not ReconciliationClassification:
            raise TypeError("persisted conflict classification is invalid")
        if self.classification is ReconciliationClassification.MATCH:
            raise ValueError("persisted conflict cannot use the match classification")
        for name in ("source_references", "target_references"):
            references = cast(object, getattr(self, name))
            if type(references) is not tuple:
                raise ValueError(f"persisted conflict {name} are invalid")
            reference_items = cast(tuple[object, ...], references)
            if len(reference_items) > MAX_CONFLICT_REFERENCES:
                raise ValueError(f"persisted conflict {name} are invalid")
            for member_value in reference_items:
                if type(member_value) is not tuple:
                    raise ValueError(f"persisted conflict {name} contain an invalid member")
                member = cast(tuple[object, ...], member_value)
                if len(member) != 2:
                    raise ValueError(f"persisted conflict {name} contain an invalid member")
                first, second = member[0], member[1]
                if (
                    type(first) is not int
                    or first < 0
                    or type(second) is not str
                    or not 1 <= len(second) <= 128
                ):
                    raise ValueError(f"persisted conflict {name} contain an invalid member")
        differences = cast(object, self.differences)
        if type(differences) is not tuple:
            raise ValueError("persisted conflict differences are invalid")
        difference_items = cast(tuple[object, ...], differences)
        if len(difference_items) > MAX_CONFLICT_DIFFERENCES:
            raise ValueError("persisted conflict differences are invalid")
        if any(type(item) is not FieldDifference for item in difference_items):
            raise TypeError("persisted conflict differences must contain FieldDifference values")
        if self.suggested_resolution is not None and type(self.suggested_resolution) is not (
            SuggestedResolution
        ):
            raise TypeError("persisted conflict suggested resolution is invalid")
        if type(self.created_at) is not UtcTimestamp:
            raise TypeError("persisted conflict creation time is invalid")


@dataclass(frozen=True, slots=True)
class ReconciliationSummaryRecord:
    """The durable reconciliation snapshot for exactly one run."""

    run_id: RunId
    reconciliation_fingerprint: StateFingerprint
    source_fingerprint: StateFingerprint
    target_fingerprint: StateFingerprint
    counts: tuple[tuple[ReconciliationClassification, int], ...]
    total_count: int
    analytical_query_version: int
    created_at: UtcTimestamp

    def __post_init__(self) -> None:
        if type(self.run_id) is not RunId:
            raise TypeError("reconciliation summary run identity is invalid")
        for name in (
            "reconciliation_fingerprint",
            "source_fingerprint",
            "target_fingerprint",
        ):
            if type(getattr(self, name)) is not StateFingerprint:
                raise TypeError(f"reconciliation summary {name} is invalid")
        counts = cast(object, self.counts)
        if not isinstance(counts, tuple):
            raise TypeError("reconciliation summary counts must be a tuple")
        expected = tuple(sorted(ReconciliationClassification, key=lambda item: item.value))
        classifications = tuple(
            classification
            for classification, _count in cast(tuple[tuple[object, object], ...], counts)
        )
        if classifications != expected:
            raise ValueError("reconciliation summary counts must list every classification")
        for pair in cast(tuple[tuple[object, object], ...], counts):
            classification, count = pair
            if not isinstance(classification, ReconciliationClassification) or not isinstance(
                count, int
            ):
                raise TypeError("reconciliation summary counts must use typed pairs")
            if count < 0:
                raise ValueError("reconciliation summary counts must be nonnegative")
        if type(self.total_count) is not int or self.total_count < 0:
            raise ValueError("reconciliation summary total count is invalid")
        if type(self.analytical_query_version) is not int or self.analytical_query_version < 1:
            raise ValueError("reconciliation summary analytical query version is invalid")
        if type(self.created_at) is not UtcTimestamp:
            raise TypeError("reconciliation summary creation time is invalid")

    def count(self, classification: ReconciliationClassification) -> int:
        """Return the stored count for one classification."""
        for candidate, count in self.counts:
            if candidate is classification:
                return count
        raise KeyError(classification)


@dataclass(frozen=True, slots=True)
class ReconciliationResultRecord:
    """One persisted reconciliation snapshot with its conflict evidence."""

    summary: ReconciliationSummaryRecord
    conflicts: tuple[PersistedConflict, ...]

    def __post_init__(self) -> None:
        keys = [conflict.canonical_key for conflict in self.conflicts]
        if keys != sorted(keys) or len(set(keys)) != len(keys):
            raise ValueError("persisted conflicts must be sorted by unique canonical key")
        if len(self.conflicts) > MAX_RECONCILIATION_CONFLICTS:
            raise ValueError("persisted conflicts exceed the supported bound")
        for conflict in self.conflicts:
            if conflict.created_at != self.summary.created_at:
                raise ValueError("persisted conflicts must share the summary creation time")
        counted: dict[ReconciliationClassification, int] = {}
        for conflict in self.conflicts:
            counted[conflict.classification] = counted.get(conflict.classification, 0) + 1
        for classification, count in self.summary.counts:
            expected_conflicts = (
                0 if classification is ReconciliationClassification.MATCH else count
            )
            if counted.get(classification, 0) != expected_conflicts:
                raise ValueError("persisted conflicts must cover the summary classification counts")


@dataclass(frozen=True, slots=True, repr=False)
class TargetVerificationRecord:
    """One immutable independently observed target-state verification fact."""

    verification_id: TargetVerificationId
    run_id: RunId
    repair_plan_id: RepairPlanId | None
    reconciliation_fingerprint: StateFingerprint
    plan_content_fingerprint: StateFingerprint | None
    observed_fingerprint: StateFingerprint
    observed_fingerprint_version: int
    expected_fingerprint: StateFingerprint
    verdict: TargetVerificationVerdict
    observed_record_count: int
    expected_record_count: int
    observed_target_version: int
    observed_at: UtcTimestamp
    detail: RedactedDocument

    def __post_init__(self) -> None:
        if type(self.verification_id) is not TargetVerificationId:
            raise TypeError("target verification requires TargetVerificationId")
        if type(self.run_id) is not RunId:
            raise TypeError("target verification requires RunId")
        if self.repair_plan_id is not None and type(self.repair_plan_id) is not RepairPlanId:
            raise TypeError("target verification repair-plan identity is invalid")
        for name in (
            "reconciliation_fingerprint",
            "observed_fingerprint",
            "expected_fingerprint",
        ):
            if type(getattr(self, name)) is not StateFingerprint:
                raise TypeError(f"target verification {name} is invalid")
        if (
            self.plan_content_fingerprint is not None
            and type(self.plan_content_fingerprint) is not StateFingerprint
        ):
            raise TypeError("target verification plan content fingerprint is invalid")
        if (
            type(self.observed_fingerprint_version) is not int
            or self.observed_fingerprint_version < 1
        ):
            raise ValueError("target verification fingerprint version is invalid")
        if type(self.verdict) is not TargetVerificationVerdict:
            raise TypeError("target verification verdict is invalid")
        for name in (
            "observed_record_count",
            "expected_record_count",
            "observed_target_version",
        ):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError(f"target verification {name} is invalid")
        if type(self.observed_at) is not UtcTimestamp:
            raise TypeError("target verification observation time is invalid")
        if type(self.detail) is not RedactedDocument:
            raise TypeError("target verification detail must be a RedactedDocument")

    def __repr__(self) -> str:
        return (
            "TargetVerificationRecord("
            f"verification_id={self.verification_id!r}, run_id={self.run_id!r}, "
            f"verdict={self.verdict!r}, observed_record_count={self.observed_record_count!r}, "
            f"expected_record_count={self.expected_record_count!r}, "
            f"observed_target_version={self.observed_target_version!r}, "
            f"observed_at={self.observed_at!r}, fingerprints=<redacted>, detail=<redacted>)"
        )


class ReconciliationResultRepository(Protocol):
    """Persistence contract for immutable reconciliation snapshots."""

    def persist(
        self,
        *,
        run_id: RunId,
        summary: ReconciliationSummary,
        conflicts: Sequence[PersistedConflict],
        created_at: UtcTimestamp,
    ) -> ReconciliationResultRecord: ...

    def get_summary(self, run_id: RunId) -> ReconciliationSummaryRecord | None: ...

    def get_result(self, run_id: RunId) -> ReconciliationResultRecord | None: ...


class TargetVerificationRepository(Protocol):
    """Persistence contract for immutable target-state verification facts."""

    def record(self, verification: TargetVerificationRecord) -> TargetVerificationRecord: ...

    def get(self, verification_id: TargetVerificationId) -> TargetVerificationRecord | None: ...

    def latest_for_run(self, run_id: RunId) -> TargetVerificationRecord | None: ...


__all__ = [
    "MAX_CONFLICT_DIFFERENCES",
    "MAX_CONFLICT_REFERENCES",
    "MAX_RECONCILIATION_CONFLICTS",
    "MAX_VERIFICATION_DETAIL_BYTES",
    "PersistedConflict",
    "ReconciliationCorruptionError",
    "ReconciliationInvalidRequestError",
    "ReconciliationPersistenceError",
    "ReconciliationRecordNotFoundError",
    "ReconciliationResultConflictError",
    "ReconciliationResultRecord",
    "ReconciliationResultRepository",
    "ReconciliationStorageError",
    "ReconciliationStorageUnavailableError",
    "ReconciliationSummaryRecord",
    "TargetVerificationConflictError",
    "TargetVerificationCorruptionError",
    "TargetVerificationError",
    "TargetVerificationInvalidRequestError",
    "TargetVerificationRecord",
    "TargetVerificationRepository",
    "TargetVerificationStorageError",
    "TargetVerificationStorageUnavailableError",
    "TargetVerificationVerdict",
]
