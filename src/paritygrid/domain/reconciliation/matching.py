"""Canonical-key matching and deliberate duplicate detection.

Matching groups normalized records by their canonical key (the SKU). Groups are
built by appending to explicit per-key lists in input order, then emitted in
canonical SKU order: no map of one value per key is ever built, so a canonical
key collision can never silently discard a record through overwrite order.
Duplicate detection reports every side with more than one record per key and
analyzes whether the members repeat one canonical business value or disagree.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar

from paritygrid.domain.models import InventoryRecord
from paritygrid.domain.reconciliation.normalization import (
    MAX_SOURCE_OBSERVATIONS,
    NormalizedRecord,
)
from paritygrid.domain.reconciliation.outcomes import ReconciliationOutcome

MAX_MATCHED_KEYS = 2 * MAX_SOURCE_OBSERVATIONS


class MatchingError(ValueError):
    """A matching input violates the bounded canonical-key contract."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class RecordSide(StrEnum):
    """Which reconciliation side one record belongs to."""

    SOURCE = "source"
    TARGET = "target"


@dataclass(frozen=True, slots=True)
class CanonicalKeyMatch:
    """Every normalized record sharing one canonical key on both sides."""

    sku: str
    source_records: tuple[NormalizedRecord, ...] = ()
    target_records: tuple[NormalizedRecord, ...] = ()

    def records(self, side: RecordSide) -> tuple[NormalizedRecord, ...]:
        """Return the members of one side in canonical order."""
        if side is RecordSide.SOURCE:
            return self.source_records
        return self.target_records

    def record_count(self, side: RecordSide) -> int:
        """Return how many records one side contributes to this key."""
        return len(self.records(side))

    def __post_init__(self) -> None:
        sku = self.sku
        if type(sku) is not str or not sku:
            raise MatchingError("canonical key match requires a nonempty SKU")
        for side in (self.source_records, self.target_records):
            if type(side) is not tuple:
                raise TypeError("canonical key match records must be tuples")
            if len(side) > ReconciliationOutcome.MAX_RECORDS_PER_SIDE:
                raise MatchingError("canonical key match exceeds the per-side record limit")
            positions = [record.position for record in side]
            if positions != sorted(positions) or len(set(positions)) != len(positions):
                raise MatchingError("canonical key match members must use ordered unique positions")
            for record in side:
                if record.record.sku != sku:
                    raise MatchingError("canonical key match members must share one SKU")


@dataclass(frozen=True, slots=True)
class CanonicalKeyCollision:
    """Deliberate evidence that one canonical key holds several records."""

    MAX_MEMBER_KEYS: ClassVar[int] = ReconciliationOutcome.MAX_RECORDS_PER_SIDE

    side: RecordSide
    sku: str
    record_count: int
    member_keys: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.side) is not RecordSide:
            raise TypeError("canonical key collision side must be a RecordSide")
        if type(self.sku) is not str or not self.sku:
            raise MatchingError("canonical key collision requires a nonempty SKU")
        if type(self.record_count) is not int or self.record_count < 2:
            raise MatchingError("canonical key collision requires at least two records")
        if self.record_count > self.MAX_MEMBER_KEYS:
            raise MatchingError("canonical key collision exceeds the member limit")
        if type(self.member_keys) is not tuple or len(self.member_keys) != self.record_count:
            raise MatchingError("canonical key collision member keys must match the record count")
        keys = list(self.member_keys)
        if any(type(key) is not str or not key for key in keys):
            raise MatchingError("canonical key collision member keys must be nonempty text")
        if keys != sorted(keys):
            raise MatchingError("canonical key collision member keys must be sorted")


@dataclass(frozen=True, slots=True)
class DuplicateRecordGroup:
    """One side's duplicate members for a canonical key with content analysis."""

    side: RecordSide
    sku: str
    members: tuple[NormalizedRecord, ...]
    distinct_contents: int
    identical_members: bool

    def __post_init__(self) -> None:
        if type(self.side) is not RecordSide:
            raise TypeError("duplicate record group side must be a RecordSide")
        if type(self.sku) is not str or not self.sku:
            raise MatchingError("duplicate record group requires a nonempty SKU")
        members = self.members
        if type(members) is not tuple or len(members) < 2:
            raise MatchingError("duplicate record group requires at least two members")
        if len(members) > ReconciliationOutcome.MAX_RECORDS_PER_SIDE:
            raise MatchingError("duplicate record group exceeds the per-side record limit")
        if any(member.record.sku != self.sku for member in members):
            raise MatchingError("duplicate record group members must share one SKU")
        if type(self.distinct_contents) is not int or not 1 <= self.distinct_contents <= len(
            members
        ):
            raise MatchingError("duplicate record group content count is invalid")
        if type(self.identical_members) is not bool:
            raise TypeError("duplicate record group identity flag must be boolean")
        if self.identical_members != (self.distinct_contents == 1):
            raise MatchingError("duplicate record group identity flag disagrees with content count")


def match_by_canonical_key(
    source: Iterable[NormalizedRecord],
    target: Iterable[NormalizedRecord],
) -> tuple[CanonicalKeyMatch, ...]:
    """Group both sides by canonical SKU without ever discarding a record."""
    grouped: dict[str, dict[RecordSide, list[NormalizedRecord]]] = {}
    for side, records in ((RecordSide.SOURCE, source), (RecordSide.TARGET, target)):
        for record in records:
            if type(record) is not NormalizedRecord:
                raise TypeError("matching input must contain NormalizedRecord values")
            existing = grouped.get(record.record.sku)
            if existing is None:
                if len(grouped) == MAX_MATCHED_KEYS:
                    raise MatchingError("matching exceeds the canonical key limit")
                fresh: dict[RecordSide, list[NormalizedRecord]] = {
                    RecordSide.SOURCE: [],
                    RecordSide.TARGET: [],
                }
                grouped[record.record.sku] = fresh
                existing = fresh
            members = existing[side]
            if len(members) == ReconciliationOutcome.MAX_RECORDS_PER_SIDE:
                raise MatchingError("matching exceeds the per-side duplicate record limit")
            members.append(record)
    return tuple(
        CanonicalKeyMatch(
            sku=sku,
            source_records=tuple(bucket[RecordSide.SOURCE]),
            target_records=tuple(bucket[RecordSide.TARGET]),
        )
        for sku, bucket in sorted(grouped.items())
    )


def detect_canonical_key_collisions(
    matches: Iterable[CanonicalKeyMatch],
) -> tuple[CanonicalKeyCollision, ...]:
    """Report every canonical key holding more than one record on a side."""
    collisions: list[CanonicalKeyCollision] = []
    for match in matches:
        if type(match) is not CanonicalKeyMatch:
            raise TypeError("collision detection requires CanonicalKeyMatch values")
        for side in (RecordSide.SOURCE, RecordSide.TARGET):
            members = match.records(side)
            if len(members) < 2:
                continue
            collisions.append(
                CanonicalKeyCollision(
                    side=side,
                    sku=match.sku,
                    record_count=len(members),
                    member_keys=tuple(
                        sorted(record.record.source_record_key for record in members)
                    ),
                )
            )
    return tuple(collisions)


def detect_duplicate_record_groups(
    matches: Iterable[CanonicalKeyMatch],
) -> tuple[DuplicateRecordGroup, ...]:
    """Analyze duplicate members for repeated or divergent canonical content."""
    groups: list[DuplicateRecordGroup] = []
    for match in matches:
        if type(match) is not CanonicalKeyMatch:
            raise TypeError("duplicate detection requires CanonicalKeyMatch values")
        for side in (RecordSide.SOURCE, RecordSide.TARGET):
            members = match.records(side)
            if len(members) < 2:
                continue
            distinct = len({_business_content(record.record) for record in members})
            groups.append(
                DuplicateRecordGroup(
                    side=side,
                    sku=match.sku,
                    members=members,
                    distinct_contents=distinct,
                    identical_members=distinct == 1,
                )
            )
    return tuple(groups)


def _business_content(record: InventoryRecord) -> tuple[object, ...]:
    """Project the canonical business state that determines duplicate identity."""
    return (
        record.name,
        record.quantity,
        record.unit_price,
        record.updated_at,
        record.attributes,
    )
