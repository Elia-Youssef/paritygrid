"""The exclusive classification engine for one reconciliation analysis.

Every normalized record is assigned exactly one primary classification: the
classification of its canonical key group, computed by the domain
``ReconciliationOutcome`` contract. Completeness and exclusivity are structural
invariants of the result — every normalized source and target record appears
exactly once, and no record can carry a classification that disagrees with its
key. Useful secondary evidence (differing fields, duplicate content shape,
opponent-content matches) rides alongside without ever replacing the primary
classification.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar

from paritygrid.domain.reconciliation.differences import (
    FieldDifference,
    build_field_differences,
)
from paritygrid.domain.reconciliation.matching import (
    CanonicalKeyMatch,
    RecordSide,
)
from paritygrid.domain.reconciliation.outcomes import (
    ReconciliationClassification,
    ReconciliationOutcome,
)

_MAX_SECONDARY_VALUE_CHARACTERS = 256


class SuggestedResolution(StrEnum):
    """The closed review disposition suggested for one classified key."""

    NONE = "none"
    CREATE_TARGET = "create_target"
    UPDATE_TARGET = "update_target"
    REVIEW_TARGET_ONLY = "review_target_only"
    REVIEW_DUPLICATES = "review_duplicates"


class SecondaryEvidenceKind(StrEnum):
    """Closed secondary evidence kinds that never replace a classification."""

    MISMATCH_FIELDS = "mismatch_fields"
    IDENTICAL_DUPLICATE_CONTENT = "identical_duplicate_content"
    DISTINCT_DUPLICATE_CONTENT = "distinct_duplicate_content"
    OPPONENT_CONTENT_MATCHES_MEMBER = "opponent_content_matches_member"


@dataclass(frozen=True, slots=True)
class SecondaryEvidence:
    """One bounded secondary fact attached to a classification."""

    kind: SecondaryEvidenceKind
    value: str

    def __post_init__(self) -> None:
        if type(self.kind) is not SecondaryEvidenceKind:
            raise TypeError("secondary evidence kind must be a SecondaryEvidenceKind")
        if type(self.value) is not str or not self.value:
            raise ValueError("secondary evidence value must be nonempty text")
        if len(self.value) > _MAX_SECONDARY_VALUE_CHARACTERS:
            raise ValueError("secondary evidence value exceeds its size limit")


@dataclass(frozen=True, slots=True)
class RecordClassification:
    """The single primary classification of one normalized record."""

    side: RecordSide
    position: int
    sku: str
    source_record_key: str
    classification: ReconciliationClassification
    secondary: tuple[SecondaryEvidence, ...] = ()

    def __post_init__(self) -> None:
        if type(self.side) is not RecordSide:
            raise TypeError("record classification side must be a RecordSide")
        if type(self.position) is not int or self.position < 0:
            raise ValueError("record classification position must be a nonnegative integer")
        if type(self.sku) is not str or not self.sku:
            raise ValueError("record classification requires a nonempty SKU")
        if type(self.source_record_key) is not str or not self.source_record_key:
            raise ValueError("record classification requires a source record key")
        if type(self.classification) is not ReconciliationClassification:
            raise TypeError("record classification must use ReconciliationClassification")
        if type(self.secondary) is not tuple or any(
            type(item) is not SecondaryEvidence for item in self.secondary
        ):
            raise TypeError("record classification secondary evidence is invalid")


@dataclass(frozen=True, slots=True)
class ClassifiedKey:
    """One canonical key's domain outcome plus stable difference evidence."""

    outcome: ReconciliationOutcome
    differences: tuple[FieldDifference, ...]
    secondary: tuple[SecondaryEvidence, ...]
    suggested_resolution: SuggestedResolution

    def __post_init__(self) -> None:
        if type(self.outcome) is not ReconciliationOutcome:
            raise TypeError("classified key outcome must be a ReconciliationOutcome")
        if type(self.differences) is not tuple or any(
            type(item) is not FieldDifference for item in self.differences
        ):
            raise TypeError("classified key differences must be FieldDifference values")
        if type(self.secondary) is not tuple or any(
            type(item) is not SecondaryEvidence for item in self.secondary
        ):
            raise TypeError("classified key secondary evidence is invalid")
        if type(self.suggested_resolution) is not SuggestedResolution:
            raise TypeError("classified key resolution must be a SuggestedResolution")
        if self.suggested_resolution is not suggested_resolution_for(self.outcome.classification):
            raise ValueError("classified key resolution does not match its classification")


@dataclass(frozen=True, slots=True)
class ClassificationResult:
    """The complete, exclusive classification of every in-scope record."""

    MAX_RECORDS_PER_SIDE: ClassVar[int] = ReconciliationOutcome.MAX_RECORDS_PER_SIDE

    keys: tuple[ClassifiedKey, ...]
    records: tuple[RecordClassification, ...]

    def __post_init__(self) -> None:
        if type(self.keys) is not tuple or type(self.records) is not tuple:
            raise TypeError("classification result members must be tuples")
        key_skus = [key.outcome.sku for key in self.keys]
        if key_skus != sorted(key_skus) or len(set(key_skus)) != len(key_skus):
            raise ValueError("classification keys must use sorted unique SKUs")
        identities = [(record.side, record.position) for record in self.records]
        if identities != sorted(identities, key=lambda item: (item[0].value, item[1])):
            raise ValueError("classification records must be sorted by side and position")
        if len(set(identities)) != len(identities):
            raise ValueError("classification records must have unique identities")
        by_sku = {key.outcome.sku: key for key in self.keys}
        expected = sum(
            len(key.outcome.source_records) + len(key.outcome.target_records) for key in self.keys
        )
        if expected != len(self.records):
            raise ValueError("classification must cover every in-scope record exactly once")
        for record in self.records:
            key = by_sku.get(record.sku)
            if key is None or record.classification is not key.outcome.classification:
                raise ValueError("classification records must agree with their key")
            members = (
                key.outcome.source_records
                if record.side is RecordSide.SOURCE
                else key.outcome.target_records
            )
            if not any(
                member.sku == record.sku and member.source_record_key == record.source_record_key
                for member in members
            ):
                raise ValueError("classification record provenance is not part of its key")

    def count(self, classification: ReconciliationClassification) -> int:
        """Return how many keys carry one primary classification."""
        return sum(1 for key in self.keys if key.outcome.classification is classification)


def classify_matches(
    matches: tuple[CanonicalKeyMatch, ...],
) -> ClassificationResult:
    """Classify every matched key and assign every member record exactly once."""
    if type(matches) is not tuple:
        raise TypeError("classification input matches must be a tuple")
    keys: list[ClassifiedKey] = []
    records: list[RecordClassification] = []
    for match in matches:
        if type(match) is not CanonicalKeyMatch:
            raise TypeError("classification input matches must contain CanonicalKeyMatch values")
        outcome = ReconciliationOutcome(
            tuple(record.record for record in match.source_records),
            tuple(record.record for record in match.target_records),
        )
        secondary = _key_secondary(outcome)
        differences = _key_differences(match)
        keys.append(
            ClassifiedKey(
                outcome=outcome,
                differences=differences,
                secondary=secondary,
                suggested_resolution=suggested_resolution_for(outcome.classification),
            )
        )
        for side, members in (
            (RecordSide.SOURCE, match.source_records),
            (RecordSide.TARGET, match.target_records),
        ):
            records.extend(
                RecordClassification(
                    side=side,
                    position=member.position,
                    sku=match.sku,
                    source_record_key=member.record.source_record_key,
                    classification=outcome.classification,
                    secondary=secondary,
                )
                for member in members
            )
    ordered_records = tuple(sorted(records, key=lambda item: (item.side.value, item.position)))
    return ClassificationResult(keys=tuple(keys), records=ordered_records)


def suggested_resolution_for(
    classification: ReconciliationClassification,
) -> SuggestedResolution:
    """Return the sole suggested resolution allowed for one classification."""
    if type(classification) is not ReconciliationClassification:
        raise TypeError("resolution classification must use ReconciliationClassification")
    if classification is ReconciliationClassification.MISSING_FROM_TARGET:
        return SuggestedResolution.CREATE_TARGET
    if classification is ReconciliationClassification.FIELD_MISMATCH:
        return SuggestedResolution.UPDATE_TARGET
    if classification is ReconciliationClassification.MISSING_FROM_SOURCE:
        return SuggestedResolution.REVIEW_TARGET_ONLY
    if classification is ReconciliationClassification.MATCH:
        return SuggestedResolution.NONE
    return SuggestedResolution.REVIEW_DUPLICATES


def _key_secondary(outcome: ReconciliationOutcome) -> tuple[SecondaryEvidence, ...]:
    classification = outcome.classification
    if classification is ReconciliationClassification.FIELD_MISMATCH:
        fields = ",".join(mismatch.field.value for mismatch in outcome.mismatches)
        return (SecondaryEvidence(SecondaryEvidenceKind.MISMATCH_FIELDS, fields),)
    if classification in {
        ReconciliationClassification.DUPLICATE_SOURCE,
        ReconciliationClassification.DUPLICATE_TARGET,
        ReconciliationClassification.DUPLICATE_BOTH,
    }:
        evidence: list[SecondaryEvidence] = []
        for records in (outcome.source_records, outcome.target_records):
            if len(records) < 2:
                continue
            distinct = len(
                {
                    (
                        record.name,
                        record.quantity,
                        record.unit_price,
                        record.updated_at,
                        record.attributes,
                    )
                    for record in records
                }
            )
            evidence.append(
                SecondaryEvidence(
                    SecondaryEvidenceKind.IDENTICAL_DUPLICATE_CONTENT
                    if distinct == 1
                    else SecondaryEvidenceKind.DISTINCT_DUPLICATE_CONTENT,
                    str(distinct),
                )
            )
        opponent_key = _opponent_content_member(outcome)
        evidence.append(
            SecondaryEvidence(
                SecondaryEvidenceKind.OPPONENT_CONTENT_MATCHES_MEMBER,
                opponent_key,
            )
        )
        return tuple(evidence)
    return ()


def _opponent_content_member(outcome: ReconciliationOutcome) -> str:
    """Name the opponent member whose full canonical content matches, if any."""
    classification = outcome.classification
    if (
        classification is ReconciliationClassification.DUPLICATE_SOURCE
        and len(outcome.target_records) == 1
    ):
        single, members = outcome.target_records[0], outcome.source_records
    elif (
        classification is ReconciliationClassification.DUPLICATE_TARGET
        and len(outcome.source_records) == 1
    ):
        single, members = outcome.source_records[0], outcome.target_records
    else:
        return "none"
    for member in members:
        if (
            member.name,
            member.quantity,
            member.unit_price,
            member.updated_at,
            member.attributes,
        ) == (
            single.name,
            single.quantity,
            single.unit_price,
            single.updated_at,
            single.attributes,
        ):
            return member.source_record_key
    return "none"


def _key_differences(match: CanonicalKeyMatch) -> tuple[FieldDifference, ...]:
    """Build path-level evidence for one-to-one keys with differing fields."""
    source_records = match.source_records
    target_records = match.target_records
    if len(source_records) != 1 or len(target_records) != 1:
        return ()
    return build_field_differences(
        source_records[0].document,
        target_records[0].document,
    )
