"""The independent Python reference reconciliation analysis.

This service composes the pure domain pipeline — normalization, canonical-key
matching, duplicate detection, classification, differences, and the summary
fingerprint — into one deterministic analysis. It owns no I/O and never
mutates operational or target state; analytical artifacts are published
separately through :mod:`~paritygrid.application.reconciliation.publication`.
The same domain functions back the DuckDB agreement proofs, so this module is
the Python reference implementation those proofs compare against.
"""

from dataclasses import dataclass

from paritygrid.application.ports.parquet import ReconciliationConflictRow
from paritygrid.domain.canonical.encoding import CanonicalVersion
from paritygrid.domain.canonical.fingerprints import FingerprintScope, fingerprint_state
from paritygrid.domain.reconciliation import (
    CanonicalKeyCollision,
    CanonicalKeyMatch,
    ClassificationResult,
    DuplicateRecordGroup,
    NormalizedRecord,
    QuarantinedObservation,
    ReconciliationClassification,
    ReconciliationSummary,
    RecordSide,
    SourceObservation,
    build_reconciliation_summary,
    classify_matches,
    detect_canonical_key_collisions,
    detect_duplicate_record_groups,
    match_by_canonical_key,
    normalize_source_observations,
)

DEFAULT_ANALYTICAL_QUERY_VERSION = 1


@dataclass(frozen=True, slots=True)
class ReconciliationAnalysisRequest:
    """The exact observations and input identities for one analysis."""

    source_observations: tuple[SourceObservation, ...]
    target_observations: tuple[SourceObservation, ...]
    source_input_identity: str
    target_input_identity: str
    analytical_query_version: int = DEFAULT_ANALYTICAL_QUERY_VERSION


@dataclass(frozen=True, slots=True)
class ReconciliationAnalysis:
    """The complete deterministic result of one reconciliation analysis."""

    summary: ReconciliationSummary
    classification: ClassificationResult
    conflicts: tuple[ReconciliationConflictRow, ...]
    matches: tuple[CanonicalKeyMatch, ...]
    collisions: tuple[CanonicalKeyCollision, ...]
    duplicate_groups: tuple[DuplicateRecordGroup, ...]
    source_quarantined: tuple[QuarantinedObservation, ...]
    target_quarantined: tuple[QuarantinedObservation, ...]

    def __post_init__(self) -> None:
        for name in (
            "conflicts",
            "matches",
            "collisions",
            "duplicate_groups",
            "source_quarantined",
            "target_quarantined",
        ):
            if type(getattr(self, name)) is not tuple:
                raise TypeError(f"reconciliation analysis {name} must be a tuple")
        if type(self.summary) is not ReconciliationSummary:
            raise TypeError("reconciliation analysis summary is invalid")
        if type(self.classification) is not ClassificationResult:
            raise TypeError("reconciliation analysis classification is invalid")


def analyze_reconciliation(
    request: ReconciliationAnalysisRequest,
) -> ReconciliationAnalysis:
    """Run the complete analytical pipeline for one request."""
    if type(request) is not ReconciliationAnalysisRequest:
        raise TypeError("reconciliation analysis requires a ReconciliationAnalysisRequest")
    source = normalize_source_observations(request.source_observations)
    target = normalize_source_observations(request.target_observations)
    matches = match_by_canonical_key(source.records, target.records)
    classification = classify_matches(matches)
    summary = build_reconciliation_summary(
        classification=classification,
        source_normalization=source,
        target_normalization=target,
        source_input_identity=request.source_input_identity,
        target_input_identity=request.target_input_identity,
        analytical_query_version=request.analytical_query_version,
        outcome_state_digest=fingerprint_state(
            (key.outcome for key in classification.keys),
            scope=FingerprintScope.RECONCILIATION_STATE,
            version=CanonicalVersion.V1,
        ),
    )
    conflicts = tuple(_conflict_rows(matches, classification))
    return ReconciliationAnalysis(
        summary=summary,
        classification=classification,
        conflicts=conflicts,
        matches=matches,
        collisions=detect_canonical_key_collisions(matches),
        duplicate_groups=detect_duplicate_record_groups(matches),
        source_quarantined=source.quarantined,
        target_quarantined=target.quarantined,
    )


def _conflict_rows(
    matches: tuple[CanonicalKeyMatch, ...],
    classification: ClassificationResult,
) -> list[ReconciliationConflictRow]:
    classified = {key.outcome.sku: key for key in classification.keys}
    rows: list[ReconciliationConflictRow] = []
    for match in matches:
        key = classified[match.sku]
        if key.outcome.classification is ReconciliationClassification.MATCH:
            continue
        source_positions, source_record_keys = _member_provenance(match, RecordSide.SOURCE)
        target_positions, target_record_keys = _member_provenance(match, RecordSide.TARGET)
        rows.append(
            ReconciliationConflictRow(
                conflict_index=len(rows),
                sku=match.sku,
                classification=key.outcome.classification,
                suggested_resolution=key.suggested_resolution,
                source_positions=source_positions,
                target_positions=target_positions,
                source_record_keys=source_record_keys,
                target_record_keys=target_record_keys,
                differences=key.differences,
                secondary=key.secondary,
            )
        )
    return rows


def _ordered_members(match: CanonicalKeyMatch, side: RecordSide) -> tuple[NormalizedRecord, ...]:
    members = match.records(side)
    return tuple(sorted(members, key=lambda record: record.position)) if members else ()


def _member_provenance(
    match: CanonicalKeyMatch,
    side: RecordSide,
) -> tuple[tuple[int, ...], tuple[str, ...]]:
    """Return parallel member positions and keys under one shared ordering."""
    members = _ordered_members(match, side)
    return (
        tuple(record.position for record in members),
        tuple(record.record.source_record_key for record in members),
    )
