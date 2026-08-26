"""Deterministic reconciliation summaries and the reconciliation fingerprint.

The reconciliation fingerprint is its own fingerprint kind: it identifies one
analytical reconciliation snapshot from versioned query and input identities
plus the canonical classification state and summary counts. It is computed
independently of every other fingerprint kind (plan, execution-evidence, and
the later target-state fingerprint) and is never derived from or compared with
the Phase 6 execution-evidence digest.
"""

import json
import re
from dataclasses import dataclass
from hashlib import sha256
from typing import cast

from paritygrid.domain.models import StateFingerprint
from paritygrid.domain.reconciliation.classification import ClassificationResult
from paritygrid.domain.reconciliation.matching import RecordSide
from paritygrid.domain.reconciliation.normalization import (
    NORMALIZATION_RULES_VERSION,
    QuarantineCode,
    SourceNormalization,
)
from paritygrid.domain.reconciliation.outcomes import ReconciliationClassification

RECONCILIATION_FINGERPRINT_KIND = "reconciliation"
RECONCILIATION_FINGERPRINT_VERSION = 1
RECONCILIATION_ANALYSIS_VERSION = 1
_LENGTH_BYTES = 8
_LOWER_SHA256 = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)
_FINGERPRINT_DOMAIN = b"paritygrid:reconciliation-fingerprint:v1\0"


@dataclass(frozen=True, slots=True)
class QuarantineCount:
    """How many observations one side quarantined for one closed reason."""

    side: RecordSide
    code: QuarantineCode
    count: int

    def __post_init__(self) -> None:
        if type(self.side) is not RecordSide:
            raise TypeError("quarantine count side must be a RecordSide")
        if type(self.code) is not QuarantineCode:
            raise TypeError("quarantine count code must be a QuarantineCode")
        if type(self.count) is not int or self.count < 1:
            raise ValueError("quarantine count must be a positive integer")


@dataclass(frozen=True, slots=True)
class ReconciliationCountSummary:
    """Every closed count required to reproduce one reconciliation snapshot."""

    by_classification: tuple[tuple[ReconciliationClassification, int], ...]
    source_record_count: int
    target_record_count: int
    canonical_key_count: int
    source_quarantined_count: int
    target_quarantined_count: int
    quarantine_breakdown: tuple[QuarantineCount, ...]

    def __post_init__(self) -> None:
        if type(self.by_classification) is not tuple:
            raise TypeError("classification counts must be a tuple")
        classifications = tuple(classification for classification, _count in self.by_classification)
        expected = tuple(sorted(ReconciliationClassification, key=lambda item: item.value))
        if classifications != expected:
            raise ValueError("classification counts must list every classification exactly once")
        for classification, count in self.by_classification:
            if type(classification) is not ReconciliationClassification or type(count) is not int:
                raise TypeError("classification counts must use typed pairs")
            if count < 0:
                raise ValueError("classification counts must be nonnegative")
        for name in (
            "source_record_count",
            "target_record_count",
            "canonical_key_count",
            "source_quarantined_count",
            "target_quarantined_count",
        ):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if type(self.quarantine_breakdown) is not tuple or any(
            type(item) is not QuarantineCount for item in self.quarantine_breakdown
        ):
            raise TypeError("quarantine breakdown must contain QuarantineCount values")
        breakdown_order = [(item.side.value, item.code.value) for item in self.quarantine_breakdown]
        if breakdown_order != sorted(breakdown_order) or len(set(breakdown_order)) != len(
            breakdown_order
        ):
            raise ValueError("quarantine breakdown must be sorted and unique")
        if sum(item.count for item in self.quarantine_breakdown) != (
            self.source_quarantined_count + self.target_quarantined_count
        ):
            raise ValueError("quarantine breakdown must cover every quarantined observation")


@dataclass(frozen=True, slots=True)
class ReconciliationSummary:
    """One complete analytical reconciliation snapshot identity and counts."""

    fingerprint_kind: str
    fingerprint_version: int
    analysis_version: int
    rules_version: int
    counts: ReconciliationCountSummary
    source_input_identity: str
    target_input_identity: str
    analytical_query_version: int
    fingerprint: StateFingerprint

    def __post_init__(self) -> None:
        if self.fingerprint_kind != RECONCILIATION_FINGERPRINT_KIND:
            raise ValueError("reconciliation summary fingerprint kind is invalid")
        if self.fingerprint_version != RECONCILIATION_FINGERPRINT_VERSION:
            raise ValueError("reconciliation summary fingerprint version is unsupported")
        if self.analysis_version != RECONCILIATION_ANALYSIS_VERSION:
            raise ValueError("reconciliation summary analysis version is unsupported")
        if self.rules_version != NORMALIZATION_RULES_VERSION:
            raise ValueError("reconciliation summary rules version is unsupported")
        if type(self.counts) is not ReconciliationCountSummary:
            raise TypeError("reconciliation summary counts are invalid")
        for identity in (self.source_input_identity, self.target_input_identity):
            if type(identity) is not str or _LOWER_SHA256.fullmatch(identity) is None:
                raise ValueError("reconciliation input identity must be lowercase SHA-256")
        if type(self.analytical_query_version) is not int or self.analytical_query_version < 1:
            raise ValueError("reconciliation analytical query version is invalid")
        if type(self.fingerprint) is not StateFingerprint:
            raise TypeError("reconciliation fingerprint must be a StateFingerprint")


def build_reconciliation_summary(
    *,
    classification: ClassificationResult,
    source_normalization: SourceNormalization,
    target_normalization: SourceNormalization,
    source_input_identity: str,
    target_input_identity: str,
    analytical_query_version: int,
    outcome_state_digest: StateFingerprint,
) -> ReconciliationSummary:
    """Assemble the deterministic summary and fingerprint for one analysis.

    ``outcome_state_digest`` is the order-independent reconciliation-state
    digest of the classified outcomes. The caller supplies it because the
    canonical fingerprint module owns that computation; this module only
    composes the kind-specific reconciliation fingerprint.
    """
    if type(classification) is not ClassificationResult:
        raise TypeError("reconciliation summary requires a ClassificationResult")
    if type(source_normalization) is not SourceNormalization or (
        type(target_normalization) is not SourceNormalization
    ):
        raise TypeError("reconciliation summary requires SourceNormalization values")
    counts = ReconciliationCountSummary(
        by_classification=_classification_counts(classification),
        source_record_count=len(source_normalization.records),
        target_record_count=len(target_normalization.records),
        canonical_key_count=len(classification.keys),
        source_quarantined_count=len(source_normalization.quarantined),
        target_quarantined_count=len(target_normalization.quarantined),
        quarantine_breakdown=_quarantine_breakdown(source_normalization, target_normalization),
    )
    fingerprint = compute_reconciliation_fingerprint(
        source_input_identity=source_input_identity,
        target_input_identity=target_input_identity,
        analytical_query_version=analytical_query_version,
        counts=counts,
        outcome_state_digest=outcome_state_digest,
    )
    return ReconciliationSummary(
        fingerprint_kind=RECONCILIATION_FINGERPRINT_KIND,
        fingerprint_version=RECONCILIATION_FINGERPRINT_VERSION,
        analysis_version=RECONCILIATION_ANALYSIS_VERSION,
        rules_version=NORMALIZATION_RULES_VERSION,
        counts=counts,
        source_input_identity=source_input_identity,
        target_input_identity=target_input_identity,
        analytical_query_version=analytical_query_version,
        fingerprint=fingerprint,
    )


def compute_reconciliation_fingerprint(
    *,
    source_input_identity: str,
    target_input_identity: str,
    analytical_query_version: int,
    counts: ReconciliationCountSummary,
    outcome_state_digest: StateFingerprint,
) -> StateFingerprint:
    """Fingerprint the canonical inputs, classifications, and summary counts.

    The caller supplies the order-independent digest of the classified outcome
    multiset; the header frames the exact query and input identities so the
    composed digest is sensitive to any changed input while staying
    independent of outcome order.
    """
    if type(counts) is not ReconciliationCountSummary:
        raise TypeError("reconciliation fingerprint requires a ReconciliationCountSummary")
    if type(outcome_state_digest) is not StateFingerprint:
        raise TypeError("reconciliation outcome state digest must be a StateFingerprint")
    for identity in (source_input_identity, target_input_identity):
        if type(identity) is not str or _LOWER_SHA256.fullmatch(identity) is None:
            raise ValueError("reconciliation input identity must be lowercase SHA-256")
    if type(analytical_query_version) is not int or analytical_query_version < 1:
        raise ValueError("reconciliation analytical query version is invalid")
    header = json.dumps(
        {
            "analytical_query_version": analytical_query_version,
            "counts": _counts_header(counts),
            "fingerprint_kind": RECONCILIATION_FINGERPRINT_KIND,
            "fingerprint_version": RECONCILIATION_FINGERPRINT_VERSION,
            "source_input_identity": source_input_identity,
            "target_input_identity": target_input_identity,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    preimage = _FINGERPRINT_DOMAIN + _frame(header) + _frame(outcome_state_digest.to_bytes())
    return StateFingerprint(sha256(preimage).hexdigest())


def _classification_counts(
    classification: ClassificationResult,
) -> tuple[tuple[ReconciliationClassification, int], ...]:
    return tuple(
        (candidate, classification.count(candidate))
        for candidate in sorted(ReconciliationClassification, key=lambda item: item.value)
    )


def _quarantine_breakdown(
    source: SourceNormalization,
    target: SourceNormalization,
) -> tuple[QuarantineCount, ...]:
    counts: dict[tuple[RecordSide, QuarantineCode], int] = {}
    for side, normalization in ((RecordSide.SOURCE, source), (RecordSide.TARGET, target)):
        for quarantined in normalization.quarantined:
            key = (side, quarantined.code)
            counts[key] = counts.get(key, 0) + 1
    return tuple(
        QuarantineCount(side=side, code=code, count=counts[(side, code)])
        for side, code in sorted(counts, key=lambda item: (item[0].value, item[1].value))
    )


def _counts_header(counts: ReconciliationCountSummary) -> dict[str, object]:
    quarantine = cast(
        "dict[str, object]",
        {
            f"{item.side.value}:{item.code.value}": item.count
            for item in counts.quarantine_breakdown
        },
    )
    return {
        "by_classification": {
            classification.value: count for classification, count in counts.by_classification
        },
        "canonical_key_count": counts.canonical_key_count,
        "quarantine": quarantine,
        "source_quarantined_count": counts.source_quarantined_count,
        "source_record_count": counts.source_record_count,
        "target_quarantined_count": counts.target_quarantined_count,
        "target_record_count": counts.target_record_count,
    }


def _frame(value: bytes) -> bytes:
    return len(value).to_bytes(_LENGTH_BYTES, byteorder="big") + value
