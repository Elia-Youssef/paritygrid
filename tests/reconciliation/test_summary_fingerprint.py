"""Golden counts, deterministic fingerprint, and kind-separation tests."""

from hashlib import sha256

import pytest
from hypothesis import given
from hypothesis import strategies as st

from paritygrid.application.reconciliation import (
    ReconciliationAnalysis,
    ReconciliationAnalysisRequest,
    analyze_reconciliation,
)
from paritygrid.domain.canonical.fingerprints import FingerprintScope, fingerprint_state
from paritygrid.domain.models import StateFingerprint
from paritygrid.domain.reconciliation import (
    RECONCILIATION_ANALYSIS_VERSION,
    RECONCILIATION_FINGERPRINT_KIND,
    RECONCILIATION_FINGERPRINT_VERSION,
    QuarantineCode,
    QuarantineCount,
    ReconciliationClassification,
    ReconciliationCountSummary,
    ReconciliationSummary,
    RecordSide,
    SourceObservation,
    compute_reconciliation_fingerprint,
)
from paritygrid.domain.reconciliation.classification import ClassificationResult
from tests.reconciliation.conftest import SOURCE_CONNECTOR, wire_payload

_SOURCE_IDENTITY = sha256(b"phase-10-source-inputs").hexdigest()
_TARGET_IDENTITY = sha256(b"phase-10-target-inputs").hexdigest()
_GOLDEN_FINGERPRINT = "e71f349e4beff5ee3f7dd49670e835ff4878cdaf5f7762d5a17957b48856efa7"

_ZERO_COUNTS = ReconciliationCountSummary(
    by_classification=tuple(
        (classification, 0)
        for classification in sorted(ReconciliationClassification, key=lambda item: item.value)
    ),
    source_record_count=0,
    target_record_count=0,
    canonical_key_count=0,
    source_quarantined_count=0,
    target_quarantined_count=0,
    quarantine_breakdown=(),
)


def _golden_analysis() -> ReconciliationAnalysis:
    source = (
        SourceObservation(0, SOURCE_CONNECTOR, wire_payload(sku="GRID-A", source_record_key="s-a")),
        SourceObservation(
            1, SOURCE_CONNECTOR, wire_payload(sku="GRID-B", source_record_key="s-b", quantity=2)
        ),
        SourceObservation(2, SOURCE_CONNECTOR, wire_payload(sku="GRID-C", source_record_key="s-c")),
    )
    target = (
        SourceObservation(0, SOURCE_CONNECTOR, wire_payload(sku="GRID-A", source_record_key="t-a")),
        SourceObservation(
            1, SOURCE_CONNECTOR, wire_payload(sku="GRID-B", source_record_key="t-b", quantity=3)
        ),
    )
    return analyze_reconciliation(
        ReconciliationAnalysisRequest(
            source_observations=source,
            target_observations=target,
            source_input_identity=_SOURCE_IDENTITY,
            target_input_identity=_TARGET_IDENTITY,
        )
    )


def test_golden_counts_reproduce_exactly() -> None:
    counts = _golden_analysis().summary.counts
    by_classification = dict(counts.by_classification)
    assert by_classification[ReconciliationClassification.MATCH] == 1
    assert by_classification[ReconciliationClassification.FIELD_MISMATCH] == 1
    assert by_classification[ReconciliationClassification.MISSING_FROM_TARGET] == 1
    assert counts.source_record_count == 3
    assert counts.target_record_count == 2
    assert counts.canonical_key_count == 3


def test_golden_fingerprint_is_locked_to_exact_bytes() -> None:
    summary = _golden_analysis().summary
    assert summary.fingerprint == StateFingerprint(_GOLDEN_FINGERPRINT)


def test_fingerprint_is_stable_across_repeated_computation() -> None:
    assert _golden_analysis().summary.fingerprint == _golden_analysis().summary.fingerprint


def test_fingerprint_is_sensitive_to_each_changed_input() -> None:
    baseline = _golden_analysis().summary
    changed_target = analyze_reconciliation(
        ReconciliationAnalysisRequest(
            source_observations=(
                SourceObservation(
                    0, SOURCE_CONNECTOR, wire_payload(sku="GRID-A", source_record_key="s-a")
                ),
                SourceObservation(
                    1,
                    SOURCE_CONNECTOR,
                    wire_payload(sku="GRID-B", source_record_key="s-b", quantity=2),
                ),
                SourceObservation(
                    2, SOURCE_CONNECTOR, wire_payload(sku="GRID-C", source_record_key="s-c")
                ),
            ),
            target_observations=(
                SourceObservation(
                    0, SOURCE_CONNECTOR, wire_payload(sku="GRID-A", source_record_key="t-a")
                ),
                SourceObservation(
                    1,
                    SOURCE_CONNECTOR,
                    wire_payload(sku="GRID-B", source_record_key="t-b", quantity=4),
                ),
            ),
            source_input_identity=_SOURCE_IDENTITY,
            target_input_identity=_TARGET_IDENTITY,
        )
    ).summary
    assert baseline.fingerprint != changed_target.fingerprint

    changed_identity = analyze_reconciliation(
        ReconciliationAnalysisRequest(
            source_observations=(
                SourceObservation(
                    0, SOURCE_CONNECTOR, wire_payload(sku="GRID-A", source_record_key="s-a")
                ),
            ),
            target_observations=(
                SourceObservation(
                    0, SOURCE_CONNECTOR, wire_payload(sku="GRID-A", source_record_key="t-a")
                ),
            ),
            source_input_identity=sha256(b"changed").hexdigest(),
            target_input_identity=_TARGET_IDENTITY,
        )
    ).summary
    assert changed_identity.fingerprint != baseline.fingerprint
    assert changed_identity.fingerprint != changed_target.fingerprint


def test_fingerprint_kind_and_version_are_explicit() -> None:
    summary = _golden_analysis().summary
    assert summary.fingerprint_kind == RECONCILIATION_FINGERPRINT_KIND == "reconciliation"
    assert summary.fingerprint_version == RECONCILIATION_FINGERPRINT_VERSION == 1
    assert summary.analysis_version == RECONCILIATION_ANALYSIS_VERSION == 1
    assert summary.source_input_identity == _SOURCE_IDENTITY
    assert summary.target_input_identity == _TARGET_IDENTITY


def test_reconciliation_fingerprint_is_not_a_reused_domain_digest() -> None:
    analysis = _golden_analysis()
    outcome_digest = fingerprint_state(
        (key.outcome for key in analysis.classification.keys),
        scope=FingerprintScope.RECONCILIATION_STATE,
    )
    assert analysis.summary.fingerprint.value != outcome_digest.value
    assert analysis.summary.fingerprint_version != 2


def test_count_summary_contract_enforces_closed_coverage() -> None:
    assert _ZERO_COUNTS.source_record_count == 0
    with pytest.raises(ValueError, match="exactly once"):
        ReconciliationCountSummary(
            by_classification=_ZERO_COUNTS.by_classification[:-1],
            source_record_count=0,
            target_record_count=0,
            canonical_key_count=0,
            source_quarantined_count=0,
            target_quarantined_count=0,
            quarantine_breakdown=(),
        )
    with pytest.raises(ValueError, match="cover every quarantined"):
        ReconciliationCountSummary(
            by_classification=_ZERO_COUNTS.by_classification,
            source_record_count=0,
            target_record_count=0,
            canonical_key_count=0,
            source_quarantined_count=1,
            target_quarantined_count=0,
            quarantine_breakdown=(),
        )


def test_quarantine_breakdown_covers_every_quarantined_observation() -> None:
    null_payload = dict(wire_payload(sku="GRID-Z", source_record_key="s-z"))
    null_payload["quantity"] = None  # type: ignore[assignment]
    summary = analyze_reconciliation(
        ReconciliationAnalysisRequest(
            source_observations=(
                SourceObservation(
                    0, SOURCE_CONNECTOR, wire_payload(sku="GRID-A", source_record_key="s-a")
                ),
                SourceObservation(1, SOURCE_CONNECTOR, null_payload),
                SourceObservation(2, SOURCE_CONNECTOR, dict(null_payload, sku="GRID-Y")),
            ),
            target_observations=(
                SourceObservation(
                    0, SOURCE_CONNECTOR, wire_payload(sku="GRID-A", source_record_key="t-a")
                ),
            ),
            source_input_identity=_SOURCE_IDENTITY,
            target_input_identity=_TARGET_IDENTITY,
        )
    ).summary
    assert summary.counts.source_quarantined_count == 2
    breakdown = {(item.side, item.code): item.count for item in summary.counts.quarantine_breakdown}
    assert breakdown == {(RecordSide.SOURCE, QuarantineCode.NULL_FIELD): 2}


def test_quarantine_count_contract() -> None:
    assert QuarantineCount(RecordSide.SOURCE, QuarantineCode.NULL_FIELD, 2).count == 2
    with pytest.raises(ValueError, match="positive integer"):
        QuarantineCount(RecordSide.SOURCE, QuarantineCode.NULL_FIELD, 0)


def test_compute_reconciliation_fingerprint_validates_inputs() -> None:
    digest = fingerprint_state((), scope=FingerprintScope.RECONCILIATION_STATE)
    with pytest.raises(ValueError, match="identity"):
        compute_reconciliation_fingerprint(
            source_input_identity="not-a-digest",
            target_input_identity=_TARGET_IDENTITY,
            analytical_query_version=1,
            counts=_ZERO_COUNTS,
            outcome_state_digest=digest,
        )
    with pytest.raises(ValueError, match="query version"):
        compute_reconciliation_fingerprint(
            source_input_identity=_SOURCE_IDENTITY,
            target_input_identity=_TARGET_IDENTITY,
            analytical_query_version=0,
            counts=_ZERO_COUNTS,
            outcome_state_digest=digest,
        )
    with pytest.raises(TypeError):
        compute_reconciliation_fingerprint(
            source_input_identity=_SOURCE_IDENTITY,
            target_input_identity=_TARGET_IDENTITY,
            analytical_query_version=1,
            counts="invalid",  # type: ignore[arg-type]
            outcome_state_digest=digest,
        )
    with pytest.raises(TypeError, match="outcome state digest"):
        compute_reconciliation_fingerprint(
            source_input_identity=_SOURCE_IDENTITY,
            target_input_identity=_TARGET_IDENTITY,
            analytical_query_version=1,
            counts=_ZERO_COUNTS,
            outcome_state_digest="invalid",  # type: ignore[arg-type]
        )


def test_build_summary_requires_exact_contract_types() -> None:
    from paritygrid.domain.reconciliation import SourceNormalization, build_reconciliation_summary

    digest = fingerprint_state((), scope=FingerprintScope.RECONCILIATION_STATE)
    with pytest.raises(TypeError):
        build_reconciliation_summary(
            classification="invalid",  # type: ignore[arg-type]
            source_normalization=SourceNormalization(1, (), ()),
            target_normalization=SourceNormalization(1, (), ()),
            source_input_identity=_SOURCE_IDENTITY,
            target_input_identity=_TARGET_IDENTITY,
            analytical_query_version=1,
            outcome_state_digest=digest,
        )


@given(seed=st.integers(min_value=1, max_value=40_000))
def test_generated_dataset_fingerprint_reproduces_from_inputs(seed: int) -> None:
    from paritygrid.demo.datasets import (
        DatasetProfile,
        ScenarioSeed,
        ScenarioVersion,
        generate_dataset,
    )

    dataset = generate_dataset(
        ScenarioSeed(seed), ScenarioVersion(1), DatasetProfile(record_count=16, malformed_count=2)
    )
    observations = tuple(
        SourceObservation(row.index, SOURCE_CONNECTOR, dict(row.payload)) for row in dataset.rows
    )
    request = ReconciliationAnalysisRequest(
        source_observations=observations,
        target_observations=(),
        source_input_identity=_SOURCE_IDENTITY,
        target_input_identity=_TARGET_IDENTITY,
    )
    first = analyze_reconciliation(request).summary
    second = analyze_reconciliation(request).summary
    assert first.fingerprint == second.fingerprint
    assert isinstance(first, ReconciliationSummary)
    assert isinstance(analyze_reconciliation(request).classification, ClassificationResult)
