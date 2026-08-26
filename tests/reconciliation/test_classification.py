"""Classification completeness, exclusivity, and secondary-evidence tests."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from paritygrid.application.reconciliation import (
    ReconciliationAnalysis,
    ReconciliationAnalysisRequest,
    analyze_reconciliation,
)
from paritygrid.domain.reconciliation import (
    ReconciliationClassification,
    RecordClassification,
    RecordSide,
    SecondaryEvidenceKind,
    SourceObservation,
    SuggestedResolution,
    classify_matches,
    match_by_canonical_key,
    normalize_source_observations,
)
from tests.reconciliation.conftest import SOURCE_CONNECTOR, wire_payload

_IDENTITIES = (
    "1111111111111111111111111111111111111111111111111111111111111111",
    "2222222222222222222222222222222222222222222222222222222222222222",
)


def _analyze(
    source_payloads: list[dict[str, object]],
    target_payloads: list[dict[str, object]],
) -> ReconciliationAnalysis:
    return analyze_reconciliation(
        ReconciliationAnalysisRequest(
            source_observations=tuple(
                SourceObservation(index, SOURCE_CONNECTOR, payload)
                for index, payload in enumerate(source_payloads)
            ),
            target_observations=tuple(
                SourceObservation(index, SOURCE_CONNECTOR, payload)
                for index, payload in enumerate(target_payloads)
            ),
            source_input_identity=_IDENTITIES[0],
            target_input_identity=_IDENTITIES[1],
        )
    )


def _golden_analysis() -> ReconciliationAnalysis:
    return _analyze(
        [
            wire_payload(sku="GRID-A", source_record_key="s-a"),
            wire_payload(sku="GRID-B", source_record_key="s-b", quantity=2),
            wire_payload(sku="GRID-C", source_record_key="s-c"),
            wire_payload(sku="GRID-D", source_record_key="s-d1"),
            wire_payload(sku="GRID-D", source_record_key="s-d2"),
            wire_payload(sku="GRID-E", source_record_key="s-e1"),
            wire_payload(sku="GRID-E", source_record_key="s-e2"),
        ],
        [
            wire_payload(sku="GRID-A", source_record_key="t-a"),
            wire_payload(sku="GRID-B", source_record_key="t-b", quantity=3),
            wire_payload(sku="GRID-F", source_record_key="t-f"),
            wire_payload(sku="GRID-G", source_record_key="t-g1"),
            wire_payload(sku="GRID-G", source_record_key="t-g2"),
            wire_payload(sku="GRID-E", source_record_key="t-e1"),
            wire_payload(sku="GRID-E", source_record_key="t-e2"),
        ],
    )


def test_every_classification_is_reachable_and_counted_once() -> None:
    analysis = _golden_analysis()
    counts = dict(analysis.summary.counts.by_classification)
    assert counts == {
        ReconciliationClassification.DUPLICATE_BOTH: 1,
        ReconciliationClassification.DUPLICATE_SOURCE: 1,
        ReconciliationClassification.DUPLICATE_TARGET: 1,
        ReconciliationClassification.FIELD_MISMATCH: 1,
        ReconciliationClassification.MATCH: 1,
        ReconciliationClassification.MISSING_FROM_SOURCE: 1,
        ReconciliationClassification.MISSING_FROM_TARGET: 1,
    }


def test_every_record_is_classified_exactly_once() -> None:
    analysis = _golden_analysis()
    records = analysis.classification.records
    assert len(records) == 7 + 7
    identities = [(record.side, record.position) for record in records]
    assert len(set(identities)) == len(identities)
    assert identities == sorted(identities, key=lambda item: (item[0].value, item[1]))
    key_by_sku = {key.outcome.sku: key for key in analysis.classification.keys}
    for record in records:
        assert record.classification is key_by_sku[record.sku].outcome.classification


def test_repeated_record_keys_are_classified_with_lossless_provenance() -> None:
    analysis = _analyze(
        [
            wire_payload(sku="GRID-A", source_record_key="z-key", quantity=1),
            wire_payload(sku="GRID-A", source_record_key="a-key", quantity=2),
            wire_payload(sku="GRID-B", source_record_key="shared", quantity=1),
            wire_payload(sku="GRID-B", source_record_key="shared", quantity=2),
        ],
        [],
    )

    conflicts = {row.sku: row for row in analysis.conflicts}
    assert conflicts["GRID-A"].source_positions == (0, 1)
    assert conflicts["GRID-A"].source_record_keys == ("z-key", "a-key")
    assert conflicts["GRID-B"].source_positions == (2, 3)
    assert conflicts["GRID-B"].source_record_keys == ("shared", "shared")
    assert analysis.collisions[1].member_keys == ("shared", "shared")
    assert len(analysis.classification.records) == 4


def test_record_classifications_follow_key_group_members() -> None:
    analysis = _golden_analysis()
    by_sku_side: dict[tuple[str, RecordSide], list[RecordClassification]] = {}
    for record in analysis.classification.records:
        by_sku_side.setdefault((record.sku, record.side), []).append(record)
    assert [item.classification for item in by_sku_side[("GRID-D", RecordSide.SOURCE)]] == [
        ReconciliationClassification.DUPLICATE_SOURCE,
        ReconciliationClassification.DUPLICATE_SOURCE,
    ]
    assert by_sku_side.get(("GRID-C", RecordSide.TARGET), []) == []
    assert by_sku_side.get(("GRID-F", RecordSide.SOURCE), []) == []
    assert by_sku_side[("GRID-G", RecordSide.TARGET)][0].classification is (
        ReconciliationClassification.DUPLICATE_TARGET
    )


def test_secondary_evidence_enriches_without_replacing_classification() -> None:
    analysis = _golden_analysis()
    key_by_sku = {key.outcome.sku: key for key in analysis.classification.keys}
    mismatch_secondary = key_by_sku["GRID-B"].secondary
    assert [(item.kind, item.value) for item in mismatch_secondary] == [
        (SecondaryEvidenceKind.MISMATCH_FIELDS, "quantity"),
    ]
    assert key_by_sku["GRID-B"].differences[0].field == "quantity"
    duplicate_secondary = key_by_sku["GRID-D"].secondary
    assert (duplicate_secondary[0].kind, duplicate_secondary[0].value) == (
        SecondaryEvidenceKind.IDENTICAL_DUPLICATE_CONTENT,
        "1",
    )


def test_opponent_content_match_names_the_matching_member() -> None:
    analysis = _analyze(
        [
            wire_payload(sku="GRID-A", source_record_key="s-a1", quantity=5),
            wire_payload(sku="GRID-A", source_record_key="s-a2", quantity=9),
        ],
        [wire_payload(sku="GRID-A", source_record_key="t-a1", quantity=9)],
    )
    key = analysis.classification.keys[0]
    evidence = {item.kind: item.value for item in key.secondary}
    assert evidence[SecondaryEvidenceKind.OPPONENT_CONTENT_MATCHES_MEMBER] == "s-a2"
    counts = dict(analysis.summary.counts.by_classification)
    assert counts[ReconciliationClassification.DUPLICATE_SOURCE] == 1


def test_suggested_resolution_matrix_is_complete() -> None:
    analysis = _golden_analysis()
    expected = {
        "GRID-A": SuggestedResolution.NONE,
        "GRID-B": SuggestedResolution.UPDATE_TARGET,
        "GRID-C": SuggestedResolution.CREATE_TARGET,
        "GRID-D": SuggestedResolution.REVIEW_DUPLICATES,
        "GRID-E": SuggestedResolution.REVIEW_DUPLICATES,
        "GRID-F": SuggestedResolution.REVIEW_TARGET_ONLY,
        "GRID-G": SuggestedResolution.REVIEW_DUPLICATES,
    }
    actual = {key.outcome.sku: key.suggested_resolution for key in analysis.classification.keys}
    assert actual == expected
    conflicts = {row.sku: row for row in analysis.conflicts}
    assert set(conflicts) == set(expected) - {"GRID-A"}
    assert conflicts["GRID-C"].source_positions == (2,)
    assert conflicts["GRID-C"].target_positions == ()
    assert conflicts["GRID-D"].source_positions == (3, 4)
    assert conflicts["GRID-D"].source_record_keys == ("s-d1", "s-d2")


def test_quarantined_observations_are_evidence_not_canonical_records() -> None:
    source_payloads = [
        wire_payload(sku="GRID-A", source_record_key="s-a"),
        wire_payload(sku="GRID-B", source_record_key="s-b", quantity=-1),
    ]
    analysis = _analyze(source_payloads, [wire_payload(sku="GRID-A", source_record_key="t-a")])
    assert len(analysis.classification.records) == 2
    assert analysis.summary.counts.source_quarantined_count == 1
    assert len(analysis.source_quarantined) == 1
    counts = dict(analysis.summary.counts.by_classification)
    assert counts[ReconciliationClassification.MATCH] == 1


def test_classification_result_rejects_incomplete_or_divergent_records() -> None:
    from dataclasses import replace

    analysis = _golden_analysis()
    kept = analysis.classification.records[:-1]
    with pytest.raises(ValueError, match="exactly once"):
        replace(analysis.classification, records=kept)


@given(
    source_quantity=st.integers(min_value=0, max_value=50),
    target_quantity=st.integers(min_value=0, max_value=50),
)
def test_classification_agrees_with_domain_outcome_contract(
    source_quantity: int, target_quantity: int
) -> None:
    analysis = _analyze(
        [wire_payload(sku="GRID-A", source_record_key="s-a", quantity=source_quantity)],
        [wire_payload(sku="GRID-A", source_record_key="t-a", quantity=target_quantity)],
    )
    key = analysis.classification.keys[0]
    expected = (
        ReconciliationClassification.MATCH
        if source_quantity == target_quantity
        else ReconciliationClassification.FIELD_MISMATCH
    )
    assert key.outcome.classification is expected
    assert bool(key.differences) == (source_quantity != target_quantity)
    if expected is ReconciliationClassification.MATCH:
        assert analysis.conflicts == ()
    else:
        assert len(analysis.conflicts) == 1


@given(
    seed=st.integers(min_value=1, max_value=30_000),
)
def test_generated_dataset_analysis_is_complete_and_exclusive(seed: int) -> None:
    from paritygrid.demo.datasets import (
        DatasetProfile,
        ScenarioSeed,
        ScenarioVersion,
        generate_dataset,
    )

    dataset = generate_dataset(
        ScenarioSeed(seed), ScenarioVersion(1), DatasetProfile(record_count=20, malformed_count=3)
    )
    observations = [
        SourceObservation(row.index, SOURCE_CONNECTOR, dict(row.payload)) for row in dataset.rows
    ]
    normalization = normalize_source_observations(observations)
    matches = match_by_canonical_key(normalization.records, ())
    result = classify_matches(matches)
    assert len(result.records) == len(normalization.records)
    identities = [(record.side, record.position) for record in result.records]
    assert len(set(identities)) == len(identities)
    total_members = sum(
        len(key.outcome.source_records) + len(key.outcome.target_records) for key in result.keys
    )
    assert total_members == len(result.records)
    reversed_matches = match_by_canonical_key(
        normalize_source_observations(list(reversed(observations))).records, ()
    )
    assert classify_matches(reversed_matches).records == result.records
