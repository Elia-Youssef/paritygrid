"""Contract-boundary tests: every defensive validation branch of Phase 10."""

from dataclasses import replace
from typing import Any, cast

import pytest

from paritygrid.application.reconciliation import (
    ConflictPublicationError,
    ReconciliationAnalysisRequest,
    analyze_reconciliation,
    publish_conflict_artifact,
)
from paritygrid.domain.models import StateFingerprint, UtcTimestamp
from paritygrid.domain.reconciliation import (
    CanonicalKeyCollision,
    CanonicalKeyMatch,
    ClassificationResult,
    ClassifiedKey,
    ComparisonDocument,
    ComparisonDocumentError,
    ComparisonValue,
    ComparisonValueKind,
    DuplicateRecordGroup,
    FieldDifference,
    FieldDifferenceKind,
    MatchingError,
    NormalizationError,
    NormalizedRecord,
    QuarantineCode,
    QuarantineCount,
    QuarantinedObservation,
    ReconciliationClassification,
    ReconciliationCountSummary,
    ReconciliationSummary,
    RecordClassification,
    RecordSide,
    SecondaryEvidence,
    SecondaryEvidenceKind,
    SourceNormalization,
    SourceObservation,
    SuggestedResolution,
    build_field_differences,
    classify_matches,
    match_by_canonical_key,
    normalize_source_observations,
)
from paritygrid.domain.reconciliation.normalization import MAX_PAYLOAD_FIELDS
from paritygrid.domain.reconciliation.outcomes import ReconciliationOutcome
from paritygrid.domain.reconciliation.summaries import (
    build_reconciliation_summary,
)
from tests.reconciliation.conftest import SOURCE_CONNECTOR, source_observation, wire_payload

_DIGEST = StateFingerprint("0" * 64)


def _normalized(position: int = 0) -> NormalizedRecord:
    result = normalize_source_observations([source_observation(position, wire_payload())])
    return result.records[0]


def _classification() -> ClassificationResult:
    records = tuple(_normalized(index) for index in range(2))
    matches = (
        CanonicalKeyMatch(sku="GRID-0001", source_records=records[:1], target_records=records[1:]),
    )
    return classify_matches(matches)


def _counts() -> ReconciliationCountSummary:
    return ReconciliationCountSummary(
        by_classification=tuple(
            (classification, 0)
            for classification in sorted(ReconciliationClassification, key=lambda item: item.value)
        ),
        source_record_count=1,
        target_record_count=1,
        canonical_key_count=1,
        source_quarantined_count=0,
        target_quarantined_count=0,
        quarantine_breakdown=(),
    )


# ---------------------------------------------------------------- differences


def test_comparison_value_leaf_contract_rejects_wrong_payloads() -> None:
    with pytest.raises(TypeError):
        ComparisonValue(kind="text")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="requires text"):
        ComparisonValue(kind=ComparisonValueKind.TEXT)
    with pytest.raises(ValueError, match="requires integer"):
        ComparisonValue(kind=ComparisonValueKind.INTEGER)
    with pytest.raises(ValueError, match="requires money"):
        ComparisonValue(kind=ComparisonValueKind.MONEY_AMOUNT)
    with pytest.raises(ValueError, match="requires timestamp"):
        ComparisonValue(kind=ComparisonValueKind.TIMESTAMP)
    with pytest.raises(ValueError, match="must not carry"):
        ComparisonValue(
            kind=ComparisonValueKind.MISSING,
            text="extra",  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="must not carry"):
        ComparisonValue(kind=ComparisonValueKind.NULL, integer=3)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="supported range"):
        ComparisonValue.integer_value(2**63)
    with pytest.raises(ValueError, match="bounded evidence"):
        ComparisonValue(kind=ComparisonValueKind.WRONG_TYPE, text="")


def test_comparison_value_leaf_factories_and_renderings() -> None:
    assert ComparisonValue.missing().canonical_text() == ""
    assert ComparisonValue.null().canonical_text() == "null"
    assert ComparisonValue.wrong_type(object()).canonical_text()
    assert ComparisonValue.wrong_type({"b": 1, "a": 2}).canonical_text() == '{"a":2,"b":1}'
    long_raw = "x" * 600
    assert len(ComparisonValue.wrong_type(long_raw).canonical_text()) == 512
    with pytest.raises(ValueError, match="size limit"):
        ComparisonValue.text_value("x" * 513)
    with pytest.raises(TypeError):
        ComparisonValue.text_value(7)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        ComparisonValue(ComparisonValueKind.MISSING).is_equivalent("not-a-leaf")  # type: ignore[arg-type]


def test_comparison_document_contract_rejects_wrong_members() -> None:
    leaf = ComparisonValue.text_value("A")
    with pytest.raises(TypeError):
        ComparisonDocument(values=["name"])  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        ComparisonDocument(values=(("name", "not-a-leaf"),))  # pyright: ignore[reportArgumentType]
    with pytest.raises(TypeError):
        ComparisonDocument(values=(("name", leaf), ("quantity", "bad")))  # pyright: ignore[reportArgumentType]
    with pytest.raises(ComparisonDocumentError, match="not canonical"):
        ComparisonDocument(values=(("not-a-field", leaf),))
    with pytest.raises(ComparisonDocumentError, match="not canonical"):
        ComparisonDocument(values=(("attributes/BAD", leaf),))
    with pytest.raises(ComparisonDocumentError, match="byte limit"):
        ComparisonDocument(values=((f"attributes/{'k' * 200}", leaf),))
    empty = ComparisonDocument.empty()
    assert empty.paths() == ()
    assert empty.get("name").kind is ComparisonValueKind.MISSING


def test_field_difference_contract_rejects_wrong_members() -> None:
    with pytest.raises(TypeError):
        FieldDifference(
            field="name",
            kind="value_mismatch",  # type: ignore[arg-type]
            source_text="a",
            target_text="b",
        )
    with pytest.raises(TypeError):
        FieldDifference(
            field=7,  # type: ignore[arg-type]
            kind=FieldDifferenceKind.VALUE_MISMATCH,
            source_text="a",
            target_text="b",
        )
    with pytest.raises(ComparisonDocumentError, match="not canonical"):
        FieldDifference(
            field="bad field",
            kind=FieldDifferenceKind.VALUE_MISMATCH,
            source_text="a",
            target_text="b",
        )
    with pytest.raises(ValueError, match="size limit"):
        FieldDifference(
            field="name",
            kind=FieldDifferenceKind.VALUE_MISMATCH,
            source_text="x" * 513,
            target_text="b",
        )
    with pytest.raises(TypeError):
        build_field_differences(ComparisonDocument.empty(), None)  # type: ignore[arg-type]


# ------------------------------------------------------------------ matching


def test_canonical_key_match_contract_rejects_wrong_members() -> None:
    record = _normalized()
    with pytest.raises(MatchingError, match="nonempty SKU"):
        CanonicalKeyMatch(sku="")
    with pytest.raises(TypeError):
        CanonicalKeyMatch(sku="GRID-0001", source_records=[record])  # type: ignore[arg-type]
    with pytest.raises(MatchingError, match="ordered unique positions"):
        CanonicalKeyMatch(sku="GRID-0001", source_records=(replace(record, position=5), record))
    with pytest.raises(MatchingError, match="share one SKU"):
        CanonicalKeyMatch(
            sku="GRID-0001",
            source_records=(
                _normalized(),
                normalize_source_observations(
                    [source_observation(1, wire_payload(sku="GRID-0002"))]
                ).records[0],
            ),
        )


def test_collision_and_duplicate_group_contracts() -> None:
    with pytest.raises(TypeError):
        CanonicalKeyCollision(side="source", sku="GRID-A", record_count=2, member_keys=("a", "b"))  # type: ignore[arg-type]
    with pytest.raises(MatchingError, match="nonempty SKU"):
        CanonicalKeyCollision(
            side=RecordSide.SOURCE, sku="", record_count=2, member_keys=("a", "b")
        )
    record_a = _normalized()
    record_b = replace(record_a, position=1)
    with pytest.raises(TypeError):
        DuplicateRecordGroup(
            side="source",  # type: ignore[arg-type]
            sku="GRID-0001",
            members=(record_a, record_b),
            distinct_contents=1,
            identical_members=True,
        )
    with pytest.raises(MatchingError, match="at least two members"):
        DuplicateRecordGroup(
            side=RecordSide.SOURCE,
            sku="GRID-0001",
            members=(record_a,),
            distinct_contents=1,
            identical_members=True,
        )
    with pytest.raises(MatchingError, match="content count"):
        DuplicateRecordGroup(
            side=RecordSide.SOURCE,
            sku="GRID-0001",
            members=(record_a, record_b),
            distinct_contents=0,
            identical_members=False,
        )
    with pytest.raises(MatchingError, match="identity flag"):
        DuplicateRecordGroup(
            side=RecordSide.SOURCE,
            sku="GRID-0001",
            members=(record_a, record_b),
            distinct_contents=2,
            identical_members=True,
        )


def test_collision_and_duplicate_detection_reject_foreign_matches() -> None:
    from paritygrid.domain.reconciliation import (
        detect_canonical_key_collisions,
        detect_duplicate_record_groups,
    )

    with pytest.raises(TypeError, match="CanonicalKeyMatch"):
        detect_canonical_key_collisions(["nope"])  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="CanonicalKeyMatch"):
        detect_duplicate_record_groups(["nope"])  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="NormalizedRecord"):
        match_by_canonical_key(["nope"], ())  # pyright: ignore[reportArgumentType]


# ------------------------------------------------------------- classification


def test_classification_value_contracts_reject_wrong_members() -> None:
    with pytest.raises(TypeError):
        SecondaryEvidence(kind="mismatch_fields", value="name")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="nonempty"):
        SecondaryEvidence(kind=SecondaryEvidenceKind.MISMATCH_FIELDS, value="")
    with pytest.raises(ValueError, match="size limit"):
        SecondaryEvidence(kind=SecondaryEvidenceKind.MISMATCH_FIELDS, value="x" * 300)
    with pytest.raises(TypeError):
        RecordClassification(
            side="source",  # type: ignore[arg-type]
            position=0,
            sku="GRID-A",
            source_record_key="s",
            classification=ReconciliationClassification.MATCH,
        )
    for overrides in (
        {"position": -1},
        {"sku": ""},
        {"source_record_key": ""},
        {"classification": "match"},  # type: ignore[dict-item]
        {"secondary": "none"},  # type: ignore[dict-item]
    ):
        with pytest.raises((TypeError, ValueError)):
            RecordClassification(
                side=RecordSide.SOURCE,
                position=0,
                sku="GRID-A",
                source_record_key="s",
                classification=ReconciliationClassification.MATCH,
                **overrides,  # type: ignore[arg-type]
            )


def test_classified_key_and_result_contracts_reject_wrong_members() -> None:
    key = _classification().keys[0]
    with pytest.raises(TypeError):
        ClassifiedKey(
            outcome="match",  # type: ignore[arg-type]
            differences=(),
            secondary=(),
            suggested_resolution=SuggestedResolution.NONE,
        )
    with pytest.raises(TypeError):
        ClassifiedKey(
            outcome=key.outcome,
            differences="none",  # type: ignore[arg-type]
            secondary=(),
            suggested_resolution=SuggestedResolution.NONE,
        )
    with pytest.raises(TypeError):
        ClassifiedKey(
            outcome=key.outcome,
            differences=(),
            secondary="none",  # type: ignore[arg-type]
            suggested_resolution=SuggestedResolution.NONE,
        )
    with pytest.raises(TypeError):
        ClassifiedKey(
            outcome=key.outcome,
            differences=(),
            secondary=(),
            suggested_resolution="none",  # type: ignore[arg-type]
        )
    with pytest.raises(TypeError):
        ClassificationResult(keys=[], records=())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="sorted unique SKUs"):
        ClassificationResult(keys=(key, key), records=_classification().records)
    with pytest.raises(ValueError, match="unique identities"):
        ClassificationResult(
            keys=(key,),
            records=(
                _classification().records[0],
                _classification().records[0],
            ),
        )
    with pytest.raises(ValueError, match="exactly once"):
        ClassificationResult(keys=(key,), records=(_classification().records[0],))
    complete = _classification().records
    disagreeing = (
        replace(complete[0], classification=ReconciliationClassification.DUPLICATE_SOURCE),
        complete[1],
    )
    with pytest.raises(ValueError, match="agree with their key"):
        ClassificationResult(keys=(key,), records=disagreeing)
    complete_records = _classification().records
    foreign_provenance = (
        replace(complete_records[0], source_record_key="not-a-member"),
        complete_records[1],
    )
    with pytest.raises(ValueError, match="provenance"):
        ClassificationResult(keys=(key,), records=foreign_provenance)
    with pytest.raises(TypeError, match="a tuple"):
        classify_matches([key])  # type: ignore[arg-type]


def test_duplicate_secondary_evidence_covers_divergent_content() -> None:
    source = (_normalized(0), _other_quantity_record(1))
    matches = (CanonicalKeyMatch(sku="GRID-0001", source_records=source),)
    result = classify_matches(matches)
    evidence = {item.kind: item.value for item in result.keys[0].secondary}
    assert evidence[SecondaryEvidenceKind.DISTINCT_DUPLICATE_CONTENT] == "2"
    assert evidence[SecondaryEvidenceKind.OPPONENT_CONTENT_MATCHES_MEMBER] == "none"


def _other_quantity_record(position: int) -> NormalizedRecord:
    return normalize_source_observations(
        [source_observation(position, wire_payload(quantity=9))]
    ).records[0]


# ------------------------------------------------------------- normalization


def test_source_observation_contract_rejects_wrong_members() -> None:
    with pytest.raises(NormalizationError, match="nonnegative integer"):
        SourceObservation(-1, SOURCE_CONNECTOR, wire_payload())
    with pytest.raises(TypeError):
        SourceObservation(0, "con_demo", wire_payload())  # type: ignore[arg-type]
    with pytest.raises(NormalizationError, match="must be a mapping"):
        SourceObservation(0, SOURCE_CONNECTOR, "not-a-mapping")  # type: ignore[arg-type]
    with pytest.raises(NormalizationError, match="nonempty text"):
        SourceObservation(0, SOURCE_CONNECTOR, None, malformed_reason="")
    with pytest.raises(NormalizationError, match="size limit"):
        SourceObservation(0, SOURCE_CONNECTOR, None, malformed_reason="x" * 201)
    with pytest.raises(NormalizationError, match="must not carry"):
        SourceObservation(0, SOURCE_CONNECTOR, wire_payload(), "reason")


def test_normalization_result_contracts_reject_wrong_members() -> None:
    record = _normalized()
    with pytest.raises(NormalizationError, match="nonnegative"):
        NormalizedRecord(position=-1, record=record.record, document=record.document)
    with pytest.raises(TypeError):
        NormalizedRecord(position=0, record="record", document=record.document)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        NormalizedRecord(position=0, record=record.record, document="doc")  # type: ignore[arg-type]
    with pytest.raises(NormalizationError, match="nonnegative"):
        QuarantinedObservation(
            position=-1,
            connector_id=SOURCE_CONNECTOR,
            code=QuarantineCode.NULL_FIELD,
            field="quantity",
            detail="null",
        )
    with pytest.raises(TypeError):
        QuarantinedObservation(
            position=0,
            connector_id=SOURCE_CONNECTOR,
            code="null_field",  # type: ignore[arg-type]
            field="quantity",
            detail="null",
        )
    for field, detail, key in (
        ("", "detail", ""),
        ("x" * 200, "detail", ""),
        ("field", "", ""),
        ("field", "detail", "x" * 200),
    ):
        with pytest.raises(NormalizationError):
            QuarantinedObservation(
                position=0,
                connector_id=SOURCE_CONNECTOR,
                code=QuarantineCode.NULL_FIELD,
                field=field,
                detail=detail,
                source_record_key=key,
            )
    with pytest.raises(NormalizationError, match="rules version"):
        SourceNormalization(2, (record,), ())
    with pytest.raises(TypeError):
        SourceNormalization(1, [record], ())  # type: ignore[arg-type]
    with pytest.raises(NormalizationError, match="ordered unique"):
        SourceNormalization(1, (replace(record, position=3), record), ())
    with pytest.raises(NormalizationError, match="ordered unique"):
        SourceNormalization(1, (record, record), ())
    with pytest.raises(TypeError, match="SourceObservation"):
        normalize_source_observations(["nope"])  # type: ignore[arg-type]


def test_normalization_rejects_oversized_and_foreign_payloads() -> None:
    result = normalize_source_observations(
        [source_observation(0, dict(wire_payload(), name="x" * 600))]
    )
    assert result.quarantined[0].code is QuarantineCode.WRONG_TYPE
    assert result.quarantined[0].field == "name"

    payload = wire_payload()
    for index in range(MAX_PAYLOAD_FIELDS):
        payload[f"extra-{index}"] = 1
    observation = SourceObservation(0, SOURCE_CONNECTOR, payload)
    result = normalize_source_observations([observation])
    assert result.quarantined[0].code is QuarantineCode.WRONG_TYPE
    assert result.quarantined[0].field == "payload"

    object_payload = wire_payload()
    object_payload["unit_price"] = {"amount": "12.34", "currency": 7}  # type: ignore[assignment]
    result = normalize_source_observations([source_observation(1, object_payload)])
    assert result.quarantined[0].code is QuarantineCode.WRONG_TYPE
    assert result.quarantined[0].field == "unit_price/currency"


# ------------------------------------------------------------------ summaries


def test_count_summary_contract_rejects_wrong_members() -> None:
    counts = _counts()
    with pytest.raises(TypeError):
        ReconciliationCountSummary(
            by_classification=list(counts.by_classification),  # type: ignore[arg-type]
            source_record_count=1,
            target_record_count=1,
            canonical_key_count=1,
            source_quarantined_count=0,
            target_quarantined_count=0,
            quarantine_breakdown=(),
        )
    with pytest.raises(TypeError, match="typed pairs"):
        ReconciliationCountSummary(
            by_classification=tuple(
                (classification, "1")  # type: ignore[list-item]
                for classification in sorted(
                    ReconciliationClassification, key=lambda item: item.value
                )
            ),
            source_record_count=1,
            target_record_count=1,
            canonical_key_count=1,
            source_quarantined_count=0,
            target_quarantined_count=0,
            quarantine_breakdown=(),
        )
    negative = cast("dict[str, Any]", {**_fields(counts), "source_record_count": -1})
    with pytest.raises(ValueError, match="source_record_count"):
        ReconciliationCountSummary(**negative)
    unsorted = cast(
        "dict[str, Any]",
        {
            **_fields(counts),
            "source_quarantined_count": 2,
            "quarantine_breakdown": (
                QuarantineCount(RecordSide.SOURCE, QuarantineCode.NULL_FIELD, 1),
                QuarantineCount(RecordSide.SOURCE, QuarantineCode.MISSING_FIELD, 1),
            ),
        },
    )
    with pytest.raises(ValueError, match="sorted and unique"):
        ReconciliationCountSummary(**unsorted)
    wrong_breakdown = cast("dict[str, Any]", {**_fields(counts), "quarantine_breakdown": ("nope",)})
    with pytest.raises(TypeError):
        ReconciliationCountSummary(**wrong_breakdown)


def _fields(counts: ReconciliationCountSummary) -> dict[str, object]:
    return {
        "by_classification": counts.by_classification,
        "source_record_count": counts.source_record_count,
        "target_record_count": counts.target_record_count,
        "canonical_key_count": counts.canonical_key_count,
        "source_quarantined_count": counts.source_quarantined_count,
        "target_quarantined_count": counts.target_quarantined_count,
        "quarantine_breakdown": counts.quarantine_breakdown,
    }


def test_summary_contract_rejects_wrong_kinds_and_versions() -> None:
    summary = _summary()
    for overrides in (
        {"fingerprint_kind": "other"},
        {"fingerprint_version": 2},
        {"analysis_version": 2},
        {"rules_version": 2},
        {"counts": "counts"},  # type: ignore[dict-item]
        {"source_input_identity": "nope"},
        {"target_input_identity": "nope"},
        {"analytical_query_version": 0},
        {"fingerprint": "digest"},  # type: ignore[dict-item]
    ):
        with pytest.raises((TypeError, ValueError)):
            ReconciliationSummary(
                **cast("dict[str, Any]", {**_summary_fields(summary), **overrides})
            )


def _summary() -> ReconciliationSummary:
    classification = _classification()
    source = normalize_source_observations([source_observation(0, wire_payload())])
    target = normalize_source_observations(
        [source_observation(0, wire_payload(sku="GRID-0001", source_record_key="t"))]
    )
    return build_reconciliation_summary(
        classification=classification,
        source_normalization=source,
        target_normalization=target,
        source_input_identity="0" * 64,
        target_input_identity="1" * 64,
        analytical_query_version=1,
        outcome_state_digest=_DIGEST,
    )


def _summary_fields(summary: ReconciliationSummary) -> dict[str, object]:
    return {
        "fingerprint_kind": summary.fingerprint_kind,
        "fingerprint_version": summary.fingerprint_version,
        "analysis_version": summary.analysis_version,
        "rules_version": summary.rules_version,
        "counts": summary.counts,
        "source_input_identity": summary.source_input_identity,
        "target_input_identity": summary.target_input_identity,
        "analytical_query_version": summary.analytical_query_version,
        "fingerprint": summary.fingerprint,
    }


def test_build_summary_rejects_foreign_normalizations() -> None:
    classification = _classification()
    with pytest.raises(TypeError, match="SourceNormalization"):
        build_reconciliation_summary(
            classification=classification,
            source_normalization="nope",  # type: ignore[arg-type]
            target_normalization="nope",  # type: ignore[arg-type]
            source_input_identity="0" * 64,
            target_input_identity="1" * 64,
            analytical_query_version=1,
            outcome_state_digest=_DIGEST,
        )


def test_analysis_result_contract_rejects_wrong_members() -> None:
    analysis = analyze_reconciliation(_request())
    for name in ("conflicts", "matches", "collisions", "duplicate_groups"):
        with pytest.raises(TypeError, match="tuple"):
            replace(analysis, **{name: list(getattr(analysis, name))})  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="summary"):
        replace(analysis, summary="nope")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="classification"):
        replace(analysis, classification="nope")  # type: ignore[arg-type]


def _request() -> ReconciliationAnalysisRequest:
    return ReconciliationAnalysisRequest(
        source_observations=(source_observation(0, wire_payload()),),
        target_observations=(
            source_observation(0, wire_payload(sku="GRID-0001", source_record_key="t")),
        ),
        source_input_identity="0" * 64,
        target_input_identity="1" * 64,
    )


def test_publication_requires_manifest_repository_contract() -> None:
    class Writer:
        def write_conflicts(self, **_kwargs: object) -> None:
            raise AssertionError("must not be called")

    with pytest.raises(ConflictPublicationError, match="ArtifactManifestRepository"):
        publish_conflict_artifact(
            writer=Writer(),  # type: ignore[arg-type]
            manifests=object(),  # type: ignore[arg-type]
            artifact_id="art_conflicts-x",  # type: ignore[arg-type]
            run_id="run_reconciliation",  # type: ignore[arg-type]
            node_id="nod_reconcile",  # type: ignore[arg-type]
            partition_key="page-0001",  # type: ignore[arg-type]
            partition_number=0,
            batch="batch",  # type: ignore[arg-type]
            created_at=UtcTimestamp.parse("2026-08-25T12:00:00.000000Z"),
        )


def test_publication_reraises_nested_publication_failures() -> None:
    class ExplodingWriter:
        def write_conflicts(self, **_kwargs: object) -> None:
            raise ConflictPublicationError("writer refused")

    class Manifests:
        def register(self, **_kwargs: object) -> None:
            raise AssertionError("must not be called")

    with pytest.raises(ConflictPublicationError, match="writer refused"):
        publish_conflict_artifact(
            writer=ExplodingWriter(),  # type: ignore[arg-type]
            manifests=Manifests(),  # type: ignore[arg-type]
            artifact_id="art_conflicts-x",  # type: ignore[arg-type]
            run_id="run_reconciliation",  # type: ignore[arg-type]
            node_id="nod_reconcile",  # type: ignore[arg-type]
            partition_key="page-0001",  # type: ignore[arg-type]
            partition_number=0,
            batch="batch",  # type: ignore[arg-type]
            created_at=UtcTimestamp.parse("2026-08-25T12:00:00.000000Z"),
        )


# ---------------------------------------------------- normalization branches


def test_normalization_covers_marker_and_type_branches() -> None:
    cases: tuple[tuple[dict[str, object], QuarantineCode, str]] = (
        (dict(wire_payload(), name=7), QuarantineCode.WRONG_TYPE, "name"),  # type: ignore[arg-type]
        (dict(wire_payload(), updated_at=11), QuarantineCode.WRONG_TYPE, "updated_at"),  # type: ignore[arg-type]
        (dict(wire_payload(), unit_price=None), QuarantineCode.NULL_FIELD, "unit_price"),  # type: ignore[arg-type]
        (
            dict(wire_payload(), unit_price={"amount": "12.34", "currency": None}),  # type: ignore[assignment]
            QuarantineCode.NULL_FIELD,
            "unit_price/currency",
        ),
        (
            dict(wire_payload(), unit_price={"amount": 12, "currency": "USD"}),  # type: ignore[assignment]
            QuarantineCode.WRONG_TYPE,
            "unit_price/amount",
        ),
        (
            dict(wire_payload(), unit_price={"amount": "x" * 600, "currency": "USD"}),  # type: ignore[assignment]
            QuarantineCode.WRONG_TYPE,
            "unit_price/amount",
        ),
        (dict(wire_payload(), attributes=None), QuarantineCode.NULL_FIELD, "attributes"),  # type: ignore[assignment]
        (dict(wire_payload(), attributes="blue"), QuarantineCode.WRONG_TYPE, "attributes"),  # type: ignore[assignment]
        (
            dict(wire_payload(), attributes={"color": None}),  # type: ignore[assignment]
            QuarantineCode.NULL_FIELD,
            "attributes/color",
        ),
        (
            dict(wire_payload(), attributes={"color": 7}),  # type: ignore[assignment]
            QuarantineCode.WRONG_TYPE,
            "attributes/color",
        ),
        (
            dict(wire_payload(), attributes={"color": "x" * 600}),  # type: ignore[assignment]
            QuarantineCode.WRONG_TYPE,
            "attributes/color",
        ),
    )
    for index, (payload, code, field) in enumerate(cases):
        result = normalize_source_observations([source_observation(index, payload)])
        quarantined = result.quarantined[0]
        assert quarantined.code is code, (quarantined.code, field)
        assert quarantined.field == field, (quarantined.field, field)


def test_normalization_reports_smallest_noncanonical_attribute_key() -> None:
    payload = dict(wire_payload())
    payload["attributes"] = {"Zed": "1", "Alpha": "2"}  # type: ignore[assignment]
    result = normalize_source_observations([source_observation(0, payload)])
    quarantined = result.quarantined[0]
    assert quarantined.code is QuarantineCode.INVALID_VALUE
    assert "Alpha" in quarantined.detail


def test_normalization_probes_attributes_for_domain_failures() -> None:
    payload = dict(wire_payload())
    payload["attributes"] = {f"k{index:02d}": "v" for index in range(33)}  # type: ignore[assignment]
    result = normalize_source_observations([source_observation(0, payload)])
    quarantined = result.quarantined[0]
    assert quarantined.code is QuarantineCode.INVALID_VALUE
    assert quarantined.field == "attributes"


def test_quarantine_key_drops_oversize_provenance() -> None:
    payload = dict(wire_payload())
    payload["quantity"] = None  # type: ignore[assignment]
    payload["source_record_key"] = "k" * 200
    result = normalize_source_observations([source_observation(0, payload)])
    assert result.quarantined[0].source_record_key == ""


# ------------------------------------------------------- matching branches


def test_match_records_helper_and_per_side_limits() -> None:
    record = _normalized()
    match = CanonicalKeyMatch(sku="GRID-0001", source_records=(record,))
    assert match.record_count(RecordSide.SOURCE) == 1
    assert match.record_count(RecordSide.TARGET) == 0
    from paritygrid.domain.reconciliation.outcomes import ReconciliationOutcome

    limit = ReconciliationOutcome.MAX_RECORDS_PER_SIDE
    overflow = tuple(replace(record, position=index) for index in range(limit + 1))
    with pytest.raises(MatchingError, match="per-side record limit"):
        CanonicalKeyMatch(sku="GRID-0001", source_records=overflow)
    observations = [
        SourceObservation(
            position=index,
            connector_id=SOURCE_CONNECTOR,
            payload=dict(wire_payload(), source_record_key=f"s{index}"),
        )
        for index in range(limit + 1)
    ]
    records = normalize_source_observations(observations).records
    with pytest.raises(MatchingError, match="per-side duplicate record limit"):
        match_by_canonical_key(records, ())


def test_duplicate_group_contract_rejects_mixed_and_foreign_members() -> None:
    record = _normalized()
    other_sku = normalize_source_observations(
        [source_observation(1, wire_payload(sku="GRID-0002"))]
    ).records[0]
    with pytest.raises(MatchingError, match="share one SKU"):
        DuplicateRecordGroup(
            side=RecordSide.SOURCE,
            sku="GRID-0001",
            members=(record, other_sku),
            distinct_contents=2,
            identical_members=False,
        )
    limit = ReconciliationOutcome.MAX_RECORDS_PER_SIDE
    with pytest.raises(MatchingError, match="per-side record limit"):
        DuplicateRecordGroup(
            side=RecordSide.SOURCE,
            sku="GRID-0001",
            members=tuple(replace(record, position=index) for index in range(limit + 1)),
            distinct_contents=1,
            identical_members=True,
        )


# -------------------------------------------------- classification branches


def test_classification_result_rejects_unsorted_records_and_foreign_matches() -> None:
    result = _classification()
    source_record, target_record = result.records
    with pytest.raises(ValueError, match="sorted by side"):
        ClassificationResult(keys=result.keys, records=(target_record, source_record))
    with pytest.raises(TypeError, match="CanonicalKeyMatch"):
        classify_matches(("nope",))  # type: ignore[arg-type]


def test_record_classification_rejects_wrong_secondary() -> None:
    with pytest.raises(TypeError, match="secondary"):
        RecordClassification(
            side=RecordSide.SOURCE,
            position=0,
            sku="GRID-0001",
            source_record_key="s",
            classification=ReconciliationClassification.MATCH,
            secondary="none",  # type: ignore[arg-type]
        )


# ------------------------------------------------------ summaries branches


def test_quarantine_count_rejects_wrong_member_types() -> None:
    with pytest.raises(TypeError):
        QuarantineCount("source", QuarantineCode.NULL_FIELD, 1)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        QuarantineCount(RecordSide.SOURCE, "null_field", 1)  # type: ignore[arg-type]


def test_count_summary_rejects_negative_classification_counts() -> None:
    with pytest.raises(ValueError, match="nonnegative"):
        ReconciliationCountSummary(
            by_classification=tuple(
                (classification, -1 if classification is ReconciliationClassification.MATCH else 0)
                for classification in sorted(
                    ReconciliationClassification, key=lambda item: item.value
                )
            ),
            source_record_count=1,
            target_record_count=1,
            canonical_key_count=1,
            source_quarantined_count=0,
            target_quarantined_count=0,
            quarantine_breakdown=(),
        )
