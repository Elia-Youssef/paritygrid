"""Golden and property tests for the field-difference builder."""

from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from paritygrid.domain.models import CurrencyCode, Money
from paritygrid.domain.reconciliation import (
    ComparisonDocument,
    ComparisonDocumentError,
    ComparisonValue,
    FieldDifferenceKind,
    NormalizedRecord,
    build_field_differences,
    normalize_observation,
)
from paritygrid.domain.reconciliation.differences import canonical_attribute_path
from tests.reconciliation.conftest import source_observation, wire_payload


def _document(pairs: tuple[tuple[str, ComparisonValue], ...]) -> ComparisonDocument:
    return ComparisonDocument(values=tuple(sorted(pairs)))


def test_difference_kind_matrix_is_total_and_stable() -> None:
    money = Money(Decimal("12.34"), CurrencyCode("USD"), 2)
    source = _document(
        (
            ("attributes/color", ComparisonValue.attribute_text_value("blue")),
            ("name", ComparisonValue.text_value("A")),
            ("quantity", ComparisonValue.integer_value(5)),
            ("unit_price/amount", ComparisonValue.money_amount_value(money)),
            ("updated_at", ComparisonValue.text_value("2024-01-01T00:00:00.000000Z")),
        )
    )
    target = _document(
        (
            ("attributes/grade", ComparisonValue.attribute_text_value("a-1")),
            ("name", ComparisonValue.wrong_type(7)),
            ("quantity", ComparisonValue.null()),
            ("unit_price/amount", ComparisonValue.text_value("12.34")),
            ("updated_at", ComparisonValue.null()),
        )
    )
    differences = build_field_differences(source, target)
    assert [(difference.field, difference.kind) for difference in differences] == [
        ("attributes/color", FieldDifferenceKind.MISSING_ON_TARGET),
        ("attributes/grade", FieldDifferenceKind.MISSING_ON_SOURCE),
        ("name", FieldDifferenceKind.TYPE_MISMATCH),
        ("quantity", FieldDifferenceKind.NULL_ON_TARGET),
        ("unit_price/amount", FieldDifferenceKind.TYPE_MISMATCH),
        ("updated_at", FieldDifferenceKind.NULL_ON_TARGET),
    ]
    assert differences[0].source_text == "blue"
    assert differences[0].target_text == ""


def test_value_mismatch_reports_canonical_renderings() -> None:
    source = _document((("quantity", ComparisonValue.integer_value(5)),))
    target = _document((("quantity", ComparisonValue.integer_value(6)),))
    difference = build_field_differences(source, target)[0]
    assert difference.kind is FieldDifferenceKind.VALUE_MISMATCH
    assert difference.source_text == "5"
    assert difference.target_text == "6"


def test_null_on_source_and_missing_on_source_kinds() -> None:
    source = _document(
        (
            ("name", ComparisonValue.null()),
            ("quantity", ComparisonValue.integer_value(1)),
        )
    )
    target = _document(
        (
            ("name", ComparisonValue.text_value("A")),
            ("quantity", ComparisonValue.integer_value(1)),
            ("updated_at", ComparisonValue.text_value("2024-01-01T00:00:00.000000Z")),
        )
    )
    differences = build_field_differences(source, target)
    assert [(difference.field, difference.kind) for difference in differences] == [
        ("name", FieldDifferenceKind.NULL_ON_SOURCE),
        ("updated_at", FieldDifferenceKind.MISSING_ON_SOURCE),
    ]


def test_unicode_normalized_values_produce_no_difference() -> None:
    source = _document((("name", ComparisonValue.text_value("Cafe\u0301 valve")),))
    target = _document((("name", ComparisonValue.text_value("Caf\u00e9 valve")),))
    assert build_field_differences(source, target) == ()


def test_equivalent_money_amounts_produce_no_difference() -> None:
    source = _document(
        (
            (
                "unit_price/amount",
                ComparisonValue.money_amount_value(Money(Decimal("12.34"), CurrencyCode("USD"), 2)),
            ),
        )
    )
    target = _document(
        (
            (
                "unit_price/amount",
                ComparisonValue.money_amount_value(
                    Money(Decimal("12.340"), CurrencyCode("USD"), 2)
                ),
            ),
        )
    )
    assert build_field_differences(source, target) == ()


def test_equal_documents_produce_no_differences() -> None:
    document = _document(
        (
            ("name", ComparisonValue.text_value("A")),
            ("quantity", ComparisonValue.integer_value(5)),
        )
    )
    assert build_field_differences(document, document) == ()


def test_comparison_document_contract_rejects_noncanonical_input() -> None:
    with pytest.raises(TypeError):
        ComparisonDocument(values=(("name", "not-a-leaf"),))  # pyright: ignore[reportArgumentType]
    with pytest.raises(ComparisonDocumentError, match="sorted"):
        ComparisonDocument(
            values=(
                ("quantity", ComparisonValue.integer_value(1)),
                ("name", ComparisonValue.text_value("A")),
            )
        )
    with pytest.raises(ComparisonDocumentError, match="unique"):
        ComparisonDocument(
            values=(
                ("name", ComparisonValue.text_value("A")),
                ("name", ComparisonValue.text_value("B")),
            )
        )
    with pytest.raises(ComparisonDocumentError, match="not canonical"):
        ComparisonDocument(values=(("sku", ComparisonValue.text_value("A")),))
    with pytest.raises(TypeError):
        build_field_differences("not-a-document", ComparisonDocument.empty())


def test_canonical_attribute_path_accepts_domain_keys_only() -> None:
    assert canonical_attribute_path("bin-12") == "attributes/bin-12"
    assert canonical_attribute_path("lane_two") == "attributes/lane_two"
    assert canonical_attribute_path("Color") is None
    assert canonical_attribute_path(7) is None


def test_nested_attribute_differences_cover_added_and_removed_keys() -> None:
    source = _document(
        (
            ("attributes/color", ComparisonValue.attribute_text_value("blue")),
            ("attributes/grade", ComparisonValue.attribute_text_value("a-1")),
        )
    )
    target = _document(
        (
            ("attributes/color", ComparisonValue.attribute_text_value("red")),
            ("attributes/origin", ComparisonValue.attribute_text_value("north-line")),
        )
    )
    differences = build_field_differences(source, target)
    assert [(difference.field, difference.kind) for difference in differences] == [
        ("attributes/color", FieldDifferenceKind.VALUE_MISMATCH),
        ("attributes/grade", FieldDifferenceKind.MISSING_ON_TARGET),
        ("attributes/origin", FieldDifferenceKind.MISSING_ON_SOURCE),
    ]


def test_normalized_payload_documents_support_stable_differences() -> None:
    source_payload = wire_payload(sku="GRID-0001", source_record_key="s1", quantity=5)
    target_payload = wire_payload(sku="GRID-0001", source_record_key="t1", quantity=6)
    source = normalize_observation(source_observation(0, source_payload))
    target = normalize_observation(source_observation(0, target_payload))
    assert isinstance(source, NormalizedRecord)
    assert isinstance(target, NormalizedRecord)
    differences = build_field_differences(source.document, target.document)
    assert [(difference.field, difference.kind) for difference in differences] == [
        ("quantity", FieldDifferenceKind.VALUE_MISMATCH),
    ]


@given(
    left=st.integers(min_value=0, max_value=1_000),
    right=st.integers(min_value=0, max_value=1_000),
)
def test_integer_difference_equivalence_matches_equality(left: int, right: int) -> None:
    source = _document((("quantity", ComparisonValue.integer_value(left)),))
    target = _document((("quantity", ComparisonValue.integer_value(right)),))
    differences = build_field_differences(source, target)
    assert bool(differences) == (left != right)
    if differences:
        assert differences[0].source_text == str(left)
        assert differences[0].target_text == str(right)


@given(
    text=st.text(min_size=1, max_size=40),
)
def test_text_leaves_compare_after_nfc_normalization(text: str) -> None:
    import unicodedata

    source = _document((("name", ComparisonValue.text_value(text)),))
    target = _document((("name", ComparisonValue.text_value(text + "\u0301")),))
    differences = build_field_differences(source, target)
    combining = text + "\u0301"
    assert bool(differences) == (
        unicodedata.normalize("NFC", text) != unicodedata.normalize("NFC", combining)
    )
