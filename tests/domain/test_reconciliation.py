"""Example verification for complete reconciliation outcomes."""

from itertools import product

import pytest

from paritygrid.domain.models import (
    ConnectorId,
    InventoryAttributes,
    InventoryRecord,
    Money,
    UtcTimestamp,
)
from paritygrid.domain.reconciliation import (
    FieldMismatch,
    ReconciliationClassification,
    ReconciliationField,
    ReconciliationOutcome,
    differences_between,
)


def _record(
    *,
    sku: str = "SKU-001",
    name: str = "Widget",
    quantity: int = 10,
    price: str = "12.34",
    updated_at: str = "2026-08-12T10:00:00Z",
    connector: str = "con_source-api",
    source_key: str = "source-001",
    attributes: dict[str, str] | None = None,
) -> InventoryRecord:
    return InventoryRecord.create(
        sku=sku,
        name=name,
        quantity=quantity,
        unit_price=Money.parse(f"USD {price}"),
        updated_at=UtcTimestamp.parse(updated_at),
        connector_id=ConnectorId(connector),
        source_record_key=source_key,
        attributes=attributes or {"color": "blue"},
    )


def _records(count: int, *, side: str) -> tuple[InventoryRecord, ...]:
    return tuple(
        _record(
            connector=f"con_{side}-{index}",
            source_key=f"{side}-{index}",
        )
        for index in range(count)
    )


CARDINALITY_CLASSIFICATIONS = {
    (0, 1): ReconciliationClassification.MISSING_FROM_SOURCE,
    (0, 2): ReconciliationClassification.DUPLICATE_TARGET,
    (1, 0): ReconciliationClassification.MISSING_FROM_TARGET,
    (1, 1): ReconciliationClassification.MATCH,
    (1, 2): ReconciliationClassification.DUPLICATE_TARGET,
    (2, 0): ReconciliationClassification.DUPLICATE_SOURCE,
    (2, 1): ReconciliationClassification.DUPLICATE_SOURCE,
    (2, 2): ReconciliationClassification.DUPLICATE_BOTH,
}


@pytest.mark.parametrize(("source_count", "target_count"), CARDINALITY_CLASSIFICATIONS)
def test_every_nonempty_record_cardinality_has_one_primary_classification(
    source_count: int, target_count: int
) -> None:
    outcome = ReconciliationOutcome(
        source_records=_records(source_count, side="source"),
        target_records=_records(target_count, side="target"),
    )

    assert outcome.classification is CARDINALITY_CLASSIFICATIONS[(source_count, target_count)]
    assert isinstance(outcome.classification, ReconciliationClassification)
    assert outcome.mismatches == ()


def test_business_field_differences_have_fixed_order_and_exact_values() -> None:
    source = _record()
    target = _record(
        name="Older Widget",
        quantity=3,
        price="11.00",
        updated_at="2026-08-11T10:00:00Z",
        connector="con_target-api",
        source_key="target-001",
        attributes={"color": "red"},
    )

    outcome = ReconciliationOutcome((source,), (target,))

    assert outcome.classification is ReconciliationClassification.FIELD_MISMATCH
    assert tuple(mismatch.field for mismatch in outcome.mismatches) == tuple(ReconciliationField)
    assert outcome.mismatches[0] == FieldMismatch(
        ReconciliationField.NAME,
        "Widget",
        "Older Widget",
    )
    assert outcome.mismatches[-1] == FieldMismatch(
        ReconciliationField.ATTRIBUTES,
        InventoryAttributes.from_mapping({"color": "blue"}),
        InventoryAttributes.from_mapping({"color": "red"}),
    )
    assert differences_between(source, target) == outcome.mismatches
    assert outcome.is_repairable


def test_provenance_fields_do_not_change_business_parity() -> None:
    source = _record()
    target = _record(connector="con_target-api", source_key="target-record")

    outcome = ReconciliationOutcome((source,), (target,))

    assert outcome.classification is ReconciliationClassification.MATCH
    assert not outcome.is_repairable


@pytest.mark.parametrize(
    ("source_count", "target_count"),
    [counts for counts in product(range(3), repeat=2) if counts != (1, 0) and counts != (1, 1)],
)
def test_only_unambiguous_source_desired_states_are_repairable(
    source_count: int, target_count: int
) -> None:
    if source_count == target_count == 0:
        with pytest.raises(ValueError, match="at least one"):
            ReconciliationOutcome((), ())
        return
    outcome = ReconciliationOutcome(
        _records(source_count, side="source"),
        _records(target_count, side="target"),
    )

    assert not outcome.is_repairable


def test_record_order_is_canonical_for_duplicate_evidence() -> None:
    first = _record(connector="con_source-z", source_key="z")
    second = _record(connector="con_source-a", source_key="a")

    forward = ReconciliationOutcome((first, second), ())
    reverse = ReconciliationOutcome((second, first), ())

    assert forward == reverse
    assert hash(forward) == hash(reverse)
    assert tuple(record.source_record_key for record in forward.source_records) == ("a", "z")


def test_outcome_rejects_wrong_containers_records_and_mixed_keys() -> None:
    with pytest.raises(TypeError, match="source records must be a tuple"):
        ReconciliationOutcome([_record()], ())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="target records must contain"):
        ReconciliationOutcome((), ("record",))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="share one SKU"):
        ReconciliationOutcome((_record(), _record(sku="SKU-002")), ())


def test_outcome_bounds_duplicate_evidence_per_side() -> None:
    record = _record()
    at_limit = (record,) * ReconciliationOutcome.MAX_RECORDS_PER_SIDE

    outcome = ReconciliationOutcome(at_limit, ())

    assert outcome.classification is ReconciliationClassification.DUPLICATE_SOURCE
    with pytest.raises(ValueError, match="source records must contain at most"):
        ReconciliationOutcome((*at_limit, record), ())


def test_difference_builder_rejects_wrong_values_and_keys() -> None:
    with pytest.raises(TypeError, match="InventoryRecord"):
        differences_between(_record(), "target")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="matching SKUs"):
        differences_between(_record(), _record(sku="SKU-002"))


@pytest.mark.parametrize(
    ("field", "source", "target", "message"),
    [
        ("name", "a", "b", "ReconciliationField"),
        (ReconciliationField.NAME, 1, "b", "source value"),
        (ReconciliationField.NAME, "a", 1, "target value"),
        (ReconciliationField.QUANTITY, True, 1, "source value"),
    ],
)
def test_field_mismatch_rejects_wrong_field_value_types(
    field: object, source: object, target: object, message: str
) -> None:
    with pytest.raises(TypeError, match=message):
        FieldMismatch(field, source, target)  # type: ignore[arg-type]


def test_field_mismatch_rejects_equal_values() -> None:
    with pytest.raises(ValueError, match="must differ"):
        FieldMismatch(ReconciliationField.NAME, "same", "same")
