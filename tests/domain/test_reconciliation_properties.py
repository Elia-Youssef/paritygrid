"""Property verification for reconciliation exclusivity and evidence."""

from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from paritygrid.domain.models import ConnectorId, InventoryRecord, Money, UtcTimestamp
from paritygrid.domain.reconciliation import (
    ReconciliationClassification,
    ReconciliationField,
    ReconciliationOutcome,
)


def _record(
    *,
    name: str,
    quantity: int,
    price_minor_units: int,
    day: int,
    color: str,
    source_key: str,
) -> InventoryRecord:
    return InventoryRecord.create(
        sku="SKU-001",
        name=name,
        quantity=quantity,
        unit_price=Money(
            Decimal(price_minor_units).scaleb(-2),
            Money.parse("USD 0.00").currency,
            2,
        ),
        updated_at=UtcTimestamp.parse(f"2026-08-{day:02d}T10:00:00Z"),
        connector_id=ConnectorId("con_property-source"),
        source_record_key=source_key,
        attributes={"color": color},
    )


@given(
    source_name=st.sampled_from(("Widget", "Gadget")),
    target_name=st.sampled_from(("Widget", "Gadget")),
    source_quantity=st.integers(min_value=0, max_value=20),
    target_quantity=st.integers(min_value=0, max_value=20),
    source_price=st.integers(min_value=0, max_value=2_000),
    target_price=st.integers(min_value=0, max_value=2_000),
    source_day=st.integers(min_value=1, max_value=28),
    target_day=st.integers(min_value=1, max_value=28),
    source_color=st.sampled_from(("blue", "red")),
    target_color=st.sampled_from(("blue", "red")),
)
def test_field_evidence_is_complete_and_exclusive(
    source_name: str,
    target_name: str,
    source_quantity: int,
    target_quantity: int,
    source_price: int,
    target_price: int,
    source_day: int,
    target_day: int,
    source_color: str,
    target_color: str,
) -> None:
    source = _record(
        name=source_name,
        quantity=source_quantity,
        price_minor_units=source_price,
        day=source_day,
        color=source_color,
        source_key="source",
    )
    target = _record(
        name=target_name,
        quantity=target_quantity,
        price_minor_units=target_price,
        day=target_day,
        color=target_color,
        source_key="target",
    )
    outcome = ReconciliationOutcome((source,), (target,))
    expected_fields = {
        field
        for field, is_different in (
            (ReconciliationField.NAME, source_name != target_name),
            (ReconciliationField.QUANTITY, source_quantity != target_quantity),
            (ReconciliationField.UNIT_PRICE, source_price != target_price),
            (ReconciliationField.UPDATED_AT, source_day != target_day),
            (ReconciliationField.ATTRIBUTES, source_color != target_color),
        )
        if is_different
    }

    assert {mismatch.field for mismatch in outcome.mismatches} == expected_fields
    assert outcome.classification is (
        ReconciliationClassification.FIELD_MISMATCH
        if expected_fields
        else ReconciliationClassification.MATCH
    )


@given(st.lists(st.integers(min_value=0, max_value=50), min_size=2, max_size=8, unique=True))
def test_duplicate_input_order_does_not_change_outcome(keys: list[int]) -> None:
    records = tuple(
        _record(
            name="Widget",
            quantity=1,
            price_minor_units=100,
            day=1,
            color="blue",
            source_key=f"source-{key:02d}",
        )
        for key in keys
    )

    assert ReconciliationOutcome(records, ()) == ReconciliationOutcome(tuple(reversed(records)), ())
