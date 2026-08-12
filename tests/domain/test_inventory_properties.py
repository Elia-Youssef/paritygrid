"""Property verification for canonical inventory records."""

import unicodedata
from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from paritygrid.domain.models import (
    ConnectorId,
    CurrencyCode,
    InventoryAttributes,
    InventoryRecord,
    Money,
    UtcTimestamp,
)

_SAFE_TEXT = st.text(
    alphabet=st.characters(categories=("Lu", "Ll", "Lt", "Lm", "Lo", "Mn", "Mc", "Me", "Nd")),
    min_size=1,
    max_size=40,
)


def _base_record(*, name: str, quantity: int, attributes: InventoryAttributes) -> InventoryRecord:
    return InventoryRecord(
        sku="SKU-0001",
        name=name,
        quantity=quantity,
        unit_price=Money(
            amount=Decimal("1.25"),
            currency=CurrencyCode.parse("USD"),
            minor_unit_exponent=2,
        ),
        updated_at=UtcTimestamp.parse("2026-01-01T00:00:00Z"),
        connector_id=ConnectorId.parse("con_source-001"),
        source_record_key="source-001",
        attributes=attributes,
    )


@given(_SAFE_TEXT, st.integers(min_value=0, max_value=2_147_483_647))
def test_inventory_record_normalizes_safe_unicode_and_preserves_quantity(
    name: str, quantity: int
) -> None:
    record = _base_record(name=name, quantity=quantity, attributes=InventoryAttributes())

    assert record.name == unicodedata.normalize("NFC", name)
    assert record.quantity == quantity


@given(
    st.dictionaries(
        keys=st.from_regex(r"[a-z][a-z0-9]{0,7}", fullmatch=True),
        values=st.text(alphabet="abcdefghijklmnopqrstuvwxyz", max_size=20),
        max_size=12,
    )
)
def test_attribute_equality_is_independent_of_input_order(values: dict[str, str]) -> None:
    forward = InventoryAttributes.from_mapping(values)
    reverse = InventoryAttributes(items=tuple(reversed(tuple(values.items()))))

    assert forward == reverse
    assert hash(forward) == hash(reverse)
