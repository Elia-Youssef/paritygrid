"""Example-based verification of canonical inventory records."""

from collections.abc import Iterator, Mapping
from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from paritygrid.domain.models import (
    ConnectorId,
    CurrencyCode,
    InventoryAttributes,
    InventoryRecord,
    Money,
    UtcTimestamp,
)


class _MoneySubclass(Money):
    pass


def _record(**changes: object) -> InventoryRecord:
    values: dict[str, object] = {
        "sku": "SKU-0042",
        "name": "Café Mug",
        "quantity": 12,
        "unit_price": Money(
            amount=Decimal("19.95"),
            currency=CurrencyCode.parse("USD"),
            minor_unit_exponent=2,
        ),
        "updated_at": UtcTimestamp.parse("2026-08-12T09:30:00Z"),
        "connector_id": ConnectorId.parse("con_legacy-source"),
        "source_record_key": "legacy-0042",
        "attributes": InventoryAttributes.from_mapping({"color": "Crème", "size": "Large"}),
    }
    values.update(changes)
    return InventoryRecord(**values)  # type: ignore[arg-type]


def test_inventory_record_normalizes_unicode_nfc() -> None:
    record = _record(
        name="Cafe\u0301 Mug",
        source_record_key="cafe\u0301-0042",
        attributes=InventoryAttributes.from_mapping({"finish": "Cafe\u0301"}),
    )

    assert record.name == "Café Mug"
    assert record.source_record_key == "café-0042"
    assert record.attributes.get("finish") == "Café"


def test_inventory_attributes_are_sorted_copied_and_immutable() -> None:
    source = {"size": "Large", "color": "Blue"}
    attributes = InventoryAttributes.from_mapping(source)
    source["color"] = "Red"

    assert attributes.items == (("color", "Blue"), ("size", "Large"))
    assert tuple(attributes) == attributes.items
    assert len(attributes) == 2
    assert attributes.get("color") == "Blue"
    assert attributes.get("missing") is None
    assert attributes.get("missing", "fallback") == "fallback"
    assert hash(attributes) == hash(
        InventoryAttributes(items=(("size", "Large"), ("color", "Blue")))
    )
    with pytest.raises(FrozenInstanceError):
        attributes.items = ()  # type: ignore[misc]


def test_inventory_record_creation_copies_mapping_and_uses_explicit_values() -> None:
    source = {"warehouse": "North"}
    record = InventoryRecord.create(
        sku="SKU-0001",
        name="Desk Lamp",
        quantity=5,
        unit_price=Money.parse("USD 39.50"),
        updated_at=UtcTimestamp.parse("2026-01-02T03:04:05Z"),
        connector_id=ConnectorId.parse("con_async-source"),
        source_record_key="source-1",
        attributes=source,
    )
    source["warehouse"] = "South"

    assert record.attributes.get("warehouse") == "North"
    assert record.updated_at == UtcTimestamp.parse("2026-01-02T03:04:05Z")


def test_inventory_record_creation_defaults_to_empty_attributes() -> None:
    record = InventoryRecord.create(
        sku="SKU-0001",
        name="Desk Lamp",
        quantity=5,
        unit_price=Money.parse("USD 39.50"),
        updated_at=UtcTimestamp.parse("2026-01-02T03:04:05Z"),
        connector_id=ConnectorId.parse("con_async-source"),
        source_record_key="source-1",
    )

    assert record.attributes == InventoryAttributes()


def test_inventory_record_equality_and_hash_are_independent_of_mapping_order() -> None:
    first = _record(attributes=InventoryAttributes.from_mapping({"size": "Large", "color": "Blue"}))
    second = _record(
        attributes=InventoryAttributes.from_mapping({"color": "Blue", "size": "Large"})
    )

    assert first == second
    assert hash(first) == hash(second)


@pytest.mark.parametrize(
    "value",
    ["", "sku-1", "SKU_1", "SKU--1", "-SKU", "SKU-", "SKU/1", "É-SKU", "A" * 65],
)
def test_inventory_record_rejects_invalid_sku(value: str) -> None:
    with pytest.raises(ValueError, match="SKU"):
        _record(sku=value)


def test_inventory_record_rejects_non_text_sku() -> None:
    with pytest.raises(TypeError, match="SKU"):
        _record(sku=42)


@pytest.mark.parametrize(
    "value",
    ["", " Leading", "Trailing ", "Two  spaces", "Line\nbreak", "Tab\tvalue", "Zero\u200bwidth"],
)
def test_inventory_record_rejects_noncanonical_name_text(value: str) -> None:
    with pytest.raises(ValueError, match="inventory name"):
        _record(name=value)


def test_inventory_record_rejects_unencodable_unicode_as_invalid_text() -> None:
    with pytest.raises(ValueError, match="unsupported code point"):
        _record(name="Widget\ud800")


def test_inventory_record_rejects_name_character_and_byte_limits() -> None:
    with pytest.raises(ValueError, match="size"):
        _record(name="A" * 161)
    with pytest.raises(ValueError, match="size"):
        _record(name="😀" * 160)


def test_inventory_record_rejects_non_text_name_and_source_key() -> None:
    with pytest.raises(TypeError, match="inventory name"):
        _record(name=42)
    with pytest.raises(TypeError, match="source record key"):
        _record(source_record_key=42)


@pytest.mark.parametrize("value", ["", " source", "source ", "source\tkey", "source\u202ekey"])
def test_inventory_record_rejects_noncanonical_source_key(value: str) -> None:
    with pytest.raises(ValueError, match="source record key"):
        _record(source_record_key=value)


def test_inventory_record_rejects_source_key_limits() -> None:
    with pytest.raises(ValueError, match="size"):
        _record(source_record_key="A" * 129)
    with pytest.raises(ValueError, match="size"):
        _record(source_record_key="😀" * 65)


@pytest.mark.parametrize("value", [-1, 2_147_483_648])
def test_inventory_record_rejects_out_of_range_quantity(value: int) -> None:
    with pytest.raises(ValueError, match="quantity"):
        _record(quantity=value)


@pytest.mark.parametrize("value", [True, 1.0, "1"])
def test_inventory_record_rejects_non_integer_quantity(value: object) -> None:
    with pytest.raises(TypeError, match="integer"):
        _record(quantity=value)


def test_inventory_record_rejects_negative_unit_price() -> None:
    with pytest.raises(ValueError, match="negative"):
        _record(unit_price=Money.parse("USD -0.01"))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("unit_price", Decimal("1.00"), "Money"),
        ("updated_at", "2026-01-01T00:00:00Z", "UtcTimestamp"),
        ("connector_id", "con_source", "ConnectorId"),
        ("attributes", (), "InventoryAttributes"),
    ],
)
def test_inventory_record_rejects_wrong_composed_value_type(
    field: str, value: object, message: str
) -> None:
    with pytest.raises(TypeError, match=message):
        _record(**{field: value})


def test_inventory_record_rejects_a_registered_value_subclass() -> None:
    with pytest.raises(TypeError, match="Money"):
        _record(unit_price=_MoneySubclass.parse("USD 19.95"))


def test_inventory_attributes_reject_non_mapping_and_non_tuple_storage() -> None:
    with pytest.raises(TypeError, match="mapping"):
        InventoryAttributes.from_mapping([])  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="tuple"):
        InventoryAttributes(items=[])  # type: ignore[arg-type]


@pytest.mark.parametrize("item", ["color", ("color",), ("color", "Blue", "extra")])
def test_inventory_attributes_reject_malformed_items(item: object) -> None:
    with pytest.raises(TypeError, match="key-value tuple"):
        InventoryAttributes(items=(item,))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "key", ["", "Color", "1color", "color space", "color--tone", "color__tone", "é"]
)
def test_inventory_attributes_reject_noncanonical_keys(key: str) -> None:
    with pytest.raises(ValueError, match="keys"):
        InventoryAttributes(items=((key, "Blue"),))


def test_inventory_attributes_reject_overlong_and_duplicate_keys() -> None:
    with pytest.raises(ValueError, match="64"):
        InventoryAttributes(items=(("a" * 65, "Blue"),))
    with pytest.raises(ValueError, match="unique"):
        InventoryAttributes(items=(("color", "Blue"), ("color", "Red")))


def test_inventory_attributes_reject_too_many_items() -> None:
    values = tuple((f"key-{index}", "value") for index in range(33))

    with pytest.raises(ValueError, match="too many"):
        InventoryAttributes(items=values)


def test_inventory_attribute_mapping_stops_at_the_item_bound() -> None:
    class OversizedMapping(Mapping[str, str]):
        reads = 0

        def __getitem__(self, key: str) -> str:
            del key
            self.reads += 1
            if self.reads > InventoryAttributes.MAX_ITEMS + 1:
                raise AssertionError("mapping was read beyond the rejection boundary")
            return "value"

        def __iter__(self) -> Iterator[str]:
            for index in range(InventoryAttributes.MAX_ITEMS + 2):
                yield f"key-{index}"

        def __len__(self) -> int:
            return InventoryAttributes.MAX_ITEMS + 2

    values = OversizedMapping()

    with pytest.raises(ValueError, match="too many"):
        InventoryAttributes.from_mapping(values)

    assert values.reads == InventoryAttributes.MAX_ITEMS + 1


def test_inventory_attributes_reject_total_encoded_size_overflow() -> None:
    values = tuple((f"key-{index}", "é" * 256) for index in range(9))

    with pytest.raises(ValueError, match="encoded-size"):
        InventoryAttributes(items=values)


@pytest.mark.parametrize("value", [42, " leading", "trailing ", "two  spaces", "line\nbreak"])
def test_inventory_attributes_reject_invalid_values(value: object) -> None:
    expected = TypeError if not isinstance(value, str) else ValueError
    with pytest.raises(expected, match="attribute value"):
        InventoryAttributes(items=(("color", value),))  # type: ignore[arg-type]
