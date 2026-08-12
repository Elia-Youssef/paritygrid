"""Property verification for canonical ordering and semantic sensitivity."""

from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from paritygrid.domain.canonical import encode_canonical
from paritygrid.domain.models import (
    ConnectorId,
    InventoryAttributes,
    InventoryRecord,
    Money,
    NodeId,
    UtcTimestamp,
)
from paritygrid.domain.pipeline import NodeKind, PipelineNode, PortName


@given(
    st.dictionaries(
        keys=st.from_regex(r"[a-z][a-z0-9]{0,7}", fullmatch=True),
        values=st.text(alphabet="abcdefghijklmnopqrstuvwxyz", max_size=20),
        max_size=12,
    )
)
def test_attribute_input_order_never_changes_canonical_bytes(values: dict[str, str]) -> None:
    forward = InventoryAttributes.from_mapping(values)
    reverse = InventoryAttributes(items=tuple(reversed(tuple(values.items()))))

    assert encode_canonical(forward) == encode_canonical(reverse)


@given(st.lists(st.from_regex(r"[a-z][a-z0-9]{0,7}", fullmatch=True), unique=True, max_size=12))
def test_pipeline_port_input_order_never_changes_canonical_bytes(names: list[str]) -> None:
    forward = PipelineNode(
        node_id=_node_id(),
        kind=NodeKind("transform.normalize"),
        input_ports=tuple(PortName(name) for name in names),
    )
    reverse = PipelineNode(
        node_id=_node_id(),
        kind=NodeKind("transform.normalize"),
        input_ports=tuple(PortName(name) for name in reversed(names)),
    )

    assert encode_canonical(forward) == encode_canonical(reverse)


@given(
    st.integers(min_value=0, max_value=2_147_483_647),
    st.text(
        alphabet=st.characters(categories=("Lu", "Ll", "Lt", "Lm", "Lo", "Mn", "Mc", "Nd")),
        min_size=1,
        max_size=32,
    ),
)
def test_inventory_semantic_changes_are_visible_in_canonical_bytes(
    quantity: int,
    name: str,
) -> None:
    first = _record(quantity=quantity, name=name)
    changed_quantity = 0 if quantity else 1
    second = _record(quantity=changed_quantity, name=name)

    assert encode_canonical(first) == encode_canonical(first)
    assert encode_canonical(first) != encode_canonical(second)


def _node_id() -> NodeId:
    return NodeId("nod_transform-001")


def _record(*, quantity: int, name: str) -> InventoryRecord:
    return InventoryRecord.create(
        sku="SKU-001",
        name=name,
        quantity=quantity,
        unit_price=Money(Decimal("1.25"), Money.parse("USD 0.00").currency, 2),
        updated_at=UtcTimestamp.parse("2026-08-12T10:00:00Z"),
        connector_id=ConnectorId("con_source-api"),
        source_record_key="source-001",
    )
