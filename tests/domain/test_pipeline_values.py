"""Example verification for immutable pipeline values."""

from dataclasses import FrozenInstanceError

import pytest

from paritygrid.domain.models import NodeId
from paritygrid.domain.pipeline import NodeKind, PartitionKey, PipelineEdge, PipelineNode, PortName


@pytest.mark.parametrize(
    ("value_type", "text"),
    [
        (NodeKind, "source.http.async"),
        (PortName, "canonical-records"),
        (PartitionKey, "region:emea.batch-001"),
    ],
)
def test_stable_text_values_round_trip_and_are_hashable(
    value_type: type[NodeKind] | type[PortName] | type[PartitionKey], text: str
) -> None:
    value = value_type.parse(text)

    assert str(value) == text
    assert bytes(value) == text.encode("ascii")
    assert value_type.from_bytes(bytes(value)) == value
    assert len({value, value_type.parse(text)}) == 1


@pytest.mark.parametrize(
    ("value_type", "value"),
    [
        (NodeKind, "ab"),
        (NodeKind, "Source.Http"),
        (NodeKind, "source..http"),
        (NodeKind, f"a{'b' * 96}"),
        (PortName, ""),
        (PortName, "record_set"),
        (PortName, "records--out"),
        (PortName, f"p{'a' * 64}"),
        (PartitionKey, ""),
        (PartitionKey, "batch/01"),
        (PartitionKey, ".batch"),
        (PartitionKey, f"p{'a' * 128}"),
    ],
)
def test_stable_text_values_reject_noncanonical_or_out_of_range_forms(
    value_type: type[NodeKind] | type[PortName] | type[PartitionKey], value: str
) -> None:
    with pytest.raises(ValueError, match=r"between|canonical"):
        value_type.parse(value)


@pytest.mark.parametrize("value_type", [NodeKind, PortName, PartitionKey])
def test_stable_text_values_reject_wrong_types_and_non_ascii(
    value_type: type[NodeKind] | type[PortName] | type[PartitionKey],
) -> None:
    with pytest.raises(TypeError, match="text"):
        value_type.parse(42)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="bytes"):
        value_type.from_bytes(bytearray(b"example"))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="ASCII"):
        value_type.from_bytes("café".encode())
    with pytest.raises(ValueError, match="ASCII"):
        value_type.parse("café")


def test_stable_text_values_are_immutable_and_type_specific() -> None:
    kind = NodeKind("source.csv")

    with pytest.raises(FrozenInstanceError):
        kind.value = "changed"  # type: ignore[misc]

    assert kind != PortName("source-csv")


def test_pipeline_node_canonicalizes_port_order_and_encoding() -> None:
    node = PipelineNode(
        node_id=NodeId("nod_normalize-01"),
        kind=NodeKind("transform.normalize"),
        input_ports=(PortName("records-z"), PortName("records-a")),
        output_ports=(PortName("canonical-records"),),
    )
    same = PipelineNode(
        node_id=NodeId("nod_normalize-01"),
        kind=NodeKind("transform.normalize"),
        input_ports=(PortName("records-a"), PortName("records-z")),
        output_ports=(PortName("canonical-records"),),
    )

    assert node == same
    assert hash(node) == hash(same)
    assert node.input_ports == (PortName("records-a"), PortName("records-z"))
    assert node.to_primitive() == {
        "id": "nod_normalize-01",
        "inputs": ["records-a", "records-z"],
        "kind": "transform.normalize",
        "outputs": ["canonical-records"],
    }
    assert bytes(node) == (
        b'{"id":"nod_normalize-01","inputs":["records-a","records-z"],'
        b'"kind":"transform.normalize","outputs":["canonical-records"]}'
    )


def test_pipeline_edge_has_stable_directional_encoding() -> None:
    edge = PipelineEdge(
        source_node_id=NodeId("nod_source-01"),
        source_port=PortName("records"),
        target_node_id=NodeId("nod_target-01"),
        target_port=PortName("records-in"),
    )
    same = PipelineEdge(
        source_node_id=NodeId("nod_source-01"),
        source_port=PortName("records"),
        target_node_id=NodeId("nod_target-01"),
        target_port=PortName("records-in"),
    )

    assert edge == same
    assert hash(edge) == hash(same)
    assert edge.to_primitive() == {
        "source": {"node_id": "nod_source-01", "port": "records"},
        "target": {"node_id": "nod_target-01", "port": "records-in"},
    }
    assert bytes(edge) == (
        b'{"source":{"node_id":"nod_source-01","port":"records"},'
        b'"target":{"node_id":"nod_target-01","port":"records-in"}}'
    )


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        ("node_id", "nod_source-01"),
        ("kind", "source.csv"),
        ("input_ports", [PortName("records")]),
        ("input_ports", ("records",)),
        ("output_ports", (PortName("records"), PortName("records"))),
    ],
)
def test_pipeline_node_rejects_untrusted_field_values(field_name: str, replacement: object) -> None:
    values: dict[str, object] = {
        "node_id": NodeId("nod_source-01"),
        "kind": NodeKind("source.csv"),
        "input_ports": (),
        "output_ports": (),
    }
    values[field_name] = replacement

    with pytest.raises((TypeError, ValueError)):
        PipelineNode(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        ("source_node_id", "nod_source-01"),
        ("source_port", "records"),
        ("target_node_id", "nod_target-01"),
        ("target_port", "records"),
    ],
)
def test_pipeline_edge_rejects_untrusted_field_values(field_name: str, replacement: object) -> None:
    values: dict[str, object] = {
        "source_node_id": NodeId("nod_source-01"),
        "source_port": PortName("records"),
        "target_node_id": NodeId("nod_target-01"),
        "target_port": PortName("records"),
    }
    values[field_name] = replacement

    with pytest.raises(TypeError):
        PipelineEdge(**values)  # type: ignore[arg-type]
