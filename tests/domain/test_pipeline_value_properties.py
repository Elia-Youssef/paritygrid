"""Property verification for canonical pipeline values."""

from hypothesis import given
from hypothesis import strategies as st

from paritygrid.domain.models import NodeId
from paritygrid.domain.pipeline import NodeKind, PartitionKey, PipelineNode, PortName

_LOWER_ALPHANUMERIC = "abcdefghijklmnopqrstuvwxyz0123456789"


@st.composite
def node_kinds(draw: st.DrawFn) -> str:
    """Build canonical dotted node-kind identifiers."""
    segments = draw(
        st.lists(
            st.tuples(
                st.sampled_from(tuple("abcdefghijklmnopqrstuvwxyz")),
                st.text(alphabet=_LOWER_ALPHANUMERIC, min_size=0, max_size=12),
            ).map(lambda pair: "".join(pair)),
            min_size=1,
            max_size=5,
        )
    )
    value = ".".join(segments)
    if len(value) < 3:
        value += "a" * (3 - len(value))
    return value[:96].rstrip(".")


@st.composite
def port_names(draw: st.DrawFn) -> str:
    """Build canonical kebab-case port names."""
    first = draw(st.sampled_from(tuple("abcdefghijklmnopqrstuvwxyz")))
    tail = draw(st.text(alphabet=_LOWER_ALPHANUMERIC, min_size=0, max_size=20))
    return f"{first}{tail}"


@st.composite
def partition_keys(draw: st.DrawFn) -> str:
    """Build canonical partition keys with supported separators."""
    segments = draw(
        st.lists(
            st.text(alphabet=_LOWER_ALPHANUMERIC, min_size=1, max_size=16),
            min_size=1,
            max_size=5,
        )
    )
    separators = draw(
        st.lists(
            st.sampled_from(("-", "_", ".", ":")),
            min_size=len(segments) - 1,
            max_size=len(segments) - 1,
        )
    )
    return "".join(
        part
        for index, segment in enumerate(segments)
        for part in ((separators[index - 1] if index else ""), segment)
    )


@given(node_kinds())
def test_node_kind_round_trip_is_stable(value: str) -> None:
    kind = NodeKind(value)

    assert NodeKind.from_bytes(bytes(kind)) == kind


@given(port_names())
def test_port_name_round_trip_is_stable(value: str) -> None:
    port = PortName(value)

    assert PortName.from_bytes(bytes(port)) == port


@given(partition_keys())
def test_partition_key_round_trip_is_stable(value: str) -> None:
    partition = PartitionKey(value)

    assert PartitionKey.from_bytes(bytes(partition)) == partition


@given(st.lists(port_names(), unique=True, max_size=8))
def test_pipeline_node_encoding_is_independent_of_port_declaration_order(
    names: list[str],
) -> None:
    forward = tuple(PortName(name) for name in names)
    reverse = tuple(reversed(forward))

    first = PipelineNode(
        NodeId("nod_property-01"),
        NodeKind("transform.property"),
        input_ports=forward,
    )
    second = PipelineNode(
        NodeId("nod_property-01"),
        NodeKind("transform.property"),
        input_ports=reverse,
    )

    assert first == second
    assert bytes(first) == bytes(second)
