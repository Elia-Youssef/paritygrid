"""Version 1 pipeline-document contract tests."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.application.planner import (
    MAX_PIPELINE_DOCUMENT_BYTES,
    MAX_PIPELINE_EDGES,
    MAX_PIPELINE_LAYOUT_COORDINATE,
    MAX_PIPELINE_NODES,
    PIPELINE_DOCUMENT_SCHEMA_VERSION,
    PIPELINE_PLANNER_FORMAT_VERSION,
    InvalidPipelineDocumentError,
    PipelineDocument,
    PipelineNodeLayout,
    PipelineNodeSpecification,
    UnsupportedPipelineDocumentVersionError,
    decode_pipeline_document,
    encode_pipeline_document,
)
from paritygrid.application.planner import documents as contract
from paritygrid.application.ports import ConfigurationDocument
from paritygrid.domain.models import ConnectorId, NodeId
from paritygrid.domain.pipeline import NodeKind, PipelineEdge, PortName


def _mapping() -> dict[str, object]:
    return {
        "schema_version": 1,
        "canonical_format_version": 1,
        "nodes": [
            {
                "id": "nod_target-01",
                "kind": "target.verify",
                "configuration_version": 1,
                "configuration": {},
                "connector_id": "con_target-01",
            },
            {
                "id": "nod_source-01",
                "kind": "source.csv",
                "configuration_version": 1,
                "configuration": {"encoding": "utf-8", "header": True},
                "connector_id": "con_source-01",
            },
        ],
        "edges": [
            {
                "source_node_id": "nod_source-01",
                "source_port": "records",
                "target_node_id": "nod_target-01",
                "target_port": "records",
            }
        ],
        "resource_policy": {"max_concurrent_work_items": 4},
        "layout": [
            {"node_id": "nod_target-01", "x": 300, "y": -20},
            {"node_id": "nod_source-01", "x": -300, "y": -20},
        ],
    }


def _node(identity: str = "nod_source-01") -> PipelineNodeSpecification:
    return PipelineNodeSpecification(
        NodeId(identity),
        NodeKind("source.csv"),
        1,
        ConfigurationDocument.from_mapping({"path": "synthetic.csv"}),
        ConnectorId("con_source-01"),
    )


def _document() -> PipelineDocument:
    return PipelineDocument.from_mapping(_mapping())


def test_frozen_schema_constants_and_dependency_neutrality() -> None:
    assert PIPELINE_DOCUMENT_SCHEMA_VERSION == 1
    assert PIPELINE_PLANNER_FORMAT_VERSION == 1
    assert MAX_PIPELINE_NODES == 256
    assert MAX_PIPELINE_EDGES == 4_096
    assert MAX_PIPELINE_DOCUMENT_BYTES == 1_048_576
    source = Path(contract.__file__).read_text(encoding="utf-8")
    assert "pydantic" not in source
    assert "sqlalchemy" not in source
    assert "fastapi" not in source


def test_valid_example_is_detached_immutable_sorted_and_redacted() -> None:
    source = _mapping()
    document = PipelineDocument.from_mapping(source)
    cast(dict[str, object], cast(list[object], source["nodes"])[0])["kind"] = "changed"

    assert tuple(str(node.node_id) for node in document.nodes) == (
        "nod_source-01",
        "nod_target-01",
    )
    assert tuple(str(position.node_id) for position in document.layout) == (
        "nod_source-01",
        "nod_target-01",
    )
    assert document.nodes[0].kind == NodeKind("source.csv")
    assert "synthetic.csv" not in repr(_node())
    assert "max_concurrent_work_items" not in repr(document)
    with pytest.raises(FrozenInstanceError):
        document.schema_version = 2  # type: ignore[misc]


def test_golden_encoding_and_durable_round_trip() -> None:
    document = _document()
    encoded = encode_pipeline_document(document)

    assert encoded == (
        b'{"canonical_format_version":1,"edges":[{"source_node_id":"nod_source-01",'
        b'"source_port":"records","target_node_id":"nod_target-01",'
        b'"target_port":"records"}],"layout":[{"node_id":"nod_source-01","x":-300,'
        b'"y":-20},{"node_id":"nod_target-01","x":300,"y":-20}],"nodes":['
        b'{"configuration":{"encoding":"utf-8","header":true},'
        b'"configuration_version":1,"connector_id":"con_source-01",'
        b'"id":"nod_source-01","kind":"source.csv"},{"configuration":{},'
        b'"configuration_version":1,"connector_id":"con_target-01",'
        b'"id":"nod_target-01","kind":"target.verify"}],"resource_policy":'
        b'{"max_concurrent_work_items":4},"schema_version":1}'
    )
    assert decode_pipeline_document(encoded) == document
    assert (
        PipelineDocument.from_configuration_document(document.to_configuration_document())
        == document
    )
    assert encode_pipeline_document(document, include_layout=False) == encode_pipeline_document(
        replace(document, layout=())
    )


def test_json_key_and_collection_order_do_not_change_canonical_document() -> None:
    first = _mapping()
    second = {key: first[key] for key in reversed(tuple(first))}
    second["nodes"] = list(reversed(cast(list[object], first["nodes"])))
    second["layout"] = list(reversed(cast(list[object], first["layout"])))

    first_bytes = encode_pipeline_document(PipelineDocument.from_mapping(first))
    second_bytes = encode_pipeline_document(PipelineDocument.from_mapping(second))
    assert first_bytes == second_bytes
    assert decode_pipeline_document(json.dumps(second)) == _document()


@pytest.mark.parametrize(
    "encoded",
    [
        b"[]",
        b"{",
        b'{"schema_version":1,"schema_version":1}',
        b'{"schema_version":1.0}',
        b'{"schema_version":NaN}',
        b'{"schema_version":9223372036854775808}',
        b"\xef\xbb\xbf{}",
        b"\xff",
        "\ud800",
    ],
)
def test_decoder_rejects_adversarial_json(encoded: str | bytes) -> None:
    with pytest.raises(InvalidPipelineDocumentError):
        decode_pipeline_document(encoded)


def test_decoder_and_encoder_enforce_exact_public_inputs_and_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(TypeError, match="text or bytes"):
        decode_pipeline_document(cast(Any, bytearray(b"{}")))
    with pytest.raises(TypeError, match="PipelineDocument"):
        encode_pipeline_document(cast(Any, object()))
    with pytest.raises(TypeError, match="boolean"):
        encode_pipeline_document(_document(), include_layout=cast(Any, 1))
    monkeypatch.setattr(contract, "MAX_PIPELINE_DOCUMENT_BYTES", 1)
    with pytest.raises(InvalidPipelineDocumentError, match="byte limit"):
        decode_pipeline_document(b"{}")
    with pytest.raises(InvalidPipelineDocumentError, match="byte limit"):
        decode_pipeline_document("{}")
    with pytest.raises(InvalidPipelineDocumentError, match="byte limit"):
        encode_pipeline_document(_document())


@pytest.mark.parametrize(
    ("change", "error"),
    [
        ({"schema_version": True}, TypeError),
        ({"schema_version": 2}, UnsupportedPipelineDocumentVersionError),
        ({"canonical_format_version": True}, TypeError),
        ({"canonical_format_version": 2}, UnsupportedPipelineDocumentVersionError),
        ({"nodes": {}}, TypeError),
        ({"edges": {}}, TypeError),
        ({"layout": {}}, TypeError),
        ({"resource_policy": []}, TypeError),
    ],
)
def test_root_rejects_wrong_structural_values(
    change: dict[str, object], error: type[Exception]
) -> None:
    value = _mapping()
    value.update(change)
    with pytest.raises(error):
        PipelineDocument.from_mapping(value)


def test_root_rejects_missing_unknown_and_nontext_fields() -> None:
    missing = _mapping()
    missing.pop("layout")
    with pytest.raises(InvalidPipelineDocumentError, match="missing"):
        PipelineDocument.from_mapping(missing)
    unknown = _mapping()
    unknown["executable"] = "print(1)"
    with pytest.raises(InvalidPipelineDocumentError, match="unknown"):
        PipelineDocument.from_mapping(unknown)
    with pytest.raises(TypeError, match="field names"):
        PipelineDocument.from_mapping(cast(Any, {**_mapping(), 1: None}))
    with pytest.raises(TypeError, match="object"):
        PipelineDocument.from_mapping(cast(Any, []))
    with pytest.raises(TypeError, match="ConfigurationDocument"):
        PipelineDocument.from_configuration_document(cast(Any, object()))


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("id", 1, TypeError),
        ("id", "bad", ValueError),
        ("kind", 1, TypeError),
        ("kind", "shell.execute", None),
        ("configuration_version", True, TypeError),
        ("configuration_version", 0, InvalidPipelineDocumentError),
        ("configuration_version", 2_147_483_648, InvalidPipelineDocumentError),
        ("configuration", [], TypeError),
        ("connector_id", 1, TypeError),
        ("connector_id", "bad", ValueError),
    ],
)
def test_node_parser_rejects_invalid_fields(
    field: str, value: object, error: type[Exception] | None
) -> None:
    mapping = _mapping()
    node = cast(dict[str, object], cast(list[object], mapping["nodes"])[0])
    node[field] = value
    if error is None:
        assert PipelineDocument.from_mapping(mapping).nodes[1].kind == NodeKind("shell.execute")
    else:
        with pytest.raises(error):
            PipelineDocument.from_mapping(mapping)


def test_node_parser_rejects_wrong_object_shape() -> None:
    mapping = _mapping()
    cast(list[object], mapping["nodes"])[0] = []
    with pytest.raises(TypeError, match="object"):
        PipelineDocument.from_mapping(mapping)
    mapping = _mapping()
    node = cast(dict[str, object], cast(list[object], mapping["nodes"])[0])
    node.pop("kind")
    with pytest.raises(InvalidPipelineDocumentError, match="missing"):
        PipelineDocument.from_mapping(mapping)
    mapping = _mapping()
    node = cast(dict[str, object], cast(list[object], mapping["nodes"])[0])
    node["command"] = "value"
    with pytest.raises(InvalidPipelineDocumentError, match="unknown"):
        PipelineDocument.from_mapping(mapping)


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("source_node_id", 1, TypeError),
        ("source_node_id", "bad", ValueError),
        ("source_port", 1, TypeError),
        ("source_port", "bad_port", ValueError),
        ("target_node_id", 1, TypeError),
        ("target_node_id", "bad", ValueError),
        ("target_port", 1, TypeError),
        ("target_port", "bad_port", ValueError),
    ],
)
def test_edge_parser_rejects_invalid_fields(
    field: str, value: object, error: type[Exception]
) -> None:
    mapping = _mapping()
    edge = cast(dict[str, object], cast(list[object], mapping["edges"])[0])
    edge[field] = value
    with pytest.raises(error):
        PipelineDocument.from_mapping(mapping)


def test_edge_parser_rejects_wrong_object_shape() -> None:
    mapping = _mapping()
    cast(list[object], mapping["edges"])[0] = []
    with pytest.raises(TypeError, match="object"):
        PipelineDocument.from_mapping(mapping)
    mapping = _mapping()
    edge = cast(dict[str, object], cast(list[object], mapping["edges"])[0])
    edge.pop("target_port")
    with pytest.raises(InvalidPipelineDocumentError, match="missing"):
        PipelineDocument.from_mapping(mapping)
    mapping = _mapping()
    edge = cast(dict[str, object], cast(list[object], mapping["edges"])[0])
    edge["condition"] = "always"
    with pytest.raises(InvalidPipelineDocumentError, match="unknown"):
        PipelineDocument.from_mapping(mapping)


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("node_id", 1, TypeError),
        ("node_id", "bad", ValueError),
        ("x", True, TypeError),
        ("x", MAX_PIPELINE_LAYOUT_COORDINATE + 1, InvalidPipelineDocumentError),
        ("y", True, TypeError),
        ("y", -MAX_PIPELINE_LAYOUT_COORDINATE - 1, InvalidPipelineDocumentError),
    ],
)
def test_layout_parser_rejects_invalid_fields(
    field: str, value: object, error: type[Exception]
) -> None:
    mapping = _mapping()
    position = cast(dict[str, object], cast(list[object], mapping["layout"])[0])
    position[field] = value
    with pytest.raises(error):
        PipelineDocument.from_mapping(mapping)


def test_layout_parser_rejects_wrong_object_shape() -> None:
    mapping = _mapping()
    cast(list[object], mapping["layout"])[0] = []
    with pytest.raises(TypeError, match="object"):
        PipelineDocument.from_mapping(mapping)
    mapping = _mapping()
    position = cast(dict[str, object], cast(list[object], mapping["layout"])[0])
    position.pop("y")
    with pytest.raises(InvalidPipelineDocumentError, match="missing"):
        PipelineDocument.from_mapping(mapping)
    mapping = _mapping()
    position = cast(dict[str, object], cast(list[object], mapping["layout"])[0])
    position["width"] = 10
    with pytest.raises(InvalidPipelineDocumentError, match="unknown"):
        PipelineDocument.from_mapping(mapping)


def test_direct_contracts_reject_untrusted_exact_types() -> None:
    with pytest.raises(TypeError, match="NodeId"):
        replace(_node(), node_id="nod_source-01")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="NodeKind"):
        replace(_node(), kind="source.csv")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="integer"):
        replace(_node(), configuration_version=True)
    with pytest.raises(TypeError, match="ConfigurationDocument"):
        replace(_node(), configuration={})  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="ConnectorId"):
        replace(_node(), connector_id="con_source-01")  # type: ignore[arg-type]
    assert replace(_node(), connector_id=None).connector_id is None
    with pytest.raises(TypeError, match="NodeId"):
        PipelineNodeLayout("nod_source-01", 0, 0)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="integer"):
        PipelineNodeLayout(NodeId("nod_source-01"), cast(Any, True), 0)
    assert PipelineNodeLayout(NodeId("nod_source-01"), -1_000_000, 1_000_000).x == -1_000_000
    with pytest.raises(TypeError, match="integer"):
        replace(_document(), schema_version=True)


def test_document_rejects_invalid_collections_relationships_and_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _node()
    edge = PipelineEdge(
        node.node_id,
        PortName("records"),
        NodeId("nod_target-01"),
        PortName("records"),
    )
    position = PipelineNodeLayout(node.node_id, 0, 0)
    policy = ConfigurationDocument.from_mapping({})

    with pytest.raises(TypeError, match="tuple"):
        PipelineDocument(cast(Any, [node]), (), policy, ())
    with pytest.raises(TypeError, match="invalid"):
        PipelineDocument(cast(Any, (object(),)), (), policy, ())
    with pytest.raises(TypeError, match="invalid"):
        PipelineDocument((node,), cast(Any, (object(),)), policy, ())
    with pytest.raises(TypeError, match="invalid"):
        PipelineDocument((node,), (), policy, cast(Any, (object(),)))
    with pytest.raises(TypeError, match="ConfigurationDocument"):
        PipelineDocument((node,), (), cast(Any, {}), ())
    with pytest.raises(InvalidPipelineDocumentError, match="at least"):
        PipelineDocument((), (), policy, ())
    with pytest.raises(InvalidPipelineDocumentError, match="unique"):
        PipelineDocument((node, node), (), policy, ())
    with pytest.raises(InvalidPipelineDocumentError, match="unknown"):
        PipelineDocument((node,), (edge,), policy, ())
    with pytest.raises(InvalidPipelineDocumentError, match="unique"):
        PipelineDocument((node, _node("nod_target-01")), (edge, edge), policy, ())
    with pytest.raises(InvalidPipelineDocumentError, match="unique"):
        PipelineDocument((node,), (), policy, (position, position))
    with pytest.raises(InvalidPipelineDocumentError, match="unknown"):
        PipelineDocument(
            (node,),
            (),
            policy,
            (PipelineNodeLayout(NodeId("nod_other-01"), 0, 0),),
        )

    monkeypatch.setattr(contract, "MAX_PIPELINE_NODES", 0)
    with pytest.raises(InvalidPipelineDocumentError, match="node limit"):
        PipelineDocument((node,), (), policy, ())
    monkeypatch.setattr(contract, "MAX_PIPELINE_NODES", 256)
    monkeypatch.setattr(contract, "MAX_PIPELINE_EDGES", -1)
    with pytest.raises(InvalidPipelineDocumentError, match="edge limit"):
        PipelineDocument((node,), (), policy, ())
    monkeypatch.setattr(contract, "MAX_PIPELINE_EDGES", 4_096)


def test_public_mapping_is_detached_and_layout_can_be_excluded() -> None:
    document = _document()
    mapping = document.to_mapping()
    cast(dict[str, object], cast(list[object], mapping["nodes"])[0])["kind"] = "changed"
    assert document.nodes[0].kind == NodeKind("source.csv")
    assert document.to_mapping(include_layout=False)["layout"] == []
