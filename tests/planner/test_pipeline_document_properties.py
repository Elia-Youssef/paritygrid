"""Generated verification for canonical pipeline documents."""

from __future__ import annotations

from copy import deepcopy
from typing import cast

import pytest
from hypothesis import given
from hypothesis import strategies as st

from paritygrid.application.planner import (
    InvalidPipelineDocumentError,
    PipelineDocument,
    encode_pipeline_document,
)


def _node(index: int) -> dict[str, object]:
    return {
        "configuration": {"ordinal": index, "enabled": index % 2 == 0},
        "configuration_version": 1,
        "connector_id": None,
        "id": f"nod_generated-{index:03d}",
        "kind": "transform.generated",
    }


def _document(size: int) -> dict[str, object]:
    nodes = [_node(index) for index in range(size)]
    edges = [
        {
            "source_node_id": f"nod_generated-{index:03d}",
            "source_port": "records",
            "target_node_id": f"nod_generated-{index + 1:03d}",
            "target_port": "records",
        }
        for index in range(size - 1)
    ]
    return {
        "canonical_format_version": 1,
        "edges": edges,
        "layout": [
            {"node_id": f"nod_generated-{index:03d}", "x": index, "y": -index}
            for index in range(size)
        ],
        "nodes": nodes,
        "resource_policy": {},
        "schema_version": 1,
    }


@given(st.integers(min_value=1, max_value=25), st.data())
def test_canonical_encoding_is_independent_of_declaration_order(
    size: int, data: st.DataObject
) -> None:
    source = _document(size)
    reordered = deepcopy(source)
    nodes = cast(list[object], reordered["nodes"])
    layout = cast(list[object], reordered["layout"])
    edges = cast(list[object], reordered["edges"])
    node_order = data.draw(st.permutations(tuple(range(size))))
    edge_order = data.draw(st.permutations(tuple(range(max(size - 1, 0)))))
    reordered["nodes"] = [nodes[index] for index in node_order]
    reordered["layout"] = [layout[index] for index in reversed(node_order)]
    reordered["edges"] = [edges[index] for index in edge_order]

    first = encode_pipeline_document(PipelineDocument.from_mapping(source))
    second = encode_pipeline_document(PipelineDocument.from_mapping(reordered))
    assert first == second


@given(st.integers(min_value=1, max_value=25))
def test_generated_duplicate_node_identity_is_rejected(size: int) -> None:
    source = _document(size)
    nodes = cast(list[object], source["nodes"])
    nodes.append(deepcopy(nodes[0]))

    with pytest.raises(InvalidPipelineDocumentError, match="unique"):
        PipelineDocument.from_mapping(source)
