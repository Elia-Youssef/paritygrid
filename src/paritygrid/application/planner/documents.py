"""Dependency-neutral version 1 pipeline-document contract."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Never, cast

from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.domain.models import ConnectorId, NodeId
from paritygrid.domain.pipeline import NodeKind, PipelineEdge, PortName

PIPELINE_DOCUMENT_SCHEMA_VERSION = 1
PIPELINE_PLANNER_FORMAT_VERSION = 1
MAX_PIPELINE_DOCUMENT_BYTES = 1_048_576
MAX_PIPELINE_NODES = 256
MAX_PIPELINE_EDGES = 4_096
MAX_PIPELINE_LAYOUT_COORDINATE = 1_000_000
MAX_PIPELINE_CONFIGURATION_VERSION = 2_147_483_647
MAX_PIPELINE_JSON_INTEGER = 9_223_372_036_854_775_807

_ROOT_FIELDS = frozenset(
    {
        "canonical_format_version",
        "edges",
        "layout",
        "nodes",
        "resource_policy",
        "schema_version",
    }
)
_NODE_FIELDS = frozenset({"configuration", "configuration_version", "connector_id", "id", "kind"})
_EDGE_FIELDS = frozenset({"source_node_id", "source_port", "target_node_id", "target_port"})
_LAYOUT_FIELDS = frozenset({"node_id", "x", "y"})


class PipelineDocumentError(ValueError):
    """Base failure for an untrusted pipeline document."""


class InvalidPipelineDocumentError(PipelineDocumentError):
    """The document violates the frozen structural contract."""


class UnsupportedPipelineDocumentVersionError(PipelineDocumentError):
    """The document requests an unsupported schema or canonical format."""


@dataclass(frozen=True, slots=True, repr=False)
class PipelineNodeSpecification:
    """One node identity and its versioned, untrusted configuration."""

    node_id: NodeId
    kind: NodeKind
    configuration_version: int
    configuration: ConfigurationDocument
    connector_id: ConnectorId | None

    def __post_init__(self) -> None:
        _require_exact(self.node_id, NodeId, "pipeline node identity")
        _require_exact(self.kind, NodeKind, "pipeline node kind")
        version = cast(object, self.configuration_version)
        if type(version) is not int:
            raise TypeError("pipeline node configuration version must be an integer")
        if not 1 <= version <= MAX_PIPELINE_CONFIGURATION_VERSION:
            raise InvalidPipelineDocumentError(
                "pipeline node configuration version is outside the supported range"
            )
        _require_exact(self.configuration, ConfigurationDocument, "pipeline node configuration")
        connector = cast(object, self.connector_id)
        if connector is not None and type(connector) is not ConnectorId:
            raise TypeError("pipeline node connector must be a ConnectorId or None")

    def to_mapping(self) -> dict[str, object]:
        """Return the exact version 1 node object."""
        return {
            "configuration": self.configuration.to_mapping(),
            "configuration_version": self.configuration_version,
            "connector_id": None if self.connector_id is None else str(self.connector_id),
            "id": str(self.node_id),
            "kind": str(self.kind),
        }

    def __repr__(self) -> str:
        return (
            "PipelineNodeSpecification("
            f"node_id={self.node_id!r}, kind={self.kind!r}, "
            f"configuration_version={self.configuration_version!r}, "
            f"connector_id={self.connector_id!r}, configuration=<redacted>)"
        )


@dataclass(frozen=True, slots=True)
class PipelineNodeLayout:
    """Non-logical visual position for one pipeline node."""

    node_id: NodeId
    x: int
    y: int

    def __post_init__(self) -> None:
        _require_exact(self.node_id, NodeId, "pipeline layout node identity")
        _validate_layout_coordinate(self.x, "x")
        _validate_layout_coordinate(self.y, "y")

    def to_mapping(self) -> dict[str, object]:
        """Return the exact version 1 layout object."""
        return {"node_id": str(self.node_id), "x": self.x, "y": self.y}


@dataclass(frozen=True, slots=True, repr=False)
class PipelineDocument:
    """A canonical, bounded pipeline specification and its visual layout."""

    nodes: tuple[PipelineNodeSpecification, ...]
    edges: tuple[PipelineEdge, ...]
    resource_policy: ConfigurationDocument
    layout: tuple[PipelineNodeLayout, ...]
    schema_version: int = PIPELINE_DOCUMENT_SCHEMA_VERSION
    canonical_format_version: int = 1

    def __post_init__(self) -> None:
        _require_supported_version(
            self.schema_version,
            PIPELINE_DOCUMENT_SCHEMA_VERSION,
            "pipeline document schema version",
        )
        _require_supported_version(
            self.canonical_format_version,
            1,
            "pipeline canonical format version",
        )
        nodes = _require_exact_tuple(
            self.nodes,
            PipelineNodeSpecification,
            "pipeline nodes",
        )
        edges = _require_exact_tuple(self.edges, PipelineEdge, "pipeline edges")
        layout = _require_exact_tuple(self.layout, PipelineNodeLayout, "pipeline layout")
        _require_exact(self.resource_policy, ConfigurationDocument, "pipeline resource policy")
        if not nodes:
            raise InvalidPipelineDocumentError("pipeline document requires at least one node")
        if len(nodes) > MAX_PIPELINE_NODES:
            raise InvalidPipelineDocumentError("pipeline document exceeds the node limit")
        if len(edges) > MAX_PIPELINE_EDGES:
            raise InvalidPipelineDocumentError("pipeline document exceeds the edge limit")
        node_ids = tuple(node.node_id for node in nodes)
        _require_unique(node_ids, "pipeline node identities")
        edge_keys = tuple(_edge_key(edge) for edge in edges)
        _require_unique(edge_keys, "pipeline edges")
        known_nodes = frozenset(node_ids)
        if any(
            edge.source_node_id not in known_nodes or edge.target_node_id not in known_nodes
            for edge in edges
        ):
            raise InvalidPipelineDocumentError("pipeline edge references an unknown node")
        layout_ids = tuple(position.node_id for position in layout)
        _require_unique(layout_ids, "pipeline layout node identities")
        if any(node_id not in known_nodes for node_id in layout_ids):
            raise InvalidPipelineDocumentError("pipeline layout references an unknown node")

        object.__setattr__(self, "nodes", tuple(sorted(nodes, key=lambda node: str(node.node_id))))
        object.__setattr__(self, "edges", tuple(sorted(edges, key=_edge_key)))
        object.__setattr__(
            self,
            "layout",
            tuple(sorted(layout, key=lambda position: str(position.node_id))),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> PipelineDocument:
        """Parse an untrusted version 1 mapping without accepting unknown fields."""
        root = _require_object(value, "pipeline document")
        _require_fields(root, _ROOT_FIELDS, "pipeline document")
        nodes = tuple(
            _parse_node(item, index)
            for index, item in enumerate(_require_array(root["nodes"], "pipeline nodes"))
        )
        edges = tuple(
            _parse_edge(item, index)
            for index, item in enumerate(_require_array(root["edges"], "pipeline edges"))
        )
        layout = tuple(
            _parse_layout(item, index)
            for index, item in enumerate(_require_array(root["layout"], "pipeline layout"))
        )
        policy = ConfigurationDocument.from_mapping(
            _require_object(root["resource_policy"], "pipeline resource policy")
        )
        return cls(
            nodes=nodes,
            edges=edges,
            resource_policy=policy,
            layout=layout,
            schema_version=_require_integer(root["schema_version"], "pipeline schema version"),
            canonical_format_version=_require_integer(
                root["canonical_format_version"],
                "pipeline canonical format version",
            ),
        )

    @classmethod
    def from_configuration_document(cls, value: ConfigurationDocument) -> PipelineDocument:
        """Parse a detached durable configuration document."""
        _require_exact(value, ConfigurationDocument, "pipeline configuration document")
        return cls.from_mapping(value.to_mapping())

    def to_mapping(self, *, include_layout: bool = True) -> dict[str, object]:
        """Return a detached canonical mapping, optionally excluding visual layout."""
        if type(include_layout) is not bool:
            raise TypeError("include_layout must be boolean")
        return {
            "canonical_format_version": self.canonical_format_version,
            "edges": [_edge_mapping(edge) for edge in self.edges],
            "layout": [position.to_mapping() for position in self.layout] if include_layout else [],
            "nodes": [node.to_mapping() for node in self.nodes],
            "resource_policy": self.resource_policy.to_mapping(),
            "schema_version": self.schema_version,
        }

    def to_configuration_document(self) -> ConfigurationDocument:
        """Return the exact durable document stored for publication."""
        return ConfigurationDocument.from_mapping(self.to_mapping())

    def __repr__(self) -> str:
        return (
            "PipelineDocument("
            f"schema_version={self.schema_version!r}, "
            f"canonical_format_version={self.canonical_format_version!r}, "
            f"nodes={len(self.nodes)}, edges={len(self.edges)}, "
            f"layout={len(self.layout)}, resource_policy=<redacted>)"
        )


def decode_pipeline_document(value: str | bytes) -> PipelineDocument:
    """Decode bounded UTF-8 JSON with duplicate-key and numeric rejection."""
    raw = cast(object, value)
    if not isinstance(raw, str | bytes):
        raise TypeError("pipeline document encoding must be text or bytes")
    if isinstance(raw, bytes):
        if len(raw) > MAX_PIPELINE_DOCUMENT_BYTES:
            raise InvalidPipelineDocumentError("pipeline document exceeds the byte limit")
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            raise InvalidPipelineDocumentError("pipeline document must use valid UTF-8") from None
    else:
        text = raw
        try:
            encoded_size = len(text.encode("utf-8"))
        except UnicodeEncodeError:
            raise InvalidPipelineDocumentError("pipeline document must use valid Unicode") from None
        if encoded_size > MAX_PIPELINE_DOCUMENT_BYTES:
            raise InvalidPipelineDocumentError("pipeline document exceeds the byte limit")
    if text.startswith("\ufeff"):
        raise InvalidPipelineDocumentError("pipeline document must not contain a byte-order mark")
    try:
        decoded = json.loads(
            text,
            object_pairs_hook=_decode_object,
            parse_float=_reject_json_float,
            parse_int=_decode_json_integer,
            parse_constant=_reject_json_constant,
        )
    except json.JSONDecodeError, InvalidPipelineDocumentError:
        raise InvalidPipelineDocumentError("pipeline document contains invalid JSON") from None
    if not isinstance(decoded, dict):
        raise InvalidPipelineDocumentError("pipeline document JSON root must be an object")
    return PipelineDocument.from_mapping(cast(dict[str, object], decoded))


def encode_pipeline_document(document: PipelineDocument, *, include_layout: bool = True) -> bytes:
    """Encode one validated document as deterministic ASCII JSON bytes."""
    _require_exact(document, PipelineDocument, "pipeline document")
    mapping = document.to_mapping(include_layout=include_layout)
    encoded = json.dumps(
        mapping,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    if len(encoded) > MAX_PIPELINE_DOCUMENT_BYTES:
        raise InvalidPipelineDocumentError("pipeline document exceeds the byte limit")
    return encoded


def _parse_node(value: object, index: int) -> PipelineNodeSpecification:
    subject = f"pipeline node {index}"
    item = _require_object(value, subject)
    _require_fields(item, _NODE_FIELDS, subject)
    connector_value = item["connector_id"]
    if connector_value is not None and not isinstance(connector_value, str):
        raise TypeError(f"{subject} connector must be text or null")
    return PipelineNodeSpecification(
        node_id=NodeId(_require_text(item["id"], f"{subject} identity")),
        kind=NodeKind(_require_text(item["kind"], f"{subject} kind")),
        configuration_version=_require_integer(
            item["configuration_version"],
            f"{subject} configuration version",
        ),
        configuration=ConfigurationDocument.from_mapping(
            _require_object(item["configuration"], f"{subject} configuration")
        ),
        connector_id=None if connector_value is None else ConnectorId(connector_value),
    )


def _parse_edge(value: object, index: int) -> PipelineEdge:
    subject = f"pipeline edge {index}"
    item = _require_object(value, subject)
    _require_fields(item, _EDGE_FIELDS, subject)
    return PipelineEdge(
        source_node_id=NodeId(_require_text(item["source_node_id"], f"{subject} source node")),
        source_port=PortName(_require_text(item["source_port"], f"{subject} source port")),
        target_node_id=NodeId(_require_text(item["target_node_id"], f"{subject} target node")),
        target_port=PortName(_require_text(item["target_port"], f"{subject} target port")),
    )


def _parse_layout(value: object, index: int) -> PipelineNodeLayout:
    subject = f"pipeline layout {index}"
    item = _require_object(value, subject)
    _require_fields(item, _LAYOUT_FIELDS, subject)
    return PipelineNodeLayout(
        node_id=NodeId(_require_text(item["node_id"], f"{subject} node")),
        x=_require_integer(item["x"], f"{subject} x coordinate"),
        y=_require_integer(item["y"], f"{subject} y coordinate"),
    )


def _require_object(value: object, subject: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{subject} must be an object")
    result: dict[str, object] = {}
    for raw_key, item in cast(Mapping[object, object], value).items():
        if not isinstance(raw_key, str):
            raise TypeError(f"{subject} field names must be text")
        result[raw_key] = item
    return result


def _require_array(value: object, subject: str) -> tuple[object, ...]:
    if not isinstance(value, list | tuple):
        raise TypeError(f"{subject} must be an array")
    return tuple(cast(list[object] | tuple[object, ...], value))


def _require_fields(value: Mapping[str, object], fields: frozenset[str], subject: str) -> None:
    actual = frozenset(value)
    if fields - actual:
        raise InvalidPipelineDocumentError(f"{subject} is missing required fields")
    if actual - fields:
        raise InvalidPipelineDocumentError(f"{subject} contains unknown fields")


def _require_text(value: object, subject: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{subject} must be text")
    return value


def _require_integer(value: object, subject: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    return value


def _require_supported_version(value: object, supported: int, subject: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    if value != supported:
        raise UnsupportedPipelineDocumentVersionError(f"{subject} is unsupported")


def _require_exact(value: object, expected: type[object], subject: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{subject} must use {expected.__name__}")


def _require_exact_tuple[T](
    value: object,
    item_type: type[T],
    subject: str,
) -> tuple[T, ...]:
    if not isinstance(value, tuple):
        raise TypeError(f"{subject} must be a tuple")
    items = cast(tuple[object, ...], value)
    if any(type(item) is not item_type for item in items):
        raise TypeError(f"{subject} contains an invalid value")
    return cast(tuple[T, ...], items)


def _require_unique(values: tuple[object, ...], subject: str) -> None:
    if len(set(values)) != len(values):
        raise InvalidPipelineDocumentError(f"{subject} must be unique")


def _validate_layout_coordinate(value: object, axis: str) -> None:
    if type(value) is not int:
        raise TypeError(f"pipeline layout {axis} coordinate must be an integer")
    if not -MAX_PIPELINE_LAYOUT_COORDINATE <= value <= MAX_PIPELINE_LAYOUT_COORDINATE:
        raise InvalidPipelineDocumentError(
            f"pipeline layout {axis} coordinate is outside the supported range"
        )


def _edge_key(edge: PipelineEdge) -> tuple[str, str, str, str]:
    return (
        str(edge.source_node_id),
        str(edge.source_port),
        str(edge.target_node_id),
        str(edge.target_port),
    )


def _edge_mapping(edge: PipelineEdge) -> dict[str, object]:
    return {
        "source_node_id": str(edge.source_node_id),
        "source_port": str(edge.source_port),
        "target_node_id": str(edge.target_node_id),
        "target_port": str(edge.target_port),
    }


def _decode_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise InvalidPipelineDocumentError("pipeline document contains duplicate fields")
        result[key] = value
    return result


def _decode_json_integer(value: str) -> int:
    decoded = int(value)
    if not -MAX_PIPELINE_JSON_INTEGER <= decoded <= MAX_PIPELINE_JSON_INTEGER:
        raise InvalidPipelineDocumentError(
            "pipeline document integer is outside the supported range"
        )
    return decoded


def _reject_json_float(_value: str) -> Never:
    raise InvalidPipelineDocumentError("pipeline document must not contain floating-point values")


def _reject_json_constant(_value: str) -> Never:
    raise InvalidPipelineDocumentError("pipeline document must not contain non-finite values")
