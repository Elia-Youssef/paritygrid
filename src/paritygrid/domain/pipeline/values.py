"""Stable pipeline values without graph-planning behavior."""

import json
import re
from dataclasses import dataclass
from typing import ClassVar, Self, cast

from paritygrid.domain.models import NodeId

_NODE_KIND_PATTERN = re.compile(r"[a-z][a-z0-9]*(?:\.[a-z][a-z0-9]*)*", flags=re.ASCII)
_PORT_NAME_PATTERN = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*", flags=re.ASCII)
_PARTITION_KEY_PATTERN = re.compile(
    r"[a-z0-9]+(?:[-_.:][a-z0-9]+)*",
    flags=re.ASCII,
)


@dataclass(frozen=True, slots=True, order=True)
class _StableText:
    """Base contract for canonical ASCII text values."""

    value: str
    _subject: ClassVar[str]
    _pattern: ClassVar[re.Pattern[str]]
    _minimum_length: ClassVar[int]
    _maximum_length: ClassVar[int]

    def __post_init__(self) -> None:
        canonical = _validate_stable_text(
            self.value,
            subject=self._subject,
            pattern=self._pattern,
            minimum_length=self._minimum_length,
            maximum_length=self._maximum_length,
        )
        object.__setattr__(self, "value", canonical)

    @classmethod
    def parse(cls, value: str) -> Self:
        """Parse a canonical text representation without normalization."""
        return cls(value=value)

    @classmethod
    def from_bytes(cls, value: object) -> Self:
        """Parse a canonical ASCII byte representation."""
        if not isinstance(value, bytes):
            raise TypeError(f"{cls._subject} encoding must be bytes")
        try:
            text = value.decode("ascii")
        except UnicodeDecodeError as error:
            raise ValueError(f"{cls._subject} encoding must contain only ASCII") from error
        return cls.parse(text)

    def to_bytes(self) -> bytes:
        """Return the stable ASCII representation."""
        return self.value.encode("ascii")

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True, order=True)
class NodeKind(_StableText):
    """Version-independent registry key for one pipeline node kind."""

    _subject: ClassVar[str] = "node kind"
    _pattern: ClassVar[re.Pattern[str]] = _NODE_KIND_PATTERN
    _minimum_length: ClassVar[int] = 3
    _maximum_length: ClassVar[int] = 96


@dataclass(frozen=True, slots=True, order=True)
class PortName(_StableText):
    """Stable name of a typed input or output port."""

    _subject: ClassVar[str] = "port name"
    _pattern: ClassVar[re.Pattern[str]] = _PORT_NAME_PATTERN
    _minimum_length: ClassVar[int] = 1
    _maximum_length: ClassVar[int] = 64


@dataclass(frozen=True, slots=True, order=True)
class PartitionKey(_StableText):
    """Stable key that identifies a partition within a node's work."""

    _subject: ClassVar[str] = "partition key"
    _pattern: ClassVar[re.Pattern[str]] = _PARTITION_KEY_PATTERN
    _minimum_length: ClassVar[int] = 1
    _maximum_length: ClassVar[int] = 128


@dataclass(frozen=True, slots=True)
class PipelineNode:
    """A pipeline node's immutable identity and declared ports."""

    node_id: NodeId
    kind: NodeKind
    input_ports: tuple[PortName, ...] = ()
    output_ports: tuple[PortName, ...] = ()

    def __post_init__(self) -> None:
        _require_instance(self.node_id, NodeId, field_name="node_id")
        _require_instance(self.kind, NodeKind, field_name="kind")
        _validate_ports(self.input_ports, field_name="input_ports")
        _validate_ports(self.output_ports, field_name="output_ports")

        object.__setattr__(self, "input_ports", tuple(sorted(self.input_ports)))
        object.__setattr__(self, "output_ports", tuple(sorted(self.output_ports)))

    def to_primitive(self) -> dict[str, object]:
        """Return a deterministic primitive representation."""
        return {
            "id": str(self.node_id),
            "inputs": [str(port) for port in self.input_ports],
            "kind": str(self.kind),
            "outputs": [str(port) for port in self.output_ports],
        }

    def to_bytes(self) -> bytes:
        """Return the local unversioned value encoding, not fingerprint input."""
        return _encode_primitive(self.to_primitive())

    def __bytes__(self) -> bytes:
        return self.to_bytes()


@dataclass(frozen=True, slots=True)
class PipelineEdge:
    """A directed connection between two named pipeline ports."""

    source_node_id: NodeId
    source_port: PortName
    target_node_id: NodeId
    target_port: PortName

    def __post_init__(self) -> None:
        _require_instance(self.source_node_id, NodeId, field_name="source_node_id")
        _require_instance(self.source_port, PortName, field_name="source_port")
        _require_instance(self.target_node_id, NodeId, field_name="target_node_id")
        _require_instance(self.target_port, PortName, field_name="target_port")

    def to_primitive(self) -> dict[str, object]:
        """Return a deterministic primitive representation."""
        return {
            "source": {
                "node_id": str(self.source_node_id),
                "port": str(self.source_port),
            },
            "target": {
                "node_id": str(self.target_node_id),
                "port": str(self.target_port),
            },
        }

    def to_bytes(self) -> bytes:
        """Return the local unversioned value encoding, not fingerprint input."""
        return _encode_primitive(self.to_primitive())

    def __bytes__(self) -> bytes:
        return self.to_bytes()


def _validate_stable_text(
    value: object,
    *,
    subject: str,
    pattern: re.Pattern[str],
    minimum_length: int,
    maximum_length: int,
) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{subject} must be text")
    if not value.isascii():
        raise ValueError(f"{subject} must contain only ASCII")
    if not minimum_length <= len(value) <= maximum_length:
        raise ValueError(
            f"{subject} must be between {minimum_length} and {maximum_length} characters"
        )
    if pattern.fullmatch(value) is None:
        raise ValueError(f"{subject} must use its canonical lowercase form")
    return value


def _validate_ports(ports: object, *, field_name: str) -> None:
    if not isinstance(ports, tuple):
        raise TypeError(f"{field_name} must be a tuple")
    port_values = cast(tuple[object, ...], ports)
    if any(type(port) is not PortName for port in port_values):
        raise TypeError(f"{field_name} must contain only PortName values")
    if len(set(port_values)) != len(port_values):
        raise ValueError(f"{field_name} must not contain duplicate ports")


def _require_instance(value: object, expected_type: type[object], *, field_name: str) -> None:
    if type(value) is not expected_type:
        raise TypeError(f"{field_name} must be a {expected_type.__name__}")


def _encode_primitive(value: dict[str, object]) -> bytes:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode(
        "ascii"
    )
