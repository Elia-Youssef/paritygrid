"""Dependency-neutral typed port contracts for pipeline nodes."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from paritygrid.domain.pipeline import PortName

MAX_NODE_INPUT_PORTS = 32
MAX_NODE_OUTPUT_PORTS = 32
MAX_INPUT_ACCEPTED_TYPES = 16
MAX_INPUT_CONNECTIONS = 256


class PortContractError(ValueError):
    """Base failure for invalid typed port declarations."""


class InvalidPortConnectionError(PortContractError):
    """A pipeline edge violates one registered port contract."""


class PortValueType(StrEnum):
    """Closed logical payload types exchanged between built-in nodes."""

    RAW_RECORDS = "records.raw"
    NORMALIZED_RECORDS = "records.normalized"
    VALIDATED_RECORDS = "records.validated"
    PARTITIONED_RECORDS = "records.partitioned"
    RECONCILIATION = "reconciliation.result"
    REPAIR_PLAN = "repair.plan"
    APPROVED_REPAIR_PLAN = "repair.plan.approved"
    REPAIR_RESULT = "repair.result"
    VERIFICATION = "verification.result"


@dataclass(frozen=True, slots=True, order=True)
class InputPortDefinition:
    """One named input with closed accepted types and bounded fan-in."""

    name: PortName
    accepted_types: tuple[PortValueType, ...]
    required: bool = True
    maximum_connections: int = 1

    def __post_init__(self) -> None:
        _require_exact(self.name, PortName, "input port name")
        accepted = _require_exact_tuple(
            self.accepted_types,
            PortValueType,
            "input port accepted types",
        )
        if not accepted:
            raise PortContractError("input port requires at least one accepted type")
        if len(accepted) > MAX_INPUT_ACCEPTED_TYPES:
            raise PortContractError("input port exceeds the accepted type limit")
        _require_unique(accepted, "input port accepted types")
        if type(self.required) is not bool:
            raise TypeError("input port required marker must be boolean")
        maximum = cast(object, self.maximum_connections)
        if type(maximum) is not int:
            raise TypeError("input port maximum connections must be an integer")
        if not 1 <= maximum <= MAX_INPUT_CONNECTIONS:
            raise PortContractError("input port maximum connections is outside the limit")
        object.__setattr__(
            self,
            "accepted_types",
            tuple(sorted(accepted, key=lambda value_type: value_type.value)),
        )

    def accepts(self, value_type: PortValueType) -> bool:
        """Return whether this input accepts one exact closed payload type."""
        _require_exact(value_type, PortValueType, "port value type")
        return value_type in self.accepted_types


@dataclass(frozen=True, slots=True, order=True)
class OutputPortDefinition:
    """One named output and its exact logical payload type."""

    name: PortName
    value_type: PortValueType

    def __post_init__(self) -> None:
        _require_exact(self.name, PortName, "output port name")
        _require_exact(self.value_type, PortValueType, "output port value type")


@dataclass(frozen=True, slots=True, repr=False)
class NodePortSchema:
    """Immutable typed inputs and outputs for one node definition."""

    inputs: tuple[InputPortDefinition, ...]
    outputs: tuple[OutputPortDefinition, ...]

    def __post_init__(self) -> None:
        inputs = _require_exact_tuple(self.inputs, InputPortDefinition, "node input ports")
        outputs = _require_exact_tuple(self.outputs, OutputPortDefinition, "node output ports")
        if len(inputs) > MAX_NODE_INPUT_PORTS:
            raise PortContractError("node exceeds the input port limit")
        if len(outputs) > MAX_NODE_OUTPUT_PORTS:
            raise PortContractError("node exceeds the output port limit")
        _require_unique(tuple(item.name for item in inputs), "node input port names")
        _require_unique(tuple(item.name for item in outputs), "node output port names")
        object.__setattr__(self, "inputs", tuple(sorted(inputs, key=lambda item: item.name)))
        object.__setattr__(self, "outputs", tuple(sorted(outputs, key=lambda item: item.name)))

    def input(self, name: PortName) -> InputPortDefinition | None:
        """Return one exact input declaration, if present."""
        _require_exact(name, PortName, "input port name")
        return next((item for item in self.inputs if item.name == name), None)

    def output(self, name: PortName) -> OutputPortDefinition | None:
        """Return one exact output declaration, if present."""
        _require_exact(name, PortName, "output port name")
        return next((item for item in self.outputs if item.name == name), None)

    def __repr__(self) -> str:
        return f"NodePortSchema(inputs={len(self.inputs)}, outputs={len(self.outputs)})"


def _require_exact(value: object, expected: type[object], subject: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{subject} must use {expected.__name__}")


def _require_exact_tuple[T](value: object, item_type: type[T], subject: str) -> tuple[T, ...]:
    if not isinstance(value, tuple):
        raise TypeError(f"{subject} must be a tuple")
    items = cast(tuple[object, ...], value)
    if any(type(item) is not item_type for item in items):
        raise TypeError(f"{subject} contains an invalid value")
    return cast(tuple[T, ...], items)


def _require_unique(values: tuple[object, ...], subject: str) -> None:
    if len(set(values)) != len(values):
        raise PortContractError(f"{subject} must be unique")


EMPTY_NODE_PORT_SCHEMA = NodePortSchema((), ())
