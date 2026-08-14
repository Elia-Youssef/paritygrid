from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Any, cast

import pytest

from paritygrid.application.planner import (
    EMPTY_NODE_PORT_SCHEMA,
    MAX_INPUT_ACCEPTED_TYPES,
    MAX_INPUT_CONNECTIONS,
    MAX_NODE_INPUT_PORTS,
    MAX_NODE_OUTPUT_PORTS,
    InputPortDefinition,
    NodePortSchema,
    OutputPortDefinition,
    PortContractError,
    PortValueType,
)
from paritygrid.domain.pipeline import PortName


def _input(
    name: str = "records",
    accepted_types: tuple[PortValueType, ...] = (PortValueType.RAW_RECORDS,),
    *,
    required: bool = True,
    maximum_connections: int = 1,
) -> InputPortDefinition:
    return InputPortDefinition(
        PortName(name),
        accepted_types,
        required,
        maximum_connections,
    )


def _output(
    name: str = "records",
    value_type: PortValueType = PortValueType.RAW_RECORDS,
) -> OutputPortDefinition:
    return OutputPortDefinition(PortName(name), value_type)


def test_port_value_types_are_closed_and_stable() -> None:
    assert tuple(item.value for item in PortValueType) == (
        "records.raw",
        "records.normalized",
        "records.validated",
        "records.partitioned",
        "reconciliation.result",
        "repair.plan",
        "repair.plan.approved",
        "repair.result",
        "verification.result",
    )


def test_input_port_is_immutable_sorted_and_exactly_typed() -> None:
    definition = _input(
        accepted_types=(PortValueType.VALIDATED_RECORDS, PortValueType.RAW_RECORDS),
        required=False,
        maximum_connections=2,
    )
    assert definition.accepted_types == (
        PortValueType.RAW_RECORDS,
        PortValueType.VALIDATED_RECORDS,
    )
    assert definition.accepts(PortValueType.RAW_RECORDS)
    assert not definition.accepts(PortValueType.REPAIR_PLAN)
    with pytest.raises(FrozenInstanceError):
        definition.required = True  # type: ignore[misc]
    with pytest.raises(TypeError, match="port value type must use PortValueType"):
        definition.accepts(cast(Any, "records.raw"))


@pytest.mark.parametrize(
    ("arguments", "error", "message"),
    [
        ((cast(Any, "records"), (PortValueType.RAW_RECORDS,), True, 1), TypeError, "name"),
        ((PortName("records"), cast(Any, []), True, 1), TypeError, "tuple"),
        ((PortName("records"), cast(Any, ("records.raw",)), True, 1), TypeError, "invalid"),
        ((PortName("records"), (), True, 1), PortContractError, "at least one"),
        (
            (
                PortName("records"),
                (PortValueType.RAW_RECORDS,) * (MAX_INPUT_ACCEPTED_TYPES + 1),
                True,
                1,
            ),
            PortContractError,
            "accepted type limit",
        ),
        (
            (
                PortName("records"),
                (PortValueType.RAW_RECORDS, PortValueType.RAW_RECORDS),
                True,
                1,
            ),
            PortContractError,
            "unique",
        ),
        (
            (PortName("records"), (PortValueType.RAW_RECORDS,), cast(Any, 1), 1),
            TypeError,
            "boolean",
        ),
        (
            (PortName("records"), (PortValueType.RAW_RECORDS,), True, cast(Any, True)),
            TypeError,
            "integer",
        ),
        (
            (PortName("records"), (PortValueType.RAW_RECORDS,), True, 0),
            PortContractError,
            "outside",
        ),
        (
            (PortName("records"), (PortValueType.RAW_RECORDS,), True, MAX_INPUT_CONNECTIONS + 1),
            PortContractError,
            "outside",
        ),
    ],
)
def test_input_port_rejects_invalid_contracts(
    arguments: tuple[object, ...],
    error: type[Exception],
    message: str,
) -> None:
    with pytest.raises(error, match=message):
        InputPortDefinition(*cast(Any, arguments))


def test_output_port_requires_exact_contract_values() -> None:
    definition = _output()
    assert definition.name == PortName("records")
    assert definition.value_type is PortValueType.RAW_RECORDS
    with pytest.raises(TypeError, match="output port name"):
        OutputPortDefinition(cast(Any, "records"), PortValueType.RAW_RECORDS)
    with pytest.raises(TypeError, match="output port value type"):
        OutputPortDefinition(PortName("records"), cast(Any, "records.raw"))


def test_node_port_schema_is_sorted_bounded_unique_and_redacted() -> None:
    schema = NodePortSchema(
        (_input("z-input"), _input("a-input")),
        (_output("z-output"), _output("a-output")),
    )
    assert tuple(str(item.name) for item in schema.inputs) == ("a-input", "z-input")
    assert tuple(str(item.name) for item in schema.outputs) == ("a-output", "z-output")
    assert schema.input(PortName("a-input")) == _input("a-input")
    assert schema.input(PortName("missing")) is None
    assert schema.output(PortName("z-output")) == _output("z-output")
    assert schema.output(PortName("missing")) is None
    assert repr(schema) == "NodePortSchema(inputs=2, outputs=2)"
    with pytest.raises(TypeError, match="input port name"):
        schema.input(cast(Any, "a-input"))
    with pytest.raises(TypeError, match="output port name"):
        schema.output(cast(Any, "z-output"))
    with pytest.raises(FrozenInstanceError):
        schema.inputs = ()  # type: ignore[misc]


@pytest.mark.parametrize(
    ("inputs", "outputs", "error", "message"),
    [
        (cast(Any, []), (), TypeError, "tuple"),
        (cast(Any, (object(),)), (), TypeError, "invalid"),
        ((), cast(Any, []), TypeError, "tuple"),
        ((), cast(Any, (object(),)), TypeError, "invalid"),
        (
            tuple(_input(f"input-{index}") for index in range(MAX_NODE_INPUT_PORTS + 1)),
            (),
            PortContractError,
            "input port limit",
        ),
        (
            (),
            tuple(_output(f"output-{index}") for index in range(MAX_NODE_OUTPUT_PORTS + 1)),
            PortContractError,
            "output port limit",
        ),
        ((_input(), _input()), (), PortContractError, "input port names"),
        ((), (_output(), _output()), PortContractError, "output port names"),
    ],
)
def test_node_port_schema_rejects_invalid_collections(
    inputs: tuple[InputPortDefinition, ...],
    outputs: tuple[OutputPortDefinition, ...],
    error: type[Exception],
    message: str,
) -> None:
    with pytest.raises(error, match=message):
        NodePortSchema(inputs, outputs)


def test_empty_node_port_schema_is_a_canonical_contract() -> None:
    assert EMPTY_NODE_PORT_SCHEMA is not None
    assert EMPTY_NODE_PORT_SCHEMA.inputs == ()
    assert EMPTY_NODE_PORT_SCHEMA.outputs == ()
