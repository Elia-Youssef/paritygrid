"""Closed node-registry contract tests."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.application.planner import (
    EMPTY_NODE_PORT_SCHEMA,
    MAX_NODE_CONFIGURATION_FIELD_LENGTH,
    NODE_REGISTRY_VERSION,
    ConnectorRequirement,
    InvalidNodeConfigurationError,
    NodeConfigurationField,
    NodeConfigurationSchema,
    NodeConfigurationValueKind,
    NodeDefinition,
    NodeRegistry,
    NodeRegistryError,
    NodeRole,
    PlannerRunnerKind,
    RetryBehavior,
    UnknownNodeKindError,
    UnsupportedNodeConfigurationVersionError,
)
from paritygrid.application.planner import registry as contract
from paritygrid.application.ports import ConfigurationDocument
from paritygrid.domain.pipeline import NodeKind


def _field(
    name: str = "batch_size",
    kind: NodeConfigurationValueKind = NodeConfigurationValueKind.INTEGER,
    required: bool = True,
) -> NodeConfigurationField:
    return NodeConfigurationField(name, kind, required)


def _schema(*fields: NodeConfigurationField, version: int = 1) -> NodeConfigurationSchema:
    return NodeConfigurationSchema(version, tuple(fields))


def _definition(
    kind: str = "source.example",
    *,
    role: NodeRole = NodeRole.SOURCE,
    schema: NodeConfigurationSchema | None = None,
    connector: ConnectorRequirement = ConnectorRequirement.SOURCE,
    runners: tuple[PlannerRunnerKind, ...] = (
        PlannerRunnerKind.THREADED,
        PlannerRunnerKind.SEQUENTIAL,
    ),
    retry: RetryBehavior = RetryBehavior.CONNECTOR,
    idempotency: bool = False,
) -> NodeDefinition:
    return NodeDefinition(
        NodeKind(kind),
        role,
        schema or _schema(),
        EMPTY_NODE_PORT_SCHEMA,
        connector,
        runners,
        retry,
        idempotency,
    )


def test_registry_contract_is_dependency_neutral_and_versioned() -> None:
    assert NODE_REGISTRY_VERSION == 1
    source = Path(contract.__file__).read_text(encoding="utf-8")
    assert "sqlalchemy" not in source
    assert "fastapi" not in source
    assert "pydantic" not in source


def test_closed_enum_values_are_frozen() -> None:
    assert tuple(NodeRole) == (
        NodeRole.SOURCE,
        NodeRole.TRANSFORM,
        NodeRole.RECONCILIATION,
        NodeRole.REPAIR_PLAN,
        NodeRole.APPROVAL,
        NodeRole.REPAIR_EFFECT,
        NodeRole.VERIFICATION,
        NodeRole.EXPORT,
    )
    assert tuple(ConnectorRequirement) == (
        ConnectorRequirement.NONE,
        ConnectorRequirement.SOURCE,
        ConnectorRequirement.TARGET,
    )
    assert tuple(RetryBehavior) == (RetryBehavior.NEVER, RetryBehavior.CONNECTOR)
    assert tuple(PlannerRunnerKind) == (
        PlannerRunnerKind.SEQUENTIAL,
        PlannerRunnerKind.THREADED,
        PlannerRunnerKind.ASYNCIO,
        PlannerRunnerKind.PROCESS,
    )
    assert tuple(NodeConfigurationValueKind) == (
        NodeConfigurationValueKind.BOOLEAN,
        NodeConfigurationValueKind.INTEGER,
        NodeConfigurationValueKind.TEXT,
    )


def test_configuration_field_is_exact_immutable_and_canonical() -> None:
    field = _field()
    assert field.name == "batch_size"
    with pytest.raises(FrozenInstanceError):
        field.name = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError, match="text"):
        NodeConfigurationField(cast(Any, 1), NodeConfigurationValueKind.INTEGER, True)
    for name in ("", "BatchSize", "batch-size", "batch__size", "é", "x" * 65):
        with pytest.raises(NodeRegistryError):
            NodeConfigurationField(name, NodeConfigurationValueKind.INTEGER, True)
    with pytest.raises(TypeError, match="NodeConfigurationValueKind"):
        NodeConfigurationField("field", cast(Any, "integer"), True)
    with pytest.raises(TypeError, match="boolean"):
        NodeConfigurationField("field", NodeConfigurationValueKind.INTEGER, cast(Any, 1))
    assert MAX_NODE_CONFIGURATION_FIELD_LENGTH == 64


def test_configuration_schema_sorts_fields_and_redacts_shape() -> None:
    schema = _schema(
        _field("zulu", NodeConfigurationValueKind.TEXT, False),
        _field("alpha", NodeConfigurationValueKind.BOOLEAN, True),
    )
    assert tuple(field.name for field in schema.fields) == ("alpha", "zulu")
    assert repr(schema) == "NodeConfigurationSchema(version=1, fields=2)"
    with pytest.raises(TypeError, match="integer"):
        _schema(version=cast(Any, True))
    for version in (0, 2_147_483_648):
        with pytest.raises(NodeRegistryError, match="version"):
            _schema(version=version)
    with pytest.raises(TypeError, match="tuple"):
        NodeConfigurationSchema(1, cast(Any, []))
    with pytest.raises(TypeError, match="invalid"):
        NodeConfigurationSchema(1, cast(Any, (object(),)))
    with pytest.raises(NodeRegistryError, match="unique"):
        _schema(_field(), _field())


def test_configuration_schema_enforces_field_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(contract, "MAX_NODE_CONFIGURATION_FIELDS", 0)
    with pytest.raises(NodeRegistryError, match="field limit"):
        _schema(_field())


@pytest.mark.parametrize(
    ("field", "valid", "invalid"),
    [
        (_field("enabled", NodeConfigurationValueKind.BOOLEAN), True, 1),
        (_field("batch_size", NodeConfigurationValueKind.INTEGER), 1, True),
        (_field("encoding", NodeConfigurationValueKind.TEXT), "utf-8", ["utf-8"]),
    ],
)
def test_configuration_schema_validates_exact_scalar_kinds(
    field: NodeConfigurationField, valid: object, invalid: object
) -> None:
    schema = _schema(field)
    schema.validate(ConfigurationDocument.from_mapping({field.name: valid}))
    with pytest.raises(InvalidNodeConfigurationError, match="invalid"):
        schema.validate(ConfigurationDocument.from_mapping({field.name: invalid}))


def test_configuration_schema_rejects_missing_unknown_and_wrong_contract() -> None:
    schema = _schema(_field(), _field("label", NodeConfigurationValueKind.TEXT, False))
    with pytest.raises(InvalidNodeConfigurationError, match="missing"):
        schema.validate(ConfigurationDocument.from_mapping({}))
    with pytest.raises(InvalidNodeConfigurationError, match="unknown"):
        schema.validate(ConfigurationDocument.from_mapping({"batch_size": 1, "command": "value"}))
    with pytest.raises(TypeError, match="ConfigurationDocument"):
        schema.validate(cast(Any, {}))


def test_definition_canonicalizes_runners_and_enforces_repair_safety() -> None:
    definition = _definition()
    assert definition.supported_runners == (
        PlannerRunnerKind.SEQUENTIAL,
        PlannerRunnerKind.THREADED,
    )
    with pytest.raises(TypeError, match="NodeKind"):
        replace(definition, kind="source.example")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="NodeRole"):
        replace(definition, role="source")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="NodeConfigurationSchema"):
        replace(definition, configuration_schema={})  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="NodePortSchema"):
        replace(definition, port_schema={})  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="ConnectorRequirement"):
        replace(definition, connector_requirement="source")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="tuple"):
        replace(definition, supported_runners=cast(Any, []))
    with pytest.raises(TypeError, match="invalid"):
        replace(definition, supported_runners=cast(Any, ("threaded",)))
    with pytest.raises(NodeRegistryError, match="at least"):
        replace(definition, supported_runners=())
    with pytest.raises(NodeRegistryError, match="unique"):
        replace(
            definition,
            supported_runners=(PlannerRunnerKind.SEQUENTIAL, PlannerRunnerKind.SEQUENTIAL),
        )
    with pytest.raises(TypeError, match="RetryBehavior"):
        replace(definition, retry_behavior="never")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="boolean"):
        replace(definition, requires_idempotency=cast(Any, 1))
    with pytest.raises(NodeRegistryError, match="idempotency"):
        _definition(
            role=NodeRole.REPAIR_EFFECT,
            connector=ConnectorRequirement.TARGET,
            retry=RetryBehavior.CONNECTOR,
        )
    assert _definition(
        role=NodeRole.REPAIR_EFFECT,
        connector=ConnectorRequirement.TARGET,
        idempotency=True,
    ).requires_idempotency


def test_registry_is_immutable_sorted_bounded_and_unique(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _definition("source.zulu")
    transform = _definition(
        "transform.alpha",
        role=NodeRole.TRANSFORM,
        connector=ConnectorRequirement.NONE,
        retry=RetryBehavior.NEVER,
    )
    registry = NodeRegistry((source, transform))
    assert tuple(str(item.kind) for item in registry.definitions) == (
        "source.zulu",
        "transform.alpha",
    )
    assert repr(registry) == "NodeRegistry(version=1, definitions=2)"
    with pytest.raises(TypeError, match="integer"):
        NodeRegistry((source,), cast(Any, True))
    with pytest.raises(NodeRegistryError, match="unsupported"):
        NodeRegistry((source,), 2)
    with pytest.raises(TypeError, match="tuple"):
        NodeRegistry(cast(Any, [source]))
    with pytest.raises(TypeError, match="invalid"):
        NodeRegistry(cast(Any, (object(),)))
    with pytest.raises(NodeRegistryError, match="at least"):
        NodeRegistry(())
    with pytest.raises(NodeRegistryError, match="unique"):
        NodeRegistry((source, source))
    monkeypatch.setattr(contract, "MAX_NODE_REGISTRY_DEFINITIONS", 0)
    with pytest.raises(NodeRegistryError, match="definition limit"):
        NodeRegistry((source,))


def test_registry_lookup_requires_exact_known_kind_and_configuration_version() -> None:
    definition = _definition()
    registry = NodeRegistry((definition,))
    assert registry.get(NodeKind("source.example")) == definition
    assert registry.get(NodeKind("source.unknown")) is None
    assert registry.require(NodeKind("source.example"), 1) == definition
    with pytest.raises(TypeError, match="NodeKind"):
        registry.get(cast(Any, "source.example"))
    with pytest.raises(UnknownNodeKindError):
        registry.require(NodeKind("source.unknown"), 1)
    with pytest.raises(TypeError, match="integer"):
        registry.require(NodeKind("source.example"), cast(Any, True))
    with pytest.raises(UnsupportedNodeConfigurationVersionError):
        registry.require(NodeKind("source.example"), 2)
