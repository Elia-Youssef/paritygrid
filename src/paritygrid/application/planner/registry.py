"""Dependency-neutral contracts for the closed pipeline node registry."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from paritygrid.application.planner.documents import PipelineDocument
from paritygrid.application.planner.ports import (
    InputPortDefinition,
    NodePortSchema,
    OutputPortDefinition,
    PortValueType,
)
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.domain.pipeline import NodeKind, PortName

NODE_REGISTRY_VERSION = 1
MAX_NODE_REGISTRY_DEFINITIONS = 128
MAX_NODE_CONFIGURATION_FIELDS = 64
MAX_NODE_CONFIGURATION_FIELD_LENGTH = 64

_CONFIGURATION_FIELD_PATTERN = re.compile(
    r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*",
    flags=re.ASCII,
)


class NodeRegistryError(ValueError):
    """Base failure for registry definitions and lookups."""


class UnknownNodeKindError(NodeRegistryError):
    """A pipeline references a kind outside the closed registry."""


class UnsupportedNodeConfigurationVersionError(NodeRegistryError):
    """A node requests a configuration version the registry does not define."""


class InvalidNodeConfigurationError(NodeRegistryError):
    """A node configuration violates its registered structural schema."""


class NodeRole(StrEnum):
    """Closed semantic roles used by later planner validators."""

    SOURCE = "source"
    TRANSFORM = "transform"
    RECONCILIATION = "reconciliation"
    REPAIR_PLAN = "repair_plan"
    APPROVAL = "approval"
    REPAIR_EFFECT = "repair_effect"
    VERIFICATION = "verification"
    EXPORT = "export"


class ConnectorRequirement(StrEnum):
    """Connector relationship required by a node definition."""

    NONE = "none"
    SOURCE = "source"
    TARGET = "target"


class RetryBehavior(StrEnum):
    """Closed retry ownership declared by a node definition."""

    NEVER = "never"
    CONNECTOR = "connector"


class PlannerRunnerKind(StrEnum):
    """Runner families a compiled node may support."""

    SEQUENTIAL = "sequential"
    THREADED = "threaded"
    ASYNCIO = "asyncio"
    PROCESS = "process"


class NodeConfigurationValueKind(StrEnum):
    """Exact scalar kinds accepted by a registered configuration field."""

    BOOLEAN = "boolean"
    INTEGER = "integer"
    TEXT = "text"


@dataclass(frozen=True, slots=True, order=True)
class NodeConfigurationField:
    """One exact field in a versioned node configuration schema."""

    name: str
    value_kind: NodeConfigurationValueKind
    required: bool

    def __post_init__(self) -> None:
        name = cast(object, self.name)
        if not isinstance(name, str):
            raise TypeError("node configuration field name must be text")
        if not 1 <= len(name) <= MAX_NODE_CONFIGURATION_FIELD_LENGTH:
            raise NodeRegistryError("node configuration field name is outside the size limit")
        if not name.isascii() or _CONFIGURATION_FIELD_PATTERN.fullmatch(name) is None:
            raise NodeRegistryError("node configuration field name must use canonical snake case")
        _require_exact(
            self.value_kind,
            NodeConfigurationValueKind,
            "node configuration value kind",
        )
        if type(self.required) is not bool:
            raise TypeError("node configuration field required marker must be boolean")


@dataclass(frozen=True, slots=True, repr=False)
class NodeConfigurationSchema:
    """One immutable exact-key scalar configuration schema."""

    version: int
    fields: tuple[NodeConfigurationField, ...]

    def __post_init__(self) -> None:
        version = cast(object, self.version)
        if type(version) is not int:
            raise TypeError("node configuration schema version must be an integer")
        if not 1 <= version <= 2_147_483_647:
            raise NodeRegistryError(
                "node configuration schema version is outside the supported range"
            )
        fields = _require_exact_tuple(
            self.fields,
            NodeConfigurationField,
            "node configuration fields",
        )
        if len(fields) > MAX_NODE_CONFIGURATION_FIELDS:
            raise NodeRegistryError("node configuration schema exceeds the field limit")
        names = tuple(field.name for field in fields)
        _require_unique(names, "node configuration field names")
        object.__setattr__(self, "fields", tuple(sorted(fields, key=lambda field: field.name)))

    def validate(self, document: ConfigurationDocument) -> None:
        """Reject missing, unknown, or wrongly typed configuration fields."""
        _require_exact(document, ConfigurationDocument, "node configuration")
        mapping = document.to_mapping()
        definitions = {field.name: field for field in self.fields}
        missing = tuple(
            field.name for field in self.fields if field.required and field.name not in mapping
        )
        if missing:
            raise InvalidNodeConfigurationError("node configuration is missing required fields")
        if frozenset(mapping) - frozenset(definitions):
            raise InvalidNodeConfigurationError("node configuration contains unknown fields")
        if any(
            not _matches_value_kind(value, definitions[name].value_kind)
            for name, value in mapping.items()
        ):
            raise InvalidNodeConfigurationError(
                "node configuration contains an invalid field value"
            )

    def __repr__(self) -> str:
        return f"NodeConfigurationSchema(version={self.version!r}, fields={len(self.fields)})"


@dataclass(frozen=True, slots=True)
class NodeDefinition:
    """Complete dependency-neutral metadata for one closed node kind."""

    kind: NodeKind
    role: NodeRole
    configuration_schema: NodeConfigurationSchema
    port_schema: NodePortSchema
    connector_requirement: ConnectorRequirement
    supported_runners: tuple[PlannerRunnerKind, ...]
    retry_behavior: RetryBehavior
    requires_idempotency: bool

    def __post_init__(self) -> None:
        _require_exact(self.kind, NodeKind, "node definition kind")
        _require_exact(self.role, NodeRole, "node definition role")
        _require_exact(
            self.configuration_schema,
            NodeConfigurationSchema,
            "node definition configuration schema",
        )
        _require_exact(self.port_schema, NodePortSchema, "node definition port schema")
        _require_exact(
            self.connector_requirement,
            ConnectorRequirement,
            "node definition connector requirement",
        )
        runners = _require_exact_tuple(
            self.supported_runners,
            PlannerRunnerKind,
            "node definition supported runners",
        )
        if not runners:
            raise NodeRegistryError("node definition requires at least one supported runner")
        _require_unique(runners, "node definition supported runners")
        _require_exact(self.retry_behavior, RetryBehavior, "node definition retry behavior")
        if type(self.requires_idempotency) is not bool:
            raise TypeError("node definition idempotency marker must be boolean")
        if self.role is NodeRole.REPAIR_EFFECT and not self.requires_idempotency:
            raise NodeRegistryError("repair effect nodes must require idempotency")
        object.__setattr__(
            self,
            "supported_runners",
            tuple(sorted(runners, key=lambda runner: runner.value)),
        )


@dataclass(frozen=True, slots=True, repr=False)
class NodeRegistry:
    """An immutable versioned set of unique node definitions."""

    definitions: tuple[NodeDefinition, ...]
    version: int = NODE_REGISTRY_VERSION

    def __post_init__(self) -> None:
        version = cast(object, self.version)
        if type(version) is not int:
            raise TypeError("node registry version must be an integer")
        if version != NODE_REGISTRY_VERSION:
            raise NodeRegistryError("node registry version is unsupported")
        definitions = _require_exact_tuple(
            self.definitions,
            NodeDefinition,
            "node registry definitions",
        )
        if not definitions:
            raise NodeRegistryError("node registry requires at least one definition")
        if len(definitions) > MAX_NODE_REGISTRY_DEFINITIONS:
            raise NodeRegistryError("node registry exceeds the definition limit")
        kinds = tuple(definition.kind for definition in definitions)
        _require_unique(kinds, "node registry kinds")
        object.__setattr__(
            self,
            "definitions",
            tuple(sorted(definitions, key=lambda definition: str(definition.kind))),
        )

    def get(self, kind: NodeKind) -> NodeDefinition | None:
        """Return one exact kind definition, if registered."""
        _require_exact(kind, NodeKind, "node kind")
        return next(
            (definition for definition in self.definitions if definition.kind == kind), None
        )

    def require(self, kind: NodeKind, configuration_version: int) -> NodeDefinition:
        """Resolve one kind and exact registered configuration version."""
        definition = self.get(kind)
        if definition is None:
            raise UnknownNodeKindError("pipeline node kind is not registered")
        version = cast(object, configuration_version)
        if type(version) is not int:
            raise TypeError("node configuration version must be an integer")
        if version != definition.configuration_schema.version:
            raise UnsupportedNodeConfigurationVersionError(
                "pipeline node configuration version is unsupported"
            )
        return definition

    def __repr__(self) -> str:
        return f"NodeRegistry(version={self.version!r}, definitions={len(self.definitions)})"


def _matches_value_kind(value: object, kind: NodeConfigurationValueKind) -> bool:
    if kind is NodeConfigurationValueKind.BOOLEAN:
        return type(value) is bool
    if kind is NodeConfigurationValueKind.INTEGER:
        return type(value) is int
    return isinstance(value, str)


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
        raise NodeRegistryError(f"{subject} must be unique")


_STANDARD_RUNNERS = (
    PlannerRunnerKind.SEQUENTIAL,
    PlannerRunnerKind.THREADED,
    PlannerRunnerKind.ASYNCIO,
)
_CPU_RUNNERS = (*_STANDARD_RUNNERS, PlannerRunnerKind.PROCESS)
_EMPTY_CONFIGURATION_V1 = NodeConfigurationSchema(1, ())
_HTTP_SOURCE_CONFIGURATION_V1 = NodeConfigurationSchema(
    1,
    (NodeConfigurationField("page_size", NodeConfigurationValueKind.INTEGER, False),),
)
_CSV_SOURCE_CONFIGURATION_V1 = NodeConfigurationSchema(
    1,
    (
        NodeConfigurationField("encoding", NodeConfigurationValueKind.TEXT, False),
        NodeConfigurationField("header", NodeConfigurationValueKind.BOOLEAN, False),
    ),
)
_JSONL_SOURCE_CONFIGURATION_V1 = NodeConfigurationSchema(
    1,
    (NodeConfigurationField("encoding", NodeConfigurationValueKind.TEXT, False),),
)
_PARTITION_CONFIGURATION_V1 = NodeConfigurationSchema(
    1,
    (NodeConfigurationField("partition_count", NodeConfigurationValueKind.INTEGER, False),),
)
_EXPORT_CONFIGURATION_V1 = NodeConfigurationSchema(
    1,
    (NodeConfigurationField("compression", NodeConfigurationValueKind.TEXT, False),),
)


def _input(
    name: str,
    *accepted_types: PortValueType,
) -> InputPortDefinition:
    return InputPortDefinition(PortName(name), accepted_types)


def _output(name: str, value_type: PortValueType) -> OutputPortDefinition:
    return OutputPortDefinition(PortName(name), value_type)


_SOURCE_PORTS = NodePortSchema((), (_output("records", PortValueType.RAW_RECORDS),))
_NORMALIZE_PORTS = NodePortSchema(
    (_input("records", PortValueType.RAW_RECORDS),),
    (_output("records", PortValueType.NORMALIZED_RECORDS),),
)
_VALIDATE_PORTS = NodePortSchema(
    (_input("records", PortValueType.NORMALIZED_RECORDS),),
    (_output("records", PortValueType.VALIDATED_RECORDS),),
)
_PARTITION_PORTS = NodePortSchema(
    (_input("records", PortValueType.VALIDATED_RECORDS),),
    (_output("records", PortValueType.PARTITIONED_RECORDS),),
)
_RECONCILE_PORTS = NodePortSchema(
    (
        _input(
            "records",
            PortValueType.NORMALIZED_RECORDS,
            PortValueType.VALIDATED_RECORDS,
            PortValueType.PARTITIONED_RECORDS,
        ),
    ),
    (_output("reconciliation", PortValueType.RECONCILIATION),),
)
_REPAIR_GENERATE_PORTS = NodePortSchema(
    (_input("reconciliation", PortValueType.RECONCILIATION),),
    (_output("repair-plan", PortValueType.REPAIR_PLAN),),
)
_REPAIR_APPROVAL_PORTS = NodePortSchema(
    (_input("repair-plan", PortValueType.REPAIR_PLAN),),
    (_output("approved-plan", PortValueType.APPROVED_REPAIR_PLAN),),
)
_REPAIR_APPLY_PORTS = NodePortSchema(
    (_input("approved-plan", PortValueType.APPROVED_REPAIR_PLAN),),
    (_output("repair-result", PortValueType.REPAIR_RESULT),),
)
_VERIFY_PORTS = NodePortSchema(
    (_input("repair-result", PortValueType.REPAIR_RESULT),),
    (_output("verification", PortValueType.VERIFICATION),),
)
_EXPORT_PORTS = NodePortSchema(
    (
        _input(
            "records",
            PortValueType.RAW_RECORDS,
            PortValueType.NORMALIZED_RECORDS,
            PortValueType.VALIDATED_RECORDS,
            PortValueType.PARTITIONED_RECORDS,
        ),
    ),
    (),
)


def _definition(
    kind: str,
    role: NodeRole,
    configuration_schema: NodeConfigurationSchema,
    port_schema: NodePortSchema,
    connector_requirement: ConnectorRequirement,
    supported_runners: tuple[PlannerRunnerKind, ...],
    retry_behavior: RetryBehavior,
    *,
    requires_idempotency: bool = False,
) -> NodeDefinition:
    return NodeDefinition(
        NodeKind(kind),
        role,
        configuration_schema,
        port_schema,
        connector_requirement,
        supported_runners,
        retry_behavior,
        requires_idempotency,
    )


BUILTIN_NODE_REGISTRY = NodeRegistry(
    (
        _definition(
            "source.http.async",
            NodeRole.SOURCE,
            _HTTP_SOURCE_CONFIGURATION_V1,
            _SOURCE_PORTS,
            ConnectorRequirement.SOURCE,
            _STANDARD_RUNNERS,
            RetryBehavior.CONNECTOR,
        ),
        _definition(
            "source.http.blocking",
            NodeRole.SOURCE,
            _HTTP_SOURCE_CONFIGURATION_V1,
            _SOURCE_PORTS,
            ConnectorRequirement.SOURCE,
            _STANDARD_RUNNERS,
            RetryBehavior.CONNECTOR,
        ),
        _definition(
            "source.csv",
            NodeRole.SOURCE,
            _CSV_SOURCE_CONFIGURATION_V1,
            _SOURCE_PORTS,
            ConnectorRequirement.SOURCE,
            _STANDARD_RUNNERS,
            RetryBehavior.CONNECTOR,
        ),
        _definition(
            "source.jsonl",
            NodeRole.SOURCE,
            _JSONL_SOURCE_CONFIGURATION_V1,
            _SOURCE_PORTS,
            ConnectorRequirement.SOURCE,
            _STANDARD_RUNNERS,
            RetryBehavior.CONNECTOR,
        ),
        _definition(
            "transform.normalize",
            NodeRole.TRANSFORM,
            _EMPTY_CONFIGURATION_V1,
            _NORMALIZE_PORTS,
            ConnectorRequirement.NONE,
            _CPU_RUNNERS,
            RetryBehavior.NEVER,
        ),
        _definition(
            "transform.validate",
            NodeRole.TRANSFORM,
            _EMPTY_CONFIGURATION_V1,
            _VALIDATE_PORTS,
            ConnectorRequirement.NONE,
            _CPU_RUNNERS,
            RetryBehavior.NEVER,
        ),
        _definition(
            "transform.partition",
            NodeRole.TRANSFORM,
            _PARTITION_CONFIGURATION_V1,
            _PARTITION_PORTS,
            ConnectorRequirement.NONE,
            _CPU_RUNNERS,
            RetryBehavior.NEVER,
        ),
        _definition(
            "reconcile.target",
            NodeRole.RECONCILIATION,
            _EMPTY_CONFIGURATION_V1,
            _RECONCILE_PORTS,
            ConnectorRequirement.TARGET,
            _STANDARD_RUNNERS,
            RetryBehavior.CONNECTOR,
        ),
        _definition(
            "repair.generate",
            NodeRole.REPAIR_PLAN,
            _EMPTY_CONFIGURATION_V1,
            _REPAIR_GENERATE_PORTS,
            ConnectorRequirement.NONE,
            _CPU_RUNNERS,
            RetryBehavior.NEVER,
        ),
        _definition(
            "repair.approval",
            NodeRole.APPROVAL,
            _EMPTY_CONFIGURATION_V1,
            _REPAIR_APPROVAL_PORTS,
            ConnectorRequirement.NONE,
            _STANDARD_RUNNERS,
            RetryBehavior.NEVER,
        ),
        _definition(
            "repair.apply",
            NodeRole.REPAIR_EFFECT,
            _EMPTY_CONFIGURATION_V1,
            _REPAIR_APPLY_PORTS,
            ConnectorRequirement.TARGET,
            _STANDARD_RUNNERS,
            RetryBehavior.CONNECTOR,
            requires_idempotency=True,
        ),
        _definition(
            "verify.target",
            NodeRole.VERIFICATION,
            _EMPTY_CONFIGURATION_V1,
            _VERIFY_PORTS,
            ConnectorRequirement.TARGET,
            _STANDARD_RUNNERS,
            RetryBehavior.CONNECTOR,
        ),
        _definition(
            "export.parquet",
            NodeRole.EXPORT,
            _EXPORT_CONFIGURATION_V1,
            _EXPORT_PORTS,
            ConnectorRequirement.NONE,
            _STANDARD_RUNNERS,
            RetryBehavior.NEVER,
        ),
    )
)
BUILTIN_NODE_DEFINITIONS = BUILTIN_NODE_REGISTRY.definitions


def registered_node_definition(
    kind: NodeKind,
    configuration_version: int,
) -> NodeDefinition:
    """Resolve one kind only from the immutable built-in registry."""
    return BUILTIN_NODE_REGISTRY.require(kind, configuration_version)


def validate_registered_nodes(document: PipelineDocument) -> None:
    """Reject unknown kinds, versions, and configuration shapes in one draft."""
    _require_exact(document, PipelineDocument, "pipeline document")
    for node in document.nodes:
        definition = registered_node_definition(node.kind, node.configuration_version)
        definition.configuration_schema.validate(node.configuration)
