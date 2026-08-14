"""Dependency-neutral contracts for the closed pipeline node registry."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.domain.pipeline import NodeKind

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
