"""Dependency-neutral connector capability and snapshot contracts."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.domain.models import ConnectorId

MAX_CONNECTOR_SNAPSHOT_REFERENCES = 64
MAX_CONNECTOR_KIND_LENGTH = 96
MAX_CONNECTOR_REFERENCE_LENGTH = 128

_KIND_PATTERN = re.compile(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*", flags=re.ASCII)
_REFERENCE_PATTERN = re.compile(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*", flags=re.ASCII)
_ENVIRONMENT_PATTERN = re.compile(r"[A-Z][A-Z0-9_]*", flags=re.ASCII)


class ConnectorValidationError(ValueError):
    """Base failure for connector bindings and capabilities."""


class MissingConnectorError(ConnectorValidationError):
    """A pipeline connector binding is absent or unavailable."""


class MissingConnectorCapabilityError(ConnectorValidationError):
    """A connector lacks a capability required by its node."""


class InvalidConnectorSnapshotError(ConnectorValidationError):
    """Connector metadata cannot form a safe immutable snapshot."""


class ConnectorCapability(StrEnum):
    """Closed connector behaviors relevant to planning."""

    READ = "read"
    WRITE = "write"
    ASYNC_IO = "async_io"
    BLOCKING_IO = "blocking_io"
    IDEMPOTENCY = "idempotency"
    SCHEMA_DISCOVERY = "schema_discovery"


@dataclass(frozen=True, slots=True, repr=False)
class ConnectorCapabilitySet:
    """Canonical immutable set of closed connector capabilities."""

    values: tuple[ConnectorCapability, ...]

    def __post_init__(self) -> None:
        values = _require_exact_tuple(
            self.values,
            ConnectorCapability,
            "connector capabilities",
        )
        if len(set(values)) != len(values):
            raise ConnectorValidationError("connector capabilities must be unique")
        object.__setattr__(
            self,
            "values",
            tuple(sorted(values, key=lambda capability: capability.value)),
        )

    def supports(self, capability: ConnectorCapability) -> bool:
        """Return whether one exact closed capability is present."""
        _require_exact(capability, ConnectorCapability, "connector capability")
        return capability in self.values

    def to_mapping(self) -> dict[str, bool]:
        """Return every closed capability as a stable boolean object."""
        return {capability.value: capability in self.values for capability in ConnectorCapability}

    def __repr__(self) -> str:
        return f"ConnectorCapabilitySet(enabled={len(self.values)})"


@dataclass(frozen=True, slots=True, order=True, repr=False)
class ConnectorReferenceSnapshot:
    """One reference name and environment-variable name, never its value."""

    reference_name: str
    environment_variable_name: str

    def __post_init__(self) -> None:
        _validate_text(
            self.reference_name,
            _REFERENCE_PATTERN,
            "connector reference name",
        )
        _validate_text(
            self.environment_variable_name,
            _ENVIRONMENT_PATTERN,
            "connector environment variable name",
        )

    def to_mapping(self) -> dict[str, str]:
        """Return names only for immutable publication metadata."""
        return {
            "environment_variable_name": self.environment_variable_name,
            "reference_name": self.reference_name,
        }

    def __repr__(self) -> str:
        return "ConnectorReferenceSnapshot(redacted=True)"


@dataclass(frozen=True, slots=True, repr=False)
class ConnectorBindingSnapshot:
    """Immutable non-secret connector definition captured for publication."""

    connector_id: ConnectorId
    kind: str
    revision: int
    configuration: ConfigurationDocument
    capabilities: ConnectorCapabilitySet
    schema_discovery: ConfigurationDocument | None
    secret_references: tuple[ConnectorReferenceSnapshot, ...]

    def __post_init__(self) -> None:
        _require_exact(self.connector_id, ConnectorId, "connector snapshot identity")
        _validate_text(
            self.kind,
            _KIND_PATTERN,
            "connector snapshot kind",
            MAX_CONNECTOR_KIND_LENGTH,
        )
        revision = cast(object, self.revision)
        if type(revision) is not int:
            raise TypeError("connector snapshot revision must be an integer")
        if not 1 <= revision <= 2_147_483_647:
            raise InvalidConnectorSnapshotError("connector snapshot revision is outside the limit")
        _require_exact(
            self.configuration,
            ConfigurationDocument,
            "connector snapshot configuration",
        )
        _require_exact(
            self.capabilities,
            ConnectorCapabilitySet,
            "connector snapshot capabilities",
        )
        schema = cast(object, self.schema_discovery)
        if schema is not None and type(schema) is not ConfigurationDocument:
            raise TypeError(
                "connector snapshot schema discovery must use ConfigurationDocument or None"
            )
        references = _require_exact_tuple(
            self.secret_references,
            ConnectorReferenceSnapshot,
            "connector snapshot secret references",
        )
        if len(references) > MAX_CONNECTOR_SNAPSHOT_REFERENCES:
            raise InvalidConnectorSnapshotError("connector snapshot exceeds the reference limit")
        names = tuple(reference.reference_name for reference in references)
        if len(set(names)) != len(names):
            raise InvalidConnectorSnapshotError("connector snapshot reference names must be unique")
        object.__setattr__(self, "secret_references", tuple(sorted(references)))

    def to_mapping(self) -> dict[str, object]:
        """Return exact non-secret metadata for a published specification."""
        return {
            "capabilities": self.capabilities.to_mapping(),
            "configuration": self.configuration.to_mapping(),
            "connector_id": str(self.connector_id),
            "kind": self.kind,
            "revision": self.revision,
            "schema_discovery": (
                None if self.schema_discovery is None else self.schema_discovery.to_mapping()
            ),
            "secret_references": [reference.to_mapping() for reference in self.secret_references],
        }

    def __repr__(self) -> str:
        return (
            "ConnectorBindingSnapshot("
            f"connector_id={self.connector_id!r}, kind={self.kind!r}, "
            f"revision={self.revision!r}, configuration=<redacted>, "
            "capabilities=<redacted>, schema_discovery=<redacted>, "
            f"secret_references={len(self.secret_references)})"
        )


def _validate_text(
    value: object,
    pattern: re.Pattern[str],
    subject: str,
    maximum_length: int = MAX_CONNECTOR_REFERENCE_LENGTH,
) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{subject} must be text")
    if not 1 <= len(value) <= maximum_length:
        raise InvalidConnectorSnapshotError(f"{subject} is outside the size limit")
    if not value.isascii() or pattern.fullmatch(value) is None:
        raise InvalidConnectorSnapshotError(f"{subject} is not canonical")


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
