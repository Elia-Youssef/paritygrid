"""Dependency-neutral immutable published pipeline specification contract."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from paritygrid.application.planner.connectors import (
    ConnectorBindingSnapshot,
    ConnectorReferenceSnapshot,
    connector_capabilities_from_document,
)
from paritygrid.application.planner.documents import PipelineDocument
from paritygrid.application.planner.resources import validate_resource_policy
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.domain.models import ConnectorId

PUBLISHED_PIPELINE_SPECIFICATION_VERSION = 1

_ROOT_FIELDS = frozenset({"connector_bindings", "pipeline", "published_specification_version"})
_BINDING_FIELDS = frozenset(
    {
        "capabilities",
        "configuration",
        "connector_id",
        "kind",
        "revision",
        "schema_discovery",
        "secret_references",
    }
)
_REFERENCE_FIELDS = frozenset({"environment_variable_name", "reference_name"})


class PipelinePublicationError(ValueError):
    """Base failure for a published pipeline specification or publication."""


class InvalidPublishedSpecificationError(PipelinePublicationError):
    """A published envelope is incomplete, extended, or internally inconsistent."""


@dataclass(frozen=True, slots=True, repr=False)
class PublishedPipelineSpecification:
    """A total pipeline document and the immutable connector definitions it uses."""

    pipeline: PipelineDocument
    connector_bindings: tuple[ConnectorBindingSnapshot, ...]
    published_specification_version: int = PUBLISHED_PIPELINE_SPECIFICATION_VERSION

    def __post_init__(self) -> None:
        if type(self.pipeline) is not PipelineDocument:
            raise TypeError("published pipeline must use PipelineDocument")
        if type(self.connector_bindings) is not tuple:
            raise TypeError("published connector bindings must be a tuple")
        bindings = cast(tuple[object, ...], self.connector_bindings)
        if any(type(binding) is not ConnectorBindingSnapshot for binding in bindings):
            raise TypeError("published connector bindings contain an invalid value")
        version = cast(object, self.published_specification_version)
        if type(version) is not int:
            raise TypeError("published specification version must be an integer")
        if version != PUBLISHED_PIPELINE_SPECIFICATION_VERSION:
            raise InvalidPublishedSpecificationError(
                "published specification version is unsupported"
            )
        typed_bindings = cast(tuple[ConnectorBindingSnapshot, ...], bindings)
        binding_ids = tuple(binding.connector_id for binding in typed_bindings)
        if len(set(binding_ids)) != len(binding_ids):
            raise InvalidPublishedSpecificationError("published connector bindings must be unique")
        referenced_ids = frozenset(
            node.connector_id for node in self.pipeline.nodes if node.connector_id is not None
        )
        if frozenset(binding_ids) != referenced_ids:
            raise InvalidPublishedSpecificationError(
                "published connector bindings must exactly match pipeline references"
            )
        total_policy = validate_resource_policy(self.pipeline).to_mapping()
        if self.pipeline.resource_policy.to_mapping() != total_policy:
            raise InvalidPublishedSpecificationError(
                "published resource policy must materialize every field"
            )
        object.__setattr__(
            self,
            "connector_bindings",
            tuple(sorted(typed_bindings, key=lambda binding: str(binding.connector_id))),
        )

    @classmethod
    def from_configuration_document(
        cls,
        value: ConfigurationDocument,
    ) -> PublishedPipelineSpecification:
        """Parse one exact durable published envelope without repository state."""
        if type(value) is not ConfigurationDocument:
            raise TypeError("published specification must use ConfigurationDocument")
        root = _require_object(value.to_mapping(), "published specification")
        _require_fields(root, _ROOT_FIELDS, "published specification")
        pipeline = PipelineDocument.from_mapping(
            _require_object(root["pipeline"], "published pipeline")
        )
        bindings = tuple(
            _parse_binding(item, index)
            for index, item in enumerate(
                _require_array(root["connector_bindings"], "published connector bindings")
            )
        )
        return cls(
            pipeline=pipeline,
            connector_bindings=bindings,
            published_specification_version=_require_integer(
                root["published_specification_version"],
                "published specification version",
            ),
        )

    def to_mapping(self) -> dict[str, object]:
        """Return the exact durable envelope with non-secret connector snapshots."""
        return {
            "connector_bindings": [binding.to_mapping() for binding in self.connector_bindings],
            "pipeline": self.pipeline.to_mapping(),
            "published_specification_version": self.published_specification_version,
        }

    def to_configuration_document(self) -> ConfigurationDocument:
        """Return the immutable repository-ready envelope."""
        return ConfigurationDocument.from_mapping(self.to_mapping())

    def __repr__(self) -> str:
        return (
            "PublishedPipelineSpecification("
            f"version={self.published_specification_version!r}, "
            f"nodes={len(self.pipeline.nodes)}, "
            f"connector_bindings={len(self.connector_bindings)})"
        )


def _parse_binding(value: object, index: int) -> ConnectorBindingSnapshot:
    subject = f"published connector binding {index}"
    item = _require_object(value, subject)
    _require_fields(item, _BINDING_FIELDS, subject)
    discovery = item["schema_discovery"]
    if discovery is not None and not isinstance(discovery, Mapping):
        raise TypeError(f"{subject} schema discovery must be an object or null")
    return ConnectorBindingSnapshot(
        connector_id=ConnectorId(_require_text(item["connector_id"], f"{subject} identity")),
        kind=_require_text(item["kind"], f"{subject} kind"),
        revision=_require_integer(item["revision"], f"{subject} revision"),
        configuration=ConfigurationDocument.from_mapping(
            _require_object(item["configuration"], f"{subject} configuration")
        ),
        capabilities=connector_capabilities_from_document(
            ConfigurationDocument.from_mapping(
                _require_object(item["capabilities"], f"{subject} capabilities")
            )
        ),
        schema_discovery=(
            None
            if discovery is None
            else ConfigurationDocument.from_mapping(
                _require_object(cast(object, discovery), f"{subject} schema discovery")
            )
        ),
        secret_references=tuple(
            _parse_reference(reference, subject, reference_index)
            for reference_index, reference in enumerate(
                _require_array(item["secret_references"], f"{subject} secret references")
            )
        ),
    )


def _parse_reference(
    value: object,
    binding_subject: str,
    index: int,
) -> ConnectorReferenceSnapshot:
    subject = f"{binding_subject} secret reference {index}"
    item = _require_object(value, subject)
    _require_fields(item, _REFERENCE_FIELDS, subject)
    return ConnectorReferenceSnapshot(
        reference_name=_require_text(item["reference_name"], f"{subject} name"),
        environment_variable_name=_require_text(
            item["environment_variable_name"],
            f"{subject} environment variable",
        ),
    )


def _require_object(value: object, subject: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{subject} must be an object")
    return dict(cast(Mapping[str, object], value))


def _require_array(value: object, subject: str) -> tuple[object, ...]:
    if not isinstance(value, list | tuple):
        raise TypeError(f"{subject} must be an array")
    return tuple(cast(list[object] | tuple[object, ...], value))


def _require_fields(value: Mapping[str, object], fields: frozenset[str], subject: str) -> None:
    actual = frozenset(value)
    if fields - actual:
        raise InvalidPublishedSpecificationError(f"{subject} is missing required fields")
    if actual - fields:
        raise InvalidPublishedSpecificationError(f"{subject} contains unknown fields")


def _require_text(value: object, subject: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{subject} must be text")
    return value


def _require_integer(value: object, subject: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    return value
