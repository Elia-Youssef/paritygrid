"""Frozen contract tests for immutable published pipeline envelopes."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import FrozenInstanceError
from typing import Any, cast

import pytest

from paritygrid.application.planner import (
    PUBLISHED_PIPELINE_SPECIFICATION_VERSION,
    ConnectorBindingSnapshot,
    ConnectorCapability,
    ConnectorCapabilitySet,
    ConnectorReferenceSnapshot,
    InvalidPublishedSpecificationError,
    PipelineDocument,
    PipelinePublicationError,
    PublishedPipelineSpecification,
)
from paritygrid.application.ports import ConfigurationDocument
from paritygrid.domain.models import ConnectorId


def _pipeline(
    *, connector_id: str | None = "con_source-001", total: bool = True
) -> PipelineDocument:
    policy: dict[str, object]
    if total:
        policy = {
            "max_concurrency": 4,
            "max_in_flight": 16,
            "memory_limit_bytes": 536_870_912,
            "operation_timeout_seconds": 60,
            "queue_capacity": 256,
        }
    else:
        policy = {}
    value: dict[str, object] = {
        "canonical_format_version": 1,
        "edges": [],
        "layout": [{"node_id": "nod_source-001", "x": -1, "y": 2}],
        "nodes": [
            {
                "configuration": {},
                "configuration_version": 1,
                "connector_id": connector_id,
                "id": "nod_source-001",
                "kind": "source.csv",
            }
        ],
        "resource_policy": policy,
        "schema_version": 1,
    }
    return PipelineDocument.from_mapping(value)


def _binding(connector_id: str = "con_source-001") -> ConnectorBindingSnapshot:
    return ConnectorBindingSnapshot(
        connector_id=ConnectorId(connector_id),
        kind="csv.local",
        revision=3,
        configuration=ConfigurationDocument.from_mapping(
            {"path": "inventory.csv", "token": {"secret_ref": "source_token"}}
        ),
        capabilities=ConnectorCapabilitySet((ConnectorCapability.READ,)),
        schema_discovery=ConfigurationDocument.from_mapping({"columns": ["sku"]}),
        secret_references=(ConnectorReferenceSnapshot("source_token", "PARITYGRID_SOURCE_TOKEN"),),
    )


def _mapping() -> dict[str, object]:
    return PublishedPipelineSpecification(_pipeline(), (_binding(),)).to_mapping()


def test_publication_contract_version_and_error_family_are_frozen() -> None:
    assert PUBLISHED_PIPELINE_SPECIFICATION_VERSION == 1
    assert issubclass(InvalidPublishedSpecificationError, PipelinePublicationError)


def test_envelope_round_trips_exact_non_secret_snapshot() -> None:
    envelope = PublishedPipelineSpecification(_pipeline(), (_binding(),))
    durable = envelope.to_configuration_document()
    assert PublishedPipelineSpecification.from_configuration_document(durable) == envelope
    assert envelope.to_mapping() == durable.to_mapping()
    assert repr(envelope) == (
        "PublishedPipelineSpecification(version=1, nodes=1, connector_bindings=1)"
    )
    rendered = repr(envelope)
    assert "inventory.csv" not in rendered
    assert "PARITYGRID_SOURCE_TOKEN" not in rendered


def test_envelope_sorts_bindings_and_allows_no_connector_references() -> None:
    value = _pipeline(connector_id=None).to_mapping()
    value["nodes"] = [
        {
            "configuration": {},
            "configuration_version": 1,
            "connector_id": None,
            "id": "nod_export-001",
            "kind": "export.parquet",
        }
    ]
    value["layout"] = []
    pipeline = PipelineDocument.from_mapping(value)
    assert PublishedPipelineSpecification(pipeline, ()).connector_bindings == ()


def test_bindings_must_exactly_match_references() -> None:
    with pytest.raises(InvalidPublishedSpecificationError, match="exactly match"):
        PublishedPipelineSpecification(_pipeline(), ())
    with pytest.raises(InvalidPublishedSpecificationError, match="exactly match"):
        PublishedPipelineSpecification(_pipeline(connector_id=None), (_binding(),))


def test_duplicate_bindings_fail_closed_before_reference_comparison() -> None:
    binding = _binding()
    with pytest.raises(InvalidPublishedSpecificationError, match="unique"):
        PublishedPipelineSpecification(_pipeline(), (binding, binding))


def test_published_policy_must_be_total() -> None:
    with pytest.raises(InvalidPublishedSpecificationError, match="every field"):
        PublishedPipelineSpecification(_pipeline(total=False), (_binding(),))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("pipeline", {}, "PipelineDocument"),
        ("connector_bindings", [], "tuple"),
        ("connector_bindings", (object(),), "invalid value"),
        ("published_specification_version", True, "integer"),
    ],
)
def test_envelope_requires_exact_public_types(field: str, value: object, message: str) -> None:
    values: dict[str, object] = {
        "pipeline": _pipeline(),
        "connector_bindings": (_binding(),),
        "published_specification_version": 1,
    }
    values[field] = value
    with pytest.raises(TypeError, match=message):
        PublishedPipelineSpecification(**cast(Any, values))


def test_envelope_rejects_unsupported_version() -> None:
    with pytest.raises(InvalidPublishedSpecificationError, match="unsupported"):
        PublishedPipelineSpecification(_pipeline(), (_binding(),), 2)


@pytest.mark.parametrize("change", ["missing", "unknown"])
def test_root_fields_are_closed(change: str) -> None:
    value = _mapping()
    if change == "missing":
        value.pop("pipeline")
    else:
        value["extension"] = True
    with pytest.raises(InvalidPublishedSpecificationError, match=change):
        PublishedPipelineSpecification.from_configuration_document(
            ConfigurationDocument.from_mapping(value)
        )


@pytest.mark.parametrize("change", ["missing", "unknown"])
def test_binding_fields_are_closed(change: str) -> None:
    value = _mapping()
    binding = cast(list[dict[str, object]], value["connector_bindings"])[0]
    if change == "missing":
        binding.pop("kind")
    else:
        binding["extension"] = True
    with pytest.raises(InvalidPublishedSpecificationError, match=change):
        PublishedPipelineSpecification.from_configuration_document(
            ConfigurationDocument.from_mapping(value)
        )


@pytest.mark.parametrize("change", ["missing", "unknown"])
def test_reference_fields_are_closed(change: str) -> None:
    value = _mapping()
    binding = cast(list[dict[str, object]], value["connector_bindings"])[0]
    reference = cast(list[dict[str, object]], binding["secret_references"])[0]
    if change == "missing":
        reference.pop("reference_name")
    else:
        reference["extension"] = True
    with pytest.raises(InvalidPublishedSpecificationError, match=change):
        PublishedPipelineSpecification.from_configuration_document(
            ConfigurationDocument.from_mapping(value)
        )


def _set_pipeline_array(value: dict[str, object]) -> None:
    value["pipeline"] = []


def _set_bindings_object(value: dict[str, object]) -> None:
    value["connector_bindings"] = {}


def _set_discovery_array(value: dict[str, object]) -> None:
    binding = cast(list[dict[str, object]], value["connector_bindings"])[0]
    binding["schema_discovery"] = []


def _set_references_object(value: dict[str, object]) -> None:
    binding = cast(list[dict[str, object]], value["connector_bindings"])[0]
    binding["secret_references"] = {}


def _set_binding_identity_integer(value: dict[str, object]) -> None:
    binding = cast(list[dict[str, object]], value["connector_bindings"])[0]
    binding["connector_id"] = 1


def _set_publication_version_boolean(value: dict[str, object]) -> None:
    value["published_specification_version"] = True


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (_set_pipeline_array, "published pipeline must be an object"),
        (_set_bindings_object, "bindings must be an array"),
        (_set_discovery_array, "schema discovery must be an object or null"),
        (_set_references_object, "secret references must be an array"),
        (_set_binding_identity_integer, "identity must be text"),
        (_set_publication_version_boolean, "version must be an integer"),
    ],
)
def test_nested_envelope_shapes_fail_closed(
    mutate: Callable[[dict[str, object]], None], message: str
) -> None:
    value = _mapping()
    mutate(value)
    with pytest.raises(TypeError, match=message):
        PublishedPipelineSpecification.from_configuration_document(
            ConfigurationDocument.from_mapping(value)
        )


def test_parser_requires_exact_configuration_document() -> None:
    with pytest.raises(TypeError, match="ConfigurationDocument"):
        PublishedPipelineSpecification.from_configuration_document(cast(Any, {}))


def test_envelope_is_immutable() -> None:
    envelope = PublishedPipelineSpecification(_pipeline(), (_binding(),))
    with pytest.raises(FrozenInstanceError):
        envelope.connector_bindings = ()  # type: ignore[misc]
