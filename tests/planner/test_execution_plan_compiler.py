"""Golden and corruption tests for deterministic execution-plan compilation."""

from __future__ import annotations

from typing import Any, cast

import pytest

from paritygrid.application.planner import (
    ConnectorBindingSnapshot,
    ConnectorCapability,
    ConnectorCapabilitySet,
    ConnectorValidationError,
    InvalidConnectorSnapshotError,
    MissingConnectorCapabilityError,
    MissingConnectorError,
    PipelineDocument,
    PublishedPipelineSpecification,
    compile_execution_plan,
)
from paritygrid.application.ports import ConfigurationDocument
from paritygrid.domain.models import ConnectorId

_CONNECTOR_ID = ConnectorId("con_source-001")


def _pipeline(*, layout_offset: int = 0) -> PipelineDocument:
    value: dict[str, object] = {
        "canonical_format_version": 1,
        "edges": [
            {
                "source_node_id": "nod_source-001",
                "source_port": "records",
                "target_node_id": "nod_normalize-001",
                "target_port": "records",
            },
            {
                "source_node_id": "nod_normalize-001",
                "source_port": "records",
                "target_node_id": "nod_export-001",
                "target_port": "records",
            },
        ],
        "layout": [
            {"node_id": "nod_source-001", "x": layout_offset, "y": 0},
            {"node_id": "nod_normalize-001", "x": layout_offset + 10, "y": 20},
            {"node_id": "nod_export-001", "x": layout_offset + 20, "y": 40},
        ],
        "nodes": [
            {
                "configuration": {"compression": "zstd"},
                "configuration_version": 1,
                "connector_id": None,
                "id": "nod_export-001",
                "kind": "export.parquet",
            },
            {
                "configuration": {},
                "configuration_version": 1,
                "connector_id": None,
                "id": "nod_normalize-001",
                "kind": "transform.normalize",
            },
            {
                "configuration": {"encoding": "utf-8", "header": True},
                "configuration_version": 1,
                "connector_id": str(_CONNECTOR_ID),
                "id": "nod_source-001",
                "kind": "source.csv",
            },
        ],
        "resource_policy": {
            "max_concurrency": 2,
            "max_in_flight": 8,
            "memory_limit_bytes": 268_435_456,
            "operation_timeout_seconds": 30,
            "queue_capacity": 32,
        },
        "schema_version": 1,
    }
    return PipelineDocument.from_mapping(value)


def _binding(
    *,
    capabilities: tuple[ConnectorCapability, ...] = (ConnectorCapability.READ,),
    schema_discovery: ConfigurationDocument | None = None,
) -> ConnectorBindingSnapshot:
    return ConnectorBindingSnapshot(
        connector_id=_CONNECTOR_ID,
        kind="csv-local",
        revision=7,
        configuration=ConfigurationDocument.from_mapping({"path": "inventory.csv"}),
        capabilities=ConnectorCapabilitySet(capabilities),
        schema_discovery=schema_discovery,
        secret_references=(),
    )


def _specification(
    *,
    pipeline: PipelineDocument | None = None,
    binding: ConnectorBindingSnapshot | None = None,
) -> PublishedPipelineSpecification:
    return PublishedPipelineSpecification(
        _pipeline() if pipeline is None else pipeline,
        (_binding() if binding is None else binding,),
    )


def test_compiler_matches_golden_logical_plan() -> None:
    plan = compile_execution_plan(_specification())
    assert [str(node.node_id) for node in plan.nodes] == [
        "nod_source-001",
        "nod_normalize-001",
        "nod_export-001",
    ]
    assert plan.to_mapping() == {
        "connector_bindings": [
            {
                "capabilities": {
                    "async_io": False,
                    "blocking_io": False,
                    "idempotency": False,
                    "read": True,
                    "schema_discovery": False,
                    "write": False,
                },
                "configuration": {"path": "inventory.csv"},
                "connector_id": "con_source-001",
                "kind": "csv-local",
                "revision": 7,
                "schema_discovery": None,
                "secret_references": [],
            }
        ],
        "edges": [
            {
                "source": {"node_id": "nod_normalize-001", "port": "records"},
                "target": {"node_id": "nod_export-001", "port": "records"},
            },
            {
                "source": {"node_id": "nod_source-001", "port": "records"},
                "target": {"node_id": "nod_normalize-001", "port": "records"},
            },
        ],
        "nodes": [
            {
                "configuration": {"encoding": "utf-8", "header": True},
                "configuration_version": 1,
                "connector_id": "con_source-001",
                "connector_requirement": "source",
                "id": "nod_source-001",
                "kind": "source.csv",
                "requires_idempotency": False,
                "retry_behavior": "connector",
                "role": "source",
                "supported_runners": ["asyncio", "sequential", "threaded"],
            },
            {
                "configuration": {},
                "configuration_version": 1,
                "connector_id": None,
                "connector_requirement": "none",
                "id": "nod_normalize-001",
                "kind": "transform.normalize",
                "requires_idempotency": False,
                "retry_behavior": "never",
                "role": "transform",
                "supported_runners": ["asyncio", "process", "sequential", "threaded"],
            },
            {
                "configuration": {"compression": "zstd"},
                "configuration_version": 1,
                "connector_id": None,
                "connector_requirement": "none",
                "id": "nod_export-001",
                "kind": "export.parquet",
                "requires_idempotency": False,
                "retry_behavior": "never",
                "role": "export",
                "supported_runners": ["asyncio", "sequential", "threaded"],
            },
        ],
        "resource_policy": {
            "max_concurrency": 2,
            "max_in_flight": 8,
            "memory_limit_bytes": 268_435_456,
            "operation_timeout_seconds": 30,
            "queue_capacity": 32,
        },
        "version": 1,
    }


def test_layout_changes_do_not_change_logical_plan() -> None:
    first = compile_execution_plan(_specification(pipeline=_pipeline(layout_offset=0)))
    second = compile_execution_plan(_specification(pipeline=_pipeline(layout_offset=99)))
    assert first == second
    assert first.to_mapping() == second.to_mapping()


def test_compiler_rejects_missing_required_connector() -> None:
    mapping = _pipeline().to_mapping()
    nodes = cast(list[dict[str, object]], mapping["nodes"])
    next(node for node in nodes if node["id"] == "nod_source-001")["connector_id"] = None
    pipeline = PipelineDocument.from_mapping(mapping)
    specification = PublishedPipelineSpecification(pipeline, ())
    with pytest.raises(MissingConnectorError):
        compile_execution_plan(specification)


def test_compiler_rejects_connector_on_node_that_forbids_it() -> None:
    mapping = _pipeline().to_mapping()
    nodes = cast(list[dict[str, object]], mapping["nodes"])
    next(node for node in nodes if node["id"] == "nod_export-001")["connector_id"] = str(
        _CONNECTOR_ID
    )
    pipeline = PipelineDocument.from_mapping(mapping)
    specification = PublishedPipelineSpecification(pipeline, (_binding(),))
    with pytest.raises(ConnectorValidationError, match="does not permit"):
        compile_execution_plan(specification)


def test_compiler_rejects_missing_capability_in_embedded_snapshot() -> None:
    with pytest.raises(MissingConnectorCapabilityError):
        compile_execution_plan(_specification(binding=_binding(capabilities=())))


def test_compiler_rejects_schema_metadata_without_discovery_capability() -> None:
    binding = _binding(schema_discovery=ConfigurationDocument.from_mapping({"fields": []}))
    with pytest.raises(InvalidConnectorSnapshotError, match="schema discovery"):
        compile_execution_plan(_specification(binding=binding))


def test_compiler_accepts_schema_metadata_with_discovery_capability() -> None:
    binding = _binding(
        capabilities=(ConnectorCapability.READ, ConnectorCapability.SCHEMA_DISCOVERY),
        schema_discovery=ConfigurationDocument.from_mapping({"fields": []}),
    )
    assert compile_execution_plan(_specification(binding=binding)).connector_bindings == (binding,)


def test_compiler_requires_exact_published_specification() -> None:
    with pytest.raises(TypeError, match="PublishedPipelineSpecification"):
        compile_execution_plan(cast(Any, {}))
