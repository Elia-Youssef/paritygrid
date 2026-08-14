"""Golden and independence tests for logical execution-plan fingerprints."""

from __future__ import annotations

from dataclasses import replace

import pytest

from paritygrid.application.planner import (
    PARTITION_NODE_KIND,
    ConnectorBindingSnapshot,
    ConnectorCapability,
    ConnectorCapabilitySet,
    ExecutionPlan,
    PartitionStrategy,
    PartitionStrategyKind,
    PipelineDocument,
    PublishedPipelineSpecification,
    ResourcePolicy,
    compile_execution_plan,
    fingerprint_execution_plan,
)
from paritygrid.application.ports import ConfigurationDocument
from paritygrid.domain.models import ConnectorId

_CONNECTOR_ID = ConnectorId("con_source-001")


def _pipeline(*, layout_offset: int = 0, reverse_json_order: bool = False) -> PipelineDocument:
    source_configuration = (
        {"header": True, "encoding": "utf-8"}
        if reverse_json_order
        else {"encoding": "utf-8", "header": True}
    )
    resource_policy = (
        {
            "queue_capacity": 32,
            "operation_timeout_seconds": 30,
            "memory_limit_bytes": 268_435_456,
            "max_in_flight": 8,
            "max_concurrency": 2,
        }
        if reverse_json_order
        else {
            "max_concurrency": 2,
            "max_in_flight": 8,
            "memory_limit_bytes": 268_435_456,
            "operation_timeout_seconds": 30,
            "queue_capacity": 32,
        }
    )
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
                "configuration": source_configuration,
                "configuration_version": 1,
                "connector_id": str(_CONNECTOR_ID),
                "id": "nod_source-001",
                "kind": "source.csv",
            },
        ],
        "resource_policy": resource_policy,
        "schema_version": 1,
    }
    return PipelineDocument.from_mapping(value)


def _binding(*, reverse_json_order: bool = False) -> ConnectorBindingSnapshot:
    configuration = (
        {"delimiter": ",", "path": "inventory.csv"}
        if reverse_json_order
        else {"path": "inventory.csv", "delimiter": ","}
    )
    return ConnectorBindingSnapshot(
        connector_id=_CONNECTOR_ID,
        kind="csv-local",
        revision=7,
        configuration=ConfigurationDocument.from_mapping(configuration),
        capabilities=ConnectorCapabilitySet((ConnectorCapability.READ,)),
        schema_discovery=None,
        secret_references=(),
    )


def _plan(*, layout_offset: int = 0, reverse_json_order: bool = False) -> ExecutionPlan:
    specification = PublishedPipelineSpecification(
        _pipeline(layout_offset=layout_offset, reverse_json_order=reverse_json_order),
        (_binding(reverse_json_order=reverse_json_order),),
    )
    return compile_execution_plan(specification)


def test_plan_fingerprint_matches_frozen_golden_digest() -> None:
    assert str(fingerprint_execution_plan(_plan())) == (
        "12e63364db2bf102fe7ae79cb42eaa4843be884b5d03cc3ba7e0dda7f221129f"
    )


def test_plan_fingerprint_ignores_layout_and_json_object_order() -> None:
    first = fingerprint_execution_plan(_plan(layout_offset=0, reverse_json_order=False))
    second = fingerprint_execution_plan(_plan(layout_offset=500, reverse_json_order=True))
    assert first == second


def test_plan_fingerprint_changes_for_logical_semantics() -> None:
    plan = _plan()
    original = fingerprint_execution_plan(plan)
    changed_policy = replace(
        plan,
        resource_policy=ResourcePolicy(
            max_concurrency=3,
            max_in_flight=8,
            memory_limit_bytes=268_435_456,
            operation_timeout_seconds=30,
            queue_capacity=32,
        ),
    )
    binding = plan.connector_bindings[0]
    changed_binding = replace(plan, connector_bindings=(replace(binding, revision=8),))
    node = plan.nodes[1]
    changed_partition = replace(
        plan,
        nodes=(
            plan.nodes[0],
            replace(
                node,
                kind=PARTITION_NODE_KIND,
                partition_strategy=PartitionStrategy(PartitionStrategyKind.FIXED, 2),
            ),
            plan.nodes[2],
        ),
    )
    assert fingerprint_execution_plan(changed_policy) != original
    assert fingerprint_execution_plan(changed_binding) != original
    assert fingerprint_execution_plan(changed_partition) != original


def test_plan_fingerprint_requires_exact_execution_plan() -> None:
    with pytest.raises(TypeError, match="ExecutionPlan"):
        fingerprint_execution_plan({})  # type: ignore[arg-type]
