"""Built-in closed node-registry behavior tests."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Any, cast

import pytest

from paritygrid.application.planner import (
    BUILTIN_NODE_DEFINITIONS,
    BUILTIN_NODE_REGISTRY,
    InvalidNodeConfigurationError,
    PipelineDocument,
    PlannerRunnerKind,
    UnknownNodeKindError,
    UnsupportedNodeConfigurationVersionError,
    registered_node_definition,
    validate_registered_nodes,
)
from paritygrid.domain.pipeline import NodeKind


def _node(
    identity: str,
    kind: str,
    *,
    configuration_version: int = 1,
    configuration: dict[str, object] | None = None,
    connector_id: str | None = None,
) -> dict[str, object]:
    return {
        "configuration": configuration or {},
        "configuration_version": configuration_version,
        "connector_id": connector_id,
        "id": identity,
        "kind": kind,
    }


def _document(*nodes: dict[str, object]) -> PipelineDocument:
    return PipelineDocument.from_mapping(
        {
            "canonical_format_version": 1,
            "edges": [],
            "layout": [],
            "nodes": list(nodes),
            "resource_policy": {},
            "schema_version": 1,
        }
    )


def test_builtin_registry_snapshot_is_exact_and_closed() -> None:
    assert tuple(
        (
            str(item.kind),
            item.role.value,
            item.connector_requirement.value,
            tuple(runner.value for runner in item.supported_runners),
            item.retry_behavior.value,
            item.requires_idempotency,
            item.configuration_schema.version,
            tuple(field.name for field in item.configuration_schema.fields),
        )
        for item in BUILTIN_NODE_DEFINITIONS
    ) == (
        (
            "export.parquet",
            "export",
            "none",
            ("asyncio", "sequential", "threaded"),
            "never",
            False,
            1,
            ("compression",),
        ),
        (
            "reconcile.target",
            "reconciliation",
            "target",
            ("asyncio", "sequential", "threaded"),
            "connector",
            False,
            1,
            (),
        ),
        (
            "repair.apply",
            "repair_effect",
            "target",
            ("asyncio", "sequential", "threaded"),
            "connector",
            True,
            1,
            (),
        ),
        (
            "repair.approval",
            "approval",
            "none",
            ("asyncio", "sequential", "threaded"),
            "never",
            False,
            1,
            (),
        ),
        (
            "repair.generate",
            "repair_plan",
            "none",
            ("asyncio", "process", "sequential", "threaded"),
            "never",
            False,
            1,
            (),
        ),
        (
            "source.csv",
            "source",
            "source",
            ("asyncio", "sequential", "threaded"),
            "connector",
            False,
            1,
            ("encoding", "header"),
        ),
        (
            "source.http.async",
            "source",
            "source",
            ("asyncio", "sequential", "threaded"),
            "connector",
            False,
            1,
            ("page_size",),
        ),
        (
            "source.http.blocking",
            "source",
            "source",
            ("asyncio", "sequential", "threaded"),
            "connector",
            False,
            1,
            ("page_size",),
        ),
        (
            "source.jsonl",
            "source",
            "source",
            ("asyncio", "sequential", "threaded"),
            "connector",
            False,
            1,
            ("encoding",),
        ),
        (
            "transform.normalize",
            "transform",
            "none",
            ("asyncio", "process", "sequential", "threaded"),
            "never",
            False,
            1,
            (),
        ),
        (
            "transform.partition",
            "transform",
            "none",
            ("asyncio", "process", "sequential", "threaded"),
            "never",
            False,
            1,
            ("partition_count",),
        ),
        (
            "transform.validate",
            "transform",
            "none",
            ("asyncio", "process", "sequential", "threaded"),
            "never",
            False,
            1,
            (),
        ),
        (
            "verify.target",
            "verification",
            "target",
            ("asyncio", "sequential", "threaded"),
            "connector",
            False,
            1,
            (),
        ),
    )
    assert len(BUILTIN_NODE_REGISTRY.definitions) == 13
    with pytest.raises(FrozenInstanceError):
        BUILTIN_NODE_REGISTRY.version = 2  # type: ignore[misc]


def test_registry_resolves_every_builtin_and_rejects_unknown_or_version_drift() -> None:
    for definition in BUILTIN_NODE_DEFINITIONS:
        assert registered_node_definition(definition.kind, 1) is definition
    with pytest.raises(UnknownNodeKindError, match="not registered"):
        registered_node_definition(NodeKind("shell.execute"), 1)
    with pytest.raises(UnsupportedNodeConfigurationVersionError, match="unsupported"):
        registered_node_definition(NodeKind("source.csv"), 2)


def test_document_validation_accepts_exact_builtin_configuration_schemas() -> None:
    document = _document(
        _node(
            "nod_source-01",
            "source.csv",
            configuration={"encoding": "utf-8", "header": True},
            connector_id="con_source-01",
        ),
        _node("nod_partition-01", "transform.partition", configuration={"partition_count": 8}),
        _node("nod_target-01", "verify.target", connector_id="con_target-01"),
    )
    validate_registered_nodes(document)
    assert registered_node_definition(NodeKind("transform.normalize"), 1).supported_runners == (
        PlannerRunnerKind.ASYNCIO,
        PlannerRunnerKind.PROCESS,
        PlannerRunnerKind.SEQUENTIAL,
        PlannerRunnerKind.THREADED,
    )


@pytest.mark.parametrize(
    "node",
    [
        _node("nod_unknown-01", "shell.execute"),
        _node("nod_source-01", "source.csv", configuration_version=2),
        _node("nod_source-01", "source.csv", configuration={"command": "value"}),
        _node("nod_source-01", "source.csv", configuration={"header": 1}),
    ],
)
def test_document_validation_rejects_unknown_kinds_versions_and_configuration(
    node: dict[str, object],
) -> None:
    with pytest.raises(
        (
            UnknownNodeKindError,
            UnsupportedNodeConfigurationVersionError,
            InvalidNodeConfigurationError,
        )
    ):
        validate_registered_nodes(_document(node))


def test_document_validation_requires_exact_public_contract() -> None:
    with pytest.raises(TypeError, match="PipelineDocument"):
        validate_registered_nodes(cast(Any, {}))
