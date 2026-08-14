"""Compatibility-matrix and adversarial tests for built-in typed ports."""

from __future__ import annotations

from typing import Any, cast

import pytest

from paritygrid.application.planner import (
    BUILTIN_NODE_DEFINITIONS,
    InputPortDefinition,
    InvalidPortConnectionError,
    OutputPortDefinition,
    PipelineDocument,
    PortValueType,
    ports_are_compatible,
    validate_typed_ports,
)
from paritygrid.domain.pipeline import PortName


def _node(identity: str, kind: str, connector_id: str | None = None) -> dict[str, object]:
    return {
        "configuration": {},
        "configuration_version": 1,
        "connector_id": connector_id,
        "id": identity,
        "kind": kind,
    }


def _edge(
    source: str,
    source_port: str,
    target: str,
    target_port: str,
) -> dict[str, str]:
    return {
        "source_node_id": source,
        "source_port": source_port,
        "target_node_id": target,
        "target_port": target_port,
    }


def _document(
    nodes: list[dict[str, object]],
    edges: list[dict[str, str]],
) -> PipelineDocument:
    return PipelineDocument.from_mapping(
        {
            "canonical_format_version": 1,
            "edges": edges,
            "layout": [],
            "nodes": nodes,
            "resource_policy": {},
            "schema_version": 1,
        }
    )


def test_builtin_port_schema_snapshot_is_exact() -> None:
    assert tuple(
        (
            str(definition.kind),
            tuple(
                (
                    str(port.name),
                    tuple(value_type.value for value_type in port.accepted_types),
                    port.required,
                    port.maximum_connections,
                )
                for port in definition.port_schema.inputs
            ),
            tuple(
                (str(port.name), port.value_type.value) for port in definition.port_schema.outputs
            ),
        )
        for definition in BUILTIN_NODE_DEFINITIONS
    ) == (
        (
            "export.parquet",
            (
                (
                    "records",
                    (
                        "records.normalized",
                        "records.partitioned",
                        "records.raw",
                        "records.validated",
                    ),
                    True,
                    1,
                ),
            ),
            (),
        ),
        (
            "reconcile.target",
            (
                (
                    "records",
                    (
                        "records.normalized",
                        "records.partitioned",
                        "records.validated",
                    ),
                    True,
                    1,
                ),
            ),
            (("reconciliation", "reconciliation.result"),),
        ),
        (
            "repair.apply",
            (("approved-plan", ("repair.plan.approved",), True, 1),),
            (("repair-result", "repair.result"),),
        ),
        (
            "repair.approval",
            (("repair-plan", ("repair.plan",), True, 1),),
            (("approved-plan", "repair.plan.approved"),),
        ),
        (
            "repair.generate",
            (("reconciliation", ("reconciliation.result",), True, 1),),
            (("repair-plan", "repair.plan"),),
        ),
        ("source.csv", (), (("records", "records.raw"),)),
        ("source.http.async", (), (("records", "records.raw"),)),
        ("source.http.blocking", (), (("records", "records.raw"),)),
        ("source.jsonl", (), (("records", "records.raw"),)),
        (
            "transform.normalize",
            (("records", ("records.raw",), True, 1),),
            (("records", "records.normalized"),),
        ),
        (
            "transform.partition",
            (("records", ("records.validated",), True, 1),),
            (("records", "records.partitioned"),),
        ),
        (
            "transform.validate",
            (("records", ("records.normalized",), True, 1),),
            (("records", "records.validated"),),
        ),
        (
            "verify.target",
            (("repair-result", ("repair.result",), True, 1),),
            (("verification", "verification.result"),),
        ),
    )


def test_complete_port_type_compatibility_matrix_is_exact() -> None:
    accepted = {
        PortValueType.RAW_RECORDS,
        PortValueType.VALIDATED_RECORDS,
        PortValueType.REPAIR_PLAN,
    }
    target = InputPortDefinition(PortName("input"), tuple(accepted))
    for value_type in PortValueType:
        source = OutputPortDefinition(PortName("output"), value_type)
        assert ports_are_compatible(source, target) is (value_type in accepted)
    with pytest.raises(TypeError, match="OutputPortDefinition"):
        ports_are_compatible(cast(Any, object()), target)
    with pytest.raises(TypeError, match="InputPortDefinition"):
        ports_are_compatible(
            OutputPortDefinition(PortName("output"), PortValueType.RAW_RECORDS),
            cast(Any, object()),
        )


def test_typed_port_validation_accepts_record_and_repair_paths() -> None:
    record_nodes = [
        _node("nod_source-01", "source.csv", "con_source-01"),
        _node("nod_normalize-01", "transform.normalize"),
        _node("nod_validate-01", "transform.validate"),
        _node("nod_partition-01", "transform.partition"),
        _node("nod_export-01", "export.parquet"),
    ]
    record_edges = [
        _edge("nod_source-01", "records", "nod_normalize-01", "records"),
        _edge("nod_normalize-01", "records", "nod_validate-01", "records"),
        _edge("nod_validate-01", "records", "nod_partition-01", "records"),
        _edge("nod_partition-01", "records", "nod_export-01", "records"),
    ]
    validate_typed_ports(_document(record_nodes, record_edges))

    repair_nodes = [
        _node("nod_source-01", "source.jsonl", "con_source-01"),
        _node("nod_normalize-01", "transform.normalize"),
        _node("nod_reconcile-01", "reconcile.target", "con_target-01"),
        _node("nod_generate-01", "repair.generate"),
        _node("nod_approval-01", "repair.approval"),
        _node("nod_apply-01", "repair.apply", "con_target-01"),
        _node("nod_verify-01", "verify.target", "con_target-01"),
    ]
    repair_edges = [
        _edge("nod_source-01", "records", "nod_normalize-01", "records"),
        _edge("nod_normalize-01", "records", "nod_reconcile-01", "records"),
        _edge("nod_reconcile-01", "reconciliation", "nod_generate-01", "reconciliation"),
        _edge("nod_generate-01", "repair-plan", "nod_approval-01", "repair-plan"),
        _edge("nod_approval-01", "approved-plan", "nod_apply-01", "approved-plan"),
        _edge("nod_apply-01", "repair-result", "nod_verify-01", "repair-result"),
    ]
    validate_typed_ports(_document(repair_nodes, repair_edges))


@pytest.mark.parametrize(
    ("nodes", "edges", "message"),
    [
        (
            [_node("nod_source-01", "source.csv"), _node("nod_export-01", "export.parquet")],
            [_edge("nod_source-01", "missing", "nod_export-01", "records")],
            "source port",
        ),
        (
            [_node("nod_source-01", "source.csv"), _node("nod_export-01", "export.parquet")],
            [_edge("nod_source-01", "records", "nod_export-01", "missing")],
            "target port",
        ),
        (
            [_node("nod_source-01", "source.csv"), _node("nod_validate-01", "transform.validate")],
            [_edge("nod_source-01", "records", "nod_validate-01", "records")],
            "incompatible",
        ),
        (
            [_node("nod_normalize-01", "transform.normalize")],
            [],
            "missing a required",
        ),
        (
            [
                _node("nod_source-01", "source.csv"),
                _node("nod_source-02", "source.jsonl"),
                _node("nod_export-01", "export.parquet"),
            ],
            [
                _edge("nod_source-01", "records", "nod_export-01", "records"),
                _edge("nod_source-02", "records", "nod_export-01", "records"),
            ],
            "connection limit",
        ),
    ],
)
def test_typed_port_validation_rejects_adversarial_edges(
    nodes: list[dict[str, object]],
    edges: list[dict[str, str]],
    message: str,
) -> None:
    with pytest.raises(InvalidPortConnectionError, match=message):
        validate_typed_ports(_document(nodes, edges))


def test_typed_port_validation_requires_exact_document() -> None:
    with pytest.raises(TypeError, match="PipelineDocument"):
        validate_typed_ports(cast(Any, {}))
