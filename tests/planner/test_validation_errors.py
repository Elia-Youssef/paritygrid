"""Error-code snapshots for safe human-readable Phase 5 validation."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from paritygrid.application.planner import (
    PipelineDocument,
    PipelineValidationCode,
    PipelineValidationReport,
    RepairSafetyError,
    validate_pipeline,
)
from paritygrid.application.planner import validation_errors as validation_module
from paritygrid.application.ports import ConfigurationDocument, ConnectorRecord
from paritygrid.domain.models import ConnectorId, UtcTimestamp

_CONNECTOR_ID = ConnectorId("con_validation-001")


def _node(index: int, kind: str, connector_id: str | None = None) -> dict[str, object]:
    return {
        "configuration": {},
        "configuration_version": 1,
        "connector_id": connector_id,
        "id": f"nod_validation-{index:03d}",
        "kind": kind,
    }


def _edge(source: int, target: int) -> dict[str, str]:
    return {
        "source_node_id": f"nod_validation-{source:03d}",
        "source_port": f"out-{source}-{target}",
        "target_node_id": f"nod_validation-{target:03d}",
        "target_port": f"in-{source}-{target}",
    }


def _document(
    nodes: list[dict[str, object]],
    edges: list[dict[str, str]],
    *,
    resource_policy: Mapping[str, object] | None = None,
) -> PipelineDocument:
    value: dict[str, object] = {
        "canonical_format_version": 1,
        "edges": edges,
        "layout": [],
        "nodes": nodes,
        "resource_policy": {} if resource_policy is None else resource_policy,
        "schema_version": 1,
    }
    return PipelineDocument.from_mapping(value)


def _valid_document(*, resource_policy: Mapping[str, object] | None = None) -> PipelineDocument:
    return _document(
        [
            _node(1, "source.csv", str(_CONNECTOR_ID)),
            _node(2, "transform.normalize"),
            _node(3, "export.parquet"),
        ],
        [_edge(1, 2), _edge(2, 3)],
        resource_policy=resource_policy,
    )


def _record(
    capabilities: Mapping[str, object],
    *,
    secret_marker: str = "not-sensitive",
) -> ConnectorRecord:
    timestamp = UtcTimestamp(datetime(2026, 8, 14, tzinfo=UTC))
    return ConnectorRecord(
        connector_id=_CONNECTOR_ID,
        kind="validation-source",
        display_name="Validation source",
        configuration=ConfigurationDocument.from_mapping({"token": secret_marker}),
        capabilities=ConfigurationDocument.from_mapping(capabilities),
        schema_discovery=None,
        secret_references=(),
        revision=1,
        created_at=timestamp,
        updated_at=timestamp,
        archived_at=None,
        row_version=1,
    )


def _codes(report: PipelineValidationReport) -> tuple[str, ...]:
    return tuple(issue.code.value for issue in report.issues)


@pytest.mark.parametrize(
    ("document", "records", "expected"),
    [
        (
            _document(
                [
                    _node(1, "source.csv", str(_CONNECTOR_ID)),
                    _node(2, "transform.normalize"),
                    _node(3, "export.parquet"),
                ],
                [_edge(1, 2), _edge(2, 3), _edge(3, 2)],
            ),
            (_record({"read": True}),),
            ("graph_cycle",),
        ),
        (
            _document(
                [
                    _node(1, "source.csv", str(_CONNECTOR_ID)),
                    _node(2, "export.parquet"),
                    _node(3, "source.jsonl", str(_CONNECTOR_ID)),
                    _node(4, "export.parquet"),
                ],
                [_edge(1, 2), _edge(3, 4)],
            ),
            (_record({"read": True}),),
            ("graph_disconnected",),
        ),
        (
            _document(
                [
                    _node(1, "source.csv", str(_CONNECTOR_ID)),
                    _node(2, "transform.normalize"),
                ],
                [_edge(1, 2)],
            ),
            (_record({"read": True}),),
            ("graph_invalid_terminal",),
        ),
        (_valid_document(), (), ("connector_missing",)),
        (
            _valid_document(),
            (_record({}),),
            ("connector_capability_missing",),
        ),
        (
            _valid_document(),
            (_record({"read": 1}, secret_marker="runtime-secret-marker"),),
            ("connector_invalid",),
        ),
        (
            _valid_document(resource_policy={"max_concurrency": 0}),
            (_record({"read": True}),),
            ("resource_policy_invalid",),
        ),
        (
            _document(
                [
                    _node(1, "source.csv", str(_CONNECTOR_ID)),
                    _node(2, "reconcile.target", str(_CONNECTOR_ID)),
                    _node(3, "repair.generate"),
                    _node(4, "repair.apply", str(_CONNECTOR_ID)),
                    _node(5, "verify.target", str(_CONNECTOR_ID)),
                ],
                [_edge(1, 2), _edge(2, 3), _edge(3, 4), _edge(4, 5)],
            ),
            (_record({"idempotency": True, "read": True, "write": True}),),
            ("repair_approval_required",),
        ),
    ],
)
def test_validation_error_code_snapshots_use_real_phase_five_validators(
    document: PipelineDocument,
    records: tuple[ConnectorRecord, ...],
    expected: tuple[str, ...],
) -> None:
    report = validate_pipeline(document, records)
    assert _codes(report) == expected
    assert report.is_valid is False


def test_validation_issue_snapshot_has_exact_safe_display_content() -> None:
    report = validate_pipeline(
        _valid_document(),
        (_record({"read": 1}, secret_marker="runtime-secret-marker"),),
    )
    assert report.to_mapping() == {
        "issues": [
            {
                "code": "connector_invalid",
                "message": "Connector definitions must satisfy the immutable connector contract.",
                "path": "/connector_bindings",
            }
        ],
        "valid": False,
        "version": 1,
    }
    assert "runtime-secret-marker" not in str(report.to_mapping())
    assert "runtime-secret-marker" not in repr(report)


def test_validation_report_collects_and_orders_independent_failures() -> None:
    document = _document(
        [
            _node(1, "source.csv", str(_CONNECTOR_ID)),
            _node(2, "transform.normalize"),
        ],
        [_edge(1, 2)],
        resource_policy={"max_concurrency": True},
    )
    report = validate_pipeline(document, ())
    assert _codes(report) == (
        "graph_invalid_terminal",
        "connector_missing",
        "resource_policy_invalid",
    )


def test_valid_pipeline_produces_empty_report() -> None:
    report = validate_pipeline(_valid_document(), (_record({"read": True}),))
    assert report == PipelineValidationReport(())
    report.require_valid()


def test_generic_repair_contract_failure_has_stable_fallback_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_repair(_document: PipelineDocument) -> None:
        raise RepairSafetyError("implementation detail must not be displayed")

    monkeypatch.setattr(validation_module, "validate_repair_safety", reject_repair)
    report = validate_pipeline(_valid_document(), (_record({"read": True}),))
    assert _codes(report) == (PipelineValidationCode.REPAIR_POLICY_INVALID.value,)
    assert "implementation detail" not in str(report.to_mapping())


def test_validation_composer_requires_exact_public_inputs() -> None:
    with pytest.raises(TypeError, match="PipelineDocument"):
        validate_pipeline(cast(Any, {}), ())
    with pytest.raises(TypeError, match="tuple"):
        validate_pipeline(_valid_document(), cast(Any, []))
    with pytest.raises(TypeError, match="invalid value"):
        validate_pipeline(_valid_document(), cast(Any, (object(),)))
