"""Frozen contracts for bounded human-readable validation reports."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import pytest

from paritygrid.application.planner import (
    MAX_VALIDATION_ISSUES,
    MAX_VALIDATION_MESSAGE_LENGTH,
    MAX_VALIDATION_PATH_LENGTH,
    VALIDATION_REPORT_VERSION,
    InvalidPipelineValidationReportError,
    PipelineValidationCode,
    PipelineValidationError,
    PipelineValidationFailedError,
    PipelineValidationIssue,
    PipelineValidationReport,
)


def _issue(
    code: PipelineValidationCode = PipelineValidationCode.GRAPH_CYCLE,
    *,
    path: str = "/edges",
    message: str = "Pipeline graph must not contain a directed cycle.",
) -> PipelineValidationIssue:
    return PipelineValidationIssue(code, path, message)


def test_validation_constants_codes_and_error_family_are_frozen() -> None:
    assert VALIDATION_REPORT_VERSION == 1
    assert MAX_VALIDATION_ISSUES == 64
    assert MAX_VALIDATION_PATH_LENGTH == 256
    assert MAX_VALIDATION_MESSAGE_LENGTH == 512
    assert tuple(PipelineValidationCode) == (
        PipelineValidationCode.GRAPH_CYCLE,
        PipelineValidationCode.GRAPH_DISCONNECTED,
        PipelineValidationCode.GRAPH_INVALID_TERMINAL,
        PipelineValidationCode.CONNECTOR_MISSING,
        PipelineValidationCode.CONNECTOR_CAPABILITY_MISSING,
        PipelineValidationCode.CONNECTOR_INVALID,
        PipelineValidationCode.RESOURCE_POLICY_INVALID,
        PipelineValidationCode.REPAIR_APPROVAL_REQUIRED,
        PipelineValidationCode.REPAIR_POLICY_INVALID,
    )
    assert issubclass(InvalidPipelineValidationReportError, PipelineValidationError)
    assert issubclass(PipelineValidationFailedError, PipelineValidationError)


def test_issue_has_exact_mapping_and_is_immutable() -> None:
    issue = _issue()
    assert issue.to_mapping() == {
        "code": "graph_cycle",
        "message": "Pipeline graph must not contain a directed cycle.",
        "path": "/edges",
    }
    with pytest.raises(FrozenInstanceError):
        issue.path = "/nodes"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("code", "graph_cycle", "PipelineValidationCode"),
        ("path", 1, "path must be text"),
        ("message", 1, "message must be text"),
    ],
)
def test_issue_requires_exact_public_types(field: str, value: object, message: str) -> None:
    with pytest.raises(TypeError, match=message):
        replace(_issue(), **{field: value})


@pytest.mark.parametrize(
    "path",
    ["", "edges", "//edges", "/edges/has space", "/edges?", "/" + "a" * 256],
)
def test_issue_rejects_noncanonical_or_unbounded_paths(path: str) -> None:
    with pytest.raises(InvalidPipelineValidationReportError, match="path"):
        _issue(path=path)


@pytest.mark.parametrize(
    "message",
    ["", "a" * (MAX_VALIDATION_MESSAGE_LENGTH + 1), "line\nbreak", "Cafe\u0301"],
)
def test_issue_rejects_unbounded_control_or_non_normalized_messages(message: str) -> None:
    with pytest.raises(InvalidPipelineValidationReportError, match="message"):
        _issue(message=message)


def test_report_sorts_codes_and_maps_validity_deterministically() -> None:
    connector = _issue(
        PipelineValidationCode.CONNECTOR_MISSING,
        path="/nodes",
        message="Every connector-requiring node must reference an available connector.",
    )
    cycle = _issue()
    report = PipelineValidationReport((connector, cycle))
    assert report.issues == (cycle, connector)
    assert report.is_valid is False
    assert report.to_mapping() == {
        "issues": [cycle.to_mapping(), connector.to_mapping()],
        "valid": False,
        "version": 1,
    }


def test_empty_report_is_valid_and_require_valid_is_a_noop() -> None:
    report = PipelineValidationReport(())
    assert report.is_valid is True
    assert report.require_valid() is None
    assert report.to_mapping() == {"issues": [], "valid": True, "version": 1}


def test_invalid_report_raises_typed_failure_with_report() -> None:
    report = PipelineValidationReport((_issue(),))
    with pytest.raises(PipelineValidationFailedError, match="1 issue") as captured:
        report.require_valid()
    assert captured.value.report is report
    with pytest.raises(TypeError, match="PipelineValidationReport"):
        PipelineValidationFailedError(())  # type: ignore[arg-type]


def test_report_requires_exact_tuple_issue_and_version_types() -> None:
    with pytest.raises(TypeError, match="tuple"):
        PipelineValidationReport([])  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="invalid issue"):
        PipelineValidationReport((object(),))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="version"):
        PipelineValidationReport((), version=True)


def test_report_rejects_duplicate_excessive_and_unsupported_content() -> None:
    issue = _issue()
    with pytest.raises(InvalidPipelineValidationReportError, match="unique"):
        PipelineValidationReport((issue, issue))
    excessive = tuple(_issue(path=f"/nodes/{index}") for index in range(MAX_VALIDATION_ISSUES + 1))
    with pytest.raises(InvalidPipelineValidationReportError, match="limit"):
        PipelineValidationReport(excessive)
    with pytest.raises(InvalidPipelineValidationReportError, match="unsupported"):
        PipelineValidationReport((), version=2)
