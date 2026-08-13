"""Dependency-neutral artifact integrity report contract tests."""

from dataclasses import replace

import pytest

from paritygrid.application.ports import (
    ArtifactIntegrityIssue,
    ArtifactIntegrityIssueKind,
    ArtifactIntegrityScanInvalidError,
    ArtifactIntegrityScanReport,
    ArtifactRelativePath,
)
from paritygrid.application.ports import artifact_integrity as contract
from paritygrid.domain.models import ArtifactId


def _missing() -> ArtifactIntegrityIssue:
    return ArtifactIntegrityIssue(
        ArtifactIntegrityIssueKind.MISSING_FILE,
        ArtifactRelativePath("runs/missing.parquet"),
        ArtifactId("art_missing"),
        None,
    )


def test_issue_shape_is_locked_to_classification() -> None:
    assert _missing().order_key == (
        "missing_file",
        "runs/missing.parquet",
        "art_missing",
        "",
    )
    with pytest.raises(ArtifactIntegrityScanInvalidError, match="fields"):
        replace(_missing(), artifact_id=None)
    with pytest.raises(TypeError, match="digest"):
        ArtifactIntegrityIssue(ArtifactIntegrityIssueKind.UNSAFE_ENTRY, None, None, "BAD")
    with pytest.raises(TypeError, match="kind"):
        replace(_missing(), kind="missing_file")
    with pytest.raises(TypeError, match="path"):
        replace(_missing(), relative_path=object())
    with pytest.raises(TypeError, match="identity"):
        replace(_missing(), artifact_id=object())


def test_report_requires_sorted_unique_bounded_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing = _missing()
    orphan = ArtifactIntegrityIssue(
        ArtifactIntegrityIssueKind.ORPHAN_FILE,
        ArtifactRelativePath("runs/orphan.parquet"),
        None,
        None,
    )
    report = ArtifactIntegrityScanReport(1, 1, 0, (missing, orphan), "0" * 64)

    assert not report.is_clean
    assert ArtifactIntegrityScanReport(0, 0, 0, (), "1" * 64).is_clean
    with pytest.raises(ArtifactIntegrityScanInvalidError, match="sorted"):
        replace(report, issues=(orphan, missing))
    with pytest.raises(ArtifactIntegrityScanInvalidError, match="exceeds"):
        replace(report, verified_manifest_count=2)
    with pytest.raises(ValueError, match="count"):
        replace(report, manifest_count=-1)
    with pytest.raises(TypeError, match="digest"):
        replace(report, inventory_sha256="bad")
    with pytest.raises(TypeError, match="immutable tuple"):
        replace(report, issues=[missing])
    monkeypatch.setattr(contract, "MAX_ARTIFACT_INTEGRITY_ISSUES", 0)
    with pytest.raises(contract.ArtifactIntegrityScanLimitError, match="issue count"):
        replace(report, issues=(missing,))
