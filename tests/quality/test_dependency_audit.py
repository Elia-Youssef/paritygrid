"""Dependency-audit classification: findings versus transport failures (P22.2)."""

import importlib.util
import json
import re
import subprocess
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from paritygrid.quality.dependency_audit import (
    AuditAttempt,
    AuditOutcome,
    ScannerError,
    ScanResult,
    ScanVerdict,
    VulnerabilityFinding,
    VulnerabilitySuppression,
    apply_suppressions,
    classify_scan,
    load_suppressions,
    npm_threshold_breached,
    run_with_transport_retry,
    severity_at_or_above,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "dependency_audit"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _npm(stdout: str, returncode: int = 0) -> ScanResult:
    return ScanResult(tool="npm-audit", returncode=returncode, stdout=stdout, stderr="")


def _pip(stdout: str, stderr: str = "", returncode: int = 0) -> ScanResult:
    return ScanResult(tool="pip-audit", returncode=returncode, stdout=stdout, stderr=stderr)


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


class TestRecordedFixtureShapes:
    """The committed fixtures keep proving the shapes they were recorded for."""

    def test_npm_clean_fixture_classifies_clean(self) -> None:
        verdict = classify_scan(_npm(_fixture("npm_clean.json")))
        assert verdict.outcome is AuditOutcome.CLEAN
        assert verdict.findings == ()
        assert ("high", 0) in verdict.severity_counts

    def test_npm_findings_fixture_classifies_findings(self) -> None:
        verdict = classify_scan(_npm(_fixture("npm_findings_high.json"), returncode=1))
        assert verdict.outcome is AuditOutcome.FINDINGS
        assert verdict.severity_counts == (
            ("info", 0),
            ("low", 0),
            ("moderate", 0),
            ("high", 1),
            ("critical", 0),
        )
        assert (
            VulnerabilityFinding(
                package="synthetic-package", identifier="GHSA-c4wx-2mqr-f35j", severity="high"
            )
            in verdict.findings
        )

    def test_npm_transport_fixture_classifies_transport_failure(self) -> None:
        verdict = classify_scan(_npm(_fixture("npm_transport_error.json")))
        assert verdict.outcome is AuditOutcome.TRANSPORT_FAILURE
        assert "EAI_AGAIN" in verdict.detail

    def test_real_npm_transport_shape_has_no_error_code(self) -> None:
        """npm 11 puts the resolver reason in the message, not error.code."""
        document = json.dumps(
            {
                "message": "request to https://registry.npmjs.org/-/npm/v1/security/"
                "advisories/bulk failed, reason: connect ECONNREFUSED 127.0.0.1:9",
                "error": {"summary": "", "detail": ""},
            }
        )
        verdict = classify_scan(_npm(document))
        assert verdict.outcome is AuditOutcome.TRANSPORT_FAILURE
        assert "ECONNREFUSED" in verdict.detail

    def test_error_text_without_a_transport_marker_stays_unclassified(self) -> None:
        document = json.dumps(
            {
                "message": "request failed for an unknown reason",
                "error": {"summary": "", "detail": ""},
            }
        )
        verdict = classify_scan(_npm(document))
        assert verdict.outcome is AuditOutcome.UNCLASSIFIED

    def test_pip_audit_clean_fixture_classifies_clean(self) -> None:
        verdict = classify_scan(_pip(_fixture("pip_audit_clean.json")))
        assert verdict.outcome is AuditOutcome.CLEAN

    def test_pip_audit_findings_fixture_classifies_findings(self) -> None:
        verdict = classify_scan(_pip(_fixture("pip_audit_findings.json"), returncode=1))
        assert verdict.outcome is AuditOutcome.FINDINGS
        assert verdict.findings == (
            VulnerabilityFinding(
                package="synthetic-package==1.0.0",
                identifier="PYSEC-2026-0001",
                severity="unspecified",
            ),
        )

    def test_pip_audit_transport_stderr_classifies_transport_failure(self) -> None:
        verdict = classify_scan(_pip(stdout="", stderr=_fixture("pip_audit_transport_stderr.txt")))
        assert verdict.outcome is AuditOutcome.TRANSPORT_FAILURE

    def test_pip_audit_transport_traceback_classifies_transport_failure(self) -> None:
        """pip-audit can crash with the raw HTTP stack instead of its fatal log."""
        verdict = classify_scan(
            _pip(stdout="", stderr=_fixture("pip_audit_transport_traceback.txt"))
        )
        assert verdict.outcome is AuditOutcome.TRANSPORT_FAILURE

    def test_non_transport_traceback_is_unclassified(self) -> None:
        stderr = "Traceback (most recent call last):\nValueError: bad requirements input\n"
        verdict = classify_scan(_pip(stdout="", stderr=stderr))
        assert verdict.outcome is AuditOutcome.UNCLASSIFIED


class TestNpmClassification:
    def test_exit_code_is_never_evidence(self) -> None:
        findings = _fixture("npm_findings_high.json")
        # npm --audit-level=critical exits 0 while reporting a high finding.
        assert classify_scan(_npm(findings, returncode=0)).outcome is AuditOutcome.FINDINGS

    def test_service_error_without_transport_code_is_configuration_failure(self) -> None:
        document = json.dumps({"error": {"code": "EUSAGE", "summary": "bad usage"}})
        verdict = classify_scan(_npm(document))
        assert verdict.outcome is AuditOutcome.CONFIGURATION_FAILURE

    @pytest.mark.parametrize("code", ["E429", "E500", "E502", "E503", "E504"])
    def test_registry_http_outage_codes_are_transport_failures(self, code: str) -> None:
        document = json.dumps({"error": {"code": code, "summary": "registry unavailable"}})
        verdict = classify_scan(_npm(document))
        assert verdict.outcome is AuditOutcome.TRANSPORT_FAILURE
        assert code in verdict.detail

    def test_client_error_codes_are_not_retryable_transport_failures(self) -> None:
        document = json.dumps({"error": {"code": "E404", "summary": "not found"}})
        verdict = classify_scan(_npm(document))
        assert verdict.outcome is AuditOutcome.CONFIGURATION_FAILURE

    def test_error_without_code_is_unclassified(self) -> None:
        verdict = classify_scan(_npm(json.dumps({"error": {"summary": "broken"}})))
        assert verdict.outcome is AuditOutcome.UNCLASSIFIED

    def test_non_json_output_is_unclassified(self) -> None:
        verdict = classify_scan(_npm("npm ERR! whatever"))
        assert verdict.outcome is AuditOutcome.UNCLASSIFIED

    def test_missing_vulnerability_metadata_is_unclassified(self) -> None:
        verdict = classify_scan(_npm(json.dumps({"auditReportVersion": 2})))
        assert verdict.outcome is AuditOutcome.UNCLASSIFIED

    def test_non_integer_severity_count_is_unclassified(self) -> None:
        document = json.dumps({"metadata": {"vulnerabilities": {"info": 0, "low": "many"}}})
        verdict = classify_scan(_npm(document))
        assert verdict.outcome is AuditOutcome.UNCLASSIFIED

    def test_boolean_severity_count_is_unclassified(self) -> None:
        document = json.dumps({"metadata": {"vulnerabilities": {"info": True}}})
        verdict = classify_scan(_npm(document))
        assert verdict.outcome is AuditOutcome.UNCLASSIFIED

    def test_unknown_severity_key_is_unclassified(self) -> None:
        document = json.dumps(
            {
                "vulnerabilities": {},
                "metadata": {
                    "vulnerabilities": {
                        "info": 0,
                        "low": 0,
                        "moderate": 0,
                        "high": 0,
                        "critical": 0,
                        "total": 0,
                        "severe": 1,
                    }
                },
            }
        )
        verdict = classify_scan(_npm(document))
        assert verdict.outcome is AuditOutcome.UNCLASSIFIED

    def test_total_contradicting_the_severity_counts_is_unclassified(self) -> None:
        document = json.dumps(
            {
                "vulnerabilities": {},
                "metadata": {
                    "vulnerabilities": {
                        "info": 0,
                        "low": 0,
                        "moderate": 0,
                        "high": 0,
                        "critical": 0,
                        "total": 3,
                    }
                },
            }
        )
        verdict = classify_scan(_npm(document))
        assert verdict.outcome is AuditOutcome.UNCLASSIFIED

    def test_zero_counts_beside_a_nonempty_vulnerabilities_map_is_unclassified(self) -> None:
        document = json.dumps(
            {
                "vulnerabilities": {
                    "pkg": {"name": "pkg", "severity": "low", "via": ["GHSA-c4wx-2mqr-f35j"]}
                },
                "metadata": {
                    "vulnerabilities": {
                        "info": 0,
                        "low": 0,
                        "moderate": 0,
                        "high": 0,
                        "critical": 0,
                        "total": 0,
                    }
                },
            }
        )
        verdict = classify_scan(_npm(document))
        assert verdict.outcome is AuditOutcome.UNCLASSIFIED

    def test_counts_without_a_vulnerabilities_map_are_unclassified(self) -> None:
        document = json.dumps(
            {
                "metadata": {
                    "vulnerabilities": {
                        "info": 0,
                        "low": 1,
                        "moderate": 0,
                        "high": 0,
                        "critical": 0,
                        "total": 1,
                    }
                }
            }
        )
        verdict = classify_scan(_npm(document))
        assert verdict.outcome is AuditOutcome.UNCLASSIFIED

    def test_missing_severity_count_is_unclassified(self) -> None:
        document = json.dumps({"metadata": {"vulnerabilities": {"info": 0, "low": 0}}})
        verdict = classify_scan(_npm(document))
        assert verdict.outcome is AuditOutcome.UNCLASSIFIED

    def test_non_object_document_is_unclassified(self) -> None:
        verdict = classify_scan(_npm("[1, 2, 3]"))
        assert verdict.outcome is AuditOutcome.UNCLASSIFIED

    def test_below_threshold_source_titles_never_match_suppression_identifiers(
        self,
    ) -> None:
        document = json.dumps(
            {
                "vulnerabilities": {
                    "pkg": {"name": "pkg", "severity": "low", "via": [{"title": "DoS"}]}
                },
                "metadata": {
                    "vulnerabilities": {
                        "info": 1,
                        "low": 0,
                        "moderate": 0,
                        "high": 0,
                        "critical": 0,
                        "total": 1,
                    }
                },
            }
        )
        verdict = classify_scan(_npm(document))
        assert verdict.findings[0].identifier == "unknown-source"


class TestPipAuditClassification:
    @staticmethod
    def _report(*entries: dict[str, object]) -> str:
        return json.dumps({"dependencies": list(entries), "fixes": []})

    def test_exit_code_is_never_evidence(self) -> None:
        # pip-audit exits 1 for findings and for fatal errors alike; the
        # structured report decides, not the code.
        clean = self._report({"name": "uvicorn", "version": "0.52.4", "vulns": []})
        assert classify_scan(_pip(clean, returncode=1)).outcome is AuditOutcome.CLEAN

    def test_transport_stderr_without_json_is_transport_failure(self) -> None:
        verdict = classify_scan(_pip("", stderr="Could not connect to PyPI's vulnerability feed"))
        assert verdict.outcome is AuditOutcome.TRANSPORT_FAILURE

    def test_fatal_output_without_marker_is_unclassified(self) -> None:
        verdict = classify_scan(_pip("", stderr="ERROR: invalid requirements input: bad"))
        assert verdict.outcome is AuditOutcome.UNCLASSIFIED

    def test_report_without_dependencies_list_is_configuration_failure(self) -> None:
        verdict = classify_scan(_pip(json.dumps({"ok": True})))
        assert verdict.outcome is AuditOutcome.CONFIGURATION_FAILURE

    def test_non_object_json_report_is_configuration_failure(self) -> None:
        verdict = classify_scan(_pip("[1, 2, 3]"))
        assert verdict.outcome is AuditOutcome.CONFIGURATION_FAILURE

    def test_entry_without_name_is_configuration_failure(self) -> None:
        report = self._report({"version": "1.0", "vulns": []})
        verdict = classify_scan(_pip(report))
        assert verdict.outcome is AuditOutcome.CONFIGURATION_FAILURE

    def test_entry_without_the_vulns_field_never_counts_as_clean(self) -> None:
        report = self._report({"name": "pkg", "version": "1.0"})
        verdict = classify_scan(_pip(report))
        assert verdict.outcome is AuditOutcome.CONFIGURATION_FAILURE

    def test_vulnerability_without_identifier_is_configuration_failure(self) -> None:
        report = self._report({"name": "pkg", "version": "1.0", "vulns": [{"fix_versions": []}]})
        verdict = classify_scan(_pip(report))
        assert verdict.outcome is AuditOutcome.CONFIGURATION_FAILURE


class TestToolContract:
    def test_unknown_tool_is_rejected(self) -> None:
        with pytest.raises(ScannerError, match="unknown scanner tool"):
            classify_scan(ScanResult(tool="mystery-scanner", returncode=0, stdout="", stderr=""))

    def test_findings_beyond_the_evidence_bound_are_truncated(self) -> None:
        entries = [
            {
                "name": f"pkg-{index}",
                "version": "1.0",
                "vulns": [{"id": f"CVE-2026-{index:05d}"}],
            }
            for index in range(300)
        ]
        verdict = classify_scan(_pip(json.dumps({"dependencies": entries, "fixes": []})))
        assert verdict.outcome is AuditOutcome.FINDINGS
        assert len(verdict.findings) == 200
        assert verdict.findings_truncated


class TestSeverityPolicy:
    @pytest.mark.parametrize(
        ("severity", "expected"),
        [
            ("info", False),
            ("low", False),
            ("moderate", False),
            ("high", True),
            ("critical", True),
        ],
    )
    def test_recorded_threshold_is_high(self, severity: str, expected: bool) -> None:
        assert severity_at_or_above(severity) is expected

    def test_unknown_severity_fails_closed(self) -> None:
        assert severity_at_or_above("unspecified") is True

    def test_threshold_decision_uses_complete_counts_not_truncated_findings(self) -> None:
        # The finding list is evidence-bounded; the decision must read the
        # complete metadata counts so severe entries beyond the bound are
        # never invisible.
        truncated = ScanVerdict(
            outcome=AuditOutcome.FINDINGS,
            detail="",
            findings=(),
            severity_counts=(
                ("info", 0),
                ("low", 0),
                ("moderate", 0),
                ("high", 1),
                ("critical", 0),
            ),
        )
        assert npm_threshold_breached(truncated) is True
        below = ScanVerdict(
            outcome=AuditOutcome.FINDINGS,
            detail="",
            findings=(VulnerabilityFinding("p", "CVE-2026-10001", "low"),),
            severity_counts=(
                ("info", 1),
                ("low", 1),
                ("moderate", 0),
                ("high", 0),
                ("critical", 0),
            ),
        )
        assert npm_threshold_breached(below) is False
        assert npm_threshold_breached(ScanVerdict(outcome=AuditOutcome.CLEAN, detail="")) is False


class TestTransportRetry:
    def test_findings_are_never_retried(self) -> None:
        calls: list[int] = []

        def runner() -> ScanResult:
            calls.append(1)
            return _npm(_fixture("npm_findings_high.json"), returncode=1)

        outcome = run_with_transport_retry(runner)
        assert len(calls) == 1
        assert outcome.outcome is AuditOutcome.FINDINGS
        assert not outcome.retried

    def test_clean_is_never_retried(self) -> None:
        calls: list[int] = []

        def runner() -> ScanResult:
            calls.append(1)
            return _npm(_fixture("npm_clean.json"))

        outcome = run_with_transport_retry(runner)
        assert len(calls) == 1
        assert outcome.outcome is AuditOutcome.CLEAN

    def test_transport_failure_is_retried_once_on_the_unchanged_input(self) -> None:
        attempts: list[int] = []

        def runner() -> ScanResult:
            attempts.append(len(attempts))
            if len(attempts) == 1:
                return _npm(_fixture("npm_transport_error.json"))
            return _npm(_fixture("npm_clean.json"))

        outcome = run_with_transport_retry(runner)
        assert attempts == [0, 1]
        assert outcome.outcome is AuditOutcome.CLEAN
        assert outcome.retried
        assert outcome.attempts[0].verdict.outcome is AuditOutcome.TRANSPORT_FAILURE
        assert all(isinstance(attempt, AuditAttempt) for attempt in outcome.attempts)

    def test_exhausted_transport_failure_stays_a_transport_failure(self) -> None:
        calls: list[int] = []

        def runner() -> ScanResult:
            calls.append(1)
            return _pip("", stderr="Could not connect to PyPI's vulnerability feed")

        outcome = run_with_transport_retry(runner, max_attempts=3)
        assert len(calls) == 3
        assert outcome.outcome is AuditOutcome.TRANSPORT_FAILURE
        assert len(outcome.attempts) == 3

    def test_max_attempts_below_one_is_rejected(self) -> None:
        with pytest.raises(ScannerError, match="max_attempts"):
            run_with_transport_retry(lambda: _npm(_fixture("npm_clean.json")), max_attempts=0)


class TestSuppressions:
    def _finding(self) -> VulnerabilityFinding:
        return VulnerabilityFinding(
            package="synthetic-package", identifier="GHSA-c4wx-2mqr-f35j", severity="high"
        )

    def _suppression(self, *, expires: str = "2099-01-01") -> VulnerabilitySuppression:
        return VulnerabilitySuppression(
            ecosystem="npm",
            package="synthetic-package",
            identifier="GHSA-c4wx-2mqr-f35j",
            reason="fixed upstream in the locked range",
            owner="repository owner",
            approval_authority="maintainer decision record",
            expires=expires,
            upstream_reference="GHSA-c4wx-2mqr-f35j",
        )

    def test_valid_suppression_masks_exactly_its_finding(self) -> None:
        verdict = ScanVerdict(outcome=AuditOutcome.FINDINGS, detail="", findings=(self._finding(),))
        application = apply_suppressions(verdict, (self._suppression(),), today="2026-09-04")
        assert application.remaining == ()
        assert application.applied == (self._finding(),)

    def test_expired_suppression_never_masks_a_finding(self) -> None:
        verdict = ScanVerdict(outcome=AuditOutcome.FINDINGS, detail="", findings=(self._finding(),))
        expired = self._suppression(expires="2026-01-01")
        application = apply_suppressions(verdict, (expired,), today="2026-09-04")
        assert application.remaining == (self._finding(),)
        assert application.applied == ()

    def test_suppression_for_another_package_does_not_mask(self) -> None:
        verdict = ScanVerdict(outcome=AuditOutcome.FINDINGS, detail="", findings=(self._finding(),))
        foreign = VulnerabilitySuppression(
            ecosystem="npm",
            package="other-package",
            identifier="GHSA-c4wx-2mqr-f35j",
            reason="unrelated",
            owner="repository owner",
            approval_authority="maintainer decision record",
            expires="2099-01-01",
            upstream_reference="GHSA-c4wx-2mqr-f35j",
        )
        application = apply_suppressions(verdict, (foreign,), today="2026-09-04")
        assert application.remaining == (self._finding(),)

    def test_pip_audit_package_versions_still_match_by_name(self) -> None:
        finding = VulnerabilityFinding(
            package="synthetic-package==1.0.0",
            identifier="PYSEC-2026-0001",
            severity="unspecified",
        )
        verdict = ScanVerdict(outcome=AuditOutcome.FINDINGS, detail="", findings=(finding,))
        suppression = VulnerabilitySuppression(
            ecosystem="pypi",
            package="synthetic-package",
            identifier="PYSEC-2026-0001",
            reason="fixed upstream",
            owner="repository owner",
            approval_authority="maintainer decision record",
            expires="2099-01-01",
            upstream_reference="PYSEC-2026-0001",
        )
        application = apply_suppressions(verdict, (suppression,), today="2026-09-04")
        assert application.applied == (finding,)

    @pytest.mark.parametrize(
        "overrides",
        [
            {"identifier": "*"},
            {"identifier": "not-an-advisory-id"},
            {"package": "*"},
            {"package": ""},
            {"reason": ""},
            {"owner": ""},
            {"approval_authority": ""},
            {"upstream_reference": ""},
            {"expires": "01-01-2099"},
            {"expires": "2099-13-45"},
            {"ecosystem": "cargo"},
        ],
    )
    def test_malformed_and_overbroad_suppressions_fail_closed(
        self, overrides: dict[str, str]
    ) -> None:
        fields = {
            "ecosystem": "npm",
            "package": "synthetic-package",
            "identifier": "GHSA-c4wx-2mqr-f35j",
            "reason": "narrow documented reason",
            "owner": "repository owner",
            "approval_authority": "maintainer decision record",
            "expires": "2099-01-01",
            "upstream_reference": "GHSA-c4wx-2mqr-f35j",
        }
        fields.update(overrides)
        with pytest.raises(ScannerError):
            VulnerabilitySuppression(**fields)

    def test_load_rejects_non_list_documents_and_missing_fields(self) -> None:
        with pytest.raises(ScannerError, match="must be a list"):
            load_suppressions({})
        with pytest.raises(ScannerError, match="lacks required fields"):
            load_suppressions([{"ecosystem": "npm"}])

    def test_load_rejects_duplicate_suppressions(self) -> None:
        duplicate = {
            "ecosystem": "npm",
            "package": "synthetic-package",
            "identifier": "GHSA-c4wx-2mqr-f35j",
            "reason": "first",
            "owner": "repository owner",
            "approval_authority": "maintainer decision record",
            "expires": "2099-01-01",
            "upstream_reference": "GHSA-c4wx-2mqr-f35j",
        }
        with pytest.raises(ScannerError, match="duplicate suppression"):
            load_suppressions([duplicate, dict(duplicate)])

    def test_load_rejects_documents_beyond_the_recorded_bound(self) -> None:
        entry = {
            "ecosystem": "npm",
            "package": "synthetic-package",
            "identifier": "GHSA-c4wx-2mqr-f35j",
            "reason": "narrow documented reason",
            "owner": "repository owner",
            "approval_authority": "maintainer decision record",
            "expires": "2099-01-01",
            "upstream_reference": "GHSA-c4wx-2mqr-f35j",
        }
        with pytest.raises(ScannerError, match="exceeds the recorded bound"):
            load_suppressions([dict(entry) for _ in range(101)])

    def test_load_rejects_non_object_entries(self) -> None:
        with pytest.raises(ScannerError, match="must be an object"):
            load_suppressions(["not-an-object"])

    def test_loaded_registry_applies_end_to_end(self) -> None:
        document = [
            {
                "ecosystem": "npm",
                "package": "synthetic-package",
                "identifier": "GHSA-c4wx-2mqr-f35j",
                "reason": "narrow documented reason",
                "owner": "repository owner",
                "approval_authority": "maintainer decision record",
                "expires": "2099-01-01",
                "upstream_reference": "GHSA-c4wx-2mqr-f35j",
            }
        ]
        registry = load_suppressions(document)
        verdict = ScanVerdict(outcome=AuditOutcome.FINDINGS, detail="", findings=(self._finding(),))
        application = apply_suppressions(verdict, registry, today="2026-09-04")
        assert application.remaining == ()


def _fake_which_uv(name: str) -> str:
    del name
    return "uv"


def _fake_which_npm(name: str) -> str:
    del name
    return "npm"


def _load_script(module_name: str) -> ModuleType:
    """Load a repository verification script as a module."""
    path = PROJECT_ROOT / "scripts" / f"{module_name}.py"
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _completed(returncode: int, stdout: str = "", stderr: str = "") -> SimpleNamespace:
    return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)


class TestPythonWrapperScript:
    """The wrapper script owns one unchanged export and one bounded retry."""

    def test_retry_runs_on_the_same_unchanged_export(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        script = _load_script("verify_python_dependencies")
        export_commands: list[tuple[str, ...]] = []
        scan_requirements: list[Path] = []
        clean = json.dumps({"dependencies": [{"name": "pkg", "version": "1", "vulns": []}]})
        transport_stderr = "ERROR:Could not connect to PyPI's vulnerability feed"

        def fake_run(command: tuple[str, ...], **kwargs: object) -> SimpleNamespace:
            if "export" in command:
                export_commands.append(command)
                return _completed(0)
            requirement = Path(str(command[command.index("--requirement") + 1]))
            scan_requirements.append(requirement)
            if len(scan_requirements) == 1:
                return _completed(1, stdout="", stderr=transport_stderr)
            return _completed(0, stdout=clean)

        monkeypatch.setattr(script.shutil, "which", _fake_which_uv)
        monkeypatch.setattr(script.subprocess, "run", fake_run)

        script.verify_dependencies(PROJECT_ROOT)

        assert len(export_commands) == 1
        assert "--locked" in export_commands[0]
        assert "--all-groups" in export_commands[0]
        assert len(scan_requirements) == 2
        assert scan_requirements[0] == scan_requirements[1]
        assert "unchanged-input retry" in capsys.readouterr().out

    def test_findings_are_reported_and_never_retried(self, monkeypatch: pytest.MonkeyPatch) -> None:
        script = _load_script("verify_python_dependencies")
        scan_calls = 0
        finding_report = json.dumps(
            {
                "dependencies": [
                    {
                        "name": "pkg",
                        "version": "1.0",
                        "vulns": [{"id": "PYSEC-2026-0001"}],
                    }
                ]
            }
        )

        def fake_run(command: tuple[str, ...], **kwargs: object) -> SimpleNamespace:
            nonlocal scan_calls
            if "export" in command:
                return _completed(0)
            scan_calls += 1
            return _completed(1, stdout=finding_report)

        monkeypatch.setattr(script.shutil, "which", _fake_which_uv)
        monkeypatch.setattr(script.subprocess, "run", fake_run)

        with pytest.raises(RuntimeError, match="outcome findings"):
            script.verify_dependencies(PROJECT_ROOT)
        assert scan_calls == 1

    def test_export_failure_output_never_contains_command_paths(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        script = _load_script("verify_python_dependencies")

        def boom(command: tuple[str, ...], **kwargs: object) -> SimpleNamespace:
            raise subprocess.CalledProcessError(1, ("uv", "/abs/tmp/locked-requirements.txt"))

        monkeypatch.setattr(script.shutil, "which", _fake_which_uv)
        monkeypatch.setattr(script.subprocess, "run", boom)

        assert script.main([]) == 1
        err = capsys.readouterr().err
        assert "exited with 1" in err
        assert "/abs/tmp" not in err


class TestFrontendWrapperScript:
    def test_retry_runs_on_the_unchanged_lockfile(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        script = _load_script("verify_frontend_dependencies")
        monkeypatch.setattr(script.shutil, "which", _fake_which_npm)
        scan_cwds: list[Path] = []
        clean = json.dumps(
            {
                "vulnerabilities": {},
                "metadata": {
                    "vulnerabilities": {
                        "info": 0,
                        "low": 0,
                        "moderate": 0,
                        "high": 0,
                        "critical": 0,
                        "total": 0,
                    }
                },
            }
        )
        transport = json.dumps({"error": {"code": "EAI_AGAIN", "summary": "getaddrinfo EAI_AGAIN"}})

        def fake_run(command: tuple[str, ...], **kwargs: object) -> SimpleNamespace:
            scan_cwds.append(Path(str(kwargs["cwd"])))
            if len(scan_cwds) == 1:
                return _completed(1, stdout=transport)
            return _completed(0, stdout=clean)

        monkeypatch.setattr(script.subprocess, "run", fake_run)

        script.verify_frontend_dependencies(PROJECT_ROOT)

        assert scan_cwds == [PROJECT_ROOT / "web", PROJECT_ROOT / "web"]
        assert "unchanged-lockfile retry" in capsys.readouterr().out

    def test_threshold_findings_fail_without_a_retry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        script = _load_script("verify_frontend_dependencies")
        monkeypatch.setattr(script.shutil, "which", _fake_which_npm)
        scan_calls = 0
        findings = json.dumps(
            {
                "vulnerabilities": {
                    "synthetic-package": {
                        "name": "synthetic-package",
                        "severity": "high",
                        "via": ["GHSA-c4wx-2mqr-f35j"],
                    }
                },
                "metadata": {
                    "vulnerabilities": {
                        "info": 0,
                        "low": 0,
                        "moderate": 0,
                        "high": 1,
                        "critical": 0,
                        "total": 1,
                    }
                },
            }
        )

        def fake_run(command: tuple[str, ...], **kwargs: object) -> SimpleNamespace:
            nonlocal scan_calls
            scan_calls += 1
            return _completed(1, stdout=findings)

        monkeypatch.setattr(script.subprocess, "run", fake_run)

        with pytest.raises(RuntimeError, match="high severity threshold"):
            script.verify_frontend_dependencies(PROJECT_ROOT)
        assert scan_calls == 1

    def test_below_threshold_findings_report_and_pass(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Below-threshold findings pass the recorded policy with a full report."""
        script = _load_script("verify_frontend_dependencies")
        monkeypatch.setattr(script.shutil, "which", _fake_which_npm)
        scan_calls = 0
        findings = json.dumps(
            {
                "vulnerabilities": {
                    "synthetic-package": {
                        "name": "synthetic-package",
                        "severity": "low",
                        "via": ["GHSA-c4wx-2mqr-f35j"],
                    }
                },
                "metadata": {
                    "vulnerabilities": {
                        "info": 0,
                        "low": 1,
                        "moderate": 0,
                        "high": 0,
                        "critical": 0,
                        "total": 1,
                    }
                },
            }
        )

        def fake_run(command: tuple[str, ...], **kwargs: object) -> SimpleNamespace:
            nonlocal scan_calls
            scan_calls += 1
            return _completed(1, stdout=findings)

        monkeypatch.setattr(script.subprocess, "run", fake_run)

        script.verify_frontend_dependencies(PROJECT_ROOT)

        output = capsys.readouterr().out
        assert scan_calls == 1
        assert "BELOW-THRESHOLD" in output
        assert "below-threshold findings" in output


class TestDependencySurfaces:
    def test_no_container_or_deployment_inputs_are_tracked(self) -> None:
        """The container/deployment audit surface is provably absent."""
        listed = subprocess.run(
            ["git", "ls-files"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.splitlines()
        pattern = re.compile(
            r"(^|/)(dockerfile[^/]*|[^/]*\.dockerfile|docker-compose\.ya?ml"
            r"|kustomization\.ya?ml|chart\.ya?ml|[^/]*\.tf)$",
            re.IGNORECASE,
        )
        offenders = [name for name in listed if pattern.search(name)]
        assert offenders == []
