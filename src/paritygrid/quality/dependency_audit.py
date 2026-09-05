"""Dependency-audit classification with bounded transport retry (P22.2).

A dependency scanner can fail in two very different ways: it can report a
vulnerability, or it can fail to reach the advisory registry at all.  The
Phase 21 acceptance needed repeated frontend audit runs solely because the
advisory endpoint was unreachable, and the distinction had to be argued by
hand from raw output.  This module owns that distinction mechanically so
no future run can silently relabel one as the other.

Classification rules:

- ``CLEAN`` and ``FINDINGS`` are decided from the scanner's structured
  output alone, never from the exit code.  pip-audit exits 1 both for
  findings and for fatal errors, and npm's exit code depends on the
  configured audit level, so an exit code alone is never evidence.
- ``TRANSPORT_FAILURE`` requires positive proof of a network-class
  failure in the scanner output.  Only this outcome may be retried, on
  the unchanged scanner input, a bounded number of times, with every
  attempt retained as evidence.
- ``CONFIGURATION_FAILURE`` covers deterministic bad inputs such as an
  unreadable lock export or a strict dependency-collection failure.
- ``UNCLASSIFIED`` is the fail-closed bucket: output that matches no
  known shape is never treated as clean and is never retried.

The transport evidence is pinned to the scanner versions resolved by the
repository lockfile (pip-audit 2.9 through <3, npm 11) and was validated
against real induced registry failures on an unreachable loopback
registry/proxy.  Upgrading either scanner requires re-validating the
marker fixtures in ``tests/fixtures/dependency_audit/``.
"""

from __future__ import annotations

import datetime
import json
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, cast

MAX_DETAIL_LENGTH = 512
MAX_RECORDED_FINDINGS = 200
MAX_SUPPRESSIONS = 100

NPM_AUDIT_SEVERITY_THRESHOLD = "high"
_SEVERITY_ORDER = ("info", "low", "moderate", "high", "critical")

# Transport evidence, validated against real induced registry failures
# (an unreachable loopback registry/proxy; npm 11.7 and pip-audit 2.10.1):
# npm reports an error object whose ``message`` carries the resolver or
# HTTP-status reason ("request to <url> failed, reason: connect
# ECONNREFUSED ..."), while pip-audit either logs its fatal message or
# crashes with the raw network traceback of the underlying HTTP stack.
_NPM_TRANSPORT_CODE = (
    r"E(?:AI_AGAIN|NOTFOUND|TIMEDOUT|CONNREFUSED|CONNRESET|PROTO|HOSTUNREACH|NETUNREACH|429|5\d\d)"
)
# npm maps non-2xx registry responses to ``E<status>`` codes.  Rate
# limiting (429) and registry-side 5xx outages are service/transport
# conditions, not audit findings, so they share the bounded retry with
# the resolver-level failures.  Any other ``E<status>`` code (a 4xx
# client error) stays a non-retryable configuration failure.
_NPM_TRANSPORT_CODE_PATTERN = re.compile(rf"\b{_NPM_TRANSPORT_CODE}\b")
# pip-audit fatal transport output, pinned to the locked 2.9..<3 line:
# the logged fatal messages and the exception classes that appear only in
# the network stack traces of a failed advisory-service fetch.
_PIP_AUDIT_TRANSPORT_MARKERS = (
    "could not connect to pypi's vulnerability feed",
    "pypi is not redirecting properly",
    "your network may be blocking this service",
    "requests.exceptions.proxyerror",
    "requests.exceptions.connectionerror",
    "requests.exceptions.connecttimeout",
    "requests.exceptions.readtimeout",
    "urllib3.exceptions.maxretryerror",
    "urllib3.exceptions.newconnectionerror",
    "urllib3.exceptions.protocolerror",
    "connectionrefusederror",
    "connectionreseterror",
    "socket.gaierror",
    "getaddrinfo failed",
    "temporary failure in name resolution",
)

_SUPPRESSION_IDENTIFIER_PATTERN = re.compile(
    r"^(PYSEC-\d{4}-\d{4,}|GHSA-[23456789cfghjmpqrvwx]{4}-[23456789cfghjmpqrvwx]{4}-[23456789cfghjmpqrvwx]{4}|CVE-\d{4}-\d{4,})$"
)
_SUPPRESSION_ECOSYSTEMS = frozenset({"pypi", "npm"})


class AuditOutcome(StrEnum):
    """Closed classification of one scanner attempt."""

    CLEAN = "clean"
    FINDINGS = "findings"
    TRANSPORT_FAILURE = "transport_failure"
    CONFIGURATION_FAILURE = "configuration_failure"
    UNCLASSIFIED = "unclassified"


#: Only a proven scanner/registry transport failure may ever be retried.
RETRYABLE_OUTCOMES = frozenset({AuditOutcome.TRANSPORT_FAILURE})


@dataclass(frozen=True)
class ScanResult:
    """Raw result of one scanner invocation."""

    tool: str
    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class VulnerabilityFinding:
    """One bounded, identified vulnerability reported by a scanner."""

    package: str
    identifier: str
    severity: str


@dataclass(frozen=True)
class ScanVerdict:
    """Classification of one scanner attempt with bounded evidence."""

    outcome: AuditOutcome
    detail: str
    findings: tuple[VulnerabilityFinding, ...] = ()
    severity_counts: tuple[tuple[str, int], ...] = ()

    @property
    def findings_truncated(self) -> bool:
        """Whether the recorded finding list hit the evidence bound."""
        return len(self.findings) >= MAX_RECORDED_FINDINGS


class ScannerError(ValueError):
    """A scanner result violates the classifier's input contract."""


def _bounded(text: str) -> str:
    return text if len(text) <= MAX_DETAIL_LENGTH else text[:MAX_DETAIL_LENGTH]


def classify_scan(result: ScanResult) -> ScanVerdict:
    """Classify one scanner result; unknown shapes fail closed."""
    if result.tool == "pip-audit":
        return _classify_pip_audit(result)
    if result.tool == "npm-audit":
        return _classify_npm_audit(result)
    raise ScannerError(f"unknown scanner tool: {result.tool!r}")


def _classify_pip_audit(result: ScanResult) -> ScanVerdict:
    document: object
    try:
        document = json.loads(result.stdout)
    except json.JSONDecodeError:
        lowered = result.stderr.lower()
        marker = next((m for m in _PIP_AUDIT_TRANSPORT_MARKERS if m in lowered), None)
        if marker is not None:
            return ScanVerdict(
                outcome=AuditOutcome.TRANSPORT_FAILURE,
                detail=_bounded(f"pip-audit transport failure marker: {marker}"),
            )
        return ScanVerdict(
            outcome=AuditOutcome.UNCLASSIFIED,
            detail=_bounded("pip-audit produced no JSON report and no known transport marker"),
        )
    if not isinstance(document, dict):
        return ScanVerdict(
            outcome=AuditOutcome.CONFIGURATION_FAILURE,
            detail="pip-audit JSON report is not an object",
        )
    report = cast("dict[str, object]", document)
    dependencies = report.get("dependencies")
    if not isinstance(dependencies, list):
        return ScanVerdict(
            outcome=AuditOutcome.CONFIGURATION_FAILURE,
            detail="pip-audit JSON report lacks the dependencies list",
        )
    findings: list[VulnerabilityFinding] = []
    for entry in cast("list[object]", dependencies):
        if not isinstance(entry, dict):
            return ScanVerdict(
                outcome=AuditOutcome.CONFIGURATION_FAILURE,
                detail="pip-audit JSON report contains a non-object entry",
            )
        record = cast("dict[str, object]", entry)
        package = record.get("name")
        version = record.get("version")
        if "vulns" not in record:
            return ScanVerdict(
                outcome=AuditOutcome.CONFIGURATION_FAILURE,
                detail="pip-audit JSON entry lacks the vulns field",
            )
        vulns = record["vulns"]
        if not isinstance(package, str) or not isinstance(version, str):
            return ScanVerdict(
                outcome=AuditOutcome.CONFIGURATION_FAILURE,
                detail="pip-audit JSON entry lacks package name or version",
            )
        if not isinstance(vulns, list):
            return ScanVerdict(
                outcome=AuditOutcome.CONFIGURATION_FAILURE,
                detail="pip-audit JSON entry vulns field is not a list",
            )
        for vuln in cast("list[object]", vulns):
            identifier = (
                cast("dict[str, object]", vuln).get("id") if isinstance(vuln, dict) else None
            )
            if not isinstance(identifier, str) or not identifier:
                return ScanVerdict(
                    outcome=AuditOutcome.CONFIGURATION_FAILURE,
                    detail="pip-audit vulnerability lacks an identifier",
                )
            findings.append(
                VulnerabilityFinding(
                    package=f"{package}=={version}",
                    identifier=identifier,
                    severity="unspecified",
                )
            )
    findings = findings[:MAX_RECORDED_FINDINGS]
    if findings:
        return ScanVerdict(
            outcome=AuditOutcome.FINDINGS,
            detail=_bounded(f"pip-audit reported {len(findings)} vulnerabilities"),
            findings=tuple(findings),
        )
    return ScanVerdict(
        outcome=AuditOutcome.CLEAN,
        detail="pip-audit JSON report contains no vulnerabilities",
    )


def _classify_npm_audit(result: ScanResult) -> ScanVerdict:
    document: object
    try:
        document = json.loads(result.stdout)
    except json.JSONDecodeError:
        return ScanVerdict(
            outcome=AuditOutcome.UNCLASSIFIED,
            detail="npm audit output is not a JSON document",
        )
    if not isinstance(document, dict):
        return ScanVerdict(
            outcome=AuditOutcome.UNCLASSIFIED,
            detail="npm audit JSON document is not an object",
        )
    report = cast("dict[str, object]", document)
    error = report.get("error")
    if error is not None:
        if not isinstance(error, dict):
            return ScanVerdict(
                outcome=AuditOutcome.UNCLASSIFIED,
                detail="npm audit error field is not an object",
            )
        error_record = cast("dict[str, object]", error)
        message = report.get("message")
        evidence = " ".join(
            part
            for part in (message, error_record.get("summary"), error_record.get("detail"))
            if isinstance(part, str)
        )
        reason = _NPM_TRANSPORT_CODE_PATTERN.search(evidence)
        if reason is not None:
            return ScanVerdict(
                outcome=AuditOutcome.TRANSPORT_FAILURE,
                detail=_bounded(f"npm audit transport error code: {reason.group(0)}"),
            )
        code = error_record.get("code")
        if isinstance(code, str):
            if _NPM_TRANSPORT_CODE_PATTERN.fullmatch(code) is not None:
                return ScanVerdict(
                    outcome=AuditOutcome.TRANSPORT_FAILURE,
                    detail=_bounded(f"npm audit transport error code: {code}"),
                )
            return ScanVerdict(
                outcome=AuditOutcome.CONFIGURATION_FAILURE,
                detail=_bounded(f"npm audit error code: {code}"),
            )
        return ScanVerdict(
            outcome=AuditOutcome.UNCLASSIFIED,
            detail="npm audit reported an error without a transport marker",
        )
    metadata = report.get("metadata")
    vulnerabilities: object = (
        cast("dict[str, object]", metadata).get("vulnerabilities")
        if isinstance(metadata, dict)
        else None
    )
    if not isinstance(vulnerabilities, dict):
        return ScanVerdict(
            outcome=AuditOutcome.UNCLASSIFIED,
            detail="npm audit JSON document lacks vulnerability metadata",
        )
    counts_map = cast("dict[str, object]", vulnerabilities)
    counts: list[tuple[str, int]] = []
    total = 0
    reported_total: int | None = None
    for severity, value in counts_map.items():
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return ScanVerdict(
                outcome=AuditOutcome.UNCLASSIFIED,
                detail=f"npm audit severity count for {severity!r} is not a count",
            )
        if severity == "total":
            reported_total = value
            continue
        if severity not in _SEVERITY_ORDER:
            return ScanVerdict(
                outcome=AuditOutcome.UNCLASSIFIED,
                detail=f"npm audit metadata carries unknown severity {severity!r}",
            )
        counts.append((severity, value))
        total += value
    if len(counts) != len(_SEVERITY_ORDER):
        return ScanVerdict(
            outcome=AuditOutcome.UNCLASSIFIED,
            detail="npm audit metadata lacks one or more severity counts",
        )
    if reported_total is not None and reported_total != total:
        return ScanVerdict(
            outcome=AuditOutcome.UNCLASSIFIED,
            detail="npm audit metadata total contradicts the severity counts",
        )
    vulns_map = report.get("vulnerabilities")
    if total == 0:
        if isinstance(vulns_map, dict) and vulns_map:
            return ScanVerdict(
                outcome=AuditOutcome.UNCLASSIFIED,
                detail="npm audit reports zero counts beside a non-empty vulnerabilities map",
            )
        return ScanVerdict(
            outcome=AuditOutcome.CLEAN,
            detail="npm audit reported zero vulnerabilities",
            severity_counts=tuple(counts),
        )
    if not isinstance(vulns_map, dict) or not vulns_map:
        return ScanVerdict(
            outcome=AuditOutcome.UNCLASSIFIED,
            detail="npm audit reports findings without a vulnerabilities map",
        )
    findings = _npm_findings(report)
    threshold = NPM_AUDIT_SEVERITY_THRESHOLD
    threshold_index = _SEVERITY_ORDER.index(threshold)
    at_or_above = sum(
        count for severity, count in counts if _SEVERITY_ORDER.index(severity) >= threshold_index
    )
    return ScanVerdict(
        outcome=AuditOutcome.FINDINGS,
        detail=_bounded(
            f"npm audit reported {total} vulnerabilities, "
            f"{at_or_above} at or above the {threshold} threshold"
        ),
        findings=findings,
        severity_counts=tuple(counts),
    )


def _npm_findings(report: dict[str, object]) -> tuple[VulnerabilityFinding, ...]:
    """Extract bounded per-package findings from the vulnerabilities map."""
    vulnerabilities = report.get("vulnerabilities")
    if not isinstance(vulnerabilities, dict):
        return ()
    findings: list[VulnerabilityFinding] = []
    for package, entry in cast("dict[str, object]", vulnerabilities).items():
        if not isinstance(entry, dict):
            continue
        record = cast("dict[str, object]", entry)
        severity = record.get("severity")
        via = record.get("via")
        identifiers = [
            source
            for source in (cast("list[object]", via) if isinstance(via, list) else [])
            if isinstance(source, str)
        ]
        if not identifiers:
            identifiers = ["unknown-source"]
        for identifier in identifiers[:4]:
            findings.append(
                VulnerabilityFinding(
                    package=package,
                    identifier=identifier,
                    severity=severity if isinstance(severity, str) else "unspecified",
                )
            )
            if len(findings) >= MAX_RECORDED_FINDINGS:
                return tuple(findings)
    return tuple(findings)


def severity_at_or_above(severity: str) -> bool:
    """Apply the recorded severity policy: fail on ``high`` and above."""
    if severity not in _SEVERITY_ORDER:
        return True
    return _SEVERITY_ORDER.index(severity) >= _SEVERITY_ORDER.index(NPM_AUDIT_SEVERITY_THRESHOLD)


def npm_threshold_breached(verdict: ScanVerdict) -> bool:
    """Whether an npm verdict reports any finding at or above the threshold.

    The decision reads the complete severity counts from the audit
    metadata, never the per-finding list: the recorded finding list is
    evidence-bounded, so a decision taken from it could miss severe
    entries beyond the bound.
    """
    if verdict.outcome is not AuditOutcome.FINDINGS:
        return False
    return any(
        severity_at_or_above(severity) and count > 0 for severity, count in verdict.severity_counts
    )


class ScannerRunner(Protocol):
    """Boundary that executes one scanner invocation."""

    def __call__(self) -> ScanResult:
        """Run the scanner once and return its raw result."""
        ...


@dataclass(frozen=True)
class AuditAttempt:
    """One retained scanner attempt with its classification."""

    attempt: int
    result: ScanResult
    verdict: ScanVerdict


@dataclass(frozen=True)
class AuditOutcomeWithAttempts:
    """Final outcome plus the complete, retained attempt evidence."""

    outcome: AuditOutcome
    attempts: tuple[AuditAttempt, ...]

    @property
    def retried(self) -> bool:
        return len(self.attempts) > 1


def run_with_transport_retry(
    runner: ScannerRunner,
    *,
    max_attempts: int = 2,
) -> AuditOutcomeWithAttempts:
    """Run the scanner, retrying only proven transport failures.

    The scanner input never changes between attempts: ``runner`` is a
    fixed zero-argument closure over one unchanged invocation.  A
    vulnerability finding, a configuration failure, or an unclassified
    result stops the run immediately; a transport failure is retried up
    to ``max_attempts`` total attempts and, if it persists, stays a
    transport failure — it never becomes clean and never becomes a
    finding.
    """
    if max_attempts < 1:
        raise ScannerError("max_attempts must be at least one")
    attempts: list[AuditAttempt] = []
    verdict: ScanVerdict | None = None
    for attempt in range(1, max_attempts + 1):
        result = runner()
        verdict = classify_scan(result)
        attempts.append(AuditAttempt(attempt=attempt, result=result, verdict=verdict))
        if verdict.outcome not in RETRYABLE_OUTCOMES:
            break
    assert verdict is not None
    return AuditOutcomeWithAttempts(outcome=verdict.outcome, attempts=tuple(attempts))


@dataclass(frozen=True)
class VulnerabilitySuppression:
    """One narrow, expiring, owned suppression of a single vulnerability.

    A suppression is valid only for one exact (ecosystem, package,
    identifier) triple.  Wildcards, empty fields, and malformed
    identifiers are rejected at construction, so no stored suppression
    can be broader than its named finding.
    """

    ecosystem: str
    package: str
    identifier: str
    reason: str
    owner: str
    approval_authority: str
    expires: str
    upstream_reference: str

    def __post_init__(self) -> None:
        if self.ecosystem not in _SUPPRESSION_ECOSYSTEMS:
            raise ScannerError(
                f"suppression ecosystem must be one of {sorted(_SUPPRESSION_ECOSYSTEMS)}"
            )
        if not self.package or "*" in self.package or "?" in self.package:
            raise ScannerError("suppression package must be one exact package name")
        if not _SUPPRESSION_IDENTIFIER_PATTERN.match(self.identifier):
            raise ScannerError(
                "suppression identifier must be one exact PYSEC-, GHSA-, or CVE- identifier"
            )
        for field_name in ("reason", "owner", "approval_authority", "upstream_reference"):
            if not getattr(self, field_name):
                raise ScannerError(f"suppression {field_name} must be recorded")
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", self.expires):
            raise ScannerError("suppression expiry must be an ISO YYYY-MM-DD date")
        try:
            datetime.date.fromisoformat(self.expires)
        except ValueError as error:
            raise ScannerError("suppression expiry must be a real calendar date") from error

    def is_expired(self, today: str) -> bool:
        """Whether the suppression expiry has passed by the given date."""
        return self.expires < today

    def matches(self, finding: VulnerabilityFinding) -> bool:
        """Whether this suppression covers exactly the given finding.

        The npm classifier records the package as the lock name and the
        advisory identifier from the ``via`` list, so npm findings match
        by name and identifier; pip-audit findings record
        ``name==version`` and a PYSEC/CVE/GHSA identifier.  An npm
        ``via`` entry that is a source title rather than an advisory id
        never matches a stored suppression, because stored identifiers
        are validated advisory ids.
        """
        package_name = finding.package.split("==", 1)[0]
        return finding.identifier == self.identifier and package_name == self.package


def load_suppressions(entries: object) -> tuple[VulnerabilitySuppression, ...]:
    """Load suppressions from decoded JSON, failing closed on any defect."""
    if not isinstance(entries, list):
        raise ScannerError("suppressions document must be a list")
    document_entries = cast("list[object]", entries)
    if len(document_entries) > MAX_SUPPRESSIONS:
        raise ScannerError("suppressions document exceeds the recorded bound")
    required = (
        "ecosystem",
        "package",
        "identifier",
        "reason",
        "owner",
        "approval_authority",
        "expires",
        "upstream_reference",
    )
    suppressions: list[VulnerabilitySuppression] = []
    seen: set[tuple[str, str, str]] = set()
    for entry in document_entries:
        if not isinstance(entry, dict):
            raise ScannerError("suppression entry must be an object")
        record = cast("dict[str, object]", entry)
        missing = [
            field_name for field_name in required if not isinstance(record.get(field_name), str)
        ]
        if missing:
            raise ScannerError(f"suppression entry lacks required fields: {sorted(missing)}")
        values = {field_name: cast("str", record[field_name]) for field_name in required}
        suppression = VulnerabilitySuppression(
            ecosystem=values["ecosystem"],
            package=values["package"],
            identifier=values["identifier"],
            reason=values["reason"],
            owner=values["owner"],
            approval_authority=values["approval_authority"],
            expires=values["expires"],
            upstream_reference=values["upstream_reference"],
        )
        key = (suppression.ecosystem, suppression.package, suppression.identifier)
        if key in seen:
            raise ScannerError("duplicate suppression for the same finding")
        seen.add(key)
        suppressions.append(suppression)
    return tuple(suppressions)


@dataclass(frozen=True)
class SuppressionApplication:
    """Result of applying active suppressions to one verdict's findings."""

    remaining: tuple[VulnerabilityFinding, ...]
    applied: tuple[VulnerabilityFinding, ...]


def apply_suppressions(
    verdict: ScanVerdict,
    suppressions: tuple[VulnerabilitySuppression, ...],
    *,
    today: str,
) -> SuppressionApplication:
    """Mask findings covered by an active, exact suppression.

    An expired suppression never masks anything: a finding matched only
    by an expired suppression stays a finding.  A suppression whose
    identifier never occurs in the findings is recorded by the caller's
    review of the registry; it cannot widen coverage here because
    matching is exact.
    """
    remaining: list[VulnerabilityFinding] = []
    applied: list[VulnerabilityFinding] = []
    active = [s for s in suppressions if not s.is_expired(today)]
    for finding in verdict.findings:
        if any(s.matches(finding) for s in active):
            applied.append(finding)
        else:
            remaining.append(finding)
    return SuppressionApplication(remaining=tuple(remaining), applied=tuple(applied))
