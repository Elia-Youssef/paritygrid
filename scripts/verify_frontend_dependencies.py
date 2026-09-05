"""Audit the locked frontend dependency set with npm audit.

Runs ``npm audit --json --audit-level=high`` over the committed
``web/package-lock.json`` and classifies the structured output — never
the exit code, which depends on the configured audit level — through
``paritygrid.quality.dependency_audit``.  A proven registry transport
failure is retried once on the unchanged lockfile; a real vulnerability
finding is never retried, suppressed, or relabeled.  The recorded
severity policy fails the audit on any finding at or above ``high``.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

from paritygrid.quality.dependency_audit import (
    AuditOutcome,
    ScanResult,
    npm_threshold_breached,
    run_with_transport_retry,
    severity_at_or_above,
)

MAX_ATTEMPTS = 2
_WEB_DIRECTORY = "web"


def _npm_executable() -> str:
    npm = shutil.which("npm")
    if npm is None:
        raise RuntimeError("npm executable was not found")
    return npm


def _scan_result(checkout: Path) -> ScanResult:
    completed = subprocess.run(
        (
            _npm_executable(),
            "audit",
            "--json",
            "--audit-level=high",
        ),
        cwd=checkout / _WEB_DIRECTORY,
        capture_output=True,
        text=True,
        check=False,
    )
    return ScanResult(
        tool="npm-audit",
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def verify_frontend_dependencies(checkout: Path) -> None:
    """Audit the committed frontend lockfile with classified outcomes."""
    outcome_with_attempts = run_with_transport_retry(
        lambda: _scan_result(checkout),
        max_attempts=MAX_ATTEMPTS,
    )
    final = outcome_with_attempts.attempts[-1].verdict
    if outcome_with_attempts.outcome is AuditOutcome.FINDINGS:
        threshold_findings = [
            finding for finding in final.findings if severity_at_or_above(finding.severity)
        ]
        for finding in final.findings:
            marker = "VULNERABILITY" if finding in threshold_findings else "BELOW-THRESHOLD"
            print(f"{marker} {finding.package} {finding.identifier} ({finding.severity})")
        if final.findings_truncated:
            print(
                "Note: the recorded finding list is truncated; the severity decision "
                "below uses the complete audit metadata counts.",
                file=sys.stderr,
            )
        # The severity decision reads the complete metadata counts, not the
        # evidence-bounded finding list.
        if npm_threshold_breached(final):
            raise RuntimeError(
                "frontend dependency audit found vulnerabilities at or above the "
                "high severity threshold"
            )
        print(
            "Frontend dependency audit reported only below-threshold findings; "
            "the recorded high-severity policy passes with the report above."
        )
        return
    if outcome_with_attempts.outcome is not AuditOutcome.CLEAN:
        for attempt in outcome_with_attempts.attempts:
            print(
                f"attempt {attempt.attempt}: outcome={attempt.verdict.outcome.value} "
                f"detail={attempt.verdict.detail}",
                file=sys.stderr,
            )
        raise RuntimeError(
            f"frontend dependency audit ended in outcome {outcome_with_attempts.outcome.value}"
        )
    if outcome_with_attempts.retried:
        first = outcome_with_attempts.attempts[0].verdict
        print(
            f"First attempt ended in a proven transport failure "
            f"({first.detail}); the unchanged-lockfile retry completed the audit."
        )
    print("Frontend dependency audit passed.")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the frontend dependency audit."""
    del argv
    try:
        verify_frontend_dependencies(Path(__file__).resolve().parents[1])
    except (OSError, RuntimeError, ValueError) as error:
        print(f"Frontend dependency audit failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
