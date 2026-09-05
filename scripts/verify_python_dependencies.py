"""Audit the exact locked Python dependency set without the local project.

The audit exports every locked dependency group to a hash-pinned
requirements file and runs pip-audit over that exact set in strict mode.
The scanner's structured JSON output — never its exit code, which
pip-audit uses both for findings and for fatal errors — is classified by
``paritygrid.quality.dependency_audit`` so a vulnerability finding can
never be mistaken for a registry transport failure.  Only a proven
transport failure is retried, once, on the unchanged export; every
attempt is retained in the failure output.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path

from paritygrid.quality.dependency_audit import (
    AuditOutcome,
    ScanResult,
    run_with_transport_retry,
)

MAX_ATTEMPTS = 2


def _export_locked_requirements(checkout: Path, requirements: Path) -> None:
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("uv executable was not found")
    subprocess.run(
        (
            uv,
            "export",
            "--locked",
            "--all-groups",
            "--no-emit-project",
            "--format",
            "requirements.txt",
            "--output-file",
            str(requirements),
        ),
        cwd=checkout,
        stdout=subprocess.DEVNULL,
        check=True,
    )


def _scan_result(checkout: Path, requirements: Path) -> ScanResult:
    completed = subprocess.run(
        (
            sys.executable,
            "-m",
            "pip_audit",
            "--strict",
            "--disable-pip",
            "--format",
            "json",
            "--require-hashes",
            "--requirement",
            str(requirements),
        ),
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )
    return ScanResult(
        tool="pip-audit",
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def verify_dependencies(checkout: Path) -> None:
    """Export all locked groups and audit the resulting requirements file."""
    with tempfile.TemporaryDirectory(prefix="paritygrid-audit-") as temporary_directory:
        requirements = Path(temporary_directory) / "locked-requirements.txt"
        _export_locked_requirements(checkout, requirements)
        outcome_with_attempts = run_with_transport_retry(
            lambda: _scan_result(checkout, requirements),
            max_attempts=MAX_ATTEMPTS,
        )
    if outcome_with_attempts.outcome is not AuditOutcome.CLEAN:
        for attempt in outcome_with_attempts.attempts:
            print(
                f"attempt {attempt.attempt}: outcome={attempt.verdict.outcome.value} "
                f"detail={attempt.verdict.detail}",
                file=sys.stderr,
            )
        for finding in outcome_with_attempts.attempts[-1].verdict.findings:
            print(f"VULNERABILITY {finding.package} {finding.identifier}", file=sys.stderr)
        raise RuntimeError(
            f"locked Python dependency audit ended in outcome {outcome_with_attempts.outcome.value}"
        )
    if outcome_with_attempts.retried:
        first = outcome_with_attempts.attempts[0].verdict
        print(
            f"First attempt ended in a proven transport failure "
            f"({first.detail}); the unchanged-input retry completed the audit."
        )
    print("Locked Python dependency audit passed.")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the locked dependency audit."""
    del argv
    try:
        verify_dependencies(Path(__file__).resolve().parents[1])
    except subprocess.CalledProcessError as error:
        # The exception text embeds the full command line, including the
        # temporary export path; record only the exit status.
        print(
            "Locked Python dependency audit failed: locked requirements export "
            f"exited with {error.returncode}",
            file=sys.stderr,
        )
        return 1
    except (OSError, RuntimeError, ValueError) as error:
        print(f"Locked Python dependency audit failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
