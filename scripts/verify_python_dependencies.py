"""Audit the exact locked Python dependency set without the local project."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path


def verify_dependencies(checkout: Path) -> None:
    """Export all locked groups and audit the resulting requirements file."""
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("uv executable was not found")
    with tempfile.TemporaryDirectory(prefix="paritygrid-audit-") as temporary_directory:
        requirements = Path(temporary_directory) / "locked-requirements.txt"
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
        subprocess.run(
            (
                sys.executable,
                "-m",
                "pip_audit",
                "--strict",
                "--disable-pip",
                "--require-hashes",
                "--requirement",
                str(requirements),
            ),
            cwd=checkout,
            check=True,
        )
    print("Locked Python dependency audit passed.")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the locked dependency audit."""
    del argv
    try:
        verify_dependencies(Path(__file__).resolve().parents[1])
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"Locked Python dependency audit failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
