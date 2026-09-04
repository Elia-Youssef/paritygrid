"""Build and verify an isolated wheel, CLI smoke, and spawned-process import."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path

from paritygrid.quality.platform_matrix import (
    FRONTEND_PROBE as _FRONTEND_PROBE,
)
from paritygrid.quality.platform_matrix import (
    SPAWN_PROBE as _SPAWN_PROBE,
)
from paritygrid.quality.platform_matrix import (
    isolated_environment as _isolated_environment,
)


def _run(
    arguments: Sequence[str | Path],
    *,
    cwd: Path,
    environment: Mapping[str, str] | None = None,
) -> None:
    command = tuple(str(argument) for argument in arguments)
    print(f"+ {' '.join(command)}")
    subprocess.run(command, cwd=cwd, env=environment, check=True)


def verify_wheel(checkout: Path) -> None:
    """Perform the complete isolated wheel verification."""
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("uv executable was not found")
    with tempfile.TemporaryDirectory(prefix="paritygrid-wheel-") as temporary_directory:
        temporary_root = Path(temporary_directory)
        distribution_root = temporary_root / "dist"
        environment_root = temporary_root / "venv"
        outside_checkout = temporary_root / "Café % عربي"
        outside_checkout.mkdir()
        requirements = temporary_root / "runtime-requirements.txt"

        _run((uv, "build", "--wheel", "--out-dir", distribution_root), cwd=checkout)
        wheels = tuple(distribution_root.glob("*.whl"))
        if len(wheels) != 1:
            raise RuntimeError(f"expected one wheel, found {len(wheels)}")
        _run(
            (
                uv,
                "export",
                "--locked",
                "--no-dev",
                "--no-emit-project",
                "--no-hashes",
                "--format",
                "requirements.txt",
                "--output-file",
                requirements,
            ),
            cwd=checkout,
        )
        _run((uv, "venv", "--python", sys.executable, environment_root), cwd=outside_checkout)
        environment_python = environment_root / (
            "Scripts/python.exe" if os.name == "nt" else "bin/python"
        )
        _run(
            (uv, "pip", "sync", "--python", environment_python, requirements), cwd=outside_checkout
        )
        _run(
            (uv, "pip", "install", "--python", environment_python, "--no-deps", wheels[0]),
            cwd=outside_checkout,
        )

        isolated_environment = _isolated_environment()
        import_probe = (
            "from pathlib import Path; import paritygrid, sys; "
            "module=Path(paritygrid.__file__).resolve(); "
            "checkout=Path(sys.argv[1]).resolve(); environment=Path(sys.argv[2]).resolve(); "
            "assert module.is_relative_to(environment), module; "
            "assert not module.is_relative_to(checkout), module; print(module)"
        )
        _run(
            (environment_python, "-I", "-c", import_probe, checkout, environment_root),
            cwd=outside_checkout,
            environment=isolated_environment,
        )
        executable = environment_root / (
            "Scripts/paritygrid.exe" if os.name == "nt" else "bin/paritygrid"
        )
        _run((executable, "smoke"), cwd=outside_checkout, environment=isolated_environment)

        frontend_probe = temporary_root / "frontend_probe.py"
        frontend_probe.write_text(_FRONTEND_PROBE, encoding="utf-8")
        _run(
            (environment_python, "-I", frontend_probe),
            cwd=outside_checkout,
            environment=isolated_environment,
        )

        spawn_probe = temporary_root / "spawn_probe.py"
        spawn_probe.write_text(_SPAWN_PROBE, encoding="utf-8")
        _run(
            (environment_python, "-I", spawn_probe, checkout, environment_root),
            cwd=outside_checkout,
            environment=isolated_environment,
        )
    print("Isolated wheel verification passed.")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the isolated wheel gate."""
    del argv
    try:
        verify_wheel(Path(__file__).resolve().parents[1])
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"Isolated wheel verification failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
