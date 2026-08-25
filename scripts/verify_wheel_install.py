"""Build and verify an isolated wheel, CLI smoke, and spawned-process import."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path

_SPAWN_PROBE = """from __future__ import annotations

import multiprocessing
import sys
from pathlib import Path


def import_in_child(queue: object) -> None:
    import paritygrid

    queue.put(str(Path(paritygrid.__file__).resolve()))


def process_pool_probe() -> None:
    from datetime import UTC, datetime

    from paritygrid.adapters.runners import SubordinateProcessPool
    from paritygrid.application.execution.capacity import (
        ScheduledWorkLimiters,
        SubordinateCallLimiter,
    )
    from paritygrid.application.execution.clock_policy import ManualClock
    from paritygrid.application.execution.concurrency_settings import CapturedConcurrencySettings
    from paritygrid.domain.models import UtcTimestamp

    timestamp = UtcTimestamp(datetime(2026, 8, 25, 8, 0, 0, tzinfo=UTC))
    clock = ManualClock(timestamp)
    settings = CapturedConcurrencySettings(cpu_pool_operations=1)
    parent = ScheduledWorkLimiters(
        settings,
        strategy_id="threaded",
        node_ids=("wheel-probe",),
        clock=clock,
    )
    capacity = SubordinateCallLimiter(
        category="cpu_pool",
        limit=1,
        clock=clock,
        parent_limiter=parent,
    )
    owner = "wheel-process-probe"
    permits = parent.acquire(owner, "wheel-probe")
    pool = SubordinateProcessPool(capacity=capacity, timeout_seconds=10.0)
    try:
        result = pool.submit(
            owner,
            "sort_integers",
            {"values": [3, 1, 2]},
            parent=permits,
        )
        if result.operation_id != "sort_integers" or result.result != {"sorted": [1, 2, 3]}:
            raise SystemExit("installed process-pool probe returned an invalid result")
    finally:
        pool.close()
        parent.release(owner, "wheel-probe")


if __name__ == "__main__":
    checkout = Path(sys.argv[1]).resolve()
    environment = Path(sys.argv[2]).resolve()
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    process = context.Process(target=import_in_child, args=(queue,))
    process.start()
    process.join(30)
    if process.is_alive():
        process.terminate()
        process.join(5)
        raise SystemExit("spawned import probe timed out")
    if process.exitcode != 0:
        raise SystemExit(f"spawned import probe exited with {process.exitcode}")
    module_path = Path(queue.get(timeout=5)).resolve()
    if not module_path.is_relative_to(environment):
        raise SystemExit(f"spawned process imported outside venv: {module_path}")
    if module_path.is_relative_to(checkout):
        raise SystemExit(f"spawned process imported from checkout: {module_path}")
    process_pool_probe()
    print(f"Spawned-process import passed: {module_path}")
"""


def _run(
    arguments: Sequence[str | Path],
    *,
    cwd: Path,
    environment: Mapping[str, str] | None = None,
) -> None:
    command = tuple(str(argument) for argument in arguments)
    print(f"+ {' '.join(command)}")
    subprocess.run(command, cwd=cwd, env=environment, check=True)


def _isolated_environment() -> dict[str, str]:
    environment = dict(os.environ)
    for name in ("PYTHONHOME", "PYTHONPATH", "VIRTUAL_ENV"):
        environment.pop(name, None)
    environment.update(
        {
            "PYTHONIOENCODING": "utf-8",
            "PYTHONNOUSERSITE": "1",
            "PYTHONUTF8": "1",
        }
    )
    return environment


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
