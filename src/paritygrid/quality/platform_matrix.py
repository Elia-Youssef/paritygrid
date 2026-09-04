"""Windows and Linux verification matrix over an isolated installed wheel.

The matrix builds the wheel, installs it with locked runtime dependencies
into a fresh virtual environment outside the checkout, and verifies the
installed package from clean bounded temporary roots: import resolution,
CLI startup and help, subprocess spawn semantics, packaged production
frontend assets, backend smoke, the three canonical headless demo
profiles, the cross-runner comparison, subordinate process-pool behavior,
and bounded startup/cleanup with orphan detection over OS-visible
children.

Windows-specific coverage exercises valid 8.3-style path components
(such as ``RUNNER~1``), paths containing spaces and Unicode, and junction
safety without rejecting ordinary path normalization.  Linux-specific
coverage exercises path and permission behavior.  A capability the
current platform cannot provide is reported as unavailable with a
bounded reason in the summary instead of failing the matrix.

``scripts/verify_platform_matrix.py`` is the command-line wrapper for
this module; ``scripts/verify_wheel_install.py`` reuses the shared
spawn and frontend probes.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

_CHECKOUT = Path(__file__).resolve().parents[3]

SPAWN_PROBE = """from __future__ import annotations

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

FRONTEND_PROBE = """from __future__ import annotations

import asyncio
from pathlib import Path

import paritygrid
from paritygrid.runtime.composition import create_runtime_app


async def request_shell() -> None:
    app = create_runtime_app()
    messages: list[dict[str, object]] = []
    delivered = False

    async def receive() -> dict[str, object]:
        nonlocal delivered
        if delivered:
            return {"type": "http.disconnect"}
        delivered = True
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, object]) -> None:
        messages.append(message)

    await app(
        {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/",
            "raw_path": b"/",
            "query_string": b"",
            "root_path": "",
            "headers": [(b"host", b"127.0.0.1")],
            "client": ("127.0.0.1", 12345),
            "server": ("127.0.0.1", 8000),
        },
        receive,
        send,
    )
    start = next(message for message in messages if message["type"] == "http.response.start")
    assert start["status"] == 200, start
    body = b"".join(
        message.get("body", b"")
        for message in messages
        if message["type"] == "http.response.body"
    )
    assert b"<!doctype html" in body.lower()
    package_root = Path(paritygrid.__file__).resolve().parent
    assert (package_root / "_frontend" / "index.html").is_file()


asyncio.run(request_shell())
"""


def isolated_environment() -> dict[str, str]:
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


_DEMO_TIMEOUT_SECONDS = 600
_SMOKE_TIMEOUT_SECONDS = 180
_HELP_TIMEOUT_SECONDS = 60
_BUILD_TIMEOUT_SECONDS = 600
_SYNC_TIMEOUT_SECONDS = 900
_UNICODE_CWD_NAME = "Parité Grid vérificatioN 网格"
_TILDE_DIR_NAME = "RUNNE~1.mat"
_SPACES_DIR_NAME = "Parity Grid Matrix"
_SUMMARY_VERSION = 1


class MatrixStepError(RuntimeError):
    """One matrix step failed; the message is safe for the summary."""


class MatrixUsageError(RuntimeError):
    """The matrix cannot run on this platform or invocation."""


def _run(
    arguments: Sequence[str | Path],
    *,
    cwd: Path,
    environment: Mapping[str, str] | None = None,
    timeout_seconds: float = _SMOKE_TIMEOUT_SECONDS,
) -> str:
    command = tuple(str(argument) for argument in arguments)
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout_seconds,
    )
    if completed.returncode != 0:
        diagnostic = (completed.stderr or completed.stdout or "").strip().splitlines()
        tail = diagnostic[-1] if diagnostic else f"exit code {completed.returncode}"
        raise MatrixStepError(f"{' '.join(command[:3])}... failed: {tail[:200]}")
    return completed.stdout


def _venv_python(venv_root: Path) -> Path:
    return venv_root / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def _venv_paritygrid(venv_root: Path) -> Path:
    return venv_root / ("Scripts/paritygrid.exe" if os.name == "nt" else "bin/paritygrid")


def windows_short_path(path: Path) -> Path | None:
    """Return the 8.3 short path for one directory, or None when unavailable."""
    if os.name != "nt":
        return None
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.windll.kernel32
    kernel32.GetShortPathNameW.argtypes = (wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD)
    kernel32.GetShortPathNameW.restype = wintypes.DWORD
    buffer = ctypes.create_unicode_buffer(1024)
    length = kernel32.GetShortPathNameW(str(path), buffer, 1024)
    if length == 0:
        return None
    return Path(buffer.value)


def _windows_create_junction(link: Path, target: Path) -> bool:
    """Create one junction through the CPython Windows helper, or mklink."""
    if os.name != "nt":
        return False
    try:
        import _winapi

        _winapi.CreateJunction(str(target), str(link))
        return True
    except OSError:
        completed = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(target)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        return completed.returncode == 0


def _venv_child_processes(venv_root: Path) -> list[int] | None:
    """List OS-visible processes whose executable lives in the venv.

    Returns None when the platform cannot enumerate processes safely.
    """
    resolved = str(venv_root.resolve()).lower()
    if os.name == "nt":
        return _venv_children_windows(resolved)
    if os.name == "posix":
        return _venv_children_posix(resolved)
    return None


def _path_is_within(candidate: str, root: str) -> bool:
    """Match executable containment by path components, never string prefix."""
    try:
        normalized_candidate = os.path.normcase(os.path.abspath(candidate))
        normalized_root = os.path.normcase(os.path.abspath(root))
        return os.path.commonpath((normalized_candidate, normalized_root)) == normalized_root
    except ValueError:
        return False


def _venv_children_windows(resolved_venv: str) -> list[int] | None:
    import ctypes
    from ctypes import wintypes

    class _ProcessEntry(ctypes.Structure):
        _fields_ = [
            ("dwSize", wintypes.DWORD),
            ("cntUsage", wintypes.DWORD),
            ("th32ProcessID", wintypes.DWORD),
            ("th32DefaultHeapID", ctypes.c_size_t),
            ("th32ModuleID", wintypes.DWORD),
            ("cntThreads", wintypes.DWORD),
            ("th32ParentProcessID", wintypes.DWORD),
            ("pcPriClassBase", wintypes.LONG),
            ("dwFlags", wintypes.DWORD),
            ("szExeFile", wintypes.WCHAR * 260),
        ]

    kernel32 = ctypes.windll.kernel32
    kernel32.CreateToolhelp32Snapshot.argtypes = (wintypes.DWORD, wintypes.DWORD)
    kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    kernel32.Process32FirstW.argtypes = (wintypes.HANDLE, ctypes.POINTER(_ProcessEntry))
    kernel32.Process32FirstW.restype = wintypes.BOOL
    kernel32.Process32NextW.argtypes = (wintypes.HANDLE, ctypes.POINTER(_ProcessEntry))
    kernel32.Process32NextW.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL
    snapshot = kernel32.CreateToolhelp32Snapshot(0x2, 0)
    if snapshot == wintypes.HANDLE(-1).value:
        return None
    children: list[int] = []
    try:
        entry = _ProcessEntry()
        entry.dwSize = ctypes.sizeof(_ProcessEntry)
        listed = kernel32.Process32FirstW(snapshot, ctypes.byref(entry))
        while listed:
            process_path = _process_image_path(int(entry.th32ProcessID))
            if process_path and _path_is_within(process_path, resolved_venv):
                children.append(int(entry.th32ProcessID))
            listed = kernel32.Process32NextW(snapshot, ctypes.byref(entry))
    finally:
        kernel32.CloseHandle(snapshot)
    return children


def _process_image_path(process_id: int) -> str | None:
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.windll.kernel32
    kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.QueryFullProcessImageNameW.argtypes = (
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.LPWSTR,
        ctypes.POINTER(wintypes.DWORD),
    )
    kernel32.QueryFullProcessImageNameW.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL
    process_query_limited_information = 0x1000
    handle = kernel32.OpenProcess(process_query_limited_information, False, process_id)
    if not handle:
        return None
    try:
        size = wintypes.DWORD(1024)
        buffer = ctypes.create_unicode_buffer(1024)
        if not kernel32.QueryFullProcessImageNameW(handle, 0, buffer, ctypes.byref(size)):
            return None
        return buffer.value
    finally:
        kernel32.CloseHandle(handle)


def _venv_children_posix(resolved_venv: str) -> list[int] | None:
    entries = Path("/proc").iterdir() if Path("/proc").is_dir() else None
    if entries is None:
        return None
    children: list[int] = []
    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            executable = os.readlink(entry / "exe")
        except OSError:
            continue
        if _path_is_within(executable, resolved_venv):
            children.append(int(entry.name))
    return children


def _require_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def build_summary(
    platform_name: str, steps: Sequence[Mapping[str, object]]
) -> Mapping[str, object]:
    """Return the bounded deterministic JSON summary document.

    A step the host cannot execute at all is marked ``unavailable`` with
    its reason and still fails this required platform matrix.  The exit
    consumer reads ``passed``; unavailable evidence can never become green.
    """
    return {
        "format": "paritygrid-platform-matrix-summary",
        "version": _SUMMARY_VERSION,
        "platform": platform_name,
        "steps": [dict(step) for step in steps],
        "unavailable": [str(step["name"]) for step in steps if step.get("unavailable") is True],
        "passed": all(step.get("passed") is True for step in steps),
    }


def is_supported_matrix_platform(platform_name: str) -> bool:
    """Return whether the named platform is a supported matrix target."""
    return platform_name == "win32" or platform_name.startswith("linux")


def run_matrix(checkout: Path) -> Mapping[str, object]:
    """Run every step of the current platform's matrix and return the summary."""
    platform_name = sys.platform
    if not is_supported_matrix_platform(platform_name):
        raise MatrixUsageError(
            f"platform {platform_name} is not a supported matrix target (Windows or Linux)"
        )
    uv = shutil.which("uv")
    if uv is None:
        raise MatrixUsageError("uv executable was not found")
    steps: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory(prefix="paritygrid-platform-") as temporary:
        temporary_root = Path(temporary)
        spaces_root = _require_directory(temporary_root / _SPACES_DIR_NAME / "work")
        unicode_root = _require_directory(temporary_root / _UNICODE_CWD_NAME / "work")
        venv_parent = temporary_root / _UNICODE_CWD_NAME / "venv"
        distribution_root = _require_directory(temporary_root / "dist")
        requirements = temporary_root / "runtime-requirements.txt"

        _run(
            (uv, "build", "--wheel", "--out-dir", distribution_root),
            cwd=checkout,
            timeout_seconds=_BUILD_TIMEOUT_SECONDS,
        )
        wheels = sorted(distribution_root.glob("*.whl"))
        if len(wheels) != 1:
            raise MatrixStepError(f"expected one wheel, found {len(wheels)}")
        steps.append({"name": "build_wheel", "passed": True, "detail": wheels[0].name})

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
            timeout_seconds=_BUILD_TIMEOUT_SECONDS,
        )
        steps.append({"name": "export_locked_runtime", "passed": True})

        isolated = isolated_environment()
        _run(
            (uv, "venv", "--python", sys.executable, venv_parent),
            cwd=spaces_root,
            timeout_seconds=_SYNC_TIMEOUT_SECONDS,
        )
        environment_python = _venv_python(venv_parent)
        _run(
            (uv, "pip", "sync", "--python", environment_python, requirements),
            cwd=spaces_root,
            timeout_seconds=_SYNC_TIMEOUT_SECONDS,
        )
        _run(
            (uv, "pip", "install", "--python", environment_python, "--no-deps", wheels[0]),
            cwd=spaces_root,
            timeout_seconds=_SYNC_TIMEOUT_SECONDS,
        )
        steps.append(
            {
                "name": "install_wheel_outside_checkout",
                "passed": True,
                "detail": str(venv_parent.name),
            }
        )

        import_probe = (
            "from pathlib import Path; import paritygrid, sys; "
            "module=Path(paritygrid.__file__).resolve(); "
            "checkout=Path(sys.argv[1]).resolve(); environment=Path(sys.argv[2]).resolve(); "
            "assert module.is_relative_to(environment), module; "
            "assert not module.is_relative_to(checkout), module; print(module)"
        )
        _run(
            (environment_python, "-I", "-X", "utf8", "-c", import_probe, checkout, venv_parent),
            cwd=unicode_root,
            environment=isolated,
            timeout_seconds=_HELP_TIMEOUT_SECONDS,
        )
        steps.append({"name": "imports_resolve_from_wheel", "passed": True})

        executable = _venv_paritygrid(venv_parent)
        for help_arguments in (
            ("--help",),
            ("demo", "--help"),
            ("demo-compare", "--help"),
            ("stress", "--help"),
            ("stress", "performance", "--help"),
            ("stress", "resources", "--help"),
            ("stress", "capabilities", "--help"),
        ):
            _run(
                (executable, *help_arguments),
                cwd=spaces_root,
                environment=isolated,
                timeout_seconds=_HELP_TIMEOUT_SECONDS,
            )
        capabilities = _run(
            (executable, "stress", "capabilities", "--json"),
            cwd=spaces_root,
            environment=isolated,
            timeout_seconds=_HELP_TIMEOUT_SECONDS,
        )
        capability_document = json.loads(capabilities)
        pool_states = {
            entry["profile_id"]: entry["state"] for entry in capability_document["profiles"]
        }
        steps.append(
            {
                "name": "cli_startup_and_help",
                "passed": True,
                "detail": f"process pool: {pool_states.get('subordinate-process-pool')}",
            }
        )

        frontend_probe = temporary_root / "frontend_probe.py"
        frontend_probe.write_text(FRONTEND_PROBE, encoding="utf-8")
        _run(
            (environment_python, "-I", "-X", "utf8", frontend_probe),
            cwd=unicode_root,
            environment=isolated,
            timeout_seconds=_HELP_TIMEOUT_SECONDS,
        )
        steps.append({"name": "packaged_frontend_assets", "passed": True})

        spawn_probe = temporary_root / "spawn_probe.py"
        spawn_probe.write_text(SPAWN_PROBE, encoding="utf-8")
        _run(
            (environment_python, "-I", "-X", "utf8", spawn_probe, checkout, venv_parent),
            cwd=unicode_root,
            environment=isolated,
            timeout_seconds=_HELP_TIMEOUT_SECONDS,
        )
        steps.append({"name": "subprocess_spawn_and_process_pool", "passed": True})

        _run(
            (executable, "smoke"),
            cwd=spaces_root,
            environment=isolated,
            timeout_seconds=_SMOKE_TIMEOUT_SECONDS,
        )
        steps.append({"name": "backend_smoke", "passed": True})

        # Demo roots keep ASCII components by the accepted ownership
        # contract; the Unicode and space coverage comes from the working
        # directory and the venv location above.
        demo_root = temporary_root / "demo-root"
        for runner_name in ("sequential", "threaded", "asyncio"):
            _run(
                (executable, "demo", "--headless", "--runner", runner_name, "--root", demo_root),
                cwd=unicode_root,
                environment=isolated,
                timeout_seconds=_DEMO_TIMEOUT_SECONDS,
            )
            _no_venv_children(venv_parent)
            steps.append(
                {
                    "name": f"demo_headless_{runner_name}",
                    "passed": True,
                    "detail": str(demo_root.name),
                }
            )
        _run(
            (executable, "demo-compare", "--root", demo_root),
            cwd=unicode_root,
            environment=isolated,
            timeout_seconds=_DEMO_TIMEOUT_SECONDS,
        )
        steps.append({"name": "cross_runner_evidence", "passed": True})

        # The Phase 21 harness and profile commands must also work from the
        # installed wheel, not only from the source checkout.
        wheel_performance_root = temporary_root / "harness" / "performance"
        wheel_performance_root.mkdir(parents=True, exist_ok=True)
        _run(
            (
                executable,
                "stress",
                "performance",
                "--root",
                wheel_performance_root,
                "--report",
                unicode_root / "wheel-performance-report.json",
                "--story-warmups",
                "0",
                "--story-repetitions",
                "1",
                "--runner-warmups",
                "0",
                "--runner-repetitions",
                "1",
                "--create-parent",
            ),
            cwd=unicode_root,
            environment=isolated,
            timeout_seconds=_DEMO_TIMEOUT_SECONDS,
        )
        steps.append({"name": "wheel_performance_harness", "passed": True})
        wheel_resources_root = temporary_root / "harness" / "resources"
        wheel_resources_root.mkdir(parents=True, exist_ok=True)
        _run(
            (
                executable,
                "stress",
                "resources",
                "--root",
                wheel_resources_root,
                "--report",
                unicode_root / "wheel-resource-bounds-report.json",
                "--repetitions",
                "1",
                "--create-parent",
            ),
            cwd=unicode_root,
            environment=isolated,
            timeout_seconds=_DEMO_TIMEOUT_SECONDS,
        )
        steps.append({"name": "wheel_resource_bounds", "passed": True})

        if sys.platform == "win32":
            steps.extend(_windows_steps(temporary_root, venv_parent, environment_python, isolated))
        else:
            steps.extend(_linux_steps(temporary_root, venv_parent, isolated, unicode_root))

        orphans = _venv_child_processes(venv_parent)
        steps.append(
            {
                "name": "orphan_detection",
                "passed": orphans is not None and not orphans,
                "detail": "unavailable" if orphans is None else f"{len(orphans)} venv processes",
            }
        )
    return build_summary(platform_name, steps)


def _no_venv_children(venv_root: Path) -> None:
    orphans = _venv_child_processes(venv_root)
    if orphans:
        raise MatrixStepError(f"orphaned venv processes remained: {orphans}")


def _windows_steps(
    temporary_root: Path,
    venv_parent: Path,
    environment_python: Path,
    isolated: Mapping[str, str],
) -> list[dict[str, object]]:
    """Run the Windows-specific path, junction, and handle-cleanup coverage."""
    steps: list[dict[str, object]] = []
    scenario_probe = (
        "from pathlib import Path; import sys; "
        "from paritygrid.demo.scenario_runner import open_scenario_root; "
        "opened=open_scenario_root(Path(sys.argv[1])); "
        "assert opened.path == Path(sys.argv[2]).resolve(), opened.path"
    )

    short_path = windows_short_path(temporary_root)
    if short_path is not None and "~" in short_path.name:
        _run(
            (
                environment_python,
                "-I",
                "-X",
                "utf8",
                "-c",
                scenario_probe,
                short_path / "matrix-scenario",
                temporary_root / "matrix-scenario",
            ),
            cwd=short_path,
            environment=isolated,
            timeout_seconds=_HELP_TIMEOUT_SECONDS,
        )
        steps.append({"name": "windows_8_3_short_path", "passed": True, "detail": short_path.name})
    else:
        steps.append(
            {
                "name": "windows_8_3_short_path",
                "passed": False,
                "unavailable": True,
                "detail": "the volume did not return an 8.3 short path",
            }
        )

    tilde_dir = _require_directory(temporary_root / _TILDE_DIR_NAME / "work")
    _run(
        (
            environment_python,
            "-I",
            "-X",
            "utf8",
            "-c",
            scenario_probe,
            tilde_dir / "matrix-scenario",
            tilde_dir / "matrix-scenario",
        ),
        cwd=tilde_dir,
        environment=isolated,
        timeout_seconds=_HELP_TIMEOUT_SECONDS,
    )
    steps.append({"name": "windows_tilde_component", "passed": True, "detail": _TILDE_DIR_NAME})

    junction_link = temporary_root / "junction-link"
    junction_target = _require_directory(temporary_root / _UNICODE_CWD_NAME)
    if _windows_create_junction(junction_link, junction_target):
        rejection_probe = (
            "from pathlib import Path; import sys; "
            "from paritygrid.demo.scenario_runner import ScenarioPathError, open_scenario_root; "
            "target=Path(sys.argv[1]); "
            "\ntry: open_scenario_root(target)\n"
            "except ScenarioPathError: pass\n"
            "else: raise SystemExit('junction root was accepted')"
        )
        _run(
            (
                environment_python,
                "-I",
                "-X",
                "utf8",
                "-c",
                rejection_probe,
                junction_link / "work" / "matrix-scenario",
            ),
            cwd=temporary_root,
            environment=isolated,
            timeout_seconds=_HELP_TIMEOUT_SECONDS,
        )
        steps.append({"name": "windows_junction_safety", "passed": True})
    else:
        steps.append(
            {
                "name": "windows_junction_safety",
                "passed": False,
                "unavailable": True,
                "detail": "junction creation was refused on this volume",
            }
        )

    import ctypes

    kernel32 = ctypes.windll.kernel32
    handle_before = wintypes_handle_count(kernel32)
    for _repeat in range(2):
        _run(
            (_venv_python(venv_parent), "-I", "-c", "import paritygrid"),
            cwd=temporary_root,
            environment=isolated,
            timeout_seconds=_HELP_TIMEOUT_SECONDS,
        )
    handle_after = wintypes_handle_count(kernel32)
    if handle_before is None or handle_after is None:
        steps.append(
            {
                "name": "handle_cleanup_observation",
                "passed": False,
                "detail": "unavailable: the handle count query failed on this system",
            }
        )
        return steps
    bounded_slack = 64
    within_bound = handle_after <= handle_before + bounded_slack
    steps.append(
        {
            "name": "handle_cleanup_observation",
            "passed": within_bound,
            "detail": (
                f"before={handle_before} after={handle_after} "
                f"bound={bounded_slack} repeated_children=2"
            ),
        }
    )
    return steps


def wintypes_handle_count(kernel32: object) -> int | None:
    """Return this process's handle count, or None when the query fails."""
    import ctypes
    from ctypes import wintypes

    query = getattr(kernel32, "GetProcessHandleCount", None)
    current = getattr(kernel32, "GetCurrentProcess", None)
    if query is None or current is None:
        return None
    query.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
    query.restype = wintypes.BOOL
    received = wintypes.DWORD(0)
    if not query(current(), ctypes.byref(received)):
        return None
    return int(received.value)


def _linux_steps(
    temporary_root: Path,
    venv_parent: Path,
    isolated: Mapping[str, str],
    work_root: Path,
) -> list[dict[str, object]]:
    """Run the Linux-specific permission behavior coverage."""
    steps: list[dict[str, object]] = []
    geteuid = getattr(os, "geteuid", None)
    if geteuid is not None and geteuid() == 0:
        steps.append(
            {
                "name": "linux_readonly_permission",
                "passed": False,
                "unavailable": True,
                "detail": "running as root ignores permission bits",
            }
        )
        return steps
    readonly_parent = _require_directory(temporary_root / "readonly")
    demo_target = readonly_parent / "demo-root"
    readonly_parent.chmod(0o500)
    try:
        permission_probe = (
            "from pathlib import Path; import sys; "
            "from paritygrid.demo.ownership import open_or_create_demo_root; "
            "target=Path(sys.argv[1]); "
            "\ntry: open_or_create_demo_root(target)\n"
            "except PermissionError: pass\n"
            "else: raise SystemExit('read-only parent accepted a demo root')\n"
            "assert not target.exists(), 'refused root left a target behind'"
        )
        completed = subprocess.run(
            (str(_venv_python(venv_parent)), "-I", "-c", permission_probe, str(demo_target)),
            cwd=work_root,
            env=isolated,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=_DEMO_TIMEOUT_SECONDS,
            check=False,
        )
        refused = completed.returncode == 0 and not demo_target.exists()
    finally:
        readonly_parent.chmod(0o700)
    if refused:
        steps.append(
            {
                "name": "linux_readonly_permission",
                "passed": True,
                "detail": "permission refusal was specific and left no target",
            }
        )
    else:
        steps.append(
            {
                "name": "linux_readonly_permission",
                "passed": False,
                "detail": f"probe_exit={completed.returncode} target_exists={demo_target.exists()}",
            }
        )
    return steps


__all__ = [
    "FRONTEND_PROBE",
    "SPAWN_PROBE",
    "build_summary",
    "is_supported_matrix_platform",
    "isolated_environment",
    "main",
    "run_matrix",
    "windows_short_path",
]


def main(argv: Sequence[str] | None = None) -> int:
    """Run the platform verification matrix."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, default=None, help="write the JSON summary")
    arguments = parser.parse_args(argv)
    try:
        summary = run_matrix(_CHECKOUT)
    except (MatrixStepError, MatrixUsageError) as error:
        print(f"Platform matrix failed: {error}", file=sys.stderr)
        return 1
    document = json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True)
    if arguments.summary is not None:
        arguments.summary.write_text(document + "\n", encoding="ascii")
    steps_document = cast("list[dict[str, object]]", summary["steps"])
    for step in steps_document:
        detail = f": {step['detail']}" if step.get("detail") else ""
        if step.get("unavailable") is True:
            state = "unavailable"
        elif step["passed"] is True:
            state = "pass"
        else:
            state = "FAIL"
        print(f"  [{state}] {step['name']}{detail}")
    unavailable = cast("list[str]", summary["unavailable"])
    if unavailable:
        print(f"Unavailable capabilities: {', '.join(unavailable)}")
    if summary["passed"] is True:
        print(f"Platform matrix passed ({summary['platform']}).")
        return 0
    print(f"Platform matrix FAILED ({summary['platform']}).", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
