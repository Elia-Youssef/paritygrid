"""Prove the installed wheel runs the canonical demo without Node.js.

The script builds the wheel into a temporary directory, installs it with the
locked runtime dependencies into a fresh virtual environment outside this
checkout, and then proves five facts about the installed product:

1. The installed ``paritygrid`` package and its demo orchestration layer come
   from the virtual environment, never from the checkout.
2. A deliberately constructed demo ``PATH`` cannot resolve Node.js tooling
   (``node``, ``npm``, ``npx``, ``vite``).
3. ``paritygrid demo --headless --runner sequential --json`` exits 0 and
   prints the deterministic result document with its engine fingerprint.
4. The packaged frontend ships inside the wheel and lands in site-packages.
5. A live installed server binds only to loopback, serves that frontend,
   closes its port on termination, and permits safe reset of its owned root.

The developer's own Node.js installation is never inspected beyond the
``PATH`` probes above and is never modified.
"""

from __future__ import annotations

import hashlib
import json
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

_DEMO_TIMEOUT_SECONDS = 300.0
_PROBE_TIMEOUT_SECONDS = 60.0
_ERROR_OUTPUT_LIMIT = 2000
_SERVE_TIMEOUT_SECONDS = 120.0
_SERVE_EXIT_TIMEOUT_SECONDS = 20.0

_SCRUBBED_ENVIRONMENT_PREFIXES = ("node_", "npm_", "paritygrid_")

_IMPORT_PROBE = """from pathlib import Path
import sys

import paritygrid
import paritygrid.demo.orchestration

environment = Path(sys.argv[1]).resolve()
checkout = Path(sys.argv[2]).resolve()
package_module = Path(paritygrid.__file__).resolve()
demo_module = Path(paritygrid.demo.orchestration.__file__).resolve()
for module in (package_module, demo_module):
    assert module.is_relative_to(environment), module
    assert not module.is_relative_to(checkout), module
print(f"import provenance ok: {package_module}")
"""

_NODE_ABSENCE_PROBE = """import json
import shutil

found = {name: shutil.which(name) for name in ("node", "npm", "npx", "vite")}
print(json.dumps(found, sort_keys=True))
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


def _demo_child_environment(environment_root: Path) -> dict[str, str]:
    """Build the no-Node child environment for the installed demo."""
    environment = _isolated_environment()
    scripts_directory = environment_root / ("Scripts" if os.name == "nt" else "bin")
    system_paths = (
        (r"C:\Windows\System32", r"C:\Windows") if os.name == "nt" else ("/usr/bin", "/bin")
    )
    environment["PATH"] = os.pathsep.join((str(scripts_directory), *system_paths))
    for name in tuple(environment):
        if name.lower().startswith(_SCRUBBED_ENVIRONMENT_PREFIXES):
            environment.pop(name, None)
    leftovers = sorted(
        name for name in environment if name.lower().startswith(_SCRUBBED_ENVIRONMENT_PREFIXES)
    )
    if leftovers:
        raise RuntimeError(f"the demo child environment still carries overrides: {leftovers}")
    for name in ("node.exe", "npm.cmd", "npx.cmd"):
        if shutil.which(name, path=environment["PATH"]) is not None:
            raise RuntimeError(f"the demo PATH must not resolve {name}")
    return environment


def _capture(
    arguments: Sequence[str | Path],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    timeout_seconds: float,
) -> subprocess.CompletedProcess[str]:
    command = tuple(str(argument) for argument in arguments)
    print(f"+ {' '.join(command)}")
    try:
        return subprocess.run(
            command,
            cwd=cwd,
            env=environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(
            f"the probe exceeded its {timeout_seconds:.0f}s budget: {' '.join(command)}"
        ) from error


def _require_success(completed: subprocess.CompletedProcess[str], label: str) -> str:
    if completed.returncode != 0:
        raise RuntimeError(
            f"{label} failed with exit code {completed.returncode}: "
            f"{(completed.stdout + completed.stderr)[-_ERROR_OUTPUT_LIMIT:]}"
        )
    return completed.stdout


def _sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _result_document(stdout: str) -> dict[str, object]:
    """Return the last JSON object line of the demo output."""
    for line in reversed(stdout.splitlines()):
        candidate = line.strip()
        if not candidate.startswith("{"):
            continue
        try:
            decoded = cast(object, json.loads(candidate))
        except ValueError:
            continue
        if type(decoded) is not dict:
            continue
        return cast("dict[str, object]", decoded)
    raise RuntimeError("the demo run printed no parseable JSON result document")


def _site_packages(environment_root: Path) -> Path:
    if os.name == "nt":
        return environment_root / "Lib" / "site-packages"
    candidates = sorted(environment_root.glob("lib/python*/site-packages"))
    if len(candidates) != 1:
        raise RuntimeError(f"expected one site-packages directory, found {len(candidates)}")
    return candidates[0]


def _probe_installed_serve(
    environment_python: Path,
    demo_root: Path,
    *,
    cwd: Path,
    environment: Mapping[str, str],
) -> str:
    """Observe the installed package serving its bundled UI on loopback."""
    command = (
        str(environment_python),
        "-I",
        "-c",
        "from paritygrid.cli import app; app()",
        "demo",
        "--root",
        str(demo_root),
    )
    print(f"+ {' '.join(command)}")
    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    lines: queue.Queue[str | None] = queue.Queue()
    output: list[str] = []

    def _read_output() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            lines.put(line)
        lines.put(None)

    reader = threading.Thread(target=_read_output, name="demo-serve-output", daemon=True)
    reader.start()
    deadline = time.monotonic() + _SERVE_TIMEOUT_SECONDS
    serve_url: str | None = None
    try:
        while time.monotonic() < deadline:
            try:
                line = lines.get(timeout=min(1.0, max(0.01, deadline - time.monotonic())))
            except queue.Empty:
                if process.poll() is not None:
                    break
                continue
            if line is None:
                break
            output.append(line)
            prefix = "Serving the packaged application at "
            if line.startswith(prefix):
                serve_url = line.removeprefix(prefix).strip()
                break
        if serve_url is None:
            raise RuntimeError(
                "the installed serve probe did not announce a URL: "
                f"{''.join(output)[-_ERROR_OUTPUT_LIMIT:]}"
            )
        parsed = urllib.parse.urlparse(serve_url)
        if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.port is None:
            raise RuntimeError(f"the installed demo announced a non-loopback URL: {serve_url!r}")
        with urllib.request.urlopen(
            urllib.parse.urljoin(serve_url, "healthz"), timeout=_PROBE_TIMEOUT_SECONDS
        ) as response:
            health = cast("dict[str, object]", json.loads(response.read().decode("utf-8")))
        if health.get("status") != "ok" or health.get("service") != "ParityGrid":
            raise RuntimeError(f"the installed loopback health response is invalid: {health!r}")
        with urllib.request.urlopen(serve_url, timeout=_PROBE_TIMEOUT_SECONDS) as response:
            index = response.read(1_000_000).decode("utf-8")
        if "<title>ParityGrid</title>" not in index:
            raise RuntimeError("the installed serve probe did not return the packaged frontend")
        return serve_url
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=_SERVE_EXIT_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=_SERVE_EXIT_TIMEOUT_SECONDS)
        reader.join(timeout=2.0)
        if reader.is_alive():
            raise RuntimeError("the installed serve output reader did not close")
        if serve_url is not None:
            try:
                with urllib.request.urlopen(serve_url, timeout=1.0) as response:
                    response.read(1)
            except OSError, urllib.error.URLError:
                pass
            else:
                raise RuntimeError("the installed demo port still answers after shutdown")


def verify_demo_no_node(checkout: Path) -> None:
    """Perform the complete installed no-Node demo verification."""
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("uv executable was not found")
    if not (checkout / "web" / "dist" / "index.html").is_file():
        raise RuntimeError("web/dist/index.html is missing; the packaged frontend is required")
    with tempfile.TemporaryDirectory(prefix="paritygrid-demo-no-node-") as temporary_directory:
        temporary_root = Path(temporary_directory)
        distribution_root = temporary_root / "dist"
        environment_root = temporary_root / "venv"
        outside_checkout = temporary_root / "Café % عربي"
        outside_checkout.mkdir()
        demo_root = temporary_root / "demo-root"
        requirements = temporary_root / "runtime-requirements.txt"

        _run((uv, "build", "--wheel", "--out-dir", distribution_root), cwd=checkout)
        wheels = tuple(distribution_root.glob("*.whl"))
        if len(wheels) != 1:
            raise RuntimeError(f"expected one wheel, found {len(wheels)}")
        wheel = wheels[0]
        wheel_sha256 = _sha256_of(wheel)
        with zipfile.ZipFile(wheel, "r") as archive:
            wheel_names = frozenset(archive.namelist())
        bundled_node = sorted(name for name in wheel_names if name.endswith((".node", "node.exe")))
        if bundled_node:
            raise RuntimeError(f"the wheel bundles Node artifacts: {bundled_node}")
        if "paritygrid/_frontend/index.html" not in wheel_names:
            raise RuntimeError(
                "the wheel does not package the frontend (paritygrid/_frontend/index.html)"
            )
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
            (uv, "pip", "install", "--python", environment_python, "--no-deps", wheel),
            cwd=outside_checkout,
        )

        site_packages = _site_packages(environment_root)
        installed_frontend = site_packages / "paritygrid" / "_frontend" / "index.html"
        if not installed_frontend.is_file():
            raise RuntimeError("the installed environment lacks the packaged frontend index.html")
        demo_environment = _demo_child_environment(environment_root)

        import_completed = _capture(
            (
                environment_python,
                "-I",
                "-c",
                _IMPORT_PROBE,
                environment_root,
                checkout,
            ),
            cwd=outside_checkout,
            environment=demo_environment,
            timeout_seconds=_PROBE_TIMEOUT_SECONDS,
        )
        import_stdout = _require_success(import_completed, "the import-provenance probe").strip()

        node_completed = _capture(
            (environment_python, "-I", "-c", _NODE_ABSENCE_PROBE),
            cwd=outside_checkout,
            environment=demo_environment,
            timeout_seconds=_PROBE_TIMEOUT_SECONDS,
        )
        node_stdout = _require_success(node_completed, "the node-absence probe").strip()
        try:
            found_node_tools = cast("dict[str, object]", json.loads(node_stdout))
        except json.JSONDecodeError as error:
            raise RuntimeError("the node-absence probe printed no JSON") from error
        unresolved = sorted(
            name for name, resolved in found_node_tools.items() if resolved is not None
        )
        if unresolved:
            raise RuntimeError(f"Node.js tooling resolved from the child PATH: {unresolved}")

        demo_completed = _capture(
            (
                environment_root
                / ("Scripts/paritygrid.exe" if os.name == "nt" else "bin/paritygrid"),
                "demo",
                "--headless",
                "--runner",
                "sequential",
                "--root",
                demo_root,
                "--json",
            ),
            cwd=outside_checkout,
            environment=demo_environment,
            timeout_seconds=_DEMO_TIMEOUT_SECONDS,
        )
        demo_stdout = _require_success(demo_completed, "the canonical demo run")
        document = _result_document(demo_stdout)
        if document.get("format") != "paritygrid.demo.result":
            raise RuntimeError(f"the demo result carries an unexpected format: {document!r}")
        if document.get("runner") != "sequential":
            raise RuntimeError(f"the demo result names an unexpected runner: {document!r}")
        engine_value = document.get("engine")
        if type(engine_value) is not dict:
            raise RuntimeError(f"the demo result carries no engine mapping: {document!r}")
        engine = cast("dict[str, object]", engine_value)
        fingerprint = engine.get("execution_evidence_fingerprint")
        if not isinstance(fingerprint, str) or not fingerprint:
            raise RuntimeError("the demo result carries no engine execution-evidence fingerprint")

        serve_url = _probe_installed_serve(
            environment_python,
            demo_root,
            cwd=outside_checkout,
            environment=demo_environment,
        )
        reset_completed = _capture(
            (
                environment_root
                / ("Scripts/paritygrid.exe" if os.name == "nt" else "bin/paritygrid"),
                "demo-reset",
                "--root",
                demo_root,
            ),
            cwd=outside_checkout,
            environment=demo_environment,
            timeout_seconds=_PROBE_TIMEOUT_SECONDS,
        )
        _require_success(reset_completed, "the installed demo reset")
        if demo_root.exists():
            raise RuntimeError("the installed demo reset left its exact owned root behind")

        print(f"wheel: {wheel.name}")
        print(f"wheel sha256: {wheel_sha256}")
        print(f"probe import-provenance: passed ({import_stdout})")
        print(f"probe node-absence: passed ({node_stdout}; PATH={demo_environment['PATH']})")
        print(
            "probe canonical-demo: passed (exit 0, format=paritygrid.demo.result, "
            f"runner=sequential, engine fingerprint={fingerprint})"
        )
        print("probe packaged-frontend: passed (wheel and installed _frontend/index.html)")
        print(
            "probe loopback-binding: passed "
            f"(observed live installed server at {serve_url}; packaged index served; "
            "port closed and owned root reset)"
        )
    print("demo no-node verification passed")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the installed no-Node demo gate."""
    del argv
    try:
        verify_demo_no_node(Path(__file__).resolve().parents[1])
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"demo no-node verification failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
