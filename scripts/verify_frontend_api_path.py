"""Verify that the frontend development server proxies operational API requests."""

import json
import shutil
import socket
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WEB_ROOT = PROJECT_ROOT / "web"
BACKEND_PORT = 8000
FRONTEND_PORT = 5173


def assert_port_available(port: int) -> None:
    """Fail before startup when another local process already owns a smoke port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.2)
        if probe.connect_ex(("127.0.0.1", port)) == 0:
            raise RuntimeError(f"Local port {port} is already in use.")


def start_process(command: Sequence[str], *, working_directory: Path) -> subprocess.Popen[str]:
    """Start one visible-log child process for the bounded smoke check."""
    return subprocess.Popen(
        command,
        cwd=working_directory,
        stdin=subprocess.DEVNULL,
        text=True,
    )


def read_json(url: str) -> Mapping[str, Any]:
    """Read a local JSON response with a short per-attempt timeout."""
    with urlopen(url, timeout=1.0) as response:
        payload: object = json.load(response)
    if not isinstance(payload, dict):
        raise TypeError(f"Expected an object response from {url}.")
    return cast("dict[str, Any]", payload)


def wait_for_status(
    url: str,
    *,
    expected_status: str,
    process: subprocess.Popen[str],
    timeout_seconds: float = 20.0,
) -> Mapping[str, Any]:
    """Poll until the expected response arrives or the child exits."""
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        exit_code = process.poll()
        if exit_code is not None:
            raise RuntimeError(f"Process exited with code {exit_code} while waiting for {url}.")
        try:
            payload = read_json(url)
            if payload.get("status") == expected_status:
                return payload
            last_error = RuntimeError(f"Unexpected status payload from {url}: {payload!r}")
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, TypeError) as error:
            last_error = error
        time.sleep(0.1)

    raise TimeoutError(f"Timed out waiting for {url}: {last_error}")


def stop_process(process: subprocess.Popen[str] | None) -> None:
    """Terminate a smoke child and bound the cleanup wait."""
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5.0)


def main() -> int:
    """Run the FastAPI-to-Vite proxy smoke path."""
    node = shutil.which("node")
    if node is None:
        raise RuntimeError("Node.js is required for the frontend API smoke check.")

    vite_entry = WEB_ROOT / "node_modules" / "vite" / "bin" / "vite.js"
    if not vite_entry.is_file():
        raise RuntimeError("Frontend dependencies are missing; run npm ci in web first.")

    assert_port_available(BACKEND_PORT)
    assert_port_available(FRONTEND_PORT)

    backend: subprocess.Popen[str] | None = None
    frontend: subprocess.Popen[str] | None = None
    try:
        backend = start_process(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "paritygrid.api:create_app",
                "--factory",
                "--host",
                "127.0.0.1",
                "--port",
                str(BACKEND_PORT),
                "--log-level",
                "warning",
            ],
            working_directory=PROJECT_ROOT,
        )
        wait_for_status(
            f"http://127.0.0.1:{BACKEND_PORT}/healthz",
            expected_status="ok",
            process=backend,
        )

        frontend = start_process(
            [
                node,
                str(vite_entry),
                "--host",
                "127.0.0.1",
                "--port",
                str(FRONTEND_PORT),
                "--strictPort",
            ],
            working_directory=WEB_ROOT,
        )
        payload = wait_for_status(
            f"http://127.0.0.1:{FRONTEND_PORT}/healthz",
            expected_status="ok",
            process=frontend,
        )
        print(
            "Frontend API smoke passed: "
            f"service={payload.get('service')}, version={payload.get('version')}"
        )
        return 0
    finally:
        stop_process(frontend)
        stop_process(backend)


if __name__ == "__main__":
    raise SystemExit(main())
