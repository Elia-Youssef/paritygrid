"""Installed-source boundary for the packaged crash-reopen worker."""

from __future__ import annotations

import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_wheel_contains_quality_worker_without_runtime_artifacts(tmp_path: Path) -> None:
    distribution = tmp_path / "wheel"
    subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(distribution)],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    wheel = next(distribution.glob("paritygrid-*.whl"))
    with zipfile.ZipFile(wheel) as archive:
        members = set(archive.namelist())
    expected = {
        "paritygrid/quality/crash_reopen_protocol.py",
        "paritygrid/quality/crash_reopen_scenario.py",
        "paritygrid/quality/crash_reopen_harness.py",
        "paritygrid/quality/crash_reopen_worker.py",
    }
    assert expected <= members
    forbidden_suffixes = (".db", ".sqlite", ".sqlite3", "-wal", "-shm", ".jsonl")
    assert not any(member.casefold().endswith(forbidden_suffixes) for member in members)
    entry_points = next(member for member in members if member.endswith("entry_points.txt"))
    with zipfile.ZipFile(wheel) as archive:
        scripts = archive.read(entry_points).decode("utf-8")
    assert "crash" not in scripts.casefold()

    outside = tmp_path / "Café % Cafe\u0301 عربي"
    outside.mkdir()
    source = """
import io
import pathlib
import sys
sys.path.insert(0, sys.argv[1])
from paritygrid.quality import crash_reopen_protocol as protocol
from paritygrid.quality import crash_reopen_worker as worker
assert pathlib.Path(protocol.__file__).is_relative_to(pathlib.Path(sys.argv[1]))
stderr = io.StringIO()
result = worker.main(
    ["--control", sys.argv[2]],
    stdin=io.BytesIO(),
    stdout=io.BytesIO(),
    stderr=stderr,
)
assert result == 2
assert stderr.getvalue() == '{"error":"crash_worker_failed"}\\n'
"""
    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            source,
            str(wheel),
            str(outside / "missing-control.json"),
        ],
        cwd=outside,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
