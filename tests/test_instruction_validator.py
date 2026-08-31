from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


def test_candidate_files_exclude_deleted_index_entries() -> None:
    powershell = shutil.which("pwsh")
    assert powershell is not None, "PowerShell is required by the repository validation lane"

    project_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-File",
            str(project_root / "scripts" / "validate_instructions.ps1"),
            "-CandidateFileSelfTest",
        ],
        cwd=project_root,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert "Repository candidate-file self-test passed." in result.stdout
