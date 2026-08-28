"""Verify the production frontend distribution reproduces byte-for-byte.

Rebuilds the committed ``web/dist`` from the locked frontend toolchain into
a temporary directory and compares sorted file manifests with content
hashes.  A mismatch fails with the first differing path, so CI can gate on
committed distribution drift without Node.js at serving time.

Usage:

    uv run python scripts/verify_frontend_dist.py
"""

import hashlib
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WEB_ROOT = PROJECT_ROOT / "web"
DIST_ROOT = WEB_ROOT / "dist"
VITE_ENTRYPOINT = WEB_ROOT / "node_modules" / "vite" / "bin" / "vite.js"


def _manifest(root: Path) -> dict[str, str]:
    entries: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_file():
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            entries[path.relative_to(root).as_posix()] = digest
    return entries


def main(argv: list[str] | None = None) -> int:
    """Compare a clean rebuild against the committed distribution."""
    if not DIST_ROOT.is_dir() or not (DIST_ROOT / "index.html").is_file():
        print(
            "web/dist is missing; run `npm --prefix web run build` first",
            file=sys.stderr,
        )
        return 1
    node = shutil.which("node")
    if node is None or not VITE_ENTRYPOINT.is_file():
        print("node or the locked vite installation is unavailable", file=sys.stderr)
        return 1
    with tempfile.TemporaryDirectory(prefix="paritygrid-dist-") as temporary:
        output = Path(temporary) / "dist"
        completed = subprocess.run(
            [
                node,
                str(VITE_ENTRYPOINT),
                "build",
                "--outDir",
                str(output),
                "--emptyOutDir",
            ],
            cwd=WEB_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            print(completed.stdout, file=sys.stderr)
            print(completed.stderr, file=sys.stderr)
            print("frontend rebuild failed", file=sys.stderr)
            return 1
        committed = _manifest(DIST_ROOT)
        rebuilt = _manifest(output)
        if committed == rebuilt:
            print(f"frontend distribution reproduces exactly ({len(committed)} files).")
            return 0
        missing = sorted(set(committed) - set(rebuilt))
        extra = sorted(set(rebuilt) - set(committed))
        changed = sorted(
            path for path in set(committed) & set(rebuilt) if committed[path] != rebuilt[path]
        )
        for path in missing:
            print(f"missing from rebuild: {path}", file=sys.stderr)
        for path in extra:
            print(f"unexpected rebuild output: {path}", file=sys.stderr)
        for path in changed:
            print(f"content differs: {path}", file=sys.stderr)
        print(
            "frontend distribution drift detected; rebuild with `npm --prefix web run build`",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
