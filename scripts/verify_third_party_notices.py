"""Verify the committed third-party inventory and bundled notices reproduce.

Regenerates ``docs/generated/third-party-notices.json`` from the two
audited lockfiles, the locked Python environment's package metadata, and
the committed frontend distribution, then compares it with the committed
file.  A mismatch fails with the first differing line.

The generated inventory records the MIT compatibility decision for every
locked dependency.  The final notice reproduces the copyright and license
text for every third-party asset bundled into the frontend and therefore the
wheel.  Both artifacts are generated only from locked local inputs.

Usage:

    uv run python scripts/verify_third_party_notices.py
"""

from __future__ import annotations

import argparse
import sys
import sysconfig
from collections.abc import Sequence
from pathlib import Path

from paritygrid.quality.third_party_notices import (
    InventoryInputs,
    build_inventory,
    render_bundled_notices,
    render_inventory,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INVENTORY_PATH = PROJECT_ROOT / "docs" / "generated" / "third-party-notices.json"
NOTICE_PATH = PROJECT_ROOT / "THIRD_PARTY_NOTICES.txt"
PUBLIC_NOTICE_PATH = PROJECT_ROOT / "web" / "public" / NOTICE_PATH.name
DIST_NOTICE_PATH = PROJECT_ROOT / "web" / "dist" / NOTICE_PATH.name


def _site_packages() -> Path:
    return Path(sysconfig.get_paths()["purelib"])


def _current_inputs() -> InventoryInputs:
    return InventoryInputs(
        uv_lock_path=PROJECT_ROOT / "uv.lock",
        python_site_packages=_site_packages(),
        package_lock_path=PROJECT_ROOT / "web" / "package-lock.json",
        frontend_dist_path=PROJECT_ROOT / "web" / "dist",
    )


def _render_current_inventory() -> str:
    return render_inventory(build_inventory(_current_inputs()))


def _render_current_notices() -> str:
    return render_bundled_notices(_current_inputs())


def _write_generated_artifacts(inventory: str, notices: str) -> None:
    INVENTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_NOTICE_PATH.parent.mkdir(parents=True, exist_ok=True)
    INVENTORY_PATH.write_text(inventory, encoding="utf-8")
    NOTICE_PATH.write_text(notices, encoding="utf-8")
    PUBLIC_NOTICE_PATH.write_text(notices, encoding="utf-8")


def _check_exact(path: Path, expected: str) -> bool:
    if not path.is_file():
        relative = path.relative_to(PROJECT_ROOT).as_posix()
        print(f"committed generated artifact is missing at {relative}", file=sys.stderr)
        return False
    if path.read_text(encoding="utf-8") == expected:
        return True
    relative = path.relative_to(PROJECT_ROOT).as_posix()
    print(f"generated artifact drift detected at {relative}", file=sys.stderr)
    return False


def main(argv: Sequence[str] | None = None) -> int:
    """Compare the committed inventory against a fresh regeneration."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="regenerate committed artifacts")
    arguments = parser.parse_args(argv)
    try:
        regenerated = _render_current_inventory()
        notices = _render_current_notices()
    except (OSError, ValueError) as error:
        print(f"third-party inventory regeneration failed: {error}", file=sys.stderr)
        return 1
    if arguments.write:
        _write_generated_artifacts(regenerated, notices)
        print("third-party inventory and bundled notices regenerated.")
        return 0
    if not _check_exact(INVENTORY_PATH, regenerated):
        return 1
    if not _check_exact(NOTICE_PATH, notices) or not _check_exact(PUBLIC_NOTICE_PATH, notices):
        return 1
    if not _check_exact(DIST_NOTICE_PATH, notices):
        return 1
    print("third-party inventory and bundled notices reproduce exactly.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
