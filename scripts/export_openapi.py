"""Deterministic OpenAPI export and drift check.

Exports the OpenAPI document from the real application factory with stable
ordering and normalized output: no timestamps, machine paths, secrets, or
nondeterministic identifiers.  The WebSocket live channel is recorded as an
explicit transport extension because OpenAPI 3.1 does not model WebSocket
routes; the SSE route is covered by its normal path entry.

Usage:

    uv run python scripts/export_openapi.py           # regenerate
    uv run python scripts/export_openapi.py --check    # fail on drift
"""

import argparse
import sys
from pathlib import Path

from paritygrid.api.openapi_export import build_document

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = PROJECT_ROOT / "docs" / "generated" / "openapi.json"


def main(argv: list[str] | None = None) -> int:
    """Regenerate the contract or verify it, exiting nonzero on drift."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the committed document matches a fresh export",
    )
    arguments = parser.parse_args(argv)
    fresh = build_document()
    if arguments.check:
        if not OUTPUT_PATH.is_file():
            print(f"missing generated contract: {OUTPUT_PATH}", file=sys.stderr)
            return 1
        committed = OUTPUT_PATH.read_text(encoding="utf-8")
        if committed != fresh:
            print(
                "OpenAPI drift detected; regenerate with `uv run python scripts/export_openapi.py`",
                file=sys.stderr,
            )
            return 1
        print("OpenAPI contract is up to date.")
        return 0
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(fresh, encoding="utf-8", newline="\n")
    print(f"wrote {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
