"""Deterministic TypeScript type generation and drift check.

Reads the committed ``docs/generated/openapi.json`` and emits the typed API
boundary at ``web/src/api/generated/schema.d.ts``.  A clean regeneration
never differs.  Generated output is never hand-edited; handwritten frontend
code imports types from this module only.

Usage:

    uv run python scripts/generate_api_types.py           # regenerate
    uv run python scripts/generate_api_types.py --check    # fail on drift
"""

import argparse
import json
import sys
from pathlib import Path

from paritygrid.api.ts_generation import build_document

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = PROJECT_ROOT / "docs" / "generated" / "openapi.json"
OUTPUT_PATH = PROJECT_ROOT / "web" / "src" / "api" / "generated" / "schema.d.ts"


def main(argv: list[str] | None = None) -> int:
    """Regenerate the typed boundary or verify it, exiting nonzero on drift."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the generated module matches a fresh generation",
    )
    arguments = parser.parse_args(argv)
    if not INPUT_PATH.is_file():
        print(f"missing OpenAPI contract: {INPUT_PATH}", file=sys.stderr)
        return 1
    source = json.loads(INPUT_PATH.read_text(encoding="utf-8"))
    fresh = build_document(source)
    if arguments.check:
        if not OUTPUT_PATH.is_file():
            print(f"missing generated module: {OUTPUT_PATH}", file=sys.stderr)
            return 1
        committed = OUTPUT_PATH.read_text(encoding="utf-8")
        if committed != fresh:
            print(
                "generated TypeScript drift detected; regenerate with "
                "`uv run python scripts/generate_api_types.py`",
                file=sys.stderr,
            )
            return 1
        print("generated TypeScript types are up to date.")
        return 0
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(fresh, encoding="utf-8", newline="\n")
    print(f"wrote {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
