"""Run the Windows/Linux verification matrix over an isolated installed wheel.

The matrix logic lives in :mod:`paritygrid.quality.platform_matrix`; this
wrapper keeps the documented command entry point:

    python scripts/verify_platform_matrix.py [--summary PATH]
"""

from __future__ import annotations

from paritygrid.quality.platform_matrix import main

if __name__ == "__main__":  # pragma: no cover - thin command wrapper
    raise SystemExit(main())
