"""Spawn-safe process worker for registered subordinate CPU operations.

This module is the process-isolation boundary root for the transitive
import gate: it imports nothing beyond the closed subordinate codec.
At startup the worker installs a runtime audit guard that rejects
forbidden module imports and every filesystem open, so a worker can
never open the operational database or reach persistence, connectors,
or artifact ownership even if a payload asked it to.
"""

from __future__ import annotations

import sys

from paritygrid.adapters.runners.subordinate_codec import (
    SubordinateCodecError,
    decode_request,
    dispatch_registered_operation,
    encode_response,
)

_FORBIDDEN_IMPORT_PREFIXES = (
    "sqlite3",
    "sqlalchemy",
    "duckdb",
    "alembic",
    "os",
    "pathlib",
    "io",
    "shutil",
    "subprocess",
    "socket",
    "http",
    "urllib",
    "paritygrid.adapters.persistence",
    "paritygrid.adapters.artifacts",
    "paritygrid.adapters.analytics",
    "paritygrid.application.execution",
    "paritygrid.application.ports",
    "paritygrid.application.writes",
    "paritygrid.runtime",
    "paritygrid.cli",
)

_guard_installed = False


def install_runtime_guard() -> None:
    """Install the audit hook rejecting forbidden imports and opens."""
    global _guard_installed
    if _guard_installed:
        return
    _guard_installed = True

    def _hook(event: str, args: tuple[object, ...]) -> None:
        if event == "import" and args:
            candidate: object = args[0]
            if type(candidate) is str:
                module_name: str = candidate
                for prefix in _FORBIDDEN_IMPORT_PREFIXES:
                    if module_name == prefix or module_name.startswith(f"{prefix}."):
                        raise ImportError(f"process worker refused forbidden import: {module_name}")
        elif event == "open":
            raise PermissionError("process worker refused a filesystem open")

    sys.addaudithook(_hook)


def worker_entry(request: bytes) -> bytes:
    """Decode, execute, and encode exactly one registered operation."""
    install_runtime_guard()
    try:
        operation, _version, payload = decode_request(request)
    except SubordinateCodecError as error:
        # A malformed request has no trusted operation identity.  This fallback
        # is never accepted by the parent because it will not match its request.
        return _error_response("sort_integers", f"request rejected: {error.__class__.__name__}")
    try:
        result = dispatch_registered_operation(operation, payload)
    except SubordinateCodecError as error:
        return _error_response(operation, f"operation rejected: {error.__class__.__name__}")
    except Exception as error:
        return _error_response(operation, f"operation failed: {error.__class__.__name__}")
    return encode_response(operation, result)


def _error_response(operation: str, detail: str) -> bytes:
    from paritygrid.adapters.runners.subordinate_codec import encode_response

    return encode_response(
        operation,
        {"sorted": [], "worker_error": detail[:256]},
    )


__all__ = [
    "install_runtime_guard",
    "worker_entry",
]
