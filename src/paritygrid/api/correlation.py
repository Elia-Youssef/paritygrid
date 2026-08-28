"""Correlation identity handling for the HTTP boundary."""

import re
import secrets
from typing import cast

CORRELATION_HEADER = "X-Correlation-ID"
MAX_CORRELATION_ID_LENGTH = 96
_CORRELATION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]*", flags=re.ASCII)


def generate_correlation_id() -> str:
    """Generate an opaque, safe correlation identity."""
    return f"pg-{secrets.token_hex(16)}"


def validate_correlation_id(value: str) -> str:
    """Validate a supplied correlation identity without normalization."""
    if not 1 <= len(value) <= MAX_CORRELATION_ID_LENGTH:
        raise ValueError(f"correlation id length must be 1 to {MAX_CORRELATION_ID_LENGTH}")
    if _CORRELATION_PATTERN.fullmatch(value) is None:
        raise ValueError("correlation id must use portable ASCII characters")
    return value


def correlation_from_scope(scope: object) -> str:
    """Read the correlation identity installed by the middleware."""
    if not isinstance(scope, dict):
        return ""
    state = cast(dict[str, object], scope).get("state")
    if isinstance(state, dict):
        value = cast(dict[str, object], state).get("correlation_id")
        if isinstance(value, str):
            return value
    return ""
