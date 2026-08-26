"""Application-owned connector secret and error redaction (P9.7).

Every value a connector exposes beyond its own boundary — exception text,
structured event fields, logs, and returned failure details — must pass
through this module. Redaction is layered so an adversarial fixture cannot
slip raw secret material through an unconsidered channel:

1. Known secret *values* supplied by the caller are replaced wherever they
   occur, regardless of the surrounding key or text shape.
2. Sensitive *keys* (authorization, cookies, tokens, credentials) have their
   values replaced structurally in mappings.
3. Secret-shaped *marker patterns* (``bearer …``, ``token=…``, credential
   URLs) are masked even when no value was registered, so an upstream error
   body quoting a credential it generated itself is still redacted.
4. Payload fragments are length-bounded and stripped of control characters
   before they can appear in any public detail.
"""

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from typing import cast

REDACTION_PLACEHOLDER = "<redacted>"

MAX_PUBLIC_DETAIL_LENGTH = 512
MAX_REDACTED_TEXT_LENGTH = 512
MAX_FRAGMENT_BYTES = 128

_SECRET_MARKERS: tuple[str, ...] = ("password=", "token:", "bearer ", "secret/")
_CREDENTIAL_URL_PATTERN = re.compile(
    r"(?P<scheme>https?://)(?P<credentials>[^/@\s]+)@(?P<rest>[^\s]+)"
)
_BEARER_PATTERN = re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+")
_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)\b(password|passwd|secret|token|api[_-]?key|access[_-]?key)\s*[=:]\s*[^\s,;&'\"]+"
)
_CONTROL_CHARACTER_PATTERN = re.compile(r"[^\x20-\x7e\t]")

#: Header names whose values are always secret regardless of content.
SENSITIVE_HEADER_NAMES: frozenset[str] = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "x-api-key",
        "api-key",
        "x-auth-token",
        "x-session-token",
        "proxy-secret",
    }
)

#: Mapping keys whose values are always redacted in structured documents.
SENSITIVE_KEY_NAMES: frozenset[str] = SENSITIVE_HEADER_NAMES | frozenset(
    {
        "password",
        "passwd",
        "secret",
        "token",
        "access_token",
        "refresh_token",
        "api_key",
        "apikey",
        "private_key",
        "client_secret",
        "credentials",
    }
)


class RedactionError(ValueError):
    """Raised when redaction inputs or public details are invalid."""


@dataclass(frozen=True, slots=True, repr=False)
class SecretMaterial:
    """Registered secret values that must never cross the connector boundary.

    The container refuses empty or oversized values, never exposes the raw
    values through ``repr`` or ``str``, and fingerprints each value so
    diagnostics can name *which* secret was redacted without repeating it.
    """

    values: tuple[str, ...]

    def __init__(self, values: Sequence[str] = ()) -> None:
        cleaned: list[str] = []
        for value in values:
            if type(value) is not str:
                raise RedactionError("secret values must be text")
            if not value:
                raise RedactionError("secret values must not be empty")
            if len(value) > 256:
                raise RedactionError("secret values must not exceed 256 characters")
            cleaned.append(value)
        if len(set(cleaned)) != len(cleaned):
            raise RedactionError("secret values must be unique")
        object.__setattr__(self, "values", tuple(cleaned))

    @classmethod
    def empty(cls) -> SecretMaterial:
        """Return the container holding no secrets."""
        return cls(())

    @classmethod
    def combine(cls, *materials: SecretMaterial) -> SecretMaterial:
        """Combine registries while preserving order and removing duplicates."""
        combined: list[str] = []
        seen: set[str] = set()
        for material in materials:
            if type(material) is not cls:
                raise TypeError("combined secret material must use SecretMaterial")
            for value in material.values:
                if value not in seen:
                    seen.add(value)
                    combined.append(value)
        return cls(combined)

    def __len__(self) -> int:
        return len(self.values)

    def fingerprints(self) -> tuple[str, ...]:
        """Return stable short fingerprints of the registered secrets."""
        return tuple(sha256(value.encode("utf-8")).hexdigest()[:12] for value in self.values)

    def __repr__(self) -> str:
        return f"SecretMaterial(count={len(self.values)}, redacted=True)"

    def __str__(self) -> str:
        return repr(self)


def redact_text(text: str, secrets: SecretMaterial | None = None) -> str:
    """Return public-safe text with all known and shaped secrets removed."""
    redacted = text
    if secrets is not None:
        # Longest first, so a secret that contains a shorter registered
        # secret cannot leave a suffix behind after replacement.
        for value in sorted(secrets.values, key=len, reverse=True):
            if value in redacted:
                redacted = redacted.replace(value, REDACTION_PLACEHOLDER)
    redacted = _CREDENTIAL_URL_PATTERN.sub(
        lambda match: f"{match.group('scheme')}{REDACTION_PLACEHOLDER}@{match.group('rest')}",
        redacted,
    )
    redacted = _BEARER_PATTERN.sub(
        lambda match: f"{match.group(1)}{REDACTION_PLACEHOLDER}", redacted
    )
    redacted = _ASSIGNMENT_PATTERN.sub(
        lambda match: f"{match.group(1)}={REDACTION_PLACEHOLDER}", redacted
    )
    for marker in _SECRET_MARKERS:
        position = redacted.lower().find(marker)
        while position != -1:
            start = position + len(marker)
            end = start
            while end < len(redacted) and redacted[end] not in " \t\r\n,;&)'\"":
                end += 1
            if end > start:
                redacted = redacted[:start] + REDACTION_PLACEHOLDER + redacted[end:]
            # The replacement changes later offsets, so the scan restarts on
            # the updated text instead of advancing a stale cursor.
            position = redacted.lower().find(marker, start + len(REDACTION_PLACEHOLDER))
    if len(redacted) > MAX_REDACTED_TEXT_LENGTH:
        redacted = redacted[: MAX_REDACTED_TEXT_LENGTH - 3] + "..."
    return redacted


def redact_headers(
    headers: Mapping[str, str],
    secrets: SecretMaterial | None = None,
) -> dict[str, str]:
    """Return a public-safe copy of response or request headers."""
    redacted: dict[str, str] = {}
    for name, value in headers.items():
        safe_name = redact_text(str(name), secrets)
        if str(name).strip().lower() in SENSITIVE_HEADER_NAMES:
            redacted[safe_name] = REDACTION_PLACEHOLDER
        else:
            redacted[safe_name] = redact_text(str(value), secrets)
    return redacted


def redact_payload_fragment(fragment: str | bytes, secrets: SecretMaterial | None = None) -> str:
    """Return a short, sanitized payload fragment safe for public detail."""
    if isinstance(fragment, bytes):
        try:
            text = fragment.decode("utf-8")
        except UnicodeDecodeError:
            text = fragment[:MAX_FRAGMENT_BYTES].decode("utf-8", errors="replace")
    else:
        if type(fragment) is not str:
            raise RedactionError("payload fragments must be text or bytes")
        text = fragment
    if len(text) > MAX_FRAGMENT_BYTES:
        text = text[:MAX_FRAGMENT_BYTES] + "..."
    sanitized = _CONTROL_CHARACTER_PATTERN.sub("?", text)
    return redact_text(sanitized, secrets)


def redact_value(value: object, secrets: SecretMaterial | None = None) -> object:
    """Recursively redact a closed structured value for public surfaces."""
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, str):
        return redact_text(value, secrets)
    if isinstance(value, Mapping):
        mapping = cast("Mapping[object, object]", value)
        redacted: dict[str, object] = {}
        for key, item in mapping.items():
            key_text = str(key)
            if key_text.strip().lower() in SENSITIVE_KEY_NAMES:
                redacted[redact_text(key_text, secrets)] = REDACTION_PLACEHOLDER
            else:
                redacted[redact_text(key_text, secrets)] = redact_value(item, secrets)
        return redacted
    if isinstance(value, Sequence):
        return [redact_value(item, secrets) for item in cast("Sequence[object]", value)]
    return redact_text(repr(value), secrets)


def redact_exception(error: BaseException, secrets: SecretMaterial | None = None) -> str:
    """Flatten one exception (and its cause chain) into bounded public text."""
    parts: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = error
    depth = 0
    while current is not None and depth < 4:
        if id(current) in seen:
            break
        seen.add(id(current))
        type_name = type(current).__name__
        # str() of an arbitrary exception may quote raw secret-bearing
        # transport detail, so it is always filtered, never passed through.
        message = redact_text(str(current), secrets)
        parts.append(f"{type_name}: {message}" if message else type_name)
        current = current.__cause__ or current.__context__
        depth += 1
    return " <- ".join(parts)[:MAX_PUBLIC_DETAIL_LENGTH]


def build_public_detail(
    summary: str,
    *,
    fragment: str | bytes | None = None,
    details: Mapping[str, object] | None = None,
    secrets: SecretMaterial | None = None,
) -> str:
    """Compose one bounded, fully redacted public failure detail string."""
    if type(summary) is not str or not summary:
        raise RedactionError("public detail requires a non-empty summary")
    safe_summary = redact_text(summary, secrets)
    segments = [safe_summary]
    if fragment is not None:
        segments.append(f"fragment={redact_payload_fragment(fragment, secrets)}")
    if details is not None:
        rendered = cast("dict[str, object]", redact_value(dict(details), secrets))
        for key in sorted(rendered):
            value = rendered[key]
            if isinstance(value, str):
                segments.append(f"{key}={redact_text(value, secrets)}")
            else:
                segments.append(f"{key}={value!r}")
    joined = "; ".join(segments)
    if len(joined) > MAX_PUBLIC_DETAIL_LENGTH:
        joined = joined[: MAX_PUBLIC_DETAIL_LENGTH - 3] + "..."
    return joined


def assert_public_text_is_safe(text: str, secrets: SecretMaterial | None = None) -> str:
    """Validate that finished public text carries no registered secret.

    Connectors call this as the final gate before a message, event, or error
    detail leaves the boundary; a leak raises instead of escaping.
    """
    if type(text) is not str:
        raise RedactionError("public text must be text")
    if secrets is not None:
        for value in secrets.values:
            if value and value in text:
                raise RedactionError("public text carries registered secret material")
    return text
