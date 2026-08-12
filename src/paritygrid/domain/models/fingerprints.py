"""Opaque content fingerprints shared by pure domain values."""

import re
from dataclasses import dataclass
from typing import Self

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)


@dataclass(frozen=True, slots=True, order=True)
class StateFingerprint:
    """A validated SHA-256 digest without responsibility for computing it."""

    value: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _validate_fingerprint(self.value))

    @classmethod
    def parse(cls, value: str) -> Self:
        """Parse an exact lowercase hexadecimal representation."""
        return cls(value=value)

    @classmethod
    def from_bytes(cls, value: object) -> Self:
        """Parse an ASCII hexadecimal representation."""
        if not isinstance(value, bytes):
            raise TypeError("state fingerprint encoding must be bytes")
        try:
            text = value.decode("ascii")
        except UnicodeDecodeError as error:
            raise ValueError("state fingerprint encoding must contain only ASCII") from error
        return cls.parse(text)

    def to_bytes(self) -> bytes:
        """Return the stable lowercase hexadecimal representation."""
        return self.value.encode("ascii")

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def __str__(self) -> str:
        return self.value


def _validate_fingerprint(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("state fingerprint must be text")
    if _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError("state fingerprint must be exactly 64 lowercase hexadecimal characters")
    return value
