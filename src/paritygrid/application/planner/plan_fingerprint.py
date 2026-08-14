"""Dependency-neutral contracts for versioned logical-plan fingerprints."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from hashlib import sha256
from typing import Self, cast

from paritygrid.application.planner.execution_plan import ExecutionPlan

PLAN_FINGERPRINT_VERSION = 1
PLAN_FINGERPRINT_ALGORITHM = "sha256"
PLAN_FINGERPRINT_HEX_LENGTH = 64

_PLAN_FINGERPRINT_PATTERN = re.compile(
    rf"[0-9a-f]{{{PLAN_FINGERPRINT_HEX_LENGTH}}}",
    flags=re.ASCII,
)
_PLAN_FINGERPRINT_DOMAIN = b"paritygrid:logical-execution-plan:v1\0"
_LENGTH_BYTES = 8


class PlanFingerprintError(ValueError):
    """Base failure for an invalid or uncomputable plan fingerprint."""


class InvalidPlanFingerprintError(PlanFingerprintError):
    """A plan fingerprint does not use the frozen lowercase SHA-256 form."""


@dataclass(frozen=True, slots=True, order=True)
class PlanFingerprint:
    """An opaque logical-plan digest distinct from publication content hashes."""

    value: str

    def __post_init__(self) -> None:
        value = cast(object, self.value)
        if type(value) is not str:
            raise TypeError("plan fingerprint must be text")
        if _PLAN_FINGERPRINT_PATTERN.fullmatch(value) is None:
            raise InvalidPlanFingerprintError(
                "plan fingerprint must be exactly 64 lowercase hexadecimal characters"
            )

    @classmethod
    def parse(cls, value: str) -> Self:
        """Parse the frozen lowercase hexadecimal representation."""
        return cls(value)

    @classmethod
    def from_bytes(cls, value: bytes) -> Self:
        """Parse an exact ASCII hexadecimal representation."""
        raw = cast(object, value)
        if type(raw) is not bytes:
            raise TypeError("plan fingerprint encoding must be bytes")
        try:
            text = raw.decode("ascii")
        except UnicodeDecodeError as error:
            raise InvalidPlanFingerprintError(
                "plan fingerprint encoding must contain only ASCII"
            ) from error
        return cls.parse(text)

    def to_bytes(self) -> bytes:
        """Return the stable lowercase hexadecimal representation."""
        return self.value.encode("ascii")

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def __str__(self) -> str:
        return self.value


def fingerprint_execution_plan(plan: ExecutionPlan) -> PlanFingerprint:
    """Hash the complete logical plan while excluding visual pipeline layout."""
    if type(plan) is not ExecutionPlan:
        raise TypeError("logical plan fingerprint input must use ExecutionPlan")
    canonical = json.dumps(
        plan.to_mapping(),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    framed = len(canonical).to_bytes(_LENGTH_BYTES, byteorder="big") + canonical
    return PlanFingerprint(sha256(_PLAN_FINGERPRINT_DOMAIN + framed).hexdigest())
