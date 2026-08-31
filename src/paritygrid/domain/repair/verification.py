"""Deterministic identity for one independently observed target state.

The target-state fingerprint is its own fingerprint kind: it identifies the
canonical business inventory read back from the target after repair effects,
framed with the observation version and record count. It is computed only
from independently observed records. It is never derived from a repair plan,
an expected state, the reconciliation fingerprint, or the execution-evidence
fingerprint, and it is not an alias of any of them.
"""

import json
from dataclasses import dataclass
from hashlib import sha256

from paritygrid.domain.models import StateFingerprint

TARGET_STATE_FINGERPRINT_KIND = "target_state"
TARGET_STATE_FINGERPRINT_VERSION = 1
TARGET_OBSERVATION_VERSION = 1
_MAX_TARGET_RECORDS = 10_000_000
_LENGTH_BYTES = 8
_FINGERPRINT_DOMAIN = b"paritygrid:target-state-fingerprint:v1\0"


@dataclass(frozen=True, slots=True)
class TargetStateIdentity:
    """The complete identity of one observed target state."""

    fingerprint_kind: str
    fingerprint_version: int
    observation_version: int
    record_count: int
    fingerprint: StateFingerprint

    def __post_init__(self) -> None:
        if self.fingerprint_kind != TARGET_STATE_FINGERPRINT_KIND:
            raise ValueError("target-state fingerprint kind is invalid")
        if self.fingerprint_version != TARGET_STATE_FINGERPRINT_VERSION:
            raise ValueError("target-state fingerprint version is unsupported")
        if self.observation_version != TARGET_OBSERVATION_VERSION:
            raise ValueError("target-state observation version is unsupported")
        if type(self.record_count) is not int or not 0 <= self.record_count <= _MAX_TARGET_RECORDS:
            raise ValueError("target-state record count is outside the supported range")
        if type(self.fingerprint) is not StateFingerprint:
            raise TypeError("target-state fingerprint must be a StateFingerprint")


def compute_target_state_fingerprint(
    *,
    observation_version: int,
    record_count: int,
    inventory_digest: StateFingerprint,
) -> StateFingerprint:
    """Fingerprint one observed target inventory and its observation inputs.

    The caller supplies the order-independent digest of the canonical
    observed inventory multiset; the header frames the observation version
    and record count so the composed digest is sensitive to any changed
    observed record while staying independent of observation order.
    """
    if type(observation_version) is not int or observation_version < 1:
        raise ValueError("target-state observation version is invalid")
    if type(record_count) is not int or not 0 <= record_count <= _MAX_TARGET_RECORDS:
        raise ValueError("target-state record count is outside the supported range")
    if type(inventory_digest) is not StateFingerprint:
        raise TypeError("target inventory digest must be a StateFingerprint")
    header = json.dumps(
        {
            "fingerprint_kind": TARGET_STATE_FINGERPRINT_KIND,
            "fingerprint_version": TARGET_STATE_FINGERPRINT_VERSION,
            "observation_version": observation_version,
            "record_count": record_count,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    preimage = _FINGERPRINT_DOMAIN + _frame(header) + _frame(inventory_digest.to_bytes())
    return StateFingerprint(sha256(preimage).hexdigest())


def _frame(value: bytes) -> bytes:
    return len(value).to_bytes(_LENGTH_BYTES, byteorder="big") + value
