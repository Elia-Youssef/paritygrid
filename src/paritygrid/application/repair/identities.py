"""Deterministic identities for repair workflow facts and target effects.

Every identity is derived from the exact facts it names (run, reconciliation
snapshot, canonical key, or canonical plan content) so regenerating a plan
from the same reconciliation reproduces byte-identical identities and
idempotency keys instead of creating a second logical fact. When a derived
slug would exceed its identifier bound the derivation truncates and appends
a digest of the full preimage, keeping the mapping injective.
"""

from hashlib import sha256

from paritygrid.domain.models import (
    ConflictId,
    RepairActionId,
    RepairPlanId,
    RunId,
    StateFingerprint,
    TargetVerificationId,
)

_ID_PAYLOAD_MAXIMUM = 64
_IDEMPOTENCY_KEY_MAXIMUM = 128


def derive_plan_id(run_id: RunId, reconciliation: StateFingerprint) -> RepairPlanId:
    """Derive the stable plan identity for one run and reconciliation snapshot."""
    return RepairPlanId(f"rpl_{_slug((_run_slug(run_id), reconciliation.value[:24]))}")


def derive_conflict_id(run_id: RunId, canonical_key: str) -> ConflictId:
    """Derive the stable conflict identity for one canonical key."""
    return ConflictId(f"cnf_{_slug((_run_slug(run_id), canonical_key.lower()))}")


def derive_action_id(
    run_id: RunId, reconciliation: StateFingerprint, canonical_key: str
) -> RepairActionId:
    """Derive the stable action identity for one repairable key."""
    return RepairActionId(
        f"rac_{_slug((_run_slug(run_id), reconciliation.value[:16], canonical_key.lower()))}"
    )


def derive_action_idempotency_key(
    run_id: RunId, content_fingerprint: StateFingerprint, canonical_key: str
) -> str:
    """Derive the durable target idempotency key for one logical effect.

    The key binds the run and the canonical plan content so the same logical
    effect is stable across replays of one plan while distinct runs never
    collide on the globally unique stored key.
    """
    key = f"repair.{_run_slug(run_id)}.{content_fingerprint.value[:24]}.{canonical_key}"
    if len(key) > _IDEMPOTENCY_KEY_MAXIMUM:
        digest = sha256(key.encode("utf-8")).hexdigest()[:32]
        key = f"repair.{_run_slug(run_id)}.{digest}"
    return key


def derive_verification_id(
    run_id: RunId, reconciliation: StateFingerprint, observed: StateFingerprint
) -> TargetVerificationId:
    """Derive the stable verification identity for one observed target state."""
    return TargetVerificationId(
        f"tgv_{_slug((_run_slug(run_id), reconciliation.value[:16], observed.value[:24]))}"
    )


def _run_slug(run_id: RunId) -> str:
    return run_id.value.removeprefix("run_").lower()


def _slug(parts: tuple[str, ...]) -> str:
    """Render bounded lowercase slug parts, digest-suffixed when truncated."""
    joined = "-".join(part for part in parts if part)
    if len(joined) <= _ID_PAYLOAD_MAXIMUM:
        return joined
    digest = sha256(joined.encode("utf-8")).hexdigest()[:16]
    truncated = joined[: _ID_PAYLOAD_MAXIMUM - 17]
    return f"{truncated.rstrip('-')}-{digest}"


__all__ = [
    "derive_action_id",
    "derive_action_idempotency_key",
    "derive_conflict_id",
    "derive_plan_id",
    "derive_verification_id",
]
