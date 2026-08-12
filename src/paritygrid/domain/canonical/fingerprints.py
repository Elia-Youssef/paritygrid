"""Order-independent logical state fingerprints."""

from collections.abc import Iterable
from enum import StrEnum
from hashlib import sha256
from typing import cast

from paritygrid.domain.canonical.encoding import (
    CanonicalEncoder,
    CanonicalVersion,
    encode_repair_plan_content,
)
from paritygrid.domain.errors import CanonicalEncodingError, CanonicalErrorCode
from paritygrid.domain.models import InventoryRecord, StateFingerprint
from paritygrid.domain.reconciliation import ReconciliationOutcome
from paritygrid.domain.repair import RepairPlan

MAX_FINGERPRINT_ITEMS = 10_000
_LENGTH_BYTES = 8
_LEAF_DOMAIN = b"paritygrid:canonical-leaf:v1\0"
_ROOT_DOMAIN = b"paritygrid:canonical-state:v1\0"


class FingerprintScope(StrEnum):
    """Closed logical states with separate digest domains."""

    INVENTORY_STATE = "inventory_state"
    RECONCILIATION_STATE = "reconciliation_state"
    REPAIR_PLAN_CONTENT = "repair_plan_content"


def fingerprint_state(
    values: Iterable[object],
    *,
    scope: FingerprintScope,
    version: CanonicalVersion = CanonicalVersion.V1,
) -> StateFingerprint:
    """Fingerprint a bounded unordered multiset while retaining duplicate multiplicity."""
    encoder = CanonicalEncoder(version=version)
    scope = _require_scope(scope)
    leaves: list[bytes] = []
    try:
        iterator = iter(values)
    except TypeError as error:
        raise _invalid_state("fingerprint.values") from error

    for value in iterator:
        if len(leaves) == MAX_FINGERPRINT_ITEMS:
            raise _invalid_state("fingerprint.item-count")
        canonical_bytes = _encode_state_value(value, scope=scope, encoder=encoder)
        leaf_preimage = _LEAF_DOMAIN + _frame(canonical_bytes)
        leaves.append(sha256(leaf_preimage).digest())

    scope_bytes = scope.value.encode("ascii")
    root_preimage = (
        _ROOT_DOMAIN
        + _frame(scope_bytes)
        + len(leaves).to_bytes(_LENGTH_BYTES, byteorder="big")
        + b"".join(sorted(leaves))
    )
    return StateFingerprint(sha256(root_preimage).hexdigest())


def _require_scope(value: object) -> FingerprintScope:
    if type(value) is not FingerprintScope:
        raise _invalid_state("fingerprint.scope")
    return value


def _encode_state_value(
    value: object,
    *,
    scope: FingerprintScope,
    encoder: CanonicalEncoder,
) -> bytes:
    expected_type: type[object]
    if scope is FingerprintScope.INVENTORY_STATE:
        expected_type = InventoryRecord
    elif scope is FingerprintScope.RECONCILIATION_STATE:
        expected_type = ReconciliationOutcome
    else:
        expected_type = RepairPlan
    if type(value) is not expected_type:
        raise CanonicalEncodingError(
            reason=CanonicalErrorCode.UNSUPPORTED_CANONICAL_TYPE,
            subject_type=f"fingerprint.{scope.value}",
        )
    if scope is FingerprintScope.REPAIR_PLAN_CONTENT:
        return encode_repair_plan_content(cast(RepairPlan, value), version=encoder.version)
    return encoder.encode(value)


def _frame(value: bytes) -> bytes:
    return len(value).to_bytes(_LENGTH_BYTES, byteorder="big") + value


def _invalid_state(subject_type: str) -> CanonicalEncodingError:
    return CanonicalEncodingError(
        reason=CanonicalErrorCode.INVALID_CANONICAL_VALUE,
        subject_type=subject_type,
    )
