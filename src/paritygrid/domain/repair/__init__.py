"""Immutable, non-destructive repair plan values."""

from paritygrid.domain.errors import StaleRepairPlanError
from paritygrid.domain.repair.plans import (
    RepairAction,
    RepairActionKind,
    RepairPlan,
    RepairPlanBinding,
)
from paritygrid.domain.repair.verification import (
    TARGET_OBSERVATION_VERSION,
    TARGET_STATE_FINGERPRINT_KIND,
    TARGET_STATE_FINGERPRINT_VERSION,
    TargetStateIdentity,
    compute_target_state_fingerprint,
)

__all__ = [
    "TARGET_OBSERVATION_VERSION",
    "TARGET_STATE_FINGERPRINT_KIND",
    "TARGET_STATE_FINGERPRINT_VERSION",
    "RepairAction",
    "RepairActionKind",
    "RepairPlan",
    "RepairPlanBinding",
    "StaleRepairPlanError",
    "TargetStateIdentity",
    "compute_target_state_fingerprint",
]
