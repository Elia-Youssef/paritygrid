"""Versioned transport schemas for repair workflow routes."""

from typing import Literal

from pydantic import Field, model_validator

from paritygrid.api.schemas.common import TRANSPORT_SCHEMA_VERSION, TransportModel

_FINGERPRINT_PATTERN = r"^[0-9a-f]{64}$"
_MAX_OBSERVATIONS_PER_SIDE = 10_000


class ObservationInput(TransportModel):
    """One reconciliation observation exactly as the source side read it."""

    position: int = Field(ge=0, le=2_147_483_647)
    payload: dict[str, object] | None = None
    malformed_reason: str | None = Field(default=None, max_length=256)

    @model_validator(mode="after")
    def _require_exclusive_shape(self) -> ObservationInput:
        if (self.payload is None) == (self.malformed_reason is None):
            raise ValueError("an observation carries either a payload or a malformed reason")
        return self


class ObservationSideInput(TransportModel):
    """One bounded side of reconciliation observations."""

    connector_id: str = Field(
        min_length=7, max_length=68, pattern=r"^con_[a-z0-9]+(?:-[a-z0-9]+)*$"
    )
    input_identity: str = Field(pattern=_FINGERPRINT_PATTERN)
    observations: list[ObservationInput] = Field(max_length=_MAX_OBSERVATIONS_PER_SIDE)


class RepairPlanCreateRequest(TransportModel):
    """Body of ``POST /api/v1/runs/{run_id}/repair-plans``."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    source: ObservationSideInput
    target: ObservationSideInput


class RepairApprovalRequestBody(TransportModel):
    """Body of ``POST /api/v1/repair-plans/{plan_id}/approve``."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    approved_by: str = Field(min_length=1, max_length=128)
    approved_content_fingerprint: str = Field(pattern=_FINGERPRINT_PATTERN)
    approved_reconciliation_fingerprint: str = Field(pattern=_FINGERPRINT_PATTERN)


class RepairActionResponse(TransportModel):
    """One persisted repair action of a plan."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    action_id: str
    canonical_key: str
    kind: str
    status: str
    before_sha256: str
    proposed_after_sha256: str
    applied_at: str | None = None
    failed_at: str | None = None
    target_version: int | None = None


class RepairApprovalSummary(TransportModel):
    """The durable approval fact of one plan."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    approved_by: str
    approved_at: str
    correlation_id: str
    approval_schema_version: int


class RepairPlanResponse(TransportModel):
    """One repair plan aggregate with run and fingerprint coherence."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    plan_id: str
    run_id: str
    run_version: int
    state: str
    observed_at: str
    status: str
    reconciliation_fingerprint: str
    content_fingerprint: str
    created_at: str
    applying_at: str | None = None
    applied_at: str | None = None
    rejected_at: str | None = None
    failed_at: str | None = None
    approval: RepairApprovalSummary | None = None
    actions: list[RepairActionResponse]


class RepairApplyResponse(TransportModel):
    """One application outcome with durable disposition evidence."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    plan_id: str
    run_id: str
    run_version: int
    state: str
    observed_at: str
    status: str
    reconciliation_fingerprint: str
    content_fingerprint: str
    disposition: str
    resumed: bool
    effects: list[dict[str, object]]
