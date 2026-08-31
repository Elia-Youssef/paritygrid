"""Versioned transport schemas for reconciliation routes."""

from typing import Literal

from pydantic import Field

from paritygrid.api.schemas.common import TRANSPORT_SCHEMA_VERSION, TransportModel

CLASSIFICATION_VALUES = (
    "match",
    "missing_from_target",
    "missing_from_source",
    "field_mismatch",
    "duplicate_source",
    "duplicate_target",
    "duplicate_both",
)


class ReconciliationResponse(TransportModel):
    """One durable reconciliation snapshot with run coherence fields."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    run_id: str
    run_version: int
    state: str
    observed_at: str
    reconciliation_fingerprint: str
    source_input_identity: str
    target_input_identity: str
    total_count: int
    counts: dict[str, int]
    analytical_query_version: int
    reconciliation_observed_at: str


class ConflictReference(TransportModel):
    """One persisted member-record reference."""

    position: int
    record_key: str


class ConflictDifference(TransportModel):
    """One persisted field-difference fact."""

    field: str
    kind: str
    source_text: str
    target_text: str


class ConflictResponse(TransportModel):
    """One persisted conflict with its evidence projection."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    conflict_id: str
    canonical_key: str = Field(min_length=1, max_length=64)
    classification: str
    source_references: list[ConflictReference]
    target_references: list[ConflictReference]
    differences: list[ConflictDifference]
    suggested_resolution: str | None
    created_at: str


class ConflictPageResponse(TransportModel):
    """Paginated conflict collection with run and fingerprint coherence."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    run_id: str
    run_version: int
    state: str
    observed_at: str
    reconciliation_fingerprint: str
    limit: int
    next_cursor: str | None
    items: list[ConflictResponse]
