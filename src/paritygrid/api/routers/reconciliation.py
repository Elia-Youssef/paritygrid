"""Reconciliation read routes."""

from typing import Annotated

from fastapi import APIRouter, Query, Request

from paritygrid.api.dependencies import get_services
from paritygrid.api.schemas.reconciliation import (
    ConflictDifference,
    ConflictPageResponse,
    ConflictReference,
    ConflictResponse,
    ReconciliationResponse,
)
from paritygrid.application.ports.execution import RunRecord
from paritygrid.application.ports.reconciliation_persistence import (
    PersistedConflict,
    ReconciliationSummaryRecord,
)
from paritygrid.domain.reconciliation import ReconciliationClassification

router = APIRouter(prefix="/api/v1/runs", tags=["reconciliation"])

_CANONICAL_CURSOR_PATTERN = r"^[A-Z0-9]+(?:-[A-Z0-9]+)*$"
_MAX_CONFLICT_CURSOR_LENGTH = 64


@router.get("/{run_id}/reconciliation", response_model=ReconciliationResponse)
def get_reconciliation(run_id: str, request: Request) -> ReconciliationResponse:
    services = get_services(request)
    snapshot = services.reconciliation.snapshot(run_id)
    return _summary_response(snapshot.run, snapshot.summary, observed_at=services.clock())


@router.get("/{run_id}/conflicts", response_model=ConflictPageResponse)
def list_conflicts(
    run_id: str,
    request: Request,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    cursor: Annotated[
        str | None,
        Query(
            min_length=1,
            max_length=_MAX_CONFLICT_CURSOR_LENGTH,
            pattern=_CANONICAL_CURSOR_PATTERN,
        ),
    ] = None,
) -> ConflictPageResponse:
    services = get_services(request)
    view = services.reconciliation.conflicts(run_id=run_id, limit=limit, after=cursor)
    return ConflictPageResponse(
        run_id=view.run.run_id.value,
        run_version=view.run.row_version,
        state=view.run.state.value,
        observed_at=str(services.clock()),
        reconciliation_fingerprint=view.summary.reconciliation_fingerprint.value,
        limit=limit,
        next_cursor=view.page.next_cursor,
        items=[_conflict_response(item) for item in view.page.items],
    )


def _summary_response(
    run: RunRecord, summary: ReconciliationSummaryRecord, *, observed_at: object
) -> ReconciliationResponse:
    counts = {
        classification.value: summary.count(classification)
        for classification in ReconciliationClassification
    }
    return ReconciliationResponse(
        run_id=run.run_id.value,
        run_version=run.row_version,
        state=run.state.value,
        observed_at=str(observed_at),
        reconciliation_fingerprint=summary.reconciliation_fingerprint.value,
        source_input_identity=summary.source_fingerprint.value,
        target_input_identity=summary.target_fingerprint.value,
        total_count=summary.total_count,
        counts=counts,
        analytical_query_version=summary.analytical_query_version,
        reconciliation_observed_at=str(summary.created_at),
    )


def _conflict_response(conflict: PersistedConflict) -> ConflictResponse:
    return ConflictResponse(
        conflict_id=conflict.conflict_id.value,
        canonical_key=conflict.canonical_key,
        classification=conflict.classification.value,
        source_references=[
            ConflictReference(position=position, record_key=record_key)
            for position, record_key in conflict.source_references
        ],
        target_references=[
            ConflictReference(position=position, record_key=record_key)
            for position, record_key in conflict.target_references
        ],
        differences=[
            ConflictDifference(
                field=difference.field,
                kind=difference.kind.value,
                source_text=difference.source_text,
                target_text=difference.target_text,
            )
            for difference in conflict.differences
        ],
        suggested_resolution=(
            None if conflict.suggested_resolution is None else conflict.suggested_resolution.value
        ),
        created_at=str(conflict.created_at),
    )
