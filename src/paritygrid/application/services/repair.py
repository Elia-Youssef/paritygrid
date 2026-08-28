"""Transport-facing composition of the accepted Phase 11 repair workflow.

The service owns no repair policy: it rebuilds the deterministic
reconciliation analysis from bounded transport inputs, delegates planning,
approval, and application to the Phase 11 services, and resolves the run's
target connector binding so application effects stay idempotent and fenced.
"""

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import cast

from paritygrid.adapters.connectors.warehouse_target import (
    WarehouseTargetConfig,
    WarehouseTargetConnector,
)
from paritygrid.application.planner.publication import PublishedPipelineSpecification
from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.application.ports.execution import RunRecord
from paritygrid.application.ports.operations import OperationalUnitOfWork
from paritygrid.application.ports.reconciliation_persistence import (
    ReconciliationSummaryRecord,
)
from paritygrid.application.ports.repair_audit import RepairPlanAggregate
from paritygrid.application.ports.writer import TransactionalWriter
from paritygrid.application.reconciliation.analysis import (
    ReconciliationAnalysisRequest,
    analyze_reconciliation,
)
from paritygrid.application.repair.applier import (
    RepairApplicationReport,
    RepairApplicationService,
)
from paritygrid.application.repair.approval import (
    RepairApprovalOutcome,
    RepairApprovalRequest,
    RepairApprovalService,
)
from paritygrid.application.repair.evidence import RepairWorkflowReader
from paritygrid.application.repair.planning_service import (
    CreatedRepairPlan,
    RepairPlanningService,
)
from paritygrid.application.services.errors import (
    OperationalRecordNotFoundError,
    OperationalRequestError,
)
from paritygrid.domain.models import (
    ConnectorId,
    RepairPlanId,
    RunId,
    StateFingerprint,
    UtcTimestamp,
)
from paritygrid.domain.reconciliation import SourceObservation

MAX_REPAIR_INPUT_OBSERVATIONS_PER_SIDE = 10_000
_FINGERPRINT_PATTERN = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)


@dataclass(frozen=True, slots=True)
class ObservationSide:
    """One bounded side of reconciliation observations from the transport."""

    connector_id: str
    input_identity: str
    observations: tuple[Mapping[str, object], ...]


@dataclass(frozen=True, slots=True)
class PlanView:
    """One plan aggregate with its coherent run and reconciliation context."""

    run: RunRecord
    summary: ReconciliationSummaryRecord | None
    aggregate: RepairPlanAggregate


@dataclass(frozen=True, slots=True)
class ApprovalView:
    """One approval outcome with its coherent run context."""

    run: RunRecord
    outcome: RepairApprovalOutcome


@dataclass(frozen=True, slots=True)
class ApplicationView:
    """One application outcome with its coherent run context."""

    run: RunRecord
    report: RepairApplicationReport


class RepairService:
    """Create, read, and approve repair plans through the Phase 11 services."""

    def __init__(
        self,
        *,
        writer: TransactionalWriter,
        reader: RepairWorkflowReader,
        unit_of_work: OperationalUnitOfWork,
        now: Callable[[], UtcTimestamp],
        timeout_seconds: float = 30.0,
    ) -> None:
        self._planning = RepairPlanningService(
            writer, reader, now=now, timeout_seconds=timeout_seconds
        )
        self._approval = RepairApprovalService(writer, reader, now=now)
        self._reader = reader
        self._unit_of_work = unit_of_work
        self._now = now

    def create_plan(
        self,
        *,
        run_id: str,
        source: ObservationSide,
        target: ObservationSide,
        actor: str,
        correlation_id: str,
    ) -> PlanView:
        """Regenerate the reconciliation analysis and create the one safe plan."""
        identity = _run_id(run_id)
        request = ReconciliationAnalysisRequest(
            source_observations=_observations(source, side_name="source"),
            target_observations=_observations(target, side_name="target"),
            source_input_identity=_input_identity(source, side_name="source"),
            target_input_identity=_input_identity(target, side_name="target"),
        )
        try:
            analysis = analyze_reconciliation(request)
        except ValueError as error:
            raise OperationalRequestError(
                "reconciliation observations violate the analytical contract",
                field="observations",
            ) from error
        created: CreatedRepairPlan = self._planning.create(
            run_id=identity,
            analysis=analysis,
            actor=actor,
            correlation_id=correlation_id,
        )
        return self._view(created.aggregate)

    def plan(self, plan_id: str) -> PlanView:
        """Return one durable plan aggregate with its run context."""
        return self._view(self._load(plan_id))

    def approve(
        self,
        *,
        plan_id: str,
        approved_by: str,
        approved_content_fingerprint: str,
        approved_reconciliation_fingerprint: str,
        correlation_id: str,
    ) -> ApprovalView:
        """Approve one plan fenced on the exact reviewed fingerprints."""
        aggregate = self._load(plan_id)
        outcome = self._approval.approve(
            RepairApprovalRequest(
                run_id=aggregate.plan.run_id,
                repair_plan_id=aggregate.plan.repair_plan_id,
                approved_by=approved_by,
                correlation_id=correlation_id,
                approved_content_fingerprint=_fingerprint(
                    approved_content_fingerprint, "approved_content_fingerprint"
                ),
                approved_reconciliation_fingerprint=_fingerprint(
                    approved_reconciliation_fingerprint, "approved_reconciliation_fingerprint"
                ),
                detail=RedactedDocument.from_mapping({"transport": "http"}),
            )
        )
        run = self._run_record(outcome.aggregate.plan.run_id)
        return ApprovalView(run=run, outcome=outcome)

    def _view(self, aggregate: RepairPlanAggregate | None) -> PlanView:
        if aggregate is None:
            raise OperationalRequestError(
                "the reconciliation snapshot permits no repairable actions",
                field="observations",
            )
        run = self._run_record(aggregate.plan.run_id)
        with self._unit_of_work.transaction() as repositories:
            summary = repositories.reconciliation.get_summary(aggregate.plan.run_id)
        return PlanView(run=run, summary=summary, aggregate=aggregate)

    def _load(self, plan_id: str) -> RepairPlanAggregate:
        aggregate = self._reader.load_plan(_plan_id(plan_id))
        if aggregate is None:
            raise OperationalRecordNotFoundError("repair plan", plan_id)
        return aggregate

    def _run_record(self, identity: RunId) -> RunRecord:
        with self._unit_of_work.transaction() as repositories:
            record = repositories.runs.get(identity)
        if record is None:
            raise OperationalRecordNotFoundError("run", identity.value)
        return record


class RepairApplyService:
    """Apply one approved plan through the run's bound warehouse target."""

    def __init__(
        self,
        *,
        writer: TransactionalWriter,
        reader: RepairWorkflowReader,
        unit_of_work: OperationalUnitOfWork,
        now: Callable[[], UtcTimestamp],
    ) -> None:
        self._applier = RepairApplicationService(writer, reader, now=now)
        self._reader = reader
        self._unit_of_work = unit_of_work

    async def apply(self, *, plan_id: str, context_id: str) -> ApplicationView:
        """Apply the plan idempotently and report the durable disposition."""
        aggregate = self._reader.load_plan(_plan_id(plan_id))
        if aggregate is None:
            raise OperationalRecordNotFoundError("repair plan", plan_id)
        identity = aggregate.plan.run_id
        target = await _open_target(self._resolve_target_binding(identity))
        try:
            report = await self._applier.apply(
                run_id=identity,
                repair_plan_id=aggregate.plan.repair_plan_id,
                target=target,
                context_id=context_id,
            )
        finally:
            await target.aclose()
        with self._unit_of_work.transaction() as repositories:
            run = repositories.runs.get(identity)
        if run is None:
            raise OperationalRecordNotFoundError("run", identity.value)
        return ApplicationView(run=run, report=report)

    def _resolve_target_binding(self, identity: RunId) -> str:
        """Return the base URL of the run pipeline's warehouse target binding."""
        with self._unit_of_work.transaction() as repositories:
            run = repositories.runs.get(identity)
            if run is None:
                raise OperationalRecordNotFoundError("run", identity.value)
            published = repositories.pipelines.get_version(run.pipeline_id, run.pipeline_version)
        if published is None:
            raise OperationalRecordNotFoundError(
                "pipeline version", f"{run.pipeline_id.value} v{run.pipeline_version.number}"
            )
        specification = PublishedPipelineSpecification.from_configuration_document(
            published.specification
        )
        kinds = {binding.connector_id: binding.kind for binding in specification.connector_bindings}
        warehouse_ids = {
            node.connector_id
            for node in specification.pipeline.nodes
            if node.connector_id is not None and kinds.get(node.connector_id) == "warehouse_target"
        }
        apply_ids = {
            node.connector_id
            for node in specification.pipeline.nodes
            if str(node.kind) == "repair.apply" and node.connector_id is not None
        }
        selected = apply_ids & warehouse_ids or warehouse_ids
        if len(selected) != 1:
            raise OperationalRequestError(
                "the run pipeline must bind exactly one warehouse target connector",
                field="pipeline",
            )
        binding_configuration = next(
            binding.configuration
            for binding in specification.connector_bindings
            if binding.connector_id in selected
        )
        base_url = binding_configuration.to_mapping().get("base_url")
        if type(base_url) is not str or not base_url:
            raise OperationalRequestError(
                "the warehouse target binding must configure a base url",
                field="pipeline",
            )
        return base_url


async def _open_target(base_url: str) -> WarehouseTargetConnector:
    try:
        connector = WarehouseTargetConnector(WarehouseTargetConfig(base_url))
    except Exception as error:
        raise OperationalRequestError(
            "the warehouse target binding is not usable",
            field="pipeline",
        ) from error
    await connector.open_async()
    return connector


def _observations(side: ObservationSide, *, side_name: str) -> tuple[SourceObservation, ...]:
    if len(side.observations) > MAX_REPAIR_INPUT_OBSERVATIONS_PER_SIDE:
        raise OperationalRequestError(
            "one reconciliation side exceeds the observation limit",
            field=f"{side_name}_observations",
        )
    connector = _connector_id(side.connector_id, side=side_name)
    records: list[SourceObservation] = []
    for item in side.observations:
        position = item.get("position")
        payload = item.get("payload")
        reason = item.get("malformed_reason")
        if type(position) is not int:
            raise OperationalRequestError(
                "each observation requires an integer position",
                field=f"{side_name}_observations",
            )
        if payload is not None and not isinstance(payload, Mapping):
            raise OperationalRequestError(
                "observation payloads must be objects",
                field=f"{side_name}_observations",
            )
        if reason is not None and type(reason) is not str:
            raise OperationalRequestError(
                "observation malformed reasons must be text",
                field=f"{side_name}_observations",
            )
        try:
            records.append(
                SourceObservation(
                    position=position,
                    connector_id=connector,
                    payload=cast("Mapping[str, object] | None", payload),
                    malformed_reason=reason,
                )
            )
        except ValueError as error:
            raise OperationalRequestError(
                str(error),
                field=f"{side_name}_observations",
            ) from error
    return tuple(records)


def _input_identity(side: ObservationSide, *, side_name: str) -> str:
    value = side.input_identity
    if type(value) is not str or _FINGERPRINT_PATTERN.fullmatch(value) is None:
        raise OperationalRequestError(
            "input identities must use the lowercase sha256 format",
            field=f"{side_name}_input_identity",
        )
    return value


def _connector_id(value: str, *, side: str) -> ConnectorId:
    try:
        return ConnectorId.parse(value)
    except ValueError as error:
        raise OperationalRequestError(
            "connector identity must use the canonical connector format",
            field=f"{side}_connector_id",
        ) from error


def _fingerprint(value: str, field: str) -> StateFingerprint:
    if type(value) is not str or _FINGERPRINT_PATTERN.fullmatch(value) is None:
        raise OperationalRequestError(
            "fingerprints must use the lowercase sha256 format",
            field=field,
        )
    return StateFingerprint(value)


def _run_id(run_id: str) -> RunId:
    try:
        return RunId.parse(run_id)
    except ValueError as error:
        raise OperationalRequestError(
            "run identity must use the canonical run format",
            field="run_id",
        ) from error


def _plan_id(plan_id: str) -> RepairPlanId:
    try:
        return RepairPlanId.parse(plan_id)
    except ValueError as error:
        raise OperationalRequestError(
            "repair plan identity must use the canonical repair plan format",
            field="plan_id",
        ) from error


__all__ = [
    "MAX_REPAIR_INPUT_OBSERVATIONS_PER_SIDE",
    "ApplicationView",
    "ApprovalView",
    "ObservationSide",
    "PlanView",
    "RepairApplyService",
    "RepairService",
]
