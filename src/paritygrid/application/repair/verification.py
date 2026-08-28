"""Independent target-state observation and parity verification.

The verifier reads the observed target through the Phase 9 target
connector after repair effects and computes the Phase 11 target-state
fingerprint from those independently observed records alone. The expected
fingerprint is computed separately from the expected post-repair
inventory; parity holds only when the two fingerprints and the observed
record count agree exactly, no expected key diverges, and no key expected
to be absent is present. The observed fingerprint is never derived from
the repair plan, the expected state, the reconciliation fingerprint, or
any execution-evidence digest.
"""

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from paritygrid.application.ports.connector_redaction import redact_exception
from paritygrid.application.ports.connectors import (
    NEVER_CANCELLED,
    ConnectorCallContext,
    ConnectorCancellationToken,
    ConnectorCancelledError,
    TargetConnector,
)
from paritygrid.application.ports.consistency import RedactedDocument
from paritygrid.application.ports.reconciliation_persistence import (
    TargetVerificationRecord,
    TargetVerificationVerdict,
)
from paritygrid.application.ports.repair_audit import RepairPlanStatus
from paritygrid.application.ports.writer import TransactionalWriter
from paritygrid.application.reconciliation.analysis import ReconciliationAnalysis
from paritygrid.application.repair.companions import (
    build_companions,
    frontier_from_evidence,
    submit_command,
)
from paritygrid.application.repair.errors import (
    RepairPlanMismatchError,
    RepairReconciliationMissingError,
    RepairReconciliationStaleError,
)
from paritygrid.application.repair.evidence import RepairWorkflowReader
from paritygrid.application.repair.identities import derive_verification_id
from paritygrid.application.repair.payloads import parse_observed_payload
from paritygrid.application.writes.reconciliation import (
    RecordTargetVerification,
    RecordTargetVerificationResult,
)
from paritygrid.domain.canonical import (
    FingerprintScope,
    encode_inventory_observation,
    fingerprint_state,
)
from paritygrid.domain.canonical.encoding import CanonicalVersion
from paritygrid.domain.models import (
    InventoryRecord,
    RepairPlanId,
    RunId,
    StateFingerprint,
    UtcTimestamp,
)
from paritygrid.domain.reconciliation import ReconciliationOutcome
from paritygrid.domain.repair import (
    TARGET_OBSERVATION_VERSION,
    TARGET_STATE_FINGERPRINT_KIND,
    TARGET_STATE_FINGERPRINT_VERSION,
    RepairPlan,
    TargetStateIdentity,
    compute_target_state_fingerprint,
)

MAX_DIVERGENCE_EVIDENCE = 64
MAX_DIVERGENCE_DETAIL_ENTRIES = 8
_MAX_FINGERPRINT_ITEMS = 10_000
UNOBSERVED_FINGERPRINT = StateFingerprint("0" * 64)
"""The explicit zero sentinel stored when an observation failed before reading."""


class TargetObservationDisposition(StrEnum):
    """Closed outcomes of one independent observation attempt."""

    OBSERVED = "observed"
    INTERRUPTED = "interrupted"
    OBSERVATION_FAILED = "observation_failed"


@dataclass(frozen=True, slots=True)
class ExpectedInventory:
    """The expected post-repair target inventory for one reconciliation."""

    records: tuple[InventoryRecord, ...]
    absent_keys: tuple[str, ...]
    ambiguous_keys: tuple[str, ...]

    def __post_init__(self) -> None:
        skus = [record.sku for record in self.records]
        if skus != sorted(skus) or len(set(skus)) != len(skus):
            raise ValueError("expected inventory must use sorted unique keys")
        if len(self.records) > _MAX_FINGERPRINT_ITEMS:
            raise ValueError("expected inventory exceeds the fingerprint bound")
        for name in ("absent_keys", "ambiguous_keys"):
            keys = getattr(self, name)
            if tuple(sorted(set(keys))) != keys or any(
                type(key) is not str or not key for key in keys
            ):
                raise ValueError(f"expected inventory {name} must use sorted unique keys")
        record_keys = set(skus)
        if record_keys.intersection(self.absent_keys):
            raise ValueError("expected inventory keys must not overlap")


@dataclass(frozen=True, slots=True)
class InventoryDivergence:
    """Bounded evidence for one canonical key that failed parity."""

    canonical_key: str
    reason: str


@dataclass(frozen=True, slots=True)
class TargetVerificationReport:
    """One complete independently observed verification result."""

    disposition: TargetObservationDisposition
    verdict: TargetVerificationVerdict | None
    observed: TargetStateIdentity | None
    expected_fingerprint: StateFingerprint
    expected_record_count: int
    observed_record_count: int | None
    divergences: tuple[InventoryDivergence, ...]
    observed_target_version: int | None
    observed_at: UtcTimestamp
    detail: str | None

    def __post_init__(self) -> None:
        observed = self.disposition is TargetObservationDisposition.OBSERVED
        if observed != (self.verdict is not None) or observed != (self.observed is not None):
            raise ValueError("an observed report must carry its verdict and identity")


def build_expected_inventory(
    analysis: ReconciliationAnalysis, plan: RepairPlan | None
) -> ExpectedInventory:
    """Build the expected post-repair inventory for one snapshot and plan."""
    if type(analysis) is not ReconciliationAnalysis:
        raise TypeError("expected inventory requires ReconciliationAnalysis")
    if plan is not None and type(plan) is not RepairPlan:
        raise TypeError("expected inventory requires RepairPlan or None")
    repairs = {} if plan is None else {action.sku: action for action in plan.actions}
    records: list[InventoryRecord] = []
    absent: list[str] = []
    ambiguous: list[str] = []
    for key in analysis.classification.keys:
        outcome = key.outcome
        action = repairs.get(outcome.sku)
        if action is not None:
            records.append(action.proposed_record)
            continue
        if not outcome.target_records:
            absent.append(outcome.sku)
            continue
        records.append(_expected_target_record(outcome, ambiguous))
    return ExpectedInventory(
        records=tuple(sorted(records, key=lambda item: item.sku)),
        absent_keys=tuple(sorted(set(absent))),
        ambiguous_keys=tuple(sorted(set(ambiguous))),
    )


def expected_fingerprint(inventory: ExpectedInventory) -> StateFingerprint:
    """Compute the expected target-state fingerprint for one inventory."""
    return compute_target_state_fingerprint(
        observation_version=TARGET_OBSERVATION_VERSION,
        record_count=len(inventory.records),
        inventory_digest=fingerprint_state(
            inventory.records, scope=FingerprintScope.TARGET_OBSERVATION_STATE
        ),
    )


class TargetParityVerifier:
    """Observe the target independently and prove or refute parity."""

    def __init__(self, *, now: Callable[[], UtcTimestamp]) -> None:
        self._now = now

    async def verify(
        self,
        *,
        target: TargetConnector,
        inventory: ExpectedInventory,
        context_id: str,
        cancellation: ConnectorCancellationToken = NEVER_CANCELLED,
    ) -> TargetVerificationReport:
        """Enumerate the target independently and compare complete inventories."""
        context = ConnectorCallContext(correlation_id=context_id, cancellation_token=cancellation)
        observed_at = self._now()
        expected = expected_fingerprint(inventory)
        try:
            snapshot = await target.state_snapshot_async(context)
        except ConnectorCancelledError:
            return _interrupted_report(observed_at, expected, len(inventory.records))
        except Exception as error:
            return _failed_report(observed_at, expected, len(inventory.records), error)
        if snapshot.record_count > _MAX_FINGERPRINT_ITEMS:
            return _failed_report(
                observed_at,
                expected,
                len(inventory.records),
                RuntimeError("the observed target exceeds the verification inventory bound"),
            )
        try:
            observed_records: tuple[InventoryRecord, ...] = await self._observe_inventory(
                target, context
            )
        except ConnectorCancelledError:
            return _interrupted_report(observed_at, expected, len(inventory.records))
        except Exception as error:
            return _failed_report(observed_at, expected, len(inventory.records), error)
        divergences = _inventory_divergences(inventory, observed_records)
        divergences.extend(
            InventoryDivergence(key, "expected content is ambiguous after duplicate review")
            for key in inventory.ambiguous_keys
        )
        # The observation must be one coherent cut of the target: a target
        # that changed while it was being read would pair a stale count with
        # fresh contents, so the bounding snapshots must agree exactly. A
        # change during observation fails closed instead of guessing.
        try:
            closing = await target.state_snapshot_async(context)
        except ConnectorCancelledError:
            return _interrupted_report(observed_at, expected, len(inventory.records))
        except Exception as error:
            return _failed_report(observed_at, expected, len(inventory.records), error)
        if (
            closing.record_count != snapshot.record_count
            or closing.target_version != snapshot.target_version
            or closing.content_fingerprint != snapshot.content_fingerprint
            or closing.record_count != len(observed_records)
        ):
            return _failed_report(
                observed_at,
                expected,
                len(inventory.records),
                RuntimeError("the target changed while it was being observed"),
            )
        observed_digest = fingerprint_state(
            observed_records,
            scope=FingerprintScope.TARGET_OBSERVATION_STATE,
        )
        identity = TargetStateIdentity(
            fingerprint_kind=TARGET_STATE_FINGERPRINT_KIND,
            fingerprint_version=TARGET_STATE_FINGERPRINT_VERSION,
            observation_version=TARGET_OBSERVATION_VERSION,
            record_count=closing.record_count,
            fingerprint=compute_target_state_fingerprint(
                observation_version=TARGET_OBSERVATION_VERSION,
                record_count=closing.record_count,
                inventory_digest=observed_digest,
            ),
        )
        parity_holding = (
            identity.fingerprint == expected
            and closing.record_count == len(inventory.records)
            and not divergences
        )
        return TargetVerificationReport(
            disposition=TargetObservationDisposition.OBSERVED,
            verdict=(
                TargetVerificationVerdict.PARITY_HOLDING
                if parity_holding
                else TargetVerificationVerdict.PARITY_DIVERGENT
            ),
            observed=identity,
            expected_fingerprint=expected,
            expected_record_count=len(inventory.records),
            observed_record_count=closing.record_count,
            divergences=tuple(divergences[:MAX_DIVERGENCE_EVIDENCE]),
            observed_target_version=closing.target_version,
            observed_at=observed_at,
            detail=None,
        )

    async def _observe_inventory(
        self,
        target: TargetConnector,
        context: ConnectorCallContext,
    ) -> tuple[InventoryRecord, ...]:
        """Return the entire bounded target inventory from cursor pages.

        No partial inventory receives a target-state fingerprint: malformed,
        repeated, cyclic, or over-bound pages fail observation instead.  The
        opening and closing target snapshots in :meth:`verify` then prove that
        this enumeration observed one coherent target cut.
        """
        cursor: str | None = None
        seen_cursors: set[str] = set()
        seen_keys: set[str] = set()
        records: list[InventoryRecord] = []
        while True:
            context.raise_if_cancelled()
            page = await target.list_records_async(cursor, context)
            for target_record in page.records:
                context.raise_if_cancelled()
                if target_record.sku in seen_keys:
                    raise RuntimeError("the target enumeration repeated a record key")
                seen_keys.add(target_record.sku)
                parsed = parse_observed_payload(len(records), target_record.payload)
                if parsed.record is None:
                    raise RuntimeError("the target enumeration contains an unparsable record")
                records.append(parsed.record)
                if len(records) > _MAX_FINGERPRINT_ITEMS:
                    raise RuntimeError(
                        "the target enumeration exceeds the verification inventory bound"
                    )
            next_cursor = page.next_cursor
            if next_cursor is None:
                return tuple(records)
            if next_cursor in seen_cursors:
                raise RuntimeError("the target enumeration cursor repeated")
            seen_cursors.add(next_cursor)
            if len(seen_cursors) > _MAX_FINGERPRINT_ITEMS:
                raise RuntimeError("the target enumeration exceeds the page bound")
            cursor = next_cursor


class TargetVerificationService:
    """Persist one verification fact through the transactional writer."""

    def __init__(
        self,
        writer: TransactionalWriter,
        reader: RepairWorkflowReader,
        *,
        now: Callable[[], UtcTimestamp],
        timeout_seconds: float = 30.0,
    ) -> None:
        self._writer = writer
        self._reader = reader
        self._now = now
        self._timeout_seconds = timeout_seconds

    async def verify_and_record(
        self,
        *,
        run_id: RunId,
        target: TargetConnector,
        inventory: ExpectedInventory,
        reconciliation_fingerprint: StateFingerprint,
        repair_plan_id: RepairPlanId,
        plan_content_fingerprint: StateFingerprint,
        actor: str,
        correlation_id: str,
        cancellation: ConnectorCancellationToken = NEVER_CANCELLED,
    ) -> TargetVerificationRecord:
        """Independently observe and persist one repair-backed verification."""
        report = await TargetParityVerifier(now=self._now).verify(
            target=target,
            inventory=inventory,
            context_id=correlation_id,
            cancellation=cancellation,
        )
        return self._record(
            run_id=run_id,
            report=report,
            reconciliation_fingerprint=reconciliation_fingerprint,
            repair_plan_id=repair_plan_id,
            plan_content_fingerprint=plan_content_fingerprint,
            actor=actor,
            correlation_id=correlation_id,
            owned_observation=True,
        )

    def record(
        self,
        *,
        run_id: RunId,
        report: TargetVerificationReport,
        reconciliation_fingerprint: StateFingerprint,
        repair_plan_id: RepairPlanId | None,
        plan_content_fingerprint: StateFingerprint | None,
        actor: str,
        correlation_id: str,
    ) -> TargetVerificationRecord:
        """Record the immutable verification fact for one observed target state."""
        return self._record(
            run_id=run_id,
            report=report,
            reconciliation_fingerprint=reconciliation_fingerprint,
            repair_plan_id=repair_plan_id,
            plan_content_fingerprint=plan_content_fingerprint,
            actor=actor,
            correlation_id=correlation_id,
            owned_observation=False,
        )

    def _record(
        self,
        *,
        run_id: RunId,
        report: TargetVerificationReport,
        reconciliation_fingerprint: StateFingerprint,
        repair_plan_id: RepairPlanId | None,
        plan_content_fingerprint: StateFingerprint | None,
        actor: str,
        correlation_id: str,
        owned_observation: bool,
    ) -> TargetVerificationRecord:
        """Persist a report after the public ownership and plan fences."""
        if type(run_id) is not RunId:
            raise TypeError("verification recording requires RunId")
        if type(report) is not TargetVerificationReport:
            raise TypeError("verification recording requires TargetVerificationReport")
        if type(reconciliation_fingerprint) is not StateFingerprint:
            raise TypeError("verification recording requires StateFingerprint")
        evidence = self._reader.load(run_id)
        if evidence.summary is None:
            raise RepairReconciliationMissingError("the run has no reconciliation snapshot")
        current = evidence.summary.reconciliation_fingerprint
        if current != reconciliation_fingerprint:
            raise RepairReconciliationStaleError(
                expected=current.value, actual=reconciliation_fingerprint.value
            )
        if repair_plan_id is None:
            if plan_content_fingerprint is not None:
                raise RepairPlanMismatchError("verification plan content requires a repair plan")
        else:
            if plan_content_fingerprint is None:
                raise RepairPlanMismatchError(
                    "verification repair plan requires its content fingerprint"
                )
            aggregate = self._reader.load_plan(repair_plan_id)
            if aggregate is None:
                raise RepairPlanMismatchError("verified repair plan does not exist")
            if aggregate.plan.run_id != run_id:
                raise RepairPlanMismatchError("verified repair plan belongs to another run")
            if aggregate.plan.status is not RepairPlanStatus.APPLIED:
                raise RepairPlanMismatchError("verified repair plan has not been applied")
            if aggregate.plan.reconciliation_fingerprint != reconciliation_fingerprint:
                raise RepairPlanMismatchError(
                    "verified repair plan addresses another reconciliation"
                )
            if aggregate.plan.content_fingerprint != plan_content_fingerprint:
                raise RepairPlanMismatchError("verified repair plan contents do not match")
        verdict = _recordable_verdict(report)
        if verdict is TargetVerificationVerdict.PARITY_HOLDING and not owned_observation:
            raise RepairPlanMismatchError("parity-holding verification must use verify_and_record")
        observed = report.observed
        if verdict is not TargetVerificationVerdict.OBSERVATION_FAILED and observed is None:
            raise RepairPlanMismatchError("verification report carries no observation")
        occurred_at = self._now()
        # A failed observation has no observed fingerprint: the stored column
        # is the explicit zero sentinel, never a copy of the expected value,
        # so naive fingerprint equality can never read as parity.
        observed_fingerprint = (
            observed.fingerprint if observed is not None else UNOBSERVED_FINGERPRINT
        )
        verification = TargetVerificationRecord(
            verification_id=derive_verification_id(
                run_id,
                reconciliation_fingerprint,
                observed_fingerprint,
            ),
            run_id=run_id,
            repair_plan_id=repair_plan_id,
            reconciliation_fingerprint=reconciliation_fingerprint,
            plan_content_fingerprint=plan_content_fingerprint,
            observed_fingerprint=observed_fingerprint,
            observed_fingerprint_version=TARGET_STATE_FINGERPRINT_VERSION,
            expected_fingerprint=report.expected_fingerprint,
            verdict=verdict,
            observed_record_count=_count_or_zero(report.observed_record_count),
            expected_record_count=report.expected_record_count,
            observed_target_version=_count_or_zero(report.observed_target_version),
            observed_at=occurred_at,
            detail=_verification_detail(report),
        )
        existing = self._reader.load_target_verification(verification.verification_id)
        if existing is not None:
            # Recording the same observed state under its derived identity
            # returns the immutable stored fact instead of submitting a second
            # mutation whose fresh timestamp could not replay it.
            _require_same_verification(existing, verification)
            return existing
        companions = build_companions(
            frontier=frontier_from_evidence(evidence),
            run_id=run_id,
            operation="target_state_verified",
            object_kind="target_state_verification",
            object_id=verification.verification_id.value,
            actor=actor,
            correlation_id=correlation_id,
            occurred_at=occurred_at,
            payload={
                "divergence_count": len(report.divergences),
                "observed_fingerprint": verification.observed_fingerprint.value,
                "verdict": verdict.value,
            },
        )
        command = RecordTargetVerification(
            run_id=run_id,
            verification=verification,
            companions=companions,
        )
        _, result, _mutated = submit_command(
            self._writer, command, timeout_seconds=self._timeout_seconds
        )
        return cast(RecordTargetVerificationResult, result).record


def _require_same_verification(
    stored: TargetVerificationRecord, requested: TargetVerificationRecord
) -> None:
    from paritygrid.application.ports.reconciliation_persistence import (
        TargetVerificationConflictError,
    )

    if (
        stored.run_id != requested.run_id
        or stored.repair_plan_id != requested.repair_plan_id
        or stored.reconciliation_fingerprint != requested.reconciliation_fingerprint
        or stored.plan_content_fingerprint != requested.plan_content_fingerprint
        or stored.observed_fingerprint != requested.observed_fingerprint
        or stored.observed_fingerprint_version != requested.observed_fingerprint_version
        or stored.expected_fingerprint != requested.expected_fingerprint
        or stored.verdict is not requested.verdict
        or stored.observed_record_count != requested.observed_record_count
        or stored.expected_record_count != requested.expected_record_count
        or stored.observed_target_version != requested.observed_target_version
        or stored.detail.to_mapping() != requested.detail.to_mapping()
    ):
        raise TargetVerificationConflictError(
            "target verification replay differs from durable state"
        )


def _expected_target_record(
    outcome: ReconciliationOutcome, ambiguous: list[str]
) -> InventoryRecord:
    distinct = {_observation_bytes(record) for record in outcome.target_records}
    if len(distinct) > 1:
        ambiguous.append(outcome.sku)
    return outcome.target_records[0]


def _inventory_divergences(
    expected: ExpectedInventory,
    observed_records: tuple[InventoryRecord, ...],
) -> list[InventoryDivergence]:
    """Compare the complete independently enumerated target inventory."""
    observed = {record.sku: record for record in observed_records}
    divergences: list[InventoryDivergence] = []
    for expected_record in expected.records:
        actual = observed.pop(expected_record.sku, None)
        if actual is None:
            divergences.append(InventoryDivergence(expected_record.sku, "target record is missing"))
        elif _observation_bytes(actual) != _observation_bytes(expected_record):
            divergences.append(
                InventoryDivergence(expected_record.sku, "target record content differs")
            )
    divergences.extend(
        InventoryDivergence(key, "unexpected target record is present")
        for key in expected.absent_keys
        if observed.pop(key, None) is not None
    )
    divergences.extend(
        InventoryDivergence(key, "unexpected target record is present") for key in sorted(observed)
    )
    return divergences


def _observation_bytes(record: InventoryRecord) -> bytes:
    return encode_inventory_observation(record, version=CanonicalVersion.V1)


def _failed_report(
    observed_at: UtcTimestamp,
    expected: StateFingerprint,
    expected_count: int,
    error: BaseException,
) -> TargetVerificationReport:
    return TargetVerificationReport(
        disposition=TargetObservationDisposition.OBSERVATION_FAILED,
        verdict=None,
        observed=None,
        expected_fingerprint=expected,
        expected_record_count=expected_count,
        observed_record_count=None,
        divergences=(),
        observed_target_version=None,
        observed_at=observed_at,
        detail=redact_exception(error),
    )


def _interrupted_report(
    observed_at: UtcTimestamp, expected: StateFingerprint, expected_count: int
) -> TargetVerificationReport:
    return TargetVerificationReport(
        disposition=TargetObservationDisposition.INTERRUPTED,
        verdict=None,
        observed=None,
        expected_fingerprint=expected,
        expected_record_count=expected_count,
        observed_record_count=None,
        divergences=(),
        observed_target_version=None,
        observed_at=observed_at,
        detail="the observation was cancelled before completing",
    )


def _recordable_verdict(report: TargetVerificationReport) -> TargetVerificationVerdict:
    if report.disposition is TargetObservationDisposition.OBSERVATION_FAILED:
        return TargetVerificationVerdict.OBSERVATION_FAILED
    if report.disposition is TargetObservationDisposition.INTERRUPTED:
        raise RepairPlanMismatchError(
            "a cancelled observation proved nothing and cannot be recorded"
        )
    verdict = report.verdict
    if verdict is None:
        raise RepairPlanMismatchError("verification report carries no verdict")
    return verdict


def _count_or_zero(value: int | None) -> int:
    return value if value is not None else 0


def _verification_detail(report: TargetVerificationReport) -> RedactedDocument:
    # The full divergence list stays on the report; the durable fact keeps a
    # bounded prefix so a heavily divergent observation remains recordable.
    bounded = {
        item.canonical_key: item.reason
        for item in report.divergences[:MAX_DIVERGENCE_DETAIL_ENTRIES]
    }
    return RedactedDocument.from_mapping(
        {
            "detail": report.detail if report.detail is not None else "",
            "divergence_count": len(report.divergences),
            "divergences": bounded,
            "observed_target_version": report.observed_target_version,
        }
    )


__all__ = [
    "MAX_DIVERGENCE_DETAIL_ENTRIES",
    "MAX_DIVERGENCE_EVIDENCE",
    "ExpectedInventory",
    "InventoryDivergence",
    "TargetObservationDisposition",
    "TargetParityVerifier",
    "TargetVerificationReport",
    "TargetVerificationService",
    "build_expected_inventory",
    "expected_fingerprint",
]
