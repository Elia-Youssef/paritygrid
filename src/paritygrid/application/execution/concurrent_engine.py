"""The concurrent run engine: parent-owned admission through durable results.

:class:`ConcurrentRunEngine` is the parent orchestrator every full-plan
strategy runs behind.  It alone owns the concurrent scheduler, the
scheduled-work capacity ledger, the work-lease service, the result
coordinator, the bounded channel set, the lifecycle coordinator, and
the cleanup registry.  Workers — threads, tasks, or the inline
sequential worker — only ever see assignment envelopes and return
result envelopes.

The engine's loop preserves the Phase 7 invariants: capacity is held
from admission until durable acknowledgement; dependencies are
released only after commit; a rejected result retains its lease for
one corrected resubmission; a known writer rollback retries boundedly;
an unknown outcome stops admission and marks the run
recovery-required; pause quiesces at a stable durable boundary with a
single compare-and-set winner; cancellation closes producer flow in
dependency order and synthesizes durable cancelled results for work
still in flight; and every owned resource closes through the bounded
idempotent cleanup registry with structured unresolved evidence.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol, cast, runtime_checkable

from paritygrid.application.execution.capacity import (
    CapacityTimeoutError,
    ScheduledWorkLimiters,
)
from paritygrid.application.execution.channels import (
    ChannelClosedError,
    ChannelSet,
    ChannelTimeoutError,
)
from paritygrid.application.execution.concurrency_settings import (
    CapturedConcurrencySettings,
)
from paritygrid.application.execution.concurrent_cleanup import (
    ConcurrentCleanupCoordinator,
)
from paritygrid.application.execution.concurrent_lifecycle import (
    ConcurrentLifecycleCoordinator,
    ConcurrentLifecycleError,
    ConcurrentLifecycleRejectedError,
    ConcurrentLifecycleReport,
    ConcurrentPausedProof,
    ConcurrentPauseSignal,
)
from paritygrid.application.execution.concurrent_scheduler import (
    ConcurrentScheduler,
    SchedulerFrontierV2,
    WorkIdentity,
)
from paritygrid.application.execution.full_plan_strategy import (
    ExecutedWork,
    FullPlanStrategy,
    ResultFactsRegistry,
    StrategyContext,
    StrategyMode,
    WorkOperationExecutor,
    require_strategy_timeout,
)
from paritygrid.application.execution.leasing import (
    AcquireWorkLeaseRequest,
    WorkLease,
    WorkLeaseCompletionDisposition,
    WorkLeasePauseReservation,
    WorkLeaseService,
)
from paritygrid.application.execution.result_coordinator import (
    ConcurrentResultCoordinator,
    RegisteredAssignment,
    ResultCoordinatorClosedError,
    ResultForgedReferenceRejection,
    ResultOutcomeUnknownError,
    ResultStaleRejection,
    ResultValidationRejection,
    ResultWriterRetryableError,
)
from paritygrid.application.execution.runner import CancellationToken
from paritygrid.application.execution.runner_contract import (
    RUNNER_CONTRACT_VERSION,
    WORK_ASSIGNMENT_PROTOCOL,
    WORK_RESULT_PROTOCOL,
    ContractCleanupEvidence,
    ContractCleanupStatus,
    ContractDocument,
    ContractOutcome,
    ControlGeneration,
    WorkAssignmentV1,
    WorkResultV1,
)
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.writer import EventAppendRequest
from paritygrid.domain.models import AttemptNumber, NodeId, RunId, UtcTimestamp, WorkItemId

CONCURRENT_ENGINE_VERSION = 1
MAX_ENGINE_REDISPATCH = 1
MAX_ENGINE_WRITER_RETRIES = 3
MAX_ENGINE_NODE_KIND_LENGTH = 64
MAX_ENGINE_LEASE_OWNER_LENGTH = 128
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_CARRIER_LEASE_EVENT_PAYLOAD_SCHEMA = 1


class ConcurrentEngineError(RuntimeError):
    """Base failure for the concurrent engine."""


class ConcurrentEngineInvalidRequestError(ConcurrentEngineError):
    """An engine request violated the orchestration contract."""


class ConcurrentEngineStateError(ConcurrentEngineError):
    """The engine lifecycle state rejected the request."""


class EngineStatus(StrEnum):
    """Terminal status of one engine run pass."""

    COMPLETED = "completed"
    PAUSED = "paused"
    CANCELLED = "cancelled"
    RECOVERY_REQUIRED = "recovery_required"


@dataclass(frozen=True, slots=True)
class AdmissionFacts:
    """Current durable admission evidence for one ready identity."""

    work_item_id: str
    partition_key: str
    work_row_version: int
    node_row_version: int
    run_row_version: int
    completed_attempt_count: int
    next_event_sequence: int
    event_counter_row_version: int
    state: str

    def __post_init__(self) -> None:
        for field in ("work_item_id", "partition_key"):
            value = getattr(self, field)
            if type(value) is not str or not 1 <= len(value) <= 128:
                raise ConcurrentEngineInvalidRequestError(f"admission {field} is invalid")
        for field in (
            "work_row_version",
            "node_row_version",
            "run_row_version",
            "next_event_sequence",
            "event_counter_row_version",
        ):
            value = getattr(self, field)
            if type(value) is not int or not 1 <= value <= 2_147_483_647:
                raise ConcurrentEngineInvalidRequestError(
                    f"admission {field} is outside the supported range"
                )
        if type(self.completed_attempt_count) is not int or not (
            0 <= self.completed_attempt_count <= 2_147_483_647
        ):
            raise ConcurrentEngineInvalidRequestError(
                "admission attempt count is outside the supported range"
            )
        if self.state not in _ADMISSION_STATES:
            raise ConcurrentEngineInvalidRequestError("admission state is unknown")


_ADMISSION_STATES = frozenset({"pending", "retry_wait"})


@runtime_checkable
class AdmissionStateReader(Protocol):
    """Reads current durable admission evidence for one identity."""

    def read(self, run_id: str, node_id: str, partition_key: str) -> AdmissionFacts: ...


@runtime_checkable
class EngineClock(Protocol):
    """Injected clock contract for engine decisions."""

    def now(self) -> UtcTimestamp: ...


@dataclass(frozen=True, slots=True)
class ConcurrentRunReport:
    """Terminal outcome of one engine pass."""

    status: EngineStatus
    frontier: SchedulerFrontierV2
    pause_proof: ConcurrentPausedProof | None
    recovery_reason: str | None
    admitted_count: int
    committed_count: int

    def __post_init__(self) -> None:
        if type(self.status) is not EngineStatus:
            raise TypeError("engine report status must use EngineStatus")
        if type(self.frontier) is not SchedulerFrontierV2:
            raise TypeError("engine report frontier must use SchedulerFrontierV2")
        if self.status is EngineStatus.PAUSED and self.pause_proof is None:
            raise ConcurrentEngineInvalidRequestError(
                "a paused report must carry its durable pause proof"
            )
        if self.status is EngineStatus.RECOVERY_REQUIRED and not self.recovery_reason:
            raise ConcurrentEngineInvalidRequestError(
                "a recovery-required report must carry its reason"
            )
        for counter in (self.admitted_count, self.committed_count):
            if type(counter) is not int or not 0 <= counter <= 2_147_483_647:
                raise ConcurrentEngineInvalidRequestError(
                    "engine report counters are outside the supported range"
                )


@dataclass(frozen=True, slots=True)
class _InFlight:
    """Engine-side tracking of one admitted assignment."""

    identity: WorkIdentity
    assignment: WorkAssignmentV1
    facts: RegisteredAssignment
    lease: WorkLease
    redispatches: int = 0


class ConcurrentRunEngine:
    """Parent-owned orchestration for one concurrent run.

    The engine is single-threaded by design: every durable decision —
    admission, commit, pause, cancellation, recovery — happens in the
    parent's loop, and strategies only provide worker mechanics.
    """

    __slots__ = (
        "_admission_reader",
        "_admitted_count",
        "_allowance",
        "_cancel",
        "_capacity",
        "_channels",
        "_cleanup",
        "_clock",
        "_clock_wait",
        "_coordinator",
        "_correlation_id",
        "_edges",
        "_executor",
        "_in_flight",
        "_lease_owner",
        "_lease_service",
        "_lifecycle",
        "_node_kinds",
        "_node_order",
        "_partitions_by_node",
        "_pause_reservation",
        "_pause_signal",
        "_plan_fingerprint",
        "_result_wait_seconds",
        "_run_id",
        "_scheduler",
        "_settings",
        "_shutdown_done",
        "_started",
        "_strategy",
        "_worker_facts",
        "_writer_retries",
    )

    def __init__(
        self,
        *,
        run_id: str,
        plan_fingerprint: str,
        node_order: tuple[str, ...],
        edges: tuple[tuple[str, str], ...],
        partitions_by_node: dict[str, tuple[str, ...]],
        node_kinds: dict[str, str],
        settings: CapturedConcurrencySettings,
        clock: EngineClock,
        strategy: FullPlanStrategy,
        executor: WorkOperationExecutor,
        admission_reader: AdmissionStateReader,
        lease_service: WorkLeaseService,
        lifecycle: ConcurrentLifecycleCoordinator,
        coordinator: ConcurrentResultCoordinator,
        channels: ChannelSet,
        capacity: ScheduledWorkLimiters,
        pause_signal: ConcurrentPauseSignal,
        cancellation: CancellationToken,
        cleanup: ConcurrentCleanupCoordinator,
        control_generation: int = 1,
        scheduler: ConcurrentScheduler | None = None,
        lease_owner: str = "engine-main",
        correlation_id: str | None = None,
        artifact_allowance: Callable[[WorkIdentity], tuple[str, ...]] | None = None,
        result_wait_seconds: float = 0.05,
        clock_wait: Callable[[int], None] | None = None,
    ) -> None:
        if type(run_id) is not str or not run_id:
            raise ConcurrentEngineInvalidRequestError("engine run identity is invalid")
        if type(plan_fingerprint) is not str or len(plan_fingerprint) != 64:
            raise ConcurrentEngineInvalidRequestError("engine plan fingerprint is invalid")
        if type(node_order) is not tuple or not node_order:
            raise ConcurrentEngineInvalidRequestError("engine node order is invalid")
        if type(edges) is not tuple:
            raise ConcurrentEngineInvalidRequestError("engine edges are invalid")
        if type(partitions_by_node) is not dict or type(node_kinds) is not dict:
            raise TypeError("engine topology must use mappings")
        if type(settings) is not CapturedConcurrencySettings:
            raise TypeError("engine settings must use CapturedConcurrencySettings")
        clock_value = cast(object, clock)
        if not isinstance(clock_value, EngineClock):
            raise TypeError("engine clock must implement the clock protocol")
        strategy_value = cast(object, strategy)
        if not isinstance(strategy_value, FullPlanStrategy):
            raise TypeError("engine strategy must implement FullPlanStrategy")
        executor_value = cast(object, executor)
        if not isinstance(executor_value, WorkOperationExecutor):
            raise TypeError("engine executor must implement WorkOperationExecutor")
        reader_value = cast(object, admission_reader)
        if not isinstance(reader_value, AdmissionStateReader):
            raise TypeError("engine admission reader must implement AdmissionStateReader")
        if type(lease_service) is not WorkLeaseService:
            raise TypeError("engine lease service must use WorkLeaseService")
        if type(lifecycle) is not ConcurrentLifecycleCoordinator:
            raise TypeError("engine lifecycle must use ConcurrentLifecycleCoordinator")
        if type(coordinator) is not ConcurrentResultCoordinator:
            raise TypeError("engine coordinator must use ConcurrentResultCoordinator")
        if type(channels) is not ChannelSet:
            raise TypeError("engine channels must use ChannelSet")
        if type(capacity) is not ScheduledWorkLimiters:
            raise TypeError("engine capacity must use ScheduledWorkLimiters")
        if type(pause_signal) is not ConcurrentPauseSignal:
            raise TypeError("engine pause signal must use ConcurrentPauseSignal")
        if type(cancellation) is not CancellationToken:
            raise TypeError("engine cancellation must use CancellationToken")
        if type(cleanup) is not ConcurrentCleanupCoordinator:
            raise TypeError("engine cleanup must use ConcurrentCleanupCoordinator")
        if type(control_generation) is not int or not 1 <= control_generation <= 2_147_483_647:
            raise ConcurrentEngineInvalidRequestError("engine control generation is invalid")
        if type(lease_owner) is not str or not 1 <= len(lease_owner) <= 128:
            raise ConcurrentEngineInvalidRequestError("engine lease owner is invalid")
        for node, kind in node_kinds.items():
            if node not in partitions_by_node:
                raise ConcurrentEngineInvalidRequestError(
                    "engine node kinds must reference plan nodes"
                )
            if type(kind) is not str or not 1 <= len(kind) <= MAX_ENGINE_NODE_KIND_LENGTH:
                raise ConcurrentEngineInvalidRequestError("engine node kind is invalid")
        if artifact_allowance is not None and not callable(artifact_allowance):
            raise TypeError("engine artifact allowance must be callable")
        if clock_wait is not None and not callable(clock_wait):
            raise TypeError("engine clock wait must be callable")
        require_strategy_timeout(result_wait_seconds, "engine result wait")
        self._run_id = run_id
        self._plan_fingerprint = plan_fingerprint
        self._node_order = node_order
        self._edges = edges
        self._partitions_by_node = partitions_by_node
        self._node_kinds = dict(node_kinds)
        self._settings = settings
        self._clock = clock_value
        self._strategy = strategy_value
        self._executor = executor_value
        self._admission_reader = reader_value
        self._lease_service = lease_service
        self._lifecycle = lifecycle
        self._coordinator = coordinator
        self._channels = channels
        self._capacity = capacity
        self._pause_signal = pause_signal
        self._cancel = cancellation
        self._cleanup = cleanup
        self._lease_owner = lease_owner
        self._correlation_id = correlation_id
        self._allowance = artifact_allowance
        self._result_wait_seconds = result_wait_seconds
        self._clock_wait = clock_wait
        self._scheduler = scheduler or ConcurrentScheduler(
            run_id=run_id,
            plan_fingerprint=plan_fingerprint,
            node_order=node_order,
            edges=edges,
            partitions_by_node=partitions_by_node,
            control_generation=ControlGeneration(control_generation),
        )
        self._worker_facts = ResultFactsRegistry()
        self._in_flight: dict[WorkIdentity, _InFlight] = {}
        self._pause_reservation: WorkLeasePauseReservation | None = None
        self._started = False
        self._shutdown_done = False
        self._admitted_count = 0
        self._writer_retries: dict[WorkIdentity, int] = {}

    @property
    def frontier(self) -> SchedulerFrontierV2:
        """Return the engine's current scheduler frontier."""
        return self._scheduler.frontier

    @property
    def run_id(self) -> RunId:
        """Return the exact durable run identity this engine owns."""
        return RunId(self._run_id)

    @property
    def is_shutdown(self) -> bool:
        """Report whether terminal cleanup has begun for this engine."""
        return self._shutdown_done

    @property
    def last_lifecycle_report(self) -> ConcurrentLifecycleReport | None:
        """Return the latest durable lifecycle receipt from this owner."""
        return self._lifecycle.last_report

    @property
    def in_flight_identities(self) -> tuple[WorkIdentity, ...]:
        """Return every currently admitted identity in deterministic order."""
        return tuple(sorted(self._in_flight, key=WorkIdentity.sort_key))

    @property
    def admitted_count(self) -> int:
        """Return how many assignments this engine admitted."""
        return self._admitted_count

    @property
    def executor(self) -> WorkOperationExecutor:
        """Return the parent-supplied operation executor."""
        return self._executor

    @property
    def cancellation(self) -> CancellationToken:
        """Return the run's cancellation token."""
        return self._cancel

    def restore_frontier(self, frontier: SchedulerFrontierV2) -> None:
        """Rebuild the scheduler exactly from one durable frontier."""
        if self._started:
            raise ConcurrentEngineStateError("a started engine cannot restore its frontier")
        self._scheduler.restore(frontier)

    def request_pause(self, *, correlation_id: str | None = None) -> None:
        """Install one pause request from any thread."""
        if self._shutdown_done:
            raise ConcurrentEngineStateError("a shut-down engine cannot pause")
        self._lifecycle.set_correlation_id(correlation_id)
        generation = self._scheduler.frontier.control_generation.value
        self._pause_signal.request(generation)

    def abort_pause(self) -> None:
        """Abort a pending pause before it is acknowledged."""
        generation = self._pause_signal.requested_generation
        if generation is None:
            return
        self._pause_signal.try_abort(generation)

    def run(self) -> ConcurrentRunReport:
        """Drive the run to its next terminal boundary."""
        self._ensure_started()
        while True:
            if self._cancel.is_requested:
                return self._cancel_flow()
            self._drain_results(block=False)
            frontier = self._scheduler.frontier
            if frontier.is_recovery_required:
                return self._recovery_report(frontier.recovery_required_reason)
            if frontier.control_state.value == "paused":
                raise ConcurrentEngineStateError("a paused engine must resume before running")
            self._install_pause_if_requested()
            frontier = self._scheduler.frontier
            if frontier.control_state.value == "quiescing":
                report = self._quiesce_step()
                if report is not None:
                    return report
                continue
            self._scheduler.retry_eligible(self._now_micros())
            if self._scheduler.is_finished:
                return self._finish_completed()
            if self._strategy.mode is StrategyMode.INLINE:
                admitted = self._admit_next()
                if admitted or self._channels.assignment.queued:
                    # Inline workers also consume corrected resubmissions
                    # that re-entered the assignment channel.
                    self._strategy.execute_pending()
                elif self._in_flight:
                    self._drain_results(block=True)
                elif self._wait_for_retry_eligibility():
                    continue
                elif self._scheduler.failed_node_ids:
                    # A blocked-terminal aggregate permanently blocks its
                    # successors; the durable outcome is final and the
                    # finalizer derives the run result from the evidence.
                    return self._finish_completed()
                else:
                    raise ConcurrentEngineStateError(
                        "the engine stalled with no ready, in-flight, or finished work"
                    )
            else:
                admitted = self._admit_until_limited()
                if admitted == 0:
                    if not self._in_flight:
                        if self._wait_for_retry_eligibility():
                            continue
                        if not self._scheduler.next_ready(1):
                            if self._scheduler.failed_node_ids:
                                # Blocked-terminal successors can never
                                # become admissible; the durable outcome
                                # is final for the finalizer to classify.
                                return self._finish_completed()
                            raise ConcurrentEngineStateError(
                                "the engine stalled with no ready, in-flight, or finished work"
                            )
                    self._drain_results(block=True)

    def resume(
        self,
        proof: ConcurrentPausedProof,
        *,
        correlation_id: str | None = None,
    ) -> None:
        """Resume one durably paused engine run."""
        reservation = self._pause_reservation
        if reservation is None:
            raise ConcurrentEngineStateError("resume requires the engine's pause reservation")
        self._lifecycle.set_correlation_id(correlation_id)
        self._lifecycle.resume(
            proof,
            lease_service=self._lease_service,
            reservation=reservation,
            signal=self._pause_signal,
        )
        self._scheduler.resume()
        self._pause_reservation = None

    def request_cancellation(self, *, correlation_id: str | None = None) -> None:
        """Request owned cancellation with the request correlation identity."""
        if self._shutdown_done:
            raise ConcurrentEngineStateError("a shut-down engine cannot cancel")
        self._lifecycle.set_correlation_id(correlation_id)
        self._cancel.request()

    def cleanup(self) -> None:
        """Run the bounded idempotent cleanup for every owned resource."""
        self._shutdown_owned()

    # -- internal orchestration ------------------------------------------

    def _ensure_started(self) -> None:
        if self._started:
            return
        context = StrategyContext(
            run_id=self._run_id,
            plan_fingerprint=self._plan_fingerprint,
            settings=self._settings,
            assignment_channel=self._channels.assignment,
            result_channel=self._channels.result,
            executor=self._executor,
            facts=self._worker_facts,
        )
        self._strategy.start(context)
        self._cleanup.register(_ChannelSetResource(self._channels))
        self._cleanup.register(_CapacityResource(self._capacity))
        self._cleanup.register(_CoordinatorResource(self._coordinator))
        self._cleanup.register(
            _StrategyResource(self._strategy, self._settings.shutdown_timeout_seconds)
        )
        self._started = True

    def _install_pause_if_requested(self) -> None:
        if not self._pause_signal.is_requested:
            return
        frontier = self._scheduler.frontier
        if frontier.control_state.value != "running":
            return
        if self._pause_reservation is None:
            self._pause_reservation = self._lease_service.reserve_pause(RunId(self._run_id))
        self._scheduler.request_pause()

    def _quiesce_step(self) -> ConcurrentRunReport | None:
        """Advance one quiescing step; return a report at the stable boundary."""
        if self._in_flight:
            self._drain_results(block=True)
            return None
        generation = self._pause_signal.requested_generation
        if generation is None:
            self._scheduler.abort_pause()
            self._release_pause_reservation()
            return None
        reservation = self._pause_reservation
        if reservation is None:
            reservation = self._lease_service.reserve_pause(RunId(self._run_id))
            self._pause_reservation = reservation
        try:
            proof = self._lifecycle.complete_pause(
                RunId(self._run_id),
                lease_service=self._lease_service,
                reservation=reservation,
                signal=self._pause_signal,
                generation=generation,
            )
        except ConcurrentLifecycleRejectedError:
            # The abort won the compare-and-set: resume admission.
            self._scheduler.abort_pause()
            self._release_pause_reservation()
            return None
        except ConcurrentLifecycleError as error:
            self._scheduler.mark_recovery_required(f"pause completion failed: {error}")
            return self._recovery_report("pause completion failed")
        self._scheduler.mark_paused()
        return ConcurrentRunReport(
            status=EngineStatus.PAUSED,
            frontier=self._scheduler.frontier,
            pause_proof=proof,
            recovery_reason=None,
            admitted_count=self._admitted_count,
            committed_count=self._coordinator.committed_count,
        )

    def _release_pause_reservation(self) -> None:
        if self._pause_reservation is not None:
            self._lease_service.release_pause(self._pause_reservation)
            self._pause_reservation = None

    def _admit_next(self) -> bool:
        ready = self._scheduler.next_ready(1)
        if not ready:
            return False
        if self._channels.assignment.queued >= self._channels.assignment.capacity:
            return False
        return self._admit_one(ready[0])

    def _admit_until_limited(self) -> int:
        admitted = 0
        while self._channels.assignment.queued < self._channels.assignment.capacity:
            ready = self._scheduler.next_ready(1)
            if not ready:
                break
            if not self._admit_one(ready[0]):
                break
            admitted += 1
        return admitted

    def _admit_one(self, identity: WorkIdentity) -> bool:
        node_id = identity.node_id
        facts: AdmissionFacts | None = None
        owner_key: str | None = None
        try:
            provisional = self._admission_reader.read(self._run_id, node_id, identity.partition_key)
            work_id = WorkItemId(provisional.work_item_id)
            # Every in-flight work item holds its scheduled-work permits
            # under a unique owner key: the durable lease owner doubles as
            # the capacity owner so the coordinator releases the exact
            # triple after durable acknowledgement.
            owner_key = f"{self._lease_owner}:{work_id.value}"
            self._capacity.acquire(owner_key, node_id)
            facts = provisional
        except CapacityTimeoutError:
            return False
        try:
            work_id = WorkItemId(facts.work_item_id)
            now = self._clock.now()
            if type(now) is not UtcTimestamp:
                raise ConcurrentEngineStateError("engine clock returned an invalid timestamp")
            lease = self._lease_service.acquire(
                AcquireWorkLeaseRequest(
                    run_id=RunId(self._run_id),
                    node_id=NodeId(node_id),
                    work_item_id=work_id,
                    expected_attempt_number=AttemptNumber(facts.completed_attempt_count + 1),
                    expected_work_row_version=facts.work_row_version,
                    expected_node_row_version=facts.node_row_version,
                    expected_run_row_version=facts.run_row_version,
                    lease_owner=owner_key,
                    runner_kind=self._strategy.strategy_id,
                    worker_identity=f"{self._strategy.strategy_id}-engine",
                    event=_carrier_event(
                        facts.next_event_sequence,
                        facts.event_counter_row_version,
                        work_id,
                        now,
                        self._correlation_id,
                    ),
                )
            )
        except ConcurrentEngineError:
            self._capacity.release(owner_key, node_id)
            raise
        except Exception as error:
            self._capacity.release(owner_key, node_id)
            raise ConcurrentEngineStateError("work lease acquisition failed") from error
        claim = lease.claim
        generation = self._scheduler.frontier.control_generation.value
        self._scheduler.register_admission(identity, claim.row_version)
        registered = RegisteredAssignment(
            identity=identity,
            work_item_id=work_id.value,
            attempt_number=int(claim.attempt_number),
            lease_fence=claim.row_version,
            lease_owner=claim.lease_owner,
            control_generation=generation,
            deadline_micros=_to_micros(claim.lease_expires_at),
            allowed_artifact_ids=self._allowance_for(identity),
        )
        self._coordinator.register_assignment(registered)
        assignment = WorkAssignmentV1(
            protocol=WORK_ASSIGNMENT_PROTOCOL,
            contract_version=RUNNER_CONTRACT_VERSION,
            plan_fingerprint=self._plan_fingerprint,
            run_id=self._run_id,
            node_id=node_id,
            partition_key=identity.partition_key,
            work_item_id=work_id.value,
            attempt_number=int(claim.attempt_number),
            lease_fence=claim.row_version,
            lease_owner=claim.lease_owner,
            control_generation=ControlGeneration(generation),
            deadline_utc=str(claim.lease_expires_at),
            operation_descriptor=ContractDocument((("node_kind", self._node_kinds[node_id]),)),
            input_references=(),
            captured_settings_ref="captured-concurrency-settings.v1",
        )
        self._in_flight[identity] = _InFlight(
            identity=identity,
            assignment=assignment,
            facts=registered,
            lease=lease,
        )
        self._admitted_count += 1
        if not self._channels.assignment.try_send(assignment):
            raise ConcurrentEngineStateError(
                "assignment channel rejected an admitted assignment within its bound"
            )
        return True

    def _allowance_for(self, identity: WorkIdentity) -> tuple[str, ...]:
        if self._allowance is None:
            return ()
        allowed = self._allowance(identity)
        if type(allowed) is not tuple:
            raise ConcurrentEngineStateError("artifact allowance must return a tuple")
        return tuple(sorted(set(allowed)))

    def _drain_results(self, *, block: bool) -> int:
        drained = 0
        while True:
            try:
                if block and drained == 0:
                    message = self._channels.result.recv(timeout=self._result_wait_seconds)
                else:
                    message = self._channels.result.try_recv()
            except ChannelTimeoutError, ChannelClosedError:
                return drained
            if message is None:
                return drained
            self._commit_envelope(cast(WorkResultV1, message))
            drained += 1
            if self._scheduler.frontier.is_recovery_required:
                return drained

    def _commit_envelope(
        self,
        envelope: WorkResultV1,
        *,
        facts_override: ExecutedWork | None = None,
    ) -> None:
        identity = WorkIdentity(envelope.run_id, envelope.node_id, envelope.partition_key)
        tracking = self._in_flight.get(identity)
        facts = facts_override or self._worker_facts.take(identity)
        if tracking is None or facts is None:
            self._scheduler.mark_recovery_required(
                "result arrived for an untracked or factless identity"
            )
            return
        attempts = self._writer_retries.get(identity, 0)
        reservation = self._lease_service.reserve_completion(tracking.lease)
        try:
            self._coordinator.submit_result(
                envelope,
                retry_eligible_at_micros=facts.retry_eligible_at_micros,
                failure_classification=facts.failure_classification,
            )
        except ResultValidationRejection, ResultForgedReferenceRejection:
            self._lease_service.finalize_completion(
                reservation, WorkLeaseCompletionDisposition.RETAIN_ACTIVE
            )
            if tracking.redispatches < MAX_ENGINE_REDISPATCH:
                self._in_flight[identity] = _InFlight(
                    identity=identity,
                    assignment=tracking.assignment,
                    facts=tracking.facts,
                    lease=tracking.lease,
                    redispatches=tracking.redispatches + 1,
                )
                if not self._channels.assignment.try_send(tracking.assignment):
                    self._scheduler.mark_recovery_required(
                        "corrected resubmission exceeded the assignment bound"
                    )
                return
            self._scheduler.mark_recovery_required(
                "result rejected after its corrected resubmission bound"
            )
        except ResultStaleRejection:
            self._scheduler.mark_recovery_required(
                "durable evidence no longer honors an in-flight result"
            )
        except ResultWriterRetryableError:
            self._lease_service.finalize_completion(
                reservation, WorkLeaseCompletionDisposition.RETAIN_ACTIVE
            )
            if attempts >= MAX_ENGINE_WRITER_RETRIES:
                self._scheduler.mark_recovery_required("writer retries exhausted for one result")
                return
            self._writer_retries[identity] = attempts + 1
            self._worker_facts.record(
                identity,
                ExecutedWork(
                    result=envelope,
                    failure_classification=facts.failure_classification,
                    retry_eligible_at_micros=facts.retry_eligible_at_micros,
                ),
            )
            if not self._channels.result.try_send(envelope):
                self._scheduler.mark_recovery_required(
                    "result retry exceeded the result channel bound"
                )
        except ResultOutcomeUnknownError, ResultCoordinatorClosedError:
            self._scheduler.mark_recovery_required("result writer outcome stayed unknown")
        else:
            self._lease_service.finalize_completion(
                reservation, WorkLeaseCompletionDisposition.RETIRE_COMMITTED
            )
            self._writer_retries.pop(identity, None)
            self._in_flight.pop(identity, None)

    def _cancel_flow(self) -> ConcurrentRunReport:
        frontier = self._scheduler.frontier
        if frontier.control_state.value == "quiescing":
            self._scheduler.abort_pause()
            generation = self._pause_signal.requested_generation
            if generation is not None:
                self._pause_signal.try_abort(generation)
            self._release_pause_reservation()
            frontier = self._scheduler.frontier
        if frontier.control_state.value == "running":
            self._scheduler.request_cancel()
        self._lifecycle.begin_cancellation(RunId(self._run_id))
        self._channels.assignment.close()
        drained_any = True
        while self._in_flight and drained_any:
            drained_any = self._drain_results(block=True)
        for identity in self.in_flight_identities:
            tracking = self._in_flight[identity]
            self._commit_envelope(
                _cancelled_envelope(tracking.assignment),
                facts_override=ExecutedWork(
                    result=_cancelled_envelope(tracking.assignment),
                    failure_classification="user_cancellation",
                ),
            )
        self._lifecycle.finish_cancellation(RunId(self._run_id))
        return self._terminal_report(EngineStatus.CANCELLED)

    def _finish_completed(self) -> ConcurrentRunReport:
        return self._terminal_report(EngineStatus.COMPLETED)

    def _recovery_report(self, reason: str | None) -> ConcurrentRunReport:
        return self._terminal_report(
            EngineStatus.RECOVERY_REQUIRED, reason or "unspecified recovery reason"
        )

    def _terminal_report(
        self,
        status: EngineStatus,
        recovery_reason: str | None = None,
        pause_proof: ConcurrentPausedProof | None = None,
    ) -> ConcurrentRunReport:
        if status is not EngineStatus.PAUSED:
            self._shutdown_owned()
        return ConcurrentRunReport(
            status=status,
            frontier=self._scheduler.frontier,
            pause_proof=pause_proof,
            recovery_reason=recovery_reason,
            admitted_count=self._admitted_count,
            committed_count=self._coordinator.committed_count,
        )

    def _shutdown_owned(self) -> None:
        if self._shutdown_done:
            return
        self._shutdown_done = True
        self._coordinator.close()
        self._release_pause_reservation_silent()
        # The structured evidence lives on the cleanup coordinator;
        # re-running cleanup re-attempts every unresolved resource.
        with contextlib.suppress(Exception):
            self._cleanup.cleanup(timeout_seconds=self._settings.cleanup_timeout_seconds)

    def _release_pause_reservation_silent(self) -> None:
        if self._pause_reservation is None:
            return
        with contextlib.suppress(Exception):
            self._lease_service.release_pause(self._pause_reservation)
        self._pause_reservation = None

    def _wait_for_retry_eligibility(self) -> bool:
        """Wait deterministically for the earliest pending retry eligibility.

        All delay decisions read the injected clock; the injected
        ``clock_wait`` hook performs the physical wait, so a manual
        clock advances exactly to the next eligibility while a real
        clock sleeps boundedly.  Returns ``False`` when no retry wait
        is pending.
        """

        waits = self._scheduler.frontier.retry_waits
        if not waits:
            return False
        earliest = min(wait.eligible_at_micros for wait in waits)
        now = self._now_micros()
        if earliest > now and self._clock_wait is not None:
            self._clock_wait(earliest)
        return True

    def _now_micros(self) -> int:
        now = self._clock.now()
        if type(now) is not UtcTimestamp:
            raise ConcurrentEngineStateError("engine clock returned an invalid timestamp")
        return _to_micros(now)


class _ChannelSetResource:
    __slots__ = ("_channels",)

    def __init__(self, channels: ChannelSet) -> None:
        self._channels = channels

    @property
    def kind(self) -> str:
        return "channels"

    @property
    def name(self) -> str:
        return "engine-channel-set"

    def close(self, *, timeout_seconds: float) -> None:
        del timeout_seconds
        self._channels.close_all()


class _CapacityResource:
    __slots__ = ("_capacity",)

    def __init__(self, capacity: ScheduledWorkLimiters) -> None:
        self._capacity = capacity

    @property
    def kind(self) -> str:
        return "capacity"

    @property
    def name(self) -> str:
        return "engine-scheduled-capacity"

    def close(self, *, timeout_seconds: float) -> None:
        del timeout_seconds
        self._capacity.close()


class _CoordinatorResource:
    __slots__ = ("_coordinator",)

    def __init__(self, coordinator: ConcurrentResultCoordinator) -> None:
        self._coordinator = coordinator

    @property
    def kind(self) -> str:
        return "coordinator"

    @property
    def name(self) -> str:
        return "engine-result-coordinator"

    def close(self, *, timeout_seconds: float) -> None:
        del timeout_seconds
        self._coordinator.close()


class _StrategyResource:
    __slots__ = ("_shutdown_seconds", "_strategy")

    def __init__(self, strategy: FullPlanStrategy, shutdown_seconds: float) -> None:
        self._strategy = strategy
        self._shutdown_seconds = shutdown_seconds

    @property
    def kind(self) -> str:
        return "strategy"

    @property
    def name(self) -> str:
        return f"strategy-{self._strategy.strategy_id}"

    def close(self, *, timeout_seconds: float) -> None:
        seconds = min(timeout_seconds, self._shutdown_seconds)
        self._strategy.shutdown(timeout_seconds=seconds)


def _cancelled_envelope(assignment: WorkAssignmentV1) -> WorkResultV1:
    return WorkResultV1(
        protocol=WORK_RESULT_PROTOCOL,
        contract_version=RUNNER_CONTRACT_VERSION,
        plan_fingerprint=assignment.plan_fingerprint,
        run_id=assignment.run_id,
        node_id=assignment.node_id,
        partition_key=assignment.partition_key,
        work_item_id=assignment.work_item_id,
        attempt_number=assignment.attempt_number,
        lease_fence=assignment.lease_fence,
        lease_owner=assignment.lease_owner,
        control_generation=assignment.control_generation,
        outcome=ContractOutcome.CANCELLED,
        metrics=(),
        artifact_references=(),
        checkpoint_proposal=False,
        failure_detail="cancelled by engine request",
        cleanup=ContractCleanupEvidence(ContractCleanupStatus.COMPLETED, (), None),
    )


def _carrier_event(
    sequence: int,
    counter: int,
    work_item_id: WorkItemId,
    at: UtcTimestamp,
    correlation_id: str | None,
) -> EventAppendRequest:
    return EventAppendRequest(
        EventSequence(sequence),
        counter,
        PendingExecutionEvent(
            "work_claimed",
            at,
            EventSubjectKind.WORK_ITEM,
            work_item_id,
            correlation_id,
            _CARRIER_LEASE_EVENT_PAYLOAD_SCHEMA,
            RedactedDocument.from_mapping({"kind": "work_claimed"}),
        ),
    )


def _to_micros(value: UtcTimestamp) -> int:
    delta = value.to_datetime() - _EPOCH
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


__all__ = [
    "CONCURRENT_ENGINE_VERSION",
    "MAX_ENGINE_REDISPATCH",
    "MAX_ENGINE_WRITER_RETRIES",
    "AdmissionFacts",
    "AdmissionStateReader",
    "ConcurrentEngineError",
    "ConcurrentEngineInvalidRequestError",
    "ConcurrentEngineStateError",
    "ConcurrentRunEngine",
    "ConcurrentRunReport",
    "EngineClock",
    "EngineStatus",
]
