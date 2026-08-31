"""Run creation and HTTP-facing lifecycle use cases.

Creation compiles the addressed published pipeline version into its node
identities and captures the run through the Phase 6 transactional writer.
Queued cancellation follows the accepted before-start writer boundary.
Active pause, resume, and cancellation are handed to the runtime that owns
the executor, its admission gate, and its durable lifecycle evidence; the
HTTP service never substitutes a bare state transition for that owner.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from paritygrid.application.planner.execution_plan import compile_execution_plan
from paritygrid.application.planner.publication import PublishedPipelineSpecification
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSequenceConflictError,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.execution import (
    ExecutionDuplicateError,
    ExecutionRecordNotFoundError,
    ExecutionStaleRowVersionError,
    ExecutionStateConflictError,
    RunPage,
    RunRecord,
)
from paritygrid.application.ports.operations import OperationalUnitOfWork
from paritygrid.application.ports.run_control import (
    ActiveRunControlBusyError,
    ActiveRunControlClosedError,
    ActiveRunControlError,
    ActiveRunControlEvidenceError,
    ActiveRunControlNotFoundError,
    ActiveRunControlRegistry,
    ActiveRunControlTimeoutError,
    RunControlAction,
    RunControlEvidence,
)
from paritygrid.application.ports.writer import (
    EventAppendRequest,
    TransactionalWriter,
    WriterCommand,
    WriterCommandResult,
    WriterReceipt,
)
from paritygrid.application.services.errors import (
    OperationalConflictError,
    OperationalRecordNotFoundError,
    OperationalRequestError,
    OperationalUnavailableError,
    RunInvalidTransitionError,
)
from paritygrid.application.writes.execution import (
    CreateCapturedRun,
    CreateCapturedRunResult,
    TransitionRun,
    TransitionRunResult,
)
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import NodeId, PipelineId, PipelineVersion, RunId, UtcTimestamp

RUN_CREATED_EVENT_PAYLOAD_SCHEMA_VERSION = 1
RUN_TRANSITION_EVENT_PAYLOAD_SCHEMA_VERSION = 1
FULL_PLAN_RUNNER_KINDS = frozenset({"sequential", "threaded", "asyncio"})
MAX_LIFECYCLE_ATTEMPTS = 3
DEFAULT_SUBMIT_TIMEOUT_SECONDS = 5.0
_COMPLETED_STATE = {
    "pause": RunState.PAUSED,
    "resume": RunState.RUNNING,
    "cancel": RunState.CANCELLED,
}
_EVENT_KINDS = {
    RunState.PAUSING: "run_pausing",
    RunState.PAUSED: "run_paused",
    RunState.RESUMING: "run_resuming",
    RunState.RUNNING: "run_started",
    RunState.CANCELLING: "run_cancelling",
    RunState.CANCELLED: "run_cancelled",
}


@dataclass(frozen=True, slots=True)
class _Frontier:
    sequence: int
    counter_row_version: int


@dataclass(frozen=True, slots=True)
class RunCreationRequest:
    """Validated run creation input before durable capture."""

    run_id: RunId
    pipeline_id: PipelineId
    pipeline_version: PipelineVersion
    runner_kind: str
    runner_configuration: ConfigurationDocument
    scenario_seed: int | None


class RunService:
    """Create, inspect, and list runs on top of accepted execution storage."""

    def __init__(
        self,
        *,
        unit_of_work: OperationalUnitOfWork,
        writer: TransactionalWriter,
        now: Callable[[], UtcTimestamp],
        submit_timeout_seconds: float = DEFAULT_SUBMIT_TIMEOUT_SECONDS,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._writer = writer
        self._now = now
        self._submit_timeout_seconds = submit_timeout_seconds

    def create(
        self,
        *,
        run_id: str,
        pipeline_id: str,
        pipeline_version: int,
        runner_kind: str,
        scenario_seed: int | None,
        runner_configuration: Mapping[str, object] | None,
        correlation_id: str | None,
        converge_on_duplicate: bool = False,
    ) -> RunRecord:
        request = _creation_request(
            run_id=run_id,
            pipeline_id=pipeline_id,
            pipeline_version=pipeline_version,
            runner_kind=runner_kind,
            scenario_seed=scenario_seed,
            runner_configuration=runner_configuration,
        )
        node_ids = self._compile_nodes(request.pipeline_id, request.pipeline_version)
        occurred_at = self._now()
        command = CreateCapturedRun(
            run_id=request.run_id,
            pipeline_id=request.pipeline_id,
            pipeline_version=request.pipeline_version,
            runner_kind=request.runner_kind,
            runner_configuration=request.runner_configuration,
            scenario_seed=request.scenario_seed,
            node_ids=node_ids,
            created_at=occurred_at,
            event=_event(
                sequence=1,
                counter_row_version=1,
                run_id=request.run_id,
                event_kind="run_created",
                occurred_at=occurred_at,
                correlation_id=correlation_id,
                payload={"kind": "run_created"},
                payload_schema_version=RUN_CREATED_EVENT_PAYLOAD_SCHEMA_VERSION,
            ),
        )
        try:
            result = _submit(self._writer, command, self._submit_timeout_seconds)
        except ExecutionDuplicateError:
            return self._duplicate_run(request, converge_on_duplicate)
        return _captured_run(result)

    def get(self, run_id: str) -> RunRecord:
        with self._unit_of_work.transaction() as repositories:
            record = repositories.runs.get(_run_id(run_id))
        if record is None:
            raise OperationalRecordNotFoundError("run", run_id)
        return record

    def list(
        self,
        *,
        limit: int,
        after: str | None,
        state: str | None,
    ) -> RunPage:
        cursor = None if after is None else _run_id(after, field="cursor")
        filter_state = None if state is None else _run_state(state)
        with self._unit_of_work.transaction() as repositories:
            return repositories.runs.list(limit=limit, after=cursor, state=filter_state)

    def _compile_nodes(
        self, pipeline_id: PipelineId, version: PipelineVersion
    ) -> tuple[NodeId, ...]:
        with self._unit_of_work.transaction() as repositories:
            published = repositories.pipelines.get_version(pipeline_id, version)
        if published is None:
            raise OperationalRecordNotFoundError(
                "pipeline version", f"{pipeline_id.value} v{version.number}"
            )
        specification = PublishedPipelineSpecification.from_configuration_document(
            published.specification
        )
        plan = compile_execution_plan(specification)
        return tuple(node.node_id for node in plan.nodes)

    def _duplicate_run(self, request: RunCreationRequest, converge_on_duplicate: bool) -> RunRecord:
        with self._unit_of_work.transaction() as repositories:
            existing = repositories.runs.get(request.run_id)
        if existing is None:
            raise OperationalUnavailableError(
                "run creation outcome is unresolved; retry with the same idempotency key"
            )
        differs = (
            existing.pipeline_id != request.pipeline_id
            or existing.pipeline_version != request.pipeline_version
            or existing.runner_kind != request.runner_kind
            or existing.scenario_seed != request.scenario_seed
            or existing.runner_configuration.to_mapping()
            != request.runner_configuration.to_mapping()
        )
        if differs or not converge_on_duplicate:
            # A different captured configuration can never converge; an
            # identical unkeyed duplicate stays an ordinary conflict.
            raise OperationalConflictError(
                "run identity already exists with a different captured configuration"
                if differs
                else "run identity already exists",
                code="run_duplicate_identity" if differs else "duplicate_record",
            )
        return existing


class RunLifecycleService:
    """Route active lifecycle control to its execution owner.

    The operational service may cancel a queued run because it has never
    acquired execution resources.  Once a run is active, however, only the
    runtime-owned executor may cross pause, resume, or cancellation
    boundaries: it owns the admission gate, in-flight work, durable control
    evidence, and bounded cleanup.  The HTTP path therefore fails closed if
    that owner is absent rather than issuing a bare durable transition.
    """

    def __init__(
        self,
        *,
        unit_of_work: OperationalUnitOfWork,
        writer: TransactionalWriter,
        now: Callable[[], UtcTimestamp],
        submit_timeout_seconds: float = DEFAULT_SUBMIT_TIMEOUT_SECONDS,
        active_run_controls: ActiveRunControlRegistry | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._writer = writer
        self._now = now
        self._submit_timeout_seconds = submit_timeout_seconds
        self._active_run_controls = (
            _NoActiveRunControlRegistry()
            if active_run_controls is None
            else _active_run_controls(active_run_controls)
        )

    def pause(
        self,
        run_id: str,
        *,
        correlation_id: str | None,
        converge_on_duplicate: bool = False,
    ) -> RunRecord:
        return self._converge(run_id, "pause", correlation_id, converge_on_duplicate)

    def resume(
        self,
        run_id: str,
        *,
        correlation_id: str | None,
        converge_on_duplicate: bool = False,
    ) -> RunRecord:
        return self._converge(run_id, "resume", correlation_id, converge_on_duplicate)

    def cancel(
        self,
        run_id: str,
        *,
        correlation_id: str | None,
        converge_on_duplicate: bool = False,
    ) -> RunRecord:
        return self._converge(run_id, "cancel", correlation_id, converge_on_duplicate)

    def _converge(
        self,
        run_id: str,
        direction: str,
        correlation_id: str | None,
        converge_on_duplicate: bool,
    ) -> RunRecord:
        identity = _run_id(run_id)
        for _attempt in range(MAX_LIFECYCLE_ATTEMPTS):
            try:
                return self._attempt(
                    identity,
                    run_id,
                    direction,
                    correlation_id,
                    converge_on_duplicate,
                )
            except (
                ExecutionStaleRowVersionError,
                ExecutionStateConflictError,
                EventSequenceConflictError,
            ):
                # Another request moved the durable frontier first; re-read
                # and re-classify before spending the next bounded attempt.
                continue
        raise OperationalConflictError(
            "run lifecycle changed concurrently; retry the request",
            code="run_lifecycle_contention",
        )

    def _attempt(
        self,
        identity: RunId,
        run_id: str,
        direction: str,
        correlation_id: str | None,
        converge_on_duplicate: bool,
    ) -> RunRecord:
        run, frontier = self._read(identity, run_id)
        current = run.state
        completed = _COMPLETED_STATE[direction]
        if current is completed and converge_on_duplicate:
            # Idempotency recovery can safely converge on already-durable
            # evidence without an executor or a fresh transition.
            return run
        if current is completed:
            raise RunInvalidTransitionError(f"a {current.value} run cannot be {direction}d again")
        if current.is_terminal:
            raise RunInvalidTransitionError(f"a {current.value} run cannot be {direction}d")
        if current is not RunState.QUEUED:
            return self._delegate_active_control(
                identity,
                direction,
                correlation_id,
                converge_on_duplicate,
            )
        # A queued run has never acquired an execution owner or admitted
        # work.  Its direct cancellation is the accepted before-start arrow;
        # pause and resume remain invalid state-machine requests.
        if direction != "cancel":
            raise RunInvalidTransitionError(f"a {current.value} run cannot be {direction}d")
        if not current.can_transition_to(completed):  # pragma: no cover - domain guard
            raise RunInvalidTransitionError(f"a {current.value} run cannot be cancelled")
        return self._transition(
            identity,
            run,
            frontier,
            target=completed,
            correlation_id=correlation_id,
        )

    def _delegate_active_control(
        self,
        identity: RunId,
        direction: str,
        correlation_id: str | None,
        converge_on_duplicate: bool,
    ) -> RunRecord:
        """Invoke the registered owner and verify its durable result evidence."""
        try:
            action = RunControlAction(direction)
            evidence = self._active_run_controls.dispatch(
                identity,
                action=action,
                correlation_id=correlation_id,
                timeout_seconds=self._submit_timeout_seconds,
                converge_on_duplicate=converge_on_duplicate,
            )
        except (
            ActiveRunControlNotFoundError,
            ActiveRunControlBusyError,
            ActiveRunControlTimeoutError,
            ActiveRunControlClosedError,
            ActiveRunControlEvidenceError,
        ) as error:
            raise OperationalUnavailableError(
                "active run control is unavailable; the durable run was not changed"
            ) from error
        except ActiveRunControlError as error:  # pragma: no cover - future port subtype
            raise OperationalUnavailableError(
                "active run control is unavailable; the durable run was not changed"
            ) from error
        if type(evidence) is not RunControlEvidence:
            raise OperationalUnavailableError(
                "active run control returned no durable lifecycle evidence"
            )
        expected_state = _COMPLETED_STATE[direction]
        if evidence.run.run_id != identity or evidence.run.state is not expected_state:
            raise OperationalUnavailableError(
                "active run control returned inconsistent lifecycle evidence"
            )
        if not evidence.submission_ids and not converge_on_duplicate:
            raise OperationalUnavailableError(
                "active run control returned insufficient durable lifecycle evidence"
            )
        durable, _frontier = self._read(identity, identity.value)
        if (
            durable.run_id != evidence.run.run_id
            or durable.row_version != evidence.run.row_version
            or durable.state is not expected_state
        ):
            raise OperationalUnavailableError(
                "active run control durable evidence could not be verified"
            )
        # Returning the freshly read durable record prevents the transport
        # layer from treating owner memory as authoritative.
        return durable

    def _read(self, identity: RunId, run_id: str) -> tuple[RunRecord, _Frontier]:
        with self._unit_of_work.transaction() as repositories:
            run = repositories.runs.get(identity)
            if run is None:
                raise OperationalRecordNotFoundError("run", run_id)
            counter = repositories.runs.get_event_counter(identity)
        if counter is None:
            raise OperationalUnavailableError("run event frontier is unavailable")
        return run, _Frontier(counter.next_sequence_number, counter.row_version)

    def _transition(
        self,
        identity: RunId,
        run: RunRecord,
        frontier: _Frontier,
        *,
        target: RunState,
        correlation_id: str | None,
    ) -> RunRecord:
        occurred_at = self._now()
        command = TransitionRun(
            run_id=identity,
            expected_run_row_version=run.row_version,
            target_state=target,
            transitioned_at=occurred_at,
            execution_evidence_fingerprint=None,
            execution_evidence_fingerprint_version=None,
            event=_event(
                sequence=frontier.sequence,
                counter_row_version=frontier.counter_row_version,
                run_id=identity,
                event_kind=_EVENT_KINDS[target],
                occurred_at=occurred_at,
                correlation_id=correlation_id,
                payload={"from_state": run.state.value, "to_state": target.value},
                payload_schema_version=RUN_TRANSITION_EVENT_PAYLOAD_SCHEMA_VERSION,
            ),
        )
        try:
            result = _submit(self._writer, command, self._submit_timeout_seconds)
        except ExecutionRecordNotFoundError as error:
            raise OperationalRecordNotFoundError("run", identity.value) from error
        return _transitioned_run(result)


class _NoActiveRunControlRegistry(ActiveRunControlRegistry):
    """Fail closed until runtime has explicitly registered an owner."""

    def dispatch(
        self,
        run_id: RunId,
        *,
        action: RunControlAction,
        correlation_id: str | None,
        timeout_seconds: float,
        converge_on_duplicate: bool,
    ) -> RunControlEvidence:
        del run_id, action, correlation_id, timeout_seconds, converge_on_duplicate
        raise ActiveRunControlNotFoundError("no active execution owner is registered")


def _active_run_controls(value: object) -> ActiveRunControlRegistry:
    if not isinstance(value, ActiveRunControlRegistry):
        raise TypeError("active run controls must implement the registry contract")
    return value


def _event(
    *,
    sequence: int,
    counter_row_version: int,
    run_id: RunId,
    event_kind: str,
    occurred_at: UtcTimestamp,
    correlation_id: str | None,
    payload: Mapping[str, object],
    payload_schema_version: int,
) -> EventAppendRequest:
    return EventAppendRequest(
        expected_next_sequence=EventSequence(sequence),
        expected_counter_row_version=counter_row_version,
        event=PendingExecutionEvent(
            event_kind=event_kind,
            occurred_at=occurred_at,
            subject_kind=EventSubjectKind.RUN,
            subject_id=run_id,
            correlation_id=correlation_id,
            payload_schema_version=payload_schema_version,
            payload=RedactedDocument.from_mapping(payload),
        ),
    )


def _submit(
    writer: TransactionalWriter, command: WriterCommand, timeout_seconds: float
) -> WriterCommandResult:
    ticket = writer.submit(command, timeout_seconds=timeout_seconds)
    receipt: WriterReceipt = ticket.result(timeout_seconds=timeout_seconds)
    return receipt.result


def _captured_run(result: WriterCommandResult) -> RunRecord:
    if isinstance(result, CreateCapturedRunResult):
        return result.run
    raise OperationalUnavailableError("writer returned an unexpected run result")


def _transitioned_run(result: WriterCommandResult) -> RunRecord:
    if isinstance(result, TransitionRunResult):
        return result.run
    raise OperationalUnavailableError("writer returned an unexpected transition result")


def _creation_request(
    *,
    run_id: str,
    pipeline_id: str,
    pipeline_version: int,
    runner_kind: str,
    scenario_seed: int | None,
    runner_configuration: Mapping[str, object] | None,
) -> RunCreationRequest:
    if runner_kind not in FULL_PLAN_RUNNER_KINDS:
        raise OperationalRequestError(
            "runner kind must be one of the registered full-plan strategies",
            field="runner_kind",
        )
    if scenario_seed is not None and (
        type(scenario_seed) is not int or not 0 <= scenario_seed <= 2_147_483_647
    ):
        raise OperationalRequestError(
            "scenario seed must be a nonnegative 32-bit integer",
            field="scenario_seed",
        )
    configuration = (
        ConfigurationDocument.from_mapping({})
        if runner_configuration is None
        else _configuration(runner_configuration)
    )
    return RunCreationRequest(
        run_id=_run_id(run_id),
        pipeline_id=_pipeline_id(pipeline_id),
        pipeline_version=_pipeline_version(pipeline_version),
        runner_kind=runner_kind,
        runner_configuration=configuration,
        scenario_seed=scenario_seed,
    )


def _configuration(value: Mapping[str, object]) -> ConfigurationDocument:
    try:
        return ConfigurationDocument.from_mapping(value)
    except Exception as error:
        raise OperationalRequestError(
            f"runner configuration is not representable: {error}",
            field="runner_configuration",
        ) from error


def _run_id(value: str, *, field: str = "run_id") -> RunId:
    try:
        return RunId.parse(value)
    except ValueError as error:
        raise OperationalRequestError(
            "run identity must use the canonical run format",
            field=field,
        ) from error


def _pipeline_id(value: str) -> PipelineId:
    try:
        return PipelineId.parse(value)
    except ValueError as error:
        raise OperationalRequestError(
            "pipeline identity must use the canonical pipeline format",
            field="pipeline_id",
        ) from error


def _pipeline_version(value: int) -> PipelineVersion:
    try:
        return PipelineVersion(value)
    except ValueError as error:
        raise OperationalRequestError(
            "pipeline version must be a positive integer",
            field="pipeline_version",
        ) from error


def _run_state(value: str) -> RunState:
    try:
        return RunState(value)
    except ValueError as error:
        raise OperationalRequestError(
            "run state filter must be a known run state",
            field="state",
        ) from error
