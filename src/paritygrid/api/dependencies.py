"""Service access and command helpers shared by the API routers."""

import re
from collections.abc import Callable, Mapping
from typing import Protocol, cast

from fastapi import Request
from fastapi.responses import JSONResponse

from paritygrid.api.correlation import correlation_from_scope
from paritygrid.api.errors.mapping import translate_error
from paritygrid.api.errors.problems import (
    PROBLEM_CONTENT_TYPE,
    ProblemError,
    invalid_input_problem,
    unavailable_problem,
)
from paritygrid.application.services.artifacts import ArtifactService
from paritygrid.application.services.capabilities import CapabilitiesView
from paritygrid.application.services.connectors import (
    ConnectorService,
    ConnectorTestService,
)
from paritygrid.application.services.events import DurableEventStreamService
from paritygrid.application.services.idempotency import (
    CommandExecution,
    CommandOutcome,
    IdempotentCommandService,
)
from paritygrid.application.services.pipelines import PipelineService
from paritygrid.application.services.reconciliation import ReconciliationService
from paritygrid.application.services.repair import RepairApplyService, RepairService
from paritygrid.application.services.runs import RunLifecycleService, RunService
from paritygrid.application.services.telemetry import LiveTelemetryChannel
from paritygrid.domain.models import UtcTimestamp

IDEMPOTENCY_KEY_HEADER = "Idempotency-Key"
MAX_IDEMPOTENCY_KEY_LENGTH = 128
_IDEMPOTENCY_KEY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]*", flags=re.ASCII)


class ApiServices(Protocol):
    """The composed service surface the transport layer may reach."""

    @property
    def pipelines(self) -> PipelineService: ...

    @property
    def connectors(self) -> ConnectorService: ...

    @property
    def connector_tests(self) -> ConnectorTestService: ...

    @property
    def runs(self) -> RunService: ...

    @property
    def run_lifecycle(self) -> RunLifecycleService: ...

    @property
    def artifacts(self) -> ArtifactService: ...

    @property
    def idempotency(self) -> IdempotentCommandService: ...

    @property
    def capabilities(self) -> CapabilitiesView: ...

    @property
    def reconciliation(self) -> ReconciliationService: ...

    @property
    def repair(self) -> RepairService: ...

    @property
    def repair_application(self) -> RepairApplyService: ...

    @property
    def event_stream(self) -> DurableEventStreamService: ...

    @property
    def telemetry(self) -> LiveTelemetryChannel: ...

    @property
    def clock(self) -> Callable[[], UtcTimestamp]: ...


def get_services(request: Request) -> ApiServices:
    """Return the composed services or fail with a bounded problem."""
    services = getattr(request.app.state, "services", None)
    if services is None:
        raise unavailable_problem(
            "the runtime composition is not configured for this application",
            code="runtime_unavailable",
        )
    return cast(ApiServices, services)


def correlation_of(request: Request) -> str:
    """Return the request correlation identity."""
    return correlation_from_scope(request.scope)


def idempotency_key_from(request: Request) -> str | None:
    """Validate the optional Idempotency-Key header."""
    supplied = request.headers.get(IDEMPOTENCY_KEY_HEADER)
    if supplied is None:
        return None
    if len(supplied) > MAX_IDEMPOTENCY_KEY_LENGTH or (
        _IDEMPOTENCY_KEY_PATTERN.fullmatch(supplied) is None
    ):
        raise invalid_input_problem(
            "the idempotency key must use 1 to 128 portable ASCII characters",
            code="invalid_idempotency_key",
        )
    return supplied


def execute_command(
    request: Request,
    *,
    scope: str,
    canonical_request: Mapping[str, object],
    handler: Callable[[], tuple[int, Mapping[str, object]]],
    reclaimed_handler: Callable[[], tuple[int, Mapping[str, object]]] | None = None,
) -> CommandExecution:
    """Run one mutating command under the durable idempotency boundary."""
    services = get_services(request)
    key = idempotency_key_from(request)

    def wrapped(selected: Callable[[], tuple[int, Mapping[str, object]]]) -> CommandOutcome:
        try:
            status_code, body = selected()
            return CommandOutcome(
                status_code=status_code,
                media_type="application/json",
                body=body,
                terminal=True,
            )
        except Exception as error:
            problem = translate_error(error)
            if problem.status >= 500:
                raise problem from error
            return CommandOutcome(
                status_code=problem.status,
                media_type=PROBLEM_CONTENT_TYPE,
                body=problem_document(problem, request),
                terminal=True,
            )

    try:
        return services.idempotency.execute(
            scope=scope,
            key=key,
            request=canonical_request,
            handler=lambda: wrapped(handler),
            reclaimed_handler=(
                None if reclaimed_handler is None else lambda: wrapped(reclaimed_handler)
            ),
        )
    except ProblemError:
        raise
    except Exception as error:
        # Boundary failures (replay conflicts, in-progress leases, storage
        # errors) become bounded problems rather than escaping the route.
        raise translate_error(error) from error


def problem_document(problem: ProblemError, request: Request) -> dict[str, object]:
    """Render one problem document for the live request identity."""
    return problem.to_document(
        instance=request.url.path,
        correlation_id=correlation_of(request),
    )


def command_response(execution: CommandExecution) -> JSONResponse:
    """Render one command execution outcome verbatim."""
    outcome = execution.outcome
    return JSONResponse(
        status_code=outcome.status_code,
        content=dict(outcome.body),
        media_type=outcome.media_type,
    )
