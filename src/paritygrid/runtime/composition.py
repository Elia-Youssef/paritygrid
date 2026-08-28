"""Runtime composition: ordered startup, lifespan ownership, and services.

The composition root is the only place that instantiates the full dependency
graph.  FastAPI's lifespan owns every startup and shutdown resource through
an ordered startup sequence that rolls back partially started resources in
reverse order, so a failed startup never leaks a database engine or writer
thread, and shutdown closes everything in reverse startup order.
"""

import os
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from anyio.to_thread import run_sync as run_in_thread
from fastapi import FastAPI

from paritygrid import __version__
from paritygrid.adapters.artifacts.paths import resolve_artifact_root
from paritygrid.adapters.persistence.migration import HEAD_REVISION, upgrade_to_head
from paritygrid.adapters.persistence.operational import SQLOperationalUnitOfWork
from paritygrid.adapters.persistence.repair_workflow import SQLiteRepairWorkflowReader
from paritygrid.adapters.persistence.sqlite import (
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    create_session_factory,
)
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter
from paritygrid.api.app import create_app
from paritygrid.api.frontend import FrontendAssets
from paritygrid.api.middleware.request_limits import RequestLimitSettings
from paritygrid.api.operational import ReadinessResult
from paritygrid.application.execution.concurrency_settings import (
    CapturedConcurrencySettings,
)
from paritygrid.application.execution.runtime_capabilities import RuntimeStrategyCatalog
from paritygrid.application.ports.execution import RunRecord
from paritygrid.application.ports.writer import WriterSettings
from paritygrid.application.services.artifacts import ArtifactService
from paritygrid.application.services.capabilities import (
    CapabilitiesView,
    OperationalLimitsView,
    RunnerStrategyView,
    SqliteCapabilityView,
    SubordinatePoolView,
)
from paritygrid.application.services.connectors import (
    ConnectorService,
    ConnectorTestService,
)
from paritygrid.application.services.events import DurableEventStreamService
from paritygrid.application.services.idempotency import (
    IdempotencyLeasePolicy,
    IdempotentCommandService,
)
from paritygrid.application.services.pipelines import PipelineService
from paritygrid.application.services.reconciliation import ReconciliationService
from paritygrid.application.services.repair import RepairApplyService, RepairService
from paritygrid.application.services.runs import RunLifecycleService, RunService
from paritygrid.application.services.telemetry import (
    LiveTelemetryChannel,
    LiveTelemetryHub,
)
from paritygrid.domain.models import RunId, UtcTimestamp
from paritygrid.runtime.config import Settings
from paritygrid.runtime.execution_owners import RuntimeExecutionOwnership
from paritygrid.runtime.run_controls import RuntimeActiveRunControlRegistry

DEFAULT_SERVICE_NAME = "ParityGrid"
MAX_PAGE_SIZE = 100
WRITER_CLOSE_TIMEOUT_SECONDS = 5.0
DEFAULT_FRONTEND_DIST = Path("web/dist")
PACKAGED_FRONTEND_DIRECTORY = "_frontend"


class SystemEnvironment:
    """Resolve environment-variable existence without reading values."""

    def has(self, name: str) -> bool:
        return name in os.environ


@dataclass(slots=True)
class RuntimeServices:
    """The composed service surface installed on ``app.state.services``."""

    pipelines: PipelineService
    connectors: ConnectorService
    connector_tests: ConnectorTestService
    runs: RunService
    run_lifecycle: RunLifecycleService
    artifacts: ArtifactService
    idempotency: IdempotentCommandService
    capabilities: CapabilitiesView
    reconciliation: ReconciliationService
    repair: RepairService
    repair_application: RepairApplyService
    event_stream: DurableEventStreamService
    telemetry: LiveTelemetryChannel
    clock: Callable[[], UtcTimestamp]


@dataclass(slots=True)
class RuntimeContainer:
    """Every resource the lifespan owns, in startup order."""

    settings: Settings
    database: SQLiteDatabase
    writer: SQLiteTransactionalWriter
    active_run_controls: RuntimeActiveRunControlRegistry
    execution_ownership: RuntimeExecutionOwnership
    services: RuntimeServices
    limits: RequestLimitSettings
    started_steps: tuple[str, ...]
    shutdown: Callable[[], None]


@dataclass(frozen=True, slots=True)
class StartupStep:
    """One named resource with its opener and exactly-once closer."""

    name: str
    opener: Callable[[], object]
    closer: Callable[[object], object]


def run_startup_sequence(
    steps: list[StartupStep],
) -> tuple[dict[str, object], tuple[str, ...]]:
    """Open every step in order or roll back started steps in reverse."""
    started: dict[str, object] = {}
    order: list[str] = []
    for step in steps:
        try:
            started[step.name] = step.opener()
        except BaseException:
            shutdown_started(steps, started)
            raise
        order.append(step.name)
    return started, tuple(order)


def shutdown_started(steps: list[StartupStep], started: dict[str, object]) -> None:
    """Close started resources in reverse startup order."""
    closers = {step.name: step.closer for step in steps}
    first_error: BaseException | None = None
    for name in reversed(list(started)):
        try:
            closers[name](started[name])
        except BaseException as error:
            # Cleanup is best-effort for every owned resource. Preserve the
            # first failure only after later resources have also been closed.
            if first_error is None:
                first_error = error
    if first_error is not None:
        raise first_error


def _real_now() -> UtcTimestamp:
    from datetime import UTC, datetime

    return UtcTimestamp(datetime.now(UTC))


def compose_runtime(settings: Settings) -> RuntimeContainer:
    """Open every runtime resource in order or roll back cleanly."""
    resources: dict[str, object] = {}

    def open_database() -> SQLiteDatabase:
        database = SQLiteDatabase.open(
            SQLiteDatabaseConfig(settings.database_path, create_parent=True)
        )
        resources["database"] = database
        return database

    def run_migration() -> str:
        return _run_migration(cast(SQLiteDatabase, resources["database"]))

    def open_writer() -> SQLiteTransactionalWriter:
        return _start_writer(cast(SQLiteDatabase, resources["database"]), settings)

    steps = [
        StartupStep(
            name="data-root",
            opener=lambda: _prepare_directory(settings.data_root),
            closer=lambda _value: None,
        ),
        StartupStep(
            name="database",
            opener=open_database,
            closer=lambda value: cast(SQLiteDatabase, value).close(),
        ),
        StartupStep(
            name="migration",
            opener=run_migration,
            closer=lambda _value: None,
        ),
        StartupStep(
            name="artifact-root",
            opener=lambda: _prepare_artifact_root(settings.artifact_root_path),
            closer=lambda _value: None,
        ),
        StartupStep(
            name="writer",
            opener=open_writer,
            closer=lambda value: cast(SQLiteTransactionalWriter, value).close(
                timeout_seconds=WRITER_CLOSE_TIMEOUT_SECONDS
            ),
        ),
    ]
    started, order = run_startup_sequence(steps)

    active_run_controls: RuntimeActiveRunControlRegistry | None = None
    try:
        database = cast(SQLiteDatabase, started["database"])
        writer = cast(SQLiteTransactionalWriter, started["writer"])
        active_run_controls = RuntimeActiveRunControlRegistry()
        unit_of_work = SQLOperationalUnitOfWork(
            database,
            artifact_root=settings.artifact_root_path,
            artifact_chunk_bytes=settings.artifact_chunk_bytes,
        )
        execution_ownership = RuntimeExecutionOwnership(
            active_run_controls=active_run_controls,
            read_run=lambda run_id: _read_owned_run(unit_of_work, run_id),
        )
        limits = RequestLimitSettings(
            max_body_bytes=settings.max_request_body_bytes,
            max_json_depth=settings.max_json_depth,
            request_timeout_seconds=settings.request_timeout_seconds,
            max_concurrent_requests=settings.max_concurrent_requests,
        )
        repair_reader = SQLiteRepairWorkflowReader(database)
        event_stream = DurableEventStreamService(
            unit_of_work=unit_of_work,
            heartbeat_seconds=settings.stream_heartbeat_seconds,
            poll_seconds=settings.stream_poll_seconds,
        )
        telemetry_hub = LiveTelemetryHub(
            queue_capacity=settings.telemetry_queue_capacity,
            max_subscribers_per_run=settings.telemetry_max_subscribers_per_run,
        )
        telemetry_channel = LiveTelemetryChannel(
            hub=telemetry_hub,
            writer_snapshot=writer.snapshot,
            clock=_real_now,
            send_timeout_seconds=settings.telemetry_send_timeout_seconds,
            poll_seconds=settings.telemetry_poll_seconds,
        )
        services = RuntimeServices(
            pipelines=PipelineService(unit_of_work=unit_of_work, now=_real_now),
            connectors=ConnectorService(unit_of_work=unit_of_work, now=_real_now),
            connector_tests=ConnectorTestService(
                unit_of_work=unit_of_work,
                environment=SystemEnvironment(),
                now=_real_now,
            ),
            runs=RunService(unit_of_work=unit_of_work, writer=writer, now=_real_now),
            run_lifecycle=RunLifecycleService(
                unit_of_work=unit_of_work,
                writer=writer,
                now=_real_now,
                active_run_controls=active_run_controls,
            ),
            artifacts=ArtifactService(unit_of_work=unit_of_work, now=_real_now),
            idempotency=IdempotentCommandService(
                unit_of_work=unit_of_work,
                policy=IdempotencyLeasePolicy(lease_seconds=settings.idempotency_lease_seconds),
                now=_real_now,
            ),
            capabilities=_capabilities_from(database, settings),
            reconciliation=ReconciliationService(unit_of_work=unit_of_work),
            repair=RepairService(
                writer=writer,
                reader=repair_reader,
                unit_of_work=unit_of_work,
                now=_real_now,
            ),
            repair_application=RepairApplyService(
                writer=writer,
                reader=repair_reader,
                unit_of_work=unit_of_work,
                now=_real_now,
            ),
            event_stream=event_stream,
            telemetry=telemetry_channel,
            clock=_real_now,
        )
        return RuntimeContainer(
            settings=settings,
            database=database,
            writer=writer,
            active_run_controls=active_run_controls,
            execution_ownership=execution_ownership,
            services=services,
            limits=limits,
            started_steps=order,
            shutdown=lambda: _shutdown_runtime(
                active_run_controls, event_stream, telemetry_hub, steps, started
            ),
        )
    except BaseException:
        if active_run_controls is not None:
            active_run_controls.close()
        shutdown_started(steps, started)
        raise


def _shutdown_runtime(
    active_run_controls: RuntimeActiveRunControlRegistry,
    event_stream: DurableEventStreamService,
    telemetry_hub: LiveTelemetryHub,
    steps: list[StartupStep],
    started: dict[str, object],
) -> None:
    """Release live channels and execution owners before durable resources."""
    first_error: BaseException | None = None
    for release in (event_stream.stop, telemetry_hub.close, active_run_controls.close):
        try:
            release()
        except BaseException as error:
            if first_error is None:
                first_error = error
    try:
        shutdown_started(steps, started)
    except BaseException as error:
        if first_error is None:
            first_error = error
    if first_error is not None:
        raise first_error


def _read_owned_run(unit_of_work: SQLOperationalUnitOfWork, run_id: object) -> RunRecord:
    """Read one durable run for runtime-owned active execution evidence."""
    if type(run_id) is not RunId:
        raise TypeError("runtime execution ownership requires RunId")
    with unit_of_work.transaction() as repositories:
        run = repositories.runs.get(run_id)
    if run is None:
        raise LookupError("durable run does not exist")
    return run


def _prepare_directory(path: Path) -> Path:
    resolved = path.resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def _prepare_artifact_root(path: Path) -> Path:
    _prepare_directory(path)
    return resolve_artifact_root(path)


def _run_migration(database: SQLiteDatabase) -> str:
    with database.engine.connect() as connection:
        report = upgrade_to_head(connection)
    if report.current_revision != HEAD_REVISION:  # pragma: no cover - defensive
        raise RuntimeError("migration did not reach the head revision")
    return report.current_revision


def _start_writer(database: SQLiteDatabase, settings: Settings) -> SQLiteTransactionalWriter:
    writer = SQLiteTransactionalWriter(
        create_session_factory(database.engine),
        settings=WriterSettings(queue_capacity=settings.writer_queue_capacity),
    )
    writer.start()
    return writer


def _capabilities_from(database: SQLiteDatabase, settings: Settings) -> CapabilitiesView:
    sqlite = database.capabilities
    catalog = RuntimeStrategyCatalog(CapturedConcurrencySettings())
    catalog.register_detected()
    return CapabilitiesView(
        service=DEFAULT_SERVICE_NAME,
        version=__version__,
        sqlite=SqliteCapabilityView(
            library_version=sqlite.library_version,
            minimum_supported_version=".".join(
                str(part) for part in sqlite.minimum_supported_version
            ),
            threadsafety=sqlite.threadsafety,
            journal_mode=sqlite.journal_mode,
            synchronous_level=sqlite.synchronous_level,
            busy_timeout_ms=sqlite.busy_timeout_ms,
            supports_json_sql=sqlite.supports_json_sql,
            supports_returning=sqlite.supports_returning,
        ),
        runners=tuple(
            RunnerStrategyView(
                strategy_id=runner.strategy_id,
                available=runner.available,
                unavailability_reason=runner.unavailability_reason,
            )
            for runner in catalog.full_plan_strategies
        ),
        pools=tuple(
            SubordinatePoolView(
                pool_id=pool.pool_id,
                available=pool.available,
                unavailability_reason=pool.unavailability_reason,
            )
            for pool in catalog.subordinate_pools
        ),
        limits=OperationalLimitsView(
            max_request_body_bytes=settings.max_request_body_bytes,
            max_json_depth=settings.max_json_depth,
            max_concurrent_requests=settings.max_concurrent_requests,
            request_timeout_seconds=settings.request_timeout_seconds,
            max_page_size=MAX_PAGE_SIZE,
            idempotency_lease_seconds=settings.idempotency_lease_seconds,
            artifact_chunk_bytes=settings.artifact_chunk_bytes,
        ),
        features=(
            ("process_pool", _pool_available(catalog, "process")),
            ("interpreter_pool", _pool_available(catalog, "interpreter")),
        ),
    )


def _pool_available(catalog: RuntimeStrategyCatalog, pool_id: str) -> bool:
    return any(pool.pool_id == pool_id and pool.available for pool in catalog.subordinate_pools)


@dataclass(frozen=True, slots=True)
class RuntimeReadinessProbe:
    """Verify storage, writer, and composition health for ``/readyz``."""

    container_provider: Callable[[], RuntimeContainer | None]

    async def check(self) -> ReadinessResult:
        container = self.container_provider()
        if container is None:
            return ReadinessResult(ready=False, detail="Runtime composition has not started.")
        return await run_in_thread(_check_container_sync, container)


def _check_container_sync(container: RuntimeContainer) -> ReadinessResult:
    try:
        with container.database.engine.connect() as connection:
            connection.exec_driver_sql("SELECT 1")
    except Exception:
        return ReadinessResult(ready=False, detail="Operational storage is unavailable.")
    if container.writer.snapshot().state.value != "running":
        return ReadinessResult(ready=False, detail="The durable writer is not running.")
    return ReadinessResult(ready=True, detail="Runtime initialization is complete.")


def create_runtime_app(settings: Settings | None = None) -> FastAPI:
    """Compose the full runtime application with lifespan ownership."""
    captured = settings if settings is not None else Settings()
    holder: dict[str, RuntimeContainer | None] = {"container": None}

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncGenerator[None]:
        container = await run_in_thread(compose_runtime, captured)
        holder["container"] = container
        application.state.services = container.services
        application.state.container = container
        try:
            yield
        finally:
            holder["container"] = None
            await run_in_thread(_shutdown_container, container, abandon_on_cancel=False)

    application = create_app(
        service_name=DEFAULT_SERVICE_NAME,
        version=__version__,
        readiness=RuntimeReadinessProbe(container_provider=lambda: holder["container"]),
        limits=RequestLimitSettings(
            max_body_bytes=captured.max_request_body_bytes,
            max_json_depth=captured.max_json_depth,
            request_timeout_seconds=captured.request_timeout_seconds,
            max_concurrent_requests=captured.max_concurrent_requests,
        ),
        lifespan=lifespan,
        frontend=_resolve_frontend_assets(captured),
    )
    application.state.settings = captured
    return application


def _resolve_frontend_assets(settings: Settings) -> FrontendAssets | None:
    """Locate the packaged frontend distribution without opening resources.

    The explicit setting wins. Installed wheels use the frontend embedded
    alongside the Python package; source checkouts then fall back to their
    committed build. A missing distribution leaves the application fully
    operational without the SPA surface.
    """
    if settings.frontend_dist is not None:
        return _frontend_or_none(Path(settings.frontend_dist))
    installed_package_root = Path(__file__).resolve().parents[1]
    checkout_root = Path(__file__).resolve().parents[3]
    return (
        _frontend_or_none(installed_package_root / PACKAGED_FRONTEND_DIRECTORY)
        or _frontend_or_none(checkout_root / DEFAULT_FRONTEND_DIST)
        or _frontend_or_none(Path.cwd() / DEFAULT_FRONTEND_DIST)
    )


def _frontend_or_none(candidate: Path) -> FrontendAssets | None:
    try:
        return FrontendAssets(candidate)
    except ValueError:
        return None


def _shutdown_container(container: RuntimeContainer) -> None:
    container.shutdown()
