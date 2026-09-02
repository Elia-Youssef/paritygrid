"""One bounded demo lifecycle: root, runtime, simulators, app, story, proof.

The lifecycle composes the accepted runtime through the existing composition
root — it never builds a second dependency graph.  Startup is an ordered
sequence that rolls back in reverse order on any failure; shutdown is
idempotent, bounded, and releases every owned resource after success,
failure, cancellation, partial startup, or an ordinary shutdown.

Dynamic ports are race-free: the application server's loopback socket is
bound to port zero and ownership of that socket is retained until uvicorn
consumes it; the simulators bind port zero inside their accepted Phase 8
lifecycles.  Nothing ever closes a socket to "test" whether a port is free.

Headless mode never opens a browser: it runs the canonical story and the
selected engine run, verifies every required fact, writes the deterministic
result, and exits.  Serve mode keeps the application up so a browser can
drive the product, and owns an execution launcher that runs canonical engine
runs for runs created through the public API — with pause, resume, and
cancellation handled by the ordinary product run controls.
"""

import asyncio
import contextlib
import json
import socket
import threading
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

import uvicorn
from sqlalchemy import func, select

from paritygrid.adapters.analytics.duckdb import DuckDBLifecycleCoordinator
from paritygrid.adapters.analytics.run_statistics import DuckDBRunStatisticsQueryEngine
from paritygrid.adapters.artifacts.paths import resolve_artifact_root
from paritygrid.adapters.persistence import SQLiteFinalizationStateReader
from paritygrid.adapters.persistence.repositories import SqlAlchemyRunRepository
from paritygrid.adapters.persistence.schema import runs as runs_table
from paritygrid.adapters.persistence.schema import work_items as work_items_table
from paritygrid.application.execution.finalization import FinalizationSettings, RunFinalizer
from paritygrid.application.planner import PlanFingerprint
from paritygrid.application.ports.analytics import AnalyticalDatabaseConfig
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.writer import EventAppendRequest
from paritygrid.application.writes.execution import TransitionRun
from paritygrid.demo.engine_runner import (
    ENGINE_STRATEGIES,
    RealtimePacingClock,
    bootstrap_engine_work,
    build_canonical_engine,
    canonical_engine_nodes,
    demo_engine_harness,
    injected_engine_clock,
)
from paritygrid.demo.ownership import DemoRoot, open_or_create_demo_root
from paritygrid.demo.scenario_runner import (
    DATABASE_FILENAME,
    _event_frontier,  # pyright: ignore[reportPrivateUsage]  # accepted event-frontier reader
)
from paritygrid.demo.scenarios import (
    CANONICAL_CORRELATION_ID,
    CANONICAL_PIPELINE_ID,
    CANONICAL_PIPELINE_VERSION,
    CANONICAL_SCENARIO_VERSION,
    FAST_PROFILE,
    ScenarioExpectedEvidence,
    canonical_plan_fingerprint,
    derive_scenario,
)
from paritygrid.demo.simulators.async_source import AsyncInventorySource
from paritygrid.demo.simulators.blocking_source import BlockingInventorySource
from paritygrid.demo.simulators.lifecycle import probe_service_health
from paritygrid.demo.simulators.warehouse import SimulatedWarehouse
from paritygrid.demo.story import StoryOutcome, execute_demo_story
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import NodeId, PipelineVersion, RunId, UtcTimestamp
from paritygrid.quality.concurrent_scenario import ConcurrentScenarioHarness
from paritygrid.runtime.composition import RuntimeContainer, compose_runtime
from paritygrid.runtime.config import Settings

if TYPE_CHECKING:
    from paritygrid.application.execution.full_plan_strategy import ExecutedWork
    from paritygrid.application.execution.runner_contract import WorkAssignmentV1
    from paritygrid.demo.failures import FailureScript
    from paritygrid.demo.verification import RunnerExecutionRecord

DEMO_BIND_HOST = "127.0.0.1"
DEFAULT_READINESS_TIMEOUT_SECONDS = 30.0
DUCKDB_ANALYTICS_FILENAME = "analytics.duckdb"
ENGINE_ANALYTICS_FILENAME = "engine-analytics.duckdb"
CANONICAL_STORY_RUN_ID = "run_canonical-demo"
LAUNCHER_RUN_PREFIX = "run_can-engine"
LAUNCHER_POLL_SECONDS = 0.5
LAUNCHER_MAX_TRACKED_RUNS = 32
_LAUNCHER_RUN_TIMEOUT_SECONDS = 180.0
PACED_WORK_DELAY_SECONDS = 0.25
_ENGINE_STRATEGY_RUNNER_KINDS = frozenset(ENGINE_STRATEGIES)


class DemoLifecycleError(RuntimeError):
    """Raised when the demo lifecycle is misused or fails a startup step."""


class DemoReadinessTimeoutError(DemoLifecycleError):
    """Raised when the application did not become ready within its budget."""


class DemoUsageError(ValueError):
    """Raised when demo options are invalid before any resource is owned."""


@dataclass(frozen=True, slots=True)
class DemoOptions:
    """Validated options for one demo lifecycle."""

    root: Path
    runner: str
    headless: bool = True
    open_browser: bool = False
    readiness_timeout_seconds: float = DEFAULT_READINESS_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        if self.runner not in _ENGINE_STRATEGY_RUNNER_KINDS:
            raise DemoUsageError(
                f"runner {self.runner!r} is not a full-plan runner; the closed set is "
                f"{ENGINE_STRATEGIES}"
            )


@dataclass(frozen=True, slots=True)
class DemoStartupFacts:
    """Everything the startup sequence established."""

    demo_root: DemoRoot
    created_root: bool
    container: RuntimeContainer
    application_port: int
    simulator_ports: tuple[int, int, int]
    analytics_initialized: bool


class DemoLifecycle:
    """Own every demo resource for one bounded lifecycle."""

    def __init__(self, options: DemoOptions) -> None:
        self._options = options
        self._facts: DemoStartupFacts | None = None
        self._app_socket: socket.socket | None = None
        self._server: uvicorn.Server | None = None
        self._serve_task: asyncio.Task[None] | None = None
        self._async_source: AsyncInventorySource | None = None
        self._blocking_source: BlockingInventorySource | None = None
        self._warehouse: SimulatedWarehouse | None = None
        self._launcher: _RunLauncher | None = None
        self._container: RuntimeContainer | None = None
        self._closed = False

    @property
    def options(self) -> DemoOptions:
        """Return the frozen lifecycle options."""
        return self._options

    @property
    def facts(self) -> DemoStartupFacts:
        """Return the startup facts once startup has completed."""
        if self._facts is None:
            raise DemoLifecycleError("the demo lifecycle has not completed startup")
        return self._facts

    @property
    def browser_url(self) -> str:
        """Return the application URL; only meaningful after readiness."""
        return f"http://{DEMO_BIND_HOST}:{self.facts.application_port}/"

    def is_closed(self) -> bool:
        """Report whether shutdown has run."""
        return self._closed

    async def start(self) -> DemoStartupFacts:
        """Start every owned resource in order, or roll back cleanly."""
        if self._facts is not None:
            raise DemoLifecycleError("the demo lifecycle already started")
        if self._closed:
            raise DemoLifecycleError("the closed demo lifecycle cannot restart")
        evidence = derive_scenario(FAST_PROFILE)
        demo_root, created_root = open_or_create_demo_root(self._options.root)
        try:
            container = compose_runtime(self._runtime_settings(demo_root))
            self._container = container
            application_port = await self._start_application(
                demo_root, container, self._runtime_settings(demo_root)
            )
            ports = await self._start_simulators(evidence, state_root=demo_root.scenario_path)
            analytics_initialized = self._initialize_analytics(demo_root)
        except BaseException:
            await self.aclose()
            raise
        self._facts = DemoStartupFacts(
            demo_root=demo_root,
            created_root=created_root,
            container=container,
            application_port=application_port,
            simulator_ports=ports,
            analytics_initialized=analytics_initialized,
        )
        return self._facts

    async def run_canonical_evidence(
        self,
        *,
        failpoint: Callable[[str], None] | None = None,
    ) -> tuple[StoryOutcome, RunnerExecutionRecord, float, float]:
        """Run the canonical story and the selected engine run to terminal facts."""
        facts = self.facts
        evidence = derive_scenario(FAST_PROFILE)
        container = facts.container
        loop = asyncio.get_running_loop()
        async_source = self._async_source
        blocking_source = self._blocking_source
        warehouse = self._warehouse
        if async_source is None or blocking_source is None or warehouse is None:
            raise DemoLifecycleError("the canonical evidence requires started simulators")
        story_started = loop.time()
        story = await execute_demo_story(
            container,
            facts.demo_root.scenario_path,
            evidence,
            FAST_PROFILE,
            async_source=async_source,
            blocking_source=blocking_source,
            warehouse=warehouse,
            failpoint=failpoint,
        )
        story_seconds = loop.time() - story_started
        engine_started = loop.time()
        engine_record = await loop.run_in_executor(
            None,
            self._execute_headless_engine,
        )
        engine_seconds = loop.time() - engine_started
        return story, engine_record, story_seconds, engine_seconds

    def start_launcher(self) -> None:
        """Start the serve-mode execution launcher for public run creation."""
        if self._facts is None:
            raise DemoLifecycleError("the launcher requires completed startup")
        if self._launcher is not None:
            raise DemoLifecycleError("the execution launcher is already running")
        self._launcher = _RunLauncher(self)
        self._launcher.start()

    def stop_launcher(self) -> None:
        """Stop the execution launcher and wait for its bounded shutdown."""
        if self._launcher is not None:
            self._launcher.stop()
            self._launcher = None

    async def serve_until(self, stop: asyncio.Event) -> None:
        """Keep the application serving until the stop event fires."""
        await stop.wait()

    async def aclose(self) -> None:
        """Close every owned resource in reverse startup order; idempotent."""
        if self._closed:
            return
        self._closed = True
        self.stop_launcher()
        if self._server is not None:
            self._server.should_exit = True
        if self._serve_task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self._serve_task), timeout=10.0)
            except TimeoutError, Exception:
                self._serve_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._serve_task
            self._serve_task = None
        self._server = None
        for service in (self._warehouse, self._async_source, self._blocking_source):
            if service is not None:
                with contextlib.suppress(RuntimeError, OSError):
                    await service.aclose()
        self._warehouse = None
        self._async_source = None
        self._blocking_source = None
        if self._app_socket is not None:
            with contextlib.suppress(OSError):
                self._app_socket.close()
            self._app_socket = None
        # The composed container shuts down even when startup failed before
        # the facts snapshot was published, so no writer thread can survive.
        if self._container is not None:
            with contextlib.suppress(Exception):
                self._container.shutdown()
            self._container = None
            if self._facts is not None:
                self._facts = None

    def _runtime_settings(self, demo_root: DemoRoot) -> Settings:
        # The demo binds its own loopback socket on port zero and hands it to
        # the server, so the settings port field stays an unused placeholder.
        return Settings(
            bind_host=DEMO_BIND_HOST,
            data_root=demo_root.scenario_path,
            database_filename=DATABASE_FILENAME,
            artifact_root_name="artifacts",
            log_level="warning",
        )

    async def _start_application(
        self, demo_root: DemoRoot, container: RuntimeContainer, settings: Settings
    ) -> int:
        """Bind the loopback socket, keep ownership, and serve the composed app.

        The application serves the demo's already-composed runtime container:
        the demo layer never builds a second dependency graph, so every fact
        the story commits durably is visible through the API immediately.
        """
        from collections.abc import AsyncGenerator

        from fastapi import FastAPI

        from paritygrid import __version__
        from paritygrid.api.app import create_app
        from paritygrid.api.middleware.request_limits import RequestLimitSettings
        from paritygrid.runtime.composition import (
            DEFAULT_SERVICE_NAME,
            RuntimeReadinessProbe,
            # The frontend asset resolver is composition-root owned; the demo
            # must serve the exact same packaged assets as the ordinary app.
            _resolve_frontend_assets,  # pyright: ignore[reportPrivateUsage]
        )

        holder: dict[str, RuntimeContainer] = {"container": container}

        @asynccontextmanager
        async def lifespan(application: FastAPI) -> AsyncGenerator[None]:
            application.state.services = holder["container"].services
            application.state.container = holder["container"]
            try:
                yield
            finally:
                # The demo lifecycle, not the application lifespan, owns the
                # composed container's shutdown ordering.
                pass

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((DEMO_BIND_HOST, 0))
        sock.listen(128)
        self._app_socket = sock
        port = int(sock.getsockname()[1])
        application = create_app(
            service_name=DEFAULT_SERVICE_NAME,
            version=__version__,
            readiness=RuntimeReadinessProbe(container_provider=lambda: holder["container"]),
            limits=RequestLimitSettings(
                max_body_bytes=settings.max_request_body_bytes,
                max_json_depth=settings.max_json_depth,
                request_timeout_seconds=settings.request_timeout_seconds,
                max_concurrent_requests=settings.max_concurrent_requests,
            ),
            lifespan=lifespan,
            frontend=_resolve_frontend_assets(settings),
        )
        application.state.settings = settings
        config = uvicorn.Config(
            application,
            log_level="warning",
            lifespan="on",
            access_log=False,
        )
        server = uvicorn.Server(config)
        self._server = server
        self._serve_task = asyncio.get_running_loop().create_task(server.serve(sockets=[sock]))
        await self._await_application_readiness(port)
        return port

    async def _await_application_readiness(self, port: int) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._options.readiness_timeout_seconds
        while True:
            if self._server is not None and self._server.started:
                try:
                    await probe_application_health(port, timeout_seconds=2.0)
                    return
                except OSError, DemoLifecycleError, TimeoutError:
                    pass
            if loop.time() >= deadline:
                raise DemoReadinessTimeoutError(
                    "the application did not become ready within its bounded budget"
                )
            await asyncio.sleep(0.1)

    async def _start_simulators(
        self,
        evidence: ScenarioExpectedEvidence,
        *,
        state_root: Path,
    ) -> tuple[int, int, int]:
        profile = FAST_PROFILE
        self._async_source = AsyncInventorySource(
            evidence.slice_for("async_http").dataset,
            evidence.source_failure_script,
            max_page_size=profile.async_page_size,
            request_latency_microseconds=profile.source_latency_microseconds,
        )
        await self._async_source.start()
        self._blocking_source = BlockingInventorySource(
            evidence.slice_for("blocking_http").dataset,
            _empty_script(),
            max_page_size=profile.blocking_page_size,
            request_latency_microseconds=profile.source_latency_microseconds,
        )
        self._blocking_source.start()
        # The demo root is validated and explicitly owned before simulator
        # startup.  Persisting the target's logical effects and idempotency
        # receipts here makes an interruption restart continue against the
        # same external-target model instead of silently starting a fresh one.
        self._warehouse = SimulatedWarehouse(
            evidence.warehouse_failure_script,
            state_root=state_root,
        )
        await self._warehouse.start()
        for service_name, base_url in (
            ("async-source", self._async_source.base_url),
            ("blocking-source", self._blocking_source.base_url),
            ("warehouse", self._warehouse.base_url),
        ):
            await probe_service_health(
                base_url, expected_service=service_name, timeout_seconds=10.0
            )
        return (self._async_source.port, self._blocking_source.port, self._warehouse.port)

    def _initialize_analytics(self, demo_root: DemoRoot) -> bool:
        analytics_path = (demo_root.scenario_path / DUCKDB_ANALYTICS_FILENAME).resolve()
        coordinator = DuckDBLifecycleCoordinator(AnalyticalDatabaseConfig(analytics_path))
        coordinator.open()
        coordinator.close()
        return True

    def _execute_headless_engine(self) -> RunnerExecutionRecord:
        """Run the selected engine strategy with the deterministic clock.

        Each full-plan runner owns one stable engine run identity — the same
        identities the accepted Phase 19 cross-runner manifest freezes — so a
        demo root holds all three runs side by side and no runner can be
        silently substituted by another's evidence.
        """
        facts = self.facts
        container = facts.container
        harness = demo_engine_harness(
            container.database,
            container.writer,
            resolve_artifact_root(container.settings.artifact_root_path),
            injected_engine_clock(),
            PipelineVersion(CANONICAL_PIPELINE_VERSION),
        )
        from paritygrid.demo.engine_runner import ENGINE_RUN_OFFSETS, run_demo_engine_strategy

        run_id = RunId(f"{LAUNCHER_RUN_PREFIX}-{ENGINE_RUN_OFFSETS[self._options.runner]:04d}")
        return run_demo_engine_strategy(
            harness,
            self._options.runner,
            run_id,
            analytics_path=(facts.demo_root.scenario_path / ENGINE_ANALYTICS_FILENAME).resolve(),
            runner_configuration={"plane": "engine", "scenario": CANONICAL_SCENARIO_VERSION},
        )


def _empty_script() -> FailureScript:
    from paritygrid.demo.failures import FailureScript

    return FailureScript.empty()


async def probe_application_health(port: int, *, timeout_seconds: float) -> None:
    """Require a real loopback health answer before readiness is published."""
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(DEMO_BIND_HOST, port), timeout=timeout_seconds
    )
    try:
        request = (
            f"GET /healthz HTTP/1.1\r\nHost: {DEMO_BIND_HOST}:{port}\r\nConnection: close\r\n\r\n"
        ).encode("ascii")
        writer.write(request)
        await asyncio.wait_for(writer.drain(), timeout=timeout_seconds)
        raw = await asyncio.wait_for(reader.read(-1), timeout=timeout_seconds)
    finally:
        writer.close()
        with contextlib.suppress(ConnectionError, OSError):
            await writer.wait_closed()
    head, _, body = raw.partition(b"\r\n\r\n")
    status_line = head.split(b"\r\n", 1)[0]
    if len(status_line.split(b" ")) < 2 or status_line.split(b" ")[1] != b"200":
        raise DemoLifecycleError("the application health probe did not return 200")
    try:
        document: object = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as error:
        raise DemoLifecycleError(
            "the application health probe returned an unreadable body"
        ) from error
    if not isinstance(document, dict):
        raise DemoLifecycleError("the application health probe saw no ok status")
    # json.loads is untyped at the boundary; the isinstance guard above proves
    # the object shape before the bounded health document is read.
    health = cast("dict[str, object]", document)
    if health.get("status") != "ok":
        raise DemoLifecycleError("the application health probe saw no ok status")


class _RunLauncher:
    """Serve-mode execution owner for runs created through the public API.

    The HTTP boundary does not own runners, so the demo owns exactly one
    launcher: it durably polls for queued canonical runs, transitions them to
    running, bootstraps the canonical engine work, and hands the real engine
    to the runtime execution ownership bridge so the product's own pause,
    resume, and cancellation controls operate on it.  Polling reads short
    transactions over committed rows only; pacing is a demo scenario knob,
    never a correctness mechanism.
    """

    def __init__(self, lifecycle: DemoLifecycle) -> None:
        self._lifecycle = lifecycle
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._claimed: set[str] = set()
        self._claim_lock = threading.Lock()

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._poll_loop, name="paritygrid-demo-launcher", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=10.0)
        self._thread = None

    def _poll_loop(self) -> None:
        while not self._stop.wait(LAUNCHER_POLL_SECONDS):
            try:
                run_ids = self._queued_canonical_runs()
            except Exception:
                continue
            for run_id in run_ids:
                with self._claim_lock:
                    if run_id in self._claimed:
                        continue
                    if len(self._claimed) >= LAUNCHER_MAX_TRACKED_RUNS:
                        _launcher_diagnostic(
                            "the launcher reached its tracked-run bound; further "
                            "public runs will not be executed in this session"
                        )
                        return
                    self._claimed.add(run_id)
                try:
                    self._launch(RunId(run_id))
                except Exception as error:
                    # One bounded redacted line per failed launch: the demo
                    # operator must see why a public run is not executing,
                    # and the run returns to the claimable pool.
                    detail = str(error)[:200]
                    suffix = f": {detail}" if detail else ""
                    _launcher_diagnostic(
                        f"run {run_id} was not executed: {type(error).__name__}{suffix}"
                    )
                    with self._claim_lock:
                        self._claimed.discard(run_id)

    def _queued_canonical_runs(self) -> list[str]:
        facts = self._lifecycle.facts
        with facts.container.database.transaction() as session:
            rows = session.execute(
                select(runs_table.c.run_id, runs_table.c.runner_kind).where(
                    runs_table.c.pipeline_id == CANONICAL_PIPELINE_ID,
                    runs_table.c.state == RunState.QUEUED.value,
                )
            ).all()
        return [
            str(row.run_id) for row in rows if str(row.runner_kind) in _ENGINE_STRATEGY_RUNNER_KINDS
        ]

    def _launch(self, run_id: RunId) -> None:
        facts = self._lifecycle.facts
        container = facts.container
        strategy_id = self._strategy_of(run_id)
        analytics_path = (facts.demo_root.scenario_path / ENGINE_ANALYTICS_FILENAME).resolve()
        clock = RealtimePacingClock()
        harness = demo_engine_harness(
            container.database,
            container.writer,
            resolve_artifact_root(container.settings.artifact_root_path),
            clock,
            PipelineVersion(CANONICAL_PIPELINE_VERSION),
        )
        self._transition_running(harness, run_id)
        # The run was created through the public API, so only its work items
        # are bootstrapped here — never a second run creation.
        bootstrap_engine_work(harness, run_id)
        engine = build_canonical_engine(
            harness,
            run_id,
            strategy=_strategy_type(strategy_id)(),
            executor=_PacedCanonicalExecutor(harness, PACED_WORK_DELAY_SECONDS),
        )
        owner = container.execution_ownership.start_concurrent(engine)
        try:
            self._await_finalizable(run_id)
        finally:
            owner.close(timeout_seconds=10.0)
        self._finalize_if_succeeded(harness, run_id, analytics_path)
        self._require_terminal(run_id)

    def _strategy_of(self, run_id: RunId) -> str:
        facts = self._lifecycle.facts
        with facts.container.database.transaction() as session:
            run = SqlAlchemyRunRepository(session).get(run_id)
        if run is None or run.runner_kind not in _ENGINE_STRATEGY_RUNNER_KINDS:
            raise DemoLifecycleError("the launcher run does not carry a full-plan runner")
        return run.runner_kind

    def _transition_running(self, harness: ConcurrentScenarioHarness, run_id: RunId) -> None:
        facts = self._lifecycle.facts
        with facts.container.database.transaction() as session:
            row = session.execute(
                select(runs_table.c.row_version).where(runs_table.c.run_id == run_id.value)
            ).first()
        if row is None:
            raise DemoLifecycleError("the launcher run vanished before its start transition")
        # The transition instant and its durable event instant are one shared
        # timestamp: the writer rejects a transition whose event time differs.
        now = UtcTimestamp(_utc_now())
        receipt = facts.container.writer.submit(
            TransitionRun(
                run_id=run_id,
                expected_run_row_version=int(row.row_version),
                target_state=RunState.RUNNING,
                transitioned_at=now,
                execution_evidence_fingerprint=None,
                execution_evidence_fingerprint_version=None,
                event=self._launcher_event(run_id, "run_started", now),
            ),
            timeout_seconds=5.0,
        )
        receipt.result(timeout_seconds=30.0)

    def _launcher_event(
        self, run_id: RunId, kind: str, occurred_at: UtcTimestamp
    ) -> EventAppendRequest:
        facts = self._lifecycle.facts
        sequence, _counter_row_version = _event_frontier(facts.container.database, run_id)
        return EventAppendRequest(
            EventSequence(sequence),
            sequence,
            PendingExecutionEvent(
                kind,
                occurred_at,
                EventSubjectKind.RUN,
                run_id,
                CANONICAL_CORRELATION_ID,
                1,
                RedactedDocument.from_mapping({"kind": kind}),
            ),
        )

    def _await_finalizable(self, run_id: RunId) -> None:
        """Wait until the engine pass leaves the run finalizable or terminal.

        The concurrent engine moves work items to terminal states but never
        finalizes the run, so the durable predicate is "running with every
        work item terminal" — finalization then transitions the run row.
        """
        facts = self._lifecycle.facts
        deadline = time.monotonic() + _LAUNCHER_RUN_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            with facts.container.database.transaction() as session:
                row = session.execute(
                    select(
                        select(runs_table.c.state)
                        .where(runs_table.c.run_id == run_id.value)
                        .scalar_subquery(),
                        select(func.count())
                        .select_from(work_items_table)
                        .where(
                            work_items_table.c.run_id == run_id.value,
                            work_items_table.c.state.not_in(
                                ("succeeded", "quarantined", "failed", "cancelled")
                            ),
                        )
                        .scalar_subquery(),
                    )
                ).first()
            if row is not None:
                state = str(row[0])
                open_work = int(row[1])
                if state in ("succeeded", "cancelled", "failed"):
                    return
                if state == "running" and open_work == 0:
                    return
            time.sleep(0.25)
        raise DemoLifecycleError("the launcher run did not become finalizable within its budget")

    def _require_terminal(self, run_id: RunId) -> None:
        facts = self._lifecycle.facts
        with facts.container.database.transaction() as session:
            row = session.execute(
                select(runs_table.c.state).where(runs_table.c.run_id == run_id.value)
            ).first()
        state = None if row is None else str(row.state)
        if state == "running":
            raise DemoLifecycleError("the launcher run did not finalize after execution")
        if state not in ("succeeded", "cancelled", "failed"):
            raise DemoLifecycleError(
                f"the launcher run ended in an unexpected durable state: {state}"
            )

    def _finalize_if_succeeded(
        self, harness: ConcurrentScenarioHarness, run_id: RunId, analytics_path: Path
    ) -> None:
        """Finalize the executed launcher run.

        A completed engine pass leaves a finalizable ``running`` run, which
        the accepted finalizer transitions. Cancellation and failure already
        own their terminal arrows and are deliberately not success-finalized.
        """
        facts = self._lifecycle.facts
        with facts.container.database.transaction() as session:
            row = session.execute(
                select(runs_table.c.state).where(runs_table.c.run_id == run_id.value)
            ).first()
        if row is None:
            raise DemoLifecycleError("the launcher run vanished before finalization")
        if str(row.state) in ("cancelled", "failed"):
            # Cancellation and failure are already terminal lifecycle facts.
            # The accepted cancellation contract permits never-admitted work
            # to remain pending, so the success finalizer must not be invoked.
            return
        coordinator = DuckDBLifecycleCoordinator(AnalyticalDatabaseConfig(analytics_path))
        coordinator.open()
        try:
            finalizer = RunFinalizer(
                facts.container.writer,
                SQLiteFinalizationStateReader(facts.container.database),
                DuckDBRunStatisticsQueryEngine(coordinator),
                injected_engine_clock(),
                settings=FinalizationSettings(5.0, 5.0),
            )
            finalizer.finalize(
                run_id,
                plan_nodes=tuple(NodeId(node) for node in canonical_engine_nodes()),
                plan_fingerprint=PlanFingerprint(canonical_plan_fingerprint()),
            )
        finally:
            coordinator.close()


def _strategy_type(strategy_id: str) -> type:
    # The closed strategy registry is engine_runner's private table; the demo
    # launcher resolves through it rather than duplicating the mapping.
    from paritygrid.demo.engine_runner import _STRATEGY_TYPES  # pyright: ignore[reportPrivateUsage]

    strategy_type = _STRATEGY_TYPES.get(strategy_id)
    if strategy_type is None:
        raise DemoLifecycleError(f"{strategy_id!r} is not a full-plan runner")
    return strategy_type


def _launcher_diagnostic(message: str) -> None:
    """Emit one bounded, redacted launcher diagnostic on stderr."""
    import sys

    print(f"[demo-launcher] {message}", file=sys.stderr, flush=True)


class _PacedCanonicalExecutor:
    """Wrap the canonical executor with fixed per-work scenario pacing.

    Pacing makes live engine runs observable in a browser the same way the
    simulators' request latency makes scenario reads observable.  It changes
    wall duration only — never a durable decision, identity, or fingerprint.
    """

    def __init__(self, harness: ConcurrentScenarioHarness, delay_seconds: float) -> None:
        from paritygrid.demo.verification import CanonicalEngineExecutor

        self._inner = CanonicalEngineExecutor(harness)
        self._delay = delay_seconds

    @property
    def executed(self) -> list[tuple[str, str, int]]:
        return self._inner.executed

    def execute(self, assignment: WorkAssignmentV1) -> ExecutedWork:
        time.sleep(self._delay)
        return self._inner.execute(assignment)

    def close(self) -> None:
        self._inner.close()


def _utc_now() -> datetime:
    return datetime.now(UTC)
