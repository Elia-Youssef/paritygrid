"""Executable canonical scenario: an isolated, complete product story run.

The runner composes the accepted building blocks into the versioned canonical
story inside one caller-provided isolated root: deterministic fixtures on
disk, three loopback simulators, the production connector adapters, the
durable SQLite execution plane with real work-item attempts and retries, the
run finalizer, reconciliation analysis, the approval-gated repair workflow,
independent target verification, and a content-addressed conflict artifact.

Every step is deterministic.  Time comes from an injected clock, identifiers
are stable strings, dynamic ports stay out of every canonical document, and
the final scenario manifest is published atomically only after every locked
fact has been verified against the pure derivation.  A failed or interrupted
run can therefore never leave a falsely accepted manifest.

Path safety: the runner writes only beneath the validated isolated root and
never deletes anything.  The root and every write target are validated against
traversal, absolute paths where relative ones are required, symlink escapes,
Windows alternate data streams, reserved filenames, and broad system
directories before any byte is written.
"""

import asyncio
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PureWindowsPath
from typing import cast

from sqlalchemy import select

import paritygrid.demo.scenarios as scenario
from paritygrid.adapters.analytics.duckdb import DuckDBLifecycleCoordinator
from paritygrid.adapters.analytics.run_statistics import DuckDBRunStatisticsQueryEngine
from paritygrid.adapters.artifacts import (
    FileSystemArtifactManifestRepository,
    FileSystemArtifactWriter,
)
from paritygrid.adapters.artifacts.parquet.partitions import AtomicParquetPartitionWriter
from paritygrid.adapters.connectors.async_source import (
    AsyncHttpSourceConfig,
    AsyncHttpSourceConnector,
)
from paritygrid.adapters.connectors.blocking_source import (
    BlockingHttpSourceConfig,
    BlockingHttpSourceConnector,
)
from paritygrid.adapters.connectors.csv_source import CsvFileSourceConfig, CsvFileSourceConnector
from paritygrid.adapters.connectors.file_support import SourceFileLocation
from paritygrid.adapters.connectors.jsonl_source import (
    JsonlFileSourceConfig,
    JsonlFileSourceConnector,
)
from paritygrid.adapters.connectors.warehouse_target import (
    WarehouseTargetConfig,
    WarehouseTargetConnector,
)
from paritygrid.adapters.persistence.finalization import SQLiteFinalizationStateReader
from paritygrid.adapters.persistence.migration import upgrade_to_head
from paritygrid.adapters.persistence.operational import SQLOperationalUnitOfWork
from paritygrid.adapters.persistence.repair_workflow import SQLiteRepairWorkflowReader
from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyRunRepository,
    SqlAlchemyWorkItemRepository,
)
from paritygrid.adapters.persistence.schema import (
    artifact_manifests,
    run_event_counters,
    work_attempts,
    work_items,
)
from paritygrid.adapters.persistence.sqlite import (
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    create_session_factory,
)
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter, WriterSettings
from paritygrid.application.execution import (
    AttemptEventContext,
    AttemptFailed,
    AttemptSucceeded,
    TransactionalCheckpointResultSink,
    submit_work_result,
)
from paritygrid.application.execution.finalization import FinalizationSettings, RunFinalizer
from paritygrid.application.execution.leasing import (
    AcquireWorkLeaseRequest,
    WorkLeaseService,
    WorkLeaseSettings,
)
from paritygrid.application.execution.result_sink import (
    ResultCheckpoint,
    ResultMetrics,
    ResultSubmission,
    SuccessfulWorkResult,
    UnsuccessfulWorkResult,
)
from paritygrid.application.execution.retry_policy import (
    BoundedExponentialRetryPolicy,
    Http429RetryDelay,
    RetryPolicyRequest,
    RetryScheduledDecision,
    SeededRetryJitterSource,
)
from paritygrid.application.planner import PlanFingerprint, PlannerRunnerKind
from paritygrid.application.planner.execution_plan import compile_execution_plan
from paritygrid.application.planner.plan_fingerprint import fingerprint_execution_plan
from paritygrid.application.planner.publication import PublishedPipelineSpecification
from paritygrid.application.ports.analytics import AnalyticalDatabaseConfig
from paritygrid.application.ports.artifacts import ArtifactRelativePath
from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.connectors import (
    ConnectorAmbiguousError,
    ConnectorCallContext,
    ConnectorRateLimitedError,
    SourceOutcome,
    SourceRecord,
    TargetWritePrecondition,
    TargetWriteRequest,
)
from paritygrid.application.ports.consistency import (
    EventSequence,
    EventSubjectKind,
    PendingExecutionEvent,
    RedactedDocument,
)
from paritygrid.application.ports.parquet import ReconciliationConflictBatch
from paritygrid.application.ports.reconciliation_persistence import (
    TargetVerificationVerdict,
)
from paritygrid.application.ports.repair_audit import (
    RepairActionRecord,
    RepairPlanAggregate,
)
from paritygrid.application.ports.run_aggregates import WorkMetricDelta
from paritygrid.application.ports.writer import EventAppendRequest
from paritygrid.application.reconciliation.analysis import (
    ReconciliationAnalysis,
    ReconciliationAnalysisRequest,
    analyze_reconciliation,
)
from paritygrid.application.reconciliation.publication import publish_conflict_artifact
from paritygrid.application.repair import (
    ReconciliationResultService,
    RepairApplicationPolicy,
    RepairApplicationService,
    RepairApprovalRequest,
    RepairApprovalService,
    RepairPlanningService,
    TargetParityVerifier,
    TargetVerificationService,
    build_expected_inventory,
)
from paritygrid.application.repair.errors import RepairPlanStateError
from paritygrid.application.repair.payloads import render_effect_payload
from paritygrid.application.services.connectors import ConnectorService
from paritygrid.application.services.pipelines import PipelineService
from paritygrid.application.writes.execution import (
    BootstrapWork,
    CreateCapturedRun,
    TransitionRun,
)
from paritygrid.demo.datasets import SyntheticDataset
from paritygrid.demo.failures import AppliedFailure, FailureScript, ScriptedFailureKind
from paritygrid.demo.fixtures import write_csv_fixture, write_jsonl_fixture
from paritygrid.demo.scenarios import (
    CANONICAL_CORRELATION_ID,
    CANONICAL_PIPELINE_ID,
    CANONICAL_PIPELINE_VERSION,
    CANONICAL_RUN_ID,
    CANONICAL_SCENARIO_SEED,
    SCENARIO_FORMAT_NAME,
    CanonicalScenarioManifest,
    CanonicalScenarioProfile,
    ScenarioError,
    ScenarioExpectedEvidence,
    SourceSlice,
    build_manifest,
    derive_scenario,
)
from paritygrid.demo.simulators.async_source import AsyncInventorySource
from paritygrid.demo.simulators.blocking_source import BlockingInventorySource
from paritygrid.demo.simulators.lifecycle import probe_service_health
from paritygrid.demo.simulators.warehouse import SimulatedWarehouse
from paritygrid.domain.execution import FailureClassification, RunState
from paritygrid.domain.models import (
    ArtifactId,
    AttemptNumber,
    ConnectorId,
    Duration,
    NodeId,
    PipelineId,
    PipelineVersion,
    RunId,
    UtcTimestamp,
    WorkItemId,
)
from paritygrid.domain.pipeline import PartitionKey
from paritygrid.domain.reconciliation import (
    ReconciliationClassification,
    SourceObservation,
)

MANIFEST_FILENAME = "scenario-manifest.json"
FIXTURES_DIRNAME = "fixtures"
ARTIFACTS_DIRNAME = "artifacts"
DATABASE_FILENAME = "canonical.db"
ANALYTICS_FILENAME = "analytics.duckdb"
CSV_FIXTURE_NAME = "canonical-source.csv"
JSONL_FIXTURE_NAME = "canonical-source.jsonl"

_LEASE_OWNER = "canonical-runner"
_WORKER_IDENTITY = "canonical-worker"
_RUNNER_KIND = "sequential"
_DEFAULT_LEASE_DURATION = Duration(3_600_000_000)
_ARTIFACT_BYTE_LIMIT = 64 * 1024 * 1024
# Fixed per-source position offsets keep every observation identity unique
# while the connectors number their own pages from zero.
_POSITION_BASES: dict[str, int] = {
    "async_http": 0,
    "blocking_http": 100_000,
    "csv": 200_000,
    "jsonl": 300_000,
}
_TARGET_POSITION_BASE = 1_000_000
_SOURCE_NODE_FOR_KEY: dict[str, str] = {
    "async_http": scenario.NODE_ASYNC_SOURCE,
}

# Named durable story boundaries.  A failpoint hook is invoked only after the
# boundary's commits returned durably, so interrupting at a name never guesses
# with timing and never loses a committed fact.
STORY_FAILPOINT_ATTEMPTS_RECORDED = "attempts.recorded"
STORY_FAILPOINT_RECONCILIATION_PERSISTED = "reconciliation.persisted"
STORY_FAILPOINT_REPAIR_APPROVED = "repair.approved"
STORY_FAILPOINT_REPAIR_APPLIED = "repair.applied"
STORY_FAILPOINT_NAMES: tuple[str, ...] = (
    STORY_FAILPOINT_ATTEMPTS_RECORDED,
    STORY_FAILPOINT_RECONCILIATION_PERSISTED,
    STORY_FAILPOINT_REPAIR_APPROVED,
    STORY_FAILPOINT_REPAIR_APPLIED,
)

# The tilde is accepted because valid Windows 8.3 short-name components
# (such as RUNNER~1 on standard runner accounts) may appear in any parent
# directory of an explicitly chosen root; traversal, reserved-name, and
# link checks are independent of this character class.
_SAFE_COMPONENT = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._~-]{0,63}\Z")
_RESERVED_NAMES = frozenset(
    {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        *(f"COM{number}" for number in range(10)),
        *(f"LPT{number}" for number in range(10)),
    }
)
_BROAD_HOME_FOLDERS = ("Desktop", "Documents", "Downloads")


def _only_case_change(candidate: Path, resolved: Path) -> bool:
    """Report whether resolve() only changed the drive-letter case."""
    return str(candidate).lower() == str(resolved).lower()


def _is_short_name_alias(candidate: Path, resolved: Path) -> bool:
    """Report whether candidate is the 8.3 short-name form of resolved.

    Windows ``resolve()`` expands 8.3 short-name components of existing
    directories, so a perfectly ordinary ``RUNNER~1``-style root would
    otherwise look like a link escape.  The operating system itself
    confirms the alias mapping on the deepest existing ancestor — final
    components may not exist yet because the root creates them — by
    requiring the long-name expansion of the candidate prefix to equal
    the resolved prefix.  Expanding the candidate is important when only
    one ancestor uses its 8.3 spelling while later components retain their
    long names.  Junctions and symlinks never match this check because
    long-name expansion preserves the link path while ``resolve()``
    rewrites it to the target.
    """
    if os.name != "nt":
        return False
    probe_candidate = candidate
    probe_resolved = resolved
    while not probe_candidate.exists() and probe_candidate.parent != probe_candidate:
        probe_candidate = probe_candidate.parent
        probe_resolved = probe_resolved.parent
    if not probe_candidate.exists():
        return False
    for existing in (probe_candidate, *probe_candidate.parents):
        if existing.is_symlink() or existing.is_junction():
            return False
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.windll.kernel32
    get_long_path_name = getattr(kernel32, "GetLongPathNameW", None)
    if get_long_path_name is None:
        return False
    get_long_path_name.argtypes = (wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD)
    get_long_path_name.restype = wintypes.DWORD
    buffer = ctypes.create_unicode_buffer(32768)
    if get_long_path_name(str(probe_candidate), buffer, len(buffer)) == 0:
        return False
    return buffer.value.lower() == str(probe_resolved).lower()


class ScenarioPathError(ScenarioError):
    """Raised when a scenario root or write target is not safely confined."""


def _validate_component(part: str) -> None:
    if part in (".", ".."):
        raise ScenarioPathError("scenario paths must not traverse")
    if ":" in part:
        raise ScenarioPathError("scenario paths must not carry drive or stream separators")
    if not _SAFE_COMPONENT.fullmatch(part):
        raise ScenarioPathError(f"scenario path component is malformed: {part!r}")
    if part.split(".")[0].upper() in _RESERVED_NAMES:
        raise ScenarioPathError(f"scenario path component is reserved: {part!r}")
    if part.endswith((".", " ")):
        raise ScenarioPathError("scenario path components must not end with a dot or space")


def _reject_broad_roots(resolved: Path) -> None:
    home = Path.home().resolve()
    cwd = Path.cwd().resolve()
    if resolved == home or resolved in home.parents:
        raise ScenarioPathError("the scenario root must not be a broad user directory")
    for name in _BROAD_HOME_FOLDERS:
        if resolved == home / name:
            raise ScenarioPathError(f"the {name} folder is not an isolated scenario root")
    if resolved == cwd or resolved in cwd.parents:
        raise ScenarioPathError(
            "the scenario root must not contain the working directory or any of its ancestors"
        )


@dataclass(frozen=True, slots=True)
class ScenarioRoot:
    """One validated isolated root that owns every generated file."""

    path: Path

    @property
    def fixtures(self) -> Path:
        """Return the validated fixtures directory."""
        return self._existing_dir(FIXTURES_DIRNAME)

    @property
    def artifacts(self) -> Path:
        """Return the validated artifacts directory."""
        return self._existing_dir(ARTIFACTS_DIRNAME)

    def manifest_path(self) -> Path:
        """Return the final manifest target inside the root."""
        return self.child(MANIFEST_FILENAME)

    def database_path(self) -> Path:
        """Return the operational database path inside the root."""
        return self.child(DATABASE_FILENAME)

    def analytics_path(self) -> Path:
        """Return the analytical database path inside the root."""
        return self.child(ANALYTICS_FILENAME)

    def fixture_path(self, name: str) -> Path:
        """Return a validated fixture target path."""
        return self.child(f"{FIXTURES_DIRNAME}/{name}")

    def child(self, relative: str) -> Path:
        """Validate one relative target and confine it beneath the root."""
        if not relative or os.path.isabs(relative) or PureWindowsPath(relative).is_absolute():
            raise ScenarioPathError("scenario write targets must be non-empty relative paths")
        if "\\" in relative:
            raise ScenarioPathError("scenario write targets must use forward slashes")
        current = self.path
        for part in relative.split("/"):
            _validate_component(part)
            current = current / part
        resolved = Path(os.path.normpath(str(current)))
        try:
            resolved.relative_to(self.path)
        except ValueError as error:
            raise ScenarioPathError("the write target escapes the isolated root") from error
        for existing in (resolved, *resolved.parents):
            if existing == self.path:
                break
            if existing.is_symlink() or existing.is_junction():
                raise ScenarioPathError("the write target traverses a symbolic link or junction")
        try:
            resolved.resolve(strict=False).relative_to(self.path)
        except ValueError as error:
            raise ScenarioPathError(
                "the write target resolves outside the isolated root"
            ) from error
        return resolved

    def _existing_dir(self, name: str) -> Path:
        path = self.child(name)
        if not path.is_dir():
            raise ScenarioPathError(f"the isolated root is missing its {name} directory")
        return path


def open_scenario_root(root: Path) -> ScenarioRoot:
    """Validate or create one isolated scenario root.

    Rejects the filesystem root, the user home and its broad standard folders,
    the current working directory and every ancestor of it (which covers the
    repository and workspace roots), symbolic-link components, reserved or
    malformed names, and any already-published manifest.
    """
    if not root.is_absolute():
        raise ScenarioPathError("the scenario root must be an absolute path")
    cleaned = Path(os.path.normpath(str(root)))
    parts = cleaned.parts
    if len(parts) <= 1:
        raise ScenarioPathError("the scenario root must not be a filesystem root")
    for part in parts:
        if part == cleaned.anchor:
            continue
        _validate_component(part)
    resolved = cleaned.resolve()
    if (
        resolved != cleaned
        and not _only_case_change(cleaned, resolved)
        and not _is_short_name_alias(cleaned, resolved)
    ):
        # resolve() rewrites the path when a symlink or junction is
        # involved, so textual divergence is an escape attempt — unless the
        # operating system confirms the candidate is just the 8.3
        # short-name rendering of the same existing directory.
        raise ScenarioPathError("the scenario root traverses a symbolic link or junction")
    for existing in (resolved, *resolved.parents):
        if existing == resolved:
            continue
        if existing.is_symlink():
            raise ScenarioPathError("the scenario root traverses a symbolic link")
    _reject_broad_roots(resolved)
    if resolved.exists():
        if not resolved.is_dir():
            raise ScenarioPathError("the scenario root could not be created as a directory")
        if any(resolved.iterdir()):
            raise ScenarioPathError("the scenario root must be empty before generation")
    else:
        resolved.mkdir(parents=True)
    scenario_root = ScenarioRoot(path=resolved)
    for directory in (FIXTURES_DIRNAME, ARTIFACTS_DIRNAME):
        target = resolved / directory
        target.mkdir()
    return scenario_root


class ScenarioClock:
    """A strictly increasing injected clock; canonical time is explicit."""

    __slots__ = ("_next",)

    def __init__(self, first: datetime) -> None:
        self._next = first

    @classmethod
    def create(cls) -> ScenarioClock:
        """Start at the fixed canonical epoch."""
        return cls(datetime(2026, 9, 1, tzinfo=UTC))

    def now(self) -> UtcTimestamp:
        """Return the current injected instant and advance one second."""
        current = self._next
        self._next = current + timedelta(seconds=1)
        return UtcTimestamp(current)

    def peek(self) -> UtcTimestamp:
        """Return the current injected instant without advancing."""
        return UtcTimestamp(self._next)

    def advance_to(self, moment: UtcTimestamp) -> None:
        """Advance to a later injected instant without moving backwards."""
        target = moment.to_datetime()
        if target > self._next:
            self._next = target


@dataclass(frozen=True, slots=True)
class AttemptEvidence:
    """One recorded source-read attempt awaiting its durable result.

    ``retry_after_seconds`` freezes the server-advised delay the failed
    attempt observed so the durable retry decision re-derives from exactly
    that evidence at the claim instant.
    """

    attempt: int
    succeeded: bool
    records: int
    bytes_processed: int
    classification: FailureClassification | None
    retry_after_seconds: int | None = None


@dataclass(frozen=True, slots=True)
class SourceRead:
    """One completed source read with its records and request evidence."""

    key: str
    connector_id: str
    records: tuple[SourceRecord, ...]
    byte_count: int
    attempts: tuple[AttemptEvidence, ...]
    simulator_requests: int


@dataclass(frozen=True, slots=True)
class CanonicalScenarioResult:
    """The verified outcome of one complete canonical scenario run."""

    manifest: CanonicalScenarioManifest
    manifest_bytes: bytes
    manifest_path: Path
    run_id: str
    execution_evidence_fingerprint: str
    observed_target_fingerprint: str
    reconciliation_fingerprint: str
    analysis: ReconciliationAnalysis
    total_target_requests: int
    repair_replay_disposition: str


async def run_canonical_scenario(
    profile: CanonicalScenarioProfile,
    root: Path,
) -> CanonicalScenarioResult:
    """Execute the complete canonical story for one profile in an isolated root."""
    evidence = derive_scenario(profile)
    scenario_root = open_scenario_root(root)
    clock = ScenarioClock.create()
    csv_manifest = write_csv_fixture(
        evidence.slice_for("csv").dataset,
        scenario_root.fixture_path(CSV_FIXTURE_NAME),
    )
    jsonl_manifest = write_jsonl_fixture(
        evidence.slice_for("jsonl").dataset,
        scenario_root.fixture_path(JSONL_FIXTURE_NAME),
    )
    if csv_manifest.byte_size != len(evidence.csv_fixture_bytes):
        raise ScenarioError("the written csv fixture diverges from the derivation")
    if jsonl_manifest.byte_size != len(evidence.jsonl_fixture_bytes):
        raise ScenarioError("the written jsonl fixture diverges from the derivation")

    database = SQLiteDatabase.open(SQLiteDatabaseConfig(scenario_root.database_path()))
    writer: SQLiteTransactionalWriter | None = None
    async_source = AsyncInventorySource(
        evidence.slice_for("async_http").dataset,
        evidence.source_failure_script,
        max_page_size=profile.async_page_size,
        request_latency_microseconds=profile.source_latency_microseconds,
    )
    blocking_source = BlockingInventorySource(
        evidence.slice_for("blocking_http").dataset,
        FailureScript.empty(),
        max_page_size=profile.blocking_page_size,
        request_latency_microseconds=profile.source_latency_microseconds,
    )
    warehouse = SimulatedWarehouse(evidence.warehouse_failure_script)
    try:
        with database.engine.connect() as connection:
            upgrade_to_head(connection)
        _publish_canonical_pipeline(database, scenario_root, clock, evidence.plan_fingerprint)
        writer = SQLiteTransactionalWriter(
            create_session_factory(database.engine),
            WriterSettings(contention_delay_seconds=0.0),
        )
        writer.start()
        run_id = RunId(CANONICAL_RUN_ID)
        _create_run(writer, database, profile, clock, run_id)

        await async_source.start()
        blocking_source.start()
        await warehouse.start()
        return await execute_canonical_story(
            scenario_root=scenario_root,
            database=database,
            writer=writer,
            clock=clock,
            run_id=run_id,
            evidence=evidence,
            profile=profile,
            async_source=async_source,
            blocking_source=blocking_source,
            warehouse=warehouse,
        )
    finally:
        # Every teardown step runs even when an earlier one fails; the first
        # failure is re-raised only after later resources were also closed,
        # so no simulator, writer thread, or database handle can leak.
        first_error: BaseException | None = None
        for close in (
            warehouse.aclose,
            async_source.aclose,
            blocking_source.aclose,
        ):
            try:
                await close()
            except BaseException as error:
                if first_error is None:
                    first_error = error
        if writer is not None:
            try:
                writer.close(timeout_seconds=10.0)
            except BaseException as error:
                if first_error is None:
                    first_error = error
        database.close()
        if first_error is not None:
            raise first_error


async def _replay_applied_repair_effects(
    aggregate: RepairPlanAggregate,
    target_connector: WarehouseTargetConnector,
) -> int:
    """Reconstruct the target's repaired state from durable plan facts.

    Recovery replays every applied action's exact request — same SKU, same
    rendered payload, same precondition, and crucially the same external
    idempotency key.  The demo's persisted target simulator returns its
    durable receipt for those keys, so restart cannot apply a second logical
    effect.  Ambiguous connection losses resolve by same-key replay just like
    the accepted applier.
    """
    replayed = 0
    context = ConnectorCallContext(correlation_id=CANONICAL_CORRELATION_ID)
    for action in aggregate.actions:
        request = _warm_start_request(action)
        ambiguous = 0
        while True:
            try:
                await target_connector.write_record_async(request, context)
            except ConnectorAmbiguousError:
                ambiguous += 1
                if ambiguous > 3:
                    raise
                continue
            replayed += 1
            break
    return replayed


def _warm_start_request(action: RepairActionRecord) -> TargetWriteRequest:
    proposed = action.effect.proposed
    expected_target = action.effect.expected_target
    return TargetWriteRequest(
        sku=proposed.sku,
        payload=render_effect_payload(proposed),
        idempotency_key=action.external_idempotency_key,
        precondition=(
            TargetWritePrecondition.must_be_absent()
            if expected_target is None
            else TargetWritePrecondition.expected_payload(render_effect_payload(expected_target))
        ),
    )


async def execute_canonical_story(
    *,
    scenario_root: ScenarioRoot,
    database: SQLiteDatabase,
    writer: SQLiteTransactionalWriter,
    clock: ScenarioClock,
    run_id: RunId,
    evidence: ScenarioExpectedEvidence,
    profile: CanonicalScenarioProfile,
    async_source: AsyncInventorySource,
    blocking_source: BlockingInventorySource,
    warehouse: SimulatedWarehouse,
    resume_enabled: bool = False,
    failpoint: Callable[[str], None] | None = None,
) -> CanonicalScenarioResult:
    """Run the durable canonical story from readiness probes to the manifest.

    The three simulators must already be started.  Every checkpoint boundary
    named in ``STORY_FAILPOINT_NAMES`` is invoked on ``failpoint`` only after
    its commits returned durably, so an interruption harness can stop exactly
    at a committed boundary without timing guesses.  ``resume_enabled`` makes
    each stage tolerate durable evidence an earlier process committed — it
    replays idempotent service calls, skips recorded attempts and registered
    artifacts, and replays the persisted target's idempotency receipts from
    the durable plan.  The single-shot canonical run keeps
    its strict first-pass assertions.
    """

    completed_before_start = scenario_root.manifest_path().is_file()

    def _failpoint(name: str) -> None:
        if failpoint is not None:
            failpoint(name)

    for service_name, base_url in (
        ("async-source", async_source.base_url),
        ("blocking-source", blocking_source.base_url),
        ("warehouse", warehouse.base_url),
    ):
        await probe_service_health(base_url, expected_service=service_name, timeout_seconds=10.0)

    reads = await _read_all_sources(
        evidence, scenario_root, clock, run_id, async_source, blocking_source
    )
    if resume_enabled and (
        _has_durably_applied_repair(database, run_id)
        or _has_persisted_initial_target(warehouse, evidence)
    ):
        # The external target survives a demo-child restart.  Reloading the
        # already-persisted divergent baseline, or a repaired target, would
        # move scripted fault sequencing onto idempotency replays and could
        # become an untruthful second mutation.  The immutable durable
        # reconciliation and repair records below still fence this locked
        # baseline analysis; the later parity verifier independently reads
        # the persisted target and refuses any divergence.
        target = _locked_target_observation(evidence)
    else:
        target = await _load_and_observe_target(warehouse, evidence)
    analysis = _analyze(evidence, reads, target)
    _verify_derived_counts(evidence, analysis, async_source, blocking_source, warehouse)
    _record_durable_attempts(
        database,
        writer,
        clock,
        run_id,
        reads,
        skip_attempts=_durable_attempt_keys(database, run_id),
    )
    _failpoint(STORY_FAILPOINT_ATTEMPTS_RECORDED)

    analytics: DuckDBLifecycleCoordinator | None = None
    try:
        analytics = DuckDBLifecycleCoordinator(
            AnalyticalDatabaseConfig(scenario_root.analytics_path().resolve())
        )
        analytics.open()
        finalizer = RunFinalizer(
            writer,
            SQLiteFinalizationStateReader(database),
            DuckDBRunStatisticsQueryEngine(analytics),
            clock,
            settings=FinalizationSettings(5.0, 5.0),
        )
        report = finalizer.finalize(
            run_id,
            plan_nodes=tuple(NodeId(node) for node in scenario.CANONICAL_NODES),
            plan_fingerprint=PlanFingerprint(scenario.canonical_plan_fingerprint()),
        )
        if report.fingerprint is None:
            raise ScenarioError("finalization produced no execution-evidence fingerprint")
        execution_fingerprint = report.fingerprint.value
    finally:
        if analytics is not None:
            analytics.close()

    _publish_conflict_artifact(database, scenario_root, clock, run_id, analysis, skip_existing=True)
    _verify_artifact_accounting(database, scenario_root, run_id)

    reader = SQLiteRepairWorkflowReader(database)
    persisted = ReconciliationResultService(writer, reader, now=clock.now).persist(
        run_id=run_id,
        analysis=analysis,
        actor="canonical-operator",
        correlation_id=CANONICAL_CORRELATION_ID,
    )
    if persisted.replayed and not resume_enabled:
        raise ScenarioError("the canonical reconciliation must persist exactly once")
    created = RepairPlanningService(writer, reader, now=clock.now).create(
        run_id=run_id,
        analysis=analysis,
        actor="canonical-operator",
        correlation_id=CANONICAL_CORRELATION_ID,
    )
    generated_plan = created.generated.plan
    if generated_plan is None:
        raise ScenarioError("the canonical scenario must produce a repair plan")
    durable_plan = created.aggregate.plan if created.aggregate is not None else None
    if durable_plan is None:
        raise ScenarioError("the canonical scenario must durably persist the repair plan")
    for action in generated_plan.actions:
        if action.kind.value not in ("create_target", "update_target"):
            raise ScenarioError("the plan carries a non-representable action kind")
    _failpoint(STORY_FAILPOINT_RECONCILIATION_PERSISTED)

    target_connector = WarehouseTargetConnector(WarehouseTargetConfig(warehouse.base_url))
    await target_connector.open_async()
    try:
        applier = RepairApplicationService(
            writer,
            reader,
            now=clock.now,
            policy=RepairApplicationPolicy(delay_seconds=0.0, timeout_seconds=30.0),
        )
        already_approved = created.aggregate is not None and created.aggregate.approval is not None
        if not (resume_enabled and already_approved):
            # The story proves the approval gate on every first pass: an
            # unapproved plan can never reach the target.
            try:
                await applier.apply(
                    run_id=run_id,
                    repair_plan_id=durable_plan.repair_plan_id,
                    target=target_connector,
                    context_id=CANONICAL_CORRELATION_ID,
                )
            except RepairPlanStateError:
                pass
            else:
                raise ScenarioError("application succeeded before the explicit approval gate")
        RepairApprovalService(writer, reader, now=clock.now).approve(
            RepairApprovalRequest(
                run_id=run_id,
                repair_plan_id=durable_plan.repair_plan_id,
                approved_by="canonical-approver",
                correlation_id=CANONICAL_CORRELATION_ID,
                approved_content_fingerprint=durable_plan.content_fingerprint,
                approved_reconciliation_fingerprint=analysis.summary.fingerprint,
                detail=RedactedDocument.from_mapping({"decision": "canonical-demo"}),
            )
        )
        _failpoint(STORY_FAILPOINT_REPAIR_APPROVED)
        application = await applier.apply(
            run_id=run_id,
            repair_plan_id=durable_plan.repair_plan_id,
            target=target_connector,
            context_id=CANONICAL_CORRELATION_ID,
        )
        if application.disposition.value == "already_applied":
            if not resume_enabled:
                raise ScenarioError("an unexpected already-applied disposition")
            # A previous process committed the repair effects durably.  The
            # persisted simulator holds those same-key receipts, so replay
            # cannot apply a second logical effect.
            if created.aggregate is None:
                raise ScenarioError("an applied plan must carry its durable aggregate")
            if not completed_before_start:
                # A process interrupted after the external commit must prove
                # the persisted same-key receipts across restart. A root that
                # already carried a verified final manifest has completed that
                # proof; reissuing target writes on every ordinary rerun would
                # mutate request diagnostics and break byte-stable replay.
                await _replay_applied_repair_effects(created.aggregate, target_connector)
        elif application.disposition.value != "completed":
            raise ScenarioError("the canonical repair application must complete")
        _failpoint(STORY_FAILPOINT_REPAIR_APPLIED)
        inventory = build_expected_inventory(analysis, generated_plan)
        verifier = TargetParityVerifier(now=clock.now)
        verification = await verifier.verify(
            target=target_connector,
            inventory=inventory,
            context_id=CANONICAL_CORRELATION_ID,
        )
        if verification.verdict is not TargetVerificationVerdict.PARITY_HOLDING:
            raise ScenarioError("independent target observation did not reach parity")
        if verification.observed is None:
            raise ScenarioError("parity verification produced no observed identity")
        observed_fingerprint = verification.observed.fingerprint.value
        if observed_fingerprint != evidence.expected_target_fingerprint:
            raise ScenarioError("the observed target state diverges from the locked value")
        await TargetVerificationService(writer, reader, now=clock.now).verify_and_record(
            run_id=run_id,
            target=target_connector,
            inventory=inventory,
            reconciliation_fingerprint=analysis.summary.fingerprint,
            repair_plan_id=durable_plan.repair_plan_id,
            plan_content_fingerprint=durable_plan.content_fingerprint,
            actor="canonical-operator",
            correlation_id=CANONICAL_CORRELATION_ID,
        )
        requests_before_replay = warehouse.request_count()
        replay = await RepairApplicationService(
            writer,
            reader,
            now=clock.now,
            policy=RepairApplicationPolicy(delay_seconds=0.0, timeout_seconds=30.0),
        ).apply(
            run_id=run_id,
            repair_plan_id=durable_plan.repair_plan_id,
            target=target_connector,
            context_id=f"{CANONICAL_CORRELATION_ID}-replay",
        )
        if replay.disposition.value != "already_applied":
            raise ScenarioError("repair replay must be an idempotent no-op")
        if warehouse.request_count() != requests_before_replay:
            raise ScenarioError("repair replay must not touch the target")
        reverify = await verifier.verify(
            target=target_connector,
            inventory=inventory,
            context_id=f"{CANONICAL_CORRELATION_ID}-reverify",
        )
        if reverify.observed is None or (
            reverify.observed.fingerprint.value != observed_fingerprint
        ):
            raise ScenarioError("re-verification diverged from the first observation")
    finally:
        await target_connector.aclose()

    expected_faults = (
        AppliedFailure(
            sequence=len(evidence.target.payloads) + profile.warehouse_fault_action,
            kind=ScriptedFailureKind.CONNECTION_LOSS,
        ),
    )
    if warehouse.applied_failures() != expected_faults:
        raise ScenarioError("the warehouse must apply exactly the locked transient connection loss")
    manifest = build_manifest(
        evidence,
        execution_evidence_fingerprint=execution_fingerprint,
        verification_result="parity_holding",
    )
    manifest_bytes = manifest.canonical_bytes()
    _publish_manifest(scenario_root, manifest_bytes)
    return CanonicalScenarioResult(
        manifest=manifest,
        manifest_bytes=manifest_bytes,
        manifest_path=scenario_root.manifest_path(),
        run_id=run_id.value,
        execution_evidence_fingerprint=execution_fingerprint,
        observed_target_fingerprint=observed_fingerprint,
        reconciliation_fingerprint=analysis.summary.fingerprint.value,
        analysis=analysis,
        total_target_requests=warehouse.request_count(),
        repair_replay_disposition=replay.disposition.value,
    )


def _publish_manifest(scenario_root: ScenarioRoot, manifest_bytes: bytes) -> None:
    """Publish the final manifest atomically after every fact is verified."""
    target = scenario_root.manifest_path()
    partial = scenario_root.child(f"{MANIFEST_FILENAME}.partial")
    with open(partial, "wb") as handle:
        handle.write(manifest_bytes)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(partial, target)


def _publish_canonical_pipeline(
    database: SQLiteDatabase,
    scenario_root: ScenarioRoot,
    clock: ScenarioClock,
    expected_plan_fingerprint: str,
) -> None:
    """Register connectors and publish the scenario through the public services."""
    unit_of_work = SQLOperationalUnitOfWork(
        database,
        artifact_root=scenario_root.artifacts,
    )
    connectors = ConnectorService(unit_of_work=unit_of_work, now=clock.now)
    for definition in scenario.canonical_connector_definitions():
        connectors.register(
            connector_id=str(definition.connector_id),
            kind=definition.kind,
            display_name=definition.display_name,
            configuration=definition.configuration.to_mapping(),
            capabilities=definition.capabilities.to_mapping(),
            schema_discovery=None,
            secret_references=(),
        )
    pipelines = PipelineService(unit_of_work=unit_of_work, now=clock.now)
    pipelines.create(
        pipeline_id=CANONICAL_PIPELINE_ID,
        display_name="Canonical demonstration pipeline",
        description=None,
    )
    published = pipelines.publish(
        pipeline_id=CANONICAL_PIPELINE_ID,
        document=scenario.canonical_pipeline_document().to_mapping(),
        expected_latest_version=None,
    )
    if published.version.number != scenario.CANONICAL_PIPELINE_VERSION:
        raise ScenarioError("the public boundary published an unexpected pipeline version")
    specification = PublishedPipelineSpecification.from_configuration_document(
        published.specification
    )
    actual_plan_fingerprint = fingerprint_execution_plan(
        compile_execution_plan(specification)
    ).value
    if actual_plan_fingerprint != expected_plan_fingerprint:
        raise ScenarioError("the published pipeline diverges from the locked plan fingerprint")


def _submit(
    writer: SQLiteTransactionalWriter,
    command: CreateCapturedRun | TransitionRun | BootstrapWork,
) -> None:
    receipt = writer.submit(command, timeout_seconds=5.0)
    receipt.result(timeout_seconds=30.0)


def _create_run(
    writer: SQLiteTransactionalWriter,
    database: SQLiteDatabase,
    profile: CanonicalScenarioProfile,
    clock: ScenarioClock,
    run_id: RunId,
) -> None:
    """Create the captured run, start it, and bootstrap the source work items."""
    created_at = clock.now()
    _submit(
        writer,
        CreateCapturedRun(
            run_id=run_id,
            pipeline_id=PipelineId(CANONICAL_PIPELINE_ID),
            pipeline_version=PipelineVersion(CANONICAL_PIPELINE_VERSION),
            runner_kind=_RUNNER_KIND,
            runner_configuration=ConfigurationDocument.from_mapping(
                {"profile": profile.profile_id, "scenario": SCENARIO_FORMAT_NAME}
            ),
            scenario_seed=CANONICAL_SCENARIO_SEED,
            node_ids=tuple(NodeId(node) for node in scenario.CANONICAL_NODES),
            created_at=created_at,
            event=_event(database, clock, run_id, "run_created", run_id, at=created_at),
        ),
    )
    started_at = clock.now()
    _submit(
        writer,
        TransitionRun(
            run_id=run_id,
            expected_run_row_version=1,
            target_state=RunState.RUNNING,
            transitioned_at=started_at,
            execution_evidence_fingerprint=None,
            execution_evidence_fingerprint_version=None,
            event=_event(database, clock, run_id, "run_started", run_id, at=started_at),
        ),
    )
    for node in _SOURCE_NODE_FOR_KEY.values():
        node_id = NodeId(node)
        work_item_id = _work_item_id(node)
        bootstrapped_at = clock.now()
        _submit(
            writer,
            BootstrapWork(
                run_id=run_id,
                node_id=node_id,
                work_item_id=work_item_id,
                partition_key=PartitionKey("p0"),
                input_reference=None,
                created_at=bootstrapped_at,
                expected_node_row_version=_node_row_version(database, run_id, node_id),
                expected_run_row_version=_run_row_version(database, run_id),
                event=_event(
                    database, clock, run_id, "work_created", work_item_id, at=bootstrapped_at
                ),
            ),
        )


def _work_item_id(node_name: str) -> WorkItemId:
    return WorkItemId(f"wrk_can-{node_name.removeprefix('nod_can-')}")


def _event(
    database: SQLiteDatabase,
    clock: ScenarioClock,
    run_id: RunId,
    kind: str,
    subject: RunId | WorkItemId,
    at: UtcTimestamp | None = None,
) -> EventAppendRequest:
    sequence, counter_row_version = _event_frontier(database, run_id)
    subject_kind = EventSubjectKind.RUN if type(subject) is RunId else EventSubjectKind.WORK_ITEM
    return EventAppendRequest(
        EventSequence(sequence),
        counter_row_version,
        PendingExecutionEvent(
            kind,
            clock.peek() if at is None else at,
            subject_kind,
            subject,
            CANONICAL_CORRELATION_ID,
            1,
            RedactedDocument.from_mapping({"kind": kind}),
        ),
    )


def _event_frontier(database: SQLiteDatabase, run_id: RunId) -> tuple[int, int]:
    with database.transaction() as session:
        row = session.execute(
            select(
                run_event_counters.c.next_sequence_number,
                run_event_counters.c.row_version,
            ).where(run_event_counters.c.run_id == run_id.value)
        ).one_or_none()
    if row is None:
        return (1, 1)
    return (int(row[0]), int(row[1]))


def _run_row_version(database: SQLiteDatabase, run_id: RunId) -> int:
    with database.transaction() as session:
        run = SqlAlchemyRunRepository(session).get(run_id)
        if run is None:
            raise ScenarioError("the canonical run disappeared from durable state")
        return run.row_version


def _node_row_version(database: SQLiteDatabase, run_id: RunId, node_id: NodeId) -> int:
    with database.transaction() as session:
        node = SqlAlchemyRunRepository(session).get_node(run_id, node_id)
        if node is None:
            raise ScenarioError("the canonical node disappeared from durable state")
        return node.row_version


def _work_row_version(database: SQLiteDatabase, work_item_id: WorkItemId) -> int:
    with database.transaction() as session:
        item = SqlAlchemyWorkItemRepository(session).get(work_item_id)
        if item is None:
            raise ScenarioError("the canonical work item disappeared from durable state")
        return item.row_version


async def _read_all_sources(
    evidence: ScenarioExpectedEvidence,
    scenario_root: ScenarioRoot,
    clock: ScenarioClock,
    run_id: RunId,
    async_source: AsyncInventorySource,
    blocking_source: BlockingInventorySource,
) -> dict[str, SourceRead]:
    """Read every source kind through its accepted connector adapter."""

    async def read_async_http() -> SourceRead:
        connector = AsyncHttpSourceConnector(AsyncHttpSourceConfig(base_url=async_source.base_url))
        await connector.open_async()
        try:
            return await _read_async_source(evidence.slice_for("async_http"), connector, clock)
        finally:
            await connector.aclose()

    async def read_blocking_http() -> SourceRead:
        # The blocking connector is opened, read, and closed entirely on a
        # worker thread: it refuses an active event loop by contract.
        connector = BlockingHttpSourceConnector(
            BlockingHttpSourceConfig(base_url=blocking_source.base_url)
        )
        records, byte_count, requests = await asyncio.to_thread(_read_blocking_source, connector)
        return _successful_read("blocking_http", evidence, records, byte_count, requests)

    async def read_file(key: str, name: str) -> SourceRead:
        # File connectors refuse an active event loop, so the open, page
        # reads, and close all run on a worker thread like the blocking HTTP
        # source does.
        file_connector: CsvFileSourceConnector | JsonlFileSourceConnector
        if key == "csv":
            file_connector = CsvFileSourceConnector(
                CsvFileSourceConfig(SourceFileLocation.create(scenario_root.fixtures, name))
            )
        else:
            file_connector = JsonlFileSourceConnector(
                JsonlFileSourceConfig(SourceFileLocation.create(scenario_root.fixtures, name))
            )
        records, byte_count, requests = await asyncio.to_thread(_read_bounded, file_connector)
        return _successful_read(key, evidence, records, byte_count, requests)

    completed = await asyncio.gather(
        read_async_http(),
        read_blocking_http(),
        read_file("csv", CSV_FIXTURE_NAME),
        read_file("jsonl", JSONL_FIXTURE_NAME),
    )
    return dict(zip(scenario.SOURCE_KEYS, completed, strict=True))


def _read_bounded(
    connector: CsvFileSourceConnector | JsonlFileSourceConnector,
) -> tuple[list[SourceRecord], int, int]:
    connector.open()
    try:
        return _read_pages(connector)
    finally:
        connector.close()


def _successful_read(
    key: str,
    evidence: ScenarioExpectedEvidence,
    records: list[SourceRecord],
    byte_count: int,
    requests: int,
) -> SourceRead:
    return SourceRead(
        key=key,
        connector_id=str(evidence.slice_for(key).connector),
        records=tuple(records),
        byte_count=byte_count,
        attempts=(
            AttemptEvidence(
                attempt=1,
                succeeded=True,
                records=len(records),
                bytes_processed=byte_count,
                classification=None,
            ),
        ),
        simulator_requests=requests,
    )


def _read_blocking_source(
    connector: BlockingHttpSourceConnector,
) -> tuple[list[SourceRecord], int, int]:
    """Open, page through, and close the blocking source on one thread."""
    connector.open()
    try:
        return _read_pages(connector)
    finally:
        connector.close()


def _decode_csv_dialect(payload: dict[str, object]) -> dict[str, object]:
    """Decode one canonical-CSV-dialect record into the canonical payload."""
    currency = payload.get("currency")
    amount = payload.get("amount")
    canonical: dict[str, object] = {
        "attributes": {},
        "name": payload["name"],
        "quantity": int(cast("str", payload["quantity"])),
        "sku": payload["sku"],
        "source_record_key": payload["source_record_key"],
        "updated_at": payload["updated_at"],
    }
    if isinstance(currency, str) and currency and isinstance(amount, str) and amount:
        canonical["unit_price"] = {"amount": amount, "currency": currency}
    return canonical


def _read_pages(
    connector: BlockingHttpSourceConnector | CsvFileSourceConnector | JsonlFileSourceConnector,
) -> tuple[list[SourceRecord], int, int]:
    """Read every remaining page of one connector until exhaustion."""
    records: list[SourceRecord] = []
    byte_count = 0
    requests = 0
    cursor: str | None = None
    context = ConnectorCallContext(correlation_id=CANONICAL_CORRELATION_ID)
    while True:
        page = connector.read_page(cursor, context)
        requests += page.request_count
        byte_count += page.byte_count
        records.extend(page.records)
        if page.next_cursor is None:
            return records, byte_count, requests
        cursor = page.next_cursor


async def _read_async_source(
    slice_value: SourceSlice,
    connector: AsyncHttpSourceConnector,
    clock: ScenarioClock,
) -> SourceRead:
    """Read the asynchronous slice with exactly one deterministic retry.

    The scripted rate-limit fault is classified HTTP 429 by the connector,
    decided by the accepted named retry policy, and retried once as the second
    attempt of the same work-item identity.
    """
    policy = BoundedExponentialRetryPolicy(clock, SeededRetryJitterSource(CANONICAL_SCENARIO_SEED))
    work_item_id = _work_item_id(scenario.NODE_ASYNC_SOURCE)
    attempts: list[AttemptEvidence] = []
    context = ConnectorCallContext(correlation_id=CANONICAL_CORRELATION_ID)
    attempt = 1
    while True:
        records: list[SourceRecord] = []
        byte_count = 0
        requests = 0
        cursor: str | None = None
        attempt_started_at = clock.now()
        try:
            while True:
                page = await connector.read_page_async(cursor, context)
                requests += page.request_count
                byte_count += page.byte_count
                records.extend(page.records)
                if page.next_cursor is None:
                    break
                cursor = page.next_cursor
        except ConnectorRateLimitedError as error:
            failed_at = clock.now()
            decision = policy.decide(
                RetryPolicyRequest(
                    work_item_id=work_item_id,
                    attempt_number=AttemptNumber(attempt),
                    classification=FailureClassification.HTTP_429,
                    failed_at=failed_at,
                    http_429_delay=Http429RetryDelay(
                        Duration(int(error.retry_after_seconds or 1) * 1_000_000)
                    ),
                )
            )
            if not isinstance(decision, RetryScheduledDecision):
                raise
            attempts.append(
                AttemptEvidence(
                    attempt=attempt,
                    succeeded=False,
                    records=len(records),
                    bytes_processed=byte_count,
                    classification=FailureClassification.HTTP_429,
                    retry_after_seconds=error.retry_after_seconds,
                )
            )
            clock.advance_to(decision.retry_available_at)
            attempt += 1
            continue
        del attempt_started_at
        attempts.append(
            AttemptEvidence(
                attempt=attempt,
                succeeded=True,
                records=len(records),
                bytes_processed=byte_count,
                classification=None,
            )
        )
        return SourceRead(
            key=slice_value.key,
            connector_id=str(slice_value.connector),
            records=tuple(records),
            byte_count=byte_count,
            attempts=tuple(attempts),
            simulator_requests=requests,
        )


async def _load_and_observe_target(
    warehouse: SimulatedWarehouse,
    evidence: ScenarioExpectedEvidence,
) -> tuple[list[SourceRecord], int]:
    """Load the divergent inventory and observe every record back by read."""
    connector = WarehouseTargetConnector(WarehouseTargetConfig(warehouse.base_url))
    await connector.open_async()
    try:
        context = ConnectorCallContext(correlation_id=CANONICAL_CORRELATION_ID)
        for index, payload in enumerate(evidence.target.payloads):
            sku = cast("str", payload["sku"])
            await connector.write_record_async(
                TargetWriteRequest(
                    sku=sku,
                    payload=payload,
                    idempotency_key=f"canonical-load-{index:06d}",
                ),
                context,
            )
        records: list[SourceRecord] = []
        for index, payload in enumerate(evidence.target.payloads):
            sku = cast("str", payload["sku"])
            record = await connector.read_record_async(sku, context)
            if record is None:
                raise ScenarioError(f"the loaded target record vanished: {sku}")
            records.append(
                SourceRecord(
                    position=_TARGET_POSITION_BASE + index,
                    outcome=SourceOutcome.VALID,
                    payload=dict(record.payload),
                )
            )
        return records, len(evidence.target.payloads)
    finally:
        await connector.aclose()


def _locked_target_observation(
    evidence: ScenarioExpectedEvidence,
) -> tuple[list[SourceRecord], int]:
    """Rebuild the immutable pre-repair target projection during recovery.

    This is available only after a durable applied repair plan exists.  It is
    not a substitute for target verification: the resumed workflow still
    observes the preserved target independently before it records success.
    """
    records = [
        SourceRecord(
            position=_TARGET_POSITION_BASE + index,
            outcome=SourceOutcome.VALID,
            payload=dict(payload),
        )
        for index, payload in enumerate(evidence.target.payloads)
    ]
    return records, len(records)


def _has_durably_applied_repair(database: SQLiteDatabase, run_id: RunId) -> bool:
    """Return whether recovery must preserve an already-mutated target."""
    from paritygrid.adapters.persistence.schema import repair_plans

    with database.transaction() as session:
        rows = session.execute(
            select(repair_plans.c.status).where(repair_plans.c.run_id == run_id.value)
        ).all()
    if len(rows) > 1:
        raise ScenarioError("the canonical run carries more than one repair plan")
    return bool(rows) and str(rows[0].status) == "applied"


def _has_persisted_initial_target(
    warehouse: SimulatedWarehouse,
    evidence: ScenarioExpectedEvidence,
) -> bool:
    """Recognize the complete immutable baseline loaded by an earlier child.

    Reissuing the baseline load after a crash would move the deterministic
    warehouse fault's request sequence onto an idempotency replay.  Require
    the whole expected inventory, version, and every stable load receipt
    before using the locked projection; a partial or divergent target still
    follows the ordinary HTTP load path and fails closed if it cannot recover.
    """
    snapshot = warehouse.behavior.state_snapshot()
    expected_records = {
        cast("str", payload["sku"]): {"payload": payload, "record_version": 1}
        for payload in evidence.target.payloads
    }
    expected_keys = tuple(
        f"canonical-load-{index:06d}" for index, _payload in enumerate(evidence.target.payloads)
    )
    return snapshot["records"] == expected_records and warehouse.behavior.has_idempotency_keys(
        expected_keys
    )


def _analyze(
    evidence: ScenarioExpectedEvidence,
    reads: dict[str, SourceRead],
    target: tuple[list[SourceRecord], int],
) -> ReconciliationAnalysis:
    """Analyze the actual source and independently read-back target records."""
    read_back, _write_count = target
    if len(read_back) != len(evidence.target.payloads):
        raise ScenarioError("the observed target record count diverges from the derivation")
    for index, (record, expected) in enumerate(
        zip(read_back, evidence.target.payloads, strict=True)
    ):
        if record.payload != expected:
            raise ScenarioError(f"the observed target record {index} diverges from the derivation")
    source_observations: list[SourceObservation] = []
    for key in scenario.SOURCE_KEYS:
        read = reads[key]
        base = _POSITION_BASES[key]
        for record in read.records:
            position = base + record.position
            if record.outcome is SourceOutcome.MALFORMED:
                source_observations.append(
                    SourceObservation(
                        position=position,
                        connector_id=ConnectorId(read.connector_id),
                        payload=None,
                        malformed_reason=(
                            record.malformed_reason or "the connector rejected the record"
                        ),
                    )
                )
            else:
                payload = cast("dict[str, object] | None", record.payload)
                if key == "csv" and payload is not None:
                    # The canonical CSV dialect: the accepted connector
                    # contract emits every column as text with one column per
                    # scalar, so the scenario decodes its dialect back into
                    # the canonical payload shape (nested unit price, integer
                    # quantity, no attributes column).
                    payload = _decode_csv_dialect(payload)
                source_observations.append(
                    SourceObservation(
                        position=position,
                        connector_id=ConnectorId(read.connector_id),
                        payload=payload,
                    )
                )
    return analyze_reconciliation(
        ReconciliationAnalysisRequest(
            source_observations=tuple(source_observations),
            target_observations=tuple(
                SourceObservation(
                    position=record.position,
                    connector_id=scenario.WAREHOUSE_CONNECTOR,
                    payload=record.payload,
                )
                for record in read_back
            ),
            source_input_identity=evidence.source_input_identity,
            target_input_identity=evidence.target_input_identity,
        )
    )


def _verify_derived_counts(
    evidence: ScenarioExpectedEvidence,
    analysis: ReconciliationAnalysis,
    async_source: AsyncInventorySource,
    blocking_source: BlockingInventorySource,
    warehouse: SimulatedWarehouse,
) -> None:
    """Prove every locked count against the executed story."""
    counts = evidence.counts
    if async_source.request_count() != counts.async_http_requests:
        raise ScenarioError("the asynchronous source request count diverged from the lock")
    if blocking_source.request_count() != counts.blocking_http_requests:
        raise ScenarioError("the blocking source request count diverged from the lock")
    if async_source.applied_failures() != (
        AppliedFailure(
            sequence=scenario.ASYNC_RATE_LIMIT_REQUEST, kind=ScriptedFailureKind.RATE_LIMIT
        ),
    ):
        raise ScenarioError("the async source must apply exactly the locked rate-limit fault")
    classified = dict(analysis.summary.counts.by_classification)
    locked = (
        (ReconciliationClassification.MATCH, counts.match),
        (ReconciliationClassification.MISSING_FROM_TARGET, counts.missing_from_target),
        (ReconciliationClassification.MISSING_FROM_SOURCE, counts.missing_from_source),
        (ReconciliationClassification.FIELD_MISMATCH, counts.field_mismatch),
        (ReconciliationClassification.DUPLICATE_SOURCE, counts.duplicate_source),
        (ReconciliationClassification.DUPLICATE_TARGET, counts.duplicate_target),
        (ReconciliationClassification.DUPLICATE_BOTH, counts.duplicate_both),
    )
    for classification, expected in locked:
        if classified[classification] != expected:
            raise ScenarioError(
                f"classification {classification.value} diverged from the locked count"
            )
    if len(analysis.source_quarantined) != counts.quarantined_rows:
        raise ScenarioError("quarantined rows diverged from the locked count")
    if analysis.summary.fingerprint.value != evidence.reconciliation_fingerprint:
        raise ScenarioError("the reconciliation fingerprint diverged from the locked value")


def _durable_attempt_keys(database: SQLiteDatabase, run_id: RunId) -> frozenset[tuple[str, int]]:
    """Return the (node, attempt) pairs already durably recorded for one run."""
    with database.transaction() as session:
        rows = session.execute(
            select(work_items.c.node_id, work_attempts.c.attempt_number)
            .join(work_items, work_attempts.c.work_item_id == work_items.c.work_item_id)
            .where(work_items.c.run_id == run_id.value)
        ).all()
    return frozenset((str(row.node_id), int(row.attempt_number)) for row in rows)


def _record_durable_attempts(
    database: SQLiteDatabase,
    writer: SQLiteTransactionalWriter,
    clock: ScenarioClock,
    run_id: RunId,
    reads: dict[str, SourceRead],
    *,
    skip_attempts: frozenset[tuple[str, int]] = frozenset(),
) -> None:
    """Record the real attempts as durable work-item attempts.

    The representative execution-plan source becomes immutable durable attempt
    evidence, so the locked single retry is visible in SQLite history and
    covered by the finalization fingerprint.  The other three concurrent
    acquisitions are scenario inputs locked by the input manifest; the
    accepted planner has no version-1 multi-input aggregation contract and we
    do not fabricate one here.  Attempts whose (node, attempt) identity is
    already durable are skipped so a resumed story never duplicates them.
    """
    service = WorkLeaseService(
        writer,
        clock,
        settings=WorkLeaseSettings(
            lease_duration=_DEFAULT_LEASE_DURATION,
            admission_timeout_seconds=5.0,
            result_timeout_seconds=30.0,
        ),
    )
    sink = TransactionalCheckpointResultSink(writer)
    retry_decision: RetryScheduledDecision | None = None
    for key in _SOURCE_NODE_FOR_KEY:
        read = reads[key]
        node_name = _SOURCE_NODE_FOR_KEY[key]
        node_id = NodeId(node_name)
        work_item_id = _work_item_id(node_name)
        accepted = sum(1 for record in read.records if record.outcome is SourceOutcome.VALID)
        malformed = len(read.records) - accepted
        for attempt_evidence in read.attempts:
            if (node_name, attempt_evidence.attempt) in skip_attempts:
                continue
            lease = service.acquire(
                AcquireWorkLeaseRequest(
                    run_id=run_id,
                    node_id=node_id,
                    work_item_id=work_item_id,
                    expected_attempt_number=AttemptNumber(attempt_evidence.attempt),
                    expected_work_row_version=_work_row_version(database, work_item_id),
                    expected_node_row_version=_node_row_version(database, run_id, node_id),
                    expected_run_row_version=_run_row_version(database, run_id),
                    lease_owner=_LEASE_OWNER,
                    runner_kind=_RUNNER_KIND,
                    worker_identity=_WORKER_IDENTITY,
                    event=_event(database, clock, run_id, "work_claimed", work_item_id),
                )
            )
            claimed_at = lease.claim.started_at
            context = AttemptEventContext(
                run_id,
                node_id,
                work_item_id,
                AttemptNumber(attempt_evidence.attempt),
                claimed_at,
                PlannerRunnerKind.SEQUENTIAL,
                _WORKER_IDENTITY,
                CANONICAL_CORRELATION_ID,
            )
            if attempt_evidence.succeeded:
                submission = ResultSubmission(
                    lease,
                    SuccessfulWorkResult(
                        AttemptSucceeded(context, claimed_at),
                        ResultCheckpoint(PartitionKey("p0"), 1, None, None, None),
                        ResultMetrics(
                            attempt_evidence.records,
                            attempt_evidence.bytes_processed,
                            WorkMetricDelta(
                                records_read=attempt_evidence.records,
                                records_written=accepted,
                                records_quarantined=malformed,
                                bytes_read=attempt_evidence.bytes_processed,
                                bytes_written=0,
                            ),
                        ),
                    ),
                )
            else:
                if (
                    attempt_evidence.classification is None
                    or attempt_evidence.retry_after_seconds is None
                ):
                    raise ScenarioError("a failed attempt must carry its classification")
                decision = BoundedExponentialRetryPolicy(
                    clock, SeededRetryJitterSource(CANONICAL_SCENARIO_SEED)
                ).decide(
                    RetryPolicyRequest(
                        work_item_id=work_item_id,
                        attempt_number=AttemptNumber(attempt_evidence.attempt),
                        classification=attempt_evidence.classification,
                        failed_at=claimed_at,
                        http_429_delay=Http429RetryDelay(
                            Duration(attempt_evidence.retry_after_seconds * 1_000_000)
                        ),
                    )
                )
                if not isinstance(decision, RetryScheduledDecision):
                    raise ScenarioError("the locked fault must schedule exactly one retry")
                retry_decision = decision
                submission = ResultSubmission(
                    lease,
                    UnsuccessfulWorkResult(
                        AttemptFailed(context, claimed_at, attempt_evidence.classification),
                        decision,
                        ResultMetrics(
                            attempt_evidence.records,
                            attempt_evidence.bytes_processed,
                            WorkMetricDelta(
                                records_read=attempt_evidence.records,
                                bytes_read=attempt_evidence.bytes_processed,
                            ),
                        ),
                    ),
                )
            submit_work_result(sink, submission, lease_service=service)
            if retry_decision is not None:
                # The durable frontier only re-admits the retry once the
                # named-policy eligibility instant has arrived.
                clock.advance_to(retry_decision.retry_available_at)
                retry_decision = None


def _verify_artifact_accounting(
    database: SQLiteDatabase,
    scenario_root: ScenarioRoot,
    run_id: RunId,
) -> None:
    """Prove the locked artifact count against the durable registry.

    The locked three artifacts are the two generated fixture files and the
    one conflict Parquet artifact registered durably for the run.
    """
    from sqlalchemy import select

    from paritygrid.adapters.persistence.schema import artifact_manifests

    with database.transaction() as session:
        registered = tuple(
            session.execute(
                select(artifact_manifests.c.artifact_id)
                .where(artifact_manifests.c.run_id == run_id.value)
                .order_by(artifact_manifests.c.artifact_id)
            )
            .scalars()
            .all()
        )
    fixtures = sorted(path.name for path in scenario_root.fixtures.iterdir())
    if registered != (scenario.CONFLICT_ARTIFACT_ID,):
        raise ScenarioError("the run must register the exact canonical conflict artifact")
    if fixtures != sorted((CSV_FIXTURE_NAME, JSONL_FIXTURE_NAME)):
        raise ScenarioError("the fixture directory must hold exactly the two canonical fixtures")
    if 1 + len(fixtures) != scenario.ARTIFACT_COUNT:
        raise ScenarioError("the durable artifacts must match the locked artifact count")


def _conflict_artifact_registered(database: SQLiteDatabase, run_id: RunId) -> bool:
    """Report whether the run's canonical conflict artifact is already durable."""
    with database.transaction() as session:
        registered = (
            session.execute(
                select(artifact_manifests.c.artifact_id).where(
                    artifact_manifests.c.run_id == run_id.value,
                    artifact_manifests.c.artifact_id == scenario.CONFLICT_ARTIFACT_ID,
                )
            )
            .scalars()
            .first()
        )
    return registered is not None


def _publish_conflict_artifact(
    database: SQLiteDatabase,
    scenario_root: ScenarioRoot,
    clock: ScenarioClock,
    run_id: RunId,
    analysis: ReconciliationAnalysis,
    *,
    skip_existing: bool = False,
) -> None:
    """Publish the conflict Parquet artifact through the accepted protocol.

    A resumed story finds the artifact already durably registered by the
    interrupted process and keeps the committed original; a single-shot run
    always publishes exactly once.
    """
    if skip_existing and _conflict_artifact_registered(database, run_id):
        return
    artifact_writer = FileSystemArtifactWriter(
        scenario_root.artifacts,
        maximum_bytes=_ARTIFACT_BYTE_LIMIT,
    )
    parquet_writer = AtomicParquetPartitionWriter(artifact_writer)
    with database.transaction() as session:
        publish_conflict_artifact(
            writer=parquet_writer,
            manifests=FileSystemArtifactManifestRepository(session, scenario_root.artifacts),
            artifact_id=ArtifactId(scenario.CONFLICT_ARTIFACT_ID),
            run_id=run_id,
            node_id=NodeId(scenario.NODE_RECONCILE),
            partition_key=PartitionKey("p0"),
            partition_number=1,
            batch=ReconciliationConflictBatch(analysis.conflicts),
            created_at=clock.now(),
        )


# The artifact relative path type is re-exported for callers that inspect the
# published conflict artifact through the accepted boundary.
_ArtifactRelativePath = ArtifactRelativePath
_SyntheticDataset = SyntheticDataset
