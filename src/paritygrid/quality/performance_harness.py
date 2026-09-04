"""Deterministic, correctness-first performance harness (P21.4).

The harness measures the accepted Phase 19 showcase scenario and the
Phase 20 canonical engine orchestration under an explicit, bounded
method.  Correctness gates run first and are absolute: the showcase
derivation identity, the executed story manifest golden hash, and the
cross-runner execution-evidence equality must all hold before any
timing is accepted, and every measured repetition re-proves its own
manifest equality.  A correctness failure aborts measurement
acceptance entirely.

Every elapsed measurement uses the monotonic ``time.perf_counter``
clock.  Percentiles use the nearest-rank definition on the sorted
per-repetition durations.  The versioned JSON document records the
environment, hardware, method, metric definitions with units, raw
bounded per-repetition observations, and structured unavailability
reasons; it never records hostnames, usernames, local paths,
environment variables, or secrets.

"Reproducible" describes the stable schema, ordering, inputs, and
method: real timing and memory values are measurements and are not
byte-identical between executions.  The report makes no universal
speed claim and never claims one runner is faster than another; the
per-runner durations exist only as bounded diagnostics next to the
correctness proof, and no release performance threshold is proposed
until a measured baseline has been collected and documented.
"""

from __future__ import annotations

import asyncio
import json
import math
import multiprocessing
import os
import platform
import sqlite3
import sys
import threading
import time
import tracemalloc
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from itertools import pairwise as itertools_pairwise
from pathlib import Path
from typing import cast

from paritygrid import __version__
from paritygrid.adapters.analytics.duckdb import DuckDBLifecycleCoordinator
from paritygrid.adapters.analytics.run_statistics import (
    DuckDBRunStatisticsQueryEngine,
    RunStatisticsQuerySnapshot,
    RunStatisticsSourceSnapshot,
    RunStatisticsSummary,
)
from paritygrid.adapters.persistence import SQLiteFinalizationStateReader
from paritygrid.adapters.persistence.concurrent_execution import SQLiteAdmissionStateReader
from paritygrid.application.execution import DurableResultCommitFactory
from paritygrid.application.execution.asyncio_strategy import AsyncioFullPlanStrategy
from paritygrid.application.execution.concurrent_engine import (
    AdmissionFacts,
    ConcurrentRunEngine,
)
from paritygrid.application.execution.finalization import FinalizationSettings, RunFinalizer
from paritygrid.application.execution.full_plan_strategy import (
    ExecutedWork,
    FullPlanStrategy,
    SequentialFullPlanStrategy,
)
from paritygrid.application.execution.result_coordinator_writer import (
    TransactionalResultCoordinatorWriter,
)
from paritygrid.application.execution.runner_contract import WorkAssignmentV1
from paritygrid.application.execution.threaded_strategy import ThreadedFullPlanStrategy
from paritygrid.application.planner import PlanFingerprint
from paritygrid.application.ports.analytics import AnalyticalDatabaseConfig
from paritygrid.demo.scenario_runner import run_canonical_scenario
from paritygrid.demo.scenarios import (
    CANONICAL_CORRELATION_ID,
    CANONICAL_NODES,
    CANONICAL_SCENARIO_SEED,
    CANONICAL_SCENARIO_VERSION,
    SCENARIO_FORMAT_NAME,
    SCENARIO_FORMAT_VERSION,
    SHOWCASE_PROFILE,
    ScenarioExpectedEvidence,
    build_manifest,
    canonical_plan_fingerprint,
    derive_scenario,
)
from paritygrid.demo.verification import (
    ASYNCIO_STRATEGY,
    REQUIRED_STRATEGIES,
    SEQUENTIAL_STRATEGY,
    THREADED_STRATEGY,
    CanonicalEngineExecutor,
    ConcurrentScenarioHarness,
    RunnerExecutionRecord,
    bootstrap_canonical_run,
    build_canonical_engine_with_observation,
    build_cross_runner_verification,
    canonical_run_id,
)
from paritygrid.domain.models import NodeId, RunId
from paritygrid.quality.concurrent_scenario import prepare_concurrent_harness
from paritygrid.quality.fresh_root import FreshRootError, claim_fresh_root

PERFORMANCE_REPORT_FORMAT = "paritygrid-performance-report"
PERFORMANCE_REPORT_VERSION = 1

# The accepted Phase 19 golden locks.  A scenario change is a deliberate
# scenario-version change that must refresh these hashes; a mismatch here is
# a correctness failure, never a timing concern.
ACCEPTED_SHOWCASE_DERIVATION_MANIFEST_SHA256 = (
    "c732777145b0bdcf9742773e16a2263bb5bcca606468ff4eef5a6776de9afc2a"
)
ACCEPTED_SHOWCASE_RUN_MANIFEST_SHA256 = (
    "a60df038b0d933732877175fca637a7dbe31634c754d4db75d0fa70c20480f4e"
)

# The canonical story scripts exactly one rate-limit retry; every measured
# repetition re-proves that fact by executed-manifest equality.
STORY_RETRY_COUNT = 1

_WARMUP_BOUND = 5
_MEASURED_RUNS_BOUND = 20
_REPORT_TEXT_BOUND = 128
_ENGINE_RUN_ID_BASE = 100
_ENGINE_RUN_ID_LIMIT = 900
_FINALIZATION_SETTINGS = FinalizationSettings(5.0, 5.0)

_RUNNER_STRATEGY_TYPES: dict[str, type[FullPlanStrategy]] = {
    SEQUENTIAL_STRATEGY: SequentialFullPlanStrategy,
    THREADED_STRATEGY: ThreadedFullPlanStrategy,
    ASYNCIO_STRATEGY: AsyncioFullPlanStrategy,
}


class PerformanceHarnessError(RuntimeError):
    """The harness could not produce an accepted measurement report."""


class PerformanceCorrectnessError(PerformanceHarnessError):
    """A correctness gate failed; no timing may be accepted."""


class PerformanceReportError(ValueError):
    """A performance report payload violated the closed versioned contract."""


def _sha256_hex(payload: bytes) -> str:
    return sha256(payload).hexdigest()


def nearest_rank_percentile(sorted_values: Sequence[float], percentile: float) -> float:
    """Return the nearest-rank percentile of a non-decreasing sequence.

    The rank is ``max(1, ceil(percentile / 100 * n))`` over ``n`` sorted
    values, so the reported value is always an observed sample.  Inputs must
    be finite, non-negative, and sorted non-decreasingly; violations raise
    :class:`PerformanceReportError` rather than being clamped.
    """
    if type(percentile) is not float and type(percentile) is not int:
        raise PerformanceReportError("percentile must be a real number")
    percent = float(percentile)
    if not math.isfinite(percent) or not 0.0 <= percent <= 100.0:
        raise PerformanceReportError("percentile must be a finite value between 0 and 100")
    values = tuple(sorted_values)
    if not values:
        raise PerformanceReportError("percentile requires at least one observation")
    for value in values:
        if type(value) is not float and type(value) is not int:
            raise PerformanceReportError("observations must be real numbers")
        if not math.isfinite(value) or value < 0.0:
            raise PerformanceReportError("observations must be finite and non-negative")
    for earlier, later in itertools_pairwise(values):
        if later < earlier:
            raise PerformanceReportError("observations must be sorted non-decreasingly")
    rank = max(1, min(len(values), math.ceil(percent / 100.0 * len(values))))
    return float(values[rank - 1])


def _latency_percentiles(values: Sequence[float]) -> tuple[float, float, float]:
    """Calculate p50/p95/p99 after ordering chronological observations."""
    sorted_values = sorted(values)
    return (
        nearest_rank_percentile(sorted_values, 50),
        nearest_rank_percentile(sorted_values, 95),
        nearest_rank_percentile(sorted_values, 99),
    )


def _bounded_text(value: object) -> str | None:
    """Return printable-ASCII text clipped to the report bound, else None."""
    if type(value) is not str:
        return None
    clipped = value[:_REPORT_TEXT_BOUND]
    if not clipped or any(not ("\x20" <= character <= "\x7e") for character in clipped):
        return None
    return clipped


@dataclass(frozen=True, slots=True)
class TotalMemoryFacts:
    """Hardware memory facts read from the operating system, or unavailability."""

    total_bytes: int | None
    source: str | None


def _read_total_memory() -> TotalMemoryFacts:
    """Read total physical memory from the OS, or report it unavailable."""
    if sys.platform == "win32":
        return _read_total_memory_windows()
    if sys.platform.startswith("linux"):
        return _read_total_memory_linux()
    return TotalMemoryFacts(None, None)


def _read_total_memory_windows() -> TotalMemoryFacts:
    if sys.platform != "win32":
        return TotalMemoryFacts(None, None)
    import ctypes
    from ctypes import wintypes

    class _MemoryStatus(ctypes.Structure):
        _fields_ = [
            ("dwLength", wintypes.DWORD),
            ("dwMemoryLoad", wintypes.DWORD),
            ("ullTotalPhys", ctypes.c_size_t),
            ("ullAvailPhys", ctypes.c_size_t),
            ("ullTotalPageFile", ctypes.c_size_t),
            ("ullAvailPageFile", ctypes.c_size_t),
            ("ullTotalVirtual", ctypes.c_size_t),
            ("ullAvailVirtual", ctypes.c_size_t),
            ("ullAvailExtendedVirtual", ctypes.c_size_t),
        ]

    try:
        status = _MemoryStatus()
        status.dwLength = ctypes.sizeof(_MemoryStatus)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return TotalMemoryFacts(int(status.ullTotalPhys), "GlobalMemoryStatusEx")
    except Exception:  # hardware facts degrade to unavailability, never crash
        pass
    return TotalMemoryFacts(None, None)


def _read_total_memory_linux() -> TotalMemoryFacts:
    try:
        with open("/proc/meminfo", encoding="ascii") as stream:
            for line in stream:
                if line.startswith("MemTotal:"):
                    fields = line.split()
                    if len(fields) >= 2:
                        return TotalMemoryFacts(int(fields[1]) * 1024, "/proc/meminfo")
    except OSError, ValueError:
        pass
    return TotalMemoryFacts(None, None)


@dataclass(frozen=True, slots=True)
class PerformanceConfig:
    """Bounded, validated measurement plan for one harness execution."""

    story_warmup_runs: int = 1
    story_measured_runs: int = 3
    runner_warmup_runs: int = 1
    runner_measured_runs: int = 3

    def __post_init__(self) -> None:
        for name in (
            "story_warmup_runs",
            "story_measured_runs",
            "runner_warmup_runs",
            "runner_measured_runs",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise PerformanceHarnessError(f"{name} must be an integer")
        if not 0 <= self.story_warmup_runs <= _WARMUP_BOUND:
            raise PerformanceHarnessError("story warm-up count is outside the supported bound")
        if not 1 <= self.story_measured_runs <= _MEASURED_RUNS_BOUND:
            raise PerformanceHarnessError("story measured count is outside the supported bound")
        if not 0 <= self.runner_warmup_runs <= _WARMUP_BOUND:
            raise PerformanceHarnessError("runner warm-up count is outside the supported bound")
        if not 1 <= self.runner_measured_runs <= _MEASURED_RUNS_BOUND:
            raise PerformanceHarnessError("runner measured count is outside the supported bound")


@dataclass(frozen=True, slots=True)
class MetricDefinition:
    """One exact metric name, unit, and measurement definition."""

    name: str
    unit: str
    definition: str


METRIC_DEFINITIONS: tuple[MetricDefinition, ...] = (
    MetricDefinition(
        name="total_duration_seconds",
        unit="seconds",
        definition=(
            "wall-clock duration of one complete measured repetition, read from the "
            "monotonic time.perf_counter clock around the entire workload"
        ),
    ),
    MetricDefinition(
        name="records_per_second",
        unit="rows per second",
        definition=(
            "the showcase dataset total_input_rows divided by that repetition's "
            "total_duration_seconds"
        ),
    ),
    MetricDefinition(
        name="latency_pNN_seconds",
        unit="seconds",
        definition=(
            "nearest-rank percentile over the sorted per-repetition "
            "total_duration_seconds observations; the reported value is always an "
            "observed sample"
        ),
    ),
    MetricDefinition(
        name="queue_wait_seconds",
        unit="seconds",
        definition=(
            "per engine repetition: sum and mean over work items with an observed "
            "admission of (first executor entry) minus (first durable admission "
            "read) for that identity, timed at the admission-reader and executor "
            "boundaries"
        ),
    ),
    MetricDefinition(
        name="service_time_seconds",
        unit="seconds",
        definition=(
            "per engine repetition: total and mean duration of executor.execute() "
            "calls for executed work items"
        ),
    ),
    MetricDefinition(
        name="retry_count",
        unit="count",
        definition=(
            "durable attempts beyond one per work identity, summed for the "
            "repetition from the frozen attempt-outcome counts; the story "
            "repetitions carry the scripted rate-limit retry proven by their "
            "executed-manifest equality"
        ),
    ),
    MetricDefinition(
        name="peak_in_flight_work",
        unit="count",
        definition=(
            "maximum len(engine.in_flight_identities) observed at every executor "
            "entry and exit of the repetition"
        ),
    ),
    MetricDefinition(
        name="peak_concurrent_service",
        unit="count",
        definition=(
            "maximum number of simultaneously open executor.execute() calls "
            "observed for the repetition"
        ),
    ),
    MetricDefinition(
        name="sqlite_commit_seconds",
        unit="seconds",
        definition=(
            "durable result-commit transactions timed at the result-coordinator "
            "writer boundary (submit through durable acknowledgement); reported "
            "as count, total, and mean per repetition"
        ),
    ),
    MetricDefinition(
        name="duckdb_query_seconds",
        unit="seconds",
        definition=(
            "run-statistics analytical work timed at the query-engine boundary "
            "during run finalization (rebuild plus summary)"
        ),
    ),
    MetricDefinition(
        name="process_peak_rss_bytes",
        unit="bytes",
        definition="peak resident working-set size sampled while the repetition ran",
    ),
    MetricDefinition(
        name="python_heap_peak_bytes",
        unit="bytes",
        definition="peak tracemalloc-traced heap size sampled while the repetition ran",
    ),
)

_DISCLAIMERS: tuple[str, ...] = (
    "measurements are bounded local diagnostics, not universal speed claims",
    "no runner is claimed faster than another on any sample size",
    "no release performance threshold is established by this document",
    "timing and environment data never enter the canonical correctness evidence",
)


class _ObservationBoundary:
    """Mutable, lock-guarded accumulator for one instrumented engine run.

    The engine mutates its in-flight registry only on its main thread, so
    reading its length from executor entry and exit is a safe CPython
    snapshot; every other accumulation happens under this boundary's lock.
    """

    __slots__ = (
        "_admission_first_seen",
        "_commit_count",
        "_commit_seconds_total",
        "_duckdb_seconds",
        "_execute_count",
        "_execute_seconds_total",
        "_lock",
        "_open_executes",
        "_peak_concurrent_service",
        "_peak_in_flight",
        "_queue_wait_total",
        "_waits_observed",
    )

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._admission_first_seen: dict[tuple[str, str], float] = {}
        self._execute_count = 0
        self._execute_seconds_total = 0.0
        self._open_executes = 0
        self._peak_concurrent_service = 0
        self._peak_in_flight = 0
        self._queue_wait_total = 0.0
        self._waits_observed = 0
        self._commit_count = 0
        self._commit_seconds_total = 0.0
        self._duckdb_seconds = 0.0

    def record_admission(self, node_id: str, partition_key: str) -> None:
        now = time.perf_counter()
        with self._lock:
            self._admission_first_seen.setdefault((node_id, partition_key), now)

    def observe_in_flight(self, in_flight: int) -> None:
        with self._lock:
            if in_flight > self._peak_in_flight:
                self._peak_in_flight = in_flight

    def record_execute_start(self, node_id: str, partition_key: str) -> float:
        started = time.perf_counter()
        with self._lock:
            self._open_executes += 1
            if self._open_executes > self._peak_concurrent_service:
                self._peak_concurrent_service = self._open_executes
            admission = self._admission_first_seen.get((node_id, partition_key))
            if admission is not None:
                self._queue_wait_total += started - admission
                self._waits_observed += 1
        return started

    def record_execute_end(self, started: float) -> None:
        ended = time.perf_counter()
        with self._lock:
            self._open_executes -= 1
            self._execute_count += 1
            self._execute_seconds_total += ended - started

    def record_commit(self, seconds: float) -> None:
        with self._lock:
            self._commit_count += 1
            self._commit_seconds_total += seconds

    def record_duckdb(self, seconds: float) -> None:
        with self._lock:
            self._duckdb_seconds += seconds

    def queue_wait_mean(self) -> float | None:
        with self._lock:
            if self._waits_observed == 0:
                return None
            return self._queue_wait_total / self._waits_observed

    def queue_wait_total(self) -> float:
        with self._lock:
            return self._queue_wait_total

    def waits_observed(self) -> int:
        with self._lock:
            return self._waits_observed

    def service_mean(self) -> float | None:
        with self._lock:
            if self._execute_count == 0:
                return None
            return self._execute_seconds_total / self._execute_count

    def service_total(self) -> float:
        with self._lock:
            return self._execute_seconds_total

    def peak_concurrent_service(self) -> int:
        with self._lock:
            return self._peak_concurrent_service

    def peak_in_flight(self) -> int:
        with self._lock:
            return self._peak_in_flight

    def commit_count(self) -> int:
        with self._lock:
            return self._commit_count

    def commit_total(self) -> float:
        with self._lock:
            return self._commit_seconds_total

    def commit_mean(self) -> float | None:
        with self._lock:
            if self._commit_count == 0:
                return None
            return self._commit_seconds_total / self._commit_count

    def duckdb_seconds(self) -> float:
        with self._lock:
            return self._duckdb_seconds


class _InstrumentedExecutor:
    """Executor wrapper timing real execute() calls of the canonical executor."""

    __slots__ = ("_boundary", "_engine", "_inner")

    def __init__(self, inner: CanonicalEngineExecutor, boundary: _ObservationBoundary) -> None:
        self._inner = inner
        self._boundary = boundary
        self._engine: ConcurrentRunEngine | None = None

    def bind_engine(self, engine: ConcurrentRunEngine) -> None:
        self._engine = engine

    def execute(self, assignment: WorkAssignmentV1) -> ExecutedWork:
        node = str(assignment.node_id)
        partition = str(assignment.partition_key)
        self._boundary.record_admission(node, partition)
        engine = self._engine
        if engine is not None:
            self._boundary.observe_in_flight(len(engine.in_flight_identities))
        started = self._boundary.record_execute_start(node, partition)
        try:
            return self._inner.execute(assignment)
        finally:
            self._boundary.record_execute_end(started)

    def close(self) -> None:
        self._inner.close()


class _TimedAdmissionReader:
    """Admission reader wrapper recording the first admission read per identity."""

    __slots__ = ("_boundary", "_inner")

    def __init__(self, inner: SQLiteAdmissionStateReader, boundary: _ObservationBoundary) -> None:
        self._inner = inner
        self._boundary = boundary

    def read(self, run_id: str, node_id: str, partition_key: str) -> AdmissionFacts:
        self._boundary.record_admission(node_id, partition_key)
        return self._inner.read(run_id, node_id, partition_key)


class _TimedResultWriter:
    """Result-writer wrapper timing each durable commit transaction."""

    __slots__ = ("_boundary", "_inner")

    def __init__(
        self, inner: TransactionalResultCoordinatorWriter, boundary: _ObservationBoundary
    ) -> None:
        self._inner = inner
        self._boundary = boundary

    def submit(self, command: object, *, timeout_seconds: float) -> object:
        started = time.perf_counter()
        try:
            return self._inner.submit(command, timeout_seconds=timeout_seconds)
        finally:
            self._boundary.record_commit(time.perf_counter() - started)


class _TimedQueryEngine(DuckDBRunStatisticsQueryEngine):
    """Run-statistics engine timing its analytical rebuild and summary work."""

    __slots__ = ("_boundary",)

    def __init__(
        self,
        database: DuckDBLifecycleCoordinator,
        boundary: _ObservationBoundary,
    ) -> None:
        super().__init__(database)
        self._boundary = boundary

    def rebuild(self, source: RunStatisticsSourceSnapshot) -> RunStatisticsQuerySnapshot:
        started = time.perf_counter()
        try:
            return super().rebuild(source)
        finally:
            self._boundary.record_duckdb(time.perf_counter() - started)

    def get_summary(self, snapshot: RunStatisticsQuerySnapshot) -> RunStatisticsSummary:
        started = time.perf_counter()
        try:
            return super().get_summary(snapshot)
        finally:
            self._boundary.record_duckdb(time.perf_counter() - started)


def _finalize_with_timing(
    harness: ConcurrentScenarioHarness,
    run_id: RunId,
    boundary: _ObservationBoundary,
) -> str:
    """Finalize one measured engine run through the timed analytical boundary."""
    analytics_path = harness.artifact_root / "engine-analytics.duckdb"
    coordinator = DuckDBLifecycleCoordinator(AnalyticalDatabaseConfig(analytics_path.resolve()))
    coordinator.open()
    try:
        finalizer = RunFinalizer(
            harness.writer,
            SQLiteFinalizationStateReader(harness.database),
            _TimedQueryEngine(coordinator, boundary),
            harness.clock,
            settings=_FINALIZATION_SETTINGS,
        )
        finalization = finalizer.finalize(
            run_id,
            plan_nodes=tuple(NodeId(node) for node in CANONICAL_NODES),
            plan_fingerprint=PlanFingerprint(canonical_plan_fingerprint()),
        )
    finally:
        coordinator.close()
    if finalization.fingerprint is None:
        raise PerformanceHarnessError("a measured engine run finalized without a fingerprint")
    return finalization.fingerprint.value


def _freeze_record(
    harness: ConcurrentScenarioHarness,
    strategy_id: str,
    run_id: RunId,
    fingerprint: str,
) -> RunnerExecutionRecord:
    from paritygrid.demo.verification import freeze_runner_record

    return freeze_runner_record(
        harness, strategy_id, run_id, execution_evidence_fingerprint=fingerprint
    )


def _verify_showcase_correctness() -> tuple[ScenarioExpectedEvidence, list[dict[str, object]]]:
    """Run the derivation-level correctness checks and return their evidence."""
    checks: list[dict[str, object]] = []
    evidence = derive_scenario(SHOWCASE_PROFILE)
    plan_fingerprint = canonical_plan_fingerprint()
    if canonical_plan_fingerprint() != plan_fingerprint:
        raise PerformanceCorrectnessError("the canonical plan fingerprint is not stable")
    checks.append({"name": "plan_fingerprint_stable", "passed": True, "detail": plan_fingerprint})
    if (
        SCENARIO_FORMAT_NAME != "paritygrid-canonical-scenario"
        or SCENARIO_FORMAT_VERSION != 1
        or CANONICAL_SCENARIO_VERSION != 1
        or CANONICAL_SCENARIO_SEED != 19
        or evidence.profile.profile_id != "showcase"
    ):
        raise PerformanceCorrectnessError("the canonical scenario identity drifted")
    checks.append({"name": "scenario_identity", "passed": True, "detail": "showcase/seed=19"})
    derivation = build_manifest(
        evidence,
        execution_evidence_fingerprint=None,
        verification_result="parity_holding",
    ).canonical_bytes()
    derivation_hash = _sha256_hex(derivation)
    if derivation_hash != ACCEPTED_SHOWCASE_DERIVATION_MANIFEST_SHA256:
        raise PerformanceCorrectnessError(
            "the showcase derivation manifest does not match the accepted golden hash"
        )
    checks.append({"name": "derivation_manifest_golden", "passed": True, "detail": derivation_hash})
    if evidence.counts.total_input_rows != SHOWCASE_PROFILE.record_count:
        raise PerformanceCorrectnessError("the derived row count contradicts the profile")
    checks.append(
        {"name": "row_counts", "passed": True, "detail": str(evidence.counts.total_input_rows)}
    )
    return evidence, checks


def _verify_executed_manifest(manifest_bytes: bytes, label: str) -> dict[str, object]:
    """Prove one executed story manifest against the accepted golden hash."""
    executed_hash = _sha256_hex(manifest_bytes)
    if executed_hash != ACCEPTED_SHOWCASE_RUN_MANIFEST_SHA256:
        raise PerformanceCorrectnessError(
            f"the executed showcase manifest for {label} does not match the accepted golden hash"
        )
    return {"name": f"executed_manifest_golden[{label}]", "passed": True, "detail": executed_hash}


def _run_measured_engine(
    harness: ConcurrentScenarioHarness,
    strategy_id: str,
    run_offset: int,
    measurement_index: int,
) -> tuple[RunnerExecutionRecord, dict[str, object]]:
    """Run one instrumented canonical engine repetition for one runner."""
    boundary = _ObservationBoundary()
    executor = _InstrumentedExecutor(CanonicalEngineExecutor(harness), boundary)
    strategy = _RUNNER_STRATEGY_TYPES[strategy_id]()
    run_id = canonical_run_id(run_offset)
    admission_reader = SQLiteAdmissionStateReader(harness.database)
    result_writer = TransactionalResultCoordinatorWriter(
        harness.writer,
        DurableResultCommitFactory(correlation_id=CANONICAL_CORRELATION_ID),
    )
    engine, channels = build_canonical_engine_with_observation(
        harness,
        run_id,
        strategy=strategy,
        executor=executor,
        admission_reader=_TimedAdmissionReader(admission_reader, boundary),
        result_writer=_TimedResultWriter(result_writer, boundary),
    )
    executor.bind_engine(engine)
    bootstrap_canonical_run(harness, run_id)
    started = time.perf_counter()
    report = engine.run()
    if report.status.value != "completed":
        raise PerformanceHarnessError(
            f"the measured {strategy_id} engine run ended {report.status.value}"
        )
    fingerprint = _finalize_with_timing(harness, run_id, boundary)
    record = _freeze_record(harness, strategy_id, run_id, fingerprint)
    duration = time.perf_counter() - started
    high_water = {
        kind: max_observed for kind, _capacity, _queued, max_observed in channels.snapshots()
    }
    attempts = sum(record.attempt_outcome_counts.values())
    work_items = len(record.evidence.work_states)
    observation: dict[str, object] = {
        "repetition": measurement_index,
        "total_duration_seconds": duration,
        "admitted_count": report.admitted_count,
        "committed_count": report.committed_count,
        "retry_count": attempts - work_items,
        "queue_wait_seconds_total": boundary.queue_wait_total(),
        "queue_wait_seconds_mean": boundary.queue_wait_mean(),
        "queue_waits_observed": boundary.waits_observed(),
        "service_time_seconds_total": boundary.service_total(),
        "service_time_seconds_mean": boundary.service_mean(),
        "peak_in_flight_work": boundary.peak_in_flight(),
        "peak_concurrent_service": boundary.peak_concurrent_service(),
        "sqlite_commit_count": boundary.commit_count(),
        "sqlite_commit_seconds_total": boundary.commit_total(),
        "sqlite_commit_seconds_mean": boundary.commit_mean(),
        "duckdb_query_seconds": boundary.duckdb_seconds(),
        "channel_high_water": high_water,
    }
    return record, observation


def _compare_durable_shape(
    gate_record: RunnerExecutionRecord,
    measured_record: RunnerExecutionRecord,
    label: str,
) -> None:
    """Re-prove one measured runner repetition against its gate record.

    Run-local identities (run id, work and artifact identities, causal
    event order) legitimately differ between runs, so the comparison uses
    the run-independent durable facts: attempt outcome counts, node
    metrics, and checkpoint identity count.  A divergence is a correctness
    failure, and the repetition's timing is never accepted.
    """
    if measured_record.attempt_outcome_counts != gate_record.attempt_outcome_counts:
        raise PerformanceCorrectnessError(
            f"{label} attempt outcomes diverged from the correctness gate"
        )
    if measured_record.node_metrics != gate_record.node_metrics:
        raise PerformanceCorrectnessError(
            f"{label} durable node metrics diverged from the correctness gate"
        )
    if measured_record.checkpoint_count != gate_record.checkpoint_count:
        raise PerformanceCorrectnessError(
            f"{label} checkpoint count diverged from the correctness gate"
        )


class _StoryTrace:
    """Bounded resource trace of one story repetition."""

    __slots__ = ("heap_peak", "manifest_bytes", "rss_peak")

    def __init__(self, manifest_bytes: bytes, rss_peak: int | None, heap_peak: int | None) -> None:
        self.manifest_bytes = manifest_bytes
        self.rss_peak = rss_peak
        self.heap_peak = heap_peak


def _profiled_story(root: Path) -> _StoryTrace:
    """Run one story repetition under the resource sampler and tracemalloc."""
    from paritygrid.quality.resource_profile import ResourceSampler, TracemallocHeapProbe

    sampler = ResourceSampler(heap_probe=TracemallocHeapProbe())
    tracemalloc_was_tracing = tracemalloc.is_tracing()
    if not tracemalloc_was_tracing:
        tracemalloc.start()
    sampler.start()
    try:
        result = asyncio.run(run_canonical_scenario(SHOWCASE_PROFILE, root))
        # The peak must be read before tracemalloc stops: stopping resets the
        # traced counters, so a post-stop read fabricates a numeric zero.
        _heap_current, heap_peak = tracemalloc.get_traced_memory()
    finally:
        sampler.stop()
        if not tracemalloc_was_tracing:
            tracemalloc.stop()
        else:
            # Another owner owns tracing; this repetition's peak is unowned.
            heap_peak = None
    rss_peak = max(
        (sample.rss_bytes for sample in sampler.samples() if sample.rss_bytes is not None),
        default=None,
    )
    return _StoryTrace(result.manifest_bytes, rss_peak, heap_peak)


def _environment_document() -> dict[str, object]:
    memory = _read_total_memory()
    processor = _bounded_text(platform.processor())
    unavailable: list[dict[str, str]] = []
    if processor is None:
        unavailable.append(
            {"observation": "processor", "reason": "processor text is not bounded ASCII"}
        )
    if memory.total_bytes is None or memory.source is None:
        unavailable.append(
            {
                "observation": "total_memory_bytes",
                "reason": "total physical memory is not exposed by this platform",
            }
        )
    return {
        "system": _bounded_text(platform.system()) or "unknown",
        "machine": _bounded_text(platform.machine()) or "unknown",
        "processor": processor,
        "python_implementation": _bounded_text(platform.python_implementation()) or "unknown",
        "python_version": platform.python_version(),
        "sqlite_version": sqlite3.sqlite_version,
        "cpu_count": os.cpu_count(),
        "total_memory_bytes": memory.total_bytes,
        "total_memory_source": memory.source,
        "unavailable": unavailable,
    }


def _method_document(config: PerformanceConfig) -> dict[str, object]:
    return {
        "clock": "time.perf_counter (monotonic)",
        "percentile_method": (
            "nearest rank over sorted per-repetition durations: "
            "rank = max(1, ceil(p / 100 * n)); the value is an observed sample"
        ),
        "story_warmup_runs": config.story_warmup_runs,
        "story_measured_runs": config.story_measured_runs,
        "runner_warmup_runs": config.runner_warmup_runs,
        "runner_measured_runs": config.runner_measured_runs,
        "runners": list(REQUIRED_STRATEGIES),
        "metric_definitions": [
            {"name": metric.name, "unit": metric.unit, "definition": metric.definition}
            for metric in METRIC_DEFINITIONS
        ],
        "disclaimers": list(_DISCLAIMERS),
    }


def _scenario_document(evidence: ScenarioExpectedEvidence) -> dict[str, object]:
    return {
        "format_name": SCENARIO_FORMAT_NAME,
        "format_version": SCENARIO_FORMAT_VERSION,
        "scenario_version": CANONICAL_SCENARIO_VERSION,
        "seed": CANONICAL_SCENARIO_SEED,
        "profile_id": evidence.profile.profile_id,
        "profile_identity": evidence.profile.identity_bytes().decode("ascii"),
        "plan_fingerprint": canonical_plan_fingerprint(),
        "record_count": evidence.counts.total_input_rows,
        "expected_counts": {
            "total_input_rows": evidence.counts.total_input_rows,
            "accepted_rows": evidence.counts.accepted_rows,
            "quarantined_rows": evidence.counts.quarantined_rows,
            "planned_repairs": evidence.counts.planned_repairs,
            "applied_repairs": evidence.counts.applied_repairs,
        },
        "derivation_manifest_sha256": ACCEPTED_SHOWCASE_DERIVATION_MANIFEST_SHA256,
        "executed_manifest_sha256": ACCEPTED_SHOWCASE_RUN_MANIFEST_SHA256,
    }


_RUNNER_UNAVAILABLE_REASONS: dict[str, tuple[dict[str, str], ...]] = {
    SEQUENTIAL_STRATEGY: (
        {
            "observation": "concurrent_overlap",
            "reason": "the sequential runner executes one work item at a time by contract",
        },
    ),
    THREADED_STRATEGY: (
        {
            "observation": "asyncio_task_count",
            "reason": (
                "the threaded strategy runs worker threads, not an event loop; "
                "asyncio task counts do not apply"
            ),
        },
    ),
    ASYNCIO_STRATEGY: (
        {
            "observation": "asyncio_task_count",
            "reason": (
                "the asyncio strategy owns its event loop internally; task counts "
                "are not observable at the harness boundary"
            ),
        },
    ),
}


def _runner_unavailable(strategy_id: str) -> list[dict[str, str]]:
    """Declare the observations this runner cannot honestly provide."""
    return [dict(entry) for entry in _RUNNER_UNAVAILABLE_REASONS[strategy_id]]


def _cleanup_document() -> dict[str, object]:
    children = multiprocessing.active_children()
    owned = sorted(
        thread.name for thread in threading.enumerate() if thread.name.startswith("paritygrid")
    )
    return {
        "retired_within_deadline": not children and not owned,
        "child_process_count": len(children),
        "owned_thread_names": owned,
        "zero_orphan_children": not children,
    }


def _warmup_engine_run(
    harness: ConcurrentScenarioHarness, strategy_id: str, run_offset: int
) -> float:
    """Execute one warm-up engine run and return its wall duration."""
    from paritygrid.demo.verification import run_canonical_strategy

    started = time.perf_counter()
    run_canonical_strategy(harness, strategy_id, canonical_run_id(run_offset))
    return time.perf_counter() - started


def build_performance_report(root: Path, config: PerformanceConfig) -> dict[str, object]:
    """Run the gated measurements and return the closed report document.

    ``root`` must be a fresh bounded directory the caller owns; the harness
    creates isolated story and engine roots below it and removes them before
    returning.  A correctness failure raises before any measurement is
    accepted, and every measured repetition re-proves its own manifest.
    """
    evidence, derivation_checks = _verify_showcase_correctness()
    try:
        claimed_root = claim_fresh_root(root, prefix="paritygrid-performance-")
    except FreshRootError as error:
        raise PerformanceHarnessError(str(error)) from error
    harness_root = claimed_root.work / "engine"
    story_parent = claimed_root.work / "story"
    harness_root.mkdir(parents=True, exist_ok=True)
    story_parent.mkdir(parents=True, exist_ok=True)
    correctness_checks: list[dict[str, object]] = list(derivation_checks)
    story_observations: list[dict[str, object]] = []
    story_unavailable: list[dict[str, str]] = []
    story_warmup_durations: list[float] = []
    runner_sections: dict[str, dict[str, object]] = {}
    engine_harness: ConcurrentScenarioHarness | None = None
    try:
        engine_harness = prepare_concurrent_harness(
            harness_root / "engine.db", harness_root / "artifacts"
        )
        # Correctness gate: the executed showcase story must reproduce the
        # accepted golden manifest before any timing is accepted.
        gate_result = asyncio.run(run_canonical_scenario(SHOWCASE_PROFILE, story_parent / "gate"))
        correctness_checks.append(_verify_executed_manifest(gate_result.manifest_bytes, "gate"))
        # Correctness gate: cross-runner execution-evidence equality.  The
        # gate records become the durable shape every measured runner
        # repetition must reproduce before its timing is accepted.
        manifest = build_cross_runner_verification(engine_harness)
        if not manifest.equal:
            raise PerformanceCorrectnessError(
                "cross-runner execution evidence is not equal; timing is not accepted"
            )
        correctness_checks.append(
            {"name": "cross_runner_evidence_equal", "passed": True, "detail": None}
        )
        gate_records = {record.strategy_id: record for record in manifest.records}

        for repetition in range(config.story_warmup_runs):
            started = time.perf_counter()
            warm_result = asyncio.run(
                run_canonical_scenario(SHOWCASE_PROFILE, story_parent / f"warm-{repetition}")
            )
            story_warmup_durations.append(time.perf_counter() - started)
            _verify_executed_manifest(warm_result.manifest_bytes, f"warm-{repetition}")
        for repetition in range(config.story_measured_runs):
            started = time.perf_counter()
            trace = _profiled_story(story_parent / f"measured-{repetition}")
            duration = cast_duration(time.perf_counter() - started)
            _verify_executed_manifest(trace.manifest_bytes, f"measured-{repetition}")
            story_observations.append(
                {
                    "repetition": repetition,
                    "total_duration_seconds": duration,
                    "records_per_second": evidence.counts.total_input_rows / duration,
                    "retry_count": STORY_RETRY_COUNT,
                    "peak_process_rss_bytes": trace.rss_peak,
                    "peak_python_heap_bytes": trace.heap_peak,
                    "manifest_sha256": ACCEPTED_SHOWCASE_RUN_MANIFEST_SHA256,
                }
            )
            if trace.rss_peak is None and not any(
                entry["observation"] == "process_peak_rss_bytes" for entry in story_unavailable
            ):
                story_unavailable.append(
                    {
                        "observation": "process_peak_rss_bytes",
                        "reason": "the platform sampler could not observe the working set",
                    }
                )
            if trace.heap_peak is None and not any(
                entry["observation"] == "peak_python_heap_bytes" for entry in story_unavailable
            ):
                story_unavailable.append(
                    {
                        "observation": "peak_python_heap_bytes",
                        "reason": "tracemalloc was already active; the repetition peak is unowned",
                    }
                )

        run_offset = _ENGINE_RUN_ID_BASE
        for strategy_id in REQUIRED_STRATEGIES:
            warmups: list[float] = []
            for _warmup in range(config.runner_warmup_runs):
                warmups.append(_warmup_engine_run(engine_harness, strategy_id, run_offset))
                run_offset += 1
            observations: list[dict[str, object]] = []
            durations: list[float] = []
            for index in range(config.runner_measured_runs):
                measured_record, observation = _run_measured_engine(
                    engine_harness, strategy_id, run_offset, index
                )
                _compare_durable_shape(
                    gate_records[strategy_id],
                    measured_record,
                    f"{strategy_id} repetition {index}",
                )
                durations.append(cast_duration(observation["total_duration_seconds"]))
                observations.append(observation)
                run_offset += 1
                if run_offset > _ENGINE_RUN_ID_LIMIT:
                    raise PerformanceHarnessError("engine run identities exceeded their bound")
            latency_p50, latency_p95, latency_p99 = _latency_percentiles(durations)
            runner_sections[strategy_id] = {
                "warmup_durations_seconds": warmups,
                "observations": observations,
                "latency_p50_seconds": latency_p50,
                "latency_p95_seconds": latency_p95,
                "latency_p99_seconds": latency_p99,
                "unavailable": _runner_unavailable(strategy_id),
            }
    finally:
        try:
            if engine_harness is not None:
                engine_harness.close()
        finally:
            try:
                claimed_root.cleanup()
            except FreshRootError as error:
                raise PerformanceHarnessError(str(error)) from error
    harness_root_removed = not claimed_root.work.exists()

    story_durations = [
        cast_duration(observation["total_duration_seconds"]) for observation in story_observations
    ]
    story_latency_p50, story_latency_p95, story_latency_p99 = _latency_percentiles(story_durations)
    story_section: dict[str, object] = {
        "warmup_durations_seconds": story_warmup_durations,
        "observations": story_observations,
        "latency_p50_seconds": story_latency_p50,
        "latency_p95_seconds": story_latency_p95,
        "latency_p99_seconds": story_latency_p99,
        "records_per_second_mean": (
            sum(
                cast_duration(observation["records_per_second"])
                for observation in story_observations
            )
            / len(story_observations)
        ),
        "total_duration_seconds_mean": sum(story_durations) / len(story_durations),
        "unavailable": story_unavailable,
    }
    return {
        "format": PERFORMANCE_REPORT_FORMAT,
        "version": PERFORMANCE_REPORT_VERSION,
        "package_version": __version__,
        "environment": _environment_document(),
        "scenario": _scenario_document(evidence),
        "correctness": {
            "accepted": True,
            "checks": correctness_checks,
            "cross_runner_evidence_equal": True,
        },
        "method": _method_document(config),
        "story": story_section,
        "runners": runner_sections,
        "cleanup": {**_cleanup_document(), "harness_roots_removed": harness_root_removed},
    }


def cast_duration(value: object) -> float:
    """Narrow a measured duration to a strictly positive finite float."""
    if type(value) is not float or not math.isfinite(value) or value <= 0.0:
        raise PerformanceHarnessError("a measured duration was not a finite positive float")
    return value


def performance_report_bytes(document: Mapping[str, object]) -> bytes:
    """Encode the report deterministically with sorted keys and compact ASCII."""
    return json.dumps(
        document,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def parse_performance_report(payload: bytes) -> dict[str, object]:
    """Strictly validate a report payload and return its closed document.

    The consumer rejects malformed JSON, wrong format or version, missing or
    unknown top-level sections, non-object sections, negative or non-finite
    numbers, and reports whose correctness section does not state acceptance
    with cross-runner equality.  Unknown-version payloads fail closed.
    """
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise PerformanceReportError("report payload must be UTF-8 bytes") from error
    try:
        parsed_json: object = json.loads(text)
    except json.JSONDecodeError as error:
        raise PerformanceReportError("report payload must be valid JSON") from error
    if not isinstance(parsed_json, dict):
        raise PerformanceReportError("report payload must be a JSON object")
    document = cast_document(cast("dict[str, object]", parsed_json))
    required = frozenset(
        {
            "format",
            "version",
            "package_version",
            "environment",
            "scenario",
            "correctness",
            "method",
            "story",
            "runners",
            "cleanup",
        }
    )
    if frozenset(document) != required:
        raise PerformanceReportError("report sections must match the closed contract exactly")
    if document["format"] != PERFORMANCE_REPORT_FORMAT:
        raise PerformanceReportError("report format name is unknown")
    version = document["version"]
    if type(version) is not int or version != PERFORMANCE_REPORT_VERSION:
        raise PerformanceReportError("report version is not supported")
    for section in ("environment", "scenario", "correctness", "method", "story", "cleanup"):
        if not isinstance(document[section], dict):
            raise PerformanceReportError(f"report section {section} must be a JSON object")
    if not isinstance(document["runners"], dict):
        raise PerformanceReportError("report runners section must be a JSON object")
    for section in ("environment", "scenario", "method", "story", "cleanup"):
        if not cast_document(document[section]):
            raise PerformanceReportError(f"report section {section} must not be empty")
    runners = cast_document(cast("dict[str, object]", document["runners"]))
    if frozenset(runners) != frozenset(REQUIRED_STRATEGIES):
        raise PerformanceReportError(
            "report runners must cover exactly the required full-plan strategies"
        )
    correctness = cast_document(document["correctness"])
    if correctness.get("accepted") is not True:
        raise PerformanceReportError("only accepted reports may be consumed")
    if correctness.get("cross_runner_evidence_equal") is not True:
        raise PerformanceReportError("only evidence-equal reports may be consumed")
    _validate_performance_sections(document)
    _reject_unbounded_numbers(document)
    return document


def _mapping_with_keys(value: object, keys: frozenset[str], subject: str) -> dict[str, object]:
    mapping = cast_document(value)
    if frozenset(mapping) != keys:
        raise PerformanceReportError(f"{subject} fields must match the closed contract")
    return mapping


def _list_value(value: object, subject: str) -> list[object]:
    if not isinstance(value, list):
        raise PerformanceReportError(f"{subject} must be a JSON array")
    return cast("list[object]", value)


def _validate_performance_sections(document: dict[str, object]) -> None:
    """Validate every nested acceptance-bearing report section."""
    environment = _mapping_with_keys(
        document["environment"],
        frozenset(
            {
                "system",
                "machine",
                "processor",
                "python_implementation",
                "python_version",
                "sqlite_version",
                "cpu_count",
                "total_memory_bytes",
                "total_memory_source",
                "unavailable",
            }
        ),
        "environment",
    )
    _list_value(environment["unavailable"], "environment unavailable evidence")
    scenario = _mapping_with_keys(
        document["scenario"],
        frozenset(
            {
                "format_name",
                "format_version",
                "scenario_version",
                "profile_id",
                "profile_identity",
                "seed",
                "record_count",
                "expected_counts",
                "plan_fingerprint",
                "derivation_manifest_sha256",
                "executed_manifest_sha256",
            }
        ),
        "scenario",
    )
    _mapping_with_keys(
        scenario["expected_counts"],
        frozenset(
            {
                "total_input_rows",
                "accepted_rows",
                "quarantined_rows",
                "planned_repairs",
                "applied_repairs",
            }
        ),
        "scenario expected counts",
    )
    correctness = _mapping_with_keys(
        document["correctness"],
        frozenset({"accepted", "checks", "cross_runner_evidence_equal"}),
        "correctness",
    )
    checks = _list_value(correctness["checks"], "correctness checks")
    if not checks:
        raise PerformanceReportError("correctness checks must not be empty")
    for check in checks:
        entry = _mapping_with_keys(check, frozenset({"name", "passed", "detail"}), "check")
        if entry["passed"] is not True:
            raise PerformanceReportError("every correctness check must pass")
    method = _mapping_with_keys(
        document["method"],
        frozenset(
            {
                "clock",
                "percentile_method",
                "story_warmup_runs",
                "story_measured_runs",
                "runner_warmup_runs",
                "runner_measured_runs",
                "runners",
                "metric_definitions",
                "disclaimers",
            }
        ),
        "method",
    )
    method_runners = _list_value(method["runners"], "method runners")
    if method_runners != list(REQUIRED_STRATEGIES):
        raise PerformanceReportError("method runners must match the required strategies")
    definitions = _list_value(method["metric_definitions"], "metric definitions")
    if not definitions:
        raise PerformanceReportError("metric definitions must not be empty")
    for definition in definitions:
        _mapping_with_keys(
            definition, frozenset({"name", "unit", "definition"}), "metric definition"
        )
    _list_value(method["disclaimers"], "method disclaimers")
    story_runs = method["story_measured_runs"]
    runner_runs = method["runner_measured_runs"]
    if type(story_runs) is not int or not 1 <= story_runs <= _MEASURED_RUNS_BOUND:
        raise PerformanceReportError("story measured-run count is invalid")
    if type(runner_runs) is not int or not 1 <= runner_runs <= _MEASURED_RUNS_BOUND:
        raise PerformanceReportError("runner measured-run count is invalid")
    story = _mapping_with_keys(
        document["story"],
        frozenset(
            {
                "warmup_durations_seconds",
                "observations",
                "latency_p50_seconds",
                "latency_p95_seconds",
                "latency_p99_seconds",
                "records_per_second_mean",
                "total_duration_seconds_mean",
                "unavailable",
            }
        ),
        "story",
    )
    story_observations = _list_value(story["observations"], "story observations")
    if len(story_observations) != story_runs:
        raise PerformanceReportError("story observations must match the measured-run count")
    for observation in story_observations:
        _mapping_with_keys(
            observation,
            frozenset(
                {
                    "repetition",
                    "total_duration_seconds",
                    "records_per_second",
                    "retry_count",
                    "peak_process_rss_bytes",
                    "peak_python_heap_bytes",
                    "manifest_sha256",
                }
            ),
            "story observation",
        )
    _list_value(story["warmup_durations_seconds"], "story warmups")
    _list_value(story["unavailable"], "story unavailable evidence")
    runner_keys = frozenset(
        {
            "warmup_durations_seconds",
            "observations",
            "latency_p50_seconds",
            "latency_p95_seconds",
            "latency_p99_seconds",
            "unavailable",
        }
    )
    observation_keys = frozenset(
        {
            "repetition",
            "total_duration_seconds",
            "admitted_count",
            "committed_count",
            "retry_count",
            "queue_wait_seconds_total",
            "queue_wait_seconds_mean",
            "queue_waits_observed",
            "service_time_seconds_total",
            "service_time_seconds_mean",
            "peak_in_flight_work",
            "peak_concurrent_service",
            "sqlite_commit_count",
            "sqlite_commit_seconds_total",
            "sqlite_commit_seconds_mean",
            "duckdb_query_seconds",
            "channel_high_water",
        }
    )
    runners = cast_document(document["runners"])
    for strategy in REQUIRED_STRATEGIES:
        section = _mapping_with_keys(runners[strategy], runner_keys, f"runner {strategy}")
        observations = _list_value(section["observations"], f"runner {strategy} observations")
        if len(observations) != runner_runs:
            raise PerformanceReportError("runner observations must match the measured-run count")
        for observation in observations:
            item = _mapping_with_keys(observation, observation_keys, "runner observation")
            _mapping_with_keys(
                item["channel_high_water"],
                frozenset({"assignment", "result", "telemetry", "writer"}),
                "runner channel high-water",
            )
        _list_value(section["warmup_durations_seconds"], "runner warmups")
        _list_value(section["unavailable"], "runner unavailable evidence")
    cleanup = _mapping_with_keys(
        document["cleanup"],
        frozenset(
            {
                "retired_within_deadline",
                "child_process_count",
                "owned_thread_names",
                "zero_orphan_children",
                "harness_roots_removed",
            }
        ),
        "cleanup",
    )
    if (
        cleanup["retired_within_deadline"] is not True
        or cleanup["zero_orphan_children"] is not True
        or cleanup["harness_roots_removed"] is not True
        or cleanup["child_process_count"] != 0
        or _list_value(cleanup["owned_thread_names"], "owned thread names")
    ):
        raise PerformanceReportError("performance report cleanup evidence is not clean")


def cast_document(value: object) -> dict[str, object]:
    """Narrow a parsed JSON object for closed-contract validation."""
    if not isinstance(value, dict):
        raise PerformanceReportError("expected a JSON object")
    return cast("dict[str, object]", value)


def _reject_unbounded_numbers(value: object) -> None:
    if isinstance(value, bool):
        return
    if isinstance(value, float):
        if not math.isfinite(value) or value < 0.0:
            raise PerformanceReportError("report numbers must be finite and non-negative")
    elif isinstance(value, int):
        if value < 0:
            raise PerformanceReportError("report numbers must be non-negative")
    elif isinstance(value, dict):
        children: list[object] = list(cast("dict[str, object]", value).values())
        for child in children:
            _reject_unbounded_numbers(child)
    elif isinstance(value, list):
        children = list(cast("list[object]", value))
        for child in children:
            _reject_unbounded_numbers(child)


__all__ = [
    "ACCEPTED_SHOWCASE_DERIVATION_MANIFEST_SHA256",
    "ACCEPTED_SHOWCASE_RUN_MANIFEST_SHA256",
    "METRIC_DEFINITIONS",
    "PERFORMANCE_REPORT_FORMAT",
    "PERFORMANCE_REPORT_VERSION",
    "PerformanceConfig",
    "PerformanceCorrectnessError",
    "PerformanceHarnessError",
    "PerformanceReportError",
    "build_performance_report",
    "nearest_rank_percentile",
    "parse_performance_report",
    "performance_report_bytes",
]
