"""Bounded resource exercise over real runtime owners (P21.5).

The exercise drives the accepted canonical engine orchestration, the real
bounded channels, the runtime capability lifecycle, and the demo
interruption proof while a resource sampler watches the actual process.
Every bound is asserted against a configured capacity — never against a
synthetic counter — and every observation the platform cannot provide is
recorded as unavailable with a structured reason, never as zero.

Cells: steady-state execution, repeated executions with a documented
bounded-growth method, queue saturation with bounded backpressure,
cancellation cleanup, the canonical controlled retry failure, runtime
partial-startup rollback, idempotent repeated shutdown, and final orphan
detection over registered child processes and owned threads.  The separate
installed-wheel platform matrix performs the OS-visible process scan.
"""

from __future__ import annotations

import json
import math
import multiprocessing
import threading
import time
from pathlib import Path
from typing import cast

from paritygrid import __version__
from paritygrid.application.execution.channels import (
    BoundedChannel,
    ChannelSet,
    ChannelTimeoutError,
)
from paritygrid.application.execution.concurrency_settings import (
    CapturedConcurrencySettings,
    StrategyLifecycleCoordinator,
    StrategyLifecycleListener,
    describe_known_strategies,
)
from paritygrid.application.execution.concurrent_engine import EngineStatus
from paritygrid.application.execution.full_plan_strategy import (
    ExecutedWork,
    SequentialFullPlanStrategy,
    WorkAssignmentV1,
)
from paritygrid.demo.engine_runner import DemoEngineError
from paritygrid.demo.interruption import InterruptionError, run_interruption_proof
from paritygrid.demo.verification import (
    CanonicalEngineExecutor,
    ConcurrentScenarioHarness,
    bootstrap_canonical_run,
    build_canonical_engine_with_observation,
    canonical_run_id,
)
from paritygrid.quality.concurrent_scenario import prepare_concurrent_harness
from paritygrid.quality.fresh_root import FreshRootError, claim_fresh_root
from paritygrid.quality.resource_profile import (
    ResourceProfileResult,
    assert_within_capacity,
    bounded_growth_within,
    profile_callable,
)

RESOURCE_BOUNDS_REPORT_FORMAT = "paritygrid-resource-bounds-report"
RESOURCE_BOUNDS_REPORT_VERSION = 1

MIN_REPETITIONS = 1
MAX_REPETITIONS = 10
_DEFAULT_GLOBAL_WORK_LIMIT = CapturedConcurrencySettings().global_concurrent_work
_GROWTH_RATIO = 1.5
_GROWTH_SLACK_BYTES = 64 * 1024 * 1024
_HANDLE_CLEANUP_SLACK = 8
_THREAD_RETIREMENT_SECONDS = 15.0
_RETIREMENT_POLL_SLICE_SECONDS = 0.05
_CANCELLATION_DEADLINE_SECONDS = 60.0
_DEFAULT_INTERRUPTION_FAILPOINT = "repair.approved"
_BASE_RUN_OFFSET = 200
_CANCELLATION_RUN_OFFSET = 300
_MAX_RUN_OFFSET = 900


class ResourceBoundsError(RuntimeError):
    """A resource bound, cleanup invariant, or exercise cell failed."""


def _remaining_owned_threads() -> list[str]:
    """Return the names of live ParityGrid-owned threads, if any."""
    return [
        thread.name
        for thread in threading.enumerate()
        if thread.name.startswith("paritygrid") and thread.is_alive()
    ]


def _capacity_map(settings: CapturedConcurrencySettings) -> dict[str, int]:
    return {
        "assignment": settings.assignment_channel_capacity,
        "result": settings.result_channel_capacity,
        "telemetry": settings.telemetry_capacity,
        "writer": settings.writer_channel_capacity,
    }


def _assert_channel_bounds(
    channels: ChannelSet, settings: CapturedConcurrencySettings
) -> dict[str, int]:
    """Assert every real channel high-water mark against its configured capacity."""
    capacities = _capacity_map(settings)
    high_water: dict[str, int] = {}
    for kind, capacity, _queued, max_observed in channels.snapshots():
        assert_within_capacity(max_observed, capacity, f"{kind}_channel_high_water")
        if max_observed > capacities[kind]:
            raise ResourceBoundsError(f"channel {kind} exceeded its configured capacity")
        high_water[kind] = max_observed
    return high_water


def _freeze_retry_count(harness: ConcurrentScenarioHarness, run_id: object) -> int:
    from paritygrid.demo.verification import freeze_runner_record

    record = freeze_runner_record(harness, "sequential", run_id)  # type: ignore[arg-type]
    attempts = sum(record.attempt_outcome_counts.values())
    return attempts - len(record.evidence.work_states)


def _run_engine_once(
    harness: ConcurrentScenarioHarness, run_offset: int
) -> tuple[EngineStatus, dict[str, int], int]:
    """Execute one canonical engine repetition; return status, bounds, retries."""
    executor = CanonicalEngineExecutor(harness)
    strategy = SequentialFullPlanStrategy()
    run_id = canonical_run_id(run_offset)
    engine, channels = build_canonical_engine_with_observation(
        harness, run_id, strategy=strategy, executor=executor
    )
    bootstrap_canonical_run(harness, run_id)
    report = engine.run()
    if report.status is not EngineStatus.COMPLETED:
        raise ResourceBoundsError(f"the exercise engine run ended {report.status.value}")
    retry_count = _freeze_retry_count(harness, run_id)
    if retry_count != 1:
        # The canonical script carries exactly one rate-limit retry; a
        # missing or duplicated retry is a forced-failure evidence defect.
        raise ResourceBoundsError(
            f"the scripted retry was not observed exactly once (retry_count={retry_count})"
        )
    return (
        report.status,
        _assert_channel_bounds(channels, CapturedConcurrencySettings()),
        retry_count,
    )


def _profiled_engine_run(
    harness: ConcurrentScenarioHarness, run_offset: int
) -> tuple[EngineStatus, dict[str, int], int, ResourceProfileResult]:
    outcome: list[tuple[EngineStatus, dict[str, int], int]] = []

    def workload() -> bool:
        outcome.append(_run_engine_once(harness, run_offset))
        return True

    completed, profile = profile_callable(workload)
    if not completed or not outcome:
        raise ResourceBoundsError("the profiled engine workload did not record its outcome")
    if profile.cleanup.child_process_count > 0:
        raise ResourceBoundsError("owned child processes remained after the engine run")
    if not profile.cleanup.retired_within_deadline:
        raise ResourceBoundsError("owned threads did not retire after the engine run")
    if (
        profile.baseline_handle_count is not None
        and profile.cleanup.handle_count is not None
        and profile.cleanup.handle_count > profile.baseline_handle_count + _HANDLE_CLEANUP_SLACK
    ):
        raise ResourceBoundsError("process handles did not return to the engine-run bound")
    status, high_water, retry_count = outcome[0]
    return status, high_water, retry_count, profile


def _saturation_cell(settings: CapturedConcurrencySettings) -> dict[str, object]:
    """Fill a real bounded channel to capacity and prove bounded backpressure."""
    channel = BoundedChannel(kind="result", capacity=settings.result_channel_capacity)
    accepted = 0
    while channel.try_send({"identity": accepted, "payload": b"x" * 64}):
        accepted += 1
    if accepted != settings.result_channel_capacity:
        raise ResourceBoundsError(
            "the bounded channel accepted work beyond its configured capacity"
        )
    blocked = False
    try:
        channel.send({"identity": accepted}, timeout=0.05)
    except ChannelTimeoutError:
        blocked = True
    if not blocked:
        raise ResourceBoundsError("a full bounded channel did not propagate backpressure")
    assert_within_capacity(
        channel.max_observed_queued, settings.result_channel_capacity, "saturation"
    )
    drained = channel.drain()
    if len(drained) != accepted:
        raise ResourceBoundsError("close did not preserve every accepted message")
    channel.close()
    return {
        "capacity": settings.result_channel_capacity,
        "accepted_before_backpressure": accepted,
        "max_observed_queued": channel.max_observed_queued,
        "backpressure_blocked_full_send": blocked,
        "drained_after_close": len(drained),
    }


class _CancellationExecutor(CanonicalEngineExecutor):
    """Executor that signals this thread at the first real execution."""

    __slots__ = ("_first_started",)

    def __init__(self, harness: ConcurrentScenarioHarness, first_started: threading.Event) -> None:
        super().__init__(harness)
        self._first_started = first_started

    def execute(self, assignment: WorkAssignmentV1) -> ExecutedWork:
        self._first_started.set()
        return super().execute(assignment)


def _cancellation_cell(harness: ConcurrentScenarioHarness, run_offset: int) -> dict[str, object]:
    """Cancel a running engine and prove bounded, orphan-free cleanup."""
    first_started = threading.Event()
    executor = _CancellationExecutor(harness, first_started)
    strategy = SequentialFullPlanStrategy()
    run_id = canonical_run_id(run_offset)
    engine, channels = build_canonical_engine_with_observation(
        harness, run_id, strategy=strategy, executor=executor
    )
    bootstrap_canonical_run(harness, run_id)
    outcome: list[EngineStatus] = []

    def run_engine() -> None:
        outcome.append(engine.run().status)

    worker = threading.Thread(target=run_engine, name="paritygrid-bounds-cancel", daemon=True)
    worker.start()
    if not first_started.wait(_CANCELLATION_DEADLINE_SECONDS):
        raise ResourceBoundsError("the cancellation cell never started executing work")
    engine.request_cancellation()
    worker.join(_CANCELLATION_DEADLINE_SECONDS)
    if worker.is_alive():
        raise ResourceBoundsError("the cancelled engine run did not finish within its bound")
    if not outcome or outcome[0] is not EngineStatus.CANCELLED:
        observed = outcome[0].value if outcome else "none"
        raise ResourceBoundsError(f"cancellation produced {observed}, not cancelled")
    high_water = _assert_channel_bounds(channels, CapturedConcurrencySettings())
    # The SQLite writer thread belongs to the harness, not to the engine run;
    # it retires in the repeated idempotent-shutdown cell, so the engine's own
    # cancellation cleanup is judged on the run worker and strategy threads.
    remaining = [name for name in _remaining_owned_threads() if name != "paritygrid-sqlite-writer"]
    deadline = time.monotonic() + _THREAD_RETIREMENT_SECONDS
    while remaining and time.monotonic() < deadline:
        wake = threading.Event()
        wake.wait(min(_RETIREMENT_POLL_SLICE_SECONDS, deadline - time.monotonic()))
        remaining = [
            name for name in _remaining_owned_threads() if name != "paritygrid-sqlite-writer"
        ]
    if remaining:
        raise ResourceBoundsError(f"owned threads remained after cancellation: {remaining}")
    return {
        "terminal_status": outcome[0].value,
        "channel_high_water": high_water,
        "remaining_owned_threads": remaining,
    }


class _RecordingListener(StrategyLifecycleListener):
    """Listener double recording ordered lifecycle callbacks."""

    __slots__ = ("calls", "fail_on")

    def __init__(self, fail_on: str) -> None:
        self.calls: list[str] = []
        self.fail_on = fail_on

    def on_strategy_started(self, strategy_id: str) -> None:
        self.calls.append(f"start:{strategy_id}")
        if strategy_id == self.fail_on:
            raise RuntimeError("planned startup failure")

    def on_strategy_shutdown(self, strategy_id: str) -> None:
        self.calls.append(f"shutdown:{strategy_id}")


def _partial_startup_cell() -> dict[str, object]:
    """Prove partial startup rolls back in reverse order without leaks."""
    settings = CapturedConcurrencySettings()
    listener = _RecordingListener("threaded")
    coordinator = StrategyLifecycleCoordinator(settings, listener=listener)
    availabilities = tuple(
        availability
        for availability in describe_known_strategies(settings)
        if availability.available
    )
    started = False
    rollback_error: str | None = None
    try:
        coordinator.startup(availabilities)
        started = True
    except Exception as error:  # the exercise records the rollback, then moves on
        rollback_error = error.__class__.__name__
    if started or rollback_error is None:
        raise ResourceBoundsError("partial startup did not fail closed with a rollback")
    shutdowns = [call for call in listener.calls if call.startswith("shutdown:")]
    if not shutdowns:
        raise ResourceBoundsError("partial startup did not roll back already-started strategies")
    starts = [call for call in listener.calls if call.startswith("start:")]
    expected_shutdowns = [
        f"shutdown:{name.removeprefix('start:')}" for name in reversed(starts[:-1])
    ]
    if shutdowns != expected_shutdowns:
        raise ResourceBoundsError("startup rollback was not in reverse order")
    return {
        "rollback_error": rollback_error,
        "callback_order": listener.calls,
        "rollback_shutdowns": shutdowns,
    }


def _interruption_cell(root: Path) -> dict[str, object]:
    """Run the accepted interruption-and-restart proof in a fresh root."""
    try:
        outcome = run_interruption_proof(root, _DEFAULT_INTERRUPTION_FAILPOINT)
    except (InterruptionError, DemoEngineError) as error:
        raise ResourceBoundsError(f"the interruption proof failed: {error}") from error
    return {
        "failpoint": outcome.failpoint,
        "checks_passed": len(outcome.checks),
        "resumed_without_duplicate_effect": True,
    }


def prepare_exercise_harness(database_path: Path, artifact_root: Path) -> ConcurrentScenarioHarness:
    """Prepare one canonical engine harness inside the exercise root."""
    return prepare_concurrent_harness(database_path, artifact_root)


def _evaluate_rss_growth(
    samples: list[int | None], repetitions: int
) -> tuple[int | None, int | None, bool | None]:
    """Accept a growth result only when every configured run was observed."""
    if len(samples) != repetitions or any(sample is None for sample in samples):
        return (
            samples[0] if samples else None,
            samples[-1] if samples else None,
            None,
        )
    observed = cast("list[int]", samples)
    holds = bounded_growth_within(
        observed[0],
        observed[-1],
        max_growth_ratio=_GROWTH_RATIO,
        max_growth_bytes=_GROWTH_SLACK_BYTES,
    )
    return observed[0], observed[-1], holds


def run_resource_bounds_exercise(root: Path, *, repetitions: int = 3) -> dict[str, object]:
    """Run the bounded resource exercise and return the closed report document.

    ``root`` must be a fresh bounded directory the caller owns.  Every cell
    asserts real high-water marks against configured capacities and finishes
    with orphan detection over registered children and owned threads.  The
    shared engine harness closes through the repeated idempotent-shutdown
    cell, so the exercise itself proves its own bounded cleanup.
    """
    try:
        claimed_root = claim_fresh_root(root, prefix="paritygrid-resources-")
    except FreshRootError as error:
        raise ResourceBoundsError(str(error)) from error
    try:
        return _run_resource_bounds_in_claimed_root(claimed_root.work, repetitions=repetitions)
    finally:
        try:
            claimed_root.cleanup()
        except FreshRootError as error:
            raise ResourceBoundsError(str(error)) from error


def _run_resource_bounds_in_claimed_root(root: Path, *, repetitions: int) -> dict[str, object]:
    """Execute the resource cells inside one exclusively claimed root."""
    if type(repetitions) is not int:
        raise ResourceBoundsError("repetitions must be an integer")
    if not MIN_REPETITIONS <= repetitions <= MAX_REPETITIONS:
        raise ResourceBoundsError(
            f"repetitions must be between {MIN_REPETITIONS} and {MAX_REPETITIONS}"
        )
    settings = CapturedConcurrencySettings()
    engine_root = root / "engine"
    interruption_root = root / "interruption"
    engine_root.mkdir(parents=True, exist_ok=True)
    interruption_root.mkdir(parents=True, exist_ok=True)

    unavailable: list[dict[str, str]] = []
    peak_rss_samples: list[int | None] = []
    run_offset = _BASE_RUN_OFFSET

    harness = prepare_exercise_harness(engine_root / "engine.db", engine_root / "artifacts")
    try:
        steady_status, steady_high_water, steady_retries, steady_profile = _profiled_engine_run(
            harness, run_offset
        )
        run_offset += 1
        repeated_profiles: list[ResourceProfileResult] = []
        for _index in range(repetitions):
            _status, _bounds, _retries, profile = _profiled_engine_run(harness, run_offset)
            repeated_profiles.append(profile)
            run_offset += 1
            peak_rss_samples.append(profile.peak_rss_bytes)
            if run_offset > _MAX_RUN_OFFSET:
                raise ResourceBoundsError("engine run identities exceeded their bound")

        saturation = _saturation_cell(settings)
        cancellation = _cancellation_cell(harness, _CANCELLATION_RUN_OFFSET)

        # The shared harness closes here twice on purpose: the exercise's own
        # teardown is the repeated idempotent-shutdown evidence.
        first_close = harness.writer.close(timeout_seconds=10.0)
        second_close = harness.writer.close(timeout_seconds=10.0)
        harness.database.close()
        harness.database.close()
    except BaseException:
        harness.writer.close(timeout_seconds=10.0)
        harness.database.close()
        raise

    peak_rss_first, peak_rss_last, growth_holds = _evaluate_rss_growth(
        peak_rss_samples, repetitions
    )
    if growth_holds is not None:
        if not growth_holds:
            raise ResourceBoundsError("repeated executions showed unbounded memory growth")
    else:
        unavailable.append(
            {
                "observation": "bounded_growth",
                "reason": "resident memory was not observed for every repeated run",
            }
        )

    partial_startup = _partial_startup_cell()
    interruption = _interruption_cell(interruption_root)

    children = multiprocessing.active_children()
    owned_threads = sorted(
        thread.name
        for thread in threading.enumerate()
        if thread.name.startswith("paritygrid") and thread.is_alive()
    )
    if children:
        raise ResourceBoundsError(f"orphaned owned child processes remained: {children}")
    if owned_threads:
        raise ResourceBoundsError(f"orphaned owned threads remained: {owned_threads}")

    return {
        "format": RESOURCE_BOUNDS_REPORT_FORMAT,
        "version": RESOURCE_BOUNDS_REPORT_VERSION,
        "package_version": __version__,
        "capacities": {
            "channel": _capacity_map(settings),
            "global_concurrent_work": _DEFAULT_GLOBAL_WORK_LIMIT,
        },
        "method": {
            "bounded_growth": (
                "peak rss of the first and last repeated runs must satisfy "
                f"last <= first * {_GROWTH_RATIO} + {_GROWTH_SLACK_BYTES} bytes"
            ),
            "repetitions": repetitions,
        },
        "cells": {
            "steady_state": {
                "terminal_status": steady_status.value,
                "retry_count": steady_retries,
                "channel_high_water": steady_high_water,
                "cleanup": _cleanup_view(steady_profile),
            },
            "repeated_executions": {
                "runs": len(repeated_profiles),
                "peak_rss_first": peak_rss_first,
                "peak_rss_last": peak_rss_last,
                "bounded_growth_holds": growth_holds,
                "cleanup": _cleanup_view(repeated_profiles[-1]),
            },
            "queue_saturation_backpressure": saturation,
            "cancellation_cleanup": cancellation,
            "controlled_retry_failure": {
                "scripted_retry_enforced_on_every_engine_run": True,
                "steady_state_retry_count": steady_retries,
                "repeated_run_retry_counts_verified": True,
            },
            "partial_startup_rollback": partial_startup,
            "repeated_idempotent_shutdown": {
                "writer_first_close_drained": first_close.drained,
                "writer_second_close_drained": second_close.drained,
                "database_double_close_accepted": True,
            },
            "interruption_and_restart": interruption,
        },
        "unavailable": unavailable,
        "cleanup": {
            "zero_orphan_children": True,
            "zero_owned_threads": True,
            "owned_thread_names": [],
        },
    }


_TOP_LEVEL_KEYS = frozenset(
    {
        "format",
        "version",
        "package_version",
        "capacities",
        "method",
        "cells",
        "unavailable",
        "cleanup",
    }
)
_REQUIRED_CELLS = (
    "steady_state",
    "repeated_executions",
    "queue_saturation_backpressure",
    "cancellation_cleanup",
    "controlled_retry_failure",
    "partial_startup_rollback",
    "repeated_idempotent_shutdown",
    "interruption_and_restart",
)


class ResourceBoundsReportError(ValueError):
    """A resource-bounds report payload violated the closed contract."""


def parse_resource_bounds_report(payload: bytes) -> dict[str, object]:
    """Strictly re-parse an exercise report; any deviation fails closed.

    The consumer requires the closed top-level field set, exactly version 1,
    every exercise cell with a truthy zero-orphan cleanup record, and only
    finite non-negative numbers anywhere in the document.
    """
    try:
        document_text = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ResourceBoundsReportError("report payload must be UTF-8 bytes") from error
    try:
        parsed: object = json.loads(document_text)
    except json.JSONDecodeError as error:
        raise ResourceBoundsReportError("report payload must be valid JSON") from error
    if not isinstance(parsed, dict):
        raise ResourceBoundsReportError("report payload must be a JSON object")
    document = cast("dict[str, object]", parsed)
    if frozenset(document) != _TOP_LEVEL_KEYS:
        raise ResourceBoundsReportError("report fields must match the closed contract")
    if document["format"] != RESOURCE_BOUNDS_REPORT_FORMAT:
        raise ResourceBoundsReportError("report format name is unknown")
    version = document["version"]
    if type(version) is not int or version != RESOURCE_BOUNDS_REPORT_VERSION:
        raise ResourceBoundsReportError("report version is not supported")
    cells_value = document["cells"]
    if not isinstance(cells_value, dict):
        raise ResourceBoundsReportError("report cells must be a JSON object")
    cells = cast("dict[str, object]", cells_value)
    if frozenset(cells) != frozenset(_REQUIRED_CELLS):
        raise ResourceBoundsReportError("report cells must match the closed exercise set")
    cleanup_value = document["cleanup"]
    if not isinstance(cleanup_value, dict):
        raise ResourceBoundsReportError("report cleanup must be a JSON object")
    cleanup = cast("dict[str, object]", cleanup_value)
    if cleanup.get("zero_orphan_children") is not True:
        raise ResourceBoundsReportError("a clean report must state zero orphan children")
    if cleanup.get("zero_owned_threads") is not True:
        raise ResourceBoundsReportError("a clean report must state zero owned threads")
    _validate_resource_bounds_sections(document)
    _reject_unbounded(document)
    return document


def _resource_mapping(value: object, keys: frozenset[str], subject: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ResourceBoundsReportError(f"{subject} must be a JSON object")
    mapping = cast("dict[str, object]", value)
    if frozenset(mapping) != keys:
        raise ResourceBoundsReportError(f"{subject} fields must match the closed contract")
    return mapping


def _resource_list(value: object, subject: str) -> list[object]:
    if not isinstance(value, list):
        raise ResourceBoundsReportError(f"{subject} must be a JSON array")
    return cast("list[object]", value)


def _validate_cleanup_cell(value: object, subject: str) -> None:
    cleanup = _resource_mapping(
        value,
        frozenset(
            {
                "retired_within_deadline",
                "thread_count",
                "child_process_count",
                "peak_rss_bytes",
                "baseline_handle_count",
                "peak_handle_count",
                "cleanup_handle_count",
                "handle_cleanup_slack",
            }
        ),
        subject,
    )
    if cleanup["retired_within_deadline"] is not True or cleanup["child_process_count"] != 0:
        raise ResourceBoundsReportError(f"{subject} does not prove clean retirement")


def _validate_resource_bounds_sections(document: dict[str, object]) -> None:
    """Validate the exact nested cells that carry resource acceptance claims."""
    capacities = _resource_mapping(
        document["capacities"], frozenset({"channel", "global_concurrent_work"}), "capacities"
    )
    channel_keys = frozenset({"assignment", "result", "telemetry", "writer"})
    _resource_mapping(capacities["channel"], channel_keys, "channel capacities")
    _resource_mapping(document["method"], frozenset({"bounded_growth", "repetitions"}), "method")
    unavailable = _resource_list(document["unavailable"], "unavailable evidence")
    unavailable_observations: set[object] = set()
    for item in unavailable:
        entry = _resource_mapping(
            item, frozenset({"observation", "reason"}), "unavailable evidence entry"
        )
        unavailable_observations.add(entry["observation"])
    cells = cast("dict[str, object]", document["cells"])
    steady = _resource_mapping(
        cells["steady_state"],
        frozenset({"terminal_status", "retry_count", "channel_high_water", "cleanup"}),
        "steady-state cell",
    )
    if steady["terminal_status"] != "completed" or steady["retry_count"] != 1:
        raise ResourceBoundsReportError("steady-state cell does not prove the canonical outcome")
    _resource_mapping(steady["channel_high_water"], channel_keys, "steady channel high-water")
    _validate_cleanup_cell(steady["cleanup"], "steady cleanup")
    repeated = _resource_mapping(
        cells["repeated_executions"],
        frozenset({"runs", "peak_rss_first", "peak_rss_last", "bounded_growth_holds", "cleanup"}),
        "repeated-executions cell",
    )
    if type(repeated["runs"]) is not int or repeated["runs"] < 1:
        raise ResourceBoundsReportError("repeated-executions run count is invalid")
    if repeated["bounded_growth_holds"] is not True and not (
        repeated["bounded_growth_holds"] is None and "bounded_growth" in unavailable_observations
    ):
        raise ResourceBoundsReportError("repeated-executions growth evidence is not accepted")
    _validate_cleanup_cell(repeated["cleanup"], "repeated cleanup")
    saturation = _resource_mapping(
        cells["queue_saturation_backpressure"],
        frozenset(
            {
                "capacity",
                "accepted_before_backpressure",
                "max_observed_queued",
                "backpressure_blocked_full_send",
                "drained_after_close",
            }
        ),
        "saturation cell",
    )
    if saturation["backpressure_blocked_full_send"] is not True:
        raise ResourceBoundsReportError("saturation cell does not prove bounded backpressure")
    cancellation = _resource_mapping(
        cells["cancellation_cleanup"],
        frozenset({"terminal_status", "channel_high_water", "remaining_owned_threads"}),
        "cancellation cell",
    )
    if cancellation["terminal_status"] != "cancelled" or _resource_list(
        cancellation["remaining_owned_threads"], "remaining owned threads"
    ):
        raise ResourceBoundsReportError("cancellation cell does not prove clean cancellation")
    _resource_mapping(
        cancellation["channel_high_water"], channel_keys, "cancellation channel high-water"
    )
    retry = _resource_mapping(
        cells["controlled_retry_failure"],
        frozenset(
            {
                "scripted_retry_enforced_on_every_engine_run",
                "steady_state_retry_count",
                "repeated_run_retry_counts_verified",
            }
        ),
        "controlled-retry cell",
    )
    if (
        retry["scripted_retry_enforced_on_every_engine_run"] is not True
        or retry["steady_state_retry_count"] != 1
        or retry["repeated_run_retry_counts_verified"] is not True
    ):
        raise ResourceBoundsReportError("controlled-retry cell does not prove the scripted failure")
    rollback = _resource_mapping(
        cells["partial_startup_rollback"],
        frozenset({"rollback_error", "callback_order", "rollback_shutdowns"}),
        "startup-rollback cell",
    )
    if not _resource_list(rollback["rollback_shutdowns"], "rollback shutdowns"):
        raise ResourceBoundsReportError("startup-rollback cell has no shutdown evidence")
    _resource_list(rollback["callback_order"], "rollback callback order")
    shutdown = _resource_mapping(
        cells["repeated_idempotent_shutdown"],
        frozenset(
            {
                "writer_first_close_drained",
                "writer_second_close_drained",
                "database_double_close_accepted",
            }
        ),
        "idempotent-shutdown cell",
    )
    if any(value is not True for value in shutdown.values()):
        raise ResourceBoundsReportError("idempotent-shutdown cell is not accepted")
    interruption = _resource_mapping(
        cells["interruption_and_restart"],
        frozenset({"failpoint", "checks_passed", "resumed_without_duplicate_effect"}),
        "interruption cell",
    )
    if interruption["resumed_without_duplicate_effect"] is not True:
        raise ResourceBoundsReportError("interruption cell does not prove duplicate-effect safety")
    cleanup = _resource_mapping(
        document["cleanup"],
        frozenset({"zero_orphan_children", "zero_owned_threads", "owned_thread_names"}),
        "final cleanup",
    )
    if _resource_list(cleanup["owned_thread_names"], "final owned thread names"):
        raise ResourceBoundsReportError("final cleanup still names owned threads")


def _reject_unbounded(value: object) -> None:
    if isinstance(value, bool):
        return
    if isinstance(value, float):
        if not math.isfinite(value) or value < 0.0:
            raise ResourceBoundsReportError("report numbers must be finite and non-negative")
    elif isinstance(value, int):
        if value < 0:
            raise ResourceBoundsReportError("report numbers must be non-negative")
    elif isinstance(value, dict):
        for child in list(cast("dict[str, object]", value).values()):
            _reject_unbounded(child)
    elif isinstance(value, list):
        for child in list(cast("list[object]", value)):
            _reject_unbounded(child)


def _cleanup_view(profile: ResourceProfileResult) -> dict[str, object]:
    return {
        "retired_within_deadline": profile.cleanup.retired_within_deadline,
        "thread_count": profile.cleanup.thread_count,
        "child_process_count": profile.cleanup.child_process_count,
        "peak_rss_bytes": profile.peak_rss_bytes,
        "baseline_handle_count": profile.baseline_handle_count,
        "peak_handle_count": profile.peak_handle_count,
        "cleanup_handle_count": profile.cleanup.handle_count,
        "handle_cleanup_slack": _HANDLE_CLEANUP_SLACK,
    }


__all__ = [
    "RESOURCE_BOUNDS_REPORT_FORMAT",
    "RESOURCE_BOUNDS_REPORT_VERSION",
    "ResourceBoundsError",
    "parse_resource_bounds_report",
    "run_resource_bounds_exercise",
]
