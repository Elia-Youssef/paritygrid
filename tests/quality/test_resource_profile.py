"""Deterministic tests for the resource sampling and profiling contract."""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future
from typing import TYPE_CHECKING, cast

import pytest

from paritygrid.quality.resource_profile import (
    MAX_SERIALIZED_SAMPLES,
    MIN_INTERVAL_SECONDS,
    CapacityExceededError,
    CleanupObservation,
    ObservationId,
    ResourceCleanupError,
    ResourceProfileError,
    ResourceProfileParseError,
    ResourceProfileResult,
    ResourceSample,
    ResourceSampler,
    ResourceUnavailable,
    TracemallocHeapProbe,
    assert_within_capacity,
    bounded_growth_within,
    default_platform_probes,
    parse_resource_profile,
    profile_callable,
    resource_profile_bytes,
    resource_profile_document,
    resource_profile_summary,
    start_heap_tracing,
    stop_heap_tracing,
)

if TYPE_CHECKING:
    from multiprocessing.synchronize import Event

_WAIT_TIMEOUT_SECONDS = 10.0
_POLL_SLICE_SECONDS = 0.005
_METRIC_NULL_KEYS = ("rss_bytes", "python_heap_bytes", "handle_count", "asyncio_task_count")

_STUB_PLATFORM = "stub-platform"
_STUB_REASONS: dict[ObservationId, str] = {
    "rss_bytes": "stub probes never expose resident set size",
    "handle_count": "stub probes never expose handle count",
    "python_heap_bytes": "stub probes never expose the traced heap",
    "asyncio_task_count": "stub probes never observe asyncio tasks",
}


class _StubProbes:
    """A probe bundle that never observes anything, for honesty assertions."""

    def describe(self) -> str:
        return _STUB_PLATFORM

    def resident_set_bytes(self) -> int | None:
        return None

    def handle_count(self) -> int | None:
        return None

    def unavailable_reason(self, observation: ObservationId) -> str:
        return _STUB_REASONS[observation]


class _StubHeapProbe:
    """A heap probe that never observes anything, independent of tracemalloc."""

    def python_heap_bytes(self) -> int | None:
        return None

    def unavailable_reason(self) -> str:
        return _STUB_REASONS["python_heap_bytes"]


class _WorkloadFailureError(Exception):
    """A marker exception raised by failing test workloads."""


def _wait_until(predicate: Callable[[], bool], *, timeout: float = _WAIT_TIMEOUT_SECONDS) -> bool:
    """Poll a predicate with bounded event slices; never sleep arbitrarily."""
    wake = threading.Event()
    deadline = time.monotonic() + timeout
    while not predicate():
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            return False
        wake.wait(min(_POLL_SLICE_SECONDS, remaining))
    return True


def _hold_for(seconds: float) -> None:
    """Keep the current call stack alive for a bounded, event-sliced duration."""
    threading.Event().wait(seconds)


def _assert_unavailable_metrics_never_numeric(node: object) -> None:
    if isinstance(node, dict):
        mapping = cast("dict[str, object]", node)
        for key, item in mapping.items():
            if key in _METRIC_NULL_KEYS:
                assert item is None, f"unavailable metric {key} serialized as {item!r}"
            _assert_unavailable_metrics_never_numeric(item)
    elif isinstance(node, list):
        for item in cast("list[object]", node):
            _assert_unavailable_metrics_never_numeric(item)


def _document_keys() -> frozenset[str]:
    return frozenset(
        {
            "format",
            "version",
            "platform",
            "interval_seconds",
            "truncated",
            "sampling_overhead_note",
            "samples_total",
            "samples_serialized",
            "samples",
            "peaks",
            "baselines",
            "unavailable",
            "cleanup",
        }
    )


def _nested(document: dict[str, object], key: str) -> dict[str, object]:
    value = document[key]
    if not isinstance(value, dict):
        raise AssertionError(f"{key} should be a JSON object in the valid document")
    return cast("dict[str, object]", value)


def _sample_documents(document: dict[str, object]) -> list[dict[str, object]]:
    value = document["samples"]
    if not isinstance(value, list):
        raise AssertionError("samples should be a JSON array in the valid document")
    return [
        cast("dict[str, object]", item)
        for item in cast("list[object]", value)
        if isinstance(item, dict)
    ]


def _set(key: str, value: object) -> Callable[[dict[str, object]], None]:
    def mutator(document: dict[str, object]) -> None:
        document[key] = value

    return mutator


def _remove(key: str) -> Callable[[dict[str, object]], None]:
    def mutator(document: dict[str, object]) -> None:
        document.pop(key, None)

    return mutator


def _set_nested(outer: str, key: str, value: object) -> Callable[[dict[str, object]], None]:
    def mutator(document: dict[str, object]) -> None:
        _nested(document, outer)[key] = value

    return mutator


def _drop_sample_key(document: dict[str, object]) -> None:
    _sample_documents(document)[0].pop("thread_count")


def _set_sample_monotonic_to_nan(document: dict[str, object]) -> None:
    _sample_documents(document)[0]["monotonic_seconds"] = float("nan")


def _append_descending_sample(document: dict[str, object]) -> None:
    samples = _sample_documents(document)
    extra = dict(samples[0])
    extra["monotonic_seconds"] = 0.1
    samples.append(extra)
    document["samples"] = samples
    document["samples_total"] = len(samples)
    document["samples_serialized"] = len(samples)


def _first_unavailable_entry(document: dict[str, object]) -> dict[str, object]:
    entries = document["unavailable"]
    if not isinstance(entries, list) or not entries:
        raise AssertionError("unavailable should be a non-empty array in the valid document")
    entry = cast("list[object]", entries)[0]
    if not isinstance(entry, dict):
        raise AssertionError("unavailable entries should be JSON objects in the valid document")
    return cast("dict[str, object]", entry)


def _break_unavailable_reason(document: dict[str, object]) -> None:
    _first_unavailable_entry(document)["reason"] = "blocked/by design"


def _use_unknown_observation(document: dict[str, object]) -> None:
    _first_unavailable_entry(document)["observation"] = "threads"


_DOCUMENT_MUTATIONS: list[Callable[[dict[str, object]], None]] = [
    _set("version", 2),
    _set("version", "1"),
    _set("version", 1.0),
    _set("format", "paritygrid-other"),
    _remove("format"),
    _set("extra", 1),
    _set("truncated", "no"),
    _set("interval_seconds", 5.0),
    _set("interval_seconds", "0.02"),
    _set("platform", "stub\\platform"),
    _set("sampling_overhead_note", "cost is 5%"),
    _set("samples_total", -1),
    _set("samples_serialized", 65),
    _set("samples", {}),
    _set("samples", [1]),
    _remove("peaks"),
    _set("cleanup", []),
    _set("unavailable", {}),
    _set_nested("peaks", "rss_bytes", -5),
    _set_nested("peaks", "thread_count", False),
    _set_nested("peaks", "asyncio_task_count", "many"),
    _set_nested("cleanup", "retired_within_deadline", 1),
    _set_nested("cleanup", "thread_count", -2),
    _set_nested("cleanup", "handle_count", -1),
    _set_nested("baselines", "thread_count", 99),
    _drop_sample_key,
    _set_sample_monotonic_to_nan,
    _append_descending_sample,
    _break_unavailable_reason,
    _use_unknown_observation,
]


def _valid_document() -> dict[str, object]:
    sample = ResourceSample(
        monotonic_seconds=0.5,
        rss_bytes=None,
        python_heap_bytes=None,
        handle_count=9,
        thread_count=2,
        child_process_count=0,
        asyncio_task_count=None,
    )
    cleanup = CleanupObservation(
        thread_count=2, child_process_count=0, handle_count=8, retired_within_deadline=True
    )
    unavailable = (
        ResourceUnavailable(
            observation="rss_bytes",
            reason="resident set size is not exposed by this platform",
        ),
    )
    result = ResourceProfileResult.from_samples(
        (sample,),
        platform_label=_STUB_PLATFORM,
        unavailable=unavailable,
        cleanup=cleanup,
        interval_seconds=0.02,
        truncated=False,
    )
    return resource_profile_document(result)


def _mutated(mutator: Callable[[dict[str, object]], None]) -> bytes:
    document = _valid_document()
    mutator(document)
    return json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode(
        "ascii"
    )


def test_sampler_lifecycle_bounds_samples_and_marks_truncation() -> None:
    sampler = ResourceSampler(interval_seconds=MIN_INTERVAL_SECONDS, max_samples=6)
    sampler.start()
    try:
        assert _wait_until(lambda: len(sampler.samples()) >= 6)
    finally:
        sampler.stop()
    samples = sampler.samples()
    assert len(samples) == 6
    assert sampler.truncated() is True
    timestamps = [sample.monotonic_seconds for sample in samples]
    assert timestamps == sorted(timestamps)


def test_sampler_stop_halts_recording() -> None:
    sampler = ResourceSampler(interval_seconds=_POLL_SLICE_SECONDS)
    sampler.start()
    try:
        assert _wait_until(lambda: len(sampler.samples()) >= 2)
    finally:
        sampler.stop()
    frozen = sampler.samples()
    # A stopped sampler is given many intervals to misbehave; the count must not move.
    _hold_for(0.1)
    assert sampler.samples() == frozen
    assert sampler.truncated() is False
    with pytest.raises(ResourceProfileError, match="already been started"):
        sampler.start()
    sampler.stop()


def test_sampler_requires_start_before_stop() -> None:
    sampler = ResourceSampler()
    with pytest.raises(ResourceProfileError, match="never started"):
        sampler.stop()


@pytest.mark.parametrize(
    "interval",
    [0.0, 0.001, 2.0, 1, float("nan"), float("inf")],
)
def test_sampler_rejects_invalid_intervals(interval: float) -> None:
    with pytest.raises(ResourceProfileError, match="interval"):
        ResourceSampler(interval_seconds=interval)


@pytest.mark.parametrize(
    "max_samples",
    [1, 0, -1, 70000, True, 4096.0],
)
def test_sampler_rejects_invalid_sample_bounds(max_samples: int) -> None:
    with pytest.raises(ResourceProfileError, match="sample bound"):
        ResourceSampler(max_samples=max_samples)


def test_profile_callable_truncates_while_workload_waits() -> None:
    release = threading.Event()
    timer = threading.Timer(0.2, release.set)
    timer.start()
    try:
        value, result = profile_callable(
            lambda: int(release.wait(_WAIT_TIMEOUT_SECONDS)),
            interval_seconds=MIN_INTERVAL_SECONDS,
            max_samples=8,
        )
    finally:
        timer.join(_WAIT_TIMEOUT_SECONDS)
    assert value == 1
    assert result.truncated is True
    assert len(result.samples) == 8
    timestamps = [sample.monotonic_seconds for sample in result.samples]
    assert timestamps == sorted(timestamps)


def test_thread_peak_observed_and_cleanup_returns_to_baseline() -> None:
    interval = MIN_INTERVAL_SECONDS
    worker_count = 4
    barrier = threading.Barrier(worker_count + 1)
    hold = threading.Event()
    window = threading.Event()

    def worker() -> None:
        barrier.wait(_WAIT_TIMEOUT_SECONDS)
        hold.wait(_WAIT_TIMEOUT_SECONDS)

    def workload() -> tuple[threading.Thread, ...]:
        workers = [
            threading.Thread(target=worker, name=f"peak-worker-{index}")
            for index in range(worker_count)
        ]
        for thread in workers:
            thread.start()
        # The barrier proves all workers were alive at the same instant.
        barrier.wait(_WAIT_TIMEOUT_SECONDS)
        # The window stays open across many sampler intervals so a sample
        # provably lands inside the all-alive span before the workers exit.
        window.wait(interval * 25.0)
        hold.set()
        for thread in workers:
            thread.join(_WAIT_TIMEOUT_SECONDS)
        return tuple(workers)

    baseline_threads = threading.active_count()
    workers, result = profile_callable(workload, interval_seconds=interval)
    assert all(not thread.is_alive() for thread in workers)
    peak = result.peak_thread_count
    assert peak is not None
    assert result.baseline_thread_count is not None
    # The sampler's peak readings include its own thread; the cleanup reading is
    # taken after the sampler stopped and the workers joined. Unrelated
    # interpreter threads may exit mid-run (observed under pytest), so the
    # stable, churn-immune invariant is this delta: K workers plus the sampler.
    assert peak >= result.cleanup.thread_count + worker_count + 1
    assert result.cleanup.thread_count <= baseline_threads
    assert result.cleanup.retired_within_deadline is True


def _spawn_available() -> bool:
    try:
        multiprocessing.get_context("spawn")
    except ValueError:
        return False
    return True


def _child_holds(release: Event) -> None:
    release.wait(30.0)


@pytest.mark.skipif(not _spawn_available(), reason="spawn start method is unavailable")
def test_child_process_peak_observed_and_cleanup_returns_to_baseline() -> None:
    context = multiprocessing.get_context("spawn")
    interval = 0.01
    release = context.Event()

    def workload() -> int:
        child = context.Process(
            target=_child_holds, args=(release,), name="resource-profile-child", daemon=True
        )
        child.start()
        assert _wait_until(child.is_alive)
        # Keep the child alive across many sampler intervals so at least one
        # sample provably observes it before the workload terminates it.
        _hold_for(interval * 40.0)
        release.set()
        child.join(_WAIT_TIMEOUT_SECONDS)
        return child.exitcode if child.exitcode is not None else -1

    value, result = profile_callable(workload, interval_seconds=interval)
    assert value == 0
    assert result.peak_child_process_count is not None
    assert result.peak_child_process_count >= 1
    assert result.cleanup.child_process_count == 0
    assert result.cleanup.retired_within_deadline is True


def test_unavailable_metrics_are_reported_honestly() -> None:
    interval = MIN_INTERVAL_SECONDS

    def workload() -> int:
        _hold_for(interval * 10.0)
        return 7

    value, result = profile_callable(
        workload,
        interval_seconds=interval,
        probes=_StubProbes(),
        heap_probe=_StubHeapProbe(),
    )
    assert value == 7
    assert result.peak_rss_bytes is None
    assert result.peak_handle_count is None
    assert result.peak_python_heap_bytes is None
    reasons = {entry.observation: entry.reason for entry in result.unavailable}
    assert reasons["rss_bytes"] == _STUB_REASONS["rss_bytes"]
    assert reasons["handle_count"] == _STUB_REASONS["handle_count"]
    assert reasons["python_heap_bytes"] == _STUB_REASONS["python_heap_bytes"]
    assert reasons["asyncio_task_count"] == (
        "no event loop reference was supplied for asyncio task sampling"
    )
    document = resource_profile_document(result)
    _assert_unavailable_metrics_never_numeric(document)


def test_default_probes_match_this_platform() -> None:
    probes = default_platform_probes()
    label = probes.describe()
    rss = probes.resident_set_bytes()
    handles = probes.handle_count()
    if label == "windows":
        assert isinstance(rss, int)
        assert rss > 0
        assert isinstance(handles, int)
        assert handles > 0
    elif label == "linux":
        assert isinstance(rss, int)
        assert rss > 0
        assert handles is None
        assert "handle count" in probes.unavailable_reason("handle_count")
    else:
        assert rss is None
        assert handles is None
        assert len(probes.unavailable_reason("rss_bytes")) > 0
        assert len(probes.unavailable_reason("handle_count")) > 0


def test_assert_within_capacity_passes_at_capacity() -> None:
    assert_within_capacity(5, 5, "thread high-water")


def test_assert_within_capacity_raises_past_capacity() -> None:
    with pytest.raises(CapacityExceededError, match="thread high-water"):
        assert_within_capacity(6, 5, "thread high-water")


@pytest.mark.parametrize(
    ("observed_peak", "capacity"),
    [(-1, 5), (True, 5), (5, -1), (5, False)],
)
def test_assert_within_capacity_rejects_bad_inputs(observed_peak: int, capacity: int) -> None:
    with pytest.raises(ResourceProfileError, match="non-negative integer"):
        assert_within_capacity(observed_peak, capacity, "label")


def test_assert_within_capacity_rejects_bad_label() -> None:
    with pytest.raises(ResourceProfileError, match="reason text"):
        assert_within_capacity(1, 1, "")


def test_bounded_growth_within_boundaries() -> None:
    assert bounded_growth_within(100, 100, max_growth_ratio=1.5, max_growth_bytes=0) is True
    assert bounded_growth_within(100, 150, max_growth_ratio=1.5, max_growth_bytes=0) is True
    assert bounded_growth_within(100, 151, max_growth_ratio=1.5, max_growth_bytes=0) is False
    assert bounded_growth_within(100, 155, max_growth_ratio=1.5, max_growth_bytes=5) is True
    assert bounded_growth_within(100, 156, max_growth_ratio=1.5, max_growth_bytes=5) is False


def test_bounded_growth_within_rejects_missing_peaks() -> None:
    with pytest.raises(ResourceProfileError, match="both runs"):
        bounded_growth_within(None, 10, max_growth_ratio=1.5, max_growth_bytes=0)
    with pytest.raises(ResourceProfileError, match="both runs"):
        bounded_growth_within(10, None, max_growth_ratio=1.5, max_growth_bytes=0)


def test_bounded_growth_within_rejects_bad_bounds() -> None:
    with pytest.raises(ResourceProfileError, match=r"at least 1\.0"):
        bounded_growth_within(100, 100, max_growth_ratio=0.9, max_growth_bytes=0)
    with pytest.raises(ResourceProfileError, match="finite float"):
        bounded_growth_within(100, 100, max_growth_ratio=True, max_growth_bytes=0)
    with pytest.raises(ResourceProfileError, match="non-negative"):
        bounded_growth_within(100, 100, max_growth_ratio=1.5, max_growth_bytes=-1)
    with pytest.raises(ResourceProfileError, match="non-negative"):
        bounded_growth_within(-100, 100, max_growth_ratio=1.5, max_growth_bytes=0)


def test_serialization_round_trip_and_hygiene() -> None:
    interval = 0.02

    def workload() -> int:
        _hold_for(interval * 6.0)
        return 11

    value, result = profile_callable(workload, interval_seconds=interval)
    assert value == 11
    payload = resource_profile_bytes(result)
    assert payload == resource_profile_bytes(result)
    summary = parse_resource_profile(payload)
    assert summary == resource_profile_summary(result)
    text = payload.decode("ascii")
    assert "\\" not in text
    document = resource_profile_document(result)
    assert frozenset(document) == _document_keys()
    assert document["format"] == "paritygrid-resource-profile"
    assert document["version"] == 1
    assert document["platform"] == result.platform_label


def test_serialization_bounds_the_sample_list() -> None:
    samples = tuple(
        ResourceSample(
            monotonic_seconds=float(index) * 0.01,
            rss_bytes=index,
            python_heap_bytes=None,
            handle_count=index + 1,
            thread_count=2,
            child_process_count=0,
            asyncio_task_count=None,
        )
        for index in range(70)
    )
    result = ResourceProfileResult.from_samples(
        samples,
        platform_label=_STUB_PLATFORM,
        unavailable=(),
        cleanup=CleanupObservation(
            thread_count=2, child_process_count=0, handle_count=70, retired_within_deadline=True
        ),
        interval_seconds=0.01,
        truncated=True,
    )
    document = resource_profile_document(result)
    assert document["samples_total"] == 70
    assert document["samples_serialized"] == MAX_SERIALIZED_SAMPLES
    serialized = document["samples"]
    assert isinstance(serialized, list)
    assert len(cast("list[object]", serialized)) == MAX_SERIALIZED_SAMPLES
    summary = parse_resource_profile(resource_profile_bytes(result))
    assert summary == resource_profile_summary(result)


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"{",
        b"not json",
        b"[]",
        b'{"format": "paritygrid-resource-profile"}',
        b"\xff\xfe",
    ],
)
def test_parse_rejects_malformed_payloads(payload: bytes) -> None:
    with pytest.raises(ResourceProfileParseError, match="invalid resource profile"):
        parse_resource_profile(payload)


@pytest.mark.parametrize("mutator", _DOCUMENT_MUTATIONS)
def test_parse_rejects_document_mutations(
    mutator: Callable[[dict[str, object]], None],
) -> None:
    with pytest.raises(ResourceProfileParseError, match="invalid resource profile"):
        parse_resource_profile(_mutated(mutator))


def test_parse_accepts_the_valid_document() -> None:
    summary = parse_resource_profile(
        json.dumps(
            _valid_document(), ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ).encode("ascii")
    )
    assert summary.format_name == "paritygrid-resource-profile"
    assert summary.format_version == 1
    assert summary.platform == _STUB_PLATFORM


def test_heap_probe_reports_only_when_tracing() -> None:
    probe = TracemallocHeapProbe()
    stop_heap_tracing()
    assert probe.python_heap_bytes() is None
    assert probe.unavailable_reason() == "tracemalloc is not tracing"
    start_heap_tracing()
    try:
        before = probe.python_heap_bytes()
        assert before is not None
        assert before >= 0
        held = [bytearray(4096) for _ in range(16)]
        during = probe.python_heap_bytes()
        assert during is not None
        assert during > before
        del held
    finally:
        stop_heap_tracing()
    assert probe.python_heap_bytes() is None


def test_sampler_records_heap_bytes_while_tracing() -> None:
    start_heap_tracing()
    try:
        _, result = profile_callable(
            lambda: _hold_for(0.06),
            interval_seconds=0.01,
            heap_probe=TracemallocHeapProbe(),
        )
    finally:
        stop_heap_tracing()
    assert result.samples
    assert all(sample.python_heap_bytes is not None for sample in result.samples)


def test_workload_failure_propagates_and_sampler_stops() -> None:
    baseline = threading.active_count()

    def workload() -> None:
        raise _WorkloadFailureError("workload failed")

    with pytest.raises(_WorkloadFailureError, match="workload failed"):
        profile_callable(workload, interval_seconds=_POLL_SLICE_SECONDS)
    assert _wait_until(lambda: threading.active_count() <= baseline, timeout=5.0)


def test_failed_retirement_raises_cleanup_error_preserving_cause() -> None:
    baseline = threading.active_count()
    finished = threading.Event()

    def lingering() -> None:
        finished.wait(5.0)

    def workload() -> None:
        threading.Thread(target=lingering, name="lingering-worker", daemon=True).start()
        raise _WorkloadFailureError("workload failed")

    with pytest.raises(ResourceCleanupError, match="did not retire") as captured:
        profile_callable(
            workload, interval_seconds=_POLL_SLICE_SECONDS, cleanup_deadline_seconds=0.05
        )
    assert isinstance(captured.value.__cause__, _WorkloadFailureError)
    assert finished.set() is None
    assert _wait_until(lambda: threading.active_count() <= baseline, timeout=5.0)


def test_asyncio_task_counts_come_from_the_observed_loop() -> None:
    interval = MIN_INTERVAL_SECONDS
    baseline = threading.active_count()
    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=loop.run_forever, name="observed-loop", daemon=True)
    loop_thread.start()
    futures: list[Future[object]] = []

    async def parked() -> None:
        await asyncio.sleep(30.0)

    try:
        assert _wait_until(loop.is_running)
        futures = [asyncio.run_coroutine_threadsafe(parked(), loop) for _ in range(3)]
        assert _wait_until(lambda: len(asyncio.all_tasks(loop)) >= 3)

        def workload() -> int:
            _hold_for(interval * 30.0)
            return 5

        value, result = profile_callable(workload, interval_seconds=interval, loop=loop)
        assert value == 5
        peak = result.peak_asyncio_task_count
        assert peak is not None
        assert peak >= 3
        assert result.cleanup.retired_within_deadline is True
        assert any(
            sample.asyncio_task_count is not None and sample.asyncio_task_count >= 3
            for sample in result.samples
        )
    finally:
        for future in futures:
            future.cancel()
        # Cancellation of run_coroutine_threadsafe futures is delivered onto
        # the observed loop.  Cross the loop once before stopping it so every
        # parked task processes that cancellation instead of being destroyed
        # while pending during loop.close().
        drained = asyncio.run_coroutine_threadsafe(asyncio.sleep(0), loop)
        drained.result(_WAIT_TIMEOUT_SECONDS)
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(_WAIT_TIMEOUT_SECONDS)
        loop.close()
    assert _wait_until(lambda: threading.active_count() <= baseline, timeout=5.0)


def test_sampler_thread_returns_to_baseline_after_profile() -> None:
    baseline = threading.active_count()
    _, result = profile_callable(lambda: 1, interval_seconds=MIN_INTERVAL_SECONDS)
    assert result.cleanup.thread_count <= baseline
    assert result.cleanup.retired_within_deadline is True
    assert _wait_until(lambda: threading.active_count() <= baseline, timeout=5.0)
