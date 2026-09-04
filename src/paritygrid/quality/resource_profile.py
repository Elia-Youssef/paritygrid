"""Bounded deterministic resource sampling with a versioned report contract.

The sampler observes real process owners -- resident memory, OS handle
count, threads, child processes, traced Python heap, and asyncio tasks --
while a workload runs exactly once. A counter the platform cannot observe
is recorded as structurally unavailable with a bounded reason, never as a
numeric zero. Reports encode to canonical deterministic JSON, and a strict
mirrored consumer re-parses them fail-closed.
"""

from __future__ import annotations

import asyncio
import json
import math
import multiprocessing
import os
import sys
import threading
import time
import tracemalloc
from collections.abc import Callable
from dataclasses import dataclass
from typing import ClassVar, Literal, NoReturn, Protocol, cast

if sys.platform == "win32":
    import ctypes
    from ctypes import wintypes

RESOURCE_PROFILE_FORMAT = "paritygrid-resource-profile"
RESOURCE_PROFILE_VERSION = 1

MIN_INTERVAL_SECONDS = 0.005
MAX_INTERVAL_SECONDS = 1.0
DEFAULT_INTERVAL_SECONDS = 0.02
MIN_MAX_SAMPLES = 2
MAX_MAX_SAMPLES = 65536
DEFAULT_MAX_SAMPLES = 4096
MAX_REASON_LENGTH = 160
MAX_PLATFORM_LABEL_LENGTH = 40
MAX_SERIALIZED_SAMPLES = 64
DEFAULT_CLEANUP_DEADLINE_SECONDS = 10.0
MAX_CLEANUP_DEADLINE_SECONDS = 3600.0

# Fixed, honest statement about sampler cost serialized into every report.
SAMPLING_OVERHEAD_NOTE = (
    "Sampling runs as a background daemon thread that reads per-process OS APIs at the "
    "configured interval and adds a small platform-dependent overhead."
)

# The cleanup poll wakes on this slice so stop requests are never unbounded.
_CLEANUP_POLL_SLICE_SECONDS = 0.05
# The sampler loop ticks at most one interval late, so this join timeout is generous.
_SAMPLER_JOIN_TIMEOUT_SECONDS = 5.0

type ObservationId = Literal["rss_bytes", "python_heap_bytes", "handle_count", "asyncio_task_count"]

_OBSERVATION_IDS: tuple[ObservationId, ...] = (
    "rss_bytes",
    "python_heap_bytes",
    "handle_count",
    "asyncio_task_count",
)
_OBSERVATION_SET = frozenset(_OBSERVATION_IDS)

_PRINTABLE_ASCII = frozenset(chr(code) for code in range(0x20, 0x7F))
_FORBIDDEN_REASON_CHARACTERS = frozenset("/\\%")


class ResourceProfileError(ValueError):
    """Raised when a sampler bound, peak comparison, or report field is invalid."""


class ResourceProfileParseError(ResourceProfileError):
    """Raised when a resource profile payload fails the strict mirrored contract."""


class CapacityExceededError(ResourceProfileError):
    """Raised when an observed peak exceeds its configured capacity."""


class ResourceCleanupError(RuntimeError):
    """Raised when process resources fail to return to their pre-workload baseline."""


def _validate_reason_text(value: str) -> str:
    if type(value) is not str:
        raise ResourceProfileError("reason text must be a string")
    if not 1 <= len(value) <= MAX_REASON_LENGTH:
        raise ResourceProfileError(f"reason text must be 1..{MAX_REASON_LENGTH} characters")
    if any(character not in _PRINTABLE_ASCII for character in value):
        raise ResourceProfileError("reason text must be printable ASCII")
    if any(character in _FORBIDDEN_REASON_CHARACTERS for character in value):
        raise ResourceProfileError("reason text must not contain '/', '\\', or '%'")
    if value != value.strip():
        raise ResourceProfileError("reason text must not have leading or trailing whitespace")
    return value


def _validate_platform_label(value: str) -> str:
    if type(value) is not str:
        raise ResourceProfileError("platform label must be a string")
    if not 1 <= len(value) <= MAX_PLATFORM_LABEL_LENGTH:
        raise ResourceProfileError(
            f"platform label must be 1..{MAX_PLATFORM_LABEL_LENGTH} characters"
        )
    if any(character not in _PRINTABLE_ASCII for character in value):
        raise ResourceProfileError("platform label must be printable ASCII")
    if any(character in _FORBIDDEN_REASON_CHARACTERS for character in value):
        raise ResourceProfileError("platform label must not contain '/', '\\', or '%'")
    if value != value.strip():
        raise ResourceProfileError("platform label must not have leading or trailing whitespace")
    return value


def _validate_interval_seconds(value: float) -> float:
    if type(value) is not float:
        raise ResourceProfileError("sampler interval must be a float")
    if not math.isfinite(value) or not MIN_INTERVAL_SECONDS <= value <= MAX_INTERVAL_SECONDS:
        raise ResourceProfileError(
            f"sampler interval must be a finite float between "
            f"{MIN_INTERVAL_SECONDS} and {MAX_INTERVAL_SECONDS} seconds"
        )
    return value


def _validate_max_samples(value: int) -> int:
    if type(value) is not int:
        raise ResourceProfileError("sample bound must be an integer")
    if not MIN_MAX_SAMPLES <= value <= MAX_MAX_SAMPLES:
        raise ResourceProfileError(
            f"sample bound must be between {MIN_MAX_SAMPLES} and {MAX_MAX_SAMPLES}"
        )
    return value


def _validate_cleanup_deadline(value: float) -> float:
    if type(value) is not float:
        raise ResourceProfileError("cleanup deadline must be a float")
    if not math.isfinite(value) or not 0.0 < value <= MAX_CLEANUP_DEADLINE_SECONDS:
        raise ResourceProfileError(
            f"cleanup deadline must be a finite float between 0 and {MAX_CLEANUP_DEADLINE_SECONDS} "
            "seconds"
        )
    return value


def _validate_non_negative_int(value: int, field: str) -> int:
    if type(value) is not int or value < 0:
        raise ResourceProfileError(f"{field} must be a non-negative integer")
    return value


def _validate_metric(value: int | None) -> int | None:
    if value is None:
        return None
    return _validate_non_negative_int(value, "metric readings")


@dataclass(frozen=True, slots=True)
class ResourceSample:
    """One instantaneous observation of this process's resource owners."""

    monotonic_seconds: float
    rss_bytes: int | None
    python_heap_bytes: int | None
    handle_count: int | None
    thread_count: int
    child_process_count: int
    asyncio_task_count: int | None

    def __post_init__(self) -> None:
        if type(self.monotonic_seconds) is not float or not math.isfinite(self.monotonic_seconds):
            raise ResourceProfileError("sample timestamps must be finite floats")
        if self.monotonic_seconds < 0.0:
            raise ResourceProfileError("sample timestamps must be non-negative")
        _validate_metric(self.rss_bytes)
        _validate_metric(self.python_heap_bytes)
        _validate_metric(self.handle_count)
        _validate_metric(self.asyncio_task_count)
        _validate_non_negative_int(self.thread_count, "thread_count")
        _validate_non_negative_int(self.child_process_count, "child_process_count")


@dataclass(frozen=True, slots=True)
class ResourceUnavailable:
    """One structured explanation for a metric this platform cannot observe."""

    observation: ObservationId
    reason: str

    def __post_init__(self) -> None:
        if self.observation not in _OBSERVATION_SET:
            raise ResourceProfileError("observation must be a known observation id")
        _validate_reason_text(self.reason)


@dataclass(frozen=True, slots=True)
class CleanupObservation:
    """Post-workload final facts about thread and child-process retirement."""

    thread_count: int
    child_process_count: int
    handle_count: int | None
    retired_within_deadline: bool

    def __post_init__(self) -> None:
        _validate_non_negative_int(self.thread_count, "thread_count")
        _validate_non_negative_int(self.child_process_count, "child_process_count")
        _validate_metric(self.handle_count)
        if type(self.retired_within_deadline) is not bool:
            raise ResourceProfileError("retired_within_deadline must be a boolean")


@dataclass(frozen=True, slots=True)
class MetricExtrema:
    """Per-metric extrema where None means the metric was never observed."""

    rss_bytes: int | None
    python_heap_bytes: int | None
    handle_count: int | None
    thread_count: int | None
    child_process_count: int | None
    asyncio_task_count: int | None


@dataclass(frozen=True, slots=True)
class ResourceProfileResult:
    """The full observation of one workload run, bounded and self-describing."""

    samples: tuple[ResourceSample, ...]
    peak_rss_bytes: int | None
    peak_python_heap_bytes: int | None
    peak_handle_count: int | None
    peak_thread_count: int | None
    peak_child_process_count: int | None
    peak_asyncio_task_count: int | None
    baseline_rss_bytes: int | None
    baseline_python_heap_bytes: int | None
    baseline_handle_count: int | None
    baseline_thread_count: int | None
    baseline_child_process_count: int | None
    baseline_asyncio_task_count: int | None
    platform_label: str
    unavailable: tuple[ResourceUnavailable, ...]
    cleanup: CleanupObservation
    interval_seconds: float
    truncated: bool
    sampling_overhead_note: str

    def __post_init__(self) -> None:
        if len(self.samples) > MAX_MAX_SAMPLES:
            raise ResourceProfileError(f"results carry at most {MAX_MAX_SAMPLES} samples")
        _validate_interval_seconds(self.interval_seconds)
        _validate_platform_label(self.platform_label)
        _validate_reason_text(self.sampling_overhead_note)

    @classmethod
    def from_samples(
        cls,
        samples: tuple[ResourceSample, ...],
        *,
        platform_label: str,
        unavailable: tuple[ResourceUnavailable, ...],
        cleanup: CleanupObservation,
        interval_seconds: float,
        truncated: bool,
    ) -> ResourceProfileResult:
        """Derive peaks and baselines from samples; extrema stay None if unobserved."""
        first = samples[0] if samples else None
        return cls(
            samples=tuple(samples),
            peak_rss_bytes=_peak_of(samples, lambda sample: sample.rss_bytes),
            peak_python_heap_bytes=_peak_of(samples, lambda sample: sample.python_heap_bytes),
            peak_handle_count=_peak_of(samples, lambda sample: sample.handle_count),
            peak_thread_count=_peak_of(samples, lambda sample: sample.thread_count),
            peak_child_process_count=_peak_of(samples, lambda sample: sample.child_process_count),
            peak_asyncio_task_count=_peak_of(samples, lambda sample: sample.asyncio_task_count),
            baseline_rss_bytes=first.rss_bytes if first is not None else None,
            baseline_python_heap_bytes=first.python_heap_bytes if first is not None else None,
            baseline_handle_count=first.handle_count if first is not None else None,
            baseline_thread_count=first.thread_count if first is not None else None,
            baseline_child_process_count=first.child_process_count if first is not None else None,
            baseline_asyncio_task_count=first.asyncio_task_count if first is not None else None,
            platform_label=platform_label,
            unavailable=tuple(unavailable),
            cleanup=cleanup,
            interval_seconds=interval_seconds,
            truncated=truncated,
            sampling_overhead_note=SAMPLING_OVERHEAD_NOTE,
        )


def _peak_of(
    samples: tuple[ResourceSample, ...], reader: Callable[[ResourceSample], int | None]
) -> int | None:
    observed = [reading for sample in samples if (reading := reader(sample)) is not None]
    return max(observed) if observed else None


class PlatformProbes(Protocol):
    """Injectable per-platform counters that degrade to None with a reason."""

    def describe(self) -> str:
        """Return the bounded platform label serialized into reports."""
        ...

    def resident_set_bytes(self) -> int | None:
        """Return the working set size, or None when the platform cannot observe it."""
        ...

    def handle_count(self) -> int | None:
        """Return the OS handle count, or None when the platform cannot observe it."""
        ...

    def unavailable_reason(self, observation: ObservationId) -> str:
        """Return the bounded reason recorded when an observation stays None."""
        ...


class PythonHeapProbe(Protocol):
    """Injectable traced-heap counter backed by tracemalloc in production."""

    def python_heap_bytes(self) -> int | None:
        """Return the traced byte total, or None when heap tracing is inactive."""
        ...

    def unavailable_reason(self) -> str:
        """Return the bounded reason recorded when the heap reading stays None."""
        ...


class _ProbeSupport:
    """Shared reason dispatch so every probe bundle answers all observation ids."""

    _reasons: ClassVar[dict[ObservationId, str]] = {}

    def unavailable_reason(self, observation: ObservationId) -> str:
        reason = self._reasons.get(observation)
        if reason is None:
            return "observation is not available on this platform"
        return reason


if sys.platform == "win32":

    class _ProcessMemoryCounters(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
        ]

    class _WindowsPlatformProbes(_ProbeSupport):
        """Reads per-process working set and handle counters via psapi or kernel32."""

        _reasons: ClassVar[dict[ObservationId, str]] = {
            "rss_bytes": "the working set size query failed on this system",
            "handle_count": "the process handle count query failed on this system",
            "python_heap_bytes": "tracemalloc is not tracing",
            "asyncio_task_count": "no event loop reference was supplied for asyncio task sampling",
        }

        def __init__(self) -> None:
            self._query_memory: Callable[[int, object, int], int] | None = None
            self._query_handles: Callable[[int, object], int] | None = None
            self._current_process: Callable[[], int | None] | None = None
            try:
                query_memory = ctypes.windll.psapi.GetProcessMemoryInfo
            except AttributeError, OSError:
                query_memory = None
            if query_memory is None:
                try:
                    query_memory = ctypes.windll.kernel32.K32GetProcessMemoryInfo
                except AttributeError, OSError:
                    query_memory = None
            if query_memory is not None:
                # Pin the exact signature so every later call stays a typed BOOL query.
                query_memory.argtypes = (
                    wintypes.HANDLE,
                    ctypes.POINTER(_ProcessMemoryCounters),
                    wintypes.DWORD,
                )
                query_memory.restype = wintypes.BOOL
                self._query_memory = query_memory
            try:
                query_handles = ctypes.windll.kernel32.GetProcessHandleCount
            except AttributeError, OSError:
                query_handles = None
            if query_handles is not None:
                query_handles.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
                query_handles.restype = wintypes.BOOL
                self._query_handles = query_handles
            try:
                current_process = ctypes.windll.kernel32.GetCurrentProcess
            except AttributeError, OSError:
                current_process = None
            if current_process is not None:
                current_process.argtypes = ()
                # A void-pointer restype yields int | None, so callers guard the handle.
                current_process.restype = wintypes.HANDLE
                self._current_process = current_process

        def describe(self) -> str:
            return "windows"

        def resident_set_bytes(self) -> int | None:
            query = self._query_memory
            current_process = self._current_process
            if query is None or current_process is None:
                return None
            try:
                handle = current_process()
                if handle is None:
                    return None
                counters = _ProcessMemoryCounters()
                counters.cb = ctypes.sizeof(_ProcessMemoryCounters)
                if not query(handle, ctypes.byref(counters), ctypes.sizeof(counters)):
                    return None
                return int(counters.WorkingSetSize)
            except Exception:  # probe failures degrade; sampling must never raise
                return None

        def handle_count(self) -> int | None:
            query = self._query_handles
            current_process = self._current_process
            if query is None or current_process is None:
                return None
            try:
                handle = current_process()
                if handle is None:
                    return None
                received = wintypes.DWORD(0)
                if not query(handle, ctypes.byref(received)):
                    return None
                return int(received.value)
            except Exception:  # probe failures degrade; sampling must never raise
                return None


class _LinuxPlatformProbes(_ProbeSupport):
    """Reads resident pages from procfs; handle count has no procfs equivalent."""

    _reasons: ClassVar[dict[ObservationId, str]] = {
        "rss_bytes": "resident set size could not be read from procfs",
        "handle_count": "handle count is not exposed by this platform",
        "python_heap_bytes": "tracemalloc is not tracing",
        "asyncio_task_count": "no event loop reference was supplied for asyncio task sampling",
    }

    def describe(self) -> str:
        return "linux"

    def resident_set_bytes(self) -> int | None:
        fields = _read_statm_fields()
        page_size = _read_page_size()
        if fields is None or len(fields) < 2 or page_size is None:
            return None
        try:
            resident_pages = int(fields[1])
        except ValueError:
            return None
        return resident_pages * page_size

    def handle_count(self) -> int | None:
        return None


def _read_statm_fields() -> list[str] | None:
    try:
        with open("/proc/self/statm", encoding="ascii") as stream:
            return stream.read().split()
    except OSError:
        return None


def _read_page_size() -> int | None:
    # os.sysconf is POSIX-only, so it is reached reflectively to keep this
    # module importable and type-checkable on every supported platform.
    sysconf = cast("Callable[[str], int] | None", getattr(os, "sysconf", None))
    if sysconf is None:
        return None
    try:
        return int(sysconf("SC_PAGE_SIZE"))
    except AttributeError, OSError, ValueError:
        return None


class _UnsupportedPlatformProbes(_ProbeSupport):
    """Records every OS counter as unavailable on platforms without a reader."""

    _reasons: ClassVar[dict[ObservationId, str]] = {
        "rss_bytes": "resident set size is not exposed by this platform",
        "handle_count": "handle count is not exposed by this platform",
        "python_heap_bytes": "tracemalloc is not tracing",
        "asyncio_task_count": "no event loop reference was supplied for asyncio task sampling",
    }

    def __init__(self, raw_platform: str) -> None:
        sanitized = "".join(
            character
            for character in raw_platform
            if character in _PRINTABLE_ASCII and character not in _FORBIDDEN_REASON_CHARACTERS
        ).strip()[:MAX_PLATFORM_LABEL_LENGTH]
        self._label = _validate_platform_label(sanitized or "unknown")

    def describe(self) -> str:
        return self._label

    def resident_set_bytes(self) -> int | None:
        return None

    def handle_count(self) -> int | None:
        return None


def default_platform_probes() -> PlatformProbes:
    """Build the production probe bundle for the current operating system."""
    if sys.platform == "win32":
        return _WindowsPlatformProbes()
    if sys.platform.startswith("linux"):
        return _LinuxPlatformProbes()
    return _UnsupportedPlatformProbes(sys.platform)


class TracemallocHeapProbe:
    """Samples the traced Python heap; untraced runs report None, never zero."""

    def python_heap_bytes(self) -> int | None:
        if not tracemalloc.is_tracing():
            return None
        current, _peak = tracemalloc.get_traced_memory()
        return int(current)

    def unavailable_reason(self) -> str:
        return "tracemalloc is not tracing"


def start_heap_tracing() -> None:
    """Start tracemalloc if it is not already tracing, idempotently."""
    if not tracemalloc.is_tracing():
        tracemalloc.start()


def stop_heap_tracing() -> None:
    """Stop tracemalloc if it is tracing, idempotently."""
    if tracemalloc.is_tracing():
        tracemalloc.stop()


class ResourceSampler:
    """One daemon thread that records bounded ResourceSample values until stopped.

    A sampler instance runs a single start/stop cycle. Each tick reads the
    injected probe bundle, ``threading.active_count()``, child-process count,
    and, when a running event loop reference was supplied, the tasks scheduled
    on that loop. The tick sleeps on a stop event so ``stop`` always interrupts
    within one interval, and sampling halts permanently at the sample bound
    while recording that the bound was reached.
    """

    def __init__(
        self,
        *,
        probes: PlatformProbes | None = None,
        heap_probe: PythonHeapProbe | None = None,
        interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
        max_samples: int = DEFAULT_MAX_SAMPLES,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._probes = probes if probes is not None else default_platform_probes()
        self._heap_probe = heap_probe if heap_probe is not None else TracemallocHeapProbe()
        self._interval = _validate_interval_seconds(interval_seconds)
        self._max_samples = _validate_max_samples(max_samples)
        self._loop = loop
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._samples: list[ResourceSample] = []
        self._reasons: dict[ObservationId, str] = {}
        self._truncated = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the sampling thread; each sampler instance starts exactly once."""
        with self._lock:
            if self._thread is not None:
                raise ResourceProfileError("resource sampler has already been started")
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run, name="paritygrid-resource-sampler", daemon=True
            )
        self._thread.start()

    def stop(self) -> None:
        """Signal stop, join the thread, and fail typed if the thread will not exit."""
        thread = self._thread
        if thread is None:
            raise ResourceProfileError("resource sampler was never started")
        self._stop_event.set()
        thread.join(timeout=_SAMPLER_JOIN_TIMEOUT_SECONDS)
        if thread.is_alive():
            raise ResourceCleanupError("resource sampler thread did not stop before join timeout")

    def samples(self) -> tuple[ResourceSample, ...]:
        """Return a consistent snapshot of the samples recorded so far."""
        with self._lock:
            return tuple(self._samples)

    def truncated(self) -> bool:
        """Return whether sampling halted because the sample bound was reached."""
        with self._lock:
            return self._truncated

    def unavailable_reasons(self) -> tuple[ResourceUnavailable, ...]:
        """Return one structured reason per observation that stayed unavailable."""
        with self._lock:
            return tuple(
                ResourceUnavailable(observation=observation, reason=self._reasons[observation])
                for observation in _OBSERVATION_IDS
                if observation in self._reasons
            )

    def take_final_sample(self) -> ResourceSample:
        """Observe once more after the sampler stopped, for the cleanup record."""
        with self._lock:
            if self._thread is None:
                raise ResourceProfileError("resource sampler was never started")
            if self._thread.is_alive():
                raise ResourceProfileError("final sample requires the sampler to be stopped")
        return self._observe()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            sample = self._observe()
            with self._lock:
                if len(self._samples) >= self._max_samples:
                    self._truncated = True
                    return
                self._samples.append(sample)
                reached_bound = len(self._samples) >= self._max_samples
            if reached_bound:
                # The bound was reached while the workload was still running.
                self._truncated = True
                return
            self._stop_event.wait(self._interval)

    def _observe(self) -> ResourceSample:
        timestamp = time.perf_counter()
        return ResourceSample(
            monotonic_seconds=timestamp,
            rss_bytes=self._read_metric(
                "rss_bytes",
                self._probes.resident_set_bytes,
                lambda: self._probes.unavailable_reason("rss_bytes"),
            ),
            python_heap_bytes=self._read_metric(
                "python_heap_bytes",
                self._heap_probe.python_heap_bytes,
                self._heap_probe.unavailable_reason,
            ),
            handle_count=self._read_metric(
                "handle_count",
                self._probes.handle_count,
                lambda: self._probes.unavailable_reason("handle_count"),
            ),
            thread_count=threading.active_count(),
            child_process_count=len(multiprocessing.active_children()),
            asyncio_task_count=self._loop_task_count(),
        )

    def _read_metric(
        self,
        observation: ObservationId,
        reader: Callable[[], int | None],
        reason: Callable[[], str],
    ) -> int | None:
        try:
            value = reader()
        except Exception as exc:
            self._record_reason(observation, f"probe raised {type(exc).__name__} while sampling")
            return None
        if value is None:
            self._record_reason(observation, reason())
            return None
        if type(value) is not int or value < 0:
            self._record_reason(observation, f"probe returned an invalid {observation} reading")
            return None
        return value

    def _loop_task_count(self) -> int | None:
        loop = self._loop
        if loop is None:
            self._record_reason(
                "asyncio_task_count",
                "no event loop reference was supplied for asyncio task sampling",
            )
            return None
        try:
            if loop.is_closed():
                self._record_reason("asyncio_task_count", "the observed event loop is closed")
                return None
            # Task counts are only meaningful while the loop schedules those tasks.
            if not loop.is_running():
                self._record_reason("asyncio_task_count", "the observed event loop is not running")
                return None
            return len(asyncio.all_tasks(loop))
        except Exception as exc:
            self._record_reason(
                "asyncio_task_count", f"asyncio task enumeration raised {type(exc).__name__}"
            )
            return None

    def _record_reason(self, observation: ObservationId, reason: str) -> None:
        try:
            validated = _validate_reason_text(reason)
        except ResourceProfileError:
            validated = "probe reported a reason outside the documented contract"
        with self._lock:
            self._reasons.setdefault(observation, validated)


@dataclass(frozen=True, slots=True)
class _WorkloadCall[T]:
    """Captures the workload outcome so cleanup observation always runs first."""

    completed: bool
    value: T | None
    failure: BaseException | None


def _call_workload(workload: Callable[[], object]) -> _WorkloadCall[object]:
    try:
        return _WorkloadCall(completed=True, value=workload(), failure=None)
    except BaseException as exc:  # the failure is re-raised after cleanup observation
        return _WorkloadCall(completed=False, value=None, failure=exc)


def _await_retirement(
    baseline_threads: int, baseline_children: int, deadline_seconds: float
) -> tuple[bool, int, int]:
    wake = threading.Event()
    deadline = time.monotonic() + deadline_seconds
    threads = threading.active_count()
    children = len(multiprocessing.active_children())
    # Retirement means the counts no longer exceed the pre-workload baseline;
    # unrelated threads may exit concurrently, so at-or-below counts as retired.
    retired = threads <= baseline_threads and children <= baseline_children
    while not retired:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        wake.wait(min(_CLEANUP_POLL_SLICE_SECONDS, remaining))
        threads = threading.active_count()
        children = len(multiprocessing.active_children())
        retired = threads <= baseline_threads and children <= baseline_children
    return retired, threads, children


def profile_callable[T](
    workload: Callable[[], T],
    *,
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
    max_samples: int = DEFAULT_MAX_SAMPLES,
    loop: asyncio.AbstractEventLoop | None = None,
    cleanup_deadline_seconds: float = DEFAULT_CLEANUP_DEADLINE_SECONDS,
    probes: PlatformProbes | None = None,
    heap_probe: PythonHeapProbe | None = None,
) -> tuple[T, ResourceProfileResult]:
    """Run the workload exactly once under a bounded sampler and report resources.

    The sampler starts before the workload and stops before the cleanup
    observation, so the post-workload final sample and the retirement poll both
    run without the sampler thread inflating the process counts. The retirement
    poll wakes on bounded event slices and records whether threads and child
    processes returned to the pre-workload baseline before the deadline. A
    raising workload is re-raised after that observation; when retirement also
    failed, the typed cleanup error is raised with the workload failure as its
    cause so neither fact is lost.
    """
    validated_interval = _validate_interval_seconds(interval_seconds)
    _validate_max_samples(max_samples)
    _validate_cleanup_deadline(cleanup_deadline_seconds)
    resolved_probes = probes if probes is not None else default_platform_probes()
    resolved_heap = heap_probe if heap_probe is not None else TracemallocHeapProbe()
    baseline_threads = threading.active_count()
    baseline_children = len(multiprocessing.active_children())
    sampler = ResourceSampler(
        probes=resolved_probes,
        heap_probe=resolved_heap,
        interval_seconds=validated_interval,
        max_samples=max_samples,
        loop=loop,
    )
    sampler.start()
    call = _call_workload(workload)
    sampler.stop()
    final_sample = sampler.take_final_sample()
    retired, cleanup_threads, cleanup_children = _await_retirement(
        baseline_threads, baseline_children, cleanup_deadline_seconds
    )
    cleanup = CleanupObservation(
        thread_count=cleanup_threads,
        child_process_count=cleanup_children,
        handle_count=final_sample.handle_count,
        retired_within_deadline=retired,
    )
    result = ResourceProfileResult.from_samples(
        sampler.samples(),
        platform_label=resolved_probes.describe(),
        unavailable=sampler.unavailable_reasons(),
        cleanup=cleanup,
        interval_seconds=validated_interval,
        truncated=sampler.truncated(),
    )
    if call.failure is not None:
        if retired:
            raise call.failure
        raise ResourceCleanupError(
            "workload raised and process resources did not retire within the cleanup deadline"
        ) from call.failure
    if not call.completed:
        raise ResourceProfileError("workload outcome was not captured")
    return cast("T", call.value), result


def assert_within_capacity(observed_peak: int, capacity: int, label: str) -> None:
    """Bind one observed high-water mark to its configured capacity."""
    _validate_non_negative_int(observed_peak, "observed_peak")
    _validate_non_negative_int(capacity, "capacity")
    _validate_reason_text(label)
    if observed_peak > capacity:
        raise CapacityExceededError(
            f"observed peak {observed_peak} for {label} exceeds capacity {capacity}"
        )


def bounded_growth_within(
    first_peak: int | None,
    last_peak: int | None,
    *,
    max_growth_ratio: float,
    max_growth_bytes: int,
) -> bool:
    """Check repeated-run growth: last_peak must stay within ratio and slack.

    The bound holds when ``last_peak <= first_peak * max_growth_ratio +
    max_growth_bytes``, so growth is accepted either proportionally or by a
    small absolute allowance for allocator noise between runs.
    """
    if first_peak is None or last_peak is None:
        raise ResourceProfileError("bounded growth requires observed peaks from both runs")
    _validate_non_negative_int(first_peak, "first_peak")
    _validate_non_negative_int(last_peak, "last_peak")
    if type(max_growth_ratio) is not float or not math.isfinite(max_growth_ratio):
        raise ResourceProfileError("max_growth_ratio must be a finite float")
    if max_growth_ratio < 1.0:
        raise ResourceProfileError("max_growth_ratio must be at least 1.0")
    _validate_non_negative_int(max_growth_bytes, "max_growth_bytes")
    return last_peak <= first_peak * max_growth_ratio + max_growth_bytes


@dataclass(frozen=True, slots=True)
class ResourceProfileSummary:
    """The bounded mirrored view a consumer may rely on after strict parsing."""

    format_name: str
    format_version: int
    platform: str
    interval_seconds: float
    truncated: bool
    peaks: MetricExtrema
    baselines: MetricExtrema
    unavailable: tuple[ResourceUnavailable, ...]
    cleanup: CleanupObservation


def resource_profile_summary(result: ResourceProfileResult) -> ResourceProfileSummary:
    """Project a full result onto the bounded summary the parser produces."""
    return ResourceProfileSummary(
        format_name=RESOURCE_PROFILE_FORMAT,
        format_version=RESOURCE_PROFILE_VERSION,
        platform=result.platform_label,
        interval_seconds=result.interval_seconds,
        truncated=result.truncated,
        peaks=MetricExtrema(
            rss_bytes=result.peak_rss_bytes,
            python_heap_bytes=result.peak_python_heap_bytes,
            handle_count=result.peak_handle_count,
            thread_count=result.peak_thread_count,
            child_process_count=result.peak_child_process_count,
            asyncio_task_count=result.peak_asyncio_task_count,
        ),
        baselines=MetricExtrema(
            rss_bytes=result.baseline_rss_bytes,
            python_heap_bytes=result.baseline_python_heap_bytes,
            handle_count=result.baseline_handle_count,
            thread_count=result.baseline_thread_count,
            child_process_count=result.baseline_child_process_count,
            asyncio_task_count=result.baseline_asyncio_task_count,
        ),
        unavailable=result.unavailable,
        cleanup=result.cleanup,
    )


def _sample_document(sample: ResourceSample) -> dict[str, object]:
    return {
        "monotonic_seconds": sample.monotonic_seconds,
        "rss_bytes": sample.rss_bytes,
        "python_heap_bytes": sample.python_heap_bytes,
        "handle_count": sample.handle_count,
        "thread_count": sample.thread_count,
        "child_process_count": sample.child_process_count,
        "asyncio_task_count": sample.asyncio_task_count,
    }


def _extrema_document(values: MetricExtrema) -> dict[str, object]:
    return {
        "rss_bytes": values.rss_bytes,
        "python_heap_bytes": values.python_heap_bytes,
        "handle_count": values.handle_count,
        "thread_count": values.thread_count,
        "child_process_count": values.child_process_count,
        "asyncio_task_count": values.asyncio_task_count,
    }


def resource_profile_document(result: ResourceProfileResult) -> dict[str, object]:
    """Build the closed report document; it carries no host, user, or path values."""
    summary = resource_profile_summary(result)
    total = len(result.samples)
    serialized = min(MAX_SERIALIZED_SAMPLES, total)
    return {
        "format": RESOURCE_PROFILE_FORMAT,
        "version": RESOURCE_PROFILE_VERSION,
        "platform": summary.platform,
        "interval_seconds": summary.interval_seconds,
        "truncated": summary.truncated,
        "sampling_overhead_note": result.sampling_overhead_note,
        "samples_total": total,
        "samples_serialized": serialized,
        "samples": [_sample_document(sample) for sample in result.samples[:serialized]],
        "peaks": _extrema_document(summary.peaks),
        "baselines": _extrema_document(summary.baselines),
        "unavailable": [
            {"observation": entry.observation, "reason": entry.reason}
            for entry in summary.unavailable
        ],
        "cleanup": {
            "thread_count": summary.cleanup.thread_count,
            "child_process_count": summary.cleanup.child_process_count,
            "handle_count": summary.cleanup.handle_count,
            "retired_within_deadline": summary.cleanup.retired_within_deadline,
        },
    }


def resource_profile_bytes(result: ResourceProfileResult) -> bytes:
    """Encode the report deterministically with sorted keys and compact ASCII."""
    return json.dumps(
        resource_profile_document(result),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


_DOCUMENT_KEYS = frozenset(
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
_SAMPLE_KEYS = frozenset(
    {
        "monotonic_seconds",
        "rss_bytes",
        "python_heap_bytes",
        "handle_count",
        "thread_count",
        "child_process_count",
        "asyncio_task_count",
    }
)
_METRIC_KEYS = frozenset(
    {
        "rss_bytes",
        "python_heap_bytes",
        "handle_count",
        "thread_count",
        "child_process_count",
        "asyncio_task_count",
    }
)
_CLEANUP_KEYS = frozenset(
    {"thread_count", "child_process_count", "handle_count", "retired_within_deadline"}
)
_UNAVAILABLE_KEYS = frozenset({"observation", "reason"})


def _fail(where: str, detail: str) -> NoReturn:
    raise ResourceProfileParseError(f"invalid resource profile {where}: {detail}")


def _reject_json_constant(value: str) -> float:
    _fail("payload", f"the JSON constant {value} is not permitted")


def _require_object(value: object, where: str) -> dict[str, object]:
    if not isinstance(value, dict):
        _fail(where, "expected a JSON object")
    return cast("dict[str, object]", value)


def _require_keys(document: dict[str, object], expected: frozenset[str], where: str) -> None:
    actual = frozenset(document)
    if actual != expected:
        missing = sorted(expected - actual)
        unknown = sorted(actual - expected)
        _fail(where, f"missing keys {missing} and unknown keys {unknown}")


def _require_int(value: object, where: str) -> int:
    if type(value) is not int:
        _fail(where, "expected an integer")
    return value


def _require_non_negative_int(value: object, where: str) -> int:
    parsed = _require_int(value, where)
    if parsed < 0:
        _fail(where, "expected a non-negative integer")
    return parsed


def _require_optional_non_negative_int(value: object, where: str) -> int | None:
    if value is None:
        return None
    return _require_non_negative_int(value, where)


def _require_bool(value: object, where: str) -> bool:
    if type(value) is not bool:
        _fail(where, "expected a boolean")
    return value


def _require_float(value: object, where: str) -> float:
    if type(value) is not float:
        _fail(where, "expected a float")
    if not math.isfinite(value):
        _fail(where, "expected a finite float")
    return value


def _require_reason(value: object, where: str) -> str:
    if type(value) is not str:
        _fail(where, "expected a reason string")
    try:
        return _validate_reason_text(value)
    except ResourceProfileError as exc:
        _fail(where, str(exc))


def _require_platform(value: object, where: str) -> str:
    if type(value) is not str:
        _fail(where, "expected a platform label string")
    try:
        return _validate_platform_label(value)
    except ResourceProfileError as exc:
        _fail(where, str(exc))


def _require_observation(value: object, where: str) -> ObservationId:
    if type(value) is not str or value not in _OBSERVATION_SET:
        _fail(where, "expected a known observation id")
    return value


def _require_extrema(value: object, where: str) -> MetricExtrema:
    metrics = _require_object(value, where)
    _require_keys(metrics, _METRIC_KEYS, where)
    return MetricExtrema(
        rss_bytes=_require_optional_non_negative_int(metrics["rss_bytes"], f"{where}.rss_bytes"),
        python_heap_bytes=_require_optional_non_negative_int(
            metrics["python_heap_bytes"], f"{where}.python_heap_bytes"
        ),
        handle_count=_require_optional_non_negative_int(
            metrics["handle_count"], f"{where}.handle_count"
        ),
        thread_count=_require_optional_non_negative_int(
            metrics["thread_count"], f"{where}.thread_count"
        ),
        child_process_count=_require_optional_non_negative_int(
            metrics["child_process_count"], f"{where}.child_process_count"
        ),
        asyncio_task_count=_require_optional_non_negative_int(
            metrics["asyncio_task_count"], f"{where}.asyncio_task_count"
        ),
    )


def _require_sample(value: object, where: str) -> ResourceSample:
    document = _require_object(value, where)
    _require_keys(document, _SAMPLE_KEYS, where)
    return ResourceSample(
        monotonic_seconds=_require_float(
            document["monotonic_seconds"], f"{where}.monotonic_seconds"
        ),
        rss_bytes=_require_optional_non_negative_int(document["rss_bytes"], f"{where}.rss_bytes"),
        python_heap_bytes=_require_optional_non_negative_int(
            document["python_heap_bytes"], f"{where}.python_heap_bytes"
        ),
        handle_count=_require_optional_non_negative_int(
            document["handle_count"], f"{where}.handle_count"
        ),
        thread_count=_require_non_negative_int(document["thread_count"], f"{where}.thread_count"),
        child_process_count=_require_non_negative_int(
            document["child_process_count"], f"{where}.child_process_count"
        ),
        asyncio_task_count=_require_optional_non_negative_int(
            document["asyncio_task_count"], f"{where}.asyncio_task_count"
        ),
    )


def parse_resource_profile(payload: bytes) -> ResourceProfileSummary:
    """Strictly re-parse a report payload; any contract deviation fails closed."""
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        _fail("payload", f"payload is not decodable UTF-8: {exc.reason}")
    try:
        parsed: object = json.loads(text, parse_constant=_reject_json_constant)
    except ResourceProfileParseError:
        raise
    except ValueError:
        _fail("payload", "payload is not valid JSON")
    if not isinstance(parsed, dict):
        _fail("document", "expected a JSON object")
    document = cast("dict[str, object]", parsed)
    _require_keys(document, _DOCUMENT_KEYS, "document")
    if document["format"] != RESOURCE_PROFILE_FORMAT:
        _fail("document", f"unknown format {document['format']!r}")
    version = _require_int(document["version"], "document.version")
    if version != RESOURCE_PROFILE_VERSION:
        _fail("document", f"unknown version {version}; exactly {RESOURCE_PROFILE_VERSION} accepted")
    interval = _require_float(document["interval_seconds"], "document.interval_seconds")
    if not MIN_INTERVAL_SECONDS <= interval <= MAX_INTERVAL_SECONDS:
        _fail("document", "interval_seconds is outside the documented bounds")
    samples_total = _require_non_negative_int(document["samples_total"], "document.samples_total")
    if samples_total > MAX_MAX_SAMPLES:
        _fail("document", f"samples_total exceeds {MAX_MAX_SAMPLES}")
    samples_serialized = _require_non_negative_int(
        document["samples_serialized"], "document.samples_serialized"
    )
    if samples_serialized > MAX_SERIALIZED_SAMPLES:
        _fail("document", f"samples_serialized exceeds {MAX_SERIALIZED_SAMPLES}")
    if samples_serialized > samples_total:
        _fail("document", "samples_serialized exceeds samples_total")
    raw_samples = document["samples"]
    if not isinstance(raw_samples, list):
        _fail("document.samples", "expected a JSON array")
    samples = cast("list[object]", raw_samples)
    if len(samples) != samples_serialized:
        _fail("document.samples", "array length does not match samples_serialized")
    previous_monotonic = -1.0
    for index, raw_sample in enumerate(samples):
        sample = _require_sample(raw_sample, f"document.samples[{index}]")
        if sample.monotonic_seconds < previous_monotonic:
            _fail(f"document.samples[{index}]", "timestamps must be non-decreasing")
        previous_monotonic = sample.monotonic_seconds
    peaks = _require_extrema(document["peaks"], "document.peaks")
    baselines = _require_extrema(document["baselines"], "document.baselines")
    _require_consistent_extrema(peaks, baselines)
    raw_unavailable = document["unavailable"]
    if not isinstance(raw_unavailable, list):
        _fail("document.unavailable", "expected a JSON array")
    unavailable = tuple(
        _require_unavailable(entry, f"document.unavailable[{index}]")
        for index, entry in enumerate(cast("list[object]", raw_unavailable))
    )
    cleanup = _require_cleanup(document["cleanup"], "document.cleanup")
    _require_reason(document["sampling_overhead_note"], "document.sampling_overhead_note")
    return ResourceProfileSummary(
        format_name=RESOURCE_PROFILE_FORMAT,
        format_version=RESOURCE_PROFILE_VERSION,
        platform=_require_platform(document["platform"], "document.platform"),
        interval_seconds=interval,
        truncated=_require_bool(document["truncated"], "document.truncated"),
        peaks=peaks,
        baselines=baselines,
        unavailable=unavailable,
        cleanup=cleanup,
    )


def _require_consistent_extrema(peaks: MetricExtrema, baselines: MetricExtrema) -> None:
    for field_name in (
        "rss_bytes",
        "python_heap_bytes",
        "handle_count",
        "thread_count",
        "child_process_count",
        "asyncio_task_count",
    ):
        peak = getattr(peaks, field_name)
        baseline = getattr(baselines, field_name)
        if peak is not None and baseline is not None and peak < baseline:
            _fail("document.peaks", f"peak {field_name} is below its baseline")


def _require_unavailable(value: object, where: str) -> ResourceUnavailable:
    document = _require_object(value, where)
    _require_keys(document, _UNAVAILABLE_KEYS, where)
    return ResourceUnavailable(
        observation=_require_observation(document["observation"], f"{where}.observation"),
        reason=_require_reason(document["reason"], f"{where}.reason"),
    )


def _require_cleanup(value: object, where: str) -> CleanupObservation:
    document = _require_object(value, where)
    _require_keys(document, _CLEANUP_KEYS, where)
    return CleanupObservation(
        thread_count=_require_non_negative_int(document["thread_count"], f"{where}.thread_count"),
        child_process_count=_require_non_negative_int(
            document["child_process_count"], f"{where}.child_process_count"
        ),
        handle_count=_require_optional_non_negative_int(
            document["handle_count"], f"{where}.handle_count"
        ),
        retired_within_deadline=_require_bool(
            document["retired_within_deadline"], f"{where}.retired_within_deadline"
        ),
    )
