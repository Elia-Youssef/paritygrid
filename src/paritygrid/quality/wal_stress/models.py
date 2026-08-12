"""Immutable settings and evidence produced by the WAL stress harness."""

from dataclasses import asdict, dataclass
from enum import StrEnum
from pathlib import Path
from typing import cast

from paritygrid.application.ports.writer import (
    NotificationBufferStats,
    WriterCloseResult,
    WriterDiagnostics,
)

WAL_STRESS_REPORT_SCHEMA_VERSION = 1


class WalStressError(Exception):
    """A WAL stress run could not produce trustworthy evidence."""


class WalStressProfile(StrEnum):
    CI = "ci"
    LOCAL = "local"


@dataclass(frozen=True, slots=True)
class WalStressConfig:
    """Explicit file target and bounded workload selection."""

    database_path: Path
    profile: WalStressProfile = WalStressProfile.CI
    seed: int = 1
    create_parent: bool = False

    def __post_init__(self) -> None:
        path_value = cast(object, self.database_path)
        if not isinstance(path_value, Path):
            raise TypeError("WAL stress database path must be a Path")
        if not self.database_path.is_absolute():
            raise ValueError("WAL stress database path must be absolute")
        if type(self.profile) is not WalStressProfile:
            raise TypeError("WAL stress profile must be a WalStressProfile")
        if type(self.seed) is not int or not 0 <= self.seed <= 4_294_967_295:
            raise ValueError("WAL stress seed is outside the supported range")
        if type(self.create_parent) is not bool:
            raise TypeError("WAL stress parent creation flag must be a boolean")


@dataclass(frozen=True, slots=True)
class WalStressWorkload:
    producer_count: int
    reader_count: int
    work_commands: int
    queue_capacity: int
    admission_capacity: int
    notification_capacity: int
    max_contention_attempts: int
    timeout_seconds: float
    total_budget_seconds: float

    def __post_init__(self) -> None:
        integers = (
            self.producer_count,
            self.reader_count,
            self.work_commands,
            self.queue_capacity,
            self.admission_capacity,
            self.notification_capacity,
            self.max_contention_attempts,
        )
        if any(type(value) is not int or value < 1 for value in integers):
            raise ValueError("WAL stress workload integer bounds are invalid")
        if type(self.timeout_seconds) is not float or not 0 < self.timeout_seconds <= 60:
            raise ValueError("WAL stress operation timeout is invalid")
        if type(self.total_budget_seconds) is not float or not 0 < self.total_budget_seconds <= 300:
            raise ValueError("WAL stress total budget is invalid")


@dataclass(frozen=True, slots=True)
class ProducerEvidence:
    producer: int
    admitted: int
    admission_wait_seconds: float


@dataclass(frozen=True, slots=True)
class ReaderEvidence:
    reader: int
    operations: int
    first_frontier: int
    last_frontier: int
    maximum_latency_seconds: float


@dataclass(frozen=True, slots=True)
class WalCheckpointEvidence:
    passive_while_pinned: tuple[int, int, int]
    truncate_after_release: tuple[int, int, int]


@dataclass(frozen=True, slots=True)
class OperationalEvidence:
    execution_events: int
    next_event_sequence: int
    run_row_version: int
    work_items: int
    checkpoint_heads: int
    node_work_total: int


@dataclass(frozen=True, slots=True)
class IntegrityEvidence:
    journal_mode: str
    synchronous_level: int
    foreign_keys: bool
    busy_timeout_ms: int
    quick_check: str
    foreign_key_violations: int
    pool_checked_out: int
    writer_thread_stopped: bool
    reader_threads_stopped: bool
    sidecars_absent: bool


@dataclass(frozen=True, slots=True)
class WalStressReport:
    """Bounded machine-readable correctness evidence for one stress run."""

    schema_version: int
    profile: WalStressProfile
    seed: int
    platform: str
    python_version: str
    sqlite_version: str
    scenario_manifest_sha256: str
    workload: WalStressWorkload
    elapsed_seconds: float
    submitted: int
    admitted: int
    committed: int
    failures: int
    writer: WriterDiagnostics
    close: WriterCloseResult
    notifications: NotificationBufferStats
    contention_codes: tuple[int, ...]
    locked_probe_code: int
    producers: tuple[ProducerEvidence, ...]
    readers: tuple[ReaderEvidence, ...]
    pinned_reader_start_frontier: int
    pinned_reader_end_frontier: int
    checkpoints: WalCheckpointEvidence
    operational: OperationalEvidence
    integrity: IntegrityEvidence

    def __post_init__(self) -> None:
        if self.schema_version != WAL_STRESS_REPORT_SCHEMA_VERSION:
            raise ValueError("WAL stress report schema version is invalid")
        if type(self.profile) is not WalStressProfile:
            raise TypeError("WAL stress report profile is invalid")
        if type(self.seed) is not int or not 0 <= self.seed <= 4_294_967_295:
            raise ValueError("WAL stress report seed is invalid")
        for value, subject in (
            (self.platform, "platform"),
            (self.python_version, "Python version"),
            (self.sqlite_version, "SQLite version"),
        ):
            if type(value) is not str or not value:
                raise ValueError(f"WAL stress report {subject} is invalid")
        digest = self.scenario_manifest_sha256
        if (
            type(digest) is not str
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise ValueError("WAL stress scenario manifest digest is invalid")
        if type(self.elapsed_seconds) is not float or self.elapsed_seconds < 0:
            raise ValueError("WAL stress elapsed time is invalid")
        counts = (self.submitted, self.admitted, self.committed, self.failures)
        if any(type(value) is not int or value < 0 for value in counts):
            raise ValueError("WAL stress command counts are invalid")
        if any(type(code) is not int or code < 0 for code in self.contention_codes):
            raise ValueError("WAL stress contention codes are invalid")
        if type(self.locked_probe_code) is not int or self.locked_probe_code < 0:
            raise ValueError("WAL stress locked probe code is invalid")

    def to_mapping(self) -> dict[str, object]:
        """Return a detached JSON-compatible report mapping."""
        value = cast(dict[str, object], asdict(self))
        value["profile"] = self.profile.value
        writer = cast(dict[str, object], value["writer"])
        writer["state"] = self.writer.state.value
        return value


def workload_for(profile: WalStressProfile) -> WalStressWorkload:
    """Return the fixed finite workload for a named profile."""
    if profile is WalStressProfile.CI:
        return WalStressWorkload(4, 4, 96, 8, 8, 8, 3, 5.0, 25.0)
    if profile is WalStressProfile.LOCAL:
        return WalStressWorkload(8, 8, 384, 16, 16, 16, 4, 10.0, 120.0)
    raise TypeError("WAL stress profile is invalid")


__all__ = [
    "WAL_STRESS_REPORT_SCHEMA_VERSION",
    "IntegrityEvidence",
    "OperationalEvidence",
    "ProducerEvidence",
    "ReaderEvidence",
    "WalCheckpointEvidence",
    "WalStressConfig",
    "WalStressError",
    "WalStressProfile",
    "WalStressReport",
    "WalStressWorkload",
    "workload_for",
]
