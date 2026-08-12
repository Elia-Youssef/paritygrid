"""Dependency-neutral contracts for durable consistency records."""

import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, Self, cast

from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.domain.models import ArtifactId, NodeId, RunId, UtcTimestamp, WorkItemId
from paritygrid.domain.pipeline import PartitionKey

MAX_CONSISTENCY_PAGE_SIZE = 100
MAX_EVENT_BATCH_SIZE = 100
MAX_CONSISTENCY_SEQUENCE = 2_147_483_647


class ConsistencyRepositoryError(Exception):
    """Base class for stable consistency repository failures."""


class ConsistencyInvalidRequestError(ConsistencyRepositoryError):
    """A request violates the public consistency contract."""


class ConsistencyRecordNotFoundError(ConsistencyRepositoryError):
    """A required consistency record does not exist."""


class ConsistencyStaleRowVersionError(ConsistencyRepositoryError):
    """An optimistic row version no longer matches durable state."""


class ConsistencyStateConflictError(ConsistencyRepositoryError):
    """Current durable state rejects the requested operation."""


class CheckpointConflictError(ConsistencyStateConflictError):
    """Checkpoint history diverges from the requested append."""


class EventSequenceConflictError(ConsistencyStateConflictError):
    """Durable event history diverges from the requested allocation."""


class IdempotencyConflictError(ConsistencyStateConflictError):
    """An idempotency identity was reused for a different request or result."""


class ConsistencyCorruptionError(ConsistencyRepositoryError):
    """Persisted consistency data failed strict boundary validation."""


class ConsistencyStorageError(ConsistencyRepositoryError):
    """An unexpected persistence implementation failure prevented the operation."""


class ConsistencyStorageUnavailableError(ConsistencyStorageError):
    """Consistency storage was unavailable for the requested operation."""


@dataclass(frozen=True, slots=True, order=True)
class CheckpointVersion:
    """A nonnegative checkpoint frontier or positive history version."""

    number: int

    def __post_init__(self) -> None:
        value = cast(object, self.number)
        if type(value) is not int:
            raise TypeError("checkpoint version must be an integer")
        if not 0 <= self.number <= MAX_CONSISTENCY_SEQUENCE:
            raise ValueError("checkpoint version is outside the supported range")

    def next(self) -> Self:
        """Return the following version when the durable range permits it."""
        if self.number >= MAX_CONSISTENCY_SEQUENCE:
            raise ConsistencyStateConflictError(
                "checkpoint version cannot advance beyond the supported maximum"
            )
        return type(self)(self.number + 1)

    def __int__(self) -> int:
        return self.number


@dataclass(frozen=True, slots=True, order=True)
class EventSequence:
    """A positive durable event sequence number."""

    number: int

    def __post_init__(self) -> None:
        value = cast(object, self.number)
        if type(value) is not int:
            raise TypeError("event sequence must be an integer")
        if not 1 <= self.number <= MAX_CONSISTENCY_SEQUENCE:
            raise ValueError("event sequence is outside the supported range")

    def advance(self, count: int) -> Self:
        """Return a later sequence after a bounded positive allocation."""
        count_value = cast(object, count)
        if type(count_value) is not int or count <= 0:
            raise ConsistencyInvalidRequestError("event allocation count must be positive")
        if self.number + count > MAX_CONSISTENCY_SEQUENCE:
            raise EventSequenceConflictError(
                "event sequence cannot advance beyond the supported maximum"
            )
        return type(self)(self.number + count)

    def __int__(self) -> int:
        return self.number


ZERO_CHECKPOINT_VERSION = CheckpointVersion(0)


class EventSubjectKind(StrEnum):
    """Closed subject categories for durable execution events."""

    RUN = "run"
    WORK_ITEM = "work_item"


class IdempotencyStatus(StrEnum):
    """Public lifecycle of an idempotent command reservation."""

    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class IdempotencyBeginDisposition(StrEnum):
    """Outcome of beginning or replaying an idempotent command."""

    STARTED = "started"
    IN_PROGRESS_REPLAY = "in_progress_replay"
    COMPLETED_REPLAY = "completed_replay"
    FAILED_REPLAY = "failed_replay"


_SENSITIVE_KEY_PARTS = frozenset(
    {
        "access_key",
        "api_key",
        "authorization",
        "bearer",
        "cookie",
        "credential",
        "password",
        "private_key",
        "secret",
        "session",
        "token",
    }
)
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])", flags=re.ASCII)
_NON_IDENTIFIER = re.compile(r"[^a-z0-9]+", flags=re.ASCII)


@dataclass(frozen=True, slots=True, repr=False)
class RedactedDocument:
    """A document proven safe for durable event and logical response storage."""

    document: ConfigurationDocument

    def __post_init__(self) -> None:
        value = cast(object, self.document)
        if type(value) is not ConfigurationDocument:
            raise TypeError("redacted document must wrap ConfigurationDocument")
        _validate_redacted_mapping(self.document.to_mapping())

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> Self:
        """Copy an object into the immutable redacted-document boundary."""
        return cls(ConfigurationDocument.from_mapping(value))

    def to_mapping(self) -> dict[str, object]:
        """Return a detached mapping after construction-time redaction validation."""
        return self.document.to_mapping()

    def __repr__(self) -> str:
        return "RedactedDocument(content=<redacted>)"


def _validate_redacted_mapping(value: object) -> None:
    if isinstance(value, dict):
        for raw_key, child in cast(dict[str, object], value).items():
            expanded = _CAMEL_BOUNDARY.sub("_", raw_key)
            normalized = _NON_IDENTIFIER.sub(
                "_", unicodedata.normalize("NFC", expanded).casefold()
            ).strip("_")
            padded = f"_{normalized}_"
            if any(f"_{part}_" in padded for part in _SENSITIVE_KEY_PARTS):
                raise ConsistencyInvalidRequestError(
                    "redacted document contains a prohibited sensitive field"
                )
            _validate_redacted_mapping(child)
    elif isinstance(value, list):
        for child in cast(list[object], value):
            _validate_redacted_mapping(child)


@dataclass(frozen=True, slots=True)
class CheckpointHeadRecord:
    run_id: RunId
    node_id: NodeId
    partition_key: PartitionKey
    current_version: CheckpointVersion
    updated_at: UtcTimestamp
    row_version: int


@dataclass(frozen=True, slots=True, repr=False)
class CheckpointRecord:
    run_id: RunId
    node_id: NodeId
    partition_key: PartitionKey
    version: CheckpointVersion
    payload_schema_version: int
    source_cursor: ConfigurationDocument | None
    output_position: ConfigurationDocument | None
    artifact_id: ArtifactId | None
    committed_at: UtcTimestamp

    def __repr__(self) -> str:
        return (
            "CheckpointRecord("
            f"run_id={self.run_id!r}, node_id={self.node_id!r}, "
            f"partition_key={self.partition_key!r}, version={self.version!r}, "
            f"payload_schema_version={self.payload_schema_version!r}, "
            f"artifact_id={self.artifact_id!r}, committed_at={self.committed_at!r}, "
            "source_cursor=<redacted>, output_position=<redacted>)"
        )


@dataclass(frozen=True, slots=True)
class UpdatedWorkCheckpoint:
    work_item_id: WorkItemId
    run_id: RunId
    node_id: NodeId
    partition_key: PartitionKey
    expected_checkpoint_version: CheckpointVersion
    row_version: int


@dataclass(frozen=True, slots=True)
class CheckpointCommit:
    head: CheckpointHeadRecord
    checkpoint: CheckpointRecord
    work: UpdatedWorkCheckpoint


@dataclass(frozen=True, slots=True)
class CheckpointPage:
    items: tuple[CheckpointRecord, ...]
    next_cursor: CheckpointVersion | None


@dataclass(frozen=True, slots=True, repr=False)
class PendingExecutionEvent:
    event_kind: str
    occurred_at: UtcTimestamp
    subject_kind: EventSubjectKind
    subject_id: RunId | WorkItemId
    correlation_id: str | None
    payload_schema_version: int
    payload: RedactedDocument

    def __repr__(self) -> str:
        return (
            "PendingExecutionEvent("
            f"event_kind={self.event_kind!r}, occurred_at={self.occurred_at!r}, "
            f"subject_kind={self.subject_kind!r}, subject_id={self.subject_id!r}, "
            f"correlation_id={self.correlation_id!r}, "
            f"payload_schema_version={self.payload_schema_version!r}, payload=<redacted>)"
        )


@dataclass(frozen=True, slots=True, repr=False)
class ExecutionEventRecord:
    run_id: RunId
    sequence: EventSequence
    event_kind: str
    occurred_at: UtcTimestamp
    subject_kind: EventSubjectKind
    subject_id: RunId | WorkItemId
    correlation_id: str | None
    payload_schema_version: int
    payload: RedactedDocument

    def __repr__(self) -> str:
        return (
            "ExecutionEventRecord("
            f"run_id={self.run_id!r}, sequence={self.sequence!r}, "
            f"event_kind={self.event_kind!r}, occurred_at={self.occurred_at!r}, "
            f"subject_kind={self.subject_kind!r}, subject_id={self.subject_id!r}, "
            f"correlation_id={self.correlation_id!r}, "
            f"payload_schema_version={self.payload_schema_version!r}, payload=<redacted>)"
        )


@dataclass(frozen=True, slots=True)
class ExecutionEventBatch:
    items: tuple[ExecutionEventRecord, ...]
    next_sequence: EventSequence
    counter_row_version: int


@dataclass(frozen=True, slots=True)
class ExecutionEventPage:
    items: tuple[ExecutionEventRecord, ...]
    next_cursor: EventSequence | None


@dataclass(frozen=True, slots=True, repr=False)
class IdempotencyRecord:
    scope: str
    key: str
    status: IdempotencyStatus
    response_schema_version: int | None
    response: RedactedDocument | None
    created_at: UtcTimestamp
    updated_at: UtcTimestamp
    completed_at: UtcTimestamp | None

    def __repr__(self) -> str:
        return (
            "IdempotencyRecord("
            f"status={self.status!r}, response_schema_version={self.response_schema_version!r}, "
            f"created_at={self.created_at!r}, updated_at={self.updated_at!r}, "
            f"completed_at={self.completed_at!r}, identity=<redacted>, response=<redacted>)"
        )


@dataclass(frozen=True, slots=True)
class IdempotencyBeginResult:
    disposition: IdempotencyBeginDisposition
    record: IdempotencyRecord


@dataclass(frozen=True, slots=True, repr=False)
class IdempotencyCursor:
    created_at: UtcTimestamp
    scope: str
    key: str

    def __repr__(self) -> str:
        return f"IdempotencyCursor(created_at={self.created_at!r}, identity=<redacted>)"


@dataclass(frozen=True, slots=True)
class IdempotencyPage:
    items: tuple[IdempotencyRecord, ...]
    next_cursor: IdempotencyCursor | None


class CheckpointRepository(Protocol):
    """Atomic checkpoint frontier and immutable history operations."""

    def get_head(
        self, run_id: RunId, node_id: NodeId, partition_key: PartitionKey
    ) -> CheckpointHeadRecord | None: ...

    def get(
        self,
        run_id: RunId,
        node_id: NodeId,
        partition_key: PartitionKey,
        version: CheckpointVersion,
    ) -> CheckpointRecord | None: ...

    def list_history(
        self,
        run_id: RunId,
        node_id: NodeId,
        partition_key: PartitionKey,
        *,
        limit: int,
        after: CheckpointVersion = ZERO_CHECKPOINT_VERSION,
    ) -> CheckpointPage: ...

    def append(
        self,
        run_id: RunId,
        node_id: NodeId,
        partition_key: PartitionKey,
        *,
        expected_current_version: CheckpointVersion,
        expected_head_row_version: int,
        expected_work_row_version: int,
        payload_schema_version: int,
        source_cursor: ConfigurationDocument | None,
        output_position: ConfigurationDocument | None,
        artifact_id: ArtifactId | None,
        committed_at: UtcTimestamp,
    ) -> CheckpointCommit: ...


class ExecutionEventRepository(Protocol):
    """Contiguous allocation and replay of durable execution events."""

    def append(
        self,
        run_id: RunId,
        *,
        expected_next_sequence: EventSequence,
        expected_counter_row_version: int,
        events: Sequence[PendingExecutionEvent],
    ) -> ExecutionEventBatch: ...

    def get(self, run_id: RunId, sequence: EventSequence) -> ExecutionEventRecord | None: ...

    def list_after(
        self,
        run_id: RunId,
        *,
        after: EventSequence | None,
        limit: int,
    ) -> ExecutionEventPage: ...


class IdempotencyRepository(Protocol):
    """Replay-safe durable command reservation and terminal response operations."""

    def begin(
        self,
        *,
        scope: str,
        key: str,
        request: ConfigurationDocument,
        started_at: UtcTimestamp,
    ) -> IdempotencyBeginResult: ...

    def get(self, *, scope: str, key: str) -> IdempotencyRecord | None: ...

    def list_in_progress(
        self, *, limit: int, after: IdempotencyCursor | None = None
    ) -> IdempotencyPage: ...

    def complete(
        self,
        *,
        scope: str,
        key: str,
        request: ConfigurationDocument,
        expected_updated_at: UtcTimestamp,
        response_schema_version: int,
        response: RedactedDocument,
        completed_at: UtcTimestamp,
    ) -> IdempotencyRecord: ...

    def fail(
        self,
        *,
        scope: str,
        key: str,
        request: ConfigurationDocument,
        expected_updated_at: UtcTimestamp,
        response_schema_version: int,
        response: RedactedDocument,
        completed_at: UtcTimestamp,
    ) -> IdempotencyRecord: ...


def validate_consistency_page_limit(limit: object) -> int:
    """Validate a consistency collection page size without coercion."""
    if type(limit) is not int or not 1 <= limit <= MAX_CONSISTENCY_PAGE_SIZE:
        raise ConsistencyInvalidRequestError(
            f"page limit must be an integer between 1 and {MAX_CONSISTENCY_PAGE_SIZE}"
        )
    return limit
