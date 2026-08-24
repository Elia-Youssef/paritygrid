"""Versioned bounded concurrency telemetry for P7.8.

Telemetry is passive observability of the bounded execution machinery.
Every record is an immutable version-1 observation of queue depth,
capacity use, wait and service time, blocked writer flow, dropped
telemetry, cleanup state, or unresolved resources. Metric names,
labels, values, series counts, and wire payloads are bounded, and
label material is redaction-checked so no secret, credential, or
filesystem path shape can enter a record.

Observation time is always injected by the caller: the module
never reads a clock. Records may be sampled or dropped. The collector
keeps a bounded buffer, drops the oldest record under overflow, and
reports the drop count instead of ever growing unbounded.

Telemetry carries no authority. It cannot release work, advance
durable progress, acknowledge a result, reconstruct recovery, or
publish authoritative run progress. This module imports no
scheduling, persistence, or credential module: it only reads bounded
capacity facts, and its values are passive observations that nothing
outside this module consumes as state. The live WebSocket transport
and any durable streaming surface are later packages and are
intentionally absent here.
"""

from __future__ import annotations

import re
from collections import deque
from dataclasses import dataclass
from enum import StrEnum
from threading import Lock
from typing import cast

from paritygrid.application.execution.capacity import (
    CAPACITY_CATEGORY_CONNECTOR,
    CAPACITY_CATEGORY_CPU_POOL,
    CAPACITY_CATEGORY_GLOBAL,
    CAPACITY_CATEGORY_NODE,
    CAPACITY_CATEGORY_STRATEGY,
    CapacitySnapshot,
)

TELEMETRY_SCHEMA_VERSION = 1
MAX_METRIC_NAME_LENGTH = 64
MAX_LABEL_COUNT = 8
MAX_LABEL_LENGTH = 64
MAX_METRIC_VALUE = 2**31 - 1
MAX_SERIES_PER_RECORD = 16
MAX_TELEMETRY_RECORD_BYTES = 4_096
MAX_COLLECTOR_CAPACITY = 65_536

_MAX_LABEL_KEY_LENGTH = 32
_MAX_RUN_ID_LENGTH = 128

_METRIC_NAME_PATTERN: re.Pattern[str] = re.compile(r"[a-z][a-z0-9_]*")
_LABEL_KEY_PATTERN: re.Pattern[str] = re.compile(r"[a-z][a-z0-9_]*")

_SECRET_MARKERS: tuple[str, ...] = ("password=", "token:", "bearer ", "secret/")
_WIRE_RESERVED_CHARACTERS = frozenset("\\|;=")

_CAPACITY_CATEGORIES: frozenset[str] = frozenset(
    {
        CAPACITY_CATEGORY_CONNECTOR,
        CAPACITY_CATEGORY_CPU_POOL,
        CAPACITY_CATEGORY_GLOBAL,
        CAPACITY_CATEGORY_NODE,
        CAPACITY_CATEGORY_STRATEGY,
    }
)
_CLEANUP_STATES: frozenset[str] = frozenset({"pending", "completed", "failed"})

_RECORD_MAPPING_KEYS: frozenset[str] = frozenset(
    {"schema_version", "observed_at_micros", "run_id", "metrics"}
)
_METRIC_MAPPING_KEYS: frozenset[str] = frozenset({"name", "kind", "value", "labels"})


class TelemetryError(RuntimeError):
    """Base failure for versioned telemetry validation, schema, and redaction."""


class TelemetryValidationError(TelemetryError):
    """A telemetry input has an unsupported type, bound, or shape."""


class TelemetrySchemaError(TelemetryError):
    """A telemetry record or payload uses an unknown schema version."""


class TelemetryRedactionError(TelemetryError):
    """A telemetry value would carry a secret or filesystem path shape."""


class TelemetryMetricKind(StrEnum):
    """Closed observation kinds for version 1 concurrency telemetry."""

    QUEUE_DEPTH = "queue_depth"
    ACTIVE_CAPACITY = "active_capacity"
    CAPACITY_WAIT = "capacity_wait"
    SERVICE_DURATION = "service_duration"
    BLOCKED_WRITER = "blocked_writer"
    DROPPED_TELEMETRY = "dropped_telemetry"
    CLEANUP_STATE = "cleanup_state"
    UNRESOLVED_RESOURCES = "unresolved_resources"


def _reject_secret_and_path_markers(value: str, subject: str) -> None:
    lowered = value.lower()
    for marker in _SECRET_MARKERS:
        if marker in lowered:
            raise TelemetryRedactionError(f"{subject} must not contain secret material")
    if value.startswith(("/", "\\")):
        raise TelemetryRedactionError(f"{subject} must not be an absolute path")
    if len(value) >= 2 and value[1] == ":" and "A" <= value[0].upper() <= "Z":
        raise TelemetryRedactionError(f"{subject} must not be a filesystem path")
    if ".." in value:
        raise TelemetryRedactionError(f"{subject} must not contain path traversal")


def _validate_run_id(value: object) -> str:
    if type(value) is not str:
        raise TelemetryValidationError("telemetry run identity must be text")
    run_id = value
    if not 1 <= len(run_id) <= _MAX_RUN_ID_LENGTH:
        raise TelemetryValidationError(
            "telemetry run identity length is outside the supported range"
        )
    for character in run_id:
        if not "\x20" <= character <= "\x7e":
            raise TelemetryValidationError(
                "telemetry run identity must use printable ASCII characters"
            )
    _reject_secret_and_path_markers(run_id, "telemetry run identity")
    return run_id


def _validate_label_pair(key: object, label_value: object) -> None:
    if type(key) is not str:
        raise TelemetryValidationError("telemetry label key must be text")
    label_key = key
    if not 1 <= len(label_key) <= _MAX_LABEL_KEY_LENGTH:
        raise TelemetryValidationError("telemetry label key length is outside the supported range")
    if _LABEL_KEY_PATTERN.fullmatch(label_key) is None:
        raise TelemetryValidationError("telemetry label key must use the bounded label alphabet")
    if type(label_value) is not str:
        raise TelemetryValidationError("telemetry label value must be text")
    value = label_value
    if not 1 <= len(value) <= MAX_LABEL_LENGTH:
        raise TelemetryValidationError(
            "telemetry label value length is outside the supported range"
        )
    for character in value:
        if not "\x20" <= character <= "\x7e":
            raise TelemetryValidationError(
                "telemetry label value must use printable ASCII characters"
            )
    _reject_secret_and_path_markers(value, "telemetry label value")


@dataclass(frozen=True, slots=True, repr=False)
class TelemetryMetric:
    """One bounded integer observation series with sorted unique labels."""

    name: str
    kind: TelemetryMetricKind
    value: int
    labels: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if type(self.name) is not str:
            raise TelemetryValidationError("telemetry metric name must be text")
        name = self.name
        if not 1 <= len(name) <= MAX_METRIC_NAME_LENGTH:
            raise TelemetryValidationError(
                "telemetry metric name length is outside the supported range"
            )
        if _METRIC_NAME_PATTERN.fullmatch(name) is None:
            raise TelemetryValidationError(
                "telemetry metric name must use the bounded metric alphabet"
            )
        if type(self.kind) is not TelemetryMetricKind:
            raise TelemetryValidationError("telemetry metric kind must use TelemetryMetricKind")
        if type(self.value) is not int:
            raise TelemetryValidationError("telemetry metric value must be an integer")
        value = self.value
        if not 0 <= value <= MAX_METRIC_VALUE:
            raise TelemetryValidationError("telemetry metric value is outside the supported range")
        if type(self.labels) is not tuple:
            raise TelemetryValidationError("telemetry metric labels must be a tuple")
        entries = cast(tuple[object, ...], self.labels)
        if len(entries) > MAX_LABEL_COUNT:
            raise TelemetryValidationError(
                "telemetry metric label count exceeds the supported bound"
            )
        keys: list[str] = []
        for entry in entries:
            if type(entry) is not tuple:
                raise TelemetryValidationError("telemetry metric labels must be key-value pairs")
            pair = cast(tuple[object, object], entry)
            if len(pair) != 2:
                raise TelemetryValidationError("telemetry metric labels must be key-value pairs")
            _validate_label_pair(pair[0], pair[1])
            keys.append(cast(str, pair[0]))
        if len(set(keys)) != len(keys):
            raise TelemetryValidationError("telemetry metric label keys must be unique")
        if keys != sorted(keys):
            raise TelemetryValidationError("telemetry metric label keys must be sorted")

    def to_mapping(self) -> dict[str, object]:
        """Return the strict serializable view of this one series."""
        return {
            "name": self.name,
            "kind": self.kind.value,
            "value": self.value,
            "labels": [[key, value] for key, value in self.labels],
        }

    @classmethod
    def from_mapping(cls, data: object) -> TelemetryMetric:
        """Rebuild one series from a plain mapping, failing closed on any defect."""
        if type(data) is not dict:
            raise TelemetryValidationError("telemetry metric entry must be a mapping")
        fields = cast(dict[object, object], data)
        field_keys = set(fields)
        if len(field_keys) != len(_METRIC_MAPPING_KEYS) or not field_keys.issuperset(
            _METRIC_MAPPING_KEYS
        ):
            raise TelemetryValidationError("telemetry metric keys are missing or unknown")
        name = fields["name"]
        if type(name) is not str:
            raise TelemetryValidationError("telemetry metric name must be text")
        raw_kind = fields["kind"]
        if type(raw_kind) is not str:
            raise TelemetryValidationError("telemetry metric kind must be text")
        try:
            kind = TelemetryMetricKind(raw_kind)
        except ValueError as error:
            raise TelemetryValidationError("telemetry metric kind is unknown") from error
        value = fields["value"]
        if type(value) is not int:
            raise TelemetryValidationError("telemetry metric value must be an integer")
        raw_labels = fields["labels"]
        if type(raw_labels) is not list:
            raise TelemetryValidationError("telemetry metric labels must be a list")
        labels: list[tuple[str, str]] = []
        for pair in cast(list[object], raw_labels):
            if type(pair) is not list:
                raise TelemetryValidationError("telemetry metric labels must be key-value pairs")
            items = cast(list[object], pair)
            if len(items) != 2:
                raise TelemetryValidationError("telemetry metric labels must be key-value pairs")
            if type(items[0]) is not str or type(items[1]) is not str:
                raise TelemetryValidationError("telemetry metric label pairs must be text")
            labels.append((items[0], items[1]))
        return cls(name=name, kind=kind, value=value, labels=tuple(labels))

    def __repr__(self) -> str:
        return (
            f"TelemetryMetric(name={self.name!r}, kind={self.kind.value!r}, "
            f"value={self.value!r}, labels={len(self.labels)})"
        )


@dataclass(frozen=True, slots=True, repr=False)
class TelemetryRecord:
    """One immutable versioned telemetry observation record.

    The observation time is an injected caller-supplied microsecond
    count and is never read from a clock. A record is a passive fact:
    it carries no handle, owner identity, credential, or path, and no
    consumer may treat it as durable state.
    """

    schema_version: int
    observed_at_micros: int
    run_id: str
    metrics: tuple[TelemetryMetric, ...]

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int:
            raise TelemetryValidationError("telemetry schema version must be an integer")
        if self.schema_version != TELEMETRY_SCHEMA_VERSION:
            raise TelemetrySchemaError("telemetry record uses an unknown schema version")
        if type(self.observed_at_micros) is not int:
            raise TelemetryValidationError("telemetry observation time must be an integer")
        if self.observed_at_micros < 0:
            raise TelemetryValidationError("telemetry observation time must not be negative")
        _validate_run_id(self.run_id)
        if type(self.metrics) is not tuple:
            raise TelemetryValidationError("telemetry record metrics must be a tuple")
        metrics = cast(tuple[object, ...], self.metrics)
        if not 1 <= len(metrics) <= MAX_SERIES_PER_RECORD:
            raise TelemetryValidationError(
                "telemetry record metric count is outside the supported range"
            )
        for metric in metrics:
            if type(metric) is not TelemetryMetric:
                raise TelemetryValidationError("telemetry record metrics must use TelemetryMetric")

    def to_mapping(self) -> dict[str, object]:
        """Return the strict serializable mapping view of this record."""
        return {
            "schema_version": self.schema_version,
            "observed_at_micros": self.observed_at_micros,
            "run_id": self.run_id,
            "metrics": [metric.to_mapping() for metric in self.metrics],
        }

    @classmethod
    def from_mapping(cls, data: object) -> TelemetryRecord:
        """Rebuild one record from a plain mapping, failing closed on any defect."""
        if type(data) is not dict:
            raise TelemetryValidationError("telemetry record payload must be a mapping")
        fields = cast(dict[object, object], data)
        field_keys = set(fields)
        if len(field_keys) != len(_RECORD_MAPPING_KEYS) or not field_keys.issuperset(
            _RECORD_MAPPING_KEYS
        ):
            raise TelemetryValidationError("telemetry record keys are missing or unknown")
        schema_version = fields["schema_version"]
        if type(schema_version) is not int:
            raise TelemetryValidationError("telemetry schema version must be an integer")
        if schema_version != TELEMETRY_SCHEMA_VERSION:
            raise TelemetrySchemaError("telemetry record uses an unknown schema version")
        observed_at_micros = fields["observed_at_micros"]
        if type(observed_at_micros) is not int:
            raise TelemetryValidationError("telemetry observation time must be an integer")
        raw_metrics = fields["metrics"]
        if type(raw_metrics) is not list:
            raise TelemetryValidationError("telemetry record metrics must be a list")
        metrics = tuple(
            TelemetryMetric.from_mapping(entry) for entry in cast(list[object], raw_metrics)
        )
        return cls(
            schema_version=schema_version,
            observed_at_micros=observed_at_micros,
            run_id=_validate_run_id(fields["run_id"]),
            metrics=metrics,
        )

    def wire_bytes(self) -> bytes:
        """Encode this record as deterministic canonical UTF-8 bytes.

        The layout is fixed: three header lines for schema version,
        observation time, and run identity, then one ``name|kind|value|labels``
        line per series in tuple order, with labels as ``key=value``
        pairs joined by ``;``. Label values escape the reserved wire
        characters so decoding is an exact strict inverse.
        """
        lines = [
            f"schema_version={self.schema_version}",
            f"observed_at_micros={self.observed_at_micros}",
            f"run_id={self.run_id}",
        ]
        for metric in self.metrics:
            labels = ";".join(
                f"{key}={_escape_wire_label_value(value)}" for key, value in metric.labels
            )
            lines.append(f"{metric.name}|{metric.kind.value}|{metric.value}|{labels}")
        payload = "\n".join(lines).encode("utf-8")
        if len(payload) > MAX_TELEMETRY_RECORD_BYTES:
            raise TelemetryValidationError("telemetry record exceeds the wire byte bound")
        return payload

    @classmethod
    def from_wire_bytes(cls, data: bytes) -> TelemetryRecord:
        """Decode strict canonical telemetry bytes, failing closed on any defect."""
        candidate = cast(object, data)
        if type(candidate) is not bytes:
            raise TelemetryValidationError("telemetry wire payload must be exact bytes")
        payload = data
        if len(payload) > MAX_TELEMETRY_RECORD_BYTES:
            raise TelemetryValidationError("telemetry wire payload exceeds the byte bound")
        try:
            text = payload.decode("ascii")
        except UnicodeDecodeError as error:
            raise TelemetryValidationError("telemetry wire payload must use ASCII") from error
        if not text:
            raise TelemetryValidationError("telemetry wire payload must not be empty")
        if text.endswith("\n"):
            raise TelemetryValidationError("telemetry wire payload must not end with a newline")
        lines = text.split("\n")
        if len(lines) < 4:
            raise TelemetryValidationError("telemetry wire payload is truncated")
        if len(lines) > 3 + MAX_SERIES_PER_RECORD:
            raise TelemetryValidationError("telemetry wire payload carries too many series")
        schema_version = _read_wire_header_int(lines[0], "schema_version")
        if schema_version != TELEMETRY_SCHEMA_VERSION:
            raise TelemetrySchemaError("telemetry wire payload uses an unknown schema version")
        observed_at_micros = _read_wire_header_int(lines[1], "observed_at_micros")
        run_id = _read_wire_header_text(lines[2], "run_id")
        metrics = tuple(_parse_wire_metric_line(line) for line in lines[3:])
        return cls(
            schema_version=schema_version,
            observed_at_micros=observed_at_micros,
            run_id=run_id,
            metrics=metrics,
        )

    def __repr__(self) -> str:
        return (
            f"TelemetryRecord(run_id={self.run_id!r}, "
            f"observed_at_micros={self.observed_at_micros!r}, metrics={len(self.metrics)})"
        )


def _validate_name_segment(value: object, subject: str) -> str:
    if type(value) is not str:
        raise TelemetryValidationError(f"{subject} must be text")
    text = value
    if not 1 <= len(text) <= _MAX_LABEL_KEY_LENGTH:
        raise TelemetryValidationError(f"{subject} length is outside the supported range")
    if _LABEL_KEY_PATTERN.fullmatch(text) is None:
        raise TelemetryValidationError(f"{subject} must use the bounded segment alphabet")
    return text


def queue_depth_record(
    run_id: str,
    observed_at_micros: int,
    kind_channel: str,
    depth: int,
) -> TelemetryRecord:
    """Observe the current depth of one bounded channel kind."""
    channel = _validate_name_segment(kind_channel, "telemetry channel kind")
    return TelemetryRecord(
        schema_version=TELEMETRY_SCHEMA_VERSION,
        observed_at_micros=observed_at_micros,
        run_id=run_id,
        metrics=(
            TelemetryMetric(
                name=f"{channel}_queue_depth",
                kind=TelemetryMetricKind.QUEUE_DEPTH,
                value=depth,
                labels=(("channel", channel),),
            ),
        ),
    )


def capacity_snapshot_records(
    run_id: str,
    observed_at_micros: int,
    snapshots: tuple[CapacitySnapshot, ...],
) -> tuple[TelemetryRecord, ...]:
    """Observe in-use capacity for each bounded capacity snapshot in order.

    Every snapshot produces exactly one record; nothing is skipped, and
    the snapshot's own category validation keeps the metric names inside
    the bounded metric alphabet.
    """
    if type(snapshots) is not tuple:
        raise TelemetryValidationError("telemetry capacity snapshots must be a tuple")
    records: list[TelemetryRecord] = []
    for snapshot in snapshots:
        if type(snapshot) is not CapacitySnapshot:
            raise TelemetryValidationError("telemetry capacity snapshots must use CapacitySnapshot")
        selected = snapshot
        records.append(
            TelemetryRecord(
                schema_version=TELEMETRY_SCHEMA_VERSION,
                observed_at_micros=observed_at_micros,
                run_id=run_id,
                metrics=(
                    TelemetryMetric(
                        name=f"{selected.category}_active_capacity",
                        kind=TelemetryMetricKind.ACTIVE_CAPACITY,
                        value=selected.in_use,
                        labels=(
                            ("category", selected.category),
                            ("limit", str(selected.limit)),
                        ),
                    ),
                ),
            )
        )
    return tuple(records)


def capacity_wait_record(
    run_id: str,
    observed_at_micros: int,
    category: str,
    waited_micros: int,
) -> TelemetryRecord:
    """Observe how long one capacity level was waited for, in microseconds."""
    selected_category = _validate_name_segment(category, "telemetry capacity category")
    if selected_category not in _CAPACITY_CATEGORIES:
        raise TelemetryValidationError(
            "telemetry capacity category must name a known capacity level"
        )
    return TelemetryRecord(
        schema_version=TELEMETRY_SCHEMA_VERSION,
        observed_at_micros=observed_at_micros,
        run_id=run_id,
        metrics=(
            TelemetryMetric(
                name=f"{selected_category}_capacity_wait",
                kind=TelemetryMetricKind.CAPACITY_WAIT,
                value=waited_micros,
                labels=(("category", selected_category),),
            ),
        ),
    )


def service_duration_record(
    run_id: str,
    observed_at_micros: int,
    operation: str,
    duration_micros: int,
) -> TelemetryRecord:
    """Observe the service duration of one bounded operation, in microseconds."""
    selected_operation = _validate_name_segment(operation, "telemetry operation")
    return TelemetryRecord(
        schema_version=TELEMETRY_SCHEMA_VERSION,
        observed_at_micros=observed_at_micros,
        run_id=run_id,
        metrics=(
            TelemetryMetric(
                name=f"{selected_operation}_service_duration",
                kind=TelemetryMetricKind.SERVICE_DURATION,
                value=duration_micros,
                labels=(("operation", selected_operation),),
            ),
        ),
    )


def blocked_writer_record(
    run_id: str,
    observed_at_micros: int,
    blocked: bool,
) -> TelemetryRecord:
    """Observe whether the downstream writer flow is currently blocked."""
    if type(blocked) is not bool:
        raise TelemetryValidationError("telemetry blocked-writer flag must be a boolean")
    return TelemetryRecord(
        schema_version=TELEMETRY_SCHEMA_VERSION,
        observed_at_micros=observed_at_micros,
        run_id=run_id,
        metrics=(
            TelemetryMetric(
                name="blocked_writer",
                kind=TelemetryMetricKind.BLOCKED_WRITER,
                value=1 if blocked else 0,
                labels=(),
            ),
        ),
    )


def dropped_telemetry_record(
    run_id: str,
    observed_at_micros: int,
    dropped_count: int,
) -> TelemetryRecord:
    """Observe how many telemetry records were dropped by sampling policy."""
    return TelemetryRecord(
        schema_version=TELEMETRY_SCHEMA_VERSION,
        observed_at_micros=observed_at_micros,
        run_id=run_id,
        metrics=(
            TelemetryMetric(
                name="dropped_telemetry",
                kind=TelemetryMetricKind.DROPPED_TELEMETRY,
                value=dropped_count,
                labels=(),
            ),
        ),
    )


def cleanup_state_record(
    run_id: str,
    observed_at_micros: int,
    state: str,
) -> TelemetryRecord:
    """Observe one bounded idempotent cleanup state."""
    if type(state) is not str:
        raise TelemetryValidationError("telemetry cleanup state must be text")
    if state not in _CLEANUP_STATES:
        raise TelemetryValidationError("telemetry cleanup state is unknown")
    return TelemetryRecord(
        schema_version=TELEMETRY_SCHEMA_VERSION,
        observed_at_micros=observed_at_micros,
        run_id=run_id,
        metrics=(
            TelemetryMetric(
                name="cleanup_state",
                kind=TelemetryMetricKind.CLEANUP_STATE,
                value=1,
                labels=(("state", state),),
            ),
        ),
    )


def unresolved_resources_record(
    run_id: str,
    observed_at_micros: int,
    count: int,
) -> TelemetryRecord:
    """Observe the count of resources left unresolved by cleanup."""
    return TelemetryRecord(
        schema_version=TELEMETRY_SCHEMA_VERSION,
        observed_at_micros=observed_at_micros,
        run_id=run_id,
        metrics=(
            TelemetryMetric(
                name="unresolved_resources",
                kind=TelemetryMetricKind.UNRESOLVED_RESOURCES,
                value=count,
                labels=(),
            ),
        ),
    )


def _escape_wire_label_value(value: str) -> str:
    return "".join(
        f"\\{character}" if character in _WIRE_RESERVED_CHARACTERS else character
        for character in value
    )


def _read_wire_header_int(line: str, key: str) -> int:
    prefix = f"{key}="
    if not line.startswith(prefix):
        raise TelemetryValidationError("telemetry wire field key or order is invalid")
    digits = line[len(prefix) :]
    if not digits or not all("0" <= character <= "9" for character in digits):
        raise TelemetryValidationError(f"telemetry wire field '{key}' is not an integer")
    if len(digits) > 1 and digits[0] == "0":
        raise TelemetryValidationError(f"telemetry wire field '{key}' has a leading zero")
    return int(digits)


def _read_wire_header_text(line: str, key: str) -> str:
    prefix = f"{key}="
    if not line.startswith(prefix):
        raise TelemetryValidationError("telemetry wire field key or order is invalid")
    return line[len(prefix) :]


def _split_wire_metric_fields(line: str) -> list[str]:
    """Split one series line on unescaped ``|`` separators, keeping escapes.

    Escape pairs are preserved verbatim for the per-field parsers: the
    label-value reader validates each escape, and the name, kind, and
    value fields reject any backslash through their own alphabets.
    """
    fields: list[str] = []
    current: list[str] = []
    index = 0
    while index < len(line):
        character = line[index]
        if character == "\\" and index + 1 < len(line):
            current.append(character)
            current.append(line[index + 1])
            index += 2
            continue
        if character == "|":
            fields.append("".join(current))
            current = []
            index += 1
            continue
        current.append(character)
        index += 1
    fields.append("".join(current))
    return fields


def _parse_wire_labels(field: str) -> tuple[tuple[str, str], ...]:
    if not field:
        return ()
    pairs: list[tuple[str, str]] = []
    index = 0
    while True:
        key_characters: list[str] = []
        while index < len(field) and field[index] != "=" and field[index] != ";":
            key_characters.append(field[index])
            index += 1
        if index >= len(field) or field[index] != "=" or not key_characters:
            raise TelemetryValidationError("telemetry wire label key is malformed")
        index += 1
        value_characters: list[str] = []
        while index < len(field):
            character = field[index]
            if character == "\\":
                if index + 1 >= len(field) or field[index + 1] not in _WIRE_RESERVED_CHARACTERS:
                    raise TelemetryValidationError(
                        "telemetry wire label value has an invalid escape"
                    )
                value_characters.append(field[index + 1])
                index += 2
                continue
            if character == ";":
                break
            if character in _WIRE_RESERVED_CHARACTERS:
                raise TelemetryValidationError(
                    "telemetry wire label value must escape reserved characters"
                )
            value_characters.append(character)
            index += 1
        pairs.append(("".join(key_characters), "".join(value_characters)))
        if index >= len(field):
            return tuple(pairs)
        index += 1
        if index >= len(field):
            raise TelemetryValidationError("telemetry wire labels end with a separator")


def _parse_wire_metric_line(line: str) -> TelemetryMetric:
    fields = _split_wire_metric_fields(line)
    if len(fields) != 4:
        raise TelemetryValidationError("telemetry wire series line must carry four fields")
    try:
        kind = TelemetryMetricKind(fields[1])
    except ValueError as error:
        raise TelemetryValidationError("telemetry wire series kind is unknown") from error
    digits = fields[2]
    if not digits or not all("0" <= character <= "9" for character in digits):
        raise TelemetryValidationError("telemetry wire series value is not an integer")
    if len(digits) > 1 and digits[0] == "0":
        raise TelemetryValidationError("telemetry wire series value has a leading zero")
    labels = _parse_wire_labels(fields[3])
    return TelemetryMetric(name=fields[0], kind=kind, value=int(digits), labels=labels)


class TelemetryCollector:
    """Bounded droppable non-authoritative telemetry sink for one run.

    The collector accepts records faster than any consumer reads them.
    When the buffer is full it drops the oldest record to make room,
    counts the drop, and never blocks and never grows beyond the
    configured capacity. It holds no reference to any scheduling,
    persistence, or credential object and cannot mutate anything
    outside its own buffer.
    """

    __slots__ = ("_buffer", "_capacity", "_dropped", "_lock", "_run_id")

    def __init__(self, *, run_id: str, capacity: int) -> None:
        self._run_id = _validate_run_id(run_id)
        if type(capacity) is not int:
            raise TelemetryValidationError("telemetry collector capacity must be an integer")
        if not 1 <= capacity <= MAX_COLLECTOR_CAPACITY:
            raise TelemetryValidationError(
                "telemetry collector capacity is outside the supported range"
            )
        self._capacity = capacity
        self._buffer: deque[TelemetryRecord] = deque()
        self._dropped = 0
        self._lock = Lock()

    def __repr__(self) -> str:
        with self._lock:
            return (
                f"TelemetryCollector(run_id={self._run_id!r}, "
                f"capacity={self._capacity!r}, buffered={len(self._buffer)!r}, "
                f"dropped={self._dropped!r})"
            )

    @property
    def run_id(self) -> str:
        """Return the bounded run identity this collector observes."""
        return self._run_id

    @property
    def capacity(self) -> int:
        """Return the explicit finite buffer capacity."""
        return self._capacity

    def emit(self, record: TelemetryRecord) -> None:
        """Accept one record, dropping the oldest buffered record when full.

        This never blocks and never grows the buffer beyond the
        configured capacity; an overflow increments the reported drop
        count instead of raising.
        """
        if type(record) is not TelemetryRecord:
            raise TelemetryValidationError(
                "telemetry collector accepts exact TelemetryRecord values"
            )
        with self._lock:
            if len(self._buffer) >= self._capacity:
                self._buffer.popleft()
                self._dropped += 1
            self._buffer.append(record)

    def drain(self) -> tuple[TelemetryRecord, ...]:
        """Return and clear every buffered record in oldest-first order."""
        with self._lock:
            drained = tuple(self._buffer)
            self._buffer.clear()
            return drained

    def dropped_count(self) -> int:
        """Return how many records overflow has dropped so far."""
        with self._lock:
            return self._dropped

    def snapshot(self) -> tuple[int, int]:
        """Return the bounded (buffered, dropped) observability counts."""
        with self._lock:
            return (len(self._buffer), self._dropped)

    def dropped_record(self, observed_at_micros: int) -> TelemetryRecord | None:
        """Build a dropped-telemetry observation for the current drop count.

        Returns ``None`` while nothing has been dropped; the observation
        time is injected by the caller because the collector never reads
        a clock.
        """
        with self._lock:
            dropped = self._dropped
            run_id = self._run_id
        if dropped == 0:
            return None
        return dropped_telemetry_record(
            run_id=run_id,
            observed_at_micros=observed_at_micros,
            dropped_count=dropped,
        )


__all__ = [
    "MAX_COLLECTOR_CAPACITY",
    "MAX_LABEL_COUNT",
    "MAX_LABEL_LENGTH",
    "MAX_METRIC_NAME_LENGTH",
    "MAX_METRIC_VALUE",
    "MAX_SERIES_PER_RECORD",
    "MAX_TELEMETRY_RECORD_BYTES",
    "TELEMETRY_SCHEMA_VERSION",
    "TelemetryCollector",
    "TelemetryError",
    "TelemetryMetric",
    "TelemetryMetricKind",
    "TelemetryRecord",
    "TelemetryRedactionError",
    "TelemetrySchemaError",
    "TelemetryValidationError",
    "blocked_writer_record",
    "capacity_snapshot_records",
    "capacity_wait_record",
    "cleanup_state_record",
    "dropped_telemetry_record",
    "queue_depth_record",
    "service_duration_record",
    "unresolved_resources_record",
]
