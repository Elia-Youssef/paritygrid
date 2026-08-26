"""Runner-neutral contract version 1 envelopes, bounds, and wire encoding."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

RUNNER_CONTRACT_VERSION = 1
WORK_ASSIGNMENT_PROTOCOL = "paritygrid.work-assignment.v1"
WORK_RESULT_PROTOCOL = "paritygrid.work-result.v1"
RECOVERY_FRONTIER_PROTOCOL = "paritygrid.recovery-frontier.v1"
STRATEGY_CAPABILITIES_PROTOCOL = "paritygrid.strategy-capabilities.v1"

MAX_CONTRACT_STRING_LENGTH = 256
MAX_CONTRACT_LIST_ITEMS = 64
MAX_CONTRACT_MAP_ENTRIES = 32
MAX_CONTRACT_NESTING_DEPTH = 4
MAX_CONTRACT_BYTES = 65_536
MAX_METRIC_VALUE = 2**31 - 1
MAX_FAILURE_DETAIL_LENGTH = 512

_MAX_STRATEGY_CONCURRENCY = 256
_MAX_PLATFORM_REQUIREMENTS = 8
_CONTRACT_DEADLINE_LENGTH = 27
_CONTRACT_DEADLINE_SEPARATORS: dict[int, str] = {
    4: "-",
    7: "-",
    10: "T",
    13: ":",
    16: ":",
    19: ".",
    26: "Z",
}

_FRONTIER_FINGERPRINT_PATTERN: re.Pattern[str] = re.compile(r"[0-9a-f]{64}")

_PUBLIC_TEXT_SECRET_MARKERS: tuple[str, ...] = ("password=", "token:", "bearer ")
_WIRE_RESERVED_CHARACTERS = frozenset("\\;={}[]")
_WIRE_TEXT_TERMINATORS = frozenset("\n;}]")
_WIRE_KEY_FORBIDDEN = frozenset("\n;{}[]")
_WIRE_SCALAR_LITERALS: tuple[str, ...] = ("none", "true", "false")


class RunnerContractError(RuntimeError):
    """Base failure for runner-neutral contract admission or encoding."""


class RunnerContractVersionError(RunnerContractError):
    """An envelope or strategy uses an unknown contract version."""


class RunnerContractEncodingError(RunnerContractError):
    """A contract payload is not a well-formed canonical encoding."""


class RunnerContractLeakError(RunnerContractError):
    """A contract value would carry a live object, secret, or filesystem path."""


class RunnerContractBoundError(RunnerContractError):
    """A contract value is outside the closed contract bounds."""


class RunnerContractLoopError(RunnerContractError):
    """A sync contract entry point was used inside an active event loop."""


class ContractLifecycleState(StrEnum):
    """Closed lifecycle control states shared by every runner strategy."""

    NEW = "new"
    OPEN = "open"
    RUNNING = "running"
    QUIESCING = "quiescing"
    PAUSED = "paused"
    CANCELLING = "cancelling"
    CLOSED = "closed"
    RECOVERY_REQUIRED = "recovery_required"


class ContractOutcome(StrEnum):
    """Closed terminal outcome of one work item attempt."""

    SUCCEEDED = "succeeded"
    RETRY_WAIT = "retry_wait"
    QUARANTINED = "quarantined"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ContractCleanupStatus(StrEnum):
    """Closed idempotent cleanup status for one work item attempt."""

    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


def _has_live_attribute(value: object, name: str) -> bool:
    try:
        return hasattr(value, name)
    except Exception:
        return True


def _reject_live_object(value: object, subject: str) -> None:
    if (
        isinstance(value, BaseException)
        or callable(value)
        or _has_live_attribute(value, "__await__")
        or _has_live_attribute(value, "__fspath__")
        or _has_live_attribute(value, "aclose")
        or _has_live_attribute(value, "result")
        or _has_live_attribute(value, "execute")
        or _has_live_attribute(value, "qsize")
    ):
        raise RunnerContractLeakError(f"{subject} must not carry a live object")


def _validate_contract_text(
    value: object,
    subject: str,
    maximum: int = MAX_CONTRACT_STRING_LENGTH,
) -> None:
    _reject_live_object(value, subject)
    if type(value) is not str:
        raise TypeError(f"{subject} must be text")
    text = value
    if not 1 <= len(text) <= maximum:
        raise RunnerContractBoundError(f"{subject} length is outside the supported range")
    for character in text:
        if not "\x20" <= character <= "\x7e":
            raise RunnerContractBoundError(f"{subject} must use printable ASCII characters")


def _reject_public_text_markers(text: str, subject: str) -> None:
    lowered = text.lower()
    for marker in _PUBLIC_TEXT_SECRET_MARKERS:
        if marker in lowered:
            raise RunnerContractLeakError(f"{subject} must not contain secret material")
    if text.startswith(("/", "\\")):
        raise RunnerContractLeakError(f"{subject} must not be an absolute path")
    if len(text) >= 2 and text[1] == ":" and "A" <= text[0].upper() <= "Z":
        raise RunnerContractLeakError(f"{subject} must not be a filesystem path")
    if ".." in text:
        raise RunnerContractLeakError(f"{subject} must not contain path traversal")


def validate_public_contract_text(value: object, subject: str) -> None:
    """Reject contract text that carries secrets or unrestricted path shapes."""
    _validate_contract_text(value, subject)
    _reject_public_text_markers(cast(str, value), subject)


def _validate_contract_int(
    value: object,
    minimum: int,
    maximum: int,
    subject: str,
) -> None:
    _reject_live_object(value, subject)
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    number = value
    if not minimum <= number <= maximum:
        raise RunnerContractBoundError(f"{subject} is outside the supported range")


def _validate_contract_bool(value: object, subject: str) -> None:
    _reject_live_object(value, subject)
    if type(value) is not bool:
        raise TypeError(f"{subject} must be a boolean")


def _validate_contract_enum[E: StrEnum](value: object, enum_type: type[E], subject: str) -> None:
    _reject_live_object(value, subject)
    if type(value) is not enum_type:
        raise TypeError(f"{subject} must use {enum_type.__name__}")


def _validate_contract_version(value: object, subject: str) -> None:
    _reject_live_object(value, subject)
    if type(value) is not int:
        raise TypeError(f"{subject} must be an integer")
    if value != RUNNER_CONTRACT_VERSION:
        raise RunnerContractVersionError(f"{subject} must equal {RUNNER_CONTRACT_VERSION}")


def _validate_public_text_tuple(
    value: object,
    subject: str,
    maximum: int = MAX_CONTRACT_LIST_ITEMS,
) -> tuple[str, ...]:
    _reject_live_object(value, subject)
    if type(value) is not tuple:
        raise TypeError(f"{subject} must be a tuple")
    values = cast(tuple[object, ...], value)
    if len(values) > maximum:
        raise RunnerContractBoundError(f"{subject} exceeds the supported item bound")
    for item in values:
        validate_public_contract_text(item, f"{subject} item")
    return cast(tuple[str, ...], values)


def _validate_deadline_utc(value: object, subject: str) -> None:
    _reject_live_object(value, subject)
    if type(value) is not str:
        raise TypeError(f"{subject} must be text")
    text = value
    if len(text) != _CONTRACT_DEADLINE_LENGTH:
        raise RunnerContractBoundError(f"{subject} must use the 27-character UTC form")
    separators = _CONTRACT_DEADLINE_SEPARATORS
    for index in range(_CONTRACT_DEADLINE_LENGTH):
        separator = separators.get(index)
        character = text[index]
        if separator is not None:
            if character != separator:
                raise RunnerContractBoundError(f"{subject} separators are misplaced")
        elif not ("0" <= character <= "9"):
            raise RunnerContractBoundError(f"{subject} must use positional digits")
    month = int(text[5:7])
    day = int(text[8:10])
    hour = int(text[11:13])
    minute = int(text[14:16])
    second = int(text[17:19])
    if not 1 <= month <= 12 or not 1 <= day <= 31 or hour > 23 or minute > 59 or second > 59:
        raise RunnerContractBoundError(f"{subject} is not a plausible UTC instant")


def _validate_frontier_fingerprint(value: object, subject: str) -> None:
    _reject_live_object(value, subject)
    if type(value) is not str:
        raise TypeError(f"{subject} must be text")
    if _FRONTIER_FINGERPRINT_PATTERN.fullmatch(value) is None:
        raise RunnerContractBoundError(f"{subject} must use 64 lowercase hexadecimal characters")


@dataclass(frozen=True, slots=True)
class ControlGeneration:
    """Immutable fencing generation shared by pause, cancel, and results."""

    value: int

    def __post_init__(self) -> None:
        _validate_contract_int(self.value, 1, MAX_METRIC_VALUE, "control generation")


type ContractValue = ContractDocument | ContractList | bool | int | str | None


def _validate_contract_value(value: object, depth: int, subject: str) -> None:
    _reject_live_object(value, f"{subject} value")
    if value is None:
        return
    if type(value) is bool:
        return
    if type(value) is int:
        number = value
        if not 0 <= number <= MAX_METRIC_VALUE:
            raise RunnerContractBoundError(
                f"{subject} integer value is outside the supported range"
            )
        return
    if type(value) is str:
        text = value
        _validate_contract_text(text, f"{subject} string value")
        _reject_public_text_markers(text, f"{subject} string value")
        return
    if type(value) is ContractList:
        _validate_contract_values(value.values, depth, f"{subject} list")
        return
    if type(value) is ContractDocument:
        _validate_document_items(value.items, depth, f"{subject} document")
        return
    raise TypeError(f"{subject} value must use the closed contract value types")


def _validate_contract_values(values: object, depth: int, subject: str) -> None:
    if depth > MAX_CONTRACT_NESTING_DEPTH:
        raise RunnerContractBoundError(f"{subject} exceeds the contract nesting depth bound")
    if type(values) is not tuple:
        raise TypeError(f"{subject} values must be a tuple")
    items = cast(tuple[object, ...], values)
    if len(items) > MAX_CONTRACT_LIST_ITEMS:
        raise RunnerContractBoundError(f"{subject} exceeds the contract item bound")
    for item in items:
        _validate_contract_value(item, depth + 1, subject)


def _validate_document_items(items: object, depth: int, subject: str) -> None:
    if depth > MAX_CONTRACT_NESTING_DEPTH:
        raise RunnerContractBoundError(f"{subject} exceeds the contract nesting depth bound")
    if type(items) is not tuple:
        raise TypeError(f"{subject} items must be a tuple")
    entries = cast(tuple[object, ...], items)
    if len(entries) > MAX_CONTRACT_MAP_ENTRIES:
        raise RunnerContractBoundError(f"{subject} exceeds the contract map entry bound")
    keys: list[str] = []
    for entry in entries:
        if type(entry) is not tuple:
            raise TypeError(f"{subject} entries must be pairs")
        pair = cast(tuple[object, ...], entry)
        if len(pair) != 2:
            raise RunnerContractBoundError(f"{subject} entries must be key-value pairs")
        key = pair[0]
        if type(key) is not str:
            raise TypeError(f"{subject} keys must be text")
        validate_public_contract_text(key, f"{subject} key")
        keys.append(key)
        _validate_contract_value(pair[1], depth + 1, subject)
    if len(set(keys)) != len(keys):
        raise RunnerContractBoundError(f"{subject} keys must be unique")


@dataclass(frozen=True, slots=True)
class ContractList:
    """Bounded list of closed contract values."""

    values: tuple[ContractValue, ...] = ()

    def __post_init__(self) -> None:
        _validate_contract_values(self.values, 1, "contract list")


@dataclass(frozen=True, slots=True, repr=False)
class ContractDocument:
    """Bounded recursive key-value document for operation descriptors."""

    items: tuple[tuple[str, ContractValue], ...] = ()

    def __post_init__(self) -> None:
        _validate_document_items(self.items, 1, "contract document")

    @property
    def item_count(self) -> int:
        """Return the number of top-level document entries."""
        return len(self.items)

    def to_mapping(self) -> dict[str, object]:
        """Return a plain nested mapping view for deterministic encoding."""
        mapping: dict[str, object] = {}
        for key, value in self.items:
            mapping[key] = _contract_value_to_plain(value)
        return mapping

    def __repr__(self) -> str:
        return f"ContractDocument(items={len(self.items)})"


def _contract_value_to_plain(value: ContractValue) -> object:
    if value is None or type(value) is bool or type(value) is int or type(value) is str:
        return value
    if type(value) is ContractList:
        return [_contract_value_to_plain(item) for item in value.values]
    return {
        key: _contract_value_to_plain(item) for key, item in cast(ContractDocument, value).items
    }


@dataclass(frozen=True, slots=True)
class ContractMetric:
    """One bounded integer metric attached to a work result."""

    name: str
    value: int

    def __post_init__(self) -> None:
        validate_public_contract_text(self.name, "contract metric name")
        _validate_contract_int(self.value, 0, MAX_METRIC_VALUE, "contract metric value")


@dataclass(frozen=True, slots=True)
class ContractCleanupEvidence:
    """Idempotent cleanup evidence for one work item attempt."""

    status: ContractCleanupStatus
    actions: tuple[str, ...]
    idempotency_key: str | None

    def __post_init__(self) -> None:
        _validate_contract_enum(
            self.status,
            ContractCleanupStatus,
            "contract cleanup status",
        )
        actions = _validate_public_text_tuple(self.actions, "contract cleanup actions")
        if list(actions) != sorted(actions):
            raise RunnerContractBoundError("contract cleanup actions must be sorted")
        if len(set(actions)) != len(actions):
            raise RunnerContractBoundError("contract cleanup actions must be unique")
        _reject_live_object(self.idempotency_key, "contract cleanup idempotency key")
        if self.idempotency_key is not None:
            validate_public_contract_text(
                self.idempotency_key,
                "contract cleanup idempotency key",
            )


@dataclass(frozen=True, slots=True)
class StrategyCapabilitiesV1:
    """Immutable strategy admission capabilities for contract version 1."""

    strategy_id: str
    contract_version: int
    supports_pause: bool
    supports_cancel: bool
    supports_checkpoint: bool
    max_concurrent_work: int
    max_in_flight_records: int
    platform_requirements: tuple[str, ...]
    protocol: str

    def __post_init__(self) -> None:
        _reject_live_object(self.protocol, "strategy capabilities protocol")
        if type(self.protocol) is not str:
            raise TypeError("strategy capabilities protocol must be text")
        if self.protocol != STRATEGY_CAPABILITIES_PROTOCOL:
            raise RunnerContractVersionError("strategy capabilities protocol is unknown")
        _validate_contract_version(
            self.contract_version,
            "strategy capabilities contract version",
        )
        validate_public_contract_text(self.strategy_id, "strategy identifier")
        _validate_contract_bool(self.supports_pause, "strategy pause support")
        _validate_contract_bool(self.supports_cancel, "strategy cancel support")
        _validate_contract_bool(self.supports_checkpoint, "strategy checkpoint support")
        _validate_contract_int(
            self.max_concurrent_work,
            1,
            _MAX_STRATEGY_CONCURRENCY,
            "strategy concurrency limit",
        )
        _validate_contract_int(
            self.max_in_flight_records,
            1,
            MAX_METRIC_VALUE,
            "strategy in-flight limit",
        )
        _validate_public_text_tuple(
            self.platform_requirements,
            "strategy platform requirements",
            maximum=_MAX_PLATFORM_REQUIREMENTS,
        )


@dataclass(frozen=True, slots=True, repr=False)
class WorkAssignmentV1:
    """Immutable runner-neutral work assignment for contract version 1."""

    protocol: str
    contract_version: int
    plan_fingerprint: str
    run_id: str
    node_id: str
    partition_key: str
    work_item_id: str
    attempt_number: int
    lease_fence: int
    lease_owner: str
    control_generation: ControlGeneration
    deadline_utc: str
    operation_descriptor: ContractDocument
    input_references: tuple[str, ...]
    captured_settings_ref: str

    def __post_init__(self) -> None:
        _reject_live_object(self.protocol, "work assignment protocol")
        if type(self.protocol) is not str:
            raise TypeError("work assignment protocol must be text")
        if self.protocol != WORK_ASSIGNMENT_PROTOCOL:
            raise RunnerContractVersionError("work assignment protocol is unknown")
        _validate_contract_version(self.contract_version, "work assignment contract version")
        _validate_frontier_fingerprint(self.plan_fingerprint, "work assignment plan fingerprint")
        validate_public_contract_text(self.run_id, "work assignment run identity")
        validate_public_contract_text(self.node_id, "work assignment node identity")
        validate_public_contract_text(self.partition_key, "work assignment partition key")
        validate_public_contract_text(self.work_item_id, "work assignment item identity")
        _validate_contract_int(self.attempt_number, 1, MAX_METRIC_VALUE, "work assignment attempt")
        _validate_contract_int(self.lease_fence, 1, MAX_METRIC_VALUE, "work assignment lease fence")
        validate_public_contract_text(self.lease_owner, "work assignment lease owner")
        _reject_live_object(self.control_generation, "work assignment control generation")
        if type(self.control_generation) is not ControlGeneration:
            raise TypeError("work assignment control generation must use ControlGeneration")
        _validate_deadline_utc(self.deadline_utc, "work assignment deadline")
        _reject_live_object(self.operation_descriptor, "work assignment operation descriptor")
        if type(self.operation_descriptor) is not ContractDocument:
            raise TypeError("work assignment operation descriptor must use ContractDocument")
        _validate_public_text_tuple(self.input_references, "work assignment input references")
        validate_public_contract_text(
            self.captured_settings_ref,
            "work assignment captured settings reference",
        )

    def work_identity(self) -> tuple[str, str, str]:
        """Return the durable scheduled work identity triple."""
        return (self.run_id, self.node_id, self.partition_key)

    def __repr__(self) -> str:
        return (
            "WorkAssignmentV1("
            "plan_fingerprint=<redacted>, "
            f"run_id={self.run_id!r}, node_id={self.node_id!r}, "
            f"partition_key={self.partition_key!r}, work_item_id={self.work_item_id!r}, "
            f"attempt_number={self.attempt_number!r}, lease_fence={self.lease_fence!r}, "
            "lease_owner=<redacted>, "
            f"control_generation={self.control_generation.value!r}, "
            f"deadline_utc={self.deadline_utc!r}, "
            f"input_references={len(self.input_references)})"
        )


def _validate_metrics(value: object, subject: str) -> tuple[ContractMetric, ...]:
    _reject_live_object(value, subject)
    if type(value) is not tuple:
        raise TypeError(f"{subject} must be a tuple")
    values = cast(tuple[object, ...], value)
    if len(values) > MAX_CONTRACT_LIST_ITEMS:
        raise RunnerContractBoundError(f"{subject} exceeds the contract item bound")
    for item in values:
        _reject_live_object(item, f"{subject} item")
        if type(item) is not ContractMetric:
            raise TypeError(f"{subject} item must use ContractMetric")
    return cast(tuple[ContractMetric, ...], values)


@dataclass(frozen=True, slots=True, repr=False)
class WorkResultV1:
    """Immutable runner-neutral work result for contract version 1."""

    protocol: str
    contract_version: int
    plan_fingerprint: str
    run_id: str
    node_id: str
    partition_key: str
    work_item_id: str
    attempt_number: int
    lease_fence: int
    lease_owner: str
    control_generation: ControlGeneration
    outcome: ContractOutcome
    metrics: tuple[ContractMetric, ...]
    artifact_references: tuple[str, ...]
    checkpoint_proposal: bool
    failure_detail: str | None
    cleanup: ContractCleanupEvidence

    def __post_init__(self) -> None:
        _reject_live_object(self.protocol, "work result protocol")
        if type(self.protocol) is not str:
            raise TypeError("work result protocol must be text")
        if self.protocol != WORK_RESULT_PROTOCOL:
            raise RunnerContractVersionError("work result protocol is unknown")
        _validate_contract_version(self.contract_version, "work result contract version")
        _validate_frontier_fingerprint(self.plan_fingerprint, "work result plan fingerprint")
        validate_public_contract_text(self.run_id, "work result run identity")
        validate_public_contract_text(self.node_id, "work result node identity")
        validate_public_contract_text(self.partition_key, "work result partition key")
        validate_public_contract_text(self.work_item_id, "work result item identity")
        _validate_contract_int(self.attempt_number, 1, MAX_METRIC_VALUE, "work result attempt")
        _validate_contract_int(self.lease_fence, 1, MAX_METRIC_VALUE, "work result lease fence")
        validate_public_contract_text(self.lease_owner, "work result lease owner")
        _reject_live_object(self.control_generation, "work result control generation")
        if type(self.control_generation) is not ControlGeneration:
            raise TypeError("work result control generation must use ControlGeneration")
        _validate_contract_enum(self.outcome, ContractOutcome, "work result outcome")
        _validate_metrics(self.metrics, "work result metrics")
        _validate_public_text_tuple(self.artifact_references, "work result artifact references")
        _validate_contract_bool(self.checkpoint_proposal, "work result checkpoint proposal")
        _reject_live_object(self.failure_detail, "work result failure detail")
        if self.failure_detail is not None:
            _validate_contract_text(
                self.failure_detail,
                "work result failure detail",
                maximum=MAX_FAILURE_DETAIL_LENGTH,
            )
            _reject_public_text_markers(
                self.failure_detail,
                "work result failure detail",
            )
        _reject_live_object(self.cleanup, "work result cleanup evidence")
        if type(self.cleanup) is not ContractCleanupEvidence:
            raise TypeError("work result cleanup evidence must use ContractCleanupEvidence")

    def work_identity(self) -> tuple[str, str, str]:
        """Return the durable scheduled work identity triple."""
        return (self.run_id, self.node_id, self.partition_key)

    def __repr__(self) -> str:
        return (
            "WorkResultV1("
            "plan_fingerprint=<redacted>, "
            f"run_id={self.run_id!r}, node_id={self.node_id!r}, "
            f"partition_key={self.partition_key!r}, work_item_id={self.work_item_id!r}, "
            f"attempt_number={self.attempt_number!r}, lease_fence={self.lease_fence!r}, "
            "lease_owner=<redacted>, "
            f"control_generation={self.control_generation.value!r}, "
            f"outcome={self.outcome.value!r}, metrics={len(self.metrics)}, "
            f"artifact_references={len(self.artifact_references)}, "
            f"checkpoint_proposal={self.checkpoint_proposal!r}, "
            "failure_detail=<redacted>, "
            f"cleanup_status={self.cleanup.status.value!r})"
        )


def _validate_work_identity_triples(
    value: object,
    subject: str,
) -> tuple[tuple[str, str, str], ...]:
    _reject_live_object(value, subject)
    if type(value) is not tuple:
        raise TypeError(f"{subject} must be a tuple")
    values = cast(tuple[object, ...], value)
    if len(values) > MAX_CONTRACT_LIST_ITEMS:
        raise RunnerContractBoundError(f"{subject} exceeds the contract item bound")
    triples: list[tuple[str, str, str]] = []
    for item in values:
        _reject_live_object(item, f"{subject} entry")
        if type(item) is not tuple:
            raise TypeError(f"{subject} entries must be tuples")
        parts = cast(tuple[object, ...], item)
        if len(parts) != 3:
            raise RunnerContractBoundError(f"{subject} entries must be identity triples")
        for part in parts:
            validate_public_contract_text(part, f"{subject} triple element")
        triples.append((cast(str, parts[0]), cast(str, parts[1]), cast(str, parts[2])))
    if len(set(triples)) != len(triples):
        raise RunnerContractBoundError(f"{subject} entries must be unique")
    if triples != sorted(triples):
        raise RunnerContractBoundError(f"{subject} entries must be sorted")
    return tuple(triples)


@dataclass(frozen=True, slots=True, repr=False)
class RecoveryFrontierV1:
    """Immutable durable recovery frontier for contract version 1."""

    protocol: str
    contract_version: int
    plan_fingerprint: str
    control_generation: ControlGeneration
    completed_work: tuple[tuple[str, str, str], ...]
    unresolved_work: tuple[tuple[str, str, str], ...]
    recovery_required_reason: str | None
    state: ContractLifecycleState

    def __post_init__(self) -> None:
        _reject_live_object(self.protocol, "recovery frontier protocol")
        if type(self.protocol) is not str:
            raise TypeError("recovery frontier protocol must be text")
        if self.protocol != RECOVERY_FRONTIER_PROTOCOL:
            raise RunnerContractVersionError("recovery frontier protocol is unknown")
        _validate_contract_version(self.contract_version, "recovery frontier contract version")
        _validate_frontier_fingerprint(self.plan_fingerprint, "recovery frontier fingerprint")
        _reject_live_object(self.control_generation, "recovery frontier control generation")
        if type(self.control_generation) is not ControlGeneration:
            raise TypeError("recovery frontier control generation must use ControlGeneration")
        _validate_work_identity_triples(self.completed_work, "recovery frontier completed work")
        _validate_work_identity_triples(self.unresolved_work, "recovery frontier unresolved work")
        _reject_live_object(self.recovery_required_reason, "recovery frontier reason")
        if self.recovery_required_reason is not None:
            validate_public_contract_text(
                self.recovery_required_reason,
                "recovery frontier reason",
            )
        _validate_contract_enum(
            self.state,
            ContractLifecycleState,
            "recovery frontier state",
        )

    def __repr__(self) -> str:
        return (
            "RecoveryFrontierV1("
            f"plan_fingerprint={self.plan_fingerprint!r}, "
            f"control_generation={self.control_generation.value!r}, "
            f"completed_work={len(self.completed_work)}, "
            f"unresolved_work={len(self.unresolved_work)}, "
            f"state={self.state.value!r})"
        )


def _escape_wire_text(text: str) -> str:
    return "".join(
        f"\\{character}" if character in _WIRE_RESERVED_CHARACTERS else character
        for character in text
    )


def _encode_wire_value(value: ContractValue) -> str:
    if value is None:
        return "none"
    if type(value) is bool:
        return "true" if value else "false"
    if type(value) is int:
        return f"int:{value}"
    if type(value) is str:
        return f"str:{_escape_wire_text(value)}"
    if type(value) is ContractList:
        return "list:[" + ";".join(_encode_wire_value(item) for item in value.values) + "]"
    if type(value) is ContractDocument:
        return "doc:{" + _encode_document_body(value) + "}"
    raise TypeError("contract value must use the closed contract value types")


def _encode_document_body(document: ContractDocument) -> str:
    return "\n".join(
        f"{_escape_wire_text(key)}={_encode_wire_value(value)}"
        for key, value in sorted(document.items, key=lambda entry: entry[0])
    )


def _require_value_delimiter(text: str, pos: int, subject: str) -> None:
    if pos < len(text) and text[pos] not in _WIRE_TEXT_TERMINATORS:
        raise RunnerContractEncodingError(f"{subject} is not delimited correctly")


def _read_wire_text(text: str, pos: int, subject: str) -> tuple[str, int]:
    characters: list[str] = []
    index = pos
    while index < len(text):
        character = text[index]
        if character == "\\":
            if index + 1 >= len(text) or text[index + 1] not in _WIRE_RESERVED_CHARACTERS:
                raise RunnerContractEncodingError(f"{subject} ends with an invalid escape")
            characters.append(text[index + 1])
            index += 2
            continue
        if character in _WIRE_TEXT_TERMINATORS:
            break
        if character in _WIRE_RESERVED_CHARACTERS:
            raise RunnerContractEncodingError(f"{subject} must escape reserved characters")
        characters.append(character)
        index += 1
    if not characters:
        raise RunnerContractEncodingError(f"{subject} must not be empty")
    return "".join(characters), index


def _read_wire_key(text: str, pos: int) -> tuple[str, int]:
    characters: list[str] = []
    index = pos
    while index < len(text):
        character = text[index]
        if character == "\\":
            if index + 1 >= len(text) or text[index + 1] not in _WIRE_RESERVED_CHARACTERS:
                raise RunnerContractEncodingError(
                    "contract payload key ends with an invalid escape"
                )
            characters.append(text[index + 1])
            index += 2
            continue
        if character == "=":
            break
        if character in _WIRE_KEY_FORBIDDEN:
            raise RunnerContractEncodingError("contract payload key is malformed")
        characters.append(character)
        index += 1
    if not characters:
        raise RunnerContractEncodingError("contract payload key must not be empty")
    return "".join(characters), index


def _build_wire_document(items: list[tuple[str, ContractValue]]) -> ContractDocument:
    if [key for key, _value in items] != sorted(key for key, _value in items):
        raise RunnerContractEncodingError("contract payload document keys are not canonical")
    try:
        return ContractDocument(items=tuple(items))
    except RunnerContractVersionError:
        raise
    except (RunnerContractError, TypeError, ValueError) as error:
        raise RunnerContractEncodingError("contract payload document is invalid") from error


def _build_wire_list(values: list[ContractValue]) -> ContractList:
    try:
        return ContractList(values=tuple(values))
    except RunnerContractVersionError:
        raise
    except (RunnerContractError, TypeError, ValueError) as error:
        raise RunnerContractEncodingError("contract payload list is invalid") from error


def _parse_contract_value(text: str, pos: int, depth: int) -> tuple[ContractValue, int]:
    if text.startswith("doc:{", pos):
        if depth > MAX_CONTRACT_NESTING_DEPTH:
            raise RunnerContractEncodingError(
                "contract payload nesting exceeds the contract depth bound"
            )
        items, end = _parse_document_body(text, pos + 5, depth)
        if end >= len(text) or text[end] != "}":
            raise RunnerContractEncodingError("contract payload document is not closed")
        return _build_wire_document(items), end + 1
    if text.startswith("list:[", pos):
        if depth > MAX_CONTRACT_NESTING_DEPTH:
            raise RunnerContractEncodingError(
                "contract payload nesting exceeds the contract depth bound"
            )
        index = pos + 6
        if index < len(text) and text[index] == "]":
            return _build_wire_list([]), index + 1
        values: list[ContractValue] = []
        while True:
            value, index = _parse_contract_value(text, index, depth + 1)
            values.append(value)
            if index >= len(text):
                raise RunnerContractEncodingError("contract payload list is not closed")
            if text[index] == "]":
                return _build_wire_list(values), index + 1
            if text[index] != ";":
                raise RunnerContractEncodingError("contract payload list separator is invalid")
            index += 1
    for literal in _WIRE_SCALAR_LITERALS:
        if text.startswith(literal, pos):
            end = pos + len(literal)
            _require_value_delimiter(text, end, f"contract payload scalar '{literal}'")
            if literal == "none":
                return None, end
            return literal == "true", end
    if text.startswith("int:", pos):
        index = pos + 4
        digits: list[str] = []
        while index < len(text) and "0" <= text[index] <= "9":
            digits.append(text[index])
            index += 1
        if not digits:
            raise RunnerContractEncodingError("contract payload integer is malformed")
        _require_value_delimiter(text, index, "contract payload integer")
        number_text = "".join(digits)
        if len(number_text) > 1 and number_text[0] == "0":
            raise RunnerContractEncodingError("contract payload integer has a leading zero")
        number = int(number_text)
        if number > MAX_METRIC_VALUE:
            raise RunnerContractEncodingError("contract payload integer exceeds the bound")
        return number, index
    if text.startswith("str:", pos):
        return _read_wire_text(text, pos + 4, "contract payload string value")
    raise RunnerContractEncodingError("contract payload value is malformed")


def _parse_document_body(
    text: str,
    pos: int,
    depth: int,
) -> tuple[list[tuple[str, ContractValue]], int]:
    items: list[tuple[str, ContractValue]] = []
    index = pos
    while index < len(text) and text[index] != "}":
        if items:
            if text[index] != "\n":
                raise RunnerContractEncodingError("contract payload document separator is invalid")
            index += 1
            if index >= len(text) or text[index] == "}":
                raise RunnerContractEncodingError("contract payload document item is empty")
        key, index = _read_wire_key(text, index)
        if index >= len(text) or text[index] != "=":
            raise RunnerContractEncodingError("contract payload document key is malformed")
        index += 1
        value, index = _parse_contract_value(text, index, depth + 1)
        items.append((key, value))
    return items, index


class _WireReader:
    """Strict sequential reader over one canonical contract payload."""

    __slots__ = ("_pos", "_text")

    def __init__(self, text: str) -> None:
        self._text = text
        self._pos = 0

    def read_line(self) -> str:
        index = self._text.find("\n", self._pos)
        if index == -1:
            line = self._text[self._pos :]
            self._pos = len(self._text)
            return line
        line = self._text[self._pos : index]
        self._pos = index + 1
        return line

    def read_text(self, key: str) -> str:
        if not self._text.startswith(f"{key}=", self._pos):
            raise RunnerContractEncodingError("contract payload field key or order is invalid")
        self._pos += len(key) + 1
        return self.read_line()

    def read_value(self, key: str) -> ContractValue:
        if not self._text.startswith(f"{key}=", self._pos):
            raise RunnerContractEncodingError("contract payload field key or order is invalid")
        self._pos += len(key) + 1
        value, pos = _parse_contract_value(self._text, self._pos, 1)
        self._pos = pos
        if self._pos < len(self._text):
            if self._text[self._pos] != "\n":
                raise RunnerContractEncodingError("contract payload field terminator is invalid")
            self._pos += 1
        return value

    def expect_end(self) -> None:
        if self._pos != len(self._text):
            raise RunnerContractEncodingError("contract payload has trailing content")


def _decode_wire_text(data: bytes) -> str:
    candidate = cast(object, data)
    if type(candidate) is not bytes:
        raise RunnerContractEncodingError("contract payload must be exact bytes")
    payload = candidate
    if len(payload) > MAX_CONTRACT_BYTES:
        raise RunnerContractEncodingError("contract payload exceeds the contract byte bound")
    try:
        text = payload.decode("ascii")
    except UnicodeDecodeError as error:
        raise RunnerContractEncodingError("contract payload must use ASCII") from error
    if text.endswith("\n"):
        raise RunnerContractEncodingError("contract payload must not end with a newline")
    return text


def _finish_envelope(lines: list[str]) -> bytes:
    payload = "\n".join(lines).encode("utf-8")
    if len(payload) > MAX_CONTRACT_BYTES:
        raise RunnerContractEncodingError("contract envelope exceeds the contract byte bound")
    return payload


def _reject_running_loop(subject: str) -> None:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    raise RunnerContractLoopError(f"{subject} must not run while an event loop is active")


def is_event_loop_running() -> bool:
    """Return whether an event loop is running in the current thread."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def encode_contract_document(document: ContractDocument) -> bytes:
    """Encode one contract document as deterministic canonical UTF-8 bytes."""
    _reject_running_loop("encode_contract_document")
    if type(document) is not ContractDocument:
        raise TypeError("contract document must use ContractDocument")
    return _encode_document_body(document).encode("utf-8")


def decode_contract_document(data: bytes) -> ContractDocument:
    """Decode strict canonical contract document bytes, failing closed."""
    _reject_running_loop("decode_contract_document")
    text = _decode_wire_text(data)
    items, end = _parse_document_body(text, 0, 1)
    if end != len(text):
        raise RunnerContractEncodingError("contract payload document has trailing content")
    return _build_wire_document(items)


def _encode_work_assignment(assignment: WorkAssignmentV1) -> bytes:
    if type(assignment) is not WorkAssignmentV1:
        raise TypeError("work assignment must use WorkAssignmentV1")
    return _finish_envelope(
        [
            WORK_ASSIGNMENT_PROTOCOL,
            f"contract_version={assignment.contract_version}",
            f"plan_fingerprint={assignment.plan_fingerprint}",
            f"run_id={assignment.run_id}",
            f"node_id={assignment.node_id}",
            f"partition_key={assignment.partition_key}",
            f"work_item_id={assignment.work_item_id}",
            f"attempt_number={assignment.attempt_number}",
            f"lease_fence={assignment.lease_fence}",
            f"lease_owner={assignment.lease_owner}",
            f"generation={assignment.control_generation.value}",
            f"deadline_utc={assignment.deadline_utc}",
            "operation_descriptor=doc:{"
            + _encode_document_body(assignment.operation_descriptor)
            + "}",
            "input_references=list:["
            + ";".join(
                f"str:{_escape_wire_text(reference)}" for reference in assignment.input_references
            )
            + "]",
            f"captured_settings_ref={assignment.captured_settings_ref}",
        ]
    )


def _encode_metric(metric: ContractMetric) -> str:
    document = ContractDocument(
        items=(("name", metric.name), ("value", metric.value)),
    )
    return "doc:{" + _encode_document_body(document) + "}"


def _encode_work_result(result: WorkResultV1) -> bytes:
    if type(result) is not WorkResultV1:
        raise TypeError("work result must use WorkResultV1")
    return _finish_envelope(
        [
            WORK_RESULT_PROTOCOL,
            f"contract_version={result.contract_version}",
            f"plan_fingerprint={result.plan_fingerprint}",
            f"run_id={result.run_id}",
            f"node_id={result.node_id}",
            f"partition_key={result.partition_key}",
            f"work_item_id={result.work_item_id}",
            f"attempt_number={result.attempt_number}",
            f"lease_fence={result.lease_fence}",
            f"lease_owner={result.lease_owner}",
            f"generation={result.control_generation.value}",
            f"outcome={result.outcome.value}",
            "metrics=list:[" + ";".join(_encode_metric(metric) for metric in result.metrics) + "]",
            "artifact_references=list:["
            + ";".join(
                f"str:{_escape_wire_text(reference)}" for reference in result.artifact_references
            )
            + "]",
            f"checkpoint_proposal={'true' if result.checkpoint_proposal else 'false'}",
            f"failure_detail={result.failure_detail if result.failure_detail is not None else ''}",
            "cleanup=doc:{" + _encode_cleanup_body(result.cleanup) + "}",
        ]
    )


def _encode_cleanup_body(cleanup: ContractCleanupEvidence) -> str:
    document = ContractDocument(
        items=(
            ("status", cleanup.status.value),
            ("actions", ContractList(values=tuple(cleanup.actions))),
            ("idempotency_key", cleanup.idempotency_key),
        ),
    )
    return _encode_document_body(document)


def _encode_work_identity_triples(triples: tuple[tuple[str, str, str], ...]) -> str:
    return (
        "list:["
        + ";".join(
            "list:[" + ";".join(f"str:{_escape_wire_text(part)}" for part in triple) + "]"
            for triple in triples
        )
        + "]"
    )


def _encode_recovery_frontier(frontier: RecoveryFrontierV1) -> bytes:
    if type(frontier) is not RecoveryFrontierV1:
        raise TypeError("recovery frontier must use RecoveryFrontierV1")
    return _finish_envelope(
        [
            RECOVERY_FRONTIER_PROTOCOL,
            f"contract_version={frontier.contract_version}",
            f"plan_fingerprint={frontier.plan_fingerprint}",
            f"generation={frontier.control_generation.value}",
            f"completed_work={_encode_work_identity_triples(frontier.completed_work)}",
            f"unresolved_work={_encode_work_identity_triples(frontier.unresolved_work)}",
            "recovery_required_reason="
            + (frontier.recovery_required_reason if frontier.recovery_required_reason else ""),
            f"state={frontier.state.value}",
        ]
    )


def _expect_protocol_line(reader: _WireReader, expected: str, subject: str) -> None:
    line = reader.read_line()
    if line == expected:
        return
    family = expected[: -len(".v1")]
    suffix = line[len(family) :] if line.startswith(family) else ""
    if (
        len(suffix) > 2
        and suffix.startswith(".v")
        and suffix[2:].isascii()
        and suffix[2:].isdigit()
    ):
        raise RunnerContractVersionError(f"{subject} payload uses an unknown contract version")
    raise RunnerContractEncodingError(f"{subject} payload uses an unknown protocol identifier")


def _read_contract_version(reader: _WireReader) -> int:
    raw = reader.read_text("contract_version")
    if not raw or not all("0" <= character <= "9" for character in raw):
        raise RunnerContractEncodingError("contract payload contract version is malformed")
    version = int(raw)
    if version != RUNNER_CONTRACT_VERSION:
        raise RunnerContractVersionError("contract payload uses an unknown contract version")
    return version


def _read_int_field(reader: _WireReader, key: str) -> int:
    raw = reader.read_text(key)
    if not raw or not all("0" <= character <= "9" for character in raw):
        raise RunnerContractEncodingError(f"contract payload field '{key}' is not an integer")
    if len(raw) > 1 and raw[0] == "0":
        raise RunnerContractEncodingError(f"contract payload field '{key}' has a leading zero")
    return int(raw)


def _read_bool_field(reader: _WireReader, key: str) -> bool:
    raw = reader.read_text(key)
    if raw == "true":
        return True
    if raw == "false":
        return False
    raise RunnerContractEncodingError(f"contract payload field '{key}' is not a boolean")


def _read_enum_field[E: StrEnum](reader: _WireReader, key: str, enum_type: type[E]) -> E:
    raw = reader.read_text(key)
    try:
        return enum_type(raw)
    except ValueError as error:
        raise RunnerContractEncodingError(
            f"contract payload field '{key}' is an unknown enum value"
        ) from error


def _read_optional_text_field(reader: _WireReader, key: str) -> str | None:
    raw = reader.read_text(key)
    return raw if raw else None


def _read_document_field(reader: _WireReader, key: str) -> ContractDocument:
    value = reader.read_value(key)
    if type(value) is not ContractDocument:
        raise RunnerContractEncodingError(f"contract payload field '{key}' must hold a document")
    return value


def _read_string_list_field(reader: _WireReader, key: str) -> tuple[str, ...]:
    value = reader.read_value(key)
    if type(value) is not ContractList:
        raise RunnerContractEncodingError(f"contract payload field '{key}' must hold a list")
    references: list[str] = []
    for item in value.values:
        if type(item) is not str:
            raise RunnerContractEncodingError(
                f"contract payload field '{key}' list items must be text"
            )
        references.append(item)
    return tuple(references)


def _read_metric_field(reader: _WireReader) -> tuple[ContractMetric, ...]:
    value = reader.read_value("metrics")
    if type(value) is not ContractList:
        raise RunnerContractEncodingError("contract payload field 'metrics' must hold a list")
    metrics: list[ContractMetric] = []
    for item in value.values:
        if type(item) is not ContractDocument:
            raise RunnerContractEncodingError("contract payload metric entries must be documents")
        fields = dict(item.items)
        if set(fields) != {"name", "value"}:
            raise RunnerContractEncodingError("contract payload metric entries are malformed")
        name = fields["name"]
        metric_value = fields["value"]
        if type(name) is not str or type(metric_value) is not int:
            raise RunnerContractEncodingError("contract payload metric entry types are invalid")
        metrics.append(ContractMetric(name=name, value=metric_value))
    return tuple(metrics)


def _read_cleanup_field(reader: _WireReader) -> ContractCleanupEvidence:
    value = reader.read_value("cleanup")
    if type(value) is not ContractDocument:
        raise RunnerContractEncodingError("contract payload field 'cleanup' must hold a document")
    fields = dict(value.items)
    if set(fields) != {"status", "actions", "idempotency_key"}:
        raise RunnerContractEncodingError("contract payload cleanup entries are malformed")
    status = fields["status"]
    actions = fields["actions"]
    idempotency_key = fields["idempotency_key"]
    if type(status) is not str:
        raise RunnerContractEncodingError("contract payload cleanup status must be text")
    if type(actions) is not ContractList:
        raise RunnerContractEncodingError("contract payload cleanup actions must be a list")
    action_items: list[str] = []
    for action in actions.values:
        if type(action) is not str:
            raise RunnerContractEncodingError("contract payload cleanup actions must be text")
        action_items.append(action)
    if idempotency_key is not None and type(idempotency_key) is not str:
        raise RunnerContractEncodingError("contract payload cleanup idempotency key must be text")
    try:
        cleanup_status = ContractCleanupStatus(status)
    except ValueError as error:
        raise RunnerContractEncodingError("contract payload cleanup status is unknown") from error
    try:
        return ContractCleanupEvidence(
            status=cleanup_status,
            actions=tuple(action_items),
            idempotency_key=idempotency_key,
        )
    except (RunnerContractError, TypeError, ValueError) as error:
        raise RunnerContractEncodingError("contract payload cleanup evidence is invalid") from error


def _read_triples_field(reader: _WireReader, key: str) -> tuple[tuple[str, str, str], ...]:
    value = reader.read_value(key)
    if type(value) is not ContractList:
        raise RunnerContractEncodingError(f"contract payload field '{key}' must hold a list")
    triples: list[tuple[str, str, str]] = []
    for item in value.values:
        if type(item) is not ContractList:
            raise RunnerContractEncodingError(
                f"contract payload field '{key}' entries must be lists"
            )
        parts: list[str] = []
        for element in item.values:
            if type(element) is not str:
                raise RunnerContractEncodingError(
                    f"contract payload field '{key}' triple elements must be text"
                )
            parts.append(element)
        if len(parts) != 3:
            raise RunnerContractEncodingError(
                f"contract payload field '{key}' entries must be identity triples"
            )
        triples.append((parts[0], parts[1], parts[2]))
    return tuple(triples)


def _decode_work_assignment(data: bytes) -> WorkAssignmentV1:
    reader = _WireReader(_decode_wire_text(data))
    _expect_protocol_line(reader, WORK_ASSIGNMENT_PROTOCOL, "work assignment")
    contract_version = _read_contract_version(reader)
    plan_fingerprint = reader.read_text("plan_fingerprint")
    run_id = reader.read_text("run_id")
    node_id = reader.read_text("node_id")
    partition_key = reader.read_text("partition_key")
    work_item_id = reader.read_text("work_item_id")
    attempt_number = _read_int_field(reader, "attempt_number")
    lease_fence = _read_int_field(reader, "lease_fence")
    lease_owner = reader.read_text("lease_owner")
    generation = _read_int_field(reader, "generation")
    deadline_utc = reader.read_text("deadline_utc")
    operation_descriptor = _read_document_field(reader, "operation_descriptor")
    input_references = _read_string_list_field(reader, "input_references")
    captured_settings_ref = reader.read_text("captured_settings_ref")
    reader.expect_end()
    try:
        return WorkAssignmentV1(
            protocol=WORK_ASSIGNMENT_PROTOCOL,
            contract_version=contract_version,
            plan_fingerprint=plan_fingerprint,
            run_id=run_id,
            node_id=node_id,
            partition_key=partition_key,
            work_item_id=work_item_id,
            attempt_number=attempt_number,
            lease_fence=lease_fence,
            lease_owner=lease_owner,
            control_generation=ControlGeneration(generation),
            deadline_utc=deadline_utc,
            operation_descriptor=operation_descriptor,
            input_references=input_references,
            captured_settings_ref=captured_settings_ref,
        )
    except RunnerContractVersionError:
        raise
    except (RunnerContractError, TypeError, ValueError) as error:
        raise RunnerContractEncodingError("work assignment payload is invalid") from error


def _decode_work_result(data: bytes) -> WorkResultV1:
    reader = _WireReader(_decode_wire_text(data))
    _expect_protocol_line(reader, WORK_RESULT_PROTOCOL, "work result")
    contract_version = _read_contract_version(reader)
    plan_fingerprint = reader.read_text("plan_fingerprint")
    run_id = reader.read_text("run_id")
    node_id = reader.read_text("node_id")
    partition_key = reader.read_text("partition_key")
    work_item_id = reader.read_text("work_item_id")
    attempt_number = _read_int_field(reader, "attempt_number")
    lease_fence = _read_int_field(reader, "lease_fence")
    lease_owner = reader.read_text("lease_owner")
    generation = _read_int_field(reader, "generation")
    outcome = _read_enum_field(reader, "outcome", ContractOutcome)
    metrics = _read_metric_field(reader)
    artifact_references = _read_string_list_field(reader, "artifact_references")
    checkpoint_proposal = _read_bool_field(reader, "checkpoint_proposal")
    failure_detail = _read_optional_text_field(reader, "failure_detail")
    cleanup = _read_cleanup_field(reader)
    reader.expect_end()
    try:
        return WorkResultV1(
            protocol=WORK_RESULT_PROTOCOL,
            contract_version=contract_version,
            plan_fingerprint=plan_fingerprint,
            run_id=run_id,
            node_id=node_id,
            partition_key=partition_key,
            work_item_id=work_item_id,
            attempt_number=attempt_number,
            lease_fence=lease_fence,
            lease_owner=lease_owner,
            control_generation=ControlGeneration(generation),
            outcome=outcome,
            metrics=metrics,
            artifact_references=artifact_references,
            checkpoint_proposal=checkpoint_proposal,
            failure_detail=failure_detail,
            cleanup=cleanup,
        )
    except RunnerContractVersionError:
        raise
    except (RunnerContractError, TypeError, ValueError) as error:
        raise RunnerContractEncodingError("work result payload is invalid") from error


def _decode_recovery_frontier(data: bytes) -> RecoveryFrontierV1:
    reader = _WireReader(_decode_wire_text(data))
    _expect_protocol_line(reader, RECOVERY_FRONTIER_PROTOCOL, "recovery frontier")
    contract_version = _read_contract_version(reader)
    plan_fingerprint = reader.read_text("plan_fingerprint")
    generation = _read_int_field(reader, "generation")
    completed_work = _read_triples_field(reader, "completed_work")
    unresolved_work = _read_triples_field(reader, "unresolved_work")
    recovery_required_reason = _read_optional_text_field(reader, "recovery_required_reason")
    state = _read_enum_field(reader, "state", ContractLifecycleState)
    reader.expect_end()
    try:
        return RecoveryFrontierV1(
            protocol=RECOVERY_FRONTIER_PROTOCOL,
            contract_version=contract_version,
            plan_fingerprint=plan_fingerprint,
            control_generation=ControlGeneration(generation),
            completed_work=completed_work,
            unresolved_work=unresolved_work,
            recovery_required_reason=recovery_required_reason,
            state=state,
        )
    except RunnerContractVersionError:
        raise
    except (RunnerContractError, TypeError, ValueError) as error:
        raise RunnerContractEncodingError("recovery frontier payload is invalid") from error


def encode_work_assignment(assignment: WorkAssignmentV1) -> bytes:
    """Encode one work assignment as deterministic canonical UTF-8 bytes."""
    _reject_running_loop("encode_work_assignment")
    return _encode_work_assignment(assignment)


def decode_work_assignment(data: bytes) -> WorkAssignmentV1:
    """Decode strict canonical work assignment bytes, failing closed."""
    _reject_running_loop("decode_work_assignment")
    return _decode_work_assignment(data)


def encode_work_result(result: WorkResultV1) -> bytes:
    """Encode one work result as deterministic canonical UTF-8 bytes."""
    _reject_running_loop("encode_work_result")
    return _encode_work_result(result)


def decode_work_result(data: bytes) -> WorkResultV1:
    """Decode strict canonical work result bytes, failing closed."""
    _reject_running_loop("decode_work_result")
    return _decode_work_result(data)


def encode_recovery_frontier(frontier: RecoveryFrontierV1) -> bytes:
    """Encode one recovery frontier as deterministic canonical UTF-8 bytes."""
    _reject_running_loop("encode_recovery_frontier")
    return _encode_recovery_frontier(frontier)


def decode_recovery_frontier(data: bytes) -> RecoveryFrontierV1:
    """Decode strict canonical recovery frontier bytes, failing closed."""
    _reject_running_loop("decode_recovery_frontier")
    return _decode_recovery_frontier(data)


def contract_wire_size(
    value: WorkAssignmentV1 | WorkResultV1 | RecoveryFrontierV1,
) -> int:
    """Return the deterministic canonical byte length of one contract envelope."""
    _reject_running_loop("contract_wire_size")
    candidate = cast(object, value)
    if type(candidate) is WorkAssignmentV1:
        return len(_encode_work_assignment(candidate))
    if type(candidate) is WorkResultV1:
        return len(_encode_work_result(candidate))
    if type(candidate) is RecoveryFrontierV1:
        return len(_encode_recovery_frontier(candidate))
    raise TypeError("contract wire size input must use a version 1 contract envelope")


async def decode_work_assignment_async(data: bytes) -> WorkAssignmentV1:
    """Decode strict work assignment bytes without blocking an active loop."""
    await asyncio.sleep(0)
    return _decode_work_assignment(data)


async def encode_work_result_async(result: WorkResultV1) -> bytes:
    """Encode strict work result bytes without blocking an active loop."""
    await asyncio.sleep(0)
    return _encode_work_result(result)


__all__ = [
    "MAX_CONTRACT_BYTES",
    "MAX_CONTRACT_LIST_ITEMS",
    "MAX_CONTRACT_MAP_ENTRIES",
    "MAX_CONTRACT_NESTING_DEPTH",
    "MAX_CONTRACT_STRING_LENGTH",
    "MAX_FAILURE_DETAIL_LENGTH",
    "MAX_METRIC_VALUE",
    "RECOVERY_FRONTIER_PROTOCOL",
    "RUNNER_CONTRACT_VERSION",
    "STRATEGY_CAPABILITIES_PROTOCOL",
    "WORK_ASSIGNMENT_PROTOCOL",
    "WORK_RESULT_PROTOCOL",
    "ContractCleanupEvidence",
    "ContractCleanupStatus",
    "ContractDocument",
    "ContractLifecycleState",
    "ContractList",
    "ContractMetric",
    "ContractOutcome",
    "ContractValue",
    "ControlGeneration",
    "RecoveryFrontierV1",
    "RunnerContractBoundError",
    "RunnerContractEncodingError",
    "RunnerContractError",
    "RunnerContractLeakError",
    "RunnerContractLoopError",
    "RunnerContractVersionError",
    "StrategyCapabilitiesV1",
    "WorkAssignmentV1",
    "WorkResultV1",
    "contract_wire_size",
    "decode_contract_document",
    "decode_recovery_frontier",
    "decode_work_assignment",
    "decode_work_assignment_async",
    "decode_work_result",
    "encode_contract_document",
    "encode_recovery_frontier",
    "encode_work_assignment",
    "encode_work_result",
    "encode_work_result_async",
    "is_event_loop_running",
    "validate_public_contract_text",
]
