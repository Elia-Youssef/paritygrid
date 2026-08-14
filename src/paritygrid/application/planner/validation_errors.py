"""Dependency-neutral human-readable pipeline validation contracts."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from paritygrid.application.planner.connectors import (
    ConnectorValidationError,
    MissingConnectorCapabilityError,
    MissingConnectorError,
    validate_connector_capabilities,
)
from paritygrid.application.planner.documents import PipelineDocument
from paritygrid.application.planner.graph import PipelineCycleError, validate_acyclic_graph
from paritygrid.application.planner.reachability import (
    DisconnectedPipelineError,
    InvalidPipelineTerminalError,
    validate_graph_reachability,
)
from paritygrid.application.planner.repair_safety import (
    RepairSafetyError,
    UnapprovedRepairEffectError,
    validate_repair_safety,
)
from paritygrid.application.planner.resources import (
    ResourcePolicyError,
    validate_resource_policy,
)
from paritygrid.application.ports.configuration import ConnectorRecord

VALIDATION_REPORT_VERSION = 1
MAX_VALIDATION_ISSUES = 64
MAX_VALIDATION_PATH_LENGTH = 256
MAX_VALIDATION_MESSAGE_LENGTH = 512

_VALIDATION_PATH_PATTERN = re.compile(
    r"/(?:[A-Za-z0-9_.~-]+(?:/[A-Za-z0-9_.~-]+)*)?",
    flags=re.ASCII,
)


class PipelineValidationError(ValueError):
    """Base failure for a validation report or rejected pipeline."""


class InvalidPipelineValidationReportError(PipelineValidationError):
    """A validation issue or report violates the frozen public contract."""


class PipelineValidationCode(StrEnum):
    """Closed stable error codes for Phase 5 pipeline validation."""

    GRAPH_CYCLE = "graph_cycle"
    GRAPH_DISCONNECTED = "graph_disconnected"
    GRAPH_INVALID_TERMINAL = "graph_invalid_terminal"
    CONNECTOR_MISSING = "connector_missing"
    CONNECTOR_CAPABILITY_MISSING = "connector_capability_missing"
    CONNECTOR_INVALID = "connector_invalid"
    RESOURCE_POLICY_INVALID = "resource_policy_invalid"
    REPAIR_APPROVAL_REQUIRED = "repair_approval_required"
    REPAIR_POLICY_INVALID = "repair_policy_invalid"


_VALIDATION_CODE_ORDER = {code: index for index, code in enumerate(PipelineValidationCode)}
_ISSUE_DETAILS: dict[PipelineValidationCode, tuple[str, str]] = {
    PipelineValidationCode.GRAPH_CYCLE: (
        "/edges",
        "Pipeline graph must not contain a directed cycle.",
    ),
    PipelineValidationCode.GRAPH_DISCONNECTED: (
        "/nodes",
        "Every pipeline node must belong to one source-reachable component.",
    ),
    PipelineValidationCode.GRAPH_INVALID_TERMINAL: (
        "/nodes",
        "Every dead-end node must be an approved pipeline terminal.",
    ),
    PipelineValidationCode.CONNECTOR_MISSING: (
        "/nodes",
        "Every connector-requiring node must reference an available connector.",
    ),
    PipelineValidationCode.CONNECTOR_CAPABILITY_MISSING: (
        "/nodes",
        "Referenced connectors must provide every capability required by their nodes.",
    ),
    PipelineValidationCode.CONNECTOR_INVALID: (
        "/connector_bindings",
        "Connector definitions must satisfy the immutable connector contract.",
    ),
    PipelineValidationCode.RESOURCE_POLICY_INVALID: (
        "/resource_policy",
        "Resource policy values must satisfy the supported bounds and relationships.",
    ),
    PipelineValidationCode.REPAIR_APPROVAL_REQUIRED: (
        "/nodes",
        "Every repair effect must be downstream of an approval on every incoming path.",
    ),
    PipelineValidationCode.REPAIR_POLICY_INVALID: (
        "/nodes",
        "Repair safety metadata must satisfy the supported graph contract.",
    ),
}


@dataclass(frozen=True, slots=True)
class PipelineValidationIssue:
    """One bounded stable-code validation failure with safe display text."""

    code: PipelineValidationCode
    path: str
    message: str

    def __post_init__(self) -> None:
        if type(self.code) is not PipelineValidationCode:
            raise TypeError("pipeline validation issue code must use PipelineValidationCode")
        path = cast(object, self.path)
        if type(path) is not str:
            raise TypeError("pipeline validation issue path must be text")
        if not 1 <= len(path) <= MAX_VALIDATION_PATH_LENGTH:
            raise InvalidPipelineValidationReportError(
                "pipeline validation issue path is outside the size limit"
            )
        if _VALIDATION_PATH_PATTERN.fullmatch(path) is None:
            raise InvalidPipelineValidationReportError(
                "pipeline validation issue path must use canonical pointer syntax"
            )
        message = cast(object, self.message)
        if type(message) is not str:
            raise TypeError("pipeline validation issue message must be text")
        if not 1 <= len(message) <= MAX_VALIDATION_MESSAGE_LENGTH:
            raise InvalidPipelineValidationReportError(
                "pipeline validation issue message is outside the size limit"
            )
        if unicodedata.normalize("NFC", message) != message:
            raise InvalidPipelineValidationReportError(
                "pipeline validation issue message must use normalized Unicode"
            )
        if any(ord(character) < 32 or ord(character) == 127 for character in message):
            raise InvalidPipelineValidationReportError(
                "pipeline validation issue message must not contain control characters"
            )

    def to_mapping(self) -> dict[str, str]:
        """Return the exact stable issue representation."""
        return {
            "code": self.code.value,
            "message": self.message,
            "path": self.path,
        }


class PipelineValidationFailedError(PipelineValidationError):
    """A pipeline did not satisfy one or more Phase 5 validation rules."""

    def __init__(self, report: PipelineValidationReport) -> None:
        if type(report) is not PipelineValidationReport:
            raise TypeError("pipeline validation failure requires PipelineValidationReport")
        self.report = report
        super().__init__(f"pipeline validation failed with {len(report.issues)} issue(s)")


@dataclass(frozen=True, slots=True)
class PipelineValidationReport:
    """A canonically ordered bounded set of stable validation issues."""

    issues: tuple[PipelineValidationIssue, ...]
    version: int = VALIDATION_REPORT_VERSION

    def __post_init__(self) -> None:
        if type(self.issues) is not tuple:
            raise TypeError("pipeline validation report issues must be a tuple")
        issues = cast(tuple[object, ...], self.issues)
        if any(type(issue) is not PipelineValidationIssue for issue in issues):
            raise TypeError("pipeline validation report contains an invalid issue")
        typed_issues = cast(tuple[PipelineValidationIssue, ...], issues)
        if len(typed_issues) > MAX_VALIDATION_ISSUES:
            raise InvalidPipelineValidationReportError(
                "pipeline validation report exceeds the issue limit"
            )
        keys = tuple((issue.code, issue.path) for issue in typed_issues)
        if len(set(keys)) != len(keys):
            raise InvalidPipelineValidationReportError(
                "pipeline validation report issues must be unique"
            )
        version = cast(object, self.version)
        if type(version) is not int:
            raise TypeError("pipeline validation report version must be an integer")
        if version != VALIDATION_REPORT_VERSION:
            raise InvalidPipelineValidationReportError(
                "pipeline validation report version is unsupported"
            )
        object.__setattr__(self, "issues", tuple(sorted(typed_issues, key=_issue_key)))

    @property
    def is_valid(self) -> bool:
        """Return whether the report contains no validation issues."""
        return not self.issues

    def require_valid(self) -> None:
        """Raise one typed failure carrying this report when issues exist."""
        if not self.is_valid:
            raise PipelineValidationFailedError(self)

    def to_mapping(self) -> dict[str, object]:
        """Return the exact stable report representation."""
        return {
            "issues": [issue.to_mapping() for issue in self.issues],
            "valid": self.is_valid,
            "version": self.version,
        }


def validate_pipeline(
    document: PipelineDocument,
    connector_records: tuple[ConnectorRecord, ...],
) -> PipelineValidationReport:
    """Run Phase 5 validators and return only stable, non-sensitive display issues."""
    if type(document) is not PipelineDocument:
        raise TypeError("pipeline validation document must use PipelineDocument")
    if type(connector_records) is not tuple:
        raise TypeError("pipeline validation connector records must be a tuple")
    records = cast(tuple[object, ...], connector_records)
    if any(type(record) is not ConnectorRecord for record in records):
        raise TypeError("pipeline validation connector records contain an invalid value")
    typed_records = cast(tuple[ConnectorRecord, ...], records)
    issues: list[PipelineValidationIssue] = []

    try:
        validate_acyclic_graph(document)
    except PipelineCycleError:
        issues.append(_canonical_issue(PipelineValidationCode.GRAPH_CYCLE))

    try:
        validate_graph_reachability(document)
    except PipelineCycleError:
        pass
    except DisconnectedPipelineError:
        issues.append(_canonical_issue(PipelineValidationCode.GRAPH_DISCONNECTED))
    except InvalidPipelineTerminalError:
        issues.append(_canonical_issue(PipelineValidationCode.GRAPH_INVALID_TERMINAL))

    try:
        validate_connector_capabilities(document, typed_records)
    except MissingConnectorError:
        issues.append(_canonical_issue(PipelineValidationCode.CONNECTOR_MISSING))
    except MissingConnectorCapabilityError:
        issues.append(_canonical_issue(PipelineValidationCode.CONNECTOR_CAPABILITY_MISSING))
    except ConnectorValidationError, TypeError:
        issues.append(_canonical_issue(PipelineValidationCode.CONNECTOR_INVALID))

    try:
        validate_resource_policy(document)
    except ResourcePolicyError, TypeError:
        issues.append(_canonical_issue(PipelineValidationCode.RESOURCE_POLICY_INVALID))

    try:
        validate_repair_safety(document)
    except PipelineCycleError:
        pass
    except UnapprovedRepairEffectError:
        issues.append(_canonical_issue(PipelineValidationCode.REPAIR_APPROVAL_REQUIRED))
    except RepairSafetyError:
        issues.append(_canonical_issue(PipelineValidationCode.REPAIR_POLICY_INVALID))

    return PipelineValidationReport(tuple(issues))


def _issue_key(issue: PipelineValidationIssue) -> tuple[int, str]:
    return (_VALIDATION_CODE_ORDER[issue.code], issue.path)


def _canonical_issue(code: PipelineValidationCode) -> PipelineValidationIssue:
    path, message = _ISSUE_DETAILS[code]
    return PipelineValidationIssue(code, path, message)
