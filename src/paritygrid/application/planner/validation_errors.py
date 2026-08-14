"""Dependency-neutral human-readable pipeline validation contracts."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

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


def _issue_key(issue: PipelineValidationIssue) -> tuple[int, str]:
    return (_VALIDATION_CODE_ORDER[issue.code], issue.path)
