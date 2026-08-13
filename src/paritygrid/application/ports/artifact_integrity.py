"""Dependency-neutral contracts for read-only artifact integrity scans."""

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from paritygrid.application.ports.artifacts import ArtifactRelativePath
from paritygrid.domain.models import ArtifactId

MAX_ARTIFACT_INTEGRITY_ENTRIES = 1_000_000
MAX_ARTIFACT_INTEGRITY_ISSUES = 100_000

_SHA256 = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)


class ArtifactIntegrityIssueKind(StrEnum):
    """Closed, evidence-preserving scan classifications."""

    MISSING_FILE = "missing_file"
    ORPHAN_FILE = "orphan_file"
    INVALID_FILE = "invalid_file"
    UNSAFE_ENTRY = "unsafe_entry"


class ArtifactIntegrityScanError(RuntimeError):
    """Base failure for read-only artifact inventory scans."""


class ArtifactIntegrityScanInvalidError(ArtifactIntegrityScanError):
    """The scanner request or source boundary is invalid."""


class ArtifactIntegrityScanLimitError(ArtifactIntegrityScanError):
    """The bounded inventory cannot be represented safely."""


class ArtifactIntegrityScanCorruptionError(ArtifactIntegrityScanError):
    """Durable manifest state is malformed or internally inconsistent."""


class ArtifactIntegrityScanStorageError(ArtifactIntegrityScanError):
    """A filesystem or persistence operation failed without exposing details."""


class ArtifactIntegrityScanStorageUnavailableError(ArtifactIntegrityScanError):
    """Manifest persistence is temporarily unavailable."""


@dataclass(frozen=True, slots=True)
class ArtifactIntegrityIssue:
    """One stable finding without an absolute machine path."""

    kind: ArtifactIntegrityIssueKind
    relative_path: ArtifactRelativePath | None
    artifact_id: ArtifactId | None
    observed_path_sha256: str | None

    def __post_init__(self) -> None:
        if type(self.kind) is not ArtifactIntegrityIssueKind:
            raise TypeError("artifact integrity issue kind is invalid")
        if self.relative_path is not None and type(self.relative_path) is not ArtifactRelativePath:
            raise TypeError("artifact integrity issue path is invalid")
        if self.artifact_id is not None and type(self.artifact_id) is not ArtifactId:
            raise TypeError("artifact integrity issue identity is invalid")
        if self.observed_path_sha256 is not None and (
            type(self.observed_path_sha256) is not str
            or _SHA256.fullmatch(self.observed_path_sha256) is None
        ):
            raise TypeError("artifact integrity observed-path digest is invalid")
        expected = {
            ArtifactIntegrityIssueKind.MISSING_FILE: (True, True, False),
            ArtifactIntegrityIssueKind.ORPHAN_FILE: (True, False, False),
            ArtifactIntegrityIssueKind.INVALID_FILE: (True, True, False),
            ArtifactIntegrityIssueKind.UNSAFE_ENTRY: (False, False, True),
        }[self.kind]
        actual = (
            self.relative_path is not None,
            self.artifact_id is not None,
            self.observed_path_sha256 is not None,
        )
        if actual != expected:
            raise ArtifactIntegrityScanInvalidError(
                "artifact integrity issue fields do not match its kind"
            )

    @property
    def order_key(self) -> tuple[str, str, str, str]:
        """Return the stable ordering key without filesystem-specific values."""
        return (
            self.kind.value,
            "" if self.relative_path is None else str(self.relative_path),
            "" if self.artifact_id is None else str(self.artifact_id),
            self.observed_path_sha256 or "",
        )


@dataclass(frozen=True, slots=True)
class ArtifactIntegrityScanReport:
    """Complete bounded comparison of durable manifests and observed files."""

    manifest_count: int
    observed_file_count: int
    verified_manifest_count: int
    issues: tuple[ArtifactIntegrityIssue, ...]
    inventory_sha256: str

    def __post_init__(self) -> None:
        for value in (
            self.manifest_count,
            self.observed_file_count,
            self.verified_manifest_count,
        ):
            if type(value) is not int or not 0 <= value <= MAX_ARTIFACT_INTEGRITY_ENTRIES:
                raise ValueError("artifact integrity count is outside the range")
        if self.verified_manifest_count > self.manifest_count:
            raise ArtifactIntegrityScanInvalidError(
                "verified artifact count exceeds manifest count"
            )
        if type(self.issues) is not tuple or any(
            type(issue) is not ArtifactIntegrityIssue for issue in self.issues
        ):
            raise TypeError("artifact integrity issues must be an immutable tuple")
        if len(self.issues) > MAX_ARTIFACT_INTEGRITY_ISSUES:
            raise ArtifactIntegrityScanLimitError(
                "artifact integrity issue count exceeds the limit"
            )
        keys = tuple(issue.order_key for issue in self.issues)
        if keys != tuple(sorted(keys)) or len(set(keys)) != len(keys):
            raise ArtifactIntegrityScanInvalidError(
                "artifact integrity issues must be unique and sorted"
            )
        if (
            type(self.inventory_sha256) is not str
            or _SHA256.fullmatch(self.inventory_sha256) is None
        ):
            raise TypeError("artifact integrity inventory digest is invalid")

    @property
    def is_clean(self) -> bool:
        """Return whether every durable manifest has one exact regular file."""
        return not self.issues


class ArtifactIntegrityScanner(Protocol):
    """Compare all durable manifests with one confined artifact root."""

    def scan(self) -> ArtifactIntegrityScanReport:
        """Return every bounded finding without mutating either source."""
        ...
