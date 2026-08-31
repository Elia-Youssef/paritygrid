"""Transactional access to the operational stores behind the HTTP boundary."""

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Protocol

from paritygrid.application.ports.artifact_streaming import ArtifactStreamReader
from paritygrid.application.ports.artifacts import ArtifactManifestRepository
from paritygrid.application.ports.configuration import (
    ConnectorRepository,
    PipelineRepository,
)
from paritygrid.application.ports.consistency import (
    ExecutionEventRepository,
    IdempotencyRepository,
)
from paritygrid.application.ports.execution import RunRepository
from paritygrid.application.ports.reconciliation_persistence import (
    ReconciliationResultRepository,
)
from paritygrid.application.ports.repair_audit import RepairRepository


class OperationalStoreUnavailableError(Exception):
    """The operational stores cannot currently serve a request."""


@dataclass(frozen=True, slots=True)
class OperationalRepositories:
    """One short-lived transactional view of every operational store."""

    pipelines: PipelineRepository
    connectors: ConnectorRepository
    runs: RunRepository
    idempotency: IdempotencyRepository
    artifact_manifests: ArtifactManifestRepository
    artifact_stream: ArtifactStreamReader
    events: ExecutionEventRepository
    reconciliation: ReconciliationResultRepository
    repair: RepairRepository


class OperationalUnitOfWork(Protocol):
    """Own one short transaction at a time over the operational stores."""

    @contextmanager
    def transaction(self) -> Generator[OperationalRepositories]: ...
