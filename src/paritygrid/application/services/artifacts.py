"""Artifact listing and confined streaming use cases."""

from collections.abc import Callable
from dataclasses import dataclass

from paritygrid.application.ports.artifact_streaming import (
    ArtifactByteRange,
    ArtifactByteStream,
    ArtifactStreamMetadata,
)
from paritygrid.application.ports.artifacts import (
    ArtifactId,
    ArtifactManifestPage,
    ArtifactManifestRecord,
)
from paritygrid.application.ports.execution import RunRecord
from paritygrid.application.ports.operations import OperationalUnitOfWork
from paritygrid.application.services.errors import (
    OperationalRecordNotFoundError,
    OperationalRequestError,
)
from paritygrid.domain.models import RunId, UtcTimestamp


@dataclass(frozen=True, slots=True)
class RunArtifactPage:
    """One artifact page together with its run version evidence."""

    run: RunRecord
    observed_at: UtcTimestamp
    manifests: ArtifactManifestPage


class ArtifactService:
    """List and stream committed artifacts through manifest identities."""

    def __init__(
        self,
        *,
        unit_of_work: OperationalUnitOfWork,
        now: Callable[[], UtcTimestamp],
    ) -> None:
        self._unit_of_work = unit_of_work
        self._now = now

    def list_for_run(
        self,
        run_id: str,
        *,
        limit: int,
        after: str | None,
    ) -> RunArtifactPage:
        identity = _run_id(run_id)
        cursor = None if after is None else _artifact_id(after)
        with self._unit_of_work.transaction() as repositories:
            run = repositories.runs.get(identity)
            if run is None:
                raise OperationalRecordNotFoundError("run", run_id)
            manifests = repositories.artifact_manifests.list_for_run(
                identity, limit=limit, after=cursor
            )
        return RunArtifactPage(
            run=run,
            observed_at=self._now(),
            manifests=manifests,
        )

    def manifest(self, artifact_id: str) -> ArtifactManifestRecord:
        with self._unit_of_work.transaction() as repositories:
            record = repositories.artifact_manifests.get(_artifact_id(artifact_id))
        if record is None:
            raise OperationalRecordNotFoundError("artifact", artifact_id)
        return record

    def open(
        self,
        artifact_id: str,
        *,
        byte_range: ArtifactByteRange | None,
    ) -> tuple[ArtifactStreamMetadata, ArtifactByteStream]:
        """Open one verified byte stream; the stream outlives the transaction.

        The reader verifies the manifest, the confined path, the file
        identity, and the whole-content digest before returning, and the
        descriptor-bound stream re-verifies identity on close.
        """
        with self._unit_of_work.transaction() as repositories:
            stream = repositories.artifact_stream.open(
                _artifact_id(artifact_id), byte_range=byte_range
            )
        return stream.metadata, stream


def _run_id(value: str) -> RunId:
    try:
        return RunId.parse(value)
    except ValueError as error:
        raise OperationalRequestError(
            "run identity must use the canonical run format",
            field="run_id",
        ) from error


def _artifact_id(value: str) -> ArtifactId:
    try:
        return ArtifactId.parse(value)
    except ValueError as error:
        raise OperationalRequestError(
            "artifact identity must use the canonical artifact format",
            field="artifact_id",
        ) from error
