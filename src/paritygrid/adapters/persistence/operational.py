"""SQLite implementation of the operational unit-of-work boundary."""

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from paritygrid.adapters.artifacts.manifests import FileSystemArtifactManifestRepository
from paritygrid.adapters.artifacts.streaming import FileSystemArtifactStreamReader
from paritygrid.adapters.persistence.repositories.connectors import (
    SqlAlchemyConnectorRepository,
)
from paritygrid.adapters.persistence.repositories.execution_events import (
    SqlAlchemyExecutionEventRepository,
)
from paritygrid.adapters.persistence.repositories.idempotency import (
    SqlAlchemyIdempotencyRepository,
)
from paritygrid.adapters.persistence.repositories.pipelines import (
    SqlAlchemyPipelineRepository,
)
from paritygrid.adapters.persistence.repositories.reconciliation import (
    SqlAlchemyReconciliationResultRepository,
)
from paritygrid.adapters.persistence.repositories.repairs import SqlAlchemyRepairRepository
from paritygrid.adapters.persistence.repositories.runs import SqlAlchemyRunRepository
from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
from paritygrid.application.ports.operations import (
    OperationalRepositories,
    OperationalUnitOfWork,
)

DEFAULT_ARTIFACT_CHUNK_BYTES = 1_048_576


class SQLOperationalUnitOfWork(OperationalUnitOfWork):
    """Open one short SQLite transaction exposing every operational store."""

    def __init__(
        self,
        database: SQLiteDatabase,
        *,
        artifact_root: Path,
        artifact_chunk_bytes: int = DEFAULT_ARTIFACT_CHUNK_BYTES,
    ) -> None:
        self._database = database
        self._artifact_root = artifact_root
        self._artifact_chunk_bytes = artifact_chunk_bytes

    @contextmanager
    def transaction(self) -> Generator[OperationalRepositories]:
        with self._database.transaction() as session:
            yield OperationalRepositories(
                pipelines=SqlAlchemyPipelineRepository(session),
                connectors=SqlAlchemyConnectorRepository(session),
                runs=SqlAlchemyRunRepository(session),
                idempotency=SqlAlchemyIdempotencyRepository(session),
                artifact_manifests=FileSystemArtifactManifestRepository(
                    session, self._artifact_root
                ),
                artifact_stream=FileSystemArtifactStreamReader(
                    session,
                    self._artifact_root,
                    chunk_size=self._artifact_chunk_bytes,
                ),
                events=SqlAlchemyExecutionEventRepository(session),
                reconciliation=SqlAlchemyReconciliationResultRepository(session),
                repair=SqlAlchemyRepairRepository(session),
            )
