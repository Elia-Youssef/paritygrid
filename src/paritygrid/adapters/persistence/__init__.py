"""Authoritative SQLite persistence foundation."""

from paritygrid.adapters.persistence.cancellation import SQLiteCancellationStateReader
from paritygrid.adapters.persistence.capabilities import (
    MINIMUM_SQLITE_VERSION,
    REQUIRED_BUSY_TIMEOUT_MS,
    REQUIRED_JOURNAL_MODE,
    REQUIRED_SYNCHRONOUS_LEVEL,
    SQLiteCapabilities,
    SQLiteLibraryInfo,
    SQLitePragmaState,
    build_capability_report,
    current_sqlite_library,
    validate_sqlite_library,
    validate_sqlite_pragmas,
)
from paritygrid.adapters.persistence.errors import (
    MigrationConfigurationError,
    MigrationExecutionError,
    MigrationIntegrityError,
    PersistenceError,
    SQLiteCapabilityError,
    SQLiteConfigurationError,
)
from paritygrid.adapters.persistence.finalization import SQLiteFinalizationStateReader
from paritygrid.adapters.persistence.migration import (
    HEAD_REVISION,
    MigrationReport,
    upgrade_to_head,
)
from paritygrid.adapters.persistence.pause import SQLitePauseStateReader
from paritygrid.adapters.persistence.recovery import SQLiteRecoveryStateReader
from paritygrid.adapters.persistence.repositories import (
    MAX_CANONICAL_DOCUMENT_BYTES,
    SqlAlchemyCheckpointRepository,
    SqlAlchemyConnectorRepository,
    SqlAlchemyExecutionEventRepository,
    SqlAlchemyIdempotencyRepository,
    SqlAlchemyPipelineRepository,
    SqlAlchemyRunNodeAggregateRepository,
    SqlAlchemyRunRepository,
    SqlAlchemyRunRevisionRepository,
    SqlAlchemyWorkAttemptRepository,
    SqlAlchemyWorkItemRepository,
)
from paritygrid.adapters.persistence.result_coordinator import SQLiteResultCoordinatorReader
from paritygrid.adapters.persistence.sqlite import (
    SessionFactory,
    SQLiteDatabase,
    SQLiteDatabaseConfig,
    create_session_factory,
    create_sqlite_engine,
    inspect_sqlite_engine,
    transactional_session,
)
from paritygrid.adapters.persistence.values import (
    CanonicalStorageJson,
    EnvironmentVariableName,
    IdempotencyStatus,
    RepairActionApplicationStatus,
    RepairPlanStatus,
    RunNodeState,
    SecretReferenceName,
    Sha256Digest,
    WorkAttemptOutcome,
)
from paritygrid.adapters.persistence.writer.core import SQLiteTransactionalWriter
from paritygrid.adapters.persistence.writer.notifications import (
    BoundedCommittedNotificationBuffer,
)

__all__ = (
    "HEAD_REVISION",
    "MAX_CANONICAL_DOCUMENT_BYTES",
    "MINIMUM_SQLITE_VERSION",
    "REQUIRED_BUSY_TIMEOUT_MS",
    "REQUIRED_JOURNAL_MODE",
    "REQUIRED_SYNCHRONOUS_LEVEL",
    "BoundedCommittedNotificationBuffer",
    "CanonicalStorageJson",
    "EnvironmentVariableName",
    "IdempotencyStatus",
    "MigrationConfigurationError",
    "MigrationExecutionError",
    "MigrationIntegrityError",
    "MigrationReport",
    "PersistenceError",
    "RepairActionApplicationStatus",
    "RepairPlanStatus",
    "RunNodeState",
    "SQLiteCancellationStateReader",
    "SQLiteCapabilities",
    "SQLiteCapabilityError",
    "SQLiteConfigurationError",
    "SQLiteDatabase",
    "SQLiteDatabaseConfig",
    "SQLiteFinalizationStateReader",
    "SQLiteLibraryInfo",
    "SQLitePauseStateReader",
    "SQLitePragmaState",
    "SQLiteRecoveryStateReader",
    "SQLiteResultCoordinatorReader",
    "SQLiteTransactionalWriter",
    "SecretReferenceName",
    "SessionFactory",
    "Sha256Digest",
    "SqlAlchemyCheckpointRepository",
    "SqlAlchemyConnectorRepository",
    "SqlAlchemyExecutionEventRepository",
    "SqlAlchemyIdempotencyRepository",
    "SqlAlchemyPipelineRepository",
    "SqlAlchemyRunNodeAggregateRepository",
    "SqlAlchemyRunRepository",
    "SqlAlchemyRunRevisionRepository",
    "SqlAlchemyWorkAttemptRepository",
    "SqlAlchemyWorkItemRepository",
    "WorkAttemptOutcome",
    "build_capability_report",
    "create_session_factory",
    "create_sqlite_engine",
    "current_sqlite_library",
    "inspect_sqlite_engine",
    "transactional_session",
    "upgrade_to_head",
    "validate_sqlite_library",
    "validate_sqlite_pragmas",
)
