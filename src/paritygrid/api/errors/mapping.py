"""Translation of typed application failures into Problem Details.

The mapping is the single place that converts accepted application and
repository error families into bounded transport problems.  Unknown failures
collapse to the generic internal problem so no implementation detail leaks.
"""

from paritygrid.api.errors.problems import FieldError, ProblemError, not_found_problem
from paritygrid.application.ports.artifact_streaming import (
    ArtifactStreamError,
    ArtifactStreamIntegrityError,
    ArtifactStreamInvalidError,
    ArtifactStreamNotFoundError,
    ArtifactStreamRangeError,
)
from paritygrid.application.ports.artifacts import (
    ArtifactIntegrityError,
    ArtifactManifestError,
    ArtifactManifestInvalidError,
    ArtifactManifestStorageError,
    ArtifactManifestStorageUnavailableError,
    ArtifactPathError,
)
from paritygrid.application.ports.configuration import (
    ConfigurationRepositoryError,
    ConfigurationStorageError,
    ConfigurationStorageUnavailableError,
    CorruptRepositoryRecordError,
    DuplicateRecordError,
    InvalidRepositoryRequestError,
    PipelineVersionConflictError,
    RecordNotFoundError,
    RecordStateConflictError,
    StaleConnectorRevisionError,
    StaleRowVersionError,
    UnsafeConnectorConfigurationError,
)
from paritygrid.application.ports.connectors import ConnectorError
from paritygrid.application.ports.consistency import (
    ConsistencyInvalidRequestError,
    ConsistencyRepositoryError,
    ConsistencyStorageError,
    ConsistencyStorageUnavailableError,
    IdempotencyConflictError,
)
from paritygrid.application.ports.execution import (
    ExecutionDuplicateError,
    ExecutionInvalidRequestError,
    ExecutionLeaseLostError,
    ExecutionRecordNotFoundError,
    ExecutionRepositoryError,
    ExecutionStaleRowVersionError,
    ExecutionStateConflictError,
    ExecutionStorageError,
    ExecutionStorageUnavailableError,
)
from paritygrid.application.ports.operations import OperationalStoreUnavailableError
from paritygrid.application.ports.reconciliation_persistence import (
    ReconciliationInvalidRequestError,
    ReconciliationPersistenceError,
    ReconciliationRecordNotFoundError,
    ReconciliationResultConflictError,
    ReconciliationStorageError,
    ReconciliationStorageUnavailableError,
)
from paritygrid.application.ports.repair_audit import (
    RepairCorruptionError,
    RepairDuplicateError,
    RepairInvalidRequestError,
    RepairRecordNotFoundError,
    RepairRepositoryError,
    RepairStaleRowVersionError,
    RepairStateConflictError,
    RepairStorageError,
    RepairStorageUnavailableError,
)
from paritygrid.application.ports.writer import (
    PersistenceContentionError,
    WriterAdmissionTimeoutError,
    WriterCommitOutcomeUnknownError,
    WriterError,
    WriterResultTimeoutError,
)
from paritygrid.application.repair.errors import (
    RepairApprovalConflictError,
    RepairPlanMismatchError,
    RepairPlanStateError,
    RepairReconciliationMissingError,
    RepairReconciliationStaleError,
    RepairRunNotFoundError,
    RepairWorkflowError,
    RepairWriterOutcomeUnknownError,
    RepairWriterUnavailableError,
    TargetApplicationError,
    TargetApplicationInterruptedError,
    TargetApplicationUnresolvedError,
)
from paritygrid.application.services.errors import (
    IdempotencyInProgressError,
    IdempotencyKeyConflictError,
    IdempotencyReplayConflictError,
    OperationalConflictError,
    OperationalRecordNotFoundError,
    OperationalRequestError,
    OperationalUnavailableError,
    RunInvalidTransitionError,
)
from paritygrid.application.services.telemetry import TelemetrySubscriberLimitError
from paritygrid.domain.errors import DomainError, InvalidTransitionError


def translate_error(error: Exception) -> ProblemError:
    """Map one raised failure to its bounded Problem Details value."""
    if isinstance(error, ProblemError):
        return error
    if isinstance(error, OperationalRequestError):
        fields = (FieldError(error.field, str(error)),) if error.field else ()
        return ProblemError(
            type_slug="validation",
            title="Request validation failed",
            status=422,
            detail=str(error),
            errors=fields,
        )
    if isinstance(error, OperationalRecordNotFoundError):
        return not_found_problem(error.resource, error.identifier)
    if isinstance(error, RunInvalidTransitionError):
        return ProblemError(
            type_slug="invalid-transition",
            title="Invalid run transition",
            status=409,
            detail=str(error),
            code="run_invalid_transition",
        )
    if isinstance(error, OperationalConflictError):
        return _conflict(str(error), code=error.code)
    if isinstance(error, (IdempotencyKeyConflictError, IdempotencyConflictError)):
        return _conflict(
            "the idempotency key was already used with a different request",
            code="idempotency_key_reused",
        )
    if isinstance(error, IdempotencyInProgressError):
        return ProblemError(
            type_slug="idempotency-in-progress",
            title="Command is already in progress",
            status=409,
            detail=str(error),
        )
    if isinstance(error, IdempotencyReplayConflictError):
        return _conflict(
            "stored idempotent evidence conflicts with this request",
            code="idempotency_replay_conflict",
        )
    if isinstance(error, RepairRunNotFoundError):
        return not_found_problem("run", "the addressed run")
    if isinstance(error, RepairReconciliationMissingError):
        return ProblemError(
            type_slug="reconciliation-missing",
            title="Reconciliation result is missing",
            status=409,
            detail=str(error),
            code="reconciliation_missing",
        )
    if isinstance(error, RepairReconciliationStaleError):
        return _conflict(str(error), code="reconciliation_stale")
    if isinstance(error, (RepairPlanMismatchError, RepairApprovalConflictError)):
        return _conflict(str(error), code="repair_plan_mismatch")
    if isinstance(error, RepairPlanStateError):
        return _conflict(str(error), code="repair_plan_state")
    if isinstance(error, RepairWriterOutcomeUnknownError):
        return _unavailable(
            "the repair write outcome is unknown; retry the same request",
            code="writer_outcome_unknown",
        )
    if isinstance(error, RepairWriterUnavailableError):
        return _unavailable("the repair boundary is busy; retry the same request")
    if isinstance(error, TargetApplicationInterruptedError):
        return _unavailable(
            "repair application was interrupted; retry the same request",
            code="repair_application_interrupted",
        )
    if isinstance(error, TargetApplicationUnresolvedError):
        return _unavailable(
            "the target application outcome is unresolved; retry the same request",
            code="repair_outcome_unresolved",
        )
    if isinstance(error, TargetApplicationError):
        return ProblemError(
            type_slug="target-application-failed",
            title="Target application failed",
            status=409,
            detail=str(error)[:512],
            code="target_application_failed",
        )
    if isinstance(error, RepairWorkflowError):
        return _conflict(str(error), code="repair_workflow_conflict")
    if isinstance(error, ReconciliationRecordNotFoundError):
        return not_found_problem("reconciliation", "the addressed reconciliation")
    if isinstance(error, ReconciliationResultConflictError):
        return _conflict(str(error), code="reconciliation_conflict")
    if isinstance(error, ReconciliationInvalidRequestError):
        return _validation(str(error))
    if isinstance(error, TelemetrySubscriberLimitError):
        return ProblemError(
            type_slug="telemetry-capacity",
            title="Telemetry channel is at capacity",
            status=503,
            detail=str(error),
            code="telemetry_capacity",
        )
    if isinstance(error, RepairInvalidRequestError):
        return _validation(str(error))
    if isinstance(error, RepairRecordNotFoundError):
        return not_found_problem("repair plan", "the addressed repair plan")
    if isinstance(error, RepairDuplicateError):
        return _conflict("the addressed identity already exists", code="duplicate_record")
    if isinstance(error, RepairStaleRowVersionError):
        return _conflict(
            "the repair plan changed concurrently; retry the request",
            code="stale_row_version",
        )
    if isinstance(error, RepairStateConflictError):
        return _conflict(str(error), code="repair_state_conflict")
    if isinstance(error, RepairCorruptionError):
        return _integrity()
    if isinstance(error, RepairStorageUnavailableError):
        return _unavailable("repair storage is temporarily unavailable")
    if isinstance(error, (RepairStorageError, RepairRepositoryError)):
        return _unavailable("repair storage failed while serving the request")
    if isinstance(error, ReconciliationStorageUnavailableError):
        return _unavailable("reconciliation storage is temporarily unavailable")
    if isinstance(error, (ReconciliationStorageError, ReconciliationPersistenceError)):
        return _unavailable("reconciliation storage failed while serving the request")
    if isinstance(error, ConsistencyInvalidRequestError):
        return _validation(str(error))
    if isinstance(error, OperationalUnavailableError):
        return _unavailable(str(error))
    if isinstance(error, WriterCommitOutcomeUnknownError):
        return _unavailable(
            "the durable write outcome is unknown; retry with the same idempotency key",
            code="writer_outcome_unknown",
        )
    if isinstance(error, (WriterResultTimeoutError, WriterAdmissionTimeoutError)) or (
        isinstance(error, WriterError) and not isinstance(error, WriterCommitOutcomeUnknownError)
    ):
        return _unavailable("the durable write boundary is busy", code="writer_busy")
    if isinstance(error, PersistenceContentionError):
        return _unavailable("storage contention interrupted the request")
    if isinstance(error, InvalidTransitionError):
        return ProblemError(
            type_slug="invalid-transition",
            title="Invalid lifecycle transition",
            status=409,
            detail=str(error),
        )
    if isinstance(error, ExecutionDuplicateError):
        return _conflict("the addressed identity already exists", code="duplicate_record")
    if isinstance(error, ExecutionRecordNotFoundError):
        return not_found_problem("run", "the addressed run")
    if isinstance(error, ExecutionInvalidRequestError):
        return _validation(str(error))
    if isinstance(
        error,
        (ExecutionStaleRowVersionError, ExecutionLeaseLostError, StaleRowVersionError),
    ):
        return _conflict(
            "the record changed concurrently; retry the request",
            code="stale_row_version",
        )
    if isinstance(error, (ExecutionStateConflictError, RecordStateConflictError)):
        return _conflict("the record state does not allow this operation", code="state_conflict")
    if isinstance(error, DuplicateRecordError):
        return _conflict("the addressed identity already exists", code="duplicate_record")
    if isinstance(error, RecordNotFoundError):
        return not_found_problem("record", "the addressed record")
    if isinstance(error, PipelineVersionConflictError):
        return _conflict(
            "the pipeline version frontier changed; retry publication",
            code="pipeline_version_conflict",
        )
    if isinstance(error, StaleConnectorRevisionError):
        return _conflict(
            "the connector definition changed concurrently",
            code="connector_revision_conflict",
        )
    if isinstance(error, InvalidRepositoryRequestError):
        return _validation(str(error))
    if isinstance(error, UnsafeConnectorConfigurationError):
        return ProblemError(
            type_slug="unsafe-connector-configuration",
            title="Connector configuration is not safe to persist",
            status=422,
            detail=str(error),
        )
    if isinstance(error, CorruptRepositoryRecordError):
        return _integrity()
    if isinstance(error, ArtifactStreamRangeError):
        return ProblemError(
            type_slug="range-not-satisfiable",
            title="Requested range is not satisfiable",
            status=416,
            detail="the requested byte range lies outside the committed artifact",
        )
    if isinstance(error, ArtifactStreamNotFoundError):
        return not_found_problem("artifact", "the addressed artifact")
    if isinstance(error, ArtifactStreamInvalidError):
        return _validation(str(error))
    if isinstance(error, (ArtifactStreamIntegrityError, ArtifactIntegrityError)):
        return ProblemError(
            type_slug="artifact-integrity",
            title="Artifact integrity failure",
            status=410,
            detail="the committed artifact no longer matches its manifest",
        )
    if isinstance(error, ArtifactManifestInvalidError):
        return _validation(str(error))
    if isinstance(error, (ArtifactManifestError, ArtifactStreamError, ArtifactPathError)):
        return _unavailable("artifact storage is unavailable")
    if isinstance(error, ConnectorError):
        return _unavailable("connector operation failed")
    if isinstance(error, OperationalStoreUnavailableError):
        return _unavailable("the operational store is unavailable")
    if isinstance(
        error,
        (
            ExecutionStorageUnavailableError,
            ConfigurationStorageUnavailableError,
            ConsistencyStorageUnavailableError,
        ),
    ):
        return _unavailable("storage is temporarily unavailable")
    if isinstance(
        error,
        (
            ExecutionStorageError,
            ConfigurationStorageError,
            ConsistencyStorageError,
            ExecutionRepositoryError,
            ConfigurationRepositoryError,
            ConsistencyRepositoryError,
            ArtifactManifestStorageError,
            ArtifactManifestStorageUnavailableError,
        ),
    ):
        return _unavailable("storage failed while serving the request")
    if isinstance(error, DomainError):
        return ProblemError(
            type_slug="domain-rule",
            title="Request violates a domain rule",
            status=422,
            detail=str(error)[:512],
        )
    return ProblemError(
        type_slug="internal",
        title="Internal service error",
        status=500,
        detail="the request could not be completed",
        code="internal_error",
    )


def _conflict(detail: str, *, code: str) -> ProblemError:
    return ProblemError(
        type_slug="conflict",
        title="Request conflicts with durable state",
        status=409,
        detail=detail,
        code=code,
    )


def _validation(detail: str) -> ProblemError:
    return ProblemError(
        type_slug="validation",
        title="Request validation failed",
        status=422,
        detail=detail,
    )


def _unavailable(detail: str, *, code: str = "unavailable") -> ProblemError:
    return ProblemError(
        type_slug="unavailable",
        title="Service is temporarily unavailable",
        status=503,
        detail=detail,
        code=code,
    )


def _integrity() -> ProblemError:
    return ProblemError(
        type_slug="data-integrity",
        title="Stored data failed an integrity check",
        status=500,
        detail="stored evidence failed validation; the operation was refused",
        code="data_integrity",
    )
