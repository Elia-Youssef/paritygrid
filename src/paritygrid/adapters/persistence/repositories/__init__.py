"""Modular SQLAlchemy adapters for durable configuration."""

from paritygrid.adapters.persistence.repositories import common as _common
from paritygrid.adapters.persistence.repositories import connectors as _connectors
from paritygrid.adapters.persistence.repositories import mapping as _mapping
from paritygrid.adapters.persistence.repositories.audits import SqlAlchemyAuditRepository
from paritygrid.adapters.persistence.repositories.checkpoints import SqlAlchemyCheckpointRepository
from paritygrid.adapters.persistence.repositories.common import MAX_CANONICAL_DOCUMENT_BYTES
from paritygrid.adapters.persistence.repositories.connectors import SqlAlchemyConnectorRepository
from paritygrid.adapters.persistence.repositories.execution_events import (
    SqlAlchemyExecutionEventRepository,
)
from paritygrid.adapters.persistence.repositories.idempotency import (
    SqlAlchemyIdempotencyRepository,
)
from paritygrid.adapters.persistence.repositories.pipelines import (
    SqlAlchemyPipelineRepository,
)
from paritygrid.adapters.persistence.repositories.repairs import SqlAlchemyRepairRepository
from paritygrid.adapters.persistence.repositories.run_node_aggregates import (
    SqlAlchemyRunNodeAggregateRepository,
)
from paritygrid.adapters.persistence.repositories.run_revisions import (
    SqlAlchemyRunRevisionRepository,
)
from paritygrid.adapters.persistence.repositories.runs import SqlAlchemyRunRepository
from paritygrid.adapters.persistence.repositories.work_items import (
    SqlAlchemyWorkAttemptRepository,
    SqlAlchemyWorkItemRepository,
)

MAX_PERSISTED_INTEGER = _common.MAX_PERSISTED_INTEGER
_bounded_text = _common.bounded_text
_decode_document = _common.decode_document
_decode_optional_document = _common.decode_optional_document
_encode_document = _common.encode_document
_optional_text = _common.optional_text
_positive_int = _common.positive_int
_require_incrementable = _common.require_incrementable
_translate_storage_errors = _common.translate_storage_errors
_validate_secret_policy = _common.validate_secret_policy
_validate_secret_references = _connectors.validate_secret_references
_connector_from_row = _mapping.connector_from_row
_pipeline_from_row = _mapping.pipeline_from_row
_pipeline_version_from_row = _mapping.pipeline_version_from_row
_raise_connector_cas_failure = _mapping.raise_connector_cas_failure
_raise_cas_failure = _mapping.raise_pipeline_cas_failure
_secret_reference_from_row = _mapping.secret_reference_from_row

__all__ = [
    "MAX_CANONICAL_DOCUMENT_BYTES",
    "SqlAlchemyAuditRepository",
    "SqlAlchemyCheckpointRepository",
    "SqlAlchemyConnectorRepository",
    "SqlAlchemyExecutionEventRepository",
    "SqlAlchemyIdempotencyRepository",
    "SqlAlchemyPipelineRepository",
    "SqlAlchemyRepairRepository",
    "SqlAlchemyRunNodeAggregateRepository",
    "SqlAlchemyRunRepository",
    "SqlAlchemyRunRevisionRepository",
    "SqlAlchemyWorkAttemptRepository",
    "SqlAlchemyWorkItemRepository",
]
