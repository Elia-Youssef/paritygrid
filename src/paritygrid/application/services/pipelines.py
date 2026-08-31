"""Pipeline and pipeline-version use cases for the operational boundary."""

from collections.abc import Callable, Mapping

from paritygrid.application.planner.documents import (
    PIPELINE_PLANNER_FORMAT_VERSION,
    PipelineDocument,
)
from paritygrid.application.planner.publication import (
    PipelinePublicationError,
    PipelinePublicationService,
)
from paritygrid.application.planner.validation_errors import PipelineValidationError
from paritygrid.application.ports.configuration import (
    DuplicateRecordError,
    PipelinePage,
    PipelineRecord,
    PipelineVersionConflictError,
    PipelineVersionRecord,
)
from paritygrid.application.ports.operations import OperationalUnitOfWork
from paritygrid.application.services.errors import (
    OperationalRecordNotFoundError,
    OperationalRequestError,
)
from paritygrid.domain.models import PipelineId, PipelineVersion, UtcTimestamp

MAX_PIPELINE_DISPLAY_NAME_LENGTH = 128
MAX_PIPELINE_DESCRIPTION_LENGTH = 1_024


class PipelineService:
    """Create, list, and publish immutable pipeline versions."""

    def __init__(
        self,
        *,
        unit_of_work: OperationalUnitOfWork,
        now: Callable[[], UtcTimestamp],
    ) -> None:
        self._unit_of_work = unit_of_work
        self._now = now

    def create(
        self,
        *,
        pipeline_id: str,
        display_name: str,
        description: str | None,
        converge_on_duplicate: bool = False,
    ) -> PipelineRecord:
        """Create one pipeline; duplicate convergence serves keyed retries.

        A plain duplicate create stays a conflict.  A retry that carries an
        idempotency key may legitimately re-execute after the first owner
        committed, so the caller opts into converging on the identical
        durable record instead of surfacing a duplicate failure.
        """
        _require_display_fields(display_name, description)
        identity = _pipeline_id(pipeline_id)
        try:
            with self._unit_of_work.transaction() as repositories:
                return repositories.pipelines.create(
                    pipeline_id=identity,
                    display_name=display_name,
                    description=description,
                    created_at=self._now(),
                )
        except DuplicateRecordError:
            if not converge_on_duplicate:
                raise
            with self._unit_of_work.transaction() as repositories:
                existing = repositories.pipelines.get(identity)
            if (
                existing is not None
                and existing.display_name == display_name
                and existing.description == description
            ):
                return existing
            raise

    def get(self, pipeline_id: str) -> PipelineRecord:
        with self._unit_of_work.transaction() as repositories:
            record = repositories.pipelines.get(_pipeline_id(pipeline_id))
        if record is None:
            raise OperationalRecordNotFoundError("pipeline", pipeline_id)
        return record

    def list(
        self,
        *,
        limit: int,
        after: str | None,
        include_archived: bool,
    ) -> PipelinePage:
        cursor = None if after is None else _pipeline_id(after)
        with self._unit_of_work.transaction() as repositories:
            return repositories.pipelines.list(
                limit=limit,
                after=cursor,
                include_archived=include_archived,
            )

    def publish(
        self,
        *,
        pipeline_id: str,
        document: Mapping[str, object],
        expected_latest_version: int | None,
        converge_on_duplicate: bool = False,
    ) -> PipelineVersionRecord:
        identity = _pipeline_id(pipeline_id)
        draft = _draft(document)
        if expected_latest_version is not None and (
            type(expected_latest_version) is not int or expected_latest_version < 0
        ):
            raise OperationalRequestError(
                "expected latest version must be a nonnegative integer",
                field="expected_latest_version",
            )
        expected = (
            None if expected_latest_version is None else PipelineVersion(expected_latest_version)
        )
        with self._unit_of_work.transaction() as repositories:
            if repositories.pipelines.get(identity) is None:
                raise OperationalRecordNotFoundError("pipeline", pipeline_id)
            publication = PipelinePublicationService(
                repositories.pipelines, repositories.connectors
            )
            try:
                return publication.publish(
                    pipeline_id=identity,
                    expected_latest_version=expected,
                    draft=draft,
                    published_at=self._now(),
                )
            except PipelineVersionConflictError:
                if not converge_on_duplicate:
                    raise
                candidate_version = PipelineVersion(1 if expected is None else expected.number + 1)
                existing = repositories.pipelines.get_version(identity, candidate_version)
                candidate = publication.prepare(draft)
                if (
                    existing is not None
                    and existing.specification == candidate
                    and existing.planner_format_version == PIPELINE_PLANNER_FORMAT_VERSION
                ):
                    return existing
                raise
            except (PipelineValidationError, PipelinePublicationError) as error:
                # Planner graph, port, resource, safety, and connector
                # rejections are deterministic request outcomes.
                raise OperationalRequestError(
                    f"pipeline document is invalid: {error}",
                    field="document",
                ) from error

    def latest_version(self, pipeline_id: str) -> int:
        """Return the immutable latest-version frontier, 0 when unpublished.

        This is the value clients echo as ``expected_latest_version`` when
        publishing. Mutable metadata row versions never participate in
        publication fencing.
        """
        identity = _pipeline_id(pipeline_id)
        with self._unit_of_work.transaction() as repositories:
            if repositories.pipelines.get(identity) is None:
                raise OperationalRecordNotFoundError("pipeline", pipeline_id)
            return repositories.pipelines.latest_version(identity)

    def get_version(self, pipeline_id: str, version: int) -> PipelineVersionRecord:
        identity = _pipeline_id(pipeline_id)
        number = _version(version)
        with self._unit_of_work.transaction() as repositories:
            record = repositories.pipelines.get_version(identity, number)
        if record is None:
            raise OperationalRecordNotFoundError("pipeline version", f"{pipeline_id} v{version}")
        return record


def _pipeline_id(value: str) -> PipelineId:
    try:
        return PipelineId.parse(value)
    except ValueError as error:
        raise OperationalRequestError(
            "pipeline identity must use the canonical pipeline format",
            field="pipeline_id",
        ) from error


def _version(value: int) -> PipelineVersion:
    try:
        return PipelineVersion(value)
    except ValueError as error:
        raise OperationalRequestError(
            "pipeline version must be a positive integer",
            field="version",
        ) from error


def _draft(document: Mapping[str, object]) -> PipelineDocument:
    try:
        return PipelineDocument.from_mapping(document)
    except Exception as error:
        raise OperationalRequestError(
            f"pipeline document is invalid: {error}",
            field="document",
        ) from error


def _require_display_fields(display_name: object, description: object) -> None:
    if type(display_name) is not str or not 1 <= len(display_name) <= (
        MAX_PIPELINE_DISPLAY_NAME_LENGTH
    ):
        raise OperationalRequestError(
            "display name must be 1 to 128 characters", field="display_name"
        )
    if description is not None and (
        type(description) is not str
        or not 1 <= len(description) <= (MAX_PIPELINE_DESCRIPTION_LENGTH)
    ):
        raise OperationalRequestError(
            "description must be 1 to 1024 characters when present",
            field="description",
        )
