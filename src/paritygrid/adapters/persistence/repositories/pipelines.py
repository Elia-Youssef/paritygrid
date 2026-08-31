"""SQLAlchemy repository for pipeline configuration and immutable versions."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from sqlalchemy import func, literal, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from paritygrid.adapters.persistence.repositories.common import (
    MAX_PERSISTED_INTEGER,
    bounded_text,
    encode_document,
    optional_text,
    positive_int,
    require_document,
    require_incrementable,
    require_pipeline_id,
    require_pipeline_version,
    require_timestamp,
    stored_nonnegative_int,
    translate_storage_errors,
)
from paritygrid.adapters.persistence.repositories.mapping import (
    pipeline_from_row,
    pipeline_version_from_row,
    raise_pipeline_cas_failure,
    require_pipeline_cas,
)
from paritygrid.adapters.persistence.schema import pipeline_versions, pipelines
from paritygrid.application.ports import (
    ConfigurationDocument,
    CorruptRepositoryRecordError,
    DuplicateRecordError,
    InvalidRepositoryRequestError,
    PipelinePage,
    PipelineRecord,
    PipelineRepository,
    PipelineVersionConflictError,
    PipelineVersionPage,
    PipelineVersionRecord,
    RecordNotFoundError,
    RecordStateConflictError,
    validate_page_limit,
)
from paritygrid.domain.models import PipelineId, PipelineVersion, UtcTimestamp


@dataclass(frozen=True, slots=True)
class VersionState:
    """Validated aggregate state for one contiguous pipeline history."""

    latest: int


class SqlAlchemyPipelineRepository(PipelineRepository):
    """Persist pipeline configuration within a caller-owned Session transaction."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @translate_storage_errors
    def create(
        self,
        *,
        pipeline_id: PipelineId,
        display_name: str,
        description: str | None,
        created_at: UtcTimestamp,
    ) -> PipelineRecord:
        self._require_transaction()
        identity = require_pipeline_id(pipeline_id)
        timestamp = require_timestamp(created_at, "pipeline creation time")
        values = {
            "pipeline_id": str(identity),
            "display_name": bounded_text(display_name, "pipeline display name", 160),
            "description": optional_text(description, "pipeline description"),
            "created_at": str(timestamp),
            "archived_at": None,
            "row_version": 1,
        }
        row = (
            self._session.execute(
                sqlite_insert(pipelines)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[pipelines.c.pipeline_id])
                .returning(*pipelines.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise DuplicateRecordError("pipeline already exists")
        return pipeline_from_row(row)

    @translate_storage_errors
    def get(self, pipeline_id: PipelineId) -> PipelineRecord | None:
        self._require_transaction()
        identity = require_pipeline_id(pipeline_id)
        row = (
            self._session.execute(select(pipelines).where(pipelines.c.pipeline_id == str(identity)))
            .mappings()
            .one_or_none()
        )
        return None if row is None else pipeline_from_row(row)

    @translate_storage_errors
    def list(
        self,
        *,
        limit: int,
        after: PipelineId | None = None,
        include_archived: bool = False,
    ) -> PipelinePage:
        self._require_transaction()
        page_size = validate_page_limit(limit)
        if type(include_archived) is not bool:
            raise InvalidRepositoryRequestError("include archived must be boolean")
        cursor = None if after is None else require_pipeline_id(after)
        query = select(pipelines)
        if cursor is not None:
            query = query.where(pipelines.c.pipeline_id > str(cursor))
        if not include_archived:
            query = query.where(pipelines.c.archived_at.is_(None))
        rows = (
            self._session.execute(query.order_by(pipelines.c.pipeline_id).limit(page_size + 1))
            .mappings()
            .all()
        )
        records = tuple(pipeline_from_row(row) for row in rows[:page_size])
        next_cursor = records[-1].pipeline_id if len(rows) > page_size else None
        return PipelinePage(items=records, next_cursor=next_cursor)

    @translate_storage_errors
    def latest_version(self, pipeline_id: PipelineId) -> int:
        """Return the newest immutable published version, or 0 if none."""
        self._require_transaction()
        identity = require_pipeline_id(pipeline_id)
        return self._version_state(identity).latest

    @translate_storage_errors
    def archive(
        self,
        pipeline_id: PipelineId,
        *,
        expected_row_version: int,
        archived_at: UtcTimestamp,
    ) -> PipelineRecord:
        self._require_transaction()
        identity = require_pipeline_id(pipeline_id)
        expected = positive_int(expected_row_version, "expected row version")
        timestamp = require_timestamp(archived_at, "pipeline archive time")
        current = require_pipeline_cas(self.get(identity), expected)
        require_incrementable(expected, "pipeline row version")
        if timestamp <= current.created_at:
            raise InvalidRepositoryRequestError(
                "pipeline archive time must be later than creation time"
            )
        row = (
            self._session.execute(
                update(pipelines)
                .where(
                    pipelines.c.pipeline_id == str(identity),
                    pipelines.c.row_version == expected,
                    pipelines.c.archived_at.is_(None),
                )
                .values(archived_at=str(timestamp), row_version=expected + 1)
                .returning(*pipelines.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise_pipeline_cas_failure(self.get(identity), expected, "pipeline")
        return pipeline_from_row(row)

    @translate_storage_errors
    def update_metadata(
        self,
        pipeline_id: PipelineId,
        *,
        expected_row_version: int,
        display_name: str,
        description: str | None,
    ) -> PipelineRecord:
        self._require_transaction()
        identity = require_pipeline_id(pipeline_id)
        expected = positive_int(expected_row_version, "expected row version")
        current = require_pipeline_cas(self.get(identity), expected)
        name = bounded_text(display_name, "pipeline display name", 160)
        detail = optional_text(description, "pipeline description")
        if name == current.display_name and detail == current.description:
            return current
        require_incrementable(expected, "pipeline row version")
        row = (
            self._session.execute(
                update(pipelines)
                .where(
                    pipelines.c.pipeline_id == str(identity),
                    pipelines.c.row_version == expected,
                    pipelines.c.archived_at.is_(None),
                )
                .values(display_name=name, description=detail, row_version=expected + 1)
                .returning(*pipelines.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise_pipeline_cas_failure(self.get(identity), expected, "pipeline")
        return pipeline_from_row(row)

    @translate_storage_errors
    def publish_version(
        self,
        *,
        pipeline_id: PipelineId,
        expected_latest_version: PipelineVersion | None,
        specification: ConfigurationDocument,
        planner_format_version: int,
        published_at: UtcTimestamp,
    ) -> PipelineVersionRecord:
        self._require_transaction()
        identity = require_pipeline_id(pipeline_id)
        expected_value = (
            None
            if expected_latest_version is None
            else require_pipeline_version(expected_latest_version, "expected latest version")
        )
        spec = require_document(specification, "pipeline specification")
        timestamp = require_timestamp(published_at, "pipeline publication time")
        planner_format = positive_int(planner_format_version, "planner format version")
        expected_number = 0 if expected_value is None else int(expected_value)
        if expected_number >= MAX_PERSISTED_INTEGER:
            raise PipelineVersionConflictError(
                "pipeline version cannot advance beyond the supported maximum"
            )
        pipeline = self.get(identity)
        if pipeline is None:
            raise RecordNotFoundError("pipeline does not exist")
        if pipeline.archived_at is not None:
            raise RecordStateConflictError("archived pipeline cannot publish versions")
        if timestamp < pipeline.created_at:
            raise InvalidRepositoryRequestError(
                "pipeline publication time cannot precede creation time"
            )
        state = self._version_state(identity)
        allocated_version = PipelineVersion(expected_number + 1)
        encoded = encode_document(spec)
        digest = hashlib.sha256(encoded.text.encode("utf-8")).hexdigest()
        candidate = PipelineVersionRecord(
            pipeline_id=identity,
            version=allocated_version,
            specification=spec,
            specification_sha256=digest,
            planner_format_version=planner_format,
            published_at=timestamp,
        )
        if state.latest == int(allocated_version):
            installed = self._get_version_row(identity, allocated_version, pipeline.created_at)
            if installed == candidate:
                assert installed is not None
                return installed
            raise PipelineVersionConflictError("pipeline version replay does not match")
        if state.latest != expected_number:
            raise PipelineVersionConflictError("pipeline latest version is stale")

        count_query = select(func.count(pipeline_versions.c.version_number)).where(
            pipeline_versions.c.pipeline_id == str(identity)
        )
        maximum_query = select(
            func.coalesce(func.max(pipeline_versions.c.version_number), 0)
        ).where(pipeline_versions.c.pipeline_id == str(identity))
        guarded_values = select(
            literal(str(identity)),
            literal(int(allocated_version)),
            literal(encoded.text),
            literal(digest),
            literal(planner_format),
            literal(str(timestamp)),
        ).where(
            count_query.scalar_subquery() == expected_number,
            maximum_query.scalar_subquery() == expected_number,
        )
        row = (
            self._session.execute(
                sqlite_insert(pipeline_versions)
                .from_select(
                    (
                        "pipeline_id",
                        "version_number",
                        "specification_json",
                        "specification_sha256",
                        "planner_format_version",
                        "published_at",
                    ),
                    guarded_values,
                )
                .on_conflict_do_nothing(
                    index_elements=[
                        pipeline_versions.c.pipeline_id,
                        pipeline_versions.c.version_number,
                    ]
                )
                .returning(*pipeline_versions.c)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            new_state = self._version_state(identity)
            installed = self._get_version_row(identity, allocated_version, pipeline.created_at)
            if new_state.latest == int(allocated_version) and installed == candidate:
                assert installed is not None
                return installed
            raise PipelineVersionConflictError("pipeline latest version is stale")
        return pipeline_version_from_row(row, pipeline_created_at=pipeline.created_at)

    @translate_storage_errors
    def get_version(
        self, pipeline_id: PipelineId, version: PipelineVersion
    ) -> PipelineVersionRecord | None:
        self._require_transaction()
        identity = require_pipeline_id(pipeline_id)
        requested = require_pipeline_version(version, "pipeline version")
        pipeline = self.get(identity)
        state = self._version_state(identity)
        if pipeline is None:
            if state.latest:
                raise CorruptRepositoryRecordError("pipeline version parent is missing")
            return None
        return self._get_version_row(identity, requested, pipeline.created_at)

    @translate_storage_errors
    def list_versions(
        self,
        pipeline_id: PipelineId,
        *,
        limit: int,
        after: PipelineVersion | None = None,
    ) -> PipelineVersionPage:
        self._require_transaction()
        identity = require_pipeline_id(pipeline_id)
        cursor = None if after is None else require_pipeline_version(after, "version cursor")
        page_size = validate_page_limit(limit)
        pipeline = self.get(identity)
        state = self._version_state(identity)
        if pipeline is None:
            if state.latest:
                raise CorruptRepositoryRecordError("pipeline version parent is missing")
            return PipelineVersionPage(items=(), next_cursor=None)
        query = select(pipeline_versions).where(pipeline_versions.c.pipeline_id == str(identity))
        if cursor is not None:
            query = query.where(pipeline_versions.c.version_number > int(cursor))
        rows = (
            self._session.execute(
                query.order_by(pipeline_versions.c.version_number).limit(page_size + 1)
            )
            .mappings()
            .all()
        )
        records = tuple(
            pipeline_version_from_row(row, pipeline_created_at=pipeline.created_at)
            for row in rows[:page_size]
        )
        next_cursor = records[-1].version if len(rows) > page_size else None
        return PipelineVersionPage(items=records, next_cursor=next_cursor)

    def _get_version_row(
        self,
        pipeline_id: PipelineId,
        version: PipelineVersion,
        pipeline_created_at: UtcTimestamp,
    ) -> PipelineVersionRecord | None:
        row = (
            self._session.execute(
                select(pipeline_versions).where(
                    pipeline_versions.c.pipeline_id == str(pipeline_id),
                    pipeline_versions.c.version_number == int(version),
                )
            )
            .mappings()
            .one_or_none()
        )
        return (
            None
            if row is None
            else pipeline_version_from_row(row, pipeline_created_at=pipeline_created_at)
        )

    def _version_state(self, pipeline_id: PipelineId) -> VersionState:
        row = self._session.execute(
            select(
                func.count(pipeline_versions.c.version_number),
                func.min(pipeline_versions.c.version_number),
                func.max(pipeline_versions.c.version_number),
            ).where(pipeline_versions.c.pipeline_id == str(pipeline_id))
        ).one()
        count = stored_nonnegative_int(row[0], "pipeline version count")
        if count == 0:
            if row[1] is not None or row[2] is not None:
                raise CorruptRepositoryRecordError("pipeline version history is corrupt")
            return VersionState(latest=0)
        minimum = stored_nonnegative_int(row[1], "pipeline minimum version")
        maximum = stored_nonnegative_int(row[2], "pipeline latest version")
        if minimum != 1 or maximum == 0 or count != maximum:
            raise CorruptRepositoryRecordError("pipeline version history is not contiguous")
        return VersionState(latest=maximum)

    def _latest_version_number(self, pipeline_id: PipelineId) -> int:
        """Compatibility helper that now validates the complete version sequence."""
        identity = require_pipeline_id(pipeline_id)
        return self._version_state(identity).latest

    def _require_transaction(self) -> None:
        if not self._session.in_transaction():
            raise InvalidRepositoryRequestError("repository requires a caller-owned transaction")
