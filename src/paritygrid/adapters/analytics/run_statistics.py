"""DuckDB run statistics over an authoritative SQLite DTO snapshot."""

import hashlib
import json
from collections import defaultdict
from typing import cast

from paritygrid.adapters.analytics.duckdb import DuckDBLifecycleCoordinator
from paritygrid.adapters.analytics.views import (
    DuckDBAnalyticalViewRegistry,
    DuckDBViewColumnDefinition,
    DuckDBViewDefinition,
)
from paritygrid.application.ports.analytics import (
    AnalyticalDatabaseError,
    AnalyticalViewCatalogSnapshot,
    AnalyticalViewCorruptionError,
    AnalyticalViewName,
    AnalyticalViewSchemaError,
    AnalyticalViewVersion,
)
from paritygrid.application.ports.execution import RunNodeStatus
from paritygrid.application.ports.run_statistics import (
    RunNodeStatisticsPage,
    RunNodeStatisticsRecord,
    RunStatisticsCorruptionError,
    RunStatisticsInvalidError,
    RunStatisticsQueryEngine,
    RunStatisticsQuerySnapshot,
    RunStatisticsSourceSnapshot,
    RunStatisticsStateError,
    RunStatisticsStorageError,
    RunStatisticsSummary,
    validate_run_statistics_page_limit,
)
from paritygrid.domain.execution import RunState
from paritygrid.domain.models import NodeId, RunId, UtcTimestamp

_RUN_TABLE = "paritygrid_statistics_run_source"
_NODE_TABLE = "paritygrid_statistics_node_source"
_WORK_TABLE = "paritygrid_statistics_work_source"
_ATTEMPT_TABLE = "paritygrid_statistics_attempt_source"
_NODE_VIEW = "pgv_run_statistics_10_nodes_v1"
_SUMMARY_VIEW = "pgv_run_statistics_20_summary_v1"

_NODE_COLUMNS = (
    "node_id",
    "status",
    "row_version",
    "work_total",
    "work_pending",
    "work_running",
    "work_succeeded",
    "work_quarantined",
    "work_failed",
    "work_cancelled",
    "attempt_count",
    "retry_count",
    "records_read",
    "records_written",
    "records_quarantined",
    "bytes_read",
    "bytes_written",
    "duration_microseconds",
    "attempt_latency_p50_microseconds",
    "attempt_latency_p95_microseconds",
    "attempt_latency_p99_microseconds",
)
_SUMMARY_COLUMNS = (
    "run_id",
    "run_version",
    "state",
    "node_count",
    "work_total",
    "work_pending",
    "work_running",
    "work_succeeded",
    "work_quarantined",
    "work_failed",
    "work_cancelled",
    "attempt_count",
    "retry_count",
    "records_read",
    "records_written",
    "records_quarantined",
    "bytes_read",
    "bytes_written",
    "duration_microseconds",
    "attempt_latency_p50_microseconds",
    "attempt_latency_p95_microseconds",
    "attempt_latency_p99_microseconds",
    "started_at",
    "finished_at",
)
_NODE_SELECT = f"""WITH ranked AS (
 SELECT node_id, work_item_id, attempt_number, duration_microseconds,
  row_number() OVER (
   PARTITION BY node_id
   ORDER BY duration_microseconds, work_item_id, attempt_number
  ) AS ordinal,
  count(*) OVER (PARTITION BY node_id) AS total
 FROM {_ATTEMPT_TABLE}
), latencies AS (
 SELECT node_id, count(*) AS attempt_count,
  max(CASE WHEN ordinal = greatest(1, cast(ceil(total * 0.50) AS BIGINT))
   THEN duration_microseconds END) AS p50,
  max(CASE WHEN ordinal = greatest(1, cast(ceil(total * 0.95) AS BIGINT))
   THEN duration_microseconds END) AS p95,
  max(CASE WHEN ordinal = greatest(1, cast(ceil(total * 0.99) AS BIGINT))
   THEN duration_microseconds END) AS p99
 FROM ranked GROUP BY node_id
)
SELECT node.node_id, node.status, node.row_version,
 node.work_total, node.work_pending, node.work_running, node.work_succeeded,
 node.work_quarantined, node.work_failed, node.work_cancelled,
 coalesce(latencies.attempt_count, 0) AS attempt_count, node.retry_count,
 node.records_read, node.records_written, node.records_quarantined,
 node.bytes_read, node.bytes_written, node.duration_microseconds,
 latencies.p50 AS attempt_latency_p50_microseconds,
 latencies.p95 AS attempt_latency_p95_microseconds,
 latencies.p99 AS attempt_latency_p99_microseconds
FROM {_NODE_TABLE} AS node
LEFT JOIN latencies USING (node_id)"""
_SUMMARY_SELECT = f"""WITH ranked AS (
 SELECT work_item_id, attempt_number, duration_microseconds,
  row_number() OVER (
   ORDER BY duration_microseconds, work_item_id, attempt_number
  ) AS ordinal,
  count(*) OVER () AS total
 FROM {_ATTEMPT_TABLE}
), latencies AS (
 SELECT count(*) AS attempt_count,
  max(CASE WHEN ordinal = greatest(1, cast(ceil(total * 0.50) AS BIGINT))
   THEN duration_microseconds END) AS p50,
  max(CASE WHEN ordinal = greatest(1, cast(ceil(total * 0.95) AS BIGINT))
   THEN duration_microseconds END) AS p95,
  max(CASE WHEN ordinal = greatest(1, cast(ceil(total * 0.99) AS BIGINT))
   THEN duration_microseconds END) AS p99
 FROM ranked
), node_totals AS (
 SELECT count(*) AS node_count, sum(work_total) AS work_total,
  sum(work_pending) AS work_pending, sum(work_running) AS work_running,
  sum(work_succeeded) AS work_succeeded,
  sum(work_quarantined) AS work_quarantined, sum(work_failed) AS work_failed,
  sum(work_cancelled) AS work_cancelled, sum(retry_count) AS retry_count,
  sum(records_read) AS records_read, sum(records_written) AS records_written,
  sum(records_quarantined) AS records_quarantined,
  sum(bytes_read) AS bytes_read, sum(bytes_written) AS bytes_written,
  sum(duration_microseconds) AS duration_microseconds
 FROM {_NODE_TABLE}
)
SELECT run.run_id, run.row_version AS run_version, run.state,
 node_totals.node_count, node_totals.work_total, node_totals.work_pending,
 node_totals.work_running, node_totals.work_succeeded,
 node_totals.work_quarantined, node_totals.work_failed,
 node_totals.work_cancelled, latencies.attempt_count, node_totals.retry_count,
 node_totals.records_read, node_totals.records_written,
 node_totals.records_quarantined, node_totals.bytes_read,
 node_totals.bytes_written, node_totals.duration_microseconds,
 latencies.p50 AS attempt_latency_p50_microseconds,
 latencies.p95 AS attempt_latency_p95_microseconds,
 latencies.p99 AS attempt_latency_p99_microseconds,
 run.started_at, run.finished_at
FROM {_RUN_TABLE} AS run CROSS JOIN node_totals CROSS JOIN latencies"""


class DuckDBRunStatisticsQueryEngine(RunStatisticsQueryEngine):
    """Rebuild fixed run-statistics views and validate every returned metric."""

    __slots__ = ("_database", "_registry", "_snapshot", "_source")

    def __init__(self, database: DuckDBLifecycleCoordinator) -> None:
        value = cast(object, database)
        if type(value) is not DuckDBLifecycleCoordinator:
            raise TypeError("run statistics require a DuckDB lifecycle coordinator")
        self._database = value
        self._registry = DuckDBAnalyticalViewRegistry(value)
        self._snapshot: RunStatisticsQuerySnapshot | None = None
        self._source: RunStatisticsSourceSnapshot | None = None

    def rebuild(self, source: RunStatisticsSourceSnapshot) -> RunStatisticsQuerySnapshot:
        """Replace disposable tables and install reviewed v1 metric views."""
        value = cast(object, source)
        if type(value) is not RunStatisticsSourceSnapshot:
            raise TypeError("run statistics source snapshot is invalid")
        self._snapshot = None
        self._source = None
        try:
            self._database.recreate()
            with self._database._transaction():  # pyright: ignore[reportPrivateUsage]
                self._create_source_tables()
                self._load_source(value)
                self._require_source_counts(value)
            catalog = self._registry.synchronize(_view_definitions())
        except AnalyticalDatabaseError:
            raise RunStatisticsStorageError("run statistics rebuild failed") from None
        query_sha256 = _query_sha256(value.source_sha256, _catalog_sha256(catalog))
        snapshot = RunStatisticsQuerySnapshot(
            run_id=value.run.run_id,
            run_version=value.run.row_version,
            source_sha256=value.source_sha256,
            query_sha256=query_sha256,
            node_count=len(value.nodes),
            work_item_count=len(value.work_items),
            attempt_count=len(value.attempts),
            view_catalog=catalog,
        )
        self._source = value
        self._snapshot = snapshot
        return snapshot

    def get_summary(self, snapshot: RunStatisticsQuerySnapshot) -> RunStatisticsSummary:
        """Return the exact run summary after independent source comparison."""
        installed, source = self._require_snapshot(snapshot)
        try:
            self._require_catalog(installed)
            rows = self._database._fetch_all(  # pyright: ignore[reportPrivateUsage]
                f"SELECT * FROM {_SUMMARY_VIEW}"
            )
        except RunStatisticsCorruptionError:
            raise
        except AnalyticalViewCorruptionError, AnalyticalViewSchemaError:
            raise RunStatisticsCorruptionError(
                "run statistics analytical views are corrupt"
            ) from None
        except AnalyticalDatabaseError:
            raise RunStatisticsStorageError("run statistics query failed") from None
        if len(rows) != 1:
            raise RunStatisticsCorruptionError("run statistics summary cardinality differs")
        record = _summary_from_row(rows[0])
        if record != _expected_summary(source):
            raise RunStatisticsCorruptionError("run statistics summary differs from source")
        return record

    def list_nodes(
        self,
        snapshot: RunStatisticsQuerySnapshot,
        *,
        limit: int,
        after: NodeId | None = None,
    ) -> RunNodeStatisticsPage:
        """Return one stable node page after independent source comparison."""
        installed, source = self._require_snapshot(snapshot)
        page_size = validate_run_statistics_page_limit(limit)
        cursor = _require_cursor(after)
        try:
            self._require_catalog(installed)
            predicate = ""
            parameters: tuple[object, ...] = (page_size + 1,)
            if cursor is not None:
                predicate = " WHERE node_id > ?"
                parameters = (str(cursor), page_size + 1)
            rows = self._database._fetch_all(  # pyright: ignore[reportPrivateUsage]
                f"SELECT * FROM {_NODE_VIEW}{predicate} ORDER BY node_id LIMIT ?",
                parameters,
            )
        except RunStatisticsCorruptionError:
            raise
        except AnalyticalViewCorruptionError, AnalyticalViewSchemaError:
            raise RunStatisticsCorruptionError(
                "run statistics analytical views are corrupt"
            ) from None
        except AnalyticalDatabaseError:
            raise RunStatisticsStorageError("run statistics query failed") from None
        items = tuple(_node_from_row(row) for row in rows[:page_size])
        expected = _expected_nodes(source)
        if any(expected.get(item.node_id) != item for item in items):
            raise RunStatisticsCorruptionError("node statistics differ from source")
        next_cursor = items[-1].node_id if len(rows) > page_size else None
        return RunNodeStatisticsPage(installed, items, next_cursor)

    def _require_snapshot(
        self, value: object
    ) -> tuple[RunStatisticsQuerySnapshot, RunStatisticsSourceSnapshot]:
        if type(value) is not RunStatisticsQuerySnapshot:
            raise TypeError("run statistics snapshot is invalid")
        snapshot = value
        if self._snapshot != snapshot or self._source is None:
            raise RunStatisticsStateError("run statistics snapshot is not installed")
        return snapshot, self._source

    def _require_catalog(self, snapshot: RunStatisticsQuerySnapshot) -> None:
        if self._registry.snapshot() != snapshot.view_catalog:
            raise RunStatisticsCorruptionError("run statistics analytical views changed")

    def _create_source_tables(self) -> None:
        definitions = (
            f"CREATE TABLE {_RUN_TABLE} (run_id VARCHAR, state VARCHAR, "
            "row_version INTEGER, created_at VARCHAR, started_at VARCHAR, "
            "finished_at VARCHAR)",
            f"CREATE TABLE {_NODE_TABLE} (node_id VARCHAR, status VARCHAR, "
            "row_version INTEGER, work_total BIGINT, work_pending BIGINT, "
            "work_running BIGINT, work_succeeded BIGINT, work_quarantined BIGINT, "
            "work_failed BIGINT, work_cancelled BIGINT, records_read BIGINT, "
            "records_written BIGINT, records_quarantined BIGINT, bytes_read BIGINT, "
            "bytes_written BIGINT, retry_count BIGINT, duration_microseconds BIGINT)",
            f"CREATE TABLE {_WORK_TABLE} (work_item_id VARCHAR, node_id VARCHAR, "
            "state VARCHAR, completed_attempt_count INTEGER)",
            f"CREATE TABLE {_ATTEMPT_TABLE} (work_item_id VARCHAR, node_id VARCHAR, "
            "attempt_number INTEGER, outcome VARCHAR, duration_microseconds BIGINT)",
        )
        for statement in definitions:
            self._database._execute(statement)  # pyright: ignore[reportPrivateUsage]

    def _load_source(self, source: RunStatisticsSourceSnapshot) -> None:
        run = source.run
        self._database._execute(  # pyright: ignore[reportPrivateUsage]
            f"INSERT INTO {_RUN_TABLE} VALUES (?, ?, ?, ?, ?, ?)",
            (
                str(run.run_id),
                run.state.value,
                run.row_version,
                str(run.created_at),
                None if run.started_at is None else str(run.started_at),
                None if run.finished_at is None else str(run.finished_at),
            ),
        )
        self._database._execute_many(  # pyright: ignore[reportPrivateUsage]
            f"INSERT INTO {_NODE_TABLE} VALUES ({', '.join('?' for _ in range(17))})",
            tuple(
                (
                    str(node.node_id),
                    node.status.value,
                    node.row_version,
                    node.work_total,
                    node.work_pending,
                    node.work_running,
                    node.work_succeeded,
                    node.work_quarantined,
                    node.work_failed,
                    node.work_cancelled,
                    node.records_read,
                    node.records_written,
                    node.records_quarantined,
                    node.bytes_read,
                    node.bytes_written,
                    node.retry_count,
                    node.duration.microseconds,
                )
                for node in source.nodes
            ),
        )
        work_rows = tuple(
            (
                str(work.work_item_id),
                str(work.node_id),
                work.state.value,
                work.completed_attempt_count,
            )
            for work in source.work_items
        )
        if work_rows:
            self._database._execute_many(  # pyright: ignore[reportPrivateUsage]
                f"INSERT INTO {_WORK_TABLE} VALUES (?, ?, ?, ?)", work_rows
            )
        node_by_work = {work.work_item_id: work.node_id for work in source.work_items}
        attempt_rows = tuple(
            (
                str(attempt.work_item_id),
                str(node_by_work[attempt.work_item_id]),
                int(attempt.attempt_number),
                attempt.outcome.value,
                attempt.duration.microseconds,
            )
            for attempt in source.attempts
        )
        if attempt_rows:
            self._database._execute_many(  # pyright: ignore[reportPrivateUsage]
                f"INSERT INTO {_ATTEMPT_TABLE} VALUES (?, ?, ?, ?, ?)",
                attempt_rows,
            )

    def _require_source_counts(self, source: RunStatisticsSourceSnapshot) -> None:
        expected = (1, len(source.nodes), len(source.work_items), len(source.attempts))
        actual = tuple(
            _single_count(
                self._database._fetch_all(f"SELECT count(*) FROM {table}")  # pyright: ignore[reportPrivateUsage]
            )
            for table in (_RUN_TABLE, _NODE_TABLE, _WORK_TABLE, _ATTEMPT_TABLE)
        )
        if actual != expected:
            raise RunStatisticsCorruptionError("run statistics source counts differ")


def _view_definitions() -> tuple[DuckDBViewDefinition, ...]:
    node_columns = tuple(
        DuckDBViewColumnDefinition(
            name,
            "VARCHAR" if index in {0, 1} else "INTEGER" if index == 2 else "BIGINT",
            True,
        )
        for index, name in enumerate(_NODE_COLUMNS)
    )
    summary_columns = tuple(
        DuckDBViewColumnDefinition(
            name,
            "VARCHAR"
            if index in {0, 2, 22, 23}
            else "INTEGER"
            if index == 1
            else "HUGEINT"
            if index in {*range(4, 11), *range(12, 19)}
            else "BIGINT",
            True,
        )
        for index, name in enumerate(_SUMMARY_COLUMNS)
    )
    return (
        DuckDBViewDefinition(
            AnalyticalViewName(_NODE_VIEW),
            AnalyticalViewVersion(1),
            _NODE_SELECT,
            node_columns,
        ),
        DuckDBViewDefinition(
            AnalyticalViewName(_SUMMARY_VIEW),
            AnalyticalViewVersion(1),
            _SUMMARY_SELECT,
            summary_columns,
        ),
    )


def _summary_from_row(row: tuple[object, ...]) -> RunStatisticsSummary:
    if len(row) != len(_SUMMARY_COLUMNS):
        raise RunStatisticsCorruptionError("run statistics summary row is malformed")
    try:
        return RunStatisticsSummary(
            run_id=RunId(cast(str, row[0])),
            run_version=cast(int, row[1]),
            state=RunState(cast(str, row[2])),
            node_count=cast(int, row[3]),
            work_total=cast(int, row[4]),
            work_pending=cast(int, row[5]),
            work_running=cast(int, row[6]),
            work_succeeded=cast(int, row[7]),
            work_quarantined=cast(int, row[8]),
            work_failed=cast(int, row[9]),
            work_cancelled=cast(int, row[10]),
            attempt_count=cast(int, row[11]),
            retry_count=cast(int, row[12]),
            records_read=cast(int, row[13]),
            records_written=cast(int, row[14]),
            records_quarantined=cast(int, row[15]),
            bytes_read=cast(int, row[16]),
            bytes_written=cast(int, row[17]),
            duration_microseconds=cast(int, row[18]),
            attempt_latency_p50_microseconds=cast(int | None, row[19]),
            attempt_latency_p95_microseconds=cast(int | None, row[20]),
            attempt_latency_p99_microseconds=cast(int | None, row[21]),
            started_at=_timestamp(row[22]),
            finished_at=_timestamp(row[23]),
        )
    except TypeError, ValueError, RunStatisticsInvalidError:
        raise RunStatisticsCorruptionError("run statistics summary row is malformed") from None


def _node_from_row(row: tuple[object, ...]) -> RunNodeStatisticsRecord:
    if len(row) != len(_NODE_COLUMNS):
        raise RunStatisticsCorruptionError("node statistics row is malformed")
    try:
        return RunNodeStatisticsRecord(
            node_id=NodeId(cast(str, row[0])),
            status=RunNodeStatus(cast(str, row[1])),
            row_version=cast(int, row[2]),
            work_total=cast(int, row[3]),
            work_pending=cast(int, row[4]),
            work_running=cast(int, row[5]),
            work_succeeded=cast(int, row[6]),
            work_quarantined=cast(int, row[7]),
            work_failed=cast(int, row[8]),
            work_cancelled=cast(int, row[9]),
            attempt_count=cast(int, row[10]),
            retry_count=cast(int, row[11]),
            records_read=cast(int, row[12]),
            records_written=cast(int, row[13]),
            records_quarantined=cast(int, row[14]),
            bytes_read=cast(int, row[15]),
            bytes_written=cast(int, row[16]),
            duration_microseconds=cast(int, row[17]),
            attempt_latency_p50_microseconds=cast(int | None, row[18]),
            attempt_latency_p95_microseconds=cast(int | None, row[19]),
            attempt_latency_p99_microseconds=cast(int | None, row[20]),
        )
    except TypeError, ValueError, RunStatisticsInvalidError:
        raise RunStatisticsCorruptionError("node statistics row is malformed") from None


def _expected_summary(source: RunStatisticsSourceSnapshot) -> RunStatisticsSummary:
    nodes = source.nodes
    latencies = tuple(attempt.duration.microseconds for attempt in source.attempts)
    return RunStatisticsSummary(
        run_id=source.run.run_id,
        run_version=source.run.row_version,
        state=source.run.state,
        node_count=len(nodes),
        work_total=sum(node.work_total for node in nodes),
        work_pending=sum(node.work_pending for node in nodes),
        work_running=sum(node.work_running for node in nodes),
        work_succeeded=sum(node.work_succeeded for node in nodes),
        work_quarantined=sum(node.work_quarantined for node in nodes),
        work_failed=sum(node.work_failed for node in nodes),
        work_cancelled=sum(node.work_cancelled for node in nodes),
        attempt_count=len(source.attempts),
        retry_count=sum(node.retry_count for node in nodes),
        records_read=sum(node.records_read for node in nodes),
        records_written=sum(node.records_written for node in nodes),
        records_quarantined=sum(node.records_quarantined for node in nodes),
        bytes_read=sum(node.bytes_read for node in nodes),
        bytes_written=sum(node.bytes_written for node in nodes),
        duration_microseconds=sum(node.duration.microseconds for node in nodes),
        attempt_latency_p50_microseconds=_nearest_rank(latencies, 50),
        attempt_latency_p95_microseconds=_nearest_rank(latencies, 95),
        attempt_latency_p99_microseconds=_nearest_rank(latencies, 99),
        started_at=source.run.started_at,
        finished_at=source.run.finished_at,
    )


def _expected_nodes(
    source: RunStatisticsSourceSnapshot,
) -> dict[NodeId, RunNodeStatisticsRecord]:
    node_by_work = {work.work_item_id: work.node_id for work in source.work_items}
    durations: dict[NodeId, list[int]] = defaultdict(list)
    for attempt in source.attempts:
        durations[node_by_work[attempt.work_item_id]].append(attempt.duration.microseconds)
    return {
        node.node_id: RunNodeStatisticsRecord(
            node_id=node.node_id,
            status=node.status,
            row_version=node.row_version,
            work_total=node.work_total,
            work_pending=node.work_pending,
            work_running=node.work_running,
            work_succeeded=node.work_succeeded,
            work_quarantined=node.work_quarantined,
            work_failed=node.work_failed,
            work_cancelled=node.work_cancelled,
            attempt_count=len(durations[node.node_id]),
            retry_count=node.retry_count,
            records_read=node.records_read,
            records_written=node.records_written,
            records_quarantined=node.records_quarantined,
            bytes_read=node.bytes_read,
            bytes_written=node.bytes_written,
            duration_microseconds=node.duration.microseconds,
            attempt_latency_p50_microseconds=_nearest_rank(tuple(durations[node.node_id]), 50),
            attempt_latency_p95_microseconds=_nearest_rank(tuple(durations[node.node_id]), 95),
            attempt_latency_p99_microseconds=_nearest_rank(tuple(durations[node.node_id]), 99),
        )
        for node in source.nodes
    }


def _nearest_rank(values: tuple[int, ...], percentile: int) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, (len(ordered) * percentile + 99) // 100)
    return ordered[rank - 1]


def _timestamp(value: object) -> UtcTimestamp | None:
    if value is None:
        return None
    if type(value) is not str:
        raise TypeError("statistics timestamp is invalid")
    return UtcTimestamp.parse(value)


def _single_count(rows: tuple[tuple[object, ...], ...]) -> int:
    if len(rows) != 1 or len(rows[0]) != 1 or type(rows[0][0]) is not int:
        raise RunStatisticsCorruptionError("run statistics source count is malformed")
    return rows[0][0]


def _require_cursor(value: object) -> NodeId | None:
    if value is None:
        return None
    if type(value) is not NodeId:
        raise TypeError("node statistics cursor is invalid")
    return value


def _query_sha256(source_sha256: str, catalog_sha256: str) -> str:
    encoded = json.dumps(
        {
            "source_sha256": source_sha256,
            "view_catalog_sha256": catalog_sha256,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _catalog_sha256(catalog: object) -> str:
    if type(catalog) is not AnalyticalViewCatalogSnapshot:
        raise TypeError("run statistics view catalog is invalid")
    records = catalog.views
    value = [
        {
            "definition_sha256": record.definition_sha256,
            "name": str(record.name),
            "output_schema_sha256": record.output_schema_sha256,
            "version": record.version.value,
        }
        for record in records
    ]
    encoded = json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()
