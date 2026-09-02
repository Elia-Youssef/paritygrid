"""Deterministic CSV and JSON Lines fixture writers and bounded readers.

Fixtures render a :class:`~paritygrid.demo.datasets.SyntheticDataset` to
byte-stable files. The encoding contract is UTF-8 without a byte-order mark
and records are terminated by a single line feed, so file bytes reproduce
exactly across runs and platforms. Dataset rows whose payload is deliberately
invalid are additionally corrupted at the row level (wrong field count in
CSV, unparsable lines in JSON Lines) so later connectors meet both
value-level and row-level malformation.
"""

import csv
import hashlib
import io
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from paritygrid.demo.datasets import RowRole, SyntheticDataset, WireValue

FIXTURE_FORMAT_VERSION = 1
FIXTURE_ENCODING = "utf-8"
FIXTURE_NEWLINE = "lf"

MAX_FIXTURE_ROWS = 5_000
MAX_FIXTURE_BYTES = 8 * 1024 * 1024

_CSV_COLUMNS = (
    "source_record_key",
    "sku",
    "name",
    "quantity",
    "currency",
    "amount",
    "updated_at",
    "attributes",
)


class FixtureError(ValueError):
    """Raised when fixture settings or inputs are invalid."""


@dataclass(frozen=True, slots=True)
class FixtureManifest:
    """Canonical description of one written fixture file."""

    kind: str
    encoding: str
    newline: str
    row_count: int
    malformed_row_count: int
    duplicate_row_count: int
    byte_size: int
    sha256: str

    def canonical_bytes(self) -> bytes:
        """Return the deterministic manifest document."""
        return json.dumps(
            {
                "byte_size": self.byte_size,
                "duplicate_row_count": self.duplicate_row_count,
                "encoding": self.encoding,
                "format_version": FIXTURE_FORMAT_VERSION,
                "kind": self.kind,
                "malformed_row_count": self.malformed_row_count,
                "newline": self.newline,
                "row_count": self.row_count,
                "sha256": self.sha256,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")


@dataclass(frozen=True, slots=True)
class FixtureBounds:
    """Explicit row and byte caps for fixture writing and reading."""

    max_rows: int = MAX_FIXTURE_ROWS
    max_bytes: int = MAX_FIXTURE_BYTES

    def __post_init__(self) -> None:
        for name in ("max_rows", "max_bytes"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise FixtureError(f"{name} must be an integer")
            if value < 1:
                raise FixtureError(f"{name} must be at least one")
        if self.max_rows > MAX_FIXTURE_ROWS:
            raise FixtureError(f"max rows must not exceed {MAX_FIXTURE_ROWS}")
        if self.max_bytes > MAX_FIXTURE_BYTES:
            raise FixtureError(f"max bytes must not exceed {MAX_FIXTURE_BYTES}")


def render_csv_fixture(dataset: SyntheticDataset) -> tuple[bytes, int, int]:
    """Render the deterministic UTF-8 LF-terminated CSV payload.

    Returns the exact bytes a :func:`write_csv_fixture` call writes plus the
    deliberately corrupted and duplicated row counts, so derivation and
    writing share one rendering path.
    """
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(_CSV_COLUMNS)
    malformed_rows = 0
    duplicate_rows = 0
    for row in dataset.rows:
        if row.role is RowRole.MALFORMED:
            fields = _csv_fields(row.payload)
            if malformed_rows % 2 == 0:
                fields = fields[:-1]
            else:
                fields = [*fields, "unexpected-extra-column"]
            writer.writerow(fields)
            malformed_rows += 1
            continue
        if row.role is RowRole.DUPLICATE:
            duplicate_rows += 1
        writer.writerow(_csv_fields(row.payload))
    return buffer.getvalue().encode(FIXTURE_ENCODING), malformed_rows, duplicate_rows


def render_jsonl_fixture(dataset: SyntheticDataset) -> tuple[bytes, int, int]:
    """Render the deterministic UTF-8 LF-terminated JSON Lines payload."""
    lines: list[bytes] = []
    malformed_rows = 0
    duplicate_rows = 0
    for row in dataset.rows:
        if row.role is RowRole.MALFORMED:
            canonical = json.dumps(
                dict(row.payload), ensure_ascii=True, separators=(",", ":"), sort_keys=True
            )
            variant = malformed_rows % 3
            if variant == 0:
                line = canonical[:-2]
            elif variant == 1:
                line = "not-json"
            else:
                line = ""
            lines.append(line.encode("ascii"))
            malformed_rows += 1
            continue
        if row.role is RowRole.DUPLICATE:
            duplicate_rows += 1
        canonical = json.dumps(
            dict(row.payload), ensure_ascii=True, separators=(",", ":"), sort_keys=True
        )
        lines.append(canonical.encode("ascii"))
    return b"".join(line + b"\n" for line in lines), malformed_rows, duplicate_rows


def write_csv_fixture(
    dataset: SyntheticDataset,
    destination: Path,
    *,
    bounds: FixtureBounds | None = None,
) -> FixtureManifest:
    """Write the dataset as a deterministic UTF-8 LF-terminated CSV file."""
    effective_bounds = bounds if bounds is not None else FixtureBounds()
    payload, malformed_rows, duplicate_rows = render_csv_fixture(dataset)
    _validate_bounds(
        row_count=len(dataset.rows),
        byte_size=len(payload),
        bounds=effective_bounds,
        subject="csv fixture",
    )
    return _write_payload(
        destination,
        payload,
        kind="csv",
        dataset=dataset,
        malformed_rows=malformed_rows,
        duplicate_rows=duplicate_rows,
    )


def write_jsonl_fixture(
    dataset: SyntheticDataset,
    destination: Path,
    *,
    bounds: FixtureBounds | None = None,
) -> FixtureManifest:
    """Write the dataset as deterministic UTF-8 LF-terminated JSON Lines."""
    effective_bounds = bounds if bounds is not None else FixtureBounds()
    payload, malformed_rows, duplicate_rows = render_jsonl_fixture(dataset)
    _validate_bounds(
        row_count=len(dataset.rows),
        byte_size=len(payload),
        bounds=effective_bounds,
        subject="jsonl fixture",
    )
    return _write_payload(
        destination,
        payload,
        kind="jsonl",
        dataset=dataset,
        malformed_rows=malformed_rows,
        duplicate_rows=duplicate_rows,
    )


def read_csv_rows(
    source: Path,
    *,
    bounds: FixtureBounds | None = None,
) -> list[list[str]]:
    """Read every CSV row under explicit row and byte bounds."""
    effective_bounds = bounds if bounds is not None else FixtureBounds()
    payload = _read_bounded_payload(source, effective_bounds)
    text = payload.decode(FIXTURE_ENCODING)
    if "\r" in text:
        raise FixtureError("csv fixture must use lf line endings only")
    rows: list[list[str]] = []
    for row in csv.reader(io.StringIO(text, newline="")):
        if not row:
            continue
        rows.append(row)
        _validate_bounds(
            row_count=len(rows), byte_size=len(payload), bounds=effective_bounds, subject="csv read"
        )
    return rows


def read_jsonl_rows(
    source: Path,
    *,
    bounds: FixtureBounds | None = None,
) -> list[tuple[int, object]]:
    """Read every JSON Lines row under explicit row and byte bounds.

    Lines that are empty or fail to parse decode to ``None`` so malformed-row
    behavior stays observable to callers without hiding the line number.
    """
    effective_bounds = bounds if bounds is not None else FixtureBounds()
    payload = _read_bounded_payload(source, effective_bounds)
    if b"\r" in payload:
        raise FixtureError("jsonl fixture must use lf line endings only")
    rows: list[tuple[int, object]] = []
    for line_number, raw_line in enumerate(payload.split(b"\n")[:-1], start=1):
        text = raw_line.decode(FIXTURE_ENCODING)
        if text.strip() == "":
            rows.append((line_number, None))
        else:
            try:
                rows.append((line_number, json.loads(text)))
            except json.JSONDecodeError:
                rows.append((line_number, None))
        _validate_bounds(
            row_count=len(rows),
            byte_size=len(payload),
            bounds=effective_bounds,
            subject="jsonl read",
        )
    return rows


def _csv_fields(payload: Mapping[str, WireValue]) -> list[str]:
    unit_price = payload.get("unit_price")
    if isinstance(unit_price, dict):
        currency = unit_price.get("currency")
        amount = unit_price.get("amount")
    else:
        currency = None
        amount = None
    attributes = payload.get("attributes")
    attributes_json = (
        json.dumps(
            dict(attributes),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        if isinstance(attributes, dict)
        else ""
    )
    return [
        _field_text(payload.get("source_record_key")),
        _field_text(payload.get("sku")),
        _field_text(payload.get("name")),
        _field_text(payload.get("quantity")),
        _field_text(currency),
        _field_text(amount),
        _field_text(payload.get("updated_at")),
        attributes_json,
    ]


def _field_text(value: WireValue) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    raise FixtureError("csv fields must be scalar wire values")


def _write_payload(
    destination: Path,
    payload: bytes,
    *,
    kind: str,
    dataset: SyntheticDataset,
    malformed_rows: int,
    duplicate_rows: int,
) -> FixtureManifest:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(payload)
    return FixtureManifest(
        kind=kind,
        encoding=FIXTURE_ENCODING,
        newline=FIXTURE_NEWLINE,
        row_count=len(dataset.rows),
        malformed_row_count=malformed_rows,
        duplicate_row_count=duplicate_rows,
        byte_size=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )


def _validate_bounds(
    *, row_count: int, byte_size: int, bounds: FixtureBounds, subject: str
) -> None:
    if row_count > bounds.max_rows:
        raise FixtureError(f"{subject} exceeds the row bound of {bounds.max_rows}")
    if byte_size > bounds.max_bytes:
        raise FixtureError(f"{subject} exceeds the byte bound of {bounds.max_bytes}")


def _read_bounded_payload(source: Path, bounds: FixtureBounds) -> bytes:
    if not source.is_file():
        raise FixtureError(f"fixture file does not exist: {source}")
    byte_size = source.stat().st_size
    if byte_size > bounds.max_bytes:
        raise FixtureError(f"fixture read exceeds the byte bound of {bounds.max_bytes}")
    return source.read_bytes()
