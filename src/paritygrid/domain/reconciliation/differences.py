"""Normalized comparison values and stable field-level differences.

Normalization (version 1) projects every source or target observation onto a
closed set of comparison field paths. Each path carries exactly one leaf that
is either a typed canonical value, an explicit absence marker, a JSON null
marker, or a bounded wrong-type marker. Differences are computed pairwise over
the union of paths, so nested attribute leaves, missing fields, null values,
type mismatches, and Unicode-normalized text all produce one stable closed
result.
"""

import json
import re
import unicodedata
from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar, cast

from paritygrid.domain.models import Money, UtcTimestamp

_MAX_FIELD_PATH_BYTES = 96
_MAX_LEAF_TEXT_CHARACTERS = 512
_MAX_LEAF_TEXT_BYTES = 1_024
_MAX_DIFFERENCES = 128
# Mirrors the domain attribute-key contract so canonical keys always project.
_PATH_SEGMENT = re.compile(r"[a-z][a-z0-9]*(?:[-_][a-z0-9]+)*\Z", flags=re.ASCII)
_TOP_LEVEL_PATHS = frozenset({"name", "quantity", "updated_at"})


class ComparisonValueKind(StrEnum):
    """Closed leaf kinds on one normalized comparison field path."""

    TEXT = "text"
    INTEGER = "integer"
    MONEY_AMOUNT = "money_amount"
    TIMESTAMP = "timestamp"
    ATTRIBUTE_TEXT = "attribute_text"
    MISSING = "missing"
    NULL = "null"
    WRONG_TYPE = "wrong_type"


class FieldDifferenceKind(StrEnum):
    """Why one field path differs between the source and target projections."""

    VALUE_MISMATCH = "value_mismatch"
    MISSING_ON_SOURCE = "missing_on_source"
    MISSING_ON_TARGET = "missing_on_target"
    NULL_ON_SOURCE = "null_on_source"
    NULL_ON_TARGET = "null_on_target"
    TYPE_MISMATCH = "type_mismatch"


class ComparisonDocumentError(ValueError):
    """A comparison projection violates the closed field-path contract."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class ComparisonValue:
    """One typed or marker leaf on a normalized comparison field path."""

    kind: ComparisonValueKind
    text: str | None = None
    integer: int | None = None
    money: Money | None = None
    timestamp: UtcTimestamp | None = None

    def __post_init__(self) -> None:
        kind = _require_kind(self.kind)
        object.__setattr__(self, "kind", kind)
        _require_leaf(self, kind)

    @classmethod
    def missing(cls) -> ComparisonValue:
        """The field path is absent from the payload document."""
        return cls(kind=ComparisonValueKind.MISSING)

    @classmethod
    def null(cls) -> ComparisonValue:
        """The payload carried an explicit JSON null."""
        return cls(kind=ComparisonValueKind.NULL)

    @classmethod
    def wrong_type(cls, raw: object) -> ComparisonValue:
        """Preserve bounded evidence for a value with an unsupported type."""
        return cls(kind=ComparisonValueKind.WRONG_TYPE, text=_bounded_raw(raw))

    @classmethod
    def text_value(cls, value: object) -> ComparisonValue:
        """Normalize free text onto a comparable canonical leaf."""
        return cls(kind=ComparisonValueKind.TEXT, text=_normalized_leaf_text(value))

    @classmethod
    def attribute_text_value(cls, value: object) -> ComparisonValue:
        """Normalize one nested attribute value onto a comparable leaf."""
        return cls(kind=ComparisonValueKind.ATTRIBUTE_TEXT, text=_normalized_leaf_text(value))

    @classmethod
    def integer_value(cls, value: int) -> ComparisonValue:
        """Wrap one integer leaf in the comparison integer range."""
        return cls(kind=ComparisonValueKind.INTEGER, integer=value)

    @classmethod
    def money_amount_value(cls, value: Money) -> ComparisonValue:
        """Wrap one canonical money leaf."""
        return cls(kind=ComparisonValueKind.MONEY_AMOUNT, money=value)

    @classmethod
    def timestamp_value(cls, value: UtcTimestamp) -> ComparisonValue:
        """Wrap one canonical UTC timestamp leaf."""
        return cls(kind=ComparisonValueKind.TIMESTAMP, timestamp=value)

    def canonical_text(self) -> str:
        """Return the bounded deterministic rendering used by differences."""
        if self.kind is ComparisonValueKind.MISSING:
            return ""
        if self.kind is ComparisonValueKind.NULL:
            return "null"
        if self.kind is ComparisonValueKind.INTEGER:
            return str(self.integer)
        if self.kind is ComparisonValueKind.MONEY_AMOUNT:
            return str(self.money)
        if self.kind is ComparisonValueKind.TIMESTAMP:
            return str(self.timestamp)
        return self.text if self.text is not None else ""

    def is_equivalent(self, other: object) -> bool:
        """Report whether two leaves hold the same canonical value."""
        if type(other) is not ComparisonValue:
            raise TypeError("comparison requires ComparisonValue leaves")
        exact = other
        if self.kind is not exact.kind:
            return False
        if self.kind is ComparisonValueKind.INTEGER:
            return self.integer == exact.integer
        if self.kind is ComparisonValueKind.MONEY_AMOUNT:
            return self.money == exact.money
        if self.kind is ComparisonValueKind.TIMESTAMP:
            return self.timestamp == exact.timestamp
        if self.kind in {ComparisonValueKind.MISSING, ComparisonValueKind.NULL}:
            return True
        return self.text == exact.text


@dataclass(frozen=True, slots=True)
class ComparisonDocument:
    """A closed, deterministically ordered set of normalized field leaves."""

    MAX_PATHS: ClassVar[int] = 128

    values: tuple[tuple[str, ComparisonValue], ...]

    def __post_init__(self) -> None:
        pairs = self.values
        if type(pairs) is not tuple:
            raise TypeError("comparison document values must be a tuple")
        if len(pairs) > self.MAX_PATHS:
            raise ComparisonDocumentError("comparison document exceeds the path limit")
        for pair in cast(tuple[object, ...], pairs):
            if type(pair) is not tuple or len(cast(tuple[object, ...], pair)) != 2:
                raise TypeError("comparison document entries must be path-value tuples")
            path, value = cast(tuple[object, object], pair)
            _require_path(path)
            if type(value) is not ComparisonValue:
                raise TypeError("comparison document entries must hold ComparisonValue leaves")
        paths = tuple(
            cast(str, cast(tuple[object, object], pair)[0])
            for pair in cast(tuple[object, ...], pairs)
        )
        if len(set(paths)) != len(paths):
            raise ComparisonDocumentError("comparison document paths must be unique")
        if paths != tuple(sorted(paths)):
            raise ComparisonDocumentError("comparison document paths must be sorted")

    @classmethod
    def empty(cls) -> ComparisonDocument:
        """The projection used when a side has no comparable payload."""
        return cls(values=())

    def get(self, path: str) -> ComparisonValue:
        """Return the leaf at one path, defaulting to an absence marker."""
        for known_path, value in self.values:
            if known_path == path:
                return value
        return ComparisonValue.missing()

    def paths(self) -> tuple[str, ...]:
        """Return the sorted canonical paths present in this projection."""
        return tuple(path for path, _value in self.values)


@dataclass(frozen=True, slots=True)
class FieldDifference:
    """Stable evidence for one field path that differs between two sides."""

    field: str
    kind: FieldDifferenceKind
    source_text: str
    target_text: str

    def __post_init__(self) -> None:
        _require_path(self.field)
        if type(self.kind) is not FieldDifferenceKind:
            raise TypeError("field difference kind must be a FieldDifferenceKind")
        object.__setattr__(
            self, "source_text", _bounded_difference_text(self.source_text, "source")
        )
        object.__setattr__(
            self, "target_text", _bounded_difference_text(self.target_text, "target")
        )


def build_field_differences(
    source: object,
    target: object,
) -> tuple[FieldDifference, ...]:
    """Compare two normalized projections path by path in canonical order.

    Text leaves are NFC-normalized before comparison, so strings that differ
    only before Unicode normalization are equivalent and produce no difference.
    """
    source_document = _require_document(source, "source")
    target_document = _require_document(target, "target")
    differences: list[FieldDifference] = []
    for path in sorted(set(source_document.paths()) | set(target_document.paths())):
        difference = _difference_at(
            path,
            source_document.get(path),
            target_document.get(path),
        )
        if difference is not None:
            differences.append(difference)
            if len(differences) > _MAX_DIFFERENCES:
                raise ComparisonDocumentError("field comparison exceeds the difference limit")
    return tuple(differences)


def _difference_at(
    path: str,
    source: ComparisonValue,
    target: ComparisonValue,
) -> FieldDifference | None:
    if source.is_equivalent(target):
        return None
    return FieldDifference(
        field=path,
        kind=_difference_kind(source, target),
        source_text=source.canonical_text(),
        target_text=target.canonical_text(),
    )


def _difference_kind(source: ComparisonValue, target: ComparisonValue) -> FieldDifferenceKind:
    if source.kind is ComparisonValueKind.MISSING:
        return FieldDifferenceKind.MISSING_ON_SOURCE
    if target.kind is ComparisonValueKind.MISSING:
        return FieldDifferenceKind.MISSING_ON_TARGET
    if source.kind is ComparisonValueKind.NULL:
        return FieldDifferenceKind.NULL_ON_SOURCE
    if target.kind is ComparisonValueKind.NULL:
        return FieldDifferenceKind.NULL_ON_TARGET
    if source.kind is target.kind:
        return FieldDifferenceKind.VALUE_MISMATCH
    return FieldDifferenceKind.TYPE_MISMATCH


def _require_kind(value: object) -> ComparisonValueKind:
    if type(value) is not ComparisonValueKind:
        raise TypeError("comparison value kind must be a ComparisonValueKind")
    return value


def _require_leaf(value: ComparisonValue, kind: ComparisonValueKind) -> None:
    present: tuple[str, ...]
    if kind in {ComparisonValueKind.TEXT, ComparisonValueKind.ATTRIBUTE_TEXT}:
        present = ("text",)
    elif kind is ComparisonValueKind.INTEGER:
        present = ("integer",)
    elif kind is ComparisonValueKind.MONEY_AMOUNT:
        present = ("money",)
    elif kind is ComparisonValueKind.TIMESTAMP:
        present = ("timestamp",)
    elif kind is ComparisonValueKind.WRONG_TYPE:
        present = ("text",)
    else:
        present = ()
    for name in ("text", "integer", "money", "timestamp"):
        has_value = getattr(value, name) is not None
        if name in present:
            if not has_value:
                raise ValueError(f"comparison {kind.value} leaf requires {name}")
        elif has_value:
            raise ValueError(f"comparison {kind.value} leaf must not carry {name}")
    if kind is ComparisonValueKind.INTEGER:
        integer = value.integer
        if (
            type(integer) is not int
            or not -9_223_372_036_854_775_808 <= integer <= 9_223_372_036_854_775_807
        ):
            raise ValueError("comparison integer leaf is outside the supported range")
    if kind is ComparisonValueKind.WRONG_TYPE and not value.text:
        raise ValueError("comparison wrong-type leaf requires bounded evidence text")


def _require_path(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("comparison field path must be text")
    if not value or len(value.encode("utf-8")) > _MAX_FIELD_PATH_BYTES:
        raise ComparisonDocumentError("comparison field path is empty or exceeds the byte limit")
    segments = value.split("/")
    if segments[0] in _TOP_LEVEL_PATHS or segments[0] in {"unit_price", "attributes"}:
        for segment in segments[1:]:
            if _PATH_SEGMENT.fullmatch(segment) is None:
                raise ComparisonDocumentError("comparison field path is not canonical")
        return value
    raise ComparisonDocumentError("comparison field path is not canonical")


def canonical_attribute_path(key: object) -> str | None:
    """Return the nested comparison path for one canonical attribute key."""
    if not isinstance(key, str) or _PATH_SEGMENT.fullmatch(key) is None:
        return None
    return f"attributes/{key}"


def _normalized_leaf_text(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("comparison text leaf must be text")
    normalized = unicodedata.normalize("NFC", value)
    encoded = normalized.encode("utf-8")
    if len(normalized) > _MAX_LEAF_TEXT_CHARACTERS or len(encoded) > _MAX_LEAF_TEXT_BYTES:
        raise ValueError("comparison text leaf exceeds its size limit")
    return normalized


def _bounded_raw(raw: object) -> str:
    try:
        rendered = json.dumps(raw, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    except TypeError, ValueError:
        rendered = type(raw).__name__
    if len(rendered) > _MAX_LEAF_TEXT_CHARACTERS:
        rendered = rendered[: _MAX_LEAF_TEXT_CHARACTERS - 3] + "..."
    return rendered


def _bounded_difference_text(value: object, side: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"field difference {side} text must be text")
    if len(value) > _MAX_LEAF_TEXT_CHARACTERS:
        raise ValueError(f"field difference {side} text exceeds its size limit")
    return value


def _require_document(value: object, side: str) -> ComparisonDocument:
    if type(value) is not ComparisonDocument:
        raise TypeError(f"{side} comparison input must be a ComparisonDocument")
    return value
