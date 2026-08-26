"""Versioned source-schema normalization for reconciliation inputs.

Rules version 1 maps one closed wire payload (the Phase 8/9 synthetic source
and warehouse shape) onto a canonical inventory record plus a normalized
comparison projection. Observations that violate a rule are quarantined with a
closed code, the exact field path, and bounded detail; they never block valid
work. The rules cover field types, missing and null values, Unicode NFC text
normalization, canonical attribute keys, deterministic ordering by source
position, and canonical serialization through the domain record contract.
"""

import unicodedata
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import ClassVar, cast

from paritygrid.domain.models import (
    ConnectorId,
    CurrencyCode,
    InventoryRecord,
    Money,
    UtcTimestamp,
)
from paritygrid.domain.reconciliation.differences import (
    ComparisonDocument,
    ComparisonValue,
    ComparisonValueKind,
    canonical_attribute_path,
)

NORMALIZATION_RULES_VERSION = 1
MAX_SOURCE_OBSERVATIONS = 100_000
MAX_QUARANTINE_DETAIL_CHARACTERS = 200
MAX_QUARANTINE_FIELD_BYTES = 96
MAX_PAYLOAD_FIELDS = 32

_PROBE_TIMESTAMP = UtcTimestamp(datetime(2024, 1, 2, 3, 4, 5, 6, tzinfo=UTC))
_PROBE_MONEY = Money(Decimal("1.00"), CurrencyCode("USD"), 2)
_EXPECTED_TYPES: dict[str, str] = {
    "name": "canonical text",
    "quantity": "an integer",
    "updated_at": "canonical text",
    "unit_price": "an object",
    "unit_price/currency": "canonical text",
    "unit_price/amount": "canonical text",
    "attributes": "an object",
}


def _is_payload_mapping(value: object) -> bool:
    return isinstance(value, Mapping)


class NormalizationError(ValueError):
    """A normalization batch violates the bounded observation contract."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class QuarantineCode(StrEnum):
    """Why one observation could not become a canonical record."""

    SOURCE_MALFORMED = "source_malformed"
    PAYLOAD_NOT_OBJECT = "payload_not_object"
    MISSING_FIELD = "missing_field"
    NULL_FIELD = "null_field"
    WRONG_TYPE = "wrong_type"
    INVALID_VALUE = "invalid_value"


@dataclass(frozen=True, slots=True)
class SourceObservation:
    """One ordered connector observation before normalization."""

    position: int
    connector_id: ConnectorId
    payload: Mapping[str, object] | None
    malformed_reason: str | None = None

    def __post_init__(self) -> None:
        if type(self.position) is not int or self.position < 0:
            raise NormalizationError("observation position must be a nonnegative integer")
        if type(self.connector_id) is not ConnectorId:
            raise TypeError("observation connector must be a ConnectorId")
        payload: object = self.payload
        if payload is not None and not _is_payload_mapping(payload):
            raise NormalizationError("observation payload must be a mapping or None")
        reason = self.malformed_reason
        if reason is not None:
            if type(reason) is not str or not reason:
                raise NormalizationError("observation malformed reason must be nonempty text")
            if len(reason) > MAX_QUARANTINE_DETAIL_CHARACTERS:
                raise NormalizationError("observation malformed reason exceeds the size limit")
        if payload is None and reason is None:
            raise NormalizationError("absent payloads require a malformed reason")
        if payload is not None and reason is not None:
            raise NormalizationError("present payloads must not carry a malformed reason")


@dataclass(frozen=True, slots=True)
class NormalizedRecord:
    """One valid canonical record with its normalized comparison projection."""

    position: int
    record: InventoryRecord
    document: ComparisonDocument

    def __post_init__(self) -> None:
        if type(self.position) is not int or self.position < 0:
            raise NormalizationError("normalized record position must be a nonnegative integer")
        if type(self.record) is not InventoryRecord:
            raise TypeError("normalized record must hold an InventoryRecord")
        if type(self.document) is not ComparisonDocument:
            raise TypeError("normalized record must hold a ComparisonDocument")


@dataclass(frozen=True, slots=True)
class QuarantinedObservation:
    """Bounded quarantine evidence for one observation."""

    position: int
    connector_id: ConnectorId
    code: QuarantineCode
    field: str
    detail: str
    source_record_key: str = ""

    def __post_init__(self) -> None:
        if type(self.position) is not int or self.position < 0:
            raise NormalizationError("quarantined position must be a nonnegative integer")
        if type(self.connector_id) is not ConnectorId:
            raise TypeError("quarantined connector must be a ConnectorId")
        if type(self.code) is not QuarantineCode:
            raise TypeError("quarantined code must be a QuarantineCode")
        field = self.field
        if (
            type(field) is not str
            or not field
            or len(field.encode("utf-8")) > MAX_QUARANTINE_FIELD_BYTES
        ):
            raise NormalizationError("quarantined field must be bounded nonempty text")
        detail = self.detail
        if type(detail) is not str or not detail or len(detail) > MAX_QUARANTINE_DETAIL_CHARACTERS:
            raise NormalizationError("quarantined detail must be bounded nonempty text")
        key = self.source_record_key
        if type(key) is not str or len(key.encode("utf-8")) > MAX_QUARANTINE_FIELD_BYTES:
            raise NormalizationError("quarantined source record key must be bounded text")


@dataclass(frozen=True, slots=True)
class SourceNormalization:
    """The complete deterministic result of normalizing one observation batch."""

    MAX_OBSERVATIONS: ClassVar[int] = MAX_SOURCE_OBSERVATIONS

    rules_version: int
    records: tuple[NormalizedRecord, ...]
    quarantined: tuple[QuarantinedObservation, ...]

    def __post_init__(self) -> None:
        if type(self.rules_version) is not int or self.rules_version != NORMALIZATION_RULES_VERSION:
            raise NormalizationError("normalization rules version is unsupported")
        if type(self.records) is not tuple or type(self.quarantined) is not tuple:
            raise TypeError("normalization results must be tuples")
        for sequence in (self.records, self.quarantined):
            positions = [item.position for item in sequence]
            if positions != sorted(positions) or len(set(positions)) != len(positions):
                raise NormalizationError("normalization results must use ordered unique positions")
        if len(self.records) + len(self.quarantined) > self.MAX_OBSERVATIONS:
            raise NormalizationError("normalization batch exceeds the observation limit")


def normalize_source_observations(
    observations: Iterable[SourceObservation],
) -> SourceNormalization:
    """Normalize one observation batch, preserving input position order."""
    records: list[NormalizedRecord] = []
    quarantined: list[QuarantinedObservation] = []
    seen: set[int] = set()
    for observation in observations:
        if type(observation) is not SourceObservation:
            raise TypeError("normalization input must contain SourceObservation values")
        if observation.position in seen:
            raise NormalizationError("normalization positions must be unique")
        seen.add(observation.position)
        if len(seen) > MAX_SOURCE_OBSERVATIONS:
            raise NormalizationError("normalization batch exceeds the observation limit")
        result = normalize_observation(observation)
        if isinstance(result, NormalizedRecord):
            records.append(result)
        else:
            quarantined.append(result)
    return SourceNormalization(
        rules_version=NORMALIZATION_RULES_VERSION,
        records=tuple(sorted(records, key=lambda item: item.position)),
        quarantined=tuple(sorted(quarantined, key=lambda item: item.position)),
    )


def normalize_observation(
    observation: SourceObservation,
) -> NormalizedRecord | QuarantinedObservation:
    """Apply rules version 1 to one observation."""
    if observation.payload is None:
        return _quarantine(
            observation,
            QuarantineCode.SOURCE_MALFORMED,
            "payload",
            observation.malformed_reason
            if observation.malformed_reason is not None
            else "the connector rejected the record",
        )
    if not _is_payload_mapping(observation.payload):
        return _quarantine(
            observation, QuarantineCode.PAYLOAD_NOT_OBJECT, "payload", "payload is not an object"
        )
    payload = observation.payload
    if len(payload) > MAX_PAYLOAD_FIELDS:
        return _quarantine(
            observation, QuarantineCode.WRONG_TYPE, "payload", "payload exceeds the field limit"
        )

    projection, invalid_attribute_key = _project_payload(payload)
    if invalid_attribute_key is not None:
        return _quarantine(
            observation,
            QuarantineCode.INVALID_VALUE,
            "attributes",
            f"attribute keys must be canonical lowercase ASCII: {invalid_attribute_key}",
        )
    for quarantine in (
        _provenance_quarantine(observation, payload),
        _marker_quarantine(observation, projection),
        _required_quarantine(observation, projection, payload),
    ):
        if quarantine is not None:
            return quarantine

    currency = cast(str, projection["unit_price/currency"].text)
    amount = projection["unit_price/amount"]
    if amount.kind is ComparisonValueKind.TEXT:
        currency_value, amount_value = _parse_money(currency, cast(str, amount.text))
        if currency_value is None:
            return _quarantine(
                observation,
                QuarantineCode.INVALID_VALUE,
                "unit_price/currency",
                f"unit_price/currency is not a canonical currency: {currency}",
            )
        if amount_value is None:
            return _quarantine(
                observation,
                QuarantineCode.INVALID_VALUE,
                "unit_price/amount",
                f"unit_price/amount is not a canonical amount: {cast(str, amount.text)}",
            )
        projection["unit_price/amount"] = ComparisonValue.money_amount_value(amount_value)
    timestamp_leaf = projection["updated_at"]
    if timestamp_leaf.kind is ComparisonValueKind.TEXT:
        try:
            parsed = UtcTimestamp.parse(cast(str, timestamp_leaf.text))
        except TypeError, ValueError:
            return _quarantine(
                observation,
                QuarantineCode.INVALID_VALUE,
                "updated_at",
                f"updated_at is not a canonical timestamp: {cast(str, timestamp_leaf.text)}",
            )
        projection["updated_at"] = ComparisonValue.timestamp_value(parsed)

    attributes = {
        path.partition("/")[2]: cast(str, value.text)
        for path, value in projection.items()
        if path.startswith("attributes/") and value.kind is ComparisonValueKind.ATTRIBUTE_TEXT
    }
    try:
        record = InventoryRecord.create(
            sku=cast(str, payload["sku"]),
            name=cast(str, projection["name"].text),
            quantity=cast(int, projection["quantity"].integer),
            unit_price=cast(Money, projection["unit_price/amount"].money),
            updated_at=cast(UtcTimestamp, projection["updated_at"].timestamp),
            connector_id=observation.connector_id,
            source_record_key=cast(str, payload["source_record_key"]),
            attributes=attributes,
        )
    except (TypeError, ValueError) as error:
        return _quarantine(
            observation,
            QuarantineCode.INVALID_VALUE,
            _probe_invalid_field(projection, observation.connector_id, payload),
            _bounded_detail(error),
        )
    document = ComparisonDocument(values=tuple(sorted(projection.items())))
    return NormalizedRecord(position=observation.position, record=record, document=document)


def _project_payload(
    payload: Mapping[str, object],
) -> tuple[dict[str, ComparisonValue], str | None]:
    """Project one payload onto comparison leaves without discarding markers."""
    projection: dict[str, ComparisonValue] = {}
    for name in ("name", "quantity", "updated_at"):
        _project_leaf(payload, name, projection)
    _project_unit_price(payload, projection)
    invalid_attribute_key = _project_attributes(payload, projection)
    return projection, invalid_attribute_key


def _project_leaf(
    payload: Mapping[str, object],
    name: str,
    projection: dict[str, ComparisonValue],
) -> None:
    if name not in payload:
        return
    value = payload[name]
    if value is None:
        projection[name] = ComparisonValue.null()
        return
    if name == "quantity":
        if type(value) is int:
            projection[name] = ComparisonValue.integer_value(value)
        else:
            projection[name] = ComparisonValue.wrong_type(value)
        return
    if not isinstance(value, str):
        projection[name] = ComparisonValue.wrong_type(value)
        return
    try:
        projection[name] = ComparisonValue.text_value(value)
    except TypeError, ValueError:
        projection[name] = ComparisonValue.wrong_type(value)


def _project_unit_price(
    payload: Mapping[str, object],
    projection: dict[str, ComparisonValue],
) -> None:
    if "unit_price" not in payload:
        return
    value = payload["unit_price"]
    if value is None:
        projection["unit_price"] = ComparisonValue.null()
        return
    if not isinstance(value, Mapping):
        projection["unit_price"] = ComparisonValue.wrong_type(value)
        return
    price = cast("Mapping[str, object]", value)
    for child in ("currency", "amount"):
        if child not in price:
            continue
        child_value = price[child]
        path = f"unit_price/{child}"
        if child_value is None:
            projection[path] = ComparisonValue.null()
        elif isinstance(child_value, str):
            try:
                projection[path] = ComparisonValue.text_value(child_value)
            except TypeError, ValueError:
                projection[path] = ComparisonValue.wrong_type(child_value)
        else:
            projection[path] = ComparisonValue.wrong_type(child_value)


def _project_attributes(
    payload: Mapping[str, object],
    projection: dict[str, ComparisonValue],
) -> str | None:
    """Project nested attributes and report one non-canonical key, if any."""
    if "attributes" not in payload:
        return None
    value = payload["attributes"]
    if value is None:
        projection["attributes"] = ComparisonValue.null()
        return None
    if not isinstance(value, Mapping):
        projection["attributes"] = ComparisonValue.wrong_type(value)
        return None
    attributes = cast("Mapping[str, object]", value)
    invalid_key: str | None = None
    for key in sorted(attributes):
        item = attributes[key]
        path = canonical_attribute_path(key)
        if path is None:
            if invalid_key is None or key < invalid_key:
                invalid_key = key
            continue
        if item is None:
            projection[path] = ComparisonValue.null()
        elif isinstance(item, str):
            try:
                projection[path] = ComparisonValue.attribute_text_value(item)
            except TypeError, ValueError:
                projection[path] = ComparisonValue.wrong_type(item)
        else:
            projection[path] = ComparisonValue.wrong_type(item)
    return invalid_key


def _provenance_quarantine(
    observation: SourceObservation,
    payload: Mapping[str, object],
) -> QuarantinedObservation | None:
    for name in ("sku", "source_record_key"):
        value = payload.get(name)
        if value is None:
            code = QuarantineCode.NULL_FIELD if name in payload else QuarantineCode.MISSING_FIELD
            state = "null" if name in payload else "absent"
            return _quarantine(observation, code, name, f"{name} is {state}")
        if not isinstance(value, str):
            return _quarantine(observation, QuarantineCode.WRONG_TYPE, name, f"{name} must be text")
    return None


def _marker_quarantine(
    observation: SourceObservation,
    projection: dict[str, ComparisonValue],
) -> QuarantinedObservation | None:
    """Quarantine marker leaves that cannot participate in a canonical record."""
    for path in sorted(projection):
        value = projection[path]
        if value.kind in {
            ComparisonValueKind.MISSING,
            ComparisonValueKind.TEXT,
            ComparisonValueKind.INTEGER,
            ComparisonValueKind.TIMESTAMP,
            ComparisonValueKind.MONEY_AMOUNT,
            ComparisonValueKind.ATTRIBUTE_TEXT,
        }:
            continue
        if value.kind is ComparisonValueKind.NULL:
            return _quarantine(observation, QuarantineCode.NULL_FIELD, path, f"{path} is null")
        expected = _EXPECTED_TYPES.get(path)
        if path.startswith("attributes/"):
            expected = "canonical text"
        detail = (
            f"{path} must be {expected}"
            if expected is not None
            else f"{path} must be a supported canonical type"
        )
        return _quarantine(observation, QuarantineCode.WRONG_TYPE, path, detail)
    return None


def _required_quarantine(
    observation: SourceObservation,
    projection: dict[str, ComparisonValue],
    payload: Mapping[str, object],
) -> QuarantinedObservation | None:
    for path in ("name", "quantity", "updated_at"):
        if path not in projection:
            return _quarantine(observation, QuarantineCode.MISSING_FIELD, path, f"{path} is absent")
    if "unit_price" not in projection and "unit_price/currency" not in projection:
        return _quarantine(
            observation, QuarantineCode.MISSING_FIELD, "unit_price", "unit_price is absent"
        )
    for path in ("unit_price/currency", "unit_price/amount"):
        if path not in projection:
            return _quarantine(observation, QuarantineCode.MISSING_FIELD, path, f"{path} is absent")
    return None


def _parse_money(currency: str, amount: str) -> tuple[Money | None, Money | None]:
    """Parse a money leaf, reporting which half failed first."""
    currency_money: Money | None
    amount_money: Money | None
    try:
        currency_money = Money.parse(f"{currency} 1.00")
    except TypeError, ValueError:
        currency_money = None
    try:
        amount_money = Money.parse(f"USD {amount}")
    except TypeError, ValueError:
        amount_money = None
    if currency_money is None or amount_money is None:
        return currency_money, amount_money
    try:
        combined = Money.parse(f"{currency} {amount}")
    except TypeError, ValueError:
        return currency_money, None
    return currency_money, combined


def _probe_invalid_field(
    projection: dict[str, ComparisonValue],
    connector_id: ConnectorId,
    payload: Mapping[str, object],
) -> str:
    """Attribute a domain-contract failure to one field with canonical probes."""
    sku = cast(str, payload["sku"])
    source_record_key = cast(str, payload["source_record_key"])
    candidates: tuple[tuple[str, object], ...] = (
        ("sku", sku),
        ("name", cast(str, projection["name"].text)),
        ("quantity", cast(int, projection["quantity"].integer)),
        ("source_record_key", source_record_key),
    )
    for field, _value in candidates:
        fields: dict[str, object] = {
            "sku": "PROBE-1",
            "name": "Probe value",
            "quantity": 1,
            "source_record_key": "probe-key",
        }
        fields[field] = _value
        try:
            _probe_record(connector_id, fields)
        except TypeError, ValueError:
            return field
    attributes = {
        path.partition("/")[2]: cast(str, value.text)
        for path, value in sorted(projection.items())
        if path.startswith("attributes/")
    }
    try:
        _probe_record(
            connector_id,
            {
                "sku": "PROBE-1",
                "name": "Probe value",
                "quantity": 1,
                "source_record_key": "probe-key",
                "attributes": attributes,
            },
        )
    except TypeError, ValueError:
        return "attributes"
    return "record"


def _probe_record(connector_id: ConnectorId, overrides: dict[str, object]) -> None:
    merged: dict[str, object] = {
        "sku": "PROBE-1",
        "name": "Probe value",
        "quantity": 1,
        "source_record_key": "probe-key",
        "attributes": {},
    }
    merged.update(overrides)
    InventoryRecord.create(
        sku=cast(str, merged["sku"]),
        name=cast(str, merged["name"]),
        quantity=cast(int, merged["quantity"]),
        unit_price=_PROBE_MONEY,
        updated_at=_PROBE_TIMESTAMP,
        connector_id=connector_id,
        source_record_key=cast(str, merged["source_record_key"]),
        attributes=cast("dict[str, str]", merged["attributes"]),
    )


def _quarantine(
    observation: SourceObservation,
    code: QuarantineCode,
    field: str,
    detail: str,
) -> QuarantinedObservation:
    return QuarantinedObservation(
        position=observation.position,
        connector_id=observation.connector_id,
        code=code,
        field=field,
        detail=_bounded_detail_str(detail),
        source_record_key=_quarantine_key(observation),
    )


def _quarantine_key(observation: SourceObservation) -> str:
    payload = observation.payload
    if not isinstance(payload, Mapping):
        return ""
    key = payload.get("source_record_key")
    if type(key) is not str:
        return ""
    normalized = unicodedata.normalize("NFC", key)
    if len(normalized.encode("utf-8")) > MAX_QUARANTINE_FIELD_BYTES:
        return ""
    return normalized


def _bounded_detail(error: Exception) -> str:
    return _bounded_detail_str(str(error))


def _bounded_detail_str(detail: str) -> str:
    normalized = unicodedata.normalize("NFC", detail).strip()
    if not normalized:
        return "normalization failed"
    return normalized[:MAX_QUARANTINE_DETAIL_CHARACTERS]
