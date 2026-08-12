"""Complete reconciliation classifications for canonical inventory records."""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import ClassVar, cast

from paritygrid.domain.models import InventoryAttributes, InventoryRecord, Money, UtcTimestamp

type ComparableInventoryValue = str | int | Money | UtcTimestamp | InventoryAttributes
type _InventoryValueType = (
    type[str] | type[int] | type[Money] | type[UtcTimestamp] | type[InventoryAttributes]
)


class ReconciliationClassification(StrEnum):
    """The one primary result assigned to a canonical inventory key."""

    MATCH = "match"
    MISSING_FROM_TARGET = "missing_from_target"
    MISSING_FROM_SOURCE = "missing_from_source"
    FIELD_MISMATCH = "field_mismatch"
    DUPLICATE_SOURCE = "duplicate_source"
    DUPLICATE_TARGET = "duplicate_target"
    DUPLICATE_BOTH = "duplicate_both"


class ReconciliationField(StrEnum):
    """Business fields that determine inventory parity."""

    NAME = "name"
    QUANTITY = "quantity"
    UNIT_PRICE = "unit_price"
    UPDATED_AT = "updated_at"
    ATTRIBUTES = "attributes"


_FIELD_TYPES: dict[ReconciliationField, _InventoryValueType] = {
    ReconciliationField.NAME: str,
    ReconciliationField.QUANTITY: int,
    ReconciliationField.UNIT_PRICE: Money,
    ReconciliationField.UPDATED_AT: UtcTimestamp,
    ReconciliationField.ATTRIBUTES: InventoryAttributes,
}


@dataclass(frozen=True, slots=True)
class FieldMismatch:
    """The exact source and target values that differ for one business field."""

    field: ReconciliationField
    source_value: ComparableInventoryValue
    target_value: ComparableInventoryValue

    def __post_init__(self) -> None:
        field_value = cast(object, self.field)
        if not isinstance(field_value, ReconciliationField):
            raise TypeError("mismatch field must be a ReconciliationField")
        expected_type = _FIELD_TYPES[field_value]
        if not _is_exact_value_type(self.source_value, expected_type):
            raise TypeError(f"source value for {self.field.value} has the wrong type")
        if not _is_exact_value_type(self.target_value, expected_type):
            raise TypeError(f"target value for {self.field.value} has the wrong type")
        if self.source_value == self.target_value:
            raise ValueError("mismatch values must differ")


@dataclass(frozen=True, slots=True)
class ReconciliationOutcome:
    """An immutable, exclusive outcome for one canonical inventory key."""

    MAX_RECORDS_PER_SIDE: ClassVar[int] = 1_024

    source_records: tuple[InventoryRecord, ...] = ()
    target_records: tuple[InventoryRecord, ...] = ()
    sku: str = field(init=False)
    classification: ReconciliationClassification = field(init=False)
    mismatches: tuple[FieldMismatch, ...] = field(init=False)

    def __post_init__(self) -> None:
        sources = _canonical_records(self.source_records, side="source")
        targets = _canonical_records(self.target_records, side="target")
        records = sources + targets
        if not records:
            raise ValueError("reconciliation outcome requires at least one record")
        sku = records[0].sku
        if any(record.sku != sku for record in records):
            raise ValueError("reconciliation outcome records must share one SKU")

        classification, mismatches = _classify_records(sources, targets)
        object.__setattr__(self, "source_records", sources)
        object.__setattr__(self, "target_records", targets)
        object.__setattr__(self, "sku", sku)
        object.__setattr__(self, "classification", classification)
        object.__setattr__(self, "mismatches", mismatches)

    @property
    def is_repairable(self) -> bool:
        """Report whether a safe create or update action can restore parity."""
        return self.classification in {
            ReconciliationClassification.MISSING_FROM_TARGET,
            ReconciliationClassification.FIELD_MISMATCH,
        }


def differences_between(source: object, target: object) -> tuple[FieldMismatch, ...]:
    """Return deterministic field-level evidence for records with one SKU."""
    if type(source) is not InventoryRecord or type(target) is not InventoryRecord:
        raise TypeError("reconciliation comparison requires InventoryRecord values")
    if source.sku != target.sku:
        raise ValueError("reconciliation comparison requires matching SKUs")

    values: tuple[
        tuple[ReconciliationField, ComparableInventoryValue, ComparableInventoryValue], ...
    ] = (
        (ReconciliationField.NAME, source.name, target.name),
        (ReconciliationField.QUANTITY, source.quantity, target.quantity),
        (ReconciliationField.UNIT_PRICE, source.unit_price, target.unit_price),
        (ReconciliationField.UPDATED_AT, source.updated_at, target.updated_at),
        (ReconciliationField.ATTRIBUTES, source.attributes, target.attributes),
    )
    return tuple(
        FieldMismatch(field_name, source_value, target_value)
        for field_name, source_value, target_value in values
        if source_value != target_value
    )


def _canonical_records(value: object, *, side: str) -> tuple[InventoryRecord, ...]:
    if not isinstance(value, tuple):
        raise TypeError(f"{side} records must be a tuple")
    records = cast(tuple[object, ...], value)
    if len(records) > ReconciliationOutcome.MAX_RECORDS_PER_SIDE:
        raise ValueError(
            f"{side} records must contain at most "
            f"{ReconciliationOutcome.MAX_RECORDS_PER_SIDE} values"
        )
    if any(type(record) is not InventoryRecord for record in records):
        raise TypeError(f"{side} records must contain only InventoryRecord values")
    trusted = cast(tuple[InventoryRecord, ...], records)
    return tuple(sorted(trusted, key=_record_order_key))


def _record_order_key(record: InventoryRecord) -> tuple[object, ...]:
    return (
        record.sku,
        record.name,
        record.quantity,
        str(record.unit_price),
        str(record.updated_at),
        str(record.connector_id),
        record.source_record_key,
        record.attributes.items,
    )


def _classify_records(
    sources: tuple[InventoryRecord, ...], targets: tuple[InventoryRecord, ...]
) -> tuple[ReconciliationClassification, tuple[FieldMismatch, ...]]:
    if len(sources) > 1 and len(targets) > 1:
        return ReconciliationClassification.DUPLICATE_BOTH, ()
    if len(sources) > 1:
        return ReconciliationClassification.DUPLICATE_SOURCE, ()
    if len(targets) > 1:
        return ReconciliationClassification.DUPLICATE_TARGET, ()
    if not sources:
        return ReconciliationClassification.MISSING_FROM_SOURCE, ()
    if not targets:
        return ReconciliationClassification.MISSING_FROM_TARGET, ()

    mismatches = differences_between(sources[0], targets[0])
    classification = (
        ReconciliationClassification.FIELD_MISMATCH
        if mismatches
        else ReconciliationClassification.MATCH
    )
    return classification, mismatches


def _is_exact_value_type(value: object, expected_type: _InventoryValueType) -> bool:
    return type(value) is expected_type
