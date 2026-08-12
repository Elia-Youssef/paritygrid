"""Canonical inventory records used by reconciliation."""

import re
import unicodedata
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import ClassVar, Self, cast

from paritygrid.domain.models.identifiers import ConnectorId
from paritygrid.domain.models.money import Money
from paritygrid.domain.models.temporal import UtcTimestamp

_SKU_PATTERN = re.compile(r"[A-Z0-9]+(?:-[A-Z0-9]+)*", flags=re.ASCII)
_ATTRIBUTE_KEY_PATTERN = re.compile(r"[a-z][a-z0-9]*(?:[-_][a-z0-9]+)*", flags=re.ASCII)


@dataclass(frozen=True, slots=True)
class InventoryAttributes:
    """A sorted, immutable collection of canonical record attributes."""

    MAX_ITEMS: ClassVar[int] = 32
    MAX_TOTAL_UTF8_BYTES: ClassVar[int] = 4_096

    items: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", _canonical_attributes(self.items))

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> Self:
        """Copy a mapping into deterministic key order."""
        return cls(items=_mapping_items(values))

    def get(self, key: str, default: str | None = None) -> str | None:
        """Look up an attribute without exposing mutable state."""
        for item_key, item_value in self.items:
            if item_key == key:
                return item_value
        return default

    def __iter__(self) -> Iterator[tuple[str, str]]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)


@dataclass(frozen=True, slots=True)
class InventoryRecord:
    """A normalized source observation ready for deterministic reconciliation."""

    sku: str
    name: str
    quantity: int
    unit_price: Money
    updated_at: UtcTimestamp
    connector_id: ConnectorId
    source_record_key: str
    attributes: InventoryAttributes = field(default_factory=InventoryAttributes)

    def __post_init__(self) -> None:
        object.__setattr__(self, "sku", _validate_sku(self.sku))
        object.__setattr__(
            self,
            "name",
            _normalize_business_text(
                self.name, field="inventory name", max_characters=160, max_utf8_bytes=320
            ),
        )
        object.__setattr__(self, "quantity", _validate_quantity(self.quantity))
        object.__setattr__(self, "unit_price", _validate_unit_price(self.unit_price))
        object.__setattr__(self, "updated_at", _validate_timestamp(self.updated_at))
        object.__setattr__(self, "connector_id", _validate_connector(self.connector_id))
        object.__setattr__(
            self,
            "source_record_key",
            _normalize_business_text(
                self.source_record_key,
                field="source record key",
                max_characters=128,
                max_utf8_bytes=256,
            ),
        )
        object.__setattr__(self, "attributes", _validate_attributes(self.attributes))

    @classmethod
    def create(
        cls,
        *,
        sku: str,
        name: str,
        quantity: int,
        unit_price: Money,
        updated_at: UtcTimestamp,
        connector_id: ConnectorId,
        source_record_key: str,
        attributes: Mapping[str, str] | None = None,
    ) -> Self:
        """Create a record while defensively copying source attributes."""
        immutable_attributes = (
            InventoryAttributes()
            if attributes is None
            else InventoryAttributes.from_mapping(attributes)
        )
        return cls(
            sku=sku,
            name=name,
            quantity=quantity,
            unit_price=unit_price,
            updated_at=updated_at,
            connector_id=connector_id,
            source_record_key=source_record_key,
            attributes=immutable_attributes,
        )


def _validate_sku(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("SKU must be text")
    if not 1 <= len(value) <= 64:
        raise ValueError("SKU must contain between 1 and 64 characters")
    if _SKU_PATTERN.fullmatch(value) is None:
        raise ValueError("SKU must use canonical uppercase ASCII letters, digits, and hyphens")
    return value


def _validate_quantity(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("inventory quantity must be an integer")
    if not 0 <= value <= 2_147_483_647:
        raise ValueError("inventory quantity is outside the supported range")
    return value


def _validate_unit_price(value: object) -> Money:
    if type(value) is not Money:
        raise TypeError("inventory unit price must be Money")
    if value.amount < 0:
        raise ValueError("inventory unit price must not be negative")
    return value


def _validate_timestamp(value: object) -> UtcTimestamp:
    if type(value) is not UtcTimestamp:
        raise TypeError("inventory timestamp must be UtcTimestamp")
    return value


def _validate_connector(value: object) -> ConnectorId:
    if type(value) is not ConnectorId:
        raise TypeError("inventory connector must be ConnectorId")
    return value


def _validate_attributes(value: object) -> InventoryAttributes:
    if type(value) is not InventoryAttributes:
        raise TypeError("inventory record attributes must be InventoryAttributes")
    return value


def _canonical_attributes(value: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, tuple):
        raise TypeError("inventory attribute items must be a tuple")
    items = cast(tuple[object, ...], value)
    if len(items) > InventoryAttributes.MAX_ITEMS:
        raise ValueError("inventory record has too many attributes")

    canonical: list[tuple[str, str]] = []
    seen: set[str] = set()
    total_bytes = 0
    for item in items:
        key, raw_value = _attribute_pair(item)
        if not isinstance(key, str) or _ATTRIBUTE_KEY_PATTERN.fullmatch(key) is None:
            raise ValueError("inventory attribute keys must be canonical lowercase ASCII")
        if len(key) > 64:
            raise ValueError("inventory attribute key exceeds 64 characters")
        if key in seen:
            raise ValueError("inventory attribute keys must be unique")
        attribute_value = _normalize_business_text(
            raw_value,
            field="inventory attribute value",
            max_characters=256,
            max_utf8_bytes=512,
            allow_empty=True,
        )
        seen.add(key)
        total_bytes += len(key.encode("ascii")) + len(attribute_value.encode("utf-8"))
        canonical.append((key, attribute_value))

    if total_bytes > InventoryAttributes.MAX_TOTAL_UTF8_BYTES:
        raise ValueError("inventory attributes exceed the total encoded-size limit")
    return tuple(sorted(canonical))


def _mapping_items(value: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, Mapping):
        raise TypeError("inventory attributes must be a mapping")
    items: list[tuple[str, str]] = []
    for index, item in enumerate(cast(Mapping[object, object], value).items()):
        if index == InventoryAttributes.MAX_ITEMS:
            raise ValueError("inventory record has too many attributes")
        items.append(cast(tuple[str, str], item))
    return tuple(items)


def _attribute_pair(value: object) -> tuple[object, object]:
    if not isinstance(value, tuple):
        raise TypeError("each inventory attribute must be a key-value tuple")
    pair = cast(tuple[object, ...], value)
    if len(pair) != 2:
        raise TypeError("each inventory attribute must be a key-value tuple")
    return pair[0], pair[1]


def _normalize_business_text(
    value: object,
    *,
    field: str,
    max_characters: int,
    max_utf8_bytes: int,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be text")
    normalized = unicodedata.normalize("NFC", value)
    if not normalized and not allow_empty:
        raise ValueError(f"{field} must not be empty")
    if normalized != normalized.strip(" ") or "  " in normalized:
        raise ValueError(f"{field} must use canonical spacing")
    if any(character.isspace() and character != " " for character in normalized):
        raise ValueError(f"{field} contains unsupported whitespace")
    if any(unicodedata.category(character).startswith("C") for character in normalized):
        raise ValueError(f"{field} contains a control or unsupported code point")
    encoded = normalized.encode("utf-8")
    if len(normalized) > max_characters or len(encoded) > max_utf8_bytes:
        raise ValueError(f"{field} exceeds its size limit")
    return normalized
