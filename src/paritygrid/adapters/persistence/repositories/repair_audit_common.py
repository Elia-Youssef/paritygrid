"""Validation, canonical codecs, and error translation for repair and audit storage."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from collections.abc import Callable
from decimal import Decimal
from functools import wraps
from typing import cast

from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError

from paritygrid.adapters.persistence.repositories.common import MAX_CANONICAL_DOCUMENT_BYTES
from paritygrid.adapters.persistence.values import CanonicalStorageJson, StoragePrimitive
from paritygrid.adapters.persistence.writer.contention import is_sqlite_contention
from paritygrid.application.ports.consistency import (
    ConsistencyInvalidRequestError,
    RedactedDocument,
)
from paritygrid.application.ports.repair_audit import (
    MAX_PERSISTED_INTEGER,
    AuditCorruptionError,
    AuditInvalidRequestError,
    AuditStorageError,
    AuditStorageUnavailableError,
    InventoryEffect,
    RepairActionCursor,
    RepairActionEffect,
    RepairApplicationReservation,
    RepairApplicationResult,
    RepairCorruptionError,
    RepairInvalidRequestError,
    RepairPlanCursor,
    RepairStateConflictError,
    RepairStorageError,
    RepairStorageUnavailableError,
)
from paritygrid.application.ports.writer import PersistenceContentionError
from paritygrid.domain.canonical import FingerprintScope, fingerprint_state
from paritygrid.domain.models import (
    ConnectorId,
    CurrencyCode,
    InventoryAttributes,
    InventoryRecord,
    Money,
    RepairActionId,
    RepairPlanId,
    RunId,
    StateFingerprint,
    UtcTimestamp,
)
from paritygrid.domain.reconciliation import FieldMismatch, ReconciliationField, differences_between
from paritygrid.domain.repair import RepairAction, RepairPlan

_PORTABLE_IDENTITY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]*", flags=re.ASCII)
_SNAKE_CASE_PATTERN = re.compile(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*", flags=re.ASCII)
_CANONICAL_KEY_PATTERN = re.compile(r"[A-Z0-9]+(?:-[A-Z0-9]+)*", flags=re.ASCII)
_SYNTHETIC_CONNECTOR = ConnectorId("con_repair-effect")
_SYNTHETIC_SOURCE_KEY = "repair-effect"


def translate_repair_storage_errors[**P, R](operation: Callable[P, R]) -> Callable[P, R]:
    """Replace database implementation failures with a redacted repair error."""

    @wraps(operation)
    def translated(*args: P.args, **kwargs: P.kwargs) -> R:
        contention = False
        unavailable = False
        try:
            return operation(*args, **kwargs)
        except OperationalError as error:
            contention = is_sqlite_contention(error)
            unavailable = not contention
        except InterfaceError:
            unavailable = True
        except SQLAlchemyError:
            pass
        if contention:
            raise PersistenceContentionError("Persistence is temporarily contended.") from None
        if unavailable:
            raise RepairStorageUnavailableError("Repair storage is unavailable.") from None
        raise RepairStorageError("Repair storage operation failed.") from None

    return translated


def translate_audit_storage_errors[**P, R](operation: Callable[P, R]) -> Callable[P, R]:
    """Replace database implementation failures with a redacted audit error."""

    @wraps(operation)
    def translated(*args: P.args, **kwargs: P.kwargs) -> R:
        contention = False
        unavailable = False
        try:
            return operation(*args, **kwargs)
        except OperationalError as error:
            contention = is_sqlite_contention(error)
            unavailable = not contention
        except InterfaceError:
            unavailable = True
        except SQLAlchemyError:
            pass
        if contention:
            raise PersistenceContentionError("Persistence is temporarily contended.") from None
        if unavailable:
            raise AuditStorageUnavailableError("Audit storage is unavailable.") from None
        raise AuditStorageError("Audit storage operation failed.") from None

    return translated


def require_exact[T](value: object, expected: type[T], subject: str) -> T:
    if type(value) is not expected:
        raise RepairInvalidRequestError(f"{subject} must use {expected.__name__}")
    return cast(T, value)


def require_audit_exact[T](value: object, expected: type[T], subject: str) -> T:
    if type(value) is not expected:
        raise AuditInvalidRequestError(f"{subject} must use {expected.__name__}")
    return cast(T, value)


def bounded_text(value: object, subject: str, maximum: int) -> str:
    if type(value) is not str:
        raise RepairInvalidRequestError(f"{subject} must be text")
    if not 1 <= len(value) <= maximum:
        raise RepairInvalidRequestError(f"{subject} has an invalid length")
    if unicodedata.normalize("NFC", value) != value:
        raise RepairInvalidRequestError(f"{subject} must use normalized Unicode")
    if any(unicodedata.category(character).startswith("C") for character in value):
        raise RepairInvalidRequestError(f"{subject} contains unsupported characters")
    return value


def portable_identity(value: object, subject: str, maximum: int) -> str:
    identity = bounded_text(value, subject, maximum)
    if _PORTABLE_IDENTITY_PATTERN.fullmatch(identity) is None:
        raise RepairInvalidRequestError(f"{subject} must use portable ASCII")
    return identity


def positive_int(value: object, subject: str) -> int:
    if type(value) is not int or not 1 <= value <= MAX_PERSISTED_INTEGER:
        raise RepairInvalidRequestError(f"{subject} must be an integer within the supported range")
    return value


def incrementable_int(value: object, subject: str) -> int:
    current = positive_int(value, subject)
    if current >= MAX_PERSISTED_INTEGER:
        raise RepairStateConflictError(f"{subject} cannot advance beyond the supported maximum")
    return current


def audit_bounded_text(value: object, subject: str, maximum: int) -> str:
    try:
        return bounded_text(value, subject, maximum)
    except RepairInvalidRequestError as error:
        raise AuditInvalidRequestError(str(error)) from None


def audit_portable_identity(value: object, subject: str, maximum: int) -> str:
    try:
        return portable_identity(value, subject, maximum)
    except RepairInvalidRequestError as error:
        raise AuditInvalidRequestError(str(error)) from None


def audit_snake_case(value: object, subject: str, maximum: int) -> str:
    text = audit_bounded_text(value, subject, maximum)
    if _SNAKE_CASE_PATTERN.fullmatch(text) is None:
        raise AuditInvalidRequestError(f"{subject} must use canonical lowercase snake_case")
    return text


def audit_positive_int(value: object, subject: str) -> int:
    if type(value) is not int or not 1 <= value <= MAX_PERSISTED_INTEGER:
        raise AuditInvalidRequestError(f"{subject} must be an integer within the supported range")
    return value


def encode_effect(effect: InventoryEffect) -> CanonicalStorageJson:
    exact = require_exact(effect, InventoryEffect, "inventory effect")
    return CanonicalStorageJson.encode(cast(StoragePrimitive, _effect_primitive(exact)))


def decode_effect(value: object, subject: str) -> InventoryEffect:
    primitive = _decode_object(value, subject)
    if set(primitive) != {
        "attributes",
        "name",
        "quantity",
        "sku",
        "unit_price",
        "updated_at",
    }:
        raise RepairCorruptionError(f"{subject} is corrupt")
    try:
        price = _decode_money(primitive["unit_price"])
        attributes = _decode_attributes(primitive["attributes"])
        sku = _primitive_text(primitive["sku"])
        name = _primitive_text(primitive["name"])
        quantity = _primitive_int(primitive["quantity"])
        updated_at = UtcTimestamp.parse(_primitive_text(primitive["updated_at"]))
        return InventoryEffect(
            sku=sku,
            name=name,
            quantity=quantity,
            unit_price=price,
            updated_at=updated_at,
            attributes=attributes,
        )
    except (TypeError, ValueError) as error:
        raise RepairCorruptionError(f"{subject} is corrupt") from error


def effect_digest(effect: InventoryEffect) -> StateFingerprint:
    encoded = encode_effect(effect)
    return StateFingerprint(hashlib.sha256(encoded.text.encode("utf-8")).hexdigest())


def effect_mismatches(
    proposed: InventoryEffect, expected_target: InventoryEffect | None
) -> tuple[FieldMismatch, ...]:
    """Derive exact mismatch evidence from provenance-free business effects."""
    if expected_target is None:
        return ()
    return differences_between(_synthetic_record(proposed), _synthetic_record(expected_target))


def encode_mismatch_evidence(effect: RepairActionEffect) -> CanonicalStorageJson:
    exact = require_exact(effect, RepairActionEffect, "repair action effect")
    evidence = [_mismatch_primitive(item) for item in exact.mismatches]
    return CanonicalStorageJson.encode(cast(StoragePrimitive, evidence))


def validate_mismatch_evidence(value: object, effect: RepairActionEffect) -> None:
    if type(value) is not str:
        raise RepairCorruptionError("repair mismatch evidence is corrupt")
    try:
        stored = CanonicalStorageJson(value)
    except (TypeError, ValueError) as error:
        raise RepairCorruptionError("repair mismatch evidence is corrupt") from error
    if stored != encode_mismatch_evidence(effect):
        raise RepairCorruptionError("repair mismatch evidence is corrupt")


def plan_content_fingerprint(plan: RepairPlan) -> StateFingerprint:
    exact = require_exact(plan, RepairPlan, "repair plan")
    return fingerprint_state((exact,), scope=FingerprintScope.REPAIR_PLAN_CONTENT)


def effect_content_fingerprint(
    plan_id: RepairPlanId,
    reconciliation_fingerprint: StateFingerprint,
    effects: tuple[RepairActionEffect, ...],
) -> StateFingerprint:
    actions = tuple(_synthetic_action(effect) for effect in effects)
    plan = RepairPlan(
        plan_id=plan_id,
        state_fingerprint=reconciliation_fingerprint,
        actions=actions,
    )
    return fingerprint_state((plan,), scope=FingerprintScope.REPAIR_PLAN_CONTENT)


def encode_redacted_document(
    document: RedactedDocument,
    subject: str,
    *,
    maximum_bytes: int = MAX_CANONICAL_DOCUMENT_BYTES,
) -> CanonicalStorageJson:
    exact = require_exact(document, RedactedDocument, subject)
    try:
        encoded = CanonicalStorageJson.encode(cast(StoragePrimitive, exact.to_mapping()))
    except (TypeError, ValueError) as error:
        raise RepairInvalidRequestError(f"{subject} is invalid") from error
    if len(encoded.text.encode("utf-8")) > maximum_bytes:
        raise RepairInvalidRequestError(f"{subject} exceeds the supported encoded size")
    return encoded


def decode_redacted_document(value: object, subject: str) -> RedactedDocument:
    mapping = _decode_object(value, subject)
    try:
        return RedactedDocument.from_mapping(mapping)
    except (ConsistencyInvalidRequestError, RecursionError, TypeError, ValueError) as error:
        raise RepairCorruptionError(f"{subject} is corrupt") from error


def encode_application_result(result: RepairApplicationResult) -> CanonicalStorageJson:
    exact = require_exact(result, RepairApplicationResult, "repair application result")
    version = positive_int(exact.schema_version, "repair result schema version")
    detail = encode_redacted_document(exact.detail, "repair application result detail").decode()
    return CanonicalStorageJson.encode(
        cast(StoragePrimitive, {"detail": detail, "schema_version": version})
    )


def decode_application_result(value: object) -> RepairApplicationResult:
    mapping = _decode_object(value, "repair application result")
    if set(mapping) != {"detail", "schema_version"}:
        raise RepairCorruptionError("repair application result is corrupt")
    detail_value = mapping["detail"]
    if type(detail_value) is not dict:
        raise RepairCorruptionError("repair application result is corrupt")
    try:
        schema_version = _primitive_int(mapping["schema_version"])
        if not 1 <= schema_version <= MAX_PERSISTED_INTEGER:
            raise ValueError
        detail_json = CanonicalStorageJson.encode(cast(StoragePrimitive, detail_value)).text
        return RepairApplicationResult(
            schema_version=schema_version,
            detail=decode_redacted_document(detail_json, "repair application result detail"),
        )
    except (TypeError, ValueError) as error:
        raise RepairCorruptionError("repair application result is corrupt") from error


def encode_audit_detail(document: RedactedDocument) -> CanonicalStorageJson:
    try:
        exact = require_audit_exact(document, RedactedDocument, "audit detail")
        encoded = CanonicalStorageJson.encode(cast(StoragePrimitive, exact.to_mapping()))
    except (TypeError, ValueError) as error:
        raise AuditInvalidRequestError("audit detail is invalid") from error
    if len(encoded.text.encode("utf-8")) > MAX_CANONICAL_DOCUMENT_BYTES:
        raise AuditInvalidRequestError("audit detail exceeds the supported encoded size")
    return encoded


def decode_audit_detail(value: object) -> RedactedDocument:
    if type(value) is not str:
        raise AuditCorruptionError("audit detail is corrupt")
    try:
        decoded = CanonicalStorageJson(value).decode()
        if type(decoded) is not dict:
            raise ValueError
        return RedactedDocument.from_mapping(cast(dict[str, object], decoded))
    except (ConsistencyInvalidRequestError, RecursionError, TypeError, ValueError) as error:
        raise AuditCorruptionError("audit detail is corrupt") from error


def stored_timestamp(value: object, subject: str) -> UtcTimestamp:
    if type(value) is not str:
        raise RepairCorruptionError(f"{subject} is corrupt")
    try:
        return UtcTimestamp.parse(value)
    except (TypeError, ValueError) as error:
        raise RepairCorruptionError(f"{subject} is corrupt") from error


def stored_optional_timestamp(value: object, subject: str) -> UtcTimestamp | None:
    return None if value is None else stored_timestamp(value, subject)


def stored_identifier[T](value: object, expected: type[T], subject: str) -> T:
    if type(value) is not str:
        raise RepairCorruptionError(f"{subject} is corrupt")
    try:
        return expected(value)  # type: ignore[call-arg]
    except (TypeError, ValueError) as error:
        raise RepairCorruptionError(f"{subject} is corrupt") from error


def stored_fingerprint(value: object, subject: str) -> StateFingerprint:
    return stored_identifier(value, StateFingerprint, subject)


def stored_positive_int(value: object, subject: str) -> int:
    if type(value) is not int or not 1 <= value <= MAX_PERSISTED_INTEGER:
        raise RepairCorruptionError(f"{subject} is corrupt")
    return value


def require_reservation(value: object) -> RepairApplicationReservation:
    reservation = require_exact(
        value, RepairApplicationReservation, "repair application reservation"
    )
    require_exact(reservation.repair_plan_id, RepairPlanId, "reservation repair-plan identifier")
    require_exact(reservation.run_id, RunId, "reservation run identifier")
    require_exact(
        reservation.reconciliation_fingerprint,
        StateFingerprint,
        "reservation reconciliation fingerprint",
    )
    require_exact(
        reservation.content_fingerprint,
        StateFingerprint,
        "reservation content fingerprint",
    )
    require_exact(reservation.applying_at, UtcTimestamp, "reservation application time")
    positive_int(reservation.row_version, "reservation row version")
    return reservation


def require_plan_cursor(value: object) -> RepairPlanCursor:
    cursor = require_exact(value, RepairPlanCursor, "repair cursor")
    require_exact(cursor.created_at, UtcTimestamp, "repair cursor creation time")
    require_exact(cursor.repair_plan_id, RepairPlanId, "repair cursor plan identifier")
    return cursor


def require_action_cursor(value: object) -> RepairActionCursor:
    cursor = require_exact(value, RepairActionCursor, "repair action cursor")
    key = bounded_text(cursor.canonical_key, "repair action cursor canonical key", 64)
    if _CANONICAL_KEY_PATTERN.fullmatch(key) is None:
        raise RepairInvalidRequestError(
            "repair action cursor canonical key must use canonical uppercase ASCII"
        )
    require_exact(cursor.repair_action_id, RepairActionId, "repair action cursor identifier")
    return cursor


def _effect_primitive(effect: InventoryEffect) -> dict[str, StoragePrimitive]:
    return {
        "attributes": [[key, item_value] for key, item_value in effect.attributes.items],
        "name": effect.name,
        "quantity": effect.quantity,
        "sku": effect.sku,
        "unit_price": {
            "currency": effect.unit_price.currency.value,
            "minor_unit_exponent": effect.unit_price.minor_unit_exponent,
            "minor_units": effect.unit_price.minor_units,
        },
        "updated_at": str(effect.updated_at),
    }


def _mismatch_primitive(mismatch: FieldMismatch) -> dict[str, StoragePrimitive]:
    return {
        "field": mismatch.field.value,
        "source": _comparable_primitive(mismatch.field, mismatch.source_value),
        "target": _comparable_primitive(mismatch.field, mismatch.target_value),
    }


def _comparable_primitive(field: ReconciliationField, value: object) -> StoragePrimitive:
    if field is ReconciliationField.NAME:
        return cast(str, value)
    if field is ReconciliationField.QUANTITY:
        return cast(int, value)
    if field is ReconciliationField.UNIT_PRICE:
        money = cast(Money, value)
        return {
            "currency": money.currency.value,
            "minor_unit_exponent": money.minor_unit_exponent,
            "minor_units": money.minor_units,
        }
    if field is ReconciliationField.UPDATED_AT:
        return str(cast(UtcTimestamp, value))
    attributes = cast(InventoryAttributes, value)
    return [[key, item_value] for key, item_value in attributes.items]


def _synthetic_action(effect: RepairActionEffect) -> RepairAction:
    expected = None if effect.expected_target is None else _synthetic_record(effect.expected_target)
    return RepairAction(
        action_id=effect.action_id,
        conflict_id=effect.conflict_id,
        state_fingerprint=effect.reconciliation_fingerprint,
        kind=effect.kind,
        proposed_record=_synthetic_record(effect.proposed),
        expected_target_record=expected,
    )


def _synthetic_record(effect: InventoryEffect) -> InventoryRecord:
    return InventoryRecord(
        sku=effect.sku,
        name=effect.name,
        quantity=effect.quantity,
        unit_price=effect.unit_price,
        updated_at=effect.updated_at,
        connector_id=_SYNTHETIC_CONNECTOR,
        source_record_key=_SYNTHETIC_SOURCE_KEY,
        attributes=effect.attributes,
    )


def _decode_object(value: object, subject: str) -> dict[str, object]:
    if type(value) is not str:
        raise RepairCorruptionError(f"{subject} is corrupt")
    try:
        decoded = CanonicalStorageJson(value).decode()
        if type(decoded) is not dict:
            raise ValueError
        return cast(dict[str, object], decoded)
    except (TypeError, ValueError) as error:
        raise RepairCorruptionError(f"{subject} is corrupt") from error


def _decode_money(value: object) -> Money:
    if type(value) is not dict:
        raise TypeError
    mapping = cast(dict[str, object], value)
    if set(mapping) != {"currency", "minor_unit_exponent", "minor_units"}:
        raise ValueError
    exponent = _primitive_int(mapping["minor_unit_exponent"])
    minor_units = _primitive_int(mapping["minor_units"])
    amount = Decimal(minor_units).scaleb(-exponent)
    return Money(
        amount=amount,
        currency=CurrencyCode(_primitive_text(mapping["currency"])),
        minor_unit_exponent=exponent,
    )


def _decode_attributes(value: object) -> InventoryAttributes:
    if type(value) is not list:
        raise TypeError
    pairs: list[tuple[str, str]] = []
    for item in cast(list[object], value):
        if type(item) is not list:
            raise TypeError
        values = cast(list[object], item)
        if len(values) != 2:
            raise ValueError
        pairs.append((_primitive_text(values[0]), _primitive_text(values[1])))
    return InventoryAttributes(tuple(pairs))


def _primitive_text(value: object) -> str:
    if type(value) is not str:
        raise TypeError
    return value


def _primitive_int(value: object) -> int:
    if type(value) is not int:
        raise TypeError
    return value
