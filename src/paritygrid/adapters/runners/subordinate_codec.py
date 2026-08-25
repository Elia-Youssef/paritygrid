"""Versioned bounded primitive codec for subordinate CPU operations.

The process-pool isolation boundary never carries the runner-neutral
full-plan envelopes: subordinate CPU operations use their own closed,
versioned envelope of JSON-compatible primitives.  The codec rejects
unknown operations and versions, unknown primitive types, oversized or
over-nested payloads, secret markers, path shapes, and every kind of
live object.  Both the parent pool and the process workers import this
module; it stays pure so the transitive worker import gate holds.
"""

from __future__ import annotations

import hashlib
import json
from typing import cast

SUBORDINATE_CODEC_VERSION = 1
MAX_OPERATION_ID_LENGTH = 64
MAX_PAYLOAD_BYTES = 1_048_576
MAX_PAYLOAD_DEPTH = 4
MAX_LIST_ITEMS = 256
MAX_MAP_ENTRIES = 64
MAX_TEXT_LENGTH = 4_096

_FORBIDDEN_KEY_PARTS = frozenset(
    {"password", "secret", "token", "credential", "api_key", "authorization", "session"}
)
_PATH_SHAPE_PREFIXES = ("/", "\\", "c:", "http://", "https://", "file:")
_ALLOWED_ROOTS: dict[str, int] = {
    "sha256_digest": 1,
    "sort_integers": 1,
}


class SubordinateCodecError(ValueError):
    """Base failure for the subordinate codec."""


class SubordinateOperationError(SubordinateCodecError):
    """The operation identity or version is not registered."""


class SubordinatePayloadError(SubordinateCodecError):
    """The payload violates the closed primitive envelope."""


def _validate_operation(operation_id: object) -> str:
    if type(operation_id) is not str:
        raise SubordinateOperationError("operation id must be text")
    text = operation_id
    if not 1 <= len(text) <= MAX_OPERATION_ID_LENGTH:
        raise SubordinateOperationError("operation id length is outside the bound")
    for character in text:
        if not ("\x20" <= character <= "\x7e"):
            raise SubordinateOperationError("operation id must use printable ASCII")
    if text not in _ALLOWED_ROOTS:
        raise SubordinateOperationError("operation is not registered")
    return text


def _validate_value(value: object, depth: int, subject: str) -> None:
    if depth > MAX_PAYLOAD_DEPTH:
        raise SubordinatePayloadError(f"{subject} exceeds the nesting bound")
    if value is None or type(value) is bool:
        return
    if type(value) is int:
        if not -(2**53) <= value <= 2**53:
            raise SubordinatePayloadError(f"{subject} integer is outside the bound")
        return
    if type(value) is str:
        text = value
        if len(text) > MAX_TEXT_LENGTH:
            raise SubordinatePayloadError(f"{subject} text exceeds the bound")
        lowered = text.strip().lower()
        if any(part in lowered for part in _FORBIDDEN_KEY_PARTS):
            raise SubordinatePayloadError(f"{subject} contains a secret marker")
        for prefix in _PATH_SHAPE_PREFIXES:
            if lowered.startswith(prefix):
                raise SubordinatePayloadError(f"{subject} contains a path shape")
        return
    if type(value) is list:
        items = cast(list[object], value)
        if len(items) > MAX_LIST_ITEMS:
            raise SubordinatePayloadError(f"{subject} list exceeds the item bound")
        for item in items:
            _validate_value(item, depth + 1, subject)
        return
    if type(value) is dict:
        mapping = cast(dict[object, object], value)
        if len(mapping) > MAX_MAP_ENTRIES:
            raise SubordinatePayloadError(f"{subject} map exceeds the entry bound")
        for key, item in mapping.items():
            if type(key) is not str:
                raise SubordinatePayloadError(f"{subject} map keys must be text")
            _validate_value(key, depth + 1, subject)
            _validate_value(item, depth + 1, subject)
        return
    raise SubordinatePayloadError(f"{subject} uses an unsupported primitive")


def encode_request(operation_id: str, operation_version: int, payload: object) -> bytes:
    """Encode one subordinate operation request deterministically."""
    operation = _validate_operation(operation_id)
    if type(operation_version) is not int or not 1 <= operation_version <= 2_147_483_647:
        raise SubordinateOperationError("operation version is invalid")
    if operation_version != _ALLOWED_ROOTS[operation]:
        raise SubordinateOperationError("operation version is not registered")
    if type(payload) is not dict:
        raise SubordinatePayloadError("payload must be a primitive mapping")
    request_payload = cast(dict[str, object], payload)
    _validate_value(request_payload, 0, "payload")
    try:
        encoded = json.dumps(
            cast(
                dict[str, object],
                {
                    "codec_version": SUBORDINATE_CODEC_VERSION,
                    "operation": operation,
                    "operation_version": operation_version,
                    "payload": request_payload,
                },
            ),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as error:
        raise SubordinatePayloadError("payload is not canonical ASCII JSON") from error
    if len(encoded) > MAX_PAYLOAD_BYTES:
        raise SubordinatePayloadError("payload exceeds the byte bound")
    return encoded


def decode_request(data: bytes) -> tuple[str, int, dict[str, object]]:
    """Decode one request envelope exactly, failing closed."""
    if type(data) is not bytes:
        raise TypeError("request must be bytes")
    if len(data) > MAX_PAYLOAD_BYTES:
        raise SubordinatePayloadError("request exceeds the byte bound")
    try:
        decoded: object = json.loads(data.decode("ascii"))
    except (ValueError, UnicodeDecodeError) as error:
        raise SubordinatePayloadError("request is not canonical ASCII JSON") from error
    if type(decoded) is not dict:
        raise SubordinatePayloadError("request must be an object")
    document = cast(dict[str, object], decoded)
    if document.get("codec_version") != SUBORDINATE_CODEC_VERSION:
        raise SubordinateCodecError("request codec version is unknown")
    operation = _validate_operation(document.get("operation"))
    version = document.get("operation_version")
    if type(version) is not int or version != _ALLOWED_ROOTS[operation]:
        raise SubordinateOperationError("operation version is not registered")
    payload_value = document.get("payload")
    if type(payload_value) is not dict:
        raise SubordinatePayloadError("payload must be a primitive mapping")
    payload = cast(dict[str, object], payload_value)
    _validate_value(payload, 0, "payload")
    canonical = encode_request(operation, version, payload)
    if canonical != data:
        raise SubordinatePayloadError("request is not canonically encoded")
    return operation, version, payload


def encode_response(operation_id: str, result: object) -> bytes:
    """Encode one subordinate operation response deterministically."""
    operation = _validate_operation(operation_id)
    if type(result) is not dict:
        raise SubordinatePayloadError("result must be a primitive mapping")
    response_result = cast(dict[str, object], result)
    _validate_value(response_result, 0, "result")
    try:
        encoded = json.dumps(
            cast(
                dict[str, object],
                {
                    "codec_version": SUBORDINATE_CODEC_VERSION,
                    "operation": operation,
                    "result": response_result,
                },
            ),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as error:
        raise SubordinatePayloadError("result is not canonical ASCII JSON") from error
    if len(encoded) > MAX_PAYLOAD_BYTES:
        raise SubordinatePayloadError("result exceeds the byte bound")
    return encoded


def decode_response(data: bytes) -> tuple[str, dict[str, object]]:
    """Decode one response envelope exactly, failing closed."""
    if type(data) is not bytes:
        raise TypeError("response must be bytes")
    if len(data) > MAX_PAYLOAD_BYTES:
        raise SubordinatePayloadError("response exceeds the byte bound")
    try:
        decoded: object = json.loads(data.decode("ascii"))
    except (ValueError, UnicodeDecodeError) as error:
        raise SubordinatePayloadError("response is not canonical ASCII JSON") from error
    if type(decoded) is not dict:
        raise SubordinatePayloadError("response must be an object")
    document = cast(dict[str, object], decoded)
    if document.get("codec_version") != SUBORDINATE_CODEC_VERSION:
        raise SubordinateCodecError("response codec version is unknown")
    operation = _validate_operation(document.get("operation"))
    result_value = document.get("result")
    if type(result_value) is not dict:
        raise SubordinatePayloadError("result must be a primitive mapping")
    result = cast(dict[str, object], result_value)
    _validate_value(result, 0, "result")
    if encode_response(operation, result) != data:
        raise SubordinatePayloadError("response is not canonically encoded")
    return operation, result


# ---- registered connector-free CPU operations -----------------------------


def execute_sha256_digest(payload: dict[str, object]) -> dict[str, object]:
    """Digest one bounded hex payload with SHA-256."""
    data_hex = payload.get("data_hex")
    if type(data_hex) is not str or not data_hex:
        raise SubordinatePayloadError("sha256_digest requires a hex payload")
    try:
        digest = hashlib.sha256(bytes.fromhex(data_hex)).hexdigest()
    except ValueError as error:
        raise SubordinatePayloadError("sha256_digest payload is not hex") from error
    return {"digest": digest}


def execute_sort_integers(payload: dict[str, object]) -> dict[str, object]:
    """Sort one bounded integer list deterministically."""
    values = payload.get("values")
    if type(values) is not list:
        raise SubordinatePayloadError("sort_integers requires an integer list")
    items = cast(list[object], values)
    if any(type(item) is not int for item in items):
        raise SubordinatePayloadError("sort_integers requires an integer list")
    return {"sorted": sorted(cast(list[int], items))}


_REGISTERED_OPERATIONS = {
    "sha256_digest": execute_sha256_digest,
    "sort_integers": execute_sort_integers,
}


def dispatch_registered_operation(
    operation_id: str,
    payload: dict[str, object],
) -> dict[str, object]:
    """Execute exactly one registered connector-free CPU operation."""
    operation = _validate_operation(operation_id)
    return _REGISTERED_OPERATIONS[operation](payload)


__all__ = [
    "SUBORDINATE_CODEC_VERSION",
    "SubordinateCodecError",
    "SubordinateOperationError",
    "SubordinatePayloadError",
    "decode_request",
    "decode_response",
    "dispatch_registered_operation",
    "encode_request",
    "encode_response",
]
