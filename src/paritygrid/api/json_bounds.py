"""Bounded JSON decoding enforced before any framework parsing."""

import json
from dataclasses import dataclass
from typing import cast


class BoundedJsonError(ValueError):
    """A request body failed bounded JSON pre-validation."""


@dataclass(frozen=True, slots=True)
class JsonBounds:
    """Explicit body size and nesting limits."""

    max_body_bytes: int
    max_depth: int

    def __post_init__(self) -> None:
        if type(self.max_body_bytes) is not int or not 1 <= self.max_body_bytes <= (
            64 * 1024 * 1024
        ):
            raise ValueError("body byte limit is outside the supported range")
        if type(self.max_depth) is not int or not 1 <= self.max_depth <= 512:
            raise ValueError("json depth limit is outside the supported range")


def decode_bounded_json(raw: bytes, *, bounds: JsonBounds) -> object:
    """Decode one UTF-8 JSON document with bounded depth and strict values.

    Rejects empty input, byte-order marks, invalid UTF-8, duplicate object
    keys, non-finite numbers, and nesting beyond the configured depth.  The
    standard recursive parser is wrapped so nesting bombs inside the byte cap
    surface as the same bounded rejection instead of a server error.
    """

    if not raw:
        raise BoundedJsonError("request body is empty")
    if raw.startswith(b"\xef\xbb\xbf"):
        raise BoundedJsonError("request body must not start with a byte-order mark")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise BoundedJsonError("request body is not valid UTF-8") from error
    try:
        parsed: object = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_constant,
        )
    except json.JSONDecodeError as error:
        raise BoundedJsonError("request body is not valid JSON") from error
    except RecursionError as error:
        # A nesting bomb inside the byte cap exhausts the recursive parser
        # before any walk runs; report it as the same bounded rejection.
        raise BoundedJsonError(
            f"request body nesting exceeds the limit of {bounds.max_depth}"
        ) from error
    except BoundedJsonError:
        raise
    except ValueError as error:
        raise BoundedJsonError("request body is not valid JSON") from error
    _require_depth(parsed, bounds.max_depth)
    return parsed


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise BoundedJsonError(f"request body contains duplicate key {key!r}")
        result[key] = value
    return result


def _reject_constant(name: str) -> object:
    raise BoundedJsonError(f"request body contains the non-finite value {name}")


def _require_depth(value: object, max_depth: int) -> None:
    stack: list[tuple[object, int]] = [(value, 1)]
    while stack:
        current, depth = stack.pop()
        if depth > max_depth:
            raise BoundedJsonError(f"request body nesting exceeds the limit of {max_depth}")
        if isinstance(current, dict):
            values = cast(dict[str, object], current)
            stack.extend((child, depth + 1) for child in values.values())
        elif isinstance(current, list):
            items = cast(list[object], current)
            stack.extend((child, depth + 1) for child in items)
