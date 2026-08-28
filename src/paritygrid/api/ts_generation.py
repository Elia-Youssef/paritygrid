"""Deterministic TypeScript type generation from the OpenAPI contract.

The generator is pinned inside this repository, uses only the standard
library, and produces byte-identical output for identical input.  Generated
output is never hand-edited; the emitted module lives at
``web/src/api/generated/schema.d.ts``.
"""

import json
import re
from typing import Any, cast

GENERATOR_VERSION = 1
HEADER = (
    "/**\n"
    " * Generated from docs/generated/openapi.json by\n"
    " * scripts/generate_api_types.py (generator version "
    f"{GENERATOR_VERSION}).\n"
    " * Do not edit by hand; regenerate with the documented command.\n"
    " */\n"
)

_IDENTIFIER = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*\Z")
_HTTP_METHODS = ("get", "put", "post", "delete", "options", "head", "patch", "trace")


def _mapping(value: Any) -> dict[str, Any]:
    return cast("dict[str, Any]", value) if isinstance(value, dict) else {}


def _sequence(value: Any) -> list[Any]:
    return cast("list[Any]", value) if isinstance(value, list) else []


def _key(name: str) -> str:
    return name if _IDENTIFIER.fullmatch(name) else json.dumps(name)


def _ref_name(reference: Any) -> str:
    return str(reference).rsplit("/", maxsplit=1)[-1]


def _literal(value: Any) -> str:
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return json.dumps(value)
    return "unknown"


class SchemaEmitter:
    """Render one OpenAPI 3.1 schema subset as strict TypeScript."""

    def type_of(self, schema: Any, indent: int = 0) -> str:
        if isinstance(schema, bool):
            return "unknown" if schema else "never"
        if not isinstance(schema, dict):
            return "unknown"
        node = _mapping(schema)
        if "$ref" in node:
            return f'components["schemas"]["{_ref_name(node["$ref"])}"]'
        if "enum" in node:
            values = _sequence(node["enum"])
            if not values:
                return "never"
            return " | ".join(_literal(value) for value in values)
        if "const" in node:
            return _literal(node["const"])
        if "anyOf" in node:
            return " | ".join(self.type_of(item, indent) for item in _sequence(node["anyOf"]))
        if "oneOf" in node:
            return " | ".join(self.type_of(item, indent) for item in _sequence(node["oneOf"]))
        if "allOf" in node:
            return " & ".join(self.type_of(item, indent) for item in _sequence(node["allOf"]))
        declared = node.get("type")
        if isinstance(declared, list):
            return " | ".join(self._primitive(item) for item in _sequence(declared))
        if declared == "array":
            return f"Array<{self.type_of(node.get('items', {}), indent)}>"
        if declared == "object" or "properties" in node or "additionalProperties" in node:
            return self._object(node, indent)
        if declared is not None:
            return self._primitive(declared)
        return "unknown"

    def _primitive(self, declared: Any) -> str:
        if declared == "string":
            return "string"
        if declared in ("integer", "number"):
            return "number"
        if declared == "boolean":
            return "boolean"
        if declared == "null":
            return "null"
        return "unknown"

    def _object(self, schema: dict[str, Any], indent: int) -> str:
        properties = _mapping(schema.get("properties"))
        required = {str(name) for name in _sequence(schema.get("required"))}
        additional = schema.get("additionalProperties")
        lines: list[str] = []
        for name, sub_schema in properties.items():
            rendered = self.type_of(sub_schema, indent + 4)
            optional = "" if name in required else "?"
            lines.append(f"{_key(str(name))}{optional}: {rendered};")
        if isinstance(additional, dict):
            lines.append(f"[key: string]: {self.type_of(_mapping(additional), indent + 4)};")
        elif additional is True and not properties:
            lines.append("[key: string]: unknown;")
        if not lines:
            return "Record<string, never>"
        pad = " " * (indent + 4)
        body = "\n".join(f"{pad}{line}" for line in lines)
        return "{\n" + body + "\n" + " " * indent + "}"


def _request_body_schema(request_body: dict[str, Any], emitter: SchemaEmitter) -> str:
    content = _mapping(request_body.get("content"))
    if not content:
        return "never"
    schema = _mapping(content.get("application/json")).get("schema", {})
    return emitter.type_of(schema, indent=12)


def _responses_block(responses: dict[str, Any], emitter: SchemaEmitter) -> list[str]:
    parts: list[str] = ["            responses: {"]
    for status in sorted(responses, key=str):
        content = _mapping(_mapping(responses[status]).get("content"))
        media: list[str] = []
        for media_type in sorted(content):
            schema = _mapping(content[media_type]).get("schema", {})
            media.append(f"{json.dumps(media_type)}: {emitter.type_of(schema, indent=24)};")
        if media:
            rendered = "\n".join(f"                        {line}" for line in media)
            parts.append(
                f"                {_key(str(status))}: {{\n"
                "                    content: {\n"
                f"{rendered}\n"
                "                    };\n"
                "                };"
            )
        else:
            parts.append(f"                {_key(str(status))}: {{}};")
    parts.append("            };")
    return parts


def _parameter(parameter: Any) -> str:
    node = _mapping(parameter)
    name = json.dumps(str(node.get("name", "")))
    location = json.dumps(str(node.get("in", "")))
    required = "true" if node.get("required") else "false"
    return f"{{ name: {name}; in: {location}; required: {required} }}"


def build_document(source: dict[str, Any]) -> str:
    """Render the complete generated module."""
    components = _mapping(_mapping(source.get("components")).get("schemas"))
    emitter = SchemaEmitter()
    blocks: list[str] = [HEADER]

    schema_lines: list[str] = []
    for name in sorted(components):
        rendered = emitter.type_of(components[name], indent=8)
        schema_lines.append(f"        {_key(str(name))}: {rendered};")
    blocks.append(
        "export interface components {\n    schemas: {\n" + "\n".join(schema_lines) + "\n    };\n}"
    )

    alias_lines = [
        f'export type {name} = components["schemas"][{json.dumps(name)}];'
        for name in sorted(components)
    ]
    blocks.append("\n".join(alias_lines))

    operation_lines: list[str] = []
    paths = _mapping(source.get("paths"))
    for path in sorted(paths):
        item = _mapping(paths[path])
        for method in _HTTP_METHODS:
            operation = _mapping(item.get(method))
            if not operation:
                continue
            operation_id = operation.get("operationId")
            if not isinstance(operation_id, str):
                continue
            parts: list[str] = []
            parameters = _sequence(operation.get("parameters"))
            if parameters:
                rendered = " | ".join(_parameter(parameter) for parameter in parameters)
                parts.append(f"            parameters: Array<{rendered}>;")
            request_body = _mapping(operation.get("requestBody"))
            if request_body:
                parts.append(
                    "            requestBody: { content: { "
                    f'"application/json": {_request_body_schema(request_body, emitter)}; '
                    "}; };"
                )
            responses = _mapping(operation.get("responses"))
            if responses:
                parts.extend(_responses_block(responses, emitter))
            body = "\n".join(parts)
            operation_lines.append(f"        {_key(operation_id)}: {{\n{body}\n        }};")
    blocks.append("export interface operations {\n" + "\n".join(operation_lines) + "\n}")
    blocks.append("export type webhooks = Record<string, never>;")
    return "\n\n".join(block.rstrip("\n") for block in blocks) + "\n"


__all__ = ["GENERATOR_VERSION", "SchemaEmitter", "build_document"]
