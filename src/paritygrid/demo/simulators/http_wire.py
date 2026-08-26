"""Shared HTTP/1.1 wire primitives for the synthetic simulators.

Both the asyncio and the blocking engine parse requests and encode responses
through this module so the two transports expose identical, byte-deterministic
HTTP behavior. Responses omit wall-clock headers such as ``Date`` on purpose:
simulator responses must be reproducible byte for byte.
"""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import parse_qsl, urlsplit

MAX_REQUEST_LINE_BYTES = 8_192
MAX_HEADER_BYTES = 16_384
MAX_HEADER_COUNT = 64
MAX_BODY_BYTES = 1_048_576

_SERVER_HEADER = "paritygrid-simulator"

_STATUS_TEXT: Mapping[int, str] = {
    200: "OK",
    400: "Bad Request",
    404: "Not Found",
    405: "Method Not Allowed",
    409: "Conflict",
    411: "Length Required",
    413: "Payload Too Large",
    417: "Expectation Failed",
    429: "Too Many Requests",
    431: "Request Header Fields Too Large",
    500: "Internal Server Error",
    501: "Not Implemented",
    503: "Service Unavailable",
    507: "Insufficient Storage",
}


class HttpWireError(ValueError):
    """Raised when a request head cannot be parsed within the wire bounds."""


class RequestBodyTooLargeError(HttpWireError):
    """Raised when the declared request body exceeds the supported size."""


class ResponseAction(StrEnum):
    """How an engine must deliver an encoded response."""

    RESPOND = "respond"
    HOLD_UNTIL_DISCONNECT = "hold_until_disconnect"
    CLOSE_PARTIAL = "close_partial"


@dataclass(frozen=True, slots=True)
class HttpRequest:
    """One parsed simulator request with lower-cased header names."""

    method: str
    path: str
    query: Mapping[str, str]
    headers: Mapping[str, str]
    body: bytes


@dataclass(frozen=True, slots=True)
class PlannedResponse:
    """A transport-independent response plan produced by a behavior."""

    status: int
    body: bytes
    headers: tuple[tuple[str, str], ...] = ()
    delay_microseconds: int = 0
    action: ResponseAction = ResponseAction.RESPOND
    hold_cap_microseconds: int = 0
    partial_bytes: int = 0
    close_after_response: bool = False

    def encoded(self) -> bytes:
        """Return the full deterministic HTTP/1.1 response bytes."""
        return encode_response(self)

    @property
    def delay_seconds(self) -> float:
        """Return the response delay in fractional seconds."""
        return self.delay_microseconds / 1_000_000

    @property
    def hold_cap_seconds(self) -> float:
        """Return the hold cap in fractional seconds."""
        return self.hold_cap_microseconds / 1_000_000


def json_response(status: int, document: object) -> PlannedResponse:
    """Build a deterministic JSON response plan."""
    body = json.dumps(
        document,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return PlannedResponse(
        status=status,
        body=body,
        headers=(("Content-Type", "application/json"),),
    )


def error_response(status: int, code: str, message: str) -> PlannedResponse:
    """Build the uniform JSON error document."""
    return json_response(status, {"error": {"code": code, "message": message}})


def encode_response(planned: PlannedResponse) -> bytes:
    """Encode a response plan into deterministic HTTP/1.1 bytes."""
    reason = _STATUS_TEXT.get(planned.status, "Unknown")
    head = f"HTTP/1.1 {planned.status} {reason}\r\n"
    header_lines = [
        f"Server: {_SERVER_HEADER}",
        f"Content-Length: {len(planned.body)}",
    ]
    header_lines.extend(f"{name}: {value}" for name, value in planned.headers)
    if planned.close_after_response:
        header_lines.append("Connection: close")
    head += "\r\n".join(header_lines) + "\r\n\r\n"
    return head.encode("ascii") + planned.body


@dataclass(frozen=True, slots=True)
class RequestHead:
    """A parsed request head awaiting its body."""

    method: str
    path: str
    version: str
    header_block: str
    content_length: int = 0
    expects_continue: bool = False
    connection_close: bool = False


def parse_request_head(head_bytes: bytes) -> RequestHead:
    """Parse and bound-check one request head (without the final CRLF CRLF)."""
    try:
        head_text = head_bytes.decode("ascii")
    except UnicodeDecodeError as error:
        raise HttpWireError("request head must be ASCII") from error
    lines = head_text.split("\r\n")
    if not lines or not lines[0]:
        raise HttpWireError("request head is empty")
    request_line = lines[0]
    if len(request_line) > MAX_REQUEST_LINE_BYTES:
        raise HttpWireError("request line exceeds the supported size")
    parts = request_line.split(" ")
    if len(parts) != 3 or parts[2] not in ("HTTP/1.1", "HTTP/1.0"):
        raise HttpWireError("request line must use HTTP/1.0 or HTTP/1.1 form")
    method, path, version = parts
    if not method.isalpha() or not method.isupper():
        raise HttpWireError("request method must be uppercase ASCII")
    header_lines = [line for line in lines[1:] if line]
    if len(header_lines) > MAX_HEADER_COUNT:
        raise HttpWireError("request carries too many headers")
    if len(head_text.encode("ascii")) > MAX_HEADER_BYTES:
        raise HttpWireError("request headers exceed the supported size")
    content_length = 0
    expects_continue = False
    connection_close = version == "HTTP/1.0"
    for line in header_lines:
        name, separator, value = line.partition(":")
        if not separator or not name or not value.startswith(" "):
            raise HttpWireError("malformed header line")
        lowered = name.lower()
        if lowered == "content-length":
            if not value[1:].isdigit():
                raise HttpWireError("content-length must be a nonnegative integer")
            content_length = int(value[1:])
        elif lowered == "expect":
            expects_continue = value.strip().lower() == "100-continue"
        elif lowered == "connection":
            connection_close = value.strip().lower() == "close" or (
                version == "HTTP/1.0" and value.strip().lower() != "keep-alive"
            )
        elif lowered == "transfer-encoding":
            raise HttpWireError("transfer encoding is not supported")
    if content_length > MAX_BODY_BYTES:
        raise RequestBodyTooLargeError("request body exceeds the supported size")
    return RequestHead(
        method=method,
        path=path,
        version=version,
        header_block=head_text,
        content_length=content_length,
        expects_continue=expects_continue,
        connection_close=connection_close,
    )


def query_parameters(path: str) -> dict[str, str]:
    """Extract single-valued query parameters, rejecting repeats."""
    split = urlsplit(path)
    if split.fragment:
        raise HttpWireError("request fragments are not supported")
    parameters: dict[str, str] = {}
    for name, value in parse_qsl(split.query, keep_blank_values=True, max_num_fields=32):
        if name in parameters:
            raise HttpWireError(f"query parameter {name} is repeated")
        parameters[name] = value
    return parameters


def request_path_only(path: str) -> str:
    """Return the path without its query string."""
    return urlsplit(path).path


def build_request(head: RequestHead, body: bytes) -> HttpRequest:
    """Combine a parsed head and body with extracted headers and query."""
    if len(body) != head.content_length:
        raise HttpWireError("request body length does not match content-length")
    headers: dict[str, str] = {}
    for line in head.header_block.split("\r\n")[1:]:
        if not line:
            continue
        name, _separator, value = line.partition(":")
        headers[name.strip().lower()] = value.strip()
    return HttpRequest(
        method=head.method,
        path=head.path,
        query=query_parameters(head.path),
        headers=headers,
        body=body,
    )
