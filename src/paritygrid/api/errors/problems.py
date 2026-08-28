"""Versioned Problem Details contract for the HTTP boundary.

Every error response is an ``application/problem+json`` document with stable
problem types, bounded detail, the request correlation identity, and a stable
machine code.  Stack traces, filesystem paths, SQL, credentials, raw upstream
payloads, and unbounded field evidence never enter a problem document.
"""

import re
from dataclasses import dataclass, field

PROBLEM_CONTENT_TYPE = "application/problem+json"
PROBLEM_TYPE_BASE = "https://paritygrid.dev/problems"
PROBLEM_CONTRACT_VERSION = 1
MAX_PROBLEM_DETAIL_LENGTH = 512
MAX_PROBLEM_ERRORS = 10
MAX_PROBLEM_FIELD_LENGTH = 128

_CODE_PATTERN = re.compile(r"[a-z][a-z0-9_]*", flags=re.ASCII)


@dataclass(frozen=True, slots=True)
class FieldError:
    """One bounded request-field validation fact."""

    field: str
    message: str


@dataclass(frozen=True, slots=True)
class ProblemError(Exception):
    """A transport error rendered as one Problem Details document."""

    type_slug: str
    title: str
    status: int
    detail: str = ""
    code: str = ""
    errors: tuple[FieldError, ...] = field(default=())

    def __post_init__(self) -> None:
        if not 400 <= self.status <= 599:
            raise ValueError("problem status must be an HTTP error status")
        object.__setattr__(self, "detail", _bounded(self.detail))
        code = self.code or self.type_slug.replace("-", "_")
        if _CODE_PATTERN.fullmatch(code) is None:
            raise ValueError("problem code must be stable lowercase snake_case")
        object.__setattr__(self, "code", code)
        bounded_errors = tuple(
            FieldError(_bounded(error.field), _bounded(error.message))
            for error in self.errors[:MAX_PROBLEM_ERRORS]
        )
        object.__setattr__(self, "errors", bounded_errors)

    @property
    def type_uri(self) -> str:
        return f"{PROBLEM_TYPE_BASE}/{self.type_slug}"

    def to_document(self, *, instance: str, correlation_id: str) -> dict[str, object]:
        """Render the bounded wire document for this problem."""
        document: dict[str, object] = {
            "type": self.type_uri,
            "title": self.title,
            "status": self.status,
            "detail": self.detail,
            "instance": instance,
            "correlation_id": correlation_id,
            "code": self.code,
        }
        if self.errors:
            document["errors"] = [
                {"field": error.field, "message": error.message} for error in self.errors
            ]
        return document


def validation_problem(detail: str, *, errors: tuple[FieldError, ...] = ()) -> ProblemError:
    return ProblemError(
        type_slug="validation",
        title="Request validation failed",
        status=422,
        detail=detail,
        errors=errors,
    )


def invalid_input_problem(
    detail: str, *, code: str, errors: tuple[FieldError, ...] = ()
) -> ProblemError:
    return ProblemError(
        type_slug="invalid-input",
        title="Request input is invalid",
        status=400,
        detail=detail,
        code=code,
        errors=errors,
    )


def not_found_problem(resource: str, identifier: str) -> ProblemError:
    return ProblemError(
        type_slug="not-found",
        title="Resource does not exist",
        status=404,
        detail=f"{resource} {identifier!r} does not exist",
        code=f"{resource.replace(' ', '_')}_not_found",
    )


def conflict_problem(detail: str, *, code: str) -> ProblemError:
    return ProblemError(
        type_slug="conflict",
        title="Request conflicts with durable state",
        status=409,
        detail=detail,
        code=code,
    )


def unavailable_problem(detail: str, *, code: str = "unavailable") -> ProblemError:
    return ProblemError(
        type_slug="unavailable",
        title="Service is temporarily unavailable",
        status=503,
        detail=detail,
        code=code,
    )


def internal_problem() -> ProblemError:
    return ProblemError(
        type_slug="internal",
        title="Internal service error",
        status=500,
        detail="the request could not be completed",
        code="internal_error",
    )


def _bounded(value: str) -> str:
    trimmed = value.strip()
    if len(trimmed) > MAX_PROBLEM_DETAIL_LENGTH:
        return f"{trimmed[: MAX_PROBLEM_DETAIL_LENGTH - 3]}..."
    return trimmed
