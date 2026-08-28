"""FastAPI exception handlers rendering the Problem Details contract."""

from collections.abc import Callable
from typing import TYPE_CHECKING, cast

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from paritygrid.api.correlation import correlation_from_scope
from paritygrid.api.errors.mapping import translate_error
from paritygrid.api.errors.problems import (
    PROBLEM_CONTENT_TYPE,
    FieldError,
    ProblemError,
)
from paritygrid.application.ports.artifact_streaming import ArtifactStreamError
from paritygrid.application.ports.artifacts import (
    ArtifactManifestError,
    ArtifactPathError,
)
from paritygrid.application.ports.configuration import ConfigurationRepositoryError
from paritygrid.application.ports.connectors import ConnectorError
from paritygrid.application.ports.consistency import ConsistencyRepositoryError
from paritygrid.application.ports.execution import ExecutionRepositoryError
from paritygrid.application.ports.operations import OperationalStoreUnavailableError
from paritygrid.application.ports.writer import (
    PersistenceContentionError,
    WriterError,
)
from paritygrid.application.services.errors import OperationalServiceError
from paritygrid.domain.errors import DomainError

if TYPE_CHECKING:
    from starlette.types import ExceptionHandler

_HTTP_EXCEPTION_SLUGS = {
    404: ("not-found", "Resource does not exist"),
    405: ("method-not-allowed", "Method is not allowed"),
    415: ("unsupported-media-type", "Media type is not supported"),
}

# Every accepted application failure base that must reach a Problem Details
# response inside the routing middleware rather than the outer error net.
_TRANSLATED_ERROR_BASES: tuple[type[Exception], ...] = (
    OperationalServiceError,
    ConfigurationRepositoryError,
    ExecutionRepositoryError,
    ConsistencyRepositoryError,
    ArtifactManifestError,
    ArtifactStreamError,
    ArtifactPathError,
    WriterError,
    PersistenceContentionError,
    ConnectorError,
    DomainError,
    OperationalStoreUnavailableError,
)


async def translated_handler(request: Request, error: Exception) -> JSONResponse:
    """Render one translated application failure as Problem Details."""
    return problem_response(
        translate_error(error),
        instance=_instance(request),
        correlation_id=correlation_from_scope(request.scope),
    )


async def validation_handler(request: Request, error: RequestValidationError) -> JSONResponse:
    """Render request validation failures with bounded field evidence."""
    problem = ProblemError(
        type_slug="validation",
        title="Request validation failed",
        status=422,
        detail="one or more request fields are invalid",
        errors=_field_errors(error),
    )
    return problem_response(
        problem,
        instance=_instance(request),
        correlation_id=correlation_from_scope(request.scope),
    )


async def http_handler(request: Request, error: StarletteHTTPException) -> JSONResponse:
    """Render framework HTTP exceptions as Problem Details."""
    slug, title = _HTTP_EXCEPTION_SLUGS.get(error.status_code, ("http-error", "Request failed"))
    problem = ProblemError(
        type_slug=slug,
        title=title,
        status=error.status_code,
        detail=str(error.detail)[:512],
    )
    return problem_response(
        problem,
        instance=_instance(request),
        correlation_id=correlation_from_scope(request.scope),
    )


def register_exception_handlers(application: FastAPI) -> None:
    """Attach the closed Problem Details handlers to one application."""
    application.add_exception_handler(ProblemError, translated_handler)
    application.add_exception_handler(
        RequestValidationError, cast("ExceptionHandler", validation_handler)
    )
    application.add_exception_handler(
        StarletteHTTPException, cast("ExceptionHandler", http_handler)
    )
    application.add_exception_handler(Exception, translated_handler)
    for error_type in _TRANSLATED_ERROR_BASES:
        application.add_exception_handler(error_type, translated_handler)


def problem_response(problem: ProblemError, *, instance: str, correlation_id: str) -> JSONResponse:
    """Render one Problem Details JSON response."""
    return JSONResponse(
        status_code=problem.status,
        content=problem.to_document(instance=instance, correlation_id=correlation_id),
        media_type=PROBLEM_CONTENT_TYPE,
    )


def _instance(request: Request) -> str:
    path: object = request.scope.get("path", "")
    return path if isinstance(path, str) else ""


def _field_errors(error: RequestValidationError) -> tuple[FieldError, ...]:
    bounded: list[FieldError] = []
    for item in error.errors()[:10]:
        raw_location = item.get("loc", ())
        parts = [str(part) for part in raw_location if part != "body"]
        location = ".".join(parts)[:128] or "body"
        message = str(item.get("msg", "value is invalid"))[:128]
        bounded.append(FieldError(location, message))
    return tuple(bounded)


def problem_renderer_for_scope(
    scope: dict[str, object],
) -> Callable[[ProblemError], JSONResponse]:
    """Bind a renderer to one ASGI scope for middleware-level problems."""
    instance = scope.get("path", "")
    correlation_id = correlation_from_scope(scope)
    return lambda problem: problem_response(
        problem,
        instance=instance if isinstance(instance, str) else "",
        correlation_id=correlation_id,
    )
