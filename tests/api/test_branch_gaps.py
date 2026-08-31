"""Branch-gap tests for the Problem Details mapping and bounded helpers."""

import pytest
from starlette.types import Message, Receive, Scope, Send

from paritygrid.api.correlation import correlation_from_scope, validate_correlation_id
from paritygrid.api.errors.mapping import translate_error
from paritygrid.api.errors.problems import FieldError, ProblemError
from paritygrid.api.middleware.security_headers import SecurityHeadersMiddleware
from paritygrid.application.ports.artifact_streaming import (
    ArtifactStreamStorageError,
    ArtifactStreamStorageUnavailableError,
)
from paritygrid.application.ports.artifacts import (
    ArtifactIntegrityError,
    ArtifactManifestInvalidError,
    ArtifactPathError,
)
from paritygrid.application.ports.configuration import (
    CorruptRepositoryRecordError,
    DuplicateRecordError,
    InvalidRepositoryRequestError,
    RecordNotFoundError,
    RecordStateConflictError,
    StaleConnectorRevisionError,
    StaleRowVersionError,
    UnsafeConnectorConfigurationError,
)
from paritygrid.application.ports.consistency import (
    ConsistencyStorageError,
    ConsistencyStorageUnavailableError,
)
from paritygrid.application.ports.execution import (
    ExecutionDuplicateError,
    ExecutionLeaseExpiredError,
    ExecutionLeaseMismatchError,
    ExecutionRecordNotFoundError,
    ExecutionStorageError,
    ExecutionStorageUnavailableError,
)
from paritygrid.application.ports.operations import OperationalStoreUnavailableError
from paritygrid.application.ports.writer import (
    WriterAdmissionTimeoutError,
    WriterClosedError,
    WriterInvalidRequestError,
)
from paritygrid.application.services.errors import (
    OperationalConflictError,
    OperationalUnavailableError,
)
from paritygrid.domain.errors import (
    InvalidTransitionError,
    StaleRepairPlanError,
)
from paritygrid.domain.models import StateFingerprint


def test_repository_failure_families_map_to_bounded_problems() -> None:
    cases = [
        (DuplicateRecordError("dup"), 409, "duplicate_record"),
        (RecordNotFoundError("missing"), 404, "record_not_found"),
        (RecordStateConflictError("state"), 409, "state_conflict"),
        (StaleRowVersionError("stale"), 409, "stale_row_version"),
        (StaleConnectorRevisionError("rev"), 409, "connector_revision_conflict"),
        (InvalidRepositoryRequestError("bad"), 422, "validation"),
        (UnsafeConnectorConfigurationError("unsafe"), 422, "unsafe_connector_configuration"),
        (CorruptRepositoryRecordError("corrupt"), 500, "data_integrity"),
        (ExecutionDuplicateError("dup run"), 409, "duplicate_record"),
        (ExecutionRecordNotFoundError("no run"), 404, "run_not_found"),
        (ExecutionLeaseExpiredError("lease"), 409, "stale_row_version"),
        (ExecutionLeaseMismatchError("lease"), 409, "stale_row_version"),
        (ExecutionStorageError("down"), 503, "unavailable"),
        (ExecutionStorageUnavailableError("down"), 503, "unavailable"),
        (ConsistencyStorageError("down"), 503, "unavailable"),
        (ConsistencyStorageUnavailableError("down"), 503, "unavailable"),
        (ArtifactStreamStorageError("art"), 503, "unavailable"),
        (ArtifactStreamStorageUnavailableError("art"), 503, "unavailable"),
        (ArtifactIntegrityError("art"), 410, "artifact_integrity"),
        (ArtifactManifestInvalidError("art"), 422, "validation"),
        (ArtifactPathError("path"), 503, "unavailable"),
        (OperationalStoreUnavailableError("store"), 503, "unavailable"),
        (WriterAdmissionTimeoutError("busy"), 503, "writer_busy"),
        (WriterClosedError("closed"), 503, "writer_busy"),
        (WriterInvalidRequestError("bad"), 503, "writer_busy"),
        (OperationalUnavailableError("offline"), 503, "unavailable"),
        (OperationalConflictError("nope", code="custom_conflict"), 409, "custom_conflict"),
        (
            InvalidTransitionError(lifecycle="run", current_state="queued", target_state="failed"),
            409,
            "invalid_transition",
        ),
        (
            StaleRepairPlanError(
                expected=StateFingerprint("a" * 64), actual=StateFingerprint("b" * 64)
            ),
            422,
            "domain_rule",
        ),
        (KeyError("unexpected"), 500, "internal_error"),
    ]
    for error, status, code in cases:
        problem = translate_error(error)
        assert problem.status == status, (error, status)
        assert problem.code == code, (error, code)


def test_problem_rejects_invalid_construction() -> None:
    with pytest.raises(ValueError, match="status"):
        ProblemError(type_slug="t", title="t", status=204)
    with pytest.raises(ValueError, match="code"):
        ProblemError(type_slug="t", title="t", status=409, code="Not a Code!")


def test_problem_document_includes_bounded_field_errors() -> None:
    problem = ProblemError(
        type_slug="validation",
        title="Request validation failed",
        status=422,
        errors=tuple(FieldError(f"field_{index}", "value is invalid") for index in range(14)),
    )
    document = problem.to_document(instance="/x", correlation_id="c")
    assert len(document["errors"]) == 10  # type: ignore[arg-type]


def test_problem_detail_is_truncated_to_the_bound() -> None:
    problem = ProblemError(type_slug="validation", title="t", status=422, detail="y" * 900)
    assert len(problem.detail) == 512
    assert problem.detail.endswith("...")


def test_validate_correlation_id_bounds_and_format() -> None:
    assert validate_correlation_id("a") == "a"
    assert validate_correlation_id("A-9._:" * 9 + "x") is not None
    with pytest.raises(ValueError, match="correlation"):
        validate_correlation_id("")
    with pytest.raises(ValueError, match="correlation"):
        validate_correlation_id("x" * 97)
    with pytest.raises(ValueError, match="correlation"):
        validate_correlation_id("bad id")


def test_correlation_from_scope_falls_back_to_empty() -> None:
    assert correlation_from_scope({"type": "http"}) == ""
    assert correlation_from_scope({"type": "http", "state": {"other": 1}}) == ""
    assert correlation_from_scope({"type": "http", "state": {"correlation_id": "c1"}}) == "c1"


@pytest.mark.anyio
async def test_security_headers_middleware_passes_through_lifespan_messages() -> None:
    seen: list[str] = []

    async def inner_app(scope: Scope, receive: Receive, send: Send) -> None:
        seen.append(scope["type"])

    middleware = SecurityHeadersMiddleware(inner_app)

    async def no_receive() -> Message:
        raise AssertionError("lifespan must not read requests")

    async def no_send(message: Message) -> None:
        del message

    await middleware({"type": "lifespan"}, no_receive, no_send)
    assert seen == ["lifespan"]
