"""Problem Details contract, mapping, and handlers for the HTTP boundary."""

from paritygrid.api.errors.handlers import (
    problem_response,
    register_exception_handlers,
)
from paritygrid.api.errors.mapping import translate_error
from paritygrid.api.errors.problems import (
    PROBLEM_CONTENT_TYPE,
    PROBLEM_CONTRACT_VERSION,
    PROBLEM_TYPE_BASE,
    FieldError,
    ProblemError,
)

__all__ = [
    "PROBLEM_CONTENT_TYPE",
    "PROBLEM_CONTRACT_VERSION",
    "PROBLEM_TYPE_BASE",
    "FieldError",
    "ProblemError",
    "problem_response",
    "register_exception_handlers",
    "translate_error",
]
