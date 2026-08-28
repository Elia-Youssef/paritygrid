"""Shared versioned transport schema building blocks."""

from typing import Literal

from pydantic import BaseModel, ConfigDict

TRANSPORT_SCHEMA_VERSION = 1


class TransportModel(BaseModel):
    """Base model for every versioned transport schema."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class PageMeta(TransportModel):
    """Pagination evidence attached to every collection response."""

    schema_version: Literal[1] = TRANSPORT_SCHEMA_VERSION
    limit: int
    next_cursor: str | None


class FieldErrorBody(TransportModel):
    """One bounded request-field validation fact."""

    field: str
    message: str
