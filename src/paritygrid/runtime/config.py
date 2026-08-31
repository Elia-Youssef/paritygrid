"""Validated immutable runtime settings."""

from pathlib import Path
from typing import Literal, Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_MAX_BODY_BYTES_CEILING = 64 * 1024 * 1024
_MAX_ARTIFACT_CHUNK_BYTES = 8 * 1024 * 1024
_NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]*$"


class Settings(BaseSettings):
    """Settings loaded from the environment at the runtime boundary."""

    model_config = SettingsConfigDict(
        env_prefix="PARITYGRID_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    bind_host: Literal["127.0.0.1", "::1", "localhost"] = "127.0.0.1"
    port: int = Field(default=8000, ge=1, le=65535)
    log_level: Literal["critical", "error", "warning", "info", "debug", "trace"] = "info"
    smoke_timeout_seconds: float = Field(default=5.0, gt=0, le=30)

    data_root: Path = Path("./data")
    database_filename: str = Field(default="paritygrid.db", pattern=_NAME_PATTERN, max_length=128)
    artifact_root_name: str = Field(default="artifacts", pattern=_NAME_PATTERN, max_length=128)

    max_request_body_bytes: int = Field(default=1_048_576, ge=1, le=_MAX_BODY_BYTES_CEILING)
    max_json_depth: int = Field(default=64, ge=1, le=512)
    request_timeout_seconds: float = Field(default=30.0, ge=0.1, le=300.0)
    max_concurrent_requests: int = Field(default=64, ge=1, le=10_000)
    idempotency_lease_seconds: float = Field(default=60.0, gt=0.0, le=86_400.0)
    artifact_chunk_bytes: int = Field(default=1_048_576, ge=1, le=_MAX_ARTIFACT_CHUNK_BYTES)
    writer_queue_capacity: int = Field(default=64, ge=1, le=10_000)

    frontend_dist: Path | None = None
    stream_heartbeat_seconds: float = Field(default=15.0, ge=0.1, le=300.0)
    stream_poll_seconds: float = Field(default=0.25, ge=0.05, le=30.0)
    telemetry_queue_capacity: int = Field(default=256, ge=1, le=1_024)
    telemetry_max_subscribers_per_run: int = Field(default=16, ge=1, le=128)
    telemetry_send_timeout_seconds: float = Field(default=10.0, ge=0.1, le=60.0)
    telemetry_poll_seconds: float = Field(default=0.25, ge=0.05, le=30.0)

    @model_validator(mode="after")
    def _validate_lease_covers_request_budget(self) -> Self:
        # The idempotency lease must outlive the request timeout so a live
        # request can never have its reservation reclaimed by a retry.
        if self.idempotency_lease_seconds <= self.request_timeout_seconds:
            raise ValueError("idempotency_lease_seconds must exceed request_timeout_seconds")
        return self

    @property
    def database_path(self) -> Path:
        """Resolve the authoritative database file under the data root."""
        return (self.data_root / self.database_filename).resolve()

    @property
    def artifact_root_path(self) -> Path:
        """Resolve the authorized artifact root under the data root."""
        return (self.data_root / self.artifact_root_name).resolve()
