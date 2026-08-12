"""Runtime settings tests."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from paritygrid.runtime.config import Settings


def test_settings_load_prefixed_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PARITYGRID_BIND_HOST", "localhost")
    monkeypatch.setenv("PARITYGRID_PORT", "8123")
    monkeypatch.setenv("PARITYGRID_LOG_LEVEL", "debug")

    settings = Settings()

    assert settings.bind_host == "localhost"
    assert settings.port == 8123
    assert settings.log_level == "debug"


@pytest.mark.parametrize("port", [0, 65536])
def test_settings_reject_invalid_ports(port: int) -> None:
    with pytest.raises(ValidationError):
        Settings(port=port)


def test_settings_reject_non_loopback_bind_address() -> None:
    with pytest.raises(ValidationError):
        Settings(bind_host="0.0.0.0")  # type: ignore[arg-type]
