"""Runtime composition tests."""

from paritygrid.runtime.composition import create_runtime_app
from paritygrid.runtime.config import Settings


def test_runtime_composition_attaches_validated_settings() -> None:
    settings = Settings(port=8765)

    application = create_runtime_app(settings)

    assert application.state.settings is settings
