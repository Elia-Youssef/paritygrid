"""Application factory tests."""

from paritygrid.api.app import create_app


def test_factory_creates_independent_applications_without_starting_resources() -> None:
    first = create_app(service_name="First", version="1.0")
    second = create_app(service_name="Second", version="2.0")

    assert first is not second
    assert first.title == "First"
    assert first.version == "1.0"
    assert second.title == "Second"
    assert second.version == "2.0"
    assert not hasattr(first.state, "settings")
