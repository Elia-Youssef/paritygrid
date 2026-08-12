"""Dependency rules for configuration repository contracts and adapters."""

import ast
from pathlib import Path

from paritygrid.adapters.persistence.repositories import (
    SqlAlchemyConnectorRepository,
    SqlAlchemyPipelineRepository,
)
from paritygrid.application.ports import ConnectorRepository, PipelineRepository

ROOT = Path(__file__).parents[2]


def test_application_contract_has_no_persistence_or_sqlalchemy_imports() -> None:
    source = (ROOT / "src/paritygrid/application/ports/configuration.py").read_text(
        encoding="utf-8"
    )
    imports = {
        node.module or ""
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.ImportFrom)
    }
    assert not any(name.startswith("sqlalchemy") for name in imports)
    assert not any(name.startswith("paritygrid.adapters") for name in imports)


def test_adapters_structurally_satisfy_public_ports() -> None:
    assert PipelineRepository in SqlAlchemyPipelineRepository.__bases__
    assert ConnectorRepository in SqlAlchemyConnectorRepository.__bases__
