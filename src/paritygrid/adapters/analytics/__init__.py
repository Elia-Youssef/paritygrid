"""Rebuildable analytical database adapters."""

from paritygrid.adapters.analytics.duckdb import DuckDBLifecycleCoordinator
from paritygrid.adapters.analytics.views import (
    DuckDBAnalyticalViewRegistry,
    DuckDBViewColumnDefinition,
    DuckDBViewDefinition,
)

__all__ = [
    "DuckDBAnalyticalViewRegistry",
    "DuckDBLifecycleCoordinator",
    "DuckDBViewColumnDefinition",
    "DuckDBViewDefinition",
]
