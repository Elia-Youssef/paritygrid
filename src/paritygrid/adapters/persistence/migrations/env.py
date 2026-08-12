"""Alembic environment for caller-owned operational database connections."""

from alembic import context
from sqlalchemy.engine import Connection

from paritygrid.adapters.persistence.errors import MigrationConfigurationError


def _supplied_connection() -> Connection:
    connection = context.config.attributes.get("connection")
    if not isinstance(connection, Connection):
        raise MigrationConfigurationError(
            "Alembic migrations require a caller-owned SQLAlchemy Connection."
        )
    return connection


def run_migrations_online() -> None:
    """Run migrations only through the validated in-process connection boundary."""
    connection = _supplied_connection()
    context.configure(
        connection=connection,
        target_metadata=None,
        transactional_ddl=True,
        transaction_per_migration=False,
    )
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    raise MigrationConfigurationError("Offline SQL migration generation is not supported.")

run_migrations_online()
