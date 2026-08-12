"""Typed failures raised by persistence adapters."""


class PersistenceError(Exception):
    """Base class for failures owned by the persistence boundary."""


class SQLiteConfigurationError(PersistenceError):
    """Raised when SQLite configuration cannot produce a safe file database."""


class SQLiteCapabilityError(PersistenceError):
    """Raised when the SQLite runtime cannot meet required durability guarantees."""
