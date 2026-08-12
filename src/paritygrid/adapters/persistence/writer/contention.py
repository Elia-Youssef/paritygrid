"""Narrow SQLite contention classification for transactional retries."""

import sqlite3

from sqlalchemy.exc import OperationalError

_SQLITE_PRIMARY_CODE_MASK = 0xFF
_CONTENTION_CODES = frozenset({sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED})


def is_sqlite_contention(error: OperationalError) -> bool:
    """Return whether an operational failure is SQLite BUSY or LOCKED."""
    original = error.orig
    code = getattr(original, "sqlite_errorcode", None)
    return type(code) is int and code & _SQLITE_PRIMARY_CODE_MASK in _CONTENTION_CODES


__all__ = ["is_sqlite_contention"]
