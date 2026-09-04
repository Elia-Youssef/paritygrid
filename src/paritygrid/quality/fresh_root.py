"""Fail-closed ownership for disposable Phase 21 verification roots."""

from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path


class FreshRootError(ValueError):
    """A caller-provided verification root is not safe to claim or clean."""


def _is_link(path: Path) -> bool:
    return path.is_symlink() or path.is_junction()


def _reject_broad_root(path: Path) -> None:
    anchor = Path(path.anchor).resolve()
    home = Path.home().resolve()
    current = Path.cwd().resolve()
    if path in {anchor, home, current} or path in current.parents:
        raise FreshRootError("the verification root is too broad")


@dataclass(frozen=True, slots=True)
class ClaimedFreshRoot:
    """One exclusively-created disposable child beneath an empty caller root."""

    parent: Path
    work: Path
    prefix: str

    def cleanup(self) -> None:
        """Remove only the exact child created by :func:`claim_fresh_root`."""
        if self.work.parent != self.parent or not self.work.name.startswith(self.prefix):
            raise FreshRootError("the claimed verification root identity changed")
        if not self.work.exists():
            return
        if _is_link(self.work) or self.work.resolve() != self.work:
            raise FreshRootError("the claimed verification root became a link")
        shutil.rmtree(self.work)
        if self.work.exists():
            raise FreshRootError("the claimed verification root could not be removed")


def claim_fresh_root(root: Path, *, prefix: str) -> ClaimedFreshRoot:
    """Claim a unique work directory beneath an absolute, empty, non-link root.

    The caller-owned directory itself is never deleted.  Existing content is
    refused before any write, and cleanup is limited to the unpredictable
    direct child created exclusively here.
    """
    if not root.is_absolute():
        raise FreshRootError("the verification root must be an absolute path")
    if not prefix or any(
        character not in "abcdefghijklmnopqrstuvwxyz0123456789-" for character in prefix
    ):
        raise FreshRootError("the verification root prefix is invalid")
    cleaned = Path(os.path.normpath(str(root)))
    if not cleaned.exists() or not cleaned.is_dir():
        raise FreshRootError("the verification root must be an existing directory")
    for component in (cleaned, *cleaned.parents):
        if component == Path(cleaned.anchor):
            break
        if component.exists() and _is_link(component):
            raise FreshRootError("the verification root traverses a link")
    resolved = cleaned.resolve(strict=True)
    _reject_broad_root(resolved)
    if next(resolved.iterdir(), None) is not None:
        raise FreshRootError("the verification root must be empty")
    work = Path(tempfile.mkdtemp(prefix=prefix, dir=resolved)).resolve(strict=True)
    if work.parent != resolved or _is_link(work):
        raise FreshRootError("the verification work root escaped its parent")
    return ClaimedFreshRoot(parent=resolved, work=work, prefix=prefix)


__all__ = ["ClaimedFreshRoot", "FreshRootError", "claim_fresh_root"]
