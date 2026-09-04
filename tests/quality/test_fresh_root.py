"""Safety tests for disposable Phase 21 verification roots."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from paritygrid.quality.fresh_root import FreshRootError, claim_fresh_root


def test_claim_requires_an_absolute_empty_root(tmp_path: Path) -> None:
    with pytest.raises(FreshRootError, match="absolute"):
        claim_fresh_root(Path("relative"), prefix="paritygrid-test-")

    sentinel = tmp_path / "sentinel.txt"
    sentinel.write_text("preserve", encoding="utf-8")
    with pytest.raises(FreshRootError, match="empty"):
        claim_fresh_root(tmp_path, prefix="paritygrid-test-")
    assert sentinel.read_text(encoding="utf-8") == "preserve"


def test_cleanup_removes_only_the_exclusive_child(tmp_path: Path) -> None:
    claim = claim_fresh_root(tmp_path, prefix="paritygrid-test-")
    (claim.work / "owned.txt").write_text("owned", encoding="utf-8")

    claim.cleanup()

    assert tmp_path.is_dir()
    assert list(tmp_path.iterdir()) == []


def test_cleanup_refuses_a_replaced_link_and_preserves_target(tmp_path: Path) -> None:
    claim = claim_fresh_root(tmp_path, prefix="paritygrid-test-")
    original = tmp_path.parent / f"{tmp_path.name}-original"
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    claim.work.rename(original)
    outside.mkdir()
    sentinel = outside / "sentinel.txt"
    sentinel.write_text("preserve", encoding="utf-8")
    try:
        os.symlink(outside, claim.work, target_is_directory=True)
    except OSError:
        original.rename(claim.work)
        claim.cleanup()
        shutil.rmtree(outside, ignore_errors=True)
        pytest.skip("directory symlinks are unavailable")
    try:
        with pytest.raises(FreshRootError, match="became a link"):
            claim.cleanup()
        assert sentinel.read_text(encoding="utf-8") == "preserve"
    finally:
        claim.work.unlink()
        shutil.rmtree(original, ignore_errors=True)
        shutil.rmtree(outside, ignore_errors=True)
