"""Path confinement, content safety, and failed-generation tests (Phase 19)."""

# pyright: reportPrivateUsage=false

import asyncio
import os
import re
from pathlib import Path

import pytest

from paritygrid.demo.scenario_runner import (
    MANIFEST_FILENAME,
    ScenarioPathError,
    open_scenario_root,
    run_canonical_scenario,
)
from paritygrid.demo.scenarios import FAST_PROFILE

_IS_WINDOWS = os.name == "nt"


def _rejects(root: Path) -> None:
    with pytest.raises(ScenarioPathError):
        open_scenario_root(root)


class TestRootConfinement:
    def test_relative_roots_are_rejected(self, tmp_path: Path) -> None:
        _rejects(Path("relative/root"))

    def test_filesystem_root_is_rejected(self) -> None:
        _rejects(Path(Path.cwd().anchor))

    def test_user_home_is_rejected(self) -> None:
        _rejects(Path.home())

    def test_broad_home_folders_are_rejected(self) -> None:
        home = Path.home()
        for name in ("Desktop", "Documents", "Downloads"):
            candidate = home / name
            if candidate.exists():
                _rejects(candidate)

    def test_working_directory_and_ancestors_are_rejected(self) -> None:
        cwd = Path.cwd().resolve()
        _rejects(cwd)
        for ancestor in cwd.parents:
            _rejects(ancestor)

    def test_traversing_components_are_rejected(self, tmp_path: Path) -> None:
        root = open_scenario_root(tmp_path / "scenario")
        for relative in ("../escape.txt", "a/../b.txt", "..", "a/.."):
            with pytest.raises(ScenarioPathError):
                root.child(relative)

    def test_absolute_and_backslash_children_are_rejected(self, tmp_path: Path) -> None:
        root = open_scenario_root(tmp_path / "scenario")
        with pytest.raises(ScenarioPathError):
            root.child("C:/elsewhere.txt")
        with pytest.raises(ScenarioPathError):
            root.child("fixtures\\canonical-source.csv")

    def test_windows_alternate_data_streams_are_rejected(self, tmp_path: Path) -> None:
        root = open_scenario_root(tmp_path / "scenario")
        with pytest.raises(ScenarioPathError):
            root.child("fixtures/record.txt:stream")

    def test_reserved_and_malformed_names_are_rejected(self, tmp_path: Path) -> None:
        root = open_scenario_root(tmp_path / "scenario")
        for name in ("CON.txt", "NUL", "com1.log", "trailing.", "trailing ", "-leading"):
            with pytest.raises(ScenarioPathError):
                root.child(f"fixtures/{name}")

    def test_safe_leading_underscore_component_is_accepted(self, tmp_path: Path) -> None:
        root = tmp_path / "_temp" / "scenario"

        assert open_scenario_root(root).path == root.resolve()

    def test_already_published_roots_are_rejected(self, tmp_path: Path) -> None:
        root = tmp_path / "scenario"
        open_scenario_root(root)
        (root / MANIFEST_FILENAME).write_bytes(b"{}")
        _rejects(root)

    def test_symlink_escape_is_rejected_where_supported(self, tmp_path: Path) -> None:
        link = tmp_path / "link"
        try:
            os.symlink(tmp_path, link, target_is_directory=True)
        except NotImplementedError, OSError:
            pytest.skip("symbolic links are not supported in this environment")
        # A root beneath the symlink must be rejected; a fresh unrelated tree
        # must still be accepted so the rejection is provably about escape.
        _rejects(link / "inside")
        open_scenario_root(tmp_path / "unrelated" / "deep")

    def test_junction_escape_is_rejected_where_supported(self, tmp_path: Path) -> None:
        if not _IS_WINDOWS:
            pytest.skip("junctions are Windows-only")
        import subprocess

        junction = tmp_path / "junction"
        result = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(junction), str(tmp_path)],
            capture_output=True,
        )
        if result.returncode != 0:
            pytest.skip("junctions are not supported in this environment")
        _rejects(junction / "inside")

    def test_child_junction_swap_is_rejected_where_supported(self, tmp_path: Path) -> None:
        if not _IS_WINDOWS:
            pytest.skip("junctions are Windows-only")
        import subprocess

        root = open_scenario_root(tmp_path / "scenario")
        fixtures = root.path / "fixtures"
        fixtures.rmdir()
        outside = tmp_path / "outside"
        outside.mkdir()
        result = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(fixtures), str(outside)],
            capture_output=True,
        )
        if result.returncode != 0:
            pytest.skip("junctions are not supported in this environment")
        try:
            with pytest.raises(ScenarioPathError, match=r"junction|outside"):
                root.fixture_path("canonical-source.csv")
        finally:
            os.rmdir(fixtures)


class TestSentinelPreservation:
    def test_nonempty_root_is_rejected_without_touching_a_sentinel(self, tmp_path: Path) -> None:
        root = tmp_path / "scenario"
        root.mkdir()
        sentinel = root / "sentinel.txt"
        sentinel.write_text("owned by caller", encoding="ascii")

        _rejects(root)

        assert sentinel.read_text(encoding="ascii") == "owned by caller"
        assert sorted(path.name for path in root.iterdir()) == ["sentinel.txt"]

    def test_preexisting_owned_fixture_path_is_never_overwritten(self, tmp_path: Path) -> None:
        root = tmp_path / "scenario"
        fixtures = root / "fixtures"
        fixtures.mkdir(parents=True)
        owned = fixtures / "canonical-source.csv"
        owned.write_text("caller content", encoding="ascii")

        _rejects(root)

        assert owned.read_text(encoding="ascii") == "caller content"

    def test_generation_never_touches_files_outside_the_root(self, tmp_path: Path) -> None:
        outer = tmp_path / "outer"
        outer.mkdir()
        sentinel = outer / "sentinel.txt"
        sentinel.write_text("unchanged", encoding="ascii")
        before = sentinel.read_bytes()

        async def scenario() -> None:
            await run_canonical_scenario(FAST_PROFILE, outer / "scenario" / "fast")

        asyncio.run(scenario())
        assert sentinel.read_bytes() == before
        assert sorted(path.name for path in outer.iterdir()) == ["scenario", "sentinel.txt"]


class TestFailedGeneration:
    def test_failed_generation_publishes_no_manifest(self, tmp_path: Path) -> None:
        from paritygrid.demo import scenario_runner

        def broken(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("injected failure")

        original = scenario_runner._load_and_observe_target
        scenario_runner._load_and_observe_target = broken  # type: ignore[assignment]
        try:

            async def scenario() -> None:
                await run_canonical_scenario(FAST_PROFILE, tmp_path / "s" / "fast")

            with pytest.raises(RuntimeError, match="injected failure"):
                asyncio.run(scenario())
        finally:
            scenario_runner._load_and_observe_target = original  # type: ignore[assignment]
        root = tmp_path / "s" / "fast"
        assert not (root / MANIFEST_FILENAME).exists()
        assert not (root / f"{MANIFEST_FILENAME}.partial").exists()


class TestFailedPublication:
    def test_failure_before_replace_leaves_no_final_manifest(self, tmp_path: Path) -> None:
        from paritygrid.demo import scenario_runner

        real_replace = scenario_runner.os.replace

        def failing_replace(*arguments: object) -> None:
            raise RuntimeError("interrupted before replace")

        scenario_runner.os.replace = failing_replace  # type: ignore[assignment]
        try:

            async def scenario() -> None:
                await run_canonical_scenario(FAST_PROFILE, tmp_path / "p" / "fast")

            with pytest.raises(RuntimeError, match="interrupted before replace"):
                asyncio.run(scenario())
        finally:
            scenario_runner.os.replace = real_replace  # type: ignore[assignment]
        root = tmp_path / "p" / "fast"
        # An interrupted publication must never present a partially written
        # file as the complete manifest.
        assert (root / MANIFEST_FILENAME).exists() is False


class TestContentSafety:
    _FORBIDDEN = (
        re.compile(rb"-----BEGIN"),
        re.compile(rb"(?i)password\s*="),
        re.compile(rb"(?i)api[_-]?key\s*="),
        re.compile(rb"(?i)authorization:\s*bearer"),
        re.compile(
            rb"[A-Za-z]:\\\\",
        ),
        re.compile(rb"/home/"),
        re.compile(rb"/Users/"),
        re.compile(rb"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b"),
    )
    _TIMESTAMP = re.compile(rb"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}")

    def test_manifest_carries_no_timestamps_paths_or_run_idsentities(self, tmp_path: Path) -> None:
        async def scenario() -> None:
            await run_canonical_scenario(FAST_PROFILE, tmp_path / "s" / "fast")

        asyncio.run(scenario())
        payload = (tmp_path / "s" / "fast" / MANIFEST_FILENAME).read_bytes()
        assert self._TIMESTAMP.search(payload) is None
        for pattern in self._FORBIDDEN:
            assert pattern.search(payload) is None, pattern
        # The dynamic loopback ports and host names never enter the document.
        assert b"127.0.0.1" not in payload
        assert b"localhost" not in payload
        assert b"base_url" not in payload

    def test_generated_fixtures_are_public_safe(self, tmp_path: Path) -> None:
        async def scenario() -> None:
            await run_canonical_scenario(FAST_PROFILE, tmp_path / "s" / "fast")

        asyncio.run(scenario())
        fixtures = tmp_path / "s" / "fast" / "fixtures"
        for fixture in fixtures.iterdir():
            payload = fixture.read_bytes()
            for pattern in self._FORBIDDEN:
                assert pattern.search(payload) is None, (fixture.name, pattern)
