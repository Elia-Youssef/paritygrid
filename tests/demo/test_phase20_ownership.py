"""Explicitly owned demo roots and contained-reset tests (Phase 20)."""

# pyright: reportPrivateUsage=false

import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import cast

import pytest

from paritygrid.demo import ownership
from paritygrid.demo.ownership import (
    OWNERSHIP_MARKER_FORMAT,
    OWNERSHIP_MARKER_NAME,
    OWNERSHIP_MARKER_VERSION,
    OWNERSHIP_PROOF_FORMAT,
    OWNERSHIP_PROOF_VERSION,
    SCENARIO_DIRNAME,
    DemoRootError,
    open_or_create_demo_root,
    reset_demo_root,
    resolve_demo_root,
)

_IS_WINDOWS = os.name == "nt"
_SENTINEL_BYTES = b"owned by the caller; must survive every rejected reset"


@pytest.fixture(autouse=True)
def _isolated_ownership_proof_registry(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Keep test proofs outside each test root without polluting system temp."""
    registry_parent = tmp_path / "proof-registry-parent"
    registry_parent.mkdir()
    monkeypatch.setattr(ownership, "tempdir_root", lambda: registry_parent)


def _marker_document(root: Path) -> dict[str, object]:
    document: object = json.loads((root / OWNERSHIP_MARKER_NAME).read_bytes().decode("utf-8"))
    assert isinstance(document, dict)
    return cast("dict[str, object]", document)


def _own(root: Path) -> Path:
    demo_root, created = open_or_create_demo_root(root)
    assert created is True
    return demo_root.path


def _place_sentinel(directory: Path) -> Path:
    sentinel = directory / "sentinel.txt"
    sentinel.write_bytes(_SENTINEL_BYTES)
    return sentinel


def _assert_sentinel_survives(sentinel: Path) -> None:
    assert sentinel.read_bytes() == _SENTINEL_BYTES


class TestCreateAndOpen:
    def test_create_writes_the_exact_marker_schema_and_empty_scenario_dir(
        self, tmp_path: Path
    ) -> None:
        root = tmp_path / "demo"

        demo_root, created = open_or_create_demo_root(root)

        assert created is True
        assert demo_root.path == root.resolve()
        assert demo_root.marker_path == root.resolve() / OWNERSHIP_MARKER_NAME
        assert (root / OWNERSHIP_MARKER_NAME).is_file()
        assert (root / SCENARIO_DIRNAME).is_dir()
        marker = _marker_document(root)
        assert marker == {
            "format": OWNERSHIP_MARKER_FORMAT,
            "version": OWNERSHIP_MARKER_VERSION,
            "scenario_dirname": SCENARIO_DIRNAME,
            "ownership_id": marker["ownership_id"],
        }
        assert isinstance(marker["ownership_id"], str)
        assert re.fullmatch(r"[0-9a-f]{64}", marker["ownership_id"])

    def test_second_open_reports_created_false(self, tmp_path: Path) -> None:
        root = tmp_path / "demo"
        _own(root)

        demo_root, created = open_or_create_demo_root(root)

        assert created is False
        assert demo_root.path == root.resolve()
        assert (root / OWNERSHIP_MARKER_NAME).is_file()

    def test_scenario_directory_must_be_empty_at_creation(self, tmp_path: Path) -> None:
        root = tmp_path / "demo"
        (root / SCENARIO_DIRNAME).mkdir(parents=True)
        occupied = root / SCENARIO_DIRNAME / "unowned.txt"
        occupied.write_bytes(b"pre-existing")

        with pytest.raises(DemoRootError):
            open_or_create_demo_root(root)

        assert occupied.read_bytes() == b"pre-existing"

    def test_unowned_root_content_is_rejected(self, tmp_path: Path) -> None:
        root = tmp_path / "demo"
        root.mkdir()
        stranger = root / "unowned.txt"
        stranger.write_bytes(b"not owned")

        with pytest.raises(DemoRootError, match="unowned content"):
            open_or_create_demo_root(root)

        assert stranger.read_bytes() == b"not owned"

    def test_missing_scenario_directory_is_rejected_on_reopen(self, tmp_path: Path) -> None:
        root = _own(tmp_path / "demo")
        (root / SCENARIO_DIRNAME).rmdir()

        with pytest.raises(DemoRootError, match="scenario directory"):
            open_or_create_demo_root(root)

    def test_reopen_rejects_unexpected_root_content_and_preserves_it(self, tmp_path: Path) -> None:
        root = _own(tmp_path / "demo")
        sentinel = _place_sentinel(root)

        with pytest.raises(DemoRootError, match="unexpected owned-root entries"):
            open_or_create_demo_root(root)

        _assert_sentinel_survives(sentinel)

    def test_static_marker_without_independent_proof_never_authorizes_open_or_reset(
        self, tmp_path: Path
    ) -> None:
        root = tmp_path / "forged"
        scenario = root / SCENARIO_DIRNAME
        scenario.mkdir(parents=True)
        sentinel = _place_sentinel(scenario)
        (root / OWNERSHIP_MARKER_NAME).write_bytes(
            json.dumps(
                {
                    "format": OWNERSHIP_MARKER_FORMAT,
                    "version": OWNERSHIP_MARKER_VERSION,
                    "scenario_dirname": SCENARIO_DIRNAME,
                    "ownership_id": "0" * 64,
                }
            ).encode("utf-8")
        )

        with pytest.raises(DemoRootError, match="independent ownership proof"):
            open_or_create_demo_root(root)
        with pytest.raises(DemoRootError, match="independent ownership proof"):
            reset_demo_root(root)

        _assert_sentinel_survives(sentinel)

    def test_marker_write_failure_restores_a_preexisting_empty_root(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        root = tmp_path / "caller-created-empty-root"
        root.mkdir()

        def fail_marker_write(_marker_path: Path, _ownership_id: str) -> None:
            raise OSError("injected marker write failure")

        monkeypatch.setattr(ownership, "_write_marker_exclusively", fail_marker_write)

        with pytest.raises(OSError, match="injected marker write failure"):
            open_or_create_demo_root(root)

        assert root.is_dir()
        assert list(root.iterdir()) == []

    def test_external_proof_is_path_bound_and_uses_the_exact_schema(self, tmp_path: Path) -> None:
        root = _own(tmp_path / "demo")
        ownership_id = _marker_document(root)["ownership_id"]
        assert isinstance(ownership_id, str)
        proof_path = ownership._ownership_proof_path(ownership_id, create_registry=False)
        document: object = json.loads(proof_path.read_bytes().decode("utf-8"))
        assert document == {
            "format": OWNERSHIP_PROOF_FORMAT,
            "version": OWNERSHIP_PROOF_VERSION,
            "root_path": str(root.resolve()),
            "ownership_id": ownership_id,
        }

        sentinel = _place_sentinel(root / SCENARIO_DIRNAME)
        proof_path.write_bytes(
            json.dumps(
                {
                    "format": OWNERSHIP_PROOF_FORMAT,
                    "version": OWNERSHIP_PROOF_VERSION,
                    "root_path": str(tmp_path / "different-root"),
                    "ownership_id": ownership_id,
                }
            ).encode("utf-8")
        )

        with pytest.raises(DemoRootError, match="does not match the owned root"):
            reset_demo_root(root)

        _assert_sentinel_survives(sentinel)

    def test_open_and_reset_survive_separate_processes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # This test intentionally uses the real per-user registry so separate
        # Python processes exercise the persistent external ownership proof.
        monkeypatch.setattr(ownership, "tempdir_root", lambda: Path(tempfile.gettempdir()))
        parent = Path(tempfile.mkdtemp(prefix="pgdemo-proof-"))
        root = parent / "demo"
        _own(root)
        open_code = (
            "from pathlib import Path\n"
            "from paritygrid.demo.ownership import open_or_create_demo_root\n"
            "demo, created = open_or_create_demo_root(Path(__import__('sys').argv[1]))\n"
            "assert not created and demo.path.is_dir()\n"
        )
        reset_code = (
            "from pathlib import Path\n"
            "from paritygrid.demo.ownership import reset_demo_root\n"
            "reset_demo_root(Path(__import__('sys').argv[1]))\n"
        )
        try:
            subprocess.run([sys.executable, "-c", open_code, str(root)], check=True)
            subprocess.run([sys.executable, "-c", reset_code, str(root)], check=True)
            assert not root.exists()
        finally:
            if root.exists():
                reset_demo_root(root)
            parent.rmdir()


class TestRootRejection:
    def test_empty_paths_are_rejected(self) -> None:
        for candidate in (Path(), Path("")):
            with pytest.raises(DemoRootError, match="non-empty"):
                resolve_demo_root(candidate)

    def test_relative_paths_are_rejected(self) -> None:
        with pytest.raises(DemoRootError, match="absolute"):
            resolve_demo_root(Path("relative") / "demo")

    def test_filesystem_root_is_rejected(self) -> None:
        with pytest.raises(DemoRootError, match="root is never a demo root"):
            resolve_demo_root(Path(Path.cwd().anchor))

    def test_user_home_is_rejected(self) -> None:
        with pytest.raises(DemoRootError):
            resolve_demo_root(Path.home())

    def test_broad_home_folders_are_rejected(self) -> None:
        home = Path.home()
        for name in ("Desktop", "Documents", "Downloads"):
            candidate = home / name
            if candidate.exists():
                with pytest.raises(DemoRootError):
                    resolve_demo_root(candidate)

    def test_working_directory_and_ancestors_are_rejected(self) -> None:
        cwd = Path.cwd().resolve()
        with pytest.raises(DemoRootError):
            resolve_demo_root(cwd)
        for ancestor in cwd.parents:
            with pytest.raises(DemoRootError):
                resolve_demo_root(ancestor)

    def test_alternate_data_stream_components_are_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(DemoRootError, match="separator"):
            resolve_demo_root(tmp_path / "demo:stream")

    def test_reserved_device_names_are_rejected(self, tmp_path: Path) -> None:
        for name in ("CON", "PRN", "NUL", "COM1", "LPT3"):
            with pytest.raises(DemoRootError, match="reserved"):
                resolve_demo_root(tmp_path / name)

    def test_malformed_components_are_rejected(self, tmp_path: Path) -> None:
        for name in ("-leading", "trailing.", "trailing ", "x" * 65):
            with pytest.raises(DemoRootError):
                resolve_demo_root(tmp_path / name)

    def test_windows_short_name_components_are_accepted(self, tmp_path: Path) -> None:
        candidate = tmp_path / "RUNNER~1" / "demo"

        assert resolve_demo_root(candidate) == candidate.resolve(strict=False)

    def test_independent_proof_registry_is_never_a_demo_root(self, tmp_path: Path) -> None:
        registry = tmp_path / "proof-registry-parent" / ownership.OWNERSHIP_PROOF_DIRECTORY_NAME

        with pytest.raises(DemoRootError, match="registry is reserved"):
            resolve_demo_root(registry)


class TestLinkRejection:
    def test_symlinked_root_is_rejected_where_supported(self, tmp_path: Path) -> None:
        target = tmp_path / "real"
        target.mkdir()
        link = tmp_path / "link"
        try:
            os.symlink(target, link, target_is_directory=True)
        except NotImplementedError, OSError:
            pytest.skip("symbolic links are not supported in this environment")

        with pytest.raises(DemoRootError, match="symbolic link or junction"):
            resolve_demo_root(link)

        # A plain sibling directory must stay acceptable, so the rejection is
        # provably about the link and not about the location.
        resolve_demo_root(target)

    def test_root_beneath_a_symlink_is_rejected_where_supported(self, tmp_path: Path) -> None:
        link = tmp_path / "link"
        try:
            os.symlink(tmp_path, link, target_is_directory=True)
        except NotImplementedError, OSError:
            pytest.skip("symbolic links are not supported in this environment")

        with pytest.raises(DemoRootError):
            resolve_demo_root(link / "inside")

    def test_junctioned_root_is_rejected_where_supported(self, tmp_path: Path) -> None:
        if not _IS_WINDOWS:
            pytest.skip("junctions are Windows-only")
        target = tmp_path / "real"
        target.mkdir()
        junction = tmp_path / "junction"
        result = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(junction), str(target)],
            capture_output=True,
        )
        if result.returncode != 0:
            pytest.skip("junctions are not supported in this environment")
        try:
            with pytest.raises(DemoRootError, match="symbolic link or junction"):
                resolve_demo_root(junction)
        finally:
            os.rmdir(junction)

    def test_linked_marker_is_rejected_without_touching_the_scenario(self, tmp_path: Path) -> None:
        root = _own(tmp_path / "demo")
        sentinel = _place_sentinel(root / SCENARIO_DIRNAME)
        marker = root / OWNERSHIP_MARKER_NAME
        marker.unlink()
        outside_marker = tmp_path / "outside-marker.json"
        outside_marker.write_bytes(b"{}")
        try:
            os.symlink(outside_marker, marker)
        except NotImplementedError, OSError:
            pytest.skip("symbolic links are not supported in this environment")

        with pytest.raises(DemoRootError, match="symbolic link or junction"):
            open_or_create_demo_root(root)
        with pytest.raises(DemoRootError, match="symbolic link or junction"):
            reset_demo_root(root)

        _assert_sentinel_survives(sentinel)

    def test_linked_external_proof_is_rejected_without_touching_the_scenario(
        self, tmp_path: Path
    ) -> None:
        root = _own(tmp_path / "demo")
        sentinel = _place_sentinel(root / SCENARIO_DIRNAME)
        ownership_id = _marker_document(root)["ownership_id"]
        assert isinstance(ownership_id, str)
        proof_path = ownership._ownership_proof_path(ownership_id, create_registry=False)
        proof_path.unlink()
        outside_proof = tmp_path / "outside-proof.json"
        outside_proof.write_bytes(b"{}")
        try:
            os.symlink(outside_proof, proof_path)
        except NotImplementedError, OSError:
            pytest.skip("symbolic links are not supported in this environment")

        with pytest.raises(DemoRootError, match="symbolic link or junction"):
            reset_demo_root(root)

        _assert_sentinel_survives(sentinel)

    def test_junctioned_external_proof_is_rejected_where_supported(self, tmp_path: Path) -> None:
        if not _IS_WINDOWS:
            pytest.skip("junctions are Windows-only")
        root = _own(tmp_path / "demo")
        sentinel = _place_sentinel(root / SCENARIO_DIRNAME)
        ownership_id = _marker_document(root)["ownership_id"]
        assert isinstance(ownership_id, str)
        proof_path = ownership._ownership_proof_path(ownership_id, create_registry=False)
        proof_path.unlink()
        outside = tmp_path / "outside-proof-directory"
        outside.mkdir()
        result = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(proof_path), str(outside)],
            capture_output=True,
        )
        if result.returncode != 0:
            pytest.skip("junctions are not supported in this environment")
        try:
            with pytest.raises(DemoRootError, match="symbolic link or junction"):
                reset_demo_root(root)
        finally:
            os.rmdir(proof_path)

        _assert_sentinel_survives(sentinel)


class TestReset:
    def test_reset_of_an_owned_root_removes_the_whole_tree(self, tmp_path: Path) -> None:
        root = _own(tmp_path / "demo")
        scenario = root / SCENARIO_DIRNAME
        (scenario / "fixtures").mkdir()
        (scenario / "fixtures" / "payload.bin").write_bytes(b"durable")
        ownership_id = _marker_document(root)["ownership_id"]
        assert isinstance(ownership_id, str)
        proof_path = ownership._ownership_proof_path(ownership_id, create_registry=False)
        assert proof_path.is_file()

        removed = reset_demo_root(root)

        assert removed == root.resolve()
        assert not root.exists()
        assert not proof_path.exists()

    def test_reset_of_a_nonexistent_path_is_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(DemoRootError, match="not an existing demo root"):
            reset_demo_root(tmp_path / "missing")

    def test_reset_of_an_empty_directory_without_marker_is_rejected(self, tmp_path: Path) -> None:
        root = tmp_path / "demo"
        root.mkdir()

        with pytest.raises(DemoRootError):
            reset_demo_root(root)

        assert root.is_dir()

    def test_reset_with_an_unexpected_top_level_entry_is_rejected_and_keeps_the_sentinel(
        self, tmp_path: Path
    ) -> None:
        root = _own(tmp_path / "demo")
        sentinel = _place_sentinel(root)

        with pytest.raises(DemoRootError, match="unexpected owned-root entries"):
            reset_demo_root(root)

        assert root.is_dir()
        _assert_sentinel_survives(sentinel)
        assert sorted(path.name for path in root.iterdir()) == [
            OWNERSHIP_MARKER_NAME,
            SCENARIO_DIRNAME,
            "sentinel.txt",
        ]

    def test_reset_with_a_forged_marker_version_is_rejected_and_keeps_the_sentinel(
        self, tmp_path: Path
    ) -> None:
        root = _own(tmp_path / "demo")
        sentinel = _place_sentinel(root / SCENARIO_DIRNAME)
        (root / OWNERSHIP_MARKER_NAME).write_bytes(
            json.dumps(
                {
                    "format": OWNERSHIP_MARKER_FORMAT,
                    "version": OWNERSHIP_MARKER_VERSION + 1,
                    "scenario_dirname": SCENARIO_DIRNAME,
                }
            ).encode("utf-8")
        )

        with pytest.raises(DemoRootError, match="owned schema"):
            reset_demo_root(root)

        _assert_sentinel_survives(sentinel)

    def test_reset_with_a_forged_marker_format_is_rejected_and_keeps_the_sentinel(
        self, tmp_path: Path
    ) -> None:
        root = _own(tmp_path / "demo")
        sentinel = _place_sentinel(root / SCENARIO_DIRNAME)
        (root / OWNERSHIP_MARKER_NAME).write_bytes(
            json.dumps(
                {"format": "other.format", "version": 1, "scenario_dirname": "scenario"}
            ).encode("utf-8")
        )

        with pytest.raises(DemoRootError, match="owned schema"):
            reset_demo_root(root)

        _assert_sentinel_survives(sentinel)

    def test_reset_with_an_extra_marker_key_is_rejected_and_keeps_the_sentinel(
        self, tmp_path: Path
    ) -> None:
        root = _own(tmp_path / "demo")
        sentinel = _place_sentinel(root / SCENARIO_DIRNAME)
        (root / OWNERSHIP_MARKER_NAME).write_bytes(
            json.dumps(
                {
                    "format": OWNERSHIP_MARKER_FORMAT,
                    "version": OWNERSHIP_MARKER_VERSION,
                    "scenario_dirname": SCENARIO_DIRNAME,
                    "extra": True,
                }
            ).encode("utf-8")
        )

        with pytest.raises(DemoRootError, match="owned schema"):
            reset_demo_root(root)

        _assert_sentinel_survives(sentinel)

    def test_reset_with_a_non_json_marker_is_rejected_and_keeps_the_sentinel(
        self, tmp_path: Path
    ) -> None:
        root = _own(tmp_path / "demo")
        sentinel = _place_sentinel(root / SCENARIO_DIRNAME)
        (root / OWNERSHIP_MARKER_NAME).write_bytes(b"this is not json")

        with pytest.raises(DemoRootError, match="malformed"):
            reset_demo_root(root)

        _assert_sentinel_survives(sentinel)

    def test_reset_refuses_to_delete_through_a_link_child_and_keeps_the_sentinel(
        self, tmp_path: Path
    ) -> None:
        root = _own(tmp_path / "demo")
        scenario = root / SCENARIO_DIRNAME
        sentinel = _place_sentinel(scenario)
        outside = tmp_path / "outside"
        outside.mkdir()
        try:
            os.symlink(outside, scenario / "escape", target_is_directory=True)
        except NotImplementedError, OSError:
            pytest.skip("symbolic links are not supported in this environment")

        with pytest.raises(DemoRootError, match="symbolic link or junction"):
            reset_demo_root(root)

        _assert_sentinel_survives(sentinel)
        assert outside.is_dir()

    def test_reset_refuses_to_delete_through_a_junction_child_where_supported(
        self, tmp_path: Path
    ) -> None:
        if not _IS_WINDOWS:
            pytest.skip("junctions are Windows-only")
        root = _own(tmp_path / "demo")
        scenario = root / SCENARIO_DIRNAME
        sentinel = _place_sentinel(scenario)
        outside = tmp_path / "outside"
        outside.mkdir()
        junction = scenario / "junction"
        result = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(junction), str(outside)],
            capture_output=True,
        )
        if result.returncode != 0:
            pytest.skip("junctions are not supported in this environment")
        try:
            with pytest.raises(DemoRootError, match="symbolic link or junction"):
                reset_demo_root(root)
        finally:
            os.rmdir(junction)
        _assert_sentinel_survives(sentinel)
        assert outside.is_dir()

    def test_reset_of_a_relative_target_is_rejected_without_touching_anything(self) -> None:
        with pytest.raises(DemoRootError, match="absolute"):
            reset_demo_root(Path("relative") / "demo")

    def test_reset_revalidates_immediately_before_deletion(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        root = _own(tmp_path / "demo")
        original_validate = ownership._validate_owned_demo_root
        validation_calls = 0
        sentinels: list[Path] = []

        def validate_with_injected_change(candidate: Path) -> object:
            nonlocal validation_calls
            validation_calls += 1
            if validation_calls == 2:
                sentinels.append(_place_sentinel(root))
            return original_validate(candidate)

        monkeypatch.setattr(ownership, "_validate_owned_demo_root", validate_with_injected_change)

        with pytest.raises(DemoRootError, match="unexpected owned-root entries"):
            reset_demo_root(root)

        assert validation_calls == 2
        assert len(sentinels) == 1
        _assert_sentinel_survives(sentinels[0])
