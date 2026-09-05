"""Third-party inventory drift and fail-closed behavior tests (P22.4)."""

import json
import shutil
import sysconfig
import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest

from paritygrid.quality.third_party_notices import (
    DISTRIBUTION_TARGETS,
    INVENTORY_FORMAT,
    PROJECT_LICENSE,
    InventoryInputs,
    NoticeInventoryError,
    build_inventory,
    render_bundled_notices,
    render_inventory,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMMITTED_INVENTORY = PROJECT_ROOT / "docs" / "generated" / "third-party-notices.json"
ROOT_NOTICE = PROJECT_ROOT / "THIRD_PARTY_NOTICES.txt"
PUBLIC_NOTICE = PROJECT_ROOT / "web" / "public" / ROOT_NOTICE.name
DIST_NOTICE = PROJECT_ROOT / "web" / "dist" / ROOT_NOTICE.name
_PROJECT_PACKAGE = "paritygrid"


def _real_inputs() -> InventoryInputs:
    return InventoryInputs(
        uv_lock_path=PROJECT_ROOT / "uv.lock",
        python_site_packages=Path(sysconfig.get_paths()["purelib"]),
        package_lock_path=PROJECT_ROOT / "web" / "package-lock.json",
        frontend_dist_path=PROJECT_ROOT / "web" / "dist",
    )


def _build_real() -> dict[str, object]:
    return build_inventory(_real_inputs())


def _ecosystem_entries(ecosystem: str) -> list[dict[str, object]]:
    return cast("list[dict[str, object]]", _build_real()[ecosystem])


class TestLockParity:
    def test_project_license_and_full_distribution_model_are_recorded(self) -> None:
        project = cast("dict[str, object]", _build_real()["project"])
        assert project == {
            "license": PROJECT_LICENSE,
            "distribution_targets": list(DISTRIBUTION_TARGETS),
        }

    def test_python_inventory_covers_exactly_the_locked_set(self) -> None:
        lock = tomllib.loads((PROJECT_ROOT / "uv.lock").read_text(encoding="utf-8"))
        locked = {
            package["name"]: package["version"]
            for package in lock["package"]
            if package["name"] != _PROJECT_PACKAGE
        }
        inventory = {entry["name"]: entry["version"] for entry in _ecosystem_entries("python")}
        assert inventory == locked

    def test_frontend_inventory_covers_exactly_the_locked_pairs(self) -> None:
        lock = json.loads((PROJECT_ROOT / "web" / "package-lock.json").read_text("utf-8"))
        locked = {
            (location.rsplit("node_modules/", 1)[-1], entry["version"])
            for location, entry in lock["packages"].items()
            if location
        }
        inventory = {(entry["name"], entry["version"]) for entry in _ecosystem_entries("frontend")}
        assert inventory == locked

    def test_every_python_entry_records_identity_from_the_closed_sources(self) -> None:
        allowed = {
            "verified-override",
            "license-expression",
            "legacy-license-field",
            "trove-classifier",
        }
        for entry in _ecosystem_entries("python"):
            assert entry["license"], entry
            assert entry["license_source"] in allowed, entry
            assert entry["compatibility"] == "compatible-with-notice-obligations", entry

    def test_every_frontend_entry_records_a_lockfile_license(self) -> None:
        for entry in _ecosystem_entries("frontend"):
            assert entry["license"], entry
            assert entry["license_source"] == "lockfile"
            assert entry["compatibility"] == "compatible-with-notice-obligations", entry

    def test_distributed_font_assets_map_to_locked_font_packages(self) -> None:
        assert _build_real()["distributed_assets"] == [
            {
                "path": "web/dist/assets/inter-latin-wght-normal-Dx4kXJAl.woff2",
                "package": "@fontsource-variable/inter",
                "version": "5.3.0",
                "license": "OFL-1.1",
            },
            {
                "path": "web/dist/assets/roboto-mono-latin-wght-normal-CZtBPCCa.woff2",
                "package": "@fontsource-variable/roboto-mono",
                "version": "5.3.0",
                "license": "OFL-1.1",
            },
        ]


class TestDeterminism:
    def test_rebuild_is_byte_identical(self) -> None:
        assert render_inventory(_build_real()) == render_inventory(_build_real())

    def test_rendered_inventory_is_host_independent(self) -> None:
        text = render_inventory(_build_real())
        assert "\\" not in text
        for drive in "CDEFGH":
            assert f"{drive}:\\" not in text
            assert f"{drive}:/Users" not in text

    def test_committed_inventory_matches_regeneration(self) -> None:
        assert COMMITTED_INVENTORY.is_file()
        assert COMMITTED_INVENTORY.read_text(encoding="utf-8") == render_inventory(_build_real())

    def test_bundled_notice_copies_match_the_locked_font_license_sources(self) -> None:
        if not (PROJECT_ROOT / "web" / "node_modules").is_dir():
            pytest.skip("locked frontend package sources run in the integration notice lane")
        expected = render_bundled_notices(_real_inputs())
        assert ROOT_NOTICE.read_text(encoding="utf-8") == expected
        assert PUBLIC_NOTICE.read_text(encoding="utf-8") == expected
        assert DIST_NOTICE.read_text(encoding="utf-8") == expected
        assert "Copyright 2016 The Inter Project Authors" in expected
        assert "Copyright 2015 The Roboto Mono Project Authors" in expected
        assert expected.count("SIL OPEN FONT LICENSE Version 1.1") == 2

    def test_document_header_is_stable(self) -> None:
        inventory = _build_real()
        assert inventory["format"] == INVENTORY_FORMAT
        assert inventory["inventory_version"] == 1
        text = render_inventory(inventory)
        assert text.endswith("}\n")


def _write_uv_lock(
    root: Path, packages: list[tuple[str, str]], *, direct: list[str] | None = None
) -> Path:
    dependency_lines = "\n".join(f'    {{ name = "{name}" }},' for name in (direct or []))
    entries = "\n".join(
        f'[[package]]\nname = "{name}"\nversion = "{version}"\n' for name, version in packages
    )
    text = (
        f'[[package]]\nname = "{_PROJECT_PACKAGE}"\nversion = "0.1.0"\n'
        f"dependencies = [\n{dependency_lines}\n]\n\n" + entries
    )
    path = root / "uv.lock"
    path.write_text(text, encoding="utf-8")
    return path


def _make_dist_info(
    site_packages: Path,
    name: str,
    version: str,
    *,
    expression: str | None = "MIT",
    license_field: str | None = None,
    classifiers: list[str] | None = None,
) -> None:
    dist_info = site_packages / f"{name.replace('-', '_')}-{version}.dist-info"
    dist_info.mkdir(parents=True)
    lines = [f"Name: {name}", f"Version: {version}"]
    if expression is not None:
        lines.append(f"License-Expression: {expression}")
    if license_field is not None:
        lines.append(f"License: {license_field}")
    lines.extend(f"Classifier: {classifier}" for classifier in classifiers or [])
    (dist_info / "METADATA").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_package_lock(root: Path, entries: list[tuple[str, str, str, str | None]]) -> Path:
    packages: dict[str, dict[str, object]] = {
        "": {"name": "fixture", "version": "1.0.0", "lockfileVersion": 3}
    }
    for location, version, license_value, resolved in entries:
        entry: dict[str, object] = {"version": version, "license": license_value}
        if resolved is not None:
            entry["resolved"] = resolved
        packages[location] = entry
    path = root / "package-lock.json"
    path.write_text(
        json.dumps({"name": "fixture", "lockfileVersion": 3, "packages": packages}),
        encoding="utf-8",
    )
    return path


def _write_frontend_dist(root: Path, files: dict[str, bytes]) -> Path:
    dist = root / "dist"
    for relative, content in files.items():
        target = dist / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    return dist


def _synthetic_inputs(
    root: Path,
    *,
    lock_packages: list[tuple[str, str]] | None = None,
    installed_packages: list[tuple[str, str]] | None = None,
    dist_info_factory: Callable[[Path, str, str], None] | None = None,
    frontend: list[tuple[str, str, str, str | None]] | None = None,
    dist_files: dict[str, bytes] | None = None,
) -> InventoryInputs:
    site = root / "site-packages"
    site.mkdir(exist_ok=True)
    for name, version in installed_packages or []:
        if dist_info_factory is None:
            _make_dist_info(site, name, version)
        else:
            dist_info_factory(site, name, version)
    return InventoryInputs(
        uv_lock_path=_write_uv_lock(root, lock_packages or []),
        python_site_packages=site,
        package_lock_path=_write_package_lock(root, frontend or []),
        frontend_dist_path=_write_frontend_dist(
            root,
            dist_files
            if dist_files is not None
            else {"index.html": b"shell", "assets/index-Abc123.js": b"code"},
        ),
    )


_FIRST_PARTY_DIST = {
    "index.html": b"shell",
    "assets/index-Abc123.js": b"code",
    "assets/index-Def456.css": b"style",
}
_ROBOTO_FONT = ("web/dist", "assets/roboto-mono-latin-wght-normal-Cz1Ab2.woff2")


class TestFailClosed:
    def test_lock_package_missing_from_the_environment_fails(self, tmp_path: Path) -> None:
        inputs = _synthetic_inputs(
            tmp_path,
            lock_packages=[("alpha", "1.0.0")],
            installed_packages=[],
        )
        with pytest.raises(NoticeInventoryError, match="no installed dist-info"):
            build_inventory(inputs)

    def test_stale_environment_version_fails(self, tmp_path: Path) -> None:
        inputs = _synthetic_inputs(
            tmp_path,
            lock_packages=[("alpha", "2.0.0")],
            installed_packages=[("alpha", "1.0.0")],
        )
        with pytest.raises(NoticeInventoryError, match="not at the locked"):
            build_inventory(inputs)

    def test_python_package_without_any_license_metadata_fails(self, tmp_path: Path) -> None:
        inputs = _synthetic_inputs(
            tmp_path,
            lock_packages=[("alpha", "1.0.0")],
            installed_packages=[("alpha", "1.0.0")],
            dist_info_factory=lambda site, name, version: _make_dist_info(
                site, name, version, expression=None
            ),
        )
        with pytest.raises(NoticeInventoryError, match="no license metadata"):
            build_inventory(inputs)

    def test_unmapped_legacy_license_string_fails(self, tmp_path: Path) -> None:
        inputs = _synthetic_inputs(
            tmp_path,
            lock_packages=[("alpha", "1.0.0")],
            installed_packages=[("alpha", "1.0.0")],
            dist_info_factory=lambda site, name, version: _make_dist_info(
                site, name, version, expression=None, license_field="Freeware"
            ),
        )
        with pytest.raises(NoticeInventoryError, match="unmapped legacy license"):
            build_inventory(inputs)

    def test_expression_with_unknown_identifier_fails(self, tmp_path: Path) -> None:
        inputs = _synthetic_inputs(
            tmp_path,
            lock_packages=[("alpha", "1.0.0")],
            installed_packages=[("alpha", "1.0.0")],
            dist_info_factory=lambda site, name, version: _make_dist_info(
                site, name, version, expression="MIT AND Proprietary-1.0"
            ),
        )
        with pytest.raises(NoticeInventoryError, match="outside the audited set"):
            build_inventory(inputs)

    def test_frontend_entry_without_license_fails(self, tmp_path: Path) -> None:
        inputs = _synthetic_inputs(
            tmp_path,
            lock_packages=[("alpha", "1.0.0")],
            installed_packages=[("alpha", "1.0.0")],
            frontend=[("node_modules/beta", "1.0.0", "MIT", "https://registry.example/beta")],
        )
        lock = json.loads(inputs.package_lock_path.read_text(encoding="utf-8"))
        del lock["packages"]["node_modules/beta"]["license"]
        inputs.package_lock_path.write_text(json.dumps(lock), encoding="utf-8")
        with pytest.raises(NoticeInventoryError, match="no license field"):
            build_inventory(inputs)

    def test_frontend_license_outside_the_audited_spdx_set_fails(self, tmp_path: Path) -> None:
        inputs = _synthetic_inputs(
            tmp_path,
            lock_packages=[("alpha", "1.0.0")],
            installed_packages=[("alpha", "1.0.0")],
            frontend=[
                ("node_modules/beta", "1.0.0", "Proprietary-1.0", "https://registry.example/beta")
            ],
        )
        with pytest.raises(NoticeInventoryError, match="outside the audited set"):
            build_inventory(inputs)

    def test_conflicting_frontend_license_metadata_fails(self, tmp_path: Path) -> None:
        inputs = _synthetic_inputs(
            tmp_path,
            lock_packages=[("alpha", "1.0.0")],
            installed_packages=[("alpha", "1.0.0")],
            frontend=[
                ("node_modules/beta", "1.0.0", "MIT", "https://registry.example/beta"),
                (
                    "node_modules/outer/node_modules/beta",
                    "1.0.0",
                    "Apache-2.0",
                    "https://registry.example/beta",
                ),
            ],
        )
        with pytest.raises(NoticeInventoryError, match="conflicting license metadata"):
            build_inventory(inputs)

    def test_unknown_distributed_asset_fails(self, tmp_path: Path) -> None:
        inputs = _synthetic_inputs(
            tmp_path,
            lock_packages=[("alpha", "1.0.0")],
            installed_packages=[("alpha", "1.0.0")],
            frontend=[("node_modules/beta", "1.0.0", "MIT", "https://registry.example/beta")],
            dist_files={**_FIRST_PARTY_DIST, "assets/vendor-9zz99.bin": b"???"},
        )
        with pytest.raises(NoticeInventoryError, match="neither a first-party build"):
            build_inventory(inputs)

    def test_unmapped_font_family_fails(self, tmp_path: Path) -> None:
        inputs = _synthetic_inputs(
            tmp_path,
            lock_packages=[("alpha", "1.0.0")],
            installed_packages=[("alpha", "1.0.0")],
            frontend=[("node_modules/beta", "1.0.0", "MIT", "https://registry.example/beta")],
            dist_files={
                **_FIRST_PARTY_DIST,
                "assets/unknown-sans-latin-wght-normal-Xy3Wv4.woff2": b"font",
            },
        )
        with pytest.raises(NoticeInventoryError, match="no mapped source package"):
            build_inventory(inputs)

    def test_mapped_font_missing_from_the_lockfile_fails(self, tmp_path: Path) -> None:
        inputs = _synthetic_inputs(
            tmp_path,
            lock_packages=[("alpha", "1.0.0")],
            installed_packages=[("alpha", "1.0.0")],
            frontend=[("node_modules/beta", "1.0.0", "MIT", "https://registry.example/beta")],
            dist_files={
                **_FIRST_PARTY_DIST,
                "assets/roboto-mono-latin-wght-normal-Cz1Ab2.woff2": b"font",
            },
        )
        with pytest.raises(NoticeInventoryError, match="not in the audited lockfile"):
            build_inventory(inputs)

    def test_missing_frontend_distribution_fails(self, tmp_path: Path) -> None:
        inputs = _synthetic_inputs(
            tmp_path,
            lock_packages=[("alpha", "1.0.0")],
            installed_packages=[("alpha", "1.0.0")],
        )
        shutil.rmtree(inputs.frontend_dist_path)
        with pytest.raises(NoticeInventoryError, match="distribution is missing"):
            build_inventory(inputs)


class TestLicenseMetadataSources:
    def test_legacy_psfl_field_maps_to_the_psf_identifier(self, tmp_path: Path) -> None:
        inputs = _synthetic_inputs(
            tmp_path,
            lock_packages=[("alpha", "1.0.0")],
            installed_packages=[("alpha", "1.0.0")],
            dist_info_factory=lambda site, name, version: _make_dist_info(
                site, name, version, expression=None, license_field="PSFL"
            ),
        )
        entries = cast("list[dict[str, object]]", build_inventory(inputs)["python"])
        assert entries == [
            {
                "name": "alpha",
                "version": "1.0.0",
                "role": "transitive",
                "license": "PSF-2.0",
                "license_source": "legacy-license-field",
                "compatibility": "compatible-with-notice-obligations",
                "location": None,
            }
        ]

    def test_trove_classifier_fallback_records_the_license(self, tmp_path: Path) -> None:
        inputs = _synthetic_inputs(
            tmp_path,
            lock_packages=[("alpha", "1.0.0")],
            installed_packages=[("alpha", "1.0.0")],
            dist_info_factory=lambda site, name, version: _make_dist_info(
                site,
                name,
                version,
                expression=None,
                classifiers=["License :: OSI Approved :: MIT License"],
            ),
        )
        entries = cast("list[dict[str, object]]", build_inventory(inputs)["python"])
        assert entries[0]["license"] == "MIT"
        assert entries[0]["license_source"] == "trove-classifier"

    def test_malformed_frontend_location_fails(self, tmp_path: Path) -> None:
        inputs = _synthetic_inputs(
            tmp_path,
            lock_packages=[("alpha", "1.0.0")],
            installed_packages=[("alpha", "1.0.0")],
            frontend=[("registry/odd/path", "1.0.0", "MIT", "https://registry.example/x")],
        )
        with pytest.raises(NoticeInventoryError, match="is not a package path"):
            build_inventory(inputs)
