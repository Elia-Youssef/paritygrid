"""Explicitly owned demo roots and safe, contained reset.

Phase 20 orchestration needs recovery metadata of its own, but the accepted
Phase 19 scenario-root invariant — a fresh scenario root must be nonexistent
or empty — must not weaken.  The demo therefore owns an *outer* demo root
whose only children are the ownership marker and the ``scenario`` directory;
the scenario directory itself stays a plain Phase 19 scenario root and data
root of the composed runtime.

Reset deletes one exact owned demo root through Python filesystem APIs only
after the absolute target resolves inside its own validated location, the
ownership marker matches the exact schema, and no component of the tree is a
symbolic link, junction, or reparse point.  Every rejected reset leaves the
tree byte-identical, so unrelated files always survive.
"""

import json
import os
import re
import secrets
import shutil
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from paritygrid.demo.scenario_runner import ScenarioRoot

OWNERSHIP_MARKER_FORMAT = "paritygrid.demo.ownership"
OWNERSHIP_MARKER_VERSION = 1
OWNERSHIP_MARKER_NAME = "paritygrid-demo-ownership.json"
OWNERSHIP_PROOF_FORMAT = "paritygrid.demo.ownership-proof"
OWNERSHIP_PROOF_VERSION = 1
OWNERSHIP_PROOF_DIRECTORY_NAME = "paritygrid-demo-ownership-proofs"
SCENARIO_DIRNAME = "scenario"

_BROAD_HOME_FOLDERS = ("Desktop", "Documents", "Downloads")
_SYSTEM_ROOT_VARIABLES = ("SystemRoot", "ProgramFiles", "ProgramFiles(x86)")
# ``~`` is part of ordinary Windows 8.3 aliases (for example RUNNER~1 on
# hosted CI) and carries no traversal or alternate-stream semantics.
_SAFE_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._~-]{0,63}\Z")
_OWNERSHIP_ID = re.compile(r"[0-9a-f]{64}\Z")
_RESERVED_NAMES = frozenset(
    {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        *(f"COM{number}" for number in range(10)),
        *(f"LPT{number}" for number in range(10)),
    }
)


class DemoRootError(ValueError):
    """Raised when a demo root or reset target is not safe or not owned."""


@dataclass(frozen=True, slots=True)
class _OwnershipValidation:
    """Validated external proof required to delete one owned root."""

    ownership_id: str
    proof_path: Path


def _validate_component(part: str) -> None:
    if part in (".", ".."):
        raise DemoRootError("demo root paths must not traverse")
    if ":" in part:
        raise DemoRootError("demo root paths must not carry drive or stream separators")
    if not _SAFE_COMPONENT.fullmatch(part):
        raise DemoRootError(f"demo root path component is malformed: {part!r}")
    if part.split(".")[0].upper() in _RESERVED_NAMES:
        raise DemoRootError(f"demo root path component is reserved: {part!r}")
    if part.endswith((".", " ")):
        raise DemoRootError("demo root path components must not end with a dot or space")


def _only_case_change(candidate: Path, resolved: Path) -> bool:
    """Report whether resolve() only changed the drive-letter case."""
    return str(candidate).lower() == str(resolved).lower()


def _reject_broad_roots(resolved: Path) -> None:
    """Reject every broad or system-critical target outright."""
    home = Path.home().resolve()
    cwd = Path.cwd().resolve()
    # A drive or filesystem root has exactly one non-empty part.
    if len(resolved.parts) <= 1:
        raise DemoRootError("a filesystem or drive root is never a demo root")
    if resolved == home or resolved in home.parents:
        raise DemoRootError("a broad user directory is never a demo root")
    for name in _BROAD_HOME_FOLDERS:
        if resolved == home / name:
            raise DemoRootError(f"the {name} folder is never a demo root")
    if resolved == cwd or resolved in cwd.parents:
        raise DemoRootError(
            "the working directory, the repository, and every ancestor are never demo roots"
        )
    if resolved == Path(tempdir_root()).resolve():
        raise DemoRootError("the temporary-directory root itself is never a demo root")
    for name in _SYSTEM_ROOT_VARIABLES:
        system_root = os.environ.get(name)
        if not system_root:
            continue
        system_root_resolved = Path(system_root).resolve()
        if resolved == system_root_resolved or system_root_resolved in resolved.parents:
            raise DemoRootError(
                f"directories inside ${name} are system directories and never demo roots"
            )


def tempdir_root() -> Path:
    """Return the system temporary directory root."""
    return Path(tempfile.gettempdir())


def _validated_absolute(root: Path) -> Path:
    if root == Path() or str(root).strip() == "":
        raise DemoRootError("the demo root must be a non-empty path")
    if not root.is_absolute():
        raise DemoRootError("the demo root must be an absolute path")
    cleaned = Path(os.path.normpath(str(root)))
    for part in cleaned.parts:
        if part == cleaned.anchor:
            continue
        _validate_component(part)
    return cleaned


def _is_link_or_reparse_point(path: Path) -> bool:
    """Return whether *path* is a link, junction, or another reparse point."""
    try:
        if path.is_symlink() or path.is_junction():
            return True
        if os.name != "nt":
            return False
        attributes = getattr(path.stat(follow_symlinks=False), "st_file_attributes", 0)
        reparse_point = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        return bool(attributes & reparse_point)
    except DemoRootError, OSError:
        # The later operation reports a missing or unreadable entry.  A failed
        # metadata lookup is not evidence that a non-existent path is linked.
        return False


def _reject_link_or_reparse_point(path: Path, *, subject: str) -> None:
    if _is_link_or_reparse_point(path):
        raise DemoRootError(
            f"{subject} must not be a symbolic link or junction or other reparse point"
        )


def _validated_link_free(resolved: Path) -> None:
    """Reject any link component on the target or its ancestors."""
    for existing in (resolved, *resolved.parents):
        _reject_link_or_reparse_point(existing, subject=f"the demo root path component {existing}")


def _read_json_document(document_path: Path, *, subject: str) -> dict[str, object]:
    _reject_link_or_reparse_point(document_path, subject=subject)
    if not document_path.is_file():
        raise DemoRootError(f"{subject} is unreadable")
    try:
        raw = document_path.read_bytes()
    except OSError as error:
        raise DemoRootError(f"{subject} is unreadable") from error
    if len(raw) > 4096:
        raise DemoRootError(f"{subject} is oversized")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DemoRootError(f"{subject} is malformed") from error
    if not isinstance(value, dict):
        raise DemoRootError(f"{subject} must be a JSON object")
    # json.loads is untyped at the trust boundary; the isinstance guard above
    # proves the shape before the key set is validated by exact comparison.
    return cast("dict[str, object]", value)


def _read_marker(marker_path: Path) -> dict[str, object]:
    return _read_json_document(marker_path, subject="the demo root ownership marker")


def _require_exact_marker(value: dict[str, object]) -> str:
    expected = {
        "format": OWNERSHIP_MARKER_FORMAT,
        "version": OWNERSHIP_MARKER_VERSION,
        "scenario_dirname": SCENARIO_DIRNAME,
    }
    ownership_id = value.get("ownership_id")
    if (
        set(value) != {*expected, "ownership_id"}
        or any(value[key] != expected[key] for key in expected)
        or not isinstance(ownership_id, str)
        or not _OWNERSHIP_ID.fullmatch(ownership_id)
    ):
        raise DemoRootError("the demo root ownership marker does not match the owned schema")
    return ownership_id


def _ownership_proof_directory(*, create: bool) -> Path:
    """Return the link-free registry holding proofs outside demo roots.

    This registry deliberately lives beside, never inside, the selected demo
    root.  A copied in-tree marker therefore cannot authorize a pre-existing
    directory on its own.  The randomized proof identifier is the filename;
    it is bound again to the resolved root in the proof document.
    """
    directory = tempdir_root() / OWNERSHIP_PROOF_DIRECTORY_NAME
    _validated_link_free(directory)
    if directory.exists():
        if not directory.is_dir():
            raise DemoRootError("the independent ownership-proof registry is not a directory")
    elif create:
        try:
            directory.mkdir(mode=0o700)
        except OSError as error:
            raise DemoRootError(
                "the independent ownership-proof registry cannot be created"
            ) from error
    else:
        raise DemoRootError("the independent ownership proof is missing")
    _validated_link_free(directory)
    return directory


def _ownership_proof_path(ownership_id: str, *, create_registry: bool) -> Path:
    if not _OWNERSHIP_ID.fullmatch(ownership_id):
        raise DemoRootError("the independent ownership proof identifier is malformed")
    path = _ownership_proof_directory(create=create_registry) / f"{ownership_id}.json"
    _reject_link_or_reparse_point(path, subject="the independent ownership proof")
    return path


def _read_ownership_proof(root: Path, ownership_id: str) -> Path:
    proof_path = _ownership_proof_path(ownership_id, create_registry=False)
    value = _read_json_document(proof_path, subject="the independent ownership proof")
    expected = {
        "format": OWNERSHIP_PROOF_FORMAT,
        "version": OWNERSHIP_PROOF_VERSION,
        "root_path": str(root),
        "ownership_id": ownership_id,
    }
    if value != expected:
        raise DemoRootError("the independent ownership proof does not match the owned root")
    return proof_path


@dataclass(frozen=True, slots=True)
class DemoRoot:
    """One validated, explicitly owned demo root."""

    path: Path
    marker_path: Path
    scenario_path: Path

    @property
    def scenario_root(self) -> ScenarioRoot:
        """Return the validated Phase 19 scenario-root view of the data root."""
        return ScenarioRoot(path=self.scenario_path)


def resolve_demo_root(root: Path) -> Path:
    """Validate and resolve one explicit demo-root path without touching disk."""
    cleaned = _validated_absolute(root)
    # Inspect every existing component before resolution. This is the
    # authoritative link/junction guard; string divergence is not, because
    # Windows legitimately expands 8.3 aliases while resolving a plain path.
    _validated_link_free(cleaned)
    resolved = cleaned.resolve(strict=False)
    _validated_absolute(resolved)
    _validated_link_free(resolved)
    _reject_broad_roots(resolved)
    proof_registry = Path(os.path.normpath(str(tempdir_root() / OWNERSHIP_PROOF_DIRECTORY_NAME)))
    if (
        resolved == proof_registry
        or resolved in proof_registry.parents
        or proof_registry in resolved.parents
    ):
        raise DemoRootError("the independent ownership-proof registry is reserved for proofs")
    return resolved


def open_or_create_demo_root(root: Path) -> tuple[DemoRoot, bool]:
    """Open an owned demo root or create one with an empty scenario root.

    Creating requires the scenario directory to be nonexistent or empty —
    the accepted Phase 19 scenario-root invariant — and writes the ownership
    marker exclusively.  Opening requires the exact owned schema and rejects
    unowned or unexpected content.  The boolean result reports whether the
    root was created by this call.
    """
    resolved = resolve_demo_root(root)
    marker_path = resolved / OWNERSHIP_MARKER_NAME
    scenario_path = resolved / SCENARIO_DIRNAME
    _validated_link_free(resolved)
    _reject_link_or_reparse_point(marker_path, subject="the demo root ownership marker")
    if marker_path.exists():
        _validate_owned_demo_root(resolved)
        return DemoRoot(path=resolved, marker_path=marker_path, scenario_path=scenario_path), False
    root_created = False
    if resolved.exists():
        if not resolved.is_dir():
            raise DemoRootError("the demo root path exists and is not a directory")
        if any(resolved.iterdir()):
            raise DemoRootError(
                "the demo root exists without an ownership marker and holds unowned content"
            )
    else:
        resolved.mkdir(parents=True)
        root_created = True
    _validated_link_free(resolved)
    scenario_created = False
    ownership_id = secrets.token_hex(32)
    marker_payload = _marker_payload(ownership_id)
    try:
        scenario_created = _require_empty_scenario_target(scenario_path, create=True)
        _write_marker_exclusively(marker_path, ownership_id)
        _write_ownership_proof_exclusively(resolved, ownership_id)
    except BaseException:
        # Do not recursively remove a caller-owned empty root.  Every cleanup
        # step is exact and empty-only, so a concurrent addition is preserved
        # rather than being mistaken for demo-owned data.
        _remove_plain_file_if_exact(marker_path, marker_payload)
        if scenario_created:
            _remove_empty_plain_directory(scenario_path)
        if root_created:
            _remove_empty_plain_directory(resolved)
        raise
    return DemoRoot(path=resolved, marker_path=marker_path, scenario_path=scenario_path), True


def _require_empty_scenario_target(scenario_path: Path, *, create: bool) -> bool:
    """Enforce the Phase 19 invariant for the scenario directory itself."""
    _reject_link_or_reparse_point(scenario_path, subject="the scenario directory")
    if scenario_path.exists():
        if not scenario_path.is_dir():
            raise DemoRootError("the scenario path exists and is not a directory")
        if any(scenario_path.iterdir()):
            raise DemoRootError("the scenario directory must be empty before generation")
        return False
    elif create:
        scenario_path.mkdir()
        _reject_link_or_reparse_point(scenario_path, subject="the scenario directory")
        return True
    else:
        raise DemoRootError("the owned demo root is missing its scenario directory")


def _marker_payload(ownership_id: str) -> bytes:
    return _json_payload(
        {
            "format": OWNERSHIP_MARKER_FORMAT,
            "version": OWNERSHIP_MARKER_VERSION,
            "scenario_dirname": SCENARIO_DIRNAME,
            "ownership_id": ownership_id,
        }
    )


def _proof_payload(root: Path, ownership_id: str) -> bytes:
    return _json_payload(
        {
            "format": OWNERSHIP_PROOF_FORMAT,
            "version": OWNERSHIP_PROOF_VERSION,
            "root_path": str(root),
            "ownership_id": ownership_id,
        }
    )


def _json_payload(value: dict[str, object]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _write_bytes_exclusively(path: Path, payload: bytes) -> None:
    _reject_link_or_reparse_point(path, subject="an ownership document")
    handle: int | None = None
    created = False
    try:
        handle = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        created = True
        written = 0
        while written < len(payload):
            written += os.write(handle, payload[written:])
        os.fsync(handle)
    except BaseException:
        if handle is not None:
            os.close(handle)
            handle = None
        if created:
            _remove_plain_file(path)
        raise
    finally:
        if handle is not None:
            os.close(handle)


def _write_marker_exclusively(marker_path: Path, ownership_id: str) -> None:
    _write_bytes_exclusively(marker_path, _marker_payload(ownership_id))


def _write_ownership_proof_exclusively(root: Path, ownership_id: str) -> Path:
    proof_path = _ownership_proof_path(ownership_id, create_registry=True)
    _write_bytes_exclusively(proof_path, _proof_payload(root, ownership_id))
    return proof_path


def _remove_plain_file_if_exact(path: Path, expected: bytes) -> None:
    try:
        if _is_link_or_reparse_point(path) or not path.is_file() or path.read_bytes() != expected:
            return
        path.unlink()
    except OSError:
        return


def _remove_plain_file(path: Path) -> None:
    try:
        if not _is_link_or_reparse_point(path) and path.is_file():
            path.unlink()
    except OSError:
        return


def _remove_empty_plain_directory(path: Path) -> None:
    try:
        if not _is_link_or_reparse_point(path) and path.is_dir():
            path.rmdir()
    except OSError:
        return


def _validate_owned_demo_root(resolved: Path) -> _OwnershipValidation:
    """Validate every in-tree and external ownership boundary without deleting."""
    marker_path = resolved / OWNERSHIP_MARKER_NAME
    scenario_path = resolved / SCENARIO_DIRNAME
    if not resolved.is_dir():
        raise DemoRootError("the reset target is not an existing demo root directory")
    _validated_link_free(resolved)
    _reject_link_or_reparse_point(marker_path, subject="the demo root ownership marker")
    ownership_id = _require_exact_marker(_read_marker(marker_path))
    proof_path = _read_ownership_proof(resolved, ownership_id)
    _reject_link_or_reparse_point(scenario_path, subject="the scenario directory")
    if not scenario_path.is_dir():
        raise DemoRootError("the owned demo root is missing a plain scenario directory")
    _validated_link_free(scenario_path)
    _reject_link_tree(scenario_path)
    unexpected = sorted(
        entry.name
        for entry in resolved.iterdir()
        if entry.name not in (OWNERSHIP_MARKER_NAME, SCENARIO_DIRNAME)
    )
    if unexpected:
        raise DemoRootError(f"the demo root holds unexpected owned-root entries: {unexpected}")
    resolved_after = resolved.resolve()
    if resolved_after != resolved and not _only_case_change(resolved, resolved_after):
        raise DemoRootError("the reset target changed identity during validation")
    return _OwnershipValidation(ownership_id=ownership_id, proof_path=proof_path)


def _remove_ownership_proof(root: Path, validation: _OwnershipValidation) -> None:
    """Remove only the exact regular proof that authorized a deleted root."""
    try:
        _reject_link_or_reparse_point(
            validation.proof_path, subject="the independent ownership proof"
        )
        value = _read_json_document(
            validation.proof_path, subject="the independent ownership proof"
        )
        expected = {
            "format": OWNERSHIP_PROOF_FORMAT,
            "version": OWNERSHIP_PROOF_VERSION,
            "root_path": str(root),
            "ownership_id": validation.ownership_id,
        }
        if value == expected:
            validation.proof_path.unlink()
    except DemoRootError, OSError:
        # The root no longer exists and a stale external proof cannot grant
        # ownership to a fresh root (creation uses a new 256-bit identifier).
        # Preserve an unexpected replacement rather than deleting it.
        return


def reset_demo_root(root: Path) -> Path:
    """Delete one exact owned demo root after full containment validation.

    The target must be an explicit absolute path that resolves without any
    link, carry the exact ownership-marker schema, and stay inside its own
    validated location.  Anything else — empty, relative, broad, unowned,
    linked, or schema-divergent — is rejected before a single entry is
    unlinked, so unrelated content always survives a rejected reset.
    """
    resolved = resolve_demo_root(root)
    _validate_owned_demo_root(resolved)
    # Validate again as the final operation before deletion.  This makes a
    # concurrent replacement fail closed unless it occurs in the unavoidable
    # filesystem race between this check and the platform rmtree call.
    validation = _validate_owned_demo_root(resolved)
    shutil.rmtree(resolved)
    _remove_ownership_proof(resolved, validation)
    return resolved


def _reject_link_tree(root: Path) -> None:
    """Reject any symbolic link, junction, or reparse point below the root."""
    stack = [root]
    while stack:
        current = stack.pop()
        for entry in current.iterdir():
            if _is_link_or_reparse_point(entry):
                raise DemoRootError(
                    "the owned demo root contains a symbolic link or junction; "
                    "reset refuses to delete through a link"
                )
            if entry.is_dir():
                stack.append(entry)
