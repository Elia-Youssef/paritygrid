"""Filesystem confinement for validated artifact relative paths."""

from pathlib import Path
from typing import cast

from paritygrid.application.ports.artifacts import ArtifactPathError, ArtifactRelativePath


def resolve_artifact_path(root: Path, relative_path: ArtifactRelativePath) -> Path:
    """Resolve a validated path beneath an existing unlinked artifact root."""
    root_value = cast(object, root)
    relative_value = cast(object, relative_path)
    if not isinstance(root_value, Path):
        raise TypeError("artifact root must be a Path value")
    if not isinstance(relative_value, ArtifactRelativePath):
        raise TypeError("artifact relative path must be validated")
    if not root_value.is_absolute():
        raise ArtifactPathError("artifact root must be absolute")

    try:
        _reject_linked_components(root_value)
        resolved_root = root_value.resolve(strict=True)
        if not resolved_root.is_dir():
            raise ArtifactPathError("artifact root must be an existing directory")
        candidate = resolved_root.joinpath(*relative_value.parts)
        _reject_linked_descendants(resolved_root, relative_value)
        resolved_candidate = candidate.resolve(strict=False)
    except ArtifactPathError:
        raise
    except (OSError, RuntimeError, ValueError) as error:
        raise ArtifactPathError("artifact path could not be resolved safely") from error

    if not resolved_candidate.is_relative_to(resolved_root):
        raise ArtifactPathError("artifact path escapes the configured root")
    return resolved_candidate


def _reject_linked_components(path: Path) -> None:
    for component in (path, *path.parents):
        if component.is_symlink() or component.is_junction():
            raise ArtifactPathError("artifact root cannot traverse a filesystem link")


def _reject_linked_descendants(root: Path, relative_path: ArtifactRelativePath) -> None:
    candidate = root
    for segment in relative_path.parts:
        candidate = candidate / segment
        if candidate.is_symlink() or candidate.is_junction():
            raise ArtifactPathError("artifact path cannot traverse a filesystem link")
