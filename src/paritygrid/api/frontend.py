"""Safe serving of packaged production frontend assets.

The responder is confined to one resolved asset root: traversal, encoded
traversal, absolute or drive-shaped segments, backslashes, double-encoded
percent escapes, symlink escapes, directories, and unknown API paths are
all rejected.  Valid client-side navigation falls back to the committed
``index.html``; API, documentation, stream, and live paths never reach the
fallback.  Responses carry strict MIME types, explicit cache policy, and
the repository security headers.
"""

from dataclasses import dataclass
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse

from paritygrid.api.errors.problems import ProblemError

RESERVED_PREFIXES = ("/api", "/healthz", "/readyz")
IMMUTABLE_PREFIXES = ("/assets/",)
SAFE_EXTENSION_MEDIA_TYPES: dict[str, str] = {
    ".css": "text/css; charset=utf-8",
    ".gif": "image/gif",
    ".html": "text/html; charset=utf-8",
    ".ico": "image/x-icon",
    ".jpeg": "image/jpeg",
    ".jpg": "image/jpeg",
    ".js": "text/javascript; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".map": "application/json; charset=utf-8",
    ".png": "image/png",
    ".svg": "image/svg+xml",
    ".txt": "text/plain; charset=utf-8",
    ".wasm": "application/wasm",
    ".webmanifest": "application/manifest+json",
    ".webp": "image/webp",
    ".woff": "font/woff",
    ".woff2": "font/woff2",
}
_UNSAFE_MIME = "application/octet-stream"
_FORBIDDEN_RAW_MARKERS = (
    b"%25",
    b"%2e",
    b"%2f",
    b"%5c",
    b"\\",
    b"..",
    b"//",
    b"\x00",
)


@dataclass(frozen=True, slots=True)
class AssetResolution:
    """One confined asset or SPA fallback decision."""

    file: Path
    media_type: str
    cache_control: str


class FrontendAssets:
    """Resolve and serve one packaged frontend distribution."""

    def __init__(self, root: Path) -> None:
        if not root.is_dir():
            raise ValueError("the frontend asset root must be an existing directory")
        self._root = root.resolve()
        self._index = self._root / "index.html"
        self.router = APIRouter(tags=["frontend"])
        self.router.add_api_route(
            "/{full_path:path}",
            self.serve,
            methods=["GET"],
            name="frontend-asset",
            include_in_schema=False,
        )

    @property
    def root(self) -> Path:
        return self._root

    def serve(self, full_path: str, request: Request) -> FileResponse:
        """Return one confined asset response or the SPA fallback document."""
        raw_path = request.scope.get("raw_path", b"")
        resolution = self.resolve(full_path, raw_path=raw_path)
        return FileResponse(
            resolution.file,
            media_type=resolution.media_type,
            headers={
                "Cache-Control": resolution.cache_control,
                "X-Content-Type-Options": "nosniff",
            },
        )

    def resolve(self, path: str, *, raw_path: bytes) -> AssetResolution:
        """Classify, confine, and resolve one frontend request path."""
        normalized = path.lstrip("/")
        if _is_reserved(path):
            raise _frontend_not_found()
        if any(marker in raw_path.lower() for marker in _FORBIDDEN_RAW_MARKERS):
            raise _frontend_not_found()
        if "\\" in path or "\x00" in path or path != path.strip("\r\n\t "):
            raise _frontend_not_found()
        segments = [segment for segment in normalized.split("/") if segment not in ("", ".")]
        if any(
            not segment
            or segment.startswith(".")
            or ":" in segment
            or segment.endswith(".")
            or not segment.isprintable()
            for segment in segments
        ):
            raise _frontend_not_found()
        candidate = self._root.joinpath(*segments) if segments else self._index
        try:
            resolved = candidate.resolve(strict=True)
        except OSError, ValueError:
            resolved = None
        if resolved is not None and not _is_inside(self._root, resolved):
            raise _frontend_not_found()
        if resolved is not None and resolved.is_dir():
            # Directories are never listed and never fall back.
            raise _frontend_not_found()
        if resolved is None or not resolved.is_file():
            return self._fallback(normalized)
        return AssetResolution(
            file=resolved,
            media_type=_media_type(resolved),
            cache_control=_cache_control(normalized),
        )

    def _fallback(self, normalized: str) -> AssetResolution:
        """Serve the SPA document for valid navigation, reject asset misses."""
        last_segment = normalized.rsplit("/", maxsplit=1)[-1] if normalized else ""
        if "." in last_segment:
            raise _frontend_not_found()
        try:
            index = self._index.resolve(strict=True)
        except OSError, ValueError:
            raise _frontend_not_found() from None
        if not _is_inside(self._root, index) or not index.is_file():
            raise _frontend_not_found()
        return AssetResolution(
            file=index,
            media_type=_media_type(index),
            cache_control="no-cache",
        )


def _is_reserved(path: str) -> bool:
    stripped = path.lstrip("/")
    return any(
        stripped == prefix.lstrip("/") or stripped.startswith(f"{prefix.lstrip('/')}/")
        for prefix in RESERVED_PREFIXES
    )


def _is_inside(root: Path, candidate: Path) -> bool:
    try:
        candidate.relative_to(root)
    except ValueError:
        return False
    return True


def _media_type(file: Path) -> str:
    suffix = file.suffix.lower()
    if suffix in SAFE_EXTENSION_MEDIA_TYPES:
        return SAFE_EXTENSION_MEDIA_TYPES[suffix]
    return _UNSAFE_MIME


def _cache_control(normalized: str) -> str:
    if any(normalized.startswith(prefix.lstrip("/")) for prefix in IMMUTABLE_PREFIXES):
        return "public, max-age=31536000, immutable"
    return "no-cache"


def _frontend_not_found() -> Exception:
    return ProblemError(
        type_slug="not-found",
        title="Resource does not exist",
        status=404,
        detail="the requested frontend path does not exist",
        code="frontend_not_found",
    )


__all__ = ["RESERVED_PREFIXES", "SAFE_EXTENSION_MEDIA_TYPES", "FrontendAssets"]
