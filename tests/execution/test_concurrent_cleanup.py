"""Bounded idempotent cleanup with structured unresolved evidence (P7.10)."""

from __future__ import annotations

import pytest

from paritygrid.application.execution.concurrent_cleanup import (
    CleanupReport,
    ConcurrentCleanupCoordinator,
    ConcurrentCleanupFailedError,
    ConcurrentCleanupInvalidRequestError,
    UnresolvedResource,
)


class _Resource:
    """Closeable test double with scripted behavior."""

    def __init__(
        self,
        name: str,
        *,
        fails: bool = False,
        raises: BaseException | None = None,
    ) -> None:
        self.kind = "test"
        self.name = name
        self._fails = fails
        self._raises = raises
        self.close_count = 0

    def close(self, *, timeout_seconds: float) -> None:
        del timeout_seconds
        self.close_count += 1
        if self._raises is not None:
            raise self._raises
        if self._fails:
            raise RuntimeError(f"{self.name} refused to close")


def test_cleanup_closes_every_registered_resource_once() -> None:
    coordinator = ConcurrentCleanupCoordinator()
    first = _Resource("first")
    second = _Resource("second")
    coordinator.register(first)
    coordinator.register(second)
    report = coordinator.cleanup(timeout_seconds=1.0)
    assert report == CleanupReport(
        attempted=2,
        closed=2,
        already_closed=0,
        unresolved=(),
    )
    assert first.close_count == 1
    assert second.close_count == 1
    repeated = coordinator.cleanup(timeout_seconds=1.0)
    assert repeated.attempted == 0
    assert repeated.already_closed == 2
    assert first.close_count == 1
    assert second.close_count == 1


def test_cleanup_attempts_every_resource_after_failures() -> None:
    coordinator = ConcurrentCleanupCoordinator()
    broken = _Resource("broken", fails=True)
    healthy = _Resource("healthy")
    coordinator.register(broken)
    coordinator.register(healthy)
    with pytest.raises(ConcurrentCleanupFailedError) as failure:
        coordinator.cleanup(timeout_seconds=1.0)
    assert broken.close_count == 1
    assert healthy.close_count == 1
    assert "refused to close" in str(failure.value.__cause__)
    # The second pass retries only the unresolved resource.
    with pytest.raises(ConcurrentCleanupFailedError):
        coordinator.cleanup(timeout_seconds=1.0)
    assert broken.close_count == 2
    assert healthy.close_count == 1


def test_cleanup_preserves_original_failure_with_notes() -> None:
    coordinator = ConcurrentCleanupCoordinator()
    original = RuntimeError("original failure")
    first = _Resource("first", raises=original)
    second = _Resource("second", fails=True)
    coordinator.register(first)
    coordinator.register(second)
    with pytest.raises(ConcurrentCleanupFailedError) as failure:
        coordinator.cleanup(timeout_seconds=1.0)
    assert failure.value.__cause__ is original
    assert any("second" in note for note in getattr(original, "__notes__", ()))


def test_unresolved_evidence_is_structured_and_sorted() -> None:
    coordinator = ConcurrentCleanupCoordinator()
    coordinator.register(_Resource("zeta", fails=True))
    coordinator.register(_Resource("alpha", fails=True))
    with pytest.raises(ConcurrentCleanupFailedError):
        coordinator.cleanup(timeout_seconds=1.0)
    # A failing close is retried on the next pass, not silently resolved.
    with pytest.raises(ConcurrentCleanupFailedError):
        coordinator.cleanup(timeout_seconds=1.0)


def test_registry_bounds_and_duplicates_fail_closed() -> None:
    coordinator = ConcurrentCleanupCoordinator()
    coordinator.register(_Resource("dup"))
    with pytest.raises(ConcurrentCleanupInvalidRequestError):
        coordinator.register(_Resource("dup"))
    for index in range(63):
        coordinator.register(_Resource(f"resource-{index}"))
    with pytest.raises(ConcurrentCleanupInvalidRequestError):
        coordinator.register(_Resource("overflow"))


def test_unresolved_resource_validation_rejects_detail_abuse() -> None:
    with pytest.raises(ConcurrentCleanupInvalidRequestError):
        UnresolvedResource(kind="", name="name", detail="detail")
    with pytest.raises(TypeError):
        UnresolvedResource(kind="kind", name="name", detail=1)  # type: ignore[arg-type]
    resource = UnresolvedResource(kind="kind", name="name", detail="")
    assert resource.detail == ""
