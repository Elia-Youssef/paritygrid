"""Dependency-neutral contract tests for artifact publication."""

import hashlib
from collections.abc import Iterable
from typing import Any, cast

import pytest

from paritygrid.application.ports import (
    MAX_ARTIFACT_WRITE_BYTES,
    ArtifactAlreadyExistsError,
    ArtifactInvalidWriteError,
    ArtifactPathError,
    ArtifactPublishOutcomeUnknownError,
    ArtifactRelativePath,
    ArtifactSizeLimitError,
    ArtifactStorageError,
    ArtifactWriteError,
    ArtifactWriter,
    ArtifactWriteReceipt,
)


class _WriterImplementation:
    def write(
        self,
        relative_path: ArtifactRelativePath,
        chunks: Iterable[bytes],
    ) -> ArtifactWriteReceipt:
        content = b"".join(chunks)
        return ArtifactWriteReceipt(
            relative_path,
            len(content),
            hashlib.sha256(content).hexdigest(),
        )


def test_writer_protocol_is_structural_and_dependency_neutral() -> None:
    implementation: ArtifactWriter = _WriterImplementation()
    receipt = implementation.write(ArtifactRelativePath("runs/result.bin"), (b"a", b"b"))

    assert receipt.byte_size == 2
    assert receipt.sha256 == hashlib.sha256(b"ab").hexdigest()
    assert ArtifactAlreadyExistsError.__mro__[1] is ArtifactWriteError
    assert ArtifactInvalidWriteError.__mro__[1] is ArtifactWriteError
    assert ArtifactSizeLimitError.__mro__[1] is ArtifactWriteError
    assert ArtifactStorageError.__mro__[1] is ArtifactWriteError
    assert ArtifactPublishOutcomeUnknownError.__mro__[1] is ArtifactWriteError
    assert ArtifactPathError.__mro__[1] is ValueError


@pytest.mark.parametrize(
    ("changes", "error"),
    [
        ({"relative_path": "runs/result.bin"}, TypeError),
        ({"byte_size": True}, TypeError),
        ({"byte_size": -1}, ValueError),
        ({"byte_size": MAX_ARTIFACT_WRITE_BYTES + 1}, ValueError),
        ({"sha256": b"0" * 64}, TypeError),
        ({"sha256": "A" * 64}, ValueError),
        ({"sha256": "0" * 63}, ValueError),
    ],
)
def test_receipt_rejects_invalid_public_state(
    changes: dict[str, object], error: type[Exception]
) -> None:
    values: dict[str, object] = {
        "relative_path": ArtifactRelativePath("runs/result.bin"),
        "byte_size": 0,
        "sha256": hashlib.sha256(b"").hexdigest(),
    }
    values.update(changes)

    with pytest.raises(error):
        ArtifactWriteReceipt(
            cast(Any, values["relative_path"]),
            cast(Any, values["byte_size"]),
            cast(Any, values["sha256"]),
        )


def test_receipt_accepts_empty_and_maximum_sizes() -> None:
    path = ArtifactRelativePath("runs/result.bin")
    digest = "0" * 64

    assert ArtifactWriteReceipt(path, 0, digest).byte_size == 0
    assert ArtifactWriteReceipt(path, MAX_ARTIFACT_WRITE_BYTES, digest).byte_size == (
        MAX_ARTIFACT_WRITE_BYTES
    )
