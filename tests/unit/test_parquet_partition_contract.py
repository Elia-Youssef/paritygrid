"""Dependency-neutral contracts for Parquet partition publication."""

from pathlib import Path
from typing import Any, cast

import pytest

from paritygrid.application.ports import (
    MAX_PARQUET_PARTITION_NUMBER,
    PARQUET_MEDIA_TYPE,
    ArtifactRelativePath,
    ArtifactWriteReceipt,
    ParquetDatasetKind,
    ParquetPartitionReceipt,
)
from paritygrid.application.ports import parquet as contract
from paritygrid.domain.models import NodeId, RunId
from paritygrid.domain.pipeline import PartitionKey


def _receipt(**overrides: object) -> ParquetPartitionReceipt:
    values: dict[str, object] = {
        "run_id": RunId("run_partition"),
        "node_id": NodeId("nod_export"),
        "partition_key": PartitionKey("region:1"),
        "dataset": ParquetDatasetKind.RAW,
        "partition_number": 0,
        "row_count": 4,
        "schema_version": 1,
        "schema_fingerprint": "a" * 64,
        "write_receipt": ArtifactWriteReceipt(
            ArtifactRelativePath("runs/run_partition/raw/part.parquet"), 100, "b" * 64
        ),
    }
    values.update(overrides)
    return ParquetPartitionReceipt(**cast(Any, values))


def test_partition_contract_constants_and_closed_datasets_are_frozen() -> None:
    assert MAX_PARQUET_PARTITION_NUMBER == 2_147_483_647
    assert PARQUET_MEDIA_TYPE == "application/vnd.apache.parquet"
    assert tuple(ParquetDatasetKind) == (
        ParquetDatasetKind.RAW,
        ParquetDatasetKind.NORMALIZED,
        ParquetDatasetKind.RECONCILIATION,
    )


def test_partition_receipt_is_immutable_manifest_ready_metadata() -> None:
    receipt = _receipt()
    assert receipt.media_type == PARQUET_MEDIA_TYPE
    assert receipt.dataset is ParquetDatasetKind.RAW
    assert receipt.row_count == 4
    with pytest.raises(AttributeError):
        receipt.row_count = 5  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("run_id", "run_partition"),
        ("node_id", "nod_export"),
        ("partition_key", "region:1"),
        ("dataset", "raw"),
        ("partition_number", True),
        ("row_count", True),
        ("schema_fingerprint", b"a" * 64),
        ("write_receipt", object()),
    ],
)
def test_partition_receipt_rejects_wrong_runtime_types(field: str, value: object) -> None:
    with pytest.raises(TypeError):
        _receipt(**{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("partition_number", -1),
        ("partition_number", MAX_PARQUET_PARTITION_NUMBER + 1),
        ("row_count", -1),
        ("row_count", 100_001),
        ("schema_version", 0),
        ("schema_version", 2),
        ("schema_version", True),
        ("schema_fingerprint", "A" * 64),
        ("schema_fingerprint", "a" * 63),
        ("schema_fingerprint", "g" * 64),
    ],
)
def test_partition_receipt_rejects_invalid_bounds(field: str, value: object) -> None:
    with pytest.raises(ValueError, match=r"outside|unsupported|SHA-256"):
        _receipt(**{field: value})


def test_partition_port_remains_free_of_arrow_and_filesystem_adapters() -> None:
    source = Path(contract.__file__).read_text(encoding="utf-8")
    assert "pyarrow" not in source
    assert "FileSystemArtifactWriter" not in source
    assert "Path(" not in source
