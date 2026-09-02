"""Phase 20 controlled-interruption and durable-recovery proof tests.

The fast unit tests cover the closed failpoint set, owned child command,
atomic handshake, and handshake parsing without spawning a process.  The
integration tests terminate real owned children at every durable canonical
story boundary.  Recovery must complete from persisted facts, including the
simulator's target/idempotency receipts, and every repair key must evidence
exactly one external logical effect after restart.
"""

# pyright: reportPrivateUsage=false

import json
import sys
import threading
import time
from pathlib import Path

import pytest

from paritygrid.demo.interruption import (
    _MAX_HANDSHAKE_BYTES,
    HANDSHAKE_FILENAME,
    INTERRUPTION_FORMAT,
    INTERRUPTION_VERSION,
    InterruptionError,
    InterruptionOutcome,
    _blocking_failpoint_hook,
    _read_handshake,
    _require_failpoint,
    child_command,
    run_interruption_proof,
)
from paritygrid.demo.scenario_runner import (
    MANIFEST_FILENAME,
    STORY_FAILPOINT_NAMES,
)
from paritygrid.demo.simulators.warehouse import WAREHOUSE_STATE_FILENAME

_HOOK_HANDSHAKE_TIMEOUT_SECONDS = 10.0
_HOOK_POLL_SECONDS = 0.05


@pytest.fixture(scope="module")
def proof(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[Path, InterruptionOutcome]:
    """Run the target-effect boundary once and share its completed root."""
    root = tmp_path_factory.mktemp("phase20-interruption") / "proof-root"
    return root, run_interruption_proof(root, "repair.applied")


@pytest.mark.parametrize(
    "failpoint",
    ["", "repair.applied; rm", "attempts.recorded ", "none-ok", "REPAIR.APPROVED"],
)
def test_unknown_failpoint_names_the_closed_set(failpoint: str, tmp_path: Path) -> None:
    with pytest.raises(InterruptionError) as from_guard:
        _require_failpoint(failpoint)
    with pytest.raises(InterruptionError) as from_proof:
        run_interruption_proof(tmp_path, failpoint)
    for expected in (from_guard, from_proof):
        message = str(expected.value)
        for known in STORY_FAILPOINT_NAMES:
            assert known in message, f"the closed set message omits {known}"


def test_child_command_is_an_exact_owned_argv(tmp_path: Path) -> None:
    command = child_command(tmp_path, "repair.approved", tmp_path / "hs.json", "inv-token")
    assert command[:3] == [sys.executable, "-m", "paritygrid.demo.interruption"]
    assert command[command.index("--failpoint") + 1] == "repair.approved"
    assert command[command.index("--invocation") + 1] == "inv-token"
    assert command[command.index("--handshake-file") + 1] == str(tmp_path / "hs.json")
    assert "inv-token" in command
    forbidden = set(";|&<>\n`$")
    assert not any(forbidden & set(argument) for argument in command)


def test_blocking_failpoint_hook_ignores_other_names(tmp_path: Path) -> None:
    handshake_path = tmp_path / HANDSHAKE_FILENAME
    hook = _blocking_failpoint_hook("repair.approved", handshake_path, "inv-token")
    assert hook("attempts.recorded") is None
    assert hook("reconciliation.persisted") is None
    assert hook("repair.applied") is None
    assert not handshake_path.exists()


def test_blocking_failpoint_hook_writes_atomic_handshake_then_blocks(tmp_path: Path) -> None:
    handshake_path = tmp_path / HANDSHAKE_FILENAME
    release = threading.Event()
    hook = _blocking_failpoint_hook("repair.approved", handshake_path, "inv-token", release=release)
    worker = threading.Thread(target=hook, args=("repair.approved",))
    worker.start()
    deadline = time.monotonic() + _HOOK_HANDSHAKE_TIMEOUT_SECONDS
    while time.monotonic() < deadline and not handshake_path.exists():
        time.sleep(_HOOK_POLL_SECONDS)
    document = _read_handshake(handshake_path)
    assert document == {"failpoint": "repair.approved", "invocation": "inv-token"}
    assert not handshake_path.with_name(handshake_path.name + ".partial").exists()
    release.set()
    worker.join(timeout=1.0)
    assert not worker.is_alive(), "the unit-test release must reclaim the hook thread"


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"not-json{",
        b"[1, 2]",
        b'"a string"',
        b"x" * (_MAX_HANDSHAKE_BYTES + 1),
        b'{"failpoint": "' + b"a" * (_MAX_HANDSHAKE_BYTES + 1) + b'"}',
    ],
)
def test_read_handshake_rejects_bad_payloads(payload: bytes, tmp_path: Path) -> None:
    handshake_path = tmp_path / HANDSHAKE_FILENAME
    handshake_path.write_bytes(payload)
    with pytest.raises(InterruptionError):
        _read_handshake(handshake_path)


def test_read_handshake_rejects_a_missing_file(tmp_path: Path) -> None:
    with pytest.raises(InterruptionError):
        _read_handshake(tmp_path / "absent.json")


def test_interruption_outcome_document_is_deterministic() -> None:
    outcome = InterruptionOutcome(
        failpoint="repair.approved",
        checks=("restart_completed", "repair_effects_applied_once"),
    )
    assert outcome.document() == outcome.document()
    first = outcome.canonical_bytes()
    second = outcome.canonical_bytes()
    assert first == second
    document = json.loads(first)
    assert list(document) == sorted(document)
    assert document == {
        "checks": ["restart_completed", "repair_effects_applied_once"],
        "failpoint": "repair.approved",
        "format": INTERRUPTION_FORMAT,
        "version": INTERRUPTION_VERSION,
    }


def test_interruption_proof_verifies_full_recovery(
    proof: tuple[Path, InterruptionOutcome],
) -> None:
    root, outcome = proof
    assert isinstance(outcome, InterruptionOutcome)
    assert outcome.failpoint == "repair.applied"
    for check in (
        "no_partial_success_manifest",
        "interrupted_child_nonzero",
        "restart_completed",
        "attempts_fenced_exactly_once",
        "approval_replayed_not_duplicated",
        "repair_effects_applied_once",
        "reconciliation_single_durable_fact",
        "external_target_effects_exactly_once",
    ):
        assert check in outcome.checks
    assert (root / "scenario" / MANIFEST_FILENAME).is_file()
    assert (root / "scenario" / WAREHOUSE_STATE_FILENAME).is_file()


@pytest.mark.parametrize(
    "failpoint",
    tuple(name for name in STORY_FAILPOINT_NAMES if name != "repair.applied"),
)
def test_every_other_canonical_failpoint_recovers_from_durable_evidence(
    failpoint: str,
    tmp_path: Path,
) -> None:
    outcome = run_interruption_proof(tmp_path / failpoint, failpoint)
    assert f"failpoint_{failpoint}_recovered" in outcome.checks
    assert "interrupted_child_nonzero" in outcome.checks
    assert "external_target_effects_exactly_once" in outcome.checks


def test_repeat_interruption_on_completed_root_stays_safe(
    proof: tuple[Path, InterruptionOutcome],
) -> None:
    root, first = proof
    second = run_interruption_proof(root, "repair.applied")
    assert second.failpoint == first.failpoint
    assert "no_partial_success_manifest" in second.checks
    assert "restart_completed" in second.checks
    assert "repair_effects_applied_once" in second.checks
    assert "external_target_effects_exactly_once" in second.checks
    assert second.canonical_bytes() == second.canonical_bytes()
