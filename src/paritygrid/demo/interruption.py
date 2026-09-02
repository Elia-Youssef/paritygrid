"""Controlled process interruption and durable recovery proof.

The harness runs the headless demo in an explicitly owned child process,
waits for a durable failpoint handshake — written only after the named
boundary's commits returned durably — and terminates exactly that child
process handle.  It never uses process-name-wide termination, never targets
an unrelated process, and never guesses an interruption point with sleeps.

The same owned demo root then restarts in a second child without the
failpoint.  Recovery must reconstruct the story from SQLite, checkpoints,
attempts, artifacts, idempotency facts, and durable events, reach the
expected reconciliation and target-state facts, and leave every repair
effect applied exactly once.  Partial state from the interrupted process is
never accepted as success: the first child must not have published the
manifest, and the proof fails unless the restarted child completes it.
"""

import asyncio
import contextlib
import json
import os
import subprocess
import sys
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, cast

from paritygrid.demo.datasets import WireValue, canonical_json_bytes
from paritygrid.demo.scenario_runner import MANIFEST_FILENAME, STORY_FAILPOINT_NAMES

INTERRUPTION_FORMAT = "paritygrid.demo.interruption-proof"
INTERRUPTION_VERSION = 1
HANDSHAKE_FILENAME = "failpoint-handshake.json"
_CHILD_START_TIMEOUT_SECONDS = 240.0
_CHILD_RESTART_TIMEOUT_SECONDS = 600.0
_HANDSHAKE_POLL_SECONDS = 0.2
_MAX_HANDSHAKE_BYTES = 4096

if TYPE_CHECKING:
    import argparse
    from collections.abc import Callable

    from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
    from paritygrid.demo.orchestration import DemoLifecycle


class InterruptionError(RuntimeError):
    """Raised when the controlled interruption proof cannot proceed."""


@dataclass(frozen=True, slots=True)
class InterruptionOutcome:
    """The verified result of one controlled interruption and recovery."""

    failpoint: str
    checks: tuple[str, ...]

    def document(self) -> dict[str, WireValue]:
        """Return the bounded machine-readable proof document."""
        return {
            "checks": list(self.checks),
            "failpoint": self.failpoint,
            "format": INTERRUPTION_FORMAT,
            "version": INTERRUPTION_VERSION,
        }

    def canonical_bytes(self) -> bytes:
        """Return the byte-stable proof document."""
        return canonical_json_bytes(self.document())


def child_command(
    root: Path,
    failpoint: str,
    handshake_path: Path,
    invocation: str,
) -> list[str]:
    """Return the exact owned child command for one interruption phase.

    ``invocation`` is a parent-generated token the child must echo in its
    handshake, so a stale or foreign handshake file can never be mistaken
    for this child's durable-boundary signal.
    """
    return [
        sys.executable,
        "-m",
        "paritygrid.demo.interruption",
        "--root",
        str(root),
        "--failpoint",
        failpoint,
        "--handshake-file",
        str(handshake_path),
        "--invocation",
        invocation,
    ]


def run_interruption_proof(root: Path, failpoint: str) -> InterruptionOutcome:
    """Interrupt one owned child at a durable boundary and prove recovery."""
    _require_failpoint(failpoint)
    import uuid

    with tempfile.TemporaryDirectory(prefix="paritygrid-interrupt-") as scratch:
        handshake_path = Path(scratch) / HANDSHAKE_FILENAME
        invocation = uuid.uuid4().hex
        manifest_before = _manifest_digest(root)
        first_exit = _run_interrupted_child(root, failpoint, handshake_path, invocation)
        checks = [_assert_no_new_manifest(root, manifest_before)]
        second_exit = _run_restarted_child(root)
        checks.extend(
            _verify_recovered_root(
                root,
                failpoint,
                first_exit,
                second_exit,
            )
        )
        return InterruptionOutcome(failpoint=failpoint, checks=tuple(checks))


def _require_failpoint(failpoint: str) -> None:
    if failpoint not in STORY_FAILPOINT_NAMES:
        raise InterruptionError(
            f"unknown interruption failpoint {failpoint!r}; the closed set is "
            f"{STORY_FAILPOINT_NAMES}"
        )


def _run_interrupted_child(
    root: Path, failpoint: str, handshake_path: Path, invocation: str
) -> int:
    handshake_path.parent.mkdir(parents=True, exist_ok=True)
    if handshake_path.exists():
        handshake_path.unlink()
    child = subprocess.Popen(
        child_command(root, failpoint, handshake_path, invocation),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=tempfile.gettempdir(),
    )
    try:
        _await_handshake(handshake_path, child, failpoint, invocation)
        _terminate_exact_child(child)
        return child.returncode if child.returncode is not None else -1
    except BaseException:
        with contextlib.suppress(Exception):
            _terminate_exact_child(child)
        raise


def _run_restarted_child(root: Path) -> int:
    """Restart the same owned root without any failpoint and await success."""
    import uuid

    with tempfile.TemporaryDirectory(prefix="paritygrid-resume-") as scratch:
        handshake_path = Path(scratch) / HANDSHAKE_FILENAME
        child = subprocess.Popen(
            child_command(root, "none", handshake_path, uuid.uuid4().hex),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=tempfile.gettempdir(),
        )
        try:
            return child.wait(timeout=_CHILD_RESTART_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired as error:
            with contextlib.suppress(Exception):
                _terminate_exact_child(child)
            raise InterruptionError(
                "the restarted demo child did not finish within its bounded budget"
            ) from error
        except BaseException:
            # The restarted child must never outlive the harness on a
            # cancellation or failure path either.
            with contextlib.suppress(Exception):
                _terminate_exact_child(child)
            raise


def _await_handshake(
    handshake_path: Path,
    child: subprocess.Popen[bytes],
    failpoint: str,
    invocation: str,
) -> None:
    """Wait for the durable failpoint handshake written by the owned child."""
    import time

    deadline = time.monotonic() + _CHILD_START_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if child.poll() is not None:
            raise InterruptionError(f"the interrupted child exited before reaching {failpoint!r}")
        if handshake_path.exists():
            document = _read_handshake(handshake_path)
            if document.get("failpoint") != failpoint:
                raise InterruptionError("the handshake names an unexpected failpoint")
            if document.get("invocation") != invocation:
                raise InterruptionError(
                    "the handshake does not carry this harness's invocation token"
                )
            return
        time.sleep(_HANDSHAKE_POLL_SECONDS)
    raise InterruptionError(
        f"the owned child never reached the {failpoint!r} boundary within its budget"
    )


def _read_handshake(handshake_path: Path) -> dict[str, object]:
    try:
        raw = handshake_path.read_bytes()
    except OSError as error:
        raise InterruptionError("the failpoint handshake is unreadable") from error
    if len(raw) > _MAX_HANDSHAKE_BYTES:
        raise InterruptionError("the failpoint handshake is oversized")
    try:
        parsed: object = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InterruptionError("the failpoint handshake is malformed") from error
    if not isinstance(parsed, dict):
        raise InterruptionError("the failpoint handshake must be a JSON object")
    return cast("dict[str, object]", parsed)


def _terminate_exact_child(child: subprocess.Popen[bytes]) -> None:
    """Terminate the exact owned child handle; never a name-wide kill."""
    if child.poll() is not None:
        return
    child.terminate()
    try:
        child.wait(timeout=15.0)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait(timeout=15.0)


def _manifest_digest(root: Path) -> str | None:
    """Return the manifest's SHA-256 before a phase, or None when absent."""
    import hashlib

    manifest = root / "scenario" / MANIFEST_FILENAME
    if not manifest.is_file():
        return None
    return hashlib.sha256(manifest.read_bytes()).hexdigest()


def _assert_no_new_manifest(root: Path, manifest_before: str | None) -> str:
    """Prove the interrupted child never published a manifest of its own.

    A fresh interrupted root must stay manifest-free — partial state is
    never presented as success.  A repeat interruption of an already
    complete root keeps the earlier manifest, so the child is proven honest
    by the manifest staying byte-identical instead.
    """
    from hashlib import sha256

    manifest = root / "scenario" / MANIFEST_FILENAME
    if manifest_before is None:
        if manifest.exists():
            raise InterruptionError(
                "the interrupted process published a manifest; partial state "
                "must never be presented as success"
            )
        return "no_partial_success_manifest"
    if not manifest.is_file():
        raise InterruptionError("the previously published manifest vanished during recovery")
    digest = sha256(manifest.read_bytes()).hexdigest()
    if digest != manifest_before:
        raise InterruptionError("the interrupted process rewrote an already-published manifest")
    return "no_partial_success_manifest"


def _verify_recovered_root(
    root: Path,
    failpoint: str,
    first_exit: int,
    second_exit: int,
) -> list[str]:
    from paritygrid.demo.ownership import open_or_create_demo_root
    from paritygrid.demo.scenario_runner import MANIFEST_FILENAME

    if first_exit == 0:
        raise InterruptionError(
            "the interrupted child exited successfully; the controlled termination was not proven"
        )
    if second_exit != 0:
        raise InterruptionError(f"the restarted demo child failed with exit code {second_exit}")
    demo_root, _created = open_or_create_demo_root(root)
    manifest = demo_root.scenario_path / MANIFEST_FILENAME
    if not manifest.is_file():
        raise InterruptionError("the recovered demo root did not publish its manifest")
    checks = [
        f"failpoint_{failpoint}_recovered",
        "interrupted_child_nonzero",
        "restart_completed",
    ]
    checks.extend(_durable_recovery_checks(demo_root.scenario_path))
    return checks


def _durable_recovery_checks(scenario_path: Path) -> list[str]:
    """Read the recovered durable facts and prove single effects and fencing."""
    from paritygrid.adapters.persistence.sqlite import (
        SQLiteDatabase,
        SQLiteDatabaseConfig,
    )
    from paritygrid.demo.scenario_runner import DATABASE_FILENAME

    database = SQLiteDatabase.open(
        SQLiteDatabaseConfig((scenario_path / DATABASE_FILENAME).resolve())
    )
    try:
        return _recovery_checks_with_database(database, scenario_path)
    finally:
        database.close()


def _recovery_checks_with_database(database: SQLiteDatabase, scenario_path: Path) -> list[str]:
    from sqlalchemy import select

    from paritygrid.adapters.persistence.schema import (
        reconciliation_summaries,
        repair_actions,
        repair_approvals,
        repair_plans,
        work_attempts,
        work_items,
    )

    checks: list[str] = []
    with database.transaction() as session:
        attempts = session.execute(
            select(work_attempts.c.attempt_number)
            .join(work_items, work_attempts.c.work_item_id == work_items.c.work_item_id)
            .where(work_items.c.run_id == "run_canonical-demo")
        ).all()
        numbers = sorted(int(row.attempt_number) for row in attempts)
        if numbers != [1, 2]:
            raise InterruptionError(
                f"recovered attempts diverge from canonical facts: {numbers}; "
                "late results from old ownership must be fenced, never duplicated"
            )
        checks.append("attempts_fenced_exactly_once")
        plan_ids = tuple(
            str(row.repair_plan_id)
            for row in session.execute(
                select(repair_plans.c.repair_plan_id).where(
                    repair_plans.c.run_id == "run_canonical-demo"
                )
            ).all()
        )
        approvals = session.execute(
            select(repair_approvals.c.repair_plan_id).where(
                repair_approvals.c.repair_plan_id.in_(plan_ids)
            )
        ).all()
        if len(approvals) != 1:
            raise InterruptionError("the recovered root must carry exactly one approval fact")
        checks.append("approval_replayed_not_duplicated")
        actions = session.execute(
            select(
                repair_actions.c.application_status,
                repair_actions.c.external_idempotency_key,
            ).where(repair_actions.c.run_id == "run_canonical-demo")
        ).all()
        keys = [str(row.external_idempotency_key) for row in actions]
        if len(keys) != len(set(keys)) or not keys:
            raise InterruptionError("a repair effect would be applied twice after recovery")
        if any(str(row.application_status) != "applied" for row in actions):
            raise InterruptionError("the recovered repair actions are not all durably applied")
        checks.append("repair_effects_applied_once")
        summaries = session.execute(
            select(reconciliation_summaries.c.reconciliation_fingerprint).where(
                reconciliation_summaries.c.run_id == "run_canonical-demo"
            )
        ).all()
        if len(summaries) != 1:
            raise InterruptionError("the recovered root must carry exactly one reconciliation")
        checks.append("reconciliation_single_durable_fact")
    checks.append(_assert_persisted_external_effects(scenario_path, keys))
    return checks


def _assert_persisted_external_effects(scenario_path: Path, action_keys: list[str]) -> str:
    """Prove durable simulator receipts bind every repair key to one effect.

    SQLite repair records alone do not prove an external target was preserved
    through process restart.  The demo warehouse persists both idempotency
    receipts and the count of logical state changes, atomically under the
    validated scenario root.  This check rejects a missing receipt, a fresh
    target reset, an unexpected effect, or a repeated effect for any repair
    action.
    """
    from paritygrid.demo.failures import FailureScript
    from paritygrid.demo.simulators.warehouse import WarehouseBehavior

    behavior = WarehouseBehavior(FailureScript.empty(), state_root=scenario_path)
    effects = behavior.external_effect_counts()
    expected = set(action_keys)
    if not expected.issubset(effects):
        raise InterruptionError(
            "the persisted target effects are missing durable repair action keys"
        )
    if any(effects[key] != 1 for key in expected):
        raise InterruptionError("a persisted external logical effect was applied more than once")
    if any(count != 1 for count in effects.values()):
        raise InterruptionError("a persisted external logical effect was applied more than once")
    if behavior.target_version != sum(effects.values()):
        raise InterruptionError("the persisted target version diverges from its logical effects")
    return "external_target_effects_exactly_once"


def _blocking_failpoint_hook(
    expected: str,
    handshake_path: Path,
    invocation: str,
    *,
    release: threading.Event | None = None,
) -> Callable[[str], None]:
    """Build the child-side hook: handshake after the commit, then block."""

    def hook(name: str) -> None:
        if name != expected:
            return
        payload = json.dumps(
            {"failpoint": name, "invocation": invocation},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        partial = handshake_path.with_name(handshake_path.name + ".partial")
        with open(partial, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(partial, handshake_path)
        # Production children intentionally wait until their exact process
        # handle is terminated.  Unit tests supply a release event so they
        # can exercise the same durable handshake without leaking a daemon
        # thread for the rest of the interpreter lifetime.
        (release if release is not None else threading.Event()).wait()

    return hook


def _run_child_main(root: Path, failpoint: str, handshake_path: Path, invocation: str) -> int:
    """The owned child entry: headless demo start, then block at the boundary."""
    from paritygrid.demo.orchestration import DemoLifecycle, DemoOptions

    options = DemoOptions(root=root, runner="sequential", headless=True)
    lifecycle = DemoLifecycle(options)
    hook = _blocking_failpoint_hook(failpoint, handshake_path, invocation)
    return asyncio.run(_child_async_main(lifecycle, hook))


async def _child_async_main(lifecycle: DemoLifecycle, hook: Callable[[str], None]) -> int:
    import contextlib

    from paritygrid.demo.story import publish_canonical_pipeline

    try:
        facts = await lifecycle.start()
        publish_canonical_pipeline(facts.container)
        await lifecycle.run_canonical_evidence(failpoint=hook)
        return 0
    finally:
        with contextlib.suppress(Exception):
            await lifecycle.aclose()


def main(argv: list[str] | None = None) -> int:
    """Child entry point: run the demo until the failpoint boundary."""
    parser = _child_parser()
    args = parser.parse_args(argv)
    if args.failpoint == "none":
        # The restart phase runs a complete headless demo without failpoints.
        from paritygrid.demo.demo_app import run_demo_command
        from paritygrid.demo.orchestration import DemoOptions

        options = DemoOptions(root=Path(args.root), runner="sequential", headless=True)
        return run_demo_command(options, json_output=bool(args.json))
    return _run_child_main(
        Path(args.root), args.failpoint, Path(args.handshake_file), args.invocation
    )


def _child_parser() -> argparse.ArgumentParser:
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m paritygrid.demo.interruption",
        description="Owned interruption child of the ParityGrid demo harness.",
    )
    parser.add_argument("--root", required=True)
    parser.add_argument(
        "--failpoint",
        required=True,
        choices=(*STORY_FAILPOINT_NAMES, "none"),
    )
    parser.add_argument("--handshake-file", required=True)
    parser.add_argument("--invocation", required=True)
    parser.add_argument("--json", action="store_true")
    return parser


if __name__ == "__main__":
    sys.exit(main())
