"""Shared demo application flow behind the CLI and the interruption harness.

Both the ``paritygrid demo`` command and the interruption child run exactly
the same bounded lifecycle: start, publish the canonical pipeline, then
either verify the complete headless proof or serve the product.  Keeping one
flow guarantees the interrupted child and the restarted child exercise
identical code, and that exit codes mean the same thing everywhere.
"""

import asyncio
import contextlib
import webbrowser
from pathlib import Path

from paritygrid.adapters.persistence.sqlite import SQLiteDatabase
from paritygrid.demo.fault_controls import resolve_fault_controls
from paritygrid.demo.orchestration import (
    DemoLifecycle,
    DemoLifecycleError,
    DemoOptions,
    DemoReadinessTimeoutError,
    DemoUsageError,
)
from paritygrid.demo.ownership import DemoRootError, reset_demo_root
from paritygrid.demo.proof import (
    EXIT_CANCELLED,
    EXIT_FAILURE,
    EXIT_SUCCESS,
    EXIT_TIMEOUT,
    EXIT_USAGE,
    ProofError,
    build_demo_result,
    verify_durable_facts,
)
from paritygrid.demo.scenarios import FAST_PROFILE, derive_scenario
from paritygrid.demo.story import PublicationFacts, publish_canonical_pipeline


class DemoResetError(RuntimeError):
    """Raised when an explicitly requested demo reset was not safe."""


def default_demo_root() -> Path:
    """Return the deterministic, isolated default demo root."""
    import tempfile

    return Path(tempfile.gettempdir()) / "paritygrid-demo" / "default"


def run_demo_command(
    options: DemoOptions,
    *,
    json_output: bool = False,
    reset: bool = False,
) -> int:
    """Run one bounded demo lifecycle and return its stable exit code."""
    try:
        resolve_fault_controls(options_fault_selection())
    except ValueError as error:
        _emit(str(error))
        return EXIT_USAGE
    if reset:
        try:
            reset_demo_root(options.root)
        except ValueError as error:
            _emit(f"demo reset refused: {error}")
            return EXIT_USAGE
    try:
        return asyncio.run(_demo_main(options, json_output=json_output))
    except DemoUsageError as error:
        _emit(f"demo usage error: {error}")
        return EXIT_USAGE
    except DemoRootError as error:
        # An invalid or unsafe demo-root path is a usage error wherever the
        # command takes a root, not a runtime failure.
        _emit(f"demo usage error: {error}")
        return EXIT_USAGE
    except ProofError as error:
        _emit(f"demo proof failed [{error.code}]: {error}")
        return EXIT_FAILURE
    except DemoReadinessTimeoutError as error:
        _emit(f"demo readiness timed out: {error}")
        return EXIT_TIMEOUT
    except TimeoutError as error:
        _emit(f"demo readiness timed out: {error}")
        return EXIT_TIMEOUT
    except (DemoLifecycleError, ValueError, RuntimeError) as error:
        _emit(f"demo failed: {error}")
        return EXIT_FAILURE
    except KeyboardInterrupt:
        _emit("demo cancelled")
        return EXIT_CANCELLED


def options_fault_selection() -> str:
    """Return the only accepted canonical fault selection."""
    from paritygrid.demo.fault_controls import CANONICAL_FAULT_SELECTION

    return CANONICAL_FAULT_SELECTION


async def _demo_main(options: DemoOptions, *, json_output: bool) -> int:
    lifecycle = DemoLifecycle(options)
    stop = asyncio.Event()
    if not options.headless:
        # Serve mode shuts down gracefully on SIGINT/SIGTERM.  Headless mode
        # keeps the default handlers so Ctrl+C raises KeyboardInterrupt and
        # cancels immediately instead of being delayed to the next check.
        _install_signal_handlers(stop)
    try:
        facts = await lifecycle.start()
        publication = publish_canonical_pipeline(facts.container)
        if options.headless:
            return await _headless_completion(
                lifecycle,
                publication,
                json_output=json_output,
            )
        _emit(f"Demo root: {facts.demo_root.path}")
        _emit(f"Serving the packaged application at {lifecycle.browser_url}")
        lifecycle.start_launcher()
        if options.open_browser:
            webbrowser.open(lifecycle.browser_url)
        await lifecycle.serve_until(stop)
        return EXIT_SUCCESS
    finally:
        with contextlib.suppress(Exception):
            await lifecycle.aclose()


async def _headless_completion(
    lifecycle: DemoLifecycle,
    publication: PublicationFacts,
    *,
    json_output: bool,
) -> int:
    import sys

    story, engine_record, story_seconds, engine_seconds = await lifecycle.run_canonical_evidence()
    facts = lifecycle.facts
    evidence = derive_scenario(FAST_PROFILE)
    checks = verify_durable_facts(
        facts.container.database,
        facts.demo_root,
        story,
        engine_record,
        evidence,
    )
    migration_revision = _migration_revision(facts.container.database)
    result = build_demo_result(
        runner=lifecycle.options.runner,
        migration_revision=migration_revision,
        publication=publication,
        story=story,
        engine_record=engine_record,
        evidence=evidence,
    )
    _emit("Canonical demonstration verified:")
    _emit(f"  story run:            {story.run_id} (succeeded)")
    _emit(f"  reconciliation:       {story.reconciliation_fingerprint}")
    _emit(f"  target state:         {story.observed_target_fingerprint}")
    _emit(f"  engine runner:        {engine_record.strategy_id} ({engine_record.run_id})")
    _emit(f"  engine evidence:      {engine_record.evidence.execution_evidence_fingerprint}")
    _emit(f"  verified checks:      {len(checks)}")
    # Durations are diagnostics only and never enter canonical correctness bytes.
    print(
        f"[diagnostic] story {story_seconds:.2f}s, engine {engine_seconds:.2f}s",
        file=sys.stderr,
    )
    if json_output:
        _emit(result.canonical_bytes().decode("utf-8"))
    return EXIT_SUCCESS


def _migration_revision(database: SQLiteDatabase) -> str:
    from sqlalchemy import text

    with database.engine.connect() as connection:
        row = connection.execute(text("SELECT version_num FROM alembic_version")).first()
    if row is None:
        raise DemoLifecycleError("the demo database carries no migration revision")
    return str(row[0])


def _install_signal_handlers(stop: asyncio.Event) -> None:
    import signal

    loop = asyncio.get_running_loop()

    def _set_stop(*_: object) -> None:
        # The loop may already be closed during interpreter shutdown; the
        # process is terminating either way, so a failed wake-up is ignored.
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(stop.set)

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, ValueError, OSError):
            loop.add_signal_handler(sig, stop.set)
        with contextlib.suppress(ValueError, OSError):
            signal.signal(sig, _set_stop)


def _emit(message: str) -> None:

    print(message, flush=True)
