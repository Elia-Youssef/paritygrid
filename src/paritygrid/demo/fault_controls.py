"""Closed, versioned fault controls for the canonical demonstration.

The demo exposes exactly the two scripted Phase 19 faults — the asynchronous
source rate limit and the transient warehouse connection loss — as a closed
control catalog.  A control defines its stable identity and version, its exact
activation point, the expected durable consequence, the expected recovery
behavior, the observable evidence, the reset behavior, and the bounded failure
behavior.  There is deliberately no way to inject arbitrary code, URLs, SQL,
paths, or scripts: every control binds to a named scripted failure that the
accepted Phase 8 simulator model already owns, and the canonical story
activates exactly the canonical set every run.

Ordinary ``serve`` mode never constructs these controls: only the demo
orchestration resolves them, so the packaged application cannot be driven
into a scripted failure by accident.
"""

from dataclasses import dataclass

from paritygrid.demo.datasets import WireValue, canonical_json_bytes
from paritygrid.demo.scenarios import (
    ASYNC_RATE_LIMIT_REQUEST,
    LOCKED_RATE_LIMIT_RETRIES,
    LOCKED_TRANSIENT_CONNECTION_FAILURES,
    WAREHOUSE_FAULT_ACTION_INDEX,
)

FAULT_CONTROL_FORMAT = "paritygrid.demo.fault-control"
FAULT_CONTROL_VERSION = 1

RATE_LIMIT_CONTROL_NAME = "canonical.rate_limit"
WAREHOUSE_TRANSIENT_CONTROL_NAME = "canonical.warehouse_transient_failure"
CANONICAL_FAULT_SELECTION = "canonical"


class UnknownFaultControlError(ValueError):
    """Raised when a fault-control selection is not part of the closed set."""


@dataclass(frozen=True, slots=True)
class FaultControl:
    """One deterministic, versioned fault control of the canonical demo."""

    name: str
    version: int
    title: str
    activation_point: str
    expected_consequence: str
    recovery_behavior: str
    observable_evidence: str
    reset_behavior: str
    failure_behavior: str

    @property
    def identity(self) -> str:
        """Return the stable versioned control identity."""
        return f"{FAULT_CONTROL_FORMAT}/{self.name}/v{self.version}"

    def document(self) -> dict[str, WireValue]:
        """Return the bounded catalog document for this control."""
        return {
            "activation_point": self.activation_point,
            "expected_consequence": self.expected_consequence,
            "failure_behavior": self.failure_behavior,
            "identity": self.identity,
            "name": self.name,
            "observable_evidence": self.observable_evidence,
            "recovery_behavior": self.recovery_behavior,
            "reset_behavior": self.reset_behavior,
            "title": self.title,
            "version": self.version,
        }


def _canonical_faults() -> tuple[FaultControl, ...]:
    return (
        FaultControl(
            name=RATE_LIMIT_CONTROL_NAME,
            version=1,
            title="Canonical asynchronous-source rate limit",
            activation_point=(
                f"the asynchronous source simulator rejects request "
                f"{ASYNC_RATE_LIMIT_REQUEST} of the canonical story's first read "
                "attempt with an HTTP 429 response"
            ),
            expected_consequence=(
                f"exactly {LOCKED_RATE_LIMIT_RETRIES} bounded retry attempt is "
                "recorded durably for the asynchronous source work item before it "
                "succeeds; no record is lost or quarantined by the fault"
            ),
            recovery_behavior=(
                "the accepted named retry policy schedules one retry after the "
                "server-advised delay on the injected clock; the retry is the "
                "second attempt of the same work-item identity"
            ),
            observable_evidence=(
                "the run's durable attempt history carries one http_429 classified "
                "attempt, the run-node retry count is "
                f"{LOCKED_RATE_LIMIT_RETRIES}, and the scenario manifest locks "
                "rate_limit_retries"
            ),
            reset_behavior=(
                "a fresh demo run recreates the simulator from the canonical "
                "failure script, so the fault re-fires deterministically at the "
                "same request sequence"
            ),
            failure_behavior=(
                "if the fault does not apply, or applies more than once, the "
                "derived-count verification fails the demo before any success is "
                "reported"
            ),
        ),
        FaultControl(
            name=WAREHOUSE_TRANSIENT_CONTROL_NAME,
            version=1,
            title="Canonical transient warehouse connection loss",
            activation_point=(
                f"the simulated warehouse drops the connection of mutating write "
                f"{WAREHOUSE_FAULT_ACTION_INDEX} of the canonical repair plan, "
                "after the effect was durably recorded in the simulator"
            ),
            expected_consequence=(
                f"exactly {LOCKED_TRANSIENT_CONNECTION_FAILURES} ambiguous outcome "
                "is raised for that repair action while the logical effect exists "
                "exactly once in the target"
            ),
            recovery_behavior=(
                "the idempotent applier replays the same external idempotency key; "
                "the warehouse answers with the recorded outcome and the action "
                "completes without a second logical effect"
            ),
            observable_evidence=(
                "the scenario manifest locks transient_connection_failures and "
                "ambiguous_replays_resolved, and the simulator's applied-failure "
                "evidence shows exactly the one connection loss"
            ),
            reset_behavior=(
                "a fresh demo run recreates the warehouse from the canonical "
                "failure script, so the transient loss re-fires on the same "
                "repair action"
            ),
            failure_behavior=(
                "if the transient loss does not resolve by idempotent replay, the "
                "repair application fails closed and the demo reports failure"
            ),
        ),
    )


_FAULT_CONTROLS: tuple[FaultControl, ...] = _canonical_faults()
_CONTROLS_BY_NAME: dict[str, FaultControl] = {control.name: control for control in _FAULT_CONTROLS}


def fault_controls() -> tuple[FaultControl, ...]:
    """Return the complete closed control catalog in canonical order."""
    return _FAULT_CONTROLS


def resolve_fault_controls(selection: str) -> tuple[FaultControl, ...]:
    """Resolve one closed selection into its ordered controls.

    The only accepted selection is the canonical set identifier; any other
    value — including empty, unknown, or malformed names — is rejected before
    any simulator is constructed.
    """
    if selection == CANONICAL_FAULT_SELECTION:
        return _FAULT_CONTROLS
    raise UnknownFaultControlError(
        f"unknown fault control selection; the closed set offers only {CANONICAL_FAULT_SELECTION!r}"
    )


def resolve_fault_control(name: str) -> FaultControl:
    """Resolve one control by exact name, rejecting unknown or malformed input."""
    control = _CONTROLS_BY_NAME.get(name)
    if control is None:
        raise UnknownFaultControlError(f"unknown fault control: {name!r}")
    return control


def fault_control_catalog_bytes() -> bytes:
    """Return the byte-stable catalog document for machine-readable output."""
    document: dict[str, WireValue] = {
        "controls": [control.document() for control in _FAULT_CONTROLS],
        "format": FAULT_CONTROL_FORMAT,
        "selections": [CANONICAL_FAULT_SELECTION],
        "version": FAULT_CONTROL_VERSION,
    }
    return canonical_json_bytes(document)
