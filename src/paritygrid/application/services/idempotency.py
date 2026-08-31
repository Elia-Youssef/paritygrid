"""Durable command-idempotency boundary for mutating HTTP commands.

The boundary wraps one mutating use case with a durable reservation keyed by
``(scope, idempotency key)`` and the canonical request digest.  Successful and
deterministically-failed logical outcomes are terminalized exactly once;
retries replay the stored logical response byte-for-byte; concurrent retries
fail closed while the owning request is still inside its lease; and stranded
in-progress reservations are reclaimed by an identical retry once the bounded
lease expires. Reclamation advances the mutable lease timestamp with a
compare-and-set so exactly one retry owns each lease generation while terminal
rows remain immutable.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import cast

from paritygrid.application.ports.configuration import ConfigurationDocument
from paritygrid.application.ports.consistency import (
    ConsistencyInvalidRequestError,
    ConsistencyStaleRowVersionError,
    IdempotencyBeginDisposition,
    IdempotencyConflictError,
    IdempotencyRecord,
    IdempotencyRepository,
    IdempotencyReservation,
    IdempotencyStatus,
    RedactedDocument,
)
from paritygrid.application.ports.operations import OperationalUnitOfWork
from paritygrid.application.services.errors import (
    IdempotencyInProgressError,
    IdempotencyKeyConflictError,
    IdempotencyReplayConflictError,
)
from paritygrid.domain.models import UtcTimestamp

IDEMPOTENT_RESPONSE_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class IdempotencyLeasePolicy:
    """Bounded ownership window for in-progress reservations.

    The lease must exceed the HTTP request timeout so a live request is never
    reclaimed: by the time the window elapses the original owner has
    terminated and only durable evidence remains.
    """

    lease_seconds: float

    def __post_init__(self) -> None:
        if type(self.lease_seconds) is not float or not 0.0 < self.lease_seconds <= 86_400.0:
            raise ValueError("idempotency lease window is outside the supported range")


@dataclass(frozen=True, slots=True)
class CommandOutcome:
    """One logical response of a mutating command.

    ``terminal`` marks deterministic outcomes (success or a 4xx application
    result) that are stored and replayed.  Non-terminal outcomes keep the
    reservation durable and in progress so a bounded retry converges after
    the lease.
    """

    status_code: int
    media_type: str
    body: Mapping[str, object]
    terminal: bool


@dataclass(frozen=True, slots=True)
class CommandExecution:
    """The logical response plus whether durable evidence supplied it."""

    outcome: CommandOutcome
    replayed: bool


class IdempotentCommandService:
    """Reserve, execute, and terminalize one mutating command."""

    def __init__(
        self,
        *,
        unit_of_work: OperationalUnitOfWork,
        policy: IdempotencyLeasePolicy,
        now: Callable[[], UtcTimestamp],
    ) -> None:
        self._unit_of_work = unit_of_work
        self._policy = policy
        self._now = now

    def execute(
        self,
        *,
        scope: str,
        key: str | None,
        request: Mapping[str, object],
        handler: Callable[[], CommandOutcome],
        reclaimed_handler: Callable[[], CommandOutcome] | None = None,
    ) -> CommandExecution:
        """Run ``handler`` under the durable idempotency boundary."""
        if key is None:
            return CommandExecution(handler(), replayed=False)
        request_document = ConfigurationDocument.from_mapping(request)
        try:
            with self._unit_of_work.transaction() as repositories:
                result = repositories.idempotency.reclaim(
                    scope=scope,
                    key=key,
                    request=request_document,
                    lease_expires_after_seconds=self._policy.lease_seconds,
                    now=self._now(),
                )
        except (IdempotencyConflictError, ConsistencyStaleRowVersionError) as error:
            raise IdempotencyKeyConflictError(str(error)) from error
        except ConsistencyInvalidRequestError:
            # Key or scope format violations surface unchanged so the
            # transport layer can report a bounded validation problem.
            raise
        disposition = result.disposition
        if disposition is IdempotencyBeginDisposition.IN_PROGRESS_REPLAY:
            raise IdempotencyInProgressError(
                "an in-progress command owns this idempotency key and its lease has not expired"
            )
        if disposition in {
            IdempotencyBeginDisposition.STARTED,
            IdempotencyBeginDisposition.RECLAIMED,
        }:
            reservation = result.reservation
            if reservation is None:  # pragma: no cover - guarded by result invariant
                raise IdempotencyReplayConflictError("reservation evidence is missing")
            selected_handler = (
                reclaimed_handler
                if disposition is IdempotencyBeginDisposition.RECLAIMED
                and reclaimed_handler is not None
                else handler
            )
            outcome = selected_handler()
            if not outcome.terminal:
                # A non-terminal outcome leaves the reservation durable and
                # in progress; the bounded lease later admits an identical
                # retry while the underlying use case stays idempotent.
                return CommandExecution(outcome, replayed=False)
            return self._terminalize(scope, key, request_document, reservation, outcome)
        stored = _stored_outcome(result.record)
        if stored is None:
            raise IdempotencyReplayConflictError(
                "stored idempotent response does not match the replay schema"
            )
        return CommandExecution(stored, replayed=True)

    def _terminalize(
        self,
        scope: str,
        key: str,
        request: ConfigurationDocument,
        reservation: IdempotencyReservation,
        outcome: CommandOutcome,
    ) -> CommandExecution:
        stored = RedactedDocument.from_mapping(
            {
                "status_code": outcome.status_code,
                "media_type": outcome.media_type,
                "body": dict(outcome.body),
            }
        )
        completed_at = self._now()
        terminal = (
            IdempotencyStatus.COMPLETED if outcome.status_code < 400 else IdempotencyStatus.FAILED
        )
        try:
            with self._unit_of_work.transaction() as repositories:
                _complete(
                    repositories.idempotency,
                    reservation,
                    terminal=terminal,
                    request=request,
                    response=stored,
                    completed_at=completed_at,
                )
        except IdempotencyConflictError as error:
            # A competing owner terminalized first.  Convergence accepts the
            # winner's durable response; every other divergence fails closed.
            winner = self._stored_record(scope, key)
            if winner is not None and winner.status is terminal:
                replayed = _stored_outcome(winner)
                if replayed is not None:
                    return CommandExecution(replayed, replayed=True)
            raise IdempotencyReplayConflictError(str(error)) from error
        return CommandExecution(outcome, replayed=False)

    def _stored_record(self, scope: str, key: str) -> IdempotencyRecord | None:
        with self._unit_of_work.transaction() as repositories:
            return repositories.idempotency.get(scope=scope, key=key)

    def stranded_reservations(self) -> int:
        """Count durable in-progress reservations for diagnostics."""
        total = 0
        cursor = None
        while True:
            with self._unit_of_work.transaction() as repositories:
                page = repositories.idempotency.list_in_progress(limit=100, after=cursor)
            total += len(page.items)
            if page.next_cursor is None:
                return total
            cursor = page.next_cursor


def _complete(
    repository: IdempotencyRepository,
    reservation: IdempotencyReservation,
    *,
    terminal: IdempotencyStatus,
    request: ConfigurationDocument,
    response: RedactedDocument,
    completed_at: UtcTimestamp,
) -> None:
    method = repository.complete if terminal is IdempotencyStatus.COMPLETED else repository.fail
    method(
        reservation,
        request=request,
        response_schema_version=IDEMPOTENT_RESPONSE_SCHEMA_VERSION,
        response=response,
        completed_at=completed_at,
    )


def _stored_outcome(record: IdempotencyRecord) -> CommandOutcome | None:
    if record.response is None or record.response_schema_version != (
        IDEMPOTENT_RESPONSE_SCHEMA_VERSION
    ):
        return None
    mapping = record.response.to_mapping()
    status_code = mapping.get("status_code")
    media_type = mapping.get("media_type")
    body = mapping.get("body")
    if type(status_code) is not int or type(media_type) is not str:
        return None
    if not isinstance(body, dict):
        return None
    return CommandOutcome(
        status_code=status_code,
        media_type=media_type,
        body=cast(Mapping[str, object], body),
        terminal=True,
    )
