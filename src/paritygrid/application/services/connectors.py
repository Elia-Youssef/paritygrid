"""Connector registration and bounded connection-test use cases."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from paritygrid.application.planner.connectors import (
    InvalidConnectorSnapshotError,
    connector_capabilities_from_document,
)
from paritygrid.application.ports.configuration import (
    ConfigurationDocument,
    ConnectorPage,
    ConnectorRecord,
    ConnectorSecretReference,
    DuplicateRecordError,
)
from paritygrid.application.ports.connectors import ConnectorKind
from paritygrid.application.ports.operations import OperationalUnitOfWork
from paritygrid.application.services.errors import (
    OperationalRecordNotFoundError,
    OperationalRequestError,
)
from paritygrid.domain.models import ConnectorId, UtcTimestamp

MAX_CONNECTOR_DISPLAY_NAME_LENGTH = 128
MAX_SECRET_REFERENCES = 64
MAX_TEST_CHECKS = 8
MAX_TEST_DETAIL_LENGTH = 256
_REGISTERED_KINDS = frozenset(kind.value for kind in ConnectorKind)


class EnvironmentVariableLookup(Protocol):
    """Resolve whether a named environment variable exists, never its value."""

    def has(self, name: str) -> bool: ...


@dataclass(frozen=True, slots=True)
class ConnectorTestCheck:
    """One bounded configuration-level connection check."""

    name: str
    passed: bool
    detail: str


@dataclass(frozen=True, slots=True)
class ConnectorTestReport:
    """Bounded connection-test evidence without secret values."""

    connector_id: str
    kind: str
    passed: bool
    checks: tuple[ConnectorTestCheck, ...]
    tested_at: UtcTimestamp


class ConnectorService:
    """Register and inspect connectors with secret references only."""

    def __init__(
        self,
        *,
        unit_of_work: OperationalUnitOfWork,
        now: Callable[[], UtcTimestamp],
    ) -> None:
        self._unit_of_work = unit_of_work
        self._now = now

    def register(
        self,
        *,
        connector_id: str,
        kind: str,
        display_name: str,
        configuration: Mapping[str, object],
        capabilities: Mapping[str, object],
        schema_discovery: Mapping[str, object] | None,
        secret_references: Sequence[tuple[str, str]],
        converge_on_duplicate: bool = False,
    ) -> ConnectorRecord:
        if kind not in _REGISTERED_KINDS:
            raise OperationalRequestError(
                "connector kind is not part of the closed registry",
                field="kind",
            )
        if type(display_name) is not str or not 1 <= len(display_name) <= (
            MAX_CONNECTOR_DISPLAY_NAME_LENGTH
        ):
            raise OperationalRequestError(
                "display name must be 1 to 128 characters", field="display_name"
            )
        if len(secret_references) > MAX_SECRET_REFERENCES:
            raise OperationalRequestError(
                f"at most {MAX_SECRET_REFERENCES} secret references are accepted",
                field="secret_references",
            )
        references = tuple(
            ConnectorSecretReference(reference_name, environment_variable_name)
            for reference_name, environment_variable_name in secret_references
        )
        identity = _connector_id(connector_id)
        configuration_document = _document(configuration, "configuration")
        capabilities_document = _document(capabilities, "capabilities")
        discovery_document = (
            None if schema_discovery is None else _document(schema_discovery, "schema_discovery")
        )
        try:
            with self._unit_of_work.transaction() as repositories:
                return repositories.connectors.create(
                    connector_id=identity,
                    kind=kind,
                    display_name=display_name,
                    configuration=configuration_document,
                    capabilities=capabilities_document,
                    schema_discovery=discovery_document,
                    secret_references=references,
                    created_at=self._now(),
                )
        except DuplicateRecordError:
            if not converge_on_duplicate:
                raise
            with self._unit_of_work.transaction() as repositories:
                existing = repositories.connectors.get(identity)
            if (
                existing is not None
                and existing.kind == kind
                and existing.display_name == display_name
                and existing.configuration == configuration_document
                and existing.capabilities == capabilities_document
                and existing.schema_discovery == discovery_document
                and existing.secret_references == references
            ):
                return existing
            raise

    def list(
        self,
        *,
        limit: int,
        after: str | None,
        include_archived: bool,
    ) -> ConnectorPage:
        cursor = None if after is None else _connector_id(after)
        with self._unit_of_work.transaction() as repositories:
            return repositories.connectors.list(
                limit=limit,
                after=cursor,
                include_archived=include_archived,
            )


class ConnectorTestService:
    """Run strictly bounded configuration-level connection checks.

    The checks never open network connections and never read secret values:
    they validate the closed kind registry, the capability document contract,
    and that each referenced environment variable exists by name.  Live
    transport probes belong to the connector contracts, not this boundary.
    """

    def __init__(
        self,
        *,
        unit_of_work: OperationalUnitOfWork,
        environment: EnvironmentVariableLookup,
        now: Callable[[], UtcTimestamp],
    ) -> None:
        self._unit_of_work = unit_of_work
        self._environment = environment
        self._now = now

    def test(self, connector_id: str) -> ConnectorTestReport:
        with self._unit_of_work.transaction() as repositories:
            record = repositories.connectors.get(_connector_id(connector_id))
        if record is None:
            raise OperationalRecordNotFoundError("connector", connector_id)
        checks: list[ConnectorTestCheck] = []
        checks.append(_check_kind(record.kind))
        checks.append(_check_capabilities(record.capabilities.to_mapping()))
        checks.extend(_check_secret_references(record, self._environment))
        del checks[MAX_TEST_CHECKS:]
        return ConnectorTestReport(
            connector_id=record.connector_id.value,
            kind=record.kind,
            passed=all(check.passed for check in checks),
            checks=tuple(checks),
            tested_at=self._now(),
        )


def _connector_id(value: str) -> ConnectorId:
    try:
        return ConnectorId.parse(value)
    except ValueError as error:
        raise OperationalRequestError(
            "connector identity must use the canonical connector format",
            field="connector_id",
        ) from error


def _document(value: Mapping[str, object], field: str) -> ConfigurationDocument:
    try:
        return ConfigurationDocument.from_mapping(value)
    except Exception as error:
        raise OperationalRequestError(
            f"{field} is not a valid bounded configuration document: {error}",
            field=field,
        ) from error


def _check_kind(kind: str) -> ConnectorTestCheck:
    passed = kind in _REGISTERED_KINDS
    return ConnectorTestCheck(
        name="kind_registered",
        passed=passed,
        detail="connector kind belongs to the closed registry"
        if passed
        else "connector kind is not registered",
    )


def _check_capabilities(capabilities: Mapping[str, object]) -> ConnectorTestCheck:
    try:
        document = ConfigurationDocument.from_mapping(capabilities)
        connector_capabilities_from_document(document)
    except InvalidConnectorSnapshotError:
        return ConnectorTestCheck(
            name="capabilities_contract",
            passed=False,
            detail="capabilities document does not match the v1 contract",
        )
    except Exception:
        return ConnectorTestCheck(
            name="capabilities_contract",
            passed=False,
            detail="capabilities document is not representable",
        )
    return ConnectorTestCheck(
        name="capabilities_contract",
        passed=True,
        detail="capabilities document satisfies the v1 contract",
    )


def _check_secret_references(
    record: ConnectorRecord, environment: EnvironmentVariableLookup
) -> list[ConnectorTestCheck]:
    if not record.secret_references:
        return [
            ConnectorTestCheck(
                name="secret_references_resolvable",
                passed=True,
                detail="connector declares no secret references",
            )
        ]
    missing = sorted(
        reference.environment_variable_name
        for reference in record.secret_references
        if not environment.has(reference.environment_variable_name)
    )
    detail = (
        "every referenced environment variable exists"
        if not missing
        else "missing environment variables: " + ", ".join(missing)
    )
    return [
        ConnectorTestCheck(
            name="secret_references_resolvable",
            passed=not missing,
            detail=detail[:MAX_TEST_DETAIL_LENGTH],
        )
    ]
