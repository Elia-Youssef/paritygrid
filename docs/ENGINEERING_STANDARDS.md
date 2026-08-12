# Engineering standards

## General principles

- Prefer small modules with explicit responsibilities.
- Keep domain decisions separate from transport and storage details.
- Make invalid states difficult to construct.
- Use typed values instead of unstructured dictionaries after validation boundaries.
- Treat external data as untrusted.
- Keep resource limits explicit and testable.
- Favor deterministic behavior and reproducible fixtures.
- Fail clearly rather than silently repairing ambiguous state.

## Python baseline

- Target Python 3.14.
- Manage environments and commands with `uv`.
- Use a `src` package layout.
- Enable strict Pyright checking.
- Use Ruff for linting and formatting.
- Use modern type syntax and `from __future__ import annotations` only where needed.
- Prefer immutable dataclasses or focused value classes in the domain.
- Use Pydantic only at configuration and transport boundaries unless a domain need is documented.
- Use SQLAlchemy 2.x style exclusively.

## Type rules

- Public functions and methods must have complete annotations.
- Do not use `Any` to bypass design work; isolate unavoidable dynamic data at boundaries.
- Do not expose raw dictionaries when a stable schema exists.
- Use `Protocol` for infrastructure ports.
- Use discriminated unions for node, event, result, and failure variants.
- Exhaustively handle closed variants and test the fallback for unsupported versions.

## Async and concurrency rules

- Never perform blocking I/O directly on the event-loop thread.
- Never hold a database transaction across network or slow file I/O.
- Never share `Session` or `AsyncSession` instances concurrently.
- Use structured task groups for related asynchronous work.
- Use bounded queues and capacity limiters.
- Propagate cancellation.
- Close clients, streams, and executors through explicit lifecycle management.
- Record why a blocking operation is moved to a thread.
- Avoid nested work submission that can exhaust a bounded pool.

## Error handling

- Domain and application failures use typed exceptions or result variants.
- Preserve useful causal context internally.
- Redact sensitive or oversized detail before persistence and transport.
- Do not catch broad exceptions without translating, recording, or re-raising them.
- Cancellation exceptions must not be converted into ordinary failures.
- Retry decisions must be made by a named policy, not scattered conditionals.

## Logging and telemetry

- Use structured logs.
- Include correlation ID, run ID, node ID, work-item ID, and attempt where applicable.
- Avoid duplicate logging at multiple layers for one failure.
- Never log secrets, authorization headers, cookies, raw credentials, or unbounded payloads.
- Metrics use stable names and documented units.
- Traces separate scheduling, queue wait, connector I/O, artifact commit, database commit, and analytical query spans.

## Database rules

- Repository methods return domain or application objects, not ORM rows.
- Transactions are short and scoped to one use case or writer batch.
- Schema invariants belong in database constraints when representable.
- Every migration has upgrade tests.
- Raw SQL is allowed for clear analytical or set-based operations and must use bound parameters.
- N+1 query behavior must be tested or measured on list endpoints.

## Frontend baseline

- Use TypeScript strict mode.
- Keep API types generated from the OpenAPI contract where possible.
- Organize product behavior by feature rather than by primitive file type alone.
- Keep reusable presentational components separate from feature orchestration.
- Use TanStack Query for server state and avoid duplicating authoritative server data into a global client store.
- Use Zod at untrusted runtime boundaries where generated types do not provide runtime validation.
- Use semantic design tokens rather than feature-specific color literals.
- Use stable query keys and explicit invalidation.

## Comments and documentation strings

Comments must be written for maintainers and users of the code. They should explain:

- Why an invariant exists.
- Why operation ordering is important.
- Why a boundary or synchronization primitive is required.
- What safety or compatibility guarantee a non-obvious block provides.
- What a public interface promises.

Do not narrate obvious syntax, repeat the function name, record development conversation, or include generated-work provenance. Remove obsolete comments in the same change that makes them obsolete.

Public modules, ports, and non-obvious algorithms should have concise documentation strings. Internal helpers need them only when the contract is not clear from types and naming.

## Naming

- Use domain language consistently: pipeline, run, node, work item, attempt, checkpoint, artifact, conflict, repair plan, and fingerprint.
- Avoid vague names such as manager, helper, utility, processor, or data when a precise name exists.
- Use verbs for commands and operations.
- Use nouns for values and repositories.
- Event names use completed facts such as `checkpoint_committed` rather than commands.

## Module size and cohesion

There is no mechanical line limit, but a module should be split when it owns multiple unrelated reasons to change. New modules should not become dumping grounds for miscellaneous helpers.

Circular imports are prohibited. Resolve them through better boundaries rather than runtime import tricks.

## Tests as code

- Tests follow the same formatting and typing expectations as production code where practical.
- Test names describe behavior and expected outcome.
- Prefer behavior assertions over implementation details.
- Use real SQLite and DuckDB files for integration behavior.
- Seed nondeterminism and record seeds on failure.
- Keep canonical fixtures reviewed and versioned.
- Do not weaken assertions to accommodate flaky behavior; remove the source of flakiness.

## Dependency policy

Add a dependency only when it:

- Solves a clearly named requirement.
- Has a compatible license.
- Is actively maintained enough for the project risk.
- Does not duplicate an existing dependency.
- Can be pinned and reproduced.

Record substantial infrastructure choices in a decision document.

## Change acceptance checklist

- Scope matches one or more work packages.
- Public behavior and contracts are updated.
- Static checks pass.
- Risk-appropriate tests pass.
- Failure and cancellation paths are covered.
- Resource bounds remain explicit.
- Documentation is accurate.
- No secret, local data, or generated runtime artifact is tracked.
- Repository-content rules pass.
