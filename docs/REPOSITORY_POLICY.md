# Repository policy

## Purpose

The public repository must contain only durable product source, tests, synthetic fixtures, public technical documentation, reproducible assets, and build configuration.

## Prohibited committed content

- Internal coordination instructions.
- Conversational transcripts.
- Development-assistance attribution or provenance.
- Temporary planning scratchpads.
- Confidential client, employer, production-system, or operational details.
- Production-derived data, identifiers, schemas, screenshots, or metrics.
- Credentials, tokens, private keys, cookies, or resolved secret values.
- Local databases, WAL files, DuckDB caches, or generated Parquet run artifacts.
- Coverage reports, browser reports, logs, temporary files, or local environments.
- Unreviewed generated source or documentation.

No Markdown file may contain internal development-assistance terminology. No special instruction file for such tooling may be tracked. Public documentation is written directly for maintainers, contributors, evaluators, and users.

## Allowed documentation

- Product charter and deliberate boundaries.
- Architecture and data contracts.
- Engineering and testing standards.
- Public work packages and acceptance criteria.
- Human contribution guidance.
- Code tour and verification report.
- Threat model and responsible-disclosure information.
- Decision records.
- README and release notes.

## Code-comment policy

Comments and documentation strings must be human-facing. They may explain invariants, ordering, synchronization, compatibility, safety, protocols, and non-obvious tradeoffs. They must not record how the code was produced, narrate a conversation, or refer to automated authorship.

## Generated files

A generated file may be committed only when it is required for the no-build runtime experience or public contract review. It must have:

- A deterministic generation command.
- A source of truth.
- A CI drift check.
- A compatible license.
- No local path, secret, timestamp, or nondeterministic identifier unless the format requires one and it is normalized.

Expected generated assets include the production frontend, OpenAPI contract, generated TypeScript API types, diagrams exported for the README, and canonical screenshots.

## Synthetic-data standard

- Fixtures are created from documented deterministic generators.
- Names and identifiers are fictional.
- Values do not preserve a confidential dataset's distribution when that distribution is sensitive.
- The canonical scenario includes a seed and expected manifest.
- Screenshot data comes only from reviewed canonical fixtures.

## Tracked-file audit

Before accepting a phase or release:

1. List every tracked file.
2. Reject prohibited filenames and extensions.
3. Scan Markdown for prohibited internal terminology.
4. Scan text for credential patterns and absolute developer paths.
5. Verify generated files reproduce from documented commands.
6. Verify every media asset has an identified purpose and source.
7. Verify ignore rules cover runtime and test output.

## Git history

- Commits should be scoped and explain the product change.
- Do not rewrite shared history to conceal a known security issue; follow responsible remediation.
- Never commit a secret with the expectation that a later deletion makes it safe.
- Release tags are created only after the release verification lane passes.
