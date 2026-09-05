# Security model

## Scope

ParityGrid is a local engineering demonstration, not an internet-facing production service. Security controls still protect the host, local files, secrets, and demonstration integrity.

## Default network posture

- Bind to `127.0.0.1` by default.
- Non-loopback binds are refused by configuration validation: the
  loopback-only posture is enforced by the accepted settings contract,
  which is stricter than an opt-in acknowledgement, so the application
  cannot be started on an interface it was not designed for.
- Cross-origin access is disabled by default.
- Synthetic source systems also bind to loopback.

## File access

- All file connectors operate within configured allowlisted roots.
- Resolve and validate absolute paths before reading, writing, moving, or deleting.
- Reject traversal, alternate data streams, unsafe symlink escapes, and malformed archive paths.
- Demo cleanup operates only on the exact generated demo directory.
- Artifact download routes resolve through manifest IDs, not arbitrary user paths.
- Upload sizes, record counts, decompressed sizes, and artifact totals are bounded.

The current product has no upload endpoint and no archive-extraction
surface: the bounds above apply today to artifact writing, connector
file readers, HTTP request bodies, and demo roots. They remain
requirements for any future surface of those kinds. The
[threat-model map](THREAT_MODEL.md) records each structural absence.

## Secrets

- Connector configurations persist environment-variable names, not values.
- Secret values exist only in memory for the shortest practical duration.
- Authorization, cookie, token, key, password, and secret fields are redacted in logs, events, traces, errors, and exports.
- The repository contains only an `.env.example` with placeholders.
- Synthetic demonstration credentials are clearly non-production and scoped to loopback simulators.

## Input validation

- Treat every HTTP response, upload, file, pipeline specification, and database row read from a legacy schema as untrusted.
- Enforce content type and size before parsing where possible.
- Validate JSON shape with bounded error output.
- Limit nesting depth and collection size for flexible metadata.
- Use bound SQL parameters.
- Avoid rendering upstream text as HTML.
- Validate content-disposition filenames.

## Pipeline safety

- No arbitrary Python, JavaScript, shell, SQL, or template execution node.
- Connector kinds come from a closed registry.
- Pipeline graphs are validated before publication.
- Repair application requires an upstream approval gate.
- Target delete actions are not supported in the initial release.
- Effects use stable idempotency keys.

## API protections

- Apply strict command and upload limits.
- Use Problem Details without stack traces or sensitive context.
- Validate correlation and idempotency header lengths and formats.
- Use secure response headers and a Content Security Policy for the packaged UI.
- Prevent MIME sniffing.
- Do not expose directory listings.
- Keep API documentation local and configurable. The documentation page
  currently loads FastAPI's pinned swagger assets from a CDN origin under
  the narrow documentation policy recorded in the threat-model map;
  self-hosting those assets is the recorded remediation direction.

## WebSocket and SSE protections

- Validate run access before upgrading or streaming.
- Bound subscriber queues.
- Disconnect slow clients.
- Heartbeats contain no sensitive data.
- Client messages have strict schemas and size limits.
- Stream disconnects never change authoritative run state.

## Data integrity

- Use full SQLite durability settings for authoritative commits.
- Store content hashes for artifacts.
- Verify manifest-file consistency.
- Do not treat DuckDB cache state as authoritative.
- Detect stale repair plans by exact reconciliation fingerprint.
- Keep durable events append-only through application interfaces.

## Dependency and build security

- Pin Python and frontend dependencies with lockfiles.
- Run dependency audits in main, nightly, and release verification.
- Record third-party licenses.
- Build the frontend from the committed lockfile.
- Verify the packaged frontend matches a clean rebuild.
- Do not execute downloaded scripts during the normal demo.

## Threat scenarios to test

- Path traversal in source and artifact names.
- Symlink escape from an allowed root.
- Oversized or decompression-bomb input.
- Malformed deeply nested JSON.
- Secret fields in upstream error responses.
- Replayed mutating command.
- Reused idempotency key with a different request.
- Stale repair approval.
- Concurrent repair application.
- Slow SSE and WebSocket clients.
- Non-loopback startup attempt refused by configuration validation.
- Untrusted strings in the browser.

## Reporting

Before public release, add a responsible-disclosure contact appropriate to the repository owner. Do not claim formal security certification or production readiness.

## Threat-model verification

The threat/control/test map that binds each abuse case above to its
owning control and reproducible verification is maintained in
[the threat-model verification map](THREAT_MODEL.md). Missing evidence
there is a blocking finding.
