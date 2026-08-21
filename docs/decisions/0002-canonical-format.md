# Decision: versioned canonical state encoding

**Status:** accepted
**Date:** 2026-08-12
**Decision scope:** Canonical domain encodings and logical-state fingerprints
**Supersedes:** None

## Context

ParityGrid compares logical results produced by sequential, threaded, and asynchronous execution. Equality must not depend on arrival order, dictionary insertion order, local time settings, ambient decimal precision, process hash randomization, or a serializer default. Repair-plan comparison must also ignore generated plan, action, and conflict identities while retaining the business effect and exact reconciliation state.

The encoding is a public data-integrity contract. Its version is independent of the Python package, database schema, Parquet schema, and pipeline document versions.

## Decision

Canonical format version 1 uses a closed registry of exact domain types. Each adapter produces one ASCII JSON envelope with exactly these fields:

```json
{"format":"paritygrid-canonical","type":"pipeline-version","value":1,"version":1}
```

Keys are sorted, separators contain no optional whitespace, non-ASCII text uses JSON escapes, and no newline or byte-order mark is appended. Type tags are stable protocol names rather than Python class names. Floating-point values and unregistered primitives, containers, dataclasses, and subclasses are rejected. Money retains signed integer minor units, currency, and minor-unit exponent.

Logical state fingerprints use SHA-256. Each canonical item is length-framed with an unsigned eight-byte big-endian length and hashed with the domain prefix `paritygrid:canonical-leaf:v1` followed by a null byte. Leaf digests are sorted so arrival order is irrelevant; repeated leaf digests remain repeated so duplicate multiplicity is preserved.

The root preimage contains the null-terminated domain prefix `paritygrid:canonical-state:v1`, the length-framed ASCII scope, an unsigned eight-byte item count, and the sorted leaf digests. The final value is the 64-character lowercase hexadecimal SHA-256 digest. Version 1 defines separate scopes for inventory state, reconciliation state, and repair-plan content.

Repair-plan content fingerprints include the exact reconciliation fingerprint, action kind, proposed business state, expected target business state, and mismatch evidence. They exclude plan, action, and conflict identifiers as well as connector and source-record provenance that does not change the target effect. The exact reconciliation fingerprint retains the complete observed source and target state. Full canonical encodings of repair values retain identities and record provenance for persistence and audit uses.

Fingerprint inputs are snapshotted once and bounded to 10,000 items. Empty states are valid. Each scope accepts only its declared exact value type.

## Consequences

- Required runners can compare logical results without depending on completion order.
- Duplicate records remain observable rather than being silently collapsed.
- A format or hashing change requires a new explicit canonical version and new golden vectors.
- Adding a domain type requires an explicit adapter, stable type tag, and golden tests.
- Cross-language implementations must reproduce the documented JSON profile and length framing exactly.
- Large future datasets may require a bounded external sort of leaf digests while preserving the same root construction.

## Alternatives considered

- Default JSON serialization was rejected because serializer options, reflection, and implicit type conversion do not define a sufficiently narrow protocol.
- Hashing insertion order was rejected because concurrent runners complete work in different orders.
- Deduplicating before hashing was rejected because duplicate multiplicity is reconciliation evidence.
- Including generated repair identities in logical plan fingerprints was rejected because equivalent plans may receive different identifiers.
- A commutative arithmetic digest was rejected because sorted cryptographic leaves provide clearer collision and multiplicity behavior.

## Verification

- Exact golden bytes cover every supported type family and closed variant.
- Exact digest vectors cover empty, singleton, multi-item, duplicate, and every scope.
- Property tests permute state arrival order and change semantic fields.
- Tests vary dictionary insertion, Unicode text, decimal context, process hash seed, and concurrent callers.
- Windows and Linux continuous integration run the same locked vectors.
