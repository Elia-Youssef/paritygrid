# Decision: versioned pipeline document contract

**Status:** accepted
**Date:** 2026-08-14
**Decision scope:** Published pipeline-document syntax, bounds, and version separation
**Supersedes:** None

## Context

Pipeline publication needs one dependency-neutral document boundary before node definitions, graph
validators, persistence coordination, or execution-plan compilation can depend on it. The boundary
must reject ambiguous JSON, bound untrusted collections, preserve immutable publication input, and
keep visual layout distinct from logical planning behavior.

## Decision

Pipeline document schema version 1 is a UTF-8 JSON object with exactly these root fields:

```json
{
  "canonical_format_version": 1,
  "edges": [],
  "layout": [],
  "nodes": [],
  "resource_policy": {},
  "schema_version": 1
}
```

Every node has exactly `id`, `kind`, `configuration_version`, `configuration`, and `connector_id`.
Every edge has exactly `source_node_id`, `source_port`, `target_node_id`, and `target_port`. Every
layout entry has exactly `node_id`, `x`, and `y`. Node, edge, and layout collections are canonicalized
by stable identities. Unknown fields, duplicate JSON keys, floating-point and non-finite numbers,
invalid UTF-8, unsupported version markers, dangling references, and duplicate identities are
rejected.

Version 1 accepts at most 256 nodes, 4,096 edges, a one-mebibyte encoded document, signed 64-bit JSON
integers, and layout coordinates from -1,000,000 through 1,000,000. Configuration objects use the
existing immutable bounded configuration-document contract. Node configuration meaning remains
closed by the node registry rather than by this structural parser.

Visual layout is publication metadata. It is retained in the immutable published document but is
excluded from the logical representation used by plan fingerprinting. The pipeline document schema
version, planner format version, canonical format version, database schema version, and package
version are independent compatibility markers.

## Consequences

- Later validators receive trusted immutable structure rather than raw dictionaries.
- Unknown node kinds remain structurally representable so the closed registry can return the correct
  semantic rejection.
- Arbitrary executable node behavior is not representable through a privileged field; all behavior is
  selected by a closed node kind.
- Layout-only changes can preserve one logical plan fingerprint even when publication history retains
  the exact submitted layout.
- The operational schema, migration, and frozen migration fixture do not change.

## Verification

- Golden bytes lock the complete version 1 example.
- Invalid examples cover duplicate keys, malformed encodings, unsupported numbers and versions,
  unknown fields, dangling references, and collection bounds.
- Generated examples permute node, edge, layout, and object-key declaration order while preserving
  exact canonical bytes.
