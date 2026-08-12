# UI specification

## Product experience

The interface must feel like an operational control room rather than a generic administration template. It should make correctness, flow, contention, failure, and recovery visible without overwhelming the user.

The UI has two presentation contexts:

- A concise public overview at `/` that explains and launches the canonical demo.
- The operations console under `/app`.

Both use the same design tokens, Tailwind CSS foundation, and component source.

## Selected stack

| Concern | Technology |
|---|---|
| Application | React and TypeScript |
| Build | Vite |
| Styling | Tailwind CSS 4 |
| Component foundation | Owned and customized shadcn/ui source |
| Pipeline graph | React Flow |
| Server state | TanStack Query |
| Large tables | TanStack Table |
| Forms | React Hook Form and Zod |
| Charts | Apache ECharts |
| Routing | React Router |
| Icons | Lucide |
| Component tests | Vitest and Testing Library |
| End-to-end tests | Playwright |

Dependencies must be pinned through the frontend lockfile. The implementation phase records exact supported versions.

## Visual language

- Dark-first graphite and deep-navy surfaces.
- Cyan for active work and data movement.
- Green for verified parity and successful durable commit.
- Amber for retrying, stale, waiting, or constrained state.
- Red only for failed, unsafe, or destructive states.
- Monospaced typography for identifiers, hashes, amounts, counts, timestamps, and durations.
- Sans-serif typography for navigation and explanatory copy.
- Restrained motion tied to actual execution state.
- High information density with clear hierarchy.
- Avoid decorative gradients, excessive glass effects, and generic dashboard cards.

## Design tokens

Tokens must use semantic CSS variables and cover:

- Background and foreground.
- Surface and elevated surface.
- Muted content.
- Borders and focus rings.
- Primary and secondary actions.
- Active, verified, warning, failure, stale, paused, and cancelled states.
- Chart series and runner identities.
- Queue saturation bands.
- Typography scale.
- Spacing scale.
- Radius scale.
- Shadow and elevation.
- Motion duration and easing.

Feature code must not introduce arbitrary state colors outside the design layer.

## Routes

```text
/                         Project overview and canonical story
/app                      Operations overview
/app/pipelines            Pipeline library
/app/pipelines/:id        Pipeline studio
/app/runs                 Run history
/app/runs/:id             Live execution
/app/runs/:id/reconcile   Reconciliation workbench
/app/repairs/:id          Repair review
/app/compare              Runner comparison
/app/system               Capabilities and storage health
```

## Application shell

The shell contains:

- Persistent navigation.
- Current environment and data-root indicator.
- Global connection status.
- Current run context where applicable.
- Command palette.
- Accessible theme control if a light theme is retained.
- Breadcrumbs on nested operational screens.

Navigation must remain useful at reduced desktop widths. The primary target is a desktop operational interface; narrow layouts must remain readable but do not need to reproduce every dense visualization simultaneously.

## Operations overview

Display:

- Recent runs.
- Active and queued work.
- Verified, failed, paused, and stale counts.
- Current database and artifact health.
- Runner availability.
- A direct launch path for the canonical scenario.

Summary cards must link to supporting detail. No metric should be presented without a stable definition and source.

## Pipeline studio

Use React Flow with typed custom nodes and edges.

Required behavior:

- Drag, connect, select, delete, and configure nodes.
- Validate compatible ports while connecting.
- Display invalid configuration at the affected node.
- Provide minimap, fit-view, zoom, and keyboard operations.
- Separate visual layout changes from logical pipeline-version changes.
- Show runner compatibility and resource limits.
- Preview the compiled plan before publication.
- Prevent repair application without an upstream approval gate.

Node visuals communicate type, connector, status, record count, duration, retry count, and saturation when a run overlay is active.

## Live execution

The screen combines:

- Pipeline graph with durable state overlay.
- Worker swimlane or Gantt timeline.
- Queue-depth and capacity panels.
- Event timeline.
- Node and attempt inspector.
- Pause, resume, and cancel controls.
- Connection-stale indicator.

The UI must distinguish durable state from ephemeral telemetry. A disconnected WebSocket may stale queue charts, but durable run state continues through REST and SSE.

## Reconciliation workbench

Required areas:

- Classification summary.
- Filterable and virtualized conflict table.
- Source and target record comparison.
- Field-level differences.
- Raw-payload reference when available.
- Suggested action and rationale.
- Multi-selection for compatible repair actions.
- Fingerprint and data-version context.

The table must support large conflict sets without loading every raw payload into browser memory.

## Repair review

The repair screen must show:

- Exact reconciliation fingerprint.
- Action counts by type.
- Before and proposed-after values.
- Risks and excluded destructive actions.
- Approval status and audit context.
- Explicit confirmation before application.
- Progress and idempotent completion results.

A stale repair plan is visibly blocked and cannot be applied.

## Runner comparison

Compare only compatible runs. Display correctness before speed:

1. Fingerprint equivalence.
2. Accepted and quarantined equivalence.
3. Repair-effect equivalence.
4. Total duration and throughput.
5. p50, p95, and p99 task latency.
6. Queue wait and active service time.
7. Retry counts.
8. Peak memory and in-flight work.
9. SQLite transaction latency.
10. DuckDB analytical query duration.

Charts must include units, accessible labels, and a tabular alternative.

## State coherence

REST responses, SSE events, and telemetry snapshots carry versions. The client must:

- Ignore older events after newer durable state.
- Detect gaps in durable event sequences and reconnect.
- Refetch after an unrecoverable gap.
- Keep the last coherent state visible while marking it stale.
- Never merge conflicts and summaries from different reconciliation fingerprints.
- Surface connection recovery without discarding user selection where safe.

## Accessibility

- Meet WCAG 2.1 AA contrast and interaction expectations.
- Provide visible focus.
- Support keyboard navigation for all controls.
- Use semantic landmarks, headings, dialogs, and tables.
- Provide accessible names for icon-only buttons.
- Provide non-color state indicators.
- Support reduced motion.
- Provide text alternatives for charts and diagrams.
- Maintain a logical tab sequence in the pipeline studio.

## UI testing

Component tests cover:

- Loading, empty, success, error, stale, paused, and disconnected states.
- Pipeline validation.
- Conflict selection and filtering.
- Approval confirmation.
- Event reducer ordering and gap detection.
- SSE reconnect logic.
- WebSocket telemetry updates.
- Keyboard interaction.

Playwright covers:

- Launching the canonical run.
- Watching retry and recovery.
- Refreshing during an active run.
- Inspecting and filtering conflicts.
- Creating, approving, and applying a repair plan.
- Verifying parity.
- Comparing required runners.
- Pause, resume, and cancel.
- Chromium on each change; Chromium, Firefox, and WebKit for release.

Stable canonical states produce reproducible screenshots for visual regression and public documentation.
