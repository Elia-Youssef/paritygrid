# Media inventory

Every tracked media asset in the repository, as required by the repository
policy and `scripts/validate_instructions.ps1`. Each entry records its
source, purpose, deterministic regeneration command, and drift policy. The
repository currently tracks only the Phase 18 visual regression baselines;
no public demo media, marketing assets, or narrative screenshots exist.

## Phase 18 visual regression baselines

| Asset | Canonical state |
|---|---|
| `web/e2e/phase18-visual.spec.ts-snapshots/p18-reconciliation-summary-linux.png` | Linux Chromium reconciliation summary with the first conflict page |
| `web/e2e/phase18-visual.spec.ts-snapshots/p18-reconciliation-summary-win32.png` | Reconciliation summary with the first conflict page |
| `web/e2e/phase18-visual.spec.ts-snapshots/p18-conflict-table-inspector-linux.png` | Linux Chromium large conflict table with a keyboard-selected conflict in the inspector |
| `web/e2e/phase18-visual.spec.ts-snapshots/p18-conflict-table-inspector-win32.png` | Large conflict table (two pages loaded, filtered) with a keyboard-selected conflict in the inspector |
| `web/e2e/phase18-visual.spec.ts-snapshots/p18-reconciliation-empty-linux.png` | Linux Chromium coherent reconciliation snapshot with zero conflicts |
| `web/e2e/phase18-visual.spec.ts-snapshots/p18-reconciliation-empty-win32.png` | Coherent reconciliation snapshot with zero conflicts |
| `web/e2e/phase18-visual.spec.ts-snapshots/p18-repair-proposed-review-linux.png` | Linux Chromium proposed repair plan review before approval |
| `web/e2e/phase18-visual.spec.ts-snapshots/p18-repair-proposed-review-win32.png` | Proposed repair plan review before approval |
| `web/e2e/phase18-visual.spec.ts-snapshots/p18-repair-stale-blocked-linux.png` | Linux Chromium stale plan blocked from approval and application |
| `web/e2e/phase18-visual.spec.ts-snapshots/p18-repair-stale-blocked-win32.png` | Stale plan blocked from approval and application |
| `web/e2e/phase18-visual.spec.ts-snapshots/p18-repair-applying-progress-linux.png` | Linux Chromium application in progress with durable notifications and a recoverable unresolved outcome |
| `web/e2e/phase18-visual.spec.ts-snapshots/p18-repair-applying-progress-win32.png` | Application in progress with durable notifications and a recoverable unresolved outcome |
| `web/e2e/phase18-visual.spec.ts-snapshots/p18-repair-completed-linux.png` | Linux Chromium authoritative completed application with apply permanently disabled |
| `web/e2e/phase18-visual.spec.ts-snapshots/p18-repair-completed-win32.png` | Authoritative completed application with apply permanently disabled |

Shared properties of all seven baselines:

- **Source and generation method:** rendered by the production build of the
  frontend (`npm --prefix web run build`) served through the configured
  Playwright preview server, with every API, SSE, and WebSocket dependency
  intercepted by the deterministic fixtures in
  `web/e2e/fixtures.ts` and `web/e2e/fixtures-p18.ts`. The captures are
  element-scoped screenshots taken by
  `npm --prefix web run test:browser -- e2e/phase18-visual.spec.ts`.
- **Purpose:** P18.6 deterministic visual regression baselines. They lock
  the canonical Phase 18 states against unintended visual drift.
- **Determinism contract:** fixed synthetic data with fixed timestamps; a
  declared 1280×800 viewport; declared dark color scheme; reduced motion;
  CSS animations disabled for capture; hidden caret; no live network
  dependency; no random identifiers; stable redaction and ordering. No
  wall-clock value is rendered by the captured screens.
- **Drift policy:** regenerate only for an intentional, reviewed change to
  a canonical state, and only after the focused unit and browser suites
  pass. Clean reruns without regeneration must pass twice in a row; any
  unexplained pixel difference is a defect to fix, never a threshold to
  loosen. The platform suffix (`-win32`) records the rendering environment
  of the baseline; a new platform adds its own baseline rather than
  replacing this one.
- **Regeneration:** delete the specific baseline file, then run
  `npm --prefix web run test:browser -- e2e/phase18-visual.spec.ts` once to
  write the new baseline, then run the same command twice without changes
  and require both clean reruns to pass.
