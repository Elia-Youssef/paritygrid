import {
  ArrowRight,
  CheckCircle2,
  Database,
  FileInput,
  Fingerprint,
  Network,
  ShieldCheck,
} from "lucide-react";

import { Button } from "../../components/ui/button";
import { StatusBadge } from "../../components/ui/status-badge";

const flowStages = [
  {
    description: "Async HTTP, blocking HTTP, CSV and JSON Lines",
    icon: FileInput,
    label: "Source intake",
  },
  {
    description: "Bounded runners with durable checkpoints",
    icon: Network,
    label: "Execution plan",
  },
  {
    description: "Field conflicts, approved repairs and fingerprints",
    icon: Fingerprint,
    label: "Verified parity",
  },
] as const;

const readinessItems = [
  { detail: "Vite production path", label: "Interface bundle", state: "Ready" },
  { detail: "SQLite + DuckDB", label: "Embedded storage", state: "Local" },
  {
    detail: "Sequential · threads · async",
    label: "Runner contract",
    state: "Planned",
  },
] as const;

export function Overview() {
  return (
    <div id="overview" className="mx-auto max-w-[92rem]">
      <section className="grid gap-10 border-b border-border pb-10 xl:grid-cols-[minmax(0,1fr)_30rem] xl:items-end">
        <div className="max-w-4xl">
          <StatusBadge state="active">System foundation</StatusBadge>
          <h1 className="mt-6 max-w-4xl text-[clamp(2.6rem,6vw,6.5rem)] leading-[0.92] font-semibold tracking-[-0.055em] text-balance">
            Reconciliation you can prove.
          </h1>
          <p className="mt-7 max-w-2xl text-base leading-7 text-muted-strong sm:text-lg sm:leading-8">
            Observe every I/O boundary, recover from interruption, and verify that
            independent execution strategies reach the same logical state.
          </p>
          <div className="mt-8 flex flex-wrap items-center gap-3">
            <Button asChild>
              <a href="#readiness">
                Inspect readiness
                <ArrowRight className="size-4" aria-hidden="true" />
              </a>
            </Button>
            <Button asChild variant="secondary">
              <a href="#execution-path">Trace the execution path</a>
            </Button>
          </div>
        </div>

        <div className="border-l-2 border-active pl-5">
          <p className="font-mono text-xs tracking-[0.14em] text-active uppercase">
            Design principle 01
          </p>
          <p className="mt-3 text-xl leading-8 font-medium text-foreground">
            Correctness is the first metric. Throughput comes after fingerprints agree.
          </p>
        </div>
      </section>

      <div className="grid gap-6 py-8 xl:grid-cols-[minmax(0,1fr)_24rem]">
        <section
          id="execution-path"
          className="overflow-hidden rounded-lg border border-border bg-surface shadow-elevated"
          aria-labelledby="execution-path-title"
        >
          <header className="flex items-center justify-between border-b border-border px-5 py-4 sm:px-6">
            <div>
              <p className="font-mono text-[0.6875rem] tracking-[0.14em] text-muted uppercase">
                Canonical topology
              </p>
              <h2 id="execution-path-title" className="mt-1 text-lg font-semibold">
                Evidence travels with the work
              </h2>
            </div>
            <StatusBadge state="stale">Preview</StatusBadge>
          </header>

          <ol className="grid divide-y divide-border md:grid-cols-3 md:divide-x md:divide-y-0">
            {flowStages.map(({ description, icon: Icon, label }, index) => (
              <li key={label} className="relative p-6 sm:p-7">
                <div className="flex items-center justify-between">
                  <span className="flex size-10 items-center justify-center rounded-md border border-active/30 bg-active/10 text-active">
                    <Icon className="size-5" aria-hidden="true" />
                  </span>
                  <span className="font-mono text-xs text-muted">
                    {String(index + 1).padStart(2, "0")}
                  </span>
                </div>
                <h3 className="mt-8 text-base font-semibold">{label}</h3>
                <p className="mt-2 text-sm leading-6 text-muted">{description}</p>
              </li>
            ))}
          </ol>

          <footer className="grid gap-px border-t border-border bg-border sm:grid-cols-3">
            <div className="bg-surface-quiet p-4">
              <p className="font-mono text-[0.6875rem] text-muted uppercase">
                Delivery
              </p>
              <p className="mt-1 text-sm font-medium">At-least-once execution</p>
            </div>
            <div className="bg-surface-quiet p-4">
              <p className="font-mono text-[0.6875rem] text-muted uppercase">Effects</p>
              <p className="mt-1 text-sm font-medium">Idempotent and reviewed</p>
            </div>
            <div className="bg-surface-quiet p-4">
              <p className="font-mono text-[0.6875rem] text-muted uppercase">
                Verification
              </p>
              <p className="mt-1 text-sm font-medium">Canonical fingerprint</p>
            </div>
          </footer>
        </section>

        <aside
          id="readiness"
          className="rounded-lg border border-border bg-surface p-5 sm:p-6"
          aria-labelledby="readiness-title"
        >
          <div className="flex items-start justify-between gap-4">
            <div>
              <p className="font-mono text-[0.6875rem] tracking-[0.14em] text-muted uppercase">
                Phase 01
              </p>
              <h2 id="readiness-title" className="mt-1 text-lg font-semibold">
                Foundation readiness
              </h2>
            </div>
            <ShieldCheck className="size-5 text-verified" aria-hidden="true" />
          </div>

          <dl className="mt-6 divide-y divide-border border-y border-border">
            {readinessItems.map(({ detail, label, state }) => (
              <div key={label} className="flex items-center justify-between gap-4 py-4">
                <div>
                  <dt className="text-sm font-medium">{label}</dt>
                  <dd className="mt-1 font-mono text-[0.6875rem] text-muted">
                    {detail}
                  </dd>
                </div>
                <dd className="flex items-center gap-1.5 font-mono text-[0.6875rem] text-muted-strong uppercase">
                  <CheckCircle2 className="size-3.5 text-verified" aria-hidden="true" />
                  {state}
                </dd>
              </div>
            ))}
          </dl>

          <div className="mt-5 flex items-start gap-3 rounded-md bg-surface-elevated p-4">
            <Database
              className="mt-0.5 size-4 shrink-0 text-active"
              aria-hidden="true"
            />
            <p className="text-xs leading-5 text-muted-strong">
              The normal demonstration runs locally without a database service or
              container runtime.
            </p>
          </div>
        </aside>
      </div>
    </div>
  );
}
