import { RunList } from "../../features/runs/run-list";

export function RunHistoryScreen() {
  return (
    <div className="min-h-0 flex-1 overflow-y-auto p-6">
      <h1
        data-page-title
        tabIndex={-1}
        className="text-lg font-semibold text-foreground"
      >
        Run history
      </h1>
      <p className="mt-1 max-w-2xl text-sm text-muted">
        Durable runs with URL-backed filters, deterministic pagination, and selection.
        Live execution detail lives on each run's own page.
      </p>
      <div className="mt-4">
        <RunList />
      </div>
    </div>
  );
}
