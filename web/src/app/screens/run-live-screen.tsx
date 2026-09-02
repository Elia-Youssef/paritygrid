import { useParams } from "react-router";

import { RunLive } from "../../features/runs/run-live";

export function RunLiveScreen() {
  const { runId = "" } = useParams();
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="border-b border-border px-4 py-2">
        <h1
          data-page-title
          tabIndex={-1}
          className="text-sm font-semibold text-foreground"
        >
          Live execution
        </h1>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto p-4">
        <RunLive runId={runId} />
      </div>
    </div>
  );
}
