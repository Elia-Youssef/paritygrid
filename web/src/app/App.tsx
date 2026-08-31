import { QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Route, Routes } from "react-router";

import { appQueryClient } from "../api/query-client";
import { Overview } from "../features/overview/overview";
import { AppLayout } from "./app-shell";
import { RootErrorBoundary } from "./root-error-boundary";
import { PublicLayout } from "./public-shell";
import { ComparisonScreen } from "./screens/comparison-screen";
import { NotFoundScreen } from "./screens/not-found-screen";
import { OperationsOverviewScreen } from "./screens/operations-overview-screen";
import { PipelineLibraryScreen } from "./screens/pipeline-library-screen";
import { PipelineStudioScreen } from "./screens/pipeline-studio-screen";
import { ReconciliationScreen } from "./screens/reconciliation-screen";
import { RepairReviewScreen } from "./screens/repair-review-screen";
import { RunHistoryScreen } from "./screens/run-history-screen";
import { RunLiveScreen } from "./screens/run-live-screen";
import { SystemScreen } from "./screens/system-screen";

/**
 * Stable application routes. Direct loads and client-side navigation hit
 * the same components; unknown addresses keep their owning layout. The
 * QueryClient provider is the single server-state ownership boundary for
 * everything beneath the shell.
 */
export function App() {
  return (
    <RootErrorBoundary>
      <QueryClientProvider client={appQueryClient}>
        <BrowserRouter>
          <Routes>
            <Route element={<PublicLayout />}>
              <Route index path="/" element={<Overview />} />
              <Route path="*" element={<NotFoundScreen />} />
            </Route>

            <Route path="/app" element={<AppLayout />}>
              <Route index element={<OperationsOverviewScreen />} />
              <Route path="pipelines" element={<PipelineLibraryScreen />} />
              <Route path="pipelines/:pipelineId" element={<PipelineStudioScreen />} />
              <Route path="runs" element={<RunHistoryScreen />} />
              <Route path="runs/:runId" element={<RunLiveScreen />} />
              <Route path="runs/:runId/reconcile" element={<ReconciliationScreen />} />
              <Route path="repairs/:repairId" element={<RepairReviewScreen />} />
              <Route path="compare" element={<ComparisonScreen />} />
              <Route path="system" element={<SystemScreen />} />
              <Route path="*" element={<NotFoundScreen />} />
            </Route>
          </Routes>
        </BrowserRouter>
      </QueryClientProvider>
    </RootErrorBoundary>
  );
}
