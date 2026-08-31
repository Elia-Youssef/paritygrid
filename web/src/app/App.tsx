import { QueryClientProvider } from "@tanstack/react-query";
import { createBrowserRouter, RouterProvider } from "react-router";
import { useMemo } from "react";

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
 * Stable application routes. A data router is required so route-bound
 * guards (the studio's unsaved-draft blocker) can intercept navigation;
 * direct loads and client-side navigation hit the same components and
 * unknown addresses keep their owning layout.
 */
const routeTree = [
  {
    path: "/",
    element: <PublicLayout />,
    children: [
      { index: true, element: <Overview /> },
      { path: "*", element: <NotFoundScreen /> },
    ],
  },
  {
    path: "/app",
    element: <AppLayout />,
    children: [
      { index: true, element: <OperationsOverviewScreen /> },
      { path: "pipelines", element: <PipelineLibraryScreen /> },
      { path: "pipelines/:pipelineId", element: <PipelineStudioScreen /> },
      { path: "runs", element: <RunHistoryScreen /> },
      { path: "runs/:runId", element: <RunLiveScreen /> },
      { path: "runs/:runId/reconcile", element: <ReconciliationScreen /> },
      { path: "repairs/:repairId", element: <RepairReviewScreen /> },
      { path: "compare", element: <ComparisonScreen /> },
      { path: "system", element: <SystemScreen /> },
      { path: "*", element: <NotFoundScreen /> },
    ],
  },
];

export function App() {
  // Created per mount so tests and direct loads observe the live document
  // location; the tree itself is static.
  const router = useMemo(() => createBrowserRouter(routeTree), []);
  return (
    <RootErrorBoundary>
      <QueryClientProvider client={appQueryClient}>
        <RouterProvider router={router} />
      </QueryClientProvider>
    </RootErrorBoundary>
  );
}
