import { LayoutDashboard } from "lucide-react";

import { ScreenPlaceholder } from "./screen-placeholder";

export function OperationsOverviewScreen() {
  return (
    <ScreenPlaceholder
      icon={LayoutDashboard}
      title="Operations overview"
      lede="Recent runs, active and queued work, verified and failed counts, storage health, and runner availability in one control-room view."
      arrival="Operational summaries arrive with the operations overview workflows. Summary metrics are only shown once they have a stable definition and source."
    />
  );
}
