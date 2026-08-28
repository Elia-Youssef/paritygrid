import { History } from "lucide-react";

import { ScreenPlaceholder } from "./screen-placeholder";

export function RunHistoryScreen() {
  return (
    <ScreenPlaceholder
      icon={History}
      title="Run history"
      lede="Every reconciliation run with status, runner, pipeline version, and final fingerprints."
      arrival="Run history arrives with the execution observability workflows."
    />
  );
}
