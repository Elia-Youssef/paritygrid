import { Activity } from "lucide-react";

import { ScreenPlaceholder } from "./screen-placeholder";

export function RunLiveScreen() {
  return (
    <ScreenPlaceholder
      icon={Activity}
      title="Live execution"
      lede="Pipeline graph with durable state overlay, worker timeline, queue depth, event timeline, and run controls for one run."
      arrival="Live execution arrives with the observability workflows and the typed live-state layer."
    />
  );
}
