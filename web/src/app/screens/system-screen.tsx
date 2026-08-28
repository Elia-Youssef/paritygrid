import { Settings2 } from "lucide-react";

import { ScreenPlaceholder } from "./screen-placeholder";

export function SystemScreen() {
  return (
    <ScreenPlaceholder
      icon={Settings2}
      title="System health"
      lede="Declared capabilities, embedded storage health, and artifact integrity for this workspace."
      arrival="Capability and storage reporting arrives with the observability workflows."
    />
  );
}
