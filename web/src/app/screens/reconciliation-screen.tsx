import { GitCompareArrows } from "lucide-react";

import { ScreenPlaceholder } from "./screen-placeholder";

export function ReconciliationScreen() {
  return (
    <ScreenPlaceholder
      icon={GitCompareArrows}
      title="Reconciliation workbench"
      lede="Classification summary, filterable conflict table, field-level differences, and repair selection for one reconciliation fingerprint."
      arrival="The workbench arrives with the reconciliation and repair workflows."
    />
  );
}
