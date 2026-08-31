import { ShieldCheck } from "lucide-react";

import { ScreenPlaceholder } from "./screen-placeholder";

export function RepairReviewScreen() {
  return (
    <ScreenPlaceholder
      icon={ShieldCheck}
      title="Repair review"
      lede="Exact reconciliation fingerprint, proposed changes, risks, approval status, and idempotent application progress for one repair plan."
      arrival="Repair review arrives with the repair workflows, including the approval gate and stale-plan blocking."
    />
  );
}
