import { GitCompareArrows } from "lucide-react";

import { ScreenPlaceholder } from "./screen-placeholder";

export function ComparisonScreen() {
  return (
    <ScreenPlaceholder
      icon={GitCompareArrows}
      title="Runner comparison"
      lede="Compatible runs compared correctness-first: fingerprint equivalence before duration, latency, retry, and resource metrics."
      arrival="Comparison arrives with the observability workflows."
    />
  );
}
