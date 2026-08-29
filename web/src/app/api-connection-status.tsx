import { useQuery } from "@tanstack/react-query";

import { fetchHealth } from "../api/client";
import { queryKeys } from "../api/query-keys";
import { Button } from "../components/ui/button";
import { StatusBadge } from "../components/ui/status-badge";

/**
 * Global API reachability for the shell, served by the shared query cache.
 * Connection loss never mutates durable state; it only informs the
 * operator. The probe itself does not auto-retry — the operator's Retry
 * control is the explicit recovery action, which keeps the badge states
 * deterministic for users and tests.
 */
export function ApiConnectionStatus() {
  const health = useQuery({
    queryKey: queryKeys.health(),
    queryFn: ({ signal }) => fetchHealth({ signal }),
    staleTime: 10_000,
    retry: 0,
  });

  if (health.isPending) {
    return (
      <StatusBadge state="warning" aria-live="polite">
        API connecting
      </StatusBadge>
    );
  }

  if (health.isError) {
    return (
      <div className="flex items-center gap-1">
        <StatusBadge state="failure" aria-live="polite">
          API unavailable
        </StatusBadge>
        <Button
          type="button"
          variant="ghost"
          size="compact"
          onClick={() => {
            void health.refetch();
          }}
        >
          Retry
        </Button>
      </div>
    );
  }

  return <StatusBadge state="verified">API online</StatusBadge>;
}
