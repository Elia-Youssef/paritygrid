import { useEffect, useState } from "react";

import { Button } from "../components/ui/button";
import { StatusBadge } from "../components/ui/status-badge";

type ApiConnection = "connecting" | "online" | "unavailable";

async function probeHealth(signal: AbortSignal): Promise<void> {
  const response = await fetch("/healthz", {
    cache: "no-store",
    headers: { Accept: "application/json" },
    signal,
  });

  if (!response.ok) {
    throw new Error(`Health probe returned ${String(response.status)}`);
  }
}

/**
 * Global API reachability for the shell. Connection loss never mutates
 * durable state; it only informs the operator.
 */
export function ApiConnectionStatus() {
  const [connection, setConnection] = useState<ApiConnection>("connecting");
  const [probeRevision, setProbeRevision] = useState(0);

  useEffect(() => {
    const controller = new AbortController();

    void probeHealth(controller.signal).then(
      () => setConnection("online"),
      () => {
        if (!controller.signal.aborted) {
          setConnection("unavailable");
        }
      },
    );

    return () => controller.abort();
  }, [probeRevision]);

  const retry = (): void => {
    setConnection("connecting");
    setProbeRevision((revision) => revision + 1);
  };

  if (connection === "online") {
    return <StatusBadge state="verified">API online</StatusBadge>;
  }

  if (connection === "unavailable") {
    return (
      <div className="flex items-center gap-1">
        <StatusBadge state="failure">API unavailable</StatusBadge>
        <Button type="button" variant="ghost" size="compact" onClick={retry}>
          Retry
        </Button>
      </div>
    );
  }

  return (
    <StatusBadge state="warning" aria-live="polite">
      API connecting
    </StatusBadge>
  );
}
