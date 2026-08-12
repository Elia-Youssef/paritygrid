import { useEffect, useState } from "react";

import { StatusBadge } from "../../components/ui/status-badge";

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

export function ApiConnectionStatus() {
  const [connection, setConnection] = useState<ApiConnection>("connecting");

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
  }, []);

  if (connection === "online") {
    return <StatusBadge state="verified">API online</StatusBadge>;
  }

  if (connection === "unavailable") {
    return <StatusBadge state="failure">API unavailable</StatusBadge>;
  }

  return (
    <StatusBadge state="warning" aria-live="polite">
      API connecting
    </StatusBadge>
  );
}
