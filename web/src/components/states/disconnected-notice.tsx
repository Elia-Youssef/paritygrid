import { WifiOff } from "lucide-react";

export interface DisconnectedNoticeProps {
  detail?: string;
  label?: string;
}

/**
 * The live channel dropped. Durable state remains available through the
 * API, so the notice explains the condition instead of blocking the view.
 */
export function DisconnectedNotice({
  detail = "Live updates are interrupted. Durable state remains available; reconnecting.",
  label = "Live connection lost",
}: DisconnectedNoticeProps) {
  return (
    <div
      role="status"
      className="flex items-start gap-3 rounded-md border border-warning/40 bg-warning/10 p-4"
    >
      <WifiOff className="mt-0.5 size-4 shrink-0 text-warning" aria-hidden="true" />
      <div>
        <p className="text-sm font-semibold text-foreground">{label}</p>
        {detail && <p className="mt-0.5 text-xs text-muted-strong">{detail}</p>}
      </div>
    </div>
  );
}
