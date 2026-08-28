import { ClockAlert } from "lucide-react";

export interface StaleNoticeProps {
  /** Why the displayed data may be behind, or since when it is stale. */
  detail?: string;
  label?: string;
}

/**
 * The last coherent view is still visible but may be behind authoritative
 * state. Amber signals the waiting condition; the text carries the meaning.
 */
export function StaleNotice({
  detail,
  label = "Data may be out of date",
}: StaleNoticeProps) {
  return (
    <div
      role="status"
      className="flex items-start gap-3 rounded-md border border-stale/40 bg-stale/10 p-4"
    >
      <ClockAlert className="mt-0.5 size-4 shrink-0 text-stale" aria-hidden="true" />
      <div>
        <p className="text-sm font-semibold text-foreground">{label}</p>
        {detail && <p className="mt-0.5 text-xs text-muted-strong">{detail}</p>}
      </div>
    </div>
  );
}
