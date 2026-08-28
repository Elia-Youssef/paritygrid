import { CircleSlash } from "lucide-react";

export interface UnavailableNoticeProps {
  /** What is unavailable and, when known, why or until when. */
  detail?: string;
  label?: string;
}

/**
 * A capability or dependency is present in the interface contract but not
 * currently offered (disabled, not configured, or not licensed). Neutral
 * styling: unavailability is not a failure.
 */
export function UnavailableNotice({
  detail,
  label = "Unavailable",
}: UnavailableNoticeProps) {
  return (
    <div className="flex items-start gap-3 rounded-md border border-border bg-surface-quiet p-4">
      <CircleSlash className="mt-0.5 size-4 shrink-0 text-muted" aria-hidden="true" />
      <div>
        <p className="text-sm font-semibold text-muted-strong">{label}</p>
        {detail && <p className="mt-0.5 text-xs text-muted">{detail}</p>}
      </div>
    </div>
  );
}
