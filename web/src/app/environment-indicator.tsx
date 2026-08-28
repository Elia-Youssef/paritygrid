import { HardDrive } from "lucide-react";

import { environmentInfo } from "./environment";

/**
 * Persistent deployment identity and data root. Displayed as text so the
 * operational context never depends on color or iconography.
 */
export function EnvironmentIndicator({ compact = false }: { compact?: boolean }) {
  if (compact) {
    return (
      <div className="flex min-w-0 flex-1 items-start gap-2">
        <HardDrive
          className="mt-0.5 size-3.5 shrink-0 text-active"
          aria-hidden="true"
        />
        <p className="min-w-0 text-2xs text-muted-strong">
          <span className="font-semibold text-foreground">{environmentInfo.label}</span>
          <span aria-hidden="true"> · </span>
          <span className="break-all font-mono">{environmentInfo.dataRoot}</span>
        </p>
      </div>
    );
  }

  return (
    <div className="flex items-center gap-3 rounded-md border border-border bg-surface p-3">
      <HardDrive className="size-4 shrink-0 text-active" aria-hidden="true" />
      <div className="min-w-0">
        <p className="text-xs font-semibold text-foreground">{environmentInfo.label}</p>
        <p className="truncate font-mono text-2xs text-muted">
          {environmentInfo.dataRoot}
        </p>
      </div>
    </div>
  );
}
