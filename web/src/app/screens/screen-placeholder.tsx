import type { LucideIcon } from "lucide-react";

import { EmptyState } from "../../components/states/empty-state";

export interface ScreenPlaceholderProps {
  title: string;
  /** What the screen will operate on once its workflows arrive. */
  lede: string;
  /** Honest availability statement; placeholders never fake live data. */
  arrival: string;
  icon?: LucideIcon;
}

/**
 * Shell-level route placeholder. The route, breadcrumbs, and document
 * title already exist; the working content arrives with its owning
 * workflow phase and is marked unavailable until then.
 */
export function ScreenPlaceholder({
  title,
  lede,
  arrival,
  icon,
}: ScreenPlaceholderProps) {
  return (
    <div className="mx-auto w-full max-w-3xl">
      <p className="font-mono text-2xs tracking-eyebrow text-muted uppercase">
        Operations console
      </p>
      <h1
        data-page-title
        tabIndex={-1}
        className="mt-2 inline-block rounded-sm text-lg font-semibold text-foreground focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-[var(--focus-ring)]"
      >
        {title}
      </h1>
      <p className="mt-3 max-w-2xl text-sm leading-6 text-muted-strong">{lede}</p>
      <div className="mt-8">
        <EmptyState title="Not yet available" description={arrival} icon={icon} />
      </div>
    </div>
  );
}
