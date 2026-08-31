import type { LucideIcon } from "lucide-react";
import type { ReactNode } from "react";

export interface EmptyStateProps {
  icon?: LucideIcon;
  title: string;
  description?: string;
  /** Optional call to action, e.g. a Button that starts the first workflow. */
  action?: ReactNode;
}

/**
 * No-content presentation for lists and screens that are correctly empty.
 */
export function EmptyState({
  icon: Icon,
  title,
  description,
  action,
}: EmptyStateProps) {
  return (
    <div className="flex flex-col items-start gap-3 rounded-md border border-dashed border-border bg-surface-quiet p-6 text-left sm:p-8">
      {Icon && (
        <span className="flex size-9 items-center justify-center rounded-md border border-border bg-surface text-muted">
          <Icon className="size-4" aria-hidden="true" />
        </span>
      )}
      <div>
        <p className="text-sm font-semibold text-foreground">{title}</p>
        {description && <p className="mt-1 text-sm text-muted">{description}</p>}
      </div>
      {action && <div className="mt-1 flex flex-wrap gap-2">{action}</div>}
    </div>
  );
}
