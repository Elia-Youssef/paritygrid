import { cva, type VariantProps } from "class-variance-authority";
import type { HTMLAttributes } from "react";

import { cn } from "../../lib/cn";

const statusBadgeVariants = cva(
  "inline-flex min-h-6 items-center gap-2 whitespace-nowrap rounded-full border px-2.5 py-1 font-mono text-2xs font-semibold tracking-label uppercase",
  {
    variants: {
      state: {
        active: "border-active/30 bg-active/10 text-active",
        verified: "border-verified/30 bg-verified/10 text-verified",
        warning: "border-warning/30 bg-warning/10 text-warning",
        failure: "border-failure/30 bg-failure/10 text-failure",
        stale: "border-stale/30 bg-stale/10 text-stale",
        paused: "border-paused/30 bg-paused/10 text-paused",
        cancelled: "border-cancelled/30 bg-cancelled/10 text-cancelled",
        neutral: "border-border-strong bg-surface-elevated text-muted-strong",
      },
    },
    defaultVariants: {
      state: "stale",
    },
  },
);

export interface StatusBadgeProps
  extends HTMLAttributes<HTMLSpanElement>, VariantProps<typeof statusBadgeVariants> {}

/**
 * Operational state badge. The textual label — not the color — carries the
 * state; the dot is decorative. States map exclusively to design tokens.
 */
export function StatusBadge({
  children,
  className,
  state,
  ...props
}: StatusBadgeProps) {
  return (
    <span className={cn(statusBadgeVariants({ state }), className)} {...props}>
      <span className="size-1.5 rounded-full bg-current" aria-hidden="true" />
      {children}
    </span>
  );
}
