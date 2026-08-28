import { cn } from "../../lib/cn";

export interface SkipLinkProps {
  /** The id of the region keyboard users should land in. */
  targetId?: string;
  label?: string;
}

/**
 * First focusable element in the document: visible only while focused, and
 * moves keyboard users past persistent navigation.
 */
export function SkipLink({
  targetId = "main-content",
  label = "Skip to content",
}: SkipLinkProps) {
  return (
    <a
      href={`#${targetId}`}
      className={cn(
        "fixed top-3 left-3 z-50 -translate-y-24 rounded-md bg-primary px-4 py-2",
        "text-sm font-semibold text-primary-foreground shadow-elevated",
        "transition-transform duration-[var(--motion-fast)] ease-[var(--ease-standard)]",
        "focus-visible:translate-y-0 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--focus-ring)]",
      )}
    >
      {label}
    </a>
  );
}
