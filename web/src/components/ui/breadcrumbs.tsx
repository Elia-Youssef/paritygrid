import { ChevronRight } from "lucide-react";
import { Fragment } from "react";
import { Link } from "react-router";

import { cn } from "../../lib/cn";

export interface BreadcrumbItem {
  label: string;
  href: string;
  /** Dynamic path identifiers render in the monospace data typeface. */
  monospace?: boolean;
}

export interface BreadcrumbsProps {
  items: readonly BreadcrumbItem[];
  label?: string;
}

/**
 * Breadcrumb trail for nested operational routes. The trail scrolls
 * horizontally when identifiers are long instead of forcing page overflow,
 * and the current location is marked with `aria-current="page"`.
 */
export function Breadcrumbs({ items, label = "Breadcrumb" }: BreadcrumbsProps) {
  if (items.length === 0) {
    return null;
  }

  return (
    <nav aria-label={label} className="min-w-0">
      <ol className="flex items-center gap-1 overflow-x-auto whitespace-nowrap px-4 py-2 text-sm sm:px-6 lg:px-8">
        {items.map((item, index) => {
          const isCurrent = index === items.length - 1;
          return (
            <Fragment key={item.href}>
              {index > 0 && (
                <li aria-hidden="true" className="flex items-center text-muted">
                  <ChevronRight className="size-3.5 shrink-0" />
                </li>
              )}
              <li className="flex min-w-0 items-center">
                {isCurrent ? (
                  <span
                    aria-current="page"
                    className={cn(
                      "font-semibold text-foreground",
                      item.monospace && "font-mono text-2xs tracking-wide",
                    )}
                  >
                    {item.label}
                  </span>
                ) : (
                  <Link
                    to={item.href}
                    className={cn(
                      "rounded-sm text-muted-strong transition-colors duration-[var(--motion-fast)] hover:text-foreground hover:underline",
                      item.monospace && "font-mono text-2xs tracking-wide",
                    )}
                  >
                    {item.label}
                  </Link>
                )}
              </li>
            </Fragment>
          );
        })}
      </ol>
    </nav>
  );
}
