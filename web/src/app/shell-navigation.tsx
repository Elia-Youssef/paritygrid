import { NavLink } from "react-router";

import { cn } from "../lib/cn";
import {
  primaryNavigation,
  secondaryNavigation,
  type NavigationItem,
} from "./navigation";

const linkClasses = cn(
  "flex items-center gap-3 rounded-md px-3 text-sm font-medium text-muted-strong",
  "transition-colors duration-[var(--motion-fast)] ease-[var(--ease-standard)]",
  "hover:bg-surface-elevated hover:text-foreground",
  "aria-[current=page]:bg-active/10 aria-[current=page]:text-active",
);

function NavItemLink({ item, className }: { item: NavigationItem; className: string }) {
  const Icon = item.icon;
  return (
    <NavLink to={item.to} end={item.end} className={className}>
      <Icon className="size-4 shrink-0" aria-hidden="true" />
      {item.label}
    </NavLink>
  );
}

/**
 * Persistent sidebar navigation for wide viewports. `React Router` marks
 * the active item with `aria-current="page"`.
 */
export function SidebarNavigation() {
  return (
    <nav aria-label="Primary" className="flex flex-1 flex-col justify-between p-3">
      <div className="space-y-1">
        {primaryNavigation.map((item) => (
          <NavItemLink key={item.to} item={item} className={cn(linkClasses, "h-10")} />
        ))}
      </div>
      <div className="space-y-1 border-t border-border pt-3">
        {secondaryNavigation.map((item) => (
          <NavItemLink key={item.to} item={item} className={cn(linkClasses, "h-10")} />
        ))}
      </div>
    </nav>
  );
}

/**
 * Horizontally scrollable navigation strip for narrow viewports. Hidden at
 * the sidebar breakpoint, so exactly one primary navigation stays in the
 * accessibility tree and tab order at any width.
 */
export function CompactNavigation() {
  return (
    <nav aria-label="Primary" className="border-t border-border lg:hidden">
      <div className="flex gap-1 overflow-x-auto px-3 py-2">
        {[...primaryNavigation, ...secondaryNavigation].map((item) => (
          <NavItemLink
            key={item.to}
            item={item}
            className={cn(linkClasses, "h-9 shrink-0 whitespace-nowrap px-3")}
          />
        ))}
      </div>
    </nav>
  );
}
