import { Command, HardDrive, Search } from "lucide-react";
import type { ReactNode } from "react";

import { Button } from "../components/ui/button";
import { ApiConnectionStatus } from "../features/overview/api-connection";
import { BrandMark } from "./brand-mark";
import { primaryNavigation, secondaryNavigation } from "./navigation";

export interface ApplicationShellProps {
  children: ReactNode;
}

export function ApplicationShell({ children }: ApplicationShellProps) {
  return (
    <div className="min-h-screen bg-background text-foreground">
      <a
        href="#main-content"
        className="fixed top-3 left-3 z-50 -translate-y-20 rounded-md bg-primary px-4 py-2 text-sm font-semibold text-primary-foreground transition-transform focus:translate-y-0"
      >
        Skip to content
      </a>

      <aside className="fixed inset-y-0 left-0 z-30 hidden w-64 border-r border-border bg-surface-quiet lg:flex lg:flex-col">
        <div className="flex h-20 items-center gap-3 border-b border-border px-5">
          <BrandMark />
          <div>
            <p className="text-sm font-bold tracking-[0.12em] uppercase">ParityGrid</p>
            <p className="font-mono text-[0.625rem] tracking-[0.16em] text-muted uppercase">
              Control plane
            </p>
          </div>
        </div>

        <nav className="flex flex-1 flex-col justify-between p-3" aria-label="Primary">
          <div className="space-y-1">
            {primaryNavigation.map(({ href, icon: Icon, label }, index) => (
              <a
                key={href}
                href={href}
                aria-current={index === 0 ? "page" : undefined}
                className="flex h-10 items-center gap-3 rounded-md px-3 text-sm font-medium text-muted-strong transition-colors hover:bg-surface-elevated hover:text-foreground aria-[current=page]:bg-active/10 aria-[current=page]:text-active"
              >
                <Icon className="size-4" aria-hidden="true" />
                {label}
              </a>
            ))}
          </div>

          <div className="space-y-1 border-t border-border pt-3">
            {secondaryNavigation.map(({ href, icon: Icon, label }) => (
              <a
                key={href}
                href={href}
                className="flex h-10 items-center gap-3 rounded-md px-3 text-sm font-medium text-muted-strong transition-colors hover:bg-surface-elevated hover:text-foreground"
              >
                <Icon className="size-4" aria-hidden="true" />
                {label}
              </a>
            ))}
          </div>
        </nav>

        <div className="border-t border-border p-4">
          <div className="flex items-center gap-3 rounded-md border border-border bg-surface p-3">
            <HardDrive className="size-4 text-active" aria-hidden="true" />
            <div className="min-w-0">
              <p className="text-xs font-semibold">Local workspace</p>
              <p className="truncate font-mono text-[0.625rem] text-muted">
                .paritygrid/
              </p>
            </div>
          </div>
        </div>
      </aside>

      <div className="lg:pl-64">
        <header className="sticky top-0 z-20 flex h-16 items-center justify-between border-b border-border bg-background/95 px-4 backdrop-blur-sm sm:px-6 lg:px-8">
          <div className="flex items-center gap-3 lg:hidden">
            <BrandMark />
            <span className="text-sm font-bold tracking-[0.12em] uppercase">
              ParityGrid
            </span>
          </div>

          <div className="hidden items-center gap-2 text-sm text-muted sm:flex">
            <span>Workspace</span>
            <span aria-hidden="true">/</span>
            <span className="font-medium text-foreground">Overview</span>
          </div>

          <div className="flex items-center gap-2 sm:gap-3">
            <Button
              variant="ghost"
              size="compact"
              className="hidden text-muted sm:inline-flex"
              aria-label="Open command palette"
            >
              <Search className="size-3.5" aria-hidden="true" />
              Search
              <span className="ml-2 inline-flex items-center gap-0.5 rounded border border-border px-1.5 py-0.5 font-mono text-[0.625rem]">
                <Command className="size-2.5" aria-hidden="true" />K
              </span>
            </Button>
            <ApiConnectionStatus />
          </div>
        </header>

        <main id="main-content" className="px-4 py-8 sm:px-6 lg:px-8 lg:py-10">
          {children}
        </main>
      </div>
    </div>
  );
}
