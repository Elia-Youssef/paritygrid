import { useLocation, Outlet } from "react-router";

import { Breadcrumbs } from "../components/ui/breadcrumbs";
import { CommandPalette } from "../components/ui/command-palette";
import { SkipLink } from "../components/ui/skip-link";
import { ApiConnectionStatus } from "./api-connection-status";
import { EnvironmentIndicator } from "./environment-indicator";
import { OperationContextIndicator } from "./operation-context";
import { useDocumentTitle, useRouteFocus } from "./page-effects";
import { commandDestinations } from "./navigation";
import { buildBreadcrumbs } from "./routes-model";
import { BrandMark } from "./brand-mark";
import { CompactNavigation, SidebarNavigation } from "./shell-navigation";
import { RouteErrorBoundary } from "./route-error-boundary";

/**
 * Operational control-room shell: persistent navigation, breadcrumbs,
 * environment identity, connection status, current-run context, and the
 * command palette. Route content renders inside an error boundary so a
 * failing screen never takes the shell down with it.
 */
export function AppLayout() {
  const { pathname } = useLocation();
  useDocumentTitle(pathname);
  useRouteFocus(pathname);

  return (
    <div className="min-h-screen bg-background text-foreground">
      <SkipLink />

      <aside className="fixed inset-y-0 left-0 z-30 hidden w-[var(--shell-sidebar-width)] flex-col border-r border-border bg-surface-quiet lg:flex">
        <div className="flex h-[var(--shell-header-height)] items-center gap-3 border-b border-border px-5">
          <BrandMark />
          <div>
            <p className="text-sm font-bold tracking-brand uppercase text-foreground">
              ParityGrid
            </p>
            <p className="font-mono text-2xs tracking-console text-muted uppercase">
              Operations console
            </p>
          </div>
        </div>

        <SidebarNavigation />

        <div className="border-t border-border p-4">
          <EnvironmentIndicator />
        </div>
      </aside>

      <div className="lg:pl-[var(--shell-sidebar-width)]">
        <header className="sticky top-0 z-20 border-b border-border bg-background/95 backdrop-blur-sm">
          <div className="flex h-[var(--shell-header-height)] items-center justify-between gap-3 px-4 sm:px-6 lg:px-8">
            <div className="flex items-center gap-2 lg:hidden">
              <BrandMark />
              <span className="text-sm font-bold tracking-brand uppercase text-foreground">
                ParityGrid
              </span>
            </div>

            <div className="hidden min-w-0 flex-1 lg:block" />

            <div className="flex items-center gap-2 sm:gap-3">
              <OperationContextIndicator pathname={pathname} />
              <CommandPalette destinations={commandDestinations} />
              <ApiConnectionStatus />
            </div>
          </div>

          <div
            role="group"
            aria-label="Current environment and operation"
            className="flex flex-wrap items-center gap-x-3 gap-y-2 border-t border-border px-4 py-2 sm:px-6 lg:hidden"
          >
            <EnvironmentIndicator compact />
            <OperationContextIndicator pathname={pathname} compact />
          </div>

          <CompactNavigation />
        </header>

        <Breadcrumbs items={buildBreadcrumbs(pathname)} />

        <main
          id="main-content"
          tabIndex={-1}
          className="mx-auto w-full max-w-[var(--content-max)] px-4 py-8 sm:px-6 lg:px-8 lg:py-10 focus-visible:outline-none"
        >
          <RouteErrorBoundary>
            <Outlet />
          </RouteErrorBoundary>
        </main>
      </div>
    </div>
  );
}
