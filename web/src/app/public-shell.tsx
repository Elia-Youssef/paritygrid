import { Outlet, useLocation, Link } from "react-router";

import { Button } from "../components/ui/button";
import { SkipLink } from "../components/ui/skip-link";
import { BrandMark } from "./brand-mark";
import { useDocumentTitle, useRouteFocus } from "./page-effects";

/**
 * Concise public layout for the project overview. Shares the design-token
 * system with the operations console and links into it.
 */
export function PublicLayout() {
  const { pathname } = useLocation();
  useDocumentTitle(pathname);
  useRouteFocus(pathname);

  return (
    <div className="flex min-h-screen flex-col bg-background text-foreground">
      <SkipLink />

      <header className="border-b border-border">
        <div className="mx-auto flex h-16 w-full max-w-[var(--content-max)] items-center justify-between px-4 sm:px-6 lg:px-8">
          <Link
            to="/"
            className="flex items-center gap-3 rounded-md text-foreground"
            aria-label="ParityGrid home"
          >
            <BrandMark />
            <span className="text-sm font-bold tracking-brand uppercase">
              ParityGrid
            </span>
          </Link>
          <Button asChild variant="secondary" size="compact">
            <Link to="/app">Open operations console</Link>
          </Button>
        </div>
      </header>

      <main
        id="main-content"
        tabIndex={-1}
        className="mx-auto w-full max-w-[var(--content-max)] flex-1 px-4 py-10 sm:px-6 lg:px-8 focus-visible:outline-none"
      >
        <Outlet />
      </main>

      <footer className="border-t border-border py-8">
        <div className="mx-auto w-full max-w-[var(--content-max)] px-4 text-xs text-muted sm:px-6 lg:px-8">
          Local-first demonstration. The console and its data stay on this machine.
        </div>
      </footer>
    </div>
  );
}
