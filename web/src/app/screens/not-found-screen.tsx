import { Link } from "react-router";

import { Button } from "../../components/ui/button";

/**
 * Unknown addresses keep the owning layout (console or public) and offer
 * both entry points instead of a dead end.
 */
export function NotFoundScreen() {
  return (
    <div className="mx-auto w-full max-w-2xl">
      <p className="font-mono text-2xs tracking-eyebrow text-muted uppercase">
        Unresolved address
      </p>
      <h1
        data-page-title
        tabIndex={-1}
        className="mt-2 inline-block rounded-sm text-lg font-semibold text-foreground focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-[var(--focus-ring)]"
      >
        Screen not found
      </h1>
      <p className="mt-3 text-sm leading-6 text-muted-strong">
        This address does not match any screen. It may have moved, or the identifier may
        be misspelled.
      </p>
      <div className="mt-6 flex flex-wrap gap-2">
        <Button asChild>
          <Link to="/app">Operations overview</Link>
        </Button>
        <Button asChild variant="secondary">
          <Link to="/">Public overview</Link>
        </Button>
      </div>
    </div>
  );
}
