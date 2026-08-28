import { useEffect, useRef } from "react";

import { resolveRouteSuffix, resolveRouteTitle } from "./routes-model";

/**
 * Keep the document title in sync with the active route so history entries,
 * screen readers, and browser UI describe the current screen.
 */
export function useDocumentTitle(pathname: string): void {
  useEffect(() => {
    const title = resolveRouteTitle(pathname);
    const suffix = resolveRouteSuffix(pathname);
    document.title = title === suffix ? title : `${title} · ${suffix}`;
  }, [pathname]);
}

/**
 * After client-side navigation, move focus to the destination page heading
 * (any element marked `data-page-title`, focusable via `tabIndex={-1}`).
 * The initial mount keeps focus where the user or browser placed it.
 */
export function useRouteFocus(pathname: string): void {
  const mounted = useRef(false);

  useEffect(() => {
    if (!mounted.current) {
      mounted.current = true;
      return;
    }
    const heading = document.querySelector<HTMLElement>("[data-page-title]");
    heading?.focus();
  }, [pathname]);
}
