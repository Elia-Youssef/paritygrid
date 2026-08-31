import { axe } from "jest-axe";

import type { AxeResults } from "jest-axe";

/**
 * Run axe-core over a rendered subtree and fail on any violation.
 *
 * Two rules are disabled for the jsdom environment only, each with a
 * compensating check:
 *
 * - `document-title`: component fixtures render without the shell that
 *   owns `document.title`; route tests assert titles per path.
 * - `landmark-unique`: the shell renders the sidebar and compact primary
 *   navigations as responsive twins whose `hidden`/`lg:hidden` display
 *   classes are inert in jsdom (no compiled stylesheet). Real browsers
 *   expose exactly one per viewport; the packaged-browser check verifies
 *   this against computed styles.
 */
export async function expectNoAccessibilityViolations(
  container: HTMLElement,
): Promise<AxeResults> {
  const results = await axe(container, {
    rules: {
      "document-title": { enabled: false },
      "landmark-unique": { enabled: false },
    },
  });
  expect(results).toHaveNoViolations();
  return results;
}
