import "@testing-library/jest-dom/vitest";

import { cleanup } from "@testing-library/react";
import { toHaveNoViolations } from "jest-axe";
import { afterEach, expect } from "vitest";

expect.extend(toHaveNoViolations);

// Minimal ResizeObserver/DOMMatrixReadOnly stubs so React Flow's viewport
// measurement code can run under jsdom; observations are never fired, which
// keeps component tests deterministic.
class ResizeObserverStub {
  observe(): void {
    // Observations are intentionally never fired under jsdom.
  }

  unobserve(): void {
    // No per-element state is retained.
  }

  disconnect(): void {
    // No scheduled work exists to cancel.
  }
}
const globalResizeObserver = globalThis as { ResizeObserver?: unknown };
if (globalResizeObserver.ResizeObserver === undefined) {
  globalResizeObserver.ResizeObserver = ResizeObserverStub;
}

class DOMMatrixReadOnlyStub {
  m11 = 1;
  m12 = 0;
  m21 = 0;
  m22 = 1;
  m41 = 0;
  m42 = 0;
  constructor(transform?: string) {
    void transform;
  }
}
const globalMatrix = globalThis as { DOMMatrixReadOnly?: unknown };
if (globalMatrix.DOMMatrixReadOnly === undefined) {
  globalMatrix.DOMMatrixReadOnly = DOMMatrixReadOnlyStub;
}

afterEach(() => {
  cleanup();
});
