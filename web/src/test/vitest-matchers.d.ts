/* Vitest needs the jest-axe matcher registered on its Assertion type. */
import "vitest";

declare module "vitest" {
  interface Assertion<T = unknown> {
    toHaveNoViolations(): T;
  }
}
