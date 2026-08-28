/**
 * Minimal ambient types for jest-axe, which ships without TypeScript
 * declarations. Only the surface this repository uses is described.
 */
declare module "jest-axe" {
  export interface AxeViolationNode {
    target: readonly string[];
    html: string;
  }

  export interface AxeViolation {
    id: string;
    impact: string | null;
    description: string;
    help: string;
    nodes: readonly AxeViolationNode[];
  }

  export interface AxeResults {
    violations: readonly AxeViolation[];
    passes: readonly unknown[];
    incomplete: readonly unknown[];
    inapplicable: readonly unknown[];
  }

  export interface JestAxeOptions {
    rules?: Record<string, { enabled?: boolean }>;
  }

  export function axe(
    context: Element | string,
    options?: JestAxeOptions,
  ): Promise<AxeResults>;

  export const toHaveNoViolations: {
    toHaveNoViolations(...args: unknown[]): {
      pass: boolean;
      message: () => string;
    };
  };
}
