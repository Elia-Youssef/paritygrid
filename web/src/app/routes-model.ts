/**
 * Pure routing knowledge for the application shell: breadcrumb trails,
 * document titles, and the current operational context derived from a
 * location pathname. Keeping this outside the router makes shell behavior
 * directly testable and keeps route display rules in one place.
 */

export interface Crumb {
  label: string;
  href: string;
  /** Dynamic path identifiers render in the monospace data typeface. */
  monospace?: boolean;
}

const MAX_CRUMBS = 8;

const STATIC_SEGMENTS: Record<string, string> = {
  app: "Operations",
  pipelines: "Pipelines",
  runs: "Runs",
  reconcile: "Reconcile",
  repairs: "Repairs",
  compare: "Compare",
  system: "System",
};

const TITLES: Record<string, string> = {
  "/": "ParityGrid",
  "/app": "Operations overview",
  "/app/pipelines": "Pipeline library",
  "/app/runs": "Run history",
  "/app/compare": "Runner comparison",
  "/app/system": "System health",
};

/** Titles for screens whose address carries an operational identifier. */
const DYNAMIC_TITLES: Record<string, (id: string) => string> = {
  Pipelines: (id) => `Pipeline ${id}`,
  Runs: (id) => `Run ${id}`,
  Repairs: (id) => `Repair plan ${id}`,
};

export function normalizePath(pathname: string): string {
  const withoutTrailingSlash = pathname.replace(/\/+$/, "");
  return withoutTrailingSlash === "" ? "/" : withoutTrailingSlash;
}

export function buildBreadcrumbs(pathname: string): Crumb[] {
  const normalized = normalizePath(pathname);
  if (!normalized.startsWith("/app")) {
    return [];
  }

  const segments = normalized.split("/").filter((segment) => segment !== "");
  const crumbs: Crumb[] = [];
  let href = "";

  for (const segment of segments) {
    href += `/${segment}`;
    const staticLabel = STATIC_SEGMENTS[segment];
    crumbs.push({
      href,
      label: staticLabel ?? segment,
      monospace: staticLabel === undefined,
    });
    if (crumbs.length === MAX_CRUMBS) {
      break;
    }
  }

  return crumbs;
}

export function resolveRouteTitle(pathname: string): string {
  const normalized = normalizePath(pathname);
  const titled = TITLES[normalized];
  if (titled !== undefined) {
    return titled;
  }

  const crumbs = buildBreadcrumbs(normalized);
  const leaf = crumbs.at(-1);
  if (leaf === undefined) {
    return "ParityGrid";
  }

  if (leaf.monospace) {
    const parent = crumbs.at(-2);
    const builder = parent === undefined ? undefined : DYNAMIC_TITLES[parent.label];
    return builder === undefined ? leaf.label : builder(leaf.label);
  }

  return leaf.label;
}

export function resolveRouteSuffix(pathname: string): string {
  return normalizePath(pathname) === "/" ? "ParityGrid" : "ParityGrid console";
}

export interface OperationContext {
  kind: "run" | "repair";
  id: string;
}

const RUN_CONTEXT_PATTERN = /^\/app\/runs\/([^/]+)(?:\/|$)/;
const REPAIR_CONTEXT_PATTERN = /^\/app\/repairs\/([^/]+)(?:\/|$)/;

/**
 * The operational entity a screen is working on, when the location carries
 * one. The shell surfaces this as persistent context in the top bar.
 */
export function resolveOperationContext(pathname: string): OperationContext | null {
  const runMatch = RUN_CONTEXT_PATTERN.exec(normalizePath(pathname))?.[1];
  if (runMatch !== undefined) {
    return { kind: "run", id: runMatch };
  }

  const repairMatch = REPAIR_CONTEXT_PATTERN.exec(normalizePath(pathname))?.[1];
  if (repairMatch !== undefined) {
    return { kind: "repair", id: repairMatch };
  }

  return null;
}

export function formatOperationContext(context: OperationContext): string {
  const noun = context.kind === "run" ? "Run" : "Repair plan";
  return `${noun} ${context.id}`;
}
