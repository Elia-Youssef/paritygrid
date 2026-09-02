/**
 * URL-state contract for the run list. Parsing lives outside the component
 * file so the component module only exports components; every parameter is
 * bounded and drawn from a closed set.
 */
import { isRunStateValue, type RunStateValue } from "./run-derivations";

export const PAGE_LIMITS = [10, 25, 50] as const;
export const DEFAULT_LIMIT = 25;
export const RUNS_FILTER_TESTID = "runs-state-filter";

export interface RunListUrlState {
  readonly state: RunStateValue | null;
  readonly cursor: string | null;
  readonly limit: number;
  readonly selected: string | null;
}

function parseLimit(value: string | null): number {
  const parsed = Number.parseInt(value ?? "", 10);
  return (PAGE_LIMITS as readonly number[]).includes(parsed) ? parsed : DEFAULT_LIMIT;
}

function parseState(value: string | null): RunStateValue | null {
  return value !== null && isRunStateValue(value) ? value : null;
}

function parseCursor(value: string | null): string | null {
  return value !== null && value.length > 0 && value.length <= 256 ? value : null;
}

/** Bounded, closed-set parsing of the run-list query parameters. */
export function parseRunListParams(search: URLSearchParams): RunListUrlState {
  return {
    state: parseState(search.get("state")),
    cursor: parseCursor(search.get("cursor")),
    limit: parseLimit(search.get("limit")),
    selected: parseCursor(search.get("selected")),
  };
}
