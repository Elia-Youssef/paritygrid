/**
 * Compile-time proof that the client consumes the generated Phase 13
 * contract. These examples are exercised by `tsc -b` on every typecheck
 * and by a runtime test; the `Expect<Equal<...>>` assertions in
 * `compile-time-examples.test.ts` fail the build if any client function
 * drifts from the generated types.
 */
import type { Equal, Expect } from "./type-assertions";
import type {
  CapabilitiesResponse,
  ConflictPageResponse,
  ReconciliationResponse,
  RunCreateRequest,
  RunPageResponse,
  RunResponse,
} from "./generated/schema";
import {
  createRun,
  fetchCapabilities,
  fetchConflicts,
  fetchReconciliation,
  fetchRun,
  fetchRuns,
} from "./client";

const controller = new AbortController();
const signal = controller.signal;

// Typed queries resolve to the exact generated response types.
export const queryExamples = {
  runList: (): Promise<RunPageResponse> => fetchRuns({ limit: 50 }, { signal }),
  run: (): Promise<RunResponse> => fetchRun("run-example", { signal }),
  reconciliation: (): Promise<ReconciliationResponse> =>
    fetchReconciliation("run-example", { signal }),
  conflicts: (): Promise<ConflictPageResponse> =>
    fetchConflicts("run-example", { limit: 25, cursor: undefined }, { signal }),
  capabilities: (): Promise<CapabilitiesResponse> => fetchCapabilities({ signal }),
};

// Typed mutations accept the generated request shape.
export const mutationExamples = {
  createRun: (request: RunCreateRequest): Promise<RunResponse> =>
    createRun(request, { idempotencyKey: "demo-key-1", signal }),
};

// Compile-time identity checks (verified again in the test file). Exported
// so `noUnusedLocals` keeps them visible; they are types only and erase at
// runtime.
export type RunQueryIsGenerated = Expect<
  Equal<Awaited<ReturnType<typeof fetchRun>>, RunResponse>
>;
export type RunListIsGenerated = Expect<
  Equal<Awaited<ReturnType<typeof fetchRuns>>, RunPageResponse>
>;
export type CreateRunTakesGeneratedRequest = Expect<
  Equal<Parameters<typeof createRun>[0], RunCreateRequest>
>;
void queryExamples;
void mutationExamples;
