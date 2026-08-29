/**
 * Typed REST client over the Phase 13 generated contract. Request and
 * response types come from `./generated/schema` — never hand-maintained.
 *
 * All responses are untrusted: transport failures, non-JSON bodies,
 * non-Problem error bodies, and Problem Details payloads are normalized
 * into one bounded, client-safe error model (`ApiRequestError`) whose
 * diagnostic view comes from the shared Problem Details parser. Coherence
 * fields the client logic depends on (run identity, fingerprints, page
 * bounds) are strictly validated with type guards before any caller can
 * observe a response.
 */
import {
  GENERIC_ERROR_TITLE,
  parseProblemDetails,
  type ProblemDetailsView,
} from "../lib/problem-details";
import type {
  CapabilitiesResponse,
  ConflictPageResponse,
  HealthResponse,
  ReconciliationResponse,
  RepairPlanCreateRequest,
  RepairPlanResponse,
  RunCreateRequest,
  RunPageResponse,
  RunResponse,
} from "./generated/schema";
import {
  capabilitiesResponseSchema,
  conflictPageResponseSchema,
  healthResponseSchema,
  reconciliationResponseSchema,
  repairPlanResponseSchema,
  runPageResponseSchema,
  runResponseSchema,
} from "./runtime-schemas";

export const API_BASE = "/api/v1";

/** Upper bound for error bodies kept in memory for diagnostics. */
const MAX_ERROR_BODY_BYTES = 65_536;

export type ApiFailureKind = "network" | "invalid-response" | "problem";

export class ApiRequestError extends Error {
  readonly kind: ApiFailureKind;
  /** HTTP status when the server answered; undefined for network failures. */
  readonly status?: number;
  /** Bounded, safe-to-render view of the server's Problem Details. */
  readonly problem: ProblemDetailsView;

  constructor(kind: ApiFailureKind, problem: ProblemDetailsView, status?: number) {
    super(problem.detail ?? problem.title);
    this.name = "ApiRequestError";
    this.kind = kind;
    this.problem = problem;
    this.status = status;
  }

  /** The server's `code` Problem extension, when present and textual. */
  get problemCode(): string | undefined {
    return this.problem.extensions.find((member) => member.name === "code")?.value;
  }
}

export function isApiRequestError(error: unknown): error is ApiRequestError {
  return error instanceof ApiRequestError;
}

export interface RunIdentity {
  run_id: string;
  run_version: number;
  state: string;
  observed_at: string;
}

/** The coherence identity carried by every run-related response. */
export function runIdentity(value: unknown): RunIdentity | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }
  const record = value as Record<string, unknown>;
  const { run_id, run_version, state, observed_at } = record;
  return typeof run_id === "string" &&
    run_id.length > 0 &&
    typeof run_version === "number" &&
    Number.isInteger(run_version) &&
    run_version >= 0 &&
    typeof state === "string" &&
    typeof observed_at === "string"
    ? { run_id, run_version, state, observed_at }
    : null;
}

/** Run-bearing responses are trusted only with a coherent identity block. */
export function isRunResponse(value: unknown): value is RunResponse {
  return runResponseSchema.safeParse(value).success;
}

export function isRunPageResponse(value: unknown): value is RunPageResponse {
  return runPageResponseSchema.safeParse(value).success;
}

export function isReconciliationResponse(
  value: unknown,
): value is ReconciliationResponse {
  return reconciliationResponseSchema.safeParse(value).success;
}

function isConflictPageResponse(value: unknown): value is ConflictPageResponse {
  return conflictPageResponseSchema.safeParse(value).success;
}

export function isRepairPlanResponse(value: unknown): value is RepairPlanResponse {
  return repairPlanResponseSchema.safeParse(value).success;
}

function isHealthResponse(value: unknown): value is HealthResponse {
  return healthResponseSchema.safeParse(value).success;
}

function isCapabilitiesResponse(value: unknown): value is CapabilitiesResponse {
  return capabilitiesResponseSchema.safeParse(value).success;
}

function isAbortError(error: unknown, signal: AbortSignal | undefined): boolean {
  if (signal?.aborted === true) {
    return true;
  }
  return error instanceof Error && error.name === "AbortError";
}

/**
 * The `Idempotency-Key` header accepts 1–128 portable ASCII characters;
 * validating before send keeps malformed keys a local programmer error
 * instead of a server round trip.
 */
export function isValidIdempotencyKey(key: string): boolean {
  return key.length >= 1 && key.length <= 128 && /^[\x20-\x7e]+$/.test(key);
}

export interface RequestLike {
  signal?: AbortSignal;
  headers?: Record<string, string>;
}

async function parseErrorResponse(response: Response): Promise<ApiRequestError> {
  let bodyText = "";
  try {
    bodyText = (await response.text()).slice(0, MAX_ERROR_BODY_BYTES);
  } catch {
    bodyText = "";
  }
  let parsedBody: unknown;
  try {
    parsedBody = bodyText === "" ? undefined : (JSON.parse(bodyText) as unknown);
  } catch {
    parsedBody = undefined;
  }
  const problem = parseProblemDetails(parsedBody);
  return new ApiRequestError("problem", problem, response.status);
}

async function requestJson<T>(
  path: string,
  guard: (value: unknown) => value is T,
  init: RequestLike & { method?: string; body?: string } = {},
): Promise<T> {
  let response: Response;
  try {
    response = await fetch(path, {
      method: init.method ?? "GET",
      cache: "no-store",
      headers: {
        Accept: "application/json",
        ...(init.body === undefined ? {} : { "Content-Type": "application/json" }),
        ...init.headers,
      },
      body: init.body,
      signal: init.signal,
    });
  } catch (error) {
    // Cancellation propagates untouched so TanStack Query recognizes the
    // request as cancelled rather than failed.
    if (isAbortError(error, init.signal)) {
      throw error;
    }
    throw new ApiRequestError("network", {
      title: GENERIC_ERROR_TITLE,
      extensions: [],
    });
  }

  if (!response.ok) {
    throw await parseErrorResponse(response);
  }

  const contentType = response.headers.get("Content-Type") ?? "";
  if (!contentType.toLowerCase().includes("application/json")) {
    throw new ApiRequestError(
      "invalid-response",
      { title: GENERIC_ERROR_TITLE, extensions: [] },
      response.status,
    );
  }

  let payload: unknown;
  try {
    payload = (await response.json()) as unknown;
  } catch {
    throw new ApiRequestError(
      "invalid-response",
      { title: GENERIC_ERROR_TITLE, extensions: [] },
      response.status,
    );
  }

  if (!guard(payload)) {
    throw new ApiRequestError(
      "invalid-response",
      { title: GENERIC_ERROR_TITLE, extensions: [] },
      response.status,
    );
  }
  return payload;
}

function query(params: Record<string, string | number | undefined>): string {
  const search = new URLSearchParams();
  for (const [name, value] of Object.entries(params)) {
    if (value !== undefined) {
      search.set(name, String(value));
    }
  }
  const encoded = search.toString();
  return encoded === "" ? "" : `?${encoded}`;
}

function apiPath(
  segments: readonly string[],
  params?: Record<string, string | number | undefined>,
): string {
  const encoded = segments.map((segment) => encodeURIComponent(segment)).join("/");
  return `${API_BASE}/${encoded}${params === undefined ? "" : query(params)}`;
}

export interface PageParams {
  limit?: number;
  cursor?: string;
}

/** Liveness probe consumed by the shell connection indicator. */
export function fetchHealth(init: RequestLike = {}): Promise<HealthResponse> {
  return requestJson("/healthz", isHealthResponse, init);
}

export function fetchCapabilities(
  init: RequestLike = {},
): Promise<CapabilitiesResponse> {
  return requestJson(apiPath(["system", "capabilities"]), isCapabilitiesResponse, init);
}

export function fetchRuns(
  params: PageParams = {},
  init: RequestLike = {},
): Promise<RunPageResponse> {
  return requestJson(
    apiPath(["runs"], { limit: params.limit, cursor: params.cursor }),
    isRunPageResponse,
    init,
  );
}

export function fetchRun(runId: string, init: RequestLike = {}): Promise<RunResponse> {
  return requestJson(apiPath(["runs", runId]), isRunResponse, init);
}

export function fetchReconciliation(
  runId: string,
  init: RequestLike = {},
): Promise<ReconciliationResponse> {
  return requestJson(
    apiPath(["runs", runId, "reconciliation"]),
    isReconciliationResponse,
    init,
  );
}

export function fetchConflicts(
  runId: string,
  params: PageParams = {},
  init: RequestLike = {},
): Promise<ConflictPageResponse> {
  return requestJson(
    apiPath(["runs", runId, "conflicts"], {
      limit: params.limit,
      cursor: params.cursor,
    }),
    isConflictPageResponse,
    init,
  );
}

export function fetchRepairPlan(
  planId: string,
  init: RequestLike = {},
): Promise<RepairPlanResponse> {
  return requestJson(apiPath(["repair-plans", planId]), isRepairPlanResponse, init);
}

export interface MutationOptions extends RequestLike {
  /**
   * Durable idempotency key for command routes. Retries of the same logical
   * mutation must reuse one key so the server replays the stored response.
   */
  idempotencyKey?: string;
}

function requireIdempotencyKey(options: MutationOptions): Record<string, string> {
  if (options.idempotencyKey === undefined) {
    return {};
  }
  if (!isValidIdempotencyKey(options.idempotencyKey)) {
    throw new RangeError("Idempotency-Key must be 1-128 portable ASCII characters");
  }
  return { "Idempotency-Key": options.idempotencyKey };
}

export async function createRun(
  request: RunCreateRequest,
  options: MutationOptions = {},
): Promise<RunResponse> {
  return requestJson(apiPath(["runs"]), isRunResponse, {
    method: "POST",
    body: JSON.stringify(request),
    signal: options.signal,
    headers: requireIdempotencyKey(options),
  });
}

export async function createRepairPlan(
  runId: string,
  request: RepairPlanCreateRequest,
  options: MutationOptions = {},
): Promise<RepairPlanResponse> {
  return requestJson(apiPath(["runs", runId, "repair-plans"]), isRepairPlanResponse, {
    method: "POST",
    body: JSON.stringify(request),
    signal: options.signal,
    headers: requireIdempotencyKey(options),
  });
}
