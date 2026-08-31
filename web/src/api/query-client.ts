/**
 * The single QueryClient ownership boundary. All queries and mutations in
 * the application share these defaults; nothing else may construct policy
 * for cached server state.
 *
 * Policy summary:
 * - Deterministic client failures (every HTTP 4xx, including idempotency
 *   and conflict responses) are never retried: replaying them cannot
 *   succeed and must not re-execute server commands.
 * - Network failures and 5xx retry a bounded two times with deterministic
 *   exponential delay (no jitter — behavior must be reproducible on Windows
 *   CI and in tests).
 * - Mutations never retry automatically. Command replay happens only
 *   through an explicit user action that re-issues the same idempotency key.
 * - Cancellation (AbortError) is never retried.
 * - Server data is cached and stale-marked; window focus never triggers
 *   hidden refetches (refresh is explicit through invalidation).
 */
import { QueryClient } from "@tanstack/react-query";

import { ApiRequestError } from "./client";

export const MAX_QUERY_RETRIES = 2;
export const RETRY_BASE_DELAY_MS = 500;
export const RETRY_MAX_DELAY_MS = 4_000;

export function isAbortErrorName(error: unknown): boolean {
  // DOMException does not extend Error in every runtime (Node vs browser),
  // so the name is duck-typed off a non-null object.
  return (
    typeof error === "object" &&
    error !== null &&
    (error as { name?: unknown }).name === "AbortError"
  );
}

/** Deterministic exponential delay for the retry attempt that just failed. */
export function retryDelayMs(failureCount: number): number {
  return Math.min(RETRY_BASE_DELAY_MS * 2 ** failureCount, RETRY_MAX_DELAY_MS);
}

/** Bounded retry decision shared by every query. */
export function queryRetry(failureCount: number, error: unknown): boolean {
  if (isAbortErrorName(error)) {
    return false;
  }
  if (error instanceof ApiRequestError) {
    // Deterministic responses, including conflicts and validation
    // failures, are terminal; only transport-grade uncertainty retries.
    if (error.kind === "problem" && error.status !== undefined && error.status < 500) {
      return false;
    }
  }
  return failureCount < MAX_QUERY_RETRIES;
}

export function createQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        retry: queryRetry,
        retryDelay: retryDelayMs,
        staleTime: 15_000,
        gcTime: 5 * 60_000,
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
      },
      mutations: {
        // Command replay is always explicit; automatic retries could
        // re-issue commands whose outcome the server has already stored.
        retry: 0,
      },
    },
  });
}

/**
 * The application-wide client. One instance owns all server cache state;
 * tests construct isolated clients through `createQueryClient`.
 */
export const appQueryClient: QueryClient = createQueryClient();
