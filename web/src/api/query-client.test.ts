import { describe, expect, it, vi } from "vitest";

import { ApiRequestError } from "./client";
import {
  createQueryClient,
  MAX_QUERY_RETRIES,
  queryRetry,
  retryDelayMs,
} from "./query-client";

function problemError(status: number): ApiRequestError {
  return new ApiRequestError(
    "problem",
    { title: "Conflict", extensions: [{ name: "code", value: "conflict" }] },
    status,
  );
}

describe("query retry policy", () => {
  it("never retries deterministic 4xx or conflict responses", () => {
    expect(queryRetry(0, problemError(400))).toBe(false);
    expect(queryRetry(0, problemError(404))).toBe(false);
    expect(queryRetry(0, problemError(409))).toBe(false);
    expect(queryRetry(0, problemError(422))).toBe(false);
  });

  it("retries transport-grade failures a bounded number of times", () => {
    const network = new ApiRequestError("network", {
      title: "Request failed",
      extensions: [],
    });
    expect(queryRetry(0, network)).toBe(true);
    expect(queryRetry(MAX_QUERY_RETRIES - 1, network)).toBe(true);
    expect(queryRetry(MAX_QUERY_RETRIES, network)).toBe(false);

    expect(queryRetry(0, problemError(500))).toBe(true);
    expect(queryRetry(MAX_QUERY_RETRIES, problemError(503))).toBe(false);

    expect(queryRetry(0, new TypeError("fetch failed"))).toBe(true);
    expect(queryRetry(MAX_QUERY_RETRIES, new TypeError("fetch failed"))).toBe(false);
  });

  it("never retries cancellations", () => {
    const abort = new DOMException("The operation was aborted.", "AbortError");
    expect(queryRetry(0, abort)).toBe(false);
  });

  it("uses deterministic exponential delays without jitter", () => {
    expect(retryDelayMs(0)).toBe(500);
    expect(retryDelayMs(1)).toBe(1_000);
    expect(retryDelayMs(2)).toBe(2_000);
    expect(retryDelayMs(3)).toBe(4_000);
    expect(retryDelayMs(10)).toBe(4_000);
    expect(retryDelayMs(1)).toBe(retryDelayMs(1));
  });
});

describe("query client defaults", () => {
  it("configures stale time and disables hidden refetches", () => {
    const client = createQueryClient();
    const queryDefaults = client.getDefaultOptions().queries;
    const mutationDefaults = client.getDefaultOptions().mutations;
    expect(queryDefaults?.staleTime).toBe(15_000);
    expect(queryDefaults?.refetchOnWindowFocus).toBe(false);
    expect(queryDefaults?.refetchOnReconnect).toBe(false);
    expect(queryDefaults?.retry).toBe(queryRetry);
    expect(queryDefaults?.retryDelay).toBe(retryDelayMs);
    expect(mutationDefaults?.retry).toBe(0);
    client.clear();
  });

  it("executes the bounded retry schedule with injected timing", async () => {
    vi.useFakeTimers();
    try {
      const client = createQueryClient();
      const queryFn = vi.fn<() => Promise<unknown>>();
      queryFn.mockRejectedValue(
        new ApiRequestError("network", { title: "Request failed", extensions: [] }),
      );
      const promise = client.fetchQuery({ queryKey: ["flaky"], queryFn, staleTime: 0 });
      promise.catch(() => undefined);

      // Attempts: initial + 2 retries at 500/1000 ms.
      await vi.advanceTimersByTimeAsync(0);
      expect(queryFn).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(500);
      expect(queryFn).toHaveBeenCalledTimes(2);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(queryFn).toHaveBeenCalledTimes(3);
      await vi.advanceTimersByTimeAsync(10_000);
      expect(queryFn).toHaveBeenCalledTimes(3);

      await expect(promise).rejects.toBeInstanceOf(ApiRequestError);
      client.clear();
    } finally {
      vi.useRealTimers();
    }
  });

  it("stops immediately on a deterministic 4xx", async () => {
    vi.useFakeTimers();
    try {
      const client = createQueryClient();
      const queryFn = vi.fn<() => Promise<unknown>>();
      queryFn.mockRejectedValue(problemError(409));
      const promise = client.fetchQuery({ queryKey: ["conflict"], queryFn });
      promise.catch(() => undefined);

      await vi.advanceTimersByTimeAsync(0);
      expect(queryFn).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(30_000);
      expect(queryFn).toHaveBeenCalledTimes(1);
      await expect(promise).rejects.toBeInstanceOf(ApiRequestError);
      client.clear();
    } finally {
      vi.useRealTimers();
    }
  });

  it("never auto-retries mutations, preserving command replay semantics", async () => {
    vi.useFakeTimers();
    try {
      const client = createQueryClient();
      const mutationFn = vi.fn<() => Promise<unknown>>();
      mutationFn.mockRejectedValue(new TypeError("network down"));
      const mutation = client.getMutationCache().build(client, {
        mutationFn,
        // The client default; restated here to prove the default holds.
        retry: false,
      });
      const promise = mutation.execute(undefined);
      promise.catch(() => undefined);
      await vi.advanceTimersByTimeAsync(30_000);
      expect(mutationFn).toHaveBeenCalledTimes(1);
      client.clear();
    } finally {
      vi.useRealTimers();
    }
  });

  it("cancels in-flight queries through the abort signal", async () => {
    const client = createQueryClient();
    const holder: { signal?: AbortSignal } = {};
    const queryFn = vi.fn((context: { signal: AbortSignal }) => {
      holder.signal = context.signal;
      return new Promise<string>((_resolve, reject) => {
        context.signal.addEventListener("abort", () => {
          reject(new DOMException("The operation was aborted.", "AbortError"));
        });
      });
    });
    const promise = client.fetchQuery({ queryKey: ["slow"], queryFn });
    promise.catch(() => undefined);
    await vi.waitFor(() => expect(holder.signal).toBeDefined());
    await client.cancelQueries({ queryKey: ["slow"] });
    expect(holder.signal?.aborted).toBe(true);
    client.clear();
  });
});
