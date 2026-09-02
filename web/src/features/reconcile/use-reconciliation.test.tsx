import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { isApiRequestError } from "../../api/client";
import { useReconciliation } from "./use-reconciliation";

function reconciliationPayload(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    schema_version: 1,
    run_id: "run_recon-001",
    run_version: 4,
    state: "succeeded",
    observed_at: "2026-01-02 03:04:05",
    reconciliation_fingerprint: "a".repeat(64),
    source_input_identity: "b".repeat(64),
    target_input_identity: "c".repeat(64),
    total_count: 2,
    counts: {
      match: 1,
      missing_from_target: 0,
      missing_from_source: 0,
      field_mismatch: 1,
      duplicate_source: 0,
      duplicate_target: 0,
      duplicate_both: 0,
    },
    analytical_query_version: 2,
    reconciliation_observed_at: "2026-01-02 03:04:06",
    ...overrides,
  };
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function createWrapper(): (props: { children?: ReactNode }) => ReactNode {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return function Wrapper({ children }) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
  };
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("useReconciliation", () => {
  it("renders the first fetched snapshot", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() => Promise.resolve(jsonResponse(reconciliationPayload()))),
    );
    const { result } = renderHook(() => useReconciliation("run_recon-001"), {
      wrapper: createWrapper(),
    });
    expect(result.current.isPending).toBe(true);
    await waitFor(() => {
      expect(result.current.summary).not.toBeNull();
    });
    expect(result.current.summary?.reconciliation_fingerprint).toBe("a".repeat(64));
    expect(result.current.summary?.total_count).toBe(2);
    expect(result.current.incompatible).toBe(false);
    expect(result.current.quarantined).toBeNull();
    expect(result.current.isPending).toBe(false);
  });

  it("replaces the rendered snapshot with a compatible refetch", async () => {
    const fetchMock = vi.fn<typeof fetch>();
    fetchMock.mockResolvedValueOnce(jsonResponse(reconciliationPayload()));
    fetchMock.mockResolvedValueOnce(
      jsonResponse(
        reconciliationPayload({
          total_count: 3,
          counts: {
            match: 1,
            missing_from_target: 1,
            missing_from_source: 1,
            field_mismatch: 0,
            duplicate_source: 0,
            duplicate_target: 0,
            duplicate_both: 0,
          },
          observed_at: "2026-01-02 03:10:00",
        }),
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    const { result } = renderHook(() => useReconciliation("run_recon-001"), {
      wrapper: createWrapper(),
    });
    await waitFor(() => {
      expect(result.current.summary).not.toBeNull();
    });

    act(() => {
      result.current.refetch();
    });
    await waitFor(() => {
      expect(result.current.summary?.total_count).toBe(3);
    });
    expect(result.current.incompatible).toBe(false);
    expect(result.current.quarantined).toBeNull();
  });

  it("quarantines an incompatible refetch and adopts it only on demand", async () => {
    const fetchMock = vi.fn<typeof fetch>();
    fetchMock.mockResolvedValueOnce(jsonResponse(reconciliationPayload()));
    fetchMock.mockResolvedValueOnce(
      jsonResponse(
        reconciliationPayload({
          reconciliation_fingerprint: "f".repeat(64),
          total_count: 5,
          counts: {
            match: 1,
            missing_from_target: 1,
            missing_from_source: 1,
            field_mismatch: 1,
            duplicate_source: 1,
            duplicate_target: 0,
            duplicate_both: 0,
          },
        }),
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    const { result } = renderHook(() => useReconciliation("run_recon-001"), {
      wrapper: createWrapper(),
    });
    await waitFor(() => {
      expect(result.current.summary).not.toBeNull();
    });

    // Reconciliation re-ran: the newer snapshot must not merge into the view.
    act(() => {
      result.current.refetch();
    });
    await waitFor(() => {
      expect(result.current.incompatible).toBe(true);
    });
    expect(result.current.summary?.reconciliation_fingerprint).toBe("a".repeat(64));
    expect(result.current.summary?.total_count).toBe(2);
    expect(result.current.quarantined?.reconciliation_fingerprint).toBe("f".repeat(64));
    expect(result.current.quarantined?.total_count).toBe(5);

    // Explicit operator adoption swaps the rendered snapshot.
    act(() => {
      result.current.adoptQuarantined();
    });
    await waitFor(() => {
      expect(result.current.incompatible).toBe(false);
    });
    expect(result.current.summary?.reconciliation_fingerprint).toBe("f".repeat(64));
    expect(result.current.summary?.total_count).toBe(5);
    expect(result.current.quarantined).toBeNull();
  });

  it("surfaces a problem-details failure as the query error", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve(
          new Response(JSON.stringify({ title: "Run unavailable", status: 500 }), {
            status: 500,
            headers: { "Content-Type": "application/problem+json" },
          }),
        ),
      ),
    );
    const { result } = renderHook(() => useReconciliation("run_recon-001"), {
      wrapper: createWrapper(),
    });
    await waitFor(() => {
      expect(result.current.error).not.toBeNull();
    });
    expect(isApiRequestError(result.current.error)).toBe(true);
    if (isApiRequestError(result.current.error)) {
      expect(result.current.error.status).toBe(500);
    }
    expect(result.current.summary).toBeNull();
  });

  it("surfaces a network failure as the query error", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() => Promise.reject(new TypeError("fetch failed"))),
    );
    const { result } = renderHook(() => useReconciliation("run_recon-001"), {
      wrapper: createWrapper(),
    });
    await waitFor(() => {
      expect(result.current.error).not.toBeNull();
    });
    expect(isApiRequestError(result.current.error)).toBe(true);
    if (isApiRequestError(result.current.error)) {
      expect(result.current.error.kind).toBe("network");
    }
    expect(result.current.summary).toBeNull();
  });
});
