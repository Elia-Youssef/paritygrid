import { QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import { useEffect } from "react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type {
  ConflictPageResponse,
  ConflictResponse,
} from "../../api/generated/schema";
import { createQueryClient } from "../../api/query-client";
import type { ReconciliationIdentity } from "./coherence";
import { useConflicts, type ConflictPagesView } from "./use-conflicts";

/** The logical dataset behind the stubbed server: 10,000 conflicts. */
const TOTAL_CONFLICTS = 10_000;
const PAGE_SIZE = 100;

// The runtime contract pins reconciliation fingerprints to 64 hex chars.
const FINGERPRINT_V1 = "a".repeat(64);
const FINGERPRINT_V2 = "b".repeat(64);

const IDENTITY_V1: ReconciliationIdentity = {
  run_id: "run-001",
  run_version: 3,
  reconciliation_fingerprint: FINGERPRINT_V1,
};

const IDENTITY_V2: ReconciliationIdentity = {
  run_id: "run-001",
  run_version: 4,
  reconciliation_fingerprint: FINGERPRINT_V2,
};

function makeConflict(index: number): ConflictResponse {
  const ordinal = String(index + 1).padStart(6, "0");
  return {
    schema_version: 1,
    conflict_id: `cnf-${ordinal}`,
    canonical_key: `KEY-${ordinal}`,
    classification: index % 2 === 0 ? "field_mismatch" : "missing_from_target",
    source_references: [{ position: 0, record_key: `src-${ordinal}` }],
    target_references: [{ position: 0, record_key: `tgt-${ordinal}` }],
    differences: [
      {
        field: "amount",
        kind: "value_mismatch",
        source_text: "100",
        target_text: "200",
      },
    ],
    suggested_resolution: index % 2 === 0 ? "update_target" : "create_target",
    created_at: "2026-01-01T00:00:00Z",
  };
}

function makePage(
  pageIndex: number,
  totalPages: number,
  identity: ReconciliationIdentity,
): ConflictPageResponse {
  const start = pageIndex * PAGE_SIZE;
  return {
    schema_version: 1,
    run_id: identity.run_id,
    run_version: identity.run_version,
    state: "complete",
    observed_at: "2026-01-01T00:00:00Z",
    reconciliation_fingerprint: identity.reconciliation_fingerprint,
    limit: PAGE_SIZE,
    next_cursor:
      pageIndex + 1 < totalPages
        ? `KEY-${String((pageIndex + 1) * PAGE_SIZE).padStart(6, "0")}`
        : null,
    items: Array.from({ length: PAGE_SIZE }, (_, offset) =>
      makeConflict(start + offset),
    ),
  };
}

function stubConflictApi(
  options: { totalPages?: number; mismatchFirstPage?: boolean; fail?: boolean } = {},
): {
  requests: string[];
  serveIdentity: (identity: ReconciliationIdentity) => void;
} {
  const requests: string[] = [];
  const totalPages = options.totalPages ?? TOTAL_CONFLICTS / PAGE_SIZE;
  // The identity the server currently stamps onto pages; tests switch it to
  // model a reconciliation snapshot change.
  let currentIdentity = IDENTITY_V1;
  const serveIdentity = (identity: ReconciliationIdentity): void => {
    currentIdentity = identity;
  };
  const fetchMock = vi.fn<typeof fetch>((input: RequestInfo | URL) => {
    const url =
      typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    requests.push(url);
    if (options.fail === true) {
      return Promise.resolve(new Response("boom", { status: 500 }));
    }
    const cursor = new URLSearchParams(url.split("?")[1] ?? "").get("cursor");
    const pageIndex =
      cursor === null
        ? 0
        : Array.from({ length: totalPages - 1 }, (_, index) => index + 1).find(
            (index) => `KEY-${String(index * PAGE_SIZE).padStart(6, "0")}` === cursor,
          );
    if (pageIndex === undefined) {
      return Promise.resolve(new Response("invalid cursor", { status: 422 }));
    }
    const identity =
      options.mismatchFirstPage === true && pageIndex === 0
        ? { ...IDENTITY_V1, run_version: IDENTITY_V1.run_version + 50 }
        : currentIdentity;
    const payload = makePage(pageIndex, totalPages, identity);
    return Promise.resolve(
      new Response(JSON.stringify(payload), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
  });
  vi.stubGlobal("fetch", fetchMock);
  return { requests, serveIdentity };
}

// The most recent view rendered by the probe; effects publish it after
// each render, so tests read it after waitFor.
let latestView: ConflictPagesView | null = null;

// The assignment lives outside the component so the hook lint never sees a
// render-scope store write.
function publishView(view: ConflictPagesView): void {
  latestView = view;
}

function ConflictProbe(props: {
  runId: string;
  identity: ReconciliationIdentity | null;
}): React.JSX.Element {
  const view = useConflicts(props.runId, props.identity);
  useEffect(() => {
    publishView(view);
  }, [view]);
  return (
    <div>
      <div data-testid="resident-count">{String(view.state.residentCount)}</div>
      <div data-testid="loaded-pages">{String(view.state.loadedPages)}</div>
      <div data-testid="resident-identity">
        {view.state.identity === null
          ? "none"
          : `${String(view.state.identity.run_version)}:${view.state.identity.reconciliation_fingerprint}`}
      </div>
      <div data-testid="rejected-pages">{String(view.state.rejectedPages)}</div>
      <div data-testid="exhausted">{String(view.state.exhausted)}</div>
      <div data-testid="selection">{view.state.selection?.conflictId ?? "none"}</div>
      <div data-testid="page-error">
        {view.pageError === null
          ? "none"
          : view.pageError instanceof Error
            ? view.pageError.message
            : "error"}
      </div>
      <ul data-testid="resident-ids">
        {view.state.conflicts.map((conflict) => (
          <li key={conflict.conflict_id}>{conflict.conflict_id}</li>
        ))}
      </ul>
      <button type="button" onClick={() => view.loadMore()}>
        load more
      </button>
      <button type="button" onClick={() => view.select("cnf-000001")}>
        select first
      </button>
      <button type="button" onClick={() => view.clearSelection()}>
        clear selection
      </button>
      <button type="button" onClick={() => view.reset()}>
        reset
      </button>
    </div>
  );
}

function renderProbe(
  identity: ReconciliationIdentity | null,
  runId = "run-001",
): {
  rerender: (ui: React.JSX.Element) => void;
  queryClient: ReturnType<typeof createQueryClient>;
} {
  const queryClient = createQueryClient();
  const view = render(
    <QueryClientProvider client={queryClient}>
      <ConflictProbe runId={runId} identity={identity} />
    </QueryClientProvider>,
  );
  return {
    queryClient,
    rerender: (ui: React.JSX.Element) => {
      view.rerender(ui);
    },
  };
}

beforeEach(() => {
  latestView = null;
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("useConflicts", () => {
  it("loads exactly the first page for a fresh identity", async () => {
    const { requests } = stubConflictApi();
    renderProbe(IDENTITY_V1);

    await waitFor(() => {
      expect(screen.getByTestId("resident-count")).toHaveTextContent(/^100$/);
      expect(screen.getByTestId("loaded-pages")).toHaveTextContent(/^1$/);
      expect(screen.getByTestId("resident-identity")).toHaveTextContent(
        `3:${FINGERPRINT_V1}`,
      );
    });
    expect(requests).toHaveLength(1);
    expect(requests[0]).toContain("/api/v1/runs/run-001/conflicts");
    expect(requests[0]).toContain("limit=100");
    expect(requests[0]).not.toContain("cursor=");
    expect(latestView?.state.identity).toEqual(IDENTITY_V1);
    expect(latestView?.state.loadedPages).toBe(1);
    expect(latestView?.state.nextCursor).toBe("KEY-000100");

    // Settled queries do not trigger hidden follow-up requests.
    await waitFor(() => {
      expect(screen.getByTestId("exhausted")).toHaveTextContent("false");
    });
    expect(requests).toHaveLength(1);
  });

  it("fetches exactly one more page per loadMore call", async () => {
    const { requests } = stubConflictApi();
    const user = userEvent.setup();
    renderProbe(IDENTITY_V1);

    await waitFor(() => {
      expect(screen.getByTestId("resident-count")).toHaveTextContent(/^100$/);
    });
    await user.click(screen.getByRole("button", { name: "load more" }));
    await waitFor(() => {
      expect(screen.getByTestId("resident-count")).toHaveTextContent(/^200$/);
    });

    expect(requests).toHaveLength(2);
    expect(requests[1]).toContain("cursor=KEY-000100");
    expect(latestView?.state.loadedPages).toBe(2);
    expect(screen.getAllByRole("listitem")).toHaveLength(200);
    // Conflicts flatten in server order across pages.
    expect(latestView?.state.conflicts[0]?.conflict_id).toBe("cnf-000001");
    expect(latestView?.state.conflicts[199]?.conflict_id).toBe("cnf-000200");
  });

  it("reports exhaustion when the server ends the chain with a null cursor", async () => {
    const { requests } = stubConflictApi({ totalPages: 1 });
    const user = userEvent.setup();
    renderProbe(IDENTITY_V1);

    await waitFor(() => {
      expect(screen.getByTestId("exhausted")).toHaveTextContent("true");
    });
    await user.click(screen.getByRole("button", { name: "load more" }));
    expect(requests).toHaveLength(1);
    expect(latestView?.state.nextCursor).toBeNull();
  });

  it("fences data and selection on an identity change and refetches the first page", async () => {
    const { requests, serveIdentity } = stubConflictApi();
    const user = userEvent.setup();
    const rendered = renderProbe(IDENTITY_V1);

    await waitFor(() => {
      expect(screen.getByTestId("resident-count")).toHaveTextContent(/^100$/);
    });
    await user.click(screen.getByRole("button", { name: "load more" }));
    await waitFor(() => {
      expect(screen.getByTestId("resident-count")).toHaveTextContent(/^200$/);
    });
    await user.click(screen.getByRole("button", { name: "select first" }));
    expect(screen.getByTestId("selection")).toHaveTextContent("cnf-000001");

    // The server re-stamps pages under a new snapshot, then the caller
    // observes the new identity.
    serveIdentity(IDENTITY_V2);
    rendered.rerender(
      <QueryClientProvider client={rendered.queryClient}>
        <ConflictProbe runId="run-001" identity={IDENTITY_V2} />
      </QueryClientProvider>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("resident-count")).toHaveTextContent(/^100$/);
      expect(screen.getByTestId("loaded-pages")).toHaveTextContent(/^1$/);
      expect(screen.getByTestId("resident-identity")).toHaveTextContent(
        `4:${FINGERPRINT_V2}`,
      );
      // The probe publishes its hook view from a passive effect. Wait for
      // that publication as well as the DOM commit so hosted-runner
      // scheduling cannot expose the preceding reset state here.
      expect(latestView?.state.loadedPages).toBe(1);
      expect(latestView?.state.identity).toEqual(IDENTITY_V2);
    });
    // The selection did not survive the fingerprint/run version change, and
    // the fresh chain restarted at the first page.
    expect(screen.getByTestId("selection")).toHaveTextContent("none");
    expect(screen.getByTestId("rejected-pages")).toHaveTextContent(/^0$/);
    expect(requests[2]).not.toContain("cursor=");
    expect(requests).toHaveLength(3);
    // The new fingerprint is part of the query key.
    const cacheKeys = rendered.queryClient
      .getQueryCache()
      .getAll()
      .map((query) => JSON.stringify(query.queryKey));
    expect(cacheKeys.some((key) => key.includes(FINGERPRINT_V2))).toBe(true);
    expect(cacheKeys.some((key) => key.includes(FINGERPRINT_V1))).toBe(false);
  });

  it("surfaces page errors raw instead of merging partial data", async () => {
    const { requests } = stubConflictApi({ fail: true });
    renderProbe(IDENTITY_V1);

    await waitFor(() => {
      expect(screen.getByTestId("page-error")).toHaveTextContent("Request failed");
    });
    expect(latestView?.pageError).toBeInstanceOf(Error);
    expect(screen.getByTestId("resident-count")).toHaveTextContent(/^0$/);
    expect(requests).toHaveLength(1);
  });

  it("never merges a page whose identity mismatches the resident snapshot", async () => {
    const { requests } = stubConflictApi({ mismatchFirstPage: true });
    renderProbe(IDENTITY_V1);

    await waitFor(() => {
      expect(screen.getByTestId("rejected-pages")).toHaveTextContent(/^1$/);
    });
    expect(screen.getByTestId("resident-count")).toHaveTextContent(/^0$/);
    // The rejected page does not wedge the chain into a fetch loop.
    expect(requests).toHaveLength(1);
    expect(latestView?.state.loadedPages).toBe(0);
  });

  it("keeps resident records bounded across repeated loadMore calls", async () => {
    stubConflictApi();
    const user = userEvent.setup();
    const rendered = renderProbe(IDENTITY_V1);

    await waitFor(() => {
      expect(screen.getByTestId("resident-count")).toHaveTextContent(/^100$/);
    });
    for (let page = 2; page <= 21; page += 1) {
      await user.click(screen.getByRole("button", { name: "load more" }));
      await waitFor(() => {
        expect(screen.getByTestId("loaded-pages")).toHaveTextContent(
          new RegExp(`^${String(page)}$`),
        );
        expect(screen.getByTestId("resident-count")).toHaveTextContent(
          new RegExp(`^${String(Math.min(page, 20) * PAGE_SIZE)}$`),
        );
      });
    }

    expect(screen.getAllByRole("listitem")).toHaveLength(2_000);
    expect(latestView?.state.pages.length).toBeLessThanOrEqual(20);
    expect(latestView?.state.residentCount).toBeLessThanOrEqual(20 * PAGE_SIZE);
    expect(latestView?.state.loadedPages).toBe(21);
    const cachedConflictPages = rendered.queryClient
      .getQueryCache()
      .getAll()
      .filter((query) => query.queryKey[2] === "conflicts");
    expect(cachedConflictPages.length).toBeLessThanOrEqual(20);
  });

  it("reloads the first page after an explicit reset", async () => {
    const { requests } = stubConflictApi();
    const user = userEvent.setup();
    renderProbe(IDENTITY_V1);

    await waitFor(() => {
      expect(screen.getByTestId("resident-count")).toHaveTextContent(/^100$/);
    });
    await user.click(screen.getByRole("button", { name: "reset" }));
    await waitFor(() => {
      expect(screen.getByTestId("resident-count")).toHaveTextContent(/^100$/);
    });
    expect(requests).toHaveLength(2);
    expect(requests[1]).not.toContain("cursor=");
    expect(screen.getByTestId("selection")).toHaveTextContent("none");
  });
});
