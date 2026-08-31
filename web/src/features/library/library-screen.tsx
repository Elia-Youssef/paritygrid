/**
 * Pipeline library: deterministic, accessible discovery over the paginated
 * REST collection. Search text, page, archived flag, and the selected
 * pipeline all live in the URL, so reload, back/forward, and direct links
 * reproduce the same view. The REST query fetches a bounded set of pages;
 * search is a deterministic client-side filter over those fetched pages
 * (the collection endpoint has no search parameter). Late responses can
 * never move the selection: selection follows the URL, never the network.
 */
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router";
import { useMemo, useState } from "react";

import { createPipeline, fetchPipelines } from "../../api/client";
import { queryKeys } from "../../api/query-keys";
import type { PipelineResponseValue } from "../../api/schemas";
import { EmptyState } from "../../components/states/empty-state";
import { ErrorState } from "../../components/states/error-state";
import { Loading } from "../../components/states/loading";
import { StatusBadge } from "../../components/ui/status-badge";

const PAGE_LIMIT = 50;
const MAX_FETCHED_PAGES = 4;
const PAGE_SIZE = 10;

function canonicalPipelineId(value: string): boolean {
  return /^pip_[a-z0-9]+(?:-[a-z0-9]+)*$/.test(value);
}

interface LibraryView {
  items: readonly PipelineResponseValue[];
  complete: boolean;
}

export function PipelineLibrary() {
  const [searchParams, setSearchParams] = useSearchParams();
  const queryClient = useQueryClient();
  const [createOpen, setCreateOpen] = useState(false);

  const rawSearch = searchParams.get("q") ?? "";
  const search = rawSearch.toLowerCase();
  const archived = searchParams.get("archived") === "1";
  const selectedParam = searchParams.get("selected");
  const selection =
    selectedParam !== null && canonicalPipelineId(selectedParam) ? selectedParam : null;
  const requestedPage = Number.parseInt(searchParams.get("page") ?? "1", 10);
  const page =
    Number.isInteger(requestedPage) && requestedPage >= 1 ? requestedPage : 1;

  const pages = useQuery({
    queryKey: queryKeys.pipelineList({ limit: PAGE_LIMIT, includeArchived: archived }),
    queryFn: async ({ signal }) => {
      const collected: PipelineResponseValue[] = [];
      let cursor: string | null = null;
      for (let index = 0; index < MAX_FETCHED_PAGES; index += 1) {
        const page_ = await fetchPipelines(
          { limit: PAGE_LIMIT, cursor: cursor ?? undefined, includeArchived: archived },
          { signal },
        );
        collected.push(...page_.items);
        if (page_.next_cursor === null) {
          return { items: collected, complete: true } satisfies LibraryView;
        }
        cursor = page_.next_cursor;
      }
      return { items: collected, complete: false } satisfies LibraryView;
    },
    staleTime: 10_000,
    retry: 0,
  });

  const updateParams = (changes: Record<string, string | null>): void => {
    const next = new URLSearchParams(searchParams);
    for (const [name, value] of Object.entries(changes)) {
      if (value === null || value === "") {
        next.delete(name);
      } else {
        next.set(name, value);
      }
    }
    setSearchParams(next, { replace: false });
  };

  const filtered = useMemo(() => {
    const all = pages.data?.items ?? [];
    if (search === "") {
      return all;
    }
    return all.filter(
      (pipeline) =>
        pipeline.pipeline_id.toLowerCase().includes(search) ||
        pipeline.display_name.toLowerCase().includes(search) ||
        (pipeline.description ?? "").toLowerCase().includes(search),
    );
  }, [pages.data, search]);

  const pageCount = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const safePage = Math.min(page, pageCount);
  const pageItems = filtered.slice((safePage - 1) * PAGE_SIZE, safePage * PAGE_SIZE);
  // A selected pipeline that the current filter or page hides keeps its
  // selection state (URL) and is surfaced explicitly instead of vanishing.
  const selectedVisible =
    selection !== null &&
    pageItems.some((pipeline) => pipeline.pipeline_id === selection);
  const selectedElsewhere =
    selection !== null && !selectedVisible
      ? (pages.data?.items.find((pipeline) => pipeline.pipeline_id === selection) ??
        null)
      : null;

  return (
    <div className="space-y-4 p-6">
      <div className="flex flex-wrap items-end justify-between gap-2">
        <div>
          <h1
            data-page-title
            tabIndex={-1}
            className="text-lg font-semibold text-foreground"
          >
            Pipeline library
          </h1>
          <p className="mt-1 max-w-2xl text-sm text-muted">
            Published, immutable pipeline versions. Search filters the loaded collection
            deterministically; the URL carries the full view state.
          </p>
        </div>
        <button
          type="button"
          className="rounded bg-primary px-3 py-1.5 text-xs font-medium text-primary-foreground"
          data-testid="new-pipeline"
          onClick={() => {
            setCreateOpen(true);
          }}
        >
          New pipeline
        </button>
      </div>

      <form
        className="flex flex-wrap items-center gap-2"
        role="search"
        aria-label="Search pipelines"
        onSubmit={(event) => {
          event.preventDefault();
        }}
      >
        <label htmlFor="library-search" className="text-xs text-muted">
          Search
        </label>
        <input
          id="library-search"
          type="search"
          className="w-64 rounded border border-border bg-surface px-2 py-1.5 text-xs text-foreground"
          value={rawSearch}
          onChange={(event) => {
            // Debounce is unnecessary for correctness here: the filter is a
            // pure function of the URL, and the URL update is synchronous.
            updateParams({ q: event.target.value, page: null });
          }}
          data-testid="library-search"
        />
        <label className="flex items-center gap-1 text-xs text-muted">
          <input
            type="checkbox"
            checked={archived}
            onChange={(event) => {
              updateParams({ archived: event.target.checked ? "1" : null, page: null });
            }}
          />
          Include archived
        </label>
        <span className="text-2xs text-muted" role="status">
          {pages.isPending
            ? "Loading…"
            : `${String(filtered.length)} match(es)${pages.data?.complete === false ? " within the loaded pages" : ""}`}
        </span>
      </form>

      {pages.isPending ? (
        <Loading label="Loading pipelines" />
      ) : pages.isError ? (
        <ErrorState
          title="Pipelines unavailable"
          description="The pipeline collection could not be loaded."
          action={
            <button
              type="button"
              className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
              onClick={() => {
                void pages.refetch();
              }}
            >
              Retry
            </button>
          }
        />
      ) : pageItems.length === 0 && filtered.length === 0 ? (
        <EmptyState
          title={search === "" ? "No pipelines yet" : "No pipelines match this search"}
          description={
            search === ""
              ? "Create the first pipeline to begin authoring."
              : "Adjust the search text or clear it to see the full loaded collection."
          }
          action={
            <button
              type="button"
              className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
              onClick={() => {
                updateParams({ q: null });
              }}
            >
              Clear search
            </button>
          }
        />
      ) : (
        <table className="w-full text-left text-xs" aria-label="Pipelines">
          <thead>
            <tr className="border-b border-border text-muted">
              <th scope="col" className="py-1.5 pr-3 font-medium">
                Pipeline
              </th>
              <th scope="col" className="py-1.5 pr-3 font-medium">
                Name
              </th>
              <th scope="col" className="py-1.5 pr-3 font-medium">
                Status
              </th>
              <th scope="col" className="py-1.5 font-medium">
                Created
              </th>
            </tr>
          </thead>
          <tbody>
            {pageItems.map((pipeline) => {
              const isSelected = pipeline.pipeline_id === selection;
              return (
                <tr
                  key={pipeline.pipeline_id}
                  className={`border-b border-border/60 ${isSelected ? "bg-active/10" : ""}`}
                  data-testid={`pipeline-row-${pipeline.pipeline_id}`}
                >
                  <td className="py-1.5 pr-3 font-mono text-foreground">
                    {pipeline.pipeline_id}
                  </td>
                  <td className="py-1.5 pr-3 text-foreground">
                    {pipeline.display_name}
                  </td>
                  <td className="py-1.5 pr-3">
                    <StatusBadge
                      state={pipeline.archived_at === null ? "active" : "neutral"}
                    >
                      {pipeline.archived_at === null ? "active" : "archived"}
                    </StatusBadge>
                  </td>
                  <td className="py-1.5 font-mono text-muted">{pipeline.created_at}</td>
                  <td className="py-1.5 text-right">
                    <Link
                      to={`/app/pipelines/${pipeline.pipeline_id}`}
                      className="rounded border border-border px-2 py-0.5 text-2xs text-active hover:bg-surface-elevated"
                      aria-label={`Open pipeline studio for ${pipeline.pipeline_id}`}
                    >
                      Open studio
                    </Link>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}

      <nav aria-label="Library pages" className="flex items-center gap-2 text-xs">
        <button
          type="button"
          className="rounded border border-border px-2 py-1 text-foreground disabled:opacity-50"
          disabled={safePage <= 1}
          onClick={() => {
            updateParams({ page: String(safePage - 1) });
          }}
        >
          Previous page
        </button>
        <span className="font-mono text-muted">
          {String(safePage)} / {String(pageCount)}
        </span>
        <button
          type="button"
          className="rounded border border-border px-2 py-1 text-foreground disabled:opacity-50"
          disabled={safePage >= pageCount}
          onClick={() => {
            updateParams({ page: String(safePage + 1) });
          }}
        >
          Next page
        </button>
      </nav>

      {selectedElsewhere !== null && (
        <p className="text-2xs text-warning" role="status">
          Selected pipeline{" "}
          <span className="font-mono">{selectedElsewhere.pipeline_id}</span> is not on
          this page or matches the filter; its selection is preserved in the URL.
        </p>
      )}

      {createOpen && (
        <CreatePipelineDialog
          onClose={() => {
            setCreateOpen(false);
          }}
          onCreated={(pipeline) => {
            setCreateOpen(false);
            void queryClient.invalidateQueries({ queryKey: queryKeys.pipelinesRoot() });
            updateParams({ selected: pipeline.pipeline_id });
          }}
        />
      )}
    </div>
  );
}

function CreatePipelineDialog(props: {
  onClose: () => void;
  onCreated: (pipeline: PipelineResponseValue) => void;
}): React.JSX.Element {
  const { onClose, onCreated } = props;
  const [pipelineId, setPipelineId] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [description, setDescription] = useState("");
  const [problem, setProblem] = useState<string | null>(null);
  const create = useMutation({
    mutationFn: () =>
      createPipeline(
        {
          pipeline_id: pipelineId,
          display_name: displayName,
          description: description === "" ? null : description,
        },
        { idempotencyKey: `create-${pipelineId}` },
      ),
    retry: 0,
    onSuccess: onCreated,
    onError: (error: unknown) => {
      setProblem(
        (error as { message?: string }).message ?? "The pipeline could not be created.",
      );
    },
  });

  const valid = canonicalPipelineId(pipelineId) && displayName.length > 0;

  return (
    <div
      className="fixed inset-0 z-50 flex items-start justify-center bg-background/70"
      role="presentation"
      onKeyDown={(event) => {
        if (event.key === "Escape") {
          onClose();
        }
      }}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-label="New pipeline"
        className="mt-[12vh] w-[calc(100%-2rem)] max-w-md rounded-lg border border-border-strong bg-surface p-4 shadow-overlay"
      >
        <h2 className="text-sm font-semibold text-foreground">New pipeline</h2>
        <form
          className="mt-3 space-y-3"
          onSubmit={(event) => {
            event.preventDefault();
            if (valid) {
              create.mutate();
            }
          }}
        >
          <div>
            <label
              htmlFor="create-pipeline-id"
              className="block text-xs font-medium text-foreground"
            >
              Pipeline ID
            </label>
            <p id="create-pipeline-id-help" className="text-2xs text-muted">
              Canonical form: pip_ followed by lowercase letters, digits, or dashes.
            </p>
            <input
              id="create-pipeline-id"
              aria-describedby="create-pipeline-id-help"
              className="mt-1 w-full rounded border border-border bg-surface px-2 py-1.5 font-mono text-xs text-foreground"
              value={pipelineId}
              onChange={(event) => {
                setPipelineId(event.target.value);
              }}
              data-testid="create-pipeline-id"
            />
          </div>
          <div>
            <label
              htmlFor="create-pipeline-name"
              className="block text-xs font-medium text-foreground"
            >
              Display name
            </label>
            <input
              id="create-pipeline-name"
              className="mt-1 w-full rounded border border-border bg-surface px-2 py-1.5 text-xs text-foreground"
              value={displayName}
              onChange={(event) => {
                setDisplayName(event.target.value);
              }}
              data-testid="create-pipeline-name"
            />
          </div>
          <div>
            <label
              htmlFor="create-pipeline-description"
              className="block text-xs font-medium text-foreground"
            >
              Description (optional)
            </label>
            <input
              id="create-pipeline-description"
              className="mt-1 w-full rounded border border-border bg-surface px-2 py-1.5 text-xs text-foreground"
              value={description}
              onChange={(event) => {
                setDescription(event.target.value);
              }}
            />
          </div>
          {problem !== null && (
            <p role="alert" className="text-2xs text-failure">
              {problem}
            </p>
          )}
          <div className="flex justify-end gap-2">
            <button
              type="button"
              className="rounded border border-border px-3 py-1.5 text-xs text-foreground"
              onClick={onClose}
            >
              Cancel
            </button>
            <button
              type="submit"
              className="rounded bg-primary px-3 py-1.5 text-xs font-medium text-primary-foreground disabled:opacity-50"
              disabled={!valid || create.isPending}
              data-testid="create-pipeline-submit"
            >
              {create.isPending ? "Creating…" : "Create pipeline"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
