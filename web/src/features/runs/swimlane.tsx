/**
 * Worker swimlane timeline (P17.3). Spans derive exclusively from durable
 * SSE facts: a claim opens a span, a terminal event closes it, and service
 * time is the durable distance between them. Queue time is not durable, so
 * its column says so explicitly instead of showing zero. The visualization
 * is aria-hidden decoration; the table below carries the complete evidence
 * and is the keyboard-navigable surface.
 */
import { EmptyState } from "../../components/states/empty-state";
import { formatSeconds, type SwimlaneResult } from "./run-derivations";

export type SwimlaneZoom = "full" | "half" | "quarter";

const ZOOM_LABELS: ReadonlyMap<SwimlaneZoom, string> = new Map([
  ["full", "Full observed window"],
  ["half", "Most recent 50%"],
  ["quarter", "Most recent 25%"],
]);

const ZOOM_FRACTION: ReadonlyMap<SwimlaneZoom, number> = new Map([
  ["full", 1],
  ["half", 0.5],
  ["quarter", 0.25],
]);

function parseBound(iso: string): number | null {
  const parsed = Date.parse(iso);
  return Number.isNaN(parsed) ? null : parsed;
}

export interface SwimlaneTimelineProps {
  readonly result: SwimlaneResult;
  readonly zoom: SwimlaneZoom;
  readonly onZoomChange: (zoom: SwimlaneZoom) => void;
  /** True while the durable stream is recovering or stale. */
  readonly durableUncertain: boolean;
}

export function SwimlaneTimeline({
  result,
  zoom,
  onZoomChange,
  durableUncertain,
}: SwimlaneTimelineProps) {
  const parsed = result.spans
    .map((span) => {
      const start = parseBound(span.startedAt);
      const end = span.finishedAt === null ? null : parseBound(span.finishedAt);
      return { span, start, end };
    })
    .filter(
      (
        entry,
      ): entry is {
        span: SwimlaneResult["spans"][number];
        start: number;
        end: number | null;
      } => entry.start !== null,
    );

  if (parsed.length === 0) {
    return (
      <div className="p-4">
        <EmptyState
          title="No worker activity observed yet"
          description="Claim and completion events appear here as the run leases work. Counts of observed claims are never invented."
        />
      </div>
    );
  }

  const starts = parsed.map((entry) => entry.start);
  const ends = parsed.map((entry) => entry.end ?? entry.start);
  const minTime = Math.min(...starts);
  const maxTime = Math.max(...ends, minTime + 1);
  const windowStart =
    minTime + (maxTime - minTime) * (1 - (ZOOM_FRACTION.get(zoom) ?? 1));
  const visible = parsed.filter((entry) => entry.start >= windowStart - 0.5);
  const spanHeight = 22;
  const chartHeight = visible.length * spanHeight + 24;
  const width = 600;
  const scale = (value: number) =>
    ((value - windowStart) / Math.max(maxTime - windowStart, 1)) * width;

  return (
    <div className="p-4">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <label
          htmlFor="swimlane-zoom"
          className="text-2xs uppercase tracking-wide text-muted"
        >
          Time window
        </label>
        <select
          id="swimlane-zoom"
          data-testid="swimlane-zoom"
          className="rounded border border-border bg-surface px-2 py-1 text-xs text-foreground"
          value={zoom}
          onChange={(event) => {
            onZoomChange(event.target.value as SwimlaneZoom);
          }}
        >
          {[...ZOOM_LABELS].map(([value, label]) => (
            <option key={value} value={value}>
              {label}
            </option>
          ))}
        </select>
      </div>
      {durableUncertain && (
        <p className="mb-2 text-2xs text-warning" role="status">
          Timeline derives from durable events; the stream is recovering, so the newest
          spans may be missing.
        </p>
      )}
      {/* Decoration only: every value below also appears in the table. */}
      <svg
        aria-hidden="true"
        data-testid="swimlane-svg"
        className="w-full max-w-3xl motion-reduce:transition-none"
        viewBox={`0 0 ${String(width)} ${String(chartHeight)}`}
        role="presentation"
      >
        {visible.map((entry, index) => {
          const x = scale(entry.start);
          const end =
            entry.end ?? Math.min(maxTime, windowStart + (maxTime - windowStart));
          const barWidth = Math.max(scale(end) - x, 4);
          const y = index * spanHeight + 6;
          return (
            <g
              key={`${entry.span.workItemId}-${String(entry.span.attempt)}-${String(entry.span.lastSequence)}`}
            >
              <rect
                x={x}
                y={y}
                width={barWidth}
                height={12}
                rx={3}
                className={entry.end === null ? "fill-active/60" : "fill-verified/60"}
                stroke="currentColor"
                strokeWidth={0.5}
              />
              <text x={x + barWidth + 6} y={y + 10} className="fill-current text-2xs">
                {entry.span.nodeId}
                {entry.span.attempt !== null ? ` #${String(entry.span.attempt)}` : ""}
              </text>
            </g>
          );
        })}
      </svg>
      <div className="mt-3 overflow-x-auto">
        <table className="w-full text-left text-xs" aria-label="Worker activity spans">
          <caption className="sr-only">
            One row per claimed work item with its durable attempt, worker, start,
            completion, and service time.
          </caption>
          <thead>
            <tr className="border-b border-border text-muted">
              <th scope="col" className="px-2 py-1.5 font-medium">
                Work item
              </th>
              <th scope="col" className="px-2 py-1.5 font-medium">
                Node
              </th>
              <th scope="col" className="px-2 py-1.5 font-medium">
                Attempt
              </th>
              <th scope="col" className="px-2 py-1.5 font-medium">
                Worker
              </th>
              <th scope="col" className="px-2 py-1.5 font-medium">
                Started
              </th>
              <th scope="col" className="px-2 py-1.5 font-medium">
                Completed
              </th>
              <th scope="col" className="px-2 py-1.5 font-medium">
                Queue time
              </th>
              <th scope="col" className="px-2 py-1.5 font-medium">
                Service time
              </th>
              <th scope="col" className="px-2 py-1.5 font-medium">
                Outcome
              </th>
            </tr>
          </thead>
          <tbody data-testid="swimlane-tbody">
            {visible.map(({ span }) => (
              <tr
                key={`${span.workItemId}-${String(span.attempt)}-${String(span.lastSequence)}`}
                className="border-b border-border/60 last:border-b-0"
              >
                <td className="px-2 py-1.5 font-mono text-foreground">
                  {span.workItemId}
                </td>
                <td className="px-2 py-1.5 font-mono text-foreground">{span.nodeId}</td>
                <td className="px-2 py-1.5 font-mono text-muted">
                  {span.attempt !== null ? String(span.attempt) : "unavailable"}
                </td>
                <td className="px-2 py-1.5 font-mono text-muted">
                  {span.worker ?? "unavailable"}
                </td>
                <td className="px-2 py-1.5 font-mono text-muted">{span.startedAt}</td>
                <td className="px-2 py-1.5 font-mono text-muted">
                  {span.finishedAt ?? "active at last durable event"}
                </td>
                <td className="px-2 py-1.5 text-muted">
                  unavailable (queue time is not durable; see advisory panels)
                </td>
                <td className="px-2 py-1.5 font-mono text-foreground">
                  {span.serviceSeconds !== null
                    ? formatSeconds(span.serviceSeconds)
                    : "unavailable"}
                </td>
                <td className="px-2 py-1.5 text-muted">
                  {span.outcome ?? "running at last durable event"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="mt-2 text-2xs text-muted">
        Units: service time in seconds computed between durable claim and completion
        timestamps. {String(result.activeClaims)} claim(s) still active;{" "}
        {String(result.unpairedEvents)} work event(s) could not be paired and are never
        invented into spans.
      </p>
    </div>
  );
}
