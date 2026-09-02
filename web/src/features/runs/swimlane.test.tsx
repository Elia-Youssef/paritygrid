/**
 * Swimlane component tests (P17.3): the table is the complete evidence and
 * matches the derived spans exactly; the decoration-only chart zooms within
 * a bounded window; empty and uncertain states are explicit.
 */
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { SwimlaneTimeline } from "./swimlane";
import { deriveSwimlane } from "./run-derivations";
import type { DurableEventFrame } from "../../live/protocol";

function frame(
  sequence: number,
  eventKind: string,
  subjectId: string,
  occurredAt: string,
  payload: Record<string, unknown>,
): DurableEventFrame {
  return {
    schema_version: 1,
    channel: "durable-events",
    sequence,
    run_id: "run-a",
    event_kind: eventKind,
    subject_kind: "work_item",
    subject_id: subjectId,
    occurred_at: occurredAt,
    payload_schema_version: 1,
    payload,
  };
}

const EVENTS: readonly DurableEventFrame[] = [
  frame(1, "work_claimed", "work-1", "2026-01-01T00:00:10Z", {
    node_id: "nod_alpha-001",
    attempt_number: 1,
    worker_identity: "worker-7",
  }),
  frame(2, "work_succeeded", "work-1", "2026-01-01T00:00:14Z", {
    node_id: "nod_alpha-001",
    attempt_number: 1,
  }),
  frame(3, "work_claimed", "work-2", "2026-01-01T00:00:30Z", {
    node_id: "nod_beta-001",
    attempt_number: 2,
  }),
];

describe("SwimlaneTimeline", () => {
  it("renders a table equivalent to the derived spans", () => {
    const result = deriveSwimlane(EVENTS);
    render(
      <SwimlaneTimeline
        result={result}
        zoom="full"
        onZoomChange={vi.fn()}
        durableUncertain={false}
      />,
    );

    const rows = screen.getAllByRole("row").slice(1);
    // One row per span (after the header), and the counts line matches the
    // derivation.
    expect(rows).toHaveLength(result.spans.length);
    const row1 = screen.getByText("work-1").closest("tr");
    expect(row1).toHaveTextContent("nod_alpha-001");
    expect(row1).toHaveTextContent("worker-7");
    expect(row1).toHaveTextContent("2026-01-01T00:00:10Z");
    expect(row1).toHaveTextContent("2026-01-01T00:00:14Z");
    // Durable service time is the distance between claim and completion.
    expect(row1).toHaveTextContent("4.000 s");
    // Queue time is never invented: the column says so explicitly.
    expect(row1).toHaveTextContent("unavailable (queue time is not durable");
    // The open claim stays active with no fabricated completion.
    const row2 = screen.getByText("work-2").closest("tr");
    expect(row2).toHaveTextContent("active at last durable event");
    expect(row2).toHaveTextContent("nod_beta-001");
    expect(screen.getByTestId("swimlane-svg")).toBeInTheDocument();
    // Units and observation semantics are stated in the summary.
    expect(screen.getByText(/service time in seconds/i)).toBeInTheDocument();
  });

  it("zooms the visible window to the most recent quarter", async () => {
    const onZoomChange = vi.fn();
    const result = deriveSwimlane(EVENTS);
    render(
      <SwimlaneTimeline
        result={result}
        zoom="half"
        onZoomChange={onZoomChange}
        durableUncertain={false}
      />,
    );
    await userEvent.selectOptions(screen.getByTestId("swimlane-zoom"), "quarter");
    expect(onZoomChange).toHaveBeenCalledWith("quarter");
  });

  it("shows an explicit empty state and the recovery uncertainty note", () => {
    const { rerender } = render(
      <SwimlaneTimeline
        result={deriveSwimlane([])}
        zoom="full"
        onZoomChange={vi.fn()}
        durableUncertain={false}
      />,
    );
    expect(screen.getByText("No worker activity observed yet")).toBeVisible();

    rerender(
      <SwimlaneTimeline
        result={deriveSwimlane(EVENTS)}
        zoom="full"
        onZoomChange={vi.fn()}
        durableUncertain={true}
      />,
    );
    expect(screen.getByText(/the newest spans may be missing/i)).toBeInTheDocument();
  });
});
