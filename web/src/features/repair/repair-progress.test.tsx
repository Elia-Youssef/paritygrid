import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { ApiRequestError } from "../../api/client";
import type { RepairApplyResponse } from "../../api/generated/schema";
import { expectNoAccessibilityViolations } from "../../test/axe";
import { RepairProgress } from "./repair-progress";
import type { RepairProgress as RepairProgressView } from "./use-repair-plan";

const HEX_A = "a".repeat(64);
const HEX_B = "b".repeat(64);

function progressView(overrides: Partial<RepairProgressView> = {}): RepairProgressView {
  return {
    connected: "live",
    notifications: [],
    quarantinedEvents: 0,
    // Invariant under test: never derived from events, always null.
    lastCompletedDisposition: null,
    ...overrides,
  };
}

function applyResponse(
  overrides: Partial<RepairApplyResponse> = {},
): RepairApplyResponse {
  return {
    content_fingerprint: HEX_A,
    disposition: "completed",
    effects: [],
    observed_at: "2026-01-01 00:00:00",
    plan_id: "rp_plan-001",
    reconciliation_fingerprint: HEX_B,
    resumed: false,
    run_id: "run-001",
    run_version: 3,
    state: "reconciled",
    status: "applied",
    ...overrides,
  };
}

function renderProgress(
  overrides: {
    applyResult?: RepairApplyResponse | null;
    applyError?: unknown;
    isApplying?: boolean;
    progress?: RepairProgressView;
    planStatus?: string;
  } = {},
): HTMLElement {
  const { container } = render(
    <RepairProgress
      planStatus={overrides.planStatus ?? "approved"}
      progress={
        overrides.progress ??
        progressView({
          notifications: [
            {
              sequence: 1,
              kind: "repair_application_started",
              at: "2026-01-01 00:01:00",
            },
            { sequence: 2, kind: "repair_action_applied", at: "2026-01-01 00:01:05" },
          ],
        })
      }
      actionSummary={{ total: 5, pending: 3, applied: 1, failed: 1 }}
      applyResult={overrides.applyResult === undefined ? null : overrides.applyResult}
      applyError={overrides.applyError ?? null}
      isApplying={overrides.isApplying ?? false}
    />,
  );
  return container;
}

describe("RepairProgress", () => {
  it("renders the authoritative plan status and action counts", () => {
    renderProgress({ planStatus: "applying" });
    expect(screen.getByTestId("repair-progress")).toBeVisible();
    expect(screen.getByText("Applying")).toBeVisible();
    expect(screen.getByText(/5 total — 3 pending, 1 applied, 1 failed/)).toBeVisible();
    // The event list is explicitly labeled as non-authoritative.
    expect(
      screen.getByText(/notifications only — not authoritative state/i),
    ).toBeVisible();
    expect(screen.getByText(/sequence 1 · repair_application_started/)).toBeVisible();
    expect(screen.getByText(/sequence 2 · repair_action_applied/)).toBeVisible();
  });

  it("shows a completed disposition as a success outcome", () => {
    renderProgress({ applyResult: applyResponse() });
    expect(screen.getByTestId("outcome-disposition")).toHaveTextContent("Completed");
    expect(screen.getByTestId("progress-outcome")).toHaveTextContent(
      "reached its proposed after-state",
    );
  });

  it("labels already_applied explicitly as an idempotent replay success", () => {
    renderProgress({
      applyResult: applyResponse({ disposition: "already_applied", resumed: true }),
    });
    expect(screen.getByTestId("outcome-disposition")).toHaveTextContent(
      "Already applied — idempotent replay",
    );
    expect(screen.getByTestId("progress-outcome")).toHaveTextContent(
      "replayed the stored outcome",
    );
  });

  it("renders a failed disposition without any success wording and bounded diagnostics", () => {
    const longDetail = `connector rejected the write: ${"x".repeat(600)}`;
    const container = renderProgress({
      applyResult: applyResponse({
        disposition: "failed",
        effects: [
          {
            action_id: "act-1",
            canonical_key: "record-1",
            outcome: "failed",
            attempts: 3,
          },
        ],
      }),
      applyError: new ApiRequestError(
        "problem",
        { title: "Apply failed", detail: longDetail, extensions: [] },
        200,
      ),
    });
    expect(screen.getByTestId("outcome-disposition")).toHaveTextContent("Failed");
    const outcome = screen.getByTestId("progress-outcome");
    expect(outcome.textContent).toContain("failed application outcome");
    expect(outcome.textContent).toContain("diagnostic: connector rejected the write:");
    // The diagnostic is bounded (512 characters total, including its
    // prefix), never a stack trace.
    expect(container.textContent).toContain("x".repeat(400));
    expect(container.textContent).not.toContain("x".repeat(513));
    expect(container.textContent?.toLowerCase() ?? "").not.toContain("success");
  });

  it("renders unresolved as an explicitly recoverable outcome, not a success", () => {
    renderProgress({ applyResult: applyResponse({ disposition: "unresolved" }) });
    expect(screen.getByTestId("outcome-disposition")).toHaveTextContent(/unresolved/i);
    expect(screen.getByTestId("apply-retry")).toHaveTextContent(
      /retry resumes the same plan/i,
    );
    const outcome = screen.getByTestId("progress-outcome");
    expect(outcome.textContent?.toLowerCase() ?? "").toContain("not a success");
    expect(screen.queryByTestId("outcome-disposition")?.textContent).not.toBe(
      "Completed",
    );
  });

  it("renders interrupted as recoverable with resume language", () => {
    renderProgress({ applyResult: applyResponse({ disposition: "interrupted" }) });
    expect(screen.getByTestId("outcome-disposition")).toHaveTextContent(
      "Interrupted — retry to resume",
    );
    expect(screen.getByTestId("apply-retry")).toBeVisible();
  });

  it("renders a 503 apply error as a recoverable failure and never as success", () => {
    renderProgress({
      applyError: new ApiRequestError(
        "problem",
        {
          title: "Service Unavailable",
          detail: "application unresolved",
          extensions: [],
        },
        503,
      ),
    });
    expect(screen.getByText(/Recoverable failure \(503\)/)).toBeVisible();
    expect(screen.getByTestId("apply-retry")).toHaveTextContent(
      /retry resumes the same plan/i,
    );
    const outcome = screen.getByTestId("progress-outcome");
    expect(outcome.textContent).toContain("did not complete");
    expect(outcome.textContent).not.toContain("Completed");
  });

  it("warns visibly when stream frames were quarantined", () => {
    renderProgress({ progress: progressView({ quarantinedEvents: 3 }) });
    expect(screen.getByTestId("progress-quarantined")).toHaveTextContent(
      /3 durable stream frames/,
    );
  });

  it("renders a bounded notification list (newest last)", () => {
    const many = Array.from({ length: 60 }, (_, index) => ({
      sequence: index + 1,
      kind: "repair_action_applied",
      at: "2026-01-01 00:01:00",
    }));
    renderProgress({ progress: progressView({ notifications: many }) });
    const list = screen.getByTestId("progress-notifications");
    expect(list.children.length).toBe(50);
    expect(list.textContent).toContain("sequence 60");
    expect(list.textContent).toContain("sequence 11");
    expect(list.textContent).not.toContain("sequence 10 ");
  });

  it("shows the busy state while applying", () => {
    renderProgress({ isApplying: true });
    expect(screen.getByText(/Applying repair/)).toBeVisible();
  });

  it("passes axe on a fully populated panel", async () => {
    const container = renderProgress({
      planStatus: "applying",
      isApplying: true,
      applyResult: applyResponse({ disposition: "unresolved" }),
      progress: progressView({
        quarantinedEvents: 2,
        notifications: [
          { sequence: 4, kind: "repair_action_ambiguous", at: "2026-01-01 00:02:00" },
        ],
      }),
    });
    await expectNoAccessibilityViolations(container);
  });
});
