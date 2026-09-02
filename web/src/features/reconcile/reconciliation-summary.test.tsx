import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import type { ReconciliationResponse } from "../../api/generated/schema";
import { expectNoAccessibilityViolations } from "../../test/axe";
import { ReconciliationSummary } from "./reconciliation-summary";

function summaryFixture(
  overrides: Partial<ReconciliationResponse> = {},
): ReconciliationResponse {
  return {
    schema_version: 1,
    run_id: "run_recon-001",
    run_version: 4,
    state: "succeeded",
    observed_at: "2026-01-02 03:04:05",
    reconciliation_fingerprint: "a".repeat(64),
    source_input_identity: "b".repeat(64),
    target_input_identity: "c".repeat(64),
    total_count: 6,
    counts: {
      match: 1,
      missing_from_target: 2,
      missing_from_source: 0,
      field_mismatch: 3,
      duplicate_source: 0,
      duplicate_target: 0,
      duplicate_both: 0,
    },
    analytical_query_version: 2,
    reconciliation_observed_at: "2026-01-02 03:04:06",
    ...overrides,
  };
}

describe("ReconciliationSummary", () => {
  it("renders every identity field, both observation times, and all seven counts", () => {
    render(<ReconciliationSummary summary={summaryFixture()} empty={true} />);

    // Identity and coherence fields, each under its visible label.
    expect(screen.getByText("Run")).toBeInTheDocument();
    expect(screen.getByText("run_recon-001")).toBeInTheDocument();
    expect(screen.getByText("4")).toBeInTheDocument();
    expect(screen.getByText("succeeded")).toBeInTheDocument();
    expect(screen.getByText("a".repeat(64))).toBeInTheDocument();
    expect(screen.getByText("b".repeat(64))).toBeInTheDocument();
    expect(screen.getByText("c".repeat(64))).toBeInTheDocument();

    // All seven classification counts, including the zeros.
    expect(screen.getByTestId("count-match")).toHaveTextContent("1");
    expect(screen.getByTestId("count-missing_from_target")).toHaveTextContent("2");
    expect(screen.getByTestId("count-missing_from_source")).toHaveTextContent("0");
    expect(screen.getByTestId("count-field_mismatch")).toHaveTextContent("3");
    expect(screen.getByTestId("count-duplicate_source")).toHaveTextContent("0");
    expect(screen.getByTestId("count-duplicate_target")).toHaveTextContent("0");
    expect(screen.getByTestId("count-duplicate_both")).toHaveTextContent("0");
    expect(screen.getByText("Missing from target")).toBeInTheDocument();
    expect(screen.getByText("Duplicate both")).toBeInTheDocument();
    expect(screen.getByText("Total records")).toBeInTheDocument();
    expect(screen.getByText("Conflict records")).toBeInTheDocument();
    expect(screen.getByText("6")).toBeInTheDocument();
    expect(screen.getByText("5")).toBeInTheDocument();

    // Distinct observation times with distinct labels.
    expect(screen.getByText("Authoritative observation")).toBeInTheDocument();
    expect(screen.getByText("2026-01-02 03:04:06")).toBeInTheDocument();
    expect(screen.getByText("Response observed at")).toBeInTheDocument();
    expect(screen.getByText("2026-01-02 03:04:05")).toBeInTheDocument();

    // Inline empty note for a conflict-free reconciliation.
    expect(screen.getByTestId("reconciliation-empty")).toBeVisible();
  });

  it("renders the truthful repair-creation notice in the normal state", () => {
    render(<ReconciliationSummary summary={summaryFixture()} />);
    const notice = screen.getByTestId("repair-creation-notice");
    expect(notice).toBeVisible();
    expect(notice).toHaveTextContent("Repair-plan creation is not available");
    expect(notice).toHaveTextContent("exact complete source and target observations");
    expect(notice).toHaveTextContent(
      "Review, approval, and application of existing repair plans remain supported",
    );
  });

  it("hides the repair notice while repair actions are disabled", () => {
    render(
      <ReconciliationSummary summary={summaryFixture()} repairActionsDisabled={true} />,
    );
    expect(screen.queryByTestId("repair-creation-notice")).not.toBeInTheDocument();
  });

  it("shows the stale banner and refresh affordance only with a quarantined snapshot", async () => {
    const quarantined = summaryFixture({
      reconciliation_fingerprint: "f".repeat(64),
      observed_at: "2026-01-02 09:00:00",
    });
    const onRefresh = vi.fn();
    const user = userEvent.setup();

    const withoutQuarantine = render(
      <ReconciliationSummary
        summary={summaryFixture()}
        stale={true}
        onRefresh={onRefresh}
      />,
    );
    expect(screen.queryByTestId("reconciliation-stale-banner")).not.toBeInTheDocument();
    expect(screen.queryByTestId("reconciliation-refresh")).not.toBeInTheDocument();
    withoutQuarantine.unmount();

    render(
      <ReconciliationSummary
        summary={summaryFixture()}
        stale={true}
        quarantinedSummary={quarantined}
        onRefresh={onRefresh}
      />,
    );
    const banner = screen.getByTestId("reconciliation-stale-banner");
    expect(banner).toBeVisible();
    expect(banner).toHaveTextContent("Refresh required");
    expect(banner).toHaveTextContent("f".repeat(64));
    const refresh = screen.getByTestId("reconciliation-refresh");
    await user.click(refresh);
    expect(onRefresh).toHaveBeenCalledTimes(1);
  });

  it("omits the stale banner when the stale flag is not set", () => {
    render(
      <ReconciliationSummary
        summary={summaryFixture()}
        quarantinedSummary={summaryFixture({
          reconciliation_fingerprint: "f".repeat(64),
        })}
      />,
    );
    expect(screen.queryByTestId("reconciliation-stale-banner")).not.toBeInTheDocument();
  });

  it("renders the children slot below the summary", () => {
    render(
      <ReconciliationSummary summary={summaryFixture()}>
        <section data-testid="conflict-slot">Conflict rows</section>
      </ReconciliationSummary>,
    );
    expect(screen.getByTestId("conflict-slot")).toBeInTheDocument();
  });

  it("has no accessibility violations", async () => {
    const { container } = render(
      <ReconciliationSummary
        summary={summaryFixture()}
        stale={true}
        quarantinedSummary={summaryFixture({
          reconciliation_fingerprint: "f".repeat(64),
        })}
        onRefresh={() => undefined}
      />,
    );
    await expectNoAccessibilityViolations(container);
  });
});
