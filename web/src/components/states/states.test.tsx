import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { Button } from "../ui/button";
import { DisconnectedNotice } from "./disconnected-notice";
import { EmptyState } from "./empty-state";
import { ErrorState } from "./error-state";
import { Loading } from "./loading";
import { StaleNotice } from "./stale-notice";
import { UnavailableNotice } from "./unavailable-notice";
import { parseProblemDetails } from "../../lib/problem-details";

describe("loading state", () => {
  it("announces the wait as a live region with visible text", () => {
    render(<Loading label="Loading runs" />);

    const status = screen.getByRole("status");
    expect(status).toBeVisible();
    expect(status).toHaveTextContent("Loading runs…");
    expect(screen.queryByText("", { selector: ".animate-spin" })).toBeDefined();
  });

  it("uses a default label", () => {
    render(<Loading />);
    expect(screen.getByRole("status")).toHaveTextContent("Loading…");
  });
});

describe("empty state", () => {
  it("explains that content is correctly absent and offers an action", () => {
    render(
      <EmptyState
        title="No pipelines yet"
        description="Publish a pipeline to begin."
        action={<Button type="button">Open library</Button>}
      />,
    );

    expect(screen.getByText("No pipelines yet")).toBeVisible();
    expect(screen.getByText("Publish a pipeline to begin.")).toBeVisible();
    expect(screen.getByRole("button", { name: "Open library" })).toBeVisible();
  });
});

describe("error state", () => {
  it("announces failure through the alert role with a retry action", () => {
    render(
      <ErrorState
        description="The run could not be loaded."
        action={<Button type="button">Retry</Button>}
      />,
    );

    expect(screen.getByRole("alert")).toBeVisible();
    expect(screen.getByRole("button", { name: "Retry" })).toBeVisible();
  });

  it("renders structured Problem Details as inert bounded text", () => {
    const problem = parseProblemDetails({
      title: "Repair plan is stale",
      status: 409,
      detail: "The reconciliation fingerprint changed.",
      type: "https://paritygrid.local/problems/stale-plan",
    });
    const { container } = render(<ErrorState problem={problem} />);

    expect(screen.getByText("Repair plan is stale")).toBeVisible();
    expect(screen.getByText("409")).toBeVisible();
    expect(screen.getByText("The reconciliation fingerprint changed.")).toBeVisible();
    expect(container.querySelector("script")).toBeNull();
  });

  it("never turns hostile payload strings into markup", () => {
    const problem = parseProblemDetails({
      title: '<img src=x onerror="alert(1)">',
      detail: '<script>document.secret="stolen"</script>',
      evilExtension: "<b>bold</b>",
    });
    const { container } = render(<ErrorState problem={problem} />);

    expect(container.querySelector("script")).toBeNull();
    expect(container.querySelector("img")).toBeNull();
    expect(container.querySelector("b")).toBeNull();
    expect(screen.getByText('<img src=x onerror="alert(1)">')).toBeVisible();
    expect(
      screen.getByText('<script>document.secret="[redacted]"</script>'),
    ).toBeVisible();
  });
});

describe("stale state", () => {
  it("describes the stale condition as text in a live region", () => {
    render(<StaleNotice detail="Last update 12s ago." />);

    expect(screen.getByRole("status")).toHaveTextContent("Data may be out of date");
    expect(screen.getByText("Last update 12s ago.")).toBeVisible();
  });
});

describe("disconnected state", () => {
  it("explains the live channel loss without blocking durable state", () => {
    render(<DisconnectedNotice />);

    const status = screen.getByRole("status");
    expect(status).toHaveTextContent("Live connection lost");
    expect(status).toHaveTextContent("Durable state remains available");
  });
});

describe("unavailable state", () => {
  it("presents the capability as neutral unavailable, not failed", () => {
    render(<UnavailableNotice detail="Runner is disabled for this workspace." />);

    expect(screen.getByText("Unavailable")).toBeVisible();
    expect(screen.getByText("Runner is disabled for this workspace.")).toBeVisible();
    expect(screen.queryByRole("alert")).toBeNull();
  });
});
