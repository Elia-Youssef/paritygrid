import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { cn } from "../../lib/cn";
import { Button } from "./button";
import { StatusBadge } from "./status-badge";

describe("owned interface primitives", () => {
  it("renders an accessible native button", () => {
    render(<Button type="button">Start run</Button>);

    expect(screen.getByRole("button", { name: "Start run" })).toBeEnabled();
  });

  it("composes an anchor without adding a nested button", () => {
    render(
      <Button asChild variant="secondary">
        <a href="#details">View details</a>
      </Button>,
    );

    expect(screen.getByRole("link", { name: "View details" })).toHaveAttribute(
      "href",
      "#details",
    );
  });

  it("provides a textual state in addition to color", () => {
    render(<StatusBadge state="verified">Verified</StatusBadge>);

    expect(screen.getByText("Verified")).toBeVisible();
  });

  it("resolves conflicting utility classes predictably", () => {
    expect(cn("px-2", false, "px-4")).toBe("px-4");
  });
});
