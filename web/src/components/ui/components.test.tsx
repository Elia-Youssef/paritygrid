import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { cn } from "../../lib/cn";
import { Button } from "./button";
import { SkipLink } from "./skip-link";
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

  it("forwards refs to the underlying button", () => {
    let instance: HTMLButtonElement | null = null;
    render(
      <Button
        ref={(element) => {
          instance = element;
        }}
      >
        Ref target
      </Button>,
    );

    expect(instance).not.toBeNull();
    expect(instance).toBeInstanceOf(HTMLButtonElement);
  });

  it("keeps a visible token focus ring on every variant", () => {
    for (const variant of ["primary", "secondary", "ghost", "destructive"] as const) {
      const { container, unmount } = render(<Button variant={variant}>Variant</Button>);
      expect(container.firstElementChild?.className).toContain(
        "focus-visible:outline-[var(--focus-ring)]",
      );
      unmount();
    }
  });

  it("maps the destructive variant to the failure token", () => {
    const { container } = render(<Button variant="destructive">Delete</Button>);

    expect(container.firstElementChild?.className).toContain("bg-failure");
  });

  it("keeps compact buttons above the 24px touch-target minimum", () => {
    const { container } = render(<Button size="compact">Compact</Button>);

    // h-8 equals 2rem = 32px.
    expect(container.firstElementChild?.className).toContain("h-8");
  });

  it("provides a textual state in addition to color", () => {
    render(<StatusBadge state="verified">Verified</StatusBadge>);

    expect(screen.getByText("Verified")).toBeVisible();
  });

  it.each([
    "active",
    "verified",
    "warning",
    "failure",
    "stale",
    "paused",
    "cancelled",
    "neutral",
  ] as const)("renders the %s state from semantic tokens", (state) => {
    const { container } = render(<StatusBadge state={state}>{state}</StatusBadge>);

    expect(container.firstElementChild?.className).toContain("text-");
    expect(screen.getByText(state)).toBeVisible();
  });

  it("exposes the skip link as the content shortcut", () => {
    render(<SkipLink />);

    const link = screen.getByRole("link", { name: "Skip to content" });
    expect(link).toHaveAttribute("href", "#main-content");
  });

  it("resolves conflicting utility classes predictably", () => {
    expect(cn("px-2", false, "px-4")).toBe("px-4");
  });
});
