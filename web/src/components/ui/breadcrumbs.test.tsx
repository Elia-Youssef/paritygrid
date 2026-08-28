import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";

import { Breadcrumbs } from "./breadcrumbs";

const trail = [
  { href: "/app", label: "Operations" },
  { href: "/app/runs", label: "Runs" },
  { href: "/app/runs/run-1", label: "run-1", monospace: true },
  { href: "/app/runs/run-1/reconcile", label: "Reconcile" },
];

describe("breadcrumbs", () => {
  it("renders a labelled navigation with links for ancestors", () => {
    render(
      <MemoryRouter>
        <Breadcrumbs items={trail} />
      </MemoryRouter>,
    );

    const navigation = screen.getByRole("navigation", { name: "Breadcrumb" });
    expect(navigation).toBeVisible();
    expect(screen.getByRole("link", { name: "Operations" })).toHaveAttribute(
      "href",
      "/app",
    );
    expect(screen.getByRole("link", { name: "Runs" })).toHaveAttribute(
      "href",
      "/app/runs",
    );
  });

  it("marks the current location instead of linking it", () => {
    render(
      <MemoryRouter>
        <Breadcrumbs items={trail} />
      </MemoryRouter>,
    );

    const current = screen.getByText("Reconcile");
    expect(current).toHaveAttribute("aria-current", "page");
    expect(screen.queryByRole("link", { name: "Reconcile" })).toBeNull();
  });

  it("renders dynamic identifiers as monospace text", () => {
    const { container } = render(
      <MemoryRouter>
        <Breadcrumbs items={trail} />
      </MemoryRouter>,
    );

    const identifier = screen.getByText("run-1");
    expect(identifier.className).toContain("font-mono");
    expect(container).toBeDefined();
  });

  it("renders nothing without items", () => {
    const { container } = render(
      <MemoryRouter>
        <Breadcrumbs items={[]} />
      </MemoryRouter>,
    );

    expect(container).toBeEmptyDOMElement();
  });
});
