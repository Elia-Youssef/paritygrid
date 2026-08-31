import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactNode } from "react";
import { MemoryRouter, Route, Routes } from "react-router";
import { describe, expect, it, vi } from "vitest";

import { RootErrorBoundary } from "./root-error-boundary";
import { RouteErrorBoundary } from "./route-error-boundary";

let shouldThrow = false;

function Bomb(): ReactNode {
  if (shouldThrow) {
    throw new Error("SECRET-INTERNAL-DATABASE-URL postgres://ops:hunter2@host/db");
  }
  return <p>screen content</p>;
}

function ScreenWithBomb(): ReactNode {
  return (
    <div>
      <RouteErrorBoundary>
        <Bomb />
      </RouteErrorBoundary>
    </div>
  );
}

function renderScreen() {
  return render(
    <MemoryRouter initialEntries={["/app"]}>
      <Routes>
        <Route path="/app" element={<ScreenWithBomb />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe("route error boundary", () => {
  it("renders children untouched while nothing fails", () => {
    shouldThrow = false;
    renderScreen();

    expect(screen.getByText("screen content")).toBeVisible();
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("catches render failures with safe diagnostics, focus, and recovery actions", async () => {
    shouldThrow = true;
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const { container } = renderScreen();

    const alert = screen.getByRole("alert");
    expect(alert).toHaveTextContent("This screen could not render");

    // Protected detail never reaches the DOM.
    expect(container.textContent).not.toContain("SECRET-INTERNAL-DATABASE-URL");
    expect(container.textContent).not.toContain("hunter2");

    // Diagnostics identify the safe boundary without carrying raw failure data.
    expect(consoleError).toHaveBeenCalledWith("ParityGrid render failure", {
      boundary: "route",
    });

    await waitFor(() => {
      expect(document.activeElement).toBe(alert);
    });

    // The shell stays recoverable.
    expect(screen.getByRole("button", { name: "Try again" })).toBeEnabled();
    expect(screen.getByRole("link", { name: "Operations overview" })).toHaveAttribute(
      "href",
      "/app",
    );
  });

  it("recovers when retry renders valid content", async () => {
    shouldThrow = true;
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const user = userEvent.setup();
    const { container } = renderScreen();

    shouldThrow = false;
    await user.click(screen.getByRole("button", { name: "Try again" }));

    expect(screen.getByText("screen content")).toBeVisible();
    expect(container.textContent).not.toContain("This screen could not render");
  });
});

describe("root error boundary", () => {
  function RootBomb(): ReactNode {
    const error = new Error("router exploded");
    error.name = "SECRET-INTERNAL-DATABASE-URL";
    throw error;
  }

  it("keeps a recoverable, focused fallback without router dependencies", async () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);

    render(
      <RootErrorBoundary>
        <RootBomb />
      </RootErrorBoundary>,
    );

    const alert = screen.getByRole("alert");
    expect(alert).toHaveTextContent("ParityGrid could not start");
    expect(screen.getByRole("button", { name: "Reload" })).toBeEnabled();
    expect(screen.getByRole("link", { name: "Public overview" })).toHaveAttribute(
      "href",
      "/",
    );
    // The raw error message and controllable error name stay out of the fallback.
    expect(alert.textContent).not.toContain("router exploded");
    expect(alert.textContent).not.toContain("SECRET-INTERNAL-DATABASE-URL");
    expect(consoleError).toHaveBeenCalledWith("ParityGrid render failure", {
      boundary: "root",
    });
    await waitFor(() => {
      expect(document.activeElement).toBe(alert);
    });
  });
});
