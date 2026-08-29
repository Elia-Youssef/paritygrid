import { cleanup } from "@testing-library/react";
import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { App } from "./App";
import { appQueryClient } from "../api/query-client";
import { expectNoAccessibilityViolations } from "../test/axe";

afterEach(() => {
  cleanup();
  appQueryClient.clear();
});

describe("App", () => {
  it("renders the public overview with a path into the console", async () => {
    render(<App />);

    expect(
      screen.getByRole("heading", {
        level: 1,
        name: "Reconciliation you can prove.",
      }),
    ).toBeVisible();
    expect(
      screen.getByRole("link", { name: "Open operations console" }),
    ).toHaveAttribute("href", "/app");
    expect(screen.getByRole("link", { name: "ParityGrid home" })).toBeVisible();

    await expectNoAccessibilityViolations(document.body);
  });
});
