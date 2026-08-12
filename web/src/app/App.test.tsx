import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("App", () => {
  it("renders the operational shell and confirms an available API", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValue(new Response('{"status":"ok"}', { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    expect(
      screen.getByRole("heading", { level: 1, name: "Reconciliation you can prove." }),
    ).toBeVisible();
    expect(screen.getByRole("navigation", { name: "Primary" })).toBeVisible();
    expect(await screen.findByText("API online")).toBeVisible();
    expect(fetchMock).toHaveBeenCalledWith(
      "/healthz",
      expect.objectContaining({ cache: "no-store" }),
    );
  });

  it("surfaces an unsuccessful API health response", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 503 })),
    );

    render(<App />);

    expect(await screen.findByText("API unavailable")).toBeVisible();
  });
});
