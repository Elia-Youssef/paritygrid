import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Routes, Route } from "react-router";
import { describe, expect, it } from "vitest";

import { expectNoAccessibilityViolations } from "../../test/axe";
import { CommandPalette, type CommandDestination } from "./command-palette";

const destinations: readonly CommandDestination[] = [
  { to: "/app", label: "Overview", description: "Operations overview" },
  { to: "/app/pipelines", label: "Pipelines", description: "Pipeline library" },
  { to: "/app/runs", label: "Runs", description: "Run history" },
  { to: "/app/system", label: "System", description: "System health" },
];

function PaletteHarness() {
  return (
    <MemoryRouter initialEntries={["/app"]}>
      <Routes>
        <Route path="/app" element={<div>overview-content</div>} />
        <Route path="/app/runs" element={<div>runs-content</div>} />
        <Route path="/app/pipelines" element={<div>pipelines-content</div>} />
      </Routes>
      <CommandPalette destinations={destinations} />
    </MemoryRouter>
  );
}

describe("command palette", () => {
  it("opens as a labelled modal dialog from its trigger", async () => {
    const user = userEvent.setup();
    render(<PaletteHarness />);

    const trigger = screen.getByRole("button", { name: /Commands/ });
    expect(trigger).toHaveAttribute("aria-haspopup", "dialog");
    expect(trigger).toHaveAttribute("aria-expanded", "false");

    await user.click(trigger);

    const dialog = screen.getByRole("dialog", { name: "Command palette" });
    expect(dialog).toBeVisible();
    expect(trigger).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByRole("combobox")).toHaveFocus();
    expect(screen.getByRole("combobox", { name: "Search screens" })).toBeVisible();
    await expectNoAccessibilityViolations(dialog);
  });

  it("exposes a listbox with every destination selected by keyboard", async () => {
    const user = userEvent.setup();
    render(<PaletteHarness />);
    await user.click(screen.getByRole("button", { name: /Commands/ }));

    const listbox = screen.getByRole("listbox", { name: "Destinations" });
    const options = within(listbox).getAllByRole("option");
    expect(options).toHaveLength(destinations.length);
    expect(options[0]).toHaveAttribute("aria-selected", "true");

    const input = screen.getByRole("combobox");
    expect(input).toHaveAttribute("aria-controls", listbox.id);
    expect(input).toHaveAttribute("aria-activedescendant", options[0]?.id);

    await user.keyboard("{ArrowDown}");
    expect(within(listbox).getAllByRole("option")[1]).toHaveAttribute(
      "aria-selected",
      "true",
    );

    await user.keyboard("{ArrowUp}{ArrowUp}");
    expect(
      within(listbox).getAllByRole("option")[destinations.length - 1],
    ).toHaveAttribute("aria-selected", "true");
  });

  it("filters destinations as the user types", async () => {
    const user = userEvent.setup();
    render(<PaletteHarness />);
    await user.click(screen.getByRole("button", { name: /Commands/ }));

    await user.type(screen.getByRole("combobox"), "pipe");

    const options = screen.getAllByRole("option");
    expect(options).toHaveLength(1);
    expect(options[0]).toHaveTextContent("Pipelines");
  });

  it("announces an empty result without options", async () => {
    const user = userEvent.setup();
    render(<PaletteHarness />);
    await user.click(screen.getByRole("button", { name: /Commands/ }));

    await user.type(screen.getByRole("combobox"), "zzz-no-match");

    expect(screen.queryByRole("option")).toBeNull();
    expect(screen.getByText("No matching screen")).toBeVisible();
  });

  it("navigates with Enter and closes the dialog", async () => {
    const user = userEvent.setup();
    render(<PaletteHarness />);
    await user.click(screen.getByRole("button", { name: /Commands/ }));

    await user.type(screen.getByRole("combobox"), "runs");
    await user.keyboard("{Enter}");

    expect(screen.getByText("runs-content")).toBeVisible();
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("restores focus to the trigger on Escape", async () => {
    const user = userEvent.setup();
    render(<PaletteHarness />);
    await user.click(screen.getByRole("button", { name: /Commands/ }));

    await user.keyboard("{Escape}");

    expect(screen.queryByRole("dialog")).toBeNull();
    expect(screen.getByRole("button", { name: /Commands/ })).toHaveFocus();
  });

  it("keeps focus on the combobox when Tab is pressed inside the dialog", async () => {
    const user = userEvent.setup();
    render(<PaletteHarness />);
    await user.click(screen.getByRole("button", { name: /Commands/ }));

    await user.tab();

    expect(screen.getByRole("combobox")).toHaveFocus();
    expect(screen.getByRole("dialog")).toBeVisible();
  });

  it("opens and closes with the Ctrl+K chord", async () => {
    const user = userEvent.setup();
    render(<PaletteHarness />);

    await user.keyboard("{Control>}k{/Control}");
    expect(screen.getByRole("dialog", { name: "Command palette" })).toBeVisible();

    await user.keyboard("{Control>}k{/Control}");
    expect(screen.queryByRole("dialog")).toBeNull();
  });
});
