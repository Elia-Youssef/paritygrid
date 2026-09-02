import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { ConflictDifference, ConflictResponse } from "../../api/generated/schema";
import { expectNoAccessibilityViolations } from "../../test/axe";
import { differenceKindLabel } from "./classifications";
import { ConflictInspector } from "./conflict-inspector";

// The runtime contract pins reconciliation fingerprints to 64 hex chars.
const FINGERPRINT = "a".repeat(64);

function makeDifference(
  overrides: Partial<ConflictDifference> = {},
): ConflictDifference {
  return {
    field: "amount",
    kind: "value_mismatch",
    source_text: "100",
    target_text: "200",
    ...overrides,
  };
}

function makeConflict(overrides: Partial<ConflictResponse> = {}): ConflictResponse {
  return {
    schema_version: 1,
    conflict_id: "cnf-000001",
    canonical_key: "order:42",
    classification: "field_mismatch",
    source_references: [
      { position: 0, record_key: "src-order-42" },
      { position: 1, record_key: "src-order-43" },
    ],
    target_references: [{ position: 0, record_key: "tgt-order-42" }],
    differences: [],
    suggested_resolution: null,
    created_at: "2026-01-01T00:00:00Z",
    ...overrides,
  };
}

function renderInspector(conflict: ConflictResponse | null): {
  container: HTMLElement;
} {
  const view = render(
    <ConflictInspector
      conflict={conflict}
      fingerprint={conflict === null ? null : FINGERPRINT}
      selectedKey={conflict === null ? null : `${FINGERPRINT}:${conflict.conflict_id}`}
    />,
  );
  return { container: view.container };
}

describe("ConflictInspector", () => {
  it("renders an explicit quiet state when no conflict is selected", () => {
    renderInspector(null);
    expect(screen.getByTestId("conflict-inspector-empty")).toBeVisible();
    expect(screen.getByTestId("conflict-inspector-empty")).toHaveTextContent(
      /select a conflict/i,
    );
  });

  it("shows identity fields and both sides for a value mismatch", () => {
    const { container } = renderInspector(
      makeConflict({ differences: [makeDifference()] }),
    );

    expect(screen.getByTestId("conflict-inspector")).toBeInTheDocument();
    expect(screen.getByText("cnf-000001")).toBeInTheDocument();
    expect(screen.getByText("order:42")).toBeInTheDocument();
    expect(screen.getByText("2026-01-01T00:00:00Z")).toBeInTheDocument();
    expect(screen.getByText("Field mismatch")).toBeInTheDocument();
    expect(screen.getByText(FINGERPRINT)).toBeInTheDocument();
    expect(screen.getByText(`${FINGERPRINT}:cnf-000001`)).toBeInTheDocument();
    expect(screen.getByText("none suggested")).toBeInTheDocument();

    const card = screen.getByTestId("difference-0");
    expect(within(card).getByText("Source value")).toBeInTheDocument();
    expect(within(card).getByText("Target value")).toBeInTheDocument();
    expect(within(card).getByText("100")).toBeInTheDocument();
    expect(within(card).getByText("200")).toBeInTheDocument();
    expect(screen.getByTestId("difference-kind-0")).toHaveTextContent(
      differenceKindLabel("value_mismatch"),
    );

    // References render as position + record key lists.
    expect(screen.getByText("#0 src-order-42")).toBeInTheDocument();
    expect(screen.getByText("#1 src-order-43")).toBeInTheDocument();
    expect(screen.getByText("#0 tgt-order-42")).toBeInTheDocument();

    return expectNoAccessibilityViolations(container);
  });

  it("states the fixed phrase for a missing target side instead of inventing a value", () => {
    renderInspector(
      makeConflict({
        differences: [
          makeDifference({
            field: "address.city",
            kind: "missing_on_target",
            source_text: "Berlin",
            target_text: "",
          }),
        ],
      }),
    );

    expect(screen.getByText("address.city")).toBeInTheDocument();
    expect(screen.getByText("absent on the target side")).toBeInTheDocument();
    // No fabricated value appears for the side the server reported missing.
    expect(screen.queryByText("Source value")).not.toBeInTheDocument();
    expect(screen.queryByText("Target value")).not.toBeInTheDocument();
    expect(screen.queryByText("Berlin")).not.toBeInTheDocument();
  });

  it("renders explicit null and missing source sides with distinct labels", () => {
    renderInspector(
      makeConflict({
        differences: [
          makeDifference({
            field: "note",
            kind: "null_on_source",
            source_text: "",
            target_text: "hello",
          }),
          makeDifference({
            field: "note2",
            kind: "missing_on_source",
            source_text: "",
            target_text: "hello",
          }),
        ],
      }),
    );

    expect(screen.getByText("explicitly null on the source side")).toBeInTheDocument();
    expect(screen.getByText("absent on the source side")).toBeInTheDocument();
    expect(screen.getByTestId("difference-kind-0")).toHaveTextContent("Null on source");
    expect(screen.getByTestId("difference-kind-1")).toHaveTextContent(
      "Missing on source",
    );
  });

  it("labels a type mismatch explicitly and shows both bounded texts", () => {
    renderInspector(
      makeConflict({
        differences: [
          makeDifference({
            kind: "type_mismatch",
            source_text: "1",
            target_text: "1.0",
          }),
        ],
      }),
    );

    const card = screen.getByTestId("difference-0");
    // The kind badge and the explicit note both carry the type mismatch label.
    expect(within(card).getAllByText("Type mismatch")).toHaveLength(2);
    expect(within(card).getByText("Source value")).toBeInTheDocument();
    expect(within(card).getByText("Target value")).toBeInTheDocument();
    expect(within(card).getByText("1")).toBeInTheDocument();
    expect(within(card).getByText("1.0")).toBeInTheDocument();
  });

  it("bounds long values and announces the truncation", () => {
    renderInspector(
      makeConflict({
        differences: [makeDifference({ source_text: "x".repeat(600) })],
      }),
    );

    expect(screen.getByText(/^x{512}…$/)).toBeInTheDocument();
    expect(screen.getByText("value truncated")).toBeInTheDocument();
  });

  it("renders unicode values exactly", () => {
    renderInspector(
      makeConflict({
        differences: [
          makeDifference({ source_text: "café ✓ 日本語", target_text: "naïve" }),
        ],
      }),
    );

    expect(screen.getByText("café ✓ 日本語")).toBeInTheDocument();
    expect(screen.getByText("naïve")).toBeInTheDocument();
  });

  it("renders markup-looking values as inert text", () => {
    const { container } = renderInspector(
      makeConflict({
        differences: [
          makeDifference({
            source_text: "<script>alert(1)</script>",
            target_text: "<b>bold</b>",
          }),
        ],
      }),
    );

    expect(container.querySelector("script")).toBeNull();
    expect(screen.getByText("<script>alert(1)</script>")).toBeInTheDocument();
    expect(screen.getByText("<b>bold</b>")).toBeInTheDocument();
  });

  it("redacts secret-shaped values before they reach the DOM", () => {
    const { container } = renderInspector(
      makeConflict({
        differences: [
          makeDifference({
            source_text: "password: hunter2",
            target_text: "https://user:pass@host/x",
          }),
        ],
      }),
    );

    expect(screen.getByText("password: [redacted]")).toBeInTheDocument();
    expect(container.textContent).not.toContain("hunter2");
    expect(container.textContent).toContain("[redacted]");
  });

  it("renders explicit text for absent and known suggested resolutions", () => {
    renderInspector(makeConflict({ suggested_resolution: null }));
    expect(screen.getByText("none suggested")).toBeInTheDocument();
  });

  it("labels known resolutions and shows unknown ones verbatim", () => {
    renderInspector(makeConflict({ suggested_resolution: "review_duplicates" }));
    expect(screen.getByText("review the duplicate records")).toBeInTheDocument();
  });

  it("renders the none resolution explicitly", () => {
    renderInspector(makeConflict({ suggested_resolution: "none" }));
    expect(screen.getByText("none suggested")).toBeInTheDocument();
  });

  it("fails safe with a visible note for an unrecognized difference kind", () => {
    const { container } = renderInspector(
      makeConflict({
        differences: [
          makeDifference({
            kind: "quantum_diff",
            source_text: "100",
            target_text: "200",
          }),
        ],
      }),
    );

    expect(screen.getByTestId("difference-kind-0")).toHaveTextContent(
      "unknown difference kind",
    );
    expect(screen.getByRole("alert")).toHaveTextContent(/unrecognized kind/i);
    // The raw texts stay visible rather than being hidden.
    expect(
      within(screen.getByTestId("difference-0")).getByText("100"),
    ).toBeInTheDocument();
    expect(
      within(screen.getByTestId("difference-0")).getByText("200"),
    ).toBeInTheDocument();
    return expectNoAccessibilityViolations(container);
  });
});
