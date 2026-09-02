import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import {
  boundDisplayValue,
  describeMissingOrNull,
  MAX_DISPLAY_LENGTH,
  redactSecretLike,
} from "./value-bounds";
import { BoundedValue } from "./value-display";

describe("redactSecretLike", () => {
  it("redacts assignment-style secrets, bearer tokens, and URL credentials", () => {
    expect(redactSecretLike("password: hunter2")).toBe("password: [redacted]");
    expect(redactSecretLike("Authorization: Bearer abc123")).toBe(
      "Authorization: [redacted]",
    );
    expect(redactSecretLike("Bearer eyJhbGciOiJIUzI1NiJ9.payload.signature")).toBe(
      "Bearer [redacted]",
    );
    expect(redactSecretLike("https://user:pass@host/x")).toBe(
      "https://[redacted]@host/x",
    );
    expect(redactSecretLike("api_key=xyz")).toBe("api_key=[redacted]");
  });

  it("preserves benign text exactly", () => {
    const benign = "amount=100; currency=EUR; note: totals reconciled";
    expect(redactSecretLike(benign)).toBe(benign);
    const words = "monkey business near a keyboard factory";
    expect(redactSecretLike(words)).toBe(words);
  });

  it("is deterministic", () => {
    const value = "token: abc123 and https://user:pass@host/x";
    expect(redactSecretLike(value)).toBe(redactSecretLike(value));
  });
});

describe("boundDisplayValue", () => {
  it("strips control characters", () => {
    expect(boundDisplayValue("a\u0000b\u0007c\u001fd")).toBe("a b c d");
    expect(boundDisplayValue("line\u007fbreak")).toBe("line break");
  });

  it("truncates values longer than the bound with an ellipsis", () => {
    const long = "x".repeat(MAX_DISPLAY_LENGTH + 50);
    const bounded = boundDisplayValue(long);
    expect(bounded).toHaveLength(MAX_DISPLAY_LENGTH + 1);
    expect(bounded.endsWith("…")).toBe(true);
  });

  it("keeps values at or below the bound unchanged", () => {
    const exact = "y".repeat(MAX_DISPLAY_LENGTH);
    expect(boundDisplayValue(exact)).toBe(exact);
    expect(boundDisplayValue("short")).toBe("short");
  });

  it("honors a custom bound and is deterministic", () => {
    expect(boundDisplayValue("abcdefgh", 4)).toBe("abcd…");
    expect(boundDisplayValue("abcdefgh", 4)).toBe(boundDisplayValue("abcdefgh", 4));
  });
});

describe("describeMissingOrNull", () => {
  it("returns fixed phrases for the missing and explicit-null kinds", () => {
    expect(describeMissingOrNull("missing_on_source")).toBe(
      "absent on the source side",
    );
    expect(describeMissingOrNull("missing_on_target")).toBe(
      "absent on the target side",
    );
    expect(describeMissingOrNull("null_on_source")).toBe(
      "explicitly null on the source side",
    );
    expect(describeMissingOrNull("null_on_target")).toBe(
      "explicitly null on the target side",
    );
  });

  it("returns null when both sides carry comparable text", () => {
    expect(describeMissingOrNull("value_mismatch")).toBeNull();
    expect(describeMissingOrNull("type_mismatch")).toBeNull();
  });
});

describe("BoundedValue", () => {
  it("renders the redacted value as text with no truncation note when it fits", () => {
    render(<BoundedValue text="password: hunter2" />);
    expect(screen.getByText("password: [redacted]")).toBeInTheDocument();
    expect(screen.queryByText("value truncated")).not.toBeInTheDocument();
  });

  it("announces truncation with a visible note", () => {
    render(<BoundedValue text={"x".repeat(MAX_DISPLAY_LENGTH + 10)} />);
    expect(screen.getByText("value truncated")).toBeInTheDocument();
    expect(screen.getByText(/^x+\u2026$/)).toBeInTheDocument();
  });

  it("supports a custom bound and multiline mono presentation", () => {
    render(<BoundedValue text="abcdefgh" maxLength={4} multiline />);
    expect(screen.getByText("abcd…")).toBeInTheDocument();
    expect(screen.getByText("value truncated")).toBeInTheDocument();
    expect(screen.getByText("abcd…").tagName).toBe("PRE");
  });
});
