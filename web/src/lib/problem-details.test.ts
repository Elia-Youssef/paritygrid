import { describe, expect, it } from "vitest";

import {
  GENERIC_ERROR_TITLE,
  MAX_EXTENSION_FIELDS,
  MAX_FIELD_LENGTH,
  boundText,
  parseProblemDetails,
} from "./problem-details";

describe("boundText", () => {
  it("replaces control characters with spaces", () => {
    expect(boundText("line\u0000broken\u001ftext\u007fend")).toBe(
      "line broken text end",
    );
  });

  it("caps length and marks the truncation", () => {
    const long = "a".repeat(MAX_FIELD_LENGTH + 100);
    const bounded = boundText(long);
    expect(bounded).toHaveLength(MAX_FIELD_LENGTH + 1);
    expect(bounded.endsWith("…")).toBe(true);
  });

  it("keeps short values untouched", () => {
    expect(boundText("fine")).toBe("fine");
  });
});

describe("parseProblemDetails", () => {
  it("maps the standard RFC 9457 members", () => {
    const view = parseProblemDetails({
      type: "https://paritygrid.local/problems/stale-plan",
      title: "Repair plan is stale",
      status: 409,
      detail: "The reconciliation fingerprint changed.",
      instance: "/api/repairs/rp-1",
    });

    expect(view).toEqual({
      title: "Repair plan is stale",
      status: 409,
      type: "https://paritygrid.local/problems/stale-plan",
      detail: "The reconciliation fingerprint changed.",
      instance: "/api/repairs/rp-1",
      extensions: [],
    });
  });

  it("uses the generic title for anything that is not a record", () => {
    for (const input of [null, undefined, 42, "Conflict", [1, 2, 3]]) {
      expect(parseProblemDetails(input)).toEqual({
        title: GENERIC_ERROR_TITLE,
        extensions: [],
      });
    }
  });

  it("falls back to the generic title when the title is missing or empty", () => {
    expect(parseProblemDetails({ status: 500 }).title).toBe(GENERIC_ERROR_TITLE);
    expect(parseProblemDetails({ title: "   " }).title).toBe(GENERIC_ERROR_TITLE);
  });

  it("keeps only plausible HTTP status codes", () => {
    expect(parseProblemDetails({ status: 409 }).status).toBe(409);
    expect(parseProblemDetails({ status: "409" }).status).toBeUndefined();
    expect(parseProblemDetails({ status: 99 }).status).toBeUndefined();
    expect(parseProblemDetails({ status: 600 }).status).toBeUndefined();
    expect(parseProblemDetails({ status: 409.5 }).status).toBeUndefined();
  });

  it("bounds oversized hostile members", () => {
    const view = parseProblemDetails({
      title: "<script>".padEnd(MAX_FIELD_LENGTH + 50, "x"),
      detail: "d".repeat(MAX_FIELD_LENGTH * 4),
    });
    expect(view.title).toHaveLength(MAX_FIELD_LENGTH + 1);
    expect(view.detail).toHaveLength(MAX_FIELD_LENGTH + 1);
  });

  it("redacts secret-shaped extension names", () => {
    const view = parseProblemDetails({
      title: "Upstream failure",
      authorization: "Bearer top-secret",
      "X-Api-Key": "sk-should-not-leak",
      cookie: "session=steal",
    });
    const values = view.extensions.map((extension) => extension.value);
    expect(values).toEqual(["[redacted]", "[redacted]", "[redacted]"]);
  });

  it("redacts protected values embedded in standard members and benign extensions", () => {
    const secret = "top-secret-value";
    const view = parseProblemDetails({
      title: `Request failed: token=${secret}`,
      detail: `Authorization: Bearer ${secret}`,
      instance: `/api/v1/runs?token=${secret}`,
      upstreamMessage: `postgres://ops:${secret}@db.internal/paritygrid`,
    });

    expect(JSON.stringify(view)).not.toContain(secret);
    expect(view.title).toContain("[redacted]");
    expect(view.detail).toContain("[redacted]");
    expect(view.instance).toContain("[redacted]");
    expect(view.extensions[0]?.value).toContain("[redacted]");
  });

  it("keeps primitive extensions as bounded text", () => {
    const view = parseProblemDetails({
      title: "Conflict",
      retryAfterSeconds: 30,
      durable: true,
    });
    expect(view.extensions).toEqual([
      { name: "retryAfterSeconds", value: "30" },
      { name: "durable", value: "true" },
    ]);
  });

  it("omits structured extension values instead of serializing them", () => {
    const view = parseProblemDetails({
      title: "Conflict",
      nested: { depth: true },
    });
    expect(view.extensions).toEqual([
      { name: "nested", value: "[structured value omitted]" },
    ]);
  });

  it("caps the number of extension fields", () => {
    const hostile: Record<string, unknown> = { title: "Many fields" };
    for (let index = 0; index < MAX_EXTENSION_FIELDS + 10; index += 1) {
      hostile[`field${index}`] = index;
    }
    expect(parseProblemDetails(hostile).extensions).toHaveLength(MAX_EXTENSION_FIELDS);
  });

  it("neutralizes a hostile __proto__ member from parsed JSON", () => {
    const parsed = JSON.parse(
      '{"__proto__": {"injected": true}, "title": "Parsed"}',
    ) as Record<string, unknown>;
    const view = parseProblemDetails(parsed);
    expect(view.title).toBe("Parsed");
    expect(view.extensions).toEqual([
      { name: "__proto__", value: "[structured value omitted]" },
    ]);
    expect(({} as Record<string, unknown>).injected).toBeUndefined();
  });
});
