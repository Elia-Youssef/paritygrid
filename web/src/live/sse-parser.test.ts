import { describe, expect, it } from "vitest";

import { createSseParser } from "./sse-parser";

describe("sse parser", () => {
  it("dispatches complete events and keeps id, event, and data", () => {
    const parser = createSseParser();
    const messages = parser.push('id: 7\nevent: run.progressed\ndata: {"a":1}\n\n');
    expect(messages).toEqual([{ id: "7", event: "run.progressed", data: '{"a":1}' }]);
  });

  it("ignores heartbeat comments as non-data", () => {
    const parser = createSseParser();
    expect(parser.push(": paritygrid-heartbeat\n\n")).toEqual([]);
    expect(parser.push(":no-space-comment\n\n")).toEqual([]);
  });

  it("ignores the retry hint", () => {
    const parser = createSseParser();
    expect(parser.push("retry: 3000\n\n")).toEqual([]);
  });

  it("buffers partial frames across chunks", () => {
    const parser = createSseParser();
    expect(parser.push("id: 3\nevent: run.up")).toEqual([]);
    expect(parser.push('dated\ndata: {"seq":')).toEqual([]);
    expect(parser.push("3}\n")).toEqual([]);
    expect(parser.push("\n")).toEqual([
      { id: "3", event: "run.updated", data: '{"seq":3}' },
    ]);
  });

  it("supports CRLF terminators and multi-line data", () => {
    const parser = createSseParser();
    const messages = parser.push("data: line1\r\ndata: line2\r\n\r\n");
    expect(messages).toEqual([{ id: null, event: null, data: "line1\nline2" }]);
  });

  it("holds a trailing CR until the LF arrives in the next chunk", () => {
    const parser = createSseParser();
    expect(parser.push("data: x\r")).toEqual([]);
    // The split terminator completes the first line; the event still
    // dispatches only at the blank line, with both data lines joined.
    expect(parser.push("\ndata: y\n\n")).toEqual([
      { id: null, event: null, data: "x\ny" },
    ]);
  });

  it("splits a CR that spans chunks", () => {
    const parser = createSseParser();
    expect(parser.push("data: a\r")).toEqual([]);
    expect(parser.push("\ndata: b\r\n\r\n")).toEqual([
      { id: null, event: null, data: "a\nb" },
    ]);
  });

  it("ignores unknown fields per the SSE specification", () => {
    const parser = createSseParser();
    const messages = parser.push("x-custom: zap\ndata: v\n\n");
    expect(messages).toEqual([{ id: null, event: null, data: "v" }]);
  });

  it("resets event identity after each dispatch", () => {
    const parser = createSseParser();
    parser.push("id: 1\nevent: a\ndata: 1\n\n");
    const second = parser.push("data: 2\n\n");
    expect(second).toEqual([{ id: null, event: null, data: "2" }]);
  });

  it("flushes a final unterminated data line", () => {
    const parser = createSseParser();
    expect(parser.push("data: tail")).toEqual([]);
    expect(parser.flush()).toEqual([{ id: null, event: null, data: "tail" }]);
    expect(parser.flush()).toEqual([]);
  });

  it("does not dispatch a blank line after comments only", () => {
    const parser = createSseParser();
    expect(parser.push(": keep-alive\n\n")).toEqual([]);
  });
});
