/**
 * Minimal text/event-stream parser for the fetch-based durable transport.
 *
 * A hand-rolled transport is required because the browser EventSource
 * cannot surface the HTTP status or Problem Details body of a rejected
 * resume (409 stream_sequence_ahead) and owns a hidden reconnect loop that
 * conflicts with the bounded, deterministic reconnection this phase needs.
 *
 * The parser understands the subset of the SSE format the API emits:
 * `id:`, `event:`, `data:` fields, `retry:` hints, and comment lines. Field
 * values keep their leading space stripped per the SSE rules; events are
 * dispatched on the blank-line terminator. Comment lines (heartbeats) never
 * produce events.
 */

export interface SseMessage {
  id: string | null;
  event: string | null;
  data: string;
}

export interface ParsedSseLine {
  kind: "field" | "comment" | "dispatch" | "ignored";
  name?: string;
  value?: string;
}

interface StreamState {
  id: string | null;
  event: string | null;
  data: string[];
  dispatchPending: boolean;
}

function emptyState(): StreamState {
  return { id: null, event: null, data: [], dispatchPending: false };
}

function finalize(state: StreamState): SseMessage | null {
  if (state.data.length === 0 && !state.dispatchPending) {
    // A blank line after only comments or fields with no data dispatches
    // nothing; reset the partial event either way.
    state.id = null;
    state.event = null;
    return null;
  }
  const message: SseMessage = {
    id: state.id,
    event: state.event,
    data: state.data.join("\n"),
  };
  state.id = null;
  state.event = null;
  state.data = [];
  state.dispatchPending = false;
  return message;
}

/**
 * Feed one decoded text chunk (which may contain any number of complete or
 * partial lines) and return the complete messages it terminated. The parser
 * accepts LF and CRLF terminators; a trailing partial line is held until the
 * next chunk.
 */
export function createSseParser(): {
  push: (chunk: string) => SseMessage[];
  flush: () => SseMessage[];
} {
  let buffer = "";
  const state = emptyState();

  const handleLine = (line: string, messages: SseMessage[]): void => {
    if (line === "") {
      const message = finalize(state);
      if (message !== null) {
        messages.push(message);
      }
      return;
    }
    if (line.startsWith(":")) {
      // Comment (heartbeat); explicitly ignored as non-data.
      return;
    }
    const colon = line.indexOf(":");
    const rawName = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) {
      value = value.slice(1);
    }
    switch (rawName) {
      case "id":
        // The server never sends a reset (empty id); a canonical sequence
        // string is expected and validated by the caller.
        state.id = value;
        break;
      case "event":
        state.event = value;
        break;
      case "data":
        state.data.push(value);
        state.dispatchPending = true;
        break;
      case "retry":
        // Reconnection timing is owned by the bounded client policy, not
        // the server hint.
        break;
      default:
        // Unknown field names are ignored per the SSE specification.
        break;
    }
  };

  return {
    push(chunk: string): SseMessage[] {
      buffer += chunk;
      const messages: SseMessage[] = [];
      let newlineIndex = buffer.search(/\r?\n/);
      while (newlineIndex !== -1) {
        const line = buffer.slice(0, newlineIndex);
        buffer = buffer.slice(
          line.length + (buffer.startsWith("\r\n", line.length) ? 2 : 1),
        );
        handleLine(line, messages);
        newlineIndex = buffer.search(/\r?\n/);
      }
      return messages;
    },
    flush(): SseMessage[] {
      const messages: SseMessage[] = [];
      if (buffer !== "") {
        handleLine(buffer, messages);
        buffer = "";
      }
      // The stream has ended: a pending event without a trailing blank
      // line still dispatches.
      const message = finalize(state);
      if (message !== null) {
        messages.push(message);
      }
      return messages;
    },
  };
}
