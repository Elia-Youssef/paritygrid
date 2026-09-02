/**
 * Durable event timeline (P17.5). Renders only the bounded log of accepted
 * durable events, newest first, each with its sequence so operators can
 * verify contiguity themselves. Advisory telemetry messages are never shown
 * here; they live only in the clearly-labeled queue panels. Recovery keeps
 * the last coherent events visible under an explicit banner.
 */
import type { DurableRunState } from "../../live/live-state";
import { MAX_EVENT_LOG } from "../../live/live-state";

export interface EventTimelineProps {
  readonly durable: DurableRunState;
}

export function EventTimeline({ durable }: EventTimelineProps) {
  return (
    <div data-testid="event-timeline">
      {durable.events.length === 0 ? (
        <p className="px-4 py-3 text-xs text-muted" role="status">
          No durable events accepted yet; the timeline fills as the stream delivers
          them.
        </p>
      ) : (
        <>
          <p className="border-b border-border px-4 py-1 text-2xs text-muted">
            Showing the {String(durable.events.length)} most recent accepted durable
            events (bounded to {String(MAX_EVENT_LOG)}), newest first. Advisory
            telemetry messages are never listed here.
          </p>
          <div className="max-h-72 overflow-y-auto">
            <table
              className="w-full text-left text-xs"
              aria-label="Durable event timeline"
            >
              <caption className="sr-only">
                Accepted durable events with their contiguous sequence numbers.
              </caption>
              <thead>
                <tr className="border-b border-border text-muted">
                  <th scope="col" className="px-4 py-1.5 font-medium">
                    Sequence
                  </th>
                  <th scope="col" className="px-4 py-1.5 font-medium">
                    Event
                  </th>
                  <th scope="col" className="px-4 py-1.5 font-medium">
                    Subject
                  </th>
                  <th scope="col" className="px-4 py-1.5 font-medium">
                    Occurred
                  </th>
                </tr>
              </thead>
              <tbody data-testid="event-timeline-tbody">
                {durable.events.map((event) => (
                  <tr
                    key={event.sequence}
                    className="border-b border-border/60 last:border-b-0"
                    data-testid={`event-row-${String(event.sequence)}`}
                  >
                    <td className="px-4 py-1.5 font-mono text-foreground">
                      {String(event.sequence)}
                    </td>
                    <td className="px-4 py-1.5 font-mono text-foreground">
                      {event.event_kind}
                    </td>
                    <td className="px-4 py-1.5 text-muted">
                      {event.subject_kind}:{" "}
                      <span className="font-mono">{event.subject_id}</span>
                    </td>
                    <td className="px-4 py-1.5 font-mono text-muted">
                      {event.occurred_at}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
