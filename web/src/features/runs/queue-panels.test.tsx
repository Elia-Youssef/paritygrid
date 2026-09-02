/**
 * Queue panel component tests (P17.4): every panel names definition, unit,
 * source, and observation timestamp; absent metrics render as explicitly
 * unavailable; stale and disconnected channel states are labeled and no
 * performance conclusion is drawn for them.
 */
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { QueuePanels } from "./queue-panels";
import { deriveQueuePanels } from "./run-derivations";
import { initialTelemetrySlice } from "../../live/live-state";
import type { TelemetryRecord } from "../../live/protocol";

function record(
  observedMicros: number,
  metrics: { name: string; value: number }[],
): TelemetryRecord {
  return {
    schema_version: 1,
    observed_at_micros: observedMicros,
    run_id: "run-a",
    metrics: metrics.map((metric) => ({
      name: metric.name,
      kind: "gauge",
      value: metric.value,
      labels: {},
    })),
  };
}

describe("QueuePanels", () => {
  it("renders present metrics with capacity, saturation, source, and timestamp", () => {
    const view = deriveQueuePanels(
      {
        ...initialTelemetrySlice(),
        health: "live",
        lastObservedMicros: 5_000,
        snapshotObserved: true,
        records: [
          record(5_000, [
            { name: "writer_queue_depth", value: 2 },
            { name: "writer_queue_capacity", value: 8 },
          ]),
        ],
      },
      "run-a",
    );
    render(<QueuePanels view={view} />);

    const depth = screen.getByTestId("queue-metric-queue-depth");
    expect(depth).toHaveTextContent("2");
    expect(depth).toHaveTextContent("work items");
    expect(depth).toHaveTextContent("Definition:");
    expect(depth).toHaveTextContent("/api/v1/live/runs/");
    expect(depth).toHaveTextContent("1970-01-01T00:00:00.005Z");

    expect(screen.getByTestId("queue-metric-queue-capacity")).toHaveTextContent("8");
    const saturation = screen.getByTestId("saturation-panel");
    expect(saturation).toHaveTextContent("25%");
    expect(saturation).toHaveTextContent("yes — depth and capacity present");
    expect(screen.getByRole("meter", { name: /25 percent/ })).toBeInTheDocument();
    expect(screen.getByText(/advisory and ephemeral/i)).toBeInTheDocument();
  });

  it("renders every metric as explicitly unavailable with no telemetry", () => {
    const view = deriveQueuePanels(initialTelemetrySlice(), "run-a");
    render(<QueuePanels view={view} />);

    // Every metric value and observation timestamp says unavailable; the
    // saturation panel says why it is unsupported.
    for (const name of ["queue-depth", "queue-capacity", "in-flight-work"]) {
      expect(screen.getByTestId(`queue-metric-${name}`)).toHaveTextContent(
        "unavailable",
      );
    }
    const saturation = screen.getByTestId("saturation-panel");
    expect(saturation).toHaveTextContent(
      "no — depth or capacity missing from telemetry",
    );
    expect(screen.queryByRole("meter")).toBeNull();
    expect(
      screen.getByText(/No telemetry snapshot has arrived yet/),
    ).toBeInTheDocument();
  });

  it("labels a disconnected channel and forbids conclusions from stale samples", () => {
    const view = deriveQueuePanels(
      { ...initialTelemetrySlice(), health: "disconnected" },
      "run-a",
    );
    const { unmount: unmountDisconnected } = render(<QueuePanels view={view} />);
    expect(screen.getByText("Live connection lost")).toBeInTheDocument();
    unmountDisconnected();

    const stale = deriveQueuePanels(
      { ...initialTelemetrySlice(), health: "stale" },
      "run-a",
    );
    const { unmount } = render(<QueuePanels view={stale} />);
    expect(
      screen.getByText(/no performance conclusion should be drawn/i),
    ).toBeInTheDocument();
    unmount();
  });
});
