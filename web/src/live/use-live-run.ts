/**
 * React binding for one run's live channel. The channel is created once per
 * run identity, started after mount, and disposed on unmount or when the
 * route moves to a different run, so both transports are always torn down
 * with the screen. Snapshot stability follows the channel contract: the
 * published snapshot reference changes only when coherent state changes.
 *
 * A disposed channel is never restarted; the retry path bumps an epoch that
 * rebuilds the channel from scratch, and the effect disposes the previous
 * one while React swaps them. Transports, scheduler, and clock are
 * injectable with production defaults so tests can drive every asynchronous
 * boundary deterministically.
 */
import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useState, useSyncExternalStore } from "react";

import { LiveRunChannel, type ChannelSnapshot } from "./live-run-channel";
import {
  createBrowserTelemetryTransport,
  type TelemetryTransportFactory,
} from "./telemetry-socket";
import {
  createFetchSseTransport,
  type Scheduler,
  type SseTransport,
} from "./durable-stream";

export interface UseLiveRunOptions {
  readonly sseTransport?: SseTransport;
  readonly telemetryTransport?: TelemetryTransportFactory;
  readonly scheduler?: Scheduler;
  readonly now?: () => number;
}

export interface UseLiveRunResult {
  readonly snapshot: ChannelSnapshot;
  /** Rebuild the channel after a terminal failure; the old one is disposed. */
  readonly retry: () => void;
}

export function useLiveRun(
  runId: string,
  options: UseLiveRunOptions = {},
): UseLiveRunResult {
  const queryClient = useQueryClient();
  const [epoch, setEpoch] = useState(0);

  const channel = useMemo(() => {
    return new LiveRunChannel({
      runId,
      queryClient,
      sseTransport: options.sseTransport ?? createFetchSseTransport(),
      telemetryTransport:
        options.telemetryTransport ?? createBrowserTelemetryTransport(),
      scheduler: options.scheduler,
      now: options.now,
    });
    // The query client is the app-wide singleton, and the injected
    // transports are stable references, so the channel is owned by one
    // (run identity, epoch) pair; a different run or an explicit retry
    // must build a different channel.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    runId,
    epoch,
    options.sseTransport,
    options.telemetryTransport,
    options.scheduler,
    options.now,
  ]);

  const snapshot = useSyncExternalStore(
    channel.subscribe,
    channel.getSnapshot,
    channel.getSnapshot,
  );

  useEffect(() => {
    void channel.start();
    return () => {
      channel.dispose();
    };
  }, [channel]);

  return {
    snapshot,
    retry: () => {
      setEpoch((current) => current + 1);
    },
  };
}
