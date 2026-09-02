/**
 * React bindings for the repair review surface.
 *
 * Authority rules enforced here:
 * - The only authoritative plan state is GET /api/v1/repair-plans/{plan_id}.
 *   Durable SSE events are notifications: they may refresh the authoritative
 *   query, and they may be listed, but they never advance plan state and
 *   never declare an application complete.
 * - Approve and apply reuse one deterministic idempotency key per logical
 *   command, so an operator retry after a transport or 5xx failure replays
 *   the stored server outcome instead of re-executing the command.
 * - A 503 (unresolved/interrupted) is a recoverable failure: it surfaces as
 *   an error with the response status, never as success, and a retry resumes
 *   the same plan under the same key.
 * - The durable progress stream quarantines anything it cannot bind to the
 *   exact plan (foreign run, foreign channel, sequence anomalies, mismatched
 *   fingerprints, unknown canonical keys) instead of guessing.
 */
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";

import {
  approveRepairPlan,
  applyRepairPlan,
  fetchReconciliation,
  fetchRepairPlan,
} from "../../api/client";
import type {
  RepairApplyResponse,
  RepairApprovalRequestBody,
  RepairPlanResponse,
} from "../../api/generated/schema";
import { queryKeys } from "../../api/query-keys";
import {
  createFetchSseTransport,
  defaultBackoffDelay,
  setTimeoutScheduler,
  type Scheduler,
  type SseMessage,
  type SseTransport,
} from "../../live/durable-stream";
import { parseDurableFrame, type DurableEventFrame } from "../../live/protocol";
import { applyIdempotencyKey, approvalIdempotencyKey } from "./repair-contract";

// ---------------------------------------------------------------------------
// Authoritative plan query
// ---------------------------------------------------------------------------

export interface RepairPlanView {
  plan: RepairPlanResponse | null;
  isPending: boolean;
  error: unknown;
  refetch: () => void;
}

/** The authoritative plan document. Stale immediately; never retried. */
export function useRepairPlan(planId: string): RepairPlanView {
  const query = useQuery({
    queryKey: queryKeys.repairPlan(planId),
    queryFn: ({ signal }) => fetchRepairPlan(planId, { signal }),
    enabled: planId !== "",
    staleTime: 0,
    retry: 0,
  });
  return {
    plan: query.data ?? null,
    isPending: query.isPending,
    error: query.error,
    refetch: () => {
      void query.refetch();
    },
  };
}

export interface RunReconciliationFingerprintView {
  /** The run's current reconciliation fingerprint, or null until loaded. */
  fingerprint: string | null;
  loaded: boolean;
  error: unknown;
}

/**
 * The run's current reconciliation fingerprint, used as the staleness
 * fence: a plan whose fingerprint no longer matches was derived from a
 * superseded reconciliation.
 */
export function useRunReconciliationFingerprint(
  runId: string | null,
): RunReconciliationFingerprintView {
  const query = useQuery({
    queryKey: queryKeys.reconciliation(runId ?? "unknown"),
    queryFn: ({ signal }) => fetchReconciliation(runId ?? "unknown", { signal }),
    enabled: runId !== null,
    staleTime: 0,
    retry: 0,
  });
  return {
    fingerprint: query.data?.reconciliation_fingerprint ?? null,
    loaded: query.isSuccess,
    error: query.error,
  };
}

// ---------------------------------------------------------------------------
// Approve and apply commands
// ---------------------------------------------------------------------------

export interface ApprovalInput {
  approvedBy: string;
  contentFingerprint: string;
  reconciliationFingerprint: string;
}

export interface ApprovalMutationView {
  approve: (input: ApprovalInput) => void;
  approving: boolean;
  error: unknown;
  /** True after this plan's approval succeeded; resets on planId change. */
  done: boolean;
}

export function useApproveRepairPlan(
  planId: string,
  plan: RepairPlanResponse | null,
): ApprovalMutationView {
  const queryClient = useQueryClient();
  const mutation = useMutation({
    mutationFn: async (input: ApprovalInput) => {
      if (plan === null) {
        throw new Error(
          "The authoritative plan is not loaded; approval cannot be sent.",
        );
      }
      if (
        plan.plan_id !== planId ||
        input.contentFingerprint !== plan.content_fingerprint ||
        input.reconciliationFingerprint !== plan.reconciliation_fingerprint
      ) {
        throw new Error(
          "The approval request does not match the authoritative plan identity.",
        );
      }
      const body: RepairApprovalRequestBody = {
        schema_version: 1,
        approved_by: input.approvedBy,
        approved_content_fingerprint: input.contentFingerprint,
        approved_reconciliation_fingerprint: input.reconciliationFingerprint,
      };
      // Retries of the same logical approval must reuse the same key so the
      // server replays the stored outcome.
      return approveRepairPlan(planId, body, {
        idempotencyKey: approvalIdempotencyKey(plan, input.approvedBy),
      });
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: queryKeys.repairPlan(planId) });
    },
    retry: 0,
  });

  // The submitted plan id rides in state so a stored outcome can never be
  // reported for a different plan than the one currently on screen: a
  // plan-id change falsifies `done` at render time, before any effect.
  const [approvedForPlanId, setApprovedForPlanId] = useState<string | null>(null);
  const approve = (input: ApprovalInput): void => {
    setApprovedForPlanId(planId);
    mutation.mutate(input);
  };

  return {
    approve,
    approving: mutation.isPending,
    error: mutation.error,
    done: mutation.isSuccess && approvedForPlanId === planId,
  };
}

export interface ApplyMutationView {
  apply: () => void;
  applying: boolean;
  error: unknown;
  /** The stored apply outcome, or null while nothing succeeded for this plan. */
  result: RepairApplyResponse | null;
}

export function useApplyRepairPlan(
  planId: string,
  plan: RepairPlanResponse | null,
): ApplyMutationView {
  const queryClient = useQueryClient();
  const mutation = useMutation({
    mutationFn: async () => {
      if (plan === null) {
        throw new Error("The authoritative plan is not loaded; apply cannot be sent.");
      }
      if (
        plan.plan_id !== planId ||
        plan.status !== "approved" ||
        plan.approval == null
      ) {
        throw new Error(
          "The authoritative plan is not safely approved for application.",
        );
      }
      // Unresolved/interrupted retries resume the same plan under the same
      // key; a 503 response throws and lands in `error`, never in `result`.
      const result = await applyRepairPlan(planId, {
        idempotencyKey: applyIdempotencyKey(plan),
      });
      if (
        result.run_id !== plan.run_id ||
        result.content_fingerprint !== plan.content_fingerprint ||
        result.reconciliation_fingerprint !== plan.reconciliation_fingerprint
      ) {
        throw new Error(
          "The application outcome does not match the authoritative plan identity.",
        );
      }
      return result;
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: queryKeys.repairPlan(planId) });
    },
    retry: 0,
  });

  // The submitted plan id rides in state so a stored outcome can never be
  // reported for a different plan than the one currently on screen.
  const [appliedForPlanId, setAppliedForPlanId] = useState<string | null>(null);
  const apply = (): void => {
    setAppliedForPlanId(planId);
    mutation.mutate(undefined);
  };

  return {
    apply,
    applying: mutation.isPending,
    error: mutation.error,
    result:
      mutation.isSuccess && appliedForPlanId === planId
        ? (mutation.data ?? null)
        : null,
  };
}

// ---------------------------------------------------------------------------
// Durable application progress (notifications only — never authoritative)
// ---------------------------------------------------------------------------

export type RepairProgressConnection = "off" | "connecting" | "live" | "error";

export interface RepairProgressNotification {
  sequence: number;
  kind: string;
  at: string;
}

export interface RepairProgress {
  connected: RepairProgressConnection;
  /** Bounded (newest last) list of accepted durable events, deduped by sequence. */
  notifications: RepairProgressNotification[];
  /**
   * Count of stream frames that were ignored because they could not be
   * bound to this exact plan: invalid envelopes, foreign runs, sequence
   * anomalies, mismatched fingerprints, or unknown canonical keys.
   */
  quarantinedEvents: number;
  /**
   * Invariant: this stays null. Terminal application state is NEVER derived
   * from stream events. Completion comes only from the authoritative plan
   * query and the apply mutation result, because payloads carry no plan or
   * action identity — only fingerprints and canonical keys, which bind
   * notifications but cannot prove a disposition.
   */
  lastCompletedDisposition: string | null;
}

export interface UseRepairProgressOptions {
  /** Injectable transport for deterministic tests; production uses fetch SSE. */
  sseTransport?: SseTransport;
  /** Injectable scheduler for deterministic reconnect timing in tests. */
  scheduler?: Scheduler;
}

const MAX_NOTIFICATIONS = 50;
const MAX_RECONNECT_ATTEMPTS = 5;

/** The durable event kinds that carry repair progress for a run. */
const REPAIR_PROGRESS_EVENT_KINDS: ReadonlySet<string> = new Set([
  "repair_plan_approved",
  "repair_plan_rejected",
  "repair_application_started",
  "repair_action_applied",
  "repair_action_ambiguous",
  "repair_action_failed",
  "repair_application_completed",
]);

/**
 * The plan identity that stream frames must bind to. Application started/
 * completed events bind by exact fingerprints; per-action events bind by
 * canonical-key membership. Frames arrive only after this binding loads;
 * anything earlier is quarantined because it cannot be proven relevant.
 */
interface PlanBinding {
  bound: boolean;
  contentFingerprint: string;
  reconciliationFingerprint: string;
  canonicalKeys: ReadonlySet<string>;
}

const UNBOUND: PlanBinding = {
  bound: false,
  contentFingerprint: "",
  reconciliationFingerprint: "",
  canonicalKeys: new Set<string>(),
};

function describeBinding(plan: RepairPlanResponse | null): PlanBinding {
  if (plan === null) {
    return UNBOUND;
  }
  return {
    bound: true,
    contentFingerprint: plan.content_fingerprint,
    reconciliationFingerprint: plan.reconciliation_fingerprint,
    canonicalKeys: new Set(plan.actions.map((action) => action.canonical_key)),
  };
}

function describeBindingKey(planId: string, plan: RepairPlanResponse | null): string {
  const binding = describeBinding(plan);
  const keys = [...binding.canonicalKeys].sort().join(",");
  return `${planId}|${binding.bound ? `${binding.contentFingerprint}|${binding.reconciliationFingerprint}|${keys}` : "unbound"}`;
}

function payloadString(payload: Record<string, unknown>, key: string): string | null {
  const value = payload[key];
  return typeof value === "string" ? value : null;
}

/**
 * Durable repair progress for one plan's run. Opens the durable run stream
 * (`GET /api/v1/stream/runs/{run_id}`, resuming from the last accepted
 * sequence) over the injected or fetch-based transport, filters every frame
 * against the exact plan binding, keeps a bounded notification list, and
 * refreshes the authoritative plan query for each accepted event.
 *
 * This module deliberately does its own sequence bookkeeping instead of
 * reusing DurableRunStream: the quarantine contract must OBSERVE duplicate
 * replays (silently deduped) and out-of-order frames (counted), which
 * DurableRunStream swallows internally before its frame handler runs. SSE
 * text parsing and frame validation are still reused, never reimplemented.
 */
export function useRepairProgress(
  runId: string | null,
  planId: string,
  plan: RepairPlanResponse | null,
  options: UseRepairProgressOptions = {},
): RepairProgress {
  const queryClient = useQueryClient();
  const [connected, setConnected] = useState<RepairProgressConnection>("off");
  const [notifications, setNotifications] = useState<RepairProgressNotification[]>([]);
  const [quarantinedEvents, setQuarantinedEvents] = useState(0);

  const binding = useMemo(() => describeBinding(plan), [plan]);
  const bindingRef = useRef(binding);
  useEffect(() => {
    // Keep the frame filter current without restarting the connection on
    // every authoritative refetch (object identity changes; values may not).
    bindingRef.current = binding;
  }, [binding]);

  const lastAcceptedRef = useRef(0);
  const bindingKey = useMemo(() => describeBindingKey(planId, plan), [planId, plan]);
  const [renderedBindingKey, setRenderedBindingKey] = useState(bindingKey);
  if (renderedBindingKey !== bindingKey) {
    // Plan identity change (different plan, fingerprints, or canonical
    // keys): restart the visible notification list with the new binding.
    // The accepted sequence is deliberately NOT reset here: it belongs to
    // the transport position, and rewinding it mid-connection would
    // manufacture artificial gaps. Frames already surfaced under the old
    // binding are cleared rather than re-shown, which is the safe direction.
    setRenderedBindingKey(bindingKey);
    setNotifications([]);
  }

  useEffect(() => {
    if (runId === null || planId === "") {
      return;
    }
    let disposed = false;
    let attempts = 0;
    let cancelTimer: (() => void) | null = null;
    let controller: AbortController | null = null;
    // One stream generation per (run, plan id); sequence history never
    // carries across generations.
    lastAcceptedRef.current = 0;
    const transport = options.sseTransport ?? createFetchSseTransport();
    const scheduler = options.scheduler ?? setTimeoutScheduler;
    const activeRunId = runId;

    const quarantine = (): void => {
      setQuarantinedEvents((count) => count + 1);
    };

    const accept = (frame: DurableEventFrame): void => {
      lastAcceptedRef.current = frame.sequence;
      attempts = 0;
      setConnected("live");
      setNotifications((previous) => {
        if (previous.some((item) => item.sequence === frame.sequence)) {
          return previous;
        }
        const next = [
          ...previous,
          { sequence: frame.sequence, kind: frame.event_kind, at: frame.occurred_at },
        ];
        return next.length > MAX_NOTIFICATIONS
          ? next.slice(next.length - MAX_NOTIFICATIONS)
          : next;
      });
      // An accepted notification is a hint that authoritative state moved:
      // refresh the plan query, which remains the only authority.
      void queryClient.invalidateQueries({ queryKey: queryKeys.repairPlan(planId) });
    };

    const handleMessage = (message: SseMessage): void => {
      if (disposed) {
        return;
      }
      // Structural phase: a frame must parse, name its own sequence
      // truthfully, belong to this run, and be contiguous. Only then does it
      // advance the accepted sequence.
      let parsed: unknown;
      try {
        parsed = JSON.parse(message.data) as unknown;
      } catch {
        quarantine();
        return;
      }
      const parsedFrame = parseDurableFrame(parsed);
      if (!parsedFrame.ok) {
        quarantine();
        return;
      }
      const frame = parsedFrame.frame;
      if (message.event !== null && message.event !== frame.event_kind) {
        quarantine();
        return;
      }
      if (message.id !== null && message.id !== String(frame.sequence)) {
        quarantine();
        return;
      }
      if (frame.run_id !== runId) {
        quarantine();
        return;
      }
      if (frame.subject_kind !== "run" || frame.subject_id !== runId) {
        quarantine();
        return;
      }
      if (frame.sequence === lastAcceptedRef.current) {
        // Exact duplicate replay (reconnect replay is normal): silently
        // deduplicated, never surfaced and never counted as a quarantine.
        return;
      }
      if (frame.sequence < lastAcceptedRef.current) {
        // Out-of-order delivery within this binding: counted, not surfaced.
        quarantine();
        return;
      }
      if (frame.sequence > lastAcceptedRef.current + 1) {
        // Discontinuous durable history: quarantine the frame and let the
        // authoritative refresh heal the view; the next reconnect resumes
        // from the last accepted sequence.
        quarantine();
        void queryClient.invalidateQueries({ queryKey: queryKeys.repairPlan(planId) });
        return;
      }
      lastAcceptedRef.current = frame.sequence;

      // Binding phase: the frame is structurally sound and in sequence, but
      // it may still be irrelevant to this exact plan. Such frames advance
      // the stream position yet are counted and never surfaced.
      if (!REPAIR_PROGRESS_EVENT_KINDS.has(frame.event_kind)) {
        quarantine();
        return;
      }
      const currentBinding = bindingRef.current;
      if (!currentBinding.bound) {
        // The plan has not loaded yet, so relevance cannot be proven.
        quarantine();
        return;
      }
      if (
        frame.event_kind === "repair_plan_approved" ||
        frame.event_kind === "repair_plan_rejected" ||
        frame.event_kind === "repair_application_started" ||
        frame.event_kind === "repair_application_completed"
      ) {
        const content = payloadString(frame.payload, "content_fingerprint");
        const reconciliation = payloadString(
          frame.payload,
          "reconciliation_fingerprint",
        );
        if (
          content !== currentBinding.contentFingerprint ||
          reconciliation !== currentBinding.reconciliationFingerprint
        ) {
          quarantine();
          return;
        }
      } else if (
        frame.event_kind === "repair_action_applied" ||
        frame.event_kind === "repair_action_failed" ||
        frame.event_kind === "repair_action_ambiguous"
      ) {
        // The durable action payload identifies only a canonical key. Two
        // plans for one run may share that key, so the frame cannot be bound
        // to this exact plan. Keep it out of the notification list and use it
        // only as a hint to refresh authoritative plan state.
        quarantine();
        void queryClient.invalidateQueries({ queryKey: queryKeys.repairPlan(planId) });
        return;
      }
      accept(frame);
    };

    function handleConnectionEnd(): void {
      if (disposed) {
        return;
      }
      attempts += 1;
      if (attempts > MAX_RECONNECT_ATTEMPTS) {
        setConnected("error");
        return;
      }
      cancelTimer = scheduler.schedule(() => {
        cancelTimer = null;
        if (!disposed) {
          connect();
        }
      }, defaultBackoffDelay(attempts));
    }

    function connect(): void {
      if (disposed) {
        return;
      }
      setConnected("connecting");
      const current = new AbortController();
      controller = current;
      const url = `/api/v1/stream/runs/${encodeURIComponent(activeRunId)}?after=${lastAcceptedRef.current}`;
      void transport(url, { signal: current.signal, onEvent: handleMessage })
        .then(() => {
          if (disposed || controller !== current) {
            return;
          }
          handleConnectionEnd();
        })
        .catch(() => {
          if (disposed) {
            return;
          }
          handleConnectionEnd();
        });
    }

    connect();

    return () => {
      disposed = true;
      if (cancelTimer !== null) {
        cancelTimer();
      }
      controller?.abort();
      controller = null;
    };
    // The transport and scheduler are stable injected references; the stream
    // is owned by one (run, plan) pair and filtering adapts through refs.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId, planId, queryClient]);

  return {
    connected: runId === null || planId === "" ? "off" : connected,
    notifications,
    quarantinedEvents,
    // See the interface doc: never derived from events.
    lastCompletedDisposition: null,
  };
}
