/**
 * Review, approval, and application surface for one repair plan
 * (route component: `<RepairReview planId={repairId} />`).
 *
 * Safety properties of this composition:
 * - Everything displayed comes from the authoritative plan response and the
 *   run's current reconciliation fingerprint. The durable event list from
 *   useRepairProgress is rendered as notifications only.
 * - Approval requires the plan to be free of blockers AND an explicit
 *   confirmation form: operator identity, plus the exact reviewed content
 *   and reconciliation fingerprints typed or pasted by the operator — the
 *   inputs start empty on purpose, so approval cannot succeed by clicking
 *   through.
 * - Unsupported or destructive action kinds (anything besides create_target
 *   and update_target) are displayed visibly and block both commands.
 * - Apply is enabled only for an approved, unblocked plan, and is disabled
 *   permanently once the plan is authoritative-applied or the stored apply
 *   disposition succeeded (already_applied included). A 503 leaves the
 *   retry affordance in place; the retry reuses the same idempotency key.
 */
import { useEffect, useRef, useState, type FormEvent } from "react";

import { isApiRequestError } from "../../api/client";
import { Button } from "../../components/ui/button";
import { ErrorState } from "../../components/states/error-state";
import { Loading } from "../../components/states/loading";
import { StatusBadge } from "../../components/ui/status-badge";
import type { SseTransport } from "../../live/durable-stream";
import {
  actionSentinelProblem,
  applyBlockers,
  applyIdempotencyKey,
  approvalBlockers,
  describeDisposition,
  describePlanBlocker,
  describePlanStatus,
  isSupportedActionKind,
  planActionSummary,
} from "./repair-contract";
import { RepairProgress } from "./repair-progress";
import {
  useApplyRepairPlan,
  useApproveRepairPlan,
  useRepairPlan,
  useRepairProgress,
  useRunReconciliationFingerprint,
} from "./use-repair-plan";

/** Untrusted strings are bounded before they can reach the DOM. */
function bounded(value: string, maxLength = 512): string {
  const flattened = value.replace(/\s+/g, " ").trim();
  return flattened.length > maxLength ? `${flattened.slice(0, maxLength)}…` : flattened;
}

interface ApprovalFieldErrors {
  operator?: string;
  contentFingerprint?: string;
  reconciliationFingerprint?: string;
}

export function RepairReview({
  planId,
  sseTransport,
}: {
  planId: string;
  /** Injectable durable transport for deterministic tests. */
  sseTransport?: SseTransport;
}): React.JSX.Element {
  const planQuery = useRepairPlan(planId);
  const plan = planQuery.plan;
  const reconciliation = useRunReconciliationFingerprint(plan?.run_id ?? null);
  const approval = useApproveRepairPlan(planId, plan);
  const application = useApplyRepairPlan(planId, plan);
  const progress = useRepairProgress(plan?.run_id ?? null, planId, plan, {
    sseTransport,
  });

  const [formOpen, setFormOpen] = useState(false);
  const [operator, setOperator] = useState("");
  const [contentFingerprintInput, setContentFingerprintInput] = useState("");
  const [reconciliationFingerprintInput, setReconciliationFingerprintInput] =
    useState("");
  const [fieldErrors, setFieldErrors] = useState<ApprovalFieldErrors>({});
  const formHeadingRef = useRef<HTMLHeadingElement | null>(null);
  const prepareButtonRef = useRef<HTMLButtonElement | null>(null);
  const previouslyOpen = useRef(false);
  const [applyConfirmationOpen, setApplyConfirmationOpen] = useState(false);
  const [applyConfirmed, setApplyConfirmed] = useState(false);
  const applyFormHeadingRef = useRef<HTMLHeadingElement | null>(null);
  const prepareApplyButtonRef = useRef<HTMLButtonElement | null>(null);
  const applyPreviouslyOpen = useRef(false);

  // Focus moves to the confirmation heading when the form opens and returns
  // to the prepare control after it closes (mount never steals focus).
  useEffect(() => {
    if (formOpen) {
      formHeadingRef.current?.focus();
    } else if (previouslyOpen.current) {
      prepareButtonRef.current?.focus();
    }
    previouslyOpen.current = formOpen;
  }, [formOpen]);

  useEffect(() => {
    if (applyConfirmationOpen) {
      applyFormHeadingRef.current?.focus();
    } else if (applyPreviouslyOpen.current) {
      prepareApplyButtonRef.current?.focus();
    }
    applyPreviouslyOpen.current = applyConfirmationOpen;
  }, [applyConfirmationOpen]);

  const [seenApprovalDone, setSeenApprovalDone] = useState(approval.done);
  if (approval.done !== seenApprovalDone) {
    // Close the confirmation form as soon as the approval has succeeded
    // (render-time state adjustment; the form must not stay open stale).
    setSeenApprovalDone(approval.done);
    if (approval.done) {
      setFormOpen(false);
    }
  }

  if (planQuery.isPending) {
    return (
      <div data-testid="repair-review" className="p-4">
        <Loading label="Loading repair plan" />
      </div>
    );
  }

  if (plan === null) {
    const problem = isApiRequestError(planQuery.error)
      ? planQuery.error.problem
      : undefined;
    return (
      <div data-testid="repair-review" className="space-y-4 p-4">
        <ErrorState
          title="Repair plan unavailable"
          description="The authoritative repair plan could not be loaded. No approval or application is possible until it loads."
          problem={problem}
          action={
            <Button variant="secondary" onClick={planQuery.refetch}>
              Retry
            </Button>
          }
        />
      </div>
    );
  }

  const blockers = approvalBlockers(plan, {
    authoritativeLoaded: true,
    currentReconciliationFingerprint: reconciliation.loaded
      ? reconciliation.fingerprint
      : null,
  });
  const applyBlockerList = applyBlockers(plan, {
    authoritativeLoaded: true,
    currentReconciliationFingerprint: reconciliation.loaded
      ? reconciliation.fingerprint
      : null,
    applyInFlight: application.applying,
  });
  const applyAllowed = applyBlockerList.codes.length === 0;
  const summary = planActionSummary(plan);
  const status = describePlanStatus(plan.status);
  const applyDisposition =
    application.result !== null
      ? describeDisposition(application.result.disposition)
      : null;
  const applySucceeded = applyDisposition !== null ? applyDisposition.isSuccess : false;
  const applyDisabledPermanently = plan.status === "applied" || applySucceeded;
  const distinctCanonicalKeys = new Set(
    plan.actions.map((action) => action.canonical_key),
  ).size;

  const structuralWarnings: string[] = [];
  for (const kind of summary.unsupported) {
    structuralWarnings.push(
      `Unsupported action kind "${bounded(kind, 64)}" is present. It is treated as unsupported and destructive, and it blocks approval and application.`,
    );
  }
  for (const action of plan.actions) {
    const problem = actionSentinelProblem(action);
    if (problem !== null && isSupportedActionKind(action.kind)) {
      structuralWarnings.push(bounded(problem));
    }
  }

  const openForm = (): void => {
    setOperator("");
    setContentFingerprintInput("");
    setReconciliationFingerprintInput("");
    setFieldErrors({});
    setFormOpen(true);
  };

  const closeForm = (): void => {
    setFormOpen(false);
    setFieldErrors({});
  };

  const closeApplyConfirmation = (): void => {
    setApplyConfirmationOpen(false);
    setApplyConfirmed(false);
  };

  const handleApprovalSubmit = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault();
    if (plan === null || blockers.codes.length > 0 || approval.approving) {
      return;
    }
    const errors: ApprovalFieldErrors = {};
    const trimmedOperator = operator.trim();
    if (trimmedOperator.length < 1 || trimmedOperator.length > 128) {
      errors.operator = "Operator identity is required (1 to 128 characters).";
    }
    if (contentFingerprintInput !== plan.content_fingerprint) {
      errors.contentFingerprint =
        "The reviewed content fingerprint must match the displayed plan content fingerprint exactly.";
    }
    if (reconciliationFingerprintInput !== plan.reconciliation_fingerprint) {
      errors.reconciliationFingerprint =
        "The reviewed reconciliation fingerprint must match the displayed plan reconciliation fingerprint exactly.";
    }
    if (
      errors.operator !== undefined ||
      errors.contentFingerprint !== undefined ||
      errors.reconciliationFingerprint !== undefined
    ) {
      setFieldErrors(errors);
      return;
    }
    setFieldErrors({});
    approval.approve({
      approvedBy: trimmedOperator,
      contentFingerprint: contentFingerprintInput,
      reconciliationFingerprint: reconciliationFingerprintInput,
    });
  };

  return (
    <div data-testid="repair-review" className="space-y-4 p-4">
      <section aria-label="Repair plan identity" className="space-y-2">
        <div className="flex flex-wrap items-center gap-3">
          <h2 className="text-sm font-semibold text-foreground">
            Repair plan <span className="font-mono">{plan.plan_id}</span>
          </h2>
          <span data-testid="plan-status">
            <StatusBadge
              state={
                status.tone === "info"
                  ? "active"
                  : status.tone === "success"
                    ? "verified"
                    : status.tone === "warning"
                      ? "warning"
                      : status.tone === "failure"
                        ? "failure"
                        : "neutral"
              }
            >
              {status.label}
            </StatusBadge>
          </span>
        </div>
        <dl className="space-y-1.5 text-xs text-foreground">
          <div className="flex flex-wrap gap-x-3">
            <dt className="min-w-56 shrink-0 font-semibold text-muted-strong">
              Content fingerprint
            </dt>
            <dd data-testid="plan-fingerprint" className="font-mono">
              {plan.content_fingerprint}
            </dd>
          </div>
          <div className="flex flex-wrap gap-x-3">
            <dt className="min-w-56 shrink-0 font-semibold text-muted-strong">
              Reconciliation fingerprint
            </dt>
            <dd data-testid="plan-reconciliation-fingerprint" className="font-mono">
              {plan.reconciliation_fingerprint}
            </dd>
          </div>
          <div className="flex flex-wrap gap-x-3">
            <dt className="min-w-56 shrink-0 font-semibold text-muted-strong">
              Run identity
            </dt>
            <dd className="font-mono">
              {plan.run_id} · version {String(plan.run_version)} · state {plan.state}
            </dd>
          </div>
          <div className="flex flex-wrap gap-x-3">
            <dt className="min-w-56 shrink-0 font-semibold text-muted-strong">
              Created
            </dt>
            <dd>{plan.created_at}</dd>
          </div>
        </dl>
      </section>

      <section aria-label="Proposed actions" className="space-y-2">
        <h3 className="text-sm font-semibold text-foreground">Proposed actions</h3>
        <p className="text-xs text-muted-strong">
          {String(summary.total)} action{summary.total === 1 ? "" : "s"} across{" "}
          {String(distinctCanonicalKeys)} distinct canonical key
          {distinctCanonicalKeys === 1 ? "" : "s"}.
        </p>
        <table data-testid="action-table" className="w-full text-left text-xs">
          <caption className="sr-only">Proposed repair actions</caption>
          <thead>
            <tr className="border-b border-border text-muted">
              <th scope="col" className="py-1.5 pr-3 font-medium">
                Action id
              </th>
              <th scope="col" className="py-1.5 pr-3 font-medium">
                Canonical key
              </th>
              <th scope="col" className="py-1.5 pr-3 font-medium">
                Kind
              </th>
              <th scope="col" className="py-1.5 pr-3 font-medium">
                Status
              </th>
              <th scope="col" className="py-1.5 pr-3 font-medium">
                Before
              </th>
              <th scope="col" className="py-1.5 font-medium">
                Proposed after
              </th>
            </tr>
          </thead>
          <tbody>
            {plan.actions.map((action) => {
              const supported = isSupportedActionKind(action.kind);
              return (
                <tr key={action.action_id} className="border-b border-border/60">
                  <td className="py-1.5 pr-3 font-mono">{action.action_id}</td>
                  <td className="py-1.5 pr-3 font-mono">{action.canonical_key}</td>
                  <td className="py-1.5 pr-3">
                    <span className="font-mono">{action.kind}</span>
                    {!supported && (
                      <span
                        className="ml-2 inline-block rounded border border-failure/40 bg-failure/10 px-1.5 py-0.5 font-mono text-2xs font-semibold text-failure"
                        data-testid={`action-unsupported-${action.action_id}`}
                      >
                        unsupported/destructive — blocked
                      </span>
                    )}
                  </td>
                  <td className="py-1.5 pr-3 font-mono">{action.status}</td>
                  <td className="py-1.5 pr-3 font-mono">
                    {action.before_sha256 === "" ? (
                      <span data-testid={`before-sentinel-${action.action_id}`}>
                        none (create)
                      </span>
                    ) : (
                      action.before_sha256
                    )}
                  </td>
                  <td className="py-1.5 font-mono">{action.proposed_after_sha256}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </section>

      <section aria-label="Warnings and exclusions" className="space-y-2">
        <h3 className="text-sm font-semibold text-foreground">Warnings</h3>
        {structuralWarnings.length === 0 ? (
          <p className="text-xs text-muted-strong">No structural warnings.</p>
        ) : (
          <ul className="list-disc space-y-1 pl-5 text-xs text-warning">
            {structuralWarnings.map((warning) => (
              <li key={warning}>{warning}</li>
            ))}
          </ul>
        )}
        <h3 className="pt-2 text-sm font-semibold text-foreground">Exclusions</h3>
        <p className="text-xs text-muted-strong">
          Deletion actions do not exist in this product. A plan can only propose create
          or update actions; any foreign action kind in a plan blocks approval and
          application until a corrected plan exists.
        </p>
      </section>

      <section
        aria-label="Approval"
        data-testid="approval-section"
        className="space-y-2"
      >
        <h3 className="text-sm font-semibold text-foreground">Approval</h3>
        {plan.approval != null && (
          <p className="text-xs text-muted-strong">
            Approved by <span className="font-mono">{plan.approval.approved_by}</span>{" "}
            at {plan.approval.approved_at} (correlation{" "}
            <span className="font-mono">{plan.approval.correlation_id}</span>).
          </p>
        )}
        {blockers.codes.length === 0 ? (
          formOpen ? (
            <form onSubmit={handleApprovalSubmit} noValidate className="space-y-3">
              <h4
                ref={formHeadingRef}
                tabIndex={-1}
                className="text-xs font-semibold text-foreground"
              >
                Confirm approval
              </h4>
              <fieldset
                className="space-y-3 rounded-md border border-border p-3"
                disabled={approval.approving}
              >
                <legend className="px-1 text-2xs font-semibold tracking-label text-muted-strong uppercase">
                  Explicit confirmation required
                </legend>
                <p className="text-xs text-muted-strong">
                  Approval is a durable command. It is sent once under the idempotency
                  key derived from this exact plan and operator identity; byte-identical
                  retries reuse that key and replay the stored outcome.
                </p>
                <div className="space-y-1">
                  <label
                    htmlFor="approval-operator"
                    className="block text-xs font-medium text-foreground"
                  >
                    Operator identity (recorded as approved_by)
                  </label>
                  <input
                    id="approval-operator"
                    data-testid="approval-operator"
                    type="text"
                    autoComplete="off"
                    className="w-full rounded border border-border bg-surface px-2 py-1.5 text-xs text-foreground"
                    value={operator}
                    onChange={(event) => setOperator(event.target.value)}
                  />
                  {fieldErrors.operator !== undefined && (
                    <p role="alert" className="text-xs text-failure">
                      {fieldErrors.operator}
                    </p>
                  )}
                </div>
                <div className="space-y-1">
                  <label
                    htmlFor="approval-content-fingerprint"
                    className="block text-xs font-medium text-foreground"
                  >
                    Reviewed content fingerprint (type or paste exactly)
                  </label>
                  <input
                    id="approval-content-fingerprint"
                    data-testid="approval-content-fingerprint"
                    type="text"
                    autoComplete="off"
                    spellCheck={false}
                    className="w-full rounded border border-border bg-surface px-2 py-1.5 font-mono text-xs text-foreground"
                    value={contentFingerprintInput}
                    onChange={(event) => setContentFingerprintInput(event.target.value)}
                  />
                  <button
                    type="button"
                    className="rounded border border-border px-2 py-1 text-2xs text-foreground"
                    onClick={() => setContentFingerprintInput(plan.content_fingerprint)}
                  >
                    Use displayed content fingerprint
                  </button>
                  {fieldErrors.contentFingerprint !== undefined && (
                    <p role="alert" className="text-xs text-failure">
                      {fieldErrors.contentFingerprint}
                    </p>
                  )}
                </div>
                <div className="space-y-1">
                  <label
                    htmlFor="approval-reconciliation-fingerprint"
                    className="block text-xs font-medium text-foreground"
                  >
                    Reviewed reconciliation fingerprint (type or paste exactly)
                  </label>
                  <input
                    id="approval-reconciliation-fingerprint"
                    data-testid="approval-reconciliation-fingerprint"
                    type="text"
                    autoComplete="off"
                    spellCheck={false}
                    className="w-full rounded border border-border bg-surface px-2 py-1.5 font-mono text-xs text-foreground"
                    value={reconciliationFingerprintInput}
                    onChange={(event) =>
                      setReconciliationFingerprintInput(event.target.value)
                    }
                  />
                  <button
                    type="button"
                    className="rounded border border-border px-2 py-1 text-2xs text-foreground"
                    onClick={() =>
                      setReconciliationFingerprintInput(plan.reconciliation_fingerprint)
                    }
                  >
                    Use displayed reconciliation fingerprint
                  </button>
                  {fieldErrors.reconciliationFingerprint !== undefined && (
                    <p role="alert" className="text-xs text-failure">
                      {fieldErrors.reconciliationFingerprint}
                    </p>
                  )}
                </div>
                <div className="flex flex-wrap gap-2">
                  <Button
                    type="submit"
                    variant="primary"
                    data-testid="approval-submit"
                    disabled={approval.approving}
                  >
                    Approve plan
                  </Button>
                  <Button
                    type="button"
                    variant="secondary"
                    data-testid="approval-cancel"
                    disabled={approval.approving}
                    onClick={closeForm}
                  >
                    Cancel
                  </Button>
                </div>
              </fieldset>
            </form>
          ) : (
            <Button
              type="button"
              variant="primary"
              data-testid="approval-open"
              ref={prepareButtonRef}
              onClick={openForm}
            >
              Prepare approval
            </Button>
          )
        ) : (
          <div
            data-testid="approval-blocked"
            role="status"
            aria-live="polite"
            className="space-y-2 rounded-md border border-warning/40 bg-warning/10 p-3"
          >
            <p className="text-sm font-semibold text-foreground">
              Approval is blocked for this plan
            </p>
            <ul className="list-disc space-y-1 pl-5 text-xs text-foreground">
              {blockers.codes.map((code) => (
                <li key={code}>{describePlanBlocker(code)}</li>
              ))}
            </ul>
          </div>
        )}
        {approval.error !== null && (
          <ErrorState
            title="Approval failed"
            description="The approve command did not succeed. Retrying reuses the same idempotency key."
            problem={
              isApiRequestError(approval.error) ? approval.error.problem : undefined
            }
          />
        )}
      </section>

      <section aria-label="Application" className="space-y-2">
        <h3 className="text-sm font-semibold text-foreground">Application</h3>
        {applyAllowed ? (
          applyConfirmationOpen ? (
            <form
              className="space-y-3 rounded-md border border-warning/40 bg-warning/10 p-3"
              onSubmit={(event) => {
                event.preventDefault();
                if (!applyConfirmed || application.applying) {
                  return;
                }
                application.apply();
              }}
            >
              <h4
                ref={applyFormHeadingRef}
                tabIndex={-1}
                className="text-xs font-semibold text-foreground"
              >
                Confirm repair application
              </h4>
              <p className="text-xs text-muted-strong">
                This applies {String(summary.total)} approved create/update action
                {summary.total === 1 ? "" : "s"} to the target under the exact plan and
                reconciliation fingerprints shown above.
              </p>
              <label className="flex items-start gap-2 text-xs text-foreground">
                <input
                  type="checkbox"
                  data-testid="apply-confirmation"
                  checked={applyConfirmed}
                  onChange={(event) => setApplyConfirmed(event.target.checked)}
                />
                <span>
                  I reviewed the approved plan and deliberately authorize applying it to
                  the target.
                </span>
              </label>
              <div className="flex flex-wrap gap-2">
                <Button
                  type="submit"
                  variant="primary"
                  data-testid="apply-button"
                  disabled={
                    !applyConfirmed || application.applying || applyDisabledPermanently
                  }
                >
                  Confirm and apply repair
                </Button>
                <Button
                  type="button"
                  variant="secondary"
                  data-testid="apply-cancel"
                  disabled={application.applying}
                  onClick={closeApplyConfirmation}
                >
                  Cancel
                </Button>
              </div>
              <p className="text-2xs text-muted">
                Sent under the stable idempotency key{" "}
                <span className="font-mono">{applyIdempotencyKey(plan)}</span>. If the
                application is interrupted, retrying resumes the same plan with the same
                key.
              </p>
            </form>
          ) : (
            <div className="space-y-2">
              <Button
                type="button"
                variant="primary"
                data-testid="apply-open"
                ref={prepareApplyButtonRef}
                disabled={applyDisabledPermanently}
                onClick={() => {
                  setApplyConfirmed(false);
                  setApplyConfirmationOpen(true);
                }}
              >
                Prepare application
              </Button>
              <p className="text-xs text-muted-strong">
                A separate deliberate confirmation is required immediately before the
                approved plan can mutate the target.
              </p>
            </div>
          )
        ) : (
          <div className="space-y-2">
            {plan.approval != null && (
              <Button
                type="button"
                variant="primary"
                data-testid="apply-button"
                disabled
              >
                Apply repair
              </Button>
            )}
            <div
              data-testid="apply-blocked"
              role="status"
              aria-live="polite"
              className="space-y-2 rounded-md border border-warning/40 bg-warning/10 p-3"
            >
              <p className="text-sm font-semibold text-foreground">
                Application is blocked for this plan
              </p>
              <ul className="list-disc space-y-1 pl-5 text-xs text-foreground">
                {applyBlockerList.codes.map((code) => (
                  <li key={code}>{describePlanBlocker(code)}</li>
                ))}
              </ul>
            </div>
          </div>
        )}
        {applyDisabledPermanently && (
          <p
            role="status"
            className="text-xs text-muted-strong"
            data-testid="apply-done-note"
          >
            {plan.status === "applied"
              ? "The authoritative plan status is applied, so apply is disabled permanently."
              : "The stored application outcome succeeded (idempotent replay counts), so apply is disabled permanently."}
          </p>
        )}
      </section>

      <RepairProgress
        planStatus={plan.status}
        progress={progress}
        actionSummary={summary}
        applyResult={application.result}
        applyError={application.error}
        isApplying={application.applying}
      />

      <section aria-label="Plan creation availability" className="space-y-1">
        <h3 className="text-sm font-semibold text-foreground">Creating new plans</h3>
        <p className="text-xs text-muted-strong">
          Creating new repair plans from reconciliation projections is unavailable in
          this interface: the create-plan API requires the exact complete source and
          target observations, which the conflict projections do not carry. Review,
          approval, and application of existing plans are fully supported here.
        </p>
      </section>
    </div>
  );
}
