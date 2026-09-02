/**
 * Detail view for one selected conflict.
 *
 * The difference `kind` received from the server is the authoritative
 * statement of what changed: missing sides and explicit nulls render a
 * fixed phrase instead of a fabricated value, value mismatches render both
 * bounded texts, and type mismatches are labeled explicitly. Every string
 * is rendered as inert text through the bounded, redacted display helpers;
 * no markup from the server is ever interpreted.
 */
import type { ConflictDifference, ConflictResponse } from "../../api/generated/schema";
import { StatusBadge } from "../../components/ui/status-badge";
import {
  classificationBadgeState,
  classificationLabel,
  classificationTone,
  differenceKindLabel,
  toClassificationValue,
} from "./classifications";
import {
  boundDisplayValue,
  describeMissingOrNull,
  toFieldDifferenceKind,
} from "./value-bounds";
import { BoundedValue } from "./value-display";

export interface ConflictInspectorProps {
  conflict: ConflictResponse | null;
  /** Reconciliation fingerprint of the selection, when known. */
  fingerprint: string | null;
  /** Selection key binding the conflict id to the snapshot fingerprint. */
  selectedKey: string | null;
}

/** Labels for the suggested resolutions the contract can recommend. */
const RESOLUTION_LABELS: Record<string, string> = {
  create_target: "create the missing target record",
  none: "none suggested",
  review_duplicates: "review the duplicate records",
  review_target_only: "review a target-only record",
  update_target: "update the target record",
};

function suggestedResolutionView(resolution: string | null): React.JSX.Element {
  if (resolution === null || resolution === "none") {
    return <span className="text-muted">none suggested</span>;
  }
  const label = RESOLUTION_LABELS[resolution];
  if (label !== undefined) {
    return <span>{label}</span>;
  }
  // Fail safe: an unrecognized suggestion is shown verbatim (bounded)
  // rather than silently hidden or presented under a wrong label.
  return <BoundedValue text={resolution} maxLength={96} />;
}

function ReferenceList(props: {
  title: string;
  references: ConflictResponse["source_references"];
}): React.JSX.Element {
  return (
    <div>
      <h4 className="text-2xs font-semibold uppercase tracking-label text-muted">
        {props.title}
      </h4>
      {props.references.length === 0 ? (
        <p className="mt-1 text-2xs text-muted">none</p>
      ) : (
        <ul className="mt-1 space-y-0.5">
          {props.references.map((reference) => (
            <li
              key={`${String(reference.position)}-${reference.record_key}`}
              className="text-2xs text-muted"
            >
              <BoundedValue
                text={`#${String(reference.position)} ${reference.record_key}`}
                maxLength={80}
              />
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function KindBadge(props: { kind: string; index: number }): React.JSX.Element {
  const known = toFieldDifferenceKind(props.kind);
  if (known === null) {
    return (
      <span
        data-testid={`difference-kind-${String(props.index)}`}
        className="inline-flex items-center whitespace-nowrap rounded-full border border-failure/30 bg-failure/10 px-2.5 py-1 font-mono text-2xs font-semibold uppercase tracking-label text-failure"
      >
        unknown difference kind
      </span>
    );
  }
  return (
    <span
      data-testid={`difference-kind-${String(props.index)}`}
      className="inline-flex items-center whitespace-nowrap rounded-full border border-border-strong bg-surface-elevated px-2.5 py-1 font-mono text-2xs font-semibold uppercase tracking-label text-muted-strong"
    >
      {differenceKindLabel(known)}
    </span>
  );
}

function DifferenceCard(props: {
  difference: ConflictDifference;
  index: number;
}): React.JSX.Element {
  const { difference, index } = props;
  const kind = toFieldDifferenceKind(difference.kind);
  const missingPhrase = kind === null ? null : describeMissingOrNull(kind);

  return (
    <article
      data-testid={`difference-${String(index)}`}
      className="space-y-2 rounded-md border border-border bg-surface-quiet p-3"
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        <BoundedValue text={difference.field} maxLength={128} />
        <KindBadge kind={difference.kind} index={index} />
      </div>
      {kind === null ? (
        <>
          <p role="alert" className="text-2xs font-semibold text-failure">
            This difference carries an unrecognized kind; both raw texts are shown below
            without interpretation.
          </p>
          <div>
            <p className="text-2xs font-semibold uppercase tracking-label text-muted">
              Source text
            </p>
            <BoundedValue text={difference.source_text} multiline />
          </div>
          <div>
            <p className="text-2xs font-semibold uppercase tracking-label text-muted">
              Target text
            </p>
            <BoundedValue text={difference.target_text} multiline />
          </div>
        </>
      ) : missingPhrase !== null ? (
        // Missing and explicit-null sides are stated by the kind itself; the
        // server sends no comparable text, so none is invented here.
        <p className="text-xs text-muted">{missingPhrase}</p>
      ) : (
        <>
          {kind === "type_mismatch" && (
            <p className="text-2xs font-semibold uppercase tracking-label text-warning">
              Type mismatch
            </p>
          )}
          <div>
            <p className="text-2xs font-semibold uppercase tracking-label text-muted">
              Source value
            </p>
            <BoundedValue text={difference.source_text} multiline />
          </div>
          <div>
            <p className="text-2xs font-semibold uppercase tracking-label text-muted">
              Target value
            </p>
            <BoundedValue text={difference.target_text} multiline />
          </div>
        </>
      )}
    </article>
  );
}

export function ConflictInspector(props: ConflictInspectorProps): React.JSX.Element {
  const { conflict, fingerprint, selectedKey } = props;

  if (conflict === null) {
    return (
      <div
        data-testid="conflict-inspector-empty"
        className="rounded-md border border-dashed border-border bg-surface-quiet p-6 text-sm text-muted"
      >
        Select a conflict to inspect its differences.
      </div>
    );
  }

  const classification = toClassificationValue(conflict.classification);

  return (
    <div
      data-testid="conflict-inspector"
      className="space-y-4 rounded-md border border-border bg-surface p-4"
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-semibold text-foreground">Conflict detail</h3>
        {classification === null ? (
          <StatusBadge state="neutral">
            {boundDisplayValue(conflict.classification, 48)}
          </StatusBadge>
        ) : (
          <StatusBadge
            state={classificationBadgeState(classificationTone(classification))}
          >
            {classificationLabel(classification)}
          </StatusBadge>
        )}
      </div>

      <dl className="grid grid-cols-inspector gap-x-3 gap-y-1.5 text-xs">
        <dt className="text-muted">Conflict id</dt>
        <dd>
          <BoundedValue text={conflict.conflict_id} maxLength={96} />
        </dd>
        <dt className="text-muted">Canonical key</dt>
        <dd>
          <BoundedValue text={conflict.canonical_key} maxLength={96} />
        </dd>
        <dt className="text-muted">Created</dt>
        <dd>
          <BoundedValue text={conflict.created_at} maxLength={64} />
        </dd>
        <dt className="text-muted">Suggested resolution</dt>
        <dd>{suggestedResolutionView(conflict.suggested_resolution)}</dd>
        <dt className="text-muted">Reconciliation fingerprint</dt>
        <dd>
          {fingerprint === null ? (
            <span className="text-muted">unknown</span>
          ) : (
            <BoundedValue text={fingerprint} maxLength={96} />
          )}
        </dd>
        <dt className="text-muted">Selection key</dt>
        <dd>
          {selectedKey === null ? (
            <span className="text-muted">not selected</span>
          ) : (
            <BoundedValue text={selectedKey} maxLength={128} />
          )}
        </dd>
      </dl>

      <section aria-label="References" className="space-y-3">
        <ReferenceList
          title="Source references"
          references={conflict.source_references}
        />
        <ReferenceList
          title="Target references"
          references={conflict.target_references}
        />
      </section>

      <section aria-label="Differences" className="space-y-2">
        <h4 className="text-2xs font-semibold uppercase tracking-label text-muted">
          Differences ({String(conflict.differences.length)})
        </h4>
        {conflict.differences.map((difference, index) => (
          <DifferenceCard
            key={`${String(index)}-${difference.field}`}
            difference={difference}
            index={index}
          />
        ))}
      </section>
    </div>
  );
}
