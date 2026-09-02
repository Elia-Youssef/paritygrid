/**
 * Closed classification vocabulary for the reconciliation view, mirroring
 * the backend contract (`CLASSIFICATION_VALUES`, `FieldDifferenceKind`,
 * `SuggestedResolution`). The tuples are re-exported from the runtime
 * schema layer so the parser and the presentation layer can never drift:
 * a classification the schema rejects is also one this module cannot label.
 */
import {
  FIELD_DIFFERENCE_KIND_KEYS,
  RECONCILIATION_CLASSIFICATION_KEYS,
  SUGGESTED_RESOLUTION_KEYS,
} from "../../api/runtime-schemas";

export const RECONCILIATION_CLASSIFICATIONS = RECONCILIATION_CLASSIFICATION_KEYS;
export type ReconciliationClassificationValue =
  (typeof RECONCILIATION_CLASSIFICATIONS)[number];

export const FIELD_DIFFERENCE_KINDS = FIELD_DIFFERENCE_KIND_KEYS;
export type FieldDifferenceKind = (typeof FIELD_DIFFERENCE_KINDS)[number];

export const SUGGESTED_RESOLUTIONS = SUGGESTED_RESOLUTION_KEYS;
export type SuggestedResolutionValue = (typeof SUGGESTED_RESOLUTIONS)[number];

/**
 * Number of persisted conflict rows represented by one reconciliation
 * summary. `total_count` includes matching canonical keys, but the conflict
 * endpoint deliberately excludes matches, so workbench totals and empty
 * states must sum only the six non-match classifications.
 */
export function reconciliationConflictCount(
  counts: Readonly<Record<string, number>>,
): number {
  return RECONCILIATION_CLASSIFICATIONS.reduce(
    (total, classification) =>
      classification === "match" ? total : total + (counts[classification] ?? 0),
    0,
  );
}

const CLASSIFICATION_VALUES_SET: ReadonlySet<string> = new Set(
  RECONCILIATION_CLASSIFICATIONS,
);

const DIFFERENCE_KINDS_SET: ReadonlySet<string> = new Set(FIELD_DIFFERENCE_KINDS);

/** Narrows wire text to a known classification; unknown values stay unrendered. */
export function isReconciliationClassification(
  value: string,
): value is ReconciliationClassificationValue {
  return CLASSIFICATION_VALUES_SET.has(value);
}

/** Narrows wire text to a known field-difference kind. */
export function isFieldDifferenceKind(value: string): value is FieldDifferenceKind {
  return DIFFERENCE_KINDS_SET.has(value);
}

/** Stable, human-readable classification name used across the screen. */
export function classificationLabel(value: ReconciliationClassificationValue): string {
  switch (value) {
    case "match":
      return "Match";
    case "missing_from_target":
      return "Missing from target";
    case "missing_from_source":
      return "Missing from source";
    case "field_mismatch":
      return "Field mismatch";
    case "duplicate_source":
      return "Duplicate source";
    case "duplicate_target":
      return "Duplicate target";
    case "duplicate_both":
      return "Duplicate both";
  }
}

/** Labels in the exact order of `RECONCILIATION_CLASSIFICATIONS`. */
export const CLASSIFICATION_LABELS: readonly string[] =
  RECONCILIATION_CLASSIFICATIONS.map((value) => classificationLabel(value));

/**
 * Visual severity of one classification. The textual label carries the
 * meaning; the tone only adds scanability.
 */
export function classificationTone(
  value: ReconciliationClassificationValue,
): "neutral" | "info" | "success" | "warning" | "failure" {
  switch (value) {
    case "match":
      return "success";
    case "missing_from_target":
    case "missing_from_source":
      return "warning";
    case "field_mismatch":
      return "failure";
    case "duplicate_source":
    case "duplicate_target":
    case "duplicate_both":
      return "info";
  }
}

/** Badge presentation states keyed by classification tone. */
const TONE_TO_BADGE_STATE = {
  neutral: "neutral",
  info: "active",
  success: "verified",
  warning: "warning",
  failure: "failure",
} as const;

export type ClassificationBadgeState =
  (typeof TONE_TO_BADGE_STATE)[keyof typeof TONE_TO_BADGE_STATE];

/** Map a classification tone to the shared badge presentation state. */
export function classificationBadgeState(
  tone: keyof typeof TONE_TO_BADGE_STATE,
): ClassificationBadgeState {
  return TONE_TO_BADGE_STATE[tone];
}

/**
 * Checked conversion of a server-supplied classification to the contract's
 * closed set; null means the value is not part of the known set.
 */
export function toClassificationValue(
  value: string,
): ReconciliationClassificationValue | null {
  for (const candidate of RECONCILIATION_CLASSIFICATIONS) {
    if (candidate === value) {
      return candidate;
    }
  }
  return null;
}

/** Stable, human-readable field-difference name for evidence rows. */
export function differenceKindLabel(kind: FieldDifferenceKind): string {
  switch (kind) {
    case "value_mismatch":
      return "Value mismatch";
    case "missing_on_source":
      return "Missing on source";
    case "missing_on_target":
      return "Missing on target";
    case "null_on_source":
      return "Null on source";
    case "null_on_target":
      return "Null on target";
    case "type_mismatch":
      return "Type mismatch";
  }
}
