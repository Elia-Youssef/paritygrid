import { describe, expect, it } from "vitest";

import {
  CLASSIFICATION_LABELS,
  classificationLabel,
  classificationTone,
  differenceKindLabel,
  isFieldDifferenceKind,
  isReconciliationClassification,
  FIELD_DIFFERENCE_KINDS,
  RECONCILIATION_CLASSIFICATIONS,
  reconciliationConflictCount,
  SUGGESTED_RESOLUTIONS,
} from "./classifications";

describe("reconciliation classification closed set", () => {
  it("exposes exactly the seven backend classifications", () => {
    expect(RECONCILIATION_CLASSIFICATIONS).toEqual([
      "match",
      "missing_from_target",
      "missing_from_source",
      "field_mismatch",
      "duplicate_source",
      "duplicate_target",
      "duplicate_both",
    ]);
  });

  it("accepts every classification and rejects unknown values", () => {
    for (const value of RECONCILIATION_CLASSIFICATIONS) {
      expect(isReconciliationClassification(value)).toBe(true);
    }
    for (const unknown of ["alien_kind", "Match", "", "missing_from", "dup"]) {
      expect(isReconciliationClassification(unknown)).toBe(false);
    }
  });

  it("counts conflict records without treating matches as conflicts", () => {
    expect(
      reconciliationConflictCount({
        match: 9_994,
        missing_from_target: 1,
        missing_from_source: 1,
        field_mismatch: 1,
        duplicate_source: 1,
        duplicate_target: 1,
        duplicate_both: 1,
      }),
    ).toBe(6);
    expect(
      reconciliationConflictCount({
        match: 10_000,
        missing_from_target: 0,
        missing_from_source: 0,
        field_mismatch: 0,
        duplicate_source: 0,
        duplicate_target: 0,
        duplicate_both: 0,
      }),
    ).toBe(0);
  });

  it("exposes exactly the six field-difference kinds", () => {
    expect(FIELD_DIFFERENCE_KINDS).toEqual([
      "value_mismatch",
      "missing_on_source",
      "missing_on_target",
      "null_on_source",
      "null_on_target",
      "type_mismatch",
    ]);
    for (const kind of FIELD_DIFFERENCE_KINDS) {
      expect(isFieldDifferenceKind(kind)).toBe(true);
    }
    expect(isFieldDifferenceKind("renamed_field")).toBe(false);
    expect(isFieldDifferenceKind("")).toBe(false);
  });

  it("exposes exactly the five suggested resolutions", () => {
    expect(SUGGESTED_RESOLUTIONS).toEqual([
      "none",
      "create_target",
      "update_target",
      "review_target_only",
      "review_duplicates",
    ]);
  });
});

describe("classification labels and tones", () => {
  it("labels every classification deterministically", () => {
    expect(CLASSIFICATION_LABELS).toEqual([
      "Match",
      "Missing from target",
      "Missing from source",
      "Field mismatch",
      "Duplicate source",
      "Duplicate target",
      "Duplicate both",
    ]);
    expect(classificationLabel("missing_from_target")).toBe("Missing from target");
    // Repeated calls are stable: the label is a pure mapping.
    expect(classificationLabel("field_mismatch")).toBe(
      classificationLabel("field_mismatch"),
    );
  });

  it("assigns a tone from the closed tone set to every classification", () => {
    const tones = RECONCILIATION_CLASSIFICATIONS.map((value) =>
      classificationTone(value),
    );
    for (const tone of tones) {
      expect(["neutral", "info", "success", "warning", "failure"]).toContain(tone);
    }
    expect(classificationTone("match")).toBe("success");
    expect(classificationTone("field_mismatch")).toBe("failure");
    expect(classificationTone("missing_from_source")).toBe("warning");
    expect(classificationTone("duplicate_both")).toBe("info");
  });

  it("labels every field-difference kind deterministically", () => {
    expect(differenceKindLabel("value_mismatch")).toBe("Value mismatch");
    expect(differenceKindLabel("missing_on_source")).toBe("Missing on source");
    expect(differenceKindLabel("missing_on_target")).toBe("Missing on target");
    expect(differenceKindLabel("null_on_source")).toBe("Null on source");
    expect(differenceKindLabel("null_on_target")).toBe("Null on target");
    expect(differenceKindLabel("type_mismatch")).toBe("Type mismatch");
  });
});
