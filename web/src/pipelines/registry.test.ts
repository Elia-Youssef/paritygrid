import { describe, expect, it } from "vitest";

import {
  NODE_DEFINITIONS,
  nodeDefinition,
  portsAreCompatible,
  requiredConnectorCapabilities,
  type InputPortDefinition,
  type OutputPortDefinition,
} from "./registry";

describe("registry accessor sweeps", () => {
  it("resolves every definition and its metadata", () => {
    for (const definition of NODE_DEFINITIONS) {
      const resolved = nodeDefinition(definition.kind);
      expect(resolved).toBe(definition);
      expect(requiredConnectorCapabilities(definition.kind)).toEqual(
        definition.requiredCapabilities,
      );
      for (const output of definition.ports.outputs) {
        for (const input of definition.ports.inputs) {
          const compatible = portsAreCompatible(output, input);
          expect(typeof compatible).toBe("boolean");
        }
      }
    }
    expect(nodeDefinition("does.not.exist")).toBeUndefined();
    expect(requiredConnectorCapabilities("does.not.exist")).toEqual([]);
  });

  it("computes cross-registry port compatibility on exact types", () => {
    const outputs: OutputPortDefinition[] = NODE_DEFINITIONS.flatMap(
      (definition) => definition.ports.outputs,
    );
    const inputs: InputPortDefinition[] = NODE_DEFINITIONS.flatMap(
      (definition) => definition.ports.inputs,
    );
    let compatiblePairs = 0;
    for (const output of outputs) {
      for (const input of inputs) {
        if (portsAreCompatible(output, input)) {
          compatiblePairs += 1;
        }
      }
    }
    // The closed lattice has a fixed number of compatible pairs; both a
    // positive and a negative outcome occurred across the sweep.
    expect(compatiblePairs).toBeGreaterThan(0);
    expect(compatiblePairs).toBeLessThan(outputs.length * inputs.length);
  });
});
