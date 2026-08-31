/**
 * Closed, exhaustive frontend mirror of the accepted backend node registry
 * (`src/paritygrid/application/planner/registry.py`, registry version 1).
 *
 * Every node kind the backend planner accepts is defined here exactly once
 * with its configuration schema, typed ports, connector requirement,
 * supported runners, and capability requirements. Nothing outside this
 * registry can be added to a draft, rendered on the canvas, inspected, or
 * published; unknown kinds are rejected at parse time rather than preserved
 * as editable "unknown" objects.
 *
 * When the backend registry changes, this file must change in the same
 * change-set; the exhaustive tests assert the counts and key invariants.
 */

export const NODE_REGISTRY_VERSION = 1;

/** Exact closed logical payload types exchanged between built-in nodes. */
export const PortValueType = {
  RawRecords: "records.raw",
  NormalizedRecords: "records.normalized",
  ValidatedRecords: "records.validated",
  PartitionedRecords: "records.partitioned",
  Reconciliation: "reconciliation.result",
  RepairPlan: "repair.plan",
  ApprovedRepairPlan: "repair.plan.approved",
  RepairResult: "repair.result",
  Verification: "verification.result",
} as const;
export type PortValueType = (typeof PortValueType)[keyof typeof PortValueType];

export type NodeRole =
  | "source"
  | "transform"
  | "reconciliation"
  | "repair_plan"
  | "approval"
  | "repair_effect"
  | "verification"
  | "export";

export type ConnectorRequirement = "none" | "source" | "target";

export type RunnerKind = "sequential" | "threaded" | "asyncio" | "process";

export type RetryBehavior = "never" | "connector";

/** Closed connector behaviors, mirroring the backend capability set. */
export const ConnectorCapability = {
  Read: "read",
  Write: "write",
  AsyncIo: "async_io",
  BlockingIo: "blocking_io",
  Idempotency: "idempotency",
  SchemaDiscovery: "schema_discovery",
} as const;
export type ConnectorCapability =
  (typeof ConnectorCapability)[keyof typeof ConnectorCapability];

export interface InputPortDefinition {
  readonly name: string;
  readonly accepts: readonly PortValueType[];
  readonly required: boolean;
  readonly maximumConnections: number;
}

export interface OutputPortDefinition {
  readonly name: string;
  readonly valueType: PortValueType;
}

export interface PortSchema {
  readonly inputs: readonly InputPortDefinition[];
  readonly outputs: readonly OutputPortDefinition[];
}

/** One exact scalar field in a versioned node configuration schema. */
export interface ConfigurationField {
  readonly name: string;
  readonly valueKind: "boolean" | "integer" | "text";
  readonly required: boolean;
  readonly minimum?: number;
  readonly maximum?: number;
}

export interface ConfigurationSchema {
  readonly version: number;
  readonly fields: readonly ConfigurationField[];
}

export interface NodeDefinition {
  readonly kind: string;
  readonly role: NodeRole;
  readonly configurationSchema: ConfigurationSchema;
  readonly ports: PortSchema;
  readonly connectorRequirement: ConnectorRequirement;
  /** Exact capability keys a bound connector must support. */
  readonly requiredCapabilities: readonly ConnectorCapability[];
  readonly supportedRunners: readonly RunnerKind[];
  readonly retryBehavior: RetryBehavior;
  readonly requiresIdempotency: boolean;
  /** Short human explanation used by the canvas and inspector. */
  readonly label: string;
  readonly description: string;
}

const recordsInput = (accepts: readonly PortValueType[]): InputPortDefinition[] => [
  { name: "records", accepts, required: true, maximumConnections: 1 },
];

const singleOutput = (
  name: string,
  valueType: PortValueType,
): OutputPortDefinition[] => [{ name, valueType }];

const field = (
  name: string,
  valueKind: ConfigurationField["valueKind"],
  limits: { minimum?: number; maximum?: number } = {},
): ConfigurationField => ({ name, valueKind, required: false, ...limits });

const configuration = (
  version: number,
  fields: readonly ConfigurationField[],
): ConfigurationSchema => ({
  version,
  fields,
});

const emptyConfigurationV1 = configuration(1, []);

const httpSourceConfigurationV1 = configuration(1, [
  field("page_size", "integer", { minimum: 1, maximum: 10_000 }),
]);

const csvSourceConfigurationV1 = configuration(1, [
  field("encoding", "text"),
  field("header", "boolean"),
]);

const jsonlSourceConfigurationV1 = configuration(1, [field("encoding", "text")]);

const partitionConfigurationV1 = configuration(1, [
  field("partition_count", "integer", { minimum: 1, maximum: 10_000 }),
]);

const exportConfigurationV1 = configuration(1, [field("compression", "text")]);

const standardRunners: readonly RunnerKind[] = ["asyncio", "sequential", "threaded"];
const cpuRunners: readonly RunnerKind[] = [
  "asyncio",
  "process",
  "sequential",
  "threaded",
];

/**
 * The exhaustive definitions. Sorted by kind for deterministic iteration;
 * the shapes mirror the backend `_definition(...)` call sites exactly.
 */
export const NODE_DEFINITIONS: readonly NodeDefinition[] = [
  {
    kind: "export.parquet",
    role: "export",
    configurationSchema: exportConfigurationV1,
    ports: {
      inputs: recordsInput([
        PortValueType.RawRecords,
        PortValueType.NormalizedRecords,
        PortValueType.ValidatedRecords,
        PortValueType.PartitionedRecords,
      ]),
      outputs: [],
    },
    connectorRequirement: "none",
    requiredCapabilities: [],
    supportedRunners: standardRunners,
    retryBehavior: "never",
    requiresIdempotency: false,
    label: "Export Parquet",
    description: "Writes verified records to a content-addressed Parquet artifact.",
  },
  {
    kind: "reconcile.target",
    role: "reconciliation",
    configurationSchema: emptyConfigurationV1,
    ports: {
      inputs: recordsInput([
        PortValueType.NormalizedRecords,
        PortValueType.ValidatedRecords,
        PortValueType.PartitionedRecords,
      ]),
      outputs: singleOutput("reconciliation", PortValueType.Reconciliation),
    },
    connectorRequirement: "target",
    requiredCapabilities: [ConnectorCapability.Read],
    supportedRunners: standardRunners,
    retryBehavior: "connector",
    requiresIdempotency: false,
    label: "Reconcile with target",
    description: "Compares pipeline records against the bound target connector.",
  },
  {
    kind: "repair.apply",
    role: "repair_effect",
    configurationSchema: emptyConfigurationV1,
    ports: {
      inputs: [
        {
          name: "approved-plan",
          accepts: [PortValueType.ApprovedRepairPlan],
          required: true,
          maximumConnections: 1,
        },
      ],
      outputs: singleOutput("repair-result", PortValueType.RepairResult),
    },
    connectorRequirement: "target",
    requiredCapabilities: [ConnectorCapability.Idempotency, ConnectorCapability.Write],
    supportedRunners: standardRunners,
    retryBehavior: "connector",
    requiresIdempotency: true,
    label: "Apply repairs",
    description: "Applies the approved repair plan idempotently.",
  },
  {
    kind: "repair.approval",
    role: "approval",
    configurationSchema: emptyConfigurationV1,
    ports: {
      inputs: [
        {
          name: "repair-plan",
          accepts: [PortValueType.RepairPlan],
          required: true,
          maximumConnections: 1,
        },
      ],
      outputs: singleOutput("approved-plan", PortValueType.ApprovedRepairPlan),
    },
    connectorRequirement: "none",
    requiredCapabilities: [],
    supportedRunners: standardRunners,
    retryBehavior: "never",
    requiresIdempotency: false,
    label: "Approval gate",
    description: "Explicit human approval before any repair effect.",
  },
  {
    kind: "repair.generate",
    role: "repair_plan",
    configurationSchema: emptyConfigurationV1,
    ports: {
      inputs: [
        {
          name: "reconciliation",
          accepts: [PortValueType.Reconciliation],
          required: true,
          maximumConnections: 1,
        },
      ],
      outputs: singleOutput("repair-plan", PortValueType.RepairPlan),
    },
    connectorRequirement: "none",
    requiredCapabilities: [],
    supportedRunners: cpuRunners,
    retryBehavior: "never",
    requiresIdempotency: false,
    label: "Generate repair plan",
    description: "Plans bounded non-destructive repairs from the reconciliation.",
  },
  {
    kind: "source.csv",
    role: "source",
    configurationSchema: csvSourceConfigurationV1,
    ports: { inputs: [], outputs: singleOutput("records", PortValueType.RawRecords) },
    connectorRequirement: "source",
    requiredCapabilities: [ConnectorCapability.Read],
    supportedRunners: standardRunners,
    retryBehavior: "connector",
    requiresIdempotency: false,
    label: "CSV source",
    description: "Reads CSV files through a bound source connector.",
  },
  {
    kind: "source.http.async",
    role: "source",
    configurationSchema: httpSourceConfigurationV1,
    ports: { inputs: [], outputs: singleOutput("records", PortValueType.RawRecords) },
    connectorRequirement: "source",
    requiredCapabilities: [ConnectorCapability.AsyncIo, ConnectorCapability.Read],
    supportedRunners: standardRunners,
    retryBehavior: "connector",
    requiresIdempotency: false,
    label: "Async HTTP source",
    description: "Reads a paginated asynchronous HTTP API through a connector.",
  },
  {
    kind: "source.http.blocking",
    role: "source",
    configurationSchema: httpSourceConfigurationV1,
    ports: { inputs: [], outputs: singleOutput("records", PortValueType.RawRecords) },
    connectorRequirement: "source",
    requiredCapabilities: [ConnectorCapability.BlockingIo, ConnectorCapability.Read],
    supportedRunners: standardRunners,
    retryBehavior: "connector",
    requiresIdempotency: false,
    label: "Blocking HTTP source",
    description: "Reads a blocking HTTP API through a bound connector.",
  },
  {
    kind: "source.jsonl",
    role: "source",
    configurationSchema: jsonlSourceConfigurationV1,
    ports: { inputs: [], outputs: singleOutput("records", PortValueType.RawRecords) },
    connectorRequirement: "source",
    requiredCapabilities: [ConnectorCapability.Read],
    supportedRunners: standardRunners,
    retryBehavior: "connector",
    requiresIdempotency: false,
    label: "JSON Lines source",
    description: "Reads JSON Lines files through a bound source connector.",
  },
  {
    kind: "transform.normalize",
    role: "transform",
    configurationSchema: emptyConfigurationV1,
    ports: {
      inputs: recordsInput([PortValueType.RawRecords]),
      outputs: singleOutput("records", PortValueType.NormalizedRecords),
    },
    connectorRequirement: "none",
    requiredCapabilities: [],
    supportedRunners: cpuRunners,
    retryBehavior: "never",
    requiresIdempotency: false,
    label: "Normalize records",
    description: "Normalizes raw records into the canonical record shape.",
  },
  {
    kind: "transform.partition",
    role: "transform",
    configurationSchema: partitionConfigurationV1,
    ports: {
      inputs: recordsInput([PortValueType.ValidatedRecords]),
      outputs: singleOutput("records", PortValueType.PartitionedRecords),
    },
    connectorRequirement: "none",
    requiredCapabilities: [],
    supportedRunners: cpuRunners,
    retryBehavior: "never",
    requiresIdempotency: false,
    label: "Partition records",
    description: "Partitions validated records into bounded work units.",
  },
  {
    kind: "transform.validate",
    role: "transform",
    configurationSchema: emptyConfigurationV1,
    ports: {
      inputs: recordsInput([PortValueType.NormalizedRecords]),
      outputs: singleOutput("records", PortValueType.ValidatedRecords),
    },
    connectorRequirement: "none",
    requiredCapabilities: [],
    supportedRunners: cpuRunners,
    retryBehavior: "never",
    requiresIdempotency: false,
    label: "Validate records",
    description: "Quarantines invalid records without losing valid work.",
  },
  {
    kind: "verify.target",
    role: "verification",
    configurationSchema: emptyConfigurationV1,
    ports: {
      inputs: [
        {
          name: "repair-result",
          accepts: [PortValueType.RepairResult],
          required: true,
          maximumConnections: 1,
        },
      ],
      outputs: singleOutput("verification", PortValueType.Verification),
    },
    connectorRequirement: "target",
    requiredCapabilities: [ConnectorCapability.Read],
    supportedRunners: standardRunners,
    retryBehavior: "connector",
    requiresIdempotency: false,
    label: "Verify target state",
    description: "Proves the target reached the expected logical state.",
  },
];

const BY_KIND = new Map(
  NODE_DEFINITIONS.map((definition) => [definition.kind, definition]),
);

/** Resolve one definition, or undefined for kinds outside the registry. */
export function nodeDefinition(kind: string): NodeDefinition | undefined {
  return BY_KIND.get(kind);
}

/** True when the port type of `source` may feed `target`. */
export function portsAreCompatible(
  source: OutputPortDefinition,
  target: InputPortDefinition,
): boolean {
  return target.accepts.includes(source.valueType);
}

/** Closed connector capability requirement for one registered kind. */
export function requiredConnectorCapabilities(
  kind: string,
): readonly ConnectorCapability[] {
  return nodeDefinition(kind)?.requiredCapabilities ?? [];
}
