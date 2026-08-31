/**
 * Flow wiring shared by the studio canvas and its node cards, kept out of
 * component files so fast refresh stays component-only.
 */
import type { Edge, Node } from "@xyflow/react";

import { PipelineNodeCard } from "./pipeline-node-card";

export interface PipelineNodeFlowData {
  readonly nodeId: string;
  readonly kind: string;
  readonly configurationValid: boolean;
  readonly errorCount: number;
  readonly connectorId: string | null;
  readonly connectorLabel: string | null;
  readonly runnerLabel: string;
  readonly retryLabel: string;
  readonly [key: string]: unknown;
}

export type PipelineFlowNode = Node<PipelineNodeFlowData, "pipeline-node">;

export type PipelineFlowEdge = Edge;

export const pipelineNodeTypes = { "pipeline-node": PipelineNodeCard };
